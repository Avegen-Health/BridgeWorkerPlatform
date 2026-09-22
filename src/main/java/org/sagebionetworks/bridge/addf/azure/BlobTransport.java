package org.sagebionetworks.bridge.addf.azure;

import java.util.List;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.publish.PublishedBlob;

/**
 * ADDF §4.4.2(a) — the recommended <b>in-JVM</b> Azure transport. Uploads the snapshot delta blob-by-blob via the
 * Azure Storage SDK ({@code azure-storage-blob}, a plain Maven jar), using the container URL + SAS from
 * {@link BlobCredentialProvider}. This removes the only novel <em>native</em> runtime dependency the plan otherwise
 * carried (no AzCopy binary, no {@code .ebextensions} install, no {@code ProcessBuilder} exit-code parsing) — matching
 * the fact that BWP runs on the managed Corretto/Tomcat EB platform, not Docker.
 *
 * <p>{@link SnapshotDeltaBuilder} already computes the exact changed-file set, so {@code azcopy sync}'s recursive
 * self-heal is unnecessary: a targeted per-blob upload of that delta replaces it. Overwrite-in-place ({@code
 * overwrite=true}) is idempotent, so a re-run re-uploads the same delta cleanly — crash-safety (§4.5) still holds.</p>
 */
@Component
public class BlobTransport {
    private static final Logger LOG = LoggerFactory.getLogger(BlobTransport.class);

    // Guard against publishing before ADDI has supplied the real container (the .conf placeholder default).
    private static final String PLACEHOLDER = "PLACEHOLDER";

    // Bounded per-blob retry: absorb transient blob-store blips so a single flaky upload doesn't fail the whole day
    // (which would otherwise redeliver the message and replay the entire snapshot). A daily off-peak job can afford it.
    static final int MAX_ATTEMPTS = 3;
    static final long BACKOFF_MILLIS = 1000L;

    private BlobCredentialProvider credentialProvider;

    @Autowired
    public final void setBlobCredentialProvider(BlobCredentialProvider credentialProvider) {
        this.credentialProvider = credentialProvider;
    }

    /**
     * Upload every {@link PublishedBlob} in the delta to the staging container at its {@link PublishedBlob#getKey()}
     * (which mirrors the export-store delivery tree). Sequential — publish is a daily, I/O-bound, off-peak job of
     * minutes; per-blob throughput tuning (bounded pool) is added only on measured need (§4.4.2). Each blob is retried
     * up to {@link #MAX_ATTEMPTS} times to absorb transient blips; if a blob still fails it throws, so the worker can
     * map it to a retryable and the whole day re-runs idempotently.
     */
    public void upload(List<PublishedBlob> delta) {
        if (delta.isEmpty()) {
            LOG.info("ADDF publish: empty delta, nothing to upload");
            return;
        }
        BlobContainerClient containerClient = buildContainerClient();
        for (PublishedBlob blob : delta) {
            uploadOne(containerClient, blob);
        }
        LOG.info("ADDF publish: uploaded {} blob(s) to staging container", delta.size());
    }

    /** Upload a single blob with a bounded retry + linear backoff; rethrows the last failure once attempts run out. */
    private void uploadOne(BlobContainerClient containerClient, PublishedBlob blob) {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                doUpload(containerClient, blob);
                LOG.info("ADDF publish: uploaded blob {}{}", blob.getKey(),
                        attempt > 1 ? " (attempt " + attempt + ")" : "");
                return;
            } catch (RuntimeException ex) {
                last = ex;
                LOG.warn("ADDF publish: upload attempt {}/{} failed for blob {}: {}",
                        attempt, MAX_ATTEMPTS, blob.getKey(), ex.getMessage());
                if (attempt < MAX_ATTEMPTS) {
                    sleepBackoff(attempt);
                }
            }
        }
        throw last;
    }

    /** The single real SDK call — package-private so tests can intercept it without a live Azure container. */
    void doUpload(BlobContainerClient containerClient, PublishedBlob blob) {
        containerClient.getBlobClient(blob.getKey())
                .uploadFromFile(blob.getLocalFile().getAbsolutePath(), true);
    }

    /** Package-private so tests can override to a no-op (skip the real sleep between retry attempts). */
    void sleepBackoff(int attempt) {
        try {
            Thread.sleep(BACKOFF_MILLIS * attempt);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during ADDF blob-upload backoff", ie);
        }
    }

    BlobContainerClient buildContainerClient() {
        String containerUrl = credentialProvider.getContainerUrl();
        String sasToken = credentialProvider.getSasToken();
        if (containerUrl == null || containerUrl.isEmpty() || containerUrl.contains(PLACEHOLDER)) {
            throw new IllegalStateException("ADDF Azure container URL not configured (placeholder still in place) — "
                    + "publish blocked until ADDI supplies the real container");
        }
        if (sasToken == null || sasToken.isEmpty()) {
            throw new IllegalStateException("ADDF Azure SAS token not configured — publish blocked");
        }
        // BlobContainerClientBuilder wants the SAS as a query string; tolerate a leading '?'.
        String sas = sasToken.startsWith("?") ? sasToken.substring(1) : sasToken;
        return new BlobContainerClientBuilder()
                .endpoint(containerUrl)
                .sasToken(sas)
                .buildClient();
    }
}
