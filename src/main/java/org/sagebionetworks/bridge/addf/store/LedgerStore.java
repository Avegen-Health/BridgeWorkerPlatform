package org.sagebionetworks.bridge.addf.store;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashSet;
import java.util.Set;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;

/**
 * ADDF §3.6 — the idempotency ledger, backed by keyed objects under the export store's {@code _ledger/} prefix
 * (decision: S3-keyed, dependency-free — no DynamoDB). Idempotency is <b>not uniform</b> across tables:
 *
 * <ul>
 *   <li><b>record scope</b> (6 activity tables + {@code file_records}, keyed by {@code record_id}) &mdash;
 *       <b>presence-skip</b>: one row per record, skip on redelivery.</li>
 *   <li><b>version scope</b> ({@code participant_versions}, keyed by {@code health_code}+{@code participant_version})
 *       &mdash; presence-skip: a version is immutable once written (used by Phase 3b).</li>
 *   <li><b>{@code demographics}</b> &mdash; <b>merge-always</b>: it never consults this ledger; its idempotency comes
 *       from the merge at publish being commutative/repeatable (§3.5.1 / §3.7.2).</li>
 *   <li><b>raw scope</b> (the {@code raw/<date>/<record>-<assessment>.zip} archives, keyed by that relative key)
 *       &mdash; presence-skip, marked by the <b>publish</b> worker after the Azure upload confirms, so each raw
 *       archive is delivered to the staging container exactly once (§4.3.4).</li>
 * </ul>
 *
 * <p>This ledger is entirely ADDF-internal. It must <b>never</b> flag Bridge's {@code HealthDataRecordEx3} — that would
 * be the dual-write bug.</p>
 */
@Component
public class LedgerStore {
    private static final Logger LOG = LoggerFactory.getLogger(LedgerStore.class);

    static final String ROOT_PREFIX = "biaffect-3/";
    static final String SCOPE_RECORD = "record";
    static final String SCOPE_VERSION = "version";
    static final String SCOPE_RAW = "raw";
    static final String RAW_RELATIVE_PREFIX = "raw/";
    static final String CONFIG_KEY_EXPORTSTORE_BUCKET = "addf.exportstore.bucket";

    private AmazonS3 s3Client;
    private String bucket;

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.bucket = config.get(CONFIG_KEY_EXPORTSTORE_BUCKET);
    }

    @Autowired
    public final void setAddfS3Client(@Qualifier("addfS3Client") AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    /** Presence-skip check for an activity/file_records record. */
    public boolean containsRecord(String recordId) {
        return exists(ledgerKey(SCOPE_RECORD, recordId));
    }

    /** Mark a record fully processed (its content row + manifest row staged). */
    public void markRecord(String recordId) {
        mark(ledgerKey(SCOPE_RECORD, recordId));
    }

    /** Presence-skip check for a participant version (Phase 3b). */
    public boolean containsVersion(String healthCode, int participantVersion) {
        return exists(ledgerKey(SCOPE_VERSION, healthCode + "/" + participantVersion));
    }

    /** Mark a participant version written (Phase 3b). */
    public void markVersion(String healthCode, int participantVersion) {
        mark(ledgerKey(SCOPE_VERSION, healthCode + "/" + participantVersion));
    }

    /**
     * Every raw archive already delivered to the Azure staging container, as {@code file_records.file_name}-style
     * relative keys ({@code raw/<date>/<record>-<assessment>.zip}) — read once per publish (§4.3.4).
     *
     * <p>A paged LIST rather than a HEAD per candidate: publish re-derives its deliverable set from the whole
     * consolidated {@code file_records} table every run, so a per-row {@code doesObjectExist} would cost one S3 call
     * per record per day forever. One LIST per 1000 markers is O(delivered/1000) instead.</p>
     */
    public Set<String> listDeliveredRaw() {
        String prefix = ledgerKey(SCOPE_RAW, "");
        Set<String> delivered = new LinkedHashSet<>();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(req);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                String suffix = summary.getKey().substring(prefix.length());
                if (!suffix.isEmpty() && !suffix.endsWith("/")) {
                    delivered.add(RAW_RELATIVE_PREFIX + suffix);
                }
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return delivered;
    }

    /**
     * Mark one raw archive delivered to Azure. Called by the publish worker's {@code commit} — i.e. <b>only after</b>
     * the upload confirms — so a crash mid-upload replays the archive rather than silently dropping it (§4.5).
     */
    public void markRawDelivered(String rawRelativeKey) {
        mark(ledgerKey(SCOPE_RAW, stripRawPrefix(rawRelativeKey)));
    }

    /** {@code raw/2026-08-15/rec-PHQ-9.zip} → {@code 2026-08-15/rec-PHQ-9.zip} (the ledger scope supplies the rest). */
    private static String stripRawPrefix(String rawRelativeKey) {
        return rawRelativeKey.startsWith(RAW_RELATIVE_PREFIX)
                ? rawRelativeKey.substring(RAW_RELATIVE_PREFIX.length())
                : rawRelativeKey;
    }

    private String ledgerKey(String scope, String key) {
        return ROOT_PREFIX + "_ledger/" + scope + "/" + key;
    }

    private boolean exists(String key) {
        return s3Client.doesObjectExist(bucket, key);
    }

    private void mark(String key) {
        byte[] body = new byte[0];
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        metadata.setContentLength(body.length);
        s3Client.putObject(new PutObjectRequest(bucket, key, new ByteArrayInputStream(body), metadata));
        LOG.debug("ADDF ledger marked: {}", key);
    }
}
