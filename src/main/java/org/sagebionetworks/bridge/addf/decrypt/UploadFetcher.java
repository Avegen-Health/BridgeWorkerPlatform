package org.sagebionetworks.bridge.addf.decrypt;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.CertificateEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.bouncycastle.cms.CMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.crypto.WrongEncryptionKeyException;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

/**
 * ADDF §3.4 — re-fetches the raw encrypted upload from the upload bucket, CMS-decrypts it, and unzips the archive,
 * reusing the <em>same</em> Spring beans Exporter 3.0 uses ({@link S3Helper}, the {@code cmsEncryptorCache},
 * {@link ZipHelper}, {@link FileHelper}) — but never touching E3 code or its decrypted output.
 *
 * <p>ADDF decrypts <b>independently</b> rather than sharing E3's output; this is the isolation invariant
 * (architecture §6). The small duplicate-decrypt cost is accepted so the two pipelines never share mutable state.</p>
 */
@Component
public class UploadFetcher {
    private static final Logger LOG = LoggerFactory.getLogger(UploadFetcher.class);

    static final String CONFIG_KEY_UPLOAD_BUCKET = "upload.bucket";

    private BridgeHelper bridgeHelper;
    private LoadingCache<String, CmsEncryptor> cmsEncryptorCache;
    private FileHelper fileHelper;
    private S3Helper s3Helper;
    private String uploadBucket;
    private ZipHelper zipHelper;

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.uploadBucket = config.get(CONFIG_KEY_UPLOAD_BUCKET);
    }

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setCmsEncryptorCache(LoadingCache<String, CmsEncryptor> cmsEncryptorCache) {
        this.cmsEncryptorCache = cmsEncryptorCache;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Autowired
    public final void setZipHelper(ZipHelper zipHelper) {
        this.zipHelper = zipHelper;
    }

    /**
     * Fetch + decrypt + unzip the archive for a record into {@code tempDir} (owned and cleaned by the caller).
     *
     * @throws PollSqsWorkerBadRequestException wrong CMS key / missing encryptor / unreadable archive — routes to DLQ.
     * @throws WorkerException                  unexpected decrypt failure — surfaced as an error.
     * @throws IOException                      transient S3/file failure — the processor maps this to a retry.
     */
    public DecryptedArchive fetch(App app, HealthDataRecordEx3 record, File tempDir)
            throws IOException, PollSqsWorkerBadRequestException, WorkerException {
        String appId = app.getIdentifier();
        String recordId = record.getId();
        // In Exporter 3.0, upload ID == record ID.
        Upload upload = bridgeHelper.getUploadByUploadId(recordId);

        // Download from the upload bucket.
        File downloadedFile = fileHelper.newFile(tempDir, recordId);
        s3Helper.downloadS3File(uploadBucket, recordId, downloadedFile);

        // Decrypt if needed, producing the archive zip on disk.
        File archiveFile;
        if (upload.isEncrypted() == null || upload.isEncrypted()) {
            archiveFile = decrypt(appId, recordId, downloadedFile, tempDir);
        } else {
            archiveFile = downloadedFile;
        }

        // Unzip and parse JSON payloads.
        File unzipDir = fileHelper.createTempDir();
        Map<String, File> unzipped;
        try {
            unzipped = zipHelper.unzip(archiveFile, unzipDir);
        } catch (Exception ex) {
            throw new PollSqsWorkerBadRequestException("Unable to unzip archive for app " + appId + " record " +
                    recordId + ": " + ex.getMessage(), ex);
        }

        if (unzipped.isEmpty()) {
            // ZipInputStream does not object to being handed something that isn't a zip — it simply reports no
            // entries, so a truncated or corrupt archive "unzips" successfully to nothing. Without this check the
            // record would be flattened to a file_records row with no content row and then marked done in the ledger,
            // which is unrecoverable: it can never be retried, and nothing downstream distinguishes it from a
            // genuinely unmapped assessment type. A real Bridge upload archive always carries at least one member.
            throw new PollSqsWorkerBadRequestException("Empty or unreadable archive for app " + appId + " record " +
                    recordId);
        }

        Map<String, JsonNode> jsonFiles = new HashMap<>();
        for (Map.Entry<String, File> entry : unzipped.entrySet()) {
            String name = entry.getKey();
            if (!name.toLowerCase().endsWith(".json")) {
                continue;
            }
            try (InputStream in = new BufferedInputStream(fileHelper.getInputStream(entry.getValue()))) {
                jsonFiles.put(name, DefaultObjectMapper.INSTANCE.readTree(in));
            } catch (IOException ex) {
                // A single malformed JSON member shouldn't fail the whole record; the flattener will treat it as
                // absent. Log and continue.
                LOG.warn("Skipping unparseable JSON file " + name + " for app " + appId + " record " + recordId +
                        ": " + ex.getMessage());
            }
        }

        return new DecryptedArchive(record, upload, archiveFile, unzipped, jsonFiles);
    }

    private File decrypt(String appId, String recordId, File downloadedFile, File tempDir)
            throws IOException, PollSqsWorkerBadRequestException, WorkerException {
        CmsEncryptor encryptor;
        try {
            encryptor = cmsEncryptorCache.get(appId);
        } catch (CacheLoader.InvalidCacheLoadException ex) {
            throw new PollSqsWorkerBadRequestException("No encryptor for app " + appId, ex);
        } catch (UncheckedExecutionException | ExecutionException ex) {
            throw new WorkerException(ex);
        }

        File decryptedFile = fileHelper.newFile(tempDir, recordId + "-decrypted");
        try (InputStream inputFileStream = new BufferedInputStream(fileHelper.getInputStream(downloadedFile));
                InputStream decryptedInputFileStream = encryptor.decrypt(inputFileStream);
                OutputStream outputFileStream = new BufferedOutputStream(fileHelper.getOutputStream(decryptedFile))) {
            ByteStreams.copy(decryptedInputFileStream, outputFileStream);
        } catch (WrongEncryptionKeyException ex) {
            throw new PollSqsWorkerBadRequestException("Wrong encryption key for app " + appId + " record " +
                    recordId, ex);
        } catch (CertificateEncodingException | CMSException ex) {
            throw new WorkerException(ex);
        }
        return decryptedFile;
    }
}
