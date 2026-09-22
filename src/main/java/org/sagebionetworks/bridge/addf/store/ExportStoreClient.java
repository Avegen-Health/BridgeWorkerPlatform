package org.sagebionetworks.bridge.addf.store;

import java.io.File;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.config.Config;

/**
 * ADDF §3.7 — the write interface to the export-store bucket, bound to the {@code biaffect-3/} delivery root. The
 * accumulate worker writes only <b>immutable per-record objects</b> here; it <b>never</b> read-modify-writes a
 * consolidated {@code <table>.parquet} (that is the publish worker's sole job, §3.7.1). Unique-key staging is what
 * makes concurrent per-record processing on the shared 12-thread pool safe — no lost-update race, no O(N²) rewrite.
 *
 * <p>The stage-date partition is computed per call (current UTC date) — a housekeeping partition distinct from the
 * publish {@code snapshotDate}. The merge methods described in §3.7.2 ({@code mergeParticipantRow},
 * {@code upsertVersionRow}) run in the publish worker (Phase 4), not here.</p>
 */
@Component
public class ExportStoreClient {
    private static final Logger LOG = LoggerFactory.getLogger(ExportStoreClient.class);

    static final String ROOT_PREFIX = "biaffect-3/";
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

    /**
     * Stage one record's row as an immutable per-record object at
     * {@code biaffect-3/_staging/<table>/<stageDate>/<recordId>.parquet}. The object name is always the record id (even
     * for {@code demographics}, whose merge identity is {@code health_code} inside the row) so two source uploads for
     * the same participant never clobber each other before publish.
     */
    public void stageTableRow(String table, String recordId, File parquetFile) {
        String key = ROOT_PREFIX + "_staging/" + table + "/" + AddfDateUtils.todayUtcDate() + "/" + recordId
                + ".parquet";
        putFile(key, parquetFile);
        LOG.info("ADDF staged row: table={} record={} key={}", table, recordId, key);
    }

    /**
     * Stage one participant-version row (Phase 3b) as an immutable per-version object at
     * {@code biaffect-3/_staging/participant_versions/<stageDate>/<healthCode>_<version>.parquet}. Publish upserts the
     * staged partials into {@code participant_versions.parquet} and derives {@code participants_current} (§3.7.2). A
     * version is immutable, so a repeat key is a harmless overwrite of identical content.
     */
    public void stageVersionRow(String healthCode, int participantVersion, File parquetFile) {
        String key = ROOT_PREFIX + "_staging/" + org.sagebionetworks.bridge.addf.transform.AddfTables.PARTICIPANT_VERSIONS
                + "/" + AddfDateUtils.todayUtcDate() + "/" + healthCode + "_" + participantVersion + ".parquet";
        putFile(key, parquetFile);
        LOG.info("ADDF staged participant_version: healthCode={} version={} key={}", healthCode, participantVersion,
                key);
    }

    /**
     * Mark a participant for tombstone/compaction at the next publish (§3b.3 withdrawal path). Writing a marker under
     * {@code _tombstone/<healthCode>} lets the publish worker compact that participant's rows without exporting a
     * NO_SHARING version as data.
     */
    public void markTombstone(String healthCode) {
        String key = ROOT_PREFIX + "_tombstone/" + healthCode;
        putEmpty(key);
        LOG.info("ADDF tombstone marked for healthCode={} (compaction at next publish)", healthCode);
    }

    /**
     * Copy the raw archive verbatim to {@code biaffect-3/raw/<uploadDate>/<recordId>-<assessment>.zip} (immutable —
     * never overwrite an existing key). Returns the key <em>relative to the delivery root</em> for the
     * {@code file_records.file_name} column (e.g. {@code raw/2026-08-15/rec-...-PHQ-9.zip}).
     */
    public String putRaw(String uploadDate, String recordId, String assessment, File archiveFile) {
        String relativeKey = "raw/" + uploadDate + "/" + recordId + "-" + sanitize(assessment) + ".zip";
        String key = ROOT_PREFIX + relativeKey;
        if (s3Client.doesObjectExist(bucket, key)) {
            LOG.info("ADDF raw already present, not overwriting: {}", key);
            return relativeKey;
        }
        putFile(key, archiveFile);
        LOG.info("ADDF raw stored: {}", key);
        return relativeKey;
    }

    private void putFile(String key, File file) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        s3Client.putObject(new PutObjectRequest(bucket, key, file).withMetadata(metadata));
    }

    private void putEmpty(String key) {
        byte[] body = new byte[0];
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        metadata.setContentLength(body.length);
        s3Client.putObject(new PutObjectRequest(bucket, key, new java.io.ByteArrayInputStream(body), metadata));
    }

    private static String sanitize(String assessment) {
        if (assessment == null || assessment.isEmpty()) {
            return "unknown";
        }
        // Keep it key-friendly; the golden raw names use the assessment id verbatim (e.g. PHQ-9).
        return assessment.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    public String getBucket() {
        return bucket;
    }
}
