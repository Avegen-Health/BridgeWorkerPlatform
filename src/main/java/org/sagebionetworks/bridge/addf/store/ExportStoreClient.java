package org.sagebionetworks.bridge.addf.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
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

import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
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
    static final String STAGING_PREFIX = ROOT_PREFIX + "_staging/";
    static final String TOMBSTONE_PREFIX = ROOT_PREFIX + "_tombstone/";
    static final String PENDING_RAW_PREFIX = ROOT_PREFIX + "_pending_raw/";
    /** Public because the manifest gate (§7) recovers a table name from a delivery key rather than restating it. */
    public static final String CURRENT_TABLES_PREFIX = "current/tables/";
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
        String key = ROOT_PREFIX + "_staging/" + AddfTables.PARTICIPANT_VERSIONS
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

    // ---------------------------------------------------------------------------------------------------------------
    // Publish-side read/write (§4.3). The publish worker is the SOLE writer of every consolidated table file; these
    // helpers let SnapshotDeltaBuilder list the staged per-record objects, read them back, and coalesce them into the
    // consolidated targets. All keys returned by the list methods are full bucket keys (they include ROOT_PREFIX).
    // ---------------------------------------------------------------------------------------------------------------

    /** The consolidated single-file key for a table, relative to the bucket root: {@code biaffect-3/current/tables/<table>.parquet}. */
    public String consolidatedKey(String table) {
        return ROOT_PREFIX + CURRENT_TABLES_PREFIX + table + ".parquet";
    }

    /**
     * The keyboard part-file key for a month + publish label:
     * {@code biaffect-3/current/tables/keyboard_sessions/month=YYYY-MM/part-<snapshotDate>.parquet} (§4.3.2).
     *
     * <p>Sits under {@code current/tables/} like every other table. {@code keyboard_sessions} is a month-partitioned
     * <i>dataset</i> rather than a single file, but it is still one of the ten delivered tables and readers treat the
     * folder as one table. It previously hung off the delivery root, one level up, so a consumer pointed at
     * {@code current/tables/} — the stable path the delivery format tells researchers to use — silently saw nine
     * tables and missed the highest-volume one. The Azure blob name is this key, so the gap reached the partner.</p>
     */
    public String keyboardPartKey(String month, String snapshotDate) {
        return ROOT_PREFIX + CURRENT_TABLES_PREFIX + AddfTables.KEYBOARD_SESSIONS + "/month=" + month + "/part-"
                + snapshotDate + ".parquet";
    }

    /**
     * Full bucket key for a raw archive from its delivery-root-relative form — the inverse of what {@link #putRaw}
     * returns and what {@code file_records.file_name} stores: {@code raw/<date>/<rec>-<item>.zip} →
     * {@code biaffect-3/raw/<date>/<rec>-<item>.zip}.
     */
    public String rawKey(String rawRelativeKey) {
        return ROOT_PREFIX + rawRelativeKey;
    }

    /** True when an object exists at this full bucket key. */
    public boolean objectExists(String key) {
        return s3Client.doesObjectExist(bucket, key);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Pending-raw retry queue (§4.3.4). A raw archive whose upload fails is parked here rather than failing the run;
    // the next publish picks it up alongside that run's newly-consumed archives. Steady state the prefix is empty, so
    // the extra LIST costs one empty-page round trip. It is also the seam the one-off historical backfill uses: seed
    // keys here in controlled batches and the normal publish path drains them (see RawArchiveDelivery).
    // ---------------------------------------------------------------------------------------------------------------

    /** Park a raw archive for retry on the next publish. {@code healthCode} is carried so the retry stays attributable. */
    public void markRawPending(String healthCode, String rawRelativeKey) {
        putEmpty(pendingRawKey(healthCode, rawRelativeKey));
        LOG.info("ADDF raw delivery parked for retry: {}", rawRelativeKey);
    }

    /** Clear a parked raw archive once it has been delivered. */
    public void clearRawPending(String healthCode, String rawRelativeKey) {
        s3Client.deleteObject(bucket, pendingRawKey(healthCode, rawRelativeKey));
    }

    /**
     * Every raw archive currently parked for retry, as {@code healthCode + "\t" + rawRelativeKey} pairs. Returns the
     * raw strings so the caller owns parsing; {@code RawArchiveDelivery} converts them back into candidates.
     */
    public List<String> listRawPending() {
        List<String> pending = new ArrayList<>();
        for (String key : listKeys(PENDING_RAW_PREFIX)) {
            String suffix = key.substring(PENDING_RAW_PREFIX.length());
            int slash = suffix.indexOf('/');
            if (slash > 0 && slash < suffix.length() - 1) {
                pending.add(suffix.substring(0, slash) + "\t" + "raw/" + suffix.substring(slash + 1));
            }
        }
        return pending;
    }

    private String pendingRawKey(String healthCode, String rawRelativeKey) {
        String suffix = rawRelativeKey.startsWith("raw/") ? rawRelativeKey.substring(4) : rawRelativeKey;
        return PENDING_RAW_PREFIX + healthCode + "/" + suffix;
    }

    /** List the staged per-record object keys under {@code _staging/<table>/} (all stage-date partitions), paged. */
    public List<String> listStaged(String table) {
        return listKeys(STAGING_PREFIX + table + "/");
    }

    /**
     * List every keyboard month-part already in the delivery tree. {@code keyboard_sessions} is the one table with no
     * consolidated single file, so {@link #consolidatedExists} can never answer "is this table present?" for it — the
     * manifest gate (§7) uses this instead.
     *
     * <p>The prefix is <b>derived from {@link #keyboardPartKey}</b> rather than spelled out again. Restating it would
     * make the two silently disagree the next time the keyboard dataset moves — and it has moved once already, from
     * the delivery root to {@code current/tables/}. A stale prefix here does not fail loudly: it returns an empty
     * list, which the gate reads as "keyboard_sessions is absent" and blocks every publish.</p>
     */
    public List<String> listKeyboardParts() {
        String probe = keyboardPartKey("", "");
        return listKeys(probe.substring(0, probe.indexOf("month=")));
    }

    /** List tombstoned health codes (the basename under {@code _tombstone/}) — participants withdrawn since last publish (§3b.3). */
    public List<String> listTombstonedHealthCodes() {
        List<String> healthCodes = new ArrayList<>();
        for (String key : listKeys(TOMBSTONE_PREFIX)) {
            String hc = key.substring(TOMBSTONE_PREFIX.length());
            if (!hc.isEmpty()) {
                healthCodes.add(hc);
            }
        }
        return healthCodes;
    }

    private List<String> listKeys(String prefix) {
        List<String> keys = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(req);
            for (S3ObjectSummary summary : result.getObjectSummaries()) {
                // Skip the prefix "folder" placeholder object, if any.
                if (!summary.getKey().endsWith("/")) {
                    keys.add(summary.getKey());
                }
            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return keys;
    }

    /** True when the consolidated file for {@code table} already exists (an earlier snapshot wrote it). */
    public boolean consolidatedExists(String table) {
        return s3Client.doesObjectExist(bucket, consolidatedKey(table));
    }

    /** Download an object (by full bucket key) to {@code dest}; returns {@code dest}. */
    public File download(String key, File dest) {
        s3Client.getObject(new GetObjectRequest(bucket, key), dest);
        return dest;
    }

    /** Write a local Parquet file to a full bucket key (SSE-AES256), overwriting in place. */
    public void putObject(String key, File file) {
        putFile(key, file);
        LOG.info("ADDF published object: {}", key);
    }

    /** Delete a batch of objects (by full bucket key) — used to clear consumed staging and tombstone markers. */
    public void deleteObjects(List<String> keys) {
        for (String key : keys) {
            s3Client.deleteObject(bucket, key);
        }
    }

    /** Delete a single tombstone marker after the participant's rows have been compacted this publish. */
    public void deleteTombstone(String healthCode) {
        s3Client.deleteObject(bucket, TOMBSTONE_PREFIX + healthCode);
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
