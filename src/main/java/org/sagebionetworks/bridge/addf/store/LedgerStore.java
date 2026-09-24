package org.sagebionetworks.bridge.addf.store;

import java.io.ByteArrayInputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
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
 *   <li><b>raw scope</b> ({@code _ledger/raw/<healthCode>/<date>/<record>-<assessment>.zip}) &mdash; written by the
 *       <b>publish</b> worker after each raw archive's Azure upload confirms (§4.3.4). It serves two purposes:
 *       presence-skip on replay, and — because the key carries the <b>health code</b> — a durable record of
 *       <i>whose</i> data has left the AWS account.</li>
 * </ul>
 *
 * <p><b>Why the raw scope is health-code-keyed and the others are not.</b> Withdrawal compaction deletes the
 * participant's {@code file_records} rows, and those rows are the only index from a health code to its
 * {@code raw/…} archives. Raw archives that have already been delivered to the partner's container cannot be
 * un-delivered by this codebase (there is no blob-delete path — see {@code RawArchiveDelivery}), so without a
 * health-code-keyed ledger a withdrawal would leave delivered archives that nobody could even enumerate for a
 * manual erasure request. Prefixing the ledger key with the health code keeps that enumeration possible for the
 * lifetime of the export store: {@code aws s3 ls biaffect-3/_ledger/raw/<healthCode>/}.</p>
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
     * Presence-skip check for one raw archive, scoped to its owning participant. Bounded by the per-run candidate
     * set (new + previously-failed archives), so this is a handful of HEADs per publish rather than a scan of
     * everything ever delivered.
     */
    public boolean containsRaw(String healthCode, String rawRelativeKey) {
        return exists(rawLedgerKey(healthCode, rawRelativeKey));
    }

    /**
     * Mark one raw archive delivered to Azure — called per archive, <b>immediately after</b> its own upload
     * confirms, so an interrupted batch never claims credit for an archive that did not ship (§4.5).
     */
    public void markRawDelivered(String healthCode, String rawRelativeKey) {
        mark(rawLedgerKey(healthCode, rawRelativeKey));
    }

    /**
     * {@code (hc-1, raw/2026-08-15/rec-PHQ-9.zip)} → {@code biaffect-3/_ledger/raw/hc-1/2026-08-15/rec-PHQ-9.zip}.
     * The {@code raw/} prefix is dropped because the scope segment already says "raw"; the health code is inserted
     * so the ledger doubles as the who-has-what index (see class javadoc).
     */
    String rawLedgerKey(String healthCode, String rawRelativeKey) {
        String suffix = rawRelativeKey.startsWith(RAW_RELATIVE_PREFIX)
                ? rawRelativeKey.substring(RAW_RELATIVE_PREFIX.length())
                : rawRelativeKey;
        return ledgerKey(SCOPE_RAW, healthCode + "/" + suffix);
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
