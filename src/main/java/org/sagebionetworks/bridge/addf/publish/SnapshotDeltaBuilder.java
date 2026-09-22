package org.sagebionetworks.bridge.addf.publish;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * ADDF §4.3 — the <b>sole writer of every consolidated table file</b>. Runs single-threaded per snapshot (one
 * scheduled publish message), so nothing else ever writes a consolidated file: no concurrent-writer race. It coalesces
 * the immutable per-record objects the accumulate/dimension workers staged (§3.7.1) into their delivery targets and
 * returns the exact set of (re)written files as the snapshot delta for the Azure upload (§4.4).
 *
 * <p>Order matters: {@code participant_versions} is built first because it defines (a) the valid
 * {@code (health_code, participant_version)} set used to <b>defer orphan</b> activity/file_records/keyboard rows
 * (§3b.5 — soft referential integrity: a row whose version hasn't landed yet is held back, not dropped, and retried
 * next snapshot) and (b) the source for the full-replace {@code participants_current} derivation (§4.3.1).</p>
 *
 * <p><b>Cutoff (§4.3.3):</b> each table's staging is listed once at the moment it is processed; {@code snapshotDate}
 * only labels the run (and names keyboard part files, §4.3.2) — it is not a row filter. Objects staged after the
 * listing roll to the next publish. Consumed staging objects are <b>collected</b>, not deleted, during {@link #build};
 * {@link #commit} deletes them only after the worker confirms the Azure upload. Deferred (orphan) objects are left in
 * place.</p>
 *
 * <p><b>Withdrawal/tombstone (§3b.3):</b> health codes marked in {@code _tombstone/} are compacted out of every
 * single-file/participant-keyed table this run; their markers are cleared by {@link #commit} (again, post-upload).
 * (Historical month-partitioned keyboard parts are compacted by the optional periodic compaction pass of §4.3.2, not
 * this daily publish.)</p>
 *
 * <p><b>Crash-safety (§4.5):</b> {@code build} writes the consolidated files (idempotent overwrite-in-place) but does
 * not consume staging or clear tombstones — that is {@code commit}'s job, and the worker calls {@code commit} only
 * after the Azure upload confirms. A crash or upload failure before {@code commit} leaves staging + tombstones intact,
 * so the whole day replays and rebuilds the identical delta rather than silently losing it from the Azure mirror.</p>
 */
@Component
public class SnapshotDeltaBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotDeltaBuilder.class);

    /** Single-file, record-keyed tables coalesced by {@code record_id} (activity survey/task tables + file_records). */
    private static final List<String> RECORD_KEYED_TABLES = ImmutableList.of(
            AddfTables.PHQ9, AddfTables.SELF_RATING, AddfTables.EVENING_LOG, AddfTables.GO_NO_GO,
            AddfTables.TRAIL_MAKING, AddfTables.FILE_RECORDS);

    private ExportStoreClient exportStoreClient;
    private ParquetRowWriter parquetRowWriter;
    private ParquetTableReader parquetTableReader;
    private FileHelper fileHelper;

    @Autowired
    public final void setExportStoreClient(ExportStoreClient exportStoreClient) {
        this.exportStoreClient = exportStoreClient;
    }

    @Autowired
    public final void setParquetRowWriter(ParquetRowWriter parquetRowWriter) {
        this.parquetRowWriter = parquetRowWriter;
    }

    @Autowired
    public final void setParquetTableReader(ParquetTableReader parquetTableReader) {
        this.parquetTableReader = parquetTableReader;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /**
     * Build the snapshot: coalesce all staged rows into their consolidated targets in the export store and return the
     * set of files that changed (each paired with its local temp file for the Azure upload), together with the staging
     * keys and tombstones to retire once that upload confirms. {@code tempDir} is owned by the caller (the publish
     * worker), which cleans it up after the upload. This method does <b>not</b> delete staging or clear tombstones —
     * see {@link #commit}.
     */
    public SnapshotDelta build(String snapshotDate, File tempDir) throws IOException {
        List<PublishedBlob> delta = new ArrayList<>();
        // Deduping set: one staging object may hold several rows, so add-per-row would otherwise queue duplicate deletes.
        Set<String> consumedStagingKeys = new LinkedHashSet<>();

        Set<String> tombstoned = new TreeSet<>(exportStoreClient.listTombstonedHealthCodes());
        if (!tombstoned.isEmpty()) {
            LOG.info("ADDF publish {}: {} tombstoned participant(s) to compact", snapshotDate, tombstoned.size());
        }

        // 1) participant_versions first — defines the valid FK set and feeds participants_current.
        VersionResult versions = buildParticipantVersions(tempDir, tombstoned, delta, consumedStagingKeys);

        // 2) participants_current — full-replace from the latest version per health_code (§4.3.1).
        if (versions.mutated) {
            buildParticipantsCurrent(tempDir, versions.rowsByHealthAndVersion.values(), delta);
        }

        // 3) record-keyed single-file tables (activity + file_records), orphan-deferred + tombstone-compacted.
        for (String table : RECORD_KEYED_TABLES) {
            buildRecordKeyedTable(table, tempDir, tombstoned, versions.validKeys, delta, consumedStagingKeys);
        }

        // 4) demographics — column-merge by health_code (§3.5.1 / §3.7.2), tombstone-compacted.
        buildDemographics(tempDir, tombstoned, delta, consumedStagingKeys);

        // 5) keyboard_sessions — month-partitioned, append a date-named part per active month (§4.3.2).
        buildKeyboard(snapshotDate, tempDir, tombstoned, versions.validKeys, delta, consumedStagingKeys);

        // Staging is NOT consumed and tombstones are NOT cleared here — commit() does that, but only AFTER the worker
        // confirms the Azure upload (§4.5), so a crash/upload-failure before commit replays the identical delta.
        LOG.info("ADDF publish {}: {} consolidated file(s) changed", snapshotDate, delta.size());
        return new SnapshotDelta(delta, new ArrayList<>(consumedStagingKeys), new ArrayList<>(tombstoned));
    }

    /**
     * Retire the staging objects coalesced into this snapshot and clear the tombstone markers it compacted out — called
     * by the publish worker <b>only after</b> the Azure upload of {@link SnapshotDelta#getBlobs()} has confirmed (§4.5).
     * Split from {@link #build} so an upload failure leaves staging + tombstones in place for an idempotent replay.
     */
    public void commit(SnapshotDelta delta) {
        if (!delta.getConsumedStagingKeys().isEmpty()) {
            exportStoreClient.deleteObjects(delta.getConsumedStagingKeys());
        }
        for (String healthCode : delta.getTombstonedHealthCodes()) {
            exportStoreClient.deleteTombstone(healthCode);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // participant_versions (dimension) — presence-upsert by (health_code, participant_version).
    // ---------------------------------------------------------------------------------------------------------------
    private VersionResult buildParticipantVersions(File tempDir, Set<String> tombstoned,
            List<PublishedBlob> delta, Set<String> consumedStagingKeys) throws IOException {
        String table = AddfTables.PARTICIPANT_VERSIONS;
        Map<String, TableRow> byKey = new LinkedHashMap<>();
        boolean mutated = false;

        if (exportStoreClient.consolidatedExists(table)) {
            File existing = exportStoreClient.download(exportStoreClient.consolidatedKey(table),
                    fileHelper.newFile(tempDir, table + "-existing.parquet"));
            for (TableRow row : parquetTableReader.read(table, existing)) {
                if (tombstoned.contains(str(row.get("health_code")))) {
                    mutated = true; // drop withdrawn participant's versions
                    continue;
                }
                byKey.put(versionKey(row), row);
            }
        }

        List<String> staged = exportStoreClient.listStaged(table);
        int i = 0;
        for (String key : staged) {
            File local = exportStoreClient.download(key, fileHelper.newFile(tempDir, table + "-staged-" + (i++) + ".parquet"));
            for (TableRow row : parquetTableReader.read(table, local)) {
                if (tombstoned.contains(str(row.get("health_code")))) {
                    consumedStagingKeys.add(key); // withdrawn: consume without exporting the NO_SHARING/late version
                    continue;
                }
                byKey.put(versionKey(row), row); // a version is immutable; a repeat key overwrites identical content
                consumedStagingKeys.add(key);
                mutated = true;
            }
        }

        if (mutated) {
            writeConsolidated(table, new ArrayList<>(byKey.values()), tempDir, delta);
        }

        Set<String> validKeys = new TreeSet<>(byKey.keySet());
        return new VersionResult(byKey, validKeys, mutated);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // participants_current — 1 row per participant = their max(participant_version) row, regenerated whole (§4.3.1).
    // ---------------------------------------------------------------------------------------------------------------
    private void buildParticipantsCurrent(File tempDir, java.util.Collection<TableRow> versionRows,
            List<PublishedBlob> delta) throws IOException {
        Map<String, TableRow> latestByHealthCode = new LinkedHashMap<>();
        for (TableRow versionRow : versionRows) {
            String healthCode = str(versionRow.get("health_code"));
            TableRow current = latestByHealthCode.get(healthCode);
            if (current == null || asLong(versionRow.get("participant_version")) > asLong(current.get("participant_version"))) {
                latestByHealthCode.put(healthCode, versionRow);
            }
        }
        // Re-key rows to the participants_current table (identical 11-column schema).
        List<TableRow> currentRows = new ArrayList<>();
        for (TableRow versionRow : latestByHealthCode.values()) {
            TableRow out = new TableRow(AddfTables.PARTICIPANTS_CURRENT, str(versionRow.get("health_code")));
            for (String column : AddfTables.columnNames(AddfTables.PARTICIPANTS_CURRENT)) {
                out.put(column, versionRow.get(column));
            }
            currentRows.add(out);
        }
        writeConsolidated(AddfTables.PARTICIPANTS_CURRENT, currentRows, tempDir, delta);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Record-keyed single-file tables — upsert by record_id, defer orphans, compact tombstones.
    // ---------------------------------------------------------------------------------------------------------------
    private void buildRecordKeyedTable(String table, File tempDir, Set<String> tombstoned, Set<String> validKeys,
            List<PublishedBlob> delta, Set<String> consumedStagingKeys) throws IOException {
        Map<String, TableRow> byRecordId = new LinkedHashMap<>();
        boolean mutated = false;

        if (exportStoreClient.consolidatedExists(table)) {
            File existing = exportStoreClient.download(exportStoreClient.consolidatedKey(table),
                    fileHelper.newFile(tempDir, table + "-existing.parquet"));
            for (TableRow row : parquetTableReader.read(table, existing)) {
                if (tombstoned.contains(str(row.get("health_code")))) {
                    mutated = true;
                    continue;
                }
                byRecordId.put(str(row.get("record_id")), row);
            }
        }

        List<String> staged = exportStoreClient.listStaged(table);
        int i = 0;
        for (String key : staged) {
            File local = exportStoreClient.download(key, fileHelper.newFile(tempDir, table + "-staged-" + (i++) + ".parquet"));
            for (TableRow row : parquetTableReader.read(table, local)) {
                String healthCode = str(row.get("health_code"));
                if (tombstoned.contains(healthCode)) {
                    consumedStagingKeys.add(key);
                    continue;
                }
                if (isOrphan(healthCode, row.get("participant_version"), validKeys)) {
                    // Defer: leave staged, retry next snapshot once the participant_version lands (§3b.5).
                    LOG.info("ADDF publish: deferring orphan {} row record={} (version not yet in participant_versions)",
                            table, row.get("record_id"));
                    continue;
                }
                byRecordId.put(str(row.get("record_id")), row);
                consumedStagingKeys.add(key);
                mutated = true;
            }
        }

        if (mutated) {
            writeConsolidated(table, new ArrayList<>(byRecordId.values()), tempDir, delta);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // demographics — column-merge by health_code: overlay non-null columns, preserve prior non-nulls, never null-out.
    // ---------------------------------------------------------------------------------------------------------------
    private void buildDemographics(File tempDir, Set<String> tombstoned, List<PublishedBlob> delta,
            Set<String> consumedStagingKeys) throws IOException {
        String table = AddfTables.DEMOGRAPHICS;
        Map<String, TableRow> byHealthCode = new LinkedHashMap<>();
        boolean mutated = false;

        if (exportStoreClient.consolidatedExists(table)) {
            File existing = exportStoreClient.download(exportStoreClient.consolidatedKey(table),
                    fileHelper.newFile(tempDir, table + "-existing.parquet"));
            for (TableRow row : parquetTableReader.read(table, existing)) {
                if (tombstoned.contains(str(row.get("health_code")))) {
                    mutated = true;
                    continue;
                }
                byHealthCode.put(str(row.get("health_code")), row);
            }
        }

        List<String> staged = exportStoreClient.listStaged(table);
        int i = 0;
        for (String key : staged) {
            File local = exportStoreClient.download(key, fileHelper.newFile(tempDir, table + "-staged-" + (i++) + ".parquet"));
            for (TableRow partial : parquetTableReader.read(table, local)) {
                String healthCode = str(partial.get("health_code"));
                if (tombstoned.contains(healthCode)) {
                    consumedStagingKeys.add(key);
                    continue;
                }
                mergeInto(byHealthCode, healthCode, partial);
                consumedStagingKeys.add(key);
                mutated = true;
            }
        }

        if (mutated) {
            writeConsolidated(table, new ArrayList<>(byHealthCode.values()), tempDir, delta);
        }
    }

    /** Column-merge one partial demographics row into the accumulator: fill nulls, preserve existing non-nulls. */
    private void mergeInto(Map<String, TableRow> byHealthCode, String healthCode, TableRow partial) {
        TableRow target = byHealthCode.get(healthCode);
        if (target == null) {
            target = new TableRow(AddfTables.DEMOGRAPHICS, healthCode);
            byHealthCode.put(healthCode, target);
        }
        for (String column : AddfTables.columnNames(AddfTables.DEMOGRAPHICS)) {
            Object incoming = partial.get(column);
            if (incoming != null && target.get(column) == null) {
                target.put(column, incoming);
            }
        }
        // Always ensure the key column is set (a first partial may legitimately carry it).
        target.put("health_code", healthCode);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // keyboard_sessions — month-partitioned; each publish appends one date-named part per active month (§4.3.2).
    // ---------------------------------------------------------------------------------------------------------------
    private void buildKeyboard(String snapshotDate, File tempDir, Set<String> tombstoned, Set<String> validKeys,
            List<PublishedBlob> delta, Set<String> consumedStagingKeys) throws IOException {
        String table = AddfTables.KEYBOARD_SESSIONS;
        List<String> staged = exportStoreClient.listStaged(table);
        Map<String, List<TableRow>> byMonth = new LinkedHashMap<>();
        int i = 0;
        for (String key : staged) {
            File local = exportStoreClient.download(key, fileHelper.newFile(tempDir, table + "-staged-" + (i++) + ".parquet"));
            for (TableRow row : parquetTableReader.read(table, local)) {
                String healthCode = str(row.get("health_code"));
                if (tombstoned.contains(healthCode)) {
                    consumedStagingKeys.add(key);
                    continue;
                }
                if (isOrphan(healthCode, row.get("participant_version"), validKeys)) {
                    LOG.info("ADDF publish: deferring orphan keyboard row record={} (version not yet in participant_versions)",
                            row.get("record_id"));
                    continue;
                }
                String month = monthOf(str(row.get("session_start")), snapshotDate);
                byMonth.computeIfAbsent(month, m -> new ArrayList<>()).add(row);
                consumedStagingKeys.add(key);
            }
        }

        for (Map.Entry<String, List<TableRow>> entry : byMonth.entrySet()) {
            String key = exportStoreClient.keyboardPartKey(entry.getKey(), snapshotDate);
            File out = fileHelper.newFile(tempDir, "keyboard-" + entry.getKey() + "-part.parquet");
            parquetRowWriter.writeAll(table, entry.getValue(), out);
            exportStoreClient.putObject(key, out);
            delta.add(new PublishedBlob(key, out));
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------------
    private void writeConsolidated(String table, List<TableRow> rows, File tempDir, List<PublishedBlob> delta)
            throws IOException {
        String key = exportStoreClient.consolidatedKey(table);
        File out = fileHelper.newFile(tempDir, table + "-consolidated.parquet");
        parquetRowWriter.writeAll(table, rows, out);
        exportStoreClient.putObject(key, out);
        delta.add(new PublishedBlob(key, out));
    }

    /** A row is an orphan when it carries a non-null participant_version that is not (yet) in participant_versions. */
    private static boolean isOrphan(String healthCode, Object participantVersion, Set<String> validKeys) {
        if (participantVersion == null) {
            return false; // no FK to check — accumulate may write a null version FK (§3.5.3); the null is allowed through
        }
        return !validKeys.contains(healthCode + "/" + asLong(participantVersion));
    }

    private static String versionKey(TableRow row) {
        return str(row.get("health_code")) + "/" + asLong(row.get("participant_version"));
    }

    /** Derive {@code YYYY-MM} from an ISO-8601 {@code session_start}; fall back to the snapshot's month when absent. */
    private static String monthOf(String sessionStartIso, String snapshotDate) {
        if (sessionStartIso != null && sessionStartIso.length() >= 7) {
            return sessionStartIso.substring(0, 7);
        }
        if (snapshotDate != null && snapshotDate.length() >= 7) {
            return snapshotDate.substring(0, 7);
        }
        return "unknown";
    }

    private static String str(Object value) {
        return value == null ? null : value.toString();
    }

    private static long asLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return value == null ? Long.MIN_VALUE : Long.parseLong(value.toString().trim());
    }

    /** Result of the participant_versions pass: the surviving rows, the valid FK set, and whether anything changed. */
    private static final class VersionResult {
        final Map<String, TableRow> rowsByHealthAndVersion;
        final Set<String> validKeys;
        final boolean mutated;

        VersionResult(Map<String, TableRow> rowsByHealthAndVersion, Set<String> validKeys, boolean mutated) {
            this.rowsByHealthAndVersion = rowsByHealthAndVersion;
            this.validKeys = validKeys;
            this.mutated = mutated;
        }
    }
}
