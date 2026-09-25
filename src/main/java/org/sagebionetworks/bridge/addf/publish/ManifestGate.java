package org.sagebionetworks.bridge.addf.publish;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * ADDF §7 — the <b>manifest gate</b>. It runs between {@link SnapshotDeltaBuilder#build} and the Azure upload and
 * answers one question the operational alarms of §6.4 cannot: <b>is this snapshot correct?</b> (Those alarms ask
 * whether the pipeline is <em>alive</em>; a snapshot can be blocked here while the pipeline is perfectly healthy, and
 * vice-versa. Both exist on purpose.)
 *
 * <p>Three checks, all of which must pass before a single byte reaches ADDI:</p>
 * <ol>
 *   <li><b>Presence</b> — all 10 tables exist in the delivery tree, either rewritten in this delta or already
 *       consolidated from an earlier snapshot. <em>This is the check that catches the original defect:</em> three
 *       dimension tables shipped with no producer at all, which no per-row validation would ever have noticed.
 *       {@code keyboard_sessions} is month-partitioned and has no consolidated single file, so its presence is
 *       answered by {@link ExportStoreClient#listKeyboardParts()}.</li>
 *   <li><b>Column contract</b> — every file in the delta declares exactly the columns {@link AddfTables} declares for
 *       its table, by name and in order. Read from the file's own Parquet footer
 *       ({@link ParquetTableReader#readColumnNames}), never from the AddfTables list we wrote it with, or the check
 *       would be circular.</li>
 *   <li><b>Referential integrity</b> — no activity/{@code file_records} row in the delta carries a
 *       {@code (health_code, participant_version)} that is absent from {@code participant_versions} (§3b.5).
 *       {@link SnapshotDeltaBuilder} already defers such orphans, so a hit here means the deferral itself
 *       regressed.</li>
 * </ol>
 *
 * <p><b>Where the column counts come from.</b> The plan pins the contract at 29/19/14/15/19/14/8/11/11/15 columns, but
 * this class deliberately does <b>not</b> restate those numbers — a second hardcoded copy is exactly the independent
 * drift the plan warns about. The chain is: the FAIR workbook {@code BiAffect3_FAIR_Metadata_Draft.xlsx} is the source
 * of truth, {@code AddfTablesFairConformanceTest} pins {@link AddfTables} to it (names <em>and</em> logical types), and
 * this gate pins the delivered files to {@link AddfTables}. Asserting the full ordered name list is strictly stronger
 * than asserting a count, at the same cost.</p>
 *
 * <p><b>Scope notes.</b> (a) Tables absent from the delta are unchanged since the last snapshot and were gated when
 * they were written, so only the delta's rows are re-read — the gate never scans the whole delivery tree.
 * (b) {@code demographics} also carries a {@code participant_version} column, but publish column-merges it rather than
 * deferring orphans, and the plan scopes the FK check to activity/{@code file_records}; it is excluded here to match
 * both.</p>
 *
 * <p><b>Enforcement.</b> {@code addf.publish.gate.enforced} (default {@code true}) controls whether a failed check
 * blocks the snapshot. When set to {@code false} the gate still runs and still logs every violation at ERROR — it just
 * doesn't throw. Diagnosis is never suppressed; only the block is.</p>
 */
@Component
public class ManifestGate {
    private static final Logger LOG = LoggerFactory.getLogger(ManifestGate.class);

    static final String CONFIG_KEY_GATE_ENFORCED = "addf.publish.gate.enforced";

    /** Tables whose {@code participant_version} is a foreign key into {@code participant_versions} (§3b.5). */
    private static final List<String> FK_TABLES = ImmutableList.of(
            AddfTables.KEYBOARD_SESSIONS, AddfTables.PHQ9, AddfTables.SELF_RATING, AddfTables.EVENING_LOG,
            AddfTables.GO_NO_GO, AddfTables.TRAIL_MAKING, AddfTables.FILE_RECORDS);

    private static final String CONSOLIDATED_MARKER = "/" + ExportStoreClient.CURRENT_TABLES_PREFIX;
    private static final String KEYBOARD_PART_MARKER = "/" + AddfTables.KEYBOARD_SESSIONS + "/month=";
    private static final String PARQUET_SUFFIX = ".parquet";

    /** Cap on violations quoted in the failure message — enough to diagnose, not enough to blow up the log line. */
    private static final int MAX_REPORTED = 10;

    private Config config;
    private ExportStoreClient exportStoreClient;
    private ParquetTableReader parquetTableReader;
    private FileHelper fileHelper;

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.config = config;
    }

    @Autowired
    public final void setExportStoreClient(ExportStoreClient exportStoreClient) {
        this.exportStoreClient = exportStoreClient;
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
     * Assert {@code delta} is fit to deliver. Throws {@link ManifestGateException} on the first failing snapshot (all
     * three checks run first, so the message reports every problem at once rather than one per re-run).
     */
    public void assertDeliverable(String snapshotDate, SnapshotDelta delta, File tempDir)
            throws IOException, ManifestGateException {
        Map<String, List<PublishedBlob>> byTable = new LinkedHashMap<>();
        List<String> failures = new ArrayList<>();

        indexByTable(delta.getBlobs(), byTable, failures);
        checkAllTablesPresent(byTable, failures);
        checkColumnContract(byTable, failures);
        checkReferentialIntegrity(byTable, tempDir, failures);

        if (failures.isEmpty()) {
            LOG.info("ADDF manifest gate passed for snapshotDate={}: all {} tables present, {} delta file(s) conform",
                    snapshotDate, AddfTables.allTables().size(), delta.getBlobs().size());
            return;
        }

        String message = "ADDF manifest gate failed for snapshotDate=" + snapshotDate + ": "
                + Joiner.on("; ").join(failures);
        if (isEnforced()) {
            LOG.error("{} — snapshot BLOCKED, nothing uploaded to Azure", message);
            throw new ManifestGateException(message);
        }
        LOG.error("{} — {}=false, delivering anyway", message, CONFIG_KEY_GATE_ENFORCED);
    }

    /** Default-on: only an explicit {@code false} disables enforcement, so a missing/blank config key still blocks. */
    private boolean isEnforced() {
        String value = config.get(CONFIG_KEY_GATE_ENFORCED);
        return value == null || value.isEmpty() || Boolean.parseBoolean(value);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 1) Presence — all 10 tables in the delivery tree.
    // ---------------------------------------------------------------------------------------------------------------
    private void checkAllTablesPresent(Map<String, List<PublishedBlob>> byTable, List<String> failures) {
        List<String> missing = new ArrayList<>();
        boolean keyboardListed = false;
        boolean keyboardPartsExist = false;

        for (String table : AddfTables.allTables()) {
            if (byTable.containsKey(table)) {
                continue; // (re)written this snapshot
            }
            if (AddfTables.KEYBOARD_SESSIONS.equals(table)) {
                if (!keyboardListed) {
                    keyboardPartsExist = !exportStoreClient.listKeyboardParts().isEmpty();
                    keyboardListed = true;
                }
                if (!keyboardPartsExist) {
                    missing.add(table);
                }
            } else if (!exportStoreClient.consolidatedExists(table)) {
                missing.add(table);
            }
        }

        if (!missing.isEmpty()) {
            failures.add(missing.size() + " of " + AddfTables.allTables().size()
                    + " table(s) absent from the delivery tree " + missing
                    + " — a table with no producer must never ship as a silently incomplete snapshot");
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 2) Column contract — each delta file's own footer vs the AddfTables column list.
    // ---------------------------------------------------------------------------------------------------------------
    private void checkColumnContract(Map<String, List<PublishedBlob>> byTable, List<String> failures)
            throws IOException {
        for (Map.Entry<String, List<PublishedBlob>> entry : byTable.entrySet()) {
            String table = entry.getKey();
            List<String> expected = AddfTables.columnNames(table);
            for (PublishedBlob blob : entry.getValue()) {
                List<String> actual = parquetTableReader.readColumnNames(blob.getLocalFile());
                if (!expected.equals(actual)) {
                    failures.add(table + " column contract violated in " + blob.getKey() + ": expected "
                            + expected.size() + " column(s) " + expected + ", file declares " + actual.size()
                            + " " + actual);
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 3) Referential integrity — no orphan (health_code, participant_version) on the activity/file_records rows.
    // ---------------------------------------------------------------------------------------------------------------
    private void checkReferentialIntegrity(Map<String, List<PublishedBlob>> byTable, File tempDir,
            List<String> failures) throws IOException {
        List<PublishedBlob> fkBlobs = new ArrayList<>();
        for (String table : FK_TABLES) {
            List<PublishedBlob> blobs = byTable.get(table);
            if (blobs != null) {
                fkBlobs.addAll(blobs);
            }
        }
        if (fkBlobs.isEmpty()) {
            return; // nothing in this delta references participant_versions
        }

        Set<String> validKeys = readValidVersionKeys(byTable, tempDir);
        if (validKeys == null) {
            // participant_versions is neither in the delta nor in the store — already reported by the presence check;
            // every FK would trivially "fail" here, which would bury the real finding.
            return;
        }

        List<String> orphans = new ArrayList<>();
        int orphanCount = 0;
        for (PublishedBlob blob : fkBlobs) {
            String table = tableOf(blob.getKey());
            for (TableRow row : parquetTableReader.read(table, blob.getLocalFile())) {
                Object version = row.get("participant_version");
                if (version == null) {
                    continue; // a null FK is allowed through by design (§3.5.3)
                }
                String key = str(row.get("health_code")) + "/" + asLong(version);
                if (!validKeys.contains(key)) {
                    orphanCount++;
                    if (orphans.size() < MAX_REPORTED) {
                        orphans.add(table + "/" + row.get("record_id") + "→" + key);
                    }
                }
            }
        }

        if (orphanCount > 0) {
            failures.add(orphanCount + " orphan row(s) reference a (health_code, participant_version) absent from "
                    + AddfTables.PARTICIPANT_VERSIONS + " — publish should have deferred these (§3b.5); first "
                    + orphans.size() + ": " + orphans);
        }
    }

    /**
     * The valid {@code health_code/participant_version} set: from the delta when {@code participant_versions} was
     * rewritten this snapshot, otherwise from the consolidated file already in the store. Returns {@code null} when the
     * table exists in neither.
     */
    private Set<String> readValidVersionKeys(Map<String, List<PublishedBlob>> byTable, File tempDir)
            throws IOException {
        String table = AddfTables.PARTICIPANT_VERSIONS;
        List<File> files = new ArrayList<>();

        List<PublishedBlob> staged = byTable.get(table);
        if (staged != null) {
            for (PublishedBlob blob : staged) {
                files.add(blob.getLocalFile());
            }
        } else if (exportStoreClient.consolidatedExists(table)) {
            files.add(exportStoreClient.download(exportStoreClient.consolidatedKey(table),
                    fileHelper.newFile(tempDir, "manifest-gate-" + table + PARQUET_SUFFIX)));
        } else {
            return null;
        }

        Set<String> validKeys = new TreeSet<>();
        for (File file : files) {
            for (TableRow row : parquetTableReader.read(table, file)) {
                validKeys.add(str(row.get("health_code")) + "/" + asLong(row.get("participant_version")));
            }
        }
        return validKeys;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------------
    private void indexByTable(List<PublishedBlob> blobs, Map<String, List<PublishedBlob>> byTable,
            List<String> failures) {
        for (PublishedBlob blob : blobs) {
            String table = tableOf(blob.getKey());
            if (table == null || AddfTables.columnsFor(table) == null) {
                failures.add("delta carries an object outside the ADDF delivery contract: " + blob.getKey());
                continue;
            }
            byTable.computeIfAbsent(table, t -> new ArrayList<>()).add(blob);
        }
    }

    /** Recover the table name from a delivery key: {@code current/tables/<table>.parquet} or a keyboard month-part. */
    static String tableOf(String key) {
        // Keyboard FIRST. Its month parts now live under current/tables/ alongside the single-file tables, so the
        // consolidated branch below would match them too and derive a table name of
        // "keyboard_sessions/month=2026-08/part-2026-09-24" — which is no table at all, so the gate would reject its
        // own delta as "an object outside the ADDF delivery contract" and block every publish carrying keyboard data.
        if (key.contains(KEYBOARD_PART_MARKER)) {
            return AddfTables.KEYBOARD_SESSIONS;
        }
        int idx = key.indexOf(CONSOLIDATED_MARKER);
        if (idx >= 0) {
            String name = key.substring(idx + CONSOLIDATED_MARKER.length());
            if (name.indexOf('/') >= 0) {
                // Nested under current/tables/ but not a keyboard part: a partitioned dataset this gate does not know
                // about. Returning the raw path would fail confusingly later; null routes it to the explicit
                // "outside the delivery contract" failure, which names the key.
                return null;
            }
            return name.endsWith(PARQUET_SUFFIX) ? name.substring(0, name.length() - PARQUET_SUFFIX.length()) : name;
        }
        return null;
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
}
