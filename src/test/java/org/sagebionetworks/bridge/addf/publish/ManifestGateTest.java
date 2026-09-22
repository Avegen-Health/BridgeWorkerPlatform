package org.sagebionetworks.bridge.addf.publish;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * Exercises {@link ManifestGate}'s three checks (presence / column contract / referential integrity) with all Parquet
 * and S3 IO mocked: {@link ParquetTableReader} is stubbed per temp-file name, so each test can shape exactly what the
 * gate "reads back" out of the delta it is handed.
 */
public class ManifestGateTest {
    private static final String SNAPSHOT_DATE = "2026-09-22";
    private static final String KEYBOARD_PART_KEY =
            "biaffect-3/keyboard_sessions/month=2026-09/part-" + SNAPSHOT_DATE + ".parquet";

    private Config mockConfig;
    private ExportStoreClient mockStore;
    private ParquetTableReader mockReader;
    private FileHelper mockFileHelper;
    private ManifestGate gate;

    private File tempDir;
    private Map<String, List<String>> columnsByFileName;
    private Map<String, List<TableRow>> rowsByFileName;

    @BeforeMethod
    public void before() throws Exception {
        mockConfig = mock(Config.class);
        mockStore = mock(ExportStoreClient.class);
        mockReader = mock(ParquetTableReader.class);
        mockFileHelper = mock(FileHelper.class);
        tempDir = new File("/tmp/addf-manifest-gate-test");
        columnsByFileName = new HashMap<>();
        rowsByFileName = new HashMap<>();

        // Default: enforced (key absent), nothing already consolidated, no keyboard parts.
        when(mockConfig.get(ManifestGate.CONFIG_KEY_GATE_ENFORCED)).thenReturn(null);
        when(mockStore.consolidatedExists(anyString())).thenReturn(false);
        when(mockStore.listKeyboardParts()).thenReturn(ImmutableList.<String>of());
        when(mockStore.consolidatedKey(anyString()))
                .thenAnswer(inv -> "biaffect-3/current/tables/" + inv.getArgumentAt(0, String.class) + ".parquet");
        when(mockFileHelper.newFile(any(File.class), anyString()))
                .thenAnswer(inv -> new File(tempDir, inv.getArgumentAt(1, String.class)));
        when(mockStore.download(anyString(), any(File.class)))
                .thenAnswer(inv -> inv.getArgumentAt(1, File.class));
        when(mockReader.readColumnNames(any(File.class)))
                .thenAnswer(inv -> columnsByFileName.getOrDefault(inv.getArgumentAt(0, File.class).getName(),
                        ImmutableList.<String>of()));
        when(mockReader.read(anyString(), any(File.class)))
                .thenAnswer(inv -> rowsByFileName.getOrDefault(inv.getArgumentAt(1, File.class).getName(),
                        ImmutableList.<TableRow>of()));

        gate = new ManifestGate();
        gate.setBridgeConfig(mockConfig);
        gate.setExportStoreClient(mockStore);
        gate.setParquetTableReader(mockReader);
        gate.setFileHelper(mockFileHelper);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------------

    /** A delta blob for {@code table} whose file, by default, declares exactly the contract's columns. */
    private PublishedBlob blob(String table) {
        String key = AddfTables.KEYBOARD_SESSIONS.equals(table) ? KEYBOARD_PART_KEY
                : "biaffect-3/current/tables/" + table + ".parquet";
        File file = new File(tempDir, table + ".parquet");
        columnsByFileName.put(file.getName(), AddfTables.columnNames(table));
        return new PublishedBlob(key, file);
    }

    /** A delta that (re)wrote every one of the 10 tables this snapshot — the everything-changed case. */
    private SnapshotDelta fullDelta() {
        List<PublishedBlob> blobs = new ArrayList<>();
        for (String table : AddfTables.allTables()) {
            blobs.add(blob(table));
        }
        return new SnapshotDelta(blobs, ImmutableList.<String>of(), ImmutableList.<String>of());
    }

    private SnapshotDelta deltaOf(PublishedBlob... blobs) {
        return new SnapshotDelta(ImmutableList.copyOf(blobs), ImmutableList.<String>of(), ImmutableList.<String>of());
    }

    /** Put rows into the file backing {@code table}'s delta blob, so the gate reads them back. */
    private void rows(String table, TableRow... tableRows) {
        rowsByFileName.put(table + ".parquet", ImmutableList.copyOf(tableRows));
    }

    private static TableRow row(String table, String recordId, String healthCode, Object participantVersion) {
        return new TableRow(table, recordId)
                .put("record_id", recordId)
                .put("health_code", healthCode)
                .put("participant_version", participantVersion);
    }

    /** Assert the gate blocks and that its message mentions {@code needle}. */
    private String assertBlocked(SnapshotDelta delta) throws Exception {
        try {
            gate.assertDeliverable(SNAPSHOT_DATE, delta, tempDir);
        } catch (ManifestGateException expected) {
            return expected.getMessage();
        }
        fail("expected the manifest gate to block the snapshot");
        return null; // unreachable
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 1) Presence — all 10 tables.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void fullConformingSnapshotPasses() throws Exception {
        gate.assertDeliverable(SNAPSHOT_DATE, fullDelta(), tempDir);
    }

    @Test
    public void tablesCarriedOverFromAnEarlierSnapshotCountAsPresent() throws Exception {
        // Nothing changed this run: an empty delta is fine as long as the delivery tree is already complete.
        when(mockStore.consolidatedExists(anyString())).thenReturn(true);
        when(mockStore.listKeyboardParts()).thenReturn(ImmutableList.of(KEYBOARD_PART_KEY));

        gate.assertDeliverable(SNAPSHOT_DATE,
                new SnapshotDelta(ImmutableList.<PublishedBlob>of(), ImmutableList.<String>of(),
                        ImmutableList.<String>of()), tempDir);
    }

    @Test
    public void tableWithNoProducerBlocksSnapshot() throws Exception {
        // The original defect: dimension tables shipped with no producer at all. Everything exists except demographics.
        when(mockStore.consolidatedExists(anyString())).thenReturn(true);
        when(mockStore.consolidatedExists(AddfTables.DEMOGRAPHICS)).thenReturn(false);
        when(mockStore.listKeyboardParts()).thenReturn(ImmutableList.of(KEYBOARD_PART_KEY));

        String message = assertBlocked(new SnapshotDelta(ImmutableList.<PublishedBlob>of(),
                ImmutableList.<String>of(), ImmutableList.<String>of()));

        assertTrue(message.contains(AddfTables.DEMOGRAPHICS), message);
        assertTrue(message.contains("1 of 10 table(s) absent"), message);
    }

    @Test
    public void keyboardPresenceComesFromMonthPartsNotAConsolidatedFile() throws Exception {
        // keyboard_sessions is the one table with no consolidated single file; asking consolidatedExists() for it would
        // always answer "missing" and block every snapshot forever.
        when(mockStore.consolidatedExists(anyString())).thenReturn(true);
        when(mockStore.listKeyboardParts()).thenReturn(ImmutableList.<String>of());

        String message = assertBlocked(new SnapshotDelta(ImmutableList.<PublishedBlob>of(),
                ImmutableList.<String>of(), ImmutableList.<String>of()));

        assertTrue(message.contains(AddfTables.KEYBOARD_SESSIONS), message);
        verify(mockStore, never()).consolidatedExists(AddfTables.KEYBOARD_SESSIONS);
    }

    @Test
    public void keyboardInTheDeltaSatisfiesPresenceWithoutListingParts() throws Exception {
        when(mockStore.consolidatedExists(anyString())).thenReturn(true);

        gate.assertDeliverable(SNAPSHOT_DATE, deltaOf(blob(AddfTables.KEYBOARD_SESSIONS)), tempDir);

        verify(mockStore, never()).listKeyboardParts();
    }

    @Test
    public void objectOutsideTheDeliveryContractBlocks() throws Exception {
        when(mockStore.consolidatedExists(anyString())).thenReturn(true);
        when(mockStore.listKeyboardParts()).thenReturn(ImmutableList.of(KEYBOARD_PART_KEY));

        String message = assertBlocked(deltaOf(
                new PublishedBlob("biaffect-3/_staging/phq9/2026-09-22/rec-1.parquet", new File(tempDir, "x.parquet"))));

        assertTrue(message.contains("outside the ADDF delivery contract"), message);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 2) Column contract.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void columnContractViolationBlocks() throws Exception {
        SnapshotDelta delta = fullDelta();
        // A phq9 file that lost total_score: the exact class of drift a row-level check can't see.
        List<String> truncated = new ArrayList<>(AddfTables.columnNames(AddfTables.PHQ9));
        truncated.remove("total_score");
        columnsByFileName.put(AddfTables.PHQ9 + ".parquet", truncated);

        String message = assertBlocked(delta);

        assertTrue(message.contains("phq9 column contract violated"), message);
        assertTrue(message.contains("expected 19 column(s)"), message);
        assertTrue(message.contains("file declares 18"), message);
    }

    @Test
    public void columnOrderDriftBlocks() throws Exception {
        SnapshotDelta delta = fullDelta();
        List<String> reordered = new ArrayList<>(AddfTables.columnNames(AddfTables.SELF_RATING));
        reordered.add(reordered.remove(0)); // same names + same count, different order
        columnsByFileName.put(AddfTables.SELF_RATING + ".parquet", reordered);

        String message = assertBlocked(delta);

        assertTrue(message.contains("self_rating column contract violated"), message);
    }

    @Test
    public void columnCountsMatchTheContractedShape() {
        // The plan pins 29/19/14/15/19/14/8/11/11/15. The gate asserts the full ordered name list rather than these
        // numbers (a second hardcoded copy would drift independently) — this test is the one place they're checked.
        assertColumnCount(AddfTables.KEYBOARD_SESSIONS, 29);
        assertColumnCount(AddfTables.PHQ9, 19);
        assertColumnCount(AddfTables.SELF_RATING, 14);
        assertColumnCount(AddfTables.EVENING_LOG, 15);
        assertColumnCount(AddfTables.GO_NO_GO, 19);
        assertColumnCount(AddfTables.TRAIL_MAKING, 14);
        assertColumnCount(AddfTables.DEMOGRAPHICS, 8);
        assertColumnCount(AddfTables.PARTICIPANT_VERSIONS, 11);
        assertColumnCount(AddfTables.PARTICIPANTS_CURRENT, 11);
        assertColumnCount(AddfTables.FILE_RECORDS, 15);
    }

    private static void assertColumnCount(String table, int expected) {
        org.testng.Assert.assertEquals(AddfTables.columnsFor(table).size(), expected, table + " column count");
    }

    // ---------------------------------------------------------------------------------------------------------------
    // 3) Referential integrity.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void orphanParticipantVersionBlocks() throws Exception {
        SnapshotDelta delta = fullDelta();
        rows(AddfTables.PARTICIPANT_VERSIONS, versionRow("hc-1", 1L));
        rows(AddfTables.PHQ9, row(AddfTables.PHQ9, "rec-1", "hc-1", 2L)); // version 2 never landed

        String message = assertBlocked(delta);

        assertTrue(message.contains("1 orphan row(s)"), message);
        assertTrue(message.contains("phq9/rec-1→hc-1/2"), message);
    }

    @Test
    public void resolvedForeignKeysPass() throws Exception {
        SnapshotDelta delta = fullDelta();
        rows(AddfTables.PARTICIPANT_VERSIONS, versionRow("hc-1", 1L), versionRow("hc-1", 2L));
        rows(AddfTables.PHQ9, row(AddfTables.PHQ9, "rec-1", "hc-1", 2L));
        rows(AddfTables.FILE_RECORDS, row(AddfTables.FILE_RECORDS, "rec-1", "hc-1", 1L));
        rows(AddfTables.KEYBOARD_SESSIONS, row(AddfTables.KEYBOARD_SESSIONS, "rec-2", "hc-1", 1L));

        gate.assertDeliverable(SNAPSHOT_DATE, delta, tempDir);
    }

    @Test
    public void nullParticipantVersionIsNotAnOrphan() throws Exception {
        // A null FK is allowed through by design (§3.5.3) — accumulate writes it rather than poison-looping.
        SnapshotDelta delta = fullDelta();
        rows(AddfTables.PARTICIPANT_VERSIONS, versionRow("hc-1", 1L));
        rows(AddfTables.PHQ9, row(AddfTables.PHQ9, "rec-1", "hc-1", null));

        gate.assertDeliverable(SNAPSHOT_DATE, delta, tempDir);
    }

    @Test
    public void validKeysAreReadFromTheStoreWhenVersionsDidNotChange() throws Exception {
        // participant_versions unchanged this snapshot => not in the delta => the gate must fall back to the store copy
        // rather than treating every activity FK as an orphan.
        when(mockStore.consolidatedExists(anyString())).thenReturn(true);
        when(mockStore.listKeyboardParts()).thenReturn(ImmutableList.of(KEYBOARD_PART_KEY));
        rowsByFileName.put("manifest-gate-" + AddfTables.PARTICIPANT_VERSIONS + ".parquet",
                ImmutableList.of(versionRow("hc-1", 7L)));
        rows(AddfTables.PHQ9, row(AddfTables.PHQ9, "rec-1", "hc-1", 7L));

        gate.assertDeliverable(SNAPSHOT_DATE, deltaOf(blob(AddfTables.PHQ9)), tempDir);

        verify(mockStore).download(eq("biaffect-3/current/tables/participant_versions.parquet"), any(File.class));
    }

    @Test
    public void demographicsIsNotForeignKeyChecked() throws Exception {
        // demographics carries participant_version but publish column-merges it instead of deferring orphans, and the
        // plan scopes the FK check to activity/file_records. Excluded here to match both.
        SnapshotDelta delta = fullDelta();
        rows(AddfTables.PARTICIPANT_VERSIONS, versionRow("hc-1", 1L));
        rows(AddfTables.DEMOGRAPHICS, row(AddfTables.DEMOGRAPHICS, null, "hc-1", 99L));

        gate.assertDeliverable(SNAPSHOT_DATE, delta, tempDir);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Enforcement switch.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void enforcedByDefaultWhenTheConfigKeyIsAbsent() throws Exception {
        when(mockConfig.get(ManifestGate.CONFIG_KEY_GATE_ENFORCED)).thenReturn(null);
        when(mockStore.consolidatedExists(anyString())).thenReturn(true);
        when(mockStore.consolidatedExists(AddfTables.DEMOGRAPHICS)).thenReturn(false);
        when(mockStore.listKeyboardParts()).thenReturn(ImmutableList.of(KEYBOARD_PART_KEY));

        assertBlocked(new SnapshotDelta(ImmutableList.<PublishedBlob>of(), ImmutableList.<String>of(),
                ImmutableList.<String>of()));
    }

    @Test
    public void unenforcedGateDiagnosesButDoesNotBlock() throws Exception {
        when(mockConfig.get(ManifestGate.CONFIG_KEY_GATE_ENFORCED)).thenReturn("false");
        when(mockStore.consolidatedExists(anyString())).thenReturn(true);
        when(mockStore.consolidatedExists(AddfTables.DEMOGRAPHICS)).thenReturn(false);
        when(mockStore.listKeyboardParts()).thenReturn(ImmutableList.of(KEYBOARD_PART_KEY));

        // Same broken snapshot as the test above — logged at ERROR, but delivered.
        gate.assertDeliverable(SNAPSHOT_DATE, new SnapshotDelta(ImmutableList.<PublishedBlob>of(),
                ImmutableList.<String>of(), ImmutableList.<String>of()), tempDir);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // tableOf — key -> table recovery.
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void tableOfRecognisesBothDeliveryShapes() {
        org.testng.Assert.assertEquals(ManifestGate.tableOf("biaffect-3/current/tables/phq9.parquet"), "phq9");
        org.testng.Assert.assertEquals(ManifestGate.tableOf(KEYBOARD_PART_KEY), AddfTables.KEYBOARD_SESSIONS);
        org.testng.Assert.assertEquals(ManifestGate.tableOf("biaffect-3/raw/2026-09-22/rec-1-PHQ-9.zip"), null);
    }

    private static TableRow versionRow(String healthCode, Long version) {
        return new TableRow(AddfTables.PARTICIPANT_VERSIONS, healthCode)
                .put("health_code", healthCode)
                .put("participant_version", version);
    }
}
