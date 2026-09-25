package org.sagebionetworks.bridge.addf.publish;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * The withdrawal and edge-value paths through {@link SnapshotDeltaBuilder} that the main test does not reach.
 *
 * <p>Tombstone compaction is the one operation here with a legal obligation behind it: when a participant withdraws,
 * their rows have to leave <em>every</em> table, not just the ones that happened to change that day. Each table family
 * compacts through a different branch — the dimension, the record-keyed tables, the column-merged demographics, and
 * the month-partitioned keyboard parts — so each needs its own proof.</p>
 */
public class SnapshotDeltaBuilderEdgeCasesTest {
    private static final String SNAPSHOT_DATE = "2026-09-24";

    private ExportStoreClient mockStore;
    private ParquetRowWriter mockWriter;
    private ParquetTableReader mockReader;
    private FileHelper mockFileHelper;
    private SnapshotDeltaBuilder builder;

    private File tempDir;
    private Map<String, List<TableRow>> rowsByFileName;

    @BeforeMethod
    public void before() throws Exception {
        mockStore = mock(ExportStoreClient.class);
        mockWriter = mock(ParquetRowWriter.class);
        mockReader = mock(ParquetTableReader.class);
        mockFileHelper = mock(FileHelper.class);
        tempDir = new File("/tmp/addf-snapshot-edge-test");
        rowsByFileName = new HashMap<>();

        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.<String>of());
        when(mockStore.listStaged(anyString())).thenReturn(ImmutableList.<String>of());
        when(mockStore.consolidatedExists(anyString())).thenReturn(false);
        when(mockStore.consolidatedKey(anyString()))
                .thenAnswer(inv -> "biaffect-3/current/tables/" + inv.getArgumentAt(0, String.class) + ".parquet");
        when(mockStore.keyboardPartKey(anyString(), anyString()))
                .thenAnswer(inv -> "biaffect-3/current/tables/keyboard_sessions/month=" + inv.getArgumentAt(0, String.class)
                        + "/part-" + inv.getArgumentAt(1, String.class) + ".parquet");
        when(mockFileHelper.newFile(any(File.class), anyString()))
                .thenAnswer(inv -> new File(tempDir, inv.getArgumentAt(1, String.class)));
        when(mockStore.download(anyString(), any(File.class)))
                .thenAnswer(inv -> inv.getArgumentAt(1, File.class));
        when(mockReader.read(anyString(), any(File.class)))
                .thenAnswer(inv -> rowsByFileName.getOrDefault(inv.getArgumentAt(1, File.class).getName(),
                        ImmutableList.<TableRow>of()));
        // Default: the consolidated participant_versions in the store was written under the current column list, so
        // the schema-drift rewrite does not fire. The one test that wants drift overrides this.
        when(mockReader.readColumnNames(any(File.class)))
                .thenReturn(AddfTables.columnNames(AddfTables.PARTICIPANT_VERSIONS));
        when(mockWriter.writeAll(anyString(), anyList(), any(File.class)))
                .thenAnswer(inv -> inv.getArgumentAt(2, File.class));

        builder = new SnapshotDeltaBuilder();
        builder.setExportStoreClient(mockStore);
        builder.setParquetRowWriter(mockWriter);
        builder.setParquetTableReader(mockReader);
        builder.setFileHelper(mockFileHelper);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Schema drift: a published file written under an older column list must not survive it
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void consolidatedFileWrittenUnderAnOlderColumnListIsRewrittenWithNothingElseChanged() throws Exception {
        // The PII fix (external_id / study_memberships withheld) only reaches ADDI's container if the already-
        // published file is rewritten. Nothing is staged and nobody withdrew, so without the drift check this
        // snapshot is a no-op and the retired columns sit in the delivery tree until the next version happens to
        // arrive — which, on a quiet study, can be never.
        when(mockStore.consolidatedExists(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(true);
        List<String> staleColumns = ImmutableList.<String>builder()
                .addAll(AddfTables.columnNames(AddfTables.PARTICIPANT_VERSIONS))
                .addAll(AddfTables.PII_WITHHELD_PARTICIPANT_FIELDS)
                .build();
        when(mockReader.readColumnNames(any(File.class))).thenReturn(staleColumns);
        rowsByFileName.put("participant_versions-existing.parquet",
                ImmutableList.of(versionRow("hc-1", 1), versionRow("hc-1", 2)));

        builder.build(SNAPSHOT_DATE, tempDir);

        // Both rows survive — this is a re-serialisation under the current schema, not a deletion. The writer takes
        // its columns from AddfTables, so the withheld ones are gone by construction.
        List<TableRow> written = captureWritten(AddfTables.PARTICIPANT_VERSIONS);
        assertEquals(written.size(), 2);
        assertEquals(captureWritten(AddfTables.PARTICIPANTS_CURRENT).size(), 1,
                "participants_current must be regenerated from the rewritten dimension, not left stale");
    }

    @Test
    public void matchingColumnListLeavesTheConsolidatedFileAlone() throws Exception {
        // The mirror of the above: the drift check must not turn every quiet snapshot into a full rewrite.
        when(mockStore.consolidatedExists(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(true);
        rowsByFileName.put("participant_versions-existing.parquet", ImmutableList.of(versionRow("hc-1", 1)));

        builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter, never()).writeAll(eq(AddfTables.PARTICIPANT_VERSIONS), anyList(), any(File.class));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Withdrawal: a tombstoned participant must leave every table family
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void tombstoneDropsTheParticipantsExistingVersionRows() throws Exception {
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-gone"));
        when(mockStore.consolidatedExists(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(true);
        rowsByFileName.put("participant_versions-existing.parquet", ImmutableList.of(
                versionRow("hc-gone", 1), versionRow("hc-gone", 2), versionRow("hc-stays", 1)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        List<TableRow> written = captureWritten(AddfTables.PARTICIPANT_VERSIONS);
        assertEquals(written.size(), 1, "every version of the withdrawn participant must go, not just the latest");
        assertEquals(written.get(0).get("health_code"), "hc-stays");
        assertTrue(delta.getTombstonedHealthCodes().contains("hc-gone"));
    }

    @Test
    public void tombstonedParticipantIsAlsoRemovedFromParticipantsCurrent() throws Exception {
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-gone"));
        when(mockStore.consolidatedExists(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(true);
        rowsByFileName.put("participant_versions-existing.parquet", ImmutableList.of(
                versionRow("hc-gone", 2), versionRow("hc-stays", 1)));

        builder.build(SNAPSHOT_DATE, tempDir);

        // participants_current is derived from the surviving versions, so the withdrawal propagates for free — but
        // only because the dimension is rebuilt first. Assert it, because a reordering would silently leave the
        // withdrawn participant in the "current" table.
        List<TableRow> current = captureWritten(AddfTables.PARTICIPANTS_CURRENT);
        assertEquals(current.size(), 1);
        assertEquals(current.get(0).get("health_code"), "hc-stays");
    }

    @Test
    public void tombstonedParticipantsStagedVersionIsConsumedWithoutBeingExported() throws Exception {
        // The withdrawal and a late version for the same participant can arrive in the same window. The staged row
        // must be swallowed — consumed so it does not retry forever, but never written to the delivery tree.
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-gone"));
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("stagedV"));
        rowsByFileName.put("participant_versions-staged-0.parquet",
                ImmutableList.of(versionRow("hc-gone", 5)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter, never()).writeAll(eq(AddfTables.PARTICIPANT_VERSIONS), anyList(), any(File.class));
        assertTrue(delta.getConsumedStagingKeys().contains("stagedV"),
                "the staged row must be retired, or it retries every snapshot forever");
    }

    @Test
    public void tombstoneCompactsDemographicsToo() throws Exception {
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-gone"));
        when(mockStore.consolidatedExists(AddfTables.DEMOGRAPHICS)).thenReturn(true);
        rowsByFileName.put("demographics-existing.parquet", ImmutableList.of(
                demographicsRow("hc-gone"), demographicsRow("hc-stays")));

        builder.build(SNAPSHOT_DATE, tempDir);

        List<TableRow> written = captureWritten(AddfTables.DEMOGRAPHICS);
        assertEquals(written.size(), 1);
        assertEquals(written.get(0).get("health_code"), "hc-stays");
    }

    @Test
    public void tombstonedParticipantsStagedDemographicsPartialIsConsumedNotMerged() throws Exception {
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-gone"));
        when(mockStore.listStaged(AddfTables.DEMOGRAPHICS)).thenReturn(ImmutableList.of("stagedD"));
        rowsByFileName.put("demographics-staged-0.parquet", ImmutableList.of(demographicsRow("hc-gone")));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter, never()).writeAll(eq(AddfTables.DEMOGRAPHICS), anyList(), any(File.class));
        assertTrue(delta.getConsumedStagingKeys().contains("stagedD"));
    }

    @Test
    public void tombstonedParticipantsStagedKeyboardSessionIsConsumedNotPublished() throws Exception {
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-gone"));
        when(mockStore.listStaged(AddfTables.KEYBOARD_SESSIONS)).thenReturn(ImmutableList.of("stagedK"));
        TableRow keyboard = activityRow(AddfTables.KEYBOARD_SESSIONS, "rec-k", "hc-gone", null);
        keyboard.put("session_start", "2026-08-15T10:30:00.000Z");
        rowsByFileName.put("keyboard_sessions-staged-0.parquet", ImmutableList.of(keyboard));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter, never()).writeAll(eq(AddfTables.KEYBOARD_SESSIONS), anyList(), any(File.class));
        assertTrue(delta.getConsumedStagingKeys().contains("stagedK"));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Keyboard month partitioning
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void keyboardRowsAreSplitByTheirOwnMonthNotTheSnapshotMonth() throws Exception {
        // A backlogged session belongs to the month it was typed in. Filing it under the snapshot's month would put
        // it in a partition researchers filtering by date would never look in.
        stageValidVersion("hc-1", 2);
        when(mockStore.listStaged(AddfTables.KEYBOARD_SESSIONS)).thenReturn(ImmutableList.of("k"));
        TableRow july = activityRow(AddfTables.KEYBOARD_SESSIONS, "rec-jul", "hc-1", 2L);
        july.put("session_start", "2026-07-02T10:00:00.000Z");
        TableRow august = activityRow(AddfTables.KEYBOARD_SESSIONS, "rec-aug", "hc-1", 2L);
        august.put("session_start", "2026-08-15T10:00:00.000Z");
        rowsByFileName.put("keyboard_sessions-staged-0.parquet", ImmutableList.of(july, august));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(hasBlob(delta, "biaffect-3/current/tables/keyboard_sessions/month=2026-07/part-"
                + SNAPSHOT_DATE + ".parquet"),
                blobKeys(delta));
        assertTrue(hasBlob(delta, "biaffect-3/current/tables/keyboard_sessions/month=2026-08/part-"
                + SNAPSHOT_DATE + ".parquet"),
                blobKeys(delta));
    }

    @Test
    public void keyboardRowWithNoSessionStartFallsBackToTheSnapshotMonth() throws Exception {
        // Unfileable is worse than approximately filed: a row with no timestamp still has to land somewhere a
        // consumer will find it, rather than being dropped.
        stageValidVersion("hc-1", 2);
        when(mockStore.listStaged(AddfTables.KEYBOARD_SESSIONS)).thenReturn(ImmutableList.of("k"));
        rowsByFileName.put("keyboard_sessions-staged-0.parquet",
                ImmutableList.of(activityRow(AddfTables.KEYBOARD_SESSIONS, "rec-k", "hc-1", 2L)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(hasBlob(delta, "biaffect-3/current/tables/keyboard_sessions/month=2026-09/part-"
                + SNAPSHOT_DATE + ".parquet"),
                blobKeys(delta));
    }

    @Test
    public void keyboardRowWithNeitherTimestampNorSnapshotMonthGoesToAnUnknownPartition() throws Exception {
        stageValidVersion("hc-1", 2);
        when(mockStore.listStaged(AddfTables.KEYBOARD_SESSIONS)).thenReturn(ImmutableList.of("k"));
        rowsByFileName.put("keyboard_sessions-staged-0.parquet",
                ImmutableList.of(activityRow(AddfTables.KEYBOARD_SESSIONS, "rec-k", "hc-1", 2L)));

        SnapshotDelta delta = builder.build("", tempDir);

        // Visibly wrong beats invisibly missing: an "unknown" partition is greppable, a dropped row is not.
        assertTrue(blobKeys(delta).contains("month=unknown"), blobKeys(delta));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Foreign-key edge values
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void aVersionStoredAsTextStillMatchesTheSameVersionStoredAsANumber() throws Exception {
        // Parquet hands integers back as Long, but a row rebuilt from elsewhere can carry the version as text. If the
        // FK comparison were string-based, "2" and 2 would not match and every such row would be deferred forever.
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("v"));
        TableRow version = new TableRow(AddfTables.PARTICIPANT_VERSIONS, "hc-1");
        version.put("health_code", "hc-1");
        version.put("participant_version", " 2 "); // whitespace-padded text
        rowsByFileName.put("participant_versions-staged-0.parquet", ImmutableList.of(version));

        when(mockStore.listStaged(AddfTables.PHQ9)).thenReturn(ImmutableList.of("p"));
        rowsByFileName.put("phq9-staged-0.parquet",
                ImmutableList.of(activityRow(AddfTables.PHQ9, "rec-1", "hc-1", 2L)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter).writeAll(eq(AddfTables.PHQ9), anyList(), any(File.class));
        assertTrue(delta.getConsumedStagingKeys().contains("p"), "the row should not have been deferred");
    }

    @Test
    public void commitWithNothingToRetireIssuesNoDeletes() throws Exception {
        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        builder.commit(delta);

        verify(mockStore, never()).deleteObjects(anyList());
        verify(mockStore, never()).deleteTombstone(anyString());
    }

    // -----------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------

    private void stageValidVersion(String healthCode, int version) {
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("v"));
        rowsByFileName.put("participant_versions-staged-0.parquet",
                ImmutableList.of(versionRow(healthCode, version)));
    }

    private List<TableRow> captureWritten(String table) throws Exception {
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockWriter).writeAll(eq(table), captor.capture(), any(File.class));
        @SuppressWarnings("unchecked")
        List<TableRow> rows = captor.getValue();
        return rows;
    }

    private static boolean hasBlob(SnapshotDelta delta, String key) {
        for (PublishedBlob blob : delta.getBlobs()) {
            if (blob.getKey().equals(key)) {
                return true;
            }
        }
        return false;
    }

    private static String blobKeys(SnapshotDelta delta) {
        StringBuilder sb = new StringBuilder("delta blobs: ");
        for (PublishedBlob blob : delta.getBlobs()) {
            sb.append(blob.getKey()).append(' ');
        }
        return sb.toString();
    }

    private static TableRow versionRow(String healthCode, int version) {
        TableRow row = new TableRow(AddfTables.PARTICIPANT_VERSIONS, healthCode);
        row.put("health_code", healthCode);
        row.put("participant_version", (long) version);
        return row;
    }

    private static TableRow demographicsRow(String healthCode) {
        TableRow row = new TableRow(AddfTables.DEMOGRAPHICS, healthCode);
        row.put("health_code", healthCode);
        row.put("birth_year", 1980L);
        return row;
    }

    private static TableRow activityRow(String table, String recordId, String healthCode, Long version) {
        TableRow row = new TableRow(table, recordId);
        row.put("record_id", recordId);
        row.put("health_code", healthCode);
        row.put("participant_version", version);
        return row;
    }
}
