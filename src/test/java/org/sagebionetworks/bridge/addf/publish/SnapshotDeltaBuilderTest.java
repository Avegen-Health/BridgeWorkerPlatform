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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * Exercises {@link SnapshotDeltaBuilder}'s orchestration (tombstone compaction, orphan deferral, participants_current
 * derivation) with all Parquet/S3 IO mocked — {@link ParquetTableReader} is stubbed to return rows keyed by the temp
 * file name the builder deterministically chooses, so we can drive existing-vs-staged reads precisely.
 */
public class SnapshotDeltaBuilderTest {
    private static final String SNAPSHOT_DATE = "2026-09-22";

    private ExportStoreClient mockStore;
    private LedgerStore mockLedger;
    private ParquetRowWriter mockWriter;
    private ParquetTableReader mockReader;
    private FileHelper mockFileHelper;
    private SnapshotDeltaBuilder builder;

    private File tempDir;
    private Map<String, List<TableRow>> rowsByFileName;

    @BeforeMethod
    public void before() throws Exception {
        mockStore = mock(ExportStoreClient.class);
        mockLedger = mock(LedgerStore.class);
        mockWriter = mock(ParquetRowWriter.class);
        mockReader = mock(ParquetTableReader.class);
        mockFileHelper = mock(FileHelper.class);
        tempDir = new File("/tmp/addf-snapshot-test");
        rowsByFileName = new HashMap<>();

        // Default: nothing tombstoned, nothing staged, no existing consolidated files.
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.<String>of());
        when(mockStore.listStaged(anyString())).thenReturn(ImmutableList.<String>of());
        when(mockStore.consolidatedExists(anyString())).thenReturn(false);
        when(mockStore.consolidatedKey(anyString()))
                .thenAnswer(inv -> "biaffect-3/current/tables/" + inv.getArgumentAt(0, String.class) + ".parquet");
        when(mockStore.keyboardPartKey(anyString(), anyString()))
                .thenAnswer(inv -> "biaffect-3/keyboard_sessions/month=" + inv.getArgumentAt(0, String.class)
                        + "/part-" + inv.getArgumentAt(1, String.class) + ".parquet");

        // Raw layer: nothing delivered yet, every referenced archive present in the store.
        when(mockLedger.listDeliveredRaw()).thenReturn(new LinkedHashSet<String>());
        when(mockStore.rawKey(anyString())).thenAnswer(inv -> "biaffect-3/" + inv.getArgumentAt(0, String.class));
        when(mockStore.objectExists(anyString())).thenReturn(true);

        // newFile -> a File whose name we control; download returns that same file; reader reads rows by file name.
        when(mockFileHelper.newFile(any(File.class), anyString()))
                .thenAnswer(inv -> new File(tempDir, inv.getArgumentAt(1, String.class)));
        when(mockStore.download(anyString(), any(File.class)))
                .thenAnswer(inv -> inv.getArgumentAt(1, File.class));
        when(mockReader.read(anyString(), any(File.class)))
                .thenAnswer(inv -> rowsByFileName.getOrDefault(inv.getArgumentAt(1, File.class).getName(),
                        ImmutableList.<TableRow>of()));
        when(mockWriter.writeAll(anyString(), anyList(), any(File.class)))
                .thenAnswer(inv -> inv.getArgumentAt(2, File.class));

        builder = new SnapshotDeltaBuilder();
        builder.setExportStoreClient(mockStore);
        builder.setLedgerStore(mockLedger);
        builder.setParquetRowWriter(mockWriter);
        builder.setParquetTableReader(mockReader);
        builder.setFileHelper(mockFileHelper);
    }

    private TableRow versionRow(String healthCode, int version) {
        TableRow row = new TableRow(AddfTables.PARTICIPANT_VERSIONS, healthCode);
        row.put("health_code", healthCode);
        row.put("participant_version", (long) version);
        row.put("sharing_scope", "ALL_QUALIFIED_RESEARCHERS");
        row.put("created_on", "2026-08-01T00:00:00.000Z");
        return row;
    }

    private TableRow activityRow(String table, String recordId, String healthCode, Long version) {
        TableRow row = new TableRow(table, recordId);
        row.put("record_id", recordId);
        row.put("health_code", healthCode);
        row.put("participant_version", version);
        return row;
    }

    private TableRow fileRecordRow(String recordId, String healthCode, Long version, String fileName) {
        TableRow row = activityRow(AddfTables.FILE_RECORDS, recordId, healthCode, version);
        row.put("file_name", fileName);
        return row;
    }

    /** Stage one file_records row (with a valid participant_version) referencing {@code fileName} in the raw layer. */
    private void stageFileRecordWithRaw(String recordId, String fileName) {
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("v"));
        rowsByFileName.put("participant_versions-staged-0.parquet", ImmutableList.of(versionRow("hc-1", 2)));
        when(mockStore.listStaged(AddfTables.FILE_RECORDS)).thenReturn(ImmutableList.of("f"));
        rowsByFileName.put("file_records-staged-0.parquet",
                ImmutableList.of(fileRecordRow(recordId, "hc-1", 2L, fileName)));
    }

    private boolean deltaHasBlob(SnapshotDelta delta, String key) {
        return delta.getBlobs().stream().anyMatch(b -> b.getKey().equals(key));
    }

    /** Capture the single deleteObjects batch commit() issues and assert it contains each expected staging key. */
    private void assertCommitDeleted(String... expectedKeys) {
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockStore).deleteObjects(captor.capture());
        List<?> deleted = captor.getValue();
        for (String key : expectedKeys) {
            assertTrue(deleted.contains(key), "expected commit to delete staging key " + key + " but got " + deleted);
        }
    }

    @Test
    public void emptyEverythingProducesNoDelta() throws Exception {
        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);
        assertTrue(delta.getBlobs().isEmpty());
        verify(mockWriter, never()).writeAll(anyString(), anyList(), any(File.class));
        // Nothing consumed: commit must not issue a delete batch.
        builder.commit(delta);
        verify(mockStore, never()).deleteObjects(anyList());
    }

    @Test
    public void newVersionWritesParticipantVersionsAndCurrent() throws Exception {
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("stagedV"));
        rowsByFileName.put("participant_versions-staged-0.parquet", ImmutableList.of(versionRow("hc-1", 2)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        // participant_versions + participants_current both (re)written.
        verify(mockWriter).writeAll(eq(AddfTables.PARTICIPANT_VERSIONS), anyList(), any(File.class));
        verify(mockWriter).writeAll(eq(AddfTables.PARTICIPANTS_CURRENT), anyList(), any(File.class));
        assertEquals(delta.getBlobs().size(), 2);
        // build() does NOT delete staging; commit() does, only after the (mocked) upload would have run.
        verify(mockStore, never()).deleteObjects(anyList());
        builder.commit(delta);
        verify(mockStore).deleteObjects(ImmutableList.of("stagedV"));
    }

    @Test
    public void participantsCurrentPicksMaxVersion() throws Exception {
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("s1", "s2"));
        rowsByFileName.put("participant_versions-staged-0.parquet", ImmutableList.of(versionRow("hc-1", 2)));
        rowsByFileName.put("participant_versions-staged-1.parquet", ImmutableList.of(versionRow("hc-1", 5)));

        builder.build(SNAPSHOT_DATE, tempDir);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockWriter).writeAll(eq(AddfTables.PARTICIPANTS_CURRENT), captor.capture(), any(File.class));
        @SuppressWarnings("unchecked")
        List<TableRow> currentRows = captor.getValue();
        assertEquals(currentRows.size(), 1);
        assertEquals(currentRows.get(0).get("participant_version"), 5L);
    }

    @Test
    public void activityRowWithValidVersionIsWritten() throws Exception {
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("v"));
        rowsByFileName.put("participant_versions-staged-0.parquet", ImmutableList.of(versionRow("hc-1", 2)));
        when(mockStore.listStaged(AddfTables.PHQ9)).thenReturn(ImmutableList.of("p"));
        rowsByFileName.put("phq9-staged-0.parquet", ImmutableList.of(activityRow(AddfTables.PHQ9, "rec-1", "hc-1", 2L)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter).writeAll(eq(AddfTables.PHQ9), anyList(), any(File.class));
        // Consumed keys are aggregated across tables (version "v" + phq9 "p") into one commit batch.
        builder.commit(delta);
        assertCommitDeleted("p");
    }

    @Test
    public void orphanActivityRowIsDeferredNotWritten() throws Exception {
        // No participant_versions staged -> valid set empty -> the pv=9 row is an orphan.
        when(mockStore.listStaged(AddfTables.PHQ9)).thenReturn(ImmutableList.of("p"));
        rowsByFileName.put("phq9-staged-0.parquet", ImmutableList.of(activityRow(AddfTables.PHQ9, "rec-1", "hc-1", 9L)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(delta.getBlobs().isEmpty());
        verify(mockWriter, never()).writeAll(eq(AddfTables.PHQ9), anyList(), any(File.class));
        // Deferred: the orphan staged object is NOT consumed, so it is absent from the commit batch and retries next
        // snapshot. Nothing else is consumed either, so commit issues no delete at all.
        builder.commit(delta);
        verify(mockStore, never()).deleteObjects(anyList());
    }

    @Test
    public void nullVersionActivityRowIsNotAnOrphan() throws Exception {
        when(mockStore.listStaged(AddfTables.PHQ9)).thenReturn(ImmutableList.of("p"));
        rowsByFileName.put("phq9-staged-0.parquet",
                ImmutableList.of(activityRow(AddfTables.PHQ9, "rec-1", "hc-1", null)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        // Null participant_version has no FK to check — it is written through.
        verify(mockWriter).writeAll(eq(AddfTables.PHQ9), anyList(), any(File.class));
        builder.commit(delta);
        verify(mockStore).deleteObjects(ImmutableList.of("p"));
    }

    @Test
    public void tombstoneDropsExistingRowAndClearsMarker() throws Exception {
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-x"));
        when(mockStore.consolidatedExists(AddfTables.PHQ9)).thenReturn(true);
        rowsByFileName.put("phq9-existing.parquet", ImmutableList.of(
                activityRow(AddfTables.PHQ9, "rec-1", "hc-x", 2L),
                activityRow(AddfTables.PHQ9, "rec-2", "hc-2", 2L)));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockWriter).writeAll(eq(AddfTables.PHQ9), captor.capture(), any(File.class));
        @SuppressWarnings("unchecked")
        List<TableRow> written = captor.getValue();
        assertEquals(written.size(), 1);
        assertEquals(written.get(0).get("record_id"), "rec-2");
        // Tombstone markers are cleared by commit(), not build().
        verify(mockStore, never()).deleteTombstone(anyString());
        builder.commit(delta);
        verify(mockStore).deleteTombstone("hc-x");
    }

    @Test
    public void keyboardRowWritesDatedMonthPart() throws Exception {
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("v"));
        rowsByFileName.put("participant_versions-staged-0.parquet", ImmutableList.of(versionRow("hc-1", 2)));

        when(mockStore.listStaged(AddfTables.KEYBOARD_SESSIONS)).thenReturn(ImmutableList.of("k"));
        TableRow kb = activityRow(AddfTables.KEYBOARD_SESSIONS, "rec-kb", "hc-1", 2L);
        kb.put("session_start", "2026-08-15T10:30:00.000Z");
        rowsByFileName.put("keyboard_sessions-staged-0.parquet", ImmutableList.of(kb));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter).writeAll(eq(AddfTables.KEYBOARD_SESSIONS), anyList(), any(File.class));
        boolean hasKeyboardPart = delta.getBlobs().stream().anyMatch(
                b -> b.getKey().equals("biaffect-3/keyboard_sessions/month=2026-08/part-2026-09-22.parquet"));
        assertTrue(hasKeyboardPart, "expected a dated keyboard month part in the delta");
        // Consumed keys aggregate across tables (version "v" + keyboard "k") into one commit batch.
        builder.commit(delta);
        assertCommitDeleted("k");
    }

    // -----------------------------------------------------------------------------------------------------------
    // Raw layer (§4.3.4) — archives ride the same delta as the tables, ledger-gated to ship exactly once.
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void rawArchiveIsDeliveredAndMarkedOnCommit() throws Exception {
        stageFileRecordWithRaw("rec-1", "raw/2026-09-22/rec-1-PHQ-9.zip");

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(deltaHasBlob(delta, "biaffect-3/raw/2026-09-22/rec-1-PHQ-9.zip"),
                "expected the raw archive in the Azure upload set, got " + delta.getBlobs());
        assertEquals(delta.getDeliveredRawKeys(), ImmutableList.of("raw/2026-09-22/rec-1-PHQ-9.zip"));
        // Ledger is marked by commit(), not build() — a crash before the upload confirms must replay the archive.
        verify(mockLedger, never()).markRawDelivered(anyString());
        builder.commit(delta);
        verify(mockLedger).markRawDelivered("raw/2026-09-22/rec-1-PHQ-9.zip");
    }

    @Test
    public void alreadyDeliveredRawArchiveIsNotReUploaded() throws Exception {
        stageFileRecordWithRaw("rec-1", "raw/2026-09-22/rec-1-PHQ-9.zip");
        when(mockLedger.listDeliveredRaw()).thenReturn(ImmutableSet.of("raw/2026-09-22/rec-1-PHQ-9.zip"));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(delta.getDeliveredRawKeys().isEmpty());
        assertTrue(!deltaHasBlob(delta, "biaffect-3/raw/2026-09-22/rec-1-PHQ-9.zip"));
        verify(mockStore, never()).download(eq("biaffect-3/raw/2026-09-22/rec-1-PHQ-9.zip"), any(File.class));
    }

    @Test
    public void alreadyPublishedFileRecordStillDeliversItsUnsentRawArchive() throws Exception {
        // No staging at all — the row is already in the consolidated table from an earlier snapshot. Its archive must
        // still ship: this is the catch-up path for records published before raw delivery existed.
        when(mockStore.consolidatedExists(AddfTables.FILE_RECORDS)).thenReturn(true);
        rowsByFileName.put("file_records-existing.parquet",
                ImmutableList.of(fileRecordRow("rec-old", "hc-1", 2L, "raw/2026-09-01/rec-old-Evening_Log.zip")));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(deltaHasBlob(delta, "biaffect-3/raw/2026-09-01/rec-old-Evening_Log.zip"));
        // The table itself did not change, so only the archive is in the delta.
        assertEquals(delta.getBlobs().size(), 1);
        verify(mockWriter, never()).writeAll(eq(AddfTables.FILE_RECORDS), anyList(), any(File.class));
    }

    @Test
    public void orphanFileRecordDefersItsRawArchive() throws Exception {
        // pv=9 with no participant_versions staged -> orphan -> row deferred, so its archive must not ship either.
        when(mockStore.listStaged(AddfTables.FILE_RECORDS)).thenReturn(ImmutableList.of("f"));
        rowsByFileName.put("file_records-staged-0.parquet",
                ImmutableList.of(fileRecordRow("rec-1", "hc-1", 9L, "raw/2026-09-22/rec-1-PHQ-9.zip")));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(delta.getBlobs().isEmpty());
        assertTrue(delta.getDeliveredRawKeys().isEmpty());
    }

    @Test
    public void tombstonedParticipantsRawArchiveIsNotDelivered() throws Exception {
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-x"));
        when(mockStore.consolidatedExists(AddfTables.FILE_RECORDS)).thenReturn(true);
        rowsByFileName.put("file_records-existing.parquet",
                ImmutableList.of(fileRecordRow("rec-x", "hc-x", 2L, "raw/2026-09-01/rec-x-PHQ-9.zip")));

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(delta.getDeliveredRawKeys().isEmpty(), "withdrawn participant's archive must never ship");
        assertTrue(!deltaHasBlob(delta, "biaffect-3/raw/2026-09-01/rec-x-PHQ-9.zip"));
    }

    @Test
    public void missingRawArchiveIsSkippedNotFatal() throws Exception {
        stageFileRecordWithRaw("rec-1", "raw/2026-09-22/rec-1-PHQ-9.zip");
        when(mockStore.objectExists("biaffect-3/raw/2026-09-22/rec-1-PHQ-9.zip")).thenReturn(false);

        SnapshotDelta delta = builder.build(SNAPSHOT_DATE, tempDir);

        // The tables still publish; the absent archive is skipped (and logged) rather than failing the whole day.
        verify(mockWriter).writeAll(eq(AddfTables.FILE_RECORDS), anyList(), any(File.class));
        assertTrue(delta.getDeliveredRawKeys().isEmpty());
        builder.commit(delta);
        verify(mockLedger, never()).markRawDelivered(anyString());
    }
}
