package org.sagebionetworks.bridge.addf.publish;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
 * Exercises {@link SnapshotDeltaBuilder}'s orchestration (tombstone compaction, orphan deferral, participants_current
 * derivation) with all Parquet/S3 IO mocked — {@link ParquetTableReader} is stubbed to return rows keyed by the temp
 * file name the builder deterministically chooses, so we can drive existing-vs-staged reads precisely.
 */
public class SnapshotDeltaBuilderTest {
    private static final String SNAPSHOT_DATE = "2026-09-22";

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

    // -----------------------------------------------------------------------------------------------------------
    // demographics — the one table built by column-merging two separate uploads (§3.5.1), with merge-always
    // idempotency (§3.6: it never consults the ledger, so the same upload can legitimately be re-processed).
    // -----------------------------------------------------------------------------------------------------------

    /** The shape both demographics partials share: key, capture-time version, and the collection timestamp. */
    private TableRow demographicsPartial(String healthCode, long version, String collectedOn) {
        TableRow row = new TableRow(AddfTables.DEMOGRAPHICS, healthCode);
        row.put("health_code", healthCode);
        row.put("participant_version", version);
        row.put("collected_on", collectedOn);
        return row;
    }

    private TableRow birthGenderPartial() {
        return demographicsPartial("hc-1", 2L, "2026-08-15T20:00:47.984Z")
                .put("birth_year", 1981L)
                .put("gender", "Female");
    }

    private TableRow diagnosisPartial() {
        // 41 seconds after the birth-gender upload — the two are consecutive screens of the same onboarding flow.
        return demographicsPartial("hc-1", 2L, "2026-08-15T20:01:28.909Z")
                .put("bipolar_diagnosis", "I have never been diagnosed with bipolar disorder")
                .put("other_psych_diagnoses", "Anxiety,Depression,Seasonal affective disorder");
    }

    private void stageDemographics(TableRow first, TableRow second) {
        when(mockStore.listStaged(AddfTables.DEMOGRAPHICS)).thenReturn(ImmutableList.of("d0", "d1"));
        rowsByFileName.put("demographics-staged-0.parquet", ImmutableList.of(first));
        rowsByFileName.put("demographics-staged-1.parquet", ImmutableList.of(second));
    }

    @Test
    public void demographicsColumnMergeIsOrderIndependent() throws Exception {
        // Same two uploads, both orders. The merged row must be identical either way — the two arrive as separate
        // completeUpload events and nothing guarantees which one publish lists first.
        stageDemographics(birthGenderPartial(), diagnosisPartial());
        builder.build(SNAPSHOT_DATE, tempDir);

        stageDemographics(diagnosisPartial(), birthGenderPartial());
        builder.build(SNAPSHOT_DATE, tempDir);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockWriter, times(2)).writeAll(eq(AddfTables.DEMOGRAPHICS), captor.capture(), any(File.class));
        @SuppressWarnings("unchecked")
        List<TableRow> birthGenderFirst = (List<TableRow>) captor.getAllValues().get(0);
        @SuppressWarnings("unchecked")
        List<TableRow> diagnosisFirst = (List<TableRow>) captor.getAllValues().get(1);

        assertEquals(birthGenderFirst.size(), 1);
        assertEquals(diagnosisFirst.size(), 1);
        assertEquals(birthGenderFirst.get(0).getValues(), diagnosisFirst.get(0).getValues(),
                "demographics column-merge must be commutative");

        // And the single row is complete: one participant, all columns from both uploads.
        TableRow merged = birthGenderFirst.get(0);
        assertEquals(merged.get("health_code"), "hc-1");
        assertEquals(merged.get("birth_year"), 1981L);
        assertEquals(merged.get("gender"), "Female");
        assertEquals(merged.get("bipolar_diagnosis"), "I have never been diagnosed with bipolar disorder");
        assertEquals(merged.get("other_psych_diagnoses"), "Anxiety,Depression,Seasonal affective disorder");
        // collected_on is the only column both partials carry: the earlier of the pair wins, in either order. That
        // matches the delivered data, where collected_on tracks the birth-gender upload.
        assertEquals(merged.get("collected_on"), "2026-08-15T20:00:47.984Z");
    }

    @Test
    public void demographicsCollectedOnKeepsTheEarlierTimestamp() throws Exception {
        // Explicitly the reverse order, so a "first non-null wins" regression would show the later timestamp.
        stageDemographics(diagnosisPartial(), birthGenderPartial());

        builder.build(SNAPSHOT_DATE, tempDir);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockWriter).writeAll(eq(AddfTables.DEMOGRAPHICS), captor.capture(), any(File.class));
        @SuppressWarnings("unchecked")
        List<TableRow> rows = captor.getValue();
        assertEquals(rows.get(0).get("collected_on"), "2026-08-15T20:00:47.984Z");
    }

    @Test
    public void reprocessedDiagnosisDoesNotNullOutBirthGender() throws Exception {
        // Merge-always (§3.6): demographics never consults the ledger, so the same Diagnosis upload can be staged
        // again. The merge must be additive — a replace would wipe birth_year/gender from the delivered row.
        when(mockStore.consolidatedExists(AddfTables.DEMOGRAPHICS)).thenReturn(true);
        TableRow alreadyMerged = birthGenderPartial()
                .put("bipolar_diagnosis", "I have never been diagnosed with bipolar disorder");
        rowsByFileName.put("demographics-existing.parquet", ImmutableList.of(alreadyMerged));

        when(mockStore.listStaged(AddfTables.DEMOGRAPHICS)).thenReturn(ImmutableList.of("d"));
        rowsByFileName.put("demographics-staged-0.parquet", ImmutableList.of(diagnosisPartial()));

        builder.build(SNAPSHOT_DATE, tempDir);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockWriter).writeAll(eq(AddfTables.DEMOGRAPHICS), captor.capture(), any(File.class));
        @SuppressWarnings("unchecked")
        List<TableRow> rows = captor.getValue();
        assertEquals(rows.size(), 1, "re-processing must merge into the same participant row, not add a second");
        assertEquals(rows.get(0).get("birth_year"), 1981L);
        assertEquals(rows.get(0).get("gender"), "Female");
        assertEquals(rows.get(0).get("other_psych_diagnoses"), "Anxiety,Depression,Seasonal affective disorder");
        assertEquals(rows.get(0).get("collected_on"), "2026-08-15T20:00:47.984Z");
    }

    @Test
    public void demographicsIsMergedByHealthCodeNotByRecord() throws Exception {
        // Two participants staged together stay two rows; the merge key is health_code, not the staging object.
        when(mockStore.listStaged(AddfTables.DEMOGRAPHICS)).thenReturn(ImmutableList.of("d0", "d1"));
        rowsByFileName.put("demographics-staged-0.parquet", ImmutableList.of(birthGenderPartial()));
        rowsByFileName.put("demographics-staged-1.parquet", ImmutableList.of(
                demographicsPartial("hc-2", 3L, "2026-08-08T20:16:16.545Z").put("birth_year", 1979L)));

        builder.build(SNAPSHOT_DATE, tempDir);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockWriter).writeAll(eq(AddfTables.DEMOGRAPHICS), captor.capture(), any(File.class));
        assertEquals(captor.getValue().size(), 2);
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
}
