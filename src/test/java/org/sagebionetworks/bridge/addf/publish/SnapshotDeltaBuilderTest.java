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

    @Test
    public void emptyEverythingProducesNoDelta() throws Exception {
        List<PublishedBlob> delta = builder.build(SNAPSHOT_DATE, tempDir);
        assertTrue(delta.isEmpty());
        verify(mockWriter, never()).writeAll(anyString(), anyList(), any(File.class));
    }

    @Test
    public void newVersionWritesParticipantVersionsAndCurrent() throws Exception {
        when(mockStore.listStaged(AddfTables.PARTICIPANT_VERSIONS)).thenReturn(ImmutableList.of("stagedV"));
        rowsByFileName.put("participant_versions-staged-0.parquet", ImmutableList.of(versionRow("hc-1", 2)));

        List<PublishedBlob> delta = builder.build(SNAPSHOT_DATE, tempDir);

        // participant_versions + participants_current both (re)written.
        verify(mockWriter).writeAll(eq(AddfTables.PARTICIPANT_VERSIONS), anyList(), any(File.class));
        verify(mockWriter).writeAll(eq(AddfTables.PARTICIPANTS_CURRENT), anyList(), any(File.class));
        assertEquals(delta.size(), 2);
        // Consumed the staged version object.
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

        builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter).writeAll(eq(AddfTables.PHQ9), anyList(), any(File.class));
        verify(mockStore).deleteObjects(ImmutableList.of("p"));
    }

    @Test
    public void orphanActivityRowIsDeferredNotWritten() throws Exception {
        // No participant_versions staged -> valid set empty -> the pv=9 row is an orphan.
        when(mockStore.listStaged(AddfTables.PHQ9)).thenReturn(ImmutableList.of("p"));
        rowsByFileName.put("phq9-staged-0.parquet", ImmutableList.of(activityRow(AddfTables.PHQ9, "rec-1", "hc-1", 9L)));

        List<PublishedBlob> delta = builder.build(SNAPSHOT_DATE, tempDir);

        assertTrue(delta.isEmpty());
        verify(mockWriter, never()).writeAll(eq(AddfTables.PHQ9), anyList(), any(File.class));
        // Deferred: the staged object is NOT deleted (it retries next snapshot).
        verify(mockStore).deleteObjects(ImmutableList.<String>of());
    }

    @Test
    public void nullVersionActivityRowIsNotAnOrphan() throws Exception {
        when(mockStore.listStaged(AddfTables.PHQ9)).thenReturn(ImmutableList.of("p"));
        rowsByFileName.put("phq9-staged-0.parquet",
                ImmutableList.of(activityRow(AddfTables.PHQ9, "rec-1", "hc-1", null)));

        builder.build(SNAPSHOT_DATE, tempDir);

        // Null participant_version has no FK to check — it is written through.
        verify(mockWriter).writeAll(eq(AddfTables.PHQ9), anyList(), any(File.class));
        verify(mockStore).deleteObjects(ImmutableList.of("p"));
    }

    @Test
    public void tombstoneDropsExistingRowAndClearsMarker() throws Exception {
        when(mockStore.listTombstonedHealthCodes()).thenReturn(ImmutableList.of("hc-x"));
        when(mockStore.consolidatedExists(AddfTables.PHQ9)).thenReturn(true);
        rowsByFileName.put("phq9-existing.parquet", ImmutableList.of(
                activityRow(AddfTables.PHQ9, "rec-1", "hc-x", 2L),
                activityRow(AddfTables.PHQ9, "rec-2", "hc-2", 2L)));

        builder.build(SNAPSHOT_DATE, tempDir);

        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockWriter).writeAll(eq(AddfTables.PHQ9), captor.capture(), any(File.class));
        @SuppressWarnings("unchecked")
        List<TableRow> written = captor.getValue();
        assertEquals(written.size(), 1);
        assertEquals(written.get(0).get("record_id"), "rec-2");
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

        List<PublishedBlob> delta = builder.build(SNAPSHOT_DATE, tempDir);

        verify(mockWriter).writeAll(eq(AddfTables.KEYBOARD_SESSIONS), anyList(), any(File.class));
        boolean hasKeyboardPart = delta.stream().anyMatch(
                b -> b.getKey().equals("biaffect-3/keyboard_sessions/month=2026-08/part-2026-09-22.parquet"));
        assertTrue(hasKeyboardPart, "expected a dated keyboard month part in the delta");
        verify(mockStore).deleteObjects(ImmutableList.of("k"));
    }
}
