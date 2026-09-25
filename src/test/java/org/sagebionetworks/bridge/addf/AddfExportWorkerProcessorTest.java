package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.File;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.decrypt.DecryptedArchive;
import org.sagebionetworks.bridge.addf.decrypt.UploadFetcher;
import org.sagebionetworks.bridge.addf.gate.ConsentTestGate;
import org.sagebionetworks.bridge.addf.gate.ConsentVerdict;
import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.addf.transform.ClientInfo;
import org.sagebionetworks.bridge.addf.transform.FileRecordBuilder;
import org.sagebionetworks.bridge.addf.transform.FlattenContext;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.RecordFlattener;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

public class AddfExportWorkerProcessorTest {
    private static final String APP_ID = "app-id";
    private static final String RECORD_ID = "rec-1";
    private static final String HEALTH_CODE = "health-code";
    private static final String ITEM = "PHQ-9";
    private static final String RAW_KEY = "raw/2026-08-15/rec-1-PHQ-9.zip";
    private static final DateTime CREATED_ON = new DateTime(2026, 8, 15, 10, 30, 0, DateTimeZone.forOffsetHours(-4));
    /** What Bridge actually returns from {@code getClientInfo()} — a JSON object, not a user-agent string. */
    private static final String CLIENT_INFO_JSON = "{\"appName\":\"biaffect-3\",\"appVersion\":68,"
            + "\"deviceName\":\"iPhone 11 Pro\",\"osName\":\"iPhone OS\",\"osVersion\":\"26.5.2\"}";
    private static final String USER_AGENT = "biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)";

    private BridgeHelper mockBridgeHelper;
    private ConsentTestGate mockGate;
    private ExportStoreClient mockExportStore;
    private FileHelper mockFileHelper;
    private FileRecordBuilder mockFileRecordBuilder;
    private LedgerStore mockLedger;
    private ParquetRowWriter mockWriter;
    private RecordFlattener mockFlattener;
    private UploadFetcher mockUploadFetcher;

    private HealthDataRecordEx3 mockRecord;
    private DecryptedArchive mockArchive;
    private File tempDir;
    private AddfExportWorkerProcessor processor;

    @BeforeMethod
    public void before() throws Exception {
        mockBridgeHelper = mock(BridgeHelper.class);
        mockGate = mock(ConsentTestGate.class);
        mockExportStore = mock(ExportStoreClient.class);
        mockFileHelper = mock(FileHelper.class);
        mockFileRecordBuilder = mock(FileRecordBuilder.class);
        mockLedger = mock(LedgerStore.class);
        mockWriter = mock(ParquetRowWriter.class);
        mockFlattener = mock(RecordFlattener.class);
        mockUploadFetcher = mock(UploadFetcher.class);

        processor = new AddfExportWorkerProcessor();
        processor.setBridgeHelper(mockBridgeHelper);
        processor.setConsentTestGate(mockGate);
        processor.setExportStoreClient(mockExportStore);
        processor.setFileHelper(mockFileHelper);
        processor.setFileRecordBuilder(mockFileRecordBuilder);
        processor.setLedgerStore(mockLedger);
        processor.setParquetRowWriter(mockWriter);
        processor.setRecordFlattener(mockFlattener);
        processor.setUploadFetcher(mockUploadFetcher);

        mockRecord = mock(HealthDataRecordEx3.class);
        when(mockRecord.getHealthCode()).thenReturn(HEALTH_CODE);
        when(mockRecord.getCreatedOn()).thenReturn(CREATED_ON);
        when(mockRecord.getClientInfo()).thenReturn(CLIENT_INFO_JSON);
        when(mockRecord.getUserAgent()).thenReturn(USER_AGENT);
        when(mockRecord.getParticipantVersion()).thenReturn(3);

        mockArchive = mock(DecryptedArchive.class);
        when(mockArchive.getDecryptedArchiveFile()).thenReturn(new File("archive.zip"));

        tempDir = new File("temp-dir");
    }

    private static AddfExportRequest request() {
        AddfExportRequest request = new AddfExportRequest();
        request.setAppId(APP_ID);
        request.setRecordId(RECORD_ID);
        return request;
    }

    /** Wire up the common happy-path collaborators up to (but not including) the content-row decision. */
    private void stubThroughFetch() throws Exception {
        when(mockLedger.containsRecord(RECORD_ID)).thenReturn(false);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(APP_ID, RECORD_ID)).thenReturn(mockRecord);
        when(mockGate.evaluate(APP_ID, HEALTH_CODE)).thenReturn(ConsentVerdict.export(false));
        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(mock(App.class));
        when(mockFileHelper.createTempDir()).thenReturn(tempDir);
        when(mockUploadFetcher.fetch(any(App.class), eq(mockRecord), eq(tempDir))).thenReturn(mockArchive);
        when(mockFlattener.resolveItem(mockArchive)).thenReturn(ITEM);
        when(mockExportStore.putRaw(eq("2026-08-15"), eq(RECORD_ID), eq(ITEM), any(File.class))).thenReturn(RAW_KEY);
    }

    @Test
    public void alreadyProcessedSkips() throws Exception {
        when(mockLedger.containsRecord(RECORD_ID)).thenReturn(true);
        processor.process(request());
        verify(mockBridgeHelper, never()).getHealthDataRecordForExporter3(any(String.class), any(String.class));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void nullRecordThrowsBadRequest() throws Exception {
        when(mockLedger.containsRecord(RECORD_ID)).thenReturn(false);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(APP_ID, RECORD_ID)).thenReturn(null);
        processor.process(request());
    }

    @Test
    public void gateSkipShortCircuits() throws Exception {
        when(mockLedger.containsRecord(RECORD_ID)).thenReturn(false);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(APP_ID, RECORD_ID)).thenReturn(mockRecord);
        when(mockGate.evaluate(APP_ID, HEALTH_CODE)).thenReturn(ConsentVerdict.skip(false));

        processor.process(request());

        verify(mockBridgeHelper, never()).getApp(any(String.class));
        verify(mockFileHelper, never()).createTempDir();
    }

    @Test
    public void happyPathStagesContentAndManifest() throws Exception {
        stubThroughFetch();
        TableRow contentRow = new TableRow("phq9", RECORD_ID);
        when(mockFlattener.flattenContent(any(), eq(ITEM))).thenReturn(contentRow);
        when(mockFileHelper.newFile(tempDir, "content.parquet")).thenReturn(new File("content.parquet"));
        when(mockFileHelper.newFile(tempDir, "file_records.parquet")).thenReturn(new File("fr.parquet"));
        TableRow manifestRow = new TableRow("file_records", RECORD_ID);
        when(mockFileRecordBuilder.build(any(), eq(ITEM), eq(RAW_KEY), any(String.class))).thenReturn(manifestRow);

        processor.process(request());

        verify(mockWriter, times(2)).write(any(TableRow.class), any(File.class));
        verify(mockExportStore).stageTableRow(eq("phq9"), eq(RECORD_ID), any(File.class));
        verify(mockExportStore).stageTableRow(eq("file_records"), eq(RECORD_ID), any(File.class));
        verify(mockExportStore).putRaw(eq("2026-08-15"), eq(RECORD_ID), eq(ITEM), any(File.class));
        verify(mockLedger).markRecord(RECORD_ID);
        verify(mockFileHelper).deleteDirRecursively(tempDir);
    }

    /**
     * Regression: the worker must resolve ClientInfo from BOTH record fields. It previously passed only the
     * {@code clientInfo} JSON through the user-agent regex, which matched nothing — so {@code app_version}/
     * {@code platform} were null on every activity row and {@code app_version}/{@code device_name}/{@code os_name}/
     * {@code os_version} were null on every {@code file_records} row, in every published snapshot. Asserting on the
     * FlattenContext handed to the builders is what catches it; the builder-level tests cannot, because they
     * construct their own ClientInfo.
     */
    @Test
    public void flattenContextCarriesResolvedClientInfo() throws Exception {
        stubThroughFetch();
        when(mockFlattener.flattenContent(any(), eq(ITEM))).thenReturn(null);
        when(mockFileHelper.newFile(tempDir, "file_records.parquet")).thenReturn(new File("fr.parquet"));
        when(mockFileRecordBuilder.build(any(), eq(ITEM), eq(RAW_KEY), any(String.class)))
                .thenReturn(new TableRow("file_records", RECORD_ID));

        processor.process(request());

        ArgumentCaptor<FlattenContext> captor = ArgumentCaptor.forClass(FlattenContext.class);
        verify(mockFileRecordBuilder).build(captor.capture(), eq(ITEM), eq(RAW_KEY), any(String.class));
        ClientInfo clientInfo = captor.getValue().getClientInfo();
        assertEquals(clientInfo.getAppVersion(), "68");
        assertEquals(clientInfo.getDeviceName(), "iPhone 11 Pro");
        // "iPhone OS" proves the JSON clientInfo was the source (the user agent would have said "iOS").
        assertEquals(clientInfo.getOsName(), "iPhone OS");
        assertEquals(clientInfo.getOsVersion(), "26.5.2");
        assertEquals(clientInfo.getPlatform(), "ios");
    }

    /** A record with no clientInfo JSON still gets its columns, from the user agent. */
    @Test
    public void flattenContextFallsBackToUserAgent() throws Exception {
        stubThroughFetch();
        when(mockRecord.getClientInfo()).thenReturn(null);
        when(mockFlattener.flattenContent(any(), eq(ITEM))).thenReturn(null);
        when(mockFileHelper.newFile(tempDir, "file_records.parquet")).thenReturn(new File("fr.parquet"));
        when(mockFileRecordBuilder.build(any(), eq(ITEM), eq(RAW_KEY), any(String.class)))
                .thenReturn(new TableRow("file_records", RECORD_ID));

        processor.process(request());

        ArgumentCaptor<FlattenContext> captor = ArgumentCaptor.forClass(FlattenContext.class);
        verify(mockFileRecordBuilder).build(captor.capture(), eq(ITEM), eq(RAW_KEY), any(String.class));
        ClientInfo clientInfo = captor.getValue().getClientInfo();
        assertEquals(clientInfo.getAppVersion(), "68");
        assertEquals(clientInfo.getOsName(), "iOS");
        assertEquals(clientInfo.getPlatform(), "ios");
    }

    @Test
    public void unmappedContentAndNullVersionStillStagesManifest() throws Exception {
        stubThroughFetch();
        when(mockRecord.getParticipantVersion()).thenReturn(null);
        when(mockFlattener.flattenContent(any(), eq(ITEM))).thenReturn(null);
        when(mockFileHelper.newFile(tempDir, "file_records.parquet")).thenReturn(new File("fr.parquet"));
        TableRow manifestRow = new TableRow("file_records", RECORD_ID);
        when(mockFileRecordBuilder.build(any(), eq(ITEM), eq(RAW_KEY), any(String.class))).thenReturn(manifestRow);

        processor.process(request());

        // Only the manifest row is written when there is no content row.
        verify(mockWriter, times(1)).write(any(TableRow.class), any(File.class));
        verify(mockExportStore).stageTableRow(eq("file_records"), eq(RECORD_ID), any(File.class));
        verify(mockExportStore, never()).stageTableRow(eq("phq9"), any(String.class), any(File.class));
        verify(mockLedger).markRecord(RECORD_ID);
    }

    @Test
    public void backloggedRecordKeepsItsCaptureTimeParticipantVersion() throws Exception {
        // §3.5.3: participant_version is the version that applied when the record was *captured*, read straight off
        // HealthDataRecordEx3 — never a fresh "what is this participant on now?" lookup. A record that sat in the
        // upload backlog while the participant moved on must still be filed under its own version, or every
        // downstream join silently attributes old data to newer participant state.
        stubThroughFetch();
        when(mockRecord.getParticipantVersion()).thenReturn(3);

        when(mockFlattener.flattenContent(any(), eq(ITEM))).thenReturn(new TableRow("phq9", RECORD_ID));
        when(mockFileHelper.newFile(tempDir, "content.parquet")).thenReturn(new File("content.parquet"));
        when(mockFileHelper.newFile(tempDir, "file_records.parquet")).thenReturn(new File("fr.parquet"));
        when(mockFileRecordBuilder.build(any(), eq(ITEM), eq(RAW_KEY), any(String.class)))
                .thenReturn(new TableRow("file_records", RECORD_ID));

        processor.process(request());

        ArgumentCaptor<FlattenContext> contentContext = ArgumentCaptor.forClass(FlattenContext.class);
        verify(mockFlattener).flattenContent(contentContext.capture(), eq(ITEM));
        assertEquals(contentContext.getValue().getParticipantVersion(), Integer.valueOf(3));

        // The manifest row is built from the same context, so file_records carries the same capture-time FK.
        ArgumentCaptor<FlattenContext> manifestContext = ArgumentCaptor.forClass(FlattenContext.class);
        verify(mockFileRecordBuilder).build(manifestContext.capture(), eq(ITEM), eq(RAW_KEY), any(String.class));
        assertEquals(manifestContext.getValue().getParticipantVersion(), Integer.valueOf(3));

        // And the worker never asks Bridge for the participant's current version — that lookup would be the drift bug.
        verify(mockBridgeHelper, never()).getParticipantVersion(any(String.class), any(String.class), anyInt());
    }

    @Test
    public void ledgerPresenceSkipIsKeyedByRecordNotParticipant() throws Exception {
        // §3.6: the three idempotency modes are not uniform. Activity/file_records presence-skip by record_id; the
        // participant-keyed tables do not. Keying the skip on the record is what lets demographics stay merge-always —
        // a participant's second demographics upload must not be skipped because their first one was processed.
        stubThroughFetch();
        when(mockFlattener.flattenContent(any(), eq(ITEM))).thenReturn(null);
        when(mockFileHelper.newFile(tempDir, "file_records.parquet")).thenReturn(new File("fr.parquet"));
        when(mockFileRecordBuilder.build(any(), eq(ITEM), eq(RAW_KEY), any(String.class)))
                .thenReturn(new TableRow("file_records", RECORD_ID));

        processor.process(request());

        verify(mockLedger).containsRecord(RECORD_ID);
        verify(mockLedger).markRecord(RECORD_ID);
        // The record scope is the only one the accumulate worker touches: no version-scope entry is written here
        // (that is the dimension worker's mode), and there is no demographics/health-code scope at all.
        verify(mockLedger, never()).containsVersion(any(String.class), anyInt());
        verify(mockLedger, never()).markVersion(any(String.class), anyInt());
    }
}
