package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.gate.ConsentTestGate;
import org.sagebionetworks.bridge.addf.gate.ConsentVerdict;
import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.ParticipantVersionRowBuilder;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

public class AddfParticipantVersionWorkerProcessorTest {
    private static final String APP_ID = "app-id";
    private static final String HEALTH_CODE = "health-code";
    private static final String USER_ID = "healthCode:" + HEALTH_CODE;
    private static final int VERSION = 2;

    private BridgeHelper mockBridgeHelper;
    private ConsentTestGate mockGate;
    private ExportStoreClient mockExportStore;
    private FileHelper mockFileHelper;
    private LedgerStore mockLedger;
    private ParquetRowWriter mockWriter;
    private ParticipantVersionRowBuilder mockRowBuilder;
    private AddfParticipantVersionWorkerProcessor processor;

    @BeforeMethod
    public void before() {
        mockBridgeHelper = mock(BridgeHelper.class);
        mockGate = mock(ConsentTestGate.class);
        mockExportStore = mock(ExportStoreClient.class);
        mockFileHelper = mock(FileHelper.class);
        mockLedger = mock(LedgerStore.class);
        mockWriter = mock(ParquetRowWriter.class);
        mockRowBuilder = mock(ParticipantVersionRowBuilder.class);

        processor = new AddfParticipantVersionWorkerProcessor();
        processor.setBridgeHelper(mockBridgeHelper);
        processor.setConsentTestGate(mockGate);
        processor.setExportStoreClient(mockExportStore);
        processor.setFileHelper(mockFileHelper);
        processor.setLedgerStore(mockLedger);
        processor.setParquetRowWriter(mockWriter);
        processor.setParticipantVersionRowBuilder(mockRowBuilder);
    }

    private static AddfParticipantVersionRequest request() {
        AddfParticipantVersionRequest request = new AddfParticipantVersionRequest();
        request.setAppId(APP_ID);
        request.setHealthCode(HEALTH_CODE);
        request.setParticipantVersion(VERSION);
        return request;
    }

    private ParticipantVersion stubVersion() throws Exception {
        ParticipantVersion pv = mock(ParticipantVersion.class);
        when(pv.getSharingScope()).thenReturn(SharingScope.SPONSORS_AND_PARTNERS);
        when(mockBridgeHelper.getParticipantVersion(APP_ID, USER_ID, VERSION)).thenReturn(pv);
        return pv;
    }

    @Test
    public void alreadyProcessedSkips() throws Exception {
        when(mockLedger.containsVersion(HEALTH_CODE, VERSION)).thenReturn(true);
        processor.process(request());
        verify(mockBridgeHelper, never()).getParticipantVersion(any(), any(), org.mockito.Matchers.anyInt());
        verify(mockLedger, never()).markVersion(any(), org.mockito.Matchers.anyInt());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void entityNotFoundBecomesBadRequest() throws Exception {
        when(mockLedger.containsVersion(HEALTH_CODE, VERSION)).thenReturn(false);
        when(mockBridgeHelper.getParticipantVersion(APP_ID, USER_ID, VERSION))
                .thenThrow(new EntityNotFoundException("no version", ""));
        processor.process(request());
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void nullVersionBecomesBadRequest() throws Exception {
        when(mockLedger.containsVersion(HEALTH_CODE, VERSION)).thenReturn(false);
        when(mockBridgeHelper.getParticipantVersion(APP_ID, USER_ID, VERSION)).thenReturn(null);
        processor.process(request());
    }

    @Test
    public void testUserSkipsAndMarksWithoutTombstone() throws Exception {
        when(mockLedger.containsVersion(HEALTH_CODE, VERSION)).thenReturn(false);
        stubVersion();
        when(mockGate.evaluateVersion(any(), any())).thenReturn(ConsentVerdict.skip(true));

        processor.process(request());

        verify(mockExportStore, never()).markTombstone(any());
        verify(mockExportStore, never()).stageVersionRow(any(), org.mockito.Matchers.anyInt(), any());
        verify(mockLedger).markVersion(HEALTH_CODE, VERSION);
    }

    @Test
    public void noSharingTombstonesAndMarks() throws Exception {
        when(mockLedger.containsVersion(HEALTH_CODE, VERSION)).thenReturn(false);
        stubVersion();
        when(mockGate.evaluateVersion(any(), any())).thenReturn(ConsentVerdict.skip(false));

        processor.process(request());

        verify(mockExportStore).markTombstone(HEALTH_CODE);
        verify(mockLedger).markVersion(HEALTH_CODE, VERSION);
        verify(mockExportStore, never()).stageVersionRow(any(), org.mockito.Matchers.anyInt(), any());
    }

    @Test
    public void exportStagesRowAndMarks() throws Exception {
        when(mockLedger.containsVersion(HEALTH_CODE, VERSION)).thenReturn(false);
        ParticipantVersion pv = stubVersion();
        when(mockGate.evaluateVersion(any(), any())).thenReturn(ConsentVerdict.export(false));

        TableRow row = new TableRow("participant_versions", HEALTH_CODE);
        when(mockRowBuilder.build(eq(pv), eq(false))).thenReturn(row);

        File tempDir = new File("temp-dir");
        File parquet = new File("temp-dir/participant_version.parquet");
        when(mockFileHelper.createTempDir()).thenReturn(tempDir);
        when(mockFileHelper.newFile(tempDir, "participant_version.parquet")).thenReturn(parquet);
        when(mockWriter.write(row, parquet)).thenReturn(parquet);

        processor.process(request());

        verify(mockWriter).write(row, parquet);
        verify(mockExportStore).stageVersionRow(HEALTH_CODE, VERSION, parquet);
        verify(mockLedger).markVersion(HEALTH_CODE, VERSION);
        verify(mockFileHelper).deleteDirRecursively(tempDir);
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void acceptRejectsMissingFields() throws Exception {
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree("{\"participantVersion\":2}");
        processor.accept(node);
    }

    @Test
    public void acceptRoutesToProcess() throws Exception {
        when(mockLedger.containsVersion(HEALTH_CODE, VERSION)).thenReturn(true);
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree(
                "{\"appId\":\"" + APP_ID + "\",\"healthCode\":\"" + HEALTH_CODE + "\",\"participantVersion\":2}");
        processor.accept(node);
        verify(mockLedger).containsVersion(HEALTH_CODE, VERSION);
    }
}
