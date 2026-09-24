package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.sqs.AmazonSQS;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

public class AddfParticipantVersionBackfillWorkerProcessorTest {
    private static final String APP_ID = "app";
    private static final String S3_KEY = "s3key";
    private static final String BACKFILL_BUCKET = "backfill-bucket";
    private static final String QUEUE_URL = "queue-url";

    private BridgeHelper mockBridgeHelper;
    private DynamoHelper mockDynamoHelper;
    private S3Helper mockS3Helper;
    private AmazonSQS mockSqs;
    private AddfParticipantVersionBackfillWorkerProcessor processor;

    @BeforeMethod
    public void before() {
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(AddfParticipantVersionBackfillWorkerProcessor.CONFIG_KEY_BACKFILL_BUCKET))
                .thenReturn(BACKFILL_BUCKET);
        when(mockConfig.get(AddfParticipantVersionBackfillWorkerProcessor.CONFIG_KEY_ADDF_QUEUE_URL))
                .thenReturn(QUEUE_URL);

        mockBridgeHelper = mock(BridgeHelper.class);
        mockDynamoHelper = mock(DynamoHelper.class);
        mockS3Helper = mock(S3Helper.class);
        mockSqs = mock(AmazonSQS.class);

        processor = new AddfParticipantVersionBackfillWorkerProcessor();
        processor.setConfig(mockConfig);
        processor.setBridgeHelper(mockBridgeHelper);
        processor.setDynamoHelper(mockDynamoHelper);
        processor.setS3Helper(mockS3Helper);
        processor.setAddfSqsClient(mockSqs);
    }

    private static JsonNode listRequest() throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(
                "{\"appId\":\"" + APP_ID + "\",\"s3Key\":\"" + S3_KEY + "\"}");
    }

    private static JsonNode allAccountsRequest() throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree("{\"appId\":\"" + APP_ID + "\"}");
    }

    // NOTE: never call these inside a thenReturn(...) expression. They stub, and stubbing
    // while an outer when(...) is still open makes Mockito abort with UnfinishedStubbing.
    // Build the fixtures into locals first, then stub.
    private static ParticipantVersion version(String healthCode, int versionNum) {
        ParticipantVersion pv = mock(ParticipantVersion.class);
        when(pv.getHealthCode()).thenReturn(healthCode);
        when(pv.getParticipantVersion()).thenReturn(versionNum);
        return pv;
    }

    private static AccountSummary account(String userId) {
        AccountSummary summary = mock(AccountSummary.class);
        when(summary.getId()).thenReturn(userId);
        return summary;
    }

    // ---- targeted (s3Key) mode ------------------------------------------

    @Test
    public void listMode_enqueuesEveryVersionPerHealthCode() throws Exception {
        List<ParticipantVersion> hc1Versions = ImmutableList.of(version("hc1", 1), version("hc1", 2));
        List<ParticipantVersion> hc2Versions = ImmutableList.of(version("hc2", 1));

        // Blank/whitespace entries are skipped; " hc2 " is trimmed.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY))
                .thenReturn(ImmutableList.of("hc1", " hc2 ", ""));
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "healthCode:hc1"))
                .thenReturn(hc1Versions);
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "healthCode:hc2"))
                .thenReturn(hc2Versions);

        processor.accept(listRequest());

        verify(mockSqs, times(3)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), contains("mode=healthCodeList"));
    }

    @Test
    public void listMode_fetchErrorSkipsThatParticipantWithoutEnqueue() throws Exception {
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY)).thenReturn(ImmutableList.of("hc1"));
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "healthCode:hc1"))
                .thenThrow(new RuntimeException("boom"));

        processor.accept(listRequest());

        verify(mockSqs, never()).sendMessage(anyString(), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }

    @Test
    public void listMode_participantWithNoVersionsIsSkipped() throws Exception {
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY)).thenReturn(ImmutableList.of("hc1"));
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "healthCode:hc1"))
                .thenThrow(new EntityNotFoundException("none", ""));

        processor.accept(listRequest());

        verify(mockSqs, never()).sendMessage(anyString(), anyString());
    }

    // ---- whole-app mode (the gated CI kickoff) ---------------------------

    @Test
    public void allAccountsMode_enumeratesAccountsAndNeverTouchesS3() throws Exception {
        Iterator<AccountSummary> accounts = ImmutableList.of(account("user1"), account("user2")).iterator();
        List<ParticipantVersion> user1Versions = ImmutableList.of(version("hc1", 1), version("hc1", 2));
        List<ParticipantVersion> user2Versions = ImmutableList.of(version("hc2", 1));

        when(mockBridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accounts);
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "user1")).thenReturn(user1Versions);
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "user2")).thenReturn(user2Versions);

        processor.accept(allAccountsRequest());

        verify(mockSqs, times(3)).sendMessage(eq(QUEUE_URL), anyString());
        // No health-code list is read in this mode -- that is the whole point.
        verifyZeroInteractions(mockS3Helper);
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), contains("mode=allAccounts"));
    }

    @Test
    public void allAccountsMode_blankS3KeySelectsWholeApp() throws Exception {
        JsonNode blankKey = DefaultObjectMapper.INSTANCE.readTree(
                "{\"appId\":\"" + APP_ID + "\",\"s3Key\":\"   \"}");
        Iterator<AccountSummary> accounts = ImmutableList.of(account("user1")).iterator();
        List<ParticipantVersion> versions = ImmutableList.of(version("hc1", 1));

        when(mockBridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accounts);
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "user1")).thenReturn(versions);

        processor.accept(blankKey);

        verify(mockSqs, times(1)).sendMessage(eq(QUEUE_URL), anyString());
        verifyZeroInteractions(mockS3Helper);
    }

    @Test
    public void allAccountsMode_enqueuesHealthCodeFromTheVersionNotTheAccount() throws Exception {
        Iterator<AccountSummary> accounts = ImmutableList.of(account("user1")).iterator();
        List<ParticipantVersion> versions = ImmutableList.of(version("hc-from-version", 7));

        when(mockBridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accounts);
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "user1")).thenReturn(versions);

        processor.accept(allAccountsRequest());

        // The message must carry the health code off the version, not the account's userId.
        verify(mockSqs).sendMessage(eq(QUEUE_URL), contains("\"healthCode\":\"hc-from-version\""));
        verify(mockSqs).sendMessage(eq(QUEUE_URL), contains("\"participantVersion\":7"));
    }

    @Test
    public void allAccountsMode_incompleteVersionIsSkipped() throws Exception {
        ParticipantVersion noHealthCode = mock(ParticipantVersion.class);
        when(noHealthCode.getHealthCode()).thenReturn(null);
        when(noHealthCode.getParticipantVersion()).thenReturn(1);
        Iterator<AccountSummary> accounts = ImmutableList.of(account("user1")).iterator();
        List<ParticipantVersion> versions = ImmutableList.of(noHealthCode, version("hc1", 2));

        when(mockBridgeHelper.getAllAccountSummaries(APP_ID, false)).thenReturn(accounts);
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "user1")).thenReturn(versions);

        processor.accept(allAccountsRequest());

        verify(mockSqs, times(1)).sendMessage(eq(QUEUE_URL), anyString());
    }
}
