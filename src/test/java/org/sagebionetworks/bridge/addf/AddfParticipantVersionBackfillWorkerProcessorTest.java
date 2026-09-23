package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.AmazonSQS;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
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

    private static JsonNode requestNode() throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(
                "{\"appId\":\"" + APP_ID + "\",\"s3Key\":\"" + S3_KEY + "\"}");
    }

    @Test
    public void enumeratesVersionsAndEnqueuesPerHealthCode() throws Exception {
        // Blank/whitespace entries are skipped; " hc2 " is trimmed.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY))
                .thenReturn(ImmutableList.of("hc1", " hc2 ", ""));

        // hc1 has versions 1 and 2; hc2 has version 1.
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:hc1", 1)).thenReturn(null);
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:hc1", 2)).thenReturn(null);
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:hc1", 3))
                .thenThrow(new EntityNotFoundException("no more", ""));
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:hc2", 1)).thenReturn(null);
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:hc2", 2))
                .thenThrow(new EntityNotFoundException("no more", ""));

        processor.accept(requestNode());

        verify(mockSqs, times(3)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }

    @Test
    public void probeErrorStopsThatParticipantWithoutEnqueue() throws Exception {
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY)).thenReturn(ImmutableList.of("hc1"));
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:hc1", 1))
                .thenThrow(new RuntimeException("boom"));

        processor.accept(requestNode());

        verify(mockSqs, never()).sendMessage(anyString(), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }
}
