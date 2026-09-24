package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

/**
 * The participant-version backfill is a <b>one-shot</b> job: it runs once per environment to give
 * {@code participant_versions} a baseline, and until it has, every activity row published is an orphan the manifest
 * gate defers. That makes partial completion the expensive failure — a run that dies a third of the way through
 * leaves two thirds of the cohort with no dimension rows and no obvious signal that anything is missing.
 *
 * <p>So the contract is: individual failures are absorbed and the sweep continues. These tests hold that line for the
 * failure that is actually likely at 10 enqueues/second across thousands of participants — SQS throttling — and for a
 * single participant whose version probe misbehaves.</p>
 */
public class AddfBackfillResilienceTest {
    private static final String APP_ID = "test-app";
    private static final String BACKFILL_BUCKET = "backfill-bucket";
    private static final String S3_KEY = "health-codes.txt";
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/1234/Bridge-ADDF-Export-Request-uat";

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

    private static JsonNode request() throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(
                "{\"appId\":\"" + APP_ID + "\",\"s3Key\":\"" + S3_KEY + "\"}");
    }

    /** hc has exactly {@code versions} versions; the probe for the next one reports not-found. */
    private void stubVersions(String healthCode, int versions) throws Exception {
        for (int v = 1; v <= versions; v++) {
            when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:" + healthCode, v)).thenReturn(null);
        }
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:" + healthCode, versions + 1))
                .thenThrow(new EntityNotFoundException("no more", ""));
    }

    @Test
    public void anSqsFailureForOneVersionDoesNotAbortTheSweep() throws Exception {
        // SQS throttling mid-backfill must cost one message, not the remaining cohort. The worker logs and carries
        // on, so a re-run only has to redo what genuinely failed.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY))
                .thenReturn(ImmutableList.of("hc1", "hc2"));
        stubVersions("hc1", 2);
        stubVersions("hc2", 1);

        when(mockSqs.sendMessage(eq(QUEUE_URL), anyString()))
                .thenThrow(new AmazonServiceException("Rate exceeded"))
                .thenReturn(new SendMessageResult());

        processor.accept(request());

        // All three enqueues were attempted even though the first threw, and the run still recorded its worker log.
        verify(mockSqs, times(3)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }

    @Test
    public void everySqsSendFailingStillCompletesAndLogs() throws Exception {
        // The pathological case — the queue is unreachable for the whole run. It must end, and end visibly, rather
        // than throw halfway and leave the operator guessing how far it got.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY)).thenReturn(ImmutableList.of("hc1"));
        stubVersions("hc1", 2);
        when(mockSqs.sendMessage(anyString(), anyString()))
                .thenThrow(new AmazonServiceException("queue gone"));

        processor.accept(request());

        verify(mockSqs, times(2)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }

    @Test
    public void oneParticipantsProbeFailureDoesNotStopTheNextParticipant() throws Exception {
        // A single bad health code (deleted account, transient Bridge error) must not strand everyone after it in
        // the list — which, for a sorted list, would be a systematic rather than a random gap.
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY))
                .thenReturn(ImmutableList.of("hc-broken", "hc-fine"));
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:hc-broken", 1))
                .thenThrow(new RuntimeException("bridge blew up"));
        stubVersions("hc-fine", 1);

        processor.accept(request());

        // Exactly one enqueue: none for the broken participant, one for the one after it.
        verify(mockSqs, times(1)).sendMessage(eq(QUEUE_URL), anyString());
    }

    @Test
    public void anEmptyHealthCodeListIsAValidNoOpRun() throws Exception {
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY)).thenReturn(ImmutableList.<String>of());

        processor.accept(request());

        verify(mockSqs, times(0)).sendMessage(anyString(), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }

    @Test
    public void aMalformedRequestIsRejectedBeforeAnyEnqueueing() throws Exception {
        try {
            processor.accept(DefaultObjectMapper.INSTANCE.readTree("\"not-an-object\""));
            fail("expected a bad request");
        } catch (PollSqsWorkerBadRequestException expected) {
            // A backfill is triggered by hand; a malformed trigger must dead-letter rather than sweep with nulls.
        }
        verify(mockSqs, times(0)).sendMessage(anyString(), anyString());
    }
}
