package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

/**
 * Failure behaviour of the participant-version backfill. It is a <b>one-shot</b> job: it runs once per environment to
 * give {@code participant_versions} a baseline, and until it has, pre-existing participants' activity rows are all
 * orphan-deferred at publish. That makes <b>partial completion</b> the expensive failure — a run that dies a third of
 * the way through leaves two thirds of the cohort with no dimension rows and, unless it says so, no signal that
 * anything is missing.
 *
 * <p>The worker draws a deliberate line between two kinds of failure, and these tests hold it:</p>
 * <ul>
 *   <li><b>Per-participant failures are absorbed</b> — one unreachable account or one throttled SQS send must cost
 *       that one item, not the rest of the sweep. The run continues and completes.</li>
 *   <li><b>A failure of the walk itself is rethrown</b> — if pagination dies mid-enumeration the worker records how
 *       far it got and then <em>rethrows</em>, so the SQS message is redelivered. Returning normally there would let
 *       the callback delete the message and leave an operator believing a silently incomplete backfill had
 *       succeeded, which is precisely the condition this worker exists to remove.</li>
 * </ul>
 *
 * <p>Complements {@code AddfParticipantVersionBackfillWorkerProcessorTest}, which covers the happy paths of both
 * modes.</p>
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

    // -----------------------------------------------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------------------------------------------

    /** Targeted mode: a request naming a health-code list in the backfill bucket. */
    private static JsonNode listRequest() throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(
                "{\"appId\":\"" + APP_ID + "\",\"s3Key\":\"" + S3_KEY + "\"}");
    }

    /** Whole-app mode: no s3Key at all, so no health-code file is ever produced or handled. */
    private static JsonNode wholeAppRequest() throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree("{\"appId\":\"" + APP_ID + "\"}");
    }

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

    private void stubList(String... healthCodes) throws IOException {
        when(mockS3Helper.readS3FileAsLines(BACKFILL_BUCKET, S3_KEY)).thenReturn(ImmutableList.copyOf(healthCodes));
    }

    private void stubVersions(String userIdToken, List<ParticipantVersion> versions) throws IOException {
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, userIdToken)).thenReturn(versions);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Per-item failures are absorbed; the sweep completes
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void anSqsFailureForOneVersionDoesNotAbortTheSweep() throws Exception {
        // SQS throttling mid-backfill must cost one message, not the remaining cohort. The worker logs and carries
        // on, so a re-run only has to redo what genuinely failed — and re-running is cheap, because the version
        // worker presence-skips versions already written.
        stubList("hc1", "hc2");
        stubVersions("healthCode:hc1", ImmutableList.of(version("hc1", 1), version("hc1", 2)));
        stubVersions("healthCode:hc2", ImmutableList.of(version("hc2", 1)));

        when(mockSqs.sendMessage(eq(QUEUE_URL), anyString()))
                .thenThrow(new AmazonServiceException("Rate exceeded"))
                .thenReturn(new SendMessageResult());

        processor.accept(listRequest());

        // All three enqueues attempted even though the first threw, and the run still recorded its worker log.
        verify(mockSqs, times(3)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), contains("mode=healthCodeList"));
    }

    @Test
    public void everySqsSendFailingStillCompletesAndLogs() throws Exception {
        // The pathological case — the queue is unreachable for the whole run. It must end, and end visibly, rather
        // than throw halfway and leave the operator guessing how far it got.
        stubList("hc1");
        stubVersions("healthCode:hc1", ImmutableList.of(version("hc1", 1), version("hc1", 2)));
        when(mockSqs.sendMessage(anyString(), anyString()))
                .thenThrow(new AmazonServiceException("queue gone"));

        processor.accept(listRequest());

        verify(mockSqs, times(2)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }

    @Test
    public void oneParticipantsFetchFailureDoesNotStopTheNextParticipant() throws Exception {
        // A single bad participant (deleted account, transient Bridge error) must not strand everyone after it in
        // the list — which, for a sorted list, would be a systematic rather than a random gap.
        stubList("hc-broken", "hc-fine");
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "healthCode:hc-broken"))
                .thenThrow(new RuntimeException("bridge blew up"));
        stubVersions("healthCode:hc-fine", ImmutableList.of(version("hc-fine", 1)));

        processor.accept(listRequest());

        // Exactly one enqueue: none for the broken participant, one for the one after it.
        verify(mockSqs, times(1)).sendMessage(eq(QUEUE_URL), anyString());
    }

    @Test
    public void aParticipantWithNoVersionsIsSkippedQuietly() throws Exception {
        // EntityNotFound just means this account never had a participant version — an ordinary outcome for an
        // account that never consented, not an error worth interrupting the sweep for.
        stubList("hc-none", "hc-fine");
        when(mockBridgeHelper.getAllParticipantVersionsForUser(APP_ID, "healthCode:hc-none"))
                .thenThrow(new EntityNotFoundException("no versions", ""));
        stubVersions("healthCode:hc-fine", ImmutableList.of(version("hc-fine", 1)));

        processor.accept(listRequest());

        verify(mockSqs, times(1)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }

    @Test
    public void nullAndIncompleteVersionsAreSkippedRatherThanEnqueuedHalfFormed() throws Exception {
        // A version missing its health code or version number cannot address a row, so enqueueing it would create a
        // message the version worker can only reject. Skip it and keep the good ones.
        ParticipantVersion noHealthCode = mock(ParticipantVersion.class);
        when(noHealthCode.getParticipantVersion()).thenReturn(1);
        ParticipantVersion noVersionNum = mock(ParticipantVersion.class);
        when(noVersionNum.getHealthCode()).thenReturn("hc1");
        // Stub the null explicitly. Mockito 1.x returns the PRIMITIVE default for boxed types, so an unstubbed
        // Integer getter yields 0 rather than null — which would silently make this mock "complete" and let the test
        // pass for the wrong reason while asserting nothing about the null path.
        when(noVersionNum.getParticipantVersion()).thenReturn(null);
        ParticipantVersion complete = version("hc1", 3);

        stubList("hc1");
        stubVersions("healthCode:hc1", java.util.Arrays.asList(noHealthCode, noVersionNum, complete));

        processor.accept(listRequest());

        // Assert on the message bodies, not just the count: a half-formed version that slipped through would
        // otherwise be indistinguishable from the good one in a bare invocation count.
        org.mockito.ArgumentCaptor<String> bodies = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(mockSqs, org.mockito.Mockito.atLeastOnce()).sendMessage(eq(QUEUE_URL), bodies.capture());
        for (String body : bodies.getAllValues()) {
            org.testng.Assert.assertTrue(body.contains("\"participantVersion\":3"),
                    "only the complete version may be enqueued, but got: " + body);
        }
        org.testng.Assert.assertEquals(bodies.getAllValues().size(), 1,
                "enqueued more than the one complete version: " + bodies.getAllValues());
    }

    @Test
    public void anEmptyHealthCodeListIsAValidNoOpRun() throws Exception {
        stubList();

        processor.accept(listRequest());

        verify(mockSqs, never()).sendMessage(anyString(), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), anyString());
    }

    // -----------------------------------------------------------------------------------------------------------
    // Whole-app mode: a failure of the walk itself must NOT look like success
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void wholeAppModeNeedsNoHealthCodeFileAtAll() throws Exception {
        // The mode the gated CI kickoff uses. No s3Key means no file of health codes is ever produced, uploaded or
        // handled by an operator — so the backfill cannot be run against a stale or partial list by mistake.
        // Build the summaries BEFORE the outer when(...): account() stubs internally, and a nested stubbing inside
        // an in-progress when() is what Mockito reports as UnfinishedStubbing.
        AccountSummary user1 = account("user-1");
        AccountSummary user2 = account("user-2");
        when(mockBridgeHelper.getAllAccountSummaries(APP_ID, false))
                .thenReturn(ImmutableList.of(user1, user2).iterator());
        stubVersions("user-1", ImmutableList.of(version("hc1", 1), version("hc1", 2)));
        stubVersions("user-2", ImmutableList.of(version("hc2", 1)));

        processor.accept(wholeAppRequest());

        verify(mockSqs, times(3)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockS3Helper, never()).readS3FileAsLines(anyString(), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), contains("mode=allAccounts"));
    }

    @Test
    public void aBlankS3KeySelectsWholeAppModeRatherThanReadingAnEmptyFile() throws Exception {
        // Guard on the trigger's shape: a whitespace s3Key is an absent one, not a request to read a file named " ".
        AccountSummary user1 = account("user-1");
        when(mockBridgeHelper.getAllAccountSummaries(APP_ID, false))
                .thenReturn(ImmutableList.of(user1).iterator());
        stubVersions("user-1", ImmutableList.of(version("hc1", 1)));

        processor.accept(DefaultObjectMapper.INSTANCE.readTree(
                "{\"appId\":\"" + APP_ID + "\",\"s3Key\":\"   \"}"));

        verify(mockS3Helper, never()).readS3FileAsLines(anyString(), anyString());
        verify(mockSqs, times(1)).sendMessage(eq(QUEUE_URL), anyString());
    }

    @Test
    public void paginationFailureMidWalkRecordsPartialProgressAndRethrows() throws Exception {
        // AccountSummaryIterator loads pages lazily and wraps a page-load IOException as an unchecked exception, so a
        // transient pagination failure surfaces mid-walk. Two things must happen, and both matter:
        //   1. the partial progress is written to the worker log, so an operator can see how far it got;
        //   2. the exception is RETHROWN, so SQS redelivers instead of deleting the message.
        // Swallowing it would leave a silently incomplete backfill reported as a success — pre-existing participants
        // would stay orphan-deferred forever, which is the exact failure this worker exists to fix.
        AccountSummary user1 = account("user-1");
        when(mockBridgeHelper.getAllAccountSummaries(APP_ID, false))
                .thenReturn(failingIterator(user1, new RuntimeException("page load failed")));
        stubVersions("user-1", ImmutableList.of(version("hc1", 1)));

        try {
            processor.accept(wholeAppRequest());
            fail("a mid-walk pagination failure must not be reported as a completed backfill");
        } catch (RuntimeException expected) {
            // redelivered, and cheap to restart: the version worker presence-skips what already landed.
        }

        verify(mockSqs, times(1)).sendMessage(eq(QUEUE_URL), anyString());
        verify(mockDynamoHelper).writeWorkerLog(
                eq(AddfParticipantVersionBackfillWorkerProcessor.WORKER_ID), contains("status=FAILED"));
    }

    @Test
    public void accountSummariesWithoutAnIdAreSkipped() throws Exception {
        AccountSummary noId = mock(AccountSummary.class);
        AccountSummary user2 = account("user-2");
        when(mockBridgeHelper.getAllAccountSummaries(APP_ID, false))
                .thenReturn(java.util.Arrays.asList(noId, user2).iterator());
        stubVersions("user-2", ImmutableList.of(version("hc2", 1)));

        processor.accept(wholeAppRequest());

        verify(mockSqs, times(1)).sendMessage(eq(QUEUE_URL), anyString());
    }

    // -----------------------------------------------------------------------------------------------------------
    // Trigger validation
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void aMalformedRequestIsRejectedBeforeAnyEnqueueing() throws Exception {
        try {
            processor.accept(DefaultObjectMapper.INSTANCE.readTree("\"not-an-object\""));
            fail("expected a bad request");
        } catch (PollSqsWorkerBadRequestException expected) {
            // A backfill is triggered by hand; a malformed trigger must dead-letter rather than sweep with nulls.
        }
        verify(mockSqs, never()).sendMessage(anyString(), anyString());
    }

    /** An iterator that yields {@code items}, then throws — standing in for a page load that fails mid-walk. */
    private static Iterator<AccountSummary> failingIterator(final AccountSummary first, final RuntimeException failure) {
        return new Iterator<AccountSummary>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index <= 1;
            }

            @Override
            public AccountSummary next() {
                if (index++ == 0) {
                    return first;
                }
                throw failure;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
