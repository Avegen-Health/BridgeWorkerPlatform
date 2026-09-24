package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.azure.BlobTransport;
import org.sagebionetworks.bridge.addf.publish.ManifestGate;
import org.sagebionetworks.bridge.addf.publish.PublishMarker;
import org.sagebionetworks.bridge.addf.publish.PublishedBlob;
import org.sagebionetworks.bridge.addf.publish.SnapshotDelta;
import org.sagebionetworks.bridge.addf.publish.SnapshotDeltaBuilder;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

/**
 * The {@code accept} envelope every ADDF worker shares: parse, gate, run, and map whatever comes back out to the one
 * of three outcomes the SQS poller understands. That mapping is not cosmetic — it decides whether a failure
 * <b>retries</b>, <b>dead-letters</b>, or <b>surfaces as an error</b>, and getting it wrong is how a transient blip
 * becomes a lost record or a poison message becomes an infinite loop.
 *
 * <p>The contract, identical in all four workers:</p>
 * <ul>
 *   <li>unparseable or incomplete envelope → {@link PollSqsWorkerBadRequestException} (DLQ; retrying cannot help);</li>
 *   <li>the S3 connection pool being shut down under us — a known transient during credential refresh →
 *       {@link PollSqsWorkerRetryableException} (retry, do <b>not</b> dead-letter);</li>
 *   <li>any other {@code IllegalStateException} or {@code RuntimeException} → {@link WorkerException}.</li>
 * </ul>
 */
public class AddfWorkerErrorMappingTest {
    /** The message the AWS SDK uses when a pooled client is reused after shutdown. */
    private static final String POOL_SHUTDOWN = "Connection pool shut down";

    private LedgerStore mockLedger;
    private Config mockConfig;

    private AddfExportWorkerProcessor exportWorker;
    private AddfParticipantVersionWorkerProcessor versionWorker;
    private AddfPublishWorkerProcessor publishWorker;

    private SnapshotDeltaBuilder mockBuilder;

    @BeforeMethod
    public void before() throws Exception {
        mockLedger = mock(LedgerStore.class);
        mockConfig = mock(Config.class);
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");

        exportWorker = new AddfExportWorkerProcessor();
        exportWorker.setLedgerStore(mockLedger);

        versionWorker = new AddfParticipantVersionWorkerProcessor();
        versionWorker.setLedgerStore(mockLedger);

        mockBuilder = mock(SnapshotDeltaBuilder.class);
        FileHelper mockFileHelper = mock(FileHelper.class);
        when(mockFileHelper.createTempDir()).thenReturn(new File("/tmp/addf-error-mapping-test"));
        PublishMarker mockMarker = mock(PublishMarker.class);
        when(mockMarker.isPublished(anyString())).thenReturn(false);

        publishWorker = new AddfPublishWorkerProcessor();
        publishWorker.setBridgeConfig(mockConfig);
        publishWorker.setSnapshotDeltaBuilder(mockBuilder);
        publishWorker.setManifestGate(mock(ManifestGate.class));
        publishWorker.setBlobTransport(mock(BlobTransport.class));
        publishWorker.setPublishMarker(mockMarker);
        publishWorker.setFileHelper(mockFileHelper);
        when(mockBuilder.build(anyString(), any(File.class))).thenReturn(
                new SnapshotDelta(ImmutableList.<PublishedBlob>of(), ImmutableList.<String>of(),
                        ImmutableList.<String>of()));
    }

    private static JsonNode body(String json) throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(json);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Envelope validation — these must dead-letter, never spin
    // -----------------------------------------------------------------------------------------------------------

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void exportWorkerRejectsAScalarBody() throws Exception {
        exportWorker.accept(body("\"not-an-object\""));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void exportWorkerRejectsAMissingRecordId() throws Exception {
        exportWorker.accept(body("{\"appId\":\"app\"}"));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void exportWorkerRejectsAMissingAppId() throws Exception {
        exportWorker.accept(body("{\"recordId\":\"rec-1\"}"));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void versionWorkerRejectsAScalarBody() throws Exception {
        versionWorker.accept(body("\"not-an-object\""));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void versionWorkerRejectsAMissingHealthCode() throws Exception {
        versionWorker.accept(body("{\"appId\":\"app\",\"participantVersion\":2}"));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void publishWorkerRejectsAScalarBody() throws Exception {
        publishWorker.accept(body("\"not-an-object\""));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Transient pool shutdown — retry, do not dead-letter
    // -----------------------------------------------------------------------------------------------------------

    @Test(expectedExceptions = PollSqsWorkerRetryableException.class)
    public void exportWorkerTreatsPoolShutdownAsRetryable() throws Exception {
        when(mockLedger.containsRecord(anyString())).thenThrow(new IllegalStateException(POOL_SHUTDOWN));
        exportWorker.accept(body("{\"appId\":\"app\",\"recordId\":\"rec-1\"}"));
    }

    @Test(expectedExceptions = PollSqsWorkerRetryableException.class)
    public void versionWorkerTreatsPoolShutdownAsRetryable() throws Exception {
        when(mockLedger.containsVersion(anyString(), anyInt()))
                .thenThrow(new IllegalStateException(POOL_SHUTDOWN));
        versionWorker.accept(body("{\"appId\":\"app\",\"healthCode\":\"hc-1\",\"participantVersion\":2}"));
    }

    @Test(expectedExceptions = PollSqsWorkerRetryableException.class)
    public void publishWorkerTreatsPoolShutdownAsRetryable() throws Exception {
        doThrow(new IllegalStateException(POOL_SHUTDOWN)).when(mockBuilder).build(anyString(), any(File.class));
        publishWorker.accept(body("{\"snapshotDate\":\"2026-09-24\"}"));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Everything else — a worker error
    // -----------------------------------------------------------------------------------------------------------

    @Test(expectedExceptions = WorkerException.class)
    public void exportWorkerMapsOtherIllegalStateToWorkerError() throws Exception {
        // Same exception type, different message: only the pool-shutdown text earns a retry, so an unrelated
        // IllegalStateException must not be silently retried forever.
        when(mockLedger.containsRecord(anyString())).thenThrow(new IllegalStateException("something else entirely"));
        exportWorker.accept(body("{\"appId\":\"app\",\"recordId\":\"rec-1\"}"));
    }

    @Test(expectedExceptions = WorkerException.class)
    public void exportWorkerMapsNullMessageIllegalStateToWorkerError() throws Exception {
        when(mockLedger.containsRecord(anyString())).thenThrow(new IllegalStateException());
        exportWorker.accept(body("{\"appId\":\"app\",\"recordId\":\"rec-1\"}"));
    }

    @Test(expectedExceptions = WorkerException.class)
    public void exportWorkerMapsRuntimeExceptionToWorkerError() throws Exception {
        when(mockLedger.containsRecord(anyString())).thenThrow(new RuntimeException("boom"));
        exportWorker.accept(body("{\"appId\":\"app\",\"recordId\":\"rec-1\"}"));
    }

    @Test(expectedExceptions = WorkerException.class)
    public void versionWorkerMapsRuntimeExceptionToWorkerError() throws Exception {
        when(mockLedger.containsVersion(anyString(), anyInt())).thenThrow(new RuntimeException("boom"));
        versionWorker.accept(body("{\"appId\":\"app\",\"healthCode\":\"hc-1\",\"participantVersion\":2}"));
    }

    @Test(expectedExceptions = WorkerException.class)
    public void publishWorkerMapsRuntimeExceptionToWorkerError() throws Exception {
        doThrow(new RuntimeException("boom")).when(mockBuilder).build(anyString(), any(File.class));
        publishWorker.accept(body("{\"snapshotDate\":\"2026-09-24\"}"));
    }

    @Test(expectedExceptions = WorkerException.class)
    public void publishWorkerMapsOtherIllegalStateToWorkerError() throws Exception {
        doThrow(new IllegalStateException("disk full")).when(mockBuilder).build(anyString(), any(File.class));
        publishWorker.accept(body("{\"snapshotDate\":\"2026-09-24\"}"));
    }

    // -----------------------------------------------------------------------------------------------------------
    // A checked exception from the body must pass straight through, not get re-wrapped
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void alreadyMappedExceptionsArePassedThroughUnwrapped() throws Exception {
        // The multi-catch rethrows the worker's own checked types as-is. Re-wrapping one in a WorkerException would
        // reclassify the failure — an IOException is the poller's retry signal, so burying it would turn a transient
        // S3 blip into a dead-lettered snapshot.
        doThrow(new java.io.IOException("transient s3 read")).when(mockBuilder)
                .build(anyString(), any(File.class));
        try {
            publishWorker.accept(body("{\"snapshotDate\":\"2026-09-24\"}"));
            fail("expected the IOException to propagate unwrapped");
        } catch (java.io.IOException expected) {
            assertTrue(expected.getMessage().contains("transient s3 read"), expected.getMessage());
        }
    }

    @Test
    public void publishKillSwitchShortCircuitsBeforeAnyWork() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("false");
        // Would throw if reached.
        doThrow(new RuntimeException("must not run")).when(mockBuilder).build(anyString(), any(File.class));

        publishWorker.accept(body("{\"snapshotDate\":\"2026-09-24\"}"));
    }

    @Test
    public void publishDefaultsAnAbsentSnapshotDateToTodayRatherThanFailing() throws Exception {
        // The EventBridge schedule sends an empty body on purpose (§5): injecting the scheduler's own ISO timestamp
        // would corrupt the S3 key. An absent or blank label must default, not fault.
        publishWorker.accept(body("{}"));
        publishWorker.accept(body("{\"snapshotDate\":\"\"}"));
    }
}
