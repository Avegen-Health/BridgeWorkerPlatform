package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import com.fasterxml.jackson.databind.JsonNode;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.azure.BlobTransport;
import org.sagebionetworks.bridge.addf.publish.ManifestGate;
import org.sagebionetworks.bridge.addf.publish.ManifestGateException;
import org.sagebionetworks.bridge.addf.publish.PublishLease;
import org.sagebionetworks.bridge.addf.publish.PublishMarker;
import org.sagebionetworks.bridge.addf.publish.RawArchiveDelivery;
import org.sagebionetworks.bridge.addf.publish.RawCandidate;
import org.sagebionetworks.bridge.addf.publish.SnapshotDelta;
import org.sagebionetworks.bridge.addf.publish.SnapshotDeltaBuilder;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;

import com.google.common.collect.ImmutableList;

public class AddfPublishWorkerProcessorTest {
    private static final String SNAPSHOT_DATE = "2026-09-22";

    private Config mockConfig;
    private SnapshotDeltaBuilder mockBuilder;
    private ManifestGate mockGate;
    private BlobTransport mockTransport;
    private PublishMarker mockMarker;
    private PublishLease mockLease;
    private RawArchiveDelivery mockRawDelivery;
    private FileHelper mockFileHelper;
    private File tempDir;
    private AddfPublishWorkerProcessor processor;

    @BeforeMethod
    public void before() throws Exception {
        mockConfig = mock(Config.class);
        mockBuilder = mock(SnapshotDeltaBuilder.class);
        mockGate = mock(ManifestGate.class);
        mockTransport = mock(BlobTransport.class);
        mockMarker = mock(PublishMarker.class);
        mockLease = mock(PublishLease.class);
        mockRawDelivery = mock(RawArchiveDelivery.class);
        mockFileHelper = mock(FileHelper.class);
        tempDir = new File("/tmp/addf-publish-worker-test");

        when(mockFileHelper.createTempDir()).thenReturn(tempDir);
        when(mockBuilder.build(anyString(), any(File.class))).thenReturn(
                new SnapshotDelta(ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));
        // Default: not yet published and no competing run, so publish() proceeds.
        when(mockMarker.isPublished(anyString())).thenReturn(false);
        when(mockLease.acquire(anyString())).thenReturn(true);
        when(mockRawDelivery.deliver(anyListOf(RawCandidate.class), any(File.class)))
                .thenReturn(new RawArchiveDelivery.Result(0, 0, 0, 0));

        processor = new AddfPublishWorkerProcessor();
        processor.setBridgeConfig(mockConfig);
        processor.setSnapshotDeltaBuilder(mockBuilder);
        processor.setManifestGate(mockGate);
        processor.setBlobTransport(mockTransport);
        processor.setPublishMarker(mockMarker);
        processor.setPublishLease(mockLease);
        processor.setRawArchiveDelivery(mockRawDelivery);
        processor.setFileHelper(mockFileHelper);
    }

    private JsonNode body(String json) throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(json);
    }

    @Test
    public void enabledRunsBuildUploadCommitMarkInOrder() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");

        processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));

        // Crash-safety ordering: build -> gate -> upload -> commit (retire staging/tombstones) -> mark. The gate sits
        // before the upload so a bad snapshot never reaches ADDI; commit MUST follow the upload so an upload failure
        // leaves staging intact for an idempotent replay.
        InOrder inOrder = Mockito.inOrder(mockBuilder, mockGate, mockTransport, mockMarker);
        inOrder.verify(mockBuilder).build(eq(SNAPSHOT_DATE), eq(tempDir));
        inOrder.verify(mockGate).assertDeliverable(eq(SNAPSHOT_DATE), any(SnapshotDelta.class), eq(tempDir));
        inOrder.verify(mockTransport).upload(any());
        inOrder.verify(mockBuilder).commit(any(SnapshotDelta.class));
        inOrder.verify(mockMarker).mark(SNAPSHOT_DATE);
        // Temp dir always cleaned.
        verify(mockFileHelper).deleteDirRecursively(tempDir);
    }

    @Test
    public void uploadFailureLeavesStagingUncommittedAndUnmarked() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");
        Mockito.doThrow(new RuntimeException("azure blob 503")).when(mockTransport).upload(any());

        try {
            processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));
        } catch (Exception expected) {
            // mapped to WorkerException by accept(); the message is redelivered and the day replays.
        }

        // The failure must NOT consume staging (commit) or claim success (mark) — that is the crash-safety guarantee.
        verify(mockBuilder, never()).commit(any(SnapshotDelta.class));
        verify(mockMarker, never()).mark(anyString());
        // Temp dir still cleaned even on the failure path.
        verify(mockFileHelper).deleteDirRecursively(tempDir);
    }

    @Test
    public void manifestGateFailureBlocksUploadCommitAndMark() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");
        Mockito.doThrow(new ManifestGateException("2 of 10 table(s) absent")).when(mockGate)
                .assertDeliverable(anyString(), any(SnapshotDelta.class), any(File.class));

        try {
            processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));
            org.testng.Assert.fail("expected the manifest gate to block the snapshot");
        } catch (ManifestGateException expected) {
            // ManifestGateException extends WorkerException, so accept()'s multi-catch rethrows it unwrapped: the SQS
            // message fails, the DLQ + heartbeat alarms fire, and the day replays once the defect is fixed.
        }

        // Nothing may reach ADDI, staging must survive, and the day must not be claimed as done.
        verify(mockTransport, never()).upload(any());
        verify(mockBuilder, never()).commit(any(SnapshotDelta.class));
        verify(mockMarker, never()).mark(anyString());
        verify(mockFileHelper).deleteDirRecursively(tempDir);
    }

    @Test
    public void alreadyPublishedSkips() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");
        when(mockMarker.isPublished(SNAPSHOT_DATE)).thenReturn(true);

        processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));

        // Marker present => the snapshot already fully published; do no work and don't re-mark.
        verify(mockBuilder, never()).build(anyString(), any(File.class));
        verify(mockTransport, never()).upload(any());
        verify(mockBuilder, never()).commit(any(SnapshotDelta.class));
        verify(mockMarker, never()).mark(anyString());
    }

    @Test
    public void killSwitchOffSkipsEverything() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("false");

        processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));

        verify(mockBuilder, never()).build(anyString(), any(File.class));
        verify(mockTransport, never()).upload(any());
        verify(mockMarker, never()).mark(anyString());
        verify(mockFileHelper, never()).createTempDir();
    }

    @Test
    public void absentSnapshotDateDefaultsToToday() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");

        processor.accept(body("{}"));

        // Defaulted to a non-null yyyy-MM-dd label; the marker is written with it.
        verify(mockMarker).mark(anyString());
        verify(mockBuilder).build(anyString(), eq(tempDir));
    }

    @Test(expectedExceptions = PollSqsWorkerBadRequestException.class)
    public void malformedBodyIsBadRequest() throws Exception {
        // A scalar string cannot bind to the AddfPublishRequest bean -> parse error -> bad request (-> DLQ).
        processor.accept(body("\"not-an-object\""));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Overlap guard + raw phase ordering.
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void rawDeliveryRunsAfterUploadButBeforeCommit() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");

        processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));

        // Raw candidates are derived from the staged rows this run consumed, and commit() deletes that staging.
        // Delivering after commit would mean a failed archive could never be reconstructed.
        InOrder inOrder = Mockito.inOrder(mockTransport, mockRawDelivery, mockBuilder, mockMarker);
        inOrder.verify(mockTransport).upload(anyListOf(org.sagebionetworks.bridge.addf.publish.PublishedBlob.class));
        inOrder.verify(mockRawDelivery).deliver(anyListOf(RawCandidate.class), any(File.class));
        inOrder.verify(mockBuilder).commit(any(SnapshotDelta.class));
        inOrder.verify(mockMarker).mark(SNAPSHOT_DATE);
    }

    @Test
    public void heldLeaseSkipsTheRunEntirely() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");
        when(mockLease.acquire(anyString())).thenReturn(false);

        processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));

        // Another run is mid-flight: this one must not touch a single consolidated file.
        verify(mockBuilder, never()).build(anyString(), any(File.class));
        verify(mockTransport, never()).upload(anyListOf(org.sagebionetworks.bridge.addf.publish.PublishedBlob.class));
        verify(mockBuilder, never()).commit(any(SnapshotDelta.class));
        verify(mockMarker, never()).mark(anyString());
        // Nothing was acquired, so nothing may be released — releasing here would free the *other* run's lease.
        verify(mockLease, never()).release(anyString());
    }

    @Test
    public void leaseIsReleasedEvenWhenTheRunFails() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");
        Mockito.doThrow(new RuntimeException("azure down")).when(mockTransport)
                .upload(anyListOf(org.sagebionetworks.bridge.addf.publish.PublishedBlob.class));

        try {
            processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));
        } catch (Exception expected) {
            // the worker maps this to a WorkerException; we only care about the lease
        }

        // A leaked lease would block every subsequent run until the TTL expired.
        verify(mockLease).release(SNAPSHOT_DATE);
    }

    @Test
    public void disabledPublishNeverTakesTheLease() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("false");

        processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));

        verify(mockLease, never()).acquire(anyString());
        verify(mockRawDelivery, never()).deliver(anyListOf(RawCandidate.class), any(File.class));
    }
}
