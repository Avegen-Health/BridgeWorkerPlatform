package org.sagebionetworks.bridge.addf;

import static org.mockito.Matchers.any;
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
import org.sagebionetworks.bridge.addf.publish.PublishMarker;
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
    private BlobTransport mockTransport;
    private PublishMarker mockMarker;
    private FileHelper mockFileHelper;
    private File tempDir;
    private AddfPublishWorkerProcessor processor;

    @BeforeMethod
    public void before() throws Exception {
        mockConfig = mock(Config.class);
        mockBuilder = mock(SnapshotDeltaBuilder.class);
        mockTransport = mock(BlobTransport.class);
        mockMarker = mock(PublishMarker.class);
        mockFileHelper = mock(FileHelper.class);
        tempDir = new File("/tmp/addf-publish-worker-test");

        when(mockFileHelper.createTempDir()).thenReturn(tempDir);
        when(mockBuilder.build(anyString(), any(File.class))).thenReturn(
                new SnapshotDelta(ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));
        // Default: not yet published, so publish() proceeds.
        when(mockMarker.isPublished(anyString())).thenReturn(false);

        processor = new AddfPublishWorkerProcessor();
        processor.setBridgeConfig(mockConfig);
        processor.setSnapshotDeltaBuilder(mockBuilder);
        processor.setBlobTransport(mockTransport);
        processor.setPublishMarker(mockMarker);
        processor.setFileHelper(mockFileHelper);
    }

    private JsonNode body(String json) throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(json);
    }

    @Test
    public void enabledRunsBuildUploadCommitMarkInOrder() throws Exception {
        when(mockConfig.get("addf.publish.enabled")).thenReturn("true");

        processor.accept(body("{\"snapshotDate\":\"" + SNAPSHOT_DATE + "\"}"));

        // Crash-safety ordering: build -> upload -> commit (retire staging/tombstones) -> mark. commit MUST follow the
        // upload so an upload failure leaves staging intact for an idempotent replay.
        InOrder inOrder = Mockito.inOrder(mockBuilder, mockTransport, mockMarker);
        inOrder.verify(mockBuilder).build(eq(SNAPSHOT_DATE), eq(tempDir));
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
}
