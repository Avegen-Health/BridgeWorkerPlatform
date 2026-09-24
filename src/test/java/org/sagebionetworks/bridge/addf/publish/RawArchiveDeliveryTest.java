package org.sagebionetworks.bridge.addf.publish;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.azure.BlobTransport;
import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * Exercises {@link RawArchiveDelivery}'s streaming, failure-isolation and retry behaviour with all S3 / Azure IO
 * mocked. The properties under test are the ones the batched-into-the-delta design got wrong: one archive on disk at
 * a time, a failed archive never aborting the run, and the ledger mark landing per archive rather than per batch.
 */
public class RawArchiveDeliveryTest {
    private static final String HC = "hc-1";
    private static final String KEY = "raw/2026-09-22/rec-1-PHQ-9.zip";
    private static final String FULL_KEY = "biaffect-3/" + KEY;

    private Config mockConfig;
    private ExportStoreClient mockStore;
    private LedgerStore mockLedger;
    private BlobTransport mockTransport;
    private FileHelper mockFileHelper;
    private RawArchiveDelivery delivery;
    private File tempDir;

    @BeforeMethod
    public void before() {
        mockConfig = mock(Config.class);
        mockStore = mock(ExportStoreClient.class);
        mockLedger = mock(LedgerStore.class);
        mockTransport = mock(BlobTransport.class);
        mockFileHelper = mock(FileHelper.class);
        tempDir = new File("/tmp/addf-raw-delivery-test");

        when(mockConfig.get(RawArchiveDelivery.CONFIG_KEY_RAW_ENABLED)).thenReturn("true");
        when(mockConfig.get(RawArchiveDelivery.CONFIG_KEY_RAW_MAX_PER_RUN)).thenReturn("500");
        when(mockStore.listRawPending()).thenReturn(ImmutableList.<String>of());
        when(mockStore.rawKey(anyString())).thenAnswer(inv -> "biaffect-3/" + inv.getArgumentAt(0, String.class));
        when(mockStore.objectExists(anyString())).thenReturn(true);
        when(mockLedger.containsRaw(anyString(), anyString())).thenReturn(false);
        when(mockFileHelper.newFile(any(File.class), anyString()))
                .thenAnswer(inv -> new File(tempDir, inv.getArgumentAt(1, String.class)));
        when(mockStore.download(anyString(), any(File.class))).thenAnswer(inv -> inv.getArgumentAt(1, File.class));

        delivery = new RawArchiveDelivery();
        delivery.setBridgeConfig(mockConfig);
        delivery.setExportStoreClient(mockStore);
        delivery.setLedgerStore(mockLedger);
        delivery.setBlobTransport(mockTransport);
        delivery.setFileHelper(mockFileHelper);
    }

    private static List<RawCandidate> one() {
        return ImmutableList.of(new RawCandidate(HC, KEY));
    }

    @Test
    public void deliversUploadsThenMarksThenClearsPending() {
        RawArchiveDelivery.Result result = delivery.deliver(one(), tempDir);

        assertEquals(result.getDelivered(), 1);
        InOrder inOrder = Mockito.inOrder(mockTransport, mockLedger, mockStore);
        inOrder.verify(mockTransport).upload(anyList());
        // The mark must follow this archive's own confirmed upload — not a later batch step.
        inOrder.verify(mockLedger).markRawDelivered(HC, KEY);
        inOrder.verify(mockStore).clearRawPending(HC, KEY);
    }

    @Test
    public void uploadsOneArchiveAtATimeAndDeletesEachLocalFile() {
        List<RawCandidate> many = ImmutableList.of(new RawCandidate(HC, "raw/2026-09-01/a-X.zip"),
                new RawCandidate(HC, "raw/2026-09-02/b-X.zip"), new RawCandidate(HC, "raw/2026-09-03/c-X.zip"));

        RawArchiveDelivery.Result result = delivery.deliver(many, tempDir);

        assertEquals(result.getDelivered(), 3);
        // Peak disk is one archive: three separate single-blob uploads, each followed by its own delete — NOT one
        // batched upload of three pre-downloaded files.
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(mockTransport, times(3)).upload(captor.capture());
        for (Object batch : captor.getAllValues()) {
            assertEquals(((List<?>) batch).size(), 1, "each raw archive must upload on its own");
        }
        verify(mockFileHelper, times(3)).deleteFile(any(File.class));
    }

    @Test
    public void disabledFlagSkipsEverything() {
        when(mockConfig.get(RawArchiveDelivery.CONFIG_KEY_RAW_ENABLED)).thenReturn("false");

        RawArchiveDelivery.Result result = delivery.deliver(one(), tempDir);

        assertEquals(result.getSkipped(), 1);
        assertEquals(result.getDelivered(), 0);
        verify(mockTransport, never()).upload(anyList());
        // Nothing is parked either — a disabled layer must not silently build a backlog to flush on re-enable.
        verify(mockStore, never()).markRawPending(anyString(), anyString());
    }

    @Test
    public void alreadyDeliveredArchiveIsSkipped() {
        when(mockLedger.containsRaw(HC, KEY)).thenReturn(true);

        RawArchiveDelivery.Result result = delivery.deliver(one(), tempDir);

        assertEquals(result.getSkipped(), 1);
        verify(mockStore, never()).download(eq(FULL_KEY), any(File.class));
        verify(mockTransport, never()).upload(anyList());
        // A replayed snapshot must also drop the stale retry marker, or it is retried forever.
        verify(mockStore).clearRawPending(HC, KEY);
    }

    @Test
    public void failedUploadIsParkedNotThrown() {
        Mockito.doThrow(new RuntimeException("azure down")).when(mockTransport).upload(anyList());

        RawArchiveDelivery.Result result = delivery.deliver(one(), tempDir);

        // The run survives: the caller can still commit the snapshot's table work.
        assertEquals(result.getParked(), 1);
        assertEquals(result.getDelivered(), 0);
        verify(mockStore).markRawPending(HC, KEY);
        verify(mockLedger, never()).markRawDelivered(anyString(), anyString());
    }

    @Test
    public void parkedArchiveIsRetriedOnTheNextRun() {
        when(mockStore.listRawPending()).thenReturn(ImmutableList.of(HC + "\t" + KEY));

        RawArchiveDelivery.Result result = delivery.deliver(ImmutableList.<RawCandidate>of(), tempDir);

        assertEquals(result.getDelivered(), 1);
        verify(mockLedger).markRawDelivered(HC, KEY);
        verify(mockStore).clearRawPending(HC, KEY);
    }

    @Test
    public void parkedArchiveIsNotDeliveredTwiceWhenAlsoAFreshCandidate() {
        when(mockStore.listRawPending()).thenReturn(ImmutableList.of(HC + "\t" + KEY));

        RawArchiveDelivery.Result result = delivery.deliver(one(), tempDir);

        assertEquals(result.getDelivered(), 1);
        verify(mockTransport, times(1)).upload(anyList());
    }

    @Test
    public void missingArchiveIsSkippedAndNotParked() {
        when(mockStore.objectExists(FULL_KEY)).thenReturn(false);

        RawArchiveDelivery.Result result = delivery.deliver(one(), tempDir);

        assertEquals(result.getSkipped(), 1);
        verify(mockTransport, never()).upload(anyList());
        // Critically NOT parked: a permanently absent object would otherwise consume a slot on every future run.
        verify(mockStore, never()).markRawPending(anyString(), anyString());
        verify(mockStore).clearRawPending(HC, KEY);
    }

    @Test
    public void capBoundsTheRunAndParksTheRemainder() {
        when(mockConfig.get(RawArchiveDelivery.CONFIG_KEY_RAW_MAX_PER_RUN)).thenReturn("2");
        List<RawCandidate> five = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            five.add(new RawCandidate(HC, "raw/2026-09-0" + (i + 1) + "/rec-" + i + "-X.zip"));
        }

        RawArchiveDelivery.Result result = delivery.deliver(five, tempDir);

        assertEquals(result.getDelivered(), 2);
        assertEquals(result.getDeferred(), 3);
        verify(mockTransport, times(2)).upload(anyList());
        // Deferred, never dropped: each one over the cap is parked so the next run picks it up.
        verify(mockStore, times(3)).markRawPending(eq(HC), anyString());
    }

    @Test
    public void unparseableCapFallsBackToTheDefault() {
        when(mockConfig.get(RawArchiveDelivery.CONFIG_KEY_RAW_MAX_PER_RUN)).thenReturn("not-a-number");

        RawArchiveDelivery.Result result = delivery.deliver(one(), tempDir);

        // A typo in config must not silently disable delivery (cap 0) or uncap it.
        assertEquals(result.getDelivered(), 1);
        assertTrue(RawArchiveDelivery.DEFAULT_MAX_PER_RUN > 0);
    }

    @Test
    public void oneFailureDoesNotStopTheRest() {
        List<RawCandidate> three = ImmutableList.of(new RawCandidate(HC, "raw/2026-09-01/a-X.zip"),
                new RawCandidate(HC, "raw/2026-09-02/b-X.zip"), new RawCandidate(HC, "raw/2026-09-03/c-X.zip"));
        Mockito.doNothing().doThrow(new RuntimeException("azure blip")).doNothing().when(mockTransport)
                .upload(anyList());

        RawArchiveDelivery.Result result = delivery.deliver(three, tempDir);

        assertEquals(result.getDelivered(), 2);
        assertEquals(result.getParked(), 1);
        verify(mockStore).markRawPending(HC, "raw/2026-09-02/b-X.zip");
    }
}
