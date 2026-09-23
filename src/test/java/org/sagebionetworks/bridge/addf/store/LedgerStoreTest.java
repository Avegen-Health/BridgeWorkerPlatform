package org.sagebionetworks.bridge.addf.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;

public class LedgerStoreTest {
    private static final String BUCKET = "my-bucket";

    private AmazonS3 mockS3;
    private LedgerStore ledger;

    @BeforeMethod
    public void before() {
        mockS3 = mock(AmazonS3.class);
        Config config = mock(Config.class);
        when(config.get("addf.exportstore.bucket")).thenReturn(BUCKET);

        ledger = new LedgerStore();
        ledger.setBridgeConfig(config);
        ledger.setAddfS3Client(mockS3);
    }

    @Test
    public void containsRecord() {
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/_ledger/record/rec-1")).thenReturn(true);
        assertTrue(ledger.containsRecord("rec-1"));
        assertFalse(ledger.containsRecord("rec-missing"));
    }

    @Test
    public void markRecord() {
        ledger.markRecord("rec-1");
        assertEquals(capturePut().getKey(), "biaffect-3/_ledger/record/rec-1");
    }

    @Test
    public void containsVersion() {
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/_ledger/version/hc-1/2")).thenReturn(true);
        assertTrue(ledger.containsVersion("hc-1", 2));
        assertFalse(ledger.containsVersion("hc-1", 9));
    }

    @Test
    public void markVersion() {
        ledger.markVersion("hc-1", 2);
        assertEquals(capturePut().getKey(), "biaffect-3/_ledger/version/hc-1/2");
    }

    private PutObjectRequest capturePut() {
        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3).putObject(captor.capture());
        return captor.getValue();
    }
}
