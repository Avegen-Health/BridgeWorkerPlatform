package org.sagebionetworks.bridge.addf.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;

public class ExportStoreClientTest {
    private static final String BUCKET = "my-bucket";

    private AmazonS3 mockS3;
    private ExportStoreClient client;

    @BeforeMethod
    public void before() {
        mockS3 = mock(AmazonS3.class);
        Config config = mock(Config.class);
        when(config.get("addf.exportstore.bucket")).thenReturn(BUCKET);

        client = new ExportStoreClient();
        client.setBridgeConfig(config);
        client.setAddfS3Client(mockS3);
    }

    private PutObjectRequest capturePut() {
        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3).putObject(captor.capture());
        return captor.getValue();
    }

    @Test
    public void getBucket() {
        assertEquals(client.getBucket(), BUCKET);
    }

    @Test
    public void stageTableRow() {
        client.stageTableRow("phq9", "rec-1", new File("x.parquet"));
        PutObjectRequest req = capturePut();
        assertEquals(req.getBucketName(), BUCKET);
        assertTrue(req.getKey().startsWith("biaffect-3/_staging/phq9/"));
        assertTrue(req.getKey().endsWith("/rec-1.parquet"));
    }

    @Test
    public void stageVersionRow() {
        client.stageVersionRow("hc-1", 4, new File("x.parquet"));
        PutObjectRequest req = capturePut();
        assertTrue(req.getKey().startsWith("biaffect-3/_staging/participant_versions/"));
        assertTrue(req.getKey().endsWith("/hc-1_4.parquet"));
    }

    @Test
    public void markTombstone() {
        client.markTombstone("hc-1");
        PutObjectRequest req = capturePut();
        assertEquals(req.getKey(), "biaffect-3/_tombstone/hc-1");
    }

    @Test
    public void putRawWhenAbsentStoresAndReturnsRelativeKey() {
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/raw/2026-08-15/rec-1-PHQ-9.zip")).thenReturn(false);
        String relative = client.putRaw("2026-08-15", "rec-1", "PHQ-9", new File("a.zip"));
        assertEquals(relative, "raw/2026-08-15/rec-1-PHQ-9.zip");
        PutObjectRequest req = capturePut();
        assertEquals(req.getKey(), "biaffect-3/raw/2026-08-15/rec-1-PHQ-9.zip");
    }

    @Test
    public void putRawWhenPresentSkipsWrite() {
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/raw/2026-08-15/rec-1-PHQ-9.zip")).thenReturn(true);
        String relative = client.putRaw("2026-08-15", "rec-1", "PHQ-9", new File("a.zip"));
        assertEquals(relative, "raw/2026-08-15/rec-1-PHQ-9.zip");
        verify(mockS3, never()).putObject((PutObjectRequest) org.mockito.Matchers.any());
    }

    @Test
    public void putRawSanitizesAssessmentAndHandlesNull() {
        when(mockS3.doesObjectExist(org.mockito.Matchers.eq(BUCKET), org.mockito.Matchers.anyString()))
                .thenReturn(false);
        assertEquals(client.putRaw("2026-08-15", "rec-1", "weird name/#", new File("a.zip")),
                "raw/2026-08-15/rec-1-weird_name__.zip");
        assertEquals(client.putRaw("2026-08-15", "rec-2", null, new File("a.zip")),
                "raw/2026-08-15/rec-2-unknown.zip");
    }
}
