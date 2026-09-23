package org.sagebionetworks.bridge.addf.store;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
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

    // ------------------------------------------------------------------------------------------------------------
    // Publish-side IO (Phase 4).
    // ------------------------------------------------------------------------------------------------------------

    @Test
    public void consolidatedAndKeyboardKeys() {
        assertEquals(client.consolidatedKey("phq9"), "biaffect-3/current/tables/phq9.parquet");
        assertEquals(client.keyboardPartKey("2026-08", "2026-09-22"),
                "biaffect-3/keyboard_sessions/month=2026-08/part-2026-09-22.parquet");
    }

    @Test
    public void consolidatedExistsDelegates() {
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/current/tables/phq9.parquet")).thenReturn(true);
        assertTrue(client.consolidatedExists("phq9"));
        assertFalse(client.consolidatedExists("evening_log"));
    }

    @Test
    public void listStagedReturnsKeysAndSkipsFolderPlaceholders() {
        ListObjectsV2Result result = new ListObjectsV2Result();
        result.getObjectSummaries().add(summary("biaffect-3/_staging/phq9/2026-09-22/rec-1.parquet"));
        result.getObjectSummaries().add(summary("biaffect-3/_staging/phq9/2026-09-22/rec-2.parquet"));
        result.getObjectSummaries().add(summary("biaffect-3/_staging/phq9/")); // folder placeholder -> skipped
        when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

        List<String> keys = client.listStaged("phq9");
        assertEquals(keys, ImmutableList.of("biaffect-3/_staging/phq9/2026-09-22/rec-1.parquet",
                "biaffect-3/_staging/phq9/2026-09-22/rec-2.parquet"));
    }

    @Test
    public void listTombstonedHealthCodesStripsPrefix() {
        ListObjectsV2Result result = new ListObjectsV2Result();
        result.getObjectSummaries().add(summary("biaffect-3/_tombstone/hc-1"));
        result.getObjectSummaries().add(summary("biaffect-3/_tombstone/hc-2"));
        when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

        assertEquals(client.listTombstonedHealthCodes(), ImmutableList.of("hc-1", "hc-2"));
    }

    @Test
    public void downloadDelegatesToGetObject() {
        File dest = new File("out.parquet");
        assertEquals(client.download("biaffect-3/current/tables/phq9.parquet", dest), dest);
        verify(mockS3).getObject(any(GetObjectRequest.class), org.mockito.Matchers.eq(dest));
    }

    @Test
    public void putObjectWritesToKey() {
        client.putObject("biaffect-3/current/tables/phq9.parquet", new File("phq9.parquet"));
        PutObjectRequest req = capturePut();
        assertEquals(req.getKey(), "biaffect-3/current/tables/phq9.parquet");
    }

    @Test
    public void deleteObjectsAndTombstone() {
        client.deleteObjects(ImmutableList.of("k1", "k2"));
        verify(mockS3).deleteObject(BUCKET, "k1");
        verify(mockS3).deleteObject(BUCKET, "k2");

        client.deleteTombstone("hc-9");
        verify(mockS3).deleteObject(BUCKET, "biaffect-3/_tombstone/hc-9");
    }

    private static S3ObjectSummary summary(String key) {
        S3ObjectSummary summary = new S3ObjectSummary();
        summary.setKey(key);
        return summary;
    }
}
