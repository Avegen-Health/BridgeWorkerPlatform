package org.sagebionetworks.bridge.addf.publish;

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

public class PublishMarkerTest {
    private static final String BUCKET = "export-bucket";
    private static final String DATE = "2026-09-22";
    private static final String KEY = "biaffect-3/_publish/2026-09-22.done";

    private AmazonS3 mockS3;
    private PublishMarker marker;

    @BeforeMethod
    public void before() {
        mockS3 = mock(AmazonS3.class);
        Config config = mock(Config.class);
        when(config.get("addf.exportstore.bucket")).thenReturn(BUCKET);

        marker = new PublishMarker();
        marker.setBridgeConfig(config);
        marker.setAddfS3Client(mockS3);
    }

    @Test
    public void isPublishedTrueWhenMarkerExists() {
        when(mockS3.doesObjectExist(BUCKET, KEY)).thenReturn(true);
        assertTrue(marker.isPublished(DATE));
    }

    @Test
    public void isPublishedFalseWhenAbsent() {
        when(mockS3.doesObjectExist(BUCKET, KEY)).thenReturn(false);
        assertFalse(marker.isPublished(DATE));
    }

    @Test
    public void markWritesDoneObject() {
        marker.mark(DATE);
        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3).putObject(captor.capture());
        assertEquals(captor.getValue().getBucketName(), BUCKET);
        assertEquals(captor.getValue().getKey(), KEY);
    }
}
