package org.sagebionetworks.bridge.addf.publish;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Date;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;

/**
 * Pins the overlap guard that stops two publish runs read-modify-writing the same consolidated tables. The failure it
 * prevents is silent (last writer wins, the loser's rows vanish), so the lease behaviour needs to be explicit.
 */
public class PublishLeaseTest {
    private static final String BUCKET = "org-gvbridge-addf-exportstore-test";
    private static final String DATE = "2026-09-22";
    private static final String LEASE_KEY = "biaffect-3/_publish/2026-09-22.running";

    private AmazonS3 mockS3;
    private PublishLease lease;

    @BeforeMethod
    public void before() {
        mockS3 = mock(AmazonS3.class);
        Config mockConfig = mock(Config.class);
        when(mockConfig.get(PublishLease.CONFIG_KEY_EXPORTSTORE_BUCKET)).thenReturn(BUCKET);

        lease = new PublishLease();
        lease.setBridgeConfig(mockConfig);
        lease.setAddfS3Client(mockS3);
    }

    /** Stub the lease-probe LIST to report a lease last modified {@code ageMillis} ago, or none at all. */
    private void existingLease(Long ageMillis) {
        ListObjectsV2Result result = new ListObjectsV2Result();
        if (ageMillis != null) {
            S3ObjectSummary summary = new S3ObjectSummary();
            summary.setKey(LEASE_KEY);
            summary.setLastModified(new Date(System.currentTimeMillis() - ageMillis));
            result.getObjectSummaries().add(summary);
        }
        when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);
    }

    @Test
    public void acquiresWhenNoLeaseHeld() {
        existingLease(null);

        assertTrue(lease.acquire(DATE));

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3).putObject(captor.capture());
        assertEquals(captor.getValue().getKey(), LEASE_KEY);
        assertEquals(captor.getValue().getBucketName(), BUCKET);
    }

    @Test
    public void refusesWhenAFreshLeaseIsHeld() {
        existingLease(60_000L); // a run started a minute ago and is still going

        assertFalse(lease.acquire(DATE), "a second concurrent run must not start");
        verify(mockS3, never()).putObject(any(PutObjectRequest.class));
    }

    @Test
    public void takesOverAStaleLease() {
        existingLease(PublishLease.LEASE_TTL_MILLIS + 1000L);

        // A crashed run leaves its lease behind. Without stale takeover, publish would wedge until someone deleted
        // the object by hand — worse than the overlap the lease exists to prevent.
        assertTrue(lease.acquire(DATE));
        verify(mockS3).putObject(any(PutObjectRequest.class));
    }

    @Test
    public void ignoresAnUnrelatedKeyReturnedByTheProbe() {
        ListObjectsV2Result result = new ListObjectsV2Result();
        S3ObjectSummary other = new S3ObjectSummary();
        other.setKey("biaffect-3/_publish/2026-09-22.done");
        other.setLastModified(new Date());
        result.getObjectSummaries().add(other);
        when(mockS3.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(result);

        // The prefix probe can match the .done marker; only an exact .running hit is a held lease.
        assertTrue(lease.acquire(DATE));
    }

    @Test
    public void releaseDeletesTheLease() {
        lease.release(DATE);

        verify(mockS3).deleteObject(BUCKET, LEASE_KEY);
    }

    @Test
    public void releaseSwallowsFailuresSoItNeverMasksTheRunOutcome() {
        org.mockito.Mockito.doThrow(new RuntimeException("s3 down")).when(mockS3).deleteObject(anyString(),
                anyString());

        lease.release(DATE); // must not throw — a leaked lease self-heals after the TTL
    }

    @Test
    public void leaseIsScopedPerSnapshotDate() {
        existingLease(null);
        lease.acquire("2026-09-23");

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3).putObject(captor.capture());
        assertEquals(captor.getValue().getKey(), "biaffect-3/_publish/2026-09-23.running");
    }

    @Test
    public void releaseUsesTheSameKeyAcquireWrote() {
        existingLease(null);
        lease.acquire(DATE);
        lease.release(DATE);

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3).putObject(captor.capture());
        verify(mockS3).deleteObject(eq(BUCKET), eq(captor.getValue().getKey()));
    }

    @Test
    public void leaseTtlComfortablyExceedsAPublishRun() {
        // The lease is only safe if it outlives a legitimate run; a TTL shorter than a slow publish would hand the
        // lease to a second run mid-flight and reintroduce exactly the race this class exists to stop.
        assertTrue(PublishLease.LEASE_TTL_MILLIS >= 60L * 60 * 1000,
                "lease TTL must leave room for a slow run, was " + PublishLease.LEASE_TTL_MILLIS + "ms");
    }
}
