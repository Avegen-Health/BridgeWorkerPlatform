package org.sagebionetworks.bridge.addf.publish;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;

/**
 * ADDF §4.5 — an in-run overlap guard for the publish worker.
 *
 * <p><b>Why {@code PublishMarker} is not enough.</b> The {@code _publish/<date>.done} marker is written only after a
 * run <i>finishes</i>, so it is an idempotency check, not a lock: it says "this day is done", never "this day is
 * running". Every publish trigger that fires while a run is still in flight passes {@code isPublished()} and starts a
 * second concurrent run. Two triggers make that routine rather than theoretical — the EventBridge schedule (which has
 * run as often as every 5 minutes on uat), and SQS redelivery when a run outlives the request queue's visibility
 * timeout, which is 120s on the uat queue and carries no dead-letter policy.</p>
 *
 * <p>Concurrent runs are not a throughput problem, they are a correctness one: {@code SnapshotDeltaBuilder} is
 * documented as the sole writer of every consolidated table file, and two runs both read-modify-write the same
 * {@code current/tables/*.parquet} objects. Last writer wins and the loser's merged rows are silently gone.</p>
 *
 * <p><b>Best-effort, deliberately.</b> This is a read-then-write lease on S3, so it is not airtight — two runs that
 * check within the same few milliseconds can both acquire. It narrows the window from minutes-or-hours to
 * milliseconds, which removes the failure mode in practice. Closing it completely needs a conditional write
 * (DynamoDB {@code attribute_not_exists}, or S3 {@code If-None-Match}, which the SDK v1 on this runtime does not
 * expose); that is worth doing if publish ever becomes multi-writer by design. The lease is stale-safe: a crashed run
 * leaves its lease behind, so anything older than {@link #LEASE_TTL_MILLIS} is ignored and overwritten rather than
 * wedging publish until someone deletes the object by hand.</p>
 */
@Component
public class PublishLease {
    private static final Logger LOG = LoggerFactory.getLogger(PublishLease.class);

    static final String ROOT_PREFIX = "biaffect-3/";
    static final String LEASE_PREFIX = ROOT_PREFIX + "_publish/";
    static final String LEASE_SUFFIX = ".running";
    static final String CONFIG_KEY_EXPORTSTORE_BUCKET = "addf.exportstore.bucket";

    /**
     * How long a lease is honoured before it is treated as abandoned. Must comfortably exceed the worst-case publish
     * run; raw delivery is bounded per run ({@code addf.publish.raw.max.per.run}) specifically so that this stays a
     * meaningful number.
     */
    static final long LEASE_TTL_MILLIS = 6L * 60 * 60 * 1000; // 6 hours

    private AmazonS3 s3Client;
    private String bucket;

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.bucket = config.get(CONFIG_KEY_EXPORTSTORE_BUCKET);
    }

    @Autowired
    public final void setAddfS3Client(@Qualifier("addfS3Client") AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    /**
     * Try to take the lease for this snapshot. Returns false when another run holds a lease that has not yet gone
     * stale — the caller must then return without touching any consolidated file.
     */
    public boolean acquire(String snapshotDate) {
        String key = leaseKey(snapshotDate);
        Date heldSince = existingLeaseDate(key);
        if (heldSince != null) {
            long ageMillis = System.currentTimeMillis() - heldSince.getTime();
            if (ageMillis < LEASE_TTL_MILLIS) {
                LOG.info("ADDF publish lease already held for snapshotDate={} ({}s ago); skipping this run",
                        snapshotDate, ageMillis / 1000);
                return false;
            }
            LOG.warn("ADDF publish lease for snapshotDate={} is stale ({}s old) — a previous run likely died; taking it",
                    snapshotDate, ageMillis / 1000);
        }
        write(key, snapshotDate);
        return true;
    }

    /** Release the lease. Safe to call when the lease was never taken. */
    public void release(String snapshotDate) {
        try {
            s3Client.deleteObject(bucket, leaseKey(snapshotDate));
        } catch (RuntimeException ex) {
            // A leaked lease self-heals after the TTL, so this must never mask the run's own outcome.
            LOG.warn("ADDF publish: could not release lease for snapshotDate=" + snapshotDate + ": " + ex.getMessage());
        }
    }

    /**
     * The lease object's own S3 last-modified time, or null when no lease is held. Read via LIST rather than HEAD so
     * a missing object is an empty result instead of an exception.
     */
    private Date existingLeaseDate(String key) {
        ListObjectsV2Result result = s3Client.listObjectsV2(
                new ListObjectsV2Request().withBucketName(bucket).withPrefix(key).withMaxKeys(1));
        for (S3ObjectSummary summary : result.getObjectSummaries()) {
            if (key.equals(summary.getKey())) {
                return summary.getLastModified();
            }
        }
        return null;
    }

    private void write(String key, String snapshotDate) {
        byte[] body = snapshotDate.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        metadata.setContentLength(body.length);
        s3Client.putObject(new PutObjectRequest(bucket, key, new ByteArrayInputStream(body), metadata));
    }

    private String leaseKey(String snapshotDate) {
        return LEASE_PREFIX + snapshotDate + LEASE_SUFFIX;
    }
}
