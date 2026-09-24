package org.sagebionetworks.bridge.addf;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.azure.BlobTransport;
import org.sagebionetworks.bridge.addf.publish.PublishLease;
import org.sagebionetworks.bridge.addf.publish.PublishMarker;
import org.sagebionetworks.bridge.addf.publish.RawArchiveDelivery;
import org.sagebionetworks.bridge.addf.publish.SnapshotDelta;
import org.sagebionetworks.bridge.addf.publish.SnapshotDeltaBuilder;
import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

/**
 * ADDF §4.1 — the publish worker. Once daily (US night, off-peak) it coalesces the export-store snapshot delta and
 * copies it to the ADDI Azure Blob <b>staging</b> container, crash-safely. Modelled on the scheduled
 * {@code BridgeReporterProcessor}; unlike the accumulate/dimension workers it needs <b>no new queue or poller</b> — it
 * rides the existing {@code Bridge-WorkerPlatform-Request-{env}} queue and its {@code generalSqsWorker}, dispatched by
 * {@code service = "AddfPublishWorker"} through {@code BridgeWorkerPlatformSqsCallback}.
 *
 * <p>Flow (§4.1): parse → kill-switch gate ({@code addf.publish.enabled}) → build the delta (SnapshotDeltaBuilder, the
 * sole writer of every consolidated file) → upload the delta to Azure (BlobTransport, in-JVM SDK) → write the
 * {@code _publish/<date>.done} marker <b>only after</b> the upload confirms (PublishMarker). A crash before the marker
 * re-runs the day idempotently (overwrite-in-place).</p>
 */
@Component("AddfPublishWorker")
public class AddfPublishWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(AddfPublishWorkerProcessor.class);

    static final String CONFIG_KEY_PUBLISH_ENABLED = "addf.publish.enabled";
    private static final String CONNECTION_POOL_SHUTDOWN = "Connection pool shut down";

    private Config config;
    private SnapshotDeltaBuilder snapshotDeltaBuilder;
    private BlobTransport blobTransport;
    private RawArchiveDelivery rawArchiveDelivery;
    private PublishMarker publishMarker;
    private PublishLease publishLease;
    private FileHelper fileHelper;

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.config = config;
    }

    @Autowired
    public final void setRawArchiveDelivery(RawArchiveDelivery rawArchiveDelivery) {
        this.rawArchiveDelivery = rawArchiveDelivery;
    }

    @Autowired
    public final void setPublishLease(PublishLease publishLease) {
        this.publishLease = publishLease;
    }

    @Autowired
    public final void setSnapshotDeltaBuilder(SnapshotDeltaBuilder snapshotDeltaBuilder) {
        this.snapshotDeltaBuilder = snapshotDeltaBuilder;
    }

    @Autowired
    public final void setBlobTransport(BlobTransport blobTransport) {
        this.blobTransport = blobTransport;
    }

    @Autowired
    public final void setPublishMarker(PublishMarker publishMarker) {
        this.publishMarker = publishMarker;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Override
    public void accept(JsonNode jsonNode) throws IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, WorkerException {
        AddfPublishRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, AddfPublishRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing ADDF publish request: " + e.getMessage(), e);
        }

        // snapshotDate is a label, not a filter (§4.3.3); default to today (UTC) when the scheduler omits it.
        String snapshotDate = request.getSnapshotDate();
        if (snapshotDate == null || snapshotDate.isEmpty()) {
            snapshotDate = AddfDateUtils.todayUtcDate();
        }

        if (!Boolean.parseBoolean(config.get(CONFIG_KEY_PUBLISH_ENABLED))) {
            LOG.info("ADDF publish disabled (addf.publish.enabled=false), skipping snapshotDate=" + snapshotDate);
            return;
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            publish(snapshotDate);
        } catch (PollSqsWorkerBadRequestException | PollSqsWorkerRetryableException | WorkerException | IOException ex) {
            throw ex;
        } catch (IllegalStateException ex) {
            // S3 connection pool can be shut down transiently (credential refresh race). Treat as retryable so the
            // scheduled day re-runs rather than dead-lettering.
            if (ex.getMessage() != null && ex.getMessage().contains(CONNECTION_POOL_SHUTDOWN)) {
                throw new PollSqsWorkerRetryableException(ex.getMessage(), ex);
            }
            throw new WorkerException(ex);
        } catch (RuntimeException ex) {
            throw new WorkerException(ex);
        } finally {
            LOG.info("ADDF publish took " + stopwatch.elapsed(TimeUnit.SECONDS) + "s for snapshotDate " + snapshotDate);
        }
    }

    // Package-scoped for unit tests. Declares the worker's full checked-throws set (mirroring the accumulate worker's
    // process()) so the accept() multi-catch that maps them is legal; today only IOException is actually thrown here.
    void publish(String snapshotDate) throws IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, WorkerException {
        // Idempotency short-circuit: a duplicate delivery for a snapshot already fully published (marker present) is a
        // no-op. The marker is written only after a confirmed upload, so its presence means the day genuinely completed.
        if (publishMarker.isPublished(snapshotDate)) {
            LOG.info("ADDF publish already completed for snapshotDate=" + snapshotDate + " (marker present); skipping");
            return;
        }

        // The .done marker says "this day finished", never "this day is running", so it cannot stop a second trigger
        // from starting a concurrent run — and two runs read-modify-writing the same consolidated tables silently
        // lose rows. Take an in-run lease before touching anything (§4.5).
        if (!publishLease.acquire(snapshotDate)) {
            return;
        }

        File tempDir = fileHelper.createTempDir();
        try {
            SnapshotDelta delta = snapshotDeltaBuilder.build(snapshotDate, tempDir);
            // Upload to Azure staging BEFORE consuming staging or writing the marker: on a crash/upload failure here the
            // staging objects and tombstones survive (build() only wrote the idempotent consolidated files), so the day
            // replays and rebuilds the identical delta instead of silently dropping it from the Azure mirror (§4.5).
            blobTransport.upload(delta.getBlobs());
            // Raw archives stream one at a time, and must run while the staging that produced the candidate list is
            // still in place. Per-archive failures are parked for retry in here rather than thrown, so an unshippable
            // archive degrades one record instead of blocking the table commit below.
            RawArchiveDelivery.Result rawResult = rawArchiveDelivery.deliver(delta.getRawCandidates(), tempDir);
            // Upload confirmed: now retire consumed staging + clear tombstone markers, then write the done-marker.
            snapshotDeltaBuilder.commit(delta);
            publishMarker.mark(snapshotDate);
            LOG.info("ADDF publish succeeded snapshotDate=" + snapshotDate + " blobs=" + delta.getBlobs().size()
                    + " rawDelivered=" + rawResult.getDelivered() + " rawParked=" + rawResult.getParked()
                    + " rawDeferred=" + rawResult.getDeferred());
        } finally {
            publishLease.release(snapshotDate);
            try {
                fileHelper.deleteDirRecursively(tempDir);
            } catch (IOException ex) {
                LOG.error("Error deleting temp dir " + tempDir.getAbsolutePath() + " for publish snapshotDate "
                        + snapshotDate + ": " + ex.getMessage(), ex);
            }
        }
    }
}
