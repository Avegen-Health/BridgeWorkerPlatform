package org.sagebionetworks.bridge.addf;

/**
 * ADDF §4.2 — the publish request emitted by the daily EventBridge rule (Phase 5) onto the existing
 * {@code Bridge-WorkerPlatform-Request-{env}} queue with {@code service = "AddfPublishWorker"}.
 *
 * <p>{@code snapshotDate} is a <b>publish label / part-file name component</b> (§4.3.2–4.3.3), <b>not</b> a row filter.
 * The publish worker drains all staged-but-unpublished objects; the effective cutoff is the start-of-publish staging
 * listing, not this date. When absent the worker defaults it to today (UTC).</p>
 *
 * <p>Bean-style (no-arg + setter) to match {@link AddfExportRequest} / {@link AddfParticipantVersionRequest} — the
 * shape {@code DefaultObjectMapper} deserialises without a {@code @JsonCreator}.</p>
 */
public class AddfPublishRequest {
    private String snapshotDate;

    /** The publish label (yyyy-MM-dd), or null to let the worker default to today (UTC). */
    public String getSnapshotDate() {
        return snapshotDate;
    }

    public void setSnapshotDate(String snapshotDate) {
        this.snapshotDate = snapshotDate;
    }
}
