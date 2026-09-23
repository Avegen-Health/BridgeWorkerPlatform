package org.sagebionetworks.bridge.addf;

/**
 * Request to the one-time ADDF participant-version backfill ({@link AddfParticipantVersionBackfillWorkerProcessor}).
 * Mirrors {@code BackfillParticipantVersionsRequest}: a health-code list lives in the backfill bucket at {@code s3Key},
 * and the worker enqueues an {@code AddfParticipantVersionWorker} message per (healthCode, version).
 *
 * <p><b>CI/CD-only trigger.</b> The kickoff message must be produced by a gated pipeline job / one-shot rule, never a
 * developer {@code aws sqs send-message} — the read-only-CLI rule (implementation-plan §Deployment) forbids a laptop
 * write.</p>
 */
public class AddfParticipantVersionBackfillRequest {
    private String appId;
    private String s3Key;

    /** App ID of participants to backfill. */
    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    /** Key in the backfill bucket of a newline-delimited health-code list. */
    public String getS3Key() {
        return s3Key;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }
}
