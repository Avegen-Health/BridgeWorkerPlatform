package org.sagebionetworks.bridge.addf;

/**
 * Request to the one-time ADDF participant-version backfill ({@link AddfParticipantVersionBackfillWorkerProcessor}).
 * The worker enqueues an {@code AddfParticipantVersionWorker} message per (healthCode, version).
 *
 * <p>{@code s3Key} is <b>optional</b> and selects the mode: omit it to backfill <i>every</i> account in the app
 * (what the gated CI kickoff sends — no health-code list required), or set it to the key of a newline-delimited
 * health-code list in the backfill bucket to backfill just those participants.</p>
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

    /**
     * Optional key in the backfill bucket of a newline-delimited health-code list. Null/blank means "backfill every
     * account in the app".
     */
    public String getS3Key() {
        return s3Key;
    }

    public void setS3Key(String s3Key) {
        this.s3Key = s3Key;
    }
}
