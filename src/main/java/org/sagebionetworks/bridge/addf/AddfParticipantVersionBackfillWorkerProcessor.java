package org.sagebionetworks.bridge.addf;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.AmazonSQS;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

/**
 * ADDF §3b.4 — the one-time participant-version backfill. Participants whose versions predate ADDF (or predate
 * {@code addf.export.enabled}) never had a fan-out, so their {@code participant_versions} rows are missing. Because
 * taking an assessment does not emit a participant-version event, such a participant's <i>new</i> uploads carry a
 * {@code participant_version} that ADDF has never seen, and the publish step defers those rows as orphans
 * indefinitely (§3b.5) — so a pre-existing participant never reaches ADDF until this backfill runs.
 *
 * <p>Two modes, both enqueueing an {@code AddfParticipantVersionWorker} message per (healthCode, version) onto the
 * ADDF queue:</p>
 * <ul>
 *   <li><b>Whole-app</b> — {@code s3Key} omitted. Enumerates every account via {@code getAllAccountSummaries}. This
 *       is what the gated CI kickoff uses: no health-code list is produced, uploaded or handled by an operator.</li>
 *   <li><b>Targeted</b> — {@code s3Key} set. Reads a health-code list from the backfill bucket, as before.</li>
 * </ul>
 *
 * <p>Either way each participant costs <b>one</b> {@code getAllParticipantVersionsForUser} call, which returns every
 * version at once (each carrying its own health code). This replaces the original 1..N per-version probe loop —
 * roughly a (versions+1)× reduction in Bridge calls, which matters because the whole run happens inside a single SQS
 * message's visibility window.</p>
 *
 * <p>Runs <b>once per env at rollout</b>, not on every deploy. Precedent + shape: {@code
 * BackfillParticipantVersionsWorker}. Its kickoff is <b>CI/CD-triggered</b> (gated job / one-shot rule) — never a
 * laptop {@code sqs send-message}.</p>
 */
@Component("AddfParticipantVersionBackfillWorker")
public class AddfParticipantVersionBackfillWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(AddfParticipantVersionBackfillWorkerProcessor.class);

    static final String CONFIG_KEY_BACKFILL_BUCKET = "backfill.bucket";
    static final String CONFIG_KEY_ADDF_QUEUE_URL = "addf.export.request.sqs.queue.url";
    static final String WORKER_ID = "AddfParticipantVersionBackfillWorker";
    static final String ADDF_VERSION_WORKER = "AddfParticipantVersionWorker";
    private static final int REPORTING_INTERVAL = 1000;

    // Match Bridge/Synapse throttle ceiling (~10 rps) for the per-participant API calls.
    private final RateLimiter rateLimiter = RateLimiter.create(10.0);

    private String backfillBucket;
    private String addfQueueUrl;
    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;
    private S3Helper s3Helper;
    private AmazonSQS sqsClient;

    @Autowired
    public final void setConfig(Config config) {
        this.backfillBucket = config.get(CONFIG_KEY_BACKFILL_BUCKET);
        this.addfQueueUrl = config.get(CONFIG_KEY_ADDF_QUEUE_URL);
    }

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    @Autowired
    public final void setS3Helper(S3Helper s3Helper) {
        this.s3Helper = s3Helper;
    }

    @Autowired
    public final void setAddfSqsClient(@Qualifier("addfSqsClient") AmazonSQS sqsClient) {
        this.sqsClient = sqsClient;
    }

    @Override
    public void accept(JsonNode jsonNode) throws Exception {
        AddfParticipantVersionBackfillRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode,
                    AddfParticipantVersionBackfillRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing ADDF backfill request: " + e.getMessage(), e);
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } finally {
            LOG.info("ADDF participant-version backfill took " + stopwatch.elapsed(TimeUnit.SECONDS) + "s for app " +
                    request.getAppId() + " s3Key " + request.getS3Key());
        }
    }

    private void process(AddfParticipantVersionBackfillRequest request) throws IOException {
        String appId = request.getAppId();
        String s3Key = request.getS3Key();

        if (s3Key == null || s3Key.trim().isEmpty()) {
            processAllAccounts(appId);
        } else {
            processHealthCodeList(appId, s3Key.trim());
        }
    }

    /**
     * Whole-app mode (no {@code s3Key}). Enumerates every account in the app and backfills each one's versions. This
     * is the mode the gated CI kickoff uses: it needs no health-code list, so no file of health codes is ever
     * produced, uploaded or handled by an operator.
     */
    private void processAllAccounts(String appId) {
        LOG.info("Starting ADDF participant-version backfill for app " + appId + " over ALL accounts");

        int numAccounts = 0;
        int totalEnqueued = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        Iterator<AccountSummary> accountIter = bridgeHelper.getAllAccountSummaries(appId, false);
        try {
            while (accountIter.hasNext()) {
                AccountSummary summary = accountIter.next();
                if (summary == null || summary.getId() == null) {
                    continue;
                }
                totalEnqueued += enqueueAllVersions(appId, summary.getId(), "userId " + summary.getId());

                numAccounts++;
                if (numAccounts % REPORTING_INTERVAL == 0) {
                    LOG.info("ADDF backfill for app " + appId + ": " + numAccounts + " accounts, " + totalEnqueued +
                            " versions enqueued in " + stopwatch.elapsed(TimeUnit.SECONDS) + "s");
                }
            }
        } catch (RuntimeException ex) {
            // Page loads happen inside the iterator, and AccountSummaryIterator wraps a page-load
            // IOException as an unchecked RuntimeException ("Iterator can't throw exceptions"), so a
            // transient pagination failure surfaces here and would otherwise abort the walk with no
            // durable record of how far it got. Record the partial progress, then RETHROW.
            //
            // Deliberately NOT swallowed: returning normally would let the callback delete the SQS
            // message, leaving a silently incomplete backfill -- an operator would believe the run
            // succeeded while pre-existing participants stayed orphan-deferred, which is the exact
            // failure this worker exists to fix. Rethrowing redelivers instead, and the restart is
            // cheap because AddfParticipantVersionWorker presence-skips versions already written.
            finish("app=" + appId + ", mode=allAccounts, status=FAILED, accountsProcessed=" + numAccounts +
                    ", versionsEnqueued=" + totalEnqueued + ", error=" + ex.getMessage());
            throw ex;
        }

        finish("app=" + appId + ", mode=allAccounts, status=complete, totalAccounts=" + numAccounts +
                ", versionsEnqueued=" + totalEnqueued);
    }

    /** Targeted mode: backfill only the health codes listed in the backfill bucket at {@code s3Key}. */
    private void processHealthCodeList(String appId, String s3Key) throws IOException {
        List<String> healthCodeList = s3Helper.readS3FileAsLines(backfillBucket, s3Key);
        int totalHealthCodes = healthCodeList.size();
        LOG.info("Starting ADDF participant-version backfill for app " + appId + " s3Key " + s3Key + " with " +
                totalHealthCodes + " health codes");

        int numHealthCodes = 0;
        int totalEnqueued = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (String healthCode : healthCodeList) {
            if (healthCode == null || healthCode.trim().isEmpty()) {
                continue;
            }
            healthCode = healthCode.trim();
            totalEnqueued += enqueueAllVersions(appId, "healthCode:" + healthCode, "healthCode " + healthCode);

            numHealthCodes++;
            if (numHealthCodes % REPORTING_INTERVAL == 0) {
                LOG.info("ADDF backfill for app " + appId + ": " + numHealthCodes + " / " + totalHealthCodes +
                        " health codes, " + totalEnqueued + " versions enqueued in " +
                        stopwatch.elapsed(TimeUnit.SECONDS) + "s");
            }
        }

        finish("app=" + appId + ", mode=healthCodeList, s3Key=" + s3Key + ", totalHealthCodes=" + totalHealthCodes +
                ", versionsEnqueued=" + totalEnqueued);
    }

    private void finish(String tag) {
        dynamoHelper.writeWorkerLog(WORKER_ID, tag);
        LOG.info("Finished ADDF participant-version backfill: " + tag);
    }

    /**
     * Fetch every version for one participant and enqueue an {@code AddfParticipantVersionWorker} message per version.
     *
     * <p>{@code userIdToken} is any Bridge user-ID token — a bare {@code userId} (whole-app mode) or
     * {@code healthCode:<hc>} (list mode). One API call returns all versions, each already carrying its own
     * {@code healthCode}, so the caller never needs to know the health code up front.</p>
     *
     * <p>No consent filtering here on purpose: {@code AddfParticipantVersionWorker} applies the fail-closed gate per
     * version (§3b.3), tombstoning NO_SHARING participants rather than silently dropping them. Filtering at this
     * layer would skip those tombstones.</p>
     */
    private int enqueueAllVersions(String appId, String userIdToken, String logLabel) {
        rateLimiter.acquire();
        List<ParticipantVersion> versions;
        try {
            versions = bridgeHelper.getAllParticipantVersionsForUser(appId, userIdToken);
        } catch (EntityNotFoundException ex) {
            // Participant has no versions at all — nothing to backfill.
            return 0;
        } catch (Exception ex) {
            LOG.error("ADDF backfill: error fetching versions for app " + appId + " " + logLabel + "; skipping: " +
                    ex.getMessage(), ex);
            return 0;
        }
        if (versions == null || versions.isEmpty()) {
            return 0;
        }

        int enqueued = 0;
        for (ParticipantVersion version : versions) {
            if (version == null || version.getHealthCode() == null || version.getParticipantVersion() == null) {
                LOG.warn("ADDF backfill: incomplete participant version for app " + appId + " " + logLabel +
                        "; skipping");
                continue;
            }
            try {
                sqsClient.sendMessage(addfQueueUrl,
                        envelope(appId, version.getHealthCode(), version.getParticipantVersion()));
                enqueued++;
            } catch (Exception ex) {
                LOG.error("ADDF backfill: failed to enqueue app " + appId + " healthCode " + version.getHealthCode() +
                        " version " + version.getParticipantVersion() + ": " + ex.getMessage(), ex);
            }
        }
        return enqueued;
    }

    private static String envelope(String appId, String healthCode, int version) throws IOException {
        ObjectNode root = DefaultObjectMapper.INSTANCE.createObjectNode();
        root.put("service", ADDF_VERSION_WORKER);
        ObjectNode body = root.putObject("body");
        body.put("appId", appId);
        body.put("healthCode", healthCode);
        body.put("participantVersion", version);
        return DefaultObjectMapper.INSTANCE.writeValueAsString(root);
    }
}
