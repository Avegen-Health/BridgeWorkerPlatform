package org.sagebionetworks.bridge.addf;

import java.io.IOException;
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
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.dynamodb.DynamoHelper;

/**
 * ADDF §3b.4 — the one-time participant-version backfill. Participants whose versions predate ADDF (or predate
 * {@code addf.export.enabled}) never had a fan-out, so their {@code participant_versions} rows are missing and the
 * first snapshot's activity rows would reference absent versions. This worker takes a health-code list from the
 * backfill bucket and, for each participant, enumerates their versions (1..N, stopping at the first that doesn't
 * exist) and enqueues an {@code AddfParticipantVersionWorker} message per (healthCode, version) onto the ADDF queue.
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
    // Safety cap so a bad health code can't probe forever; far above any real participant's version count.
    static final int MAX_VERSIONS = 100_000;
    private static final int REPORTING_INTERVAL = 1000;

    // Match Bridge/Synapse throttle ceiling (~10 rps) for the version-probe API calls.
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
            totalEnqueued += enqueueAllVersions(appId, healthCode);

            numHealthCodes++;
            if (numHealthCodes % REPORTING_INTERVAL == 0) {
                LOG.info("ADDF backfill for app " + appId + ": " + numHealthCodes + " / " + totalHealthCodes +
                        " health codes, " + totalEnqueued + " versions enqueued in " +
                        stopwatch.elapsed(TimeUnit.SECONDS) + "s");
            }
        }

        String tag = "app=" + appId + ", s3Key=" + s3Key + ", totalHealthCodes=" + totalHealthCodes +
                ", versionsEnqueued=" + totalEnqueued;
        dynamoHelper.writeWorkerLog(WORKER_ID, tag);
        LOG.info("Finished ADDF participant-version backfill: " + tag);
    }

    // Enumerate versions 1..N for a health code (stop at the first missing) and enqueue an ADDF message for each.
    private int enqueueAllVersions(String appId, String healthCode) {
        int enqueued = 0;
        for (int version = 1; version <= MAX_VERSIONS; version++) {
            rateLimiter.acquire();
            try {
                bridgeHelper.getParticipantVersion(appId, "healthCode:" + healthCode, version);
            } catch (EntityNotFoundException ex) {
                // No more versions for this participant.
                break;
            } catch (Exception ex) {
                LOG.error("ADDF backfill: error probing app " + appId + " healthCode " + healthCode + " version " +
                        version + "; stopping this participant: " + ex.getMessage(), ex);
                break;
            }
            try {
                sqsClient.sendMessage(addfQueueUrl, envelope(appId, healthCode, version));
                enqueued++;
            } catch (Exception ex) {
                LOG.error("ADDF backfill: failed to enqueue app " + appId + " healthCode " + healthCode + " version " +
                        version + ": " + ex.getMessage(), ex);
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
