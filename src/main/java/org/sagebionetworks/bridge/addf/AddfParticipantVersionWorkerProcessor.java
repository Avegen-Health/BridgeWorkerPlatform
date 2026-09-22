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

import org.sagebionetworks.bridge.addf.gate.ConsentTestGate;
import org.sagebionetworks.bridge.addf.gate.ConsentVerdict;
import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.ParticipantVersionRowBuilder;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

/**
 * ADDF §3b — the dimension worker. Consumes one participant-version message, gates on the version snapshot's own
 * consent (fail-closed), fetches the {@link ParticipantVersion} from <b>BridgeServer2</b> (the same call
 * {@code Ex3ParticipantVersionWorker} uses — <b>never</b> the Synapse table {@code syn50697927}), upserts one
 * {@code participant_versions} row into the export store, and marks the ledger. Twin of the accumulate worker on the
 * participant-state trigger; shares the same package, queue, poller, and store.
 *
 * <p>Auto-registered by Spring name {@code "AddfParticipantVersionWorker"} and dispatched off the shared ADDF queue by
 * {@code service} name (§2b.3 / §3.8).</p>
 */
@Component("AddfParticipantVersionWorker")
public class AddfParticipantVersionWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(AddfParticipantVersionWorkerProcessor.class);

    private static final String CONNECTION_POOL_SHUTDOWN = "Connection pool shut down";

    private BridgeHelper bridgeHelper;
    private ConsentTestGate consentTestGate;
    private ExportStoreClient exportStoreClient;
    private FileHelper fileHelper;
    private LedgerStore ledgerStore;
    private ParquetRowWriter parquetRowWriter;
    private ParticipantVersionRowBuilder rowBuilder;

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setConsentTestGate(ConsentTestGate consentTestGate) {
        this.consentTestGate = consentTestGate;
    }

    @Autowired
    public final void setExportStoreClient(ExportStoreClient exportStoreClient) {
        this.exportStoreClient = exportStoreClient;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Autowired
    public final void setLedgerStore(LedgerStore ledgerStore) {
        this.ledgerStore = ledgerStore;
    }

    @Autowired
    public final void setParquetRowWriter(ParquetRowWriter parquetRowWriter) {
        this.parquetRowWriter = parquetRowWriter;
    }

    @Autowired
    public final void setParticipantVersionRowBuilder(ParticipantVersionRowBuilder rowBuilder) {
        this.rowBuilder = rowBuilder;
    }

    @Override
    public void accept(JsonNode jsonNode) throws IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, WorkerException {
        AddfParticipantVersionRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, AddfParticipantVersionRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing ADDF participant-version request: " +
                    e.getMessage(), e);
        }
        if (request.getAppId() == null || request.getHealthCode() == null) {
            throw new PollSqsWorkerBadRequestException("ADDF participant-version request missing appId/healthCode");
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } catch (PollSqsWorkerBadRequestException | PollSqsWorkerRetryableException | WorkerException | IOException ex) {
            throw ex;
        } catch (IllegalStateException ex) {
            if (ex.getMessage() != null && ex.getMessage().contains(CONNECTION_POOL_SHUTDOWN)) {
                throw new PollSqsWorkerRetryableException(ex.getMessage(), ex);
            }
            throw new WorkerException(ex);
        } catch (RuntimeException ex) {
            throw new WorkerException(ex);
        } finally {
            LOG.info("ADDF participant-version export took " + stopwatch.elapsed(TimeUnit.SECONDS) + "s for app " +
                    request.getAppId() + " healthCode " + request.getHealthCode() + " version " +
                    request.getParticipantVersion());
        }
    }

    // Package-scoped for unit tests.
    void process(AddfParticipantVersionRequest request) throws IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, WorkerException {
        String appId = request.getAppId();
        String healthCode = request.getHealthCode();
        int versionNum = request.getParticipantVersion();

        // A participant version is immutable — presence-skip on redelivery.
        if (ledgerStore.containsVersion(healthCode, versionNum)) {
            LOG.info("ADDF: participant version already processed, skipping: app " + appId + " healthCode " +
                    healthCode + " version " + versionNum);
            return;
        }

        // Fetch the version snapshot from BS2 — NOT from Synapse syn50697927 (isolation invariant, architecture §6).
        ParticipantVersion participantVersion;
        try {
            participantVersion = bridgeHelper.getParticipantVersion(appId, "healthCode:" + healthCode, versionNum);
        } catch (EntityNotFoundException ex) {
            throw new PollSqsWorkerBadRequestException("No participant version for app " + appId + " healthCode " +
                    healthCode + " version " + versionNum, ex);
        }
        if (participantVersion == null) {
            throw new PollSqsWorkerBadRequestException("Null participant version for app " + appId + " healthCode " +
                    healthCode + " version " + versionNum);
        }

        // Consent gate on the version snapshot's own scope + data groups (§3b.3).
        ConsentVerdict verdict = consentTestGate.evaluateVersion(participantVersion.getSharingScope(),
                participantVersion.getDataGroups());
        if (!verdict.isShouldExport()) {
            if (verdict.isTest()) {
                // Test user — never exported. Mark so we don't reprocess.
                LOG.info("ADDF: skipping test-user participant version app " + appId + " healthCode " + healthCode +
                        " version " + versionNum);
            } else {
                // Withdrawal (NO_SHARING / indeterminate): tombstone rather than exporting a "now not sharing" row.
                exportStoreClient.markTombstone(healthCode);
                LOG.info("ADDF: NO_SHARING participant version -> tombstone app " + appId + " healthCode " +
                        healthCode + " version " + versionNum);
            }
            ledgerStore.markVersion(healthCode, versionNum);
            return;
        }

        // Emit the 11-column row.
        TableRow row = rowBuilder.build(participantVersion, verdict.isTest());
        File tempDir = fileHelper.createTempDir();
        try {
            File parquet = fileHelper.newFile(tempDir, "participant_version.parquet");
            parquetRowWriter.write(row, parquet);
            exportStoreClient.stageVersionRow(healthCode, versionNum, parquet);
            ledgerStore.markVersion(healthCode, versionNum);
        } finally {
            try {
                fileHelper.deleteDirRecursively(tempDir);
            } catch (IOException ex) {
                LOG.error("Error deleting temp dir " + tempDir.getAbsolutePath() + " for app " + appId + " healthCode " +
                        healthCode + " version " + versionNum + ": " + ex.getMessage(), ex);
            }
        }
    }
}
