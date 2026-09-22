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

import org.sagebionetworks.bridge.addf.decrypt.DecryptedArchive;
import org.sagebionetworks.bridge.addf.decrypt.UploadFetcher;
import org.sagebionetworks.bridge.addf.gate.ConsentTestGate;
import org.sagebionetworks.bridge.addf.gate.ConsentVerdict;
import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.addf.transform.ClientInfo;
import org.sagebionetworks.bridge.addf.transform.FileRecordBuilder;
import org.sagebionetworks.bridge.addf.transform.FlattenContext;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.RecordFlattener;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerRetryableException;
import org.sagebionetworks.bridge.time.DateUtils;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

/**
 * ADDF §3.1 — the accumulate worker. Consumes one ADDF request, gates on consent (fail-closed), re-fetches and
 * independently decrypts the upload, shallow-flattens + summarises it into a typed row, stages that row plus the
 * always-emitted {@code file_records} manifest row as immutable per-record Parquet objects, copies the raw archive
 * into the export store's {@code raw/} layer, and marks the ledger. Runs as an isolated sibling of Exporter 3.0 —
 * never touching Synapse or the E3 code path.
 *
 * <p>Auto-registered by Spring name {@code "AddfExportWorker"} ({@code @ComponentScan("org.sagebionetworks.bridge")}),
 * dispatched by {@code BridgeWorkerPlatformSqsCallback} off the ADDF queue's poller (§3.8). No {@code SpringConfig}
 * edit.</p>
 */
@Component("AddfExportWorker")
public class AddfExportWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(AddfExportWorkerProcessor.class);

    private static final String CONNECTION_POOL_SHUTDOWN = "Connection pool shut down";

    private BridgeHelper bridgeHelper;
    private ConsentTestGate consentTestGate;
    private ExportStoreClient exportStoreClient;
    private FileHelper fileHelper;
    private FileRecordBuilder fileRecordBuilder;
    private LedgerStore ledgerStore;
    private ParquetRowWriter parquetRowWriter;
    private RecordFlattener recordFlattener;
    private UploadFetcher uploadFetcher;

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
    public final void setFileRecordBuilder(FileRecordBuilder fileRecordBuilder) {
        this.fileRecordBuilder = fileRecordBuilder;
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
    public final void setRecordFlattener(RecordFlattener recordFlattener) {
        this.recordFlattener = recordFlattener;
    }

    @Autowired
    public final void setUploadFetcher(UploadFetcher uploadFetcher) {
        this.uploadFetcher = uploadFetcher;
    }

    @Override
    public void accept(JsonNode jsonNode) throws IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, WorkerException {
        AddfExportRequest request;
        try {
            request = DefaultObjectMapper.INSTANCE.treeToValue(jsonNode, AddfExportRequest.class);
        } catch (IOException e) {
            throw new PollSqsWorkerBadRequestException("Error parsing ADDF request: " + e.getMessage(), e);
        }
        if (request.getAppId() == null || request.getRecordId() == null) {
            throw new PollSqsWorkerBadRequestException("ADDF request missing appId/recordId");
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            process(request);
        } catch (PollSqsWorkerBadRequestException | PollSqsWorkerRetryableException | WorkerException | IOException ex) {
            throw ex;
        } catch (IllegalStateException ex) {
            // S3 connection pool can be shut down transiently (credential refresh race). Treat as retryable.
            if (ex.getMessage() != null && ex.getMessage().contains(CONNECTION_POOL_SHUTDOWN)) {
                throw new PollSqsWorkerRetryableException(ex.getMessage(), ex);
            }
            throw new WorkerException(ex);
        } catch (RuntimeException ex) {
            throw new WorkerException(ex);
        } finally {
            LOG.info("ADDF export took " + stopwatch.elapsed(TimeUnit.SECONDS) + "s for app " + request.getAppId() +
                    " record " + request.getRecordId());
        }
    }

    // Package-scoped for unit tests.
    void process(AddfExportRequest request) throws IOException, PollSqsWorkerBadRequestException,
            PollSqsWorkerRetryableException, WorkerException {
        String appId = request.getAppId();
        String recordId = request.getRecordId();

        // Idempotency: presence-skip by record_id. The ledger key is the record id (not the health code), so a
        // redelivered demographics upload for a participant is NOT skipped by the *other* source's record — the
        // demographics merge-always semantics live at publish over the distinct staged partials (§3.6 / §3.7.2).
        if (ledgerStore.containsRecord(recordId)) {
            LOG.info("ADDF: record already processed, skipping: app " + appId + " record " + recordId);
            return;
        }

        // Belt-and-suspenders consent/test gate (fail-closed).
        HealthDataRecordEx3 record = bridgeHelper.getHealthDataRecordForExporter3(appId, recordId);
        if (record == null) {
            throw new PollSqsWorkerBadRequestException("No record for app " + appId + " record " + recordId);
        }
        String healthCode = record.getHealthCode();
        ConsentVerdict verdict = consentTestGate.evaluate(appId, healthCode);
        if (!verdict.isShouldExport()) {
            LOG.info("ADDF: consent/test gate skipped app " + appId + " record " + recordId);
            return;
        }

        App app = bridgeHelper.getApp(appId);

        File tempDir = fileHelper.createTempDir();
        try {
            DecryptedArchive archive = uploadFetcher.fetch(app, record, tempDir);

            // Capture-time participant version — read off the record, NOT a fresh "current" lookup (§3.5.3).
            Integer participantVersion = record.getParticipantVersion();
            if (participantVersion == null) {
                // Deviation note (§3.5.3 vs §3b.5): the plan prefers fail-closed on an unresolvable version. We log and
                // proceed with a null FK to avoid poison-looping a permanently version-less record; the Phase 7 /
                // §3b.5 referential-integrity gate at publish is the backstop that catches orphans.
                LOG.warn("ADDF: null participant_version for app " + appId + " record " + recordId +
                        "; writing null FK (referential-integrity gate is the backstop)");
            }

            String item = recordFlattener.resolveItem(archive);
            String uploadedOnUtc = AddfDateUtils.toUtcIso(record.getCreatedOn());
            ClientInfo clientInfo = ClientInfo.parse(record.getClientInfo());
            FlattenContext ctx = new FlattenContext(archive, clientInfo, participantVersion, verdict.isTest(),
                    uploadedOnUtc);

            // Content row (conditional — unknown types yield none, but still get a manifest row).
            TableRow contentRow = recordFlattener.flattenContent(ctx, item);
            if (contentRow != null) {
                File contentParquet = fileHelper.newFile(tempDir, "content.parquet");
                parquetRowWriter.write(contentRow, contentParquet);
                exportStoreClient.stageTableRow(contentRow.getTable(), recordId, contentParquet);
            }

            // Copy the raw archive verbatim, then build the manifest row referencing it.
            String uploadDate = AddfDateUtils.utcDate(record.getCreatedOn());
            String fileName = exportStoreClient.putRaw(uploadDate, recordId, item, archive.getDecryptedArchiveFile());

            String exportedOnUtc = AddfDateUtils.toUtcIso(DateUtils.getCurrentDateTime());
            TableRow manifestRow = fileRecordBuilder.build(ctx, item, fileName, exportedOnUtc);
            File manifestParquet = fileHelper.newFile(tempDir, "file_records.parquet");
            parquetRowWriter.write(manifestRow, manifestParquet);
            exportStoreClient.stageTableRow(manifestRow.getTable(), recordId, manifestParquet);

            // Mark done only after all writes succeed.
            ledgerStore.markRecord(recordId);
        } finally {
            try {
                fileHelper.deleteDirRecursively(tempDir);
            } catch (IOException ex) {
                LOG.error("Error deleting temp dir " + tempDir.getAbsolutePath() + " for app " + appId + " record " +
                        recordId + ": " + ex.getMessage(), ex);
            }
        }
    }
}
