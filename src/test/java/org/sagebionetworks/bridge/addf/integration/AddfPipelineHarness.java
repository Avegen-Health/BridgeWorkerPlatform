package org.sagebionetworks.bridge.addf.integration;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

import org.sagebionetworks.bridge.addf.AddfExportWorkerProcessor;
import org.sagebionetworks.bridge.addf.AddfParticipantVersionWorkerProcessor;
import org.sagebionetworks.bridge.addf.decrypt.UploadFetcher;
import org.sagebionetworks.bridge.addf.gate.ConsentTestGate;
import org.sagebionetworks.bridge.addf.publish.ManifestGate;
import org.sagebionetworks.bridge.addf.publish.ParquetTableReader;
import org.sagebionetworks.bridge.addf.publish.PublishLease;
import org.sagebionetworks.bridge.addf.publish.PublishMarker;
import org.sagebionetworks.bridge.addf.publish.RawArchiveDelivery;
import org.sagebionetworks.bridge.addf.publish.SnapshotDeltaBuilder;
import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.addf.transform.DemographicsBuilder;
import org.sagebionetworks.bridge.addf.transform.FileRecordBuilder;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.ParticipantVersionRowBuilder;
import org.sagebionetworks.bridge.addf.transform.RecordFlattener;
import org.sagebionetworks.bridge.addf.transform.SummaryComputer;
import org.sagebionetworks.bridge.addf.transform.ActivityRowBuilder;
import org.sagebionetworks.bridge.addf.transform.activity.EveningLogBuilder;
import org.sagebionetworks.bridge.addf.transform.activity.GoNoGoBuilder;
import org.sagebionetworks.bridge.addf.transform.activity.KeyboardSessionsBuilder;
import org.sagebionetworks.bridge.addf.transform.activity.Phq9Builder;
import org.sagebionetworks.bridge.addf.transform.activity.SelfRatingBuilder;
import org.sagebionetworks.bridge.addf.transform.activity.TrailMakingBuilder;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.ParticipantVersion;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

/**
 * Wires the whole ADDF pipeline the way Spring does — real transform stack, real {@code ExportStoreClient} and
 * {@code LedgerStore}, real Parquet writer and reader, real {@code SnapshotDeltaBuilder} and {@code ManifestGate} —
 * over an {@link InMemoryS3Client} and a real temp-dir {@link FileHelper}. Only the three genuine boundaries are
 * mocked: Bridge (records, participants, uploads), the upload bucket, and the CMS encryptor.
 *
 * <p>Everything an accumulate run writes really is serialised to Parquet bytes and stored under real keys, and
 * everything the publish run reads is parsed back out of those same bytes. That is what makes these integration tests
 * able to catch what the unit tests structurally cannot: a key written in one shape and listed in another, a
 * consolidated file clobbered instead of merged, or an orphan that survives to the delivery tree.</p>
 */
class AddfPipelineHarness {
    static final String APP_ID = "test-app";
    static final String BUCKET = "org-gvbridge-addf-exportstore-test";
    static final String UPLOAD_BUCKET = "org-sagebridge-upload-test";

    final InMemoryS3Client s3 = new InMemoryS3Client();
    final FileHelper fileHelper = new FileHelper();
    final BridgeHelper mockBridgeHelper = mock(BridgeHelper.class);
    final S3Helper mockS3Helper = mock(S3Helper.class);
    final Config config = mock(Config.class);

    final ExportStoreClient exportStoreClient = new ExportStoreClient();
    final LedgerStore ledgerStore = new LedgerStore();
    final ParquetRowWriter parquetRowWriter = new ParquetRowWriter();
    final ParquetTableReader parquetTableReader = new ParquetTableReader();
    final ConsentTestGate consentTestGate = new ConsentTestGate();
    final UploadFetcher uploadFetcher = new UploadFetcher();
    final RecordFlattener recordFlattener = new RecordFlattener();
    final SnapshotDeltaBuilder snapshotDeltaBuilder = new SnapshotDeltaBuilder();
    final ManifestGate manifestGate = new ManifestGate();
    final PublishMarker publishMarker = new PublishMarker();
    final PublishLease publishLease = new PublishLease();
    final RawArchiveDelivery rawArchiveDelivery = new RawArchiveDelivery();

    final AddfExportWorkerProcessor exportWorker = new AddfExportWorkerProcessor();
    final AddfParticipantVersionWorkerProcessor versionWorker = new AddfParticipantVersionWorkerProcessor();

    /** Archive bytes the fake upload bucket hands back, keyed by record id. */
    private final java.util.Map<String, byte[]> archivesByRecordId = new java.util.HashMap<>();
    private final List<File> tempDirs = new ArrayList<>();

    private final ZipHelper zipHelper = new ZipHelper();

    AddfPipelineHarness() throws Exception {
        // Config keys are referenced by their literal names rather than the package-private constants, because the
        // harness sits outside those packages. That is deliberate: these strings are the deployment contract, so
        // spelling them out here also asserts the .conf keys the infra sets are the ones the code reads.
        when(config.get("addf.exportstore.bucket")).thenReturn(BUCKET);
        when(config.get("upload.bucket")).thenReturn(UPLOAD_BUCKET);
        when(config.get("addf.export.include.test.users")).thenReturn("false");
        when(config.get("addf.export.enabled")).thenReturn("true");
        when(config.get("addf.publish.enabled")).thenReturn("true");
        when(config.get("addf.publish.gate.enforced")).thenReturn("true");
        when(config.get("addf.publish.raw.enabled")).thenReturn("true");
        when(config.get("addf.publish.raw.max.per.run")).thenReturn("500");

        zipHelper.setFileHelper(fileHelper);

        exportStoreClient.setBridgeConfig(config);
        exportStoreClient.setAddfS3Client(s3);
        ledgerStore.setBridgeConfig(config);
        ledgerStore.setAddfS3Client(s3);

        consentTestGate.setBridgeConfig(config);
        consentTestGate.setBridgeHelper(mockBridgeHelper);

        uploadFetcher.setBridgeConfig(config);
        uploadFetcher.setBridgeHelper(mockBridgeHelper);
        uploadFetcher.setCmsEncryptorCache(neverUsedEncryptorCache());
        uploadFetcher.setFileHelper(fileHelper);
        uploadFetcher.setS3Helper(mockS3Helper);
        uploadFetcher.setZipHelper(zipHelper);

        SummaryComputer summaryComputer = new SummaryComputer();
        KeyboardSessionsBuilder keyboardBuilder = new KeyboardSessionsBuilder();
        keyboardBuilder.setSummaryComputer(summaryComputer);
        GoNoGoBuilder goNoGoBuilder = new GoNoGoBuilder();
        goNoGoBuilder.setSummaryComputer(summaryComputer);
        recordFlattener.setActivityRowBuilders(ImmutableList.<ActivityRowBuilder>of(
                new Phq9Builder(), new SelfRatingBuilder(), new EveningLogBuilder(), new TrailMakingBuilder(),
                keyboardBuilder, goNoGoBuilder));
        recordFlattener.setDemographicsBuilder(new DemographicsBuilder());

        exportWorker.setBridgeHelper(mockBridgeHelper);
        exportWorker.setConsentTestGate(consentTestGate);
        exportWorker.setExportStoreClient(exportStoreClient);
        exportWorker.setFileHelper(fileHelper);
        exportWorker.setFileRecordBuilder(new FileRecordBuilder());
        exportWorker.setLedgerStore(ledgerStore);
        exportWorker.setParquetRowWriter(parquetRowWriter);
        exportWorker.setRecordFlattener(recordFlattener);
        exportWorker.setUploadFetcher(uploadFetcher);

        versionWorker.setBridgeHelper(mockBridgeHelper);
        versionWorker.setConsentTestGate(consentTestGate);
        versionWorker.setExportStoreClient(exportStoreClient);
        versionWorker.setFileHelper(fileHelper);
        versionWorker.setLedgerStore(ledgerStore);
        versionWorker.setParquetRowWriter(parquetRowWriter);
        versionWorker.setParticipantVersionRowBuilder(new ParticipantVersionRowBuilder());

        snapshotDeltaBuilder.setExportStoreClient(exportStoreClient);
        snapshotDeltaBuilder.setParquetRowWriter(parquetRowWriter);
        snapshotDeltaBuilder.setParquetTableReader(parquetTableReader);
        snapshotDeltaBuilder.setFileHelper(fileHelper);

        manifestGate.setBridgeConfig(config);
        manifestGate.setExportStoreClient(exportStoreClient);
        manifestGate.setParquetTableReader(parquetTableReader);
        manifestGate.setFileHelper(fileHelper);

        publishMarker.setBridgeConfig(config);
        publishMarker.setAddfS3Client(s3);

        publishLease.setBridgeConfig(config);
        publishLease.setAddfS3Client(s3);

        when(mockBridgeHelper.getApp(APP_ID)).thenReturn(new App().identifier(APP_ID));

        // The fake upload bucket serves whatever archive the test staged for that record id.
        doAnswer(invocation -> {
            String key = invocation.getArgumentAt(1, String.class);
            File dest = invocation.getArgumentAt(2, File.class);
            byte[] bytes = archivesByRecordId.get(key);
            if (bytes == null) {
                throw new IllegalStateException("no staged archive for record " + key);
            }
            Files.write(dest.toPath(), bytes);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), anyString(), any(File.class));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Fixture setup
    // -----------------------------------------------------------------------------------------------------------

    /** Register an upload: its Bridge record, its consent state, and the archive the upload bucket will serve. */
    void stageUpload(String recordId, String healthCode, Integer participantVersion, DateTime createdOn,
            String clientInfo, SharingScope scope, boolean testUser, Member... members) throws Exception {
        HealthDataRecordEx3 record = mock(HealthDataRecordEx3.class);
        when(record.getId()).thenReturn(recordId);
        when(record.getHealthCode()).thenReturn(healthCode);
        when(record.getParticipantVersion()).thenReturn(participantVersion);
        when(record.getCreatedOn()).thenReturn(createdOn);
        when(record.getClientInfo()).thenReturn(clientInfo);
        when(mockBridgeHelper.getHealthDataRecordForExporter3(APP_ID, recordId)).thenReturn(record);

        Upload upload = mock(Upload.class);
        when(upload.isEncrypted()).thenReturn(Boolean.FALSE); // decryption itself is covered by UploadFetcherTest
        when(mockBridgeHelper.getUploadByUploadId(recordId)).thenReturn(upload);

        StudyParticipant participant = new StudyParticipant().sharingScope(scope);
        if (testUser) {
            participant.setDataGroups(ImmutableList.of("test_user"));
        }
        when(mockBridgeHelper.getParticipantByHealthCode(APP_ID, healthCode, false)).thenReturn(participant);

        archivesByRecordId.put(recordId, zip(members));
    }

    /**
     * Same as {@link #stageUpload}, but serves a real de-identified archive from {@code src/test/resources} instead of
     * a synthetic one — so the snapshot tests run the genuine assessment payloads through the genuine flatteners.
     */
    void stageUploadFromResource(String recordId, String healthCode, Integer participantVersion, DateTime createdOn,
            String clientInfo, String resourcePath) throws Exception {
        stageUpload(recordId, healthCode, participantVersion, createdOn, clientInfo,
                SharingScope.SPONSORS_AND_PARTNERS, false);
        try (java.io.InputStream in = getClass().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("missing fixture: " + resourcePath);
            }
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
            archivesByRecordId.put(recordId, out.toByteArray());
        }
    }

    /** Register a participant version the dimension worker will fetch from Bridge. */
    void stageParticipantVersion(String healthCode, int versionNum, SharingScope scope, List<String> dataGroups)
            throws Exception {
        ParticipantVersion version = new ParticipantVersion();
        version.setHealthCode(healthCode);
        version.setParticipantVersion(versionNum);
        version.setSharingScope(scope);
        version.setDataGroups(dataGroups);
        version.setCreatedOn(DateTime.parse("2026-08-01T00:00:00.000Z"));
        version.setModifiedOn(DateTime.parse("2026-08-01T00:00:00.000Z"));
        when(mockBridgeHelper.getParticipantVersion(APP_ID, "healthCode:" + healthCode, versionNum))
                .thenReturn(version);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Driving the workers
    // -----------------------------------------------------------------------------------------------------------

    /**
     * Finish wiring the raw-archive delivery step. Split out because the Azure transport is the one true boundary the
     * test owns — everything else here is the real collaborator.
     */
    void wireRawDelivery(org.sagebionetworks.bridge.addf.azure.BlobTransport blobTransport) {
        rawArchiveDelivery.setBridgeConfig(config);
        rawArchiveDelivery.setExportStoreClient(exportStoreClient);
        rawArchiveDelivery.setLedgerStore(ledgerStore);
        rawArchiveDelivery.setBlobTransport(blobTransport);
        rawArchiveDelivery.setFileHelper(fileHelper);
    }

    /**
     * Drive the worker through its real SQS entry point rather than the package-scoped {@code process}, so the JSON
     * envelope parsing and the exception mapping are part of what is under test.
     */
    void runAccumulate(String recordId) throws Exception {
        exportWorker.accept(body("{\"appId\":\"" + APP_ID + "\",\"recordId\":\"" + recordId + "\"}"));
    }

    void runVersion(String healthCode, int versionNum) throws Exception {
        versionWorker.accept(body("{\"appId\":\"" + APP_ID + "\",\"healthCode\":\"" + healthCode
                + "\",\"participantVersion\":" + versionNum + "}"));
    }

    static JsonNode body(String json) throws IOException {
        return DefaultObjectMapper.INSTANCE.readTree(json);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------

    static final class Member {
        final String name;
        final String content;

        Member(String name, String content) {
            this.name = name;
            this.content = content;
        }
    }

    static Member member(String name, String content) {
        return new Member(name, content);
    }

    private byte[] zip(Member... members) throws Exception {
        File dir = fileHelper.createTempDir();
        tempDirs.add(dir);
        ImmutableList.Builder<File> files = ImmutableList.builder();
        for (Member member : members) {
            File file = fileHelper.newFile(dir, member.name);
            Files.write(file.toPath(), member.content.getBytes(Charsets.UTF_8));
            files.add(file);
        }
        File zipFile = fileHelper.newFile(dir, "archive.zip");
        zipHelper.zip(files.build(), zipFile);
        return Files.readAllBytes(zipFile.toPath());
    }

    void cleanUp() {
        for (File dir : tempDirs) {
            try {
                fileHelper.deleteDirRecursively(dir);
            } catch (IOException ignored) {
                // best effort
            }
        }
    }

    /**
     * The fixtures are all flagged unencrypted, so this cache is never consulted; it exists only so the fetcher is
     * fully wired. Decryption itself has its own dedicated coverage in {@code UploadFetcherTest}.
     */
    private static LoadingCache<String, CmsEncryptor> neverUsedEncryptorCache() {
        return CacheBuilder.newBuilder().build(new CacheLoader<String, CmsEncryptor>() {
            @Override
            public CmsEncryptor load(String key) {
                throw new UnsupportedOperationException("fixtures are unencrypted; decryption is UploadFetcherTest's");
            }
        });
    }
}
