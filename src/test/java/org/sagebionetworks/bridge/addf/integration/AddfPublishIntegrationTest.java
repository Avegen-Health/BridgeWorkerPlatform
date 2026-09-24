package org.sagebionetworks.bridge.addf.integration;

import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.addf.integration.AddfPipelineHarness.BUCKET;
import static org.sagebionetworks.bridge.addf.integration.AddfPipelineHarness.member;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.AddfPublishWorkerProcessor;
import org.sagebionetworks.bridge.addf.azure.BlobTransport;
import org.sagebionetworks.bridge.addf.publish.ManifestGateException;
import org.sagebionetworks.bridge.addf.publish.PublishedBlob;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.rest.model.SharingScope;

/**
 * ADDF §7 integration — the <b>publish</b> half, run over the same real store and real Parquet as the accumulate
 * tests. Two things the plan asks for live here:
 *
 * <ul>
 *   <li><b>All 10 tables land, and the joins resolve.</b> A full snapshot is accumulated from the real de-identified
 *       archives, published, and then read back <em>out of the delivery tree</em> — every table present, every
 *       activity row's {@code (health_code, participant_version)} resolving into {@code participant_versions}, and
 *       {@code file_records} linking every row to an archive that actually exists under {@code raw/}. This is the
 *       in-process form of the dev/uat E2E row; the environment version still has to confirm the Azure hop.</li>
 *   <li><b>Crash-safety.</b> The upload is failed mid-flight and the day replayed, asserting staging survives, the
 *       marker is never written early, the replay rebuilds an identical delta, and a third delivery of the same
 *       message is a no-op. This is the "kill mid-copy, re-run, no double-publish and no skip" row.</li>
 * </ul>
 */
public class AddfPublishIntegrationTest {
    private static final String SNAPSHOT_DATE = "2026-09-24";
    private static final String CLIENT_INFO = "biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)";
    private static final DateTime CREATED_ON = DateTime.parse("2026-08-15T20:00:48.713Z");
    private static final String HEALTH_CODE = "hc-EXAMPLE-01";
    private static final int VERSION = 2;

    private static final String CURRENT_TABLES = "biaffect-3/current/tables/";
    private static final String STAGING = "biaffect-3/_staging/";

    private AddfPipelineHarness harness;
    private BlobTransport mockTransport;
    private AddfPublishWorkerProcessor publishWorker;

    /** Every blob key handed to the transport, across all runs, so double-uploads are visible. */
    private final List<String> uploadedKeys = new ArrayList<>();

    @BeforeMethod
    public void before() throws Exception {
        harness = new AddfPipelineHarness();
        mockTransport = mock(BlobTransport.class);
        uploadedKeys.clear();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            List<PublishedBlob> blobs = (List<PublishedBlob>) invocation.getArguments()[0];
            for (PublishedBlob blob : blobs) {
                uploadedKeys.add(blob.getKey());
            }
            return null;
        }).when(mockTransport).upload(anyListOf(PublishedBlob.class));

        publishWorker = new AddfPublishWorkerProcessor();
        publishWorker.setBridgeConfig(harness.config);
        publishWorker.setSnapshotDeltaBuilder(harness.snapshotDeltaBuilder);
        publishWorker.setManifestGate(harness.manifestGate);
        publishWorker.setBlobTransport(mockTransport);
        publishWorker.setPublishMarker(harness.publishMarker);
        publishWorker.setFileHelper(harness.fileHelper);
    }

    @AfterMethod
    public void after() {
        harness.cleanUp();
    }

    // -----------------------------------------------------------------------------------------------------------
    // Full snapshot: all 10 tables + joins
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void fullSnapshotDeliversAllTenTablesWithResolvingJoins() throws Exception {
        accumulateFullSnapshot();

        runPublish();

        // 1) Every one of the 10 contract tables is present in the delivery tree.
        for (String table : AddfTables.allTables()) {
            if (AddfTables.KEYBOARD_SESSIONS.equals(table)) {
                assertFalse(harness.s3.keysUnder(BUCKET, "biaffect-3/keyboard_sessions/month=").isEmpty(),
                        "no keyboard month part in the delivery tree: " + deliveryTree());
            } else {
                assertTrue(harness.s3.exists(BUCKET, CURRENT_TABLES + table + ".parquet"),
                        table + " missing from the delivery tree: " + deliveryTree());
            }
        }

        // 2) The manifest gate passed, so the upload happened and the day was marked done.
        assertFalse(uploadedKeys.isEmpty(), "nothing was uploaded");
        assertTrue(harness.s3.exists(BUCKET, "biaffect-3/_publish/" + SNAPSHOT_DATE + ".done"));

        // 3) Joins resolve: every activity/file_records row's FK exists in participant_versions.
        Set<String> validKeys = new LinkedHashSet<>();
        for (TableRow row : delivered(AddfTables.PARTICIPANT_VERSIONS)) {
            validKeys.add(row.get("health_code") + "/" + row.get("participant_version"));
        }
        assertFalse(validKeys.isEmpty(), "participant_versions delivered empty");

        int checked = 0;
        for (String table : ImmutableList.of(AddfTables.PHQ9, AddfTables.SELF_RATING, AddfTables.EVENING_LOG,
                AddfTables.GO_NO_GO, AddfTables.TRAIL_MAKING, AddfTables.FILE_RECORDS)) {
            for (TableRow row : delivered(table)) {
                Object version = row.get("participant_version");
                if (version == null) {
                    continue;
                }
                String key = row.get("health_code") + "/" + version;
                assertTrue(validKeys.contains(key),
                        table + " row " + row.get("record_id") + " has an unresolvable FK " + key);
                checked++;
            }
        }
        assertTrue(checked >= 6, "expected at least one row per activity table to join, checked " + checked);

        // 4) participants_current is the latest version per participant, and shares the dimension's schema.
        List<TableRow> current = delivered(AddfTables.PARTICIPANTS_CURRENT);
        assertEquals(current.size(), 1);
        assertEquals(current.get(0).get("health_code"), HEALTH_CODE);
        assertEquals(current.get(0).get("participant_version"), (long) VERSION);

        // 5) file_records links every delivered row to an archive that really exists under raw/.
        List<TableRow> fileRecords = delivered(AddfTables.FILE_RECORDS);
        assertEquals(fileRecords.size(), 8, "one manifest row per upload");
        for (TableRow row : fileRecords) {
            String fileName = (String) row.get("file_name");
            assertTrue(harness.s3.exists(BUCKET, "biaffect-3/" + fileName),
                    "file_records points at a missing archive: " + fileName);
        }

        // 6) demographics merged the pair into a single complete row.
        List<TableRow> demographics = delivered(AddfTables.DEMOGRAPHICS);
        assertEquals(demographics.size(), 1, "the two demographics uploads must merge into one participant row");
        assertEquals(demographics.get(0).get("birth_year"), 1981L);
        assertEquals(demographics.get(0).get("bipolar_diagnosis"), "Never");

        // 7) Staging was consumed by the commit, so the next snapshot starts clean.
        assertTrue(harness.s3.keysUnder(BUCKET, STAGING).isEmpty(),
                "staging should be retired after a confirmed upload: " + harness.s3.keysUnder(BUCKET, STAGING));
    }

    @Test
    public void snapshotMissingATableIsBlockedBeforeAnythingIsUploaded() throws Exception {
        // Only a participant version and one activity record — seven tables never get a producer. This is the
        // original defect the gate exists for, reproduced end to end.
        harness.stageParticipantVersion(HEALTH_CODE, VERSION, SharingScope.SPONSORS_AND_PARTNERS,
                ImmutableList.<String>of());
        harness.runVersion(HEALTH_CODE, VERSION);
        stagePhq9("rec-phq9");
        harness.runAccumulate("rec-phq9");

        try {
            runPublish();
            fail("expected the manifest gate to block the snapshot");
        } catch (ManifestGateException expected) {
            assertTrue(expected.getMessage().contains("absent from the delivery tree"), expected.getMessage());
        }

        // Nothing reached ADDI, and the day is not claimed as done.
        verify(mockTransport, never()).upload(anyListOf(PublishedBlob.class));
        assertFalse(harness.s3.exists(BUCKET, "biaffect-3/_publish/" + SNAPSHOT_DATE + ".done"));
        // Staging survives, so the day replays once the missing producers land.
        assertFalse(harness.s3.keysUnder(BUCKET, STAGING).isEmpty(), "staging must not be consumed by a blocked run");
    }

    // -----------------------------------------------------------------------------------------------------------
    // Crash-safety: kill mid-copy, re-run, no double-publish and no skip
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void uploadFailureReplaysTheIdenticalSnapshotWithoutDoublePublishingOrSkipping() throws Exception {
        accumulateFullSnapshot();
        List<String> stagingBeforeFirstRun = harness.s3.keysUnder(BUCKET, STAGING);
        assertFalse(stagingBeforeFirstRun.isEmpty());

        // --- Run 1: the Azure copy dies mid-flight. ---
        doThrow(new RuntimeException("connection reset mid-copy")).when(mockTransport)
                .upload(anyListOf(PublishedBlob.class));
        try {
            runPublish();
            fail("expected the upload failure to surface");
        } catch (Exception expected) {
            // Mapped to a WorkerException by accept(); the message is redelivered.
        }

        // Staging and tombstones survive untouched, and the day is NOT marked done — otherwise the snapshot would be
        // silently missing from the Azure mirror forever.
        assertEquals(harness.s3.keysUnder(BUCKET, STAGING), stagingBeforeFirstRun,
                "a failed upload must not consume staging");
        assertFalse(harness.s3.exists(BUCKET, "biaffect-3/_publish/" + SNAPSHOT_DATE + ".done"),
                "a failed upload must not write the done-marker");
        assertTrue(uploadedKeys.isEmpty());

        // --- Run 2: the transport recovers. The replay must rebuild the identical delta. ---
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            List<PublishedBlob> blobs = (List<PublishedBlob>) invocation.getArguments()[0];
            for (PublishedBlob blob : blobs) {
                uploadedKeys.add(blob.getKey());
            }
            return null;
        }).when(mockTransport).upload(anyListOf(PublishedBlob.class));

        runPublish();

        List<String> firstSuccessfulUpload = new ArrayList<>(uploadedKeys);
        assertFalse(firstSuccessfulUpload.isEmpty(), "the replay uploaded nothing");
        assertTrue(harness.s3.exists(BUCKET, "biaffect-3/_publish/" + SNAPSHOT_DATE + ".done"));
        assertTrue(harness.s3.keysUnder(BUCKET, STAGING).isEmpty(), "the confirmed upload must retire staging");

        // The replay delivered a complete snapshot, not a partial one salvaged from the crashed run.
        for (String table : AddfTables.allTables()) {
            if (!AddfTables.KEYBOARD_SESSIONS.equals(table)) {
                assertTrue(harness.s3.exists(BUCKET, CURRENT_TABLES + table + ".parquet"), table + " missing");
            }
        }

        // --- Run 3: SQS redelivers the same scheduled message. The marker must short-circuit it. ---
        uploadedKeys.clear();
        runPublish();
        assertTrue(uploadedKeys.isEmpty(), "a duplicate delivery must not re-upload: " + uploadedKeys);
        // And no row was duplicated in the delivery tree.
        assertEquals(delivered(AddfTables.PHQ9).size(), 1);
        assertEquals(delivered(AddfTables.FILE_RECORDS).size(), 8);
    }

    @Test
    public void lateArrivingRecordIsPickedUpByTheNextSnapshotNotLost() throws Exception {
        // §4.3.3: the cutoff is the staging listing, not the snapshot label. A record that lands after one publish
        // must be delivered by the next one — exactly once, and without disturbing what was already delivered.
        accumulateFullSnapshot();
        runPublish();
        int phq9RowsAfterFirst = delivered(AddfTables.PHQ9).size();

        stagePhq9("rec-phq9-late");
        harness.runAccumulate("rec-phq9-late");
        runPublishFor("2026-09-25");

        List<TableRow> phq9 = delivered(AddfTables.PHQ9);
        assertEquals(phq9.size(), phq9RowsAfterFirst + 1, "the late record must be added, not replace the earlier one");
        Set<String> recordIds = new LinkedHashSet<>();
        for (TableRow row : phq9) {
            recordIds.add((String) row.get("record_id"));
        }
        assertTrue(recordIds.contains("rec-phq9"), recordIds.toString());
        assertTrue(recordIds.contains("rec-phq9-late"), recordIds.toString());
    }

    @Test
    public void orphanRowIsDeferredThenDeliveredOnceItsVersionLands() throws Exception {
        // §3b.5, end to end: an activity record whose participant_version has not been backfilled yet is held back
        // rather than delivered with a dangling FK, and it is not lost — the next snapshot picks it up.
        accumulateFullSnapshot();
        harness.stageUpload("rec-orphan", "hc-LATE", 7, CREATED_ON, CLIENT_INFO,
                SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"PHQ-9\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", phq9Answers()));
        harness.runAccumulate("rec-orphan");

        runPublish();

        for (TableRow row : delivered(AddfTables.PHQ9)) {
            assertFalse("rec-orphan".equals(row.get("record_id")), "the orphan must not reach the delivery tree");
        }
        // Deferred, not dropped: its staged object is still there for the next snapshot.
        assertFalse(harness.s3.keysUnder(BUCKET, STAGING + AddfTables.PHQ9 + "/").isEmpty(),
                "the orphan's staged object must survive the commit");

        // Its version lands, and the next snapshot delivers it.
        harness.stageParticipantVersion("hc-LATE", 7, SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.<String>of());
        harness.runVersion("hc-LATE", 7);
        runPublishFor("2026-09-25");

        boolean delivered = false;
        for (TableRow row : delivered(AddfTables.PHQ9)) {
            delivered |= "rec-orphan".equals(row.get("record_id"));
        }
        assertTrue(delivered, "the deferred row must be delivered once its participant_version exists");
    }

    // -----------------------------------------------------------------------------------------------------------
    // Fixture: a snapshot that populates all 10 tables
    // -----------------------------------------------------------------------------------------------------------

    private void accumulateFullSnapshot() throws Exception {
        harness.stageParticipantVersion(HEALTH_CODE, VERSION, SharingScope.SPONSORS_AND_PARTNERS,
                ImmutableList.<String>of());
        harness.runVersion(HEALTH_CODE, VERSION);

        // Five real de-identified archives, straight from the golden fixtures.
        String raw = "/addf/golden/raw/";
        harness.stageUploadFromResource("rec-phq9", HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                raw + "rec-EXAMPLE-0007-PHQ-9.zip");
        harness.stageUploadFromResource("rec-daily", HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                raw + "rec-EXAMPLE-0016-daily.zip");
        harness.stageUploadFromResource("rec-evening", HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                raw + "rec-EXAMPLE-0024-Evening_Log.zip");
        harness.stageUploadFromResource("rec-trail", HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                raw + "rec-EXAMPLE-0040-Trail_Making.zip");
        harness.stageUploadFromResource("rec-keyboard", HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                raw + "rec-EXAMPLE-0084-KeyboardSession.zip");

        // Go/No-Go is synthetic: its real archives are 7-8 MB of motion.json and are deliberately not committed.
        harness.stageUpload("rec-gonogo", HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"Go-No-Go\"}"),
                member("gonogo.json", "{\"identifier\":\"gng\",\"startDate\":\"2026-08-15T10:30:00.000-04:00\","
                        + "\"results\":[{\"go\":true,\"incorrect\":false,\"timeToThreshold\":0.5,"
                        + "\"samples\":[{},{}]},{\"go\":false,\"incorrect\":true}]}"));

        // The demographics pair.
        harness.stageUpload("rec-bg", HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"birth-gender\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", "{\"birth\":1981,\"gender\":\"Female\"}"));
        harness.stageUpload("rec-dx", HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"Diagnosis\",\"createdOn\":\"2026-08-15T16:01:28.909-04:00\"}"),
                member("answers.json", "{\"Bipolar diagnosis\":\"Never\"}"));

        for (String recordId : ImmutableList.of("rec-phq9", "rec-daily", "rec-evening", "rec-trail", "rec-keyboard",
                "rec-gonogo", "rec-bg", "rec-dx")) {
            harness.runAccumulate(recordId);
        }
    }

    private void stagePhq9(String recordId) throws Exception {
        harness.stageUpload(recordId, HEALTH_CODE, VERSION, CREATED_ON, CLIENT_INFO,
                SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"PHQ-9\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", phq9Answers()));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------

    private void runPublish() throws Exception {
        runPublishFor(SNAPSHOT_DATE);
    }

    private void runPublishFor(String snapshotDate) throws Exception {
        publishWorker.accept(AddfPipelineHarness.body("{\"snapshotDate\":\"" + snapshotDate + "\"}"));
    }

    /** Read a delivered table back out of the export store, through the real Parquet reader. */
    private List<TableRow> delivered(String table) throws Exception {
        List<TableRow> rows = new ArrayList<>();
        if (AddfTables.KEYBOARD_SESSIONS.equals(table)) {
            for (String key : harness.s3.keysUnder(BUCKET, "biaffect-3/keyboard_sessions/")) {
                rows.addAll(readKey(table, key));
            }
            return rows;
        }
        String key = CURRENT_TABLES + table + ".parquet";
        if (!harness.s3.exists(BUCKET, key)) {
            return rows;
        }
        return readKey(table, key);
    }

    private List<TableRow> readKey(String table, String key) throws Exception {
        File local = File.createTempFile("addf-delivered", ".parquet");
        local.deleteOnExit();
        Files.write(local.toPath(), harness.s3.get(BUCKET, key));
        return harness.parquetTableReader.read(table, local);
    }

    private String deliveryTree() {
        return harness.s3.keysUnder(BUCKET, "biaffect-3/").toString();
    }

    private static String phq9Answers() {
        return "{\"anhedonia\":\"Not at all\",\"depression\":\"Not at all\",\"sleep\":\"Several days\","
                + "\"energy\":\"Several days\",\"appetite\":\"Several days\",\"discouragement\":\"Several days\","
                + "\"concentration\":\"Several days\",\"speed\":\"Not at all\","
                + "\"difficulty\":\"Not difficult at all\"}";
    }

    /** Sanity guard on the fixture itself: if the golden archives move, these tests must fail loudly, not silently. */
    @Test
    public void goldenArchiveFixturesArePresent() {
        for (String name : ImmutableList.of("rec-EXAMPLE-0007-PHQ-9.zip", "rec-EXAMPLE-0016-daily.zip",
                "rec-EXAMPLE-0024-Evening_Log.zip", "rec-EXAMPLE-0040-Trail_Making.zip",
                "rec-EXAMPLE-0084-KeyboardSession.zip")) {
            assertTrue(getClass().getResourceAsStream("/addf/golden/raw/" + name) != null, "missing fixture " + name);
        }
    }
}
