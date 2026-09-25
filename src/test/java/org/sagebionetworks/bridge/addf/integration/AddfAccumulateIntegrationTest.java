package org.sagebionetworks.bridge.addf.integration;

import static org.sagebionetworks.bridge.addf.integration.AddfPipelineHarness.APP_ID;
import static org.sagebionetworks.bridge.addf.integration.AddfPipelineHarness.BUCKET;
import static org.sagebionetworks.bridge.addf.integration.AddfPipelineHarness.member;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.TableRow;
import org.sagebionetworks.bridge.rest.model.SharingScope;

/**
 * ADDF §7 integration — <b>full accumulate</b>: enqueue → fetch → flatten → export-store objects, driven through the
 * real workers over a real object store ({@link InMemoryS3Client}) with real Parquet serialisation. Covers all three
 * message kinds the plan names: an activity record, a {@code demographics} pair, and a participant-version message.
 *
 * <p>Unlike the mocked worker unit tests, nothing here asserts "the worker called method X". Every assertion reads
 * what actually landed in the store — the keys, and the rows parsed back out of the bytes.</p>
 */
public class AddfAccumulateIntegrationTest {
    private static final String STAGING = "biaffect-3/_staging/";
    private static final String LEDGER = "biaffect-3/_ledger/";
    private static final String CLIENT_INFO = "biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)";
    /** Shaped like the external IDs uat actually holds — a person's name, which is exactly why it is not delivered. */
    private static final String EXTERNAL_ID = "Jane_Doe1:biaffect-3-study";
    private static final DateTime CREATED_ON = DateTime.parse("2026-08-15T20:00:48.713Z");

    private AddfPipelineHarness harness;

    @BeforeMethod
    public void before() throws Exception {
        harness = new AddfPipelineHarness();
    }

    @AfterMethod
    public void after() {
        harness.cleanUp();
    }

    // -----------------------------------------------------------------------------------------------------------
    // Activity record
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void activityRecordStagesContentManifestAndRawAndMarksLedger() throws Exception {
        harness.stageUpload("rec-1", "hc-1", 2, CREATED_ON, CLIENT_INFO, SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"PHQ-9\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", phq9Answers()));

        harness.runAccumulate("rec-1");

        // Three writes per activity record: the content row, the file_records manifest row, and the raw archive.
        assertEquals(stagedKeys(AddfTables.PHQ9).size(), 1, keysForDiagnostics());
        assertEquals(stagedKeys(AddfTables.FILE_RECORDS).size(), 1, keysForDiagnostics());
        assertEquals(harness.s3.keysUnder(BUCKET, "biaffect-3/raw/").size(), 1, keysForDiagnostics());

        // The raw copy is keyed by upload date + record + assessment, which is the link file_records.file_name uses.
        assertTrue(harness.s3.exists(BUCKET, "biaffect-3/raw/2026-08-15/rec-1-PHQ-9.zip"), keysForDiagnostics());

        // The row really round-trips: read it back out of the Parquet bytes that were stored.
        TableRow phq9 = onlyStagedRow(AddfTables.PHQ9);
        assertEquals(phq9.get("record_id"), "rec-1");
        assertEquals(phq9.get("health_code"), "hc-1");
        assertEquals(phq9.get("participant_version"), 2L);
        assertEquals(phq9.get("total_score"), 5L);
        assertEquals(phq9.get("time_zone"), "-04:00");
        assertEquals(phq9.get("app_version"), "68");
        assertEquals(phq9.get("platform"), "ios");
        assertEquals(phq9.get("is_test"), Boolean.FALSE);

        TableRow manifest = onlyStagedRow(AddfTables.FILE_RECORDS);
        assertEquals(manifest.get("record_id"), "rec-1");
        assertEquals(manifest.get("item"), "PHQ-9");
        assertEquals(manifest.get("file_name"), "raw/2026-08-15/rec-1-PHQ-9.zip");

        // The device/OS columns must actually be populated. These were the seven columns that shipped null for
        // months because the JSON clientInfo was fed through the user-agent parser and matched nothing; asserting
        // them here means the regression cannot come back silently through the accumulate path.
        assertEquals(manifest.get("app_version"), "68");
        assertEquals(manifest.get("device_name"), "iPhone 11 Pro");
        assertEquals(manifest.get("os_version"), "26.5.2");
        // The JSON and the user agent genuinely disagree on os_name — Apple's identifier vs the marketing name — and
        // the JSON is the canonical source, so "iPhone OS" here is the proof that precedence is the right way round.
        assertEquals(manifest.get("os_name"), "iPhone OS");
        // ...while platform is normalised across both forms, so it stays a single stable token.
        assertEquals(phq9.get("platform"), "ios");

        // Ledger marked only after every write succeeded.
        assertTrue(harness.s3.exists(BUCKET, LEDGER + "record/rec-1"));
    }

    @Test
    public void redeliveredRecordIsPresenceSkipped() throws Exception {
        harness.stageUpload("rec-1", "hc-1", 2, CREATED_ON, CLIENT_INFO, SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"PHQ-9\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", phq9Answers()));
        harness.runAccumulate("rec-1");
        int putsAfterFirst = harness.s3.getPutCount();

        harness.runAccumulate("rec-1");

        // SQS redelivery is at-least-once; the second pass must write nothing at all.
        assertEquals(harness.s3.getPutCount(), putsAfterFirst, "redelivery must not re-write anything");
        assertEquals(stagedKeys(AddfTables.PHQ9).size(), 1);
    }

    @Test
    public void noSharingRecordWritesNothing() throws Exception {
        harness.stageUpload("rec-2", "hc-2", 2, CREATED_ON, CLIENT_INFO, SharingScope.NO_SHARING, false,
                member("info.json", "{\"item\":\"PHQ-9\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", phq9Answers()));

        harness.runAccumulate("rec-2");

        // Fail-closed all the way to the store: not even a raw copy or a manifest row for a withdrawn participant.
        assertTrue(harness.s3.keys(BUCKET).isEmpty(), keysForDiagnostics());
    }

    @Test
    public void unmappedAssessmentStillGetsAManifestRowAndRawCopy() throws Exception {
        // §3.5.2: file_records is written outside the type switch, so an assessment we cannot flatten is still
        // indexed and its archive still preserved — otherwise the record would vanish from the delivery entirely.
        harness.stageUpload("rec-3", "hc-1", 2, CREATED_ON, CLIENT_INFO, SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"Some-Future-Assessment\"}"),
                member("payload.json", "{\"whatever\":1}"));

        harness.runAccumulate("rec-3");

        assertEquals(stagedKeys(AddfTables.FILE_RECORDS).size(), 1);
        assertEquals(harness.s3.keysUnder(BUCKET, "biaffect-3/raw/").size(), 1);
        for (String table : AddfTables.allTables()) {
            if (!AddfTables.FILE_RECORDS.equals(table)) {
                assertTrue(stagedKeys(table).isEmpty(), table + " should have no staged row");
            }
        }
        assertTrue(harness.s3.exists(BUCKET, LEDGER + "record/rec-3"));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Demographics pair
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void demographicsPairStagesTwoPartialsForOneParticipant() throws Exception {
        // The two demographics uploads arrive as separate records, days apart. Each stages its own partial; neither
        // presence-skips the other, because the ledger is keyed by record id (§3.6).
        harness.stageUpload("rec-bg", "hc-1", 2, CREATED_ON, CLIENT_INFO, SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"birth-gender\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", "{\"birth\":1981,\"gender\":\"Female\"}"));
        harness.stageUpload("rec-dx", "hc-1", 2, CREATED_ON, CLIENT_INFO, SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"Diagnosis\",\"createdOn\":\"2026-08-15T16:01:28.909-04:00\"}"),
                member("answers.json", "{\"Bipolar diagnosis\":\"I have never been diagnosed with bipolar disorder\","
                        + "\"Other psych diagnoses\":[\"Anxiety\",\"Depression\"]}"));

        harness.runAccumulate("rec-bg");
        harness.runAccumulate("rec-dx");

        List<String> staged = stagedKeys(AddfTables.DEMOGRAPHICS);
        assertEquals(staged.size(), 2, "each upload stages its own immutable partial: " + staged);

        // Staged objects are named by record id even though demographics merges on health_code — that is what stops
        // the second upload clobbering the first before publish gets a chance to merge them.
        assertTrue(staged.get(0).endsWith("/rec-bg.parquet") || staged.get(1).endsWith("/rec-bg.parquet"), staged.toString());
        assertTrue(staged.get(0).endsWith("/rec-dx.parquet") || staged.get(1).endsWith("/rec-dx.parquet"), staged.toString());

        List<TableRow> rows = allStagedRows(AddfTables.DEMOGRAPHICS);
        TableRow birthGender = rowWithNonNull(rows, "birth_year");
        TableRow diagnosis = rowWithNonNull(rows, "bipolar_diagnosis");
        assertEquals(birthGender.get("birth_year"), 1981L);
        assertEquals(birthGender.get("gender"), "Female");
        assertNull(birthGender.get("bipolar_diagnosis"), "the birth-gender partial must not invent diagnosis columns");
        assertEquals(diagnosis.get("other_psych_diagnoses"), "Anxiety,Depression");
        assertNull(diagnosis.get("birth_year"), "the Diagnosis partial must not invent birth columns");
    }

    // -----------------------------------------------------------------------------------------------------------
    // Participant-version message
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void participantVersionMessageStagesTheDimensionRow() throws Exception {
        // The snapshot is enrolled with an external ID, the way a site-enrolled participant actually arrives.
        harness.stageParticipantVersion("hc-1", 2, SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.of("biaffect-3"),
                ImmutableMap.of("biaffect-3-study", EXTERNAL_ID));

        harness.runVersion("hc-1", 2);

        List<String> staged = stagedKeys(AddfTables.PARTICIPANT_VERSIONS);
        assertEquals(staged.size(), 1, staged.toString());
        assertTrue(staged.get(0).endsWith("/hc-1_2.parquet"), staged.get(0));

        TableRow row = onlyStagedRow(AddfTables.PARTICIPANT_VERSIONS);
        assertEquals(row.get("health_code"), "hc-1");
        assertEquals(row.get("participant_version"), 2L);
        assertEquals(row.get("sharing_scope"), "sponsors_and_partners");
        // The study survives — it comes from the membership key. The external ID (the value) does not, in any column.
        assertEquals(row.get("study_id"), "biaffect-3-study");
        assertFalse(row.getValues().values().contains(EXTERNAL_ID), row.getValues().toString());

        // Read the schema out of the Parquet footer the worker actually wrote, not out of our own column list: the
        // file that would reach ADDI must not declare the withheld columns at all.
        List<String> columns = harness.parquetTableReader.readColumnNames(stagedFile(staged.get(0)));
        assertEquals(columns, AddfTables.columnNames(AddfTables.PARTICIPANT_VERSIONS));
        for (String withheld : AddfTables.PII_WITHHELD_PARTICIPANT_FIELDS) {
            assertFalse(columns.contains(withheld), "staged dimension file declares withheld column " + withheld);
        }

        assertTrue(harness.s3.exists(BUCKET, LEDGER + "version/hc-1/2"));
    }

    @Test
    public void withdrawnVersionTombstonesInsteadOfExportingTheRow() throws Exception {
        // §3b.3: a NO_SHARING version must never be delivered as a data row. It leaves a tombstone for publish to
        // compact the participant out, and is ledger-marked so it is not reprocessed.
        harness.stageParticipantVersion("hc-9", 3, SharingScope.NO_SHARING, ImmutableList.<String>of());

        harness.runVersion("hc-9", 3);

        assertTrue(stagedKeys(AddfTables.PARTICIPANT_VERSIONS).isEmpty(), keysForDiagnostics());
        assertTrue(harness.s3.exists(BUCKET, "biaffect-3/_tombstone/hc-9"), keysForDiagnostics());
        assertTrue(harness.s3.exists(BUCKET, LEDGER + "version/hc-9/3"));
    }

    @Test
    public void redeliveredVersionIsVersionSkippedButANewVersionIsNot() throws Exception {
        harness.stageParticipantVersion("hc-1", 2, SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.<String>of());
        harness.stageParticipantVersion("hc-1", 3, SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.<String>of());

        harness.runVersion("hc-1", 2);
        int putsAfterFirst = harness.s3.getPutCount();
        harness.runVersion("hc-1", 2);
        assertEquals(harness.s3.getPutCount(), putsAfterFirst, "the same version must not be re-staged");

        harness.runVersion("hc-1", 3);
        assertEquals(stagedKeys(AddfTables.PARTICIPANT_VERSIONS).size(), 2,
                "a new version is a new row, not a skip: " + stagedKeys(AddfTables.PARTICIPANT_VERSIONS));
    }

    // -----------------------------------------------------------------------------------------------------------
    // All three together
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void allThreeMessageKindsPopulateTheStoreIndependently() throws Exception {
        harness.stageParticipantVersion("hc-1", 2, SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.<String>of());
        harness.stageUpload("rec-1", "hc-1", 2, CREATED_ON, CLIENT_INFO, SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"PHQ-9\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", phq9Answers()));
        harness.stageUpload("rec-bg", "hc-1", 2, CREATED_ON, CLIENT_INFO, SharingScope.SPONSORS_AND_PARTNERS, false,
                member("info.json", "{\"item\":\"birth-gender\",\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}"),
                member("answers.json", "{\"birth\":1981,\"gender\":\"Female\"}"));

        harness.runVersion("hc-1", 2);
        harness.runAccumulate("rec-1");
        harness.runAccumulate("rec-bg");

        assertEquals(stagedKeys(AddfTables.PARTICIPANT_VERSIONS).size(), 1);
        assertEquals(stagedKeys(AddfTables.PHQ9).size(), 1);
        assertEquals(stagedKeys(AddfTables.DEMOGRAPHICS).size(), 1);
        assertEquals(stagedKeys(AddfTables.FILE_RECORDS).size(), 2, "one manifest row per upload, not per assessment");

        // Nothing consolidated yet — accumulate never writes a delivery file, that is publish's sole job (§3.7.1).
        assertTrue(harness.s3.keysUnder(BUCKET, "biaffect-3/current/").isEmpty(), keysForDiagnostics());
    }

    // -----------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------

    private List<String> stagedKeys(String table) {
        return harness.s3.keysUnder(BUCKET, STAGING + table + "/");
    }

    private List<TableRow> allStagedRows(String table) throws Exception {
        List<TableRow> rows = new ArrayList<>();
        for (String key : stagedKeys(table)) {
            rows.addAll(harness.parquetTableReader.read(table, stagedFile(key)));
        }
        return rows;
    }

    /** The staged object's bytes as a local file, so a test can read the Parquet footer as well as the rows. */
    private File stagedFile(String key) throws Exception {
        File local = File.createTempFile("addf-staged", ".parquet");
        local.deleteOnExit();
        java.nio.file.Files.write(local.toPath(), harness.s3.get(BUCKET, key));
        return local;
    }

    private TableRow onlyStagedRow(String table) throws Exception {
        List<TableRow> rows = allStagedRows(table);
        assertEquals(rows.size(), 1, "expected exactly one staged " + table + " row, got " + rows.size());
        return rows.get(0);
    }

    private static TableRow rowWithNonNull(List<TableRow> rows, String column) {
        for (TableRow row : rows) {
            if (row.get(column) != null) {
                return row;
            }
        }
        assertFalse(true, "no row carried a non-null " + column);
        return null;
    }

    private String keysForDiagnostics() {
        return "store keys: " + harness.s3.keys(BUCKET);
    }

    private static String phq9Answers() {
        // The label->code mapping is the builder's; total_score is the sum of the 8 symptom items (not `difficulty`).
        return "{"
                + "\"anhedonia\":\"Not at all\","
                + "\"depression\":\"Not at all\","
                + "\"sleep\":\"Several days\","
                + "\"energy\":\"Several days\","
                + "\"appetite\":\"Several days\","
                + "\"discouragement\":\"Several days\","
                + "\"concentration\":\"Several days\","
                + "\"speed\":\"Not at all\","
                + "\"difficulty\":\"Not difficult at all\""
                + "}";
    }

    /** Guard so the assertNotNull import stays meaningful if the suite is trimmed. */
    @Test
    public void harnessIsWiredWithRealCollaborators() {
        assertNotNull(harness.exportStoreClient);
        assertNotNull(harness.parquetRowWriter);
        assertNotNull(harness.snapshotDeltaBuilder);
    }
}
