package org.sagebionetworks.bridge.addf.transform;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.decrypt.DecryptedArchive;
import org.sagebionetworks.bridge.addf.publish.ParquetTableReader;
import org.sagebionetworks.bridge.addf.transform.activity.EveningLogBuilder;
import org.sagebionetworks.bridge.addf.transform.activity.KeyboardSessionsBuilder;
import org.sagebionetworks.bridge.addf.transform.activity.Phq9Builder;
import org.sagebionetworks.bridge.addf.transform.activity.SelfRatingBuilder;
import org.sagebionetworks.bridge.addf.transform.activity.TrailMakingBuilder;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;

/**
 * ADDF §7 — the <b>golden-file test</b>. Runs the real flatteners over real de-identified upload archives and compares
 * each produced row against the corresponding row of the delivered Parquet tables.
 *
 * <p><b>Logical, never byte.</b> The golden files were written by the reference pyarrow pipeline; our writer (§3.9,
 * parquet-avro) differs in compression, dictionary encoding, logical-type physical representation and footer metadata.
 * A byte or hash comparison fails regardless of correctness, so the goldens are read back through
 * {@link ParquetTableReader} and compared as column names + values.</p>
 *
 * <p><b>What is and isn't asserted.</b> Every column is checked except the ones sourced from the Bridge record rather
 * than from the archive ({@link #RECORD_SOURCED_COLUMNS}) — the de-identified fixtures carry no {@code clientInfo},
 * upload timestamp or consent verdict, so those are supplied as fixed inputs and there is nothing to compare. What is
 * left is exactly the flattener/summariser output: capture timestamps, the recovered {@code time_zone} offset, coded
 * survey answers, computed summaries, and the verbatim JSON columns.</p>
 *
 * <p><b>Known gap: {@code go_no_go}.</b> Its archives are 7-8 MB each because they carry the raw {@code motion.json}
 * accelerometer stream, and committing four of them would add ~28 MB to the repo for one builder. {@code GoNoGoBuilder}
 * is covered by {@code GoNoGoBuilderTest} against synthetic fixtures instead; its {@code motion_sample_count} /
 * {@code mean_reaction_time} derivations are therefore <b>not</b> golden-verified. {@code demographics} is also absent
 * here — its two partials are merged at publish, so it is covered by {@code SnapshotDeltaBuilderTest}.</p>
 */
public class GoldenFlattenerTest {
    private static final String RAW_PREFIX = "/addf/golden/raw/";
    private static final String TABLES_PREFIX = "/addf/golden/tables/";

    /**
     * Columns that come from the {@code HealthDataRecordEx3} / consent gate, not from the archive. The fixtures are
     * de-identified exports and carry none of that, so these are supplied as inputs and excluded from the comparison.
     */
    private static final java.util.Set<String> RECORD_SOURCED_COLUMNS = ImmutableSet.of(
            "record_id", "health_code", "participant_version", "app_version", "platform", "device_name",
            "uploaded_on", "is_test");

    /** Decimal columns are compared with a tolerance; everything else must be exactly equal. */
    private static final double EPSILON = 1e-6;

    private static final Integer PARTICIPANT_VERSION = 2;
    private static final String UPLOADED_ON = "2026-08-15T20:00:48.713Z";
    private static final String CLIENT_INFO = "biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)";

    private RecordFlattener flattener;
    private ParquetTableReader parquetTableReader;
    private File tempDir;

    @BeforeClass
    public void before() throws Exception {
        SummaryComputer summaryComputer = new SummaryComputer();
        KeyboardSessionsBuilder keyboard = new KeyboardSessionsBuilder();
        keyboard.setSummaryComputer(summaryComputer);

        flattener = new RecordFlattener();
        flattener.setActivityRowBuilders(ImmutableList.<ActivityRowBuilder>of(
                new Phq9Builder(), new SelfRatingBuilder(), new EveningLogBuilder(), new TrailMakingBuilder(),
                keyboard));
        flattener.setDemographicsBuilder(new DemographicsBuilder());

        parquetTableReader = new ParquetTableReader();
        tempDir = Files.createTempDirectory("addf-golden").toFile();
    }

    @AfterClass
    public void after() {
        deleteRecursively(tempDir);
    }

    /** One row per shipped fixture: the archive file name, its record id, and the table it flattens into. */
    @DataProvider(name = "fixtures")
    public Object[][] fixtures() {
        return new Object[][] {
                { "rec-EXAMPLE-0007-PHQ-9.zip", "rec-EXAMPLE-0007", AddfTables.PHQ9 },
                { "rec-EXAMPLE-0016-daily.zip", "rec-EXAMPLE-0016", AddfTables.SELF_RATING },
                { "rec-EXAMPLE-0024-Evening_Log.zip", "rec-EXAMPLE-0024", AddfTables.EVENING_LOG },
                { "rec-EXAMPLE-0040-Trail_Making.zip", "rec-EXAMPLE-0040", AddfTables.TRAIL_MAKING },
                { "rec-EXAMPLE-0084-KeyboardSession.zip", "rec-EXAMPLE-0084", AddfTables.KEYBOARD_SESSIONS },
        };
    }

    @Test(dataProvider = "fixtures")
    public void flattenedRowMatchesTheDeliveredGolden(String archiveName, String recordId, String table)
            throws Exception {
        TableRow golden = goldenRow(table, recordId);
        assertNotNull(golden, "no golden row for " + recordId + " in " + table);

        DecryptedArchive archive = openArchive(archiveName, recordId, (String) golden.get("health_code"));
        String item = flattener.resolveItem(archive);
        assertNotNull(item, "could not resolve the assessment item for " + archiveName);

        FlattenContext ctx = new FlattenContext(archive, ClientInfo.parse(CLIENT_INFO), PARTICIPANT_VERSION, false,
                UPLOADED_ON);
        TableRow actual = flattener.flattenContent(ctx, item);
        assertNotNull(actual, "no content row built for " + archiveName + " (item=" + item + ")");
        assertEquals(actual.getTable(), table);

        // Logical schema: the row carries exactly the contract's columns, no more and no fewer. Compared as a set —
        // builders populate the shared columns first and their own after, while ParquetRowWriter emits in AddfTables
        // order, so a TableRow's insertion order is not the delivered column order. (That ordering is asserted
        // end-to-end by the manifest gate, against the written file's own footer.)
        assertEquals(new java.util.TreeSet<>(actual.getValues().keySet()),
                new java.util.TreeSet<>(AddfTables.columnNames(table)),
                table + " row must carry exactly the contract's columns");

        List<String> mismatches = new ArrayList<>();
        for (Column column : AddfTables.columnsFor(table)) {
            String name = column.getName();
            if (RECORD_SOURCED_COLUMNS.contains(name)) {
                continue;
            }
            if (!valuesMatch(column, golden.get(name), actual.get(name))) {
                mismatches.add(name + ": golden=" + render(golden.get(name)) + " actual=" + render(actual.get(name)));
            }
        }
        assertTrue(mismatches.isEmpty(), table + " (" + recordId + ") differs from the delivered golden: " + mismatches);
    }

    @Test
    public void everyGoldenTableReadsBackWithTheContractedSchema() throws Exception {
        // The goldens themselves are the FAIR contract made concrete. Reading each one back confirms the tables we are
        // asked to reproduce really do carry the columns AddfTables declares — if this fails, the fixtures are stale.
        for (String table : ImmutableList.of(AddfTables.PHQ9, AddfTables.SELF_RATING, AddfTables.EVENING_LOG,
                AddfTables.GO_NO_GO, AddfTables.TRAIL_MAKING, AddfTables.DEMOGRAPHICS,
                AddfTables.PARTICIPANT_VERSIONS, AddfTables.PARTICIPANTS_CURRENT, AddfTables.FILE_RECORDS)) {
            File file = resourceToFile(TABLES_PREFIX + table + ".parquet", table + ".parquet");
            assertEquals(withoutWithheld(parquetTableReader.readColumnNames(file)), AddfTables.columnNames(table),
                    "golden " + table + ".parquet schema");
        }
        File keyboard = resourceToFile(TABLES_PREFIX + "keyboard_sessions/month=2026-08/part-0001.parquet",
                "keyboard-part.parquet");
        assertEquals(parquetTableReader.readColumnNames(keyboard),
                AddfTables.columnNames(AddfTables.KEYBOARD_SESSIONS), "golden keyboard part schema");
    }

    /**
     * A golden's columns minus the ones we withhold. The goldens are Sage's real delivery and still declare
     * {@code external_id} / {@code study_memberships} — empty in every golden row, which is the other half of the
     * argument for dropping them. Our contract deliberately does not declare them (see
     * {@link AddfTables#PII_WITHHELD_PARTICIPANT_FIELDS}); subtracting keeps the rest of the comparison exact, so a
     * genuinely stale fixture still fails.
     */
    private static List<String> withoutWithheld(List<String> columns) {
        List<String> kept = new ArrayList<>(columns);
        kept.removeAll(AddfTables.PII_WITHHELD_PARTICIPANT_FIELDS);
        return kept;
    }

    // -----------------------------------------------------------------------------------------------------------
    // Fixture plumbing
    // -----------------------------------------------------------------------------------------------------------

    private TableRow goldenRow(String table, String recordId) throws IOException {
        String resource = AddfTables.KEYBOARD_SESSIONS.equals(table)
                ? TABLES_PREFIX + "keyboard_sessions/month=2026-08/part-0001.parquet"
                : TABLES_PREFIX + table + ".parquet";
        File file = resourceToFile(resource, table + "-golden.parquet");
        for (TableRow row : parquetTableReader.read(table, file)) {
            if (recordId.equals(row.get("record_id"))) {
                return row;
            }
        }
        return null;
    }

    /** Unzip a fixture archive and wrap it as a {@link DecryptedArchive}, exactly as UploadFetcher would. */
    private DecryptedArchive openArchive(String archiveName, String recordId, String healthCode) throws IOException {
        File archiveFile = resourceToFile(RAW_PREFIX + archiveName, archiveName);
        File unzipDir = new File(tempDir, recordId);
        if (!unzipDir.mkdirs() && !unzipDir.isDirectory()) {
            fail("could not create " + unzipDir);
        }

        Map<String, File> unzippedFiles = new LinkedHashMap<>();
        Map<String, JsonNode> jsonFiles = new LinkedHashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(archiveFile.toPath()))) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                String name = new File(entry.getName()).getName();
                File out = new File(unzipDir, name);
                copy(zip, out);
                unzippedFiles.put(name, out);
                if (name.toLowerCase().endsWith(".json")) {
                    try {
                        jsonFiles.put(name, objectMapper.readTree(out));
                    } catch (IOException ex) {
                        // Same contract as UploadFetcher: a file that isn't valid JSON simply isn't in the JSON map.
                    }
                }
            }
        }

        HealthDataRecordEx3 record = mock(HealthDataRecordEx3.class);
        when(record.getId()).thenReturn(recordId);
        when(record.getHealthCode()).thenReturn(healthCode);
        when(record.getMetadata()).thenReturn(new HashMap<String, String>());
        return new DecryptedArchive(record, null, archiveFile, unzippedFiles, jsonFiles);
    }

    private File resourceToFile(String resource, String fileName) throws IOException {
        File out = new File(tempDir, fileName);
        if (out.exists()) {
            return out;
        }
        if (out.getParentFile() != null) {
            out.getParentFile().mkdirs();
        }
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            assertNotNull(in, "missing golden fixture: " + resource);
            copy(in, out);
        }
        return out;
    }

    // -----------------------------------------------------------------------------------------------------------
    // Comparison
    // -----------------------------------------------------------------------------------------------------------

    /**
     * Golden values come back from Parquet already normalised by {@link ParquetTableReader} (INTEGER → Long, DECIMAL →
     * Double, TEXT → String); the builders produce boxed Java types that can be narrower (Integer for an INTEGER
     * column). Compare by the column's declared type rather than by {@code equals}, which would call every
     * {@code Integer}/{@code Long} pair unequal.
     */
    private static boolean valuesMatch(Column column, Object golden, Object actual) {
        if (golden == null || actual == null) {
            return golden == null && actual == null;
        }
        switch (column.getType()) {
            case INTEGER:
                return ((Number) golden).longValue() == ((Number) actual).longValue();
            case DECIMAL:
                return Math.abs(((Number) golden).doubleValue() - ((Number) actual).doubleValue()) <= EPSILON;
            case BOOLEAN:
                return golden.equals(actual);
            case TEXT:
            case DATETIME:
            default:
                return textMatches(golden.toString(), actual.toString());
        }
    }

    /**
     * The JSON columns ({@code keylogs}, {@code taps}, {@code points}, {@code results}) hold the payload verbatim, but
     * "verbatim" is about content, not bytes: the reference pipeline serialises with Python's {@code ", "}/{@code ": "}
     * separators and Jackson writes compact. Compare those as parsed trees so the test fails on a changed value and
     * not on a space. Everything else is compared literally.
     */
    private static boolean textMatches(String golden, String actual) {
        if (golden.equals(actual)) {
            return true;
        }
        if (!looksLikeJson(golden) || !looksLikeJson(actual)) {
            return false;
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readTree(golden).equals(objectMapper.readTree(actual));
        } catch (IOException ex) {
            return false;
        }
    }

    private static boolean looksLikeJson(String value) {
        String trimmed = value.trim();
        return trimmed.startsWith("[") || trimmed.startsWith("{");
    }

    private static String render(Object value) {
        if (value == null) {
            return "null";
        }
        String text = value.toString();
        // The JSON columns (keylogs/taps/points/results) run to tens of KB; a failure message doesn't need all of it.
        return text.length() > 160 ? text.substring(0, 160) + "…(" + text.length() + " chars)" : text;
    }

    private static void copy(InputStream in, File dest) throws IOException {
        try (OutputStream out = new FileOutputStream(dest)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
        }
    }

    private static void deleteRecursively(File file) {
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }
}
