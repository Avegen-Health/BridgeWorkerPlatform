package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * ADDF §7 — the <b>FAIR conformance test</b>, and the top of the schema chain.
 *
 * <p>{@code BiAffect3_FAIR_Metadata_Draft.xlsx} is the source of truth for the delivered schema. It outranks
 * {@code data-contract.md} and it outranks the 29/19/14/15/19/14/8/11/11/15 column counts quoted in the plan: where
 * they disagree, the derived document is wrong, not the workbook. This test pins {@link AddfTables} — the registry
 * every writer, reader and gate derives from — to the workbook's {@code fields} sheet by <b>name, order and logical
 * type</b>.</p>
 *
 * <p>That is deliberately stronger than a column <em>count</em>. The drift this catches is the type/semantic kind a
 * count is blind to: a decimal delivered as an integer, a datetime delivered as text, two columns swapped. The
 * manifest gate then pins the delivered Parquet files to {@link AddfTables}, so the full chain is
 * <b>workbook → AddfTables → delivered files</b> with no link asserted against itself.</p>
 *
 * <p>When the workbook legitimately changes, refresh the copy under {@code src/test/resources/addf/} from
 * {@code wiki/proposals/addf-export/assets/} and update {@link AddfTables} until this test passes again — in that
 * order.</p>
 */
public class AddfTablesFairConformanceTest {
    private static final String WORKBOOK_RESOURCE = "/addf/BiAffect3_FAIR_Metadata_Draft.xlsx";
    private static final String FIELDS_SHEET = "fields";

    // Columns of the fields sheet itself, by spreadsheet column letter (see FairWorkbook on why not by position).
    private static final String COL_DICTIONARY_CODE = "A";
    private static final String COL_NAME = "B";
    private static final String COL_TYPE = "D";

    /** Workbook logical type → the {@link ColumnType} AddfTables must declare for it. */
    private static final Map<String, ColumnType> TYPE_MAP = ImmutableMap.<String, ColumnType>builder()
            .put("text", ColumnType.TEXT)
            .put("integer", ColumnType.INTEGER)
            .put("decimal", ColumnType.DECIMAL)
            .put("number", ColumnType.DECIMAL)
            .put("float", ColumnType.DECIMAL)
            .put("boolean", ColumnType.BOOLEAN)
            .put("datetime", ColumnType.DATETIME)
            .build();

    /** table name → its fields, in workbook order. */
    private Map<String, List<Field>> fairFields;

    private static final class Field {
        final String name;
        final String type;

        Field(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }

    @BeforeClass
    public void loadWorkbook() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(WORKBOOK_RESOURCE)) {
            assertNotNull(in, "FAIR workbook missing from test resources: " + WORKBOOK_RESOURCE);
            List<Map<String, String>> rows = FairWorkbook.read(in).rows(FIELDS_SHEET);

            // Row 1 is the header; assert it so a re-exported workbook that moves columns fails here, loudly, rather
            // than silently comparing the wrong cells.
            Map<String, String> header = rows.get(0);
            assertEquals(header.get(COL_DICTIONARY_CODE), "dictionary_code");
            assertEquals(header.get(COL_NAME), "name");
            assertEquals(header.get(COL_TYPE), "type");

            fairFields = new LinkedHashMap<>();
            for (Map<String, String> row : rows.subList(1, rows.size())) {
                String table = trim(row.get(COL_DICTIONARY_CODE));
                String name = trim(row.get(COL_NAME));
                if (table.isEmpty() || name.isEmpty()) {
                    continue;
                }
                fairFields.computeIfAbsent(table, t -> new ArrayList<>())
                        .add(new Field(name, trim(row.get(COL_TYPE))));
            }
        }
    }

    @Test
    public void everyTableInTheContractIsInTheWorkbookAndViceVersa() {
        Set<String> declared = new LinkedHashSet<>(AddfTables.allTables());
        Set<String> inWorkbook = new LinkedHashSet<>(fairFields.keySet());

        Set<String> missingFromCode = new LinkedHashSet<>(inWorkbook);
        missingFromCode.removeAll(declared);
        assertTrue(missingFromCode.isEmpty(),
                "the workbook defines table(s) AddfTables does not: " + missingFromCode);

        Set<String> missingFromWorkbook = new LinkedHashSet<>(declared);
        missingFromWorkbook.removeAll(inWorkbook);
        assertTrue(missingFromWorkbook.isEmpty(),
                "AddfTables declares table(s) the workbook does not: " + missingFromWorkbook);
    }

    @Test
    public void everyColumnMatchesTheWorkbookByNameOrderAndType() {
        List<String> violations = new ArrayList<>();

        for (String table : AddfTables.allTables()) {
            List<Field> expected = fairFields.get(table);
            if (expected == null) {
                continue; // reported by the test above
            }
            List<Column> actual = AddfTables.columnsFor(table);

            List<String> expectedNames = new ArrayList<>();
            for (Field field : expected) {
                expectedNames.add(field.name);
            }
            assertEquals(AddfTables.columnNames(table), expectedNames,
                    table + " column names/order must match the FAIR workbook");

            for (int i = 0; i < expected.size(); i++) {
                Field field = expected.get(i);
                ColumnType mapped = TYPE_MAP.get(field.type.toLowerCase());
                if (mapped == null) {
                    violations.add(table + "." + field.name + ": workbook type '" + field.type
                            + "' has no ColumnType mapping");
                } else if (mapped != actual.get(i).getType()) {
                    violations.add(table + "." + field.name + ": workbook says " + field.type + " (" + mapped
                            + "), AddfTables declares " + actual.get(i).getType());
                }
            }
        }

        assertTrue(violations.isEmpty(), "FAIR conformance violations: " + violations);
    }

    @Test
    public void workbookAgreesWithTheColumnCountsQuotedInThePlan() {
        // The plan pins 29/19/14/15/19/14/8/11/11/15. Those numbers are derived from this workbook, so assert them
        // against it rather than against AddfTables — otherwise a coordinated edit to code and plan could drift the
        // pair away from the source of truth together.
        assertFairCount(AddfTables.KEYBOARD_SESSIONS, 29);
        assertFairCount(AddfTables.PHQ9, 19);
        assertFairCount(AddfTables.SELF_RATING, 14);
        assertFairCount(AddfTables.EVENING_LOG, 15);
        assertFairCount(AddfTables.GO_NO_GO, 19);
        assertFairCount(AddfTables.TRAIL_MAKING, 14);
        assertFairCount(AddfTables.DEMOGRAPHICS, 8);
        assertFairCount(AddfTables.PARTICIPANT_VERSIONS, 11);
        assertFairCount(AddfTables.PARTICIPANTS_CURRENT, 11);
        assertFairCount(AddfTables.FILE_RECORDS, 15);
    }

    @Test
    public void participantsCurrentMirrorsParticipantVersions() {
        // participants_current is the latest row per participant, not a different entity: the workbook gives it the
        // identical 11 columns, and publish re-keys version rows straight into it (§4.3.1). If the two ever diverge,
        // that derivation starts dropping or inventing columns.
        List<String> versions = namesOf(AddfTables.PARTICIPANT_VERSIONS);
        List<String> current = namesOf(AddfTables.PARTICIPANTS_CURRENT);
        assertEquals(current, versions);
    }

    private void assertFairCount(String table, int expected) {
        assertEquals(fairFields.get(table).size(), expected, table + " field count in the FAIR workbook");
    }

    private List<String> namesOf(String table) {
        List<String> names = new ArrayList<>();
        for (Field field : fairFields.get(table)) {
            names.add(field.name);
        }
        return names;
    }

    private static String trim(String value) {
        return value == null ? "" : value.trim();
    }
}
