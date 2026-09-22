package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.List;

import org.apache.avro.Schema;
import org.testng.annotations.Test;

public class AddfTablesTest {
    @Test
    public void allTablesContainsEveryTable() {
        List<String> tables = AddfTables.allTables();
        assertEquals(tables.size(), 10);
        assertTrue(tables.contains(AddfTables.KEYBOARD_SESSIONS));
        assertTrue(tables.contains(AddfTables.PHQ9));
        assertTrue(tables.contains(AddfTables.SELF_RATING));
        assertTrue(tables.contains(AddfTables.EVENING_LOG));
        assertTrue(tables.contains(AddfTables.GO_NO_GO));
        assertTrue(tables.contains(AddfTables.TRAIL_MAKING));
        assertTrue(tables.contains(AddfTables.DEMOGRAPHICS));
        assertTrue(tables.contains(AddfTables.PARTICIPANT_VERSIONS));
        assertTrue(tables.contains(AddfTables.PARTICIPANTS_CURRENT));
        assertTrue(tables.contains(AddfTables.FILE_RECORDS));
    }

    @Test
    public void columnsForKnownTable() {
        List<Column> columns = AddfTables.columnsFor(AddfTables.PHQ9);
        assertNotNull(columns);
        assertEquals(columns.get(0).getName(), "record_id");
    }

    @Test
    public void columnsForUnknownTableIsNull() {
        assertNull(AddfTables.columnsFor("no_such_table"));
    }

    @Test
    public void columnNamesMatchColumns() {
        List<String> names = AddfTables.columnNames(AddfTables.DEMOGRAPHICS);
        List<Column> columns = AddfTables.columnsFor(AddfTables.DEMOGRAPHICS);
        assertEquals(names.size(), columns.size());
        for (int i = 0; i < names.size(); i++) {
            assertEquals(names.get(i), columns.get(i).getName());
        }
    }

    @Test
    public void participantVersionsAndCurrentShareSchema() {
        assertEquals(AddfTables.columnNames(AddfTables.PARTICIPANT_VERSIONS),
                AddfTables.columnNames(AddfTables.PARTICIPANTS_CURRENT));
    }

    @Test
    public void avroSchemaForKeyboardCoversAllColumnTypes() {
        // keyboard_sessions has TEXT, INTEGER, DECIMAL, DATETIME, and BOOLEAN columns — exercises every switch branch.
        Schema schema = AddfTables.avroSchemaFor(AddfTables.KEYBOARD_SESSIONS);
        assertEquals(schema.getName(), AddfTables.KEYBOARD_SESSIONS);
        assertEquals(schema.getFields().size(), AddfTables.columnsFor(AddfTables.KEYBOARD_SESSIONS).size());

        // Every field is a nullable union that includes null.
        for (Schema.Field field : schema.getFields()) {
            assertEquals(field.schema().getType(), Schema.Type.UNION);
            assertTrue(unionContainsNull(field.schema()));
        }

        assertEquals(unionValueType(schema, "record_id"), Schema.Type.STRING);      // TEXT
        assertEquals(unionValueType(schema, "session_start"), Schema.Type.STRING);  // DATETIME
        assertEquals(unionValueType(schema, "total_keys"), Schema.Type.LONG);       // INTEGER
        assertEquals(unionValueType(schema, "duration_sec"), Schema.Type.DOUBLE);   // DECIMAL
        assertEquals(unionValueType(schema, "is_test"), Schema.Type.BOOLEAN);       // BOOLEAN
    }

    @Test
    public void avroSchemaForUnknownTableThrows() {
        try {
            AddfTables.avroSchemaFor("no_such_table");
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("no_such_table"));
        }
    }

    private static Schema.Type unionValueType(Schema record, String fieldName) {
        Schema union = record.getField(fieldName).schema();
        for (Schema branch : union.getTypes()) {
            if (branch.getType() != Schema.Type.NULL) {
                return branch.getType();
            }
        }
        throw new AssertionError("union had no non-null branch: " + fieldName);
    }

    private static boolean unionContainsNull(Schema union) {
        for (Schema branch : union.getTypes()) {
            if (branch.getType() == Schema.Type.NULL) {
                return true;
            }
        }
        return false;
    }
}
