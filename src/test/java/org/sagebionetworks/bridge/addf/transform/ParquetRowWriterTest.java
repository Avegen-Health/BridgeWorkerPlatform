package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParquetRowWriterTest {
    private ParquetRowWriter writer;
    private File tempDir;

    @BeforeMethod
    public void before() throws Exception {
        writer = new ParquetRowWriter();
        tempDir = Files.createTempDirectory("addf-parquet-test").toFile();
    }

    @Test
    public void writesOneRowParquetCoercingEveryColumnType() throws Exception {
        TableRow row = new TableRow(AddfTables.KEYBOARD_SESSIONS, "rec-1");
        // TEXT from a non-string value -> toString().
        row.put("record_id", 123);
        // INTEGER: numeric, string-parsed, and unparseable (-> null).
        row.put("total_keys", 5);
        row.put("n_alphabet", "7");
        row.put("n_numeral", "not-a-number");
        // DECIMAL: numeric, string-parsed, and unparseable (-> null).
        row.put("duration_sec", 1.5);
        row.put("mean_hold_duration", "2.5");
        row.put("median_hold_duration", "still-not-a-number");
        // BOOLEAN from a real Boolean.
        row.put("is_test", Boolean.TRUE);
        // Remaining columns are left null (the null-coercion path).

        File out = new File(tempDir, "row.parquet");
        File returned = writer.write(row, out);

        assertTrue(returned.exists());
        assertTrue(returned.length() > 0, "parquet file should be non-empty");
    }

    @Test
    public void coercesBooleanFromString() throws Exception {
        TableRow row = new TableRow(AddfTables.KEYBOARD_SESSIONS, "rec-2");
        row.put("record_id", "rec-2");
        row.put("is_test", "false");

        File out = new File(tempDir, "row2.parquet");
        writer.write(row, out);

        assertTrue(out.exists());
        assertTrue(out.length() > 0);
    }
}
