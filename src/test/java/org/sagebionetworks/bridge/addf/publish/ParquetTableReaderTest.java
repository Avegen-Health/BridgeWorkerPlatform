package org.sagebionetworks.bridge.addf.publish;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.ParquetRowWriter;
import org.sagebionetworks.bridge.addf.transform.TableRow;

/**
 * Round-trips the publish-side multi-row writer ({@link ParquetRowWriter#writeAll}) through the reader
 * ({@link ParquetTableReader}) with real Parquet, asserting logical values survive (INTEGER&rarr;Long,
 * BOOLEAN&rarr;Boolean, TEXT/DATETIME&rarr;String) — the same logical-not-byte contract the Phase 7 golden tests use.
 */
public class ParquetTableReaderTest {
    private ParquetRowWriter writer;
    private ParquetTableReader reader;
    private File tempDir;

    @BeforeMethod
    public void before() throws Exception {
        writer = new ParquetRowWriter();
        reader = new ParquetTableReader();
        tempDir = Files.createTempDirectory("addf-publish-io-test").toFile();
    }

    @Test
    public void writeAllThenReadRoundTrips() throws Exception {
        TableRow row1 = new TableRow(AddfTables.PHQ9, "rec-1");
        row1.put("record_id", "rec-1");
        row1.put("health_code", "hc-1");
        row1.put("participant_version", 3);
        row1.put("created_on", "2026-08-15T10:30:00.000Z");
        row1.put("total_score", 12);
        row1.put("is_test", Boolean.FALSE);

        TableRow row2 = new TableRow(AddfTables.PHQ9, "rec-2");
        row2.put("record_id", "rec-2");
        row2.put("health_code", "hc-2");
        row2.put("participant_version", 5);
        // total_score deliberately left null -> reads back null.

        File out = new File(tempDir, "phq9.parquet");
        writer.writeAll(AddfTables.PHQ9, ImmutableList.of(row1, row2), out);

        List<TableRow> readBack = reader.read(AddfTables.PHQ9, out);
        assertEquals(readBack.size(), 2);

        TableRow r1 = readBack.get(0);
        // Reader populates the row key from the table's natural key column (record_id for activity tables).
        assertEquals(r1.getKey(), "rec-1");
        assertEquals(r1.get("record_id"), "rec-1");
        assertEquals(r1.get("health_code"), "hc-1");
        assertEquals(r1.get("participant_version"), 3L);   // INTEGER -> Long
        assertEquals(r1.get("created_on"), "2026-08-15T10:30:00.000Z");
        assertEquals(r1.get("total_score"), 12L);
        assertEquals(r1.get("is_test"), Boolean.FALSE);

        TableRow r2 = readBack.get(1);
        assertEquals(r2.get("record_id"), "rec-2");
        assertEquals(r2.get("participant_version"), 5L);
        assertNull(r2.get("total_score"));
    }

    @Test
    public void emptyRowsWritesReadableZeroRowFile() throws Exception {
        File out = new File(tempDir, "empty.parquet");
        writer.writeAll(AddfTables.DEMOGRAPHICS, ImmutableList.of(), out);

        List<TableRow> readBack = reader.read(AddfTables.DEMOGRAPHICS, out);
        assertEquals(readBack.size(), 0);
    }
}
