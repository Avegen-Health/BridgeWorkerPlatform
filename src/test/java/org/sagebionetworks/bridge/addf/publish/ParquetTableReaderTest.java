package org.sagebionetworks.bridge.addf.publish;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

import java.io.File;
import java.nio.file.Files;
import java.util.LinkedHashSet;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.Column;
import org.sagebionetworks.bridge.addf.transform.ColumnType;
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

    /**
     * Widening a table must not break the publish that has to rewrite the previous snapshot. The consolidated file on
     * disk was written under the old, narrower column list; {@code SnapshotDeltaBuilder} reads it by the <em>new</em>
     * list in order to rewrite it. {@code GenericRecord.get(String)} throws {@code AvroRuntimeException} for a field
     * its own schema lacks rather than returning null, so without {@code ParquetTableReader#get}'s schema check the
     * first publish after any column addition would crash on the file it is trying to migrate.
     *
     * <p>Simulated by writing with a genuinely narrower schema — the table's column list minus its last column — so the
     * test keeps working whatever the contract grows to next, and does not hard-code a particular added column.</p>
     */
    @Test
    public void readsAFileWrittenBeforeAColumnWasAdded() throws Exception {
        List<String> current = AddfTables.columnNames(AddfTables.PARTICIPANT_VERSIONS);
        String addedLater = current.get(current.size() - 1);

        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record(AddfTables.PARTICIPANT_VERSIONS)
                .namespace("org.sagebionetworks.bridge.addf").fields();
        for (Column column : AddfTables.columnsFor(AddfTables.PARTICIPANT_VERSIONS)) {
            if (column.getName().equals(addedLater)) {
                continue; // the column that "does not exist yet" in this older file
            }
            fields = column.getType() == ColumnType.INTEGER
                    ? fields.name(column.getName()).type().nullable().longType().noDefault()
                    : fields.name(column.getName()).type().nullable().stringType().noDefault();
        }
        Schema olderSchema = fields.endRecord();

        GenericRecord legacy = new GenericData.Record(olderSchema);
        legacy.put("health_code", "hc-legacy");
        legacy.put("participant_version", 2L);
        legacy.put("study_id", "biaffect-3-study");

        File out = new File(tempDir, "participant_versions-legacy.parquet");
        try (ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(new Path(out.getAbsolutePath())).withSchema(olderSchema).build()) {
            parquetWriter.write(legacy);
        }

        // The drift the builder keys its forced rewrite off.
        assertNotEquals(reader.readColumnNames(out), current);

        List<TableRow> readBack = reader.read(AddfTables.PARTICIPANT_VERSIONS, out);
        assertEquals(readBack.size(), 1);
        TableRow row = readBack.get(0);
        assertEquals(row.getKey(), "hc-legacy");
        assertEquals(row.get("health_code"), "hc-legacy");
        assertEquals(row.get("participant_version"), 2L);
        assertEquals(row.get("study_id"), "biaffect-3-study");
        // Present in the contract, absent from the file -> null, not a throw.
        assertNull(row.get(addedLater));
        // Still projected onto the full contract, so the rewrite emits every current column.
        assertEquals(row.getValues().keySet(), new LinkedHashSet<>(current));
    }
}
