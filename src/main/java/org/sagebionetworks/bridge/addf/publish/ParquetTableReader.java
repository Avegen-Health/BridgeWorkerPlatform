package org.sagebionetworks.bridge.addf.publish;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.Column;
import org.sagebionetworks.bridge.addf.transform.TableRow;

/**
 * ADDF §4.3 — reads a Parquet file (a staged per-record object or an existing consolidated table file) back into
 * {@link TableRow}s so the publish worker can coalesce/upsert them. The inverse of {@link
 * org.sagebionetworks.bridge.addf.transform.ParquetRowWriter}: it reads by the table's {@link AddfTables} column list,
 * so a file written by our own writer round-trips exactly (Avro {@code Utf8} string values are normalised to
 * {@link String}; INTEGER/DECIMAL/BOOLEAN come back as {@code Long}/{@code Double}/{@code Boolean}).
 */
@Component
public class ParquetTableReader {
    /** Hadoop Configuration mirroring the writer's — local filesystem, no native codecs. */
    private Configuration newConf() {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.setBoolean("fs.file.impl.disable.cache", true);
        conf.set("io.native.lib.available", "false");
        return conf;
    }

    /**
     * Read every row of {@code file} into a {@link TableRow} for {@code table}. Each row's staging/idempotency key is
     * populated from the table's natural key column ({@code record_id} for activity/file_records, {@code health_code}
     * for the participant-keyed tables) so callers can upsert without re-deriving it.
     */
    @SuppressWarnings("deprecation") // AvroParquetReader.builder(Path) is deprecated but is the trimmed-Hadoop-safe path.
    public List<TableRow> read(String table, File file) throws IOException {
        List<Column> columns = AddfTables.columnsFor(table);
        String keyColumn = AddfTables.keyColumn(table);
        List<TableRow> rows = new ArrayList<>();

        Path path = new Path(file.getAbsolutePath());
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path)
                .withConf(newConf())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                Object keyValue = normalise(record.get(keyColumn));
                TableRow row = new TableRow(table, keyValue == null ? null : keyValue.toString());
                for (Column column : columns) {
                    row.put(column.getName(), normalise(record.get(column.getName())));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /** Avro returns {@code Utf8} for string fields; normalise to {@link String} so downstream comparisons are plain. */
    private static Object normalise(Object value) {
        if (value instanceof CharSequence) {
            return value.toString();
        }
        return value;
    }
}
