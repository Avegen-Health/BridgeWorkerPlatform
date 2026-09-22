package org.sagebionetworks.bridge.addf.transform;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Component;

/**
 * ADDF §3.5 — writes a single {@link TableRow} as a one-row Parquet file (parquet-avro, Snappy). The file is an
 * immutable per-record staging object; the publish worker (Phase 4) coalesces many such files into the consolidated
 * {@code <table>.parquet}. Column types follow {@link AddfTables} (INTEGER&rarr;long, DECIMAL&rarr;double,
 * BOOLEAN&rarr;boolean, TEXT/DATETIME&rarr;string, all nullable).
 *
 * <p>Physical encoding is deliberately unconstrained: the Phase 7 golden test compares <b>logical schema + values</b>
 * read back through a Parquet reader, never raw bytes, because this writer's encoding will not match the
 * pyarrow-generated golden files.</p>
 */
@Component
public class ParquetRowWriter {
    // Parquet's Snappy is aircompressor (pure-JVM) in 1.13.x — no native lib, matching the "no novel native dep" goal.
    private static final CompressionCodecName CODEC = CompressionCodecName.SNAPPY;

    /** Hadoop Configuration for local-filesystem writes; disables native codec loading. */
    private Configuration newConf() {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.setBoolean("fs.file.impl.disable.cache", true);
        conf.set("io.native.lib.available", "false");
        return conf;
    }

    /**
     * Write {@code row} to {@code outputFile} (which must not already exist — the caller supplies a fresh path in its
     * temp dir). Returns the same file for convenience.
     */
    public File write(TableRow row, File outputFile) throws IOException {
        String table = row.getTable();
        Schema schema = AddfTables.avroSchemaFor(table);
        List<Column> columns = AddfTables.columnsFor(table);

        GenericRecord record = new GenericData.Record(schema);
        for (Column column : columns) {
            record.put(column.getName(), coerce(column.getType(), row.get(column.getName())));
        }

        Path path = new Path(outputFile.getAbsolutePath());
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(schema)
                .withConf(newConf())
                .withCompressionCodec(CODEC)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            writer.write(record);
        }
        return outputFile;
    }

    private static Object coerce(ColumnType type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case INTEGER:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                try {
                    return Long.parseLong(value.toString().trim());
                } catch (NumberFormatException ex) {
                    return null;
                }
            case DECIMAL:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                try {
                    return Double.parseDouble(value.toString().trim());
                } catch (NumberFormatException ex) {
                    return null;
                }
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString().trim());
            case TEXT:
            case DATETIME:
            default:
                return value.toString();
        }
    }
}
