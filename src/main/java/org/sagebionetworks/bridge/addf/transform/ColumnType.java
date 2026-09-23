package org.sagebionetworks.bridge.addf.transform;

/**
 * The FAIR logical column types used by the ADDF data contract (see
 * {@code wiki/proposals/addf-export/data-contract.md}). Maps to a physical Parquet/Avro type in
 * {@link AddfTables#avroSchemaFor}.
 *
 * <ul>
 *   <li>{@link #TEXT} &rarr; Avro string. Also holds the verbatim-JSON columns ({@code keylogs}, {@code results},
 *       {@code taps}, {@code points}).</li>
 *   <li>{@link #INTEGER} &rarr; Avro long (widest int; researchers can cast down in SQL).</li>
 *   <li>{@link #DECIMAL} &rarr; Avro double.</li>
 *   <li>{@link #DATETIME} &rarr; Avro string holding an ISO-8601 UTC instant ({@code …Z}).</li>
 *   <li>{@link #BOOLEAN} &rarr; Avro boolean.</li>
 * </ul>
 */
public enum ColumnType {
    TEXT,
    INTEGER,
    DECIMAL,
    DATETIME,
    BOOLEAN
}
