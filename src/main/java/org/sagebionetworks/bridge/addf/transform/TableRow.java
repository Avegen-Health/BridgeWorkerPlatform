package org.sagebionetworks.bridge.addf.transform;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single typed row destined for an ADDF table. Column values are boxed Java types matching the column's
 * {@link ColumnType}: {@code Long} for INTEGER, {@code Double} for DECIMAL, {@code Boolean} for BOOLEAN,
 * {@code String} for TEXT/DATETIME (DATETIME strings are already ISO-8601 UTC). A missing/absent column is simply not
 * put (or put as null) and serialises to null.
 *
 * <p>The row carries its target {@code table} and the {@code key} used for staging/idempotency ({@code record_id} for
 * activity + file_records rows, {@code health_code} for demographics). Insertion order is preserved but the Parquet
 * writer reads by column name against {@link AddfTables}, so ordering here is for readability only.</p>
 */
public class TableRow {
    private final String table;
    private final String key;
    private final Map<String, Object> values = new LinkedHashMap<>();

    public TableRow(String table, String key) {
        this.table = table;
        this.key = key;
    }

    public String getTable() {
        return table;
    }

    /** Staging / idempotency key (record_id or health_code depending on the table's grain). */
    public String getKey() {
        return key;
    }

    public TableRow put(String column, Object value) {
        values.put(column, value);
        return this;
    }

    public Object get(String column) {
        return values.get(column);
    }

    public Map<String, Object> getValues() {
        return values;
    }
}
