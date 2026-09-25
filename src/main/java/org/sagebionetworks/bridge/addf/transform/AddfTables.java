package org.sagebionetworks.bridge.addf.transform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * The single source of truth for the ADDF 10-table schema, mirroring
 * {@code wiki/proposals/addf-export/data-contract.md}. Column names, order, and {@link ColumnType}s are defined here;
 * the Avro schema each Parquet file is written against is derived from them ({@link #avroSchemaFor}).
 *
 * <p>The FAIR dictionary workbook ({@code BiAffect3_FAIR_Metadata_Draft.xlsx}) is the ultimate source of truth — the
 * Phase 7 conformance test parses it and asserts these definitions match. If the workbook and this class disagree,
 * the workbook wins and this class is updated. The one deliberate exception is
 * {@link #PII_WITHHELD_PARTICIPANT_FIELDS}: a schema the workbook defines is still not a licence to deliver PII.</p>
 *
 * <p>All fields are emitted as nullable Avro unions {@code ["null", <type>]}: a partial demographics row, a missing
 * summary, or an absent optional annotation must serialise as null rather than fail the write.</p>
 */
public final class AddfTables {
    // Activity (content) tables — one row per record, from the decrypted archive payload.
    public static final String KEYBOARD_SESSIONS = "keyboard_sessions";
    public static final String PHQ9 = "phq9";
    public static final String SELF_RATING = "self_rating";
    public static final String EVENING_LOG = "evening_log";
    public static final String GO_NO_GO = "go_no_go";
    public static final String TRAIL_MAKING = "trail_making";
    // Participant-keyed table produced on the upload trigger.
    public static final String DEMOGRAPHICS = "demographics";
    // Dimension tables (Phase 3b / Phase 4 derive these) — declared here so the schema authority is complete.
    public static final String PARTICIPANT_VERSIONS = "participant_versions";
    public static final String PARTICIPANTS_CURRENT = "participants_current";
    // Raw-layer index — one row per uploaded record, always emitted.
    public static final String FILE_RECORDS = "file_records";

    /**
     * The two columns the FAIR workbook declares for {@code participant_versions} / {@code participants_current} that
     * we deliberately do <b>not</b> deliver. Both carry the account's {@code externalId} — the enrolment identifier
     * the site chooses, in practice a person's name ({@code study_memberships} carries it verbatim inside
     * {@code |studyId=externalId|}). {@code externalId} is Class A PII, which must never appear in a
     * researcher-visible export, and the delivery README promises ADDI that the participant is identified only by the
     * de-identified {@code health_code}; shipping either column defeats that promise.
     *
     * <p>Nothing analytical is lost: {@code study_id} keeps the study dimension (derived from the membership keys, not
     * the values) and {@code (health_code, participant_version)} keeps every join. The workbook remains the schema
     * authority for everything else — {@code AddfTablesFairConformanceTest} subtracts exactly this set from the
     * workbook's participant fields before comparing, so any <em>other</em> divergence still fails.</p>
     */
    public static final Set<String> PII_WITHHELD_PARTICIPANT_FIELDS =
            ImmutableSet.of("external_id", "study_memberships");

    private static final Map<String, List<Column>> COLUMNS;
    static {
        Map<String, List<Column>> m = new LinkedHashMap<>();

        m.put(KEYBOARD_SESSIONS, ImmutableList.of(
                Column.of("record_id", ColumnType.TEXT),
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("session_start", ColumnType.DATETIME),
                Column.of("time_zone", ColumnType.TEXT),
                Column.of("duration_sec", ColumnType.DECIMAL),
                Column.of("total_keys", ColumnType.INTEGER),
                Column.of("n_alphabet", ColumnType.INTEGER),
                Column.of("n_numeral", ColumnType.INTEGER),
                Column.of("n_punctuation", ColumnType.INTEGER),
                Column.of("n_symbol", ColumnType.INTEGER),
                Column.of("n_emoji", ColumnType.INTEGER),
                Column.of("n_backspace", ColumnType.INTEGER),
                Column.of("n_space", ColumnType.INTEGER),
                Column.of("n_suggestion", ColumnType.INTEGER),
                Column.of("n_autocorrection", ColumnType.INTEGER),
                Column.of("n_other", ColumnType.INTEGER),
                Column.of("mean_hold_duration", ColumnType.DECIMAL),
                Column.of("median_hold_duration", ColumnType.DECIMAL),
                Column.of("mean_dist_from_center", ColumnType.DECIMAL),
                Column.of("motion_sample_count", ColumnType.INTEGER),
                Column.of("accel_mag_mean", ColumnType.DECIMAL),
                Column.of("accel_mag_max", ColumnType.DECIMAL),
                Column.of("keylogs", ColumnType.TEXT),
                Column.of("device_name", ColumnType.TEXT),
                Column.of("app_version", ColumnType.TEXT),
                Column.of("platform", ColumnType.TEXT),
                Column.of("uploaded_on", ColumnType.DATETIME),
                Column.of("is_test", ColumnType.BOOLEAN)));

        m.put(PHQ9, ImmutableList.of(
                Column.of("record_id", ColumnType.TEXT),
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("created_on", ColumnType.DATETIME),
                Column.of("time_zone", ColumnType.TEXT),
                Column.of("anhedonia", ColumnType.INTEGER),
                Column.of("depression", ColumnType.INTEGER),
                Column.of("sleep", ColumnType.INTEGER),
                Column.of("energy", ColumnType.INTEGER),
                Column.of("appetite", ColumnType.INTEGER),
                Column.of("discouragement", ColumnType.INTEGER),
                Column.of("concentration", ColumnType.INTEGER),
                Column.of("speed", ColumnType.INTEGER),
                Column.of("difficulty", ColumnType.INTEGER),
                Column.of("total_score", ColumnType.INTEGER),
                Column.of("app_version", ColumnType.TEXT),
                Column.of("platform", ColumnType.TEXT),
                Column.of("uploaded_on", ColumnType.DATETIME),
                Column.of("is_test", ColumnType.BOOLEAN)));

        m.put(SELF_RATING, ImmutableList.of(
                Column.of("record_id", ColumnType.TEXT),
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("created_on", ColumnType.DATETIME),
                Column.of("time_zone", ColumnType.TEXT),
                Column.of("energy", ColumnType.INTEGER),
                Column.of("mood", ColumnType.INTEGER),
                Column.of("thoughts", ColumnType.INTEGER),
                Column.of("impulsiveness", ColumnType.INTEGER),
                Column.of("attention", ColumnType.INTEGER),
                Column.of("app_version", ColumnType.TEXT),
                Column.of("platform", ColumnType.TEXT),
                Column.of("uploaded_on", ColumnType.DATETIME),
                Column.of("is_test", ColumnType.BOOLEAN)));

        m.put(EVENING_LOG, ImmutableList.of(
                Column.of("record_id", ColumnType.TEXT),
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("created_on", ColumnType.DATETIME),
                Column.of("time_zone", ColumnType.TEXT),
                Column.of("mood", ColumnType.INTEGER),
                Column.of("fatigue", ColumnType.INTEGER),
                Column.of("fidgeting", ColumnType.INTEGER),
                Column.of("energy", ColumnType.INTEGER),
                Column.of("speech", ColumnType.INTEGER),
                Column.of("irritability", ColumnType.INTEGER),
                Column.of("app_version", ColumnType.TEXT),
                Column.of("platform", ColumnType.TEXT),
                Column.of("uploaded_on", ColumnType.DATETIME),
                Column.of("is_test", ColumnType.BOOLEAN)));

        m.put(GO_NO_GO, ImmutableList.of(
                Column.of("record_id", ColumnType.TEXT),
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("created_on", ColumnType.DATETIME),
                Column.of("identifier", ColumnType.TEXT),
                Column.of("time_zone", ColumnType.TEXT),
                Column.of("n_trials", ColumnType.INTEGER),
                Column.of("n_go", ColumnType.INTEGER),
                Column.of("n_nogo", ColumnType.INTEGER),
                Column.of("n_correct", ColumnType.INTEGER),
                Column.of("commission_errors", ColumnType.INTEGER),
                Column.of("omission_errors", ColumnType.INTEGER),
                Column.of("mean_reaction_time", ColumnType.DECIMAL),
                Column.of("median_reaction_time", ColumnType.DECIMAL),
                Column.of("results", ColumnType.TEXT),
                Column.of("app_version", ColumnType.TEXT),
                Column.of("platform", ColumnType.TEXT),
                Column.of("uploaded_on", ColumnType.DATETIME),
                Column.of("is_test", ColumnType.BOOLEAN)));

        m.put(TRAIL_MAKING, ImmutableList.of(
                Column.of("record_id", ColumnType.TEXT),
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("created_on", ColumnType.DATETIME),
                Column.of("number_of_errors", ColumnType.INTEGER),
                Column.of("runtime_sec", ColumnType.DECIMAL),
                Column.of("pause_interval_sec", ColumnType.DECIMAL),
                Column.of("time_zone", ColumnType.TEXT),
                Column.of("taps", ColumnType.TEXT),
                Column.of("points", ColumnType.TEXT),
                Column.of("app_version", ColumnType.TEXT),
                Column.of("platform", ColumnType.TEXT),
                Column.of("uploaded_on", ColumnType.DATETIME),
                Column.of("is_test", ColumnType.BOOLEAN)));

        m.put(DEMOGRAPHICS, ImmutableList.of(
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("birth_year", ColumnType.INTEGER),
                Column.of("gender", ColumnType.TEXT),
                Column.of("bipolar_diagnosis", ColumnType.TEXT),
                Column.of("other_psych_diagnoses", ColumnType.TEXT),
                Column.of("other_illness", ColumnType.TEXT),
                Column.of("collected_on", ColumnType.DATETIME)));

        // 9 of the workbook's 11 participant columns — external_id and study_memberships are withheld, see
        // PII_WITHHELD_PARTICIPANT_FIELDS below.
        List<Column> participantVersionCols = ImmutableList.of(
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("study_id", ColumnType.TEXT),
                Column.of("sharing_scope", ColumnType.TEXT),
                Column.of("data_groups", ColumnType.TEXT),
                Column.of("languages", ColumnType.TEXT),
                Column.of("client_time_zone", ColumnType.TEXT),
                Column.of("created_on", ColumnType.DATETIME),
                Column.of("modified_on", ColumnType.DATETIME));
        m.put(PARTICIPANT_VERSIONS, participantVersionCols);
        m.put(PARTICIPANTS_CURRENT, participantVersionCols);

        m.put(FILE_RECORDS, ImmutableList.of(
                Column.of("record_id", ColumnType.TEXT),
                Column.of("health_code", ColumnType.TEXT),
                Column.of("participant_version", ColumnType.INTEGER),
                Column.of("item", ColumnType.TEXT),
                Column.of("uploaded_on", ColumnType.DATETIME),
                Column.of("exported_on", ColumnType.DATETIME),
                Column.of("created_on", ColumnType.DATETIME),
                Column.of("content_type", ColumnType.TEXT),
                Column.of("user_agent", ColumnType.TEXT),
                Column.of("client_info", ColumnType.TEXT),
                Column.of("app_version", ColumnType.TEXT),
                Column.of("device_name", ColumnType.TEXT),
                Column.of("os_name", ColumnType.TEXT),
                Column.of("os_version", ColumnType.TEXT),
                Column.of("file_name", ColumnType.TEXT)));

        COLUMNS = ImmutableMap.copyOf(m);
    }

    private AddfTables() {
    }

    /** Every ADDF table name, in contract order. */
    public static List<String> allTables() {
        return ImmutableList.copyOf(COLUMNS.keySet());
    }

    /** Ordered columns for a table, or {@code null} if the table is unknown. */
    public static List<Column> columnsFor(String table) {
        return COLUMNS.get(table);
    }

    /**
     * The natural key column for a table's grain: {@code record_id} for the 6 activity tables + {@code file_records}
     * (1 row per uploaded record), {@code health_code} for the participant-keyed tables ({@code demographics},
     * {@code participant_versions}, {@code participants_current}). Used by the publish worker to upsert coalesced rows.
     * Note {@code participant_versions} additionally keys on {@code participant_version} — the publish builder combines
     * the two explicitly; this returns the row-identity column shared by the participant-keyed family.
     */
    public static String keyColumn(String table) {
        switch (table) {
            case DEMOGRAPHICS:
            case PARTICIPANT_VERSIONS:
            case PARTICIPANTS_CURRENT:
                return "health_code";
            default:
                return "record_id";
        }
    }

    /**
     * Build the Avro schema for a table: one nullable field per column, in contract order. Cached construction is left
     * to the caller ({@link ParquetRowWriter} builds once per write, which is negligible next to the S3 round-trip).
     */
    public static Schema avroSchemaFor(String table) {
        List<Column> columns = COLUMNS.get(table);
        if (columns == null) {
            throw new IllegalArgumentException("Unknown ADDF table: " + table);
        }
        SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.record(table)
                .namespace("org.sagebionetworks.bridge.addf").fields();
        for (Column column : columns) {
            SchemaBuilder.FieldTypeBuilder<Schema> field = assembler.name(column.getName()).type();
            switch (column.getType()) {
                case INTEGER:
                    assembler = field.nullable().longType().noDefault();
                    break;
                case DECIMAL:
                    assembler = field.nullable().doubleType().noDefault();
                    break;
                case BOOLEAN:
                    assembler = field.nullable().booleanType().noDefault();
                    break;
                case TEXT:
                case DATETIME:
                default:
                    assembler = field.nullable().stringType().noDefault();
                    break;
            }
        }
        return assembler.endRecord();
    }

    /** Convenience for tests / builders: a fresh empty ordered row for a table. */
    public static List<String> columnNames(String table) {
        List<Column> columns = COLUMNS.get(table);
        List<String> names = new ArrayList<>();
        for (Column column : columns) {
            names.add(column.getName());
        }
        return names;
    }
}
