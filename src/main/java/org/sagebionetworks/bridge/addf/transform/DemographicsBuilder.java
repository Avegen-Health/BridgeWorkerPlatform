package org.sagebionetworks.bridge.addf.transform;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

/**
 * ADDF §3.5.1 — {@code demographics} is per-participant (grain = 1 row = 1 participant), built by <b>column-merging
 * two separate uploads</b>, {@code birth-gender} and {@code Diagnosis}, that arrive as distinct {@code completeUpload}
 * events possibly days apart. This builder produces a <b>partial</b> row keyed on {@code health_code} carrying only the
 * columns the current archive provides; the publish worker performs the actual column-merge (§3.7.2) so a later
 * {@code Diagnosis} never nulls out earlier birth/gender fields (and vice-versa). Idempotency is therefore
 * merge-always, not presence-skip (§3.6).
 */
@Component
public class DemographicsBuilder {
    public static final String ITEM_BIRTH_GENDER = "birth-gender";
    public static final String ITEM_DIAGNOSIS = "Diagnosis";

    /** True if this record is one of the two demographics source uploads. */
    public boolean handles(String item) {
        if (item == null) {
            return false;
        }
        return ITEM_BIRTH_GENDER.equalsIgnoreCase(item) || ITEM_DIAGNOSIS.equalsIgnoreCase(item)
                || "birth_gender".equalsIgnoreCase(item) || "gender".equalsIgnoreCase(item);
    }

    /** Build a partial demographics row (keyed on health_code), or null if the payload is unusable. */
    public TableRow build(FlattenContext ctx, String item) {
        JsonNode answers = ctx.getArchive().getJson("answers.json");
        if (answers == null) {
            return null;
        }
        TableRow row = new TableRow(AddfTables.DEMOGRAPHICS, ctx.getHealthCode());
        row.put("health_code", ctx.getHealthCode());
        row.put("participant_version", ctx.getParticipantVersion());

        String rawCreatedOn = infoCreatedOn(ctx);
        row.put("collected_on", AddfDateUtils.toUtcIso(rawCreatedOn));

        if (ITEM_DIAGNOSIS.equalsIgnoreCase(item)) {
            putIfPresent(row, "bipolar_diagnosis", answers.get("Bipolar diagnosis"));
            putIfPresent(row, "other_psych_diagnoses", answers.get("Other psych diagnoses"));
            putIfPresent(row, "other_illness", answers.get("Other illness"));
        } else {
            // birth-gender
            if (answers.hasNonNull("birth") && answers.get("birth").isNumber()) {
                row.put("birth_year", answers.get("birth").asInt());
            }
            putIfPresent(row, "gender", answers.get("gender"));
        }
        return row;
    }

    private static void putIfPresent(TableRow row, String column, JsonNode value) {
        if (value == null || value.isNull()) {
            return;
        }
        if (value.isArray()) {
            // Multi-select answers (e.g. "Other psych diagnoses") are comma-joined, matching the golden preview.
            List<String> parts = new ArrayList<>();
            for (JsonNode item : value) {
                parts.add(item.asText());
            }
            row.put(column, String.join(",", parts));
        } else {
            String text = value.asText();
            // An empty string is a provided-but-blank answer; keep it so the merge records "answered blank".
            row.put(column, text);
        }
    }

    private static String infoCreatedOn(FlattenContext ctx) {
        JsonNode info = ctx.getArchive().getJson("info.json");
        if (info == null || !info.hasNonNull("createdOn") || !info.get("createdOn").isTextual()) {
            return null;
        }
        return info.get("createdOn").asText();
    }
}
