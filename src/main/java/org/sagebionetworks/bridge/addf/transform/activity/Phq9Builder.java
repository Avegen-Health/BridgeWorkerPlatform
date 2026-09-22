package org.sagebionetworks.bridge.addf.transform.activity;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.transform.AbstractActivityRowBuilder;
import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.FlattenContext;
import org.sagebionetworks.bridge.addf.transform.TableRow;

/**
 * {@code phq9} builder. The PHQ-9 archive's {@code answers.json} stores <b>string labels</b> ("Not at all", "Several
 * days", …); this maps them to the 0–3 numeric codes and computes {@code total_score} = sum of the 8 symptom items
 * (excluding {@code difficulty}), per data-contract §conventions.
 */
@Component
public class Phq9Builder extends AbstractActivityRowBuilder {
    // The 8 scored symptom items (difficulty is a functional-impairment item, NOT summed into total_score).
    private static final String[] SYMPTOMS = {
            "anhedonia", "depression", "sleep", "energy", "appetite", "discouragement", "concentration", "speed"
    };

    // Frequency scale 0–3 (VERIFY the exact label strings against the FAIR lookups sheet).
    private static final Map<String, Integer> FREQUENCY = ImmutableMap.of(
            "Not at all", 0,
            "Several days", 1,
            "More than half the days", 2,
            "Nearly every day", 3);

    // Difficulty has its own 0–3 scale.
    private static final Map<String, Integer> DIFFICULTY = ImmutableMap.of(
            "Not difficult at all", 0,
            "Somewhat difficult", 1,
            "Very difficult", 2,
            "Extremely difficult", 3);

    @Override
    public String table() {
        return AddfTables.PHQ9;
    }

    @Override
    public String[] handledItems() {
        return new String[] { "PHQ-9", "PHQ9" };
    }

    @Override
    public TableRow build(FlattenContext ctx) {
        JsonNode answers = answers(ctx);
        if (answers == null) {
            return null;
        }
        TableRow row = newRowWithCommon(ctx);

        String rawCreatedOn = infoCreatedOnRaw(ctx);
        row.put("created_on", AddfDateUtils.toUtcIso(rawCreatedOn));
        row.put("time_zone", AddfDateUtils.offsetFromIso(rawCreatedOn));

        int total = 0;
        boolean anyScored = false;
        for (String symptom : SYMPTOMS) {
            Integer code = codedInt(answers, symptom, FREQUENCY);
            row.put(symptom, code);
            if (code != null) {
                total += code;
                anyScored = true;
            }
        }
        row.put("difficulty", codedInt(answers, "difficulty", DIFFICULTY));
        row.put("total_score", anyScored ? total : null);
        return row;
    }
}
