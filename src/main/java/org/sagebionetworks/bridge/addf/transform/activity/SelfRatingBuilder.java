package org.sagebionetworks.bridge.addf.transform.activity;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.transform.AbstractActivityRowBuilder;
import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.FlattenContext;
import org.sagebionetworks.bridge.addf.transform.TableRow;

/**
 * {@code self_rating} builder — the {@code daily} assessment. {@code answers.json} carries integer ratings directly
 * ({@code energy}, {@code mood}, {@code thoughts}, {@code impulsiveness}, {@code attention}).
 */
@Component
public class SelfRatingBuilder extends AbstractActivityRowBuilder {
    private static final String[] ITEMS = { "energy", "mood", "thoughts", "impulsiveness", "attention" };

    @Override
    public String table() {
        return AddfTables.SELF_RATING;
    }

    @Override
    public String[] handledItems() {
        return new String[] { "daily", "self_rating", "self-rating" };
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
        for (String item : ITEMS) {
            row.put(item, intOrNull(answers, item));
        }
        return row;
    }
}
