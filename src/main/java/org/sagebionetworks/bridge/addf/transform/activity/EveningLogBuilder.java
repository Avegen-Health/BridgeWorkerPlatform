package org.sagebionetworks.bridge.addf.transform.activity;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.transform.AbstractActivityRowBuilder;
import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.FlattenContext;
import org.sagebionetworks.bridge.addf.transform.TableRow;

/**
 * {@code evening_log} builder. {@code answers.json} carries integer ratings directly ({@code mood}, {@code fatigue},
 * {@code fidgeting}, {@code energy}, {@code speech}, {@code irritability}).
 */
@Component
public class EveningLogBuilder extends AbstractActivityRowBuilder {
    private static final String[] ITEMS = { "mood", "fatigue", "fidgeting", "energy", "speech", "irritability" };

    @Override
    public String table() {
        return AddfTables.EVENING_LOG;
    }

    @Override
    public String[] handledItems() {
        return new String[] { "Evening_Log", "evening_log", "EveningLog" };
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
