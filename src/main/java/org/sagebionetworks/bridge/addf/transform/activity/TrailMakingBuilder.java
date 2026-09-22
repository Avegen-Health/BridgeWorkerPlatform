package org.sagebionetworks.bridge.addf.transform.activity;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.transform.AbstractActivityRowBuilder;
import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.FlattenContext;
import org.sagebionetworks.bridge.addf.transform.SummaryComputer;
import org.sagebionetworks.bridge.addf.transform.TableRow;

/**
 * {@code trail_making} builder. Reads {@code trailmaking.json} ({@code numberOfErrors}, {@code runtime},
 * {@code pauseInterval}, {@code startDate}, {@code taps[]}, {@code points[]}). {@code taps} and {@code points} are kept
 * as verbatim JSON text columns.
 */
@Component
public class TrailMakingBuilder extends AbstractActivityRowBuilder {
    static final String TRAILMAKING_JSON = "trailmaking.json";

    @Override
    public String table() {
        return AddfTables.TRAIL_MAKING;
    }

    @Override
    public String[] handledItems() {
        return new String[] { "Trail_Making", "trailmaking", "TrailMaking" };
    }

    @Override
    public TableRow build(FlattenContext ctx) {
        JsonNode tm = ctx.getArchive().getJson(TRAILMAKING_JSON);
        if (tm == null) {
            return null;
        }
        TableRow row = newRowWithCommon(ctx);

        String rawStart = tm.path("startDate").asText(null);
        row.put("created_on", AddfDateUtils.toUtcIso(rawStart));
        row.put("time_zone", AddfDateUtils.offsetFromIso(rawStart));

        row.put("number_of_errors", intOrNull(tm, "numberOfErrors"));
        row.put("runtime_sec", doubleOrNull(tm, "runtime"));
        row.put("pause_interval_sec", doubleOrNull(tm, "pauseInterval"));
        row.put("taps", SummaryComputer.jsonArrayToString(tm.get("taps")));
        row.put("points", SummaryComputer.jsonArrayToString(tm.get("points")));
        return row;
    }
}
