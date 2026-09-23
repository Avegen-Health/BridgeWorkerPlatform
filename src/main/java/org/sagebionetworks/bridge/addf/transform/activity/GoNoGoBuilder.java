package org.sagebionetworks.bridge.addf.transform.activity;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.transform.AbstractActivityRowBuilder;
import org.sagebionetworks.bridge.addf.transform.AddfDateUtils;
import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.FlattenContext;
import org.sagebionetworks.bridge.addf.transform.SummaryComputer;
import org.sagebionetworks.bridge.addf.transform.TableRow;

/**
 * {@code go_no_go} builder. Reads {@code gonogo.json} ({@code identifier}, {@code startDate}, {@code results[]}),
 * computes trial summaries via {@link SummaryComputer}, and stores the {@code results} JSON column with each trial's
 * heavy {@code samples[]} array replaced by {@code motion_sample_count} (§3.5). {@code motion.json} is kept in the raw
 * archive.
 */
@Component
public class GoNoGoBuilder extends AbstractActivityRowBuilder {
    static final String GONOGO_JSON = "gonogo.json";

    private SummaryComputer summaryComputer;

    @Autowired
    public final void setSummaryComputer(SummaryComputer summaryComputer) {
        this.summaryComputer = summaryComputer;
    }

    @Override
    public String table() {
        return AddfTables.GO_NO_GO;
    }

    @Override
    public String[] handledItems() {
        return new String[] { "Go-No-Go", "gonogo", "Go_No_Go", "GoNoGo" };
    }

    @Override
    public TableRow build(FlattenContext ctx) {
        JsonNode gonogo = ctx.getArchive().getJson(GONOGO_JSON);
        if (gonogo == null) {
            return null;
        }
        TableRow row = newRowWithCommon(ctx);

        String rawStart = gonogo.path("startDate").asText(null);
        row.put("created_on", AddfDateUtils.toUtcIso(rawStart));
        row.put("time_zone", AddfDateUtils.offsetFromIso(rawStart));
        row.put("identifier", gonogo.path("identifier").asText(null));

        SummaryComputer.GoNoGoSummary s = summaryComputer.computeGoNoGo(gonogo.get("results"));
        row.put("n_trials", s.nTrials);
        row.put("n_go", s.nGo);
        row.put("n_nogo", s.nNoGo);
        row.put("n_correct", s.nCorrect);
        row.put("commission_errors", s.commissionErrors);
        row.put("omission_errors", s.omissionErrors);
        row.put("mean_reaction_time", s.meanReactionTime);
        row.put("median_reaction_time", s.medianReactionTime);
        row.put("results", s.resultsJson);
        return row;
    }
}
