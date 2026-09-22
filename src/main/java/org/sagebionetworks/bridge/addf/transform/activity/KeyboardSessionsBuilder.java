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
 * {@code keyboard_sessions} builder. Reads {@code Session.json} ({@code keylogs[]}) for the typing summaries and
 * {@code motion.json} for the accelerometer summaries. The {@code keylogs} column holds the verbatim keylogs array.
 * Keyboard timestamps are Unix epoch seconds with no offset, so {@code session_start} is UTC-derived and
 * {@code time_zone} is left null (matching the golden preview).
 */
@Component
public class KeyboardSessionsBuilder extends AbstractActivityRowBuilder {
    static final String SESSION_JSON = "Session.json";
    static final String MOTION_JSON = "motion.json";

    private SummaryComputer summaryComputer;

    @Autowired
    public final void setSummaryComputer(SummaryComputer summaryComputer) {
        this.summaryComputer = summaryComputer;
    }

    @Override
    public String table() {
        return AddfTables.KEYBOARD_SESSIONS;
    }

    @Override
    public String[] handledItems() {
        return new String[] { "KeyboardSession", "keyboard", "Keyboard" };
    }

    @Override
    public TableRow build(FlattenContext ctx) {
        JsonNode session = ctx.getArchive().getJson(SESSION_JSON);
        if (session == null) {
            return null;
        }
        JsonNode keylogs = session.get("keylogs");
        TableRow row = newRowWithCommon(ctx);

        SummaryComputer.KeyboardSummary k = summaryComputer.computeKeyboard(keylogs);
        row.put("session_start", AddfDateUtils.epochSecondsToUtcIso(k.sessionStartEpoch));
        row.put("time_zone", null); // epoch timestamps carry no offset
        row.put("duration_sec", k.durationSec);
        row.put("total_keys", k.totalKeys);
        row.put("n_alphabet", k.nAlphabet);
        row.put("n_numeral", k.nNumeral);
        row.put("n_punctuation", k.nPunctuation);
        row.put("n_symbol", k.nSymbol);
        row.put("n_emoji", k.nEmoji);
        row.put("n_backspace", k.nBackspace);
        row.put("n_space", k.nSpace);
        row.put("n_suggestion", k.nSuggestion);
        row.put("n_autocorrection", k.nAutocorrection);
        row.put("n_other", k.nOther);
        row.put("mean_hold_duration", k.meanHoldDuration);
        row.put("median_hold_duration", k.medianHoldDuration);
        row.put("mean_dist_from_center", k.meanDistFromCenter);

        SummaryComputer.MotionSummary m = summaryComputer.computeMotion(ctx.getArchive().getJson(MOTION_JSON));
        row.put("motion_sample_count", m.sampleCount);
        row.put("accel_mag_mean", m.accelMagMean);
        row.put("accel_mag_max", m.accelMagMax);

        row.put("keylogs", SummaryComputer.jsonArrayToString(keylogs));
        row.put("device_name", ctx.getClientInfo().getDeviceName());
        return row;
    }
}
