package org.sagebionetworks.bridge.addf.transform;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

/**
 * ADDF §3.5 — pre-computed summary columns, calculated at accumulate time while the archive is decrypted in hand
 * (cheapest there; publish only concatenates). Covers the keyboard typing summaries, the accelerometer
 * ({@code motion.json}) summaries, and the Go/No-Go trial summaries.
 *
 * <p>Key-name assumptions were taken from the sample archives + golden previews and are flagged VERIFY where the
 * inner JSON layout was not directly observed (e.g. {@code motion.json}); the Phase 7 golden tests pin them.</p>
 */
@Component
public class SummaryComputer {

    // ---- Keyboard (Session.json keylogs[]) -------------------------------------------------------------------

    /** Per-{@code value} categories emitted as the {@code n_*} columns (the new KeyType set, data-contract §keyboard). */
    public static final class KeyboardSummary {
        public Integer totalKeys;
        public Integer nAlphabet, nNumeral, nPunctuation, nSymbol, nEmoji, nBackspace, nSpace, nSuggestion,
                nAutocorrection, nOther;
        public Double meanHoldDuration, medianHoldDuration, meanDistFromCenter, durationSec;
        public Double sessionStartEpoch; // earliest keylog timestamp (epoch seconds), for session_start
    }

    public KeyboardSummary computeKeyboard(JsonNode keylogs) {
        KeyboardSummary s = new KeyboardSummary();
        if (keylogs == null || !keylogs.isArray()) {
            return s;
        }
        int total = 0, alpha = 0, num = 0, punct = 0, sym = 0, emoji = 0, back = 0, space = 0, sugg = 0, auto = 0,
                other = 0;
        List<Double> holds = new ArrayList<>();
        List<Double> dists = new ArrayList<>();
        Double minTs = null, maxTs = null;
        for (JsonNode k : keylogs) {
            total++;
            String value = k.path("value").asText("");
            switch (value) {
                case "alphabet": alpha++; break;
                case "numeral": num++; break;
                case "punctuation": punct++; break;
                case "symbol": sym++; break;
                case "emoji": emoji++; break;
                case "backspace": back++; break;
                case "space": space++; break;
                case "suggestion": sugg++; break;
                case "autocorrection": auto++; break;
                default: other++; break;
            }
            if (k.hasNonNull("duration")) {
                holds.add(k.get("duration").asDouble());
            }
            if (k.hasNonNull("distanceFromCenter")) {
                dists.add(k.get("distanceFromCenter").asDouble());
            }
            if (k.hasNonNull("timestamp")) {
                double ts = k.get("timestamp").asDouble();
                minTs = (minTs == null) ? ts : Math.min(minTs, ts);
                maxTs = (maxTs == null) ? ts : Math.max(maxTs, ts);
            }
        }
        s.totalKeys = total;
        s.nAlphabet = alpha;
        s.nNumeral = num;
        s.nPunctuation = punct;
        s.nSymbol = sym;
        s.nEmoji = emoji;
        s.nBackspace = back;
        s.nSpace = space;
        s.nSuggestion = sugg;
        s.nAutocorrection = auto;
        s.nOther = other;
        s.meanHoldDuration = round(mean(holds), TIME_SCALE);
        s.medianHoldDuration = round(median(holds), TIME_SCALE);
        s.meanDistFromCenter = round(mean(dists), DISTANCE_SCALE);
        if (minTs != null && maxTs != null) {
            s.durationSec = maxTs - minTs;
            s.sessionStartEpoch = minTs;
        }
        return s;
    }

    // ---- Motion (motion.json accelerometer samples) ----------------------------------------------------------

    public static final class MotionSummary {
        public Integer sampleCount;
        public Double accelMagMean;
        public Double accelMagMax;
    }

    /**
     * VERIFY against a real {@code motion.json}: samples are treated as an array of objects carrying either a
     * pre-computed {@code vectorMagnitude} (as seen in the Go/No-Go per-trial {@code samples[]}) or raw {@code x/y/z}
     * from which the magnitude is derived.
     */
    public MotionSummary computeMotion(JsonNode motion) {
        MotionSummary s = new MotionSummary();
        JsonNode samples = motion;
        if (motion != null && motion.isObject() && motion.has("items") && motion.get("items").isArray()) {
            samples = motion.get("items");
        }
        if (samples == null || !samples.isArray()) {
            return s;
        }
        int count = 0;
        double sum = 0, max = Double.NEGATIVE_INFINITY;
        for (JsonNode sample : samples) {
            Double mag = magnitude(sample);
            if (mag == null) {
                continue;
            }
            count++;
            sum += mag;
            max = Math.max(max, mag);
        }
        s.sampleCount = count;
        if (count > 0) {
            s.accelMagMean = round(sum / count, TIME_SCALE);
            s.accelMagMax = round(max, TIME_SCALE);
        }
        return s;
    }

    private static Double magnitude(JsonNode sample) {
        if (sample == null) {
            return null;
        }
        if (sample.hasNonNull("vectorMagnitude")) {
            return sample.get("vectorMagnitude").asDouble();
        }
        if (sample.hasNonNull("x") && sample.hasNonNull("y") && sample.hasNonNull("z")) {
            double x = sample.get("x").asDouble();
            double y = sample.get("y").asDouble();
            double z = sample.get("z").asDouble();
            return Math.sqrt(x * x + y * y + z * z);
        }
        return null;
    }

    // ---- Go/No-Go (gonogo.json results[]) --------------------------------------------------------------------

    public static final class GoNoGoSummary {
        public Integer nTrials, nGo, nNoGo, nCorrect, commissionErrors, omissionErrors;
        public Double meanReactionTime, medianReactionTime;
        /** results[] with each trial's {@code samples[]} replaced by {@code motion_sample_count}. */
        public String resultsJson;
    }

    public GoNoGoSummary computeGoNoGo(JsonNode results) {
        GoNoGoSummary s = new GoNoGoSummary();
        if (results == null || !results.isArray()) {
            return s;
        }
        int nTrials = 0, nGo = 0, nNoGo = 0, nCorrect = 0, commission = 0, omission = 0;
        List<Double> reactionTimes = new ArrayList<>();
        ArrayNode compact = DefaultObjectMapper.INSTANCE.createArrayNode();

        for (JsonNode trial : results) {
            nTrials++;
            boolean go = trial.path("go").asBoolean(false);
            boolean incorrect = trial.path("incorrect").asBoolean(false);
            if (go) {
                nGo++;
            } else {
                nNoGo++;
            }
            if (!incorrect) {
                nCorrect++;
            } else if (go) {
                // Failed to respond to a go stimulus.
                omission++;
            } else {
                // Responded to a no-go stimulus.
                commission++;
            }
            if (go && trial.hasNonNull("timeToThreshold")) {
                double rt = trial.get("timeToThreshold").asDouble();
                if (rt > 0) {
                    reactionTimes.add(rt);
                }
            }

            // Compact the trial: drop the heavy samples[] array, keep its count.
            ObjectNode compactTrial = ((ObjectNode) trial.deepCopy());
            JsonNode samples = compactTrial.remove("samples");
            int sampleCount = (samples != null && samples.isArray()) ? samples.size() : 0;
            compactTrial.put("motion_sample_count", sampleCount);
            compact.add(compactTrial);
        }

        s.nTrials = nTrials;
        s.nGo = nGo;
        s.nNoGo = nNoGo;
        s.nCorrect = nCorrect;
        s.commissionErrors = commission;
        s.omissionErrors = omission;
        s.meanReactionTime = round(mean(reactionTimes), TIME_SCALE);
        s.medianReactionTime = round(median(reactionTimes), TIME_SCALE);
        s.resultsJson = compact.toString();
        return s;
    }

    // ---- Stats helpers ---------------------------------------------------------------------------------------

    /**
     * Decimal places the delivered data keeps for a <b>derived</b> statistic. Read off the golden tables rather than
     * chosen: durations and accelerometer magnitudes are stored at 5 dp ({@code mean_hold_duration} = {@code 0.06238},
     * {@code mean_reaction_time} = {@code 0.30514}, {@code accel_mag_max} = {@code 1.05706}), on-screen distances at 4
     * ({@code mean_dist_from_center} = {@code 10.8053}) — consistent across every delivered row. Values passed
     * straight through from the payload ({@code duration_sec}, {@code runtime_sec}, the JSON columns) keep full
     * precision; only what we compute here is rounded. Pinned by {@code GoldenFlattenerTest}.
     */
    static final int TIME_SCALE = 5;

    static final int DISTANCE_SCALE = 4;

    /** Round a derived statistic to a delivered scale. Null-safe; non-finite values pass through untouched. */
    static Double round(Double value, int scale) {
        if (value == null || value.isNaN() || value.isInfinite()) {
            return value;
        }
        return BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP).doubleValue();
    }

    static Double mean(List<Double> values) {
        if (values.isEmpty()) {
            return null;
        }
        double sum = 0;
        for (double v : values) {
            sum += v;
        }
        return sum / values.size();
    }

    static Double median(List<Double> values) {
        if (values.isEmpty()) {
            return null;
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int n = sorted.size();
        if (n % 2 == 1) {
            return sorted.get(n / 2);
        }
        return (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }

    /** Serialise a JsonNode array verbatim to a compact string for a JSON text column; null-safe. */
    public static String jsonArrayToString(JsonNode array) {
        if (array == null || array.isNull()) {
            return null;
        }
        return array.toString();
    }

    /** Iterate an object node's fields (small helper to keep builders terse). */
    static Iterator<String> fieldNames(JsonNode node) {
        return node == null ? Collections.<String>emptyIterator() : node.fieldNames();
    }
}
