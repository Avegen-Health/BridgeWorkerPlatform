package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

/**
 * The degenerate payloads {@link SummaryComputer} has to survive. Every column it produces is a <em>derived</em>
 * number, so the failure mode that matters is not an exception — it is a plausible-looking wrong value, or a zero
 * where a null belongs. A participant with no keypresses is not a participant with zero mean hold duration, and an
 * accelerometer stream we cannot read is not a stream of magnitude zero.
 */
public class SummaryComputerEdgeCasesTest {
    private SummaryComputer computer;

    @BeforeMethod
    public void before() {
        computer = new SummaryComputer();
    }

    private static JsonNode json(String text) throws IOException {
        return DefaultObjectMapper.INSTANCE.readTree(text);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Keyboard
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void nullKeylogsYieldsAnEmptySummaryNotZeroes() {
        SummaryComputer.KeyboardSummary summary = computer.computeKeyboard(null);

        // Absent, not zero: a session we could not read must not look like a session with no typing in it.
        assertNull(summary.totalKeys);
        assertNull(summary.meanHoldDuration);
        assertNull(summary.durationSec);
        assertNull(summary.sessionStartEpoch);
    }

    @Test
    public void nonArrayKeylogsYieldsAnEmptySummary() throws Exception {
        SummaryComputer.KeyboardSummary summary = computer.computeKeyboard(json("{\"not\":\"an array\"}"));
        assertNull(summary.totalKeys);
    }

    @Test
    public void emptyKeylogsCountsZeroKeysButLeavesStatisticsNull() throws Exception {
        SummaryComputer.KeyboardSummary summary = computer.computeKeyboard(json("[]"));

        // Here zero IS the truth for the counts — the array was readable and had no entries — but a mean over no
        // samples has no value, so the statistics stay null rather than collapsing to 0.0.
        assertEquals(summary.totalKeys, Integer.valueOf(0));
        assertEquals(summary.nAlphabet, Integer.valueOf(0));
        assertNull(summary.meanHoldDuration);
        assertNull(summary.medianHoldDuration);
        assertNull(summary.meanDistFromCenter);
        assertNull(summary.durationSec);
    }

    @Test
    public void keylogsMissingDurationOrDistanceAreExcludedFromTheirStatistics() throws Exception {
        // A keypress with no recorded hold time must not be counted as a zero-length hold — that would drag the mean
        // down and silently under-report typing dynamics.
        SummaryComputer.KeyboardSummary summary = computer.computeKeyboard(json("["
                + "{\"value\":\"alphabet\",\"timestamp\":100.0,\"duration\":0.10,\"distanceFromCenter\":4.0},"
                + "{\"value\":\"alphabet\",\"timestamp\":101.0},"
                + "{\"value\":\"alphabet\",\"timestamp\":102.0,\"duration\":0.20}]"));

        assertEquals(summary.totalKeys, Integer.valueOf(3));
        assertEquals(summary.meanHoldDuration, 0.15); // mean of the two that had one, not of three
        assertEquals(summary.meanDistFromCenter, 4.0);
        assertEquals(summary.durationSec, 2.0);
        assertEquals(summary.sessionStartEpoch, 100.0);
    }

    @Test
    public void everyKeyCategoryIsCountedAndUnknownOnesFallToOther() throws Exception {
        SummaryComputer.KeyboardSummary summary = computer.computeKeyboard(json("["
                + "{\"value\":\"alphabet\"},{\"value\":\"numeral\"},{\"value\":\"punctuation\"},"
                + "{\"value\":\"symbol\"},{\"value\":\"emoji\"},{\"value\":\"backspace\"},"
                + "{\"value\":\"space\"},{\"value\":\"suggestion\"},{\"value\":\"autocorrection\"},"
                + "{\"value\":\"something-new\"},{}]"));

        assertEquals(summary.totalKeys, Integer.valueOf(11));
        assertEquals(summary.nAlphabet, Integer.valueOf(1));
        assertEquals(summary.nNumeral, Integer.valueOf(1));
        assertEquals(summary.nPunctuation, Integer.valueOf(1));
        assertEquals(summary.nSymbol, Integer.valueOf(1));
        assertEquals(summary.nEmoji, Integer.valueOf(1));
        assertEquals(summary.nBackspace, Integer.valueOf(1));
        assertEquals(summary.nSpace, Integer.valueOf(1));
        assertEquals(summary.nSuggestion, Integer.valueOf(1));
        assertEquals(summary.nAutocorrection, Integer.valueOf(1));
        // A key category the app adds later must land in n_other, not be dropped — the counts have to keep summing
        // to total_keys or every downstream proportion is wrong.
        assertEquals(summary.nOther, Integer.valueOf(2));
        int summed = summary.nAlphabet + summary.nNumeral + summary.nPunctuation + summary.nSymbol + summary.nEmoji
                + summary.nBackspace + summary.nSpace + summary.nSuggestion + summary.nAutocorrection
                + summary.nOther;
        assertEquals(summed, summary.totalKeys.intValue());
    }

    // -----------------------------------------------------------------------------------------------------------
    // Motion
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void nullOrUnreadableMotionYieldsNoAccelerometerColumns() throws Exception {
        for (JsonNode motion : new JsonNode[] { null, json("{\"unexpected\":true}"), json("\"a string\"") }) {
            SummaryComputer.MotionSummary summary = computer.computeMotion(motion);
            assertNull(summary.accelMagMean);
            assertNull(summary.accelMagMax);
        }
    }

    @Test
    public void motionAcceptsBothABareArrayAndAnItemsWrapper() throws Exception {
        SummaryComputer.MotionSummary bare = computer.computeMotion(json("[{\"vectorMagnitude\":1.0}]"));
        SummaryComputer.MotionSummary wrapped = computer.computeMotion(
                json("{\"items\":[{\"vectorMagnitude\":1.0}]}"));

        assertEquals(bare.sampleCount, Integer.valueOf(1));
        assertEquals(wrapped.sampleCount, Integer.valueOf(1));
        assertEquals(bare.accelMagMean, wrapped.accelMagMean);
    }

    @Test
    public void magnitudeIsDerivedFromXyzWhenNotPreComputed() throws Exception {
        // 3-4-5 triangle plus a z of 0 -> magnitude 5. Samples missing a component are skipped, not treated as zero.
        SummaryComputer.MotionSummary summary = computer.computeMotion(json("["
                + "{\"x\":3.0,\"y\":4.0,\"z\":0.0},"
                + "{\"x\":1.0,\"y\":1.0},"
                + "{\"vectorMagnitude\":7.0}]"));

        assertEquals(summary.sampleCount, Integer.valueOf(2), "the incomplete sample must not be counted");
        assertEquals(summary.accelMagMax, 7.0);
        assertEquals(summary.accelMagMean, 6.0);
    }

    @Test
    public void motionWithNoUsableSamplesReportsZeroCountAndNullStatistics() throws Exception {
        SummaryComputer.MotionSummary summary = computer.computeMotion(json("[{\"unrelated\":1}]"));

        assertEquals(summary.sampleCount, Integer.valueOf(0));
        assertNull(summary.accelMagMean);
        // Not NEGATIVE_INFINITY, which is what an unguarded max over an empty set would leave behind.
        assertNull(summary.accelMagMax);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Go/No-Go
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void nullOrNonArrayResultsYieldsAnEmptyGoNoGoSummary() throws Exception {
        for (JsonNode results : new JsonNode[] { null, json("{\"not\":\"array\"}") }) {
            SummaryComputer.GoNoGoSummary summary = computer.computeGoNoGo(results);
            assertNull(summary.nTrials);
            assertNull(summary.meanReactionTime);
            assertNull(summary.resultsJson);
        }
    }

    @Test
    public void nonPositiveReactionTimesAreExcluded() throws Exception {
        // A zero or negative time-to-threshold is a sentinel for "no response recorded", not a superhumanly fast
        // one. Averaging it in would understate reaction time for exactly the participants who struggled.
        SummaryComputer.GoNoGoSummary summary = computer.computeGoNoGo(json("["
                + "{\"go\":true,\"incorrect\":false,\"timeToThreshold\":0.4},"
                + "{\"go\":true,\"incorrect\":false,\"timeToThreshold\":0.0},"
                + "{\"go\":true,\"incorrect\":false,\"timeToThreshold\":-1.0},"
                + "{\"go\":true,\"incorrect\":false},"
                + "{\"go\":true,\"incorrect\":false,\"timeToThreshold\":0.6}]"));

        assertEquals(summary.nTrials, Integer.valueOf(5));
        assertEquals(summary.meanReactionTime, 0.5);
        assertEquals(summary.medianReactionTime, 0.5);
    }

    @Test
    public void noGoTrialReactionTimesAreNotCounted() throws Exception {
        // Reaction time is only meaningful on a go trial; a no-go trial's timing is a commission error's latency.
        SummaryComputer.GoNoGoSummary summary = computer.computeGoNoGo(json("["
                + "{\"go\":false,\"incorrect\":true,\"timeToThreshold\":0.9},"
                + "{\"go\":true,\"incorrect\":false,\"timeToThreshold\":0.3}]"));

        assertEquals(summary.meanReactionTime, 0.3);
        assertEquals(summary.commissionErrors, Integer.valueOf(1));
        assertEquals(summary.omissionErrors, Integer.valueOf(0));
    }

    @Test
    public void goTrialMissIsAnOmissionAndNoGoResponseIsACommission() throws Exception {
        SummaryComputer.GoNoGoSummary summary = computer.computeGoNoGo(json("["
                + "{\"go\":true,\"incorrect\":true},"
                + "{\"go\":false,\"incorrect\":true},"
                + "{\"go\":true,\"incorrect\":false},"
                + "{\"go\":false,\"incorrect\":false}]"));

        assertEquals(summary.nTrials, Integer.valueOf(4));
        assertEquals(summary.nGo, Integer.valueOf(2));
        assertEquals(summary.nNoGo, Integer.valueOf(2));
        assertEquals(summary.nCorrect, Integer.valueOf(2));
        assertEquals(summary.omissionErrors, Integer.valueOf(1), "a missed go stimulus is an omission");
        assertEquals(summary.commissionErrors, Integer.valueOf(1), "a response to a no-go stimulus is a commission");
    }

    @Test
    public void trialsWithoutSamplesStillGetAMotionSampleCountOfZero() throws Exception {
        // The results JSON is the per-trial detail researchers read; every trial must carry the column, or a
        // consumer cannot tell "no motion recorded" from "this field does not exist here".
        SummaryComputer.GoNoGoSummary summary = computer.computeGoNoGo(json("["
                + "{\"go\":true,\"incorrect\":false,\"samples\":[{},{},{}]},"
                + "{\"go\":true,\"incorrect\":false},"
                + "{\"go\":true,\"incorrect\":false,\"samples\":\"not-an-array\"}]"));

        JsonNode compacted = json(summary.resultsJson);
        assertEquals(compacted.size(), 3);
        assertEquals(compacted.get(0).get("motion_sample_count").asInt(), 3);
        assertEquals(compacted.get(1).get("motion_sample_count").asInt(), 0);
        assertEquals(compacted.get(2).get("motion_sample_count").asInt(), 0);
        // And the heavy samples array is gone from every trial — that is the whole point of the compaction.
        for (JsonNode trial : compacted) {
            assertTrue(trial.get("samples") == null, "samples[] must be stripped: " + trial);
        }
    }

    // -----------------------------------------------------------------------------------------------------------
    // Statistics helpers
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void meanAndMedianOverNoValuesAreNullNotZero() {
        assertNull(SummaryComputer.mean(ImmutableList.<Double>of()));
        assertNull(SummaryComputer.median(ImmutableList.<Double>of()));
    }

    @Test
    public void medianHandlesBothOddAndEvenCounts() {
        assertEquals(SummaryComputer.median(ImmutableList.of(3.0, 1.0, 2.0)), 2.0);
        assertEquals(SummaryComputer.median(ImmutableList.of(4.0, 1.0, 3.0, 2.0)), 2.5);
    }

    @Test
    public void roundingLeavesNullAndNonFiniteValuesAlone() {
        assertNull(SummaryComputer.round(null, SummaryComputer.TIME_SCALE));
        assertTrue(SummaryComputer.round(Double.NaN, SummaryComputer.TIME_SCALE).isNaN());
        assertTrue(SummaryComputer.round(Double.POSITIVE_INFINITY, SummaryComputer.TIME_SCALE).isInfinite());
        // BigDecimal.valueOf would throw on either of those, so the guard is load-bearing rather than defensive.
    }

    @Test
    public void roundingUsesTheDeliveredScales() {
        assertEquals(SummaryComputer.round(0.0623822510, SummaryComputer.TIME_SCALE), 0.06238);
        assertEquals(SummaryComputer.round(10.8053207739, SummaryComputer.DISTANCE_SCALE), 10.8053);
        assertEquals(SummaryComputer.TIME_SCALE, 5);
        assertEquals(SummaryComputer.DISTANCE_SCALE, 4);
    }

    @Test
    public void jsonArrayToStringIsNullSafe() throws Exception {
        assertNull(SummaryComputer.jsonArrayToString(null));
        assertNull(SummaryComputer.jsonArrayToString(json("null")));
        assertEquals(SummaryComputer.jsonArrayToString(json("[1,2]")), "[1,2]");
    }
}
