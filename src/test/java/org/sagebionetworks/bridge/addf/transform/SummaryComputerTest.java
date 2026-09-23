package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class SummaryComputerTest {
    private SummaryComputer computer;

    @BeforeMethod
    public void before() {
        computer = new SummaryComputer();
    }

    private static JsonNode json(String s) throws Exception {
        return DefaultObjectMapper.INSTANCE.readTree(s);
    }

    // ---- keyboard --------------------------------------------------------------------------------------------

    @Test
    public void computeKeyboard_null() {
        SummaryComputer.KeyboardSummary s = computer.computeKeyboard(null);
        assertNull(s.totalKeys);
        assertNull(s.durationSec);
    }

    @Test
    public void computeKeyboard_notArray() throws Exception {
        SummaryComputer.KeyboardSummary s = computer.computeKeyboard(json("{\"foo\":1}"));
        assertNull(s.totalKeys);
    }

    @Test
    public void computeKeyboard_counts() throws Exception {
        JsonNode keylogs = json("[" +
                "{\"value\":\"alphabet\",\"duration\":0.1,\"distanceFromCenter\":1.0,\"timestamp\":100.0}," +
                "{\"value\":\"space\",\"duration\":0.3,\"distanceFromCenter\":3.0,\"timestamp\":102.0}," +
                "{\"value\":\"numeral\"},{\"value\":\"punctuation\"},{\"value\":\"symbol\"}," +
                "{\"value\":\"emoji\"},{\"value\":\"backspace\"},{\"value\":\"suggestion\"}," +
                "{\"value\":\"autocorrection\"},{\"value\":\"mystery\"}]");
        SummaryComputer.KeyboardSummary s = computer.computeKeyboard(keylogs);
        assertEquals(s.totalKeys, Integer.valueOf(10));
        assertEquals(s.nAlphabet, Integer.valueOf(1));
        assertEquals(s.nSpace, Integer.valueOf(1));
        assertEquals(s.nNumeral, Integer.valueOf(1));
        assertEquals(s.nPunctuation, Integer.valueOf(1));
        assertEquals(s.nSymbol, Integer.valueOf(1));
        assertEquals(s.nEmoji, Integer.valueOf(1));
        assertEquals(s.nBackspace, Integer.valueOf(1));
        assertEquals(s.nSuggestion, Integer.valueOf(1));
        assertEquals(s.nAutocorrection, Integer.valueOf(1));
        assertEquals(s.nOther, Integer.valueOf(1));
        assertEquals(s.meanHoldDuration, 0.2, 1e-9);
        assertEquals(s.medianHoldDuration, 0.2, 1e-9);
        assertEquals(s.meanDistFromCenter, 2.0, 1e-9);
        assertEquals(s.durationSec, 2.0, 1e-9);
        assertEquals(s.sessionStartEpoch, 100.0, 1e-9);
    }

    // ---- motion ----------------------------------------------------------------------------------------------

    @Test
    public void computeMotion_null() {
        SummaryComputer.MotionSummary s = computer.computeMotion(null);
        assertNull(s.sampleCount);
    }

    @Test
    public void computeMotion_itemsWrapperWithVectorMagnitude() throws Exception {
        JsonNode motion = json("{\"items\":[{\"vectorMagnitude\":3.0},{\"vectorMagnitude\":5.0}]}");
        SummaryComputer.MotionSummary s = computer.computeMotion(motion);
        assertEquals(s.sampleCount, Integer.valueOf(2));
        assertEquals(s.accelMagMean, 4.0, 1e-9);
        assertEquals(s.accelMagMax, 5.0, 1e-9);
    }

    @Test
    public void computeMotion_bareArrayWithXyzAndSkips() throws Exception {
        JsonNode motion = json("[{\"x\":3,\"y\":4,\"z\":0},{\"foo\":1}]");
        SummaryComputer.MotionSummary s = computer.computeMotion(motion);
        assertEquals(s.sampleCount, Integer.valueOf(1));
        assertEquals(s.accelMagMean, 5.0, 1e-9);
        assertEquals(s.accelMagMax, 5.0, 1e-9);
    }

    // ---- go/no-go --------------------------------------------------------------------------------------------

    @Test
    public void computeGoNoGo_null() {
        SummaryComputer.GoNoGoSummary s = computer.computeGoNoGo(null);
        assertNull(s.nTrials);
    }

    @Test
    public void computeGoNoGo_countsErrorsAndCompactsSamples() throws Exception {
        JsonNode results = json("[" +
                "{\"go\":true,\"incorrect\":false,\"timeToThreshold\":0.5,\"samples\":[{},{}]}," +
                "{\"go\":true,\"incorrect\":true,\"timeToThreshold\":0.4}," +
                "{\"go\":false,\"incorrect\":true}," +
                "{\"go\":false,\"incorrect\":false}]");
        SummaryComputer.GoNoGoSummary s = computer.computeGoNoGo(results);
        assertEquals(s.nTrials, Integer.valueOf(4));
        assertEquals(s.nGo, Integer.valueOf(2));
        assertEquals(s.nNoGo, Integer.valueOf(2));
        assertEquals(s.nCorrect, Integer.valueOf(2));
        assertEquals(s.omissionErrors, Integer.valueOf(1));
        assertEquals(s.commissionErrors, Integer.valueOf(1));
        assertEquals(s.meanReactionTime, 0.45, 1e-9);
        assertEquals(s.medianReactionTime, 0.45, 1e-9);
        assertTrue(s.resultsJson.contains("motion_sample_count"));
        // The heavy samples[] array must be dropped from the compacted results.
        assertTrue(!s.resultsJson.contains("\"samples\""));
    }

    // ---- stats + helpers -------------------------------------------------------------------------------------

    @Test
    public void mean_empty() {
        assertNull(SummaryComputer.mean(Collections.<Double>emptyList()));
    }

    @Test
    public void mean_value() {
        assertEquals(SummaryComputer.mean(Arrays.asList(2.0, 4.0)), 3.0, 1e-9);
    }

    @Test
    public void median_empty() {
        assertNull(SummaryComputer.median(Collections.<Double>emptyList()));
    }

    @Test
    public void median_odd() {
        assertEquals(SummaryComputer.median(Arrays.asList(3.0, 1.0, 2.0)), 2.0, 1e-9);
    }

    @Test
    public void median_even() {
        assertEquals(SummaryComputer.median(Arrays.asList(1.0, 2.0, 3.0, 4.0)), 2.5, 1e-9);
    }

    @Test
    public void jsonArrayToString_null() {
        assertNull(SummaryComputer.jsonArrayToString(null));
    }

    @Test
    public void jsonArrayToString_nullNode() {
        assertNull(SummaryComputer.jsonArrayToString(NullNode.getInstance()));
    }

    @Test
    public void jsonArrayToString_array() throws Exception {
        assertEquals(SummaryComputer.jsonArrayToString(json("[1,2,3]")), "[1,2,3]");
    }

    @Test
    public void fieldNames_null() {
        assertTrue(!SummaryComputer.fieldNames(null).hasNext());
    }

    @Test
    public void fieldNames_object() throws Exception {
        assertEquals(SummaryComputer.fieldNames(json("{\"a\":1}")).next(), "a");
    }
}
