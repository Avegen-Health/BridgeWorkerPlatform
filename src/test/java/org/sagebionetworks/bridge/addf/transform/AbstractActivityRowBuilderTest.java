package org.sagebionetworks.bridge.addf.transform;

import static org.sagebionetworks.bridge.addf.transform.AddfTestFixtures.node;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

/**
 * The shared plumbing every activity builder inherits. Its coercion helpers all answer the same question — "is this
 * payload field usable?" — and they all answer it by returning {@code null} rather than a default. That choice is the
 * point: a survey item the participant did not answer must arrive as a null, never as a 0, because a 0 is a valid
 * PHQ-9 response meaning "not at all". Anything that silently coerces an absent answer into a real one corrupts the
 * clinical signal rather than losing it, which is far worse.
 */
public class AbstractActivityRowBuilderTest {
    private static final Map<String, Integer> LABELS = ImmutableMap.of("Not at all", 0, "Several days", 1);

    /** A minimal concrete builder, so the abstract helpers can be exercised directly. */
    private static final class TestBuilder extends AbstractActivityRowBuilder {
        @Override
        public String table() {
            return AddfTables.PHQ9;
        }

        @Override
        public String[] handledItems() {
            return new String[] { "test-item" };
        }

        @Override
        public TableRow build(FlattenContext ctx) {
            return newRowWithCommon(ctx);
        }

        // Expose the protected helpers for direct assertion.
        String createdOn(FlattenContext ctx) {
            return infoCreatedOnRaw(ctx);
        }

        JsonNode answersOf(FlattenContext ctx) {
            return answers(ctx);
        }

        Integer coded(JsonNode answers, String field) {
            return codedInt(answers, field, LABELS);
        }

        Integer asInt(JsonNode node, String field) {
            return intOrNull(node, field);
        }

        Double asDouble(JsonNode node, String field) {
            return doubleOrNull(node, field);
        }
    }

    private final TestBuilder builder = new TestBuilder();

    private static FlattenContext ctxWith(Map<String, JsonNode> files) {
        return AddfTestFixtures.context(files);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Common columns
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void commonColumnsComeFromTheContextNotThePayload() {
        TableRow row = builder.build(ctxWith(new HashMap<String, JsonNode>()));

        assertEquals(row.get("record_id"), AddfTestFixtures.RECORD_ID);
        assertEquals(row.get("health_code"), AddfTestFixtures.HEALTH_CODE);
        assertEquals(row.get("participant_version"), 3);
        assertEquals(row.get("app_version"), "68");
        assertEquals(row.get("platform"), "ios");
        assertEquals(row.get("uploaded_on"), AddfTestFixtures.UPLOADED_ON);
        assertEquals(row.get("is_test"), Boolean.FALSE);
    }

    @Test
    public void isTestCarriesTheGateVerdictThrough() {
        // The whole point of exporting a test participant at all (in non-prod) is that the row is labelled, so a
        // consumer can filter it. A hardcoded false here would deliver test data indistinguishable from real data.
        TableRow row = builder.build(AddfTestFixtures.context(new HashMap<String, JsonNode>(), 3, true));
        assertEquals(row.get("is_test"), Boolean.TRUE);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Payload access
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void createdOnIsNullWhenInfoJsonIsAbsentOrUnusable() {
        assertNull(builder.createdOn(ctxWith(new HashMap<String, JsonNode>())));
        assertNull(builder.createdOn(ctxWith(files("info.json", "{}"))));
        assertNull(builder.createdOn(ctxWith(files("info.json", "{\"createdOn\":null}"))));
        // A numeric createdOn is an epoch, handled elsewhere; this helper only returns offset-bearing ISO strings.
        assertNull(builder.createdOn(ctxWith(files("info.json", "{\"createdOn\":1787223907}"))));
    }

    @Test
    public void createdOnIsReturnedVerbatimSoTheOffsetSurvives() {
        // Returned raw, not normalised: time_zone is recovered from this string's offset, so converting it to UTC
        // here would destroy the only capture-time zone signal the payload carries.
        String raw = builder.createdOn(ctxWith(files("info.json",
                "{\"createdOn\":\"2026-08-15T16:00:47.984-04:00\"}")));
        assertEquals(raw, "2026-08-15T16:00:47.984-04:00");
    }

    @Test
    public void answersIsNullWhenTheArchiveHasNoAnswersFile() {
        assertNull(builder.answersOf(ctxWith(new HashMap<String, JsonNode>())));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Coercion: absent must stay absent
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void codedIntAcceptsBothANumericCodeAndAKnownLabel() {
        JsonNode answers = node("{\"a\":2,\"b\":\"Several days\"}");
        assertEquals(builder.coded(answers, "a"), Integer.valueOf(2));
        assertEquals(builder.coded(answers, "b"), Integer.valueOf(1));
    }

    @Test
    public void codedIntReturnsNullForAnUnknownLabelRatherThanZero() {
        // If the app ships a new answer label, the honest outcome is a null the FAIR/golden tests will surface — not
        // a 0, which reads as a real "not at all" response and silently biases every downstream score.
        assertEquals(builder.coded(node("{\"a\":\"Brand new option\"}"), "a"), null);
    }

    @Test
    public void codedIntReturnsNullForAbsentNullOrWrongTypedAnswers() {
        assertNull(builder.coded(null, "a"));
        assertNull(builder.coded(node("{}"), "a"));
        assertNull(builder.coded(node("{\"a\":null}"), "a"));
        assertNull(builder.coded(node("{\"a\":[1,2]}"), "a"));
        assertNull(builder.coded(node("{\"a\":true}"), "a"));
    }

    @Test
    public void intOrNullAndDoubleOrNullOnlyAcceptNumbers() {
        JsonNode node = node("{\"n\":7,\"d\":1.5,\"s\":\"7\",\"b\":true,\"nul\":null}");

        assertEquals(builder.asInt(node, "n"), Integer.valueOf(7));
        assertEquals(builder.asDouble(node, "d"), 1.5);

        // A numeric-looking string is not a number: silently parsing it would mask a payload-shape change.
        assertNull(builder.asInt(node, "s"));
        assertNull(builder.asDouble(node, "s"));
        assertNull(builder.asInt(node, "b"));
        assertNull(builder.asDouble(node, "b"));
        assertNull(builder.asInt(node, "nul"));
        assertNull(builder.asDouble(node, "nul"));
        assertNull(builder.asInt(node, "missing"));
        assertNull(builder.asDouble(node, "missing"));
        assertNull(builder.asInt(null, "n"));
        assertNull(builder.asDouble(null, "n"));
    }

    @Test
    public void anIntegerFieldReadAsADoubleAndViceVersaStillWork() {
        // Jackson types 7 as an int and 7.0 as a double; both are legitimate for a numeric payload field.
        JsonNode node = node("{\"whole\":7,\"fractional\":7.5}");
        assertEquals(builder.asDouble(node, "whole"), 7.0);
        assertEquals(builder.asInt(node, "fractional"), Integer.valueOf(7));
    }

    private static Map<String, JsonNode> files(String name, String json) {
        Map<String, JsonNode> files = new HashMap<>();
        files.put(name, node(json));
        return files;
    }
}
