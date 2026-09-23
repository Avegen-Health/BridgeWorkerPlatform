package org.sagebionetworks.bridge.addf.transform.activity;

import static org.sagebionetworks.bridge.addf.transform.AddfTestFixtures.node;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.AddfTestFixtures;
import org.sagebionetworks.bridge.addf.transform.FlattenContext;
import org.sagebionetworks.bridge.addf.transform.TableRow;

public class Phq9BuilderTest {
    private Phq9Builder builder;

    @BeforeMethod
    public void before() {
        builder = new Phq9Builder();
    }

    @Test
    public void metadata() {
        assertEquals(builder.table(), AddfTables.PHQ9);
        assertEquals(ImmutableList.copyOf(builder.handledItems()), ImmutableList.of("PHQ-9", "PHQ9"));
    }

    @Test
    public void noAnswersReturnsNull() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("info.json", node("{\"createdOn\":\"2026-08-15T10:30:00.000-04:00\"}"));
        assertNull(builder.build(AddfTestFixtures.context(files)));
    }

    @Test
    public void buildScoresLabelsAndCommonColumns() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("info.json", node("{\"createdOn\":\"2026-08-15T10:30:00.000-04:00\"}"));
        files.put("answers.json", node("{" +
                "\"anhedonia\":\"Several days\",\"depression\":\"Not at all\",\"sleep\":\"Nearly every day\"," +
                "\"energy\":\"More than half the days\",\"appetite\":\"Not at all\"," +
                "\"discouragement\":\"Not at all\",\"concentration\":\"Not at all\",\"speed\":\"Not at all\"," +
                "\"difficulty\":\"Very difficult\"}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertEquals(row.getTable(), AddfTables.PHQ9);
        assertEquals(row.getKey(), AddfTestFixtures.RECORD_ID);
        assertEquals(row.get("record_id"), AddfTestFixtures.RECORD_ID);
        assertEquals(row.get("health_code"), AddfTestFixtures.HEALTH_CODE);
        assertEquals(row.get("participant_version"), 3);
        assertEquals(row.get("app_version"), "68");
        assertEquals(row.get("platform"), "ios");
        assertEquals(row.get("uploaded_on"), AddfTestFixtures.UPLOADED_ON);
        assertEquals(row.get("is_test"), false);

        assertEquals(row.get("created_on"), "2026-08-15T14:30:00.000Z");
        assertEquals(row.get("time_zone"), "-04:00");

        assertEquals(row.get("anhedonia"), 1);
        assertEquals(row.get("depression"), 0);
        assertEquals(row.get("sleep"), 3);
        assertEquals(row.get("energy"), 2);
        assertEquals(row.get("difficulty"), 2);
        // total_score sums the 8 symptom items but excludes difficulty: 1+0+3+2+0+0+0+0 = 6.
        assertEquals(row.get("total_score"), 6);
    }

    @Test
    public void numericAnswersAndUnknownLabels() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("info.json", node("{}"));
        // Numeric codes take the isNumber() path; an unknown label yields null and is excluded from the sum.
        files.put("answers.json", node("{\"anhedonia\":2,\"depression\":\"???\"}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertEquals(row.get("anhedonia"), 2);
        assertNull(row.get("depression"));
        // created_on/time_zone null because info.json has no createdOn.
        assertNull(row.get("created_on"));
        assertNull(row.get("time_zone"));
        assertEquals(row.get("total_score"), 2);
    }
}
