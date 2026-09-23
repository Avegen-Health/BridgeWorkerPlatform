package org.sagebionetworks.bridge.addf.transform.activity;

import static org.sagebionetworks.bridge.addf.transform.AddfTestFixtures.node;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.AddfTestFixtures;
import org.sagebionetworks.bridge.addf.transform.TableRow;

public class SelfRatingBuilderTest {
    private SelfRatingBuilder builder;

    @BeforeMethod
    public void before() {
        builder = new SelfRatingBuilder();
    }

    @Test
    public void metadataAndTable() {
        assertEquals(builder.table(), AddfTables.SELF_RATING);
        assertEquals(builder.handledItems().length, 3);
    }

    @Test
    public void noAnswersReturnsNull() {
        assertNull(builder.build(AddfTestFixtures.context(new HashMap<String, JsonNode>())));
    }

    @Test
    public void buildRatings() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("info.json", node("{\"createdOn\":\"2026-08-15T10:30:00.000-04:00\"}"));
        files.put("answers.json", node(
                "{\"energy\":1,\"mood\":2,\"thoughts\":3,\"impulsiveness\":4,\"attention\":5}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertEquals(row.get("energy"), 1);
        assertEquals(row.get("mood"), 2);
        assertEquals(row.get("thoughts"), 3);
        assertEquals(row.get("impulsiveness"), 4);
        assertEquals(row.get("attention"), 5);
        assertEquals(row.get("created_on"), "2026-08-15T14:30:00.000Z");
        assertEquals(row.get("time_zone"), "-04:00");
        assertEquals(row.get("record_id"), AddfTestFixtures.RECORD_ID);
    }
}
