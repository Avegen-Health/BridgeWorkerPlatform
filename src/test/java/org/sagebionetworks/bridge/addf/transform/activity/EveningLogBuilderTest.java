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

public class EveningLogBuilderTest {
    private EveningLogBuilder builder;

    @BeforeMethod
    public void before() {
        builder = new EveningLogBuilder();
    }

    @Test
    public void metadataAndTable() {
        assertEquals(builder.table(), AddfTables.EVENING_LOG);
        assertEquals(builder.handledItems().length, 3);
    }

    @Test
    public void noAnswersReturnsNull() {
        assertNull(builder.build(AddfTestFixtures.context(new HashMap<String, JsonNode>())));
    }

    @Test
    public void buildRatingsAndMissingFieldStaysNull() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("info.json", node("{\"createdOn\":\"2026-08-15T10:30:00.000-04:00\"}"));
        // irritability omitted -> intOrNull returns null.
        files.put("answers.json", node(
                "{\"mood\":1,\"fatigue\":2,\"fidgeting\":3,\"energy\":4,\"speech\":5}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertEquals(row.get("mood"), 1);
        assertEquals(row.get("fatigue"), 2);
        assertEquals(row.get("fidgeting"), 3);
        assertEquals(row.get("energy"), 4);
        assertEquals(row.get("speech"), 5);
        assertNull(row.get("irritability"));
        assertEquals(row.get("time_zone"), "-04:00");
    }
}
