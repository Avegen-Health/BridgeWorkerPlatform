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

public class TrailMakingBuilderTest {
    private TrailMakingBuilder builder;

    @BeforeMethod
    public void before() {
        builder = new TrailMakingBuilder();
    }

    @Test
    public void metadataAndTable() {
        assertEquals(builder.table(), AddfTables.TRAIL_MAKING);
        assertEquals(builder.handledItems().length, 3);
    }

    @Test
    public void noPayloadReturnsNull() {
        assertNull(builder.build(AddfTestFixtures.context(new HashMap<String, JsonNode>())));
    }

    @Test
    public void buildFieldsAndVerbatimArrays() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("trailmaking.json", node("{" +
                "\"numberOfErrors\":2,\"runtime\":12.5,\"pauseInterval\":1.5," +
                "\"startDate\":\"2026-08-15T10:30:00.000-04:00\"," +
                "\"taps\":[{\"t\":1}],\"points\":[1,2,3]}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertEquals(row.get("number_of_errors"), 2);
        assertEquals(row.get("runtime_sec"), 12.5);
        assertEquals(row.get("pause_interval_sec"), 1.5);
        assertEquals(row.get("taps"), "[{\"t\":1}]");
        assertEquals(row.get("points"), "[1,2,3]");
        assertEquals(row.get("created_on"), "2026-08-15T14:30:00.000Z");
        assertEquals(row.get("time_zone"), "-04:00");
    }

    @Test
    public void missingNumericFieldsStayNull() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("trailmaking.json", node("{\"startDate\":\"2026-08-15T10:30:00.000-04:00\"}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertNull(row.get("number_of_errors"));
        assertNull(row.get("runtime_sec"));
        assertNull(row.get("pause_interval_sec"));
        assertNull(row.get("taps"));
        assertNull(row.get("points"));
    }
}
