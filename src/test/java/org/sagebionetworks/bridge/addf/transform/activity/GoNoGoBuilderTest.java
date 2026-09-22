package org.sagebionetworks.bridge.addf.transform.activity;

import static org.sagebionetworks.bridge.addf.transform.AddfTestFixtures.node;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.addf.transform.AddfTables;
import org.sagebionetworks.bridge.addf.transform.AddfTestFixtures;
import org.sagebionetworks.bridge.addf.transform.SummaryComputer;
import org.sagebionetworks.bridge.addf.transform.TableRow;

public class GoNoGoBuilderTest {
    private GoNoGoBuilder builder;

    @BeforeMethod
    public void before() {
        builder = new GoNoGoBuilder();
        builder.setSummaryComputer(new SummaryComputer());
    }

    @Test
    public void metadataAndTable() {
        assertEquals(builder.table(), AddfTables.GO_NO_GO);
        assertEquals(builder.handledItems().length, 4);
    }

    @Test
    public void noPayloadReturnsNull() {
        assertNull(builder.build(AddfTestFixtures.context(new HashMap<String, JsonNode>())));
    }

    @Test
    public void buildSummariesAndCompactsResults() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("gonogo.json", node("{" +
                "\"identifier\":\"gng\",\"startDate\":\"2026-08-15T10:30:00.000-04:00\",\"results\":[" +
                "{\"go\":true,\"incorrect\":false,\"timeToThreshold\":0.5,\"samples\":[{},{}]}," +
                "{\"go\":false,\"incorrect\":true}]}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertEquals(row.get("identifier"), "gng");
        assertEquals(row.get("created_on"), "2026-08-15T14:30:00.000Z");
        assertEquals(row.get("time_zone"), "-04:00");
        assertEquals(row.get("n_trials"), 2);
        assertEquals(row.get("n_go"), 1);
        assertEquals(row.get("n_nogo"), 1);
        assertEquals(row.get("commission_errors"), 1);
        assertEquals(row.get("omission_errors"), 0);
        assertTrue(((String) row.get("results")).contains("motion_sample_count"));
    }
}
