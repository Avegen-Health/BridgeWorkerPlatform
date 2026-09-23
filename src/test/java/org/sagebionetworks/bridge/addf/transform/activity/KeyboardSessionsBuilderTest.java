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
import org.sagebionetworks.bridge.addf.transform.SummaryComputer;
import org.sagebionetworks.bridge.addf.transform.TableRow;

public class KeyboardSessionsBuilderTest {
    private KeyboardSessionsBuilder builder;

    @BeforeMethod
    public void before() {
        builder = new KeyboardSessionsBuilder();
        builder.setSummaryComputer(new SummaryComputer());
    }

    @Test
    public void metadataAndTable() {
        assertEquals(builder.table(), AddfTables.KEYBOARD_SESSIONS);
        assertEquals(builder.handledItems().length, 3);
    }

    @Test
    public void noSessionReturnsNull() {
        assertNull(builder.build(AddfTestFixtures.context(new HashMap<String, JsonNode>())));
    }

    @Test
    public void buildKeyboardAndMotionSummaries() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("Session.json", node("{\"keylogs\":[" +
                "{\"value\":\"alphabet\",\"duration\":0.1,\"timestamp\":100.0}," +
                "{\"value\":\"space\",\"duration\":0.3,\"timestamp\":102.0}]}"));
        files.put("motion.json", node("{\"items\":[{\"vectorMagnitude\":3.0},{\"vectorMagnitude\":5.0}]}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertEquals(row.get("total_keys"), 2);
        assertEquals(row.get("n_alphabet"), 1);
        assertEquals(row.get("n_space"), 1);
        assertEquals(row.get("duration_sec"), 2.0);
        // Epoch-seconds session_start converts to UTC; keyboard rows carry no offset.
        assertEquals(row.get("session_start"), "1970-01-01T00:01:40.000Z");
        assertNull(row.get("time_zone"));
        assertEquals(row.get("motion_sample_count"), 2);
        assertEquals(row.get("accel_mag_mean"), 4.0);
        assertEquals(row.get("accel_mag_max"), 5.0);
        assertEquals(row.get("device_name"), "iPhone 11 Pro");
    }

    @Test
    public void motionAbsentLeavesMotionColumnsNull() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("Session.json", node("{\"keylogs\":[{\"value\":\"alphabet\",\"timestamp\":5.0}]}"));

        TableRow row = builder.build(AddfTestFixtures.context(files));

        assertEquals(row.get("total_keys"), 1);
        assertNull(row.get("motion_sample_count"));
        assertNull(row.get("accel_mag_mean"));
    }
}
