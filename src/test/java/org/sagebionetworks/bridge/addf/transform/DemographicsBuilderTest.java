package org.sagebionetworks.bridge.addf.transform;

import static org.sagebionetworks.bridge.addf.transform.AddfTestFixtures.node;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DemographicsBuilderTest {
    private DemographicsBuilder builder;

    @BeforeMethod
    public void before() {
        builder = new DemographicsBuilder();
    }

    @Test
    public void handles() {
        assertTrue(builder.handles("birth-gender"));
        assertTrue(builder.handles("Diagnosis"));
        assertTrue(builder.handles("BIRTH_GENDER"));
        assertTrue(builder.handles("gender"));
        assertFalse(builder.handles("PHQ-9"));
        assertFalse(builder.handles(null));
    }

    @Test
    public void noAnswersReturnsNull() {
        assertNull(builder.build(AddfTestFixtures.context(new HashMap<String, JsonNode>()),
                DemographicsBuilder.ITEM_BIRTH_GENDER));
    }

    @Test
    public void birthGenderRow() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("info.json", node("{\"createdOn\":\"2026-08-15T10:30:00.000-04:00\"}"));
        files.put("answers.json", node("{\"birth\":1990,\"gender\":\"female\"}"));

        TableRow row = builder.build(AddfTestFixtures.context(files), DemographicsBuilder.ITEM_BIRTH_GENDER);

        assertEquals(row.getTable(), AddfTables.DEMOGRAPHICS);
        assertEquals(row.getKey(), AddfTestFixtures.HEALTH_CODE);
        assertEquals(row.get("health_code"), AddfTestFixtures.HEALTH_CODE);
        assertEquals(row.get("birth_year"), 1990);
        assertEquals(row.get("gender"), "female");
        assertEquals(row.get("collected_on"), "2026-08-15T14:30:00.000Z");
    }

    @Test
    public void diagnosisRowJoinsMultiSelect() {
        Map<String, JsonNode> files = new HashMap<>();
        files.put("info.json", node("{}"));
        files.put("answers.json", node("{" +
                "\"Bipolar diagnosis\":\"BP1\"," +
                "\"Other psych diagnoses\":[\"anxiety\",\"adhd\"]," +
                "\"Other illness\":\"none\"}"));

        TableRow row = builder.build(AddfTestFixtures.context(files), DemographicsBuilder.ITEM_DIAGNOSIS);

        assertEquals(row.get("bipolar_diagnosis"), "BP1");
        assertEquals(row.get("other_psych_diagnoses"), "anxiety,adhd");
        assertEquals(row.get("other_illness"), "none");
        assertNull(row.get("birth_year"));
        assertNull(row.get("collected_on"));
    }
}
