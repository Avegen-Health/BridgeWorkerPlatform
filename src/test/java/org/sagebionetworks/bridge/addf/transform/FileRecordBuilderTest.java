package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FileRecordBuilderTest {
    private FileRecordBuilder builder;

    @BeforeMethod
    public void before() {
        builder = new FileRecordBuilder();
    }

    @Test
    public void buildManifestRowFromMetadata() {
        FlattenContext ctx = AddfTestFixtures.context(new HashMap<String, JsonNode>());
        TableRow row = builder.build(ctx, "PHQ-9", "raw/2026-08-15/rec-1-PHQ-9.zip", "2026-08-15T15:00:00.000Z");

        assertEquals(row.getTable(), AddfTables.FILE_RECORDS);
        assertEquals(row.getKey(), AddfTestFixtures.RECORD_ID);
        assertEquals(row.get("record_id"), AddfTestFixtures.RECORD_ID);
        assertEquals(row.get("health_code"), AddfTestFixtures.HEALTH_CODE);
        assertEquals(row.get("participant_version"), 3);
        assertEquals(row.get("item"), "PHQ-9");
        assertEquals(row.get("uploaded_on"), AddfTestFixtures.UPLOADED_ON);
        assertEquals(row.get("exported_on"), "2026-08-15T15:00:00.000Z");
        assertNull(row.get("created_on"));
        assertEquals(row.get("content_type"), "application/zip");
        assertEquals(row.get("user_agent"), "BiAffect/68");
        assertEquals(row.get("client_info"), AddfTestFixtures.CLIENT_INFO_STRING);
        assertEquals(row.get("app_version"), "68");
        assertEquals(row.get("device_name"), "iPhone 11 Pro");
        assertEquals(row.get("os_name"), "iOS");
        assertEquals(row.get("os_version"), "26.5.2");
        assertEquals(row.get("file_name"), "raw/2026-08-15/rec-1-PHQ-9.zip");
    }
}
