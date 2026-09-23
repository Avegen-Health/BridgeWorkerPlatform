package org.sagebionetworks.bridge.addf;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class AddfExportRequestTest {
    @Test
    public void gettersAndSetters() {
        AddfExportRequest request = new AddfExportRequest();
        request.setAppId("app-id");
        request.setRecordId("record-id");
        assertEquals(request.getAppId(), "app-id");
        assertEquals(request.getRecordId(), "record-id");
    }

    @Test
    public void deserializesFromJson() throws Exception {
        AddfExportRequest request = DefaultObjectMapper.INSTANCE.readValue(
                "{\"appId\":\"app-id\",\"recordId\":\"record-id\"}", AddfExportRequest.class);
        assertEquals(request.getAppId(), "app-id");
        assertEquals(request.getRecordId(), "record-id");
    }
}
