package org.sagebionetworks.bridge.addf;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class AddfPublishRequestTest {
    @Test
    public void deserializesSnapshotDate() throws Exception {
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree("{\"snapshotDate\":\"2026-09-22\"}");
        AddfPublishRequest request = DefaultObjectMapper.INSTANCE.treeToValue(node, AddfPublishRequest.class);
        assertEquals(request.getSnapshotDate(), "2026-09-22");
    }

    @Test
    public void absentSnapshotDateIsNull() throws Exception {
        JsonNode node = DefaultObjectMapper.INSTANCE.readTree("{}");
        AddfPublishRequest request = DefaultObjectMapper.INSTANCE.treeToValue(node, AddfPublishRequest.class);
        assertNull(request.getSnapshotDate());
    }

    @Test
    public void setterRoundTrips() {
        AddfPublishRequest request = new AddfPublishRequest();
        request.setSnapshotDate("2026-01-01");
        assertEquals(request.getSnapshotDate(), "2026-01-01");
    }
}
