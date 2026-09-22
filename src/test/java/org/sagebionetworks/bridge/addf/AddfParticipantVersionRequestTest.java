package org.sagebionetworks.bridge.addf;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class AddfParticipantVersionRequestTest {
    @Test
    public void gettersAndSetters() {
        AddfParticipantVersionRequest request = new AddfParticipantVersionRequest();
        request.setAppId("app-id");
        request.setHealthCode("health-code");
        request.setParticipantVersion(7);
        assertEquals(request.getAppId(), "app-id");
        assertEquals(request.getHealthCode(), "health-code");
        assertEquals(request.getParticipantVersion(), 7);
    }

    @Test
    public void deserializesFromJson() throws Exception {
        AddfParticipantVersionRequest request = DefaultObjectMapper.INSTANCE.readValue(
                "{\"appId\":\"app-id\",\"healthCode\":\"health-code\",\"participantVersion\":7}",
                AddfParticipantVersionRequest.class);
        assertEquals(request.getAppId(), "app-id");
        assertEquals(request.getHealthCode(), "health-code");
        assertEquals(request.getParticipantVersion(), 7);
    }
}
