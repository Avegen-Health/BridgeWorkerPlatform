package org.sagebionetworks.bridge.addf;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

public class AddfParticipantVersionBackfillRequestTest {
    @Test
    public void gettersAndSetters() {
        AddfParticipantVersionBackfillRequest request = new AddfParticipantVersionBackfillRequest();
        request.setAppId("app-id");
        request.setS3Key("backfill/health-codes.txt");
        assertEquals(request.getAppId(), "app-id");
        assertEquals(request.getS3Key(), "backfill/health-codes.txt");
    }

    @Test
    public void deserializesFromJson() throws Exception {
        AddfParticipantVersionBackfillRequest request = DefaultObjectMapper.INSTANCE.readValue(
                "{\"appId\":\"app-id\",\"s3Key\":\"backfill/health-codes.txt\"}",
                AddfParticipantVersionBackfillRequest.class);
        assertEquals(request.getAppId(), "app-id");
        assertEquals(request.getS3Key(), "backfill/health-codes.txt");
    }
}
