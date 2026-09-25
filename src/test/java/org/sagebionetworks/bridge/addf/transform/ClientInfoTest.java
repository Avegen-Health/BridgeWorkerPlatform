package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

public class ClientInfoTest {
    private static final String JSON = "{\"appName\":\"biaffect-3\",\"appVersion\":68,"
            + "\"deviceName\":\"iPhone 11 Pro\",\"osName\":\"iPhone OS\",\"osVersion\":\"26.5.2\","
            + "\"type\":\"ClientInfo\"}";
    private static final String USER_AGENT = "biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)";

    // --- the JSON form (HealthDataRecordEx3.clientInfo) — the canonical source ------------------------------------

    @Test
    public void parseJson_full() {
        ClientInfo ci = ClientInfo.parse(JSON);
        assertEquals(ci.getAppName(), "biaffect-3");
        // Numeric in JSON — must render as "68", never "68.0".
        assertEquals(ci.getAppVersion(), "68");
        assertEquals(ci.getDeviceName(), "iPhone 11 Pro");
        assertEquals(ci.getOsName(), "iPhone OS");
        assertEquals(ci.getOsVersion(), "26.5.2");
        assertEquals(ci.getPlatform(), "ios");
    }

    @Test
    public void parseJson_missingFields() {
        ClientInfo ci = ClientInfo.parse("{\"appName\":\"biaffect-3\"}");
        assertEquals(ci.getAppName(), "biaffect-3");
        assertNull(ci.getAppVersion());
        assertNull(ci.getDeviceName());
        assertNull(ci.getOsName());
        assertNull(ci.getOsVersion());
        assertNull(ci.getPlatform());
    }

    @Test
    public void parseJson_explicitNullsAndBlanks() {
        ClientInfo ci = ClientInfo.parse("{\"appName\":null,\"deviceName\":\"  \",\"osName\":\"\"}");
        assertNull(ci.getAppName());
        assertNull(ci.getDeviceName());
        assertNull(ci.getOsName());
        assertNull(ci.getPlatform());
    }

    @Test
    public void parseJson_malformedIsEmptyNotThrown() {
        ClientInfo ci = ClientInfo.parse("{not json");
        assertNull(ci.getAppName());
        assertNull(ci.getAppVersion());
        assertNull(ci.getPlatform());
    }

    @Test
    public void parseJson_appVersionAsString() {
        ClientInfo ci = ClientInfo.parse("{\"appVersion\":\"68\"}");
        assertEquals(ci.getAppVersion(), "68");
    }

    // --- the user-agent form (HealthDataRecordEx3.userAgent) — the fallback ---------------------------------------

    @Test
    public void parseUserAgent_full() {
        ClientInfo ci = ClientInfo.parse(USER_AGENT);
        assertEquals(ci.getAppName(), "biaffect-3");
        assertEquals(ci.getAppVersion(), "68");
        assertEquals(ci.getDeviceName(), "iPhone 11 Pro");
        assertEquals(ci.getOsName(), "iOS");
        assertEquals(ci.getOsVersion(), "26.5.2");
        assertEquals(ci.getPlatform(), "ios");
    }

    @Test
    public void parseUserAgent_appAndVersionOnly() {
        ClientInfo ci = ClientInfo.parse("biaffect-3/68");
        assertEquals(ci.getAppName(), "biaffect-3");
        assertEquals(ci.getAppVersion(), "68");
        assertNull(ci.getDeviceName());
        assertNull(ci.getOsName());
        assertNull(ci.getOsVersion());
        assertNull(ci.getPlatform());
    }

    @Test
    public void parse_null() {
        ClientInfo ci = ClientInfo.parse(null);
        assertNull(ci.getAppName());
        assertNull(ci.getAppVersion());
        assertNull(ci.getDeviceName());
        assertNull(ci.getOsName());
        assertNull(ci.getOsVersion());
        assertNull(ci.getPlatform());
    }

    @Test
    public void parse_nonMatching() {
        ClientInfo ci = ClientInfo.parse("totally invalid");
        assertNull(ci.getAppName());
        assertNull(ci.getPlatform());
    }

    // --- fromRecord: JSON wins, user agent fills in ---------------------------------------------------------------

    @Test
    public void fromRecord_prefersJsonOverUserAgent() {
        ClientInfo ci = ClientInfo.fromRecord(JSON, USER_AGENT);
        // "iPhone OS" proves the JSON won; the user agent would have said "iOS".
        assertEquals(ci.getOsName(), "iPhone OS");
        assertEquals(ci.getAppVersion(), "68");
        assertEquals(ci.getDeviceName(), "iPhone 11 Pro");
        assertEquals(ci.getPlatform(), "ios");
    }

    @Test
    public void fromRecord_fallsBackToUserAgentWhenJsonMissing() {
        ClientInfo ci = ClientInfo.fromRecord(null, USER_AGENT);
        assertEquals(ci.getOsName(), "iOS");
        assertEquals(ci.getAppVersion(), "68");
        assertEquals(ci.getDeviceName(), "iPhone 11 Pro");
        assertEquals(ci.getPlatform(), "ios");
    }

    @Test
    public void fromRecord_fallsBackWhenJsonMalformed() {
        ClientInfo ci = ClientInfo.fromRecord("{not json", USER_AGENT);
        assertEquals(ci.getAppVersion(), "68");
        assertEquals(ci.getPlatform(), "ios");
    }

    @Test
    public void fromRecord_fallsBackWhenJsonEmptyObject() {
        ClientInfo ci = ClientInfo.fromRecord("{}", USER_AGENT);
        assertEquals(ci.getAppVersion(), "68");
        assertEquals(ci.getDeviceName(), "iPhone 11 Pro");
    }

    @Test
    public void fromRecord_jsonOnlyNoUserAgent() {
        // The golden file_records preview has rows with a populated client_info and an empty user_agent.
        ClientInfo ci = ClientInfo.fromRecord(JSON, null);
        assertEquals(ci.getAppVersion(), "68");
        assertEquals(ci.getOsName(), "iPhone OS");
        assertEquals(ci.getPlatform(), "ios");
    }

    @Test
    public void fromRecord_bothMissing() {
        ClientInfo ci = ClientInfo.fromRecord(null, null);
        assertNull(ci.getAppVersion());
        assertNull(ci.getPlatform());
    }

    // --- platform normalisation ------------------------------------------------------------------------------------

    @Test
    public void platform_normalisesAppleNames() {
        assertEquals(ClientInfo.parse("{\"osName\":\"iPhone OS\"}").getPlatform(), "ios");
        assertEquals(ClientInfo.parse("{\"osName\":\"iOS\"}").getPlatform(), "ios");
        assertEquals(ClientInfo.parse("{\"osName\":\"iPadOS\"}").getPlatform(), "ios");
    }

    @Test
    public void platform_normalisesAndroid() {
        assertEquals(ClientInfo.parse("{\"osName\":\"Android\"}").getPlatform(), "android");
    }

    @Test
    public void platform_unknownFallsBackToLowerCase() {
        assertEquals(ClientInfo.parse("{\"osName\":\"SomeOS\"}").getPlatform(), "someos");
    }
}
