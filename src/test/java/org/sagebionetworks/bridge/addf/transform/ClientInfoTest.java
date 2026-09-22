package org.sagebionetworks.bridge.addf.transform;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import org.testng.annotations.Test;

public class ClientInfoTest {
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

    @Test
    public void parse_fullString() {
        ClientInfo ci = ClientInfo.parse("biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)");
        assertEquals(ci.getAppName(), "biaffect-3");
        assertEquals(ci.getAppVersion(), "68");
        assertEquals(ci.getDeviceName(), "iPhone 11 Pro");
        assertEquals(ci.getOsName(), "iOS");
        assertEquals(ci.getOsVersion(), "26.5.2");
        assertEquals(ci.getPlatform(), "ios");
    }

    @Test
    public void parse_appAndVersionOnly() {
        ClientInfo ci = ClientInfo.parse("biaffect-3/68");
        assertEquals(ci.getAppName(), "biaffect-3");
        assertEquals(ci.getAppVersion(), "68");
        assertNull(ci.getDeviceName());
        assertNull(ci.getOsName());
        assertNull(ci.getOsVersion());
        assertNull(ci.getPlatform());
    }
}
