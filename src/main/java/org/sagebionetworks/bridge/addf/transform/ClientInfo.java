package org.sagebionetworks.bridge.addf.transform;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Bridge's {@code clientInfo} string, the canonical per-record source for the device/OS/app columns on the
 * activity tables and {@code file_records}. Observed format (verified against the sample archives / golden previews):
 *
 * <pre>biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)</pre>
 *
 * i.e. {@code <appName>/<appVersion> (<deviceName>; <osName>/<osVersion>)}. From this we derive:
 * <ul>
 *   <li>{@code app_version} = the build number after the slash ({@code 68}),</li>
 *   <li>{@code device_name} = the human device model ({@code iPhone 11 Pro}),</li>
 *   <li>{@code os_name} = {@code iOS}, {@code platform} = {@code ios} (os name lower-cased),</li>
 *   <li>{@code os_version} = {@code 26.5.2}.</li>
 * </ul>
 *
 * <p>Using clientInfo (rather than the per-assessment {@code metadata.json}/{@code info.json}) keeps every table's
 * device columns consistent with {@code file_records}, which is built from the same field. All getters return null
 * when the string is null or does not match, so a builder simply leaves those columns null.</p>
 */
public final class ClientInfo {
    // <app>/<version> (<device>; <os>/<osVersion>) — device and os parts are optional/best-effort.
    private static final Pattern PATTERN = Pattern.compile(
            "^\\s*([^/]+)/(\\S+)\\s*(?:\\(([^;]+);\\s*([^/]+)/([^)]+)\\))?\\s*$");

    private final String appName;
    private final String appVersion;
    private final String deviceName;
    private final String osName;
    private final String osVersion;

    private ClientInfo(String appName, String appVersion, String deviceName, String osName, String osVersion) {
        this.appName = appName;
        this.appVersion = appVersion;
        this.deviceName = deviceName;
        this.osName = osName;
        this.osVersion = osVersion;
    }

    public static ClientInfo parse(String clientInfo) {
        if (clientInfo == null) {
            return new ClientInfo(null, null, null, null, null);
        }
        Matcher matcher = PATTERN.matcher(clientInfo);
        if (!matcher.matches()) {
            return new ClientInfo(null, null, null, null, null);
        }
        return new ClientInfo(
                trimToNull(matcher.group(1)),
                trimToNull(matcher.group(2)),
                trimToNull(matcher.group(3)),
                trimToNull(matcher.group(4)),
                trimToNull(matcher.group(5)));
    }

    public String getAppName() {
        return appName;
    }

    /** Build/version string as it appears after the app-name slash — the ADDF {@code app_version} column. */
    public String getAppVersion() {
        return appVersion;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public String getOsName() {
        return osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    /** Lower-cased OS name for the {@code platform} column (e.g. {@code ios}), or null if unknown. */
    public String getPlatform() {
        return osName == null ? null : osName.toLowerCase();
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String trimmed = s.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
