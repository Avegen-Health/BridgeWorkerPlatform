package org.sagebionetworks.bridge.addf.transform;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

/**
 * Resolves the device/OS/app columns carried by the activity tables and {@code file_records} from a record's client
 * metadata. Bridge exposes that metadata in <b>two different wire forms</b>, and this class understands both:
 *
 * <ol>
 *   <li><b>{@code HealthDataRecordEx3.clientInfo} — a JSON object</b> (the canonical source, and what the golden
 *       {@code file_records} preview is derived from):
 *       <pre>{"appName":"biaffect-3","appVersion":68,"deviceName":"iPhone 11 Pro","osName":"iPhone OS","osVersion":"26.5.2"}</pre>
 *   </li>
 *   <li><b>{@code HealthDataRecordEx3.userAgent} — a user-agent string</b>, used as a fallback when the JSON is
 *       absent or unusable:
 *       <pre>biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)</pre>
 *       i.e. {@code <appName>/<appVersion> (<deviceName>; <osName>/<osVersion>)}.
 *   </li>
 * </ol>
 *
 * From either form we derive {@code app_version} ({@code 68}), {@code device_name} ({@code iPhone 11 Pro}),
 * {@code os_name}, {@code os_version} ({@code 26.5.2}) and {@code platform} ({@code ios}).
 *
 * <p><b>Why both.</b> The two forms disagree on {@code os_name} — the JSON reports Apple's OS identifier
 * ({@code iPhone OS}) where the user agent reports the marketing name ({@code iOS}) — so the JSON is preferred and the
 * user agent only fills in when it is missing. The golden preview also carries rows with a populated
 * {@code client_info} and an <i>empty</i> {@code user_agent}, which is why the JSON has to be the primary source
 * rather than the fallback. {@code platform} is normalised across both forms so it is a single stable token
 * ({@code ios}), not a verbatim lower-casing that would emit {@code iphone os} for one form and {@code ios} for the
 * other.</p>
 *
 * <p>All getters return null when nothing parses, so a builder simply leaves those columns null.</p>
 */
public final class ClientInfo {
    // <app>/<version> (<device>; <os>/<osVersion>) — device and os parts are optional/best-effort.
    private static final Pattern USER_AGENT_PATTERN = Pattern.compile(
            "^\\s*([^/]+)/(\\S+)\\s*(?:\\(([^;]+);\\s*([^/]+)/([^)]+)\\))?\\s*$");

    private static final ClientInfo EMPTY = new ClientInfo(null, null, null, null, null);

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

    /**
     * Resolve from a record's two client-metadata fields: the {@code clientInfo} JSON object first, falling back to
     * the {@code userAgent} string when the JSON is absent or yields nothing. This is the call the accumulate worker
     * uses — passing only one of the two silently nulls every derived column whenever that form is the one the record
     * does not carry.
     */
    public static ClientInfo fromRecord(String clientInfoJson, String userAgent) {
        ClientInfo fromJson = parseJson(clientInfoJson);
        if (fromJson.hasAnyValue()) {
            return fromJson;
        }
        return parseUserAgent(userAgent);
    }

    /** Parse either wire form, detected by shape. Prefer {@link #fromRecord} when both fields are available. */
    public static ClientInfo parse(String clientInfo) {
        if (clientInfo == null) {
            return EMPTY;
        }
        String trimmed = clientInfo.trim();
        if (trimmed.startsWith("{")) {
            return parseJson(trimmed);
        }
        return parseUserAgent(trimmed);
    }

    /** Parse the {@code clientInfo} JSON object form. Returns an empty instance for null/blank/non-object/malformed. */
    static ClientInfo parseJson(String json) {
        if (json == null) {
            return EMPTY;
        }
        String trimmed = json.trim();
        if (trimmed.isEmpty() || trimmed.charAt(0) != '{') {
            return EMPTY;
        }
        JsonNode node;
        try {
            node = DefaultObjectMapper.INSTANCE.readTree(trimmed);
        } catch (IOException ex) {
            return EMPTY;
        }
        if (node == null || !node.isObject()) {
            return EMPTY;
        }
        return new ClientInfo(text(node, "appName"), text(node, "appVersion"), text(node, "deviceName"),
                text(node, "osName"), text(node, "osVersion"));
    }

    /** Parse the {@code userAgent} string form. Returns an empty instance for null/non-matching input. */
    static ClientInfo parseUserAgent(String userAgent) {
        if (userAgent == null) {
            return EMPTY;
        }
        Matcher matcher = USER_AGENT_PATTERN.matcher(userAgent);
        if (!matcher.matches()) {
            return EMPTY;
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

    /** Build/version as text — the ADDF {@code app_version} column. JSON reports it as a number ({@code 68}). */
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

    /**
     * Normalised OS token for the {@code platform} column — {@code ios} for both {@code iPhone OS} (JSON) and
     * {@code iOS} (user agent), {@code android} for Android, otherwise the lower-cased name. Null when unknown.
     */
    public String getPlatform() {
        if (osName == null) {
            return null;
        }
        String lower = osName.trim().toLowerCase();
        if (lower.isEmpty()) {
            return null;
        }
        if (lower.contains("ios") || lower.contains("iphone") || lower.contains("ipad") || lower.contains("ipod")) {
            return "ios";
        }
        if (lower.contains("android")) {
            return "android";
        }
        return lower;
    }

    /** True when at least one field parsed — the signal {@link #fromRecord} uses to decide whether to fall back. */
    private boolean hasAnyValue() {
        return appName != null || appVersion != null || deviceName != null || osName != null || osVersion != null;
    }

    /** Field text, rendering an integral JSON number without a decimal point ({@code 68}, never {@code 68.0}). */
    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isIntegralNumber()) {
            return String.valueOf(value.asLong());
        }
        return trimToNull(value.asText());
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String trimmed = s.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
