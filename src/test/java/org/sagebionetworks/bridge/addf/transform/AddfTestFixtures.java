package org.sagebionetworks.bridge.addf.transform;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import org.sagebionetworks.bridge.addf.decrypt.DecryptedArchive;
import org.sagebionetworks.bridge.json.DefaultObjectMapper;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.Upload;

/**
 * Shared builders for the ADDF transform unit tests: a mocked source record + decrypted archive whose parsed JSON
 * payload files are supplied inline, wrapped in a {@link FlattenContext}.
 */
public final class AddfTestFixtures {
    public static final String RECORD_ID = "rec-1";
    public static final String HEALTH_CODE = "health-code";
    /**
     * What {@code HealthDataRecordEx3.getClientInfo()} actually returns — a JSON object, not a user-agent string.
     * The earlier fixture stubbed the user-agent string here, which is why the transform tests passed while every
     * delivered row had a null {@code app_version}/{@code platform}/{@code device_name}/{@code os_*}.
     */
    public static final String CLIENT_INFO_JSON = "{\n"
            + "  \"appName\" : \"biaffect-3\",\n"
            + "  \"appVersion\" : 68,\n"
            + "  \"deviceName\" : \"iPhone 11 Pro\",\n"
            + "  \"osName\" : \"iPhone OS\",\n"
            + "  \"osVersion\" : \"26.5.2\",\n"
            + "  \"type\" : \"ClientInfo\"\n"
            + "}";
    /** What {@code HealthDataRecordEx3.getUserAgent()} returns — the fallback form. */
    public static final String USER_AGENT = "biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)";
    public static final String UPLOADED_ON = "2026-08-15T14:30:00.000Z";

    private AddfTestFixtures() {
    }

    /** Parse a JSON string into a node, rethrowing parse failures as unchecked (test-only). */
    public static JsonNode node(String json) {
        try {
            return DefaultObjectMapper.INSTANCE.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** A mocked HealthDataRecordEx3 with the fields the builders read. */
    public static HealthDataRecordEx3 record() {
        HealthDataRecordEx3 record = mock(HealthDataRecordEx3.class);
        when(record.getId()).thenReturn(RECORD_ID);
        when(record.getHealthCode()).thenReturn(HEALTH_CODE);
        when(record.getUserAgent()).thenReturn(USER_AGENT);
        when(record.getClientInfo()).thenReturn(CLIENT_INFO_JSON);
        return record;
    }

    public static Upload upload(String contentType) {
        Upload upload = mock(Upload.class);
        when(upload.getContentType()).thenReturn(contentType);
        return upload;
    }

    /** A decrypted archive holding the supplied parsed payload files. */
    public static DecryptedArchive archive(Map<String, JsonNode> jsonFiles) {
        return new DecryptedArchive(record(), upload("application/zip"), null,
                Collections.<String, File>emptyMap(), jsonFiles);
    }

    /** A FlattenContext over the supplied payload files, using the standard client info. */
    public static FlattenContext context(Map<String, JsonNode> jsonFiles) {
        return context(jsonFiles, 3, false);
    }

    public static FlattenContext context(Map<String, JsonNode> jsonFiles, Integer participantVersion, boolean test) {
        DecryptedArchive archive = archive(jsonFiles);
        // Resolved exactly as the accumulate worker does, from both record fields.
        ClientInfo clientInfo = ClientInfo.fromRecord(CLIENT_INFO_JSON, USER_AGENT);
        return new FlattenContext(archive, clientInfo, participantVersion, test, UPLOADED_ON);
    }
}
