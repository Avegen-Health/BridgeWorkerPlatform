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
    public static final String CLIENT_INFO_STRING = "biaffect-3/68 (iPhone 11 Pro; iOS/26.5.2)";
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
        when(record.getUserAgent()).thenReturn("BiAffect/68");
        when(record.getClientInfo()).thenReturn(CLIENT_INFO_STRING);
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
        ClientInfo clientInfo = ClientInfo.parse(CLIENT_INFO_STRING);
        return new FlattenContext(archive, clientInfo, participantVersion, test, UPLOADED_ON);
    }
}
