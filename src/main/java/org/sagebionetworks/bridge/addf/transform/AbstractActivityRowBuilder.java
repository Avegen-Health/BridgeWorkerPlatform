package org.sagebionetworks.bridge.addf.transform;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Shared plumbing for the activity builders: sets the columns every activity table carries and provides payload/time
 * helpers. Concrete builders fill their assessment-specific columns and the capture-time
 * {@code created_on}/{@code session_start}/{@code time_zone}.
 */
public abstract class AbstractActivityRowBuilder implements ActivityRowBuilder {
    static final String INFO_JSON = "info.json";
    static final String ANSWERS_JSON = "answers.json";

    /** Set record_id, health_code, participant_version, app_version, platform, uploaded_on, is_test. */
    protected TableRow newRowWithCommon(FlattenContext ctx) {
        TableRow row = new TableRow(table(), ctx.getRecordId());
        row.put("record_id", ctx.getRecordId());
        row.put("health_code", ctx.getHealthCode());
        row.put("participant_version", ctx.getParticipantVersion());
        ClientInfo ci = ctx.getClientInfo();
        row.put("app_version", ci.getAppVersion());
        row.put("platform", ci.getPlatform());
        row.put("uploaded_on", ctx.getUploadedOnUtc());
        row.put("is_test", ctx.isTest());
        return row;
    }

    /** The raw offset-bearing capture timestamp from info.json {@code createdOn}, or null. */
    protected String infoCreatedOnRaw(FlattenContext ctx) {
        JsonNode info = ctx.getArchive().getJson(INFO_JSON);
        if (info == null || !info.hasNonNull("createdOn")) {
            return null;
        }
        JsonNode createdOn = info.get("createdOn");
        // info.json createdOn may be an ISO string (surveys) — epoch numbers are handled elsewhere.
        return createdOn.isTextual() ? createdOn.asText() : null;
    }

    protected JsonNode answers(FlattenContext ctx) {
        return ctx.getArchive().getJson(ANSWERS_JSON);
    }

    /** Coded PHQ-style int from a JSON answer that may be a numeric code or a string label. */
    protected Integer codedInt(JsonNode answers, String field, java.util.Map<String, Integer> labelMap) {
        if (answers == null || !answers.hasNonNull(field)) {
            return null;
        }
        JsonNode node = answers.get(field);
        if (node.isNumber()) {
            return node.asInt();
        }
        if (node.isTextual()) {
            Integer code = labelMap.get(node.asText());
            return code; // null if the label is unknown — surfaces in the golden test rather than silently 0
        }
        return null;
    }

    protected Integer intOrNull(JsonNode node, String field) {
        if (node == null || !node.hasNonNull(field)) {
            return null;
        }
        JsonNode v = node.get(field);
        return v.isNumber() ? v.asInt() : null;
    }

    protected Double doubleOrNull(JsonNode node, String field) {
        if (node == null || !node.hasNonNull(field)) {
            return null;
        }
        JsonNode v = node.get(field);
        return v.isNumber() ? v.asDouble() : null;
    }
}
