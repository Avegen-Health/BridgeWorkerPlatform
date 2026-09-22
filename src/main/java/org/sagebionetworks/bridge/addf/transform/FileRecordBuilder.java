package org.sagebionetworks.bridge.addf.transform;

import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.model.Upload;

/**
 * ADDF §3.5.2 — builds the one {@code file_records} manifest row per uploaded record from data already in hand
 * (record metadata + {@link ClientInfo} + the raw key), <b>without</b> parsing the archive payload. Sits outside
 * {@link RecordFlattener}'s type switch and runs <b>unconditionally</b> for every gated record — including
 * unknown/unmapped assessment types — so {@code file_records} indexes all uploads and is the sole index into the
 * {@code raw/} layer.
 */
@Component
public class FileRecordBuilder {

    /**
     * @param ctx           per-record context
     * @param item          assessment identifier (may be null for an unrecognised upload)
     * @param fileName      the {@code raw/…} key just written by {@code ExportStoreClient.putRaw}
     * @param exportedOnUtc accumulate-time write timestamp (ISO-8601 UTC)
     */
    public TableRow build(FlattenContext ctx, String item, String fileName, String exportedOnUtc) {
        ClientInfo ci = ctx.getClientInfo();
        Upload upload = ctx.getArchive().getUpload();

        TableRow row = new TableRow(AddfTables.FILE_RECORDS, ctx.getRecordId());
        row.put("record_id", ctx.getRecordId());
        row.put("health_code", ctx.getHealthCode());
        row.put("participant_version", ctx.getParticipantVersion());
        row.put("item", item);
        row.put("uploaded_on", ctx.getUploadedOnUtc());
        row.put("exported_on", exportedOnUtc);
        // created_on is left null to match the golden file_records preview (the record's creation maps to uploaded_on;
        // there is no distinct capture timestamp at the manifest grain). VERIFY against the FAIR workbook.
        row.put("created_on", null);
        row.put("content_type", upload != null ? upload.getContentType() : null);
        row.put("user_agent", ctx.getRecord().getUserAgent());
        row.put("client_info", ctx.getRecord().getClientInfo());
        row.put("app_version", ci.getAppVersion());
        row.put("device_name", ci.getDeviceName());
        row.put("os_name", ci.getOsName());
        row.put("os_version", ci.getOsVersion());
        row.put("file_name", fileName);
        return row;
    }
}
