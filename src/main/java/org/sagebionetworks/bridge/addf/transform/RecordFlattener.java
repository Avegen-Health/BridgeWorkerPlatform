package org.sagebionetworks.bridge.addf.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.decrypt.DecryptedArchive;

/**
 * ADDF §3.5 — dispatches a decrypted record to the correct content-row builder by assessment {@code item}. Activity
 * items map to an {@link ActivityRowBuilder}; {@code birth-gender}/{@code Diagnosis} map to {@link DemographicsBuilder}
 * (per-participant column-merge). Unknown items yield no content row — but the caller still emits a
 * {@code file_records} manifest row for them.
 *
 * <p>This is deliberately the <em>only</em> place that switches on assessment type. The manifest builder is kept
 * outside it so it fires for every record, mapped or not (§3.5.2).</p>
 */
@Component
public class RecordFlattener {
    private static final Logger LOG = LoggerFactory.getLogger(RecordFlattener.class);

    private final Map<String, ActivityRowBuilder> buildersByItem = new HashMap<>();
    private DemographicsBuilder demographicsBuilder;

    @Autowired
    public final void setActivityRowBuilders(List<ActivityRowBuilder> builders) {
        for (ActivityRowBuilder builder : builders) {
            for (String item : builder.handledItems()) {
                buildersByItem.put(item.toLowerCase(), builder);
            }
        }
    }

    @Autowired
    public final void setDemographicsBuilder(DemographicsBuilder demographicsBuilder) {
        this.demographicsBuilder = demographicsBuilder;
    }

    /**
     * Resolve the assessment {@code item} for a record: info.json {@code item}, then the record's metadata, then an
     * inference from which payload files are present. Returns null if it cannot be determined.
     */
    public String resolveItem(DecryptedArchive archive) {
        JsonNode info = archive.getJson("info.json");
        if (info != null && info.hasNonNull("item")) {
            return info.get("item").asText();
        }
        Map<String, String> recordMeta = archive.getRecord().getMetadata();
        if (recordMeta != null) {
            if (recordMeta.get("item") != null) {
                return recordMeta.get("item");
            }
            if (recordMeta.get("assessmentId") != null) {
                return recordMeta.get("assessmentId");
            }
        }
        // Infer from payload presence for the file-typed assessments.
        if (archive.hasFile("Session.json")) {
            return "KeyboardSession";
        }
        if (archive.hasFile("gonogo.json")) {
            return "gonogo";
        }
        if (archive.hasFile("trailmaking.json")) {
            return "trailmaking";
        }
        return null;
    }

    /** Build the content row for a record, or null if the item is unmapped/unusable. */
    public TableRow flattenContent(FlattenContext ctx, String item) {
        if (item == null) {
            return null;
        }
        if (demographicsBuilder.handles(item)) {
            return demographicsBuilder.build(ctx, item);
        }
        ActivityRowBuilder builder = buildersByItem.get(item.toLowerCase());
        if (builder == null) {
            LOG.info("ADDF: no content builder for item '" + item + "' (record " + ctx.getRecordId() +
                    "); manifest row still emitted");
            return null;
        }
        return builder.build(ctx);
    }
}
