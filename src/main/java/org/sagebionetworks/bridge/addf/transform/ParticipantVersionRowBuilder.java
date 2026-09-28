package org.sagebionetworks.bridge.addf.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.model.ParticipantVersion;

/**
 * ADDF §3b.1/§3b.5 — builds the 10-column {@code participant_versions} row from a BS2 {@link ParticipantVersion}
 * snapshot (the same object {@code Ex3ParticipantVersionWorker} exports, fetched from BridgeServer2 — <b>not</b> read
 * from the Synapse table {@code syn50697927}). Mirrors {@code ParticipantVersionHelper}'s field extraction but writes
 * the ADDF Parquet schema and the ADDF serialisation conventions observed in the golden previews:
 *
 * <ul>
 *   <li>{@code data_groups} &mdash; comma-joined, sorted (canonical ordering).</li>
 *   <li>{@code languages} &mdash; pipe-joined ({@code en|es}).</li>
 *   <li>{@code study_id} &mdash; derived from the {@code studyMemberships} <em>keys</em> (single study for BiAffect;
 *       multi-study joins the keys, sorted).</li>
 *   <li>{@code study_memberships} &mdash; Synapse's {@code |studyId=|} shape with the external ID always stripped,
 *       study IDs sorted. See {@link #truncateStudyMemberships} for why this is not simply E3's serialiser.</li>
 * </ul>
 *
 * <p>{@code external_id} is the only participant column withheld entirely — see
 * {@link AddfTables#PII_WITHHELD_PARTICIPANT_FIELDS}.</p>
 */
@Component
public class ParticipantVersionRowBuilder {
    public TableRow build(ParticipantVersion pv, boolean isTest) {
        String healthCode = pv.getHealthCode();
        Integer version = pv.getParticipantVersion();
        // Staging identity is health_code + version; TableRow.key carries health_code for merge/idempotency semantics.
        TableRow row = new TableRow(AddfTables.PARTICIPANT_VERSIONS, healthCode);

        row.put("health_code", healthCode);
        row.put("participant_version", version);
        row.put("study_id", deriveStudyId(pv.getStudyMemberships()));
        row.put("sharing_scope", pv.getSharingScope() == null ? null : pv.getSharingScope().toString());
        row.put("data_groups", joinSortedComma(pv.getDataGroups()));
        row.put("languages", joinPipe(pv.getLanguages()));
        row.put("client_time_zone", pv.getTimeZone());
        row.put("study_memberships", truncateStudyMemberships(pv.getStudyMemberships()));
        row.put("created_on", AddfDateUtils.toUtcIso(pv.getCreatedOn()));
        row.put("modified_on", AddfDateUtils.toUtcIso(pv.getModifiedOn()));
        return row;
    }

    /**
     * Serialises {@code studyMemberships} in Synapse's wire format but with the external ID <b>always</b> stripped:
     * {@code |studyA=|studyB=|}, study IDs sorted. Compare
     * {@code Exporter3.ParticipantVersionHelper#serializeStudyMemberships}, which emits {@code studyId + "=" + extId}
     * verbatim and only yields an empty value when the account has no external ID (the {@code <none>} sentinel).
     *
     * <p>That distinction is the whole point of this method. The Sage golden delivery reads
     * {@code |biaffect-3-study=|} on every row because no BiAffect participant currently has an external ID — not
     * because Synapse truncates. Copying E3's serialiser would therefore leak {@code externalId} (Class A PII, in
     * practice a person's name) the moment a site enrols someone with one. We keep the shape ADDI already parses and
     * drop the value unconditionally.</p>
     */
    static String truncateStudyMemberships(Map<String, String> memberships) {
        if (memberships == null || memberships.isEmpty()) {
            return null;
        }
        List<String> keys = new ArrayList<>(memberships.keySet());
        Collections.sort(keys);
        StringBuilder sb = new StringBuilder("|");
        for (String key : keys) {
            // The value is deliberately never read — see the javadoc above.
            sb.append(key).append("=|");
        }
        return sb.toString();
    }

    private static String deriveStudyId(Map<String, String> memberships) {
        if (memberships == null || memberships.isEmpty()) {
            return null;
        }
        if (memberships.size() == 1) {
            return memberships.keySet().iterator().next();
        }
        List<String> keys = new ArrayList<>(memberships.keySet());
        Collections.sort(keys);
        return String.join(",", keys);
    }

    private static String joinSortedComma(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        List<String> copy = new ArrayList<>(values);
        Collections.sort(copy);
        return String.join(",", copy);
    }

    private static String joinPipe(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return String.join("|", values);
    }
}
