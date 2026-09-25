package org.sagebionetworks.bridge.addf.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.model.ParticipantVersion;

/**
 * ADDF §3b.1/§3b.5 — builds the 9-column {@code participant_versions} row from a BS2 {@link ParticipantVersion}
 * snapshot (the same object {@code Ex3ParticipantVersionWorker} exports, fetched from BridgeServer2 — <b>not</b> read
 * from the Synapse table {@code syn50697927}). Mirrors {@code ParticipantVersionHelper}'s field extraction but writes
 * the ADDF Parquet schema and the ADDF serialisation conventions observed in the golden previews:
 *
 * <ul>
 *   <li>{@code data_groups} &mdash; comma-joined, sorted (canonical ordering).</li>
 *   <li>{@code languages} &mdash; pipe-joined ({@code en|es}).</li>
 *   <li>{@code study_id} &mdash; derived from the {@code studyMemberships} <em>keys</em> (single study for BiAffect;
 *       multi-study joins the keys, sorted).</li>
 * </ul>
 *
 * <p>The membership <em>values</em> are external IDs and are never written: neither {@code external_id} nor
 * {@code study_memberships} is delivered — see {@link AddfTables#PII_WITHHELD_PARTICIPANT_FIELDS}.</p>
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
        row.put("created_on", AddfDateUtils.toUtcIso(pv.getCreatedOn()));
        row.put("modified_on", AddfDateUtils.toUtcIso(pv.getModifiedOn()));
        return row;
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
