package org.sagebionetworks.bridge.addf.gate;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

/**
 * ADDF §3.2 — the belt-and-suspenders consent/test gate applied inside the worker (the BS2 enqueue site already
 * inherits the {@code NO_SHARING} gate, but the worker re-checks so it is safe even if a future caller fans out
 * before the sharing check).
 *
 * <p><b>Fail-closed.</b> A participant is exported only when their current sharing scope is definitely not
 * {@code NO_SHARING}. Any failure to resolve the participant, a null scope, or an unexpected error results in
 * <em>skip</em> — never a default-open export.</p>
 *
 * <p><b>Test users are environment-gated.</b> By default — and always in prod — a {@code test_user} is skipped, which
 * is the original §3.2 rule. Setting {@code addf.export.include.test.users=true} (intended for uat/dev only) lets test
 * participants through, so the pipeline can actually be validated in a non-prod environment where every account is
 * conventionally a test user and a strict gate would otherwise make end-to-end verification impossible. Such rows stay
 * clearly marked: the verdict carries {@code is_test}, which the flatteners write into the {@code is_test} column on
 * every table, so downstream consumers can filter them. The {@code NO_SHARING} rule is <em>never</em> relaxed by this
 * flag — consent is always enforced.</p>
 *
 * <p>This resolves <em>current</em> participant state to decide whether to export at all; it is a different concern
 * from the capture-time {@code participant_version} column, which is read off the record (§3.5.3).</p>
 */
@Component
public class ConsentTestGate {
    private static final Logger LOG = LoggerFactory.getLogger(ConsentTestGate.class);

    static final String DATA_GROUP_TEST_USER = "test_user";
    static final String CONFIG_KEY_INCLUDE_TEST_USERS = "addf.export.include.test.users";

    private BridgeHelper bridgeHelper;
    private Config config;

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.config = config;
    }

    /**
     * True when test participants should be exported (uat/dev opt-in). Defaults to false, so an unset or malformed
     * value keeps the strict prod behaviour.
     */
    private boolean includeTestUsers() {
        return Boolean.parseBoolean(config.get(CONFIG_KEY_INCLUDE_TEST_USERS));
    }

    /**
     * Resolve the export decision for a participant. Never throws for consent reasons — an indeterminate result is a
     * (fail-closed) skip.
     */
    public ConsentVerdict evaluate(String appId, String healthCode) {
        StudyParticipant participant;
        try {
            participant = bridgeHelper.getParticipantByHealthCode(appId, healthCode, false);
        } catch (IOException | RuntimeException ex) {
            LOG.warn("ADDF gate: could not resolve participant for app " + appId + " healthCode " + healthCode +
                    "; failing closed (skip): " + ex.getMessage());
            return ConsentVerdict.skip(false);
        }

        if (participant == null) {
            LOG.warn("ADDF gate: null participant for app " + appId + " healthCode " + healthCode +
                    "; failing closed (skip)");
            return ConsentVerdict.skip(false);
        }

        List<String> dataGroups = participant.getDataGroups();
        boolean isTest = dataGroups != null && dataGroups.contains(DATA_GROUP_TEST_USER);

        return evaluateScope(participant.getSharingScope(), isTest);
    }

    /**
     * Evaluate a self-contained {@code ParticipantVersion} snapshot's own scope + data groups (§3b.3), rather than the
     * participant's <em>current</em> state. Used by the dimension worker so a historical version is judged by the
     * consent that applied at that version. Same fail-closed rule: a NO_SHARING or test snapshot is not exported.
     */
    public ConsentVerdict evaluateVersion(SharingScope scope, List<String> dataGroups) {
        boolean isTest = dataGroups != null && dataGroups.contains(DATA_GROUP_TEST_USER);
        return evaluateScope(scope, isTest);
    }

    private ConsentVerdict evaluateScope(SharingScope scope, boolean isTest) {
        // Consent is absolute — never relaxed by the test-user flag.
        if (scope == null || scope == SharingScope.NO_SHARING) {
            return ConsentVerdict.skip(isTest);
        }
        if (isTest && !includeTestUsers()) {
            return ConsentVerdict.skip(true);
        }
        // isTest is carried through so the flatteners can populate the is_test column.
        return ConsentVerdict.export(isTest);
    }
}
