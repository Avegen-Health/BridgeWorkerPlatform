package org.sagebionetworks.bridge.addf.gate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

public class ConsentTestGateTest {
    private static final String APP_ID = "app-id";
    private static final String HEALTH_CODE = "health-code";

    private BridgeHelper mockBridgeHelper;
    private Config mockConfig;
    private ConsentTestGate gate;

    @BeforeMethod
    public void before() {
        mockBridgeHelper = mock(BridgeHelper.class);
        mockConfig = mock(Config.class);
        // Default: strict prod behaviour - test users are gated out of delivery.
        when(mockConfig.get(ConsentTestGate.CONFIG_KEY_INCLUDE_TEST_USERS)).thenReturn("false");
        gate = new ConsentTestGate();
        gate.setBridgeHelper(mockBridgeHelper);
        gate.setBridgeConfig(mockConfig);
    }

    /** Opt test users in, as uat/dev do. */
    private void includeTestUsers() {
        when(mockConfig.get(ConsentTestGate.CONFIG_KEY_INCLUDE_TEST_USERS)).thenReturn("true");
    }

    private StudyParticipant participant(SharingScope scope, java.util.List<String> dataGroups) throws Exception {
        StudyParticipant participant = mock(StudyParticipant.class);
        when(participant.getSharingScope()).thenReturn(scope);
        when(participant.getDataGroups()).thenReturn(dataGroups);
        when(mockBridgeHelper.getParticipantByHealthCode(APP_ID, HEALTH_CODE, false)).thenReturn(participant);
        return participant;
    }

    @Test
    public void evaluate_ioExceptionFailsClosed() throws Exception {
        when(mockBridgeHelper.getParticipantByHealthCode(APP_ID, HEALTH_CODE, false))
                .thenThrow(new IOException("boom"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
        assertFalse(verdict.isTest());
    }

    @Test
    public void evaluate_runtimeExceptionFailsClosed() throws Exception {
        when(mockBridgeHelper.getParticipantByHealthCode(APP_ID, HEALTH_CODE, false))
                .thenThrow(new RuntimeException("boom"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void evaluate_nullParticipantFailsClosed() throws Exception {
        when(mockBridgeHelper.getParticipantByHealthCode(APP_ID, HEALTH_CODE, false)).thenReturn(null);
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void evaluate_noSharingSkips() throws Exception {
        participant(SharingScope.NO_SHARING, null);
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void evaluate_nullScopeSkips() throws Exception {
        participant(null, null);
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void evaluate_testUserSkipsButFlagged() throws Exception {
        participant(SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.of("test_user"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
        assertTrue(verdict.isTest());
    }

    @Test
    public void evaluate_sharingNonTestExports() throws Exception {
        participant(SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.of("real-group"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertTrue(verdict.isShouldExport());
        assertFalse(verdict.isTest());
    }

    @Test
    public void evaluateVersion_noSharingSkips() {
        ConsentVerdict verdict = gate.evaluateVersion(SharingScope.NO_SHARING, ImmutableList.of("real-group"));
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void evaluateVersion_nullScopeSkips() {
        ConsentVerdict verdict = gate.evaluateVersion(null, null);
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void evaluateVersion_testUserSkips() {
        ConsentVerdict verdict = gate.evaluateVersion(SharingScope.SPONSORS_AND_PARTNERS,
                ImmutableList.of("test_user"));
        assertFalse(verdict.isShouldExport());
        assertTrue(verdict.isTest());
    }

    @Test
    public void evaluateVersion_sharingNonTestExports() {
        ConsentVerdict verdict = gate.evaluateVersion(SharingScope.SPONSORS_AND_PARTNERS, null);
        assertTrue(verdict.isShouldExport());
        assertFalse(verdict.isTest());
    }

    // ---- addf.export.include.test.users=true (uat/dev opt-in) ----

    @Test
    public void evaluate_testUserExportsWhenIncludeEnabled() throws Exception {
        includeTestUsers();
        participant(SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.of("test_user"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertTrue(verdict.isShouldExport());
        // Still flagged, so the is_test column marks the row downstream.
        assertTrue(verdict.isTest());
    }

    @Test
    public void evaluateVersion_testUserExportsWhenIncludeEnabled() {
        includeTestUsers();
        ConsentVerdict verdict = gate.evaluateVersion(SharingScope.SPONSORS_AND_PARTNERS,
                ImmutableList.of("test_user"));
        assertTrue(verdict.isShouldExport());
        assertTrue(verdict.isTest());
    }

    @Test
    public void evaluate_noSharingStillSkipsWhenIncludeEnabled() throws Exception {
        // Consent is absolute - the test-user flag must never relax NO_SHARING.
        includeTestUsers();
        participant(SharingScope.NO_SHARING, ImmutableList.of("test_user"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
        assertTrue(verdict.isTest());
    }

    @Test
    public void evaluateVersion_noSharingStillSkipsWhenIncludeEnabled() {
        includeTestUsers();
        ConsentVerdict verdict = gate.evaluateVersion(SharingScope.NO_SHARING, ImmutableList.of("test_user"));
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void evaluate_nullScopeStillSkipsWhenIncludeEnabled() throws Exception {
        includeTestUsers();
        participant(null, ImmutableList.of("test_user"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void evaluate_unresolvableParticipantStillFailsClosedWhenIncludeEnabled() throws Exception {
        includeTestUsers();
        when(mockBridgeHelper.getParticipantByHealthCode(APP_ID, HEALTH_CODE, false))
                .thenThrow(new IOException("boom"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void includeTestUsers_unsetConfigDefaultsToStrict() throws Exception {
        // A null/absent value must not be read as "include" - prod safety.
        when(mockConfig.get(ConsentTestGate.CONFIG_KEY_INCLUDE_TEST_USERS)).thenReturn(null);
        participant(SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.of("test_user"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
    }

    @Test
    public void includeTestUsers_malformedConfigDefaultsToStrict() throws Exception {
        when(mockConfig.get(ConsentTestGate.CONFIG_KEY_INCLUDE_TEST_USERS)).thenReturn("yes-please");
        participant(SharingScope.SPONSORS_AND_PARTNERS, ImmutableList.of("test_user"));
        ConsentVerdict verdict = gate.evaluate(APP_ID, HEALTH_CODE);
        assertFalse(verdict.isShouldExport());
    }
}
