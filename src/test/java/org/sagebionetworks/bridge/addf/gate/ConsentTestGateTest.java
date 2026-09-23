package org.sagebionetworks.bridge.addf.gate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.rest.model.SharingScope;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;

public class ConsentTestGateTest {
    private static final String APP_ID = "app-id";
    private static final String HEALTH_CODE = "health-code";

    private BridgeHelper mockBridgeHelper;
    private ConsentTestGate gate;

    @BeforeMethod
    public void before() {
        mockBridgeHelper = mock(BridgeHelper.class);
        gate = new ConsentTestGate();
        gate.setBridgeHelper(mockBridgeHelper);
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
}
