package org.sagebionetworks.bridge.addf;

/**
 * Request to the ADDF dimension worker ({@link AddfParticipantVersionWorkerProcessor}). Mirrors the wire shape produced
 * by BridgeServer2's {@code AddfParticipantVersionEnqueuer} and parallels
 * {@link org.sagebionetworks.bridge.exporter3.Ex3ParticipantVersionRequest}.
 */
public class AddfParticipantVersionRequest {
    private String appId;
    private String healthCode;
    private int participantVersion;

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getHealthCode() {
        return healthCode;
    }

    public void setHealthCode(String healthCode) {
        this.healthCode = healthCode;
    }

    public int getParticipantVersion() {
        return participantVersion;
    }

    public void setParticipantVersion(int participantVersion) {
        this.participantVersion = participantVersion;
    }
}
