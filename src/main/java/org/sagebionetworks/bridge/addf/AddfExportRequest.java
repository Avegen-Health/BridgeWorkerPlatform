package org.sagebionetworks.bridge.addf;

/**
 * Request to the ADDF accumulate worker ({@link AddfExportWorkerProcessor}). Mirrors the wire shape produced by
 * BridgeServer2's {@code AddfExportEnqueuer} (dedicated {@code AddfExportRequest} on the BS2 side, identical two
 * fields) and parallels {@link org.sagebionetworks.bridge.exporter3.Exporter3Request}.
 */
public class AddfExportRequest {
    private String appId;
    private String recordId;

    /** App ID of the record to be exported to ADDF. */
    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    /** Record ID of the record to be exported to ADDF. */
    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }
}
