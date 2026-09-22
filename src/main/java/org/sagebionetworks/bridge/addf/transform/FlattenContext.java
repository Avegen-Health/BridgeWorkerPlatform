package org.sagebionetworks.bridge.addf.transform;

import org.sagebionetworks.bridge.addf.decrypt.DecryptedArchive;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;

/**
 * The immutable per-record inputs shared by every activity/demographics/file-record builder: the decrypted archive,
 * the source record, the parsed {@link ClientInfo}, the capture-time {@code participant_version} (§3.5.3), the
 * {@code is_test} flag from the gate, and the UTC upload timestamp. Builders derive their table-specific columns from
 * these plus their own payload file.
 */
public class FlattenContext {
    private final DecryptedArchive archive;
    private final HealthDataRecordEx3 record;
    private final ClientInfo clientInfo;
    private final Integer participantVersion;
    private final boolean test;
    private final String uploadedOnUtc;

    public FlattenContext(DecryptedArchive archive, ClientInfo clientInfo, Integer participantVersion, boolean test,
            String uploadedOnUtc) {
        this.archive = archive;
        this.record = archive.getRecord();
        this.clientInfo = clientInfo;
        this.participantVersion = participantVersion;
        this.test = test;
        this.uploadedOnUtc = uploadedOnUtc;
    }

    public DecryptedArchive getArchive() {
        return archive;
    }

    public HealthDataRecordEx3 getRecord() {
        return record;
    }

    public ClientInfo getClientInfo() {
        return clientInfo;
    }

    public String getRecordId() {
        return record.getId();
    }

    public String getHealthCode() {
        return record.getHealthCode();
    }

    public Integer getParticipantVersion() {
        return participantVersion;
    }

    public boolean isTest() {
        return test;
    }

    public String getUploadedOnUtc() {
        return uploadedOnUtc;
    }
}
