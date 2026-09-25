package org.sagebionetworks.bridge.addf.publish;

import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

/**
 * Thrown when {@link ManifestGate} finds the snapshot it was handed is not fit to deliver (§7). It extends
 * {@link WorkerException} so the publish worker's existing throws-set carries it unchanged: the SQS message fails, the
 * {@code _publish/<date>.done} marker is never written, and both the DLQ and publish-heartbeat alarms (§6.4) fire —
 * which is the intent. A blocked snapshot is a loud failure, not a quiet skip.
 */
@SuppressWarnings("serial")
public class ManifestGateException extends WorkerException {
    public ManifestGateException(String message) {
        super(message);
    }
}
