package org.sagebionetworks.bridge.addf.publish;

/**
 * One raw upload archive that is due for delivery to the Azure staging container, paired with the health code that
 * owns it (§4.3.4).
 *
 * <p>The health code is carried explicitly rather than looked up later because the lookup becomes impossible: the
 * only index from a participant to their {@code raw/…} archives is {@code file_records}, and withdrawal compaction
 * deletes those rows. Capturing it at selection time is what lets {@code LedgerStore} record <i>whose</i> archive
 * left the account.</p>
 */
public final class RawCandidate {
    private final String healthCode;
    private final String relativeKey;

    public RawCandidate(String healthCode, String relativeKey) {
        this.healthCode = healthCode;
        this.relativeKey = relativeKey;
    }

    /** The owning participant's health code. */
    public String getHealthCode() {
        return healthCode;
    }

    /** Delivery-root-relative archive key, exactly as {@code file_records.file_name} stores it. */
    public String getRelativeKey() {
        return relativeKey;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RawCandidate)) {
            return false;
        }
        RawCandidate that = (RawCandidate) other;
        return healthCode.equals(that.healthCode) && relativeKey.equals(that.relativeKey);
    }

    @Override
    public int hashCode() {
        return 31 * healthCode.hashCode() + relativeKey.hashCode();
    }

    @Override
    public String toString() {
        return relativeKey;
    }
}
