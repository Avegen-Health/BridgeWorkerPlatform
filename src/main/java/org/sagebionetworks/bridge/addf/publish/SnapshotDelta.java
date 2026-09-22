package org.sagebionetworks.bridge.addf.publish;

import java.util.List;

/**
 * The result of a {@link SnapshotDeltaBuilder#build} pass: the consolidated files that changed (the Azure upload set),
 * plus the side effects that must be applied to the export store <b>only after</b> that upload confirms — the staging
 * objects coalesced into this snapshot and the tombstone markers it compacted out.
 *
 * <p>Splitting the mutation ({@link SnapshotDeltaBuilder#commit}) from the build is the crash-safety contract (§4.5):
 * {@code build} writes the consolidated files (idempotent, overwrite-in-place) but leaves staging + tombstones intact;
 * the worker uploads {@link #getBlobs()} to Azure, and only then calls {@code commit} to delete staging and clear
 * tombstones. A crash or upload failure before {@code commit} therefore leaves the staging/tombstone state in place, so
 * the whole day replays and rebuilds the identical delta rather than silently losing it from the Azure mirror.</p>
 */
public final class SnapshotDelta {
    private final List<PublishedBlob> blobs;
    private final List<String> consumedStagingKeys;
    private final List<String> tombstonedHealthCodes;

    public SnapshotDelta(List<PublishedBlob> blobs, List<String> consumedStagingKeys,
            List<String> tombstonedHealthCodes) {
        this.blobs = blobs;
        this.consumedStagingKeys = consumedStagingKeys;
        this.tombstonedHealthCodes = tombstonedHealthCodes;
    }

    /** The consolidated table files (re)written this run — the exact set to upload to the Azure staging container. */
    public List<PublishedBlob> getBlobs() {
        return blobs;
    }

    /** Full bucket keys of the staging objects coalesced this run — deleted by {@code commit} after the upload. */
    public List<String> getConsumedStagingKeys() {
        return consumedStagingKeys;
    }

    /** Health codes whose tombstone markers this run compacted out — cleared by {@code commit} after the upload. */
    public List<String> getTombstonedHealthCodes() {
        return tombstonedHealthCodes;
    }
}
