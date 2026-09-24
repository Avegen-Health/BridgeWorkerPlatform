package org.sagebionetworks.bridge.addf.publish;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * The result of a {@link SnapshotDeltaBuilder#build} pass: the files that changed (the Azure upload set — consolidated
 * tables, keyboard part files and first-time raw archives), plus the side effects that must be applied to the export
 * store <b>only after</b> that upload confirms — the staging objects coalesced into this snapshot, the tombstone
 * markers it compacted out, and the raw-delivery ledger marks.
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
    private final List<RawCandidate> rawCandidates;

    public SnapshotDelta(List<PublishedBlob> blobs, List<String> consumedStagingKeys,
            List<String> tombstonedHealthCodes) {
        this(blobs, consumedStagingKeys, tombstonedHealthCodes, ImmutableList.<RawCandidate>of());
    }

    public SnapshotDelta(List<PublishedBlob> blobs, List<String> consumedStagingKeys,
            List<String> tombstonedHealthCodes, List<RawCandidate> rawCandidates) {
        this.blobs = blobs;
        this.consumedStagingKeys = consumedStagingKeys;
        this.tombstonedHealthCodes = tombstonedHealthCodes;
        this.rawCandidates = rawCandidates;
    }

    /**
     * The consolidated table files (re)written this run — the set uploaded to Azure in one batch. Raw archives are
     * deliberately <b>not</b> here: they are streamed one at a time by {@link RawArchiveDelivery} so that a
     * megabyte-scale payload class never accumulates on the worker's local disk, and so that one unshippable archive
     * cannot abort the table upload.
     */
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

    /**
     * The raw upload archives belonging to the {@code file_records} rows this run consolidated — handed to
     * {@link RawArchiveDelivery} <b>before</b> {@link SnapshotDeltaBuilder#commit}, while the staging objects that
     * produced them still exist (§4.3.4). Each carries its owning health code so delivery stays attributable after
     * withdrawal compaction removes the rows.
     */
    public List<RawCandidate> getRawCandidates() {
        return rawCandidates;
    }
}
