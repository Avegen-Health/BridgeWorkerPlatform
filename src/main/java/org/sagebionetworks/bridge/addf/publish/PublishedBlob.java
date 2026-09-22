package org.sagebionetworks.bridge.addf.publish;

import java.io.File;

/**
 * One consolidated table file (re)written by {@link SnapshotDeltaBuilder} during a publish run, paired with the local
 * temp file that holds its bytes. The {@code key} is the object's path <b>relative to the export-store bucket root</b>
 * (e.g. {@code biaffect-3/current/tables/phq9.parquet}) — and, because the Azure staging container mirrors the same
 * delivery tree, it doubles as the destination blob path. The delta is exactly this set: the publish worker uploads
 * each {@link #getLocalFile()} to the Azure blob at {@link #getKey()} (§4.4).
 */
public final class PublishedBlob {
    private final String key;
    private final File localFile;

    public PublishedBlob(String key, File localFile) {
        this.key = key;
        this.localFile = localFile;
    }

    /** Object key relative to the bucket root, e.g. {@code biaffect-3/current/tables/phq9.parquet}. */
    public String getKey() {
        return key;
    }

    /** Local temp file holding the (re)written Parquet bytes for this blob. */
    public File getLocalFile() {
        return localFile;
    }
}
