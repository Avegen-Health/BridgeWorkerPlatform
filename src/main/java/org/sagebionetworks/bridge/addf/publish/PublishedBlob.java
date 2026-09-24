package org.sagebionetworks.bridge.addf.publish;

import java.io.File;

/**
 * One object to upload to the Azure staging container during a publish run, paired with the local file holding its
 * bytes — either a consolidated table file / keyboard month part written by {@link SnapshotDeltaBuilder}, or a raw
 * upload archive streamed from the export store by {@link RawArchiveDelivery}. The {@code key} is the object's path
 * <b>relative to the export-store bucket root</b> (e.g. {@code biaffect-3/current/tables/phq9.parquet}) — and, because
 * the Azure staging container mirrors the same delivery tree, it doubles as the destination blob path (§4.4).
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

    /** Local file holding this blob's bytes — a written Parquet file, or a downloaded raw archive. */
    public File getLocalFile() {
        return localFile;
    }
}
