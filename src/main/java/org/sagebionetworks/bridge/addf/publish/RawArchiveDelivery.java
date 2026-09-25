package org.sagebionetworks.bridge.addf.publish;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.addf.azure.BlobTransport;
import org.sagebionetworks.bridge.addf.store.ExportStoreClient;
import org.sagebionetworks.bridge.addf.store.LedgerStore;
import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.file.FileHelper;

/**
 * ADDF §4.3.4 — delivers the raw upload archives (the delivery tree's second layer) to the Azure staging container.
 *
 * <p><b>Streamed, not batched.</b> Each archive is downloaded, uploaded and deleted locally before the next one is
 * fetched, so peak local disk is <b>one archive</b> regardless of batch size. The archives are deliberately <i>not</i>
 * carried in {@link SnapshotDelta#getBlobs()}: raw payloads are megabytes where the consolidated tables are kilobytes,
 * and accumulating thousands of them on the worker's root volume — shared with every other Bridge worker on a single
 * EB instance — is how a publish run takes the whole JVM down with it.</p>
 *
 * <p><b>Per-archive failure isolation.</b> An archive that fails its upload is parked in the pending-raw retry queue
 * ({@code _pending_raw/}) and the run continues. A single unshippable archive therefore degrades one record instead of
 * aborting the snapshot — which, when raw rode the table delta, meant the tables stopped reaching the partner too.</p>
 *
 * <p><b>Steady state is O(new records), not O(all records).</b> Candidates are the archives whose {@code file_records}
 * rows this run actually consolidated, plus whatever is parked for retry. Publish never rescans the whole
 * {@code file_records} table for undelivered archives — that is what made the work grow without bound, forced a
 * whole-ledger LIST into heap, and starved current data behind months of history. Delivering the <b>historical</b>
 * backlog is a separate, controllable one-off: seed {@code _pending_raw/} in batches and this same path drains it.</p>
 *
 * <p><b>Ordering contract.</b> The publish worker calls this <b>before</b> {@code SnapshotDeltaBuilder.commit} — while
 * the staging objects that produced these candidates are still in place. A crash part-way through therefore replays
 * the whole snapshot and re-derives the identical candidate set; already-delivered archives are skipped by the raw
 * ledger, so the replay is cheap and no archive is lost.</p>
 */
@Component
public class RawArchiveDelivery {
    private static final Logger LOG = LoggerFactory.getLogger(RawArchiveDelivery.class);

    static final String CONFIG_KEY_RAW_ENABLED = "addf.publish.raw.enabled";
    static final String CONFIG_KEY_RAW_MAX_PER_RUN = "addf.publish.raw.max.per.run";
    static final int DEFAULT_MAX_PER_RUN = 500;

    private Config config;
    private ExportStoreClient exportStoreClient;
    private LedgerStore ledgerStore;
    private BlobTransport blobTransport;
    private FileHelper fileHelper;

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.config = config;
    }

    @Autowired
    public final void setExportStoreClient(ExportStoreClient exportStoreClient) {
        this.exportStoreClient = exportStoreClient;
    }

    @Autowired
    public final void setLedgerStore(LedgerStore ledgerStore) {
        this.ledgerStore = ledgerStore;
    }

    @Autowired
    public final void setBlobTransport(BlobTransport blobTransport) {
        this.blobTransport = blobTransport;
    }

    @Autowired
    public final void setFileHelper(FileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    /**
     * Deliver this run's raw archives. Never throws for a per-archive problem — failures are parked for retry and
     * reported in the return value, so the caller can still commit the snapshot's table work.
     *
     * @param candidates archives whose {@code file_records} rows this run consolidated
     * @param tempDir    the publish run's temp dir; each archive occupies it one at a time
     * @return how many were delivered, parked and skipped
     */
    public Result deliver(List<RawCandidate> candidates, File tempDir) {
        if (!Boolean.parseBoolean(config.get(CONFIG_KEY_RAW_ENABLED))) {
            LOG.info("ADDF raw delivery disabled ({}=false), skipping {} candidate(s)", CONFIG_KEY_RAW_ENABLED,
                    candidates.size());
            return new Result(0, 0, candidates.size(), 0);
        }

        // Retry-queue first, then this run's new archives: a previously-failed archive must not be starved behind a
        // steady stream of new ones. Both are deduped — a retry can legitimately reappear as a fresh candidate.
        Set<RawCandidate> queue = new LinkedHashSet<>(readPending());
        queue.addAll(candidates);
        if (queue.isEmpty()) {
            return new Result(0, 0, 0, 0);
        }

        int maxPerRun = maxPerRun();
        int delivered = 0;
        int parked = 0;
        int skipped = 0;
        int deferred = 0;
        int attempted = 0;

        for (RawCandidate candidate : queue) {
            if (attempted >= maxPerRun) {
                // Not silently dropped: everything past the cap is parked, so it is picked up next run.
                exportStoreClient.markRawPending(candidate.getHealthCode(), candidate.getRelativeKey());
                deferred++;
                continue;
            }
            attempted++;
            switch (deliverOne(candidate, tempDir)) {
                case DELIVERED:
                    delivered++;
                    break;
                case SKIPPED:
                    skipped++;
                    break;
                default:
                    parked++;
                    break;
            }
        }

        if (deferred > 0) {
            LOG.warn("ADDF raw delivery capped at {} archive(s) this run; {} parked for the next publish", maxPerRun,
                    deferred);
        }
        LOG.info("ADDF raw delivery: {} delivered, {} parked for retry, {} skipped, {} deferred over cap", delivered,
                parked, skipped, deferred);
        return new Result(delivered, parked, skipped, deferred);
    }

    private Outcome deliverOne(RawCandidate candidate, File tempDir) {
        String relativeKey = candidate.getRelativeKey();
        String healthCode = candidate.getHealthCode();

        if (ledgerStore.containsRaw(healthCode, relativeKey)) {
            // Already shipped on an earlier run (or an earlier attempt within a replayed snapshot).
            exportStoreClient.clearRawPending(healthCode, relativeKey);
            return Outcome.SKIPPED;
        }

        String key = exportStoreClient.rawKey(relativeKey);
        File local = null;
        try {
            if (!exportStoreClient.objectExists(key)) {
                // file_records points at an archive that is not in the store. Do not park it — a permanently absent
                // object would otherwise consume a slot on every future run forever. Log loudly and move on.
                LOG.warn("ADDF raw archive missing from export store, not delivering: {}", key);
                exportStoreClient.clearRawPending(healthCode, relativeKey);
                return Outcome.SKIPPED;
            }

            local = exportStoreClient.download(key, fileHelper.newFile(tempDir, "raw-delivery.zip"));
            blobTransport.upload(ImmutableList.of(new PublishedBlob(key, local)));
            // Mark immediately after this archive's own upload confirms — never in a later batch step that an
            // interruption could skip.
            ledgerStore.markRawDelivered(healthCode, relativeKey);
            exportStoreClient.clearRawPending(healthCode, relativeKey);
            LOG.info("ADDF raw archive delivered: {}", relativeKey);
            return Outcome.DELIVERED;
        } catch (RuntimeException ex) {
            LOG.error("ADDF raw archive delivery failed, parking for retry: " + relativeKey + ": " + ex.getMessage(),
                    ex);
            exportStoreClient.markRawPending(healthCode, relativeKey);
            return Outcome.PARKED;
        } finally {
            deleteQuietly(local);
        }
    }

    private List<RawCandidate> readPending() {
        List<RawCandidate> pending = new ArrayList<>();
        for (String entry : exportStoreClient.listRawPending()) {
            int tab = entry.indexOf('\t');
            if (tab > 0) {
                pending.add(new RawCandidate(entry.substring(0, tab), entry.substring(tab + 1)));
            }
        }
        return pending;
    }

    private int maxPerRun() {
        String configured = config.get(CONFIG_KEY_RAW_MAX_PER_RUN);
        if (configured == null || configured.trim().isEmpty()) {
            return DEFAULT_MAX_PER_RUN;
        }
        try {
            int value = Integer.parseInt(configured.trim());
            return value > 0 ? value : DEFAULT_MAX_PER_RUN;
        } catch (NumberFormatException ex) {
            LOG.warn("ADDF raw delivery: unparseable {}={}, defaulting to {}", CONFIG_KEY_RAW_MAX_PER_RUN, configured,
                    DEFAULT_MAX_PER_RUN);
            return DEFAULT_MAX_PER_RUN;
        }
    }

    private void deleteQuietly(File file) {
        if (file == null) {
            return;
        }
        try {
            fileHelper.deleteFile(file);
        } catch (RuntimeException ex) {
            // Best-effort: the publish worker deletes the whole temp dir afterwards anyway. Only worth a warning
            // because a leak here is what makes peak disk grow with batch size again.
            LOG.warn("ADDF raw delivery: could not delete temp file " + file.getAbsolutePath() + ": "
                    + ex.getMessage());
        }
    }

    private enum Outcome {
        DELIVERED, PARKED, SKIPPED
    }

    /** Per-run counts, for logging and for the worker's summary line. */
    public static final class Result {
        private final int delivered;
        private final int parked;
        private final int skipped;
        private final int deferred;

        public Result(int delivered, int parked, int skipped, int deferred) {
            this.delivered = delivered;
            this.parked = parked;
            this.skipped = skipped;
            this.deferred = deferred;
        }

        public int getDelivered() {
            return delivered;
        }

        public int getParked() {
            return parked;
        }

        public int getSkipped() {
            return skipped;
        }

        public int getDeferred() {
            return deferred;
        }
    }
}
