package org.sagebionetworks.bridge.addf.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;

public class LedgerStoreTest {
    private static final String BUCKET = "my-bucket";

    private AmazonS3 mockS3;
    private LedgerStore ledger;

    @BeforeMethod
    public void before() {
        mockS3 = mock(AmazonS3.class);
        Config config = mock(Config.class);
        when(config.get("addf.exportstore.bucket")).thenReturn(BUCKET);

        ledger = new LedgerStore();
        ledger.setBridgeConfig(config);
        ledger.setAddfS3Client(mockS3);
    }

    @Test
    public void containsRecord() {
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/_ledger/record/rec-1")).thenReturn(true);
        assertTrue(ledger.containsRecord("rec-1"));
        assertFalse(ledger.containsRecord("rec-missing"));
    }

    @Test
    public void markRecord() {
        ledger.markRecord("rec-1");
        assertEquals(capturePut().getKey(), "biaffect-3/_ledger/record/rec-1");
    }

    @Test
    public void containsVersion() {
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/_ledger/version/hc-1/2")).thenReturn(true);
        assertTrue(ledger.containsVersion("hc-1", 2));
        assertFalse(ledger.containsVersion("hc-1", 9));
    }

    @Test
    public void markVersion() {
        ledger.markVersion("hc-1", 2);
        assertEquals(capturePut().getKey(), "biaffect-3/_ledger/version/hc-1/2");
    }

    @Test
    public void recordAndVersionScopesNeverCollide() {
        // §3.6 keeps the two presence-skip modes in separate key scopes. If they shared one, an id that happens to
        // appear as both a record id and a health code would cross-skip: a real upload silently dropped because a
        // participant version of the same name was already written (or vice-versa).
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/_ledger/record/shared-id")).thenReturn(true);

        assertTrue(ledger.containsRecord("shared-id"));
        assertFalse(ledger.containsVersion("shared-id", 1));
    }

    @Test
    public void versionScopeIsKeyedByVersionNotJustParticipant() {
        // A participant's v2 being written must not skip their v3 — participant_versions is version-scoped, so each
        // new version lands as its own immutable row.
        when(mockS3.doesObjectExist(BUCKET, "biaffect-3/_ledger/version/hc-1/2")).thenReturn(true);

        assertTrue(ledger.containsVersion("hc-1", 2));
        assertFalse(ledger.containsVersion("hc-1", 3));
    }

    @Test
    public void thereIsNoDemographicsScope() {
        // The third mode of §3.6 is an absence, and it is load-bearing: demographics is merge-always, so it must never
        // consult this ledger. Its two source uploads are ledgered under *their own record ids* (record scope) — which
        // is exactly what lets the second one through to be column-merged at publish rather than presence-skipped.
        // This asserts the store exposes no health-code-keyed scope that could accidentally be wired up.
        ledger.markRecord("rec-birth-gender");
        assertEquals(capturePut().getKey(), "biaffect-3/_ledger/record/rec-birth-gender");
        assertFalse(ledger.containsRecord("rec-diagnosis"),
                "the other demographics upload for the same participant must still be processed");
    }

    private PutObjectRequest capturePut() {
        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(mockS3).putObject(captor.capture());
        return captor.getValue();
    }

    // -----------------------------------------------------------------------------------------------------------
    // Raw scope (§4.3.4) — health-code-keyed, because it is the only surviving index from a participant to the
    // archives of theirs that have left the AWS account once withdrawal compaction deletes their file_records rows.
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void markRawDeliveredWritesHealthCodeScopedKey() {
        ledger.markRawDelivered("hc-1", "raw/2026-08-15/rec-1-PHQ-9.zip");

        assertEquals(capturePut().getKey(), "biaffect-3/_ledger/raw/hc-1/2026-08-15/rec-1-PHQ-9.zip");
    }

    @Test
    public void containsRawProbesTheSameKeyMarkWrote() {
        // The write and the read are separate code paths over the same key shape; if they ever disagree, every
        // archive re-uploads on every run and nothing fails loudly. Pin them against each other.
        String key = ledger.rawLedgerKey("hc-1", "raw/2026-08-15/rec-1-PHQ-9.zip");
        ledger.markRawDelivered("hc-1", "raw/2026-08-15/rec-1-PHQ-9.zip");
        assertEquals(capturePut().getKey(), key);

        when(mockS3.doesObjectExist(BUCKET, key)).thenReturn(true);
        assertTrue(ledger.containsRaw("hc-1", "raw/2026-08-15/rec-1-PHQ-9.zip"));
        assertFalse(ledger.containsRaw("hc-2", "raw/2026-08-15/rec-1-PHQ-9.zip"));
    }

    @Test
    public void rawLedgerKeyIsToleratantOfAMissingRawPrefix() {
        // file_records.file_name always carries the raw/ prefix, but the ledger must not produce a different key
        // shape if a caller ever passes the bare relative path.
        assertEquals(ledger.rawLedgerKey("hc-1", "2026-08-15/rec-1.zip"),
                ledger.rawLedgerKey("hc-1", "raw/2026-08-15/rec-1.zip"));
    }

    @Test
    public void rawScopeCannotCollideWithRecordOrVersionScopes() {
        ledger.markRawDelivered("hc-1", "raw/2026-08-15/rec-1-PHQ-9.zip");
        String rawKey = capturePut().getKey();

        assertFalse(rawKey.startsWith("biaffect-3/_ledger/record/"));
        assertFalse(rawKey.startsWith("biaffect-3/_ledger/version/"));
        assertTrue(rawKey.startsWith("biaffect-3/_ledger/raw/"));
    }
}
