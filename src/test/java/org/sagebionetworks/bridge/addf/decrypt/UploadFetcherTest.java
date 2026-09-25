package org.sagebionetworks.bridge.addf.decrypt;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateEncodingException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.amazonaws.AmazonServiceException;
import com.google.common.base.Charsets;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.bouncycastle.cms.CMSException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.crypto.CmsEncryptor;
import org.sagebionetworks.bridge.crypto.WrongEncryptionKeyException;
import org.sagebionetworks.bridge.file.InMemoryFileHelper;
import org.sagebionetworks.bridge.rest.model.App;
import org.sagebionetworks.bridge.rest.model.HealthDataRecordEx3;
import org.sagebionetworks.bridge.rest.model.Upload;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.workerPlatform.bridge.BridgeHelper;
import org.sagebionetworks.bridge.workerPlatform.exceptions.WorkerException;

/**
 * ADDF §3.4 — the fetch/decrypt/unzip step, which had no test of its own. It is the one place ADDF touches the raw
 * upload bucket and the CMS key material, and the whole isolation invariant rests on it decrypting <em>independently</em>
 * of Exporter 3.0 rather than reading E3's output (architecture §6).
 *
 * <p>The zip and JSON parsing are exercised for real — a real {@link ZipHelper} over an {@link InMemoryFileHelper} —
 * so the archive really is written, zipped, unzipped and parsed. Only S3, Bridge and the CMS encryptor are mocked,
 * because those are the boundaries. That matters most for the malformed-JSON case, which must be tolerated rather
 * than mocked away.</p>
 */
public class UploadFetcherTest {
    private static final String APP_ID = "test-app";
    private static final String RECORD_ID = "rec-1";
    private static final String UPLOAD_BUCKET = "org-sagebridge-upload-test";

    private BridgeHelper mockBridgeHelper;
    @SuppressWarnings("unchecked")
    private LoadingCache<String, CmsEncryptor> mockCache = mock(LoadingCache.class);
    private CmsEncryptor mockEncryptor;
    private S3Helper mockS3Helper;
    private InMemoryFileHelper fileHelper;
    private ZipHelper zipHelper;
    private UploadFetcher fetcher;

    private App app;
    private HealthDataRecordEx3 record;
    private Upload upload;
    private File tempDir;

    /** The zip the fake S3 download drops on disk — built per test so each can choose its members. */
    private byte[] archiveZipBytes;

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public void before() throws Exception {
        mockBridgeHelper = mock(BridgeHelper.class);
        mockCache = mock(LoadingCache.class);
        mockEncryptor = mock(CmsEncryptor.class);
        mockS3Helper = mock(S3Helper.class);
        fileHelper = new InMemoryFileHelper();
        zipHelper = new ZipHelper();
        zipHelper.setFileHelper(fileHelper);
        tempDir = fileHelper.createTempDir();

        Config mockConfig = mock(Config.class);
        when(mockConfig.get(UploadFetcher.CONFIG_KEY_UPLOAD_BUCKET)).thenReturn(UPLOAD_BUCKET);

        app = new App().identifier(APP_ID);
        record = mock(HealthDataRecordEx3.class);
        when(record.getId()).thenReturn(RECORD_ID);
        upload = mock(Upload.class);
        when(upload.isEncrypted()).thenReturn(Boolean.TRUE);
        when(mockBridgeHelper.getUploadByUploadId(RECORD_ID)).thenReturn(upload);

        // Default archive: one well-formed JSON payload plus a non-JSON member.
        archiveZipBytes = zipOf(
                member("info.json", "{\"item\":\"PHQ-9\"}"),
                member("notes.txt", "not json and not claimed to be"));

        // S3 "download" writes whatever the test staged into the destination file.
        doAnswer(invocation -> {
            File dest = invocation.getArgumentAt(2, File.class);
            fileHelper.writeBytes(dest, archiveZipBytes);
            return null;
        }).when(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any(File.class));

        // Default encryptor: a pass-through, so the "decrypted" bytes are the archive itself.
        when(mockCache.get(APP_ID)).thenReturn(mockEncryptor);
        when(mockEncryptor.decrypt(any(InputStream.class)))
                .thenAnswer(invocation -> new ByteArrayInputStream(archiveZipBytes));

        fetcher = new UploadFetcher();
        fetcher.setBridgeConfig(mockConfig);
        fetcher.setBridgeHelper(mockBridgeHelper);
        fetcher.setCmsEncryptorCache(mockCache);
        fetcher.setFileHelper(fileHelper);
        fetcher.setS3Helper(mockS3Helper);
        fetcher.setZipHelper(zipHelper);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Happy paths
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void encryptedUploadIsDecryptedUnzippedAndParsed() throws Exception {
        DecryptedArchive archive = fetcher.fetch(app, record, tempDir);

        assertSame(archive.getRecord(), record);
        assertSame(archive.getUpload(), upload);
        assertTrue(archive.hasFile("info.json"));
        assertTrue(archive.hasFile("notes.txt"));

        // Only .json members are parsed; the text member is present as a file but carries no JsonNode.
        assertNotNull(archive.getJson("info.json"));
        assertEquals(archive.getJson("info.json").get("item").asText(), "PHQ-9");
        assertNull(archive.getJson("notes.txt"));

        // ADDF re-fetches from the upload bucket itself — it never reads Exporter 3.0's decrypted output (§6).
        verify(mockS3Helper).downloadS3File(eq(UPLOAD_BUCKET), eq(RECORD_ID), any(File.class));
        verify(mockEncryptor).decrypt(any(InputStream.class));
    }

    @Test
    public void unencryptedUploadSkipsDecryptEntirely() throws Exception {
        when(upload.isEncrypted()).thenReturn(Boolean.FALSE);

        DecryptedArchive archive = fetcher.fetch(app, record, tempDir);

        assertNotNull(archive.getJson("info.json"));
        verify(mockEncryptor, org.mockito.Mockito.never()).decrypt(any(InputStream.class));
    }

    @Test
    public void nullEncryptedFlagIsTreatedAsEncrypted() throws Exception {
        // Fail-safe on an absent flag: assume the payload is encrypted rather than handing raw ciphertext to the
        // unzipper (which would surface as an unreadable archive instead of a decrypt error).
        when(upload.isEncrypted()).thenReturn(null);

        fetcher.fetch(app, record, tempDir);

        verify(mockEncryptor).decrypt(any(InputStream.class));
    }

    @Test
    public void malformedJsonMemberIsSkippedNotFatal() throws Exception {
        // One bad member must not cost the whole record: the flattener treats it as absent and the manifest row is
        // still emitted. Real zip, real parse - this is the branch the mocked-out worker test can never reach.
        archiveZipBytes = zipOf(
                member("info.json", "{\"item\":\"PHQ-9\"}"),
                member("answers.json", "{ this is not json"));

        DecryptedArchive archive = fetcher.fetch(app, record, tempDir);

        assertNotNull(archive.getJson("info.json"));
        assertNull(archive.getJson("answers.json"), "the malformed member must parse to nothing");
        assertTrue(archive.hasFile("answers.json"), "but the file itself is still in the archive");
    }

    @Test
    public void archiveWithNoJsonMembersStillReturns() throws Exception {
        archiveZipBytes = zipOf(member("motion.bin", "binary-ish"));

        DecryptedArchive archive = fetcher.fetch(app, record, tempDir);

        assertTrue(archive.hasFile("motion.bin"));
        assertNull(archive.getJson("motion.bin"));
    }

    // -----------------------------------------------------------------------------------------------------------
    // Error mapping - which failures go to the DLQ (bad request) vs surface as worker errors vs retry (IOException)
    // -----------------------------------------------------------------------------------------------------------

    @Test
    public void missingEncryptorIsABadRequest() throws Exception {
        // No CMS key configured for the app: unfixable by retrying, so it must dead-letter rather than spin.
        when(mockCache.get(APP_ID)).thenThrow(new CacheLoader.InvalidCacheLoadException("no encryptor"));

        try {
            fetcher.fetch(app, record, tempDir);
            fail("expected a bad request");
        } catch (PollSqsWorkerBadRequestException expected) {
            assertTrue(expected.getMessage().contains(APP_ID), expected.getMessage());
        }
    }

    @Test(expectedExceptions = WorkerException.class)
    public void encryptorCacheExecutionFailureIsAWorkerError() throws Exception {
        when(mockCache.get(APP_ID)).thenThrow(new UncheckedExecutionException(new RuntimeException("boom")));
        fetcher.fetch(app, record, tempDir);
    }

    @Test(expectedExceptions = WorkerException.class)
    public void checkedEncryptorCacheFailureIsAWorkerError() throws Exception {
        when(mockCache.get(APP_ID)).thenThrow(new ExecutionException(new RuntimeException("boom")));
        fetcher.fetch(app, record, tempDir);
    }

    @Test
    public void wrongEncryptionKeyIsABadRequest() throws Exception {
        // The record was encrypted with a key this app no longer holds. Retrying cannot help.
        doThrow(new WrongEncryptionKeyException("wrong key")).when(mockEncryptor).decrypt(any(InputStream.class));

        try {
            fetcher.fetch(app, record, tempDir);
            fail("expected a bad request");
        } catch (PollSqsWorkerBadRequestException expected) {
            assertTrue(expected.getMessage().contains(RECORD_ID), expected.getMessage());
        }
    }

    @Test(expectedExceptions = WorkerException.class)
    public void certificateEncodingFailureIsAWorkerError() throws Exception {
        doThrow(new CertificateEncodingException("bad cert")).when(mockEncryptor).decrypt(any(InputStream.class));
        fetcher.fetch(app, record, tempDir);
    }

    @Test(expectedExceptions = WorkerException.class)
    public void cmsFailureIsAWorkerError() throws Exception {
        doThrow(new CMSException("cms broke")).when(mockEncryptor).decrypt(any(InputStream.class));
        fetcher.fetch(app, record, tempDir);
    }

    @Test
    public void unreadableArchiveIsABadRequest() throws Exception {
        // Decrypt "succeeded" but produced something that is not a zip - a corrupt upload, not a transient fault.
        // ZipInputStream does not complain about non-zip input, it just reports no entries, so this would otherwise
        // sail through as an archive with zero members and be marked done in the ledger, unrecoverably.
        archiveZipBytes = "this is not a zip file".getBytes(Charsets.UTF_8);

        try {
            fetcher.fetch(app, record, tempDir);
            fail("expected a bad request");
        } catch (PollSqsWorkerBadRequestException expected) {
            assertTrue(expected.getMessage().contains("Empty or unreadable archive"), expected.getMessage());
        }
    }

    @Test(expectedExceptions = AmazonServiceException.class)
    public void s3FailurePropagates() throws Exception {
        // S3Helper.downloadS3File throws unchecked, so a transient S3 fault surfaces as a RuntimeException that the
        // worker maps to a WorkerException and retries. It must not be swallowed into an empty archive.
        doThrow(new AmazonServiceException("s3 unavailable")).when(mockS3Helper)
                .downloadS3File(anyString(), anyString(), any(File.class));
        fetcher.fetch(app, record, tempDir);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------------------------

    private static final class Member {
        final String name;
        final String content;

        Member(String name, String content) {
            this.name = name;
            this.content = content;
        }
    }

    private static Member member(String name, String content) {
        return new Member(name, content);
    }

    /** Build a real zip (via the real ZipHelper) out of the given members and return its bytes. */
    private byte[] zipOf(Member... members) throws Exception {
        File stagingDir = fileHelper.createTempDir();
        ImmutableList.Builder<File> files = ImmutableList.builder();
        for (Member member : members) {
            File file = fileHelper.newFile(stagingDir, member.name);
            fileHelper.writeBytes(file, member.content.getBytes(Charsets.UTF_8));
            files.add(file);
        }
        File zipFile = fileHelper.newFile(stagingDir, "archive.zip");
        zipHelper.zip(files.build(), zipFile);
        byte[] bytes = fileHelper.getBytes(zipFile);
        assertFalse(bytes.length == 0, "zip fixture should not be empty");
        return bytes;
    }


    /** Guard: the unzipped map is keyed by bare filename, which every builder relies on. */
    @Test
    public void unzippedFilesAreKeyedByBareName() throws Exception {
        DecryptedArchive archive = fetcher.fetch(app, record, tempDir);
        Map<String, File> unzipped = archive.getUnzippedFiles();
        assertTrue(unzipped.containsKey("info.json"), unzipped.keySet().toString());
    }
}
