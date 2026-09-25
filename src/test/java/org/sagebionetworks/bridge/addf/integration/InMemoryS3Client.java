package org.sagebionetworks.bridge.addf.integration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * A real, in-process object store standing in for S3 in the ADDF integration tests: a bucket-keyed map of byte arrays
 * behind the slice of the {@link com.amazonaws.services.s3.AmazonS3} surface that {@code ExportStoreClient} and
 * {@code LedgerStore} actually use.
 *
 * <p><b>Why not a mock, and why not LocalStack.</b> A Mockito mock of {@code AmazonS3} can only replay what the test
 * stubs, so a test built on one proves the worker calls the methods the test already expected — it cannot catch a key
 * that is written in one shape and read back in another, a listing that misses a prefix, or a consolidated file that
 * is overwritten rather than merged. Those are precisely the ADDF failure modes. LocalStack would give the same
 * fidelity for this slice but costs a Docker daemon in CI. This keeps the storage <em>semantics</em> honest —
 * round-tripping real Parquet bytes through real keys — with no new dependency and no daemon.</p>
 *
 * <p>Deliberately faithful in three places the pipeline depends on: {@code listObjectsV2} is prefix-filtered and
 * <b>lexicographically ordered</b> (S3 guarantees this, and the publish builder's dedup relies on stable iteration);
 * {@code putObject} overwrites in place, which is what makes the consolidated-file writes idempotent; and
 * {@code getObject} into a {@link File} is the download path the publish worker reads every staged object through.</p>
 */
public class InMemoryS3Client extends AbstractAmazonS3 {
    /** bucket -> (key -> bytes), key-ordered so listings come back the way S3 returns them. */
    private final Map<String, TreeMap<String, byte[]>> buckets = new TreeMap<>();

    /**
     * bucket -> (key -> last-modified). S3 stamps every object and returns it on LIST, and {@code PublishLease} reads
     * exactly that to decide whether a held lease has gone stale — a listing without it silently makes every lease
     * look absent, so the fake has to carry it.
     */
    private final Map<String, TreeMap<String, Date>> lastModified = new TreeMap<>();

    /** Number of {@code putObject} calls, so tests can assert a file was written once rather than repeatedly. */
    private int putCount;

    @Override
    public PutObjectResult putObject(PutObjectRequest request) {
        byte[] body;
        if (request.getFile() != null) {
            try {
                body = Files.readAllBytes(request.getFile().toPath());
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        } else {
            try (InputStream in = request.getInputStream()) {
                body = drain(in);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        bucket(request.getBucketName()).put(request.getKey(), body);
        timestamps(request.getBucketName()).put(request.getKey(), new Date());
        putCount++;
        return new PutObjectResult();
    }

    @Override
    public ObjectMetadata getObject(GetObjectRequest request, File destination) {
        byte[] body = bucket(request.getBucketName()).get(request.getKey());
        if (body == null) {
            throw new IllegalStateException("no such key: " + request.getKey());
        }
        try (OutputStream out = new FileOutputStream(destination)) {
            out.write(body);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return new ObjectMetadata();
    }

    @Override
    public S3Object getObject(String bucketName, String key) {
        byte[] body = bucket(bucketName).get(key);
        if (body == null) {
            return null;
        }
        S3Object object = new S3Object();
        object.setBucketName(bucketName);
        object.setKey(key);
        object.setObjectContent(new ByteArrayInputStream(body));
        return object;
    }

    @Override
    public boolean doesObjectExist(String bucketName, String key) {
        return bucket(bucketName).containsKey(key);
    }

    @Override
    public ListObjectsV2Result listObjectsV2(ListObjectsV2Request request) {
        String prefix = request.getPrefix() == null ? "" : request.getPrefix();
        ListObjectsV2Result result = new ListObjectsV2Result();
        result.setBucketName(request.getBucketName());
        result.setPrefix(prefix);
        for (Map.Entry<String, byte[]> entry : bucket(request.getBucketName()).entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                S3ObjectSummary summary = new S3ObjectSummary();
                summary.setBucketName(request.getBucketName());
                summary.setKey(entry.getKey());
                summary.setSize(entry.getValue().length);
                summary.setLastModified(timestamps(request.getBucketName()).get(entry.getKey()));
                result.getObjectSummaries().add(summary);
            }
        }
        // Single page: the real client pages, and ExportStoreClient.listKeys loops on isTruncated, which this
        // exercises by terminating immediately.
        result.setTruncated(false);
        return result;
    }

    @Override
    public void deleteObject(String bucketName, String key) {
        bucket(bucketName).remove(key);
        timestamps(bucketName).remove(key);
    }

    // -----------------------------------------------------------------------------------------------------------
    // Test-side inspection
    // -----------------------------------------------------------------------------------------------------------

    /** Every key in the bucket, in S3 order. */
    public List<String> keys(String bucketName) {
        return new ArrayList<>(bucket(bucketName).keySet());
    }

    /** Every key under a prefix, in S3 order. */
    public List<String> keysUnder(String bucketName, String prefix) {
        List<String> matches = new ArrayList<>();
        for (String key : bucket(bucketName).keySet()) {
            if (key.startsWith(prefix)) {
                matches.add(key);
            }
        }
        return matches;
    }

    public byte[] get(String bucketName, String key) {
        return bucket(bucketName).get(key);
    }

    public boolean exists(String bucketName, String key) {
        return doesObjectExist(bucketName, key);
    }

    public int getPutCount() {
        return putCount;
    }

    public void resetPutCount() {
        putCount = 0;
    }

    private TreeMap<String, byte[]> bucket(String name) {
        return buckets.computeIfAbsent(name, n -> new TreeMap<>());
    }

    private TreeMap<String, Date> timestamps(String name) {
        return lastModified.computeIfAbsent(name, n -> new TreeMap<>());
    }

    /** Back-date an object's last-modified stamp, so a test can age a lease past its TTL without sleeping. */
    public void backdate(String bucketName, String key, long millisAgo) {
        timestamps(bucketName).put(key, new Date(System.currentTimeMillis() - millisAgo));
    }

    private static byte[] drain(InputStream in) throws IOException {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }
}
