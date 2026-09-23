package org.sagebionetworks.bridge.addf.publish;

import java.io.ByteArrayInputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.config.Config;

/**
 * ADDF §4.5 — the crash-safe publish marker. A {@code _publish/<snapshotDate>.done} object is written to the export
 * store <b>only after</b> the Azure upload confirms completion (§4.4). A JVM crash mid-copy therefore leaves no marker,
 * so the day re-runs cleanly on the next scheduled message; overwrite-in-place per-blob upload makes the re-run
 * idempotent (no double-publish, no skip).
 *
 * <p>The presence of {@code _publish/<snapshotDate>.done} is also the S3-side signal for the Phase 6 publish-heartbeat
 * alarm ("did publish run today?").</p>
 */
@Component
public class PublishMarker {
    private static final Logger LOG = LoggerFactory.getLogger(PublishMarker.class);

    static final String ROOT_PREFIX = "biaffect-3/";
    static final String PUBLISH_PREFIX = ROOT_PREFIX + "_publish/";
    static final String CONFIG_KEY_EXPORTSTORE_BUCKET = "addf.exportstore.bucket";

    private AmazonS3 s3Client;
    private String bucket;

    @Autowired
    public final void setBridgeConfig(Config config) {
        this.bucket = config.get(CONFIG_KEY_EXPORTSTORE_BUCKET);
    }

    @Autowired
    public final void setAddfS3Client(@Qualifier("addfS3Client") AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    /** True when this snapshot has already been published (its {@code .done} marker exists). */
    public boolean isPublished(String snapshotDate) {
        return s3Client.doesObjectExist(bucket, markerKey(snapshotDate));
    }

    /** Write the {@code .done} marker — call only after the Azure upload has confirmed completion. */
    public void mark(String snapshotDate) {
        byte[] body = new byte[0];
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        metadata.setContentLength(body.length);
        s3Client.putObject(new PutObjectRequest(bucket, markerKey(snapshotDate),
                new ByteArrayInputStream(body), metadata));
        LOG.info("ADDF publish marker written: {}", markerKey(snapshotDate));
    }

    private String markerKey(String snapshotDate) {
        return PUBLISH_PREFIX + snapshotDate + ".done";
    }
}
