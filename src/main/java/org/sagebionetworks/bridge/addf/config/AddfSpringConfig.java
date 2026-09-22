package org.sagebionetworks.bridge.addf.config;

import java.util.concurrent.ExecutorService;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.sqs.PollSqsWorker;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.workerPlatform.multiplexer.BridgeWorkerPlatformSqsCallback;

/**
 * ADDF §3.8 — the extra SQS poller for the ADDF request queue, plus a dedicated S3 client for the export store. This
 * is a <b>new</b> {@code @Configuration}, component-scanned alongside the existing
 * {@code workerPlatform.config.SpringConfig} (via {@code @ComponentScan("org.sagebionetworks.bridge")}), so no edit to
 * that file is needed.
 *
 * <ul>
 *   <li>The poller reuses the existing {@link BridgeWorkerPlatformSqsCallback} (which dispatches by {@code service}
 *       name, so both {@code AddfExportWorker} and {@code AddfParticipantVersionWorker} ride this one queue) and the
 *       existing {@code generalExecutorService}. {@code WorkerLauncher} autowires {@code Map<String, PollSqsWorker>},
 *       so this third poller bean is picked up automatically — no {@code WorkerLauncher} edit.</li>
 *   <li><b>Isolation tradeoff (§3.8):</b> sharing the 12-thread {@code generalExecutorService} means heavy ADDF work
 *       could cause {@code CallerRunsPolicy} backpressure on the E3 pool. Start shared; if per-record ADDF proves
 *       heavy, add a dedicated executor here (no existing-file edit needed).</li>
 * </ul>
 */
@Configuration
public class AddfSpringConfig {
    static final String CONFIG_KEY_ADDF_QUEUE_URL = "addf.export.request.sqs.queue.url";
    static final String CONFIG_KEY_SQS_SLEEP_MILLIS = "workerPlatform.request.sqs.sleep.time.millis";

    /** Dedicated S3 client for the export-store bucket (used by ExportStoreClient + LedgerStore). */
    @Bean(name = "addfS3Client")
    public AmazonS3 addfS3Client() {
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxConnections(50)
                .withConnectionTimeout(10_000)
                .withSocketTimeout(60_000)
                .withConnectionTTL(60_000);
        return AmazonS3ClientBuilder.standard()
                .withClientConfiguration(clientConfig)
                .build();
    }

    /** Dedicated SQS client for the ADDF backfill to enqueue AddfParticipantVersionWorker messages (Phase 3b.4). */
    @Bean(name = "addfSqsClient")
    public AmazonSQS addfSqsClient() {
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxConnections(50)
                .withConnectionTimeout(10_000)
                .withSocketTimeout(60_000)
                .withConnectionTTL(60_000);
        return AmazonSQSClientBuilder.standard()
                .withClientConfiguration(clientConfig)
                .build();
    }

    /** New poller on the ADDF request queue. Auto-registered into WorkerLauncher's poller map. */
    @Bean(name = "addfSqsWorker")
    @Autowired
    public PollSqsWorker addfSqsWorker(BridgeWorkerPlatformSqsCallback callback,
            @Qualifier("generalExecutorService") ExecutorService generalExecutorService,
            SqsHelper sqsHelper, Config config) {
        PollSqsWorker sqsWorker = new PollSqsWorker();
        sqsWorker.setCallback(callback);
        sqsWorker.setExecutorService(generalExecutorService);
        sqsWorker.setQueueUrl(config.get(CONFIG_KEY_ADDF_QUEUE_URL));
        sqsWorker.setSleepTimeMillis(config.getInt(CONFIG_KEY_SQS_SLEEP_MILLIS));
        sqsWorker.setSqsHelper(sqsHelper);
        return sqsWorker;
    }
}
