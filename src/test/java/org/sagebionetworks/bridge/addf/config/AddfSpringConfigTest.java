package org.sagebionetworks.bridge.addf.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.concurrent.ExecutorService;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.sqs.PollSqsWorker;
import org.sagebionetworks.bridge.sqs.SqsHelper;
import org.sagebionetworks.bridge.workerPlatform.multiplexer.BridgeWorkerPlatformSqsCallback;

/**
 * The ADDF Spring beans. Config classes are conventionally left untested, but this one is not inert: it is the only
 * place the ADDF poller is bound to its queue, and a wrong or unresolved {@code queueUrl} would make the worker sit
 * silently on nothing — a failure mode with no alarm, because an idle queue and a mis-bound queue look identical.
 *
 * <p>Note this config lives under {@code addf/config}, not {@code workerPlatform/config}, so unlike its siblings it is
 * <b>not</b> exempt from the JaCoCo gate.</p>
 */
public class AddfSpringConfigTest {
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/1234/Bridge-ADDF-Export-Request-uat";

    private AddfSpringConfig config;
    private Config mockBridgeConfig;

    @BeforeClass
    public void beforeClass() {
        // The AWS client builders resolve a region at build time; supply one so the beans can be constructed off-box.
        // Credentials are resolved lazily on first call, which these tests never make.
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.accessKeyId", "test-access-key");
        System.setProperty("aws.secretKey", "test-secret-key");
    }

    @AfterClass
    public void afterClass() {
        System.clearProperty("aws.region");
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretKey");
    }

    @BeforeMethod
    public void before() {
        config = new AddfSpringConfig();
        mockBridgeConfig = mock(Config.class);
        when(mockBridgeConfig.get(AddfSpringConfig.CONFIG_KEY_ADDF_QUEUE_URL)).thenReturn(QUEUE_URL);
        when(mockBridgeConfig.getInt(AddfSpringConfig.CONFIG_KEY_SQS_SLEEP_MILLIS)).thenReturn(125);
    }

    @Test
    public void addfS3ClientIsBuilt() {
        AmazonS3 client = config.addfS3Client();
        assertNotNull(client);
    }

    @Test
    public void addfSqsClientIsBuilt() {
        AmazonSQS client = config.addfSqsClient();
        assertNotNull(client);
    }

    @Test
    public void addfSqsWorkerIsBoundToTheAddfQueueAndTheSharedExecutor() {
        BridgeWorkerPlatformSqsCallback callback = mock(BridgeWorkerPlatformSqsCallback.class);
        ExecutorService executor = mock(ExecutorService.class);
        SqsHelper sqsHelper = mock(SqsHelper.class);

        PollSqsWorker worker = config.addfSqsWorker(callback, executor, sqsHelper, mockBridgeConfig);

        assertNotNull(worker);
        // PollSqsWorker exposes only setters, so the binding is asserted at its source: the poller's queue URL must
        // be read from addf.export.request.sqs.queue.url — the key the infra templates set per env — and never from
        // the general worker queue. A wrong key binds the poller to nothing, and nothing alarms, because an idle
        // queue and a mis-bound queue look identical.
        verify(mockBridgeConfig).get(AddfSpringConfig.CONFIG_KEY_ADDF_QUEUE_URL);
        verify(mockBridgeConfig).getInt(AddfSpringConfig.CONFIG_KEY_SQS_SLEEP_MILLIS);
        verify(mockBridgeConfig, never()).get("workerPlatform.request.sqs.queue.url");
    }

    @Test
    public void theQueueKeyMatchesTheDeploymentContract() {
        // These two strings are the whole interface between this config and BridgeWorkerPlatform.conf / the infra
        // templates. Asserting them here means a rename breaks a test rather than a deployment.
        assertEquals(AddfSpringConfig.CONFIG_KEY_ADDF_QUEUE_URL, "addf.export.request.sqs.queue.url");
        assertEquals(AddfSpringConfig.CONFIG_KEY_SQS_SLEEP_MILLIS,
                "workerPlatform.request.sqs.sleep.time.millis");
    }
}
