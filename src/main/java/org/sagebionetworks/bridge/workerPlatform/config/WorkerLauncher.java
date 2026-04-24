package org.sagebionetworks.bridge.workerPlatform.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PreDestroy;

import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.sqs.PollSqsWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Launches worker threads. This hooks into the Spring Boot command-line runner, which is really just a big
 * Runnable-equivalent that Spring Boot knows about.
 */
@Component("GeneralWorkerLauncher")
public class WorkerLauncher implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerLauncher.class);

    private HeartbeatLogger heartbeatLogger;
    private Map<String, PollSqsWorker> pollSqsWorkers;
    private final List<Thread> workerThreads = new ArrayList<>();

    @Autowired
    public final void setHeartbeatLogger(HeartbeatLogger heartbeatLogger) {
        this.heartbeatLogger = heartbeatLogger;
    }

    @Autowired
    public final void setPollSqsWorkers(Map<String, PollSqsWorker> pollSqsWorkers) {
        this.pollSqsWorkers = pollSqsWorkers;
    }

    /**
     * Main entry point into the app. Should only be called by Spring Boot.
     *
     * @param args
     *         command-line args
     */
    @Override
    public void run(String... args) {
        LOG.info("Worker Platform Starting heartbeat...");
        new Thread(heartbeatLogger).start();

        for (Map.Entry<String, PollSqsWorker> entry : pollSqsWorkers.entrySet()) {
            LOG.info("Worker Platform Starting " + entry.getKey() + "...");
            Thread t = new Thread(entry.getValue());
            workerThreads.add(t);
            t.start();
        }
    }

    // Interrupt polling threads during Spring context shutdown so they stop before the AWS SDK shutdown hook
    // closes connection pools, preventing the "Connection pool shut down" error storm on restart.
    @PreDestroy
    public void shutdown() {
        LOG.info("Worker Platform shutting down polling threads...");
        for (Thread t : workerThreads) {
            t.interrupt();
        }
    }
}
