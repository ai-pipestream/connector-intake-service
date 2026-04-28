package ai.pipeline.connector.intake.queue;

import ai.pipeline.connector.intake.service.EngineClient;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.registration.LocalServiceIdentity;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Lifecycle owner for the pool of {@link IntakeJobWorker} virtual threads.
 *
 * <p>Worker names follow the same identity Consul sees for this process —
 * {@code <serviceId>-w<index>} where {@code serviceId} is
 * {@code <name>-<host>-<port>} from {@link LocalServiceIdentity}. That
 * keeps per-replica private queues unique across scaled-out deployments
 * without inventing a separate naming scheme.
 */
@ApplicationScoped
public class IntakeJobWorkerPool {

    private static final Logger LOG = Logger.getLogger(IntakeJobWorkerPool.class);

    @Inject
    RedisDataSource redis;

    @Inject
    EngineClient engineClient;

    @Inject
    LocalServiceIdentity identity;

    /**
     * Master switch for the worker pool. Disable in tests that don't
     * exercise end-to-end queue behaviour — the {@link IntakeJobWorker}
     * unit tests build their own workers against the dev-services Redis
     * and don't want the production pool competing for queue items.
     */
    @ConfigProperty(name = "pipestream.intake.queue.enabled", defaultValue = "true")
    boolean enabled;

    @ConfigProperty(name = "pipestream.intake.queue.workers", defaultValue = "16")
    int workerCount;

    @ConfigProperty(name = "pipestream.intake.queue.max-attempts", defaultValue = "8")
    int maxAttempts;

    @ConfigProperty(name = "pipestream.intake.queue.handoff-timeout-ms", defaultValue = "5000")
    long handoffTimeoutMs;

    @ConfigProperty(name = "pipestream.intake.queue.processed-ttl-sec", defaultValue = "3600")
    long processedTtlSec;

    @ConfigProperty(name = "pipestream.intake.queue.soft-fail-backoff-ms", defaultValue = "100")
    long softFailBackoffMs;

    @ConfigProperty(name = "pipestream.intake.queue.blmove-timeout-ms", defaultValue = "1000")
    long blmoveTimeoutMs;

    private final List<IntakeJobWorker> workers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    /** Default constructor for CDI. */
    public IntakeJobWorkerPool() {}

    void onStart(@Observes StartupEvent ev) {
        if (!enabled) {
            LOG.info("IntakeJobWorkerPool disabled via pipestream.intake.queue.enabled=false");
            return;
        }
        Function<IntakeHandoffRequest, IntakeHandoffResponse> handoff = req ->
                engineClient.handoffToEngine(req)
                        .await().atMost(Duration.ofMillis(handoffTimeoutMs));
        startWorkers(handoff);
    }

    void startWorkers(Function<IntakeHandoffRequest, IntakeHandoffResponse> handoff) {
        String serviceId = identity.serviceId();
        for (int i = 0; i < workerCount; i++) {
            String workerName = serviceId + "-w" + i;
            IntakeJobWorker worker = new IntakeJobWorker(
                    workerName,
                    redis,
                    handoff,
                    maxAttempts,
                    Duration.ofMillis(handoffTimeoutMs),
                    Duration.ofSeconds(processedTtlSec),
                    softFailBackoffMs,
                    Duration.ofMillis(blmoveTimeoutMs));
            Thread t = Thread.ofVirtual()
                    .name("intake-job-worker-", i)
                    .start(worker);
            workers.add(worker);
            threads.add(t);
        }
        LOG.infof("IntakeJobWorkerPool started: %d workers (serviceId=%s, max-attempts=%d, handoff-timeout=%dms)",
                workerCount, serviceId, maxAttempts, handoffTimeoutMs);
    }

    void onShutdown(@Observes ShutdownEvent ev) {
        LOG.infof("IntakeJobWorkerPool shutting down %d workers", workers.size());
        for (IntakeJobWorker w : workers) w.stop();
        for (Thread t : threads) t.interrupt();
    }

    /** For tests: number of currently-registered workers. */
    public int workerCount() { return workers.size(); }
}
