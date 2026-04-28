package ai.pipeline.connector.intake.queue;

import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.list.ListCommands;
import io.quarkus.redis.datasource.list.Position;
import io.quarkus.redis.datasource.value.SetArgs;
import io.quarkus.redis.datasource.value.ValueCommands;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * One worker that drains the Redis-backed intake job queue and calls the
 * engine for each item.
 *
 * <p>Plain class (not CDI). Instances are created and lifecycled by
 * {@link IntakeJobWorkerPool}. Engine handoff is injected as a
 * {@link Function} so unit tests can stub it without spinning up an
 * engine.
 *
 * <p>Pattern (Quarkus "Redis Job Queue Reloaded" — atomic-move with
 * private queue + retry counter + DLQ):
 * <pre>
 *   on start: drain own private queue (recovery)
 *   loop while running:
 *     BLMOVE main -> private RIGHT LEFT 1s
 *     drain private queue
 * </pre>
 *
 * <p>Per-item: peek, check dedup, increment attempt counter, call engine.
 * On engine acceptance: write the dedup key with TTL, {@code LREM} the
 * item, clear the attempt counter. On engine rejection or transport
 * failure: leave the item in the private queue, sleep a short backoff,
 * retry. On {@code attempts > maxAttempts}: push to DLQ and remove.
 *
 * <p>Crash safety: if the worker dies between {@code BLMOVE} and
 * processing, the item is still in the private queue; the next start
 * picks it up via the recovery drain. If the worker dies between engine
 * acceptance and {@code LREM}, the dedup key prevents a redundant engine
 * call on replay.
 */
public class IntakeJobWorker implements Runnable {

    private static final Logger LOG = Logger.getLogger(IntakeJobWorker.class);

    private final String workerName;
    private final String privateQueue;
    private final int maxAttempts;
    private final Duration handoffTimeout;
    private final Duration processedTtl;
    private final long softFailBackoffMs;
    private final Duration blmoveTimeout;

    private final ListCommands<String, byte[]> lists;
    private final ValueCommands<String, Long> attempts;
    private final ValueCommands<String, String> markers;
    private final KeyCommands<String> keys;
    private final Function<IntakeHandoffRequest, IntakeHandoffResponse> engineHandoff;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public IntakeJobWorker(String workerName,
                           RedisDataSource redis,
                           Function<IntakeHandoffRequest, IntakeHandoffResponse> engineHandoff,
                           int maxAttempts,
                           Duration handoffTimeout,
                           Duration processedTtl,
                           long softFailBackoffMs,
                           Duration blmoveTimeout) {
        this.workerName = workerName;
        this.privateQueue = IntakeJobQueue.privateQueueFor(workerName);
        this.maxAttempts = maxAttempts;
        this.handoffTimeout = handoffTimeout;
        this.processedTtl = processedTtl;
        this.softFailBackoffMs = softFailBackoffMs;
        this.blmoveTimeout = blmoveTimeout;

        this.lists = redis.list(byte[].class);
        this.attempts = redis.value(Long.class);
        this.markers = redis.value(String.class);
        this.keys = redis.key();
        this.engineHandoff = engineHandoff;
    }

    public String workerName() { return workerName; }
    public String privateQueueKey() { return privateQueue; }

    public void stop() { running.set(false); }

    @Override
    public void run() {
        running.set(true);
        LOG.infof("IntakeJobWorker %s started (private=%s)", workerName, privateQueue);
        // Recovery: anything left in our private queue from a prior life
        // gets drained before we touch the main queue.
        drainPrivate();

        while (running.get()) {
            try {
                byte[] moved = lists.blmove(
                        IntakeJobQueue.MAIN_QUEUE,
                        privateQueue,
                        Position.RIGHT,
                        Position.LEFT,
                        blmoveTimeout);
                if (moved != null) {
                    drainPrivate();
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    LOG.infof("IntakeJobWorker %s interrupted, exiting", workerName);
                    return;
                }
                LOG.errorf(e, "IntakeJobWorker %s: blmove loop error", workerName);
                sleep(500);
            }
        }
        LOG.infof("IntakeJobWorker %s stopped", workerName);
    }

    /**
     * Drains the worker's private queue. Returns when the queue is empty
     * or processing of the head item indicates a soft-fail (engine not
     * yet ready); the soft-fail path sleeps a short backoff and retries
     * the same head item rather than abandoning it.
     */
    void drainPrivate() {
        while (running.get()) {
            byte[] head = lists.lindex(privateQueue, -1);
            if (head == null) return;
            ProcessOutcome outcome = processOne(head);
            if (outcome == ProcessOutcome.SOFT_FAIL) {
                sleep(softFailBackoffMs);
                // loop again, retry same item
            }
            // ACK (success / DLQ / dedup-hit / malformed) — head was removed,
            // continue draining.
        }
    }

    enum ProcessOutcome {
        ACK,        // item handled (success, DLQ, dedup, or malformed) — removed from queue
        SOFT_FAIL,  // transient — item left in queue, caller should backoff and retry
    }

    ProcessOutcome processOne(byte[] raw) {
        IntakeHandoffRequest request;
        try {
            request = IntakeHandoffRequest.parseFrom(raw);
        } catch (InvalidProtocolBufferException e) {
            LOG.errorf(e, "IntakeJobWorker %s: malformed payload — sending raw bytes to DLQ", workerName);
            lists.lpush(IntakeJobQueue.DLQ, raw);
            lists.lrem(privateQueue, 1, raw);
            return ProcessOutcome.ACK;
        }

        String streamId = request.getStream().getStreamId();
        String docId = request.getStream().hasDocument()
                ? request.getStream().getDocument().getDocId() : "no-doc";

        // Dedup short-circuit: if engine already accepted this stream_id
        // in a prior life (lrem failed after success), we just remove the
        // queue entry and skip the engine call.
        String processedKey = IntakeJobQueue.processedKeyFor(streamId);
        if (keys.exists(processedKey)) {
            LOG.warnf("IntakeJobWorker %s: dedup hit stream=%s doc=%s — skipping replay",
                    workerName, streamId, docId);
            lists.lrem(privateQueue, 1, raw);
            return ProcessOutcome.ACK;
        }

        long attemptCount = attempts.incr(IntakeJobQueue.attemptsKeyFor(streamId));
        if (attemptCount > maxAttempts) {
            LOG.errorf("IntakeJobWorker %s: TERMINAL drop stream=%s doc=%s after %d attempts → DLQ",
                    workerName, streamId, docId, attemptCount - 1);
            lists.lpush(IntakeJobQueue.DLQ, raw);
            lists.lrem(privateQueue, 1, raw);
            keys.del(IntakeJobQueue.attemptsKeyFor(streamId));
            return ProcessOutcome.ACK;
        }

        IntakeHandoffResponse response;
        try {
            response = engineHandoff.apply(request);
        } catch (RuntimeException e) {
            LOG.warnf("IntakeJobWorker %s: handoff threw stream=%s attempt=%d: %s — leaving for retry",
                    workerName, streamId, attemptCount, e.getMessage());
            return ProcessOutcome.SOFT_FAIL;
        }

        if (response != null && response.getAccepted()) {
            // Success: mark dedup BEFORE lrem so a crash between the two
            // is harmless (next startup sees dedup, removes the orphan).
            markers.set(processedKey, "1", new SetArgs().ex(processedTtl.toSeconds()));
            lists.lrem(privateQueue, 1, raw);
            keys.del(IntakeJobQueue.attemptsKeyFor(streamId));
            return ProcessOutcome.ACK;
        }

        // Engine returned non-accepted (queue full, validation soft-fail).
        // Leave in queue, attempt counter already incremented.
        String reason = response == null ? "null response" : response.getMessage();
        LOG.warnf("IntakeJobWorker %s: engine rejected stream=%s attempt=%d reason=%s — leaving for retry",
                workerName, streamId, attemptCount, reason);
        return ProcessOutcome.SOFT_FAIL;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
