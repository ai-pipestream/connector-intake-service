package ai.pipeline.connector.intake.queue;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.keys.KeyCommands;
import io.quarkus.redis.datasource.list.ListCommands;
import io.quarkus.redis.datasource.value.ValueCommands;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link IntakeJobWorker} against a real Redis (Quarkus
 * dev services). Tests target the per-item logic in {@code processOne}
 * and the multi-item iteration in {@code drainPrivate} — the BLMOVE
 * outer loop is exercised by {@link IntakeJobWorkerLoopTest}.
 */
@QuarkusTest
@TestProfile(IntakeQueueTestProfile.class)
class IntakeJobWorkerTest {

    @Inject
    RedisDataSource redis;

    private ListCommands<String, byte[]> lists;
    private KeyCommands<String> keys;
    private ValueCommands<String, Long> attempts;
    private ValueCommands<String, String> markers;

    private static final int MAX_ATTEMPTS = 3;
    private static final Duration HANDOFF_TIMEOUT = Duration.ofMillis(500);
    private static final Duration PROCESSED_TTL = Duration.ofSeconds(60);
    private static final long SOFT_FAIL_BACKOFF_MS = 1; // tiny, so tests don't sleep
    private static final Duration BLMOVE_TIMEOUT = Duration.ofMillis(50);

    @BeforeEach
    void clean() {
        lists = redis.list(byte[].class);
        keys = redis.key();
        attempts = redis.value(Long.class);
        markers = redis.value(String.class);
        // Wipe all queue-related keys between tests.
        keys.del(IntakeJobQueue.MAIN_QUEUE, IntakeJobQueue.DLQ);
        keys.keys(IntakeJobQueue.PRIVATE_QUEUE_PREFIX + "*").forEach(keys::del);
        keys.keys(IntakeJobQueue.ATTEMPTS_KEY_PREFIX + "*").forEach(keys::del);
        keys.keys(IntakeJobQueue.PROCESSED_KEY_PREFIX + "*").forEach(keys::del);
    }

    private IntakeJobWorker newWorker(String name,
                                       Function<IntakeHandoffRequest, IntakeHandoffResponse> handoff) {
        return new IntakeJobWorker(name, redis, handoff, MAX_ATTEMPTS,
                HANDOFF_TIMEOUT, PROCESSED_TTL, SOFT_FAIL_BACKOFF_MS, BLMOVE_TIMEOUT);
    }

    private static IntakeHandoffRequest sampleRequest(String streamId, String docId) {
        return IntakeHandoffRequest.newBuilder()
                .setStream(PipeStream.newBuilder()
                        .setStreamId(streamId)
                        .setDocument(PipeDoc.newBuilder().setDocId(docId).build())
                        .build())
                .setDatasourceId("ds-1")
                .setAccountId("acc-1")
                .build();
    }

    private static IntakeHandoffResponse accepted() {
        return IntakeHandoffResponse.newBuilder().setAccepted(true).build();
    }

    private static IntakeHandoffResponse rejected(String reason) {
        return IntakeHandoffResponse.newBuilder()
                .setAccepted(false).setMessage(reason).build();
    }

    @Test
    @DisplayName("processOne: engine accepts → item removed, dedup key set, attempts cleared")
    void engineAcceptsAcksAndMarksDedup() {
        IntakeHandoffRequest req = sampleRequest("s-1", "doc-1");
        byte[] raw = req.toByteArray();

        IntakeJobWorker worker = newWorker("w-test-1", r -> accepted());
        // Seed the private queue directly to bypass BLMOVE for this unit test.
        lists.lpush(worker.privateQueueKey(), raw);

        IntakeJobWorker.ProcessOutcome outcome = worker.processOne(raw);

        assertThat(outcome).as("engine accepted → ACK").isEqualTo(IntakeJobWorker.ProcessOutcome.ACK);
        assertThat(lists.llen(worker.privateQueueKey())).as("private queue drained").isZero();
        assertThat(keys.exists(IntakeJobQueue.processedKeyFor("s-1"))).as("dedup marker set").isTrue();
        assertThat(keys.exists(IntakeJobQueue.attemptsKeyFor("s-1"))).as("attempts cleared on success").isFalse();
    }

    @Test
    @DisplayName("processOne: engine rejects → item stays, attempts increments, returns SOFT_FAIL")
    void engineRejectsLeavesItemInQueue() {
        IntakeHandoffRequest req = sampleRequest("s-soft", "doc-soft");
        byte[] raw = req.toByteArray();

        IntakeJobWorker worker = newWorker("w-test-soft", r -> rejected("queue full"));
        lists.lpush(worker.privateQueueKey(), raw);

        IntakeJobWorker.ProcessOutcome outcome = worker.processOne(raw);

        assertThat(outcome).isEqualTo(IntakeJobWorker.ProcessOutcome.SOFT_FAIL);
        assertThat(lists.llen(worker.privateQueueKey())).as("item still queued for retry").isEqualTo(1L);
        assertThat(attempts.get(IntakeJobQueue.attemptsKeyFor("s-soft")))
                .as("attempt count incremented").isEqualTo(1L);
    }

    @Test
    @DisplayName("processOne: engine throws → SOFT_FAIL, item retained")
    void engineThrowsLeavesItemForRetry() {
        IntakeHandoffRequest req = sampleRequest("s-throw", "doc-throw");
        byte[] raw = req.toByteArray();

        IntakeJobWorker worker = newWorker("w-test-throw",
                r -> { throw new RuntimeException("simulated transport blip"); });
        lists.lpush(worker.privateQueueKey(), raw);

        IntakeJobWorker.ProcessOutcome outcome = worker.processOne(raw);

        assertThat(outcome).isEqualTo(IntakeJobWorker.ProcessOutcome.SOFT_FAIL);
        assertThat(lists.llen(worker.privateQueueKey())).isEqualTo(1L);
        assertThat(attempts.get(IntakeJobQueue.attemptsKeyFor("s-throw"))).isEqualTo(1L);
    }

    @Test
    @DisplayName("processOne: attempts > maxAttempts → moved to DLQ, attempts cleared")
    void exhaustedAttemptsGoToDlq() {
        IntakeHandoffRequest req = sampleRequest("s-dlq", "doc-dlq");
        byte[] raw = req.toByteArray();

        IntakeJobWorker worker = newWorker("w-test-dlq", r -> rejected("still full"));
        lists.lpush(worker.privateQueueKey(), raw);

        // Pre-set the attempts counter so the next incr pushes us over maxAttempts.
        attempts.set(IntakeJobQueue.attemptsKeyFor("s-dlq"), (long) MAX_ATTEMPTS);

        IntakeJobWorker.ProcessOutcome outcome = worker.processOne(raw);

        assertThat(outcome).isEqualTo(IntakeJobWorker.ProcessOutcome.ACK);
        assertThat(lists.llen(worker.privateQueueKey())).as("removed from private after DLQ").isZero();
        assertThat(lists.llen(IntakeJobQueue.DLQ)).as("pushed onto DLQ").isEqualTo(1L);
        assertThat(keys.exists(IntakeJobQueue.attemptsKeyFor("s-dlq")))
                .as("attempts cleared after DLQ").isFalse();
    }

    @Test
    @DisplayName("processOne: dedup marker present → engine NOT called, item removed")
    void dedupHitSkipsEngine() {
        IntakeHandoffRequest req = sampleRequest("s-dedup", "doc-dedup");
        byte[] raw = req.toByteArray();

        AtomicInteger engineCalls = new AtomicInteger();
        IntakeJobWorker worker = newWorker("w-test-dedup", r -> {
            engineCalls.incrementAndGet();
            return accepted();
        });

        // Pre-mark stream as processed (simulating a prior life that crashed
        // between engine accept and lrem).
        markers.set(IntakeJobQueue.processedKeyFor("s-dedup"), "1");
        lists.lpush(worker.privateQueueKey(), raw);

        IntakeJobWorker.ProcessOutcome outcome = worker.processOne(raw);

        assertThat(outcome).isEqualTo(IntakeJobWorker.ProcessOutcome.ACK);
        assertThat(engineCalls.get()).as("engine not called when dedup marker present").isZero();
        assertThat(lists.llen(worker.privateQueueKey())).as("item removed").isZero();
    }

    @Test
    @DisplayName("processOne: malformed bytes → straight to DLQ")
    void malformedPayloadGoesToDlq() {
        byte[] garbage = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF};
        IntakeJobWorker worker = newWorker("w-test-malformed", r -> accepted());
        lists.lpush(worker.privateQueueKey(), garbage);

        IntakeJobWorker.ProcessOutcome outcome = worker.processOne(garbage);

        assertThat(outcome).isEqualTo(IntakeJobWorker.ProcessOutcome.ACK);
        assertThat(lists.llen(worker.privateQueueKey())).isZero();
        assertThat(lists.llen(IntakeJobQueue.DLQ)).isEqualTo(1L);
    }

    @Test
    @DisplayName("drainPrivate: processes multiple queued items in FIFO order")
    void drainProcessesAllQueuedItems() {
        List<String> seenStreamIds = new ArrayList<>();
        IntakeJobWorker worker = newWorker("w-drain-fifo", r -> {
            seenStreamIds.add(r.getStream().getStreamId());
            return accepted();
        });

        // Producer pushes with LPUSH (left); consumer reads at right (-1) =
        // oldest-first FIFO. Push 1,2,3 → consume 1,2,3 in order.
        for (int i = 1; i <= 3; i++) {
            lists.lpush(worker.privateQueueKey(),
                    sampleRequest("s-fifo-" + i, "doc-" + i).toByteArray());
        }

        // drainPrivate needs the worker's `running` flag set; the only public
        // entry that does that is run(), so we need to start running on a
        // thread and stop quickly. Easier: call drainPrivate after marking
        // running through a tiny helper — workerName-scoped method. Here we
        // invoke the public run() but stop the worker right after the drain
        // by closing the queues and observing state.
        Thread t = Thread.ofVirtual().start(worker);
        // Wait until drain finishes (queue empty).
        long deadline = System.currentTimeMillis() + 5_000;
        while (lists.llen(worker.privateQueueKey()) > 0 && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(20); } catch (InterruptedException ignored) {}
        }
        worker.stop();
        t.interrupt();

        assertThat(seenStreamIds).as("FIFO order preserved")
                .containsExactly("s-fifo-1", "s-fifo-2", "s-fifo-3");
        assertThat(lists.llen(worker.privateQueueKey())).isZero();
    }

    @Test
    @DisplayName("recovery: items in private queue at start are drained before BLMOVE main")
    void recoveryDrainsPrivateBeforeMain() {
        AtomicInteger calls = new AtomicInteger();
        IntakeJobWorker worker = newWorker("w-recovery-" + UUID.randomUUID(), r -> {
            calls.incrementAndGet();
            return accepted();
        });

        // Pre-seed the worker's private queue (simulating a crash recovery).
        for (int i = 1; i <= 5; i++) {
            lists.lpush(worker.privateQueueKey(),
                    sampleRequest("s-rec-" + i, "doc-rec-" + i).toByteArray());
        }
        // Main queue stays empty — the recovery path must drain private without
        // any BLMOVE having returned a new item.
        Thread t = Thread.ofVirtual().start(worker);
        long deadline = System.currentTimeMillis() + 5_000;
        while (calls.get() < 5 && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(20); } catch (InterruptedException ignored) {}
        }
        worker.stop();
        t.interrupt();

        assertThat(calls.get()).as("recovered all 5 from private queue").isEqualTo(5);
        assertThat(lists.llen(worker.privateQueueKey())).isZero();
    }
}
