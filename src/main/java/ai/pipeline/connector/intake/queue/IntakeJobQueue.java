package ai.pipeline.connector.intake.queue;

import ai.pipestream.engine.v1.IntakeHandoffRequest;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.list.ListCommands;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Producer side of the Redis-backed intake job queue.
 *
 * <p>The intake bidi-stream handler hands ownership of each accepted
 * document to this queue. The handoff is a single {@code LPUSH} of the
 * serialized {@link IntakeHandoffRequest} bytes — sub-millisecond on a
 * healthy Redis. The bidi stream's HTTP/2 flow control naturally
 * back-pressures the connector when Redis itself slows down; no
 * separate in-process buffer is needed.
 *
 * <p>This class is intentionally narrow. It owns the queue keys and the
 * producer call. All consumer-side concerns (recovery, dedup, DLQ,
 * attempt counting) live in {@link IntakeJobWorker}.
 *
 * <p>Layout:
 * <ul>
 *   <li>{@link #MAIN_QUEUE} — list of pending handoffs. Producers
 *       {@code LPUSH}; workers atomically {@code BLMOVE} into a private
 *       per-worker queue before processing.</li>
 *   <li>{@link #DLQ} — list of poisoned items (max attempts exceeded or
 *       malformed payload). Inspected by humans, not by the workers.</li>
 *   <li>{@code intake:queue:attempts:<stream_id>} — integer counter
 *       (per-stream attempt count). Cleared on success or on DLQ.</li>
 *   <li>{@code intake:queue:processed:<stream_id>} — string TTL key set
 *       after engine accepts a handoff. Used to short-circuit replays
 *       caused by a recovered private queue (e.g. {@code lrem} failed
 *       after engine acked).</li>
 * </ul>
 */
@ApplicationScoped
public class IntakeJobQueue {

    /** Main queue producers push to and workers move out of. */
    public static final String MAIN_QUEUE = "intake:queue:engine-handoff";

    /** Dead-letter queue for items that exceeded the retry budget. */
    public static final String DLQ = "intake:queue:engine-handoff:dlq";

    /** Per-worker private queue prefix. Worker name appended. */
    public static final String PRIVATE_QUEUE_PREFIX = "intake:queue:engine-handoff:worker:";

    /** Per-stream attempt counter key prefix; full key {@code …:<stream_id>}. */
    public static final String ATTEMPTS_KEY_PREFIX = "intake:queue:attempts:";

    /** Per-stream "engine accepted" marker key prefix; TTL'd. */
    public static final String PROCESSED_KEY_PREFIX = "intake:queue:processed:";

    @Inject
    RedisDataSource redis;

    private ListCommands<String, byte[]> lists;

    /** Default constructor for CDI. */
    public IntakeJobQueue() {}

    @PostConstruct
    void init() {
        this.lists = redis.list(byte[].class);
    }

    /**
     * Push a fully-built engine handoff request onto the main queue. The
     * intake bidi-stream handler calls this synchronously and acks the
     * streaming item immediately on return.
     *
     * @param request the handoff request to enqueue; must already carry
     *                a stable {@code stream_id} so retries land on the
     *                same key
     */
    public void submit(IntakeHandoffRequest request) {
        lists.lpush(MAIN_QUEUE, request.toByteArray());
    }

    /**
     * Returns the private-queue key for a given worker name. Workers use
     * this to scope their {@code BLMOVE} target.
     */
    public static String privateQueueFor(String workerName) {
        return PRIVATE_QUEUE_PREFIX + workerName;
    }

    /**
     * Returns the attempt-counter key for a given stream id.
     */
    public static String attemptsKeyFor(String streamId) {
        return ATTEMPTS_KEY_PREFIX + streamId;
    }

    /**
     * Returns the processed-marker key for a given stream id.
     */
    public static String processedKeyFor(String streamId) {
        return PROCESSED_KEY_PREFIX + streamId;
    }
}
