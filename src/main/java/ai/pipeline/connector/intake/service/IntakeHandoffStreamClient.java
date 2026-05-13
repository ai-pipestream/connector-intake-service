package ai.pipeline.connector.intake.service;

import ai.pipestream.engine.v1.EngineV1ServiceGrpc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.engine.v1.IntakeHandoffStreamRequest;
import ai.pipestream.engine.v1.IntakeHandoffStreamResponse;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * intake-service → engine streaming intake-handoff lane. Mirror of the
 * kafka-sidecar's {@code IntakeHandoffStreamClient}. Keeps a single bidi
 * stream open to engine for the lifetime of the JVM so per-call buffer
 * allocation pressure cannot trigger {@code INTERNAL: Half-closed without
 * a request} on the per-doc handoff path.
 *
 * <p>Replaces N unary {@code intakeHandoff} calls per N docs with one
 * bidi stream for the whole intake. Each {@code intakeHandoff} call
 * blocks the calling virtual thread on a per-handoff future until the
 * engine acks.
 *
 * <p>Failure semantics:
 * <ul>
 *   <li>{@code STATUS_ACCEPTED} — engine admitted; method returns the
 *       inner {@code IntakeHandoffResponse}.</li>
 *   <li>{@code STATUS_RETRYABLE_REJECTED} — synthetic
 *       {@link StatusRuntimeException} with {@code UNAVAILABLE}.</li>
 *   <li>{@code STATUS_PERMANENT_REJECTED} — synthetic
 *       {@link StatusRuntimeException} with {@code FAILED_PRECONDITION}.</li>
 *   <li>Stream-level error → all in-flight futures fail; next
 *       {@link #intakeHandoff} call reopens the stream lazily.</li>
 * </ul>
 */
@ApplicationScoped
public class IntakeHandoffStreamClient {

    private static final Logger LOG = Logger.getLogger(IntakeHandoffStreamClient.class);

    @Inject
    @GrpcClient("engine")
    Channel engineChannel;

    @ConfigProperty(name = "pipestream.intake.engine.handoff-stream-ack-timeout-ms", defaultValue = "120000")
    long ackTimeoutMs;

    private EngineV1ServiceGrpc.EngineV1ServiceStub asyncStub;

    private final Map<String, CompletableFuture<IntakeHandoffStreamResponse>> inFlight =
            new ConcurrentHashMap<>();
    private final BlockingQueue<Outbound> outbound = new LinkedBlockingQueue<>();
    private final ExecutorService laneWriter = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("intake-handoff-stream-lane-", 0).factory());
    private volatile StreamObserver<IntakeHandoffStreamRequest> requestObserver;
    private final AtomicLong sequence = new AtomicLong();

    @PostConstruct
    void init() {
        this.asyncStub = EngineV1ServiceGrpc.newStub(engineChannel);
        startLaneWriter();
    }

    /**
     * Sends one intake handoff over the lane. Blocks the calling virtual
     * thread on a per-handoff future until the engine acks.
     *
     * @param request the engine handoff request
     * @return the engine's per-doc ack on STATUS_ACCEPTED
     * @throws StatusRuntimeException UNAVAILABLE on retryable rejection,
     *         FAILED_PRECONDITION on permanent rejection,
     *         DEADLINE_EXCEEDED on ack timeout
     */
    public IntakeHandoffResponse intakeHandoff(IntakeHandoffRequest request) {
        String handoffId = UUID.randomUUID().toString();
        CompletableFuture<IntakeHandoffStreamResponse> ackFuture = new CompletableFuture<>();
        inFlight.put(handoffId, ackFuture);

        try {
            outbound.put(new Outbound(handoffId, sequence.incrementAndGet(), request, ackFuture));
        } catch (InterruptedException ie) {
            inFlight.remove(handoffId);
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("intake handoff enqueue interrupted")
                    .withCause(ie)
                    .asRuntimeException();
        }

        IntakeHandoffStreamResponse ack;
        try {
            ack = ackFuture.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            inFlight.remove(handoffId);
            throw Status.DEADLINE_EXCEEDED
                    .withDescription("intake handoff ack exceeded "
                            + ackTimeoutMs + "ms (handoff=" + handoffId + ")")
                    .withCause(te)
                    .asRuntimeException();
        } catch (InterruptedException ie) {
            inFlight.remove(handoffId);
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("intake handoff ack wait interrupted")
                    .withCause(ie)
                    .asRuntimeException();
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
            if (cause instanceof StatusRuntimeException sre) {
                throw sre;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        }

        return mapAck(ack);
    }

    private static IntakeHandoffResponse mapAck(IntakeHandoffStreamResponse ack) {
        return switch (ack.getStatus()) {
            case STATUS_ACCEPTED -> ack.getResponse();
            case STATUS_PERMANENT_REJECTED -> throw Status.FAILED_PRECONDITION
                    .withDescription("intake handoff permanently rejected: "
                            + ack.getResponse().getMessage())
                    .asRuntimeException();
            case STATUS_RETRYABLE_REJECTED -> throw Status.UNAVAILABLE
                    .withDescription("intake handoff retryable rejection: "
                            + ack.getResponse().getMessage())
                    .asRuntimeException();
            default -> throw Status.UNKNOWN
                    .withDescription("unexpected ack status: " + ack.getStatus())
                    .asRuntimeException();
        };
    }

    private void startLaneWriter() {
        laneWriter.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                Outbound next;
                try {
                    next = outbound.take();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
                write(next);
            }
            return null;
        });
    }

    private void write(Outbound next) {
        if (inFlight.get(next.handoffId()) != next.response()) {
            return;
        }
        try {
            StreamObserver<IntakeHandoffStreamRequest> observer = requestObserver;
            if (observer == null) {
                observer = openStream();
                requestObserver = observer;
            }
            // Per-doc trace point #1 (intake → engine): log every send so we
            // can pin down whether a "silently-skipped at chunker" doc was
            // ever actually sent over the bidi stream from intake. Pairs with
            // the matching HANDOFF← ack log in onNext below — together they
            // bracket the time-on-wire for one doc.
            var sendStream = next.request().getStream();
            String docId = sendStream.hasDocument() ? sendStream.getDocument().getDocId()
                    : (sendStream.hasDocumentRef() ? sendStream.getDocumentRef().getDocId() : "no-doc");
            LOG.infof("HANDOFF→ intake.send seq=%d handoff=%s doc=%s ds=%s stream=%s sent_at=%d",
                    next.sequence(), next.handoffId(), docId,
                    next.request().getDatasourceId(), sendStream.getStreamId(),
                    System.currentTimeMillis());
            observer.onNext(IntakeHandoffStreamRequest.newBuilder()
                    .setHandoffId(next.handoffId())
                    .setSequence(next.sequence())
                    .setRequest(next.request())
                    .build());
        } catch (RuntimeException t) {
            inFlight.remove(next.handoffId());
            next.response().completeExceptionally(t);
            requestObserver = null;
        }
    }

    private StreamObserver<IntakeHandoffStreamRequest> openStream() {
        StreamObserver<IntakeHandoffStreamResponse> responses = new StreamObserver<>() {
            @Override
            public void onNext(IntakeHandoffStreamResponse response) {
                CompletableFuture<IntakeHandoffStreamResponse> future =
                        inFlight.remove(response.getHandoffId());
                // Per-doc trace point #2 (intake ← engine): log every ack
                // received from the engine. Compared to the matching HANDOFF→
                // log line above (same handoff_id + sequence), the timestamp
                // delta is the round-trip including engine admission. A
                // silently-skipped doc would either (a) never get an ack here
                // — meaning we'd see HANDOFF→ but no HANDOFF←, OR (b) get
                // an ack with status=ACCEPTED while the engine never actually
                // dispatched it, which is the silent-skip signature.
                LOG.infof("HANDOFF← intake.ack seq=%d handoff=%s status=%s recv_at=%d",
                        response.getSequence(), response.getHandoffId(),
                        response.getStatus().name(), System.currentTimeMillis());
                if (future == null) {
                    LOG.warnf("intakeHandoffStream received ack for unknown handoff_id=%s",
                            response.getHandoffId());
                    return;
                }
                future.complete(response);
            }

            @Override
            public void onError(Throwable t) {
                resetStream(t);
            }

            @Override
            public void onCompleted() {
                resetStream(new IllegalStateException(
                        "intakeHandoffStream completed by engine"));
            }
        };
        return asyncStub.intakeHandoffStream(responses);
    }

    private void resetStream(Throwable t) {
        requestObserver = null;
        inFlight.forEach((handoffId, future) -> {
            if (inFlight.remove(handoffId, future)) {
                future.completeExceptionally(t);
            }
        });
    }

    @PreDestroy
    void shutdown() {
        laneWriter.shutdownNow();
    }

    private record Outbound(
            String handoffId,
            long sequence,
            IntakeHandoffRequest request,
            CompletableFuture<IntakeHandoffStreamResponse> response) {
    }
}
