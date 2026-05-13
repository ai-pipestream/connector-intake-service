package ai.pipeline.connector.intake.pipedoc.streaming;

import ai.pipeline.connector.intake.pipedoc.PipeDocAcceptanceService;
import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jboss.logging.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Serializes one bidi {@code UploadPipeDocStream} call onto a small mailbox.
 *
 * <p>The gRPC transport can deliver cancellation, request messages, and
 * completion callbacks from different threads. This observer turns those events
 * into one ordered stream processed by a virtual-thread worker, so context
 * validation, per-item admission, and response writes happen in a predictable
 * sequence. Manual inbound flow control keeps at most a small number of
 * unprocessed stream events in memory.
 */
public class StreamingPipeDocObserver implements StreamObserver<UploadPipeDocStreamRequest> {

    private static final Logger LOG = Logger.getLogger(StreamingPipeDocObserver.class);

    private final StreamObserver<UploadPipeDocStreamResponse> responses;
    private final ServerCallStreamObserver<UploadPipeDocStreamResponse> serverResponses;
    private final ConfigResolutionService configResolutionService;
    private final PipeDocAcceptanceService pipeDocAcceptanceService;
    private final BlockingQueue<StreamEvent> mailbox = new ArrayBlockingQueue<>(2);

    private ai.pipestream.connector.intake.v1.StreamContext context;
    private ConfigResolutionService.ResolvedConfig resolvedConfig;
    private volatile boolean stopped;

    public StreamingPipeDocObserver(
            StreamObserver<UploadPipeDocStreamResponse> responses,
            ConfigResolutionService configResolutionService,
            PipeDocAcceptanceService pipeDocAcceptanceService) {
        this.responses = responses;
        this.configResolutionService = configResolutionService;
        this.pipeDocAcceptanceService = pipeDocAcceptanceService;
        this.serverResponses = responses instanceof ServerCallStreamObserver<UploadPipeDocStreamResponse> server
                ? server
                : null;
        if (serverResponses != null) {
            serverResponses.disableAutoInboundFlowControl();
            // Standard gRPC server-streaming idiom: when the client cancels
            // (deadline expired, RST_STREAM, network died, client crashed),
            // the framework closes the response observer underneath us. The
            // cancel handler stops the worker VT before it tries to write
            // into a closed observer, which would throw
            // IllegalStateException("call already closed"). Without this,
            // every client-side cancel during an in-flight upload produced
            // an uncaught exception in the worker VT and 1-2 dropped acks
            // per stream; the JDBC connector then tagged those as
            // "retryable failures" and the wrapping retry loop made the
            // problem look like a sustained outage.
            serverResponses.setOnCancelHandler(() -> stopped = true);
        }
        Thread.startVirtualThread(this::runMailbox);
    }

    @Override
    public void onNext(UploadPipeDocStreamRequest request) {
        offer(StreamEvent.request(request));
    }

    @Override
    public void onError(Throwable t) {
        offer(StreamEvent.error(t));
    }

    @Override
    public void onCompleted() {
        offer(StreamEvent.completeEvent());
    }

    public void requestNext() {
        if (!stopped && serverResponses != null && !serverResponses.isCancelled()) {
            serverResponses.request(1);
        }
    }

    private void offer(StreamEvent event) {
        if (stopped) {
            return;
        }
        if (!mailbox.offer(event)) {
            stopped = true;
            safeResponseOnError(Status.RESOURCE_EXHAUSTED
                    .withDescription("stream mailbox full; inbound flow control was exceeded")
                    .asRuntimeException());
        }
    }

    private void runMailbox() {
        requestNext();
        while (!stopped) {
            try {
                StreamEvent event = mailbox.take();
                if (event.error() != null) {
                    stopped = true;
                    LOG.warnf(event.error(), "uploadPipeDocStream: client stream failed");
                    return;
                }
                if (event.completed()) {
                    stopped = true;
                    safeResponseOnCompleted();
                    return;
                }

                UploadPipeDocStreamResponse response = handleStreamingRequest(event.request());
                if (!safeResponseOnNext(response)) {
                    // Client is already gone; no point processing more.
                    stopped = true;
                    return;
                }
                requestNext();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopped = true;
                safeResponseOnError(Status.CANCELLED
                        .withDescription("stream worker interrupted")
                        .withCause(e)
                        .asRuntimeException());
                return;
            } catch (RuntimeException t) {
                stopped = true;
                safeResponseOnError(t);
                return;
            }
        }
    }

    /**
     * Writes one response onto the response observer, but only if the client
     * hasn't cancelled. Returns {@code false} when the write was skipped
     * because the stream is already closed (so the caller can stop pumping).
     *
     * <p>Wrapping every write through this guard is the standard gRPC
     * server-streaming pattern. The {@link
     * ServerCallStreamObserver#isCancelled()} check rules out the normal
     * cancel race; the try/catch on {@link IllegalStateException} catches
     * the rare "framework closed between isCancelled() and onNext()" race.
     */
    private boolean safeResponseOnNext(UploadPipeDocStreamResponse response) {
        if (serverResponses != null && serverResponses.isCancelled()) {
            return false;
        }
        try {
            responses.onNext(response);
            return true;
        } catch (IllegalStateException alreadyClosed) {
            LOG.debugf("response stream already closed; dropping ack source_doc_id=%s",
                    response.getRef().getSourceDocId());
            return false;
        }
    }

    private void safeResponseOnCompleted() {
        if (serverResponses != null && serverResponses.isCancelled()) {
            return;
        }
        try {
            responses.onCompleted();
        } catch (IllegalStateException alreadyClosed) {
            LOG.debugf("response stream already closed; skipping onCompleted");
        }
    }

    private void safeResponseOnError(Throwable t) {
        if (serverResponses != null && serverResponses.isCancelled()) {
            return;
        }
        try {
            responses.onError(t);
        } catch (IllegalStateException alreadyClosed) {
            LOG.debugf("response stream already closed; skipping onError");
        }
    }

    private UploadPipeDocStreamResponse handleStreamingRequest(UploadPipeDocStreamRequest request) {
        if (request.hasContext()) {
            var ctx = request.getContext();
            LOG.infof("uploadPipeDocStream: opening stream for datasource=%s, crawl=%s, sub=%d/%d, client=%s",
                    ctx.getDatasourceId(), ctx.getCrawlId(),
                    ctx.getSubCrawlIndex(), ctx.getTotalSubCrawls(), ctx.getClientId());
            context = ctx;
            try {
                resolvedConfig = configResolutionService.resolveConfig(ctx.getDatasourceId(), ctx.getApiKey());
                return UploadPipeDocStreamResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("stream context accepted")
                        .build();
            } catch (RuntimeException throwable) {
                LOG.warnf(throwable, "uploadPipeDocStream: context resolution failed for datasource=%s",
                        ctx.getDatasourceId());
                return UploadPipeDocStreamResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("stream context rejected: " + throwable.getMessage())
                        .setRetryable(false)
                        .build();
            }
        }
        if (request.hasItem()) {
            if (context == null || resolvedConfig == null) {
                return UploadPipeDocStreamResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("first message must be StreamContext before any PipeDocItem")
                        .setRetryable(false)
                        .build();
            }
            return pipeDocAcceptanceService.acceptStreamingItem(
                    request.getItem(), context, resolvedConfig);
        }
        if (request.hasDeleteRef()) {
            return UploadPipeDocStreamResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("delete via stream not yet implemented; use DeletePipeDoc unary RPC")
                    .setRetryable(false)
                    .setRef(request.getDeleteRef())
                    .build();
        }
        return UploadPipeDocStreamResponse.newBuilder()
                .setSuccess(false)
                .setMessage("unrecognized stream payload - expected context, item, or delete_ref")
                .setRetryable(false)
                .build();
    }

    private record StreamEvent(
            UploadPipeDocStreamRequest request,
            Throwable error,
            boolean completed) {
        static StreamEvent request(UploadPipeDocStreamRequest request) {
            return new StreamEvent(request, null, false);
        }

        static StreamEvent error(Throwable error) {
            return new StreamEvent(null, error, false);
        }

        static StreamEvent completeEvent() {
            return new StreamEvent(null, null, true);
        }
    }
}
