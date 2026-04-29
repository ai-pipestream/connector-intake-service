package ai.pipeline.connector.intake.pipedoc.streaming;

import ai.pipeline.connector.intake.pipedoc.PipeDocAcceptanceService;
import ai.pipeline.connector.intake.pipedoc.PersistedPipeDocHandler;
import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jboss.logging.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StreamingPipeDocObserver implements StreamObserver<UploadPipeDocStreamRequest> {

    private static final Logger LOG = Logger.getLogger(StreamingPipeDocObserver.class);

    private final StreamObserver<UploadPipeDocStreamResponse> responses;
    private final ServerCallStreamObserver<UploadPipeDocStreamResponse> serverResponses;
    private final ConfigResolutionService configResolutionService;
    private final PipeDocAcceptanceService pipeDocAcceptanceService;
    private final PersistedPipeDocHandler persistedPipeDocHandler;
    private final BlockingQueue<StreamEvent> mailbox = new ArrayBlockingQueue<>(2);

    private ai.pipestream.connector.intake.v1.StreamContext context;
    private ConfigResolutionService.ResolvedConfig resolvedConfig;
    private volatile boolean stopped;

    public StreamingPipeDocObserver(
            StreamObserver<UploadPipeDocStreamResponse> responses,
            ConfigResolutionService configResolutionService,
            PipeDocAcceptanceService pipeDocAcceptanceService,
            PersistedPipeDocHandler persistedPipeDocHandler) {
        this.responses = responses;
        this.configResolutionService = configResolutionService;
        this.pipeDocAcceptanceService = pipeDocAcceptanceService;
        this.persistedPipeDocHandler = persistedPipeDocHandler;
        this.serverResponses = responses instanceof ServerCallStreamObserver<UploadPipeDocStreamResponse> server
                ? server
                : null;
        if (serverResponses != null) {
            serverResponses.disableAutoInboundFlowControl();
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
            responses.onError(Status.RESOURCE_EXHAUSTED
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
                    responses.onCompleted();
                    return;
                }

                UploadPipeDocStreamResponse response = handleStreamingRequest(event.request());
                responses.onNext(response);
                requestNext();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopped = true;
                responses.onError(Status.CANCELLED
                        .withDescription("stream worker interrupted")
                        .withCause(e)
                        .asRuntimeException());
                return;
            } catch (RuntimeException t) {
                stopped = true;
                responses.onError(t);
                return;
            }
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
                    request.getItem(), context, resolvedConfig, persistedPipeDocHandler);
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
