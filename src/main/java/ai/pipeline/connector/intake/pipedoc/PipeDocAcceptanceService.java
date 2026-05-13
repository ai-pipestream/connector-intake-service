package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.ingress.IntakeIngressProducer;
import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipeline.connector.intake.service.EngineClient;
import ai.pipestream.connector.intake.v1.DocReference;
import ai.pipestream.connector.intake.v1.PipeDocItem;
import ai.pipestream.connector.intake.v1.StreamContext;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Per-doc acceptance for the gRPC upload paths
 * ({@link ai.pipeline.connector.intake.pipedoc.streaming.StreamingPipeDocObserver bidi}
 * and {@link UnaryPipeDocUploadService unary}). Two responsibilities:
 *
 * <ol>
 *   <li>Route inline documents according to
 *       {@code pipestream.intake.inline-handoff-mode}. The default
 *       {@code redis} mode enqueues to {@code pipestream:intake:ingress};
 *       compatibility modes can call the engine directly.</li>
 *   <li><b>If {@code resolved.shouldPersist()} is true</b>: schedule a
 *       fire-and-forget durable copy via {@link IntakeReplayPersister}.
 *       Runs on its own VT executor; failure logs but does not block,
 *       fail, or delay the engine handoff. Used for replay; not currently
 *       on any read path.</li>
 * </ol>
 */
@ApplicationScoped
public class PipeDocAcceptanceService {

    private static final Logger LOG = Logger.getLogger(PipeDocAcceptanceService.class);

    @Inject
    PipeDocIdDeriver docIdDeriver;

    @Inject
    IntakeIngressProducer intakeIngressProducer;

    @Inject
    IntakeReplayPersister replayPersister;

    @Inject
    EngineClient engineClient;

    @ConfigProperty(name = "pipestream.intake.inline-handoff-mode", defaultValue = "redis")
    String inlineHandoffMode;

    /**
     * Accepts a fully-hydrated inline PipeDoc into the configured handoff path.
     *
     * <p>Redis mode writes an {@link ai.pipestream.engine.v1.IntakeHandoffRequest}
     * to the intake ingress stream for the sidecar to drain. Compatibility modes
     * call engine directly. In all modes, replay persistence is optional,
     * asynchronous, and not part of the admission decision.
     *
     * @param pipeDoc document after doc-id derivation and ownership stamping
     * @param resolved datasource/account configuration for this request
     * @param crawlId optional crawl session identifier
     * @param startTime start time from the transport handler, used for timing logs
     * @return upload response for the connector-facing API
     */
    public UploadPipeDocResponse enqueueAndMaybePersist(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            String crawlId,
            long startTime) {

        // Optional replay-copy persist. Fire-and-forget; no gating of engine path.
        if (resolved.shouldPersist()) {
            replayPersister.persistAsync(pipeDoc,
                    resolved.tier1Config().getAccountId(),
                    resolved.tier1Config().getDatasourceId());
        }

        var ingestionConfig = resolved.withIngressMode(IngressMode.INGRESS_MODE_GRPC_INLINE);
        var request = EngineClient.buildHandoffRequest(
                pipeDoc,
                resolved.tier1Config().getDatasourceId(),
                resolved.tier1Config().getAccountId(),
                ingestionConfig,
                crawlId);

        String mode = inlineHandoffMode == null ? "redis" : inlineHandoffMode.trim().toLowerCase();
        if ("engine-unary".equals(mode)) {
            return handoffInlineToEngineUnary(pipeDoc, resolved, ingestionConfig, crawlId, startTime);
        }
        if ("engine-stream".equals(mode)) {
            return handoffInlineToEngineStream(pipeDoc, resolved, ingestionConfig, crawlId, startTime);
        }
        if (!"redis".equals(mode)) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Unsupported pipestream.intake.inline-handoff-mode: " + inlineHandoffMode)
                    .asRuntimeException();
        }

        IntakeIngressProducer.EnqueueResult enqueueResult =
                intakeIngressProducer.enqueue(request, pipeDoc.getDocId());
        long totalTime = System.nanoTime() - startTime;
        LOG.debugf("uploadPipeDoc: accepted into Redis ingress in %.3f ms (message_id=%s, persist=%s)",
                totalTime / 1_000_000.0, enqueueResult.messageId(), resolved.shouldPersist());

        return UploadPipeDocResponse.newBuilder()
                .setSuccess(true)
                .setDocId(pipeDoc.getDocId())
                .setMessage("Document accepted by intake; engine ingress queued")
                .build();
    }

    private UploadPipeDocResponse handoffInlineToEngineUnary(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            IngestionConfig ingestionConfig,
            String crawlId,
            long startTime) {
        var response = engineClient.handoffToEngineUnary(
                pipeDoc,
                resolved.tier1Config().getDatasourceId(),
                resolved.tier1Config().getAccountId(),
                ingestionConfig,
                crawlId);
        return mapEngineResponse("engine unary", pipeDoc.getDocId(), response, startTime);
    }

    private UploadPipeDocResponse handoffInlineToEngineStream(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            IngestionConfig ingestionConfig,
            String crawlId,
            long startTime) {
        var response = engineClient.handoffToEngine(
                pipeDoc,
                resolved.tier1Config().getDatasourceId(),
                resolved.tier1Config().getAccountId(),
                ingestionConfig,
                crawlId);
        return mapEngineResponse("engine stream", pipeDoc.getDocId(), response, startTime);
    }

    private UploadPipeDocResponse mapEngineResponse(
            String path,
            String docId,
            IntakeHandoffResponse handoffResponse,
            long startTime) {
        long totalTime = System.nanoTime() - startTime;
        LOG.debugf("uploadPipeDoc: %s handoff completed in %.3f ms, accepted=%s",
                path, totalTime / 1_000_000.0, handoffResponse.getAccepted());

        if (!handoffResponse.getAccepted()) {
            return UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId(docId)
                    .setMessage("Engine rejected: " + handoffResponse.getMessage())
                    .build();
        }

        return UploadPipeDocResponse.newBuilder()
                .setSuccess(true)
                .setDocId(docId)
                .setMessage("Document engine accepted via " + path)
                .build();
    }

    public UploadPipeDocStreamResponse acceptStreamingItem(
            PipeDocItem item,
            StreamContext ctx,
            ConfigResolutionService.ResolvedConfig resolved) {

        PipeDoc originalPipeDoc = item.getPipeDoc();
        PipeDocIdDeriver.DocIdDerivationResult derivation = docIdDeriver.derive(
                ctx.getDatasourceId(),
                originalPipeDoc.getDocId(),
                item.getSourceDocId(),
                originalPipeDoc.getSearchMetadata());

        if (derivation == null) {
            return UploadPipeDocStreamResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("cannot determine doc_id deterministically")
                    .setRetryable(false)
                    .setRef(DocReference.newBuilder()
                            .setSourceDocId(item.getSourceDocId())
                            .build())
                    .build();
        }

        PipeDoc.Builder pipeDocBuilder = originalPipeDoc.toBuilder()
                .setDocId(derivation.docId)
                .setDocIdDerivation(derivation.toProto());
        if (!originalPipeDoc.hasOwnership()) {
            pipeDocBuilder.setOwnership(OwnershipContext.newBuilder()
                    .setAccountId(resolved.tier1Config().getAccountId())
                    .setDatasourceId(resolved.tier1Config().getDatasourceId())
                    .build());
        }
        PipeDoc pipeDoc = pipeDocBuilder.build();

        long startTime = System.nanoTime();
        String crawlId = ctx.getCrawlId();
        try {
            UploadPipeDocResponse unaryResp = enqueueAndMaybePersist(pipeDoc, resolved, crawlId, startTime);

            return UploadPipeDocStreamResponse.newBuilder()
                    .setSuccess(unaryResp.getSuccess())
                    .setMessage(unaryResp.getMessage())
                    .setRetryable(false)
                    .setRef(DocReference.newBuilder()
                            .setSourceDocId(item.getSourceDocId())
                            .setDocId(unaryResp.getDocId())
                            .build())
                    .build();
        } catch (RuntimeException throwable) {
            return UploadPipeDocStreamResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(throwable.getMessage())
                    .setRetryable(isRetryable(throwable))
                    .setRef(DocReference.newBuilder()
                            .setSourceDocId(item.getSourceDocId())
                            .setDocId(pipeDoc.getDocId())
                            .build())
                    .build();
        }
    }

    private static boolean isRetryable(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException sre) {
            Status.Code code = sre.getStatus().getCode();
            if (code == Status.Code.RESOURCE_EXHAUSTED
                    || code == Status.Code.UNAVAILABLE
                    || code == Status.Code.DEADLINE_EXCEEDED
                    || code == Status.Code.ABORTED) {
                return true;
            }
            if (code == Status.Code.INTERNAL) {
                String description = sre.getStatus().getDescription();
                return description != null && description.contains("Half-closed without a request");
            }
        }
        return false;
    }
}
