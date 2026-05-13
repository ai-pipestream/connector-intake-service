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
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.PipeDoc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Per-doc acceptance for the gRPC upload paths
 * ({@link ai.pipeline.connector.intake.pipedoc.streaming.StreamingPipeDocObserver bidi}
 * and {@link UnaryPipeDocUploadService unary}). Two responsibilities:
 *
 * <ol>
 *   <li><b>Always</b> enqueue the inline document to the
 *       {@code pipestream:intake:ingress} Redis stream. The kafka-sidecar
 *       drains that stream and hands off to the engine via bidi
 *       {@code intakeHandoffStream}. Intake never calls the engine
 *       directly on this path.</li>
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

    /**
     * Enqueue a fully-hydrated document onto the Redis ingress stream and
     * (optionally, async) trigger a replay-copy persist. The engine is NOT
     * called from here — the kafka-sidecar drains the stream and does that.
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
