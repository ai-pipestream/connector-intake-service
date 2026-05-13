package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.data.v1.PipeDoc;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Handles the unary {@code UploadPipeDoc} RPC.
 *
 * <p>This service owns request-level validation, deterministic document-id
 * derivation, Tier 1 config resolution, and ownership stamping. Once the
 * document is ready for admission it delegates to {@link PipeDocAcceptanceService},
 * which selects the actual inline handoff path.
 */
@ApplicationScoped
public class UnaryPipeDocUploadService {

    private static final Logger LOG = Logger.getLogger(UnaryPipeDocUploadService.class);

    @Inject
    ConfigResolutionService configResolutionService;

    @Inject
    PipeDocIdDeriver pipeDocIdDeriver;

    @Inject
    PipeDocAcceptanceService pipeDocAcceptanceService;

    /**
     * Executes one unary upload request and translates ordinary validation or
     * downstream failures into the response object expected by the public RPC.
     */
    public void uploadPipeDoc(UploadPipeDocRequest request,
                              StreamObserver<UploadPipeDocResponse> responseObserver) {
        try {
            responseObserver.onNext(handleUploadPipeDoc(request));
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    UploadPipeDocResponse handleUploadPipeDoc(UploadPipeDocRequest request) {
        if (request.getDatasourceId().isBlank()) {
            return UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId(request.hasPipeDoc() ? request.getPipeDoc().getDocId() : "")
                    .setMessage("Validation error: datasource_id is required")
                    .build();
        }
        if (request.getApiKey().isBlank()) {
            return UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId(request.hasPipeDoc() ? request.getPipeDoc().getDocId() : "")
                    .setMessage("Validation error: api_key is required")
                    .build();
        }
        if (!request.hasPipeDoc()) {
            return UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: pipe_doc is required")
                    .build();
        }

        PipeDoc originalPipeDoc = request.getPipeDoc();
        PipeDocIdDeriver.DocIdDerivationResult derivation = pipeDocIdDeriver.derive(
                request.getDatasourceId(),
                originalPipeDoc.getDocId(),
                request.getSourceDocId(),
                originalPipeDoc.getSearchMetadata());

        if (derivation == null) {
            return UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: Cannot determine doc_id deterministically. "
                            + "Provide pipe_doc.doc_id, source_doc_id, search_metadata.source_uri, or search_metadata.source_path")
                    .build();
        }

        PipeDocHandoffDraft draft = new PipeDocHandoffDraft(
                originalPipeDoc,
                derivation.docId,
                derivation.toProto());

        long startTime = System.nanoTime();
        LOG.debugf("uploadPipeDoc START: datasource=%s, doc_id=%s (derived via %s)",
                request.getDatasourceId(), draft.docId(), derivation.method);

        try {
            ConfigResolutionService.ResolvedConfig resolved =
                    configResolutionService.resolveConfig(request.getDatasourceId(), request.getApiKey());
            long configTime = System.nanoTime() - startTime;
            LOG.debugf("uploadPipeDoc: config resolution took %.3f ms, persist=%s",
                    configTime / 1_000_000.0, resolved.shouldPersist());

            if (!originalPipeDoc.hasOwnership()) {
                LOG.debugf("uploadPipeDoc: stamped ownership from resolved config (account=%s, ds=%s)",
                        resolved.tier1Config().getAccountId(), resolved.tier1Config().getDatasourceId());
            }

            PipeDoc acceptedDoc = draft.buildForHandoff(resolved);
            String crawlId = request.getCrawlId();
            // Inline handoff is selected inside enqueueAndMaybePersist.
            // The default path enqueues to Redis; compatibility modes can
            // hand off directly to engine.
            return pipeDocAcceptanceService.enqueueAndMaybePersist(
                    acceptedDoc, resolved, crawlId, startTime);
        } catch (RuntimeException e) {
            long totalTime = System.nanoTime() - startTime;
            LOG.errorf(e, "Failed to upload PipeDoc after %.3f ms", totalTime / 1_000_000.0);
            return UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId(draft.docId())
                    .setMessage(e.getMessage())
                    .build();
        }
    }
}
