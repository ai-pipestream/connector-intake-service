package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.MutinyConnectorIntakeServiceGrpc;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.connector.intake.v1.UploadBlobRequest;
import ai.pipestream.connector.intake.v1.UploadBlobResponse;
import ai.pipestream.connector.intake.v1.StartCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.StartCrawlSessionResponse;
import ai.pipestream.connector.intake.v1.EndCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.EndCrawlSessionResponse;
import ai.pipestream.connector.intake.v1.HeartbeatRequest;
import ai.pipestream.connector.intake.v1.HeartbeatResponse;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.OwnershipContext;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.DocIdDerivation;
import ai.pipestream.data.v1.DocIdDerivationMethod;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipeline.connector.intake.util.UriCanonicalizer;
import ai.pipestream.repository.filesystem.upload.v1.MutinyNodeUploadServiceGrpc;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

/**
 * gRPC service implementation handling connector ingestion endpoints.
 * <p>
 * Design: Intake is graph-agnostic. It handles Tier 1 configuration only.
 * Engine handles Tier 2 (graph-level) config resolution and routing.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Validates connector identity and API key via ConnectorValidationService.</li>
 *   <li>Resolves Tier 1 configuration via ConfigResolutionService.</li>
 *   <li>Conditionally persists documents to repository-service based on Tier 1 config.</li>
 *   <li>Hands off documents to engine (engine resolves graph routing).</li>
 *   <li>Manages crawl session lifecycle (start, heartbeat, end).</li>
 * </ul>
 */
@Singleton
@GrpcService
public class ConnectorIntakeServiceImpl extends MutinyConnectorIntakeServiceGrpc.ConnectorIntakeServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ConnectorIntakeServiceImpl.class);
    private static final String INTAKE_SOURCE_NODE_ID = "connector-intake";

    @Inject
    ConfigResolutionService configResolutionService;

    @Inject
    EngineClient engineClient;

    @Inject
    SessionManager sessionManager;

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /**
     * Default constructor for CDI / gRPC.
     */
    public ConnectorIntakeServiceImpl() {}

    /**
     * Deterministically derive a doc_id for a document.
     * <p>
     * Priority order (first match wins):
     * 1. Client-provided doc_id (if present)
     * 2. source_doc_id from request
     * 3. source_uri from search_metadata (canonicalized)
     * 4. source_path from search_metadata (normalized)
     * <p>
     * Returns null if no deterministic derivation is possible.
     */
    private DocIdDerivationResult deriveDocId(String datasourceId, String clientDocId, String sourceDocId,
                                              SearchMetadata searchMetadata) {
        // 1. Client-provided doc_id (highest priority)
        if (clientDocId != null && !clientDocId.isBlank()) {
            return new DocIdDerivationResult(
                clientDocId,
                DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CLIENT_PROVIDED,
                clientDocId
            );
        }

        // 2. source_doc_id from request
        if (sourceDocId != null && !sourceDocId.isBlank()) {
            String derivedId = datasourceId + ":" + sourceDocId;
            return new DocIdDerivationResult(
                derivedId,
                DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_SOURCE_DOC_ID,
                sourceDocId
            );
        }

        // 3. source_uri from search_metadata (canonicalized)
        if (searchMetadata != null && !searchMetadata.getSourceUri().isBlank()) {
            String canonicalUri = canonicalizeUri(searchMetadata.getSourceUri());
            String derivedId = datasourceId + ":" + canonicalUri;
            return new DocIdDerivationResult(
                derivedId,
                DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_SOURCE_URI,
                searchMetadata.getSourceUri()
            );
        }

        // 4. source_path from search_metadata (normalized)
        if (searchMetadata != null && !searchMetadata.getSourcePath().isBlank()) {
            String normalizedPath = normalizePath(searchMetadata.getSourcePath());
            String derivedId = datasourceId + ":" + normalizedPath;
            return new DocIdDerivationResult(
                derivedId,
                DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_SOURCE_PATH,
                searchMetadata.getSourcePath()
            );
        }

        // No deterministic derivation possible
        return null;
    }

    /**
     * Result of doc_id derivation containing the derived ID and provenance info.
     */
    private static class DocIdDerivationResult {
        final String docId;
        final DocIdDerivationMethod method;
        final String input;

        DocIdDerivationResult(String docId, DocIdDerivationMethod method, String input) {
            this.docId = docId;
            this.method = method;
            this.input = input;
        }

        DocIdDerivation toProto() {
            return DocIdDerivation.newBuilder()
                .setMethod(method)
                .setInput(input)
                .setCanonicalInput(docId.substring(docId.indexOf(':') + 1)) // Everything after datasource_id:
                .build();
        }
    }

    /**
     * Canonicalize a URI for consistent hashing using comprehensive normalization.
     * <p>
     * Delegates to {@link UriCanonicalizer} for proper URI validation and normalization:
     * - Validates URI syntax
     * - Normalizes path navigation (../, ./)
     * - Lowercases scheme and host only (preserves path/query case)
     * - Removes trailing slashes (except root)
     * - Sorts query parameters alphabetically
     * - Removes default ports (:80, :443)
     * - Removes fragments
     *
     * @param uri The raw URI to canonicalize
     * @return Canonicalized URI string, or null if input is null
     */
    private String canonicalizeUri(String uri) {
        if (uri == null || uri.isBlank()) {
            return null;
        }
        try {
            return UriCanonicalizer.canonicalizeUri(uri, true); // Keep query params
        } catch (IllegalArgumentException e) {
            LOG.warnf("Failed to canonicalize URI '%s': %s - using original", uri, e.getMessage());
            return uri.trim(); // Fallback to trimmed original on validation failure
        }
    }

    /**
     * Normalize a file path for consistent hashing.
     * <p>
     * Normalization rules:
     * - Remove leading/trailing whitespace
     * - Normalize separators (forward slashes)
     * - Remove redundant separators
     * - Remove leading slash (for relative paths)
     * - Handles both Unix and Windows-style paths
     *
     * @param path The raw path to normalize
     * @return Normalized path string, or null if input is null
     */
    private String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        // Normalize Windows backslashes to forward slashes
        String normalized = path.trim().replace('\\', '/');
        // Remove redundant slashes
        normalized = normalized.replaceAll("/+", "/");
        // Remove leading slash (for relative paths)
        normalized = normalized.replaceAll("^/", "");
        // Remove trailing slash
        normalized = normalized.replaceAll("/$", "");
        return normalized;
    }

    @Override
    public Uni<UploadPipeDocResponse> uploadPipeDoc(UploadPipeDocRequest request) {
        // Validation: all intake requests require datasource_id and api_key.
        if (request.getDatasourceId().isBlank()) {
            return Uni.createFrom().item(
                UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId(request.hasPipeDoc() ? request.getPipeDoc().getDocId() : "")
                    .setMessage("Validation error: datasource_id is required")
                    .build()
            );
        }
        if (request.getApiKey().isBlank()) {
            return Uni.createFrom().item(
                UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId(request.hasPipeDoc() ? request.getPipeDoc().getDocId() : "")
                    .setMessage("Validation error: api_key is required")
                    .build()
            );
        }

        // Validation: gRPC PipeDoc uploads require a deterministically derivable doc_id.
        if (!request.hasPipeDoc()) {
            return Uni.createFrom().item(
                UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: pipe_doc is required")
                    .build()
            );
        }

        PipeDoc originalPipeDoc = request.getPipeDoc();

        // Try to derive doc_id deterministically
        DocIdDerivationResult derivation = deriveDocId(
            request.getDatasourceId(),
            originalPipeDoc.getDocId(),
            request.getSourceDocId(),
            originalPipeDoc.getSearchMetadata()
        );

        if (derivation == null) {
            return Uni.createFrom().item(
                UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: Cannot determine doc_id deterministically. " +
                        "Provide pipe_doc.doc_id, source_doc_id, search_metadata.source_uri, or search_metadata.source_path")
                    .build()
            );
        }

        // Build the PipeDoc with derived doc_id and provenance
        PipeDoc.Builder pipeDocBuilder = originalPipeDoc.toBuilder()
            .setDocId(derivation.docId)
            .setDocIdDerivation(derivation.toProto());

        PipeDoc pipeDoc = pipeDocBuilder.build();

        long startTime = System.nanoTime();
        LOG.debugf("uploadPipeDoc START: datasource=%s, doc_id=%s (derived via %s)",
            request.getDatasourceId(), pipeDoc.getDocId(), derivation.method);

        return configResolutionService.resolveConfig(request.getDatasourceId(), request.getApiKey())
            .invoke(resolved -> {
                long configTime = System.nanoTime() - startTime;
                LOG.debugf("uploadPipeDoc: config resolution took %.3f ms, persist=%s",
                    configTime / 1_000_000.0, resolved.shouldPersist());
            })
            .flatMap(resolved -> {
                if (resolved.shouldPersist()) {
                    // Path 2a: Persist to repository, then hand off reference to engine
                    return persistAndHandoff(pipeDoc, resolved, startTime);
                } else {
                    // Path 2b: Skip persistence, hand off inline document to engine
                    return handoffInline(pipeDoc, resolved, startTime);
                }
            })
            .onFailure().recoverWithItem(throwable -> {
                long totalTime = System.nanoTime() - startTime;
                LOG.errorf(throwable, "Failed to upload PipeDoc after %.3f ms", totalTime / 1_000_000.0);
                return UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    // Return the derived doc_id on failure for easier retries/debugging
                    .setDocId(pipeDoc.getDocId())
                    .setMessage(throwable.getMessage())
                    .build();
            });
    }

    /**
     * Path 2a: Persist document to repository, then hand off reference to engine.
     * <p>
     * Engine will resolve graph routing from datasource_id.
     */
    private Uni<UploadPipeDocResponse> persistAndHandoff(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            long startTime) {

        LOG.debugf("Persisting document to repository: doc_id=%s", pipeDoc.getDocId());

        // Create repository upload request
        ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest repoRequest =
            ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest.newBuilder()
                .setDocument(pipeDoc)
                .build();

        return grpcClientFactory.getClient("repository", MutinyNodeUploadServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.uploadFilesystemPipeDoc(repoRequest))
            .invoke(repoResponse -> {
                long repoTime = System.nanoTime() - startTime;
                LOG.debugf("uploadPipeDoc: repository persist took %.3f ms, doc_id=%s",
                    repoTime / 1_000_000.0, repoResponse.getDocumentId());
            })
            .flatMap(repoResponse -> {
                if (!repoResponse.getSuccess()) {
                    return Uni.createFrom().item(UploadPipeDocResponse.newBuilder()
                        .setSuccess(false)
                        .setDocId(pipeDoc.getDocId())
                        .setMessage("Repository persistence failed: " + repoResponse.getMessage())
                        .build());
                }

                // Build ingestion config with HTTP_STAGED mode (persisted)
                IngestionConfig ingestionConfig = resolved.withIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED);

                // Hand off document reference to engine (engine resolves graph routing)
                return engineClient.handoffReferenceToEngine(
                    repoResponse.getDocumentId(),
                    INTAKE_SOURCE_NODE_ID,
                    resolved.tier1Config().getDatasourceId(),
                    resolved.tier1Config().getAccountId(),
                    ingestionConfig
                ).map(handoffResponse -> {
                    long totalTime = System.nanoTime() - startTime;
                    LOG.debugf("uploadPipeDoc: complete in %.3f ms, accepted=%s",
                        totalTime / 1_000_000.0, handoffResponse.getAccepted());

                    return UploadPipeDocResponse.newBuilder()
                        .setSuccess(handoffResponse.getAccepted())
                        .setDocId(repoResponse.getDocumentId())
                        .setMessage(handoffResponse.getAccepted()
                            ? "Document persisted and handed off to engine"
                            : "Engine rejected: " + handoffResponse.getMessage())
                        .build();
                });
            });
    }

    /**
     * Path 2b: Skip persistence, hand off inline document directly to engine.
     * <p>
     * Engine will resolve graph routing from datasource_id.
     */
    private Uni<UploadPipeDocResponse> handoffInline(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            long startTime) {

        LOG.debugf("Skipping persistence, handing off inline: doc_id=%s", pipeDoc.getDocId());

        // Build ingestion config with GRPC_INLINE mode (not persisted)
        IngestionConfig ingestionConfig = resolved.withIngressMode(IngressMode.INGRESS_MODE_GRPC_INLINE);

        // Hand off document to engine (engine resolves graph routing)
        return engineClient.handoffToEngine(
            pipeDoc,
            resolved.tier1Config().getDatasourceId(),
            resolved.tier1Config().getAccountId(),
            ingestionConfig
        ).map(handoffResponse -> {
            long totalTime = System.nanoTime() - startTime;
            LOG.debugf("uploadPipeDoc: complete in %.3f ms (no persist), accepted=%s",
                totalTime / 1_000_000.0, handoffResponse.getAccepted());

            return UploadPipeDocResponse.newBuilder()
                .setSuccess(handoffResponse.getAccepted())
                .setDocId(pipeDoc.getDocId())
                .setMessage(handoffResponse.getAccepted()
                    ? "Document handed off to engine (inline)"
                    : "Engine rejected: " + handoffResponse.getMessage())
                .build();
        });
    }

    @Override
    public Uni<UploadBlobResponse> uploadBlob(UploadBlobRequest request) {
        // Validation: all intake requests require datasource_id and api_key.
        if (request.getDatasourceId().isBlank()) {
            return Uni.createFrom().item(
                UploadBlobResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: datasource_id is required")
                    .build()
            );
        }
        if (request.getApiKey().isBlank()) {
            return Uni.createFrom().item(
                UploadBlobResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: api_key is required")
                    .build()
            );
        }

        long startTime = System.nanoTime();
        int requestSize = request.getSerializedSize();
        LOG.debugf("uploadBlob START: size=%d bytes", requestSize);

        return configResolutionService.resolveConfig(request.getDatasourceId(), request.getApiKey())
            .invoke(resolved -> {
                long configTime = System.nanoTime() - startTime;
                LOG.debugf("uploadBlob: config resolution took %.3f ms", configTime / 1_000_000.0);
            })
            .flatMap(resolved -> {
                // Build PipeDoc from blob
                Instant now = Instant.now();
                Timestamp timestamp = Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();

                Blob blob = Blob.newBuilder()
                    .setData(request.getContent())
                    .setFilename(request.getFilename())
                    .setMimeType(request.getMimeType())
                    .build();

                SearchMetadata metadata = SearchMetadata.newBuilder()
                    .setCreationDate(timestamp)
                    .setLastModifiedDate(timestamp)
                    .setProcessedDate(timestamp)
                    .setSourcePath(request.getPath())
                    .putAllMetadata(request.getMetadataMap())
                    .putMetadata("datasource_id", request.getDatasourceId())
                    .putMetadata("account_id", resolved.tier1Config().getAccountId())
                    .build();

                // Try to derive doc_id deterministically for blob uploads
                DocIdDerivationResult derivation = deriveDocId(
                    request.getDatasourceId(),
                    null, // No client-provided doc_id for blobs
                    request.getSourceDocId(),
                    metadata
                );

                if (derivation == null) {
                    return Uni.createFrom().item(
                        UploadBlobResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Validation error: Cannot determine doc_id deterministically for blob upload. " +
                                "Provide source_doc_id or set source_path in metadata")
                            .build()
                    );
                }

                LOG.debugf("uploadBlob: derived doc_id=%s via %s", derivation.docId, derivation.method);

                OwnershipContext ownership = OwnershipContext.newBuilder()
                    .setAccountId(resolved.tier1Config().getAccountId())
                    .setConnectorId(resolved.tier1Config().getConnectorId())
                    .setDatasourceId(request.getDatasourceId())
                    .build();

                PipeDoc pipeDoc = PipeDoc.newBuilder()
                    .setDocId(derivation.docId)
                    .setDocIdDerivation(derivation.toProto())
                    .setOwnership(ownership)
                    .setSearchMetadata(metadata)
                    .setBlobBag(BlobBag.newBuilder().setBlob(blob).build())
                    .build();

                // Blob uploads always persist (they're typically larger files)
                return persistAndHandoffBlob(pipeDoc, resolved, startTime);
            })
            .onFailure().recoverWithItem(throwable -> {
                long totalTime = System.nanoTime() - startTime;
                LOG.errorf(throwable, "Failed to upload Blob after %.3f ms", totalTime / 1_000_000.0);
                return UploadBlobResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId("") // No derived doc_id available on failure for blob uploads
                    .setMessage(throwable.getMessage())
                    .build();
            });
    }

    /**
     * Persist blob-based document and hand off to engine.
     * <p>
     * Blobs always persist (streaming requirement for large files).
     * Engine will resolve graph routing from datasource_id.
     */
    private Uni<UploadBlobResponse> persistAndHandoffBlob(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            long startTime) {

        ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest repoRequest =
            ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest.newBuilder()
                .setDocument(pipeDoc)
                .build();

        return grpcClientFactory.getClient("repository", MutinyNodeUploadServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.uploadFilesystemPipeDoc(repoRequest))
            .invoke(repoResponse -> {
                long repoTime = System.nanoTime() - startTime;
                LOG.debugf("uploadBlob: repository persist took %.3f ms", repoTime / 1_000_000.0);
            })
            .flatMap(repoResponse -> {
                if (!repoResponse.getSuccess()) {
                    return Uni.createFrom().item(UploadBlobResponse.newBuilder()
                        .setSuccess(false)
                        .setDocId(pipeDoc.getDocId())
                        .setMessage("Repository persistence failed: " + repoResponse.getMessage())
                        .build());
                }

                IngestionConfig ingestionConfig = resolved.withIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED);

                // Hand off document reference to engine (engine resolves graph routing)
                return engineClient.handoffReferenceToEngine(
                    repoResponse.getDocumentId(),
                    INTAKE_SOURCE_NODE_ID,
                    resolved.tier1Config().getDatasourceId(),
                    resolved.tier1Config().getAccountId(),
                    ingestionConfig
                ).map(handoffResponse -> {
                    long totalTime = System.nanoTime() - startTime;
                    LOG.debugf("uploadBlob: complete in %.3f ms, accepted=%s",
                        totalTime / 1_000_000.0, handoffResponse.getAccepted());

                    return UploadBlobResponse.newBuilder()
                        .setSuccess(handoffResponse.getAccepted())
                        .setDocId(repoResponse.getDocumentId())
                        .setMessage(handoffResponse.getAccepted()
                            ? "Blob persisted and handed off to engine"
                            : "Engine rejected: " + handoffResponse.getMessage())
                        .build();
                });
            });
    }

    // ============================================
    // CRAWL SESSION LIFECYCLE
    // ============================================

    @Override
    public Uni<StartCrawlSessionResponse> startCrawlSession(StartCrawlSessionRequest request) {
        // Validation: all intake requests require datasource_id and api_key.
        if (request.getDatasourceId().isBlank()) {
            return Uni.createFrom().item(StartCrawlSessionResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Validation error: datasource_id is required")
                .build());
        }
        if (request.getApiKey().isBlank()) {
            return Uni.createFrom().item(StartCrawlSessionResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Validation error: api_key is required")
                .build());
        }

        LOG.infof("Starting crawl session: datasource=%s, crawl_id=%s",
            request.getDatasourceId(), request.getCrawlId());

        // First validate the datasource
        return configResolutionService.resolveConfig(request.getDatasourceId(), request.getApiKey())
            .flatMap(resolved -> {
                // Extract metadata from CrawlMetadata proto
                ai.pipestream.connector.intake.v1.CrawlMetadata metadata = request.getMetadata();
                String metadataJson = metadata.getParametersMap().isEmpty()
                    ? "{}"
                    : new com.google.gson.Gson().toJson(metadata.getParametersMap());

                return sessionManager.createSession(
                    request.getDatasourceId(),
                    request.getCrawlId(),
                    resolved.tier1Config().getAccountId(),
                    metadata.getConnectorType(),
                    metadata.getSourceSystem(),
                    metadataJson,
                    request.getTrackDocuments(),
                    request.getDeleteOrphans()
                );
            })
            .map(sessionId -> StartCrawlSessionResponse.newBuilder()
                .setSuccess(true)
                .setSessionId((String) sessionId)
                .setCrawlId(request.getCrawlId())
                .setMessage("Session started successfully")
                .build())
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to start crawl session for datasource=%s",
                    request.getDatasourceId());
                return StartCrawlSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to start session: " + throwable.getMessage())
                    .build();
            });
    }

    @Override
    public Uni<EndCrawlSessionResponse> endCrawlSession(EndCrawlSessionRequest request) {
        LOG.infof("Ending crawl session: session_id=%s, crawl_id=%s",
            request.getSessionId(), request.getCrawlId());

        // Determine session state from the summary (if present)
        String sessionState = request.hasSummary() && request.getSummary().getDocumentsFailed() > 0
            ? "FAILED"
            : "COMPLETED";

        return sessionManager.completeSession(request.getSessionId(), sessionState)
            .map(v -> EndCrawlSessionResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Session ended successfully")
                .build())
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to end crawl session: %s", request.getSessionId());
                return EndCrawlSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to end session: " + throwable.getMessage())
                    .build();
            });
    }

    @Override
    public Uni<HeartbeatResponse> heartbeat(HeartbeatRequest request) {
        LOG.debugf("Heartbeat: session_id=%s", request.getSessionId());

        return sessionManager.updateHeartbeat(request.getSessionId())
            .map(v -> HeartbeatResponse.newBuilder()
                .setSessionValid(true)
                .build())
            .onFailure().recoverWithItem(throwable -> {
                LOG.warnf(throwable, "Heartbeat failed for session: %s", request.getSessionId());
                return HeartbeatResponse.newBuilder()
                    .setSessionValid(false)
                    .build();
            });
    }
}
