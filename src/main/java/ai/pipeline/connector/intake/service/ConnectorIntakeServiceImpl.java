package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DocReference;
import ai.pipestream.connector.intake.v1.MutinyConnectorIntakeServiceGrpc;
import ai.pipestream.connector.intake.v1.PipeDocItem;
import ai.pipestream.connector.intake.v1.StreamContext;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

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
                // Stamp ownership from resolved credentials if missing on the PipeDoc.
                // UploadBlob sets this automatically; UploadPipeDoc callers (e.g., JDBC connector)
                // may not have it. The intake has the account_id and datasource_id from the
                // validated API key — inject them so downstream services (repo, engine) accept the doc.
                PipeDoc docWithOwnership = pipeDoc;
                if (!pipeDoc.hasOwnership()) {
                    docWithOwnership = pipeDoc.toBuilder()
                        .setOwnership(ai.pipestream.data.v1.OwnershipContext.newBuilder()
                            .setAccountId(resolved.tier1Config().getAccountId())
                            .setDatasourceId(resolved.tier1Config().getDatasourceId())
                            .build())
                        .build();
                    LOG.debugf("uploadPipeDoc: stamped ownership from resolved config (account=%s, ds=%s)",
                        resolved.tier1Config().getAccountId(), resolved.tier1Config().getDatasourceId());
                }

                String crawlId = request.getCrawlId();
                if (resolved.shouldPersist()) {
                    // Path 2a: Persist to repository, then hand off reference to engine
                    return persistAndHandoff(docWithOwnership, resolved, startTime, crawlId);
                } else {
                    // Path 2b: Skip persistence, hand off inline document to engine
                    return handoffInline(docWithOwnership, resolved, startTime, crawlId);
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
            long startTime,
            String crawlId) {

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
                String datasourceId = resolved.tier1Config().getDatasourceId();
                return engineClient.handoffReferenceToEngine(
                    repoResponse.getDocumentId(),
                    datasourceId,
                    datasourceId,
                    resolved.tier1Config().getAccountId(),
                    ingestionConfig,
                    crawlId
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
            long startTime,
            String crawlId) {

        LOG.debugf("Skipping persistence, handing off inline: doc_id=%s", pipeDoc.getDocId());

        // Build ingestion config with GRPC_INLINE mode (not persisted)
        IngestionConfig ingestionConfig = resolved.withIngressMode(IngressMode.INGRESS_MODE_GRPC_INLINE);

        // Hand off document to engine (engine resolves graph routing)
        return engineClient.handoffToEngine(
            pipeDoc,
            resolved.tier1Config().getDatasourceId(),
            resolved.tier1Config().getAccountId(),
            ingestionConfig,
            crawlId
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
                return persistAndHandoffBlob(pipeDoc, resolved, startTime, request.getCrawlId());
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
            long startTime,
            String crawlId) {

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

                String datasourceId = resolved.tier1Config().getDatasourceId();
                // Hand off document reference to engine (engine resolves graph routing)
                return engineClient.handoffReferenceToEngine(
                    repoResponse.getDocumentId(),
                    datasourceId,
                    datasourceId,
                    resolved.tier1Config().getAccountId(),
                    ingestionConfig,
                    crawlId
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

    // ================================================================
    // Bidirectional streaming intake (UploadPipeDocStream)
    // ================================================================
    //
    // High-throughput path for connectors that emit a sustained stream of
    // documents (jdbc-connector cursor reads, s3-connector folder walks,
    // etc.). The first message MUST be a {@link StreamContext} carrying
    // the datasource_id + api_key + telemetry tags. All subsequent
    // messages carry one document per {@link PipeDocItem}, and the
    // server emits one {@link UploadPipeDocStreamResponse} per request
    // (or one final terminal response if context is missing).
    //
    // Backpressure model: Mutiny's {@code transformToUniAndConcatenate}
    // processes one item at a time and only requests the next from the
    // upstream Multi when the current Uni completes. The upstream Multi
    // is gRPC-bridged onto the inbound request stream, so HTTP/2
    // flow-control naturally holds the client's onNext when we're slow.
    // No manual {@code request(1)} bookkeeping needed.
    //
    // Retry model: when the engine returns RESOURCE_EXHAUSTED (its
    // per-module queue stayed full past the offer-timeout) the response
    // sets {@code retryable=true} so the client can resend the same
    // PipeDocItem on the same stream after a short backoff. Validation
    // failures and missing-pipeline drops set {@code retryable=false} —
    // resending the same payload would just fail the same way.

    @Override
    public Multi<UploadPipeDocStreamResponse> uploadPipeDocStream(
            Multi<UploadPipeDocStreamRequest> requests) {

        // Stream-scoped state: captured once on the first message,
        // referenced by every subsequent doc.
        AtomicReference<StreamContext> contextRef = new AtomicReference<>();
        AtomicReference<ConfigResolutionService.ResolvedConfig> configRef = new AtomicReference<>();

        return requests.onItem().transformToUniAndConcatenate(req -> {
            if (req.hasContext()) {
                StreamContext ctx = req.getContext();
                LOG.infof("uploadPipeDocStream: opening stream for datasource=%s, crawl=%s, sub=%d/%d, client=%s",
                        ctx.getDatasourceId(), ctx.getCrawlId(),
                        ctx.getSubCrawlIndex(), ctx.getTotalSubCrawls(), ctx.getClientId());
                contextRef.set(ctx);
                return configResolutionService.resolveConfig(ctx.getDatasourceId(), ctx.getApiKey())
                        .map(resolved -> {
                            configRef.set(resolved);
                            return UploadPipeDocStreamResponse.newBuilder()
                                    .setSuccess(true)
                                    .setMessage("stream context accepted")
                                    .build();
                        })
                        .onFailure().recoverWithItem(throwable -> {
                            LOG.warnf(throwable, "uploadPipeDocStream: context resolution failed for datasource=%s",
                                    ctx.getDatasourceId());
                            return UploadPipeDocStreamResponse.newBuilder()
                                    .setSuccess(false)
                                    .setMessage("stream context rejected: " + throwable.getMessage())
                                    .setRetryable(false)
                                    .build();
                        });
            }
            if (req.hasItem()) {
                StreamContext ctx = contextRef.get();
                ConfigResolutionService.ResolvedConfig resolved = configRef.get();
                if (ctx == null || resolved == null) {
                    return Uni.createFrom().item(UploadPipeDocStreamResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("first message must be StreamContext before any PipeDocItem")
                            .setRetryable(false)
                            .build());
                }
                return processStreamingItem(req.getItem(), ctx, resolved);
            }
            if (req.hasDeleteRef()) {
                // Streaming delete is part of the proto but not implemented in v1.
                // Acknowledge with a non-retryable failure so the client knows
                // it has to fall back to the unary DeletePipeDoc RPC.
                return Uni.createFrom().item(UploadPipeDocStreamResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("delete via stream not yet implemented; use DeletePipeDoc unary RPC")
                        .setRetryable(false)
                        .setRef(req.getDeleteRef())
                        .build());
            }
            return Uni.createFrom().item(UploadPipeDocStreamResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("unrecognized stream payload — expected context, item, or delete_ref")
                    .setRetryable(false)
                    .build());
        });
    }

    /**
     * Processes one {@link PipeDocItem} from a streaming upload — derive
     * doc_id, stamp ownership, persist (if configured), hand off to
     * engine, build the {@link UploadPipeDocStreamResponse}. Mirrors the
     * shape of {@link #uploadPipeDoc} but emits the streaming response
     * type and propagates the retryable signal from the engine.
     */
    private Uni<UploadPipeDocStreamResponse> processStreamingItem(
            PipeDocItem item,
            StreamContext ctx,
            ConfigResolutionService.ResolvedConfig resolved) {

        PipeDoc originalPipeDoc = item.getPipeDoc();

        DocIdDerivationResult derivation = deriveDocId(
                ctx.getDatasourceId(),
                originalPipeDoc.getDocId(),
                item.getSourceDocId(),
                originalPipeDoc.getSearchMetadata());

        if (derivation == null) {
            return Uni.createFrom().item(UploadPipeDocStreamResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("cannot determine doc_id deterministically")
                    .setRetryable(false)
                    .setRef(DocReference.newBuilder()
                            .setSourceDocId(item.getSourceDocId())
                            .build())
                    .build());
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
        Uni<UploadPipeDocResponse> handoff = resolved.shouldPersist()
                ? persistAndHandoff(pipeDoc, resolved, startTime, crawlId)
                : handoffInline(pipeDoc, resolved, startTime, crawlId);

        return handoff
                .map(unaryResp -> UploadPipeDocStreamResponse.newBuilder()
                        .setSuccess(unaryResp.getSuccess())
                        .setMessage(unaryResp.getMessage())
                        .setRetryable(false) // unary success/failure both end here non-retryable
                        .setRef(DocReference.newBuilder()
                                .setSourceDocId(item.getSourceDocId())
                                .setDocId(unaryResp.getDocId())
                                .build())
                        .build())
                .onFailure().recoverWithItem(throwable -> UploadPipeDocStreamResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage(throwable.getMessage())
                        .setRetryable(isRetryable(throwable))
                        .setRef(DocReference.newBuilder()
                                .setSourceDocId(item.getSourceDocId())
                                .setDocId(pipeDoc.getDocId())
                                .build())
                        .build());
    }

    /**
     * Translates an upstream gRPC failure into the {@code retryable}
     * flag the streaming client uses to decide between resend-with-backoff
     * and surface-the-error. The retryable set covers transient
     * conditions where retrying the same call has a real chance of
     * succeeding without changing inputs:
     * <ul>
     *   <li>{@code RESOURCE_EXHAUSTED} — engine queue offer-timeout</li>
     *   <li>{@code UNAVAILABLE} — transport hiccup or engine starting</li>
     *   <li>{@code DEADLINE_EXCEEDED} — per-call budget exceeded</li>
     *   <li>{@code ABORTED} — handoff aborted by transient contention
     *       (concurrency conflict, transactional rollback). The gRPC
     *       guidance is that ABORTED callers may retry at a higher level,
     *       which fits how our engine reports queue-eviction races.</li>
     * </ul>
     * Everything else is treated as terminal.
     */
    private static boolean isRetryable(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException sre) {
            Status.Code code = sre.getStatus().getCode();
            return code == Status.Code.RESOURCE_EXHAUSTED
                    || code == Status.Code.UNAVAILABLE
                    || code == Status.Code.DEADLINE_EXCEEDED
                    || code == Status.Code.ABORTED;
        }
        return false;
    }
}
