package ai.pipeline.connector.intake.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.grpc.Status;
import ai.pipestream.connector.intake.*;
import ai.pipestream.dynamic.grpc.client.DynamicGrpcClientFactory;
import ai.pipestream.repository.filesystem.upload.*;
import ai.pipeline.connector.intake.entity.CrawlSession;
import ai.pipeline.connector.intake.repository.CrawlSessionRepository;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * gRPC service implementation for Connector-Intake.
 * <p>
 * Handles document streaming, crawl session management, and heartbeat monitoring.
 * <p>
 * Proto: grpc/grpc-stubs/src/main/proto/module/connectors/connector_intake_service.proto
 */
@GrpcService
public class ConnectorIntakeServiceImpl extends MutinyConnectorIntakeServiceGrpc.ConnectorIntakeServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ConnectorIntakeServiceImpl.class);

    @Inject
    ConnectorValidationService validationService;

    @Inject
    SessionManager sessionManager;

    @Inject
    DocumentProcessor documentProcessor;

    @Inject
    CrawlSessionRepository sessionRepository;

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    // Track active sessions per stream (keyed by connector_id:crawl_id)
    private final Map<String, StreamSession> activeSessions = new ConcurrentHashMap<>();
    
    // Track active streams per connector for rate limiting
    private final Map<String, AtomicInteger> activeStreamsPerConnector = new ConcurrentHashMap<>();
    private static final int MAX_STREAMS_PER_CONNECTOR = 10000; // Effectively unlimited for testing

    // Track uploadId -> nodeId mapping for async chunked uploads
    // Key: uploadId (from connector-intake), Value: nodeId (from repository-service)
    private final Map<String, String> uploadIdToNodeId = new ConcurrentHashMap<>();
    
    // Track uploadId -> repository uploadId mapping
    // Key: uploadId (from connector-intake), Value: repository uploadId (from repository-service)
    private final Map<String, String> uploadIdToRepoUploadId = new ConcurrentHashMap<>();
    
    // Track uploadId -> highest chunk number uploaded (for determining last chunk)
    // Key: uploadId (from connector-intake), Value: highest chunk number seen
    private final Map<String, Integer> uploadIdToHighestChunk = new ConcurrentHashMap<>();

    /**
     * Stream processing entry point for the Connector Intake gRPC service.
     * <p>
     * CLIENT-SIDE STREAMING: Client sends all chunks, server returns ONE final response.
     * <p>
     * Consumes a stream of {@code DocumentIntakeRequest} messages and returns a single
     * {@code DocumentIntakeResponse} after all chunks are processed. Requests can be one of:
     * <ul>
     *   <li>{@code SessionStart} – must be sent first to authenticate and initialize session.</li>
     *   <li>{@code Document} – document payload and metadata; may be raw or chunked.</li>
     * </ul>
     * Reactive semantics:
     * <ul>
     *   <li>All incoming requests are collected and processed in parallel.</li>
     *   <li>Returns ONE final response with batch results after stream completes.</li>
     *   <li>No intermediate responses - client can fire all chunks without waiting.</li>
     * </ul>
     * Side effects:
     * <ul>
     *   <li>On {@code SessionStart}: validates the connector and creates a persisted crawl session.</li>
     *   <li>On {@code Document}: writes content to the repository-service in parallel.</li>
     * </ul>
     *
     * @param requests stream of intake requests for a logical crawl session
     * @return single final response with batch processing results
     */
    @Override
    public Uni<DocumentIntakeResponse> streamDocuments(Multi<DocumentIntakeRequest> requests) {
        // Track session and results
        final String[] sessionId = {null};
        final java.util.List<DocumentResponse> results = new java.util.concurrent.CopyOnWriteArrayList<>();
        final java.util.concurrent.atomic.AtomicInteger totalDocs = new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);

        return requests
            .onItem().transformToUni(request -> {
                if (request.hasSessionStart()) {
                    // Handle session start - store session ID
                    return handleSessionStartForBatch(request.getSessionStart(), sessionId);
                } else if (request.hasDocument()) {
                    totalDocs.incrementAndGet();
                    // Handle document (parallel processing!) - collect results
                    return handleDocument(request.getDocument())
                        .invoke(response -> {
                            if (response.hasDocumentResponse()) {
                                DocumentResponse docResponse = response.getDocumentResponse();
                                results.add(docResponse);
                                if (docResponse.getSuccess()) {
                                    successCount.incrementAndGet();
                                }
                            }
                        });
                } else {
                    return Uni.createFrom().failure(
                        Status.INVALID_ARGUMENT.withDescription("Invalid request type").asRuntimeException()
                    );
                }
            })
            .merge(100)  // Process up to 100 documents in parallel (massive throughput!)
            .collect().asList()  // Collect all intermediate results
            .map(intermediateResults -> {
                // Build final batch response
                int total = totalDocs.get();
                int successful = successCount.get();
                int failed = total - successful;

                LOG.infof("Batch processing complete: total=%d, successful=%d, failed=%d",
                    total, successful, failed);

                return DocumentIntakeResponse.newBuilder()
                    .setBatchResponse(BatchDocumentResponse.newBuilder()
                        .setSessionId(sessionId[0] != null ? sessionId[0] : "unknown")
                        .setTotalDocuments(total)
                        .setSuccessful(successful)
                        .setFailed(failed)
                        .addAllResults(results)
                        .setMessage(String.format("Processed %d documents: %d succeeded, %d failed",
                            total, successful, failed))
                        .build())
                    .build();
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Error in streamDocuments");
                return DocumentIntakeResponse.newBuilder()
                    .setBatchResponse(BatchDocumentResponse.newBuilder()
                        .setSessionId(sessionId[0] != null ? sessionId[0] : "unknown")
                        .setTotalDocuments(totalDocs.get())
                        .setSuccessful(successCount.get())
                        .setFailed(totalDocs.get() - successCount.get())
                        .setMessage("Batch processing failed: " + throwable.getMessage())
                        .build())
                    .build();
            });
    }

    private Uni<DocumentIntakeResponse> handleSessionStartForBatch(SessionStart sessionStart, String[] sessionIdHolder) {
        return handleSessionStart(sessionStart)
            .invoke(response -> {
                if (response.hasSessionResponse() && response.getSessionResponse().getAuthenticated()) {
                    sessionIdHolder[0] = response.getSessionResponse().getSessionId();
                }
            });
    }

    private Uni<DocumentIntakeResponse> handleSessionStart(SessionStart sessionStart) {
        LOG.infof("Session start: connector=%s, crawl=%s", sessionStart.getConnectorId(), sessionStart.getCrawlId());

        String sessionKey = sessionStart.getConnectorId() + ":" + sessionStart.getCrawlId();

        return validationService.validateConnector(sessionStart.getConnectorId(), sessionStart.getApiKey())
            .flatMap(config -> {
                // Check stream limit for this connector
                String connectorId = sessionStart.getConnectorId();
                AtomicInteger activeStreams = activeStreamsPerConnector.computeIfAbsent(connectorId, k -> new AtomicInteger(0));
                
                if (activeStreams.get() >= MAX_STREAMS_PER_CONNECTOR) {
                    LOG.warnf("Stream limit exceeded for connector: %s (max: %d)", connectorId, MAX_STREAMS_PER_CONNECTOR);
                    return Uni.createFrom().item(() -> {
                        return DocumentIntakeResponse.newBuilder()
                            .setSessionResponse(SessionStartResponse.newBuilder()
                                .setAuthenticated(false)
                                .setMessage("Stream limit exceeded")
                                .build())
                            .build();
                    });
                }
                
                // Increment stream count
                activeStreams.incrementAndGet();
                
                // Create session
                String metadataJson = buildMetadataJson(sessionStart.getCrawlMetadata());
                return sessionManager.createSession(
                    sessionStart.getConnectorId(),
                    sessionStart.getCrawlId(),
                    config.getAccountId(),
                    sessionStart.getCrawlMetadata().getConnectorType(),
                    sessionStart.getCrawlMetadata().getSourceSystem(),
                    metadataJson,
                    false, // trackDocuments
                    false  // deleteOrphans
                ).flatMap(sessionId -> {
                    // Get the session entity
                    return sessionManager.getSession(sessionId)
                        .map(session -> {
                            // Store session state for this stream
                            activeSessions.put(sessionKey, new StreamSession(session, config));
                            
                            LOG.infof("Session created: id=%s", sessionId);
                            return DocumentIntakeResponse.newBuilder()
                                .setSessionResponse(SessionStartResponse.newBuilder()
                                    .setAuthenticated(true)
                                    .setSessionId(sessionId)
                                    .setMessage("Session started successfully")
                                    .setConfig(config)
                                    .build())
                                .build();
                        });
                });
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to start session");
                return DocumentIntakeResponse.newBuilder()
                    .setSessionResponse(SessionStartResponse.newBuilder()
                        .setAuthenticated(false)
                        .setMessage(throwable.getMessage())
                        .build())
                    .build();
            });
    }

    private Uni<DocumentIntakeResponse> handleDocument(DocumentData document) {
        LOG.debugf("Received document: sourceId=%s", document.getSourceId());
        
        // Find the active session for this document
        // Note: In a real implementation, we'd need a better way to associate documents with sessions
        // For now, we'll use the first active session (this is a limitation)
        if (activeSessions.isEmpty()) {
            return Uni.createFrom().item(() -> {
                return DocumentIntakeResponse.newBuilder()
                    .setDocumentResponse(DocumentResponse.newBuilder()
                        .setSourceId(document.getSourceId())
                        .setSuccess(false)
                        .setErrorMessage("No active session found")
                        .build())
                    .build();
            });
        }
        
        // Get the first active session (in production, we'd need proper session tracking)
        StreamSession streamSession = activeSessions.values().iterator().next();
        
        // Process the document
        return documentProcessor.processDocument(
            streamSession.session,
            streamSession.config,
            document
        )
        .map(response -> {
            return DocumentIntakeResponse.newBuilder()
                .setDocumentResponse(response)
                .build();
        })
        .onFailure().recoverWithItem(throwable -> {
            LOG.errorf(throwable, "Failed to process document: sourceId=%s", document.getSourceId());
            return DocumentIntakeResponse.newBuilder()
                .setDocumentResponse(DocumentResponse.newBuilder()
                    .setSourceId(document.getSourceId())
                    .setSuccess(false)
                    .setErrorMessage(throwable.getMessage())
                    .build())
                .build();
        });
    }

    /**
     * Start a new crawl session using the unary RPC endpoint.
     * <p>
     * Validates the connector credentials and creates a persisted crawl session with optional
     * tracking and orphan-deletion behavior as requested by the client. Metadata is serialized
     * and stored with the session for downstream processing.
     * Reactive semantics:
     * <ul>
     *   <li>Returns a {@code Uni} that completes on the default worker pool due to persistence work.</li>
     *   <li>Failure is mapped to a response with {@code success=false} and an explanatory message.</li>
     * </ul>
     * Side effects: creates or reuses a session row in the database.
     *
     * @param request request containing connector ID, API key, crawl metadata and options
     * @return a {@code Uni} emitting {@code StartCrawlSessionResponse} with the created session ID on success
     */
    @Override
    @Transactional
    public Uni<StartCrawlSessionResponse> startCrawlSession(StartCrawlSessionRequest request) {
        LOG.infof("StartCrawlSession: connector=%s, crawl=%s", request.getConnectorId(), request.getCrawlId());

        return validationService.validateConnector(request.getConnectorId(), request.getApiKey())
            .flatMap(config -> {
                String metadataJson = buildMetadataJson(request.getMetadata());
                return sessionManager.createSession(
                    request.getConnectorId(),
                    request.getCrawlId(),
                    config.getAccountId(),
                    request.getMetadata().getConnectorType(),
                    request.getMetadata().getSourceSystem(),
                    metadataJson,
                    request.getTrackDocuments(),
                    request.getDeleteOrphans()
                ).map(sessionId -> {
                    return StartCrawlSessionResponse.newBuilder()
                        .setSuccess(true)
                        .setSessionId(sessionId)
                        .setCrawlId(request.getCrawlId())
                        .setMessage("Crawl session started successfully")
                        .build();
                });
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to start crawl session");
                return StartCrawlSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(throwable.getMessage())
                    .build();
            })
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * End a crawl session and persist final statistics.
     * <p>
     * Looks up the session, applies summary statistics if provided, and transitions the
     * session to a completed state. If the session does not exist, a response with
     * {@code success=false} is returned.
     * Reactive semantics:
     * <ul>
     *   <li>Runs on the default worker pool due to database access.</li>
     *   <li>Errors are mapped to a {@code success=false} response.</li>
     * </ul>
     * Side effects: updates the session row and marks completion.
     *
     * @param request request containing the session ID and optional {@code CrawlSummary}
     * @return a {@code Uni} emitting {@code EndCrawlSessionResponse}
     */
    @Override
    @Transactional
    public Uni<EndCrawlSessionResponse> endCrawlSession(EndCrawlSessionRequest request) {
        LOG.infof("EndCrawlSession: session=%s, crawl=%s", request.getSessionId(), request.getCrawlId());

        return Uni.createFrom().item(() -> {
            Optional<CrawlSession> sessionOpt = sessionRepository.findById(request.getSessionId());
            if (sessionOpt.isEmpty()) {
                return EndCrawlSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Session not found")
                    .build();
            }

            CrawlSession session = sessionOpt.get();
            
            // Update statistics from summary
            if (request.hasSummary()) {
                CrawlSummary summary = request.getSummary();
                session.documentsFound = summary.getDocumentsFound();
                session.documentsProcessed = summary.getDocumentsProcessed();
                session.documentsFailed = summary.getDocumentsFailed();
                session.documentsSkipped = summary.getDocumentsSkipped();
                session.bytesProcessed = summary.getBytesProcessed();
            }

            // Complete session
            sessionManager.completeSession(request.getSessionId(), "COMPLETED");

            return EndCrawlSessionResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Crawl session ended successfully")
                .build();
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Heartbeat endpoint for long-running crawls.
     * <p>
     * Validates that the session exists, updates its heartbeat timestamp, and returns a control command
     * to the client. For now the command is always {@code COMMAND_CONTINUE} when the session is valid;
     * otherwise {@code COMMAND_STOP} is returned.
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: updates the session's last heartbeat time.
     *
     * @param request heartbeat request containing {@code sessionId} and {@code crawlId}
     * @return a {@code Uni} that emits the heartbeat response indicating session validity and next command
     */
    @Override
    @Transactional
    public Uni<HeartbeatResponse> heartbeat(HeartbeatRequest request) {
        LOG.debugf("Heartbeat: session=%s, crawl=%s", request.getSessionId(), request.getCrawlId());

        return Uni.createFrom().item(() -> {
            Optional<CrawlSession> sessionOpt = sessionRepository.findById(request.getSessionId());
            if (sessionOpt.isEmpty()) {
                return HeartbeatResponse.newBuilder()
                    .setSessionValid(false)
                    .setCommand(ControlCommand.COMMAND_STOP)
                    .build();
            }

            CrawlSession session = sessionOpt.get();
            
            // Update heartbeat
            sessionManager.updateHeartbeat(request.getSessionId());

            // Return continue command (for now)
            return HeartbeatResponse.newBuilder()
                .setSessionValid(true)
                .setCommand(ControlCommand.COMMAND_CONTINUE)
                .build();
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private String buildMetadataJson(CrawlMetadata metadata) {
        if (metadata == null) {
            return "{}";
        }

        Gson gson = new Gson();
        JsonObject json = new JsonObject();
        json.addProperty("connectorType", metadata.getConnectorType());
        json.addProperty("connectorVersion", metadata.getConnectorVersion());
        json.addProperty("sourceSystem", metadata.getSourceSystem());
        
        if (metadata.hasCrawlStarted()) {
            json.addProperty("crawlStarted", metadata.getCrawlStarted().getSeconds());
        }
        
        if (!metadata.getParametersMap().isEmpty()) {
            JsonObject paramsJson = new JsonObject();
            for (var entry : metadata.getParametersMap().entrySet()) {
                paramsJson.addProperty(entry.getKey(), entry.getValue());
            }
            json.add("parameters", paramsJson);
        }
        
        return gson.toJson(json);
    }

    /**
     * Method 1: Full file upload (unary RPC) - for files up to 2GB
     */
    @Override
    public Uni<DocumentResponse> uploadDocument(UploadDocumentRequest request) {
        LOG.infof("UploadDocument: connector=%s, filename=%s", request.getConnectorId(), 
            request.getDocument().getFilename());

        // Validate connector
        return validationService.validateConnector(request.getConnectorId(), request.getApiKey())
            .flatMap(config -> {
                // Get or create session
                String sessionId = request.getSessionId();
                if (sessionId == null || sessionId.isEmpty()) {
                    // Create a temporary session for this upload
                    String crawlId = "upload-" + System.currentTimeMillis();
                    return sessionManager.createSession(
                        request.getConnectorId(),
                        crawlId,
                        config.getAccountId(),
                        "direct-upload",
                        "direct-upload",
                        "{}",
                        false,
                        false
                    ).flatMap(createdSessionId -> {
                        return processDocumentInSession(createdSessionId, config, request.getDocument());
                    });
                } else {
                    // Use existing session
                    return sessionManager.getSession(sessionId)
                        .flatMap(session -> {
                            return processDocumentInSession(sessionId, config, request.getDocument());
                        });
                }
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to upload document");
                return DocumentResponse.newBuilder()
                    .setSourceId(request.getDocument().getSourceId())
                    .setSuccess(false)
                    .setErrorMessage(throwable.getMessage())
                    .build();
            });
    }

    private Uni<DocumentResponse> processDocumentInSession(String sessionId, ConnectorConfig config, DocumentData document) {
        return sessionManager.getSession(sessionId)
            .flatMap(session -> {
                return documentProcessor.processDocument(session, config, document);
            });
    }

    /**
     * Method 3: Async chunked upload - Header handshake
     */
    @Override
    public Uni<StartChunkedUploadResponse> startChunkedUpload(StartChunkedUploadRequest request) {
        LOG.infof("StartChunkedUpload: connector=%s, filename=%s", request.getConnectorId(), request.getFilename());

        // Validate connector
        return validationService.validateConnector(request.getConnectorId(), request.getApiKey())
            .flatMap(config -> {
                // Generate upload ID
                String uploadId = "upload-" + UUID.randomUUID().toString();
                
                // Get or create session
                String sessionId = request.getSessionId();
                if (sessionId == null || sessionId.isEmpty()) {
                    String crawlId = "chunked-upload-" + System.currentTimeMillis();
                    return sessionManager.createSession(
                        request.getConnectorId(),
                        crawlId,
                        config.getAccountId(),
                        "chunked-upload",
                        "chunked-upload",
                        "{}",
                        false,
                        false
                    ).flatMap(createdSessionId -> {
                        return createChunkedUploadResponse(uploadId, config, request, createdSessionId);
                    });
                } else {
                    return createChunkedUploadResponse(uploadId, config, request, sessionId);
                }
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to start chunked upload");
                return StartChunkedUploadResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(throwable.getMessage())
                    .build();
            });
    }

    private Uni<StartChunkedUploadResponse> createChunkedUploadResponse(
            String uploadId, ConnectorConfig config, StartChunkedUploadRequest request, String sessionId) {
        
        // Call repository-service.InitiateUpload()
        return grpcClientFactory.getNodeUploadServiceClient("repository-service")
            .flatMap(stub -> {
                // Build InitiateUploadRequest from connector config and request
                ai.pipestream.repository.filesystem.upload.InitiateUploadRequest repoRequest = 
                    ai.pipestream.repository.filesystem.upload.InitiateUploadRequest.newBuilder()
                    .setDrive(config.getS3Bucket())
                    .setParentId("0")  // Root folder
                    .setName(request.getFilename())
                    .setPath(request.getPath())
                    .setConnectorId(request.getConnectorId())
                    .setExpectedSize(request.getExpectedSizeBytes())
                    .setMimeType(request.getMimeType())
                    .putAllMetadata(request.getSourceMetadataMap())
                    .build();
                
                return stub.initiateUpload(repoRequest);
            })
            .map(response -> {
                // Store the mapping: uploadId (connector-intake) -> nodeId (repository-service)
                uploadIdToNodeId.put(uploadId, response.getNodeId());
                uploadIdToRepoUploadId.put(uploadId, response.getUploadId());
                
                LOG.infof("Initiated upload: connectorUploadId=%s, nodeId=%s, repoUploadId=%s", 
                    uploadId, response.getNodeId(), response.getUploadId());
                
                return StartChunkedUploadResponse.newBuilder()
                    .setSuccess(true)
                    .setUploadId(uploadId)
                    .setMessage("Chunked upload started")
                    .setMaxChunkSize(10 * 1024 * 1024) // 10MB recommended max
                    .setTimeoutSeconds(3600) // 1 hour timeout
                    .build();
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to initiate upload in repository-service: uploadId=%s", uploadId);
                return StartChunkedUploadResponse.newBuilder()
                    .setSuccess(false)
                    .setUploadId(uploadId)
                    .setMessage("Failed to initiate upload: " + throwable.getMessage())
                    .build();
            });
    }

    /**
     * Method 3: Async chunked upload - Individual chunk upload
     */
    @Override
    public Uni<AsyncChunkedUploadChunkResponse> uploadAsyncChunk(AsyncChunkedUploadChunkRequest request) {
        LOG.debugf("UploadAsyncChunk: uploadId=%s, chunkNumber=%d, size=%d", 
            request.getUploadId(), request.getChunkNumber(), request.getChunkData().size());

        // Get nodeId from mapping
        String nodeId = uploadIdToNodeId.get(request.getUploadId());
        String repoUploadId = uploadIdToRepoUploadId.get(request.getUploadId());
        
        if (nodeId == null) {
            LOG.warnf("Upload not found: uploadId=%s", request.getUploadId());
            return Uni.createFrom().item(
                AsyncChunkedUploadChunkResponse.newBuilder()
                    .setSuccess(false)
                    .setUploadId(request.getUploadId())
                    .setChunkNumber(request.getChunkNumber())
                    .setMessage("Upload not found. Call StartChunkedUpload first.")
                    .build()
            );
        }

        // Call repository-service.UploadChunk()
        return grpcClientFactory.getNodeUploadServiceClient("repository-service")
            .flatMap(stub -> {
                ai.pipestream.repository.filesystem.upload.UploadChunkRequest repoRequest = 
                    ai.pipestream.repository.filesystem.upload.UploadChunkRequest.newBuilder()
                    .setNodeId(nodeId)
                    .setUploadId(repoUploadId)
                    .setData(request.getChunkData())
                    .setChunkNumber(request.getChunkNumber())
                    .setIsLast(false)  // We don't know if this is the last chunk yet
                    .build();
                
                return stub.uploadChunk(repoRequest);
            })
            .map(response -> {
                // Track highest chunk number for this upload
                uploadIdToHighestChunk.merge(request.getUploadId(), request.getChunkNumber(), 
                    Integer::max);
                
                LOG.debugf("Chunk uploaded: uploadId=%s, chunkNumber=%d, nodeId=%s", 
                    request.getUploadId(), request.getChunkNumber(), nodeId);
                
                return AsyncChunkedUploadChunkResponse.newBuilder()
                    .setSuccess(true)
                    .setUploadId(request.getUploadId())
                    .setChunkNumber(request.getChunkNumber())
                    .setMessage("Chunk queued successfully")
                    .build();
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to upload chunk: uploadId=%s, chunkNumber=%d", 
                    request.getUploadId(), request.getChunkNumber());
                
                return AsyncChunkedUploadChunkResponse.newBuilder()
                    .setSuccess(false)
                    .setUploadId(request.getUploadId())
                    .setChunkNumber(request.getChunkNumber())
                    .setMessage("Failed to upload chunk: " + throwable.getMessage())
                    .build();
            });
    }

    /**
     * Method 3: Async chunked upload - Footer completion
     */
    @Override
    public Uni<CompleteChunkedUploadResponse> completeChunkedUpload(CompleteChunkedUploadRequest request) {
        LOG.infof("CompleteChunkedUpload: uploadId=%s, totalChunks=%d, sha256=%s", 
            request.getUploadId(), request.getTotalChunksSent(), request.getFinalSha256());

        // Get nodeId from mapping
        String nodeId = uploadIdToNodeId.get(request.getUploadId());
        String repoUploadId = uploadIdToRepoUploadId.get(request.getUploadId());
        
        if (nodeId == null) {
            LOG.warnf("Upload not found: uploadId=%s", request.getUploadId());
            return Uni.createFrom().item(
                CompleteChunkedUploadResponse.newBuilder()
                    .setSuccess(false)
                    .setUploadId(request.getUploadId())
                    .setMessage("Upload not found. Call StartChunkedUpload first.")
                    .build()
            );
        }

        // Check if we need to mark the last chunk with isLast=true
        // The last chunk number is totalChunksSent - 1 (0-based indexing)
        int lastChunkNumber = request.getTotalChunksSent() - 1;
        Integer highestChunk = uploadIdToHighestChunk.get(request.getUploadId());
        
        // If the last chunk hasn't been uploaded with isLast=true, we need to handle it
        // For now, we'll check the status. The repository-service should handle completion
        // when all chunks are received, even if isLast wasn't set on the last chunk.
        
        // Call repository-service.GetUploadStatus()
        return grpcClientFactory.getNodeUploadServiceClient("repository-service")
            .flatMap(stub -> {
                ai.pipestream.repository.filesystem.upload.GetUploadStatusRequest statusRequest = 
                    ai.pipestream.repository.filesystem.upload.GetUploadStatusRequest.newBuilder()
                    .setNodeId(nodeId)
                    .build();
                
                return stub.getUploadStatus(statusRequest);
            })
            .map(statusResponse -> {
                LOG.infof("Upload status: uploadId=%s, nodeId=%s, state=%s, bytesUploaded=%d, totalBytes=%d",
                    request.getUploadId(), nodeId, statusResponse.getState(), 
                    statusResponse.getBytesUploaded(), statusResponse.getTotalBytes());
                
                // Check if upload is complete
                boolean isComplete = statusResponse.getState() == UploadState.UPLOAD_STATE_COMPLETED;
                
                // Build response
                CompleteChunkedUploadResponse.Builder responseBuilder = CompleteChunkedUploadResponse.newBuilder()
                    .setSuccess(isComplete)
                    .setUploadId(request.getUploadId())
                    .setMessage(isComplete ? "Upload completed successfully" : 
                        "Upload in progress. State: " + statusResponse.getState());
                
                if (isComplete) {
                    // Upload is complete - we can clean up the mappings
                    uploadIdToNodeId.remove(request.getUploadId());
                    uploadIdToRepoUploadId.remove(request.getUploadId());
                    uploadIdToHighestChunk.remove(request.getUploadId());
                } else {
                    // Upload is still in progress - return status information
                    // The client can poll by calling completeChunkedUpload again
                    if (statusResponse.getState() == UploadState.UPLOAD_STATE_UPLOADING) {
                        responseBuilder.setMessage("Upload in progress. " + 
                            statusResponse.getBytesUploaded() + " / " + 
                            statusResponse.getTotalBytes() + " bytes uploaded");
                    } else if (statusResponse.getState() == UploadState.UPLOAD_STATE_FAILED) {
                        responseBuilder.setMessage("Upload failed: " + 
                            (statusResponse.getErrorMessage().isEmpty() ? "Unknown error" : statusResponse.getErrorMessage()));
                    }
                }
                
                return responseBuilder.build();
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to get upload status: uploadId=%s", request.getUploadId());
                return CompleteChunkedUploadResponse.newBuilder()
                    .setSuccess(false)
                    .setUploadId(request.getUploadId())
                    .setMessage("Failed to get upload status: " + throwable.getMessage())
                    .build();
            });
    }

    // Removed reassembleAndStoreChunks and retrieveChunksSequentially methods
    // These will be replaced with repository-service calls in Step 4

    private Uni<String> calculateSHA256Async(byte[] content) {
        return Uni.createFrom().completionStage(
            java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
                    byte[] hash = digest.digest(content);
                    StringBuilder hexString = new StringBuilder();
                    for (byte b : hash) {
                        String hex = Integer.toHexString(0xff & b);
                        if (hex.length() == 1) {
                            hexString.append('0');
                        }
                        hexString.append(hex);
                    }
                    return hexString.toString();
                } catch (java.security.NoSuchAlgorithmException e) {
                    LOG.error("SHA-256 algorithm not available", e);
                    return "unknown";
                }
            }, Infrastructure.getDefaultWorkerPool())
        );
    }

    /**
     * Internal class to track session state per stream.
     */
    private static class StreamSession {
        final CrawlSession session;
        final ConnectorConfig config;

        StreamSession(CrawlSession session, ConnectorConfig config) {
            this.session = session;
            this.config = config;
        }
    }
}
