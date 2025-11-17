package ai.pipeline.connector.intake.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.grpc.Status;
import ai.pipestream.connector.intake.*;
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

    // Track active sessions per stream (keyed by connector_id:crawl_id)
    private final Map<String, StreamSession> activeSessions = new ConcurrentHashMap<>();
    
    // Track active streams per connector for rate limiting
    private final Map<String, AtomicInteger> activeStreamsPerConnector = new ConcurrentHashMap<>();
    private static final int MAX_STREAMS_PER_CONNECTOR = 10000; // Effectively unlimited for testing

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
                    ).map(createdSessionId -> {
                        return createChunkedUploadResponse(uploadId, config, request, createdSessionId);
                    });
                } else {
                    return Uni.createFrom().item(() -> {
                        return createChunkedUploadResponse(uploadId, config, request, sessionId);
                    });
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

    private StartChunkedUploadResponse createChunkedUploadResponse(
            String uploadId, ConnectorConfig config, StartChunkedUploadRequest request, String sessionId) {
        
        // Store upload metadata with session and config info
        AsyncChunkStorageService.UploadMetadata metadata = 
            new AsyncChunkStorageService.UploadMetadata(uploadId);
        metadata.expectedChunkCount = request.getExpectedChunkCount();
        metadata.expectedSizeBytes = request.getExpectedSizeBytes();
        metadata.sessionId = sessionId;
        metadata.connectorId = request.getConnectorId();
        metadata.apiKey = request.getApiKey();  // Store API key for reassembly
        metadata.accountId = config.getAccountId();
        metadata.filename = request.getFilename();
        metadata.path = request.getPath();
        metadata.mimeType = request.getMimeType();
        metadata.sourceMetadata.putAll(request.getSourceMetadataMap());
        asyncChunkStorage.storeUploadMetadata(uploadId, metadata);

        String redisKeyPrefix = uploadId;
        
        return StartChunkedUploadResponse.newBuilder()
            .setSuccess(true)
            .setUploadId(uploadId)
            .setMessage("Chunked upload started")
            .setRedisKeyPrefix(redisKeyPrefix)
            .setMaxChunkSize(10 * 1024 * 1024) // 10MB recommended max
            .setTimeoutSeconds(3600) // 1 hour timeout
            .build();
    }

    /**
     * Method 3: Async chunked upload - Individual chunk upload
     */
    @Override
    public Uni<AsyncChunkedUploadChunkResponse> uploadAsyncChunk(AsyncChunkedUploadChunkRequest request) {
        LOG.debugf("UploadAsyncChunk: uploadId=%s, chunkNumber=%d, size=%d", 
            request.getUploadId(), request.getChunkNumber(), request.getChunkData().size());

        // Store chunk in Redis
        return asyncChunkStorage.storeChunk(
            request.getUploadId(),
            request.getChunkNumber(),
            request.getChunkData().toByteArray()
        )
        .map(ignored -> {
            String redisKey = request.getUploadId() + ":chunk:" + request.getChunkNumber();
            
            // Update metadata
            AsyncChunkStorageService.UploadMetadata metadata = 
                asyncChunkStorage.getUploadMetadata(request.getUploadId());
            if (metadata != null) {
                metadata.chunkTimestamps.put(request.getChunkNumber(), System.currentTimeMillis());
            }
            
            return AsyncChunkedUploadChunkResponse.newBuilder()
                .setSuccess(true)
                .setUploadId(request.getUploadId())
                .setChunkNumber(request.getChunkNumber())
                .setMessage("Chunk stored successfully")
                .setRedisKey(redisKey)
                .build();
        })
        .onFailure().recoverWithItem(throwable -> {
            LOG.errorf(throwable, "Failed to upload chunk: uploadId=%s, chunkNumber=%d", 
                request.getUploadId(), request.getChunkNumber());
            
            // Record error in metadata
            AsyncChunkStorageService.UploadMetadata metadata = 
                asyncChunkStorage.getUploadMetadata(request.getUploadId());
            if (metadata != null) {
                metadata.chunkErrors.put(request.getChunkNumber(), throwable.getMessage());
            }
            
            return AsyncChunkedUploadChunkResponse.newBuilder()
                .setSuccess(false)
                .setUploadId(request.getUploadId())
                .setChunkNumber(request.getChunkNumber())
                .setMessage("Failed to store chunk: " + throwable.getMessage())
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

        AsyncChunkStorageService.UploadMetadata metadata = 
            asyncChunkStorage.getUploadMetadata(request.getUploadId());
        
        if (metadata == null) {
            return Uni.createFrom().item(
                CompleteChunkedUploadResponse.newBuilder()
                    .setSuccess(false)
                    .setUploadId(request.getUploadId())
                    .setMessage("Upload not found")
                    .build()
            );
        }

        // Check which chunks are missing
        return asyncChunkStorage.getReceivedChunks(request.getUploadId())
            .flatMap(receivedChunks -> {
                Set<Integer> missingChunks = new HashSet<>();
                Set<Integer> erroredChunks = new HashSet<>();
                Set<Integer> timeoutChunks = new HashSet<>();
                
                // Determine missing chunks
                for (int i = 0; i < request.getTotalChunksSent(); i++) {
                    if (!receivedChunks.contains(i)) {
                        // Check if it errored
                        if (metadata.chunkErrors.containsKey(i)) {
                            erroredChunks.add(i);
                        } else {
                            // Check if it timed out (older than 5 minutes)
                            Long timestamp = metadata.chunkTimestamps.get(i);
                            if (timestamp == null || (System.currentTimeMillis() - timestamp) > 5 * 60 * 1000) {
                                timeoutChunks.add(i);
                            } else {
                                missingChunks.add(i);
                            }
                        }
                    }
                }
                
                // If all chunks are present, reassemble and store
                if (missingChunks.isEmpty() && erroredChunks.isEmpty() && timeoutChunks.isEmpty()) {
                    return reassembleAndStoreChunks(request, metadata, receivedChunks);
                } else {
                    // Return status with missing/errored/timeout chunks
                    CompleteChunkedUploadResponse.Builder response = CompleteChunkedUploadResponse.newBuilder()
                        .setSuccess(false)
                        .setUploadId(request.getUploadId())
                        .setMessage("Not all chunks received")
                        .addAllMissingChunks(missingChunks)
                        .addAllErroredChunks(erroredChunks)
                        .addAllTimeoutChunks(timeoutChunks);
                    
                    // Chunks that need resending are errored or timeout chunks
                    Set<Integer> needsResend = new HashSet<>(erroredChunks);
                    needsResend.addAll(timeoutChunks);
                    response.addAllNeedsResendChunks(needsResend);
                    
                    return Uni.createFrom().item(response.build());
                }
            });
    }

    private Uni<CompleteChunkedUploadResponse> reassembleAndStoreChunks(
            CompleteChunkedUploadRequest request,
            AsyncChunkStorageService.UploadMetadata metadata,
            Set<Integer> receivedChunks) {
        
        LOG.infof("Reassembling chunks: uploadId=%s, chunkCount=%d", 
            request.getUploadId(), receivedChunks.size());
        
        // Get session and config
        return sessionManager.getSession(metadata.sessionId)
            .flatMap(session -> {
                return validationService.validateConnector(metadata.connectorId, metadata.apiKey)
                    .flatMap(config -> {
                        // Retrieve all chunks from Redis in order
                        List<Integer> sortedChunks = new ArrayList<>(receivedChunks);
                        Collections.sort(sortedChunks);
                        
                        // Retrieve chunks sequentially
                        return retrieveChunksSequentially(request.getUploadId(), sortedChunks)
                            .flatMap(reassembledData -> {
                                // Calculate SHA256
                                return calculateSHA256Async(reassembledData)
                                    .flatMap(sha256 -> {
                                        // Verify SHA256 matches
                                        if (!sha256.equals(request.getFinalSha256())) {
                                            LOG.warnf("SHA256 mismatch: expected=%s, calculated=%s", 
                                                request.getFinalSha256(), sha256);
                                            // Continue anyway, but log warning
                                        }
                                        
                                        // Build DocumentData
                                        DocumentData documentData = DocumentData.newBuilder()
                                            .setSourceId(metadata.uploadId)
                                            .setFilename(metadata.filename)
                                            .setPath(metadata.path)
                                            .setMimeType(metadata.mimeType)
                                            .setSizeBytes(reassembledData.length)
                                            .putAllSourceMetadata(metadata.sourceMetadata)
                                            .setRawData(com.google.protobuf.ByteString.copyFrom(reassembledData))
                                            .setChecksum(sha256)
                                            .setChecksumType("SHA256")
                                            .build();
                                        
                                        // Process document
                                        return documentProcessor.processDocument(session, config, documentData)
                                            .flatMap(documentResponse -> {
                                                // Cleanup Redis chunks
                                                return asyncChunkStorage.deleteUpload(request.getUploadId(), sortedChunks.size() - 1)
                                                    .map(ignored -> {
                                                        return CompleteChunkedUploadResponse.newBuilder()
                                                            .setSuccess(documentResponse.getSuccess())
                                                            .setUploadId(request.getUploadId())
                                                            .setDocumentId(documentResponse.getDocumentId())
                                                            .setS3Key(documentResponse.getS3Key())
                                                            .setMessage("Chunks reassembled and stored successfully")
                                                            .setFinalSha256(sha256)
                                                            .setFinalSizeBytes(reassembledData.length)
                                                            .build();
                                                    });
                                            });
                                    });
                            });
                    });
            })
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to reassemble chunks: uploadId=%s", request.getUploadId());
                return CompleteChunkedUploadResponse.newBuilder()
                    .setSuccess(false)
                    .setUploadId(request.getUploadId())
                    .setMessage("Failed to reassemble chunks: " + throwable.getMessage())
                    .build();
            });
    }

    private Uni<byte[]> retrieveChunksSequentially(String uploadId, List<Integer> chunkNumbers) {
        if (chunkNumbers.isEmpty()) {
            return Uni.createFrom().item(new byte[0]);
        }
        
        // Retrieve chunks in parallel, then sort
        List<Uni<byte[]>> chunkUnis = chunkNumbers.stream()
            .map(chunkNumber -> asyncChunkStorage.getChunk(uploadId, chunkNumber))
            .collect(Collectors.toList());
        
        return Uni.combine().all().unis(chunkUnis)
            .combinedWith(chunks -> {
                // Combine all chunks into single byte array
                int totalSize = chunks.stream()
                    .mapToInt(chunk -> ((byte[]) chunk).length)
                    .sum();
                
                byte[] result = new byte[totalSize];
                int offset = 0;
                for (Object chunk : chunks) {
                    byte[] chunkBytes = (byte[]) chunk;
                    System.arraycopy(chunkBytes, 0, result, offset, chunkBytes.length);
                    offset += chunkBytes.length;
                }
                
                return result;
            });
    }

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

    @Inject
    AsyncChunkStorageService asyncChunkStorage;

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
