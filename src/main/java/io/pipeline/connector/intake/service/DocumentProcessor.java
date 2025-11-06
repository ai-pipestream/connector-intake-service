package io.pipeline.connector.intake.service;

import com.google.common.util.concurrent.RateLimiter;
import io.pipeline.connector.intake.ConnectorConfig;
import io.pipeline.connector.intake.DocumentData;
import io.pipeline.connector.intake.DocumentResponse;
import io.pipeline.connector.intake.entity.CrawlSession;
import io.pipeline.connector.intake.repository.CrawlDocumentRepository;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.annotation.PreDestroy;
import org.jboss.logging.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Service for processing individual documents.
 * <p>
 * Handles validation, rate limiting, chunk assembly, metadata enrichment, and storage.
 */
@ApplicationScoped
public class DocumentProcessor {

    private static final Logger LOG = Logger.getLogger(DocumentProcessor.class);

    @Inject
    ChunkAssembler chunkAssembler;

    @Inject
    RepositoryClient repositoryClient;

    @Inject
    CrawlDocumentRepository documentRepository;

    @Inject
    StreamingChunkProcessor streamingChunkProcessor;

    // Rate limiters per connector
    private final Map<String, RateLimiter> rateLimiters = new ConcurrentHashMap<>();

    // Worker thread pool for CPU-intensive operations like SHA256 calculation
    private final ExecutorService workerPool = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors()
    );

    /**
     * Process a document.
     * <p>
     * Validates metadata, enforces rate limits, assembles chunked content when needed, enriches metadata,
     * and persists the document via repository-service. When the session is configured to track documents,
     * a record is written to the local database linking source IDs to stored document IDs.
     * Reactive semantics:
     * <ul>
     *   <li>Returns a {@code Uni<DocumentResponse>} that completes asynchronously; persistence and remote
     *       gRPC calls are executed in a non-blocking fashion.</li>
     *   <li>Errors are recovered into a {@code success=false} response with an error message.</li>
     * </ul>
     * Side effects:
     * <ul>
     *   <li>May write to repository-service (remote) and the local DB (tracking table).</li>
     *   <li>Uses an in-memory rate limiter per connector.</li>
     * </ul>
     *
     * @param session The crawl session
     * @param config The connector configuration
     * @param document The document data
     * @return a {@code Uni} emitting the {@code DocumentResponse} indicating success and document ID if applicable
     */
    public Uni<DocumentResponse> processDocument(CrawlSession session, ConnectorConfig config, DocumentData document) {
        LOG.debugf("Processing document: sourceId=%s, filename=%s", document.getSourceId(), document.getFilename());

        // Rate limiting is now handled at the stream level, not per document

        // Validate document
        if (!validateDocument(document)) {
            return Uni.createFrom().item(
                DocumentResponse.newBuilder()
                    .setSourceId(document.getSourceId())
                    .setSuccess(false)
                    .setErrorMessage("Invalid document data")
                    .build()
            );
        }

        // Process content (raw or chunked)
        if (document.hasRawData()) {
            // Small file - process directly
            return processRawDocument(session, config, document);
        } else if (document.hasChunk()) {
            // Large file - use streaming chunk processing
            return processStreamingChunk(session, config, document);
        } else {
            return Uni.createFrom().item(
                DocumentResponse.newBuilder()
                    .setSourceId(document.getSourceId())
                    .setSuccess(false)
                    .setErrorMessage("No content provided")
                    .build()
            );
        }
    }

    /**
     * Process raw document (small files loaded entirely into memory).
     */
    private Uni<DocumentResponse> processRawDocument(CrawlSession session, ConnectorConfig config, DocumentData document) {
        byte[] content = document.getRawData().toByteArray();
        
        // Calculate SHA256 for integrity validation asynchronously
        return calculateSHA256Async(content)
            .onItem().invoke(sha256 -> 
                LOG.infof("📊 Document SHA256: %s (filename: %s, size: %d bytes)", sha256, document.getFilename(), content.length)
            )
            .flatMap(sha256 -> {
                // Enrich metadata
                Map<String, String> enrichedMetadata = enrichMetadata(document, session, config);
                enrichedMetadata.put("sha256", sha256);

                // Store document
                return repositoryClient.storeDocument(
                    config.getS3Bucket(),
                    session.connectorId,
                    document.getFilename(),
                    document.getPath(),
                    document.getMimeType(),
                    content,
                    enrichedMetadata
                )
                .flatMap(documentId -> {
                    // Track document if enabled
                    if (session.trackDocuments) {
                        return Uni.createFrom().<Void>item(() -> {
                            documentRepository.trackDocument(session.id, document.getSourceId(), documentId);
                            return null;
                        })
                        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                        .replaceWith(documentId);
                    }
                    return Uni.createFrom().item(documentId);
                })
                .map(documentId -> {
                    LOG.debugf("Document processed successfully: sourceId=%s, documentId=%s", document.getSourceId(), documentId);
                    return DocumentResponse.newBuilder()
                        .setSourceId(document.getSourceId())
                        .setDocumentId(documentId)
                        .setSuccess(true)
                        .build();
                })
                .onFailure().recoverWithItem(throwable -> {
                    LOG.errorf(throwable, "Failed to process document: sourceId=%s", document.getSourceId());
                    return DocumentResponse.newBuilder()
                        .setSourceId(document.getSourceId())
                        .setSuccess(false)
                        .setErrorMessage(throwable.getMessage())
                        .build();
                });
            });
    }

    /**
     * Process streaming chunk (large files streamed without loading into memory).
     */
    private Uni<DocumentResponse> processStreamingChunk(CrawlSession session, ConnectorConfig config, DocumentData document) {
        // Build metadata
        Map<String, String> enrichedMetadata = enrichMetadata(document, session, config);
        
        // Process chunk using streaming processor
        return streamingChunkProcessor.processChunk(
            document.getChunk(),
            config.getS3Bucket(),
            session.connectorId,
            document.getFilename(),
            document.getPath(),
            enrichedMetadata
        )
        .map(result -> {
            if (result.isComplete) {
                // Chunk processing complete - track document if enabled
                if (session.trackDocuments) {
                    // Track document asynchronously
                    Uni.createFrom().<Void>item(() -> {
                        documentRepository.trackDocument(session.id, document.getSourceId(), result.uploadResult.nodeId);
                        return null;
                    })
                    .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                    .subscribe().with(
                        success -> LOG.debugf("Document tracked: %s", result.uploadResult.nodeId),
                        failure -> LOG.warnf("Failed to track document: %s", failure.getMessage())
                    );
                }
                
                LOG.infof("📊 Streaming document SHA256: %s (filename: %s, size: %d bytes)", 
                    result.contentHash, document.getFilename(), result.uploadResult.size);
                return DocumentResponse.newBuilder()
                    .setSourceId(document.getSourceId())
                    .setDocumentId(result.uploadResult.nodeId)
                    .setS3Key(result.uploadResult.s3Key)
                    .setSuccess(true)
                    .build();
            } else {
                // Chunk processing in progress
                return DocumentResponse.newBuilder()
                    .setSourceId(document.getSourceId())
                    .setSuccess(true)
                    .setDocumentId("") // No document ID yet
                    .build();
            }
        })
        .onFailure().recoverWithItem(throwable -> {
            LOG.errorf(throwable, "Failed to process streaming chunk: sourceId=%s", document.getSourceId());
            return DocumentResponse.newBuilder()
                .setSourceId(document.getSourceId())
                .setSuccess(false)
                .setErrorMessage(throwable.getMessage())
                .build();
        });
    }

    /**
     * Validate document data.
     */
    private boolean validateDocument(DocumentData document) {
        if (document.getSourceId() == null || document.getSourceId().isEmpty()) {
            LOG.warn("Document missing sourceId");
            return false;
        }
        if (document.getFilename() == null || document.getFilename().isEmpty()) {
            LOG.warn("Document missing filename");
            return false;
        }
        if (document.getMimeType() == null || document.getMimeType().isEmpty()) {
            LOG.warn("Document missing mimeType");
            return false;
        }
        return true;
    }

    /**
     * Enrich document metadata with connector and account context.
     */
    private Map<String, String> enrichMetadata(DocumentData document, CrawlSession session, ConnectorConfig config) {
        Map<String, String> metadata = new HashMap<>();

        // Add default metadata from connector config
        metadata.putAll(config.getDefaultMetadataMap());

        // Add connector context
        metadata.put("connector_id", session.connectorId);
        metadata.put("account_id", config.getAccountId());
        metadata.put("crawl_session_id", session.id);
        metadata.put("crawl_id", session.crawlId);

        // Add source metadata
        if (document.getSourceAuthor() != null && !document.getSourceAuthor().isEmpty()) {
            metadata.put("source_author", document.getSourceAuthor());
        }
        if (document.getSourceVersion() != null && !document.getSourceVersion().isEmpty()) {
            metadata.put("source_version", document.getSourceVersion());
        }
        if (!document.getSourceTagsList().isEmpty()) {
            metadata.put("source_tags", String.join(",", document.getSourceTagsList()));
        }

        // Add source timestamps
        if (document.hasSourceCreated()) {
            metadata.put("source_created", String.valueOf(document.getSourceCreated().getSeconds()));
        }
        if (document.hasSourceModified()) {
            metadata.put("source_modified", String.valueOf(document.getSourceModified().getSeconds()));
        }

        // Add ingestion timestamp
        metadata.put("ingestion_time", String.valueOf(System.currentTimeMillis() / 1000));

        // Add source metadata map
        metadata.putAll(document.getSourceMetadataMap());

        return metadata;
    }

    /**
     * Get or create rate limiter for a connector.
     */
    private RateLimiter getRateLimiter(String connectorId, long rateLimitPerMinute) {
        return rateLimiters.computeIfAbsent(connectorId, k -> {
            double permitsPerSecond = rateLimitPerMinute / 60.0;
            LOG.infof("Creating rate limiter for connector %s: %.2f permits/sec", connectorId, permitsPerSecond);
            return RateLimiter.create(permitsPerSecond);
        });
    }

    /**
     * Calculate SHA256 hash of the content for integrity validation asynchronously.
     * This method runs the CPU-intensive hash calculation on a worker thread pool
     * to avoid blocking the event loop.
     */
    private Uni<String> calculateSHA256Async(byte[] content) {
        return Uni.createFrom().completionStage(
            CompletableFuture.supplyAsync(() -> calculateSHA256(content), workerPool)
        );
    }

    /**
     * Calculate SHA256 hash of the content for integrity validation.
     */
    private String calculateSHA256(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
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
        } catch (NoSuchAlgorithmException e) {
            LOG.error("SHA-256 algorithm not available", e);
            return "unknown";
        }
    }

    /**
     * Cleanup method to properly shut down the worker thread pool.
     */
    @PreDestroy
    public void cleanup() {
        LOG.info("Shutting down DocumentProcessor worker thread pool");
        workerPool.shutdown();
    }
}
