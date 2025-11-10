package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.StreamingChunk;
import ai.pipestream.dynamic.grpc.client.DynamicGrpcClientFactory;
import ai.pipestream.repository.filesystem.upload.*;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Processor for handling streaming chunks from large file uploads.
 * <p>
 * This processor forwards chunks to repo-service's NodeUploadService which handles
 * streaming to S3. SHA256 is calculated incrementally for verification.
 */
@ApplicationScoped
public class StreamingChunkProcessor {

    private static final Logger LOG = Logger.getLogger(StreamingChunkProcessor.class);

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    // Track streaming sessions
    private final Map<String, StreamingSession> streamingSessions = new ConcurrentHashMap<>();

    // Cleanup timeout: 30 minutes
    private static final long CLEANUP_TIMEOUT = 30 * 60 * 1000;

    /**
     * Process a streaming chunk for a large file using header/raw/footer protocol.
     * <p>
     * This method handles three types of chunks:
     * 1. Header chunk: Blob with storage_ref and metadata
     * 2. Raw data chunks: Pure file content streamed to S3
     * 3. Footer chunk: Final metadata and completion status
     * 
     * @param chunk The streaming chunk data
     * @param bucketName S3 bucket name (used as drive name)
     * @param connectorId Connector identifier
     * @param fileName Original file name
     * @param path File path
     * @param metadata File metadata
     * @return ProcessingResult indicating completion status and upload metadata
     */
    public Uni<ProcessingResult> processChunk(StreamingChunk chunk, String bucketName, 
                                            String connectorId, String fileName, String path,
                                            Map<String, String> metadata) {
        
        String documentRef = chunk.getDocumentRef();
        boolean isLast = chunk.getIsLast();
        
        LOG.debugf("Processing streaming chunk: ref=%s, chunk=%d, type=%s, isLast=%s", 
            documentRef, chunk.getChunkNumber(), 
            chunk.getChunkTypeCase().name(), isLast);
        
        // Get or create streaming session
        StreamingSession session = streamingSessions.computeIfAbsent(documentRef, 
            key -> new StreamingSession(documentRef, bucketName, connectorId, fileName, path, 
                                      0, metadata)); // Size unknown initially
        
        // Validate chunk order
        if (chunk.getChunkNumber() != session.chunksReceived) {
            LOG.errorf("Chunk order mismatch: expected=%d, got=%d", session.chunksReceived, chunk.getChunkNumber());
            return Uni.createFrom().failure(new IllegalArgumentException("Chunk order mismatch"));
        }
        
        // Increment chunks received AFTER validation
        session.chunksReceived++;
        
        // Handle different chunk types
        switch (chunk.getChunkTypeCase()) {
            case HEADER:
                return handleHeaderChunk(session, chunk);
            case RAW_DATA:
                return handleRawDataChunk(session, chunk);
            case FOOTER:
                return handleFooterChunk(session, chunk);
            default:
                return Uni.createFrom().failure(new IllegalArgumentException("Unknown chunk type"));
        }
    }
    
    /**
     * Handle header chunk - contains Blob with storage_ref and metadata.
     */
    private Uni<ProcessingResult> handleHeaderChunk(StreamingSession session, StreamingChunk chunk) {
        LOG.infof("Processing header chunk: ref=%s", session.documentRef);
        
        // Extract blob information
        var blob = chunk.getHeader();
        session.blobId = blob.getBlobId();
        session.driveId = blob.getDriveId();
        session.mimeType = blob.getMimeType();
        session.filename = blob.getFilename();
        session.totalSize = blob.getSizeBytes();
        
        // Extract S3 key from storage_ref
        var storageRef = blob.getStorageRef();
        session.s3Key = storageRef.getObjectKey();
        
        LOG.infof("Header chunk processed: blobId=%s, s3Key=%s, size=%d", 
            session.blobId, session.s3Key, session.totalSize);
        
        // Initiate upload and create stream
        return initiateUploadAndCreateStream(session)
            .flatMap(ignored -> Uni.createFrom().item(new ProcessingResult(false, null, null)));
    }
    
    /**
     * Handle raw data chunk - pure file content streamed to S3.
     */
    private Uni<ProcessingResult> handleRawDataChunk(StreamingSession session, StreamingChunk chunk) {
        byte[] data = chunk.getRawData().toByteArray();
        
        LOG.debugf("Processing raw data chunk: ref=%s, size=%d", session.documentRef, data.length);
        
        // Update hash
        session.updateHash(data);
        
        // Push to stream
        return pushRawDataToStream(session, data);
    }
    
    /**
     * Handle footer chunk - final metadata and completion.
     */
    private Uni<ProcessingResult> handleFooterChunk(StreamingSession session, StreamingChunk chunk) {
        var footer = chunk.getFooter();

        LOG.infof("Processing footer chunk: ref=%s, finalSize=%d, checksum=%s",
            session.documentRef, footer.getFinalSize(), footer.getChecksum());

        // Send final chunk via unary call to mark upload complete
        UploadChunkRequest finalChunkRequest = UploadChunkRequest.newBuilder()
            .setNodeId(session.nodeId)
            .setUploadId(session.uploadId)
            .setData(com.google.protobuf.ByteString.EMPTY)
            .setChunkNumber(session.chunksReceived - 1)
            .setIsLast(true)
            .build();

        LOG.infof("📤 Sending final chunk (unary call): nodeId=%s", session.nodeId);

        // Send final chunk and wait for response (this one we wait for!)
        return grpcClientFactory.getNodeUploadServiceClient("repository-service")
            .flatMap(stub -> stub.uploadChunk(finalChunkRequest))
            .map(response -> {
                LOG.infof("✅ Final chunk uploaded: nodeId=%s, state=%s", session.nodeId, response.getState());
                session.uploadComplete = true;

                // Complete the streaming session
                streamingSessions.remove(session.documentRef);
                LOG.infof("Streaming upload complete: ref=%s, nodeId=%s, finalSize=%d, hash=%s",
                    session.documentRef, session.nodeId, footer.getFinalSize(), session.getContentHash());

                // Return final result with UploadResult
                UploadResult uploadResult = new UploadResult(
                    session.nodeId,
                    session.s3Key,
                    footer.getFinalSize(),
                    session.getContentHash(),
                    session.bucketName
                );
                return new ProcessingResult(true, uploadResult, session.getContentHash());
            })
            .onFailure().recoverWithItem(error -> {
                LOG.errorf(error, "❌ Final chunk upload failed: nodeId=%s", session.nodeId);
                session.uploadError = error;
                return new ProcessingResult(false, null, null);
            });
    }
    
    /**
     * Initiate upload and create a single stream to repo-service.
     * This stream will be reused for all chunks.
     */
    private Uni<Void> initiateUploadAndCreateStream(StreamingSession session) {
        return grpcClientFactory.getNodeUploadServiceClient("repository-service")
            .flatMap(stub -> {
                // First, initiate the upload
                InitiateUploadRequest request = InitiateUploadRequest.newBuilder()
                    .setDrive(session.bucketName)
                    .setParentId("0")  // Root folder
                    .setName(session.filename != null ? session.filename : session.fileName)
                    .setPath(session.path)
                    .setConnectorId(session.connectorId)
                    .setExpectedSize(session.totalSize) // May be 0 for unknown size streams
                    .setMimeType(session.mimeType != null ? session.mimeType : "application/octet-stream")
                    .putAllMetadata(session.metadata)
                    .build();
                
                return stub.initiateUpload(request);
            })
            .invoke(response -> {
                session.nodeId = response.getNodeId();
                session.uploadId = response.getUploadId();
                LOG.infof("Initiated upload: nodeId=%s, uploadId=%s, s3Key=%s",
                    session.nodeId, session.uploadId, session.s3Key);
            })
            .replaceWithVoid();
    }
    
    /**
     * Push raw data chunk via unary call - FIRE AND FORGET (parallel!).
     */
    private Uni<ProcessingResult> pushRawDataToStream(StreamingSession session, byte[] data) {
        // Create chunk request for raw data
        UploadChunkRequest chunkRequest = UploadChunkRequest.newBuilder()
            .setNodeId(session.nodeId)
            .setUploadId(session.uploadId)
            .setData(com.google.protobuf.ByteString.copyFrom(data))
            .setChunkNumber(session.chunksReceived - 1)
            .setIsLast(false)
            .build();

        LOG.debugf("📤 Uploading chunk %d (unary call): nodeId=%s, size=%d",
            (Object)(session.chunksReceived - 1), session.nodeId, data.length);

        // Fire-and-forget: send chunk via unary call, don't wait for response
        grpcClientFactory.getNodeUploadServiceClient("repository-service")
            .flatMap(stub -> stub.uploadChunk(chunkRequest))
            .subscribe().with(
                response -> LOG.debugf("✅ Chunk %d uploaded: nodeId=%s", response.getChunkNumber(), session.nodeId),
                error -> LOG.errorf(error, "❌ Chunk upload failed: nodeId=%s", session.nodeId)
            );

        // Return immediately without waiting
        return Uni.createFrom().item(new ProcessingResult(false, null, null));
    }
    
    /**
     * Wait for upload completion and return final result.
     */
    private Uni<ProcessingResult> waitForUploadCompletion(StreamingSession session, 
                                                          ai.pipestream.connector.intake.BlobMetadata footer) {
        return Uni.createFrom().emitter(emitter -> {
            // Poll until upload is complete
            new Thread(() -> {
                while (!session.uploadComplete && session.uploadError == null) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                if (session.uploadError != null) {
                    emitter.fail(session.uploadError);
                } else {
                    // Complete the streaming session
                    streamingSessions.remove(session.documentRef);
                    LOG.infof("Streaming upload complete: ref=%s, nodeId=%s, finalSize=%d, hash=%s", 
                        session.documentRef, session.nodeId, footer.getFinalSize(), session.getContentHash());
                    
                    UploadResult uploadResult = new UploadResult(
                        session.nodeId, 
                        session.s3Key, 
                        footer.getFinalSize(), 
                        session.getContentHash(), 
                        session.bucketName
                    );
                    emitter.complete(new ProcessingResult(true, uploadResult, session.getContentHash()));
                }
            }).start();
        });
    }


    /**
     * Clean up stale streaming sessions.
     */
    public void cleanupStaleSessions() {
        long now = System.currentTimeMillis();
        streamingSessions.entrySet().removeIf(entry -> {
            String key = entry.getKey();
            StreamingSession session = entry.getValue();
            if (now - session.lastUpdate > CLEANUP_TIMEOUT) {
                LOG.warnf("Cleaned up stale streaming session: ref=%s", key);
                return true;
            }
            return false;
        });
    }

    /**
     * Result of processing a chunk.
     */
    public static class ProcessingResult {
        public final boolean isComplete;
        public final UploadResult uploadResult;
        public final String contentHash;
        public final String documentRef;

        public ProcessingResult(boolean isComplete, UploadResult uploadResult, String contentHash) {
            this.isComplete = isComplete;
            this.uploadResult = uploadResult;
            this.contentHash = contentHash;
            this.documentRef = null;
        }
        
        public ProcessingResult(boolean isComplete, UploadResult uploadResult, String contentHash, String documentRef) {
            this.isComplete = isComplete;
            this.uploadResult = uploadResult;
            this.contentHash = contentHash;
            this.documentRef = documentRef;
        }
    }

    /**
     * Upload result containing metadata about the uploaded file.
     */
    public static class UploadResult {
        public final String nodeId;
        public final String s3Key;
        public final long size;
        public final String contentHash;
        public final String bucketName;

        public UploadResult(String nodeId, String s3Key, long size, String contentHash, String bucketName) {
            this.nodeId = nodeId;
            this.s3Key = s3Key;
            this.size = size;
            this.contentHash = contentHash;
            this.bucketName = bucketName;
        }
    }

    /**
     * Internal state for tracking streaming sessions.
     */
    private static class StreamingSession {
        final String documentRef;
        final String bucketName;
        final String connectorId;
        final String fileName;
        final String path;
        long totalSize;  // Can be unknown initially
        final Map<String, String> metadata;
        
        String s3Key;  // Set from header chunk
        
        // Blob information from header chunk
        String blobId;
        String driveId;
        String mimeType;
        String filename;
        
        String nodeId;  // Set by InitiateUpload
        String uploadId;  // Set by InitiateUpload
        
        int chunksReceived = 0;
        long lastUpdate = System.currentTimeMillis();
        private final MessageDigest hashDigest;

        // Upload state (for unary calls)
        boolean uploadComplete = false;
        Throwable uploadError;

        public StreamingSession(String documentRef, String bucketName, String connectorId, 
                              String fileName, String path, long totalSize, Map<String, String> metadata) {
            this.documentRef = documentRef;
            this.bucketName = bucketName;
            this.connectorId = connectorId;
            this.fileName = fileName;
            this.path = path;
            this.totalSize = totalSize;
            this.metadata = metadata;
            this.s3Key = path + "/" + fileName; // Default, will be updated from header
            
            try {
                this.hashDigest = MessageDigest.getInstance("SHA-256");
            } catch (Exception e) {
                throw new RuntimeException("Failed to create SHA-256 digest", e);
            }
        }

        public void updateHash(byte[] data) {
            hashDigest.update(data);
        }

        public String getContentHash() {
            byte[] hashBytes = hashDigest.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : hashBytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }
    }
}