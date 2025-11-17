package ai.pipeline.connector.intake.service;

import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.value.ReactiveValueCommands;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for storing and retrieving async chunks in Redis.
 * <p>
 * Chunks are stored with keys: {upload_id}:chunk:{chunk_number}
 * Metadata is stored with key: {upload_id}:metadata
 */
@ApplicationScoped
public class AsyncChunkStorageService {

    private static final Logger LOG = Logger.getLogger(AsyncChunkStorageService.class);

    @Inject
    RedisDataSource redisDataSource;

    private ReactiveValueCommands<String, byte[]> valueCommands;

    // In-memory tracking of upload metadata (could be moved to Redis if needed)
    private final Map<String, UploadMetadata> uploadMetadata = new ConcurrentHashMap<>();

    @jakarta.annotation.PostConstruct
    void init() {
        valueCommands = redisDataSource.value(byte[].class);
    }

    /**
     * Store a chunk in Redis.
     */
    public Uni<Void> storeChunk(String uploadId, int chunkNumber, byte[] chunkData) {
        String key = buildChunkKey(uploadId, chunkNumber);
        LOG.debugf("Storing chunk: key=%s, size=%d", key, chunkData.length);
        
        return valueCommands.set(key, chunkData)
            .onItem().invoke(ignored -> {
                // Update metadata
                uploadMetadata.computeIfAbsent(uploadId, k -> new UploadMetadata(uploadId))
                    .chunksReceived.add(chunkNumber);
            })
            .replaceWithVoid();
    }

    /**
     * Retrieve a chunk from Redis.
     */
    public Uni<byte[]> getChunk(String uploadId, int chunkNumber) {
        String key = buildChunkKey(uploadId, chunkNumber);
        LOG.debugf("Retrieving chunk: key=%s", key);
        
        return valueCommands.get(key)
            .onItem().ifNull().failWith(() -> {
                LOG.warnf("Chunk not found: key=%s", key);
                return new RuntimeException("Chunk not found: " + key);
            });
    }

    /**
     * Check which chunks are present for an upload.
     */
    public Uni<Set<Integer>> getReceivedChunks(String uploadId) {
        return Uni.createFrom().item(() -> {
            UploadMetadata metadata = uploadMetadata.get(uploadId);
            if (metadata == null) {
                return new HashSet<>();
            }
            return new HashSet<>(metadata.chunksReceived);
        });
    }

    /**
     * Store upload metadata.
     */
    public void storeUploadMetadata(String uploadId, UploadMetadata metadata) {
        uploadMetadata.put(uploadId, metadata);
    }

    /**
     * Get upload metadata.
     */
    public UploadMetadata getUploadMetadata(String uploadId) {
        return uploadMetadata.get(uploadId);
    }

    /**
     * Delete all chunks for an upload (cleanup).
     */
    public Uni<Void> deleteUpload(String uploadId, int maxChunkNumber) {
        LOG.infof("Deleting upload chunks: uploadId=%s, maxChunk=%d", uploadId, maxChunkNumber);
        
        // Delete chunks in parallel
        List<Uni<Void>> deleteUnis = new ArrayList<>();
        for (int i = 0; i <= maxChunkNumber; i++) {
            String key = buildChunkKey(uploadId, i);
            deleteUnis.add(valueCommands.del(key).replaceWithVoid());
        }
        
        return Uni.combine().all().unis(deleteUnis).combinedWith(ignored -> null)
            .onItem().invoke(ignored -> {
                // Remove metadata
                uploadMetadata.remove(uploadId);
            })
            .replaceWithVoid();
    }

    private String buildChunkKey(String uploadId, int chunkNumber) {
        return uploadId + ":chunk:" + chunkNumber;
    }

    /**
     * Internal class to track upload metadata.
     */
    public static class UploadMetadata {
        final String uploadId;
        final Set<Integer> chunksReceived = ConcurrentHashMap.newKeySet();
        final Map<Integer, Long> chunkTimestamps = new ConcurrentHashMap<>();
        final Map<Integer, String> chunkErrors = new ConcurrentHashMap<>();
        long createdAt;
        int expectedChunkCount = 0;
        long expectedSizeBytes = 0;
        
        // Session and config info for reassembly
        String sessionId;
        String connectorId;
        String apiKey;  // Store API key for reassembly
        String accountId;
        String filename;
        String path;
        String mimeType;
        Map<String, String> sourceMetadata = new HashMap<>();

        public UploadMetadata(String uploadId) {
            this.uploadId = uploadId;
            this.createdAt = System.currentTimeMillis();
        }
    }
}

