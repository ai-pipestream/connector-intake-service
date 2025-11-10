package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.StreamingChunk;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for assembling streaming chunks into complete documents.
 * <p>
 * Maintains in-memory state of partial chunks and assembles them when complete.
 */
@ApplicationScoped
public class ChunkAssembler {

    private static final Logger LOG = Logger.getLogger(ChunkAssembler.class);

    // In-memory storage for partial chunks
    private final Map<String, ChunkState> chunkStates = new ConcurrentHashMap<>();

    // Cleanup timeout: 30 minutes
    private static final long CLEANUP_TIMEOUT_MS = 30 * 60 * 1000;

    /**
     * Process a streaming chunk belonging to a logical document.
     * <p>
     * This method is deprecated in favor of the new header/raw/footer protocol.
     * The new protocol streams data directly to S3 without buffering.
     * 
     * @deprecated Use StreamingChunkProcessor with header/raw/footer protocol instead
     * @param chunk The chunk to process
     * @return the complete document bytes if this was the last chunk; otherwise {@code null}
     * @throws IllegalArgumentException if chunk numbers are out of order for a given {@code documentRef}
     */
    @Deprecated
    public byte[] processChunk(StreamingChunk chunk) {
        // This method is deprecated - the new protocol streams directly to S3
        LOG.warnf("ChunkAssembler.processChunk is deprecated. Use StreamingChunkProcessor instead.");
        
        // For backward compatibility, handle the old protocol
        if (chunk.getChunkTypeCase() == StreamingChunk.ChunkTypeCase.RAW_DATA) {
            String documentRef = chunk.getDocumentRef();
            int chunkNumber = chunk.getChunkNumber();
            byte[] data = chunk.getRawData().toByteArray();
            boolean isLast = chunk.getIsLast();

            LOG.debugf("Processing raw data chunk: ref=%s, number=%d, size=%d, isLast=%s",
                documentRef, chunkNumber, data.length, isLast);

            // Get or create chunk state
            ChunkState state = chunkStates.computeIfAbsent(documentRef, k -> new ChunkState(0));

            // Validate chunk number
            if (chunkNumber != state.chunks.size()) {
                LOG.errorf("Invalid chunk number: expected %d, got %d", state.chunks.size(), chunkNumber);
                throw new IllegalArgumentException("Invalid chunk sequence");
            }

            // Add chunk
            state.chunks.add(data);
            state.lastUpdate = System.currentTimeMillis();

            // Check if complete
            if (isLast) {
                LOG.debugf("Last chunk received for ref=%s, assembling document", documentRef);
                byte[] completeDocument = assembleDocument(state);
                chunkStates.remove(documentRef);
                return completeDocument;
            }
        }

        return null;
    }

    /**
     * Assemble chunks into complete document.
     */
    private byte[] assembleDocument(ChunkState state) {
        int totalSize = 0;
        for (byte[] chunk : state.chunks) {
            totalSize += chunk.length;
        }

        byte[] result = new byte[totalSize];
        int offset = 0;
        for (byte[] chunk : state.chunks) {
            System.arraycopy(chunk, 0, result, offset, chunk.length);
            offset += chunk.length;
        }

        LOG.debugf("Assembled document: totalSize=%d, chunks=%d", totalSize, state.chunks.size());
        return result;
    }

    /**
     * Clean up stale chunk buffers that have not been updated recently.
     * <p>
     * Iterates over in-memory chunk states and removes entries whose {@code lastUpdate}
     * exceeds {@code CLEANUP_TIMEOUT_MS}. Intended to be run periodically by a scheduler.
     * Concurrency: safe to invoke concurrently with {@code processChunk}; the underlying map is concurrent.
     * Side effects: drops partial data; clients must resend if chunks are purged.
     */
    public void cleanupStaleChunks() {
        long now = System.currentTimeMillis();
        List<String> toRemove = new ArrayList<>();

        for (Map.Entry<String, ChunkState> entry : chunkStates.entrySet()) {
            if (now - entry.getValue().lastUpdate > CLEANUP_TIMEOUT_MS) {
                toRemove.add(entry.getKey());
            }
        }

        for (String key : toRemove) {
            chunkStates.remove(key);
            LOG.warnf("Cleaned up stale chunk state: ref=%s", key);
        }
    }

    /**
     * Internal state for tracking chunks.
     */
    private static class ChunkState {
        final long expectedTotalSize;
        final List<byte[]> chunks = Collections.synchronizedList(new ArrayList<>());
        long lastUpdate;

        ChunkState(long expectedTotalSize) {
            this.expectedTotalSize = expectedTotalSize;
            this.lastUpdate = System.currentTimeMillis();
        }
    }
}
