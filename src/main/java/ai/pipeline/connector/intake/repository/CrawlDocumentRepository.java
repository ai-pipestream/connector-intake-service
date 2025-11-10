package ai.pipeline.connector.intake.repository;

import ai.pipeline.connector.intake.entity.CrawlDocument;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Repository for crawl document tracking operations.
 */
@ApplicationScoped
public class CrawlDocumentRepository implements PanacheRepository<CrawlDocument> {

    /**
     * Find a tracked document by session and source ID.
     *
     * @param sessionId crawl session identifier
     * @param sourceId source-system document identifier
     * @return an {@code Optional} containing the tracked document if present; otherwise empty
     */
    public Optional<CrawlDocument> findBySessionAndSource(String sessionId, String sourceId) {
        return Optional.ofNullable(
            find("id.crawlSessionId = ?1 AND id.sourceId = ?2", sessionId, sourceId).firstResult()
        );
    }

    /**
     * Find all tracked documents for a given session.
     *
     * @param sessionId crawl session identifier
     * @return list of {@code CrawlDocument} entries for the session (possibly empty)
     */
    public List<CrawlDocument> findBySessionId(String sessionId) {
        return find("id.crawlSessionId", sessionId).list();
    }

    /**
     * Find all tracked entries referencing the given repository document ID across sessions.
     *
     * @param documentId repository-assigned document identifier
     * @return list of tracking entries that reference the given {@code documentId} (possibly empty)
     */
    public List<CrawlDocument> findByDocumentId(String documentId) {
        return find("documentId", documentId).list();
    }

    /**
     * Track a document in a session.
     * <p>
     * Persists a mapping from the connector's {@code sourceId} to the repository {@code documentId}
     * within the given {@code sessionId}. Used later for orphan detection and reporting.
     * <p>
     * If a document with the same session and source ID already exists, this method will
     * update the existing record with the new document ID (handles re-uploads gracefully).
     *
     * @param sessionId crawl session identifier
     * @param sourceId source-system document identifier
     * @param documentId repository-assigned document identifier
     */
    @Transactional
    public void trackDocument(String sessionId, String sourceId, String documentId) {
        // Check if document already exists
        Optional<CrawlDocument> existing = findBySessionAndSource(sessionId, sourceId);
        
        if (existing.isPresent()) {
            // Update existing record with new document ID
            CrawlDocument doc = existing.get();
            doc.documentId = documentId;
            doc.processedAt = java.time.OffsetDateTime.now();
            doc.persist();
        } else {
            // Create new record
            CrawlDocument doc = new CrawlDocument(sessionId, sourceId, documentId);
            doc.persist();
        }
    }

    /**
     * Delete all tracked document entries for the given session.
     *
     * @param sessionId crawl session identifier
     */
    @Transactional
    public void deleteBySessionId(String sessionId) {
        delete("id.crawlSessionId", sessionId);
    }
}
