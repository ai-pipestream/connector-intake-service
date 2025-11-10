package ai.pipeline.connector.intake.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Document tracking entity for orphan detection.
 * <p>
 * Tracks which documents were processed in each crawl session.
 * Used to detect documents that existed in previous crawls but not in current crawl.
 * <p>
 * Database Table: {@code crawl_documents}
 */
@Entity
@Table(name = "crawl_documents", indexes = {
    @Index(name = "idx_document_id", columnList = "document_id")
})
public class CrawlDocument extends PanacheEntityBase {

    /**
     * Composite primary key.
     */
    @EmbeddedId
    public CrawlDocumentId id;

    /**
     * Repository document ID (UUID).
     * Assigned by repository-service.
     */
    @Column(name = "document_id", length = 100, nullable = false)
    public String documentId;

    /**
     * Timestamp when document was processed.
     */
    @Column(name = "processed_at", nullable = false)
    public OffsetDateTime processedAt;

    /**
     * Default constructor for JPA.
     */
    public CrawlDocument() {}

    /**
     * Create a new crawl document tracking entry.
     *
     * @param crawlSessionId Session ID
     * @param sourceId Source system ID
     * @param documentId Repository document ID
     */
    public CrawlDocument(String crawlSessionId, String sourceId, String documentId) {
        this.id = new CrawlDocumentId(crawlSessionId, sourceId);
        this.documentId = documentId;
        this.processedAt = OffsetDateTime.now();
    }

    /**
     * Composite primary key for CrawlDocument.
     */
    @Embeddable
    public static class CrawlDocumentId implements Serializable {
        @Column(name = "crawl_session_id", length = 100, nullable = false)
        public String crawlSessionId;

        @Column(name = "source_id", length = 500, nullable = false)
        public String sourceId;

        public CrawlDocumentId() {}

        public CrawlDocumentId(String crawlSessionId, String sourceId) {
            this.crawlSessionId = crawlSessionId;
            this.sourceId = sourceId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CrawlDocumentId that = (CrawlDocumentId) o;
            return Objects.equals(crawlSessionId, that.crawlSessionId) &&
                   Objects.equals(sourceId, that.sourceId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(crawlSessionId, sourceId);
        }
    }
}
