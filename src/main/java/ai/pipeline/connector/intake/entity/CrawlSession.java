package ai.pipeline.connector.intake.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;
import java.time.OffsetDateTime;

/**
 * Crawl session entity tracking document ingestion sessions.
 * <p>
 * Each crawl session represents a single ingestion run from a connector.
 * Sessions track statistics, state, and metadata for monitoring and orphan detection.
 * <p>
 * Database Table: {@code crawl_sessions}
 */
@Entity
@Table(name = "crawl_sessions", indexes = {
    @Index(name = "idx_account_id", columnList = "account_id"),
    @Index(name = "idx_state", columnList = "state"),
    @Index(name = "idx_started_at", columnList = "started_at")
})
public class CrawlSession extends PanacheEntityBase {

    /**
     * Unique session identifier (primary key).
     * Generated as UUID.
     */
    @Id
    @Column(name = "id", length = 100)
    public String id;

    /**
     * Connector ID that started this session.
     * References connector-service (not enforced cross-DB).
     */
    @Column(name = "connector_id", length = 100, nullable = false)
    public String connectorId;

    /**
     * Client-provided crawl ID.
     * Unique per connector.
     */
    @Column(name = "crawl_id", length = 100, nullable = false)
    public String crawlId;

    /**
     * Account ID this connector belongs to.
     * Copied from connector for efficient queries.
     */
    @Column(name = "account_id", length = 100, nullable = false)
    public String accountId;

    /**
     * Session state.
     * Values: RUNNING, COMPLETED, FAILED, TERMINATED
     */
    @Column(name = "state", length = 50, nullable = false)
    public String state;

    /**
     * Timestamp when session started.
     */
    @Column(name = "started_at", nullable = false)
    public OffsetDateTime startedAt;

    /**
     * Timestamp when session completed.
     * Null if session is still running.
     */
    @Column(name = "completed_at")
    public OffsetDateTime completedAt;

    /**
     * Timestamp of last heartbeat.
     * Used for timeout detection.
     */
    @Column(name = "last_heartbeat")
    public OffsetDateTime lastHeartbeat;

    /**
     * Number of documents found during crawl.
     */
    @Column(name = "documents_found")
    public Integer documentsFound = 0;

    /**
     * Number of documents successfully processed.
     */
    @Column(name = "documents_processed")
    public Integer documentsProcessed = 0;

    /**
     * Number of documents that failed processing.
     */
    @Column(name = "documents_failed")
    public Integer documentsFailed = 0;

    /**
     * Number of documents skipped.
     */
    @Column(name = "documents_skipped")
    public Integer documentsSkipped = 0;

    /**
     * Total bytes processed.
     */
    @Column(name = "bytes_processed")
    public Long bytesProcessed = 0L;

    /**
     * Whether to track documents for orphan detection.
     */
    @Column(name = "track_documents")
    public Boolean trackDocuments = false;

    /**
     * Whether to delete orphans at end of crawl.
     */
    @Column(name = "delete_orphans")
    public Boolean deleteOrphans = false;

    /**
     * Connector type identifier.
     */
    @Column(name = "connector_type", length = 50)
    public String connectorType;

    /**
     * Source system identifier.
     */
    @Column(name = "source_system", length = 500)
    public String sourceSystem;

    /**
     * Crawl metadata as JSON.
     */
    @Column(name = "metadata", columnDefinition = "JSON")
    public String metadata;

    /**
     * Default constructor for JPA.
     */
    public CrawlSession() {}

    /**
     * Create a new crawl session.
     *
     * @param id Session ID (UUID)
     * @param connectorId Connector ID
     * @param crawlId Client-provided crawl ID
     * @param accountId Account ID
     * @param state Initial state
     */
    public CrawlSession(String id, String connectorId, String crawlId, String accountId, String state) {
        this.id = id;
        this.connectorId = connectorId;
        this.crawlId = crawlId;
        this.accountId = accountId;
        this.state = state;
        this.startedAt = OffsetDateTime.now();
        this.lastHeartbeat = OffsetDateTime.now();
        this.documentsFound = 0;
        this.documentsProcessed = 0;
        this.documentsFailed = 0;
        this.documentsSkipped = 0;
        this.bytesProcessed = 0L;
        this.trackDocuments = false;
        this.deleteOrphans = false;
    }
}
