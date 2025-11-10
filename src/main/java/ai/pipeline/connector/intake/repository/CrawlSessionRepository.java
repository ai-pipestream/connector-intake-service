package ai.pipeline.connector.intake.repository;

import ai.pipeline.connector.intake.entity.CrawlSession;
import io.quarkus.hibernate.orm.panache.PanacheRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for crawl session operations.
 */
@ApplicationScoped
public class CrawlSessionRepository implements PanacheRepository<CrawlSession> {

    /**
     * Find a session by its unique identifier.
     *
     * @param sessionId session UUID
     * @return an {@code Optional} with the {@code CrawlSession} if found; otherwise empty
     */
    public Optional<CrawlSession> findById(String sessionId) {
        return Optional.ofNullable(find("id", sessionId).firstResult());
    }

    /**
     * Find a session by connector ID and crawl ID.
     *
     * @param connectorId connector identifier
     * @param crawlId client-provided crawl identifier
     * @return an {@code Optional} with the {@code CrawlSession} if found; otherwise empty
     */
    public Optional<CrawlSession> findByConnectorAndCrawl(String connectorId, String crawlId) {
        return Optional.ofNullable(
            find("connectorId = ?1 AND crawlId = ?2", connectorId, crawlId).firstResult()
        );
    }

    /**
     * Find all sessions for a connector.
     *
     * @param connectorId connector identifier
     * @return list of sessions owned by the connector (possibly empty)
     */
    public List<CrawlSession> findByConnectorId(String connectorId) {
        return find("connectorId", connectorId).list();
    }

    /**
     * Find all sessions for an account.
     *
     * @param accountId account identifier
     * @return list of sessions for the account (possibly empty)
     */
    public List<CrawlSession> findByAccountId(String accountId) {
        return find("accountId", accountId).list();
    }

    /**
     * Find sessions by state.
     *
     * @param state session state (e.g., RUNNING, COMPLETED, FAILED)
     * @return list of sessions in the given state (possibly empty)
     */
    public List<CrawlSession> findByState(String state) {
        return find("state", state).list();
    }

    /**
     * Find running sessions whose last heartbeat is older than the cutoff time.
     *
     * @param cutoffTime sessions with {@code lastHeartbeat < cutoffTime} are considered stale
     * @return list of sessions in RUNNING state that appear stale (possibly empty)
     */
    public List<CrawlSession> findStaleSessions(OffsetDateTime cutoffTime) {
        return find("state = 'RUNNING' AND (lastHeartbeat IS NULL OR lastHeartbeat < ?1)", cutoffTime).list();
    }

    /**
     * Update the persisted state for a session.
     *
     * @param sessionId session UUID
     * @param state new state value (e.g., RUNNING, COMPLETED, FAILED)
     */
    @Transactional
    public void updateState(String sessionId, String state) {
        update("state = ?1 WHERE id = ?2", state, sessionId);
    }

    /**
     * Update the last heartbeat timestamp for a session to the current time.
     *
     * @param sessionId session UUID
     */
    @Transactional
    public void updateHeartbeat(String sessionId) {
        update("lastHeartbeat = ?1 WHERE id = ?2", OffsetDateTime.now(), sessionId);
    }

    /**
     * Increment the counter of documents discovered for the session.
     *
     * @param sessionId session UUID
     */
    @Transactional
    public void incrementDocumentsFound(String sessionId) {
        CrawlSession session = findById(sessionId).orElseThrow();
        session.documentsFound = (session.documentsFound != null ? session.documentsFound : 0) + 1;
        session.persist();
    }

    /**
     * Increment the counter of documents successfully processed for the session.
     *
     * @param sessionId session UUID
     */
    @Transactional
    public void incrementDocumentsProcessed(String sessionId) {
        CrawlSession session = findById(sessionId).orElseThrow();
        session.documentsProcessed = (session.documentsProcessed != null ? session.documentsProcessed : 0) + 1;
        session.persist();
    }

    /**
     * Increment the counter of documents that failed processing for the session.
     *
     * @param sessionId session UUID
     */
    @Transactional
    public void incrementDocumentsFailed(String sessionId) {
        CrawlSession session = findById(sessionId).orElseThrow();
        session.documentsFailed = (session.documentsFailed != null ? session.documentsFailed : 0) + 1;
        session.persist();
    }

    /**
     * Increment the counter of documents skipped (not processed) for the session.
     *
     * @param sessionId session UUID
     */
    @Transactional
    public void incrementDocumentsSkipped(String sessionId) {
        CrawlSession session = findById(sessionId).orElseThrow();
        session.documentsSkipped = (session.documentsSkipped != null ? session.documentsSkipped : 0) + 1;
        session.persist();
    }

    /**
     * Add the number of processed bytes to the session total.
     *
     * @param sessionId session UUID
     * @param bytes number of bytes to add (non-negative)
     */
    @Transactional
    public void addBytesProcessed(String sessionId, long bytes) {
        CrawlSession session = findById(sessionId).orElseThrow();
        session.bytesProcessed = (session.bytesProcessed != null ? session.bytesProcessed : 0L) + bytes;
        session.persist();
    }

    /**
     * Mark a session as completed and set its terminal state.
     *
     * @param sessionId session UUID
     * @param state terminal state to set (e.g., COMPLETED, FAILED, ABORTED)
     */
    @Transactional
    public void completeSession(String sessionId, String state) {
        CrawlSession session = findById(sessionId).orElseThrow();
        session.state = state;
        session.completedAt = OffsetDateTime.now();
        session.persist();
    }
}
