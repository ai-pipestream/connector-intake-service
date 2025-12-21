package ai.pipeline.connector.intake.service;

import ai.pipeline.connector.intake.entity.CrawlSession;
import ai.pipeline.connector.intake.repository.CrawlSessionRepository;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.jboss.logging.Logger;

import java.util.UUID;

/**
 * Service for managing crawl session lifecycle.
 * <p>
 * Handles session creation, state transitions, and statistics tracking.
 */
@ApplicationScoped
public class SessionManager {

    private static final Logger LOG = Logger.getLogger(SessionManager.class);

    /**
     * Default constructor for CDI.
     */
    public SessionManager() { }

    @Inject
    CrawlSessionRepository sessionRepository;

    @Inject
    SessionFactory sessionFactory;

    /**
     * Create a new crawl session.
     * <p>
     * If a session already exists for the same {@code datasourceId} and {@code crawlId}, the existing session ID is
     * returned. Otherwise, a new session is persisted in the database and its ID is returned.
     * Reactive semantics: executes on the default worker pool due to blocking persistence.
     * Side effects: inserts a row into the {@code crawl_session} table.
     *
     * @param datasourceId DataSource ID
     * @param crawlId Client-provided crawl ID
     * @param accountId Account ID
     * @param connectorType Connector type
     * @param sourceSystem Source system identifier
     * @param metadata Crawl metadata as JSON
     * @param trackDocuments Whether to track documents for orphan detection
     * @param deleteOrphans Whether to delete orphans at end
     * @return a {@code Uni} emitting the session ID (existing or newly created)
     */
    public Uni<String> createSession(String datasourceId, String crawlId, String accountId,
                                     String connectorType, String sourceSystem, String metadata,
                                     boolean trackDocuments, boolean deleteOrphans) {
        LOG.infof("Creating crawl session: datasource=%s, crawl=%s", datasourceId, crawlId);

        // Check if session already exists
        return Uni.createFrom().item(() -> {
            var existing = sessionRepository.findByDatasourceAndCrawl(datasourceId, crawlId);
            if (existing.isPresent()) {
                LOG.warnf("Session already exists for datasource=%s, crawl=%s", datasourceId, crawlId);
                return existing.get().id;
            }

            // Create new session
            String sessionId = UUID.randomUUID().toString();
            CrawlSession session = new CrawlSession(sessionId, datasourceId, crawlId, accountId, "RUNNING");
            session.connectorType = connectorType;
            session.sourceSystem = sourceSystem;
            session.metadata = metadata;
            session.trackDocuments = trackDocuments;
            session.deleteOrphans = deleteOrphans;

            // Use Hibernate's transaction API to manually manage transaction
            Session hibernateSession = sessionFactory.openSession();
            try {
                hibernateSession.beginTransaction();
                hibernateSession.persist(session);
                hibernateSession.getTransaction().commit();
            } catch (Exception e) {
                if (hibernateSession.getTransaction().isActive()) {
                    hibernateSession.getTransaction().rollback();
                }
                throw e;
            } finally {
                hibernateSession.close();
            }

            LOG.infof("Created crawl session: id=%s, datasource=%s, crawl=%s", sessionId, datasourceId, crawlId);
            return sessionId;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Retrieve a crawl session by its identifier.
     * <p>
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: none (read-only).
     *
     * @param sessionId unique session identifier
     * @return a {@code Uni} that emits the {@code CrawlSession} if found; fails if not found
     */
    public Uni<CrawlSession> getSession(String sessionId) {
        return Uni.createFrom().item(() -> {
            return sessionRepository.findById(sessionId)
                .orElseThrow(() -> new RuntimeException("Session not found: " + sessionId));
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    /**
     * Update a session's heartbeat timestamp to the current time.
     * <p>
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: updates the {@code lastHeartbeat} column for the given session.
     *
     * @param sessionId unique session identifier
     * @return a {@code Uni} that completes when the heartbeat has been updated
     */
    public Uni<Void> updateHeartbeat(String sessionId) {
        return Uni.createFrom().item(() -> {
            sessionRepository.updateHeartbeat(sessionId);
            return null;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .replaceWithVoid();
    }

    /**
     * Mark a session as completed and persist its final state.
     * <p>
     * Sets the session's {@code state} and {@code completedAt} timestamp. No-op if the session does not exist
     * will result in a failure from the repository layer.
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: updates the {@code crawl_session} row for the given session.
     *
     * @param sessionId unique session identifier
     * @param state terminal state to set (e.g., {@code COMPLETED}, {@code FAILED}, {@code ABORTED})
     * @return a {@code Uni} that completes when the session is marked complete
     */
    public Uni<Void> completeSession(String sessionId, String state) {
        LOG.infof("Completing session: id=%s, state=%s", sessionId, state);
        return Uni.createFrom().item(() -> {
            sessionRepository.completeSession(sessionId, state);
            return null;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .replaceWithVoid();
    }

    /**
     * Increment the count of documents discovered for a session.
     * <p>
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: increments the {@code documentsFound} counter in the session row.
     *
     * @param sessionId unique session identifier
     * @return a {@code Uni} that completes after the counter has been incremented
     */
    public Uni<Void> incrementDocumentsFound(String sessionId) {
        return Uni.createFrom().item(() -> {
            sessionRepository.incrementDocumentsFound(sessionId);
            return null;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .replaceWithVoid();
    }

    /**
     * Increment the count of documents successfully processed for a session.
     * <p>
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: increments the {@code documentsProcessed} counter in the session row.
     *
     * @param sessionId unique session identifier
     * @return a {@code Uni} that completes after the counter has been incremented
     */
    public Uni<Void> incrementDocumentsProcessed(String sessionId) {
        return Uni.createFrom().item(() -> {
            sessionRepository.incrementDocumentsProcessed(sessionId);
            return null;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .replaceWithVoid();
    }

    /**
     * Increment the count of documents that failed processing for a session.
     * <p>
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: increments the {@code documentsFailed} counter in the session row.
     *
     * @param sessionId unique session identifier
     * @return a {@code Uni} that completes after the counter has been incremented
     */
    public Uni<Void> incrementDocumentsFailed(String sessionId) {
        return Uni.createFrom().item(() -> {
            sessionRepository.incrementDocumentsFailed(sessionId);
            return null;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .replaceWithVoid();
    }

    /**
     * Increment the count of documents skipped (not processed) for a session.
     * <p>
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: increments the {@code documentsSkipped} counter in the session row.
     *
     * @param sessionId unique session identifier
     * @return a {@code Uni} that completes after the counter has been incremented
     */
    public Uni<Void> incrementDocumentsSkipped(String sessionId) {
        return Uni.createFrom().item(() -> {
            sessionRepository.incrementDocumentsSkipped(sessionId);
            return null;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .replaceWithVoid();
    }

    /**
     * Add the number of processed bytes to the session total.
     * <p>
     * Reactive semantics: executes on the default worker pool due to database access.
     * Side effects: increments the {@code bytesProcessed} counter in the session row.
     *
     * @param sessionId unique session identifier
     * @param bytes number of bytes to add (must be non-negative)
     * @return a {@code Uni} that completes after the counter has been updated
     */
    public Uni<Void> addBytesProcessed(String sessionId, long bytes) {
        return Uni.createFrom().item(() -> {
            sessionRepository.addBytesProcessed(sessionId, bytes);
            return null;
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .replaceWithVoid();
    }
}
