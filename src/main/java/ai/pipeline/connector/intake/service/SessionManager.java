package ai.pipeline.connector.intake.service;

import ai.pipeline.connector.intake.entity.CrawlSession;
import ai.pipeline.connector.intake.repository.CrawlSessionRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.jboss.logging.Logger;

import java.util.UUID;

/**
 * Service for managing crawl session lifecycle.
 */
@ApplicationScoped
public class SessionManager {

    private static final Logger LOG = Logger.getLogger(SessionManager.class);

    public SessionManager() { }

    @Inject
    CrawlSessionRepository sessionRepository;

    @Inject
    SessionFactory sessionFactory;

    public String createSession(String datasourceId, String crawlId, String accountId,
                                String connectorType, String sourceSystem, String metadata,
                                boolean trackDocuments, boolean deleteOrphans) {
        LOG.infof("Creating crawl session: datasource=%s, crawl=%s", datasourceId, crawlId);

        var existing = sessionRepository.findByDatasourceAndCrawl(datasourceId, crawlId);
        if (existing.isPresent()) {
            LOG.warnf("Session already exists for datasource=%s, crawl=%s", datasourceId, crawlId);
            return existing.get().id;
        }

        String sessionId = UUID.randomUUID().toString();
        CrawlSession session = new CrawlSession(sessionId, datasourceId, crawlId, accountId, "RUNNING");
        session.connectorType = connectorType;
        session.sourceSystem = sourceSystem;
        session.metadata = metadata;
        session.trackDocuments = trackDocuments;
        session.deleteOrphans = deleteOrphans;

        Session hibernateSession = sessionFactory.openSession();
        try {
            hibernateSession.beginTransaction();
            hibernateSession.persist(session);
            hibernateSession.getTransaction().commit();
        } catch (RuntimeException e) {
            if (hibernateSession.getTransaction().isActive()) {
                hibernateSession.getTransaction().rollback();
            }
            throw e;
        } finally {
            hibernateSession.close();
        }

        LOG.infof("Created crawl session: id=%s, datasource=%s, crawl=%s", sessionId, datasourceId, crawlId);
        return sessionId;
    }

    public CrawlSession getSession(String sessionId) {
        return sessionRepository.findById(sessionId)
                .orElseThrow(() -> new RuntimeException("Session not found: " + sessionId));
    }

    public void updateHeartbeat(String sessionId) {
        sessionRepository.updateHeartbeat(sessionId);
    }

    public void completeSession(String sessionId, String state) {
        LOG.infof("Completing session: id=%s, state=%s", sessionId, state);
        sessionRepository.completeSession(sessionId, state);
    }

    public void incrementDocumentsFound(String sessionId) {
        sessionRepository.incrementDocumentsFound(sessionId);
    }

    public void incrementDocumentsProcessed(String sessionId) {
        sessionRepository.incrementDocumentsProcessed(sessionId);
    }

    public void incrementDocumentsFailed(String sessionId) {
        sessionRepository.incrementDocumentsFailed(sessionId);
    }

    public void incrementDocumentsSkipped(String sessionId) {
        sessionRepository.incrementDocumentsSkipped(sessionId);
    }

    public void addBytesProcessed(String sessionId, long bytes) {
        sessionRepository.addBytesProcessed(sessionId, bytes);
    }
}
