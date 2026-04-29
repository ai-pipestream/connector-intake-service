package ai.pipeline.connector.intake.session;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipeline.connector.intake.service.SessionManager;
import ai.pipestream.connector.intake.v1.EndCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.EndCrawlSessionResponse;
import ai.pipestream.connector.intake.v1.HeartbeatRequest;
import ai.pipestream.connector.intake.v1.HeartbeatResponse;
import ai.pipestream.connector.intake.v1.StartCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.StartCrawlSessionResponse;
import com.google.gson.Gson;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CrawlSessionRpcService {

    private static final Logger LOG = Logger.getLogger(CrawlSessionRpcService.class);

    @Inject
    ConfigResolutionService configResolutionService;

    @Inject
    SessionManager sessionManager;

    public void startCrawlSession(StartCrawlSessionRequest request,
                                  StreamObserver<StartCrawlSessionResponse> responseObserver) {
        try {
            responseObserver.onNext(handleStartCrawlSession(request));
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    public void endCrawlSession(EndCrawlSessionRequest request,
                                StreamObserver<EndCrawlSessionResponse> responseObserver) {
        try {
            responseObserver.onNext(handleEndCrawlSession(request));
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    public void heartbeat(HeartbeatRequest request,
                          StreamObserver<HeartbeatResponse> responseObserver) {
        try {
            responseObserver.onNext(handleHeartbeat(request));
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        }
    }

    StartCrawlSessionResponse handleStartCrawlSession(StartCrawlSessionRequest request) {
        if (request.getDatasourceId().isBlank()) {
            return StartCrawlSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: datasource_id is required")
                    .build();
        }
        if (request.getApiKey().isBlank()) {
            return StartCrawlSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Validation error: api_key is required")
                    .build();
        }

        LOG.infof("Starting crawl session: datasource=%s, crawl_id=%s",
                request.getDatasourceId(), request.getCrawlId());

        try {
            var resolved = configResolutionService.resolveConfig(request.getDatasourceId(), request.getApiKey());
            var metadata = request.getMetadata();
            String metadataJson = metadata.getParametersMap().isEmpty()
                    ? "{}"
                    : new Gson().toJson(metadata.getParametersMap());

            String sessionId = sessionManager.createSession(
                    request.getDatasourceId(),
                    request.getCrawlId(),
                    resolved.tier1Config().getAccountId(),
                    metadata.getConnectorType(),
                    metadata.getSourceSystem(),
                    metadataJson,
                    request.getTrackDocuments(),
                    request.getDeleteOrphans());

            return StartCrawlSessionResponse.newBuilder()
                        .setSuccess(true)
                        .setSessionId(sessionId)
                        .setCrawlId(request.getCrawlId())
                        .setMessage("Session started successfully")
                        .build();
        } catch (RuntimeException e) {
            LOG.errorf(e, "Failed to start crawl session for datasource=%s", request.getDatasourceId());
            return StartCrawlSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to start session: " + e.getMessage())
                    .build();
        }
    }

    EndCrawlSessionResponse handleEndCrawlSession(EndCrawlSessionRequest request) {
        LOG.infof("Ending crawl session: session_id=%s, crawl_id=%s",
                request.getSessionId(), request.getCrawlId());

        String sessionState = request.hasSummary() && request.getSummary().getDocumentsFailed() > 0
                ? "FAILED"
                : "COMPLETED";

        try {
            sessionManager.completeSession(request.getSessionId(), sessionState);
            return EndCrawlSessionResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Session ended successfully")
                        .build();
        } catch (RuntimeException e) {
            LOG.errorf(e, "Failed to end session: %s", request.getSessionId());
            return EndCrawlSessionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to end session: " + e.getMessage())
                    .build();
        }
    }

    HeartbeatResponse handleHeartbeat(HeartbeatRequest request) {
        LOG.debugf("Heartbeat: session_id=%s", request.getSessionId());

        try {
            sessionManager.updateHeartbeat(request.getSessionId());
            return HeartbeatResponse.newBuilder()
                        .setSessionValid(true)
                        .build();
        } catch (RuntimeException e) {
            LOG.warnf(e, "Heartbeat failed for session: %s", request.getSessionId());
            return HeartbeatResponse.newBuilder()
                    .setSessionValid(false)
                    .build();
        }
    }
}
