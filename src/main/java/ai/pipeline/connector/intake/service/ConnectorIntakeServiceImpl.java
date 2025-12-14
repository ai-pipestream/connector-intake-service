package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.MutinyConnectorIntakeServiceGrpc;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.connector.intake.v1.UploadBlobRequest;
import ai.pipestream.connector.intake.v1.UploadBlobResponse;
import ai.pipestream.connector.intake.v1.StartCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.StartCrawlSessionResponse;
import ai.pipestream.connector.intake.v1.EndCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.EndCrawlSessionResponse;
import ai.pipestream.connector.intake.v1.HeartbeatRequest;
import ai.pipestream.connector.intake.v1.HeartbeatResponse;
import ai.pipestream.connector.intake.v1.ConnectorConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.quarkus.dynamicgrpc.GrpcClientFactory;
import ai.pipestream.repository.v1.filesystem.upload.MutinyNodeUploadServiceGrpc;
// Repository UploadPipeDocResponse conflicts with connector.intake.v1.UploadPipeDocResponse - use fully qualified names
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

@Singleton
@GrpcService
/**
 * gRPC service implementation handling connector ingestion endpoints.
 * <p>
 * Responsibilities:
 * <ul>
 *   <li>Validates connector identity and API key via ConnectorValidationService.</li>
 *   <li>Streams documents and blobs to repository-service using a dynamically
 *       provisioned NodeUploadService stub.</li>
 *   <li>Manages crawl session lifecycle (start, heartbeat, end).</li>
 * </ul>
 * This class is stateful only with respect to a lazily-initialized repository
 * upload stub; all other operations are request-scoped and reactive.
 */
public class ConnectorIntakeServiceImpl extends MutinyConnectorIntakeServiceGrpc.ConnectorIntakeServiceImplBase {

    private static final Logger LOG = Logger.getLogger(ConnectorIntakeServiceImpl.class);

    @Inject
    ConnectorValidationService validationService;

    @Inject
    GrpcClientFactory grpcClientFactory;

    /**
     * Default constructor for CDI / gRPC. No custom initialization required.
     */
    public ConnectorIntakeServiceImpl() { }

    @Override
    public Uni<UploadPipeDocResponse> uploadPipeDoc(UploadPipeDocRequest request) {
        return validationService.validateConnector(request.getConnectorId(), request.getApiKey())
            .flatMap(config -> {
                // Create repository upload request
                ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocRequest repoRequest =
                    ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocRequest.newBuilder()
                        .setDocument(request.getPipeDoc())
                        .build();
                // Use GrpcClientFactory for proper service discovery and channel configuration
                return grpcClientFactory.getClient("repo-service", MutinyNodeUploadServiceGrpc::newMutinyStub)
                    .flatMap(stub -> stub.uploadPipeDoc(repoRequest));
            })
            .map(repoResponse -> ai.pipestream.connector.intake.v1.UploadPipeDocResponse.newBuilder()
                .setSuccess(repoResponse.getSuccess())
                .setDocId(repoResponse.getDocumentId())
                .setMessage(repoResponse.getMessage())
                .build())
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to upload PipeDoc");
                return ai.pipestream.connector.intake.v1.UploadPipeDocResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(throwable.getMessage())
                    .build();
            });
    }

    @Override
    public Uni<UploadBlobResponse> uploadBlob(UploadBlobRequest request) {
        long startTime = System.nanoTime();
        int requestSize = request.getSerializedSize();
        LOG.debugf("uploadBlob START: size=%d bytes", requestSize);
        
        return validationService.validateConnector(request.getConnectorId(), request.getApiKey())
            .invoke(() -> {
                long validationTime = System.nanoTime() - startTime;
                LOG.debugf("uploadBlob: validation took %.3f ms", validationTime / 1_000_000.0);
            })
            .flatMap(config -> {
                long buildStart = System.nanoTime();
                Instant now = Instant.now();
                Timestamp timestamp = Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();

                Blob blob = Blob.newBuilder()
                    .setData(request.getContent())
                    .setFilename(request.getFilename())
                    .setMimeType(request.getMimeType())
                    .build();

                SearchMetadata metadata = SearchMetadata.newBuilder()
                    .setCreationDate(timestamp)
                    .setLastModifiedDate(timestamp)
                    .setProcessedDate(timestamp)
                    .setSourcePath(request.getPath())
                    .putAllMetadata(request.getMetadataMap())
                    .putMetadata("connector_id", request.getConnectorId())
                    .putMetadata("account_id", config.getAccountId())
                    .build();

                PipeDoc pipeDoc = PipeDoc.newBuilder()
                    .setSearchMetadata(metadata)
                    .setBlobBag(BlobBag.newBuilder().setBlob(blob).build())
                    .build();

                long buildTime = System.nanoTime() - buildStart;
                int pipeDocSize = pipeDoc.getSerializedSize();
                LOG.debugf("uploadBlob: PipeDoc build took %.3f ms, size=%d bytes", buildTime / 1_000_000.0, pipeDocSize);
                
                return Uni.createFrom().item(pipeDoc);
            })
            .flatMap(pipeDoc -> {
                long repoCallStart = System.nanoTime();
                LOG.debugf("uploadBlob: calling repo-service.uploadPipeDoc via GrpcClientFactory");
                // Create repository upload request
                ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocRequest repoRequest =
                    ai.pipestream.repository.v1.filesystem.upload.UploadPipeDocRequest.newBuilder()
                        .setDocument(pipeDoc)
                        .build();
                // Use GrpcClientFactory for proper service discovery and channel configuration
                return grpcClientFactory.getClient("repo-service", MutinyNodeUploadServiceGrpc::newMutinyStub)
                    .flatMap(stub -> stub.uploadPipeDoc(repoRequest))
                    .invoke(() -> {
                        long repoCallTime = System.nanoTime() - repoCallStart;
                        LOG.debugf("uploadBlob: repo-service call took %.3f ms", repoCallTime / 1_000_000.0);
                    });
            })
            .map(repoResponse -> {
                long mapStart = System.nanoTime();
                UploadBlobResponse response = UploadBlobResponse.newBuilder()
                    .setSuccess(repoResponse.getSuccess())
                    .setDocId(repoResponse.getDocumentId())
                    .setMessage(repoResponse.getMessage())
                    .build();
                long mapTime = System.nanoTime() - mapStart;
                long totalTime = System.nanoTime() - startTime;
                LOG.debugf("uploadBlob: response mapping took %.3f ms, TOTAL=%.3f ms", mapTime / 1_000_000.0, totalTime / 1_000_000.0);
                return response;
            })
            .onFailure().recoverWithItem(throwable -> {
                long totalTime = System.nanoTime() - startTime;
                LOG.errorf(throwable, "Failed to upload Blob after %.3f ms", totalTime / 1_000_000.0);
                return UploadBlobResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(throwable.getMessage())
                    .build();
            });
    }

    @Override
    public Uni<StartCrawlSessionResponse> startCrawlSession(StartCrawlSessionRequest request) {
        String sessionId = UUID.randomUUID().toString();
        return Uni.createFrom().item(StartCrawlSessionResponse.newBuilder()
            .setSuccess(true)
            .setSessionId(sessionId)
            .setMessage("Session started (stub)")
            .build());
    }

    @Override
    public Uni<EndCrawlSessionResponse> endCrawlSession(EndCrawlSessionRequest request) {
        return Uni.createFrom().item(EndCrawlSessionResponse.newBuilder()
            .setSuccess(true)
            .setMessage("Session ended (stub)")
            .build());
    }

    @Override
    public Uni<HeartbeatResponse> heartbeat(HeartbeatRequest request) {
        return Uni.createFrom().item(HeartbeatResponse.newBuilder()
            .setSessionValid(true)
            .build());
    }
}
