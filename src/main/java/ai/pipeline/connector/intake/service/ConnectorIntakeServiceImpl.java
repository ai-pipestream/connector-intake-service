package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.MutinyConnectorIntakeServiceGrpc;
import ai.pipestream.connector.intake.UploadPipeDocRequest;
import ai.pipestream.connector.intake.UploadBlobRequest;
import ai.pipestream.connector.intake.UploadResponse;
import ai.pipestream.connector.intake.StartCrawlSessionRequest;
import ai.pipestream.connector.intake.StartCrawlSessionResponse;
import ai.pipestream.connector.intake.EndCrawlSessionRequest;
import ai.pipestream.connector.intake.EndCrawlSessionResponse;
import ai.pipestream.connector.intake.HeartbeatRequest;
import ai.pipestream.connector.intake.HeartbeatResponse;
import ai.pipestream.connector.intake.ConnectorConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.Blob;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.dynamic.grpc.client.GrpcClientProvider;
import ai.pipestream.repository.filesystem.upload.MutinyNodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.NodeUploadService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import java.util.Optional;
import java.util.UUID;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import com.google.protobuf.ByteString;

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
    GrpcClientProvider grpcClientProvider;

    @ConfigProperty(name = "quarkus.grpc.clients.repo-service.host", defaultValue = "localhost")
    String repoServiceHost;

    @ConfigProperty(name = "quarkus.grpc.clients.repo-service.port", defaultValue = "38105")
    int repoServicePort;

    // Use dynamic-grpc for proper flow control window and channel caching
    // Channels are cached by host:port in GrpcClientProvider
    // The stub class is what GrpcClientProvider returns
    // Lazy initialization - port may not be known at @PostConstruct time in tests
    private volatile MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub repoService;

    /**
     * Default constructor for CDI / gRPC. No custom initialization required.
     */
    public ConnectorIntakeServiceImpl() { }

    private MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub getRepoService() {
        if (repoService == null) {
            synchronized (this) {
                if (repoService == null) {
                    // Resolve port at runtime using gRPC-aware properties first. We run with Netty (no in-process),
                    // and in tests we may use a unified server (no separate gRPC port). In that case, the gRPC
                    // server listens on the HTTP port. We first honor an explicit system property set by tests
                    // (quarkus.grpc.server.port.actual), then fall back to HTTP test/regular port.
                    int port = repoServicePort;
                    if (port == 0) {
                        // 1) Prefer gRPC actual port if provided by tests/benchmarks
                        String grpcActual = System.getProperty("quarkus.grpc.server.port.actual");
                        if (grpcActual != null && !grpcActual.isBlank()) {
                            try {
                                port = Integer.parseInt(grpcActual);
                                LOG.debugf("Using gRPC actual port from system property: %d", port);
                            } catch (NumberFormatException nfe) {
                                LOG.warnf(nfe, "Invalid quarkus.grpc.server.port.actual value: %s", grpcActual);
                            }
                        }

                        // 2) Separate server: test-port first
                        if (port == 0) {
                            Optional<Integer> grpcTestPort = ConfigProvider.getConfig()
                                    .getOptionalValue("quarkus.grpc.server.test-port", Integer.class);
                            if (grpcTestPort.isPresent() && grpcTestPort.get() > 0) {
                                port = grpcTestPort.get();
                                LOG.debugf("Using gRPC server test-port from config: %d", port);
                            }
                        }

                        // 3) Separate server: regular gRPC port
                        if (port == 0) {
                            Optional<Integer> grpcPort = ConfigProvider.getConfig()
                                    .getOptionalValue("quarkus.grpc.server.port", Integer.class);
                            if (grpcPort.isPresent() && grpcPort.get() > 0) {
                                port = grpcPort.get();
                                LOG.debugf("Using gRPC server port from config: %d", port);
                            }
                        }

                        // 4) As a last fallback, try HTTP ports (unified mode)
                        if (port == 0) {
                            Optional<Integer> httpTestPort = ConfigProvider.getConfig()
                                    .getOptionalValue("quarkus.http.test-port", Integer.class);
                            if (httpTestPort.isPresent() && httpTestPort.get() > 0) {
                                port = httpTestPort.get();
                                LOG.debugf("Using HTTP test-port (unified gRPC fallback) from config: %d", port);
                            }
                        }
                        if (port == 0) {
                            Optional<Integer> httpPort = ConfigProvider.getConfig()
                                    .getOptionalValue("quarkus.http.port", Integer.class);
                            if (httpPort.isPresent() && httpPort.get() > 0) {
                                port = httpPort.get();
                                LOG.debugf("Using HTTP port (unified gRPC fallback) from config: %d", port);
                            }
                        }

                        if (port == 0) {
                            throw new IllegalStateException("Cannot resolve gRPC port for repo-service. Set quarkus.grpc.server.port.actual or configure quarkus.grpc.server.[test-]port.");
                        }
                    }
                    
                    // Use direct host/port connection (channel is cached by GrpcClientProvider)
                    // For service discovery, use: grpcClientProvider.getClientForService(MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub.class, "repo-service")
                    repoService = grpcClientProvider.getClient(MutinyNodeUploadServiceGrpc.MutinyNodeUploadServiceStub.class, repoServiceHost, port);
                    LOG.infof("Initialized repo-service client using dynamic-grpc at %s:%d (with proper flow control window)", 
                            repoServiceHost, port);
                }
            }
        }
        return repoService;
    }

    @Override
    public Uni<UploadResponse> uploadPipeDoc(UploadPipeDocRequest request) {
        return validationService.validateConnector(request.getConnectorId(), request.getApiKey())
            .flatMap(config -> getRepoService().uploadPipeDoc(request.getPipeDoc()))
            .map(repoResponse -> UploadResponse.newBuilder()
                .setSuccess(repoResponse.getSuccess())
                .setDocId(repoResponse.getDocumentId())
                .setMessage(repoResponse.getMessage())
                .build())
            .onFailure().recoverWithItem(throwable -> {
                LOG.errorf(throwable, "Failed to upload PipeDoc");
                return UploadResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(throwable.getMessage())
                    .build();
            });
    }

    @Override
    public Uni<UploadResponse> uploadBlob(UploadBlobRequest request) {
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
                LOG.debugf("uploadBlob: calling repoService.uploadPipeDoc");
                return getRepoService().uploadPipeDoc(pipeDoc)
                    .invoke(() -> {
                        long repoCallTime = System.nanoTime() - repoCallStart;
                        LOG.debugf("uploadBlob: repoService call took %.3f ms", repoCallTime / 1_000_000.0);
                    });
            })
            .map(repoResponse -> {
                long mapStart = System.nanoTime();
                UploadResponse response = UploadResponse.newBuilder()
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
                return UploadResponse.newBuilder()
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
