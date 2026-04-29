package ai.pipeline.connector.intake.service;

import ai.pipeline.connector.intake.pipedoc.GrpcBlobUploadService;
import ai.pipeline.connector.intake.pipedoc.UnaryPipeDocUploadService;
import ai.pipeline.connector.intake.pipedoc.streaming.StreamingPipeDocUploadService;
import ai.pipeline.connector.intake.session.CrawlSessionRpcService;
import ai.pipestream.connector.intake.v1.ConnectorIntakeServiceGrpc;
import ai.pipestream.connector.intake.v1.EndCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.EndCrawlSessionResponse;
import ai.pipestream.connector.intake.v1.HeartbeatRequest;
import ai.pipestream.connector.intake.v1.HeartbeatResponse;
import ai.pipestream.connector.intake.v1.StartCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.StartCrawlSessionResponse;
import ai.pipestream.connector.intake.v1.UploadBlobRequest;
import ai.pipestream.connector.intake.v1.UploadBlobResponse;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Thin gRPC adapter for connector intake endpoints.
 * <p>
 * Endpoint implementations live in focused services/packages so transport
 * wiring stays separate from intake behavior.
 */
@Singleton
@GrpcService
public class ConnectorIntakeServiceImpl extends ConnectorIntakeServiceGrpc.ConnectorIntakeServiceImplBase {

    @Inject
    UnaryPipeDocUploadService unaryPipeDocUploadService;

    @Inject
    GrpcBlobUploadService grpcBlobUploadService;

    @Inject
    CrawlSessionRpcService crawlSessionRpcService;

    @Inject
    StreamingPipeDocUploadService streamingPipeDocUploadService;

    public ConnectorIntakeServiceImpl() {}

    @Override
    @RunOnVirtualThread
    public void uploadPipeDoc(UploadPipeDocRequest request,
                              StreamObserver<UploadPipeDocResponse> responseObserver) {
        unaryPipeDocUploadService.uploadPipeDoc(request, responseObserver);
    }

    @Override
    @RunOnVirtualThread
    public void uploadBlob(UploadBlobRequest request,
                           StreamObserver<UploadBlobResponse> responseObserver) {
        grpcBlobUploadService.uploadBlob(request, responseObserver);
    }

    @Override
    @RunOnVirtualThread
    public void startCrawlSession(StartCrawlSessionRequest request,
                                  StreamObserver<StartCrawlSessionResponse> responseObserver) {
        crawlSessionRpcService.startCrawlSession(request, responseObserver);
    }

    @Override
    @RunOnVirtualThread
    public void endCrawlSession(EndCrawlSessionRequest request,
                                StreamObserver<EndCrawlSessionResponse> responseObserver) {
        crawlSessionRpcService.endCrawlSession(request, responseObserver);
    }

    @Override
    @RunOnVirtualThread
    public void heartbeat(HeartbeatRequest request,
                          StreamObserver<HeartbeatResponse> responseObserver) {
        crawlSessionRpcService.heartbeat(request, responseObserver);
    }

    @Override
    public StreamObserver<UploadPipeDocStreamRequest> uploadPipeDocStream(
            StreamObserver<UploadPipeDocStreamResponse> responseObserver) {
        return streamingPipeDocUploadService.open(responseObserver);
    }
}
