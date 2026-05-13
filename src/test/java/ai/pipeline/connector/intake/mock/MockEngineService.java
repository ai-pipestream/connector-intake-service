package ai.pipeline.connector.intake.mock;

import ai.pipestream.engine.v1.EngineV1ServiceGrpc;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.engine.v1.IntakeHandoffStreamRequest;
import ai.pipestream.engine.v1.IntakeHandoffStreamResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Singleton;

@Singleton
@GrpcService
public class MockEngineService extends EngineV1ServiceGrpc.EngineV1ServiceImplBase {

    @Override
    public StreamObserver<IntakeHandoffStreamRequest> intakeHandoffStream(
            StreamObserver<IntakeHandoffStreamResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(IntakeHandoffStreamRequest request) {
                MockEngineState.record(request);
                if (MockEngineState.mode() == MockEngineState.Mode.STREAM_ERROR) {
                    responseObserver.onError(Status.UNAVAILABLE
                            .withDescription("mock engine stream error")
                            .asRuntimeException());
                    return;
                }
                if (MockEngineState.mode() == MockEngineState.Mode.NO_ACK) {
                    return;
                }
                IntakeHandoffResponse response = IntakeHandoffResponse.newBuilder()
                        .setAccepted(MockEngineState.mode() == MockEngineState.Mode.ACCEPT)
                        .setAssignedStreamId("mock-engine-stream")
                        .setEntryNodeId("mock-entry-node")
                        .setMessage("mock engine " + MockEngineState.mode().name())
                        .build();
                responseObserver.onNext(IntakeHandoffStreamResponse.newBuilder()
                        .setHandoffId(request.getHandoffId())
                        .setSequence(request.getSequence())
                        .setStatus(MockEngineState.responseStatus())
                        .setResponse(response)
                        .build());
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
