package ai.pipeline.connector.intake.pipedoc.streaming;

import ai.pipeline.connector.intake.pipedoc.PipeDocAcceptanceService;
import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import io.grpc.stub.StreamObserver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class StreamingPipeDocUploadService {

    @Inject
    ConfigResolutionService configResolutionService;

    @Inject
    PipeDocAcceptanceService pipeDocAcceptanceService;

    public StreamObserver<UploadPipeDocStreamRequest> open(
            StreamObserver<UploadPipeDocStreamResponse> responseObserver) {
        StreamingPipeDocObserver observer = new StreamingPipeDocObserver(
                responseObserver,
                configResolutionService,
                pipeDocAcceptanceService);
        observer.requestNext();
        return observer;
    }
}
