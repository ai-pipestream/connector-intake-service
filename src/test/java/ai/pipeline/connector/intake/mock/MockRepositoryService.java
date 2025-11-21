package ai.pipeline.connector.intake.mock;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.filesystem.upload.GetDocumentRequest;
import ai.pipestream.repository.filesystem.upload.MutinyNodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.UploadResponse;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

import java.util.UUID;

/**
 * A test-scoped mock implementation of the Repository Service.
 * This runs as a real gRPC service within the same Quarkus test process,
 * intercepted by the client in the intake service.
 */
@Singleton
@GrpcService
public class MockRepositoryService extends MutinyNodeUploadServiceGrpc.NodeUploadServiceImplBase {

    private static final Logger LOG = Logger.getLogger(MockRepositoryService.class);

    @Override
    public Uni<UploadResponse> uploadPipeDoc(PipeDoc request) {
        long startTime = System.nanoTime();
        int requestSize = request.getSerializedSize();
        LOG.debugf("MockRepositoryService.uploadPipeDoc START: size=%d bytes", requestSize);
        
        return Uni.createFrom().item(() -> {
            long buildStart = System.nanoTime();
            UploadResponse response = UploadResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId("mock-doc-" + UUID.randomUUID())
                    .setS3Key("mock/s3/key/" + request.getSearchMetadata().getSourcePath())
                    .setMessage("Mock upload successful")
                    .build();
            long buildTime = System.nanoTime() - buildStart;
            long totalTime = System.nanoTime() - startTime;
            LOG.debugf("MockRepositoryService.uploadPipeDoc: build took %.3f ms, TOTAL=%.3f ms", 
                    buildTime / 1_000_000.0, totalTime / 1_000_000.0);
            return response;
        });
    }

    @Override
    public Uni<PipeDoc> getDocument(GetDocumentRequest request) {
        return Uni.createFrom().failure(new UnsupportedOperationException("Not implemented in mock"));
    }
}
