package ai.pipeline.connector.intake.mock;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.v1.filesystem.upload.GetUploadedDocumentRequest;
import ai.pipestream.repository.v1.filesystem.upload.GetUploadedDocumentResponse;
import ai.pipestream.repository.v1.filesystem.upload.MutinyNodeUploadServiceGrpc;
import ai.pipestream.repository.v1.filesystem.upload.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.v1.filesystem.upload.UploadFilesystemPipeDocResponse;
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
    public Uni<UploadFilesystemPipeDocResponse> uploadFilesystemPipeDoc(UploadFilesystemPipeDocRequest request) {
        long startTime = System.nanoTime();
        PipeDoc doc = request.getDocument();
        int requestSize = doc.getSerializedSize();
        LOG.debugf("MockRepositoryService.uploadFilesystemPipeDoc START: size=%d bytes", requestSize);

        return Uni.createFrom().item(() -> {
            long buildStart = System.nanoTime();
            UploadFilesystemPipeDocResponse response = UploadFilesystemPipeDocResponse.newBuilder()
                    .setSuccess(true)
                    .setDocumentId("mock-doc-" + UUID.randomUUID())
                    .setS3Key("mock/s3/key/" + doc.getSearchMetadata().getSourcePath())
                    .setMessage("Mock upload successful")
                    .build();
            long buildTime = System.nanoTime() - buildStart;
            long totalTime = System.nanoTime() - startTime;
            LOG.debugf("MockRepositoryService.uploadFilesystemPipeDoc: build took %.3f ms, TOTAL=%.3f ms",
                    buildTime / 1_000_000.0, totalTime / 1_000_000.0);
            return response;
        });
    }

    @Override
    public Uni<GetUploadedDocumentResponse> getUploadedDocument(GetUploadedDocumentRequest request) {
        return Uni.createFrom().failure(new UnsupportedOperationException("Not implemented in mock"));
    }
}
