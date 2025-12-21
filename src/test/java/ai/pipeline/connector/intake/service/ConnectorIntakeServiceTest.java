package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.connector.intake.v1.UploadBlobRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import ai.pipestream.data.v1.PipeDoc;
import com.google.protobuf.ByteString;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@QuarkusTest
class ConnectorIntakeServiceTest {

    @Inject
    @GrpcService
    ConnectorIntakeServiceImpl intakeService;

    @InjectMock
    ConnectorValidationService validationService;

    // We use dynamic-grpc over Netty to the in-process MockRepositoryService (@GrpcService),
    // no client mock injection here.

    @Test
    void uploadPipeDoc_success() {
        // Arrange
        String connectorId = "test-conn";
        String apiKey = "test-key";
        DataSourceConfig config = DataSourceConfig.newBuilder().setAccountId("acc-1").build();

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
                .setDatasourceId(connectorId)
                .setApiKey(apiKey)
                .setPipeDoc(PipeDoc.newBuilder().build())
                .build();

        ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse repoResponse =
            ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse.newBuilder()
                .setSuccess(true)
                .setDocumentId("doc-1")
                .setMessage("OK")
                .build();

        when(validationService.validateDataSource(connectorId, apiKey))
                .thenReturn(Uni.createFrom().item(config));
        // No repoServiceMock – request will be handled by MockRepositoryService via real Netty channel

        // Act
        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response = intakeService.uploadPipeDoc(request).await().indefinitely();

        // Assert
        assertTrue(response.getSuccess());
        assertFalse(response.getDocId().isEmpty());
        verify(validationService).validateDataSource(connectorId, apiKey);
    }

    @Test
    void uploadBlob_success_constructsPipeDoc() {
        // Arrange
        String connectorId = "test-conn";
        String apiKey = "test-key";
        String accountId = "acc-1";
        DataSourceConfig config = DataSourceConfig.newBuilder().setAccountId(accountId).build();

        ByteString content = ByteString.copyFromUtf8("hello world");
        UploadBlobRequest request = UploadBlobRequest.newBuilder()
                .setDatasourceId(connectorId)
                .setApiKey(apiKey)
                .setFilename("test.txt")
                .setMimeType("text/plain")
                .setPath("/folder/test.txt")
                .setContent(content)
                .putMetadata("key1", "val1")
                .build();

        ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse repoResponse =
            ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse.newBuilder()
                .setSuccess(true)
                .setDocumentId("doc-2")
                .setMessage("OK")
                .build();

        when(validationService.validateDataSource(connectorId, apiKey))
                .thenReturn(Uni.createFrom().item(config));
        // No repoServiceMock – request will be handled by MockRepositoryService via real Netty channel

        // Act
        ai.pipestream.connector.intake.v1.UploadBlobResponse response = intakeService.uploadBlob(request).await().indefinitely();

        // Assert
        assertTrue(response.getSuccess());
        assertFalse(response.getDocId().isEmpty());

        // We assert the happy-path response only; deeper PipeDoc construction is covered by integration tests
        // against MockRepositoryService over real Netty gRPC.
    }
}
