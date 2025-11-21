package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.ConnectorConfig;
import ai.pipestream.connector.intake.UploadBlobRequest;
import ai.pipestream.connector.intake.UploadPipeDocRequest;
import ai.pipestream.data.v1.BlobBag;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.SearchMetadata;
import ai.pipestream.repository.filesystem.upload.NodeUploadService;
import ai.pipestream.connector.intake.UploadResponse;
import com.google.protobuf.ByteString;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@QuarkusTest
class ConnectorIntakeServiceTest {

    @Inject
    @GrpcService
    ConnectorIntakeServiceImpl intakeService;

    @InjectMock
    ConnectorValidationService validationService;

    @InjectMock
    @GrpcClient("repo-service")
    NodeUploadService repoServiceMock;

    @Test
    void uploadPipeDoc_success() {
        // Arrange
        String connectorId = "test-conn";
        String apiKey = "test-key";
        ConnectorConfig config = ConnectorConfig.newBuilder().setAccountId("acc-1").build();

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
                .setConnectorId(connectorId)
                .setApiKey(apiKey)
                .setPipeDoc(PipeDoc.newBuilder().build())
                .build();

        ai.pipestream.repository.filesystem.upload.UploadResponse repoResponse = 
            ai.pipestream.repository.filesystem.upload.UploadResponse.newBuilder()
                .setSuccess(true)
                .setDocumentId("doc-1")
                .setMessage("OK")
                .build();

        when(validationService.validateConnector(connectorId, apiKey))
                .thenReturn(Uni.createFrom().item(config));
        when(repoServiceMock.uploadPipeDoc(any(PipeDoc.class)))
                .thenReturn(Uni.createFrom().item(repoResponse));

        // Act
        UploadResponse response = intakeService.uploadPipeDoc(request).await().indefinitely();

        // Assert
        assertTrue(response.getSuccess());
        assertEquals("doc-1", response.getDocId());
        verify(validationService).validateConnector(connectorId, apiKey);
        verify(repoServiceMock).uploadPipeDoc(any(PipeDoc.class));
    }

    @Test
    void uploadBlob_success_constructsPipeDoc() {
        // Arrange
        String connectorId = "test-conn";
        String apiKey = "test-key";
        String accountId = "acc-1";
        ConnectorConfig config = ConnectorConfig.newBuilder().setAccountId(accountId).build();

        ByteString content = ByteString.copyFromUtf8("hello world");
        UploadBlobRequest request = UploadBlobRequest.newBuilder()
                .setConnectorId(connectorId)
                .setApiKey(apiKey)
                .setFilename("test.txt")
                .setMimeType("text/plain")
                .setPath("/folder/test.txt")
                .setContent(content)
                .putMetadata("key1", "val1")
                .build();

        ai.pipestream.repository.filesystem.upload.UploadResponse repoResponse = 
            ai.pipestream.repository.filesystem.upload.UploadResponse.newBuilder()
                .setSuccess(true)
                .setDocumentId("doc-2")
                .setMessage("OK")
                .build();

        when(validationService.validateConnector(connectorId, apiKey))
                .thenReturn(Uni.createFrom().item(config));
        when(repoServiceMock.uploadPipeDoc(any(PipeDoc.class)))
                .thenReturn(Uni.createFrom().item(repoResponse));

        // Act
        UploadResponse response = intakeService.uploadBlob(request).await().indefinitely();

        // Assert
        assertTrue(response.getSuccess());
        assertEquals("doc-2", response.getDocId());

        // Verify PipeDoc construction
        ArgumentCaptor<PipeDoc> pipeDocCaptor = ArgumentCaptor.forClass(PipeDoc.class);
        verify(repoServiceMock).uploadPipeDoc(pipeDocCaptor.capture());
        PipeDoc capturedDoc = pipeDocCaptor.getValue();

        // Check Blob
        assertTrue(capturedDoc.hasBlobBag());
        BlobBag bag = capturedDoc.getBlobBag();
        assertTrue(bag.hasBlob());
        assertEquals("test.txt", bag.getBlob().getFilename());
        assertEquals("text/plain", bag.getBlob().getMimeType());
        assertEquals(content, bag.getBlob().getData());

        // Check Metadata
        SearchMetadata meta = capturedDoc.getSearchMetadata();
        assertEquals("/folder/test.txt", meta.getSourcePath());
        assertEquals("val1", meta.getMetadataOrDefault("key1", ""));
        assertEquals(connectorId, meta.getMetadataOrDefault("connector_id", ""));
        assertEquals(accountId, meta.getMetadataOrDefault("account_id", ""));
        assertTrue(meta.hasCreationDate());
    }
}
