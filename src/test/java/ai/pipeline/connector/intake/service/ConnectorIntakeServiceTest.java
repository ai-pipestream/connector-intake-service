package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.CrawlMetadata;
import ai.pipestream.connector.intake.v1.StartCrawlSessionRequest;
import ai.pipestream.connector.intake.v1.UploadBlobRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocRequest;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;
import com.google.protobuf.ByteString;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConnectorIntakeService.
 * Uses wiremock server as a normal gRPC service (no WireMock dependency).
 */
@QuarkusTest
@QuarkusTestResource(ConnectorIntakeWireMockTestResource.class)
class ConnectorIntakeServiceTest {

    @Inject
    @GrpcService
    ConnectorIntakeServiceImpl intakeService;

    @Test
    void uploadPipeDoc_withDefaultResponse_persistsAndHandsOff() {
        // Arrange - Use default datasource configured by wiremock server
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .setPipeDoc(PipeDoc.newBuilder().setDocId("test-doc").build())
            .build();

        // Act
        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response = 
            intakeService.uploadPipeDoc(request).await().indefinitely();

        // Assert
        assertTrue(response.getSuccess());
        assertFalse(response.getDocId().isEmpty());
        // Doc ID comes from MockRepositoryService (in-process @GrpcService)
        assertTrue(response.getDocId().startsWith("mock-doc-"));
    }

    @Test
    void uploadBlob_success_constructsPipeDoc() {
        // Arrange
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        ByteString content = ByteString.copyFromUtf8("hello world");
        UploadBlobRequest request = UploadBlobRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .setFilename("test.txt")
            .setMimeType("text/plain")
            .setPath("/folder/test.txt")
            .setContent(content)
            .putMetadata("key1", "val1")
            .build();

        // Act
        ai.pipestream.connector.intake.v1.UploadBlobResponse response = 
            intakeService.uploadBlob(request).await().indefinitely();

        // Assert
        assertTrue(response.getSuccess());
        assertFalse(response.getDocId().isEmpty());
        // Doc ID comes from MockRepositoryService
        assertTrue(response.getDocId().startsWith("mock-doc-"));
    }

    @Test
    void uploadPipeDoc_invalidApiKey_returnsFailureResponse() {
        String datasourceId = "valid-datasource";
        String apiKey = "invalid-api-key";

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .setPipeDoc(PipeDoc.newBuilder().setDocId("test-doc").build())
            .build();

        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response =
            intakeService.uploadPipeDoc(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertEquals("test-doc", response.getDocId());
        assertTrue(response.getMessage().contains("UNAUTHENTICATED") || response.getMessage().contains("Invalid API key"));
    }

    @Test
    void uploadPipeDoc_inactiveDatasource_returnsFailureResponse() {
        String datasourceId = "inactive-datasource";
        String apiKey = "inactive-key";

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .setPipeDoc(PipeDoc.newBuilder().setDocId("test-doc").build())
            .build();

        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response =
            intakeService.uploadPipeDoc(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertEquals("test-doc", response.getDocId());
        assertTrue(response.getMessage().contains("UNAUTHENTICATED") || response.getMessage().contains("inactive"));
    }

    @Test
    void uploadPipeDoc_inactiveAccount_returnsFailureResponse() {
        String datasourceId = "inactive-account-datasource";
        String apiKey = "inactive-account-key";

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .setPipeDoc(PipeDoc.newBuilder().setDocId("test-doc").build())
            .build();

        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response =
            intakeService.uploadPipeDoc(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertEquals("test-doc", response.getDocId());
        assertTrue(response.getMessage().contains("PERMISSION_DENIED") || response.getMessage().contains("inactive"));
    }

    @Test
    void uploadPipeDoc_missingAccount_returnsFailureResponse() {
        String datasourceId = "missing-account-datasource";
        String apiKey = "missing-account-key";

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .setPipeDoc(PipeDoc.newBuilder().setDocId("test-doc").build())
            .build();

        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response =
            intakeService.uploadPipeDoc(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertEquals("test-doc", response.getDocId());
        assertTrue(response.getMessage().contains("PERMISSION_DENIED") || response.getMessage().contains("does not exist"));
    }

    @Test
    void uploadBlob_invalidApiKey_returnsFailureResponse() {
        String datasourceId = "valid-datasource";
        String apiKey = "invalid-api-key";

        UploadBlobRequest request = UploadBlobRequest.newBuilder()
            .setDatasourceId(datasourceId)
            .setApiKey(apiKey)
            .setFilename("test.txt")
            .setMimeType("text/plain")
            .setContent(ByteString.copyFromUtf8("hello world"))
            .build();

        ai.pipestream.connector.intake.v1.UploadBlobResponse response =
            intakeService.uploadBlob(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().contains("UNAUTHENTICATED") || response.getMessage().contains("Invalid API key"));
    }

    @Test
    void uploadPipeDoc_missingDocId_rejected() {
        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId("valid-datasource")
            .setApiKey("valid-api-key")
            .setPipeDoc(PipeDoc.newBuilder().setDocId("").build())
            .build();

        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response =
            intakeService.uploadPipeDoc(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertEquals("", response.getDocId());
        assertTrue(response.getMessage().toLowerCase().contains("doc_id"));
    }

    @Test
    void uploadPipeDoc_noPipeDoc_rejected() {
        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId("valid-datasource")
            .setApiKey("valid-api-key")
            .build();

        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response =
            intakeService.uploadPipeDoc(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertEquals("", response.getDocId());
        assertTrue(response.getMessage().toLowerCase().contains("pipe_doc"));
    }

    @Test
    void uploadPipeDoc_blankDatasourceId_rejected() {
        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId("")
            .setApiKey("valid-api-key")
            .setPipeDoc(PipeDoc.newBuilder().setDocId("doc-1").build())
            .build();

        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response =
            intakeService.uploadPipeDoc(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertEquals("doc-1", response.getDocId());
        assertTrue(response.getMessage().toLowerCase().contains("datasource_id"));
    }

    @Test
    void uploadPipeDoc_blankApiKey_rejected() {
        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
            .setDatasourceId("valid-datasource")
            .setApiKey("")
            .setPipeDoc(PipeDoc.newBuilder().setDocId("doc-1").build())
            .build();

        ai.pipestream.connector.intake.v1.UploadPipeDocResponse response =
            intakeService.uploadPipeDoc(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertEquals("doc-1", response.getDocId());
        assertTrue(response.getMessage().toLowerCase().contains("api_key"));
    }

    @Test
    void uploadBlob_blankDatasourceId_rejected() {
        UploadBlobRequest request = UploadBlobRequest.newBuilder()
            .setDatasourceId("")
            .setApiKey("valid-api-key")
            .setFilename("test.txt")
            .setMimeType("text/plain")
            .setContent(ByteString.copyFromUtf8("hello world"))
            .build();

        ai.pipestream.connector.intake.v1.UploadBlobResponse response =
            intakeService.uploadBlob(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().toLowerCase().contains("datasource_id"));
    }

    @Test
    void uploadBlob_blankApiKey_rejected() {
        UploadBlobRequest request = UploadBlobRequest.newBuilder()
            .setDatasourceId("valid-datasource")
            .setApiKey("")
            .setFilename("test.txt")
            .setMimeType("text/plain")
            .setContent(ByteString.copyFromUtf8("hello world"))
            .build();

        ai.pipestream.connector.intake.v1.UploadBlobResponse response =
            intakeService.uploadBlob(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().toLowerCase().contains("api_key"));
    }

    @Test
    void startCrawlSession_blankApiKey_rejected() {
        StartCrawlSessionRequest request = StartCrawlSessionRequest.newBuilder()
            .setDatasourceId("valid-datasource")
            .setApiKey("")
            .setCrawlId("crawl-1")
            .setMetadata(CrawlMetadata.newBuilder()
                .setConnectorType("test")
                .setSourceSystem("test")
                .build())
            .build();

        var response = intakeService.startCrawlSession(request).await().indefinitely();

        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().toLowerCase().contains("api_key"));
    }
}
