package ai.pipeline.connector.intake;

import ai.pipestream.connector.intake.v1.*;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;
import com.google.protobuf.ByteString;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.grpc.GrpcClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ConnectorIntakeService.
 * Uses wiremock server as a normal gRPC service (no WireMock dependency).
 */
@QuarkusTest
@QuarkusTestResource(ConnectorIntakeWireMockTestResource.class)
public class ConnectorIntakeIntegrationTest {

    @GrpcClient("connector-intake-service")
    MutinyConnectorIntakeServiceGrpc.MutinyConnectorIntakeServiceStub intakeClient;

    @Test
    void testUploadBlob_Success() {
        // Arrange - Use default datasource configured by wiremock server
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        UploadBlobRequest request = UploadBlobRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .setFilename("test.txt")
                .setMimeType("text/plain")
                .setContent(ByteString.copyFromUtf8("Hello World"))
                .setSourceDocId("test-blob")
                .build();

        // Act
        UploadBlobResponse response = intakeClient.uploadBlob(request)
                .await().atMost(Duration.ofSeconds(10));

        // Assert
        assertTrue(response.getSuccess());
        assertNotNull(response.getDocId());
        // Doc ID comes from MockRepositoryService (in-process @GrpcService)
        assertTrue(response.getDocId().startsWith("mock-doc-"));
    }

    @Test
    void testUploadPipeDoc_Success() {
        // Arrange - Use default datasource configured by wiremock server
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .setPipeDoc(PipeDoc.newBuilder().setDocId("test-doc").build())
                .build();

        // Act
        UploadPipeDocResponse response = intakeClient.uploadPipeDoc(request)
                .await().atMost(Duration.ofSeconds(10));

        // Assert
        assertTrue(response.getSuccess());
        assertNotNull(response.getDocId());
        // Doc ID comes from MockRepositoryService (in-process @GrpcService)
        assertTrue(response.getDocId().startsWith("mock-doc-"));
    }
}
