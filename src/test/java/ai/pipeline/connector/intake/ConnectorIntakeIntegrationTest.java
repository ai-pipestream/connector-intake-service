package ai.pipeline.connector.intake;

import ai.pipeline.connector.intake.service.ConnectorValidationService;
import ai.pipestream.connector.intake.v1.*;
import ai.pipestream.data.v1.PipeDoc;
import com.google.protobuf.ByteString;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
public class ConnectorIntakeIntegrationTest {

    @GrpcClient("connector-intake-service")
    MutinyConnectorIntakeServiceGrpc.MutinyConnectorIntakeServiceStub intakeClient;

    @InjectMock
    ConnectorValidationService validationService;

    @Test
    void testUploadBlob_Success() {
        // Arrange
        String connectorId = "test-conn-1";
        String apiKey = "test-key-1";
        ConnectorConfig config = ConnectorConfig.newBuilder()
                .setAccountId("acc-1")
                .build();

        when(validationService.validateConnector(anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(config));

        UploadBlobRequest request = UploadBlobRequest.newBuilder()
                .setConnectorId(connectorId)
                .setApiKey(apiKey)
                .setFilename("test.txt")
                .setMimeType("text/plain")
                .setContent(ByteString.copyFromUtf8("Hello World")) // This is very small
                .build();

        // Act
        UploadBlobResponse response = intakeClient.uploadBlob(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertTrue(response.getSuccess());
        assertNotNull(response.getDocId());
        assertTrue(response.getDocId().startsWith("mock-doc-"));
    }

    @Test
    void testUploadPipeDoc_Success() {
        // Arrange
        String connectorId = "test-conn-2";
        String apiKey = "test-key-2";
        ConnectorConfig config = ConnectorConfig.newBuilder()
                .setAccountId("acc-2")
                .build();

        when(validationService.validateConnector(anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(config));

        UploadPipeDocRequest request = UploadPipeDocRequest.newBuilder()
                .setConnectorId(connectorId)
                .setApiKey(apiKey)
                .setPipeDoc(PipeDoc.newBuilder().build())
                .build();

        // Act
        UploadPipeDocResponse response = intakeClient.uploadPipeDoc(request)
                .await().atMost(Duration.ofSeconds(5));

        // Assert
        assertTrue(response.getSuccess());
        assertNotNull(response.getDocId());
        assertTrue(response.getDocId().startsWith("mock-doc-"));
    }
}
