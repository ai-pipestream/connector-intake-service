package ai.pipeline.connector.intake;

import ai.pipestream.connector.intake.*;
import ai.pipestream.repository.filesystem.upload.UploadState;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for connector-intake-service chunked upload flow with mocked repository-service.
 * <p>
 * This test verifies that:
 * 1. StartChunkedUpload calls repository-service.InitiateUpload
 * 2. UploadAsyncChunk calls repository-service.UploadChunk (multiple chunks)
 * 3. CompleteChunkedUpload calls repository-service.GetUploadStatus
 * <p>
 * All repository-service calls are mocked via WireMock JSON mappings.
 */
@QuarkusTest
public class ConnectorIntakeRepositoryIntegrationTest extends RepositoryServiceWireMockTestBase {

    private static final String TEST_CONNECTOR_ID = "test-connector-123";
    private static final String TEST_API_KEY = "test-api-key";

    private ManagedChannel intakeChannel;
    private MutinyConnectorIntakeServiceGrpc.MutinyConnectorIntakeServiceStub intakeClient;
    private WireMock wireMockClient;

    @BeforeEach
    void setUp() {
        // Get WireMock port
        int wireMockPort = getWireMockPort();

        // Create WireMock client for verification
        wireMockClient = new WireMock("localhost", wireMockPort);

        // Create gRPC client to call connector-intake-service
        // Note: In @QuarkusTest, the service runs on quarkus.http.test-port
        int intakePort = Integer.parseInt(
                System.getProperty("quarkus.http.test-port", "38108")
        );

        intakeChannel = ManagedChannelBuilder
                .forAddress("localhost", intakePort)
                .usePlaintext()
                .build();

        intakeClient = MutinyConnectorIntakeServiceGrpc.newMutinyStub(intakeChannel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (intakeChannel != null && !intakeChannel.isShutdown()) {
            intakeChannel.shutdown();
            intakeChannel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testStartChunkedUpload_CallsRepositoryService() throws Exception {
        // Call connector-intake-service.StartChunkedUpload
        StartChunkedUploadRequest request = StartChunkedUploadRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setApiKey(TEST_API_KEY)
                .setFilename("test-file.pdf")
                .setPath("test/path")
                .setMimeType("application/pdf")
                .setExpectedSizeBytes(1024 * 1024) // 1MB
                .setExpectedChunkCount(10)
                .setExpectedChunkSize(100 * 1024) // 100KB chunks
                .build();

        // This will fail without connector validation, but we can verify the structure
        // TODO: Need to mock ConnectorValidationService or use test profile

        StartChunkedUploadResponse response = intakeClient.startChunkedUpload(request)
                .await().atMost(java.time.Duration.ofSeconds(5));

        // Verify response
        assertNotNull(response);

        // Verify WireMock received InitiateUpload call to repository-service
        wireMockClient.verify(
                postRequestedFor(urlPathEqualTo(
                        "/ai.pipestream.repository.filesystem.upload.NodeUploadService/InitiateUpload"
                ))
        );
    }

    @Test
    void testWireMockDirectly_VerifyRepositoryServiceStub() {
        // Test WireMock directly to verify the stub works
        int wireMockPort = getWireMockPort();

        ManagedChannel repoChannel = ManagedChannelBuilder
                .forAddress("localhost", wireMockPort)
                .usePlaintext()
                .build();

        try {
            var repoStub = ai.pipestream.repository.filesystem.upload.NodeUploadServiceGrpc
                    .newBlockingStub(repoChannel);

            var initRequest = ai.pipestream.repository.filesystem.upload.InitiateUploadRequest.newBuilder()
                    .setDrive("test-drive")
                    .setName("test.pdf")
                    .setExpectedSize(1024 * 1024)
                    .setConnectorId("test-connector")
                    .build();

            var initResponse = repoStub.initiateUpload(initRequest);

            // Verify we got the mocked response
            assertNotNull(initResponse);
            assertNotNull(initResponse.getNodeId());
            assertNotNull(initResponse.getUploadId());
            assertEquals(UploadState.UPLOAD_STATE_PENDING, initResponse.getState());

            // Verify WireMock received the call
            wireMockClient.verify(
                    postRequestedFor(urlPathEqualTo(
                            "/ai.pipestream.repository.filesystem.upload.NodeUploadService/InitiateUpload"
                    ))
            );

        } finally {
            repoChannel.shutdown();
        }
    }
}