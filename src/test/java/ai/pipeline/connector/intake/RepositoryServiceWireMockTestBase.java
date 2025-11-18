package ai.pipeline.connector.intake;

import ai.pipestream.grpc.wiremock.WireMockGrpcTestBase;
import org.junit.jupiter.api.BeforeAll;

/**
 * Base test class for connector-intake-service tests that need to mock repository-service.
 * <p>
 * This class extends WireMockGrpcTestBase which automatically starts WireMock with gRPC support.
 * The repository-service client will be configured to use the WireMock port.
 */
public abstract class RepositoryServiceWireMockTestBase extends WireMockGrpcTestBase {

    /**
     * Configure repository-service client to use WireMock port.
     * This is called automatically before all tests.
     */
    @BeforeAll
    public static void configureRepositoryServiceClient() {
        int port = getWireMockPort();
        System.setProperty("quarkus.grpc.clients.NodeUploadService.port", String.valueOf(port));
        System.setProperty("test.wiremock.port", String.valueOf(port));
    }
}

