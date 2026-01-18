package ai.pipeline.connector.intake.service;

import io.quarkus.cache.CacheManager;
import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConnectorValidationService.
 *
 * Uses the pipestream-wiremock-server container as a real gRPC server (via {@link ConnectorIntakeWireMockTestResource}).
 * No WireMock client libraries and no Mockito.
 */
@QuarkusTest
@QuarkusTestResource(ConnectorIntakeWireMockTestResource.class)
class ConnectorValidationServiceTest {

    @Inject
    ConnectorValidationService connectorValidationService;

    @Inject
    CacheManager cacheManager;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        // Clear cache before each test
        cacheManager.getCache("datasource-config").orElseThrow().invalidateAll().await().indefinitely();
    }

    @Test
    void validateDataSource_success_cachesResult() {
        // Arrange
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        // Act & Assert - First call (cache miss)
        var config1 = connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely();
        assertNotNull(config1);
        assertEquals("valid-account", config1.getAccountId());

        // Act & Assert - Second call (cache hit - mocks won't be called again)
        var config2 = connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely();
        assertNotNull(config2);
        assertEquals("valid-account", config2.getAccountId());
    }

    @Test
    void validateDataSource_invalidApiKey_throwsUnauthenticated() {
        // Arrange
        String datasourceId = "valid-datasource";
        String apiKey = "invalid-api-key";

        // Act & Assert
        io.grpc.StatusRuntimeException ex = assertThrows(io.grpc.StatusRuntimeException.class, () ->
            connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely()
        );
        assertEquals(io.grpc.Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
    }

    @Test
    void validateDataSource_inactiveAccount_throwsPermissionDenied() {
        // Arrange
        String datasourceId = "inactive-account-datasource";
        String apiKey = "inactive-account-key";

        // Act & Assert
        io.grpc.StatusRuntimeException ex = assertThrows(io.grpc.StatusRuntimeException.class, () ->
            connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely()
        );
        assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    }

    @Test
    void validateDataSource_accountNotFound_throwsPermissionDenied() {
        // Arrange
        String datasourceId = "missing-account-datasource";
        String apiKey = "missing-account-key";

        // Act & Assert
        io.grpc.StatusRuntimeException ex = assertThrows(io.grpc.StatusRuntimeException.class, () ->
            connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely()
        );
        assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    }
}
