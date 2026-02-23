package ai.pipeline.connector.intake.service;

import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.grpc.Status;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConfigResolutionService.
 * Tests Tier 1 configuration resolution and IngestionConfig building.
 * Uses wiremock server as a normal gRPC service (no WireMock dependency).
 */
@QuarkusTest
@QuarkusTestResource(ConnectorIntakeWireMockTestResource.class)
class ConfigResolutionServiceTest {

    @Inject
    ConfigResolutionService configResolutionService;

    @Test
    void resolveConfig_withDefaultResponse_returnsResolvedConfig() {
        // Arrange - Use default datasource configured by wiremock server's ServiceMockInitializer
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        // Act
        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertNotNull(resolved);
        assertEquals(datasourceId, resolved.tier1Config().getDatasourceId());
        assertEquals("valid-account", resolved.tier1Config().getAccountId());
        assertNotNull(resolved.ingestionConfig());
        assertTrue(resolved.shouldPersist()); // Default behavior
    }

    @Test
    void resolveConfig_invalidApiKey_throwsException() {
        // Arrange - Use invalid API key scenario configured by wiremock server
        String datasourceId = "valid-datasource";
        String apiKey = "invalid-api-key";

        // Act & Assert
        io.grpc.StatusRuntimeException ex = assertThrows(io.grpc.StatusRuntimeException.class,
            () -> configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely());
        assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
    }

    @Test
    void resolveConfig_inactiveDatasource_throwsUnauthenticated() {
        // Arrange - inactive datasource scenario configured by wiremock server
        String datasourceId = "inactive-datasource";
        String apiKey = "inactive-key";

        // Act & Assert
        io.grpc.StatusRuntimeException ex = assertThrows(io.grpc.StatusRuntimeException.class,
            () -> configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely());
        assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
    }

    @Test
    void resolveConfig_inactiveAccount_throwsPermissionDenied() {
        // Arrange - inactive account scenario configured by wiremock server
        String datasourceId = "inactive-account-datasource";
        String apiKey = "inactive-account-key";

        // Act & Assert
        io.grpc.StatusRuntimeException ex = assertThrows(io.grpc.StatusRuntimeException.class,
            () -> configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely());
        assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    }

    @Test
    void resolveConfig_missingAccount_throwsPermissionDenied() {
        // Arrange - datasource exists but account does not
        String datasourceId = "missing-account-datasource";
        String apiKey = "missing-account-key";

        // Act & Assert
        io.grpc.StatusRuntimeException ex = assertThrows(io.grpc.StatusRuntimeException.class,
            () -> configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely());
        assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    }

    @Test
    void resolveConfig_noPersistenceConfig_defaultsToPersist() {
        // Arrange - Default response doesn't have PersistenceConfig, should default to persist
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        // Act
        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertTrue(resolved.shouldPersist()); // Default: persist (safe default)
    }

    @Test
    void resolveConfig_noHydrationConfig_usesDefaultInstance() {
        // Arrange
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        // Act
        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertNotNull(resolved.ingestionConfig());
        assertEquals(IngressMode.INGRESS_MODE_UNSPECIFIED, resolved.ingestionConfig().getIngressMode());
    }

    @Test
    void resolvedConfig_withIngressMode_createsNewIngestionConfig() {
        // Arrange
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Act
        IngestionConfig withMode = resolved.withIngressMode(IngressMode.INGRESS_MODE_GRPC_INLINE);

        // Assert
        assertEquals(IngressMode.INGRESS_MODE_GRPC_INLINE, withMode.getIngressMode());
        // Original should be unchanged
        assertEquals(IngressMode.INGRESS_MODE_UNSPECIFIED, resolved.ingestionConfig().getIngressMode());
        // Should be a new instance
        assertNotSame(resolved.ingestionConfig(), withMode);
    }

    @Test
    void resolvedConfig_defaultsRtbfFieldsToFalse() {
        // Arrange
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";

        // Act
        ConfigResolutionService.ResolvedConfig resolved =
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertFalse(resolved.ingestionConfig().getRightToBeForgotten().getDeleteSearchIndex());
        assertFalse(resolved.ingestionConfig().getRightToBeForgotten().getDeleteSourceBlobs());
    }
}
