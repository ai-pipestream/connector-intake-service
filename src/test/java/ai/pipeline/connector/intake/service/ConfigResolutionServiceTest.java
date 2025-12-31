package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.data.v1.HydrationConfig;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.repository.account.v1.Account;
import ai.pipestream.wiremock.client.AccountManagerMock;
import ai.pipestream.wiremock.client.DataSourceAdminMock;
import ai.pipeline.connector.intake.WireMockTestResource;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ConfigResolutionService.
 * Tests Tier 1 configuration resolution and IngestionConfig building.
 * Uses real services with wiremock for external dependencies.
 */
@QuarkusTest
@QuarkusTestResource(WireMockTestResource.class)
class ConfigResolutionServiceTest {

    @Inject
    ConfigResolutionService configResolutionService;

    private DataSourceAdminMock datasourceAdminMock;
    private AccountManagerMock accountManagerMock;
    private WireMock wireMock;

    @BeforeEach
    void setUp() {
        // Get wiremock client connected to the running wiremock server
        String wiremockHost = System.getProperty("wiremock.host", "localhost");
        int wiremockPort = Integer.parseInt(System.getProperty("wiremock.port", "8080"));
        wireMock = new WireMock(wiremockHost, wiremockPort);
        
        datasourceAdminMock = new DataSourceAdminMock(wireMock);
        accountManagerMock = new AccountManagerMock(wireMock);
        
        // Configure default account responses
        accountManagerMock.mockGetAccount("test-account", Account.newBuilder()
            .setAccountId("test-account")
            .setName("Test Account")
            .setActive(true)
            .build());
    }

    @Test
    void resolveConfig_withPersistenceConfig_returnsResolvedConfig() {
        // Arrange
        String datasourceId = "test-datasource-persist";
        String apiKey = "test-key";

        DataSourceConfig tier1Config = DataSourceConfig.newBuilder()
            .setDatasourceId(datasourceId)
            .setAccountId("test-account")
            .setConnectorId("test-connector")
            .setDriveName("test-drive")
            .setGlobalConfig(
                DataSourceConfig.ConnectorGlobalConfig.newBuilder()
                    .setPersistenceConfig(
                        DataSourceConfig.PersistenceConfig.newBuilder()
                            .setPersistPipedoc(true)
                            .setMaxInlineSizeBytes(1048576)
                            .build()
                    )
                    .build()
            )
            .build();

        // Configure wiremock to return full config
        datasourceAdminMock.mockValidateApiKeyWithConfig(datasourceId, apiKey, tier1Config);

        // Act
        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertNotNull(resolved);
        assertEquals(datasourceId, resolved.tier1Config().getDatasourceId());
        assertTrue(resolved.tier1Config().hasGlobalConfig());
        assertTrue(resolved.tier1Config().getGlobalConfig().hasPersistenceConfig());
        assertEquals(true, resolved.tier1Config().getGlobalConfig().getPersistenceConfig().getPersistPipedoc());
        assertNotNull(resolved.ingestionConfig());
        assertTrue(resolved.shouldPersist());
    }

    @Test
    void resolveConfig_persistPipedocFalse_shouldPersistReturnsFalse() {
        // Arrange
        String datasourceId = "test-datasource-no-persist";
        String apiKey = "test-key";

        DataSourceConfig tier1Config = DataSourceConfig.newBuilder()
            .setDatasourceId(datasourceId)
            .setAccountId("test-account")
            .setConnectorId("test-connector")
            .setDriveName("test-drive")
            .setGlobalConfig(
                DataSourceConfig.ConnectorGlobalConfig.newBuilder()
                    .setPersistenceConfig(
                        DataSourceConfig.PersistenceConfig.newBuilder()
                            .setPersistPipedoc(false)
                            .build()
                    )
                    .build()
            )
            .build();

        datasourceAdminMock.mockValidateApiKeyWithConfig(datasourceId, apiKey, tier1Config);

        // Act
        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertFalse(resolved.shouldPersist());
    }

    @Test
    void resolveConfig_noPersistenceConfig_defaultsToPersist() {
        // Arrange
        String datasourceId = "test-datasource-default";
        String apiKey = "test-key";

        // Use default mock which doesn't have PersistenceConfig
        datasourceAdminMock.mockValidateApiKey(datasourceId, apiKey, "test-account", "test-connector", "test-drive");

        // Act
        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertTrue(resolved.shouldPersist()); // Default: persist (safe default)
    }

    @Test
    void resolveConfig_withHydrationConfig_includesInIngestionConfig() {
        // Arrange
        String datasourceId = "test-datasource-hydration";
        String apiKey = "test-key";

        HydrationConfig hydrationConfig = HydrationConfig.newBuilder()
            .setDefaultHydrationPolicy(HydrationConfig.HydrationPolicy.HYDRATION_POLICY_ALWAYS_REF)
            .setMaxInlineBlobSizeBytes(5242880)
            .build();

        DataSourceConfig tier1Config = DataSourceConfig.newBuilder()
            .setDatasourceId(datasourceId)
            .setAccountId("test-account")
            .setConnectorId("test-connector")
            .setDriveName("test-drive")
            .setGlobalConfig(
                DataSourceConfig.ConnectorGlobalConfig.newBuilder()
                    .setHydrationConfig(hydrationConfig)
                    .build()
            )
            .build();

        datasourceAdminMock.mockValidateApiKeyWithConfig(datasourceId, apiKey, tier1Config);

        // Act
        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertNotNull(resolved.ingestionConfig());
        assertTrue(resolved.ingestionConfig().hasHydrationConfig());
        assertEquals(
            HydrationConfig.HydrationPolicy.HYDRATION_POLICY_ALWAYS_REF,
            resolved.ingestionConfig().getHydrationConfig().getDefaultHydrationPolicy()
        );
        assertEquals(5242880, resolved.ingestionConfig().getHydrationConfig().getMaxInlineBlobSizeBytes());
    }

    @Test
    void resolveConfig_noHydrationConfig_usesDefaultInstance() {
        // Arrange
        String datasourceId = "test-datasource-no-hydration";
        String apiKey = "test-key";

        // Use default mock
        datasourceAdminMock.mockValidateApiKey(datasourceId, apiKey, "test-account", "test-connector", "test-drive");

        // Act
        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Assert
        assertNotNull(resolved.ingestionConfig());
        // Should have default hydration config (even if empty)
        assertEquals(IngressMode.INGRESS_MODE_UNSPECIFIED, resolved.ingestionConfig().getIngressMode());
    }

    @Test
    void resolvedConfig_withIngressMode_createsNewIngestionConfig() {
        // Arrange
        String datasourceId = "test-datasource";
        String apiKey = "test-key";

        datasourceAdminMock.mockValidateApiKey(datasourceId, apiKey, "test-account", "test-connector", "test-drive");

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
    void resolvedConfig_withIngressMode_preservesOtherFields() {
        // Arrange
        String datasourceId = "test-datasource";
        String apiKey = "test-key";

        HydrationConfig hydrationConfig = HydrationConfig.newBuilder()
            .setDefaultHydrationPolicy(HydrationConfig.HydrationPolicy.HYDRATION_POLICY_AUTO)
            .build();

        DataSourceConfig tier1Config = DataSourceConfig.newBuilder()
            .setDatasourceId(datasourceId)
            .setAccountId("test-account")
            .setConnectorId("test-connector")
            .setDriveName("test-drive")
            .setGlobalConfig(
                DataSourceConfig.ConnectorGlobalConfig.newBuilder()
                    .setHydrationConfig(hydrationConfig)
                    .build()
            )
            .build();

        datasourceAdminMock.mockValidateApiKeyWithConfig(datasourceId, apiKey, tier1Config);

        ConfigResolutionService.ResolvedConfig resolved = 
            configResolutionService.resolveConfig(datasourceId, apiKey).await().indefinitely();

        // Act
        IngestionConfig withMode = resolved.withIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED);

        // Assert
        assertEquals(IngressMode.INGRESS_MODE_HTTP_STAGED, withMode.getIngressMode());
        // Hydration config should be preserved
        assertTrue(withMode.hasHydrationConfig());
        assertEquals(
            HydrationConfig.HydrationPolicy.HYDRATION_POLICY_AUTO,
            withMode.getHydrationConfig().getDefaultHydrationPolicy()
        );
    }
}
