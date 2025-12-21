package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyResponse;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.account.v1.Account;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import ai.pipestream.repository.account.v1.GetAccountResponse;
import ai.pipestream.repository.account.v1.MutinyAccountServiceGrpc;
import io.grpc.Status;
import io.quarkus.cache.CacheManager;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ConnectorValidationService.
 * Uses mocked DynamicGrpcClientFactory to simulate gRPC service responses.
 */
@QuarkusTest
class ConnectorValidationServiceTest {

    @Inject
    ConnectorValidationService connectorValidationService;

    @Inject
    CacheManager cacheManager;

    @InjectMock
    DynamicGrpcClientFactory grpcClientFactory;

    private MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub datasourceAdminStub;
    private MutinyAccountServiceGrpc.MutinyAccountServiceStub accountServiceStub;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        // Clear cache before each test
        cacheManager.getCache("datasource-config").orElseThrow().invalidateAll().await().indefinitely();

        // Create mock stubs
        datasourceAdminStub = mock(MutinyDataSourceAdminServiceGrpc.MutinyDataSourceAdminServiceStub.class);
        accountServiceStub = mock(MutinyAccountServiceGrpc.MutinyAccountServiceStub.class);

        // Configure factory to return mock stubs
        when(grpcClientFactory.getClient(eq("datasource-admin"), any()))
                .thenReturn(Uni.createFrom().item(datasourceAdminStub));
        when(grpcClientFactory.getClient(eq("account-manager"), any()))
                .thenReturn(Uni.createFrom().item(accountServiceStub));
    }

    @Test
    void validateDataSource_success_cachesResult() {
        // Arrange
        String datasourceId = "valid-datasource";
        String apiKey = "valid-api-key";
        String accountId = "valid-account";

        DataSourceConfig config = DataSourceConfig.newBuilder()
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .setConnectorId("s3")
                .setDriveName("test-drive")
                .setMaxFileSize(1000000)
                .setRateLimitPerMinute(100)
                .build();

        ValidateApiKeyResponse validateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConfig(config)
                .build();

        Account account = Account.newBuilder()
                .setAccountId(accountId)
                .setName("Test Account")
                .setActive(true)
                .build();

        GetAccountResponse accountResponse = GetAccountResponse.newBuilder()
                .setAccount(account)
                .build();

        when(datasourceAdminStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(validateResponse));
        when(accountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().item(accountResponse));

        // Act & Assert - First call (cache miss)
        DataSourceConfig config1 = connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely();
        assertNotNull(config1);
        assertEquals(accountId, config1.getAccountId());

        // Act & Assert - Second call (cache hit - mocks won't be called again)
        DataSourceConfig config2 = connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely();
        assertNotNull(config2);
        assertEquals(accountId, config2.getAccountId());
    }

    @Test
    void validateDataSource_invalidApiKey_throwsUnauthenticated() {
        // Arrange
        String datasourceId = "valid-datasource";
        String apiKey = "invalid-api-key";

        ValidateApiKeyResponse validateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(false)
                .setMessage("Invalid API key")
                .build();

        when(datasourceAdminStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(validateResponse));

        // Act & Assert
        io.grpc.StatusRuntimeException exception = assertThrows(io.grpc.StatusRuntimeException.class, () -> {
            connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely();
        });
        assertEquals(Status.Code.UNAUTHENTICATED, exception.getStatus().getCode());
    }

    @Test
    void validateDataSource_inactiveAccount_throwsPermissionDenied() {
        // Arrange
        String datasourceId = "inactive-account-datasource";
        String apiKey = "inactive-account-key";
        String accountId = "inactive-account";

        DataSourceConfig config = DataSourceConfig.newBuilder()
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .build();

        ValidateApiKeyResponse validateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConfig(config)
                .build();

        Account account = Account.newBuilder()
                .setAccountId(accountId)
                .setName("Inactive Account")
                .setActive(false)  // Inactive
                .build();

        GetAccountResponse accountResponse = GetAccountResponse.newBuilder()
                .setAccount(account)
                .build();

        when(datasourceAdminStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(validateResponse));
        when(accountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().item(accountResponse));

        // Act & Assert
        io.grpc.StatusRuntimeException exception = assertThrows(io.grpc.StatusRuntimeException.class, () -> {
            connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely();
        });
        assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    }

    @Test
    void validateDataSource_accountNotFound_throwsPermissionDenied() {
        // Arrange
        String datasourceId = "missing-account-datasource";
        String apiKey = "missing-account-key";
        String accountId = "nonexistent-account";

        DataSourceConfig config = DataSourceConfig.newBuilder()
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .build();

        ValidateApiKeyResponse validateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConfig(config)
                .build();

        when(datasourceAdminStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(validateResponse));
        when(accountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().failure(
                        Status.NOT_FOUND.withDescription("Account not found").asRuntimeException()));

        // Act & Assert
        io.grpc.StatusRuntimeException exception = assertThrows(io.grpc.StatusRuntimeException.class, () -> {
            connectorValidationService.validateDataSource(datasourceId, apiKey).await().indefinitely();
        });
        assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    }
}
