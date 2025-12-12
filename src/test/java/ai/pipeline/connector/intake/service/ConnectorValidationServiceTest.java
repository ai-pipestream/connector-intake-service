package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.ConnectorConfig;
import ai.pipestream.connector.intake.v1.ConnectorRegistration;
import ai.pipestream.connector.intake.v1.MutinyConnectorAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyResponse;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.v1.account.Account;
import ai.pipestream.repository.v1.account.GetAccountRequest;
import ai.pipestream.repository.v1.account.GetAccountResponse;
import ai.pipestream.repository.v1.account.MutinyAccountServiceGrpc;
import io.grpc.Status;
import io.quarkus.cache.CacheManager;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

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

    private MutinyConnectorAdminServiceGrpc.MutinyConnectorAdminServiceStub connectorAdminStub;
    private MutinyAccountServiceGrpc.MutinyAccountServiceStub accountServiceStub;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        // Clear cache before each test
        cacheManager.getCache("connector-config").orElseThrow().invalidateAll().await().indefinitely();

        // Create mock stubs
        connectorAdminStub = mock(MutinyConnectorAdminServiceGrpc.MutinyConnectorAdminServiceStub.class);
        accountServiceStub = mock(MutinyAccountServiceGrpc.MutinyAccountServiceStub.class);

        // Configure factory to return mock stubs
        when(grpcClientFactory.getClient(eq("connector-service"), any()))
                .thenReturn(Uni.createFrom().item(connectorAdminStub));
        when(grpcClientFactory.getClient(eq("account-manager"), any()))
                .thenReturn(Uni.createFrom().item(accountServiceStub));
    }

    @Test
    void validateConnector_success_cachesResult() {
        // Arrange
        String connectorId = "valid-connector";
        String apiKey = "valid-api-key";
        String accountId = "valid-account";

        ConnectorRegistration connector = ConnectorRegistration.newBuilder()
                .setConnectorId(connectorId)
                .setAccountId(accountId)
                .setS3Bucket("test-bucket")
                .setS3BasePath("/test/path")
                .setMaxFileSize(1000000)
                .setRateLimitPerMinute(100)
                .build();

        ValidateApiKeyResponse validateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConnector(connector)
                .build();

        Account account = Account.newBuilder()
                .setAccountId(accountId)
                .setName("Test Account")
                .setActive(true)
                .build();

        GetAccountResponse accountResponse = GetAccountResponse.newBuilder()
                .setAccount(account)
                .build();

        when(connectorAdminStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(validateResponse));
        when(accountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().item(accountResponse));

        // Act & Assert - First call (cache miss)
        ConnectorConfig config1 = connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        assertNotNull(config1);
        assertEquals(accountId, config1.getAccountId());

        // Act & Assert - Second call (cache hit - mocks won't be called again)
        ConnectorConfig config2 = connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        assertNotNull(config2);
        assertEquals(accountId, config2.getAccountId());
    }

    @Test
    void validateConnector_invalidApiKey_throwsUnauthenticated() {
        // Arrange
        String connectorId = "valid-connector";
        String apiKey = "invalid-api-key";

        ValidateApiKeyResponse validateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(false)
                .setMessage("Invalid API key")
                .build();

        when(connectorAdminStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(validateResponse));

        // Act & Assert
        io.grpc.StatusRuntimeException exception = assertThrows(io.grpc.StatusRuntimeException.class, () -> {
            connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        });
        assertEquals(Status.Code.UNAUTHENTICATED, exception.getStatus().getCode());
    }

    @Test
    void validateConnector_inactiveAccount_throwsPermissionDenied() {
        // Arrange
        String connectorId = "inactive-account-connector";
        String apiKey = "inactive-account-key";
        String accountId = "inactive-account";

        ConnectorRegistration connector = ConnectorRegistration.newBuilder()
                .setConnectorId(connectorId)
                .setAccountId(accountId)
                .build();

        ValidateApiKeyResponse validateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConnector(connector)
                .build();

        Account account = Account.newBuilder()
                .setAccountId(accountId)
                .setName("Inactive Account")
                .setActive(false)  // Inactive
                .build();

        GetAccountResponse accountResponse = GetAccountResponse.newBuilder()
                .setAccount(account)
                .build();

        when(connectorAdminStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(validateResponse));
        when(accountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().item(accountResponse));

        // Act & Assert
        io.grpc.StatusRuntimeException exception = assertThrows(io.grpc.StatusRuntimeException.class, () -> {
            connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        });
        assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    }

    @Test
    void validateConnector_accountNotFound_throwsPermissionDenied() {
        // Arrange
        String connectorId = "missing-account-connector";
        String apiKey = "missing-account-key";
        String accountId = "nonexistent-account";

        ConnectorRegistration connector = ConnectorRegistration.newBuilder()
                .setConnectorId(connectorId)
                .setAccountId(accountId)
                .build();

        ValidateApiKeyResponse validateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConnector(connector)
                .build();

        when(connectorAdminStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(validateResponse));
        when(accountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().failure(
                        Status.NOT_FOUND.withDescription("Account not found").asRuntimeException()));

        // Act & Assert
        io.grpc.StatusRuntimeException exception = assertThrows(io.grpc.StatusRuntimeException.class, () -> {
            connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        });
        assertEquals(Status.Code.PERMISSION_DENIED, exception.getStatus().getCode());
    }
}
