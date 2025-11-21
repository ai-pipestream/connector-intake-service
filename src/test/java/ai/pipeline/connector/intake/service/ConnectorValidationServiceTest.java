package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.ConnectorConfig;
import ai.pipestream.connector.intake.ConnectorRegistration;
import ai.pipestream.connector.intake.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.ValidateApiKeyResponse;
import ai.pipestream.dynamic.grpc.client.DynamicGrpcClientFactory;
import ai.pipestream.repository.account.Account;
import ai.pipestream.repository.account.GetAccountRequest;
import ai.pipestream.repository.account.MutinyAccountServiceGrpc;
import io.grpc.Status;
import io.quarkus.cache.CacheManager;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.InjectMock;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@QuarkusTest
class ConnectorValidationServiceTest {

    @Inject
    ConnectorValidationService connectorValidationService;

    @InjectMock
    DynamicGrpcClientFactory mockGrpcClientFactory;
    
    MutinyAccountServiceGrpc.MutinyAccountServiceStub mockAccountServiceStub;
    ai.pipestream.connector.intake.MutinyConnectorAdminServiceGrpc.MutinyConnectorAdminServiceStub mockConnectorAdminServiceStub;

    @Inject
    CacheManager cacheManager;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        // Initialize mocks for stubs (they are not beans, so we use simple Mockito.mock)
        mockAccountServiceStub = mock(MutinyAccountServiceGrpc.MutinyAccountServiceStub.class);
        mockConnectorAdminServiceStub = mock(ai.pipestream.connector.intake.MutinyConnectorAdminServiceGrpc.MutinyConnectorAdminServiceStub.class);

        // Clear cache before each test
        cacheManager.getCache("connector-config").orElseThrow().invalidateAll().await().indefinitely();

        // Mock DynamicGrpcClientFactory to return our stubs
        when(mockGrpcClientFactory.getAccountServiceClient(anyString())).thenReturn(Uni.createFrom().item(mockAccountServiceStub));
        when(mockGrpcClientFactory.getConnectorAdminServiceClient(anyString())).thenReturn(Uni.createFrom().item(mockConnectorAdminServiceStub));
    }

    @Test
    void validateConnector_success_cachesResult() {
        // Arrange
        String connectorId = "test-connector-id";
        String apiKey = "test-api-key";
        String accountId = "test-account-id";

        ConnectorRegistration mockRegistration = ConnectorRegistration.newBuilder()
                .setConnectorId(connectorId)
                .setAccountId(accountId)
                .setS3Bucket("test-bucket")
                .setS3BasePath("test-path")
                .build();

        ValidateApiKeyResponse mockValidateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConnector(mockRegistration)
                .build();

        Account mockAccount = Account.newBuilder()
                .setAccountId(accountId)
                .setActive(true)
                .build();

        when(mockConnectorAdminServiceStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(mockValidateResponse));
        when(mockAccountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().item(mockAccount));

        // Act & Assert - First call (cache miss)
        ConnectorConfig config1 = connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        assertNotNull(config1);
        assertEquals(accountId, config1.getAccountId());
        verify(mockConnectorAdminServiceStub, times(1)).validateApiKey(any(ValidateApiKeyRequest.class));
        verify(mockAccountServiceStub, times(1)).getAccount(any(GetAccountRequest.class));

        // Act & Assert - Second call (cache hit)
        ConnectorConfig config2 = connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        assertNotNull(config2);
        assertEquals(accountId, config2.getAccountId());
        // Verify no more interactions with external services for the second call
        verify(mockConnectorAdminServiceStub, times(1)).validateApiKey(any(ValidateApiKeyRequest.class));
        verify(mockAccountServiceStub, times(1)).getAccount(any(GetAccountRequest.class));
    }

    @Test
    void validateConnector_invalidApiKey_throwsUnauthenticated() {
        // Arrange
        String connectorId = "test-connector-id";
        String apiKey = "invalid-api-key";

        ValidateApiKeyResponse mockValidateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(false)
                .setMessage("Invalid API Key")
                .build();

        when(mockConnectorAdminServiceStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(mockValidateResponse));

        // Act & Assert
        assertThrows(Status.UNAUTHENTICATED.asRuntimeException().getClass(), () -> {
            connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        });
        verify(mockConnectorAdminServiceStub, times(1)).validateApiKey(any(ValidateApiKeyRequest.class));
        verify(mockAccountServiceStub, never()).getAccount(any(GetAccountRequest.class)); // Account service should not be called
    }

    @Test
    void validateConnector_inactiveAccount_throwsPermissionDenied() {
        // Arrange
        String connectorId = "test-connector-id";
        String apiKey = "test-api-key";
        String accountId = "test-account-id";

        ConnectorRegistration mockRegistration = ConnectorRegistration.newBuilder()
                .setConnectorId(connectorId)
                .setAccountId(accountId)
                .build();

        ValidateApiKeyResponse mockValidateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConnector(mockRegistration)
                .build();

        Account mockAccount = Account.newBuilder()
                .setAccountId(accountId)
                .setActive(false) // Inactive account
                .build();

        when(mockConnectorAdminServiceStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(mockValidateResponse));
        when(mockAccountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().item(mockAccount));

        // Act & Assert
        assertThrows(Status.PERMISSION_DENIED.asRuntimeException().getClass(), () -> {
            connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        });
        verify(mockConnectorAdminServiceStub, times(1)).validateApiKey(any(ValidateApiKeyRequest.class));
        verify(mockAccountServiceStub, times(1)).getAccount(any(GetAccountRequest.class));
    }

    @Test
    void validateConnector_accountNotFound_throwsPermissionDenied() {
        // Arrange
        String connectorId = "test-connector-id";
        String apiKey = "test-api-key";
        String accountId = "test-account-id";

        ConnectorRegistration mockRegistration = ConnectorRegistration.newBuilder()
                .setConnectorId(connectorId)
                .setAccountId(accountId)
                .build();

        ValidateApiKeyResponse mockValidateResponse = ValidateApiKeyResponse.newBuilder()
                .setValid(true)
                .setConnector(mockRegistration)
                .build();

        when(mockConnectorAdminServiceStub.validateApiKey(any(ValidateApiKeyRequest.class)))
                .thenReturn(Uni.createFrom().item(mockValidateResponse));
        when(mockAccountServiceStub.getAccount(any(GetAccountRequest.class)))
                .thenReturn(Uni.createFrom().failure(Status.NOT_FOUND.asRuntimeException()));

        // Act & Assert
        assertThrows(Status.PERMISSION_DENIED.asRuntimeException().getClass(), () -> {
            connectorValidationService.validateConnector(connectorId, apiKey).await().indefinitely();
        });
        verify(mockConnectorAdminServiceStub, times(1)).validateApiKey(any(ValidateApiKeyRequest.class));
        verify(mockAccountServiceStub, times(1)).getAccount(any(GetAccountRequest.class));
    }
}
