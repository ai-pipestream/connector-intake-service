package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.connector.intake.v1.DataSourceAdminServiceGrpc;
import ai.pipestream.connector.intake.v1.ValidateApiKeyRequest;
import ai.pipestream.connector.intake.v1.ValidateApiKeyResponse;
import ai.pipestream.repository.account.v1.AccountServiceGrpc;
import ai.pipestream.repository.account.v1.GetAccountResponse;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import io.quarkus.cache.CacheResult;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Service for validating connector API keys and fetching connector configuration.
 * <p>
 * This service provides the boundary between connector-intake-service and connector-service,
 * ensuring proper microservice architecture without database coupling.
 * <p>
 * Uses stock Quarkus gRPC clients with Stork service discovery to locate
 * connector-admin and account-manager.
 */
@ApplicationScoped
public class ConnectorValidationService {

    private static final Logger LOG = Logger.getLogger(ConnectorValidationService.class);
    /**
     * Default constructor for CDI.
     */
    public ConnectorValidationService() { }

    @GrpcClient("connector-admin")
    Channel connectorAdminChannel;

    @GrpcClient("account-manager")
    Channel accountChannel;

    private DataSourceAdminServiceGrpc.DataSourceAdminServiceStub dataSourceAdminStub;
    private AccountServiceGrpc.AccountServiceStub accountStub;

    @PostConstruct
    void init() {
        this.dataSourceAdminStub = DataSourceAdminServiceGrpc.newStub(connectorAdminChannel);
        this.accountStub = AccountServiceGrpc.newStub(accountChannel);
    }

    /**
     * Validate a datasource's API key and fetch its configuration.
     * <p>
     * Behavior:
     * <ul>
     *   <li>Discovers and calls datasource-admin to validate the API key and retrieve datasource config.</li>
     *   <li>Then verifies the owning account exists and is active via account-service.</li>
     * </ul>
     * Uses standard async gRPC stubs and StreamObserver callbacks.
     * Side effects: network calls to datasource-admin and account-service.
     *
     * @param datasourceId The datasource ID
     * @param apiKey The plaintext API key to validate
     * @return the datasource config when validation succeeds
     */
    @CacheResult(cacheName = "datasource-config")
    public DataSourceConfig validateDataSource(String datasourceId, String apiKey) {
        LOG.debugf("Validating datasource: %s", datasourceId);

        ValidateApiKeyResponse response = validateApiKey(datasourceId, apiKey);
        if (!response.getValid()) {
            LOG.warnf("API key validation failed for datasource %s: %s", datasourceId, response.getMessage());
            throw Status.UNAUTHENTICATED
                    .withDescription(response.getMessage())
                    .asRuntimeException();
        }

        DataSourceConfig config = response.getConfig();
        String accountId = config.getAccountId();
        LOG.debugf("Datasource %s API key validated successfully; account=%s", datasourceId, accountId);

        if (!isAccountActive(accountId)) {
            LOG.warnf("Account validation failed for datasource %s: account %s is inactive or missing",
                    datasourceId, accountId);
            throw Status.PERMISSION_DENIED
                    .withDescription("Account is inactive or does not exist: " + accountId)
                    .asRuntimeException();
        }

        return config;
    }

    /**
     * Cached account active status lookup. Calls account-service via gRPC and
     * caches the result to avoid repeated lookups for the same account.
     */
    @CacheResult(cacheName = "account-active")
    boolean isAccountActive(String accountId) {
        LOG.debugf("Account cache miss, looking up: %s", accountId);
        try {
            boolean active = getAccount(accountId).getAccount().getActive();
            LOG.debugf("Account %s lookup result: active=%s", accountId, active);
            return active;
        } catch (StatusRuntimeException sre) {
            if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                LOG.warnf("Account not found: %s", accountId);
                return false;
            }
            logGrpcFailure("validate account " + accountId, sre);
            throw sre;
        }
    }

    private ValidateApiKeyResponse validateApiKey(String datasourceId, String apiKey) {
        ValidateApiKeyRequest request = ValidateApiKeyRequest.newBuilder()
                .setDatasourceId(datasourceId)
                .setApiKey(apiKey)
                .build();
        CompletableFuture<ValidateApiKeyResponse> future = new CompletableFuture<>();
        dataSourceAdminStub.validateApiKey(request, new StreamObserver<>() {
            @Override
            public void onNext(ValidateApiKeyResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                // unary response already completed in onNext
            }
        });
        return awaitUnary(future, "connector-admin ValidateApiKey", datasourceId, null);
    }

    private GetAccountResponse getAccount(String accountId) {
        GetAccountRequest request = GetAccountRequest.newBuilder()
                .setAccountId(accountId)
                .build();
        CompletableFuture<GetAccountResponse> future = new CompletableFuture<>();
        accountStub.getAccount(request, new StreamObserver<>() {
            @Override
            public void onNext(GetAccountResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                // unary response already completed in onNext
            }
        });
        return awaitUnary(future, "account-manager GetAccount", null, accountId);
    }

    private <T> T awaitUnary(CompletableFuture<T> future, String phase, String datasourceId, String accountId) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription(phase + " interrupted")
                    .withCause(e)
                    .asRuntimeException();
        } catch (ExecutionException e) {
            throw (StatusRuntimeException) phaseFailure(phase, datasourceId, accountId, e.getCause());
        }
    }

    private Throwable phaseFailure(String phase, String datasourceId, String accountId, Throwable throwable) {
        StatusRuntimeException sre = toStatusRuntimeException(throwable);
        Status status = sre.getStatus();
        String subject = datasourceId != null
                ? "datasource " + datasourceId
                : "account " + accountId;
        String description = status.getDescription();
        String phaseDescription = phase + " failed for " + subject
                + (description == null || description.isBlank() ? "" : ": " + description);

        StatusRuntimeException phased = status
                .withDescription(phaseDescription)
                .withCause(throwable)
                .asRuntimeException();
        logGrpcFailure(phaseDescription, phased);
        return phased;
    }

    private StatusRuntimeException toStatusRuntimeException(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException sre) {
            return sre;
        }
        return Status.UNKNOWN
                .withDescription(throwable.getMessage())
                .withCause(throwable)
                .asRuntimeException();
    }

    private void logGrpcFailure(String context, StatusRuntimeException sre) {
        // Avoid noisy stack traces for expected auth/permission failures
        var code = sre.getStatus().getCode();
        if (code == io.grpc.Status.Code.UNAUTHENTICATED
                || code == io.grpc.Status.Code.PERMISSION_DENIED
                || code == io.grpc.Status.Code.NOT_FOUND) {
            LOG.warnf("Validation failure (%s): %s: %s", code, context, sre.getStatus().getDescription());
        } else {
            LOG.errorf(sre, "Unexpected gRPC failure (%s): %s", code, context);
        }
    }
}
