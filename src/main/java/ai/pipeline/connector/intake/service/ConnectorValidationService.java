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
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletableFuture;

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
     * Reactive semantics:
     * <ul>
     *   <li>Returns a {@code Uni<DataSourceConfig>} that emits on completion of two remote gRPC calls.</li>
     *   <li>Authentication failures are mapped to {@code io.grpc.StatusRuntimeException} with {@code UNAUTHENTICATED}.</li>
     *   <li>Missing or inactive accounts are mapped to {@code PERMISSION_DENIED}.</li>
     * </ul>
     * Side effects: network calls to datasource-admin and account-service.
     *
     * @param datasourceId The datasource ID
     * @param apiKey The plaintext API key to validate
     * @return a {@code Uni} emitting {@code DataSourceConfig} when validation succeeds
     */
    @CacheResult(cacheName = "datasource-config")
    public Uni<DataSourceConfig> validateDataSource(String datasourceId, String apiKey) {
        LOG.debugf("Validating datasource: %s", datasourceId);

        return validateApiKey(datasourceId, apiKey)
            .flatMap(response -> {
                if (!response.getValid()) {
                    LOG.warnf("API key validation failed for datasource %s: %s", datasourceId, response.getMessage());
                    return Uni.createFrom().failure(
                        Status.UNAUTHENTICATED
                            .withDescription(response.getMessage())
                            .asRuntimeException()
                    );
                }

                DataSourceConfig config = response.getConfig();
                String accountId = config.getAccountId();
                LOG.debugf("Datasource %s API key validated successfully; account=%s", datasourceId, accountId);

                // Validate account is active
                return validateAccountActive(accountId, datasourceId)
                    .replaceWith(config);
            });
    }

    /**
     * Validate that an account exists and is active.
     */
    private Uni<Void> validateAccountActive(String accountId, String datasourceId) {
        return isAccountActive(accountId)
            .flatMap(active -> {
                if (!active) {
                    LOG.warnf("Account validation failed for datasource %s: account %s is inactive or missing",
                            datasourceId, accountId);
                    return Uni.createFrom().failure(
                        Status.PERMISSION_DENIED
                            .withDescription("Account is inactive or does not exist: " + accountId)
                            .asRuntimeException()
                    );
                }
                return Uni.createFrom().voidItem();
            });
    }

    /**
     * Cached account active status lookup. Calls account-service via gRPC and
     * caches the result to avoid repeated lookups for the same account.
     */
    @CacheResult(cacheName = "account-active")
    Uni<Boolean> isAccountActive(String accountId) {
        LOG.debugf("Account cache miss, looking up: %s", accountId);

        return getAccount(accountId)
            .map(response -> {
                boolean active = response.getAccount().getActive();
                LOG.debugf("Account %s lookup result: active=%s", accountId, active);
                return active;
            })
            .onFailure(StatusRuntimeException.class)
            .recoverWithUni(throwable -> {
                StatusRuntimeException sre = (StatusRuntimeException) throwable;
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                    return Uni.createFrom().item(false);
                } else {
                    logGrpcFailure("validate account " + accountId, sre);
                    return Uni.createFrom().failure(sre);
                }
            });
    }

    private Uni<ValidateApiKeyResponse> validateApiKey(String datasourceId, String apiKey) {
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
        return Uni.createFrom().completionStage(future)
                .onFailure().transform(throwable ->
                        phaseFailure("connector-admin ValidateApiKey", datasourceId, null, throwable));
    }

    private Uni<GetAccountResponse> getAccount(String accountId) {
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
        return Uni.createFrom().completionStage(future)
                .onFailure().transform(throwable ->
                        phaseFailure("account-manager GetAccount", null, accountId, throwable));
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
