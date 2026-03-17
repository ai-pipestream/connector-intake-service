package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DataSourceConfig;
import io.grpc.Status;
import ai.pipestream.connector.intake.v1.DataSource;
import ai.pipestream.connector.intake.v1.MutinyDataSourceAdminServiceGrpc;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.account.v1.GetAccountRequest;
import ai.pipestream.repository.account.v1.MutinyAccountServiceGrpc;
import io.quarkus.cache.CacheResult;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Service for validating connector API keys and fetching connector configuration.
 * <p>
 * This service provides the boundary between connector-intake-service and connector-service,
 * ensuring proper microservice architecture without database coupling.
 * <p>
 * Uses dynamic-grpc with Stork service discovery to locate connector-service.
 */
@ApplicationScoped
public class ConnectorValidationService {

    private static final Logger LOG = Logger.getLogger(ConnectorValidationService.class);
    private static final String DATASOURCE_SERVICE_NAME = "connector-admin";
    private static final String ACCOUNT_SERVICE_NAME = "account-manager";

    /**
     * Default constructor for CDI.
     */
    public ConnectorValidationService() { }

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

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

        return grpcClientFactory.getClient(DATASOURCE_SERVICE_NAME, MutinyDataSourceAdminServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.validateApiKey(
                ai.pipestream.connector.intake.v1.ValidateApiKeyRequest.newBuilder()
                    .setDatasourceId(datasourceId)
                    .setApiKey(apiKey)
                    .build()
            ))
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
                LOG.debugf("Datasource %s validated successfully", datasourceId);

                // Validate account is active
                return validateAccountActive(config.getAccountId())
                    .replaceWith(config);
            })
            .onFailure(io.grpc.StatusRuntimeException.class)
            .transform(throwable -> {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                logGrpcFailure("validate datasource " + datasourceId, sre);
                return sre;
            });
    }

    /**
     * Validate that an account exists and is active.
     */
    private Uni<Void> validateAccountActive(String accountId) {
        return isAccountActive(accountId)
            .flatMap(active -> {
                if (!active) {
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

        return grpcClientFactory.getClient(ACCOUNT_SERVICE_NAME, MutinyAccountServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.getAccount(
                GetAccountRequest.newBuilder()
                    .setAccountId(accountId)
                    .build()
            ))
            .map(response -> {
                boolean active = response.getAccount().getActive();
                LOG.debugf("Account %s lookup result: active=%s", accountId, active);
                return active;
            })
            .onFailure(io.grpc.StatusRuntimeException.class)
            .recoverWithItem(throwable -> {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                } else {
                    logGrpcFailure("validate account " + accountId, sre);
                }
                return false;
            });
    }

    private void logGrpcFailure(String context, io.grpc.StatusRuntimeException sre) {
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
