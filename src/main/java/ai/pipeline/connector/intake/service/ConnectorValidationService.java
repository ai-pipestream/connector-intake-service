package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.ConnectorConfig;
import io.grpc.Status;
import ai.pipestream.connector.intake.ConnectorRegistration;
import ai.pipestream.dynamic.grpc.client.DynamicGrpcClientFactory;
import ai.pipestream.repository.account.GetAccountRequest;
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
    private static final String CONNECTOR_SERVICE_NAME = "connector-service";
    private static final String ACCOUNT_SERVICE_NAME = "account-manager";

    /**
     * Default constructor for CDI.
     */
    public ConnectorValidationService() { }

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /**
     * Validate a connector's API key and fetch its configuration.
     * <p>
     * Behavior:
     * <ul>
     *   <li>Discovers and calls connector-service to validate the API key and retrieve connector metadata.</li>
     *   <li>Then verifies the owning account exists and is active via account-service.</li>
     * </ul>
     * Reactive semantics:
     * <ul>
     *   <li>Returns a {@code Uni<ConnectorConfig>} that emits on completion of two remote gRPC calls.</li>
     *   <li>Authentication failures are mapped to {@code io.grpc.StatusRuntimeException} with {@code UNAUTHENTICATED}.</li>
     *   <li>Missing or inactive accounts are mapped to {@code PERMISSION_DENIED}.</li>
     * </ul>
     * Side effects: network calls to connector-service and account-service.
     *
     * @param connectorId The connector ID
     * @param apiKey The plaintext API key to validate
     * @return a {@code Uni} emitting {@code ConnectorConfig} when validation succeeds
     */
    @CacheResult(cacheName = "connector-config")
    public Uni<ConnectorConfig> validateConnector(String connectorId, String apiKey) {
        LOG.debugf("Validating connector: %s", connectorId);

        return grpcClientFactory.getConnectorAdminServiceClient(CONNECTOR_SERVICE_NAME)
            .flatMap(stub -> stub.validateApiKey(
                ai.pipestream.connector.intake.ValidateApiKeyRequest.newBuilder()
                    .setConnectorId(connectorId)
                    .setApiKey(apiKey)
                    .build()
            ))
            .flatMap(response -> {
                if (!response.getValid()) {
                    LOG.warnf("API key validation failed for connector %s: %s", connectorId, response.getMessage());
                    return Uni.createFrom().failure(
                        Status.UNAUTHENTICATED
                            .withDescription(response.getMessage())
                            .asRuntimeException()
                    );
                }

                ConnectorRegistration connector = response.getConnector();
                LOG.debugf("Connector %s validated successfully", connectorId);

                // Validate account is active
                return validateAccountActive(connector.getAccountId())
                    .replaceWith(connector);
            })
            .map(this::toConnectorConfig)
            .onFailure(io.grpc.StatusRuntimeException.class)
            .transform(throwable -> {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                LOG.errorf(sre, "Failed to validate connector %s", connectorId);
                return sre;
            });
    }

    /**
     * Validate that an account exists and is active.
     */
    private Uni<Void> validateAccountActive(String accountId) {
        LOG.debugf("Validating account is active: %s", accountId);

        return grpcClientFactory.getAccountServiceClient(ACCOUNT_SERVICE_NAME)
            .flatMap(stub -> stub.getAccount(
                GetAccountRequest.newBuilder()
                    .setAccountId(accountId)
                    .build()
            ))
            .flatMap(account -> {
                if (!account.getActive()) {
                    LOG.warnf("Account %s exists but is inactive", accountId);
                    return Uni.createFrom().failure(
                        Status.PERMISSION_DENIED
                            .withDescription("Account is inactive: " + accountId)
                            .asRuntimeException()
                    );
                }
                LOG.debugf("Account %s validated successfully", accountId);
                return Uni.createFrom().voidItem();
            })
            .onFailure(io.grpc.StatusRuntimeException.class)
            .transform(throwable -> {
                io.grpc.StatusRuntimeException sre = (io.grpc.StatusRuntimeException) throwable;
                if (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND) {
                    LOG.warnf("Account not found: %s", accountId);
                    return Status.PERMISSION_DENIED
                        .withDescription("Account does not exist: " + accountId)
                        .asRuntimeException();
                }
                LOG.errorf(sre, "Failed to validate account %s", accountId);
                return sre;
            });
    }

    /**
     * Convert ConnectorRegistration proto to ConnectorConfig.
     */
    private ConnectorConfig toConnectorConfig(ConnectorRegistration connector) {
        return ConnectorConfig.newBuilder()
            .setAccountId(connector.getAccountId())
            .setS3Bucket(connector.getS3Bucket())
            .setS3BasePath(connector.getS3BasePath())
            .setMaxFileSize(connector.getMaxFileSize())
            .setRateLimitPerMinute(connector.getRateLimitPerMinute())
            .putAllDefaultMetadata(connector.getDefaultMetadataMap())
            .build();
    }
}
