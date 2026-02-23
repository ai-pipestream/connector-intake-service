package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.data.v1.HydrationConfig;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.data.v1.RightToBeForgottenConfig;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Service for resolving Tier 1 configuration for a datasource.
 * <p>
 * Intake is graph-agnostic. It only handles Tier 1 (service-level) configuration
 * from datasource-admin. Tier 2 (graph-level) configuration is resolved by the
 * engine during IntakeHandoff - this keeps intake simple and decoupled from
 * graph internals.
 * <p>
 * Design rationale:
 * <ul>
 *   <li>Intake validates API keys and optionally persists documents</li>
 *   <li>Engine knows about graphs, routing, and Tier 2 overrides</li>
 *   <li>Separation of concerns: intake doesn't need graph knowledge</li>
 * </ul>
 */
@ApplicationScoped
public class ConfigResolutionService {

    private static final Logger LOG = Logger.getLogger(ConfigResolutionService.class);

    @Inject
    ConnectorValidationService validationService;

    /**
     * Default constructor for CDI.
     */
    public ConfigResolutionService() {}

    /**
     * Resolve Tier 1 configuration for a datasource.
     * <p>
     * Validates the datasource API key and returns the service-level config
     * from datasource-admin. Engine will handle Tier 2 (graph-level) config
     * resolution during IntakeHandoff.
     *
     * @param datasourceId The datasource ID
     * @param apiKey The API key for validation
     * @return Uni emitting ResolvedConfig containing Tier 1 config
     */
    public Uni<ResolvedConfig> resolveConfig(String datasourceId, String apiKey) {
        LOG.debugf("Resolving Tier 1 config for datasource: %s", datasourceId);

        return validationService.validateDataSource(datasourceId, apiKey)
            .map(tier1Config -> {
                LOG.debugf("Got Tier 1 config for datasource %s", datasourceId);
                return buildResolvedConfig(tier1Config);
            });
    }

    /**
     * Build ResolvedConfig from Tier 1 config only.
     * <p>
     * Extracts Tier 1 config from DataSourceConfig for use in IngestionConfig.
     * Engine will merge Tier 2 overrides during processing.
     */
    private ResolvedConfig buildResolvedConfig(DataSourceConfig tier1Config) {
        // Extract hydration config from Tier 1 (if present)
        HydrationConfig hydrationConfig = (tier1Config.hasGlobalConfig() &&
            tier1Config.getGlobalConfig().hasHydrationConfig())
                ? tier1Config.getGlobalConfig().getHydrationConfig()
                : HydrationConfig.getDefaultInstance();

        // Extract RTBF config from Tier 1 (if present)
        RightToBeForgottenConfig rightToBeForgottenConfig = (tier1Config.hasGlobalConfig() &&
            tier1Config.getGlobalConfig().hasRightToBeForgotten())
                ? tier1Config.getGlobalConfig().getRightToBeForgotten()
                : RightToBeForgottenConfig.getDefaultInstance();

        // Build base IngestionConfig with Tier 1 settings
        // Note: IngressMode is set later based on persistence decision
        // Engine will merge Tier 2 overrides (output_hints, custom_config) during IntakeHandoff
        IngestionConfig ingestionConfig = IngestionConfig.newBuilder()
            .setIngressMode(IngressMode.INGRESS_MODE_UNSPECIFIED)
            .setHydrationConfig(hydrationConfig)
            .setRightToBeForgotten(rightToBeForgottenConfig)
            .build();

        return new ResolvedConfig(tier1Config, ingestionConfig);
    }

    /**
     * Resolved configuration containing Tier 1 config and base IngestionConfig.
     * <p>
     * Note: This is Tier 1 only. Engine merges Tier 2 during IntakeHandoff.
     */
    public record ResolvedConfig(
        DataSourceConfig tier1Config,
        IngestionConfig ingestionConfig
    ) {
        /**
         * Check if persistence is enabled for gRPC PipeDoc uploads.
         * <p>
         * This is a Tier 1 setting from datasource-admin.
         */
        public boolean shouldPersist() {
            if (tier1Config.hasGlobalConfig() &&
                tier1Config.getGlobalConfig().hasPersistenceConfig()) {
                return tier1Config.getGlobalConfig().getPersistenceConfig().getPersistPipedoc();
            }
            // Default: persist (safe default)
            return true;
        }

        /**
         * Get a copy of ingestionConfig with the specified ingress mode.
         */
        public IngestionConfig withIngressMode(IngressMode mode) {
            return ingestionConfig.toBuilder()
                .setIngressMode(mode)
                .build();
        }
    }
}
