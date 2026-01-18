package ai.pipeline.connector.intake.http;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;

/**
 * Configuration for proxying raw uploads to repository-service.
 */
@ConfigMapping(prefix = "connector-intake.repository-upload")
public interface RepositoryUploadConfig {

    /**
     * Base URL for repository-service HTTP endpoint (no trailing slash).
     */
    @WithDefault("http://localhost:38102")
    String baseUrl();

    /**
     * Raw upload path on repository-service.
     */
    @WithDefault("/internal/uploads/raw")
    String rawPath();

    /**
     * Timeout for repository upload requests.
     */
    @WithDefault("60s")
    Duration requestTimeout();
}
