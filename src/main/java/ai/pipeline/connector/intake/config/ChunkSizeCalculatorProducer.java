package ai.pipeline.connector.intake.config;

import ai.pipestream.server.util.ChunkSizeCalculator;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Provides a default ChunkSizeCalculator bean when the pipestream-server
 * dependency does not expose it as a CDI bean.
 */
@ApplicationScoped
public class ChunkSizeCalculatorProducer {

    @Produces
    @DefaultBean
    public ChunkSizeCalculator chunkSizeCalculator() {
        return new ChunkSizeCalculator();
    }
}
