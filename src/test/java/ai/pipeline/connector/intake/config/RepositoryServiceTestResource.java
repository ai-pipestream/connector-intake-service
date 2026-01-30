package ai.pipeline.connector.intake.config;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.jboss.logging.Logger;

import java.util.Map;

/**
 * Test resource that configures the repository client to connect to MockRepositoryService.
 * 
 * MockRepositoryService runs as a @GrpcService in the same Quarkus test instance.
 * With in-process disabled, it runs on the test server's gRPC port (same as HTTP port).
 * 
 * The port is assigned dynamically (quarkus.http.test-port=0), so we configure it
 * to use the test server's port. Since both services are in the same instance,
 * MockRepositoryService will be on localhost:test-port.
 * 
 * Note: The actual port value will be available after Quarkus starts, but dynamic-grpc
 * reads it from config at @PostConstruct time. We set it to use the test server port
 * which Quarkus will resolve.
 */
public class RepositoryServiceTestResource implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOG = Logger.getLogger(RepositoryServiceTestResource.class);

    @Override
    public Map<String, String> start() {
        LOG.info("RepositoryServiceTestResource: Configuring repository client for MockRepositoryService");
        LOG.info("MockRepositoryService (@GrpcService) runs in same Quarkus instance");
        LOG.info("With in-process disabled, it uses real Netty channels on test server's gRPC port");

        // MockRepositoryService runs on the test server's port
        // Since quarkus.http.test-port=0, Quarkus assigns a random port
        // We configure the client to use the same port (test server port)
        // The port will be resolved by Quarkus when the config is read
        return Map.of(
            "quarkus.grpc.clients.repository.host", "localhost"
            // Port will be resolved from test server port by Quarkus
            // We can't set it here because it's not known yet, but Quarkus will use the test server port
        );
    }

    @Override
    public void stop() {
        // No cleanup needed
    }
}
