package ai.pipeline.connector.intake;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class WireMockTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> wiremock;
    // Default to true for large message support (direct gRPC port 50052 has 2GB limit vs 4MB on Jetty port 8080)
    private boolean useDirectGrpc = true;

    @Override
    public void init(Map<String, String> initArgs) {
        // Allow tests to explicitly override with false if needed, otherwise default to true
        if (initArgs != null && initArgs.containsKey("useDirectGrpc")) {
            this.useDirectGrpc = "true".equals(initArgs.get("useDirectGrpc"));
        }
        System.err.println("DEBUG: WireMockTestResource init: useDirectGrpc=" + useDirectGrpc + " args=" + initArgs);
    }

    @Override
    public Map<String, String> start() {
        // Allow configuring the image via system property, default to the latest official image
        String imageName = System.getProperty("pipestream.wiremock.image", "docker.io/pipestreamai/pipestream-wiremock-server:0.1.28");
        System.err.println("DEBUG: WireMockTestResource starting with image: " + imageName);

        wiremock = new GenericContainer<>(DockerImageName.parse(imageName))
                .withExposedPorts(8080, 50052)
                .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
                // Ensure account not-found scenario is deterministic for tests.
                // NOTE: pipestream-wiremock-server's MockConfig lowercases env-var keys, but some mock initializers
                // look up mixed-case keys (e.g. wiremock.account.GetAccount.notfound.id). To avoid that mismatch,
                // set a JVM system property inside the container via JAVA_TOOL_OPTIONS.
                .withEnv("JAVA_TOOL_OPTIONS", "-Dwiremock.account.GetAccount.notfound.id=nonexistent-account")
                .waitingFor(Wait.forLogMessage(".*WireMock Server started.*", 1));
        
        wiremock.start();

        String host = wiremock.getHost();
        String directPort = wiremock.getMappedPort(50052).toString();
        String standardPort = wiremock.getMappedPort(8080).toString();

        System.setProperty("wiremock.direct.port", directPort);

        // Determine which port repo-service uses based on initArgs
        String repoServicePort = useDirectGrpc ? directPort : standardPort;
        
        System.err.println("DEBUG: WireMockTestResource start: repoServicePort=" + repoServicePort + " (Direct=" + directPort + ", Standard=" + standardPort + ")");

        // Build the address for Stork static service discovery
        String repoServiceAddress = host + ":" + repoServicePort;

        System.err.println("DEBUG: Configuring Stork static discovery for repo-service at: " + repoServiceAddress);

        // Use standard port (8080) for connector-admin and engine (unary gRPC)
        String standardServiceAddress = host + ":" + standardPort;
        
        Map<String, String> config = new java.util.HashMap<>();
        
        // Configure Stork static service discovery for repo-service
        // This overrides the Consul-based discovery in ServiceDiscoveryManager
        config.put("stork.repository.service-discovery.type", "static");
        config.put("stork.repository.service-discovery.address-list", repoServiceAddress);

        // Configure Stork for connector-admin (unary gRPC via standard port)
        config.put("stork.connector-admin.service-discovery.type", "static");
        config.put("stork.connector-admin.service-discovery.address-list", standardServiceAddress);
        
        // Configure Stork for engine (unary gRPC via standard port)
        config.put("stork.pipestream-engine.service-discovery.type", "static");
        config.put("stork.pipestream-engine.service-discovery.address-list", standardServiceAddress);
        
        // Configure Stork for account-manager (unary gRPC via standard port)
        config.put("stork.account-manager.service-discovery.type", "static");
        config.put("stork.account-manager.service-discovery.address-list", standardServiceAddress);

        // Legacy Quarkus gRPC client config (for any direct client usage)
        config.put("quarkus.grpc.clients.repository.host", host);
        config.put("quarkus.grpc.clients.repository.port", repoServicePort);
        config.put("quarkus.grpc.clients.connector-admin.host", host);
        config.put("quarkus.grpc.clients.connector-admin.port", standardPort);
        config.put("quarkus.grpc.clients.pipestream-engine.host", host);
        config.put("quarkus.grpc.clients.pipestream-engine.port", standardPort);
        config.put("quarkus.grpc.clients.account-manager.host", host);
        config.put("quarkus.grpc.clients.account-manager.port", standardPort);

        // Point Registration Service to Direct server (it handles streaming)
        config.put("pipestream.registration.registration-service.host", host);
        config.put("pipestream.registration.registration-service.port", directPort);

        // Expose standard port in case needed for HTTP/Admin API
        config.put("wiremock.host", host);
        config.put("wiremock.port", standardPort);

        // Ensure Quarkus gRPC server allows large messages (overriding defaults)
        config.put("quarkus.grpc.server.max-inbound-message-size", "2147483647");
        
        return config;
    }

    @Override
    public void stop() {
        if (wiremock != null) {
            wiremock.stop();
        }
    }
}
