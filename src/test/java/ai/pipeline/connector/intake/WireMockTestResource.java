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
        String imageName = System.getProperty("pipestream.wiremock.image", "docker.io/pipestreamai/pipestream-wiremock-server:0.1.23");
        System.err.println("DEBUG: WireMockTestResource starting with image: " + imageName);

        wiremock = new GenericContainer<>(DockerImageName.parse(imageName))
                .withExposedPorts(8080, 50052)
                .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
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

        return Map.of(
                // Configure Stork static service discovery for repo-service
                // This overrides the Consul-based discovery in ServiceDiscoveryManager
                "stork.repo-service.service-discovery.type", "static",
                "stork.repo-service.service-discovery.address-list", repoServiceAddress,

                // Legacy Quarkus gRPC client config (for any direct client usage)
                "quarkus.grpc.clients.repo-service.host", host,
                "quarkus.grpc.clients.repo-service.port", repoServicePort,

                // Point Registration Service to Direct server (it handles streaming)
                "pipestream.registration.registration-service.host", host,
                "pipestream.registration.registration-service.port", directPort,

                // Expose standard port in case needed for HTTP/Admin API
                "wiremock.host", host,
                "wiremock.port", standardPort,

                // Ensure Quarkus gRPC server allows large messages (overriding defaults)
                "quarkus.grpc.server.max-inbound-message-size", "2147483647"
        );
    }

    @Override
    public void stop() {
        if (wiremock != null) {
            wiremock.stop();
        }
    }
}
