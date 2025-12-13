package ai.pipeline.connector.intake;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class WireMockTestResource implements QuarkusTestResourceLifecycleManager {

    private GenericContainer<?> wiremock;

    @Override
    public Map<String, String> start() {
        // Use the locally built image
        wiremock = new GenericContainer<>(DockerImageName.parse("pipestream-wiremock-server:0.1.14-SNAPSHOT"))
                .withExposedPorts(8080)
                .waitingFor(Wait.forLogMessage(".*WireMock Server started.*", 1));
        
        wiremock.start();

        return Map.of(
                "quarkus.grpc.clients.repo-service.host", wiremock.getHost(),
                "quarkus.grpc.clients.repo-service.port", wiremock.getMappedPort(8080).toString()
        );
    }

    @Override
    public void stop() {
        if (wiremock != null) {
            wiremock.stop();
        }
    }
}
