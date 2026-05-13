package ai.pipeline.connector.intake.mock;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.Map;

public class MockEngineTestResource implements QuarkusTestResourceLifecycleManager {

    private Server server;

    @Override
    public Map<String, String> start() {
        MockEngineState.reset();
        try {
            server = ServerBuilder.forPort(0)
                    .addService(new MockEngineService())
                    .build()
                    .start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start mock engine gRPC server", e);
        }
        String port = String.valueOf(server.getPort());
        return Map.of(
                "quarkus.grpc.clients.engine.host", "localhost",
                "quarkus.grpc.clients.engine.port", port,
                "quarkus.grpc.clients.engine.name-resolver", "dns",
                "stork.engine.service-discovery.type", "static",
                "stork.engine.service-discovery.address-list", "localhost:" + port,
                "pipestream.intake.engine.handoff-stream-ack-timeout-ms", "1000");
    }

    @Override
    public void stop() {
        if (server != null) {
            server.shutdownNow();
            server = null;
        }
        MockEngineState.reset();
    }

    @Override
    public int order() {
        return 1_000;
    }
}
