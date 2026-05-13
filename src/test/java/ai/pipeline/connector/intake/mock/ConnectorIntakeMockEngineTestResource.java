package ai.pipeline.connector.intake.mock;

import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.HashMap;
import java.util.Map;

public class ConnectorIntakeMockEngineTestResource extends ConnectorIntakeWireMockTestResource {

    private Server engineServer;

    @Override
    public Map<String, String> start() {
        MockEngineState.reset();
        startEngineServer();

        Map<String, String> config = new HashMap<>(super.start());
        String port = String.valueOf(engineServer.getPort());
        config.put("quarkus.grpc.clients.engine.host", "localhost");
        config.put("quarkus.grpc.clients.engine.port", port);
        config.put("quarkus.grpc.clients.engine.name-resolver", "dns");
        config.put("stork.engine.service-discovery.type", "static");
        config.put("stork.engine.service-discovery.address-list", "localhost:" + port);
        config.put("pipestream.intake.engine.handoff-stream-ack-timeout-ms", "1000");
        System.err.println("Mock engine test resource overriding engine gRPC client to localhost:" + port);
        return config;
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            if (engineServer != null) {
                engineServer.shutdownNow();
                engineServer = null;
            }
            MockEngineState.reset();
        }
    }

    private void startEngineServer() {
        try {
            engineServer = ServerBuilder.forPort(0)
                    .addService(new MockEngineService())
                    .build()
                    .start();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to start mock engine gRPC server", e);
        }
    }
}
