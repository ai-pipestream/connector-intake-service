package ai.pipeline.connector.intake.http;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Starts a lightweight HTTP server to mock repository-service raw upload endpoint.
 */
public class RepositoryUploadTestResource implements QuarkusTestResourceLifecycleManager {

    private static final Logger LOG = Logger.getLogger(RepositoryUploadTestResource.class);

    private HttpServer server;

    @Override
    public Map<String, String> start() {
        try {
            server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/internal/uploads/raw", new UploadHandler());
            server.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start mock repository upload server", e);
        }

        int port = server.getAddress().getPort();
        String baseUrl = "http://localhost:" + port;
        LOG.infof("Mock repository upload server started at %s", baseUrl);

        return Map.of(
            "connector-intake.repository-upload.base-url", baseUrl
        );
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private static class UploadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            Headers headers = exchange.getRequestHeaders();
            Map<String, String> flatHeaders = new HashMap<>();
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    flatHeaders.put(entry.getKey().toLowerCase(), entry.getValue().get(0));
                }
            }

            byte[] body = exchange.getRequestBody().readAllBytes();
            RepositoryUploadMockState.record(path, flatHeaders, body);

            String responseJson = "{\"status\":\"ok\",\"docId\":\"mock-repo-doc\"}";
            byte[] responseBytes = responseJson.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        }
    }
}
