package ai.pipeline.connector.intake.http;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

/**
 * HTTP client for proxying raw uploads to repository-service.
 */
@ApplicationScoped
public class RepositoryUploadClient {

    private static final Logger LOG = Logger.getLogger(RepositoryUploadClient.class);

    private final HttpClient httpClient;

    @Inject
    RepositoryUploadConfig config;

    public RepositoryUploadClient() {
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    public Uni<RepositoryUploadResponse> uploadRaw(InputStream body,
                                                   long contentLength,
                                                   Map<String, String> headers) {
        URI uri = buildRawUploadUri();
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
            .timeout(config.requestTimeout())
            .POST(new InputStreamBodyPublisher(body, contentLength));

        headers.forEach((key, value) -> {
            if (value != null && !value.isBlank()) {
                builder.header(key, value);
            }
        });

        HttpRequest request = builder.build();
        LOG.debugf("Proxying raw upload to repository-service: %s", uri);

        return Uni.createFrom().completionStage(
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        ).map(response -> {
            String contentType = response.headers()
                .firstValue("content-type")
                .orElse("application/json");
            return new RepositoryUploadResponse(response.statusCode(), contentType, response.body());
        });
    }

    private URI buildRawUploadUri() {
        String baseUrl = config.baseUrl();
        String rawPath = config.rawPath();
        if (baseUrl.endsWith("/") && rawPath.startsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        return URI.create(baseUrl + rawPath);
    }

    public record RepositoryUploadResponse(int statusCode, String contentType, String body) {}
}
