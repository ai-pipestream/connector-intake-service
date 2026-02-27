package ai.pipeline.connector.intake.http;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.util.Map;

/**
 * HTTP client for proxying raw uploads to repository-service.
 */
@ApplicationScoped
public class RepositoryUploadClient {

    private static final Logger LOG = Logger.getLogger(RepositoryUploadClient.class);

    @Inject
    @RestClient
    RepositoryUploadRestClient restClient;

    public Uni<RepositoryUploadResponse> uploadRaw(InputStream body,
                                                   long contentLength,
                                                   int bufferSize,
                                                   Map<String, String> headers) {
        String contentType = headers.get("Content-Type");
        String accountId = headers.get("x-account-id");
        String connectorId = headers.get("x-connector-id");
        String docId = headers.get("x-doc-id");
        String driveName = headers.get("x-drive-name");
        String filename = headers.get("x-filename");
        String checksumSha256 = headers.get("x-checksum-sha256");
        String requestId = headers.get("x-request-id");

        LOG.debugf("Proxying raw upload to repository-service via REST client (doc_id=%s)", docId);

        return restClient.uploadRaw(body, contentType, contentLength, accountId, connectorId,
                docId, driveName, filename, checksumSha256, requestId)
            .map(response -> {
                String respContentType = response.getHeaderString("content-type");
                if (respContentType == null || respContentType.isBlank()) {
                    respContentType = "application/json";
                }
                String respBody = response.readEntity(String.class);
                return new RepositoryUploadResponse(response.getStatus(), respContentType, respBody);
            });
    }

    public record RepositoryUploadResponse(int statusCode, String contentType, String body) {}
}
