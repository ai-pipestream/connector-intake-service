package ai.pipeline.connector.intake.http;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.util.Map;

/**
 * Synchronous HTTP client for proxying raw uploads to repository-service.
 *
 * <p>Plain blocking REST call — caller (the {@code @Blocking}
 * {@link RawUploadResource} handler) is on a worker thread, so blocking
 * here is correct and intended. No Mutiny operators wrap the body
 * forwarding, so the inbound HTTP request body InputStream and the outbound
 * REST client body stream are coupled directly through the worker thread
 * — backpressure is HTTP/2 native and visible.
 */
@ApplicationScoped
public class RepositoryUploadClient {

    private static final Logger LOG = Logger.getLogger(RepositoryUploadClient.class);

    @Inject
    @RestClient
    RepositoryUploadRestClient restClient;

    /**
     * Forwards the raw upload to repository-service synchronously.
     *
     * @param bufferSize unused — kept in the signature for backwards
     *                   compatibility with callers that previously needed
     *                   to pass a chunk size. Quarkus REST client streams
     *                   the body without per-chunk buffering.
     */
    public RepositoryUploadResponse uploadRaw(InputStream body,
                                              long contentLength,
                                              int bufferSize,
                                              Map<String, String> headers) {
        String contentType = headers.get("Content-Type");
        String accountId = headers.get("x-account-id");
        String connectorId = headers.get("x-connector-id");
        String datasourceId = headers.get("x-datasource-id");
        String docId = headers.get("x-doc-id");
        String driveName = headers.get("x-drive-name");
        String filename = headers.get("x-filename");
        String checksumSha256 = headers.get("x-checksum-sha256");
        String requestId = headers.get("x-request-id");

        LOG.debugf("Proxying raw upload to repository-service via REST client (doc_id=%s)", docId);

        try (Response response = restClient.uploadRaw(body, contentType, contentLength, accountId, connectorId,
                datasourceId, docId, driveName, filename, checksumSha256, requestId)) {
            String respContentType = response.getHeaderString("content-type");
            if (respContentType == null || respContentType.isBlank()) {
                respContentType = "application/json";
            }
            String respBody = response.readEntity(String.class);
            return new RepositoryUploadResponse(response.getStatus(), respContentType, respBody);
        }
    }

    public record RepositoryUploadResponse(int statusCode, String contentType, String body) {}
}
