package ai.pipeline.connector.intake.http;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import java.io.InputStream;

/**
 * Declarative REST client for proxying raw uploads to repository-service.
 *
 * <p>Synchronous {@link Response} return (not {@code Uni<Response>}). The
 * Mutiny-wrapped variant was eating backpressure events under concurrent
 * load — 9 parallel s3 uploads would all log entry on connector-intake then
 * silently lose their chains, leaving the body InputStream readers blocked
 * with no thread parked on them and no error fired. Plain blocking REST
 * client streams the body bytes directly through a worker thread; backpressure
 * is HTTP/2 native and visible.
 */
@RegisterRestClient(configKey = "repository-upload")
@Path("/internal/uploads/raw")
public interface RepositoryUploadRestClient {

    @POST
    @Consumes(MediaType.WILDCARD)
    Response uploadRaw(
        InputStream body,
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Content-Length") long contentLength,
        @HeaderParam("x-account-id") String accountId,
        @HeaderParam("x-connector-id") String connectorId,
        @HeaderParam("x-datasource-id") String datasourceId,
        @HeaderParam("x-doc-id") String docId,
        @HeaderParam("x-drive-name") String driveName,
        @HeaderParam("x-filename") String filename,
        @HeaderParam("x-checksum-sha256") String checksumSha256,
        @HeaderParam("x-request-id") String requestId
    );
}
