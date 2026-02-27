package ai.pipeline.connector.intake.http;

import io.smallrye.mutiny.Uni;
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
 */
@RegisterRestClient(configKey = "repository-upload")
@Path("/internal/uploads/raw")
public interface RepositoryUploadRestClient {

    @POST
    @Consumes(MediaType.WILDCARD)
    Uni<Response> uploadRaw(
        InputStream body,
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Content-Length") long contentLength,
        @HeaderParam("x-account-id") String accountId,
        @HeaderParam("x-connector-id") String connectorId,
        @HeaderParam("x-doc-id") String docId,
        @HeaderParam("x-drive-name") String driveName,
        @HeaderParam("x-filename") String filename,
        @HeaderParam("x-checksum-sha256") String checksumSha256,
        @HeaderParam("x-request-id") String requestId
    );
}
