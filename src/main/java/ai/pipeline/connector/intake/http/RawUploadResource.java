package ai.pipeline.connector.intake.http;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipeline.connector.intake.util.UriCanonicalizer;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * HTTP upload endpoint for connector raw uploads.
 * <p>
 * Validates API key, derives doc_id, then proxies to repository-service.
 */
@Path("/uploads")
public class RawUploadResource {

    private static final Logger LOG = Logger.getLogger(RawUploadResource.class);

    @Inject
    ConfigResolutionService configResolutionService;

    @Inject
    RepositoryUploadClient repositoryUploadClient;

    @POST
    @Path("/raw")
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public Uni<Response> uploadRaw(
        InputStream body,
        @HeaderParam("content-length") Long contentLength,
        @HeaderParam("content-type") String contentType,
        @HeaderParam("x-datasource-id") String datasourceId,
        @HeaderParam("x-api-key") String apiKey,
        @HeaderParam("x-doc-id") String clientDocId,
        @HeaderParam("x-source-doc-id") String sourceDocId,
        @HeaderParam("x-source-uri") String sourceUri,
        @HeaderParam("x-source-path") String sourcePath,
        @HeaderParam("x-filename") String filename,
        @HeaderParam("x-checksum-sha256") String checksumSha256,
        @HeaderParam("x-request-id") String requestId
    ) {
        if (body == null) {
            return Uni.createFrom().item(errorResponse(Response.Status.BAD_REQUEST, "body is required"));
        }
        if (contentLength == null || contentLength <= 0) {
            closeQuietly(body);
            return Uni.createFrom().item(errorResponse(Response.Status.BAD_REQUEST, "Content-Length header is required"));
        }
        if (datasourceId == null || datasourceId.isBlank()) {
            closeQuietly(body);
            return Uni.createFrom().item(errorResponse(Response.Status.BAD_REQUEST, "x-datasource-id is required"));
        }
        if (apiKey == null || apiKey.isBlank()) {
            closeQuietly(body);
            return Uni.createFrom().item(errorResponse(Response.Status.BAD_REQUEST, "x-api-key is required"));
        }

        String derivedDocId = deriveDocId(datasourceId, clientDocId, sourceDocId, sourceUri, sourcePath);
        if (derivedDocId == null) {
            closeQuietly(body);
            return Uni.createFrom().item(errorResponse(Response.Status.BAD_REQUEST,
                "Cannot determine doc_id. Provide x-doc-id, x-source-doc-id, x-source-uri, or x-source-path"));
        }

        return configResolutionService.resolveConfig(datasourceId, apiKey)
            .flatMap(resolved -> {
                String accountId = resolved.tier1Config().getAccountId();
                String connectorId = resolved.tier1Config().getConnectorId();
                String driveName = resolved.tier1Config().getDriveName();

                if (accountId == null || accountId.isBlank()) {
                    return Uni.createFrom().item(errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Account ID missing from config"));
                }
                if (connectorId == null || connectorId.isBlank()) {
                    return Uni.createFrom().item(errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Connector ID missing from config"));
                }
                if (driveName == null || driveName.isBlank()) {
                    return Uni.createFrom().item(errorResponse(Response.Status.INTERNAL_SERVER_ERROR, "Drive name missing from config"));
                }

                Map<String, String> headers = new HashMap<>();
                if (contentType != null && !contentType.isBlank()) {
                    headers.put("Content-Type", contentType);
                }
                headers.put("x-account-id", accountId);
                headers.put("x-connector-id", connectorId);
                headers.put("x-doc-id", derivedDocId);
                headers.put("x-drive-name", driveName);
                if (filename != null && !filename.isBlank()) {
                    headers.put("x-filename", filename);
                }
                if (checksumSha256 != null && !checksumSha256.isBlank()) {
                    headers.put("x-checksum-sha256", checksumSha256);
                }
                headers.put("x-request-id", requestId != null && !requestId.isBlank()
                    ? requestId
                    : UUID.randomUUID().toString());

                return repositoryUploadClient.uploadRaw(body, contentLength, headers)
                    .map(result -> withJaxrsClassLoader(() -> Response.status(result.statusCode())
                        .entity(result.body())
                        .type(result.contentType())
                        .build()));
            })
            .onFailure().invoke(throwable -> closeQuietly(body))
            .onFailure().recoverWithItem(throwable -> mapError(throwable));
    }

    private Response mapError(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException statusException) {
            Status.Code code = statusException.getStatus().getCode();
            return switch (code) {
                case UNAUTHENTICATED -> errorResponse(Response.Status.UNAUTHORIZED, statusException.getMessage());
                case PERMISSION_DENIED -> errorResponse(Response.Status.FORBIDDEN, statusException.getMessage());
                case INVALID_ARGUMENT -> errorResponse(Response.Status.BAD_REQUEST, statusException.getMessage());
                case NOT_FOUND -> errorResponse(Response.Status.NOT_FOUND, statusException.getMessage());
                default -> {
                    LOG.error("Raw upload failed", statusException);
                    yield errorResponse(Response.Status.INTERNAL_SERVER_ERROR, statusException.getMessage());
                }
            };
        }
        LOG.error("Raw upload failed", throwable);
        return errorResponse(Response.Status.INTERNAL_SERVER_ERROR, throwable.getMessage());
    }

    private Response errorResponse(Response.Status status, String message) {
        String body = "{\"error\":\"" + escapeJson(message) + "\"}";
        return withJaxrsClassLoader(() -> Response.status(status)
            .entity(body)
            .type(MediaType.APPLICATION_JSON)
            .build());
    }

    private Response withJaxrsClassLoader(Supplier<Response> builder) {
        ClassLoader jaxrsLoader = Response.class.getClassLoader();
        Thread current = Thread.currentThread();
        ClassLoader original = current.getContextClassLoader();
        if (original == jaxrsLoader) {
            return builder.get();
        }
        current.setContextClassLoader(jaxrsLoader);
        try {
            return builder.get();
        } finally {
            current.setContextClassLoader(original);
        }
    }

    private String deriveDocId(String datasourceId,
                               String clientDocId,
                               String sourceDocId,
                               String sourceUri,
                               String sourcePath) {
        if (clientDocId != null && !clientDocId.isBlank()) {
            return clientDocId;
        }
        if (sourceDocId != null && !sourceDocId.isBlank()) {
            return datasourceId + ":" + sourceDocId;
        }
        if (sourceUri != null && !sourceUri.isBlank()) {
            String canonical = canonicalizeUri(sourceUri);
            return canonical == null ? null : datasourceId + ":" + canonical;
        }
        if (sourcePath != null && !sourcePath.isBlank()) {
            String normalized = normalizePath(sourcePath);
            return normalized == null ? null : datasourceId + ":" + normalized;
        }
        return null;
    }

    private String canonicalizeUri(String uri) {
        try {
            return UriCanonicalizer.canonicalizeUri(uri, true);
        } catch (IllegalArgumentException e) {
            LOG.warnf("Failed to canonicalize URI '%s': %s", uri, e.getMessage());
            return uri.trim();
        }
    }

    private String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String normalized = path.trim().replace('\\', '/');
        normalized = normalized.replaceAll("/+", "/");
        normalized = normalized.replaceAll("^/", "");
        normalized = normalized.replaceAll("/$", "");
        return normalized;
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    private void closeQuietly(InputStream body) {
        try {
            body.close();
        } catch (Exception ignored) {
            // best-effort close
        }
    }
}
