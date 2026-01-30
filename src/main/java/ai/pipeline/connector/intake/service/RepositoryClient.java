package ai.pipeline.connector.intake.service;

import com.google.protobuf.Any;
import com.google.protobuf.BytesValue;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.repository.filesystem.v1.CreateFilesystemNodeRequest;
import ai.pipestream.repository.filesystem.v1.Node;
import ai.pipestream.repository.filesystem.v1.MutinyFilesystemServiceGrpc;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.UUID;

/**
 * Client for repository-service integration.
 * <p>
 * Handles document storage via repository-service's CreateNode RPC.
 */
@SuppressWarnings("unused")
@ApplicationScoped
public class RepositoryClient {

    private static final Logger LOG = Logger.getLogger(RepositoryClient.class);
    private static final String REPOSITORY_SERVICE_NAME = "repository";

    /**
     * Default constructor for CDI.
     */
    public RepositoryClient() { }

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /**
     * Store a document in the repository-service.
     * <p>
     * Builds a {@code CreateNodeRequest} and invokes the filesystem gRPC API to persist content and metadata.
     * A client-side UUID is generated and passed as {@code documentId}, but the repository-service may override it.
     * Reactive semantics:
     * <ul>
     *   <li>Returns a {@code Uni<String>} that emits the final document ID upon successful creation.</li>
     *   <li>gRPC failures are mapped to {@code io.grpc.StatusRuntimeException} with {@code INTERNAL} status.</li>
     * </ul>
     * Side effects: network call to repository-service; content is stored remotely.
     *
     * @param drive Drive/bucket name
     * @param connectorId Connector ID
     * @param name Document name
     * @param path Document path (folder-like hierarchy)
     * @param contentType MIME type of the content
     * @param data Document content bytes
     * @param metadata Arbitrary metadata map serialized as JSON
     * @return a {@code Uni} emitting the repository-assigned document ID
     */
    public Uni<String> storeDocument(String drive, String connectorId, String name, String path,
                                     String contentType, byte[] data, Map<String, String> metadata) {
        LOG.debugf("Storing document: name=%s, path=%s, size=%d", name, path, data.length);

        // Generate document ID
        String documentId = UUID.randomUUID().toString();

        // Build metadata JSON
        String metadataJson = buildMetadataJson(metadata);

        // Build CreateFilesystemNodeRequest
        CreateFilesystemNodeRequest request = CreateFilesystemNodeRequest.newBuilder()
            .setDrive(drive)
            .setDocumentId(documentId)
            .setConnectorId(connectorId)
            .setParentId(0) // Root folder
            .setName(name)
            .setPath(path)
            .setContentType(contentType)
            .setPayload(Any.pack(BytesValue.of(ByteString.copyFrom(data))))
            .setMetadata(metadataJson)
            .setType(Node.NodeType.NODE_TYPE_FILE)
            .build();

        return grpcClientFactory.getClient(REPOSITORY_SERVICE_NAME, MutinyFilesystemServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.createFilesystemNode(request))
            .map(response -> {
                // Return the document ID from the response node
                Node node = response.getNode();
                String actualDocumentId = node.getDocumentId();
                LOG.debugf("Document stored successfully: id=%s", actualDocumentId);
                return actualDocumentId;
            })
            .onFailure(io.grpc.StatusRuntimeException.class)
            .transform(throwable -> {
                LOG.errorf((io.grpc.StatusRuntimeException) throwable, "Failed to store document: name=%s", name);
                return Status.INTERNAL
                    .withDescription("Failed to store document: " + ((io.grpc.StatusRuntimeException) throwable).getMessage())
                    .asRuntimeException();
            });
    }

    /**
     * Build metadata JSON from map.
     */
    private String buildMetadataJson(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return "{}";
        }

        com.google.gson.Gson gson = new com.google.gson.Gson();
        com.google.gson.JsonObject json = new com.google.gson.JsonObject();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            json.addProperty(entry.getKey(), entry.getValue());
        }
        return gson.toJson(json);
    }
}
