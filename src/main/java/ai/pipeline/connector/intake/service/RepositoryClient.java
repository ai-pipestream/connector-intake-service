package ai.pipeline.connector.intake.service;

import ai.pipestream.repository.filesystem.v1.CreateFilesystemNodeRequest;
import ai.pipestream.repository.filesystem.v1.CreateFilesystemNodeResponse;
import ai.pipestream.repository.filesystem.v1.FilesystemServiceGrpc;
import ai.pipestream.repository.filesystem.v1.Node;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Client for repository-service integration.
 */
@SuppressWarnings("unused")
@ApplicationScoped
public class RepositoryClient {

    private static final Logger LOG = Logger.getLogger(RepositoryClient.class);

    public RepositoryClient() { }

    @GrpcClient("repository")
    Channel repositoryChannel;

    public String storeDocument(String drive, String connectorId, String name, String path,
                                String contentType, byte[] data, Map<String, String> metadata) {
        LOG.debugf("Storing document: name=%s, path=%s, size=%d", name, path, data.length);

        String documentId = UUID.randomUUID().toString();
        String metadataJson = buildMetadataJson(metadata);

        CreateFilesystemNodeRequest request = CreateFilesystemNodeRequest.newBuilder()
                .setDrive(drive)
                .setDocumentId(documentId)
                .setConnectorId(connectorId)
                .setParentId(0)
                .setName(name)
                .setPath(path)
                .setContentType(contentType)
                .setPayload(Any.pack(BytesValue.of(ByteString.copyFrom(data))))
                .setMetadata(metadataJson)
                .setType(Node.NodeType.NODE_TYPE_FILE)
                .build();

        CompletableFuture<CreateFilesystemNodeResponse> future = new CompletableFuture<>();
        FilesystemServiceGrpc.FilesystemServiceStub stub = FilesystemServiceGrpc.newStub(repositoryChannel);
        stub.createFilesystemNode(request, new StreamObserver<>() {
            @Override
            public void onNext(CreateFilesystemNodeResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                // unary response already completed in onNext
            }
        });

        CreateFilesystemNodeResponse response = awaitUnary(future, name);
        String actualDocumentId = response.getNode().getDocumentId();
        LOG.debugf("Document stored successfully: id=%s", actualDocumentId);
        return actualDocumentId;
    }

    private CreateFilesystemNodeResponse awaitUnary(CompletableFuture<CreateFilesystemNodeResponse> future, String name) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withDescription("Interrupted storing document: " + name).withCause(e).asRuntimeException();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                LOG.errorf(sre, "Failed to store document: name=%s", name);
                throw sre;
            }
            throw Status.INTERNAL
                    .withDescription("Failed to store document: " + cause.getMessage())
                    .withCause(cause)
                    .asRuntimeException();
        }
    }

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
