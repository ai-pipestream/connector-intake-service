package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipeline.connector.intake.service.EngineClient;
import ai.pipestream.connector.intake.v1.UploadBlobResponse;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.repository.filesystem.upload.v1.NodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Handles the gRPC blob upload path: persist the doc to repository
 * (durable, blobs are large so we don't route them through Redis),
 * then hand off a reference-only request to the engine.
 *
 * <p>The engine handoff goes through {@link EngineClient#intakeHandoff},
 * which after the bidi migration delegates to
 * {@link ai.pipeline.connector.intake.service.IntakeHandoffStreamClient}'s
 * long-lived stream — same as every other intake-to-engine call. No unary.
 *
 * <p>Used only by {@code GrpcBlobUploadService}. Not on the gRPC PipeDoc
 * upload path (that path is selected by {@link PipeDocAcceptanceService}).
 * Not on the POST upload path (that path uses {@code RepositoryUploadClient}
 * directly).
 *
 * <p>Renamed from {@code RepositoryPipeDocHandoffService} during the
 * intake-as-wall refactor: the {@code persistAndHandoff(PipeDoc, ...)}
 * variant was deleted because the normal PipeDoc upload path now flows
 * through Redis and the kafka-sidecar drain handles engine handoff. Only
 * the blob variant remains, hence the new class name.
 */
@ApplicationScoped
public class BlobUploadHandoffService {

    private static final Logger LOG = Logger.getLogger(BlobUploadHandoffService.class);

    @GrpcClient("repository")
    Channel repositoryChannel;

    @Inject
    EngineClient engineClient;

    /**
     * Default constructor for CDI proxying.
     */
    public BlobUploadHandoffService() {}

    /**
     * Persist a blob and hand off the reference to the engine.
     *
     * @param pipeDoc The document to hand off
     * @param resolved The resolved configuration
     * @param startTime The start time of the operation in nanoseconds
     * @param crawlId The identifier of the crawl session
     * @return The response containing blob handoff details
     */
    public UploadBlobResponse persistBlobAndHandoff(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            long startTime,
            String crawlId) {

        UploadFilesystemPipeDocResponse repoResponse = uploadFilesystemPipeDoc(pipeDoc);
        long repoTime = System.nanoTime() - startTime;
        LOG.debugf("uploadBlob: repository persist took %.3f ms", repoTime / 1_000_000.0);

        if (!repoResponse.getSuccess()) {
            return UploadBlobResponse.newBuilder()
                    .setSuccess(false)
                    .setDocId(pipeDoc.getDocId())
                    .setMessage("Repository persistence failed: " + repoResponse.getMessage())
                    .build();
        }

        IngestionConfig ingestionConfig = resolved.withIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED);
        String datasourceId = resolved.tier1Config().getDatasourceId();
        IntakeHandoffResponse handoffResponse = engineClient.handoffReferenceToEngine(
                repoResponse.getDocumentId(),
                datasourceId,
                datasourceId,
                resolved.tier1Config().getAccountId(),
                ingestionConfig,
                crawlId);

        long totalTime = System.nanoTime() - startTime;
        LOG.debugf("uploadBlob: complete in %.3f ms, accepted=%s",
                totalTime / 1_000_000.0, handoffResponse.getAccepted());

        return UploadBlobResponse.newBuilder()
                .setSuccess(handoffResponse.getAccepted())
                .setDocId(repoResponse.getDocumentId())
                .setMessage(handoffResponse.getAccepted()
                        ? "Blob persisted and handed off to engine"
                        : "Engine rejected: " + handoffResponse.getMessage())
                .build();
    }

    private UploadFilesystemPipeDocResponse uploadFilesystemPipeDoc(PipeDoc pipeDoc) {
        UploadFilesystemPipeDocRequest repoRequest = UploadFilesystemPipeDocRequest.newBuilder()
                .setDocument(pipeDoc)
                .build();
        CompletableFuture<UploadFilesystemPipeDocResponse> future = new CompletableFuture<>();
        NodeUploadServiceGrpc.NodeUploadServiceStub stub = NodeUploadServiceGrpc.newStub(repositoryChannel);
        stub.uploadFilesystemPipeDoc(repoRequest, new StreamObserver<>() {
            @Override
            public void onNext(UploadFilesystemPipeDocResponse value) {
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
        return awaitUnary(future, "repository UploadFilesystemPipeDoc");
    }

    private <T> T awaitUnary(CompletableFuture<T> future, String phase) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED.withDescription(phase + " interrupted").withCause(e).asRuntimeException();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                throw sre;
            }
            throw Status.UNKNOWN.withDescription(phase + " failed: " + cause.getMessage())
                    .withCause(cause)
                    .asRuntimeException();
        }
    }
}
