package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipeline.connector.intake.service.EngineClient;
import ai.pipestream.connector.intake.v1.UploadBlobResponse;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
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

@ApplicationScoped
public class RepositoryPipeDocHandoffService {

    private static final Logger LOG = Logger.getLogger(RepositoryPipeDocHandoffService.class);

    @GrpcClient("repository")
    Channel repositoryChannel;

    @Inject
    EngineClient engineClient;

    public UploadPipeDocResponse persistAndHandoff(
            PipeDoc pipeDoc,
            ConfigResolutionService.ResolvedConfig resolved,
            long startTime,
            String crawlId) {

        LOG.debugf("Persisting document to repository: doc_id=%s", pipeDoc.getDocId());
        UploadFilesystemPipeDocResponse repoResponse = uploadFilesystemPipeDoc(pipeDoc);

        long repoTime = System.nanoTime() - startTime;
        LOG.debugf("uploadPipeDoc: repository persist took %.3f ms, doc_id=%s",
                repoTime / 1_000_000.0, repoResponse.getDocumentId());

        if (!repoResponse.getSuccess()) {
            return UploadPipeDocResponse.newBuilder()
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
        LOG.debugf("uploadPipeDoc: complete in %.3f ms, accepted=%s",
                totalTime / 1_000_000.0, handoffResponse.getAccepted());

        return UploadPipeDocResponse.newBuilder()
                .setSuccess(handoffResponse.getAccepted())
                .setDocId(repoResponse.getDocumentId())
                .setMessage(handoffResponse.getAccepted()
                        ? "Document persisted and handed off to engine"
                        : "Engine rejected: " + handoffResponse.getMessage())
                .build();
    }

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
