package ai.pipeline.connector.intake.pipedoc;

import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.repository.filesystem.upload.v1.NodeUploadServiceGrpc;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocRequest;
import ai.pipestream.repository.filesystem.upload.v1.UploadFilesystemPipeDocResponse;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Fire-and-forget durable copy to the repository service for any document
 * whose datasource is configured with {@code persist_pipedoc=true}. Used
 * by the gRPC upload-doc paths (unary and bidi) AFTER the doc has been
 * enqueued to Redis for engine handoff. Intentionally decoupled from the
 * engine path:
 *
 * <ul>
 *   <li>Persist runs on its own VT-per-task executor — failure here
 *       cannot stall, fail, or delay the engine handoff.</li>
 *   <li>On failure: log loudly with the doc id, no retry. The replay
 *       feature this exists for is not on the hot path; a missed copy
 *       is recoverable by re-running the source crawl.</li>
 * </ul>
 *
 * <p>Future direction: replace the direct repo gRPC call with a Kafka
 * event ({@code intake-persist-events}) so a separate consumer can
 * batch / retry / handle backpressure independently. For now the direct
 * call is the simplest thing that exercises the path.
 *
 * <p>This class is NOT used for the POST upload path
 * ({@code RawUploadResource}), which has its own repo flow via
 * {@code RepositoryUploadClient}, and not for the gRPC blob upload path
 * ({@code GrpcBlobUploadService} → {@code BlobUploadHandoffService}).
 */
@ApplicationScoped
public class IntakeReplayPersister {

    private static final Logger LOG = Logger.getLogger(IntakeReplayPersister.class);

    /**
     * Synthetic owner-node-id for replay-persisted docs. Not part of any
     * graph; gives the replay consumer a dedicated path to scan when
     * doing a per-connector re-injection. Matches the convention used
     * by the previous {@code RepositoryPipeDocHandoffService} which used
     * {@code accountId + ":intake"} — preserving that so existing repo
     * data layouts are unaffected.
     */
    private static final String REPLAY_OWNER_SUFFIX = ":intake";

    @Inject
    @GrpcClient("repository")
    Channel repositoryChannel;

    /**
     * Dedicated VT-per-task executor. Each persist call runs on its own
     * virtual thread — failures are isolated, no carrier-thread starvation,
     * no shared queue to back up. Capped only by VM memory (which is fine
     * because each task is a single gRPC call's lifetime).
     */
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private NodeUploadServiceGrpc.NodeUploadServiceStub repositoryStub;

    @jakarta.annotation.PostConstruct
    void buildStub() {
        this.repositoryStub = NodeUploadServiceGrpc.newStub(repositoryChannel);
    }

    /**
     * Schedule a fire-and-forget durable persist of {@code pipeDoc} to the
     * repository service. Returns immediately. On failure, logs at WARN
     * with the doc id and exception class — does NOT throw, does NOT
     * retry, does NOT block the caller.
     *
     * @param pipeDoc the document to persist (full inline body)
     * @param accountId the account that owns the doc
     * @param datasourceId the source datasource (used as filesystem id
     *        in repository path so replay can scan per-datasource)
     */
    public void persistAsync(PipeDoc pipeDoc, String accountId, String datasourceId) {
        executor.execute(() -> doPersist(pipeDoc, accountId, datasourceId));
    }

    private void doPersist(PipeDoc pipeDoc, String accountId, String datasourceId) {
        UploadFilesystemPipeDocRequest req = UploadFilesystemPipeDocRequest.newBuilder()
                .setDocument(pipeDoc)
                .build();
        try {
            UploadFilesystemPipeDocResponse response = blockingCall(req);
            if (response.getSuccess()) {
                LOG.debugf("Replay persist accepted: doc_id=%s account=%s datasource=%s repo_id=%s",
                        pipeDoc.getDocId(), accountId, datasourceId, response.getDocumentId());
            } else {
                LOG.warnf("Replay persist rejected (no retry): doc_id=%s account=%s datasource=%s message=%s",
                        pipeDoc.getDocId(), accountId, datasourceId, response.getMessage());
            }
        } catch (RuntimeException e) {
            // Specific exception types from the gRPC client can bubble through
            // (StatusRuntimeException, IllegalStateException). We catch the
            // RuntimeException umbrella here because this is a fire-and-forget
            // worker — there's no upstream caller waiting; the only place the
            // failure can surface is this log. Catch is scoped to this single
            // helper, not propagated.
            LOG.warnf(e, "Replay persist failed (no retry): doc_id=%s account=%s datasource=%s class=%s",
                    pipeDoc.getDocId(), accountId, datasourceId, e.getClass().getSimpleName());
        }
    }

    /**
     * Bridges the async stub's StreamObserver callback to a synchronous
     * call (we're already on a VT in the persist executor). Returns the
     * unary response, throws on transport failure.
     */
    private UploadFilesystemPipeDocResponse blockingCall(UploadFilesystemPipeDocRequest req) {
        java.util.concurrent.CompletableFuture<UploadFilesystemPipeDocResponse> future =
                new java.util.concurrent.CompletableFuture<>();
        repositoryStub.uploadFilesystemPipeDoc(req, new StreamObserver<>() {
            @Override
            public void onNext(UploadFilesystemPipeDocResponse value) { future.complete(value); }
            @Override
            public void onError(Throwable t) { future.completeExceptionally(t); }
            @Override
            public void onCompleted() { /* unary response settled in onNext */ }
        });
        try {
            return future.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("repository persist interrupted", ie);
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException(cause);
        }
    }

    @PreDestroy
    void shutdown() {
        executor.shutdown();
    }
}
