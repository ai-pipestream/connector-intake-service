package ai.pipeline.connector.intake.service;

import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import ai.pipestream.engine.v1.EngineV1ServiceGrpc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.server.constants.PipestreamServices;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Client for engine service integration.
 * <p>
 * Handles handoff of documents from intake to engine for pipeline processing.
 * Intake is graph-agnostic — only datasource_id and account_id leave intake;
 * engine resolves graph routing, Tier 2 config merging, and entry-node
 * dispatch.
 * <p>
 * Wire shape: plain async-callback {@link StreamObserver} stubs (no Mutiny
 * stub wrapper). gRPC's {@link io.grpc.Context} capture happens
 * synchronously when {@code stub.intakeHandoff(req, observer)} is invoked,
 * on whatever thread is current at that moment. Channel resolution goes
 * through {@link DynamicGrpcClientFactory#getAsyncClient} which blocks
 * the calling thread for Stork resolution; on a virtual thread that's
 * essentially free, on a Vert.x event loop it would throw — so callers
 * must invoke this from a worker / VT context. The Mutiny outer return
 * type is preserved at the boundary because the {@code uploadPipeDoc}
 * handler chain in {@link ConnectorIntakeServiceImpl} is still Mutiny-
 * shaped; internally we bridge via {@link CompletableFuture}.
 */
@ApplicationScoped
public class EngineClient {

    private static final Logger LOG = Logger.getLogger(EngineClient.class);
    private static final String ENGINE_SERVICE_NAME = PipestreamServices.ENGINE.serviceName();

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    @ConfigProperty(name = "pipestream.intake.engine.deadline-ms", defaultValue = "30000")
    long deadlineMs;

    /**
     * Default constructor for CDI.
     */
    public EngineClient() {}

    /**
     * Hand off a document inline to the engine for pipeline processing.
     * Engine fast-acks (validate + enqueue) and returns; actual module
     * work runs on the engine's per-module worker pool.
     */
    public Uni<IntakeHandoffResponse> handoffToEngine(
            PipeDoc pipeDoc,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig) {
        return handoffToEngine(pipeDoc, datasourceId, accountId, ingestionConfig, null);
    }

    public Uni<IntakeHandoffResponse> handoffToEngine(
            PipeDoc pipeDoc,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig,
            String crawlId) {

        IntakeHandoffRequest request = buildRequestForDoc(pipeDoc, datasourceId, accountId,
                ingestionConfig, crawlId);

        return Uni.createFrom().completionStage(() -> callIntakeHandoff(request))
                .invoke(response -> {
                    if (response.getAccepted()) {
                        LOG.debugf("Engine accepted document: doc_id=%s, stream_id=%s, entry_node=%s",
                                pipeDoc.getDocId(), response.getAssignedStreamId(),
                                response.getEntryNodeId());
                    } else {
                        LOG.warnf("Engine rejected document: doc_id=%s, message=%s",
                                pipeDoc.getDocId(), response.getMessage());
                    }
                });
    }

    /**
     * Hand off a document reference to the engine. Same shape as the
     * inline path — engine hydrates from repository when the worker
     * picks the item up.
     */
    public Uni<IntakeHandoffResponse> handoffReferenceToEngine(
            String docId,
            String sourceNodeId,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig) {
        return handoffReferenceToEngine(docId, sourceNodeId, datasourceId, accountId,
                ingestionConfig, null);
    }

    public Uni<IntakeHandoffResponse> handoffReferenceToEngine(
            String docId,
            String sourceNodeId,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig,
            String crawlId) {

        IntakeHandoffRequest request = buildRequestForRef(docId, sourceNodeId, datasourceId,
                accountId, ingestionConfig, crawlId);

        return Uni.createFrom().completionStage(() -> callIntakeHandoff(request))
                .invoke(response -> {
                    if (response.getAccepted()) {
                        LOG.debugf("Engine accepted document ref: doc_id=%s, stream_id=%s, entry_node=%s",
                                docId, response.getAssignedStreamId(), response.getEntryNodeId());
                    } else {
                        LOG.warnf("Engine rejected document ref: doc_id=%s, message=%s",
                                docId, response.getMessage());
                    }
                });
    }

    /**
     * Resolves an async-callback stub for the engine and invokes
     * {@code intakeHandoff} once. {@link io.grpc.Context} is captured on
     * the calling thread at the moment {@code stub.intakeHandoff(...)} is
     * invoked — for callers running on a virtual thread outside an inbound
     * RPC handler, that Context is {@link io.grpc.Context#ROOT} and never
     * cancels. The returned future completes when the engine's fast-ack
     * reply arrives (typically &lt;5 ms with the worker-queue model).
     */
    private CompletableFuture<IntakeHandoffResponse> callIntakeHandoff(IntakeHandoffRequest request) {
        CompletableFuture<IntakeHandoffResponse> future = new CompletableFuture<>();
        // getChannel returns Uni<Channel> so Stork resolution is async —
        // safe to call from a Vert.x event loop where blocking is forbidden.
        // When the channel is ready, we build the async-callback stub and
        // invoke intakeHandoff inside Context.ROOT.run so the call captures
        // ROOT (never cancels) regardless of what Context the resolution
        // callback's dispatch thread carries. The stub.intakeHandoff
        // invocation is synchronous (it kicks off the underlying
        // ClientCall.start() right then), so the wrap correctly scopes
        // the Context capture.
        grpcClientFactory.getChannel(ENGINE_SERVICE_NAME)
                .subscribe().with(
                        channel -> {
                            EngineV1ServiceGrpc.EngineV1ServiceStub stub =
                                    EngineV1ServiceGrpc.newStub(channel);
                            try {
                                Context.ROOT.run(() ->
                                        stub.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS)
                                                .intakeHandoff(request,
                                                        new StreamObserver<IntakeHandoffResponse>() {
                                                            @Override
                                                            public void onNext(IntakeHandoffResponse v) {
                                                                future.complete(v);
                                                            }

                                                            @Override
                                                            public void onError(Throwable t) {
                                                                future.completeExceptionally(t);
                                                            }

                                                            @Override
                                                            public void onCompleted() { /* no-op for unary */ }
                                                        }));
                            } catch (RuntimeException e) {
                                future.completeExceptionally(e);
                            }
                        },
                        err -> future.completeExceptionally(err)
                );
        return future;
    }

    private IntakeHandoffRequest buildRequestForDoc(PipeDoc pipeDoc, String datasourceId,
                                                    String accountId, IngestionConfig ingestionConfig,
                                                    String crawlId) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        StreamMetadata.Builder metaBuilder = StreamMetadata.newBuilder()
                .setSourceId(datasourceId)
                .setCreatedAt(timestamp)
                .setLastProcessedAt(timestamp)
                .setTraceId(UUID.randomUUID().toString())
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .setIngestionConfig(ingestionConfig);
        if (crawlId != null && !crawlId.isEmpty()) {
            metaBuilder.setCrawlId(crawlId);
        }

        PipeStream pipeStream = PipeStream.newBuilder()
                .setStreamId(UUID.randomUUID().toString())
                .setMetadata(metaBuilder.build())
                .setDocument(pipeDoc)
                .setHopCount(0)
                .build();

        return IntakeHandoffRequest.newBuilder()
                .setStream(pipeStream)
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .build();
    }

    private IntakeHandoffRequest buildRequestForRef(String docId, String sourceNodeId,
                                                    String datasourceId, String accountId,
                                                    IngestionConfig ingestionConfig,
                                                    String crawlId) {
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        StreamMetadata.Builder metaBuilder = StreamMetadata.newBuilder()
                .setSourceId(datasourceId)
                .setCreatedAt(timestamp)
                .setLastProcessedAt(timestamp)
                .setTraceId(UUID.randomUUID().toString())
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .setIngestionConfig(ingestionConfig);
        if (crawlId != null && !crawlId.isEmpty()) {
            metaBuilder.setCrawlId(crawlId);
        }

        ai.pipestream.data.v1.DocumentReference docRef = ai.pipestream.data.v1.DocumentReference.newBuilder()
                .setDocId(docId)
                .setGraphAddressId(sourceNodeId)
                .setAccountId(accountId)
                .build();

        PipeStream pipeStream = PipeStream.newBuilder()
                .setStreamId(UUID.randomUUID().toString())
                .setMetadata(metaBuilder.build())
                .setDocumentRef(docRef)
                .setHopCount(0)
                .build();

        return IntakeHandoffRequest.newBuilder()
                .setStream(pipeStream)
                .setDatasourceId(datasourceId)
                .setAccountId(accountId)
                .build();
    }
}
