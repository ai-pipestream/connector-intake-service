package ai.pipeline.connector.intake.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import ai.pipestream.engine.v1.EngineV1ServiceGrpc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import com.google.protobuf.Timestamp;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.UUID;

/**
 * Builds engine intake handoff requests and sends them over the configured
 * engine transport.
 *
 * <p>The normal path uses {@link IntakeHandoffStreamClient}'s long-lived bidi
 * stream, which avoids the per-call unary listener allocation pattern that was
 * observed to drop requests under load. The legacy unary RPC remains available
 * only for compatibility deployments where inline PipeDoc uploads must talk to
 * an older engine.
 */
@ApplicationScoped
public class EngineClient {

    private static final Logger LOG = Logger.getLogger(EngineClient.class);

    @Inject
    IntakeHandoffStreamClient intakeStreamClient;

    @GrpcClient("engine")
    Channel engineChannel;

    @ConfigProperty(name = "pipestream.intake.engine.handoff-unary-timeout-ms", defaultValue = "120000")
    long unaryTimeoutMs;

    /**
     * Default constructor for CDI proxying.
     */
    public EngineClient() {}

    /**
     * Hands off an inline document through the streaming engine path without a
     * crawl ID.
     *
     * @param pipeDoc document to hand off
     * @param datasourceId datasource that owns the document
     * @param accountId account that owns the datasource
     * @param ingestionConfig Tier 1 ingestion configuration stamped by intake
     * @return engine admission response
     */
    public IntakeHandoffResponse handoffToEngine(
            PipeDoc pipeDoc,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig) {
        return handoffToEngine(pipeDoc, datasourceId, accountId, ingestionConfig, null);
    }

    /**
     * Hands off an inline document through the streaming engine path.
     *
     * @param pipeDoc document to hand off
     * @param datasourceId datasource that owns the document
     * @param accountId account that owns the datasource
     * @param ingestionConfig Tier 1 ingestion configuration stamped by intake
     * @param crawlId optional crawl session identifier
     * @return engine admission response
     */
    public IntakeHandoffResponse handoffToEngine(
            PipeDoc pipeDoc,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig,
            String crawlId) {
        IntakeHandoffRequest request = buildHandoffRequest(pipeDoc, datasourceId, accountId,
                ingestionConfig, crawlId);
        IntakeHandoffResponse response = intakeHandoff(request);
        if (response.getAccepted()) {
            LOG.debugf("Engine accepted document: doc_id=%s, stream_id=%s, entry_node=%s",
                    pipeDoc.getDocId(), response.getAssignedStreamId(), response.getEntryNodeId());
        } else {
            LOG.warnf("Engine rejected document: doc_id=%s, message=%s",
                    pipeDoc.getDocId(), response.getMessage());
        }
        return response;
    }

    /**
     * Hands off an inline document to the engine through the legacy unary RPC.
     *
     * <p>This is intended for compatibility deployments where inline gRPC
     * PipeDoc uploads should bypass Redis ingress and the target engine still
     * expects {@code EngineV1Service/IntakeHandoff}.
     *
     * @param pipeDoc document to hand off
     * @param datasourceId datasource that owns the document
     * @param accountId account that owns the datasource
     * @param ingestionConfig Tier 1 ingestion configuration stamped by intake
     * @param crawlId optional crawl session identifier
     * @return engine admission response
     */
    public IntakeHandoffResponse handoffToEngineUnary(
            PipeDoc pipeDoc,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig,
            String crawlId) {
        IntakeHandoffRequest request = buildHandoffRequest(pipeDoc, datasourceId, accountId,
                ingestionConfig, crawlId);
        IntakeHandoffResponse response = intakeHandoffUnary(request);
        if (response.getAccepted()) {
            LOG.debugf("Engine unary accepted document: doc_id=%s, stream_id=%s, entry_node=%s",
                    pipeDoc.getDocId(), response.getAssignedStreamId(), response.getEntryNodeId());
        } else {
            LOG.warnf("Engine unary rejected document: doc_id=%s, message=%s",
                    pipeDoc.getDocId(), response.getMessage());
        }
        return response;
    }

    /**
     * Hands off a repository-staged document reference to engine without a
     * crawl ID.
     *
     * @param docId The Pipestream internal document identifier
     * @param sourceNodeId The identifier of the source node
     * @param datasourceId The identifier of the datasource
     * @param accountId The identifier of the account
     * @param ingestionConfig The ingestion configuration
     * @return The engine's response to the handoff request
     */
    public IntakeHandoffResponse handoffReferenceToEngine(
            String docId,
            String sourceNodeId,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig) {
        return handoffReferenceToEngine(docId, sourceNodeId, datasourceId, accountId, ingestionConfig, null);
    }

    /**
     * Hands off a repository-staged document reference to engine.
     *
     * @param docId The Pipestream internal document identifier
     * @param sourceNodeId The identifier of the source node
     * @param datasourceId The identifier of the datasource
     * @param accountId The identifier of the account
     * @param ingestionConfig The ingestion configuration
     * @param crawlId The identifier of the crawl session (optional)
     * @return The engine's response to the handoff request
     */
    public IntakeHandoffResponse handoffReferenceToEngine(
            String docId,
            String sourceNodeId,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig,
            String crawlId) {
        IntakeHandoffRequest request = buildRequestForRef(docId, sourceNodeId, datasourceId,
                accountId, ingestionConfig, crawlId);
        IntakeHandoffResponse response = intakeHandoff(request);
        if (response.getAccepted()) {
            LOG.debugf("Engine accepted document ref: doc_id=%s, stream_id=%s, entry_node=%s",
                    docId, response.getAssignedStreamId(), response.getEntryNodeId());
        } else {
            LOG.warnf("Engine rejected document ref: doc_id=%s, message=%s", docId, response.getMessage());
        }
        return response;
    }

    /**
     * Hands off one document to the engine.
     *
     * <p>Delegates to {@link IntakeHandoffStreamClient}, which holds a single
     * long-lived bidi stream open to the engine and multiplexes per-doc
     * requests over it. Replaced the prior per-call unary
     * {@code stub.intakeHandoff(req, observer)} pattern: that path went
     * through Quarkus's {@code BlockingServerInterceptor.VirtualReplayListener}
     * on every request and was observed to drop ~5/1000 docs under load
     * with {@code INTERNAL: Half-closed without a request}. The bidi RPC
     * avoids the per-call buffer-allocation pattern that triggers the
     * drops.
     *
     * <p>Method signature unchanged from the unary version — every existing
     * caller ({@code handoffToEngine}, {@code handoffReferenceToEngine},
     * blob upload, IntakeRepoEventConsumer) automatically becomes bidi.
     *
     * @param request The handoff request to send to the engine
     * @return The engine's response to the handoff request
     */
    public IntakeHandoffResponse intakeHandoff(IntakeHandoffRequest request) {
        return intakeStreamClient.intakeHandoff(request);
    }

    /**
     * Sends one already-built intake handoff request over the legacy unary
     * engine RPC.
     */
    public IntakeHandoffResponse intakeHandoffUnary(IntakeHandoffRequest request) {
        CompletableFuture<IntakeHandoffResponse> future = new CompletableFuture<>();
        EngineV1ServiceGrpc.newStub(engineChannel).intakeHandoff(request, new StreamObserver<>() {
            @Override
            public void onNext(IntakeHandoffResponse value) {
                future.complete(value);
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                // unary response is completed in onNext
            }
        });
        return awaitUnary(future);
    }

    private IntakeHandoffResponse awaitUnary(CompletableFuture<IntakeHandoffResponse> future) {
        try {
            return future.get(unaryTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw Status.DEADLINE_EXCEEDED
                    .withDescription("engine unary intake handoff exceeded " + unaryTimeoutMs + "ms")
                    .withCause(e)
                    .asRuntimeException();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("engine unary intake handoff interrupted")
                    .withCause(e)
                    .asRuntimeException();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                throw sre;
            }
            throw Status.UNKNOWN
                    .withDescription("engine unary intake handoff failed: " + cause.getMessage())
                    .withCause(cause)
                    .asRuntimeException();
        }
    }

    /**
     * Build an IntakeHandoffRequest for an inline document.
     *
     * @param pipeDoc The document to include in the request
     * @param datasourceId The identifier of the datasource
     * @param accountId The identifier of the account
     * @param ingestionConfig The ingestion configuration
     * @param crawlId The identifier of the crawl session (optional)
     * @return A constructed IntakeHandoffRequest
     */
    public static IntakeHandoffRequest buildHandoffRequest(PipeDoc pipeDoc, String datasourceId,
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

        DocumentReference docRef = DocumentReference.newBuilder()
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
