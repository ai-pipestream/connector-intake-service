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
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Client for engine service integration. Uses standard async gRPC stubs.
 */
@ApplicationScoped
public class EngineClient {

    private static final Logger LOG = Logger.getLogger(EngineClient.class);

    @GrpcClient("engine")
    Channel engineChannel;

    @ConfigProperty(name = "pipestream.intake.engine.deadline-ms", defaultValue = "30000")
    long deadlineMs;

    public EngineClient() {}

    public IntakeHandoffResponse handoffToEngine(
            PipeDoc pipeDoc,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig) {
        return handoffToEngine(pipeDoc, datasourceId, accountId, ingestionConfig, null);
    }

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

    public IntakeHandoffResponse handoffReferenceToEngine(
            String docId,
            String sourceNodeId,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig) {
        return handoffReferenceToEngine(docId, sourceNodeId, datasourceId, accountId, ingestionConfig, null);
    }

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

    public IntakeHandoffResponse intakeHandoff(IntakeHandoffRequest request) {
        CompletableFuture<IntakeHandoffResponse> future = new CompletableFuture<>();
        EngineV1ServiceGrpc.EngineV1ServiceStub stub = EngineV1ServiceGrpc.newStub(engineChannel)
                .withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
        try {
            Context.ROOT.run(() -> stub.intakeHandoff(request, new StreamObserver<>() {
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
                    // unary response already completed in onNext
                }
            }));
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }
        return awaitUnary(future, "engine IntakeHandoff");
    }

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
