package ai.pipeline.connector.intake.service;

import ai.pipestream.data.v1.DocumentReference;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

/**
 * Client for engine service integration. Every call goes through
 * {@link IntakeHandoffStreamClient}'s long-lived bidi stream — no per-call
 * unary RPC, no Quarkus {@code BlockingServerInterceptor.VirtualReplayListener}
 * race surface on the hot path.
 */
@ApplicationScoped
public class EngineClient {

    private static final Logger LOG = Logger.getLogger(EngineClient.class);

    @Inject
    IntakeHandoffStreamClient intakeStreamClient;

    /**
     * Default constructor for CDI proxying.
     */
    public EngineClient() {}

    /**
     * Hands off a document to the engine without a crawl ID.
     *
     * @param pipeDoc The document to hand off
     * @param datasourceId The identifier of the datasource
     * @param accountId The identifier of the account
     * @param ingestionConfig The ingestion configuration
     * @return The engine's response to the handoff request
     */
    public IntakeHandoffResponse handoffToEngine(
            PipeDoc pipeDoc,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig) {
        return handoffToEngine(pipeDoc, datasourceId, accountId, ingestionConfig, null);
    }

    /**
     * Hands off a document to the engine with an optional crawl ID.
     *
     * @param pipeDoc The document to hand off
     * @param datasourceId The identifier of the datasource
     * @param accountId The identifier of the account
     * @param ingestionConfig The ingestion configuration
     * @param crawlId The identifier of the crawl session (optional)
     * @return The engine's response to the handoff request
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
     * Hands off a document reference to the engine without a crawl ID.
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
     * Hands off a document reference to the engine with an optional crawl ID.
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
