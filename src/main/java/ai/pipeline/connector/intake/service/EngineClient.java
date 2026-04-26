package ai.pipeline.connector.intake.service;

import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.engine.v1.MutinyEngineV1ServiceGrpc;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import ai.pipestream.server.constants.PipestreamServices;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.function.Supplier;

import java.time.Instant;
import java.util.UUID;

/**
 * Client for engine service integration.
 * <p>
 * Handles handoff of documents from intake to engine for pipeline processing.
 * <p>
 * Design: Intake is graph-agnostic. We only pass datasource_id and account_id.
 * Engine handles all graph resolution, Tier 2 config merging, and routing to
 * the correct entry node(s). This keeps intake simple and decoupled from
 * graph internals.
 */
@ApplicationScoped
public class EngineClient {

    private static final Logger LOG = Logger.getLogger(EngineClient.class);
    private static final String ENGINE_SERVICE_NAME = PipestreamServices.ENGINE.serviceName();

    @Inject
    DynamicGrpcClientFactory grpcClientFactory;

    /**
     * Default constructor for CDI.
     */
    public EngineClient() {}

    /**
     * Hand off a document to the engine for pipeline processing.
     * <p>
     * Wraps the PipeDoc in a PipeStream with proper metadata and calls
     * the engine's IntakeHandoff RPC. Engine resolves graph routing based
     * on datasource_id.
     *
     * @param pipeDoc The document to process
     * @param datasourceId The datasource ID for routing (engine resolves entry node)
     * @param accountId The account ID for ownership
     * @param ingestionConfig The Tier 1 ingestion configuration (engine merges Tier 2)
     * @return Uni emitting the handoff response
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

        LOG.debugf("Handing off document to engine: doc_id=%s, datasource=%s",
            pipeDoc.getDocId(), datasourceId);

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
        StreamMetadata metadata = metaBuilder.build();

        // Build PipeStream with inline document
        PipeStream pipeStream = PipeStream.newBuilder()
            .setStreamId(UUID.randomUUID().toString())
            .setMetadata(metadata)
            .setDocument(pipeDoc)
            .setHopCount(0)
            .build();

        // Build handoff request - no entry node specified, engine resolves from datasource
        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
            .setStream(pipeStream)
            .setDatasourceId(datasourceId)
            .setAccountId(accountId)
            .build();

        return underRootContext(() ->
                grpcClientFactory.getClient(ENGINE_SERVICE_NAME, MutinyEngineV1ServiceGrpc::newMutinyStub)
                        .flatMap(stub -> stub.intakeHandoff(request)))
            .invoke(response -> {
                if (response.getAccepted()) {
                    LOG.debugf("Engine accepted document: doc_id=%s, stream_id=%s, entry_node=%s",
                        pipeDoc.getDocId(), response.getAssignedStreamId(), response.getEntryNodeId());
                } else {
                    LOG.warnf("Engine rejected document: doc_id=%s, message=%s",
                        pipeDoc.getDocId(), response.getMessage());
                }
            });
    }

    /**
     * Hand off a document reference to the engine.
     * <p>
     * Used when the document was persisted to repository and only a reference is passed.
     * Engine will hydrate the document when needed based on ingestion config.
     *
     * @param docId The document ID (in repository)
     * @param sourceNodeId Graph address for repository hydration ({@code DocumentReference.graph_address_id});
     *                     for initial intake this matches {@code datasource_id}
     * @param datasourceId The datasource ID for routing (engine resolves entry node)
     * @param accountId The account ID for ownership
     * @param ingestionConfig The Tier 1 ingestion configuration (engine merges Tier 2)
     * @return Uni emitting the handoff response
     */
    public Uni<IntakeHandoffResponse> handoffReferenceToEngine(
            String docId,
            String sourceNodeId,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig) {
        return handoffReferenceToEngine(docId, sourceNodeId, datasourceId, accountId, ingestionConfig, null);
    }

    public Uni<IntakeHandoffResponse> handoffReferenceToEngine(
            String docId,
            String sourceNodeId,
            String datasourceId,
            String accountId,
            IngestionConfig ingestionConfig,
            String crawlId) {

        LOG.debugf("Handing off document reference to engine: doc_id=%s, datasource=%s",
            docId, datasourceId);

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
        StreamMetadata metadata = metaBuilder.build();

        // Build DocumentReference
        ai.pipestream.data.v1.DocumentReference docRef = ai.pipestream.data.v1.DocumentReference.newBuilder()
            .setDocId(docId)
            .setGraphAddressId(sourceNodeId)
            .setAccountId(accountId)
            .build();

        // Build PipeStream with document reference (not inline)
        PipeStream pipeStream = PipeStream.newBuilder()
            .setStreamId(UUID.randomUUID().toString())
            .setMetadata(metadata)
            .setDocumentRef(docRef)
            .setHopCount(0)
            .build();

        // Build handoff request - no entry node specified, engine resolves from datasource
        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
            .setStream(pipeStream)
            .setDatasourceId(datasourceId)
            .setAccountId(accountId)
            .build();

        return underRootContext(() ->
                grpcClientFactory.getClient(ENGINE_SERVICE_NAME, MutinyEngineV1ServiceGrpc::newMutinyStub)
                        .flatMap(stub -> stub.intakeHandoff(request)))
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
     * Subscribes to {@code supplier}'s Uni under {@link Context#ROOT} so the
     * outbound gRPC client call captures ROOT as its parent Context instead of
     * the inbound RPC handler's Context.
     * <p>
     * Without this, every {@code uploadPipeDoc} that fans out to
     * {@code engine.intakeHandoff} carries the inbound UploadPipeDoc handler's
     * Context as the parent of the outbound call. As soon as the upstream
     * Mutiny chain emits the response and the inbound handler's Context cancels
     * — under load this happens with calls still in flight — every outbound
     * call rooted at it dies with
     * {@code CANCELLED: io.grpc.Context was cancelled without error}.
     * Observed under JDBC transport-test crawls: 75 of 100 docs cancelled
     * within a single 230 ms window once the engine channel finished
     * resolving. ROOT never cancels, so the outbound call survives whatever
     * the inbound handler's Context does next.
     */
    private static <T> Uni<T> underRootContext(Supplier<Uni<T>> supplier) {
        return Uni.createFrom().<T>emitter(emitter -> {
            Context previous = Context.ROOT.attach();
            try {
                supplier.get().subscribe().with(emitter::complete, emitter::fail);
            } finally {
                Context.ROOT.detach(previous);
            }
        });
    }
}
