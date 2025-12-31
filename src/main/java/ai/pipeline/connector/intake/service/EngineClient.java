package ai.pipeline.connector.intake.service;

import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.data.v1.StreamMetadata;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.engine.v1.MutinyEngineV1ServiceGrpc;
import ai.pipestream.quarkus.dynamicgrpc.DynamicGrpcClientFactory;
import com.google.protobuf.Timestamp;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

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
    private static final String ENGINE_SERVICE_NAME = "pipestream-engine";

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

        LOG.debugf("Handing off document to engine: doc_id=%s, datasource=%s",
            pipeDoc.getDocId(), datasourceId);

        // Build StreamMetadata with Tier 1 ingestion config
        // Engine will merge Tier 2 overrides during processing
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
            .setSeconds(now.getEpochSecond())
            .setNanos(now.getNano())
            .build();

        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setSourceId(datasourceId)
            .setCreatedAt(timestamp)
            .setLastProcessedAt(timestamp)
            .setTraceId(UUID.randomUUID().toString())
            .setDatasourceId(datasourceId)
            .setAccountId(accountId)
            .setIngestionConfig(ingestionConfig)
            .build();

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

        return grpcClientFactory.getClient(ENGINE_SERVICE_NAME, MutinyEngineV1ServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.intakeHandoff(request))
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
     * @param sourceNodeId The node that produced the document (for hydration lookup)
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

        LOG.debugf("Handing off document reference to engine: doc_id=%s, datasource=%s",
            docId, datasourceId);

        // Build StreamMetadata with Tier 1 ingestion config
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
            .setSeconds(now.getEpochSecond())
            .setNanos(now.getNano())
            .build();

        StreamMetadata metadata = StreamMetadata.newBuilder()
            .setSourceId(datasourceId)
            .setCreatedAt(timestamp)
            .setLastProcessedAt(timestamp)
            .setTraceId(UUID.randomUUID().toString())
            .setDatasourceId(datasourceId)
            .setAccountId(accountId)
            .setIngestionConfig(ingestionConfig)
            .build();

        // Build DocumentReference
        ai.pipestream.data.v1.DocumentReference docRef = ai.pipestream.data.v1.DocumentReference.newBuilder()
            .setDocId(docId)
            .setSourceNodeId(sourceNodeId)
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

        return grpcClientFactory.getClient(ENGINE_SERVICE_NAME, MutinyEngineV1ServiceGrpc::newMutinyStub)
            .flatMap(stub -> stub.intakeHandoff(request))
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
}
