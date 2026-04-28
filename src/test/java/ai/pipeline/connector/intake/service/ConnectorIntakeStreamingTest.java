package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DocReference;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.connector.intake.v1.PipeDocItem;
import ai.pipestream.connector.intake.v1.StreamContext;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipeline.connector.intake.queue.IntakeJobQueue;
import ai.pipestream.data.v1.PipeStream;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConnectorIntakeServiceImpl#uploadPipeDocStream},
 * the bidi streaming RPC handler.
 * <p>
 * Mocks {@link EngineClient} so each test can deterministically control
 * what the engine handoff returns (success / RESOURCE_EXHAUSTED /
 * UNAVAILABLE / validation error / arbitrary throwable). The actual
 * Mutiny-stream protocol shape is exercised by feeding a
 * {@link Multi}{@code <Request>} into the handler and collecting the
 * resulting {@link Multi}{@code <Response>}.
 * <p>
 * Covers, from §8 of the design doc and the GPT review's open questions:
 * <ul>
 *   <li>happy path — context first, then N items, server acks each in order</li>
 *   <li>missing context — server rejects items before context</li>
 *   <li>context rejection — bad api_key surfaces as failed context ack</li>
 *   <li>non-durable engine handoff failures happen after the connector ack</li>
 *   <li>engine handoff start failure → ack retryable=false</li>
 *   <li>delete_ref payload → not-implemented ack</li>
 *   <li>unrecognized payload → not-implemented ack</li>
 * </ul>
 */
@QuarkusTest
@QuarkusTestResource(ConnectorIntakeWireMockTestResource.class)
class ConnectorIntakeStreamingTest {

    @Inject
    @GrpcService
    ConnectorIntakeServiceImpl intakeService;

    @InjectMock
    EngineClient engineClient;

    @InjectMock
    ConfigResolutionService configResolutionService;

    // The streaming-handler unit tests are not exercising real Redis; mock
    // the queue producer so submit() is a no-op LPUSH-equivalent. The
    // queue's own behaviour is covered by IntakeJobWorkerTest against a
    // dev-services Redis.
    @InjectMock
    IntakeJobQueue intakeJobQueue;

    @BeforeEach
    void setUp() {
        DataSourceConfig tier1Config = DataSourceConfig.newBuilder()
                .setDatasourceId("valid-datasource")
                .setAccountId("valid-account")
                .setConnectorId("test-connector")
                .build();
        ConfigResolutionService.ResolvedConfig resolvedConfig =
                new ConfigResolutionService.ResolvedConfig(tier1Config, IngestionConfig.getDefaultInstance());
        when(configResolutionService.resolveConfig(anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(resolvedConfig));

        // Default: build a handoff request whose stream carries the
        // submitted PipeDoc (and a placeholder stream_id). Tests that
        // care about specific fields can override.
        when(engineClient.buildHandoffRequest(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), any()))
                .thenAnswer(inv -> {
                    PipeDoc doc = inv.getArgument(0);
                    return IntakeHandoffRequest.newBuilder()
                            .setStream(PipeStream.newBuilder()
                                    .setStreamId("test-stream-" + doc.getDocId())
                                    .setDocument(doc)
                                    .build())
                            .setDatasourceId(inv.getArgument(1))
                            .setAccountId(inv.getArgument(2))
                            .build();
                });
    }

    private static StreamContext validContext() {
        return StreamContext.newBuilder()
                .setDatasourceId("valid-datasource")
                .setApiKey("valid-api-key")
                .setCrawlId("test-crawl")
                .setSubCrawlIndex(0)
                .setTotalSubCrawls(1)
                .setClientId("test-client")
                .build();
    }

    private static UploadPipeDocStreamRequest contextMessage(StreamContext ctx) {
        return UploadPipeDocStreamRequest.newBuilder().setContext(ctx).build();
    }

    private static UploadPipeDocStreamRequest itemMessage(String sourceDocId) {
        return UploadPipeDocStreamRequest.newBuilder()
                .setItem(PipeDocItem.newBuilder()
                        .setSourceDocId(sourceDocId)
                        .setPipeDoc(PipeDoc.newBuilder().setDocId(sourceDocId).build())
                        .build())
                .build();
    }

    private static IntakeHandoffResponse engineAccepted(String streamId, String entryNode) {
        return IntakeHandoffResponse.newBuilder()
                .setAccepted(true)
                .setAssignedStreamId(streamId)
                .setEntryNodeId(entryNode)
                .setMessage("queued")
                .build();
    }

    private List<UploadPipeDocStreamResponse> runStream(Multi<UploadPipeDocStreamRequest> requests) {
        return intakeService.uploadPipeDocStream(requests)
                .collect().asList()
                .await().indefinitely();
    }

    private List<UploadPipeDocStreamResponse> runStreamAtMost(
            Multi<UploadPipeDocStreamRequest> requests, Duration timeout) {
        return intakeService.uploadPipeDocStream(requests)
                .collect().asList()
                .await().atMost(timeout);
    }

    // ============================================================
    // Happy path
    // ============================================================

    @Test
    @DisplayName("happy path: context accepted, then N items each acked with retryable=false and matching DocReference")
    void uploadPipeDocStream_happyPath_acksContextThenEachItemInOrder() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().item(engineAccepted("stream-1", "tika-parser")));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-1"),
                itemMessage("src-2"),
                itemMessage("src-3"));

        List<UploadPipeDocStreamResponse> responses = runStream(requests);

        assertEquals(4, responses.size(), "expected 1 context ack + 3 per-item acks");

        UploadPipeDocStreamResponse ctxAck = responses.get(0);
        assertTrue(ctxAck.getSuccess(), "context ack should succeed");
        assertEquals("", ctxAck.getRef().getSourceDocId(),
                "context ack carries no DocReference (no doc yet)");

        for (int i = 0; i < 3; i++) {
            UploadPipeDocStreamResponse ack = responses.get(i + 1);
            assertTrue(ack.getSuccess(), "item ack " + i + " should succeed");
            assertEquals("src-" + (i + 1), ack.getRef().getSourceDocId(),
                    "ack echoes source_doc_id for client correlation");
            assertFalse(ack.getRetryable(), "happy path is never retryable");
        }
    }

    @Test
    @DisplayName("non-durable stream item acks after intake acceptance, not after engine handoff completion")
    void uploadPipeDocStream_nonDurableItemAckDoesNotWaitForEngineHandoff() {
        CompletableFuture<IntakeHandoffResponse> neverCompletes = new CompletableFuture<>();
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().completionStage(neverCompletes));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-accepted-before-engine"));

        List<UploadPipeDocStreamResponse> responses = runStreamAtMost(requests, Duration.ofMillis(500));

        assertEquals(2, responses.size(), "expected context ack + item ack without waiting for engine");
        UploadPipeDocStreamResponse ack = responses.get(1);
        assertTrue(ack.getSuccess(), "valid non-durable item should be acked once intake accepts it");
        assertFalse(ack.getRetryable(), "JDBC must not retry downstream engine uncertainty");
        assertEquals("src-accepted-before-engine", ack.getRef().getSourceDocId());
    }

    // ============================================================
    // Validation / protocol errors — server-only, no engine call needed
    // ============================================================

    @Test
    @DisplayName("missing context: items sent before StreamContext are rejected with retryable=false")
    void uploadPipeDocStream_itemBeforeContext_rejectsWithRetryableFalse() {
        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                itemMessage("src-orphan"));

        List<UploadPipeDocStreamResponse> responses = runStream(requests);

        assertEquals(1, responses.size());
        UploadPipeDocStreamResponse ack = responses.get(0);
        assertFalse(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "missing-context is a protocol error, retrying same payload won't help");
        assertTrue(ack.getMessage().toLowerCase().contains("first message must be streamcontext"),
                "message should explain the expected first-message protocol");
    }

    @Test
    @DisplayName("delete_ref payload: acked with retryable=false and not-implemented note (v1 contract)")
    void uploadPipeDocStream_deleteRefPayload_returnsNotImplementedAck() {
        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                UploadPipeDocStreamRequest.newBuilder()
                        .setDeleteRef(DocReference.newBuilder()
                                .setSourceDocId("src-to-delete")
                                .build())
                        .build());

        List<UploadPipeDocStreamResponse> responses = runStream(requests);

        assertEquals(2, responses.size());
        UploadPipeDocStreamResponse deleteAck = responses.get(1);
        assertFalse(deleteAck.getSuccess(), "v1 doesn't implement streamed deletes");
        assertFalse(deleteAck.getRetryable(),
                "not-implemented is permanent, not retryable");
        assertTrue(deleteAck.getMessage().toLowerCase().contains("delete via stream not yet implemented"),
                "ack must guide caller back to the unary DeletePipeDoc RPC");
    }

    // ============================================================
    // Async engine handoff outcomes — non-durable intake owns the connector ack
    // ============================================================

    @Test
    @DisplayName("non-durable async RESOURCE_EXHAUSTED does not make JDBC retry")
    void uploadPipeDocStream_engineResourceExhausted_setsRetryableTrue() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.RESOURCE_EXHAUSTED
                                .withDescription("engine queue full for module 'chunker'")
                                .asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-overloaded"));

        List<UploadPipeDocStreamResponse> responses = runStream(requests);

        UploadPipeDocStreamResponse ack = responses.get(1);
        assertTrue(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "after non-durable intake accepts the item, downstream engine uncertainty must not make JDBC retry");
        assertEquals("src-overloaded", ack.getRef().getSourceDocId());
    }

    @Test
    @DisplayName("non-durable async UNAVAILABLE does not make JDBC retry")
    void uploadPipeDocStream_engineUnavailable_setsRetryableTrue() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.UNAVAILABLE.withDescription("engine restarting").asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-mid-restart"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertTrue(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "engine unavailability discovered after intake acceptance is logged, not retried by JDBC");
    }

    @Test
    @DisplayName("non-durable async DEADLINE_EXCEEDED does not make JDBC retry")
    void uploadPipeDocStream_engineDeadlineExceeded_setsRetryableTrue() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.DEADLINE_EXCEEDED.withDescription("engine slow").asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-slow"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertTrue(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "DEADLINE_EXCEEDED after intake acceptance has unknown outcome and must not be retried by JDBC");
    }

    @Test
    @DisplayName("non-durable async ABORTED does not make JDBC retry")
    void uploadPipeDocStream_engineAborted_setsRetryableTrue() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.ABORTED.withDescription("queue eviction race").asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-aborted"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertTrue(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "after intake acceptance, ABORTED is a downstream best-effort failure, not a connector retry signal");
    }

    @Test
    @DisplayName("non-durable async INVALID_ARGUMENT does not change accepted connector ack")
    void uploadPipeDocStream_engineInvalidArgument_setsRetryableFalse() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.INVALID_ARGUMENT.withDescription("doc_id is blank").asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-bad"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertTrue(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "the connector ack is based on intake validation, not downstream engine validation");
    }

    @Test
    @DisplayName("non-durable async non-Status exception does not change accepted connector ack")
    void uploadPipeDocStream_engineNonStatusException_setsRetryableFalse() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("graph cache corrupt")));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-state"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertTrue(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "async engine failures after intake acceptance are logged, not returned to JDBC");
    }

    @Test
    @DisplayName("non-durable async accepted=false does not change accepted connector ack")
    void uploadPipeDocStream_engineRejected_propagatesMessage() {
        // Engine's DROPPED_NO_PIPELINE returns accepted=true after the
        // recent fix (review #1), but other accepted=false paths exist
        // (e.g. NOT_FOUND on override target_entry_node_id). We assert
        // that whatever the engine says about accepted is what the
        // streaming response says about success.
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().item(IntakeHandoffResponse.newBuilder()
                        .setAccepted(false)
                        .setMessage("entry node 'nope' not in graph 'test-graph'")
                        .build()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-nope"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertTrue(ack.getSuccess(), "intake acceptance is the connector ack boundary in non-durable mode");
        assertFalse(ack.getRetryable(),
                "engine rejection after acceptance is not a JDBC retry signal");
        assertTrue(ack.getMessage().contains("accepted by intake"),
                "ack message should describe the intake receipt boundary");
    }

    @Test
    @DisplayName("intake queue submit failure → bidi stream errors out (no swallow)")
    void uploadPipeDocStream_queueSubmitThrows_failsTheStream() {
        // When the Redis-backed intake queue cannot accept a submit (e.g.
        // Redis unreachable), intake cannot keep its ownership contract.
        // Translating that into a per-item "retryable=true" ack would bury
        // a real environment failure; instead the exception propagates
        // through the Mutiny chain so the bidi stream fails. The connector
        // sees a transport-level error and surfaces it loudly.
        org.mockito.Mockito.doThrow(new RuntimeException("redis unreachable"))
                .when(intakeJobQueue).submit(any(IntakeHandoffRequest.class));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-redis-down"));

        try {
            runStream(requests);
            org.junit.jupiter.api.Assertions.fail("expected the stream to error out");
        } catch (RuntimeException e) {
            // Walk the cause chain — Mutiny may wrap.
            Throwable cur = e;
            boolean foundRedis = false;
            while (cur != null) {
                if (cur.getMessage() != null && cur.getMessage().contains("redis unreachable")) {
                    foundRedis = true;
                    break;
                }
                cur = cur.getCause();
            }
            assertTrue(foundRedis,
                    "queue submit failure should propagate as the stream's terminal error, got: " + e);
        }
    }

    // ============================================================
    // Mixed streams — one stream, multiple outcomes
    // ============================================================

    @Test
    @DisplayName("mixed stream: async engine failures do not break send-order intake acks")
    void uploadPipeDocStream_mixedAcks_preservesOrderAndRetryablePerDoc() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().item(engineAccepted("stream-1", "tika-parser")))
                .thenReturn(Uni.createFrom().failure(
                        Status.RESOURCE_EXHAUSTED.withDescription("queue full").asRuntimeException()))
                .thenReturn(Uni.createFrom().item(engineAccepted("stream-3", "tika-parser")));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-ok-1"),
                itemMessage("src-overloaded"),
                itemMessage("src-ok-2"));

        List<UploadPipeDocStreamResponse> responses = runStream(requests);

        assertEquals(4, responses.size());

        // Item 1: success
        assertTrue(responses.get(1).getSuccess());
        assertFalse(responses.get(1).getRetryable());
        assertEquals("src-ok-1", responses.get(1).getRef().getSourceDocId());

        // Item 2: still accepted by intake; downstream failure is logged asynchronously
        assertTrue(responses.get(2).getSuccess());
        assertFalse(responses.get(2).getRetryable());
        assertEquals("src-overloaded", responses.get(2).getRef().getSourceDocId());

        // Item 3: success — proves the stream keeps processing after a transient failure
        assertTrue(responses.get(3).getSuccess());
        assertFalse(responses.get(3).getRetryable());
        assertEquals("src-ok-2", responses.get(3).getRef().getSourceDocId());
    }
}
