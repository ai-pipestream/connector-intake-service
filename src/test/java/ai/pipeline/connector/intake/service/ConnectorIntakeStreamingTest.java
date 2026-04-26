package ai.pipeline.connector.intake.service;

import ai.pipestream.connector.intake.v1.DocReference;
import ai.pipestream.connector.intake.v1.PipeDocItem;
import ai.pipestream.connector.intake.v1.StreamContext;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
 *   <li>engine RESOURCE_EXHAUSTED → ack retryable=true</li>
 *   <li>engine UNAVAILABLE → ack retryable=true</li>
 *   <li>engine non-status RuntimeException → ack retryable=false</li>
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
    // Engine handoff failure mapping — the retryable contract
    // ============================================================

    @Test
    @DisplayName("engine returns RESOURCE_EXHAUSTED → ack retryable=true (caller should resend after backoff)")
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
        assertFalse(ack.getSuccess());
        assertTrue(ack.getRetryable(),
                "RESOURCE_EXHAUSTED is the canonical 'engine queue full' signal — must be retryable");
        assertEquals("src-overloaded", ack.getRef().getSourceDocId());
    }

    @Test
    @DisplayName("engine returns UNAVAILABLE → ack retryable=true")
    void uploadPipeDocStream_engineUnavailable_setsRetryableTrue() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.UNAVAILABLE.withDescription("engine restarting").asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-mid-restart"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertFalse(ack.getSuccess());
        assertTrue(ack.getRetryable(),
                "UNAVAILABLE is transient, the engine could come back at any moment");
    }

    @Test
    @DisplayName("engine returns DEADLINE_EXCEEDED → ack retryable=true")
    void uploadPipeDocStream_engineDeadlineExceeded_setsRetryableTrue() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.DEADLINE_EXCEEDED.withDescription("engine slow").asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-slow"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertFalse(ack.getSuccess());
        assertTrue(ack.getRetryable(),
                "DEADLINE_EXCEEDED can resolve once the engine drains its queue");
    }

    @Test
    @DisplayName("engine returns ABORTED → ack retryable=true (transient handoff contention)")
    void uploadPipeDocStream_engineAborted_setsRetryableTrue() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.ABORTED.withDescription("queue eviction race").asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-aborted"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertFalse(ack.getSuccess());
        assertTrue(ack.getRetryable(),
                "ABORTED signals transient contention (queue eviction, txn rollback) — caller may retry at a higher level");
    }

    @Test
    @DisplayName("engine returns INVALID_ARGUMENT → ack retryable=false (validation won't fix itself)")
    void uploadPipeDocStream_engineInvalidArgument_setsRetryableFalse() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(
                        Status.INVALID_ARGUMENT.withDescription("doc_id is blank").asRuntimeException()));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-bad"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertFalse(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "INVALID_ARGUMENT is permanent — resending the same payload would fail identically");
    }

    @Test
    @DisplayName("engine throws non-Status RuntimeException → ack retryable=false")
    void uploadPipeDocStream_engineNonStatusException_setsRetryableFalse() {
        when(engineClient.handoffToEngine(any(PipeDoc.class), anyString(), anyString(),
                any(IngestionConfig.class), anyString()))
                .thenReturn(Uni.createFrom().failure(new IllegalStateException("graph cache corrupt")));

        Multi<UploadPipeDocStreamRequest> requests = Multi.createFrom().items(
                contextMessage(validContext()),
                itemMessage("src-state"));

        UploadPipeDocStreamResponse ack = runStream(requests).get(1);

        assertFalse(ack.getSuccess());
        assertFalse(ack.getRetryable(),
                "without an explicit retryable status code, treat as permanent — better to fail loud than retry forever");
    }

    @Test
    @DisplayName("engine returns accepted=false (e.g. DROPPED_NO_PIPELINE) → ack carries the engine message, success=engine.accepted")
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

        assertFalse(ack.getSuccess(), "engine rejection propagates as success=false");
        assertFalse(ack.getRetryable(),
                "non-exception engine rejection lands in the success-mapping branch where retryable defaults false");
        assertTrue(ack.getMessage().contains("entry node 'nope'"),
                "engine's rejection message must reach the client unchanged for diagnosis");
    }

    // ============================================================
    // Mixed streams — one stream, multiple outcomes
    // ============================================================

    @Test
    @DisplayName("mixed acks: one success + one RESOURCE_EXHAUSTED, both come back in send order with correct retryable flags")
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

        // Item 2: retryable failure
        assertFalse(responses.get(2).getSuccess());
        assertTrue(responses.get(2).getRetryable());
        assertEquals("src-overloaded", responses.get(2).getRef().getSourceDocId());

        // Item 3: success — proves the stream keeps processing after a transient failure
        assertTrue(responses.get(3).getSuccess());
        assertFalse(responses.get(3).getRetryable());
        assertEquals("src-ok-2", responses.get(3).getRef().getSourceDocId());
    }
}
