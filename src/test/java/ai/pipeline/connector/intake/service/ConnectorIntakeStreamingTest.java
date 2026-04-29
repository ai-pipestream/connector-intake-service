package ai.pipeline.connector.intake.service;

import ai.pipeline.connector.intake.ingress.IntakeIngressProducer;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.connector.intake.v1.DocReference;
import ai.pipestream.connector.intake.v1.PipeDocItem;
import ai.pipestream.connector.intake.v1.StreamContext;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamRequest;
import ai.pipestream.connector.intake.v1.UploadPipeDocStreamResponse;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.test.support.ConnectorIntakeWireMockTestResource;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuarkusTest
@QuarkusTestResource(ConnectorIntakeWireMockTestResource.class)
class ConnectorIntakeStreamingTest {

    @Inject
    @GrpcService
    ConnectorIntakeServiceImpl intakeService;

    @InjectMock
    IntakeIngressProducer intakeIngressProducer;

    @InjectMock
    ConfigResolutionService configResolutionService;

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

    private List<UploadPipeDocStreamResponse> runStream(UploadPipeDocStreamRequest... requests)
            throws Exception {
        ConfigResolutionService.ResolvedConfig resolvedConfig =
                new ConfigResolutionService.ResolvedConfig(
                        DataSourceConfig.newBuilder()
                                .setDatasourceId("valid-datasource")
                                .setAccountId("acct-1")
                                .setConnectorId("connector-1")
                                .build(),
                        IngestionConfig.getDefaultInstance());
        when(configResolutionService.resolveConfig("valid-datasource", "valid-api-key"))
                .thenReturn(resolvedConfig);

        RecordingObserver recorder = new RecordingObserver();
        StreamObserver<UploadPipeDocStreamRequest> requestObserver =
                intakeService.uploadPipeDocStream(recorder);
        for (UploadPipeDocStreamRequest request : requests) {
            int expectedResponses = recorder.responseCount() + 1;
            requestObserver.onNext(request);
            recorder.awaitCountAtLeast(expectedResponses);
        }
        requestObserver.onCompleted();
        return recorder.await();
    }

    @Test
    @DisplayName("streaming items are accepted after Redis ingress enqueue")
    void uploadPipeDocStream_enqueuesNonDurableItemsToRedisIngress() throws Exception {
        when(intakeIngressProducer.enqueue(any(IntakeHandoffRequest.class), eq("src-1")))
                .thenReturn(new IntakeIngressProducer.EnqueueResult("redis-id-1"));

        List<UploadPipeDocStreamResponse> responses = runStream(
                contextMessage(validContext()),
                itemMessage("src-1"));

        assertEquals(2, responses.size());
        assertTrue(responses.get(0).getSuccess());
        UploadPipeDocStreamResponse itemAck = responses.get(1);
        assertTrue(itemAck.getSuccess());
        assertFalse(itemAck.getRetryable());
        assertEquals("src-1", itemAck.getRef().getSourceDocId());
        assertEquals("src-1", itemAck.getRef().getDocId());
        assertTrue(itemAck.getMessage().contains("engine ingress queued"));

        verify(intakeIngressProducer).enqueue(any(IntakeHandoffRequest.class), eq("src-1"));
    }

    @Test
    @DisplayName("items before context are rejected without touching Redis")
    void uploadPipeDocStream_itemBeforeContext_rejects() throws Exception {
        List<UploadPipeDocStreamResponse> responses = runStream(itemMessage("src-orphan"));

        assertEquals(1, responses.size());
        assertFalse(responses.get(0).getSuccess());
        assertFalse(responses.get(0).getRetryable());
        assertTrue(responses.get(0).getMessage().toLowerCase().contains("first message must be streamcontext"));
    }

    @Test
    @DisplayName("delete_ref payload remains explicitly unsupported")
    void uploadPipeDocStream_deleteRefPayload_returnsNotImplementedAck() throws Exception {
        List<UploadPipeDocStreamResponse> responses = runStream(
                contextMessage(validContext()),
                UploadPipeDocStreamRequest.newBuilder()
                        .setDeleteRef(DocReference.newBuilder().setSourceDocId("src-to-delete").build())
                        .build());

        assertEquals(2, responses.size());
        assertFalse(responses.get(1).getSuccess());
        assertFalse(responses.get(1).getRetryable());
        assertTrue(responses.get(1).getMessage().toLowerCase().contains("delete via stream not yet implemented"));
    }

    private static final class RecordingObserver implements StreamObserver<UploadPipeDocStreamResponse> {
        private final List<UploadPipeDocStreamResponse> responses = new ArrayList<>();
        private final CompletableFuture<List<UploadPipeDocStreamResponse>> done = new CompletableFuture<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();

        @Override
        public void onNext(UploadPipeDocStreamResponse value) {
            synchronized (responses) {
                responses.add(value);
                responses.notifyAll();
            }
        }

        @Override
        public void onError(Throwable t) {
            error.set(t);
            done.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            synchronized (responses) {
                done.complete(List.copyOf(responses));
            }
        }

        int responseCount() {
            synchronized (responses) {
                return responses.size();
            }
        }

        void awaitCountAtLeast(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            synchronized (responses) {
                while (responses.size() < count && System.nanoTime() < deadline) {
                    TimeUnit.MILLISECONDS.timedWait(responses, 25);
                }
            }
        }

        List<UploadPipeDocStreamResponse> await() throws Exception {
            try {
                return done.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                Throwable t = error.get();
                if (t != null) {
                    throw new AssertionError("stream failed", t);
                }
                throw e;
            }
        }
    }
}
