package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.ingress.IntakeIngressProducer;
import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipeline.connector.intake.service.EngineClient;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.connector.intake.v1.UploadPipeDocResponse;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PipeDocAcceptanceServiceHandoffModeTest {

    @Test
    void enqueueAndMaybePersist_defaultModeQueuesRedisAndDoesNotCallEngine() {
        PipeDocAcceptanceService service = serviceWithMode(null);
        when(service.intakeIngressProducer.enqueue(any(IntakeHandoffRequest.class), eq("doc-1")))
                .thenReturn(new IntakeIngressProducer.EnqueueResult("redis-1"));

        UploadPipeDocResponse response = service.enqueueAndMaybePersist(
                pipeDoc(), resolvedConfig(), null, System.nanoTime());

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("engine ingress queued"));
        verify(service.intakeIngressProducer).enqueue(any(IntakeHandoffRequest.class), eq("doc-1"));
        verify(service.engineClient, never()).handoffToEngineUnary(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
        verify(service.engineClient, never()).handoffToEngine(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
    }

    @Test
    void enqueueAndMaybePersist_explicitRedisModeQueuesRedisAndDoesNotCallEngine() {
        PipeDocAcceptanceService service = serviceWithMode("redis");
        when(service.intakeIngressProducer.enqueue(any(IntakeHandoffRequest.class), eq("doc-1")))
                .thenReturn(new IntakeIngressProducer.EnqueueResult("redis-1"));

        UploadPipeDocResponse response = service.enqueueAndMaybePersist(
                pipeDoc(), resolvedConfig(), null, System.nanoTime());

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("engine ingress queued"));
        verify(service.intakeIngressProducer).enqueue(any(IntakeHandoffRequest.class), eq("doc-1"));
        verify(service.engineClient, never()).handoffToEngineUnary(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
        verify(service.engineClient, never()).handoffToEngine(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
    }

    @Test
    void enqueueAndMaybePersist_engineUnaryModeCallsEngineAndDoesNotQueueRedis() {
        PipeDocAcceptanceService service = serviceWithMode("engine-unary");
        when(service.engineClient.handoffToEngineUnary(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null)))
                .thenReturn(IntakeHandoffResponse.newBuilder()
                        .setAccepted(true)
                        .setAssignedStreamId("stream-1")
                        .setMessage("accepted")
                        .build());

        UploadPipeDocResponse response = service.enqueueAndMaybePersist(
                pipeDoc(), resolvedConfig(), null, System.nanoTime());

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("engine accepted"));
        verify(service.engineClient).handoffToEngineUnary(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
        verify(service.intakeIngressProducer, never()).enqueue(any(IntakeHandoffRequest.class), eq("doc-1"));
    }

    @Test
    void enqueueAndMaybePersist_engineStreamModeCallsEngineStreamAndDoesNotQueueRedis() {
        PipeDocAcceptanceService service = serviceWithMode("engine-stream");
        when(service.engineClient.handoffToEngine(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null)))
                .thenReturn(IntakeHandoffResponse.newBuilder()
                        .setAccepted(true)
                        .setAssignedStreamId("stream-1")
                        .setMessage("accepted")
                        .build());

        UploadPipeDocResponse response = service.enqueueAndMaybePersist(
                pipeDoc(), resolvedConfig(), null, System.nanoTime());

        assertTrue(response.getSuccess());
        assertTrue(response.getMessage().contains("engine accepted"));
        verify(service.engineClient).handoffToEngine(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
        verify(service.engineClient, never()).handoffToEngineUnary(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
        verify(service.intakeIngressProducer, never()).enqueue(any(IntakeHandoffRequest.class), eq("doc-1"));
    }

    @Test
    void enqueueAndMaybePersist_engineUnaryRejectionReturnsFailedUploadResponse() {
        PipeDocAcceptanceService service = serviceWithMode("engine-unary");
        when(service.engineClient.handoffToEngineUnary(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null)))
                .thenReturn(IntakeHandoffResponse.newBuilder()
                        .setAccepted(false)
                        .setMessage("no route")
                        .build());

        UploadPipeDocResponse response = service.enqueueAndMaybePersist(
                pipeDoc(), resolvedConfig(), null, System.nanoTime());

        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().contains("Engine rejected"));
        verify(service.intakeIngressProducer, never()).enqueue(any(IntakeHandoffRequest.class), eq("doc-1"));
    }

    @Test
    void enqueueAndMaybePersist_unsupportedModeFailsBeforeHandoff() {
        PipeDocAcceptanceService service = serviceWithMode("surprise");

        StatusRuntimeException thrown = assertThrows(StatusRuntimeException.class,
                () -> service.enqueueAndMaybePersist(pipeDoc(), resolvedConfig(), null, System.nanoTime()));

        assertEquals(Status.Code.FAILED_PRECONDITION, thrown.getStatus().getCode());
        assertTrue(thrown.getStatus().getDescription().contains("Unsupported"));
        verify(service.intakeIngressProducer, never()).enqueue(any(IntakeHandoffRequest.class), eq("doc-1"));
        verify(service.engineClient, never()).handoffToEngineUnary(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
        verify(service.engineClient, never()).handoffToEngine(
                any(PipeDoc.class), eq("ds-1"), eq("acct-1"), any(IngestionConfig.class), eq(null));
    }

    private static PipeDocAcceptanceService serviceWithMode(String mode) {
        PipeDocAcceptanceService service = new PipeDocAcceptanceService();
        service.intakeIngressProducer = mock(IntakeIngressProducer.class);
        service.replayPersister = mock(IntakeReplayPersister.class);
        service.engineClient = mock(EngineClient.class);
        service.inlineHandoffMode = mode;
        return service;
    }

    private static PipeDoc pipeDoc() {
        return PipeDoc.newBuilder().setDocId("doc-1").build();
    }

    private static ConfigResolutionService.ResolvedConfig resolvedConfig() {
        return new ConfigResolutionService.ResolvedConfig(
                DataSourceConfig.newBuilder()
                        .setDatasourceId("ds-1")
                        .setAccountId("acct-1")
                        .setConnectorId("connector-1")
                        .build(),
                IngestionConfig.getDefaultInstance());
    }
}
