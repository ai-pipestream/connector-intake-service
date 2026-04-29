package ai.pipeline.connector.intake.service;

import ai.pipeline.connector.intake.ingress.IntakeIngressEnvelope;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IntakeIngressProducerTest {

    @Test
    void redisEnvelopeRoundTripPreservesEngineHandoffRequest() throws Exception {
        IntakeHandoffRequest request = IntakeHandoffRequest.newBuilder()
                .setDatasourceId("ds-1")
                .setAccountId("acct-1")
                .setStream(EngineClient.buildHandoffRequest(
                        PipeDoc.newBuilder().setDocId("doc-1").build(),
                        "ds-1",
                        "acct-1",
                        IngestionConfig.newBuilder()
                                .setIngressMode(IngressMode.INGRESS_MODE_GRPC_INLINE)
                                .build(),
                        "crawl-1").getStream())
                .build();

        IntakeIngressEnvelope envelope = IntakeIngressEnvelope.fromRequest(request, "src-1");
        Map<String, String> fields = envelope.toRedisFields();
        IntakeIngressEnvelope decoded = IntakeIngressEnvelope.fromRedisFields(fields);

        assertEquals("src-1", decoded.sourceDocId());
        assertEquals(request, decoded.request());
        assertFalse(decoded.request().getStream().getStreamId().isBlank());
        assertEquals("1", fields.get(IntakeIngressEnvelope.FIELD_SCHEMA_VERSION));
        assertEquals("ds-1", fields.get(IntakeIngressEnvelope.FIELD_DATASOURCE_ID));
        assertEquals("acct-1", fields.get(IntakeIngressEnvelope.FIELD_ACCOUNT_ID));
        assertEquals("crawl-1", fields.get(IntakeIngressEnvelope.FIELD_CRAWL_ID));
        assertEquals("doc-1", fields.get(IntakeIngressEnvelope.FIELD_DOC_ID));
        assertEquals(decoded.request().getStream().getStreamId(), fields.get(IntakeIngressEnvelope.FIELD_STREAM_ID));
        assertEquals("inline", fields.get(IntakeIngressEnvelope.FIELD_PAYLOAD_MODE));
        assertEquals("0", fields.get(IntakeIngressEnvelope.FIELD_RETRY_COUNT));
        assertFalse(fields.get(IntakeIngressEnvelope.FIELD_ACCEPTED_AT_MS).isBlank());
    }
}
