package ai.pipeline.connector.intake.ingress;

import ai.pipestream.engine.v1.IntakeHandoffRequest;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Redis Stream payload shared by connector-intake and the engine sidecar.
 */
public record IntakeIngressEnvelope(
        IntakeHandoffRequest request,
        String sourceDocId,
        String schemaVersion,
        String datasourceId,
        String accountId,
        String crawlId,
        String docId,
        String streamId,
        String payloadMode,
        int retryCount,
        long acceptedAtMs) {
    public static final String FIELD_REQUEST = "request";
    public static final String FIELD_SOURCE_DOC_ID = "source_doc_id";
    public static final String FIELD_SCHEMA_VERSION = "schema_version";
    public static final String FIELD_DATASOURCE_ID = "datasource_id";
    public static final String FIELD_ACCOUNT_ID = "account_id";
    public static final String FIELD_CRAWL_ID = "crawl_id";
    public static final String FIELD_DOC_ID = "doc_id";
    public static final String FIELD_STREAM_ID = "stream_id";
    public static final String FIELD_PAYLOAD_MODE = "payload_mode";
    public static final String FIELD_RETRY_COUNT = "retry_count";
    public static final String FIELD_ACCEPTED_AT_MS = "accepted_at_ms";

    private static final String SCHEMA_VERSION = "1";
    private static final String PAYLOAD_MODE_INLINE = "inline";
    private static final String PAYLOAD_MODE_REFERENCE = "reference";

    public static IntakeIngressEnvelope fromRequest(IntakeHandoffRequest request, String sourceDocId) {
        var stream = request.getStream();
        String docId = "";
        String payloadMode = PAYLOAD_MODE_INLINE;
        if (stream.hasDocumentRef()) {
            docId = stream.getDocumentRef().getDocId();
            payloadMode = PAYLOAD_MODE_REFERENCE;
        } else if (stream.hasDocument()) {
            docId = stream.getDocument().getDocId();
        }

        String crawlId = stream.hasMetadata() ? stream.getMetadata().getCrawlId() : "";
        String accountId = request.getAccountId();
        if (accountId.isBlank() && stream.hasMetadata()) {
            accountId = stream.getMetadata().getAccountId();
        }

        return new IntakeIngressEnvelope(
                request,
                sourceDocId == null ? "" : sourceDocId,
                SCHEMA_VERSION,
                request.getDatasourceId(),
                accountId,
                crawlId,
                docId,
                stream.getStreamId(),
                payloadMode,
                0,
                System.currentTimeMillis());
    }

    public Map<String, String> toRedisFields() {
        Map<String, String> fields = new HashMap<>();
        fields.put(FIELD_REQUEST, Base64.getEncoder().encodeToString(request.toByteArray()));
        fields.put(FIELD_SOURCE_DOC_ID, sourceDocId);
        fields.put(FIELD_SCHEMA_VERSION, schemaVersion);
        fields.put(FIELD_DATASOURCE_ID, datasourceId);
        fields.put(FIELD_ACCOUNT_ID, accountId);
        fields.put(FIELD_CRAWL_ID, crawlId);
        fields.put(FIELD_DOC_ID, docId);
        fields.put(FIELD_STREAM_ID, streamId);
        fields.put(FIELD_PAYLOAD_MODE, payloadMode);
        fields.put(FIELD_RETRY_COUNT, Integer.toString(retryCount));
        fields.put(FIELD_ACCEPTED_AT_MS, Long.toString(acceptedAtMs));
        return fields;
    }

    public static IntakeIngressEnvelope fromRedisFields(Map<String, String> fields)
            throws InvalidProtocolBufferException {
        String encodedRequest = fields.get(FIELD_REQUEST);
        if (encodedRequest == null || encodedRequest.isBlank()) {
            throw new IllegalArgumentException("missing Redis ingress field: " + FIELD_REQUEST);
        }
        byte[] requestBytes = Base64.getDecoder().decode(encodedRequest);
        IntakeHandoffRequest request = IntakeHandoffRequest.parseFrom(requestBytes);
        return new IntakeIngressEnvelope(
                request,
                fields.getOrDefault(FIELD_SOURCE_DOC_ID, ""),
                fields.getOrDefault(FIELD_SCHEMA_VERSION, SCHEMA_VERSION),
                fields.getOrDefault(FIELD_DATASOURCE_ID, request.getDatasourceId()),
                fields.getOrDefault(FIELD_ACCOUNT_ID, request.getAccountId()),
                fields.getOrDefault(FIELD_CRAWL_ID,
                        request.getStream().hasMetadata() ? request.getStream().getMetadata().getCrawlId() : ""),
                fields.getOrDefault(FIELD_DOC_ID, ""),
                fields.getOrDefault(FIELD_STREAM_ID, request.getStream().getStreamId()),
                fields.getOrDefault(FIELD_PAYLOAD_MODE, PAYLOAD_MODE_INLINE),
                Integer.parseInt(fields.getOrDefault(FIELD_RETRY_COUNT, "0")),
                Long.parseLong(fields.getOrDefault(FIELD_ACCEPTED_AT_MS, "0")));
    }
}
