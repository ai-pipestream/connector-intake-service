package ai.pipeline.connector.intake.service;

import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.events.v1.IntakeRepoEvent;
import ai.pipestream.events.v1.IntakeRepoEventType;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Consumes IntakeRepoEvent messages and initiates intake handoff.
 */
@ApplicationScoped
public class IntakeRepoEventConsumer {

    private static final Logger LOG = Logger.getLogger(IntakeRepoEventConsumer.class);
    private static final String INTAKE_SOURCE_NODE_ID = "connector-intake";
    private static final String API_KEY_METADATA_FIELD = "api_key";

    @Inject
    EngineClient engineClient;

    @Inject
    ConfigResolutionService configResolutionService;

    @Incoming("intake-repo-events-in")
    public Uni<Void> consumeIntakeRepoEvents(Message<IntakeRepoEvent> message) {
        IntakeRepoEvent event = message.getPayload();

        if (event == null) {
            LOG.warn("Received empty IntakeRepoEvent message");
            return ackMessage(message);
        }

        String eventType = event.getEventType() == IntakeRepoEventType.UNRECOGNIZED
                ? "UNRECOGNIZED"
                : event.getEventType().toString();

        if (event.getEventType() == IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_UNSPECIFIED) {
            LOG.warnf("Skipping intake event without explicit event type: doc_id=%s, event_id=%s", event.getDocId(), event.getEventId());
            return ackMessage(message);
        }

        if (event.getEventType() == IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_DELETED) {
            LOG.infof("Received intake deleted event for doc_id=%s (document-aware cleanup deferred)",
                    event.getDocId());
            return ackMessage(message);
        }

        if (!event.getEventType().equals(IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_CREATED)) {
            LOG.warnf("Skipping unsupported intake event type=%s for doc_id=%s", eventType, event.getDocId());
            return ackMessage(message);
        }

        if (event.getDatasourceId().isBlank() || event.getAccountId().isBlank() || event.getDocId().isBlank()) {
            LOG.warnf("Skipping invalid intake created event: doc_id=%s, datasource_id=%s, account_id=%s",
                    event.getDocId(), event.getDatasourceId(), event.getAccountId());
            return ackMessage(message);
        }

        return buildIngestionConfig(event)
                .flatMap(ingestionConfig ->
                        handoffToEngine(event, ingestionConfig))
                .onItem().ignore().andContinueWith(() -> null)
                .onFailure().invoke(throwable ->
                        LOG.errorf(throwable, "Failed to consume intake repo event: doc_id=%s, event_id=%s",
                                event.getDocId(), event.getEventId()))
                .onItemOrFailure().transformToUni((ignored, error) -> {
                    if (error != null) {
                        return Uni.createFrom().failure(error);
                    }
                    return ackMessage(message);
                })
                .onFailure().recoverWithUni(failure ->
                        nackMessage(message, failure));
    }

    private Uni<IngestionConfig> buildIngestionConfig(IntakeRepoEvent event) {
        String apiKey = extractMetadataValue(event.getMetadata(), API_KEY_METADATA_FIELD);
        if (apiKey == null || apiKey.isBlank()) {
            LOG.debugf("No API key in IntakeRepoEvent metadata; using default ingestion config for doc_id=%s",
                    event.getDocId());
            return Uni.createFrom().item(IngestionConfig.newBuilder()
                    .setIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED)
                    .build());
        }

        return configResolutionService.resolveConfig(event.getDatasourceId(), apiKey)
                .map(resolved -> resolved.withIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED))
                .onFailure().recoverWithItem(IngestionConfig.newBuilder()
                        .setIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED)
                        .build());
    }

    private Uni<Void> handoffToEngine(IntakeRepoEvent event, IngestionConfig ingestionConfig) {
        return engineClient.handoffReferenceToEngine(
                event.getDocId(),
                event.getSourceNodeId().isBlank() ? INTAKE_SOURCE_NODE_ID : event.getSourceNodeId(),
                event.getDatasourceId(),
                event.getAccountId(),
                ingestionConfig
        ).invoke(response -> {
            if (response.getAccepted()) {
                LOG.debugf("Engine accepted intake handoff for doc_id=%s, stream_id=%s, entry_node=%s",
                        event.getDocId(), response.getAssignedStreamId(), response.getEntryNodeId());
            } else {
                LOG.warnf("Engine rejected intake handoff for doc_id=%s: %s",
                        event.getDocId(), response.getMessage());
            }
        }).replaceWithVoid();
    }

    private String extractMetadataValue(Struct metadata, String key) {
        if (metadata == null || metadata.getFieldsMap().isEmpty()) {
            return null;
        }

        Value value = metadata.getFieldsOrDefault(key, null);
        if (value == null || !value.hasStringValue()) {
            return null;
        }
        return value.getStringValue();
    }

    private Uni<Void> ackMessage(Message<IntakeRepoEvent> message) {
        return Uni.createFrom().<Void>completionStage(message.ack());
    }

    private Uni<Void> nackMessage(Message<IntakeRepoEvent> message, Throwable error) {
        return Uni.createFrom().<Void>completionStage(message.nack(error));
    }
}
