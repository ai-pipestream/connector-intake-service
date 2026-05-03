package ai.pipeline.connector.intake.service;

import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.IngressMode;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.events.v1.IntakeRepoEvent;
import ai.pipestream.events.v1.IntakeRepoEventType;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.smallrye.common.annotation.RunOnVirtualThread;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.concurrent.CompletionStage;

/**
 * Consumes IntakeRepoEvent messages and initiates intake handoff.
 */
@ApplicationScoped
public class IntakeRepoEventConsumer {

    private static final Logger LOG = Logger.getLogger(IntakeRepoEventConsumer.class);
    private static final String API_KEY_METADATA_FIELD = "api_key";

    @Inject
    EngineClient engineClient;

    @Inject
    ConfigResolutionService configResolutionService;

    /**
     * @RunOnVirtualThread tells SmallRye Reactive Messaging to dispatch each
     * incoming Kafka record on its own virtual thread instead of the Vert.x
     * event loop. The handler body calls
     * {@link EngineClient#handoffReferenceToEngine} which does a synchronous
     * {@code CompletableFuture.get()} on the engine gRPC response — without
     * this annotation, that {@code .get()} blocks the event loop and Vert.x
     * fires {@code BlockedThreadChecker} after 2s. On a virtual thread the
     * blocking call parks without pinning a carrier thread, so it's safe.
     *
     * <p>Same shape SmallRye accepts for {@code @Incoming} as Mutiny
     * ({@code Uni<Void>}) and plain async ({@code CompletionStage<Void>});
     * we keep {@code CompletionStage<Void>} so the explicit
     * {@link Message#ack()} / {@link Message#nack(Throwable)} calls drive
     * Kafka offset commits with the same skip-and-ack semantics for
     * unsupported event types.
     */
    @Incoming("intake-repo-events-in")
    @RunOnVirtualThread
    public CompletionStage<Void> consumeIntakeRepoEvents(Message<IntakeRepoEvent> message) {
        try {
            IntakeRepoEvent event = message.getPayload();
            if (event == null) {
                LOG.warn("Received empty IntakeRepoEvent message");
                return message.ack();
            }

            String eventType = event.getEventType() == IntakeRepoEventType.UNRECOGNIZED
                    ? "UNRECOGNIZED"
                    : event.getEventType().toString();

            if (event.getEventType() == IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_UNSPECIFIED) {
                LOG.warnf("Skipping intake event without explicit event type: doc_id=%s, event_id=%s",
                        event.getDocId(), event.getEventId());
                return message.ack();
            }

            if (event.getEventType() == IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_DELETED) {
                LOG.infof("Received intake deleted event for doc_id=%s (document-aware cleanup deferred)",
                        event.getDocId());
                return message.ack();
            }

            if (!event.getEventType().equals(IntakeRepoEventType.INTAKE_REPO_EVENT_TYPE_CREATED)) {
                LOG.warnf("Skipping unsupported intake event type=%s for doc_id=%s", eventType, event.getDocId());
                return message.ack();
            }

            if (event.getDatasourceId().isBlank() || event.getAccountId().isBlank() || event.getDocId().isBlank()) {
                LOG.warnf("Skipping invalid intake created event: doc_id=%s, datasource_id=%s, account_id=%s",
                        event.getDocId(), event.getDatasourceId(), event.getAccountId());
                return message.ack();
            }

            IngestionConfig ingestionConfig = buildIngestionConfig(event);
            handoffToEngine(event, ingestionConfig);
            return message.ack();
        } catch (RuntimeException e) {
            IntakeRepoEvent event = message.getPayload();
            LOG.errorf(e, "Failed to consume intake repo event: doc_id=%s, event_id=%s",
                    event == null ? "" : event.getDocId(),
                    event == null ? "" : event.getEventId());
            return message.nack(e);
        }
    }

    private IngestionConfig buildIngestionConfig(IntakeRepoEvent event) {
        String apiKey = extractMetadataValue(event.getMetadata(), API_KEY_METADATA_FIELD);
        if (apiKey == null || apiKey.isBlank()) {
            LOG.debugf("No API key in IntakeRepoEvent metadata; using default ingestion config for doc_id=%s",
                    event.getDocId());
            return defaultHttpStagedConfig();
        }

        try {
            return configResolutionService.resolveConfig(event.getDatasourceId(), apiKey)
                    .withIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED);
        } catch (RuntimeException e) {
            LOG.warnf(e, "Failed to resolve config from intake repo event metadata; using default config for doc_id=%s",
                    event.getDocId());
            return defaultHttpStagedConfig();
        }
    }

    private void handoffToEngine(IntakeRepoEvent event, IngestionConfig ingestionConfig) {
        String graphAddressId = event.getDatasourceId();
        IntakeHandoffResponse response = engineClient.handoffReferenceToEngine(
                event.getDocId(),
                graphAddressId,
                event.getDatasourceId(),
                event.getAccountId(),
                ingestionConfig);
        if (response.getAccepted()) {
            LOG.debugf("Engine accepted intake handoff for doc_id=%s, stream_id=%s, entry_node=%s",
                    event.getDocId(), response.getAssignedStreamId(), response.getEntryNodeId());
        } else {
            LOG.warnf("Engine rejected intake handoff for doc_id=%s: %s",
                    event.getDocId(), response.getMessage());
        }
    }

    private IngestionConfig defaultHttpStagedConfig() {
        return IngestionConfig.newBuilder()
                .setIngressMode(IngressMode.INGRESS_MODE_HTTP_STAGED)
                .build();
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
}
