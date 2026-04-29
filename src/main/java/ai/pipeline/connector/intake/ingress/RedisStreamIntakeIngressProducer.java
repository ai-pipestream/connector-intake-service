package ai.pipeline.connector.intake.ingress;

import ai.pipestream.engine.v1.IntakeHandoffRequest;
import io.quarkus.redis.datasource.RedisDataSource;
import io.quarkus.redis.datasource.stream.StreamCommands;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RedisStreamIntakeIngressProducer implements IntakeIngressProducer {

    @Inject
    RedisDataSource redis;

    @ConfigProperty(name = "pipestream.intake.ingress.stream-key", defaultValue = "pipestream:intake:ingress")
    String streamKey;

    private StreamCommands<String, String, String> streams;

    @PostConstruct
    void init() {
        this.streams = redis.stream(String.class);
    }

    @Override
    public EnqueueResult enqueue(IntakeHandoffRequest request, String sourceDocId) {
        String messageId = streams.xadd(streamKey, IntakeIngressEnvelope.fromRequest(request, sourceDocId).toRedisFields());
        return new EnqueueResult(messageId);
    }
}
