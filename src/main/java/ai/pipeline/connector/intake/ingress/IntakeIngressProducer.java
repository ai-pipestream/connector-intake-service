package ai.pipeline.connector.intake.ingress;

import ai.pipestream.engine.v1.IntakeHandoffRequest;

public interface IntakeIngressProducer {
    EnqueueResult enqueue(IntakeHandoffRequest request, String sourceDocId);

    record EnqueueResult(String messageId) {}
}
