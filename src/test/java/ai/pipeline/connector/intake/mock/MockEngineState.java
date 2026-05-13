package ai.pipeline.connector.intake.mock;

import ai.pipestream.engine.v1.IntakeHandoffStreamRequest;
import ai.pipestream.engine.v1.IntakeHandoffStreamResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public final class MockEngineState {

    public enum Mode {
        ACCEPT,
        RETRYABLE_REJECT,
        PERMANENT_REJECT,
        NO_ACK,
        STREAM_ERROR
    }

    private static final AtomicReference<Mode> mode = new AtomicReference<>(Mode.ACCEPT);
    private static final CopyOnWriteArrayList<IntakeHandoffStreamRequest> received = new CopyOnWriteArrayList<>();

    private MockEngineState() {}

    public static void reset() {
        mode.set(Mode.ACCEPT);
        received.clear();
    }

    public static void mode(Mode value) {
        mode.set(value);
    }

    public static Mode mode() {
        return mode.get();
    }

    public static void record(IntakeHandoffStreamRequest request) {
        received.add(request);
    }

    public static List<IntakeHandoffStreamRequest> received() {
        return new ArrayList<>(received);
    }

    public static IntakeHandoffStreamResponse.Status responseStatus() {
        return switch (mode.get()) {
            case ACCEPT, NO_ACK, STREAM_ERROR -> IntakeHandoffStreamResponse.Status.STATUS_ACCEPTED;
            case RETRYABLE_REJECT -> IntakeHandoffStreamResponse.Status.STATUS_RETRYABLE_REJECTED;
            case PERMANENT_REJECT -> IntakeHandoffStreamResponse.Status.STATUS_PERMANENT_REJECTED;
        };
    }
}
