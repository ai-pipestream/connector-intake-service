package ai.pipeline.connector.intake.http;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Captures the last raw upload request received by the mock repository server.
 */
public final class RepositoryUploadMockState {

    private RepositoryUploadMockState() {}

    private static volatile Map<String, String> lastHeaders = Map.of();
    private static volatile byte[] lastBody = new byte[0];
    private static volatile String lastPath = "";

    public static void reset() {
        lastHeaders = Map.of();
        lastBody = new byte[0];
        lastPath = "";
    }

    public static void record(String path, Map<String, String> headers, byte[] body) {
        lastPath = path;
        lastHeaders = Collections.unmodifiableMap(new HashMap<>(headers));
        lastBody = body != null ? body : new byte[0];
    }

    public static String lastPath() {
        return lastPath;
    }

    public static Map<String, String> lastHeaders() {
        return lastHeaders;
    }

    public static byte[] lastBody() {
        return lastBody;
    }
}
