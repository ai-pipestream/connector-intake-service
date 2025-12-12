package ai.pipeline.connector.intake;

import ai.pipeline.connector.intake.service.ConnectorValidationService;
import ai.pipestream.connector.intake.v1.*;
import com.google.protobuf.ByteString;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.http.TestHTTPResource;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@QuarkusTest
public class BenchmarkIntakeTest {

    private static final Logger LOG = Logger.getLogger(BenchmarkIntakeTest.class);

    @GrpcClient("connector-intake-service")
    MutinyConnectorIntakeServiceGrpc.MutinyConnectorIntakeServiceStub intakeClient;

    @InjectMock
    ConnectorValidationService validationService;

    @TestHTTPResource
    URL testUrl;

    @BeforeEach
    void setupPort() {
        // When use-separate-server=true, gRPC runs on a different port than HTTP
        // In test mode, Quarkus uses test-port (defaults to 9001) instead of port
        // When test-port=0, Quarkus assigns a random port
        // After Quarkus starts, the actual assigned port is available via ConfigProvider
        org.eclipse.microprofile.config.Config config = org.eclipse.microprofile.config.ConfigProvider.getConfig();
        
        // Read the actual assigned gRPC test port from config (Quarkus sets this after server starts)
        // In test mode with use-separate-server=true, this will be the actual assigned port
        int grpcPort = config.getOptionalValue("quarkus.grpc.server.test-port", Integer.class)
                .orElse(config.getOptionalValue("quarkus.grpc.server.port", Integer.class)
                        .orElse(9001)); // Fallback to default if not found
        
        if (grpcPort <= 0) {
            throw new IllegalStateException("Cannot determine gRPC server port - server may not have started. " +
                    "Expected quarkus.grpc.server.test-port or quarkus.grpc.server.port to be set by Quarkus.");
        }
        
        // Set system property for ConnectorIntakeServiceImpl to read
        System.setProperty("quarkus.grpc.server.port.actual", String.valueOf(grpcPort));
        LOG.infof("Set gRPC test port system property: quarkus.grpc.server.port.actual=%d (HTTP port is %d, use-separate-server=true)", 
                grpcPort, testUrl.getPort());
    }

    @Test
    void benchmarkParallelUpload100MB() {
        // Arrange
        int fileSize = 10 * 1024 * 1024; // 10 MB per file
        int fileCount = 10; // 10 parallel uploads
        long totalBytes = (long) fileSize * fileCount; // 100 MB total

        byte[] data = new byte[fileSize];
        Arrays.fill(data, (byte) 1);
        ByteString content = ByteString.copyFrom(data);

        String connectorId = "bench-conn";
        String apiKey = "bench-key";
        ConnectorConfig config = ConnectorConfig.newBuilder()
                .setAccountId("bench-acc")
                .build();

        when(validationService.validateConnector(anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(config));

        UploadBlobRequest request = UploadBlobRequest.newBuilder()
                .setConnectorId(connectorId)
                .setApiKey(apiKey)
                .setFilename("benchmark_parallel.bin")
                .setMimeType("application/octet-stream")
                .setContent(content)
                .build();

        LOG.info("Warming up (1 request)...");
        long warmupStart = System.nanoTime();
        intakeClient.uploadBlob(request).await().atMost(Duration.ofSeconds(10));
        long warmupTime = System.nanoTime() - warmupStart;
        LOG.infof("Warmup complete in %.3f ms", warmupTime / 1_000_000.0);

        LOG.infof("Starting benchmark: %d files of %d MB each (Total: %d MB)...", fileCount, fileSize / (1024*1024), totalBytes / (1024*1024));
        
        // Measure request building time
        long buildStart = System.nanoTime();
        List<Uni<UploadBlobResponse>> tasks = new ArrayList<>();
        List<Long> taskStartTimes = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            long taskStart = System.nanoTime();
            final int taskIndex = i;
            tasks.add(intakeClient.uploadBlob(request)
                .invoke(() -> {
                    long taskTime = System.nanoTime() - taskStart;
                    LOG.debugf("Task %s completed in %.3f ms", String.valueOf(taskIndex), taskTime / 1_000_000.0);
                }));
            taskStartTimes.add(taskStart);
        }
        long buildTime = System.nanoTime() - buildStart;
        LOG.infof("Built %d tasks in %.3f ms", fileCount, buildTime / 1_000_000.0);

        // Act
        long startTime = System.nanoTime();
        LOG.info("Starting parallel execution...");
        
        // Execute in parallel
        Uni.join().all(tasks).andFailFast().await().atMost(Duration.ofSeconds(60));
        
        long endTime = System.nanoTime();
        LOG.info("Parallel execution complete.");
        
        // Calculate per-task timing
        long firstTaskStart = taskStartTimes.stream().mapToLong(Long::longValue).min().orElse(startTime);
        long lastTaskEnd = endTime;
        long totalWallTime = lastTaskEnd - firstTaskStart;
        LOG.infof("Wall clock time: %.3f ms (first task start to last task end)", totalWallTime / 1_000_000.0);

        // Calculate Metrics
        long durationNanos = endTime - startTime;
        double seconds = durationNanos / 1_000_000_000.0;
        double megabytes = totalBytes / (1024.0 * 1024.0);
        double throughput = megabytes / seconds;

        LOG.infof("Benchmark Result: Uploaded %.2f MB in %.4f seconds", megabytes, seconds);
        LOG.infof("Throughput: %.2f MB/s", throughput);
        LOG.infof("Per-file average: %.3f ms per 10MB file", (seconds * 1000.0) / fileCount);
        LOG.infof("Per-file throughput: %.2f MB/s per file", (megabytes / fileCount) / (seconds / fileCount));

        // Assert > 5 MB/s (Limited by Test Harness Flow Control Defaults)
        if (throughput < 100.0) {
            LOG.warnf("Throughput %.2f MB/s is below target 100 MB/s. I don't know why and I won't make up bullshit because LLMs tend to do that.", throughput);
        }
        assertTrue(throughput > 5.0, "Throughput should be > 5 MB/s (Actual: " + throughput + " MB/s)");
    }

    @Test
    void benchmarkLargeMessage250MB() {
        // Arrange - Test with a single 250MB message
        int fileSize = 250 * 1024 * 1024; // 250 MB per file
        int fileCount = 1; // Single large message
        long totalBytes = (long) fileSize * fileCount; // 250 MB total

        byte[] data = new byte[fileSize];
        Arrays.fill(data, (byte) 1);
        ByteString content = ByteString.copyFrom(data);

        String connectorId = "bench-conn";
        String apiKey = "bench-key";
        ConnectorConfig config = ConnectorConfig.newBuilder()
                .setAccountId("bench-acc")
                .build();

        when(validationService.validateConnector(anyString(), anyString()))
                .thenReturn(Uni.createFrom().item(config));

        UploadBlobRequest request = UploadBlobRequest.newBuilder()
                .setConnectorId(connectorId)
                .setApiKey(apiKey)
                .setFilename("benchmark_large_250mb.bin")
                .setMimeType("application/octet-stream")
                .setContent(content)
                .build();

        LOG.info("Warming up (1 request with smaller payload)...");
        // Warmup with smaller payload
        byte[] warmupData = new byte[10 * 1024 * 1024]; // 10MB warmup
        Arrays.fill(warmupData, (byte) 1);
        UploadBlobRequest warmupRequest = request.toBuilder()
                .setContent(ByteString.copyFrom(warmupData))
                .build();
        long warmupStart = System.nanoTime();
        intakeClient.uploadBlob(warmupRequest).await().atMost(Duration.ofSeconds(30));
        long warmupTime = System.nanoTime() - warmupStart;
        LOG.infof("Warmup complete in %.3f ms", warmupTime / 1_000_000.0);

        LOG.infof("Starting large message benchmark: 1 file of %d MB (Total: %d MB)...", 
                fileSize / (1024*1024), totalBytes / (1024*1024));

        // Act
        long startTime = System.nanoTime();
        LOG.info("Starting large message upload...");

        UploadBlobResponse response = intakeClient.uploadBlob(request)
                .await().atMost(Duration.ofSeconds(120)); // 2 minute timeout for large message
        
        long endTime = System.nanoTime();
        LOG.info("Large message upload complete.");

        // Calculate Metrics
        long durationNanos = endTime - startTime;
        double seconds = durationNanos / 1_000_000_000.0;
        double megabytes = totalBytes / (1024.0 * 1024.0);
        double throughput = megabytes / seconds;

        LOG.infof("Large Message Benchmark Result: Uploaded %.2f MB in %.4f seconds", megabytes, seconds);
        LOG.infof("Throughput: %.2f MB/s", throughput);
        LOG.infof("Message size: %d bytes (%.2f MB)", fileSize, megabytes);
        LOG.infof("Time per MB: %.3f ms/MB", (seconds * 1000.0) / megabytes);

        // Assert success and reasonable throughput
        assertTrue(response.getSuccess(), "Upload should succeed");
        assertTrue(throughput > 50.0, "Throughput should be > 50 MB/s for large messages (Actual: " + throughput + " MB/s)");
        
        if (throughput > 200.0) {
            LOG.infof("Excellent! Throughput of %.2f MB/s shows the flow control window optimization is working well for large messages!", throughput);
        }
    }
}