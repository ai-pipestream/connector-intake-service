package ai.pipeline.connector.intake;

import ai.pipestream.connector.intake.*;
import ai.pipestream.grpc.wiremock.AccountManagerMock;
import ai.pipestream.grpc.wiremock.ConnectorIntakeTestResource;
import ai.pipestream.grpc.wiremock.ConnectorServiceMock;
import ai.pipestream.grpc.wiremock.InjectWireMock;
import ai.pipestream.grpc.wiremock.RepositoryServiceMock;
import static ai.pipestream.grpc.wiremock.WireMockGrpcCompat.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Benchmark test for connector-intake-service.
 * Measures throughput of the intake service with mocked downstream dependencies.
 */
@QuarkusTest
@QuarkusTestResource(ConnectorIntakeTestResource.class)
public class BenchmarkIntakeTest {

    private static final Logger LOG = Logger.getLogger(BenchmarkIntakeTest.class);
    private static final String TEST_CONNECTOR_ID = "benchmark-connector";
    private static final String TEST_API_KEY = "benchmark-key";
    private static final String TEST_ACCOUNT_ID = "benchmark-account";
    
    // Path to the sample file
    private static final String SAMPLE_FILE_PATH = "/home/krickert/IdeaProjects/ai-pipestream/sample-documents/sample-doc-types/database/flights-1m.parquet";

    @InjectWireMock
    WireMockServer wireMockServer;

    private ManagedChannel intakeChannel;
    private MutinyConnectorIntakeServiceGrpc.MutinyConnectorIntakeServiceStub intakeClient;
    private RepositoryServiceMock repositoryServiceMock;
    private ConnectorServiceMock connectorServiceMock;
    private AccountManagerMock accountManagerMock;

    @BeforeEach
    void setUp() {
        int wireMockPort = wireMockServer.port();
        
        // Set up repository service mocks - fast response
        repositoryServiceMock = new RepositoryServiceMock(wireMockPort);
        repositoryServiceMock.mockInitiateUpload("bench-node-id", "bench-upload-id");
        
        // Use a simple fixed response for all chunks to avoid JSON templating issues under high concurrency.
        // This matches ANY UploadChunk request and returns a fixed response.
        // For benchmark testing, we don't need dynamic responses - just fast, reliable mocks.
        repositoryServiceMock.getService().stubFor(
            method("UploadChunk")
                .willReturn(message(
                    ai.pipestream.repository.filesystem.upload.UploadChunkResponse.newBuilder()
                        .setNodeId("bench-node-id")
                        .setChunkNumber(0)  // Fixed value - not critical for benchmark
                        .setState(ai.pipestream.repository.filesystem.upload.UploadState.UPLOAD_STATE_UPLOADING)
                        .setBytesUploaded(0)
                        .setIsFileComplete(false)
                        .build()
                ))
        );
        
        repositoryServiceMock.mockGetUploadStatus("bench-node-id", ai.pipestream.repository.filesystem.upload.UploadState.UPLOAD_STATE_COMPLETED);

        // Set up connector service mocks
        connectorServiceMock = new ConnectorServiceMock(wireMockPort);
        connectorServiceMock.mockValidateApiKey(TEST_CONNECTOR_ID, TEST_API_KEY, TEST_ACCOUNT_ID);

        // Set up account manager mocks
        accountManagerMock = new AccountManagerMock(wireMockPort);
        accountManagerMock.mockGetAccount(TEST_ACCOUNT_ID, "Benchmark Account", "Test account", true);

        // Create gRPC client
        int intakePort = Integer.parseInt(
                System.getProperty("quarkus.http.test-port", "38108")
        );

        intakeChannel = ManagedChannelBuilder
                .forAddress("localhost", intakePort)
                .usePlaintext()
                .build();

        intakeClient = MutinyConnectorIntakeServiceGrpc.newMutinyStub(intakeChannel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (intakeChannel != null && !intakeChannel.isShutdown()) {
            intakeChannel.shutdown();
            intakeChannel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void benchmarkThroughput() throws Exception {
        // 1. Load file into memory
        Path path = Paths.get(SAMPLE_FILE_PATH);
        if (!Files.exists(path)) {
            LOG.warn("Sample file not found at " + SAMPLE_FILE_PATH + ". Skipping benchmark.");
            return;
        }
        
        byte[] fileBytes = Files.readAllBytes(path);
        int fileSize = fileBytes.length;
        LOG.info("Loaded benchmark file: " + fileSize + " bytes (" + (fileSize / 1024.0 / 1024.0) + " MB)");

        // 2. Prepare chunks
        int chunkSize = 10 * 1024 * 1024; // 10MB chunks (good balance between overhead and parallelism)
        List<ByteString> chunks = new ArrayList<>();
        for (int i = 0; i < fileSize; i += chunkSize) {
            int end = Math.min(fileSize, i + chunkSize);
            chunks.add(ByteString.copyFrom(fileBytes, i, end - i));
        }
        int chunkCount = chunks.size();
        LOG.info("Prepared " + chunkCount + " chunks of size " + chunkSize + " bytes");

        // 3. Run benchmark loop
        int iterations = 20; // Upload the file 20 times
        long totalBytes = (long) fileSize * iterations;
        
        LOG.info("Starting benchmark: " + iterations + " iterations, total data: " + (totalBytes / 1024.0 / 1024.0) + " MB");
        
        long startTime = System.nanoTime();
        
        // Run sequentially for simplicity in measurement, but async within each upload
        for (int i = 0; i < iterations; i++) {
            runSingleUpload(i, fileSize, chunkCount, chunks);
        }
        
        long endTime = System.nanoTime();
        long durationNs = endTime - startTime;
        double durationSeconds = durationNs / 1_000_000_000.0;
        
        double throughputBytesPerSec = totalBytes / durationSeconds;
        double throughputMBPerSec = throughputBytesPerSec / 1024.0 / 1024.0;
        
        LOG.info("--------------------------------------------------");
        LOG.info("BENCHMARK RESULTS");
        LOG.info("Total Data: " + (totalBytes / 1024.0 / 1024.0) + " MB");
        LOG.info("Duration: " + durationSeconds + " seconds");
        LOG.info("Throughput: " + String.format("%.2f", throughputMBPerSec) + " MB/s");
        LOG.info("--------------------------------------------------");
        
        System.out.println("--------------------------------------------------");
        System.out.println("BENCHMARK RESULTS");
        System.out.println("Total Data: " + (totalBytes / 1024.0 / 1024.0) + " MB");
        System.out.println("Duration: " + durationSeconds + " seconds");
        System.out.println("Throughput: " + String.format("%.2f", throughputMBPerSec) + " MB/s");
        System.out.println("--------------------------------------------------");
        
        assertTrue(throughputMBPerSec > 10.0, "Throughput should be > 10 MB/s");
    }

    private void runSingleUpload(int iteration, int fileSize, int chunkCount, List<ByteString> chunks) {
        // Start - use unique sessionId to avoid database constraint violations in parallel uploads
        String uniqueSessionId = "bench-session-" + UUID.randomUUID().toString();
        StartChunkedUploadRequest startRequest = StartChunkedUploadRequest.newBuilder()
                .setConnectorId(TEST_CONNECTOR_ID)
                .setApiKey(TEST_API_KEY)
                .setSessionId(uniqueSessionId)
                .setFilename("bench-file-" + iteration + ".parquet")
                .setPath("benchmark")
                .setMimeType("application/octet-stream")
                .setExpectedSizeBytes(fileSize)
                .setExpectedChunkCount(chunkCount)
                .build();

        StartChunkedUploadResponse startResponse = intakeClient.startChunkedUpload(startRequest)
                .await().indefinitely();
        
        String uploadId = startResponse.getUploadId();
        
        // Upload chunks (simulate parallel upload of chunks)
        List<CompletableFuture<Void>> chunkFutures = new ArrayList<>();
        
        for (int i = 0; i < chunks.size(); i++) {
            int chunkNum = i;
            ByteString data = chunks.get(i);
            
            AsyncChunkedUploadChunkRequest chunkRequest = AsyncChunkedUploadChunkRequest.newBuilder()
                    .setUploadId(uploadId)
                    .setChunkNumber(chunkNum)
                    .setChunkData(data)
                    .build();
            
            // Fire and forget style for client, but we wait for ack
            chunkFutures.add(intakeClient.uploadAsyncChunk(chunkRequest)
                    .subscribeAsCompletionStage()
                    .thenAccept(r -> {}));
        }
        
        // Wait for all chunks
        CompletableFuture.allOf(chunkFutures.toArray(new CompletableFuture[0])).join();
        
        // Complete
        CompleteChunkedUploadRequest completeRequest = CompleteChunkedUploadRequest.newBuilder()
                .setUploadId(uploadId)
                .setTotalChunksSent(chunkCount)
                .setFinalSha256("dummy-hash")
                .build();
                
        intakeClient.completeChunkedUpload(completeRequest).await().indefinitely();
    }

    @Test
    void benchmarkThroughputParallel() throws Exception {
        // 1. Load file into memory
        Path path = Paths.get(SAMPLE_FILE_PATH);
        if (!Files.exists(path)) {
            LOG.warn("Sample file not found at " + SAMPLE_FILE_PATH + ". Skipping benchmark.");
            return;
        }
        
        byte[] fileBytes = Files.readAllBytes(path);
        int fileSize = fileBytes.length;
        LOG.info("Loaded benchmark file: " + fileSize + " bytes (" + (fileSize / 1024.0 / 1024.0) + " MB)");

        // 2. Prepare chunks
        // Use 2MB chunks to stay under WireMock's 4MB gRPC message limit
        int chunkSize = 2 * 1024 * 1024; // 2MB chunks (under WireMock's 4MB limit)
        List<ByteString> chunks = new ArrayList<>();
        for (int i = 0; i < fileSize; i += chunkSize) {
            int end = Math.min(fileSize, i + chunkSize);
            chunks.add(ByteString.copyFrom(fileBytes, i, end - i));
        }
        int chunkCount = chunks.size();
        LOG.info("Prepared " + chunkCount + " chunks of size " + chunkSize + " bytes");

        // 3. Run benchmark loop - PARALLEL UPLOADS
        int iterations = 20; // Upload the file 20 times
        int parallelUploads = 10; // Run 10 uploads concurrently
        long totalBytes = (long) fileSize * iterations;
        
        LOG.info("Starting PARALLEL benchmark: " + iterations + " iterations, " + parallelUploads + " parallel uploads, total data: " + (totalBytes / 1024.0 / 1024.0) + " MB");
        
        long startTime = System.nanoTime();
        
        // Run uploads in parallel batches using simple threading
        List<CompletableFuture<Void>> uploadFutures = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            int iteration = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                runSingleUpload(iteration, fileSize, chunkCount, chunks);
            });
            uploadFutures.add(future);
            
            // Limit concurrent uploads to avoid overwhelming the system
            if (uploadFutures.size() >= parallelUploads) {
                // Wait for at least one to complete before starting more
                CompletableFuture.anyOf(uploadFutures.toArray(new CompletableFuture[0])).join();
                uploadFutures.removeIf(CompletableFuture::isDone);
            }
        }
        
        // Wait for all remaining uploads to complete
        CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0])).join();
        
        long endTime = System.nanoTime();
        long durationNs = endTime - startTime;
        double durationSeconds = durationNs / 1_000_000_000.0;
        
        double throughputBytesPerSec = totalBytes / durationSeconds;
        double throughputMBPerSec = throughputBytesPerSec / 1024.0 / 1024.0;
        
        LOG.info("--------------------------------------------------");
        LOG.info("PARALLEL BENCHMARK RESULTS");
        LOG.info("Total Data: " + (totalBytes / 1024.0 / 1024.0) + " MB");
        LOG.info("Duration: " + durationSeconds + " seconds");
        LOG.info("Throughput: " + String.format("%.2f", throughputMBPerSec) + " MB/s");
        LOG.info("Parallel Uploads: " + parallelUploads);
        LOG.info("--------------------------------------------------");
        
        System.out.println("--------------------------------------------------");
        System.out.println("PARALLEL BENCHMARK RESULTS");
        System.out.println("Total Data: " + (totalBytes / 1024.0 / 1024.0) + " MB");
        System.out.println("Duration: " + durationSeconds + " seconds");
        System.out.println("Throughput: " + String.format("%.2f", throughputMBPerSec) + " MB/s");
        System.out.println("Parallel Uploads: " + parallelUploads);
        System.out.println("--------------------------------------------------");
        
        assertTrue(throughputMBPerSec > 10.0, "Throughput should be > 10 MB/s");
    }
}
