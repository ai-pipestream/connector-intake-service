# Connector Intake Hardening and E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make connector intake Mutiny-free in owned service/test code, restore deterministic tests for the current Redis/engine-stream architecture, and add operator-ready S3/direct-upload E2E scripts and docs.

**Architecture:** Keep the existing focused service boundaries. Fix test infrastructure first by replacing accidental Stork/engine dependencies with explicit fake repository and fake engine gRPC services. Then remove remaining Mutiny test clients, add coverage for Redis ingress/replay/engine stream behavior, and layer safe shell scripts plus README workflows on top.

**Tech Stack:** Java 21, Quarkus gRPC, standard gRPC `StreamObserver` stubs, JUnit 5, Mockito/Quarkus mocks where already used, Redis Stream producer, Bash, `grpcurl`, `jq`, AWS CLI-compatible S3 commands.

---

## File Structure

Create:

- `src/test/java/ai/pipeline/connector/intake/mock/MockEngineService.java`  
  Test-only engine gRPC service that implements `EngineV1ServiceGrpc.EngineV1ServiceImplBase` and deterministically acks bidi handoff requests.
- `src/test/java/ai/pipeline/connector/intake/mock/MockEngineState.java`  
  Static test helper for configuring engine ack mode and inspecting received handoff requests.
- `src/test/java/ai/pipeline/connector/intake/pipedoc/PipeDocAcceptanceServiceTest.java`  
  Component tests for Redis enqueue and replay scheduling.
- `src/test/java/ai/pipeline/connector/intake/pipedoc/BlobUploadHandoffServiceTest.java`  
  Component tests for repository persistence plus engine handoff responses.
- `src/test/java/ai/pipeline/connector/intake/pipedoc/IntakeReplayPersisterTest.java`  
  Component tests for fire-and-forget replay persistence behavior.
- `src/test/java/ai/pipeline/connector/intake/service/IntakeHandoffStreamClientTest.java`  
  Component tests for ack mapping, stream reset, and timeout behavior.
- `src/test/java/ai/pipeline/connector/intake/pipedoc/streaming/StreamingPipeDocObserverTest.java`  
  Focused observer tests outside the full Quarkus boot path.
- `scripts/e2e/lib/common.sh`  
  Shared shell helpers for strict mode, dependency checks, JSON, temp dirs, and secret masking.
- `scripts/e2e/seed-s3-documents.sh`  
  Uploads sample documents to an S3-compatible bucket.
- `scripts/e2e/create-datasource.sh`  
  Creates or reuses account/datasource and writes a local credential env file with `0600` permissions.
- `scripts/e2e/upload-intake-docs.sh`  
  Demonstrates HTTP raw upload, gRPC `UploadPipeDoc`, and gRPC `UploadBlob`.
- `scripts/e2e/run-s3-crawl.sh`  
  Starts an S3 connector crawl with masked config output.
- `scripts/e2e/verify-intake.sh`  
  Verifies accepted responses and, when configured, Redis ingress evidence.

Modify:

- `build.gradle`  
  Disable Mutiny proto generation if safe; otherwise document why generated Mutiny classes remain available but unused.
- `src/test/java/ai/pipeline/connector/intake/mock/MockRepositoryService.java`  
  Convert from `MutinyNodeUploadServiceGrpc.NodeUploadServiceImplBase` to standard `NodeUploadServiceGrpc.NodeUploadServiceImplBase`.
- `src/test/java/ai/pipeline/connector/intake/ConnectorIntakeIntegrationTest.java`  
  Convert from `MutinyConnectorIntakeServiceGrpc` to standard blocking/future gRPC client.
- `src/test/java/ai/pipeline/connector/intake/BenchmarkIntakeTest.java`  
  Convert from Mutiny `Uni` to Java `CompletableFuture` and move/gate heavy benchmarks outside the normal suite if needed.
- `src/test/resources/application.properties`  
  Add deterministic test client wiring for repository and engine gRPC services; lower engine ack timeout for tests.
- `README.md`  
  Add copy-paste direct upload, S3 E2E, troubleshooting, and performance notes.
- `scripts/setup-s3-crawl.sh`  
  Either replace with a compatibility wrapper pointing to `scripts/e2e/run-s3-crawl.sh` or mark as legacy and stop printing secrets.

---

## Task 1: Add Deterministic Fake Engine for Tests

**Files:**
- Create: `src/test/java/ai/pipeline/connector/intake/mock/MockEngineState.java`
- Create: `src/test/java/ai/pipeline/connector/intake/mock/MockEngineService.java`
- Modify: `src/test/resources/application.properties`

- [ ] **Step 1: Write the fake engine state helper**

Create `src/test/java/ai/pipeline/connector/intake/mock/MockEngineState.java`:

```java
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
```

- [ ] **Step 2: Write the fake engine bidi service**

Create `src/test/java/ai/pipeline/connector/intake/mock/MockEngineService.java`:

```java
package ai.pipeline.connector.intake.mock;

import ai.pipestream.engine.v1.EngineV1ServiceGrpc;
import ai.pipestream.engine.v1.IntakeHandoffResponse;
import ai.pipestream.engine.v1.IntakeHandoffStreamRequest;
import ai.pipestream.engine.v1.IntakeHandoffStreamResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import jakarta.inject.Singleton;

@Singleton
@GrpcService
public class MockEngineService extends EngineV1ServiceGrpc.EngineV1ServiceImplBase {

    @Override
    public StreamObserver<IntakeHandoffStreamRequest> intakeHandoffStream(
            StreamObserver<IntakeHandoffStreamResponse> responseObserver) {
        return new StreamObserver<>() {
            @Override
            public void onNext(IntakeHandoffStreamRequest request) {
                MockEngineState.record(request);
                if (MockEngineState.mode() == MockEngineState.Mode.STREAM_ERROR) {
                    responseObserver.onError(Status.UNAVAILABLE
                            .withDescription("mock engine stream error")
                            .asRuntimeException());
                    return;
                }
                if (MockEngineState.mode() == MockEngineState.Mode.NO_ACK) {
                    return;
                }
                IntakeHandoffResponse response = IntakeHandoffResponse.newBuilder()
                        .setAccepted(MockEngineState.mode() == MockEngineState.Mode.ACCEPT)
                        .setAssignedStreamId("mock-engine-stream")
                        .setEntryNodeId("mock-entry-node")
                        .setMessage("mock engine " + MockEngineState.mode().name())
                        .build();
                responseObserver.onNext(IntakeHandoffStreamResponse.newBuilder()
                        .setHandoffId(request.getHandoffId())
                        .setSequence(request.getSequence())
                        .setStatus(MockEngineState.responseStatus())
                        .setResponse(response)
                        .build());
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}
```

- [ ] **Step 3: Wire the engine test client to the Quarkus test gRPC server**

Modify `src/test/resources/application.properties` so the engine client does not use Stork in ordinary tests:

```properties
%test.quarkus.grpc.clients.engine.host=localhost
%test.quarkus.grpc.clients.engine.port=${quarkus.grpc.server.test-port}
%test.quarkus.grpc.clients.engine.name-resolver=dns
%test.pipestream.intake.engine.handoff-stream-ack-timeout-ms=1000
```

Also ensure repository test gRPC client points at the same test server when `MockRepositoryService` is used:

```properties
%test.quarkus.grpc.clients.repository.host=localhost
%test.quarkus.grpc.clients.repository.port=${quarkus.grpc.server.test-port}
%test.quarkus.grpc.clients.repository.name-resolver=dns
```

- [ ] **Step 4: Run the previously failing tests**

Run:

```bash
./gradlew test --tests ai.pipeline.connector.intake.ConnectorIntakeIntegrationTest --tests ai.pipeline.connector.intake.service.ConnectorIntakeServiceTest
```

Expected before subsequent fixes: blob upload failures should no longer show `NoSuchServiceDefinitionException` or 120-second ack timeout. Any remaining failures should be assertion/behavior failures, not test infrastructure panics.

---

## Task 2: Convert Test Repository Mock Off Mutiny

**Files:**
- Modify: `src/test/java/ai/pipeline/connector/intake/mock/MockRepositoryService.java`

- [ ] **Step 1: Replace Mutiny imports and base class**

Change:

```java
import ai.pipestream.repository.filesystem.upload.v1.MutinyNodeUploadServiceGrpc;
import io.smallrye.mutiny.Uni;
```

to:

```java
import ai.pipestream.repository.filesystem.upload.v1.NodeUploadServiceGrpc;
import io.grpc.stub.StreamObserver;
```

Change the class declaration to:

```java
public class MockRepositoryService extends NodeUploadServiceGrpc.NodeUploadServiceImplBase {
```

- [ ] **Step 2: Replace unary method implementations**

Use standard observer responses:

```java
@Override
public void uploadFilesystemPipeDoc(
        UploadFilesystemPipeDocRequest request,
        StreamObserver<UploadFilesystemPipeDocResponse> responseObserver) {
    long startTime = System.nanoTime();
    PipeDoc doc = request.getDocument();
    int requestSize = doc.getSerializedSize();
    LOG.debugf("MockRepositoryService.uploadFilesystemPipeDoc START: size=%d bytes", requestSize);

    long buildStart = System.nanoTime();
    UploadFilesystemPipeDocResponse response = UploadFilesystemPipeDocResponse.newBuilder()
            .setSuccess(true)
            .setDocumentId("mock-doc-" + UUID.randomUUID())
            .setS3Key("mock/s3/key/" + doc.getSearchMetadata().getSourcePath())
            .setMessage("Mock upload successful")
            .build();
    long buildTime = System.nanoTime() - buildStart;
    long totalTime = System.nanoTime() - startTime;
    LOG.debugf("MockRepositoryService.uploadFilesystemPipeDoc: build took %.3f ms, TOTAL=%.3f ms",
            buildTime / 1_000_000.0, totalTime / 1_000_000.0);

    responseObserver.onNext(response);
    responseObserver.onCompleted();
}

@Override
public void getUploadedDocument(
        GetUploadedDocumentRequest request,
        StreamObserver<GetUploadedDocumentResponse> responseObserver) {
    responseObserver.onError(new UnsupportedOperationException("Not implemented in mock"));
}
```

- [ ] **Step 3: Verify no Mutiny remains in the mock**

Run:

```bash
rg "Mutiny|Uni|io.smallrye.mutiny" src/test/java/ai/pipeline/connector/intake/mock/MockRepositoryService.java
```

Expected: no matches.

- [ ] **Step 4: Run blob tests**

Run:

```bash
./gradlew test --tests ai.pipeline.connector.intake.ConnectorIntakeIntegrationTest.testUploadBlob_Success --tests ai.pipeline.connector.intake.service.ConnectorIntakeServiceTest.uploadBlob_success_constructsPipeDoc
```

Expected: both tests pass with the fake repository and fake engine.

---

## Task 3: Convert Integration Clients Off Mutiny

**Files:**
- Modify: `src/test/java/ai/pipeline/connector/intake/ConnectorIntakeIntegrationTest.java`
- Modify: `src/test/java/ai/pipeline/connector/intake/BenchmarkIntakeTest.java`

- [ ] **Step 1: Convert `ConnectorIntakeIntegrationTest` client type**

Replace:

```java
@GrpcClient("connector-intake-service")
MutinyConnectorIntakeServiceGrpc.MutinyConnectorIntakeServiceStub intakeClient;
```

with:

```java
@GrpcClient("connector-intake-service")
ConnectorIntakeServiceGrpc.ConnectorIntakeServiceBlockingStub intakeClient;
```

Replace:

```java
UploadBlobResponse response = intakeClient.uploadBlob(request)
        .await().atMost(Duration.ofSeconds(10));
```

with:

```java
UploadBlobResponse response = intakeClient.uploadBlob(request);
```

Replace:

```java
UploadPipeDocResponse response = intakeClient.uploadPipeDoc(request)
        .await().atMost(Duration.ofSeconds(10));
```

with:

```java
UploadPipeDocResponse response = intakeClient.uploadPipeDoc(request);
```

Remove the unused `java.time.Duration` import.

- [ ] **Step 2: Convert `BenchmarkIntakeTest` client type and parallel execution**

Replace the Mutiny client field with:

```java
@GrpcClient("connector-intake-service")
ConnectorIntakeServiceGrpc.ConnectorIntakeServiceBlockingStub intakeClient;
```

Replace warmup:

```java
intakeClient.uploadBlob(request);
```

Replace the task list:

```java
List<CompletableFuture<UploadBlobResponse>> tasks = new ArrayList<>();
List<Long> taskStartTimes = new ArrayList<>();
for (int i = 0; i < fileCount; i++) {
    long taskStart = System.nanoTime();
    final int taskIndex = i;
    tasks.add(CompletableFuture.supplyAsync(() -> {
        UploadBlobResponse response = intakeClient.uploadBlob(request);
        long taskTime = System.nanoTime() - taskStart;
        LOG.debugf("Task %s completed in %.3f ms", String.valueOf(taskIndex), taskTime / 1_000_000.0);
        return response;
    }));
    taskStartTimes.add(taskStart);
}
```

Replace join:

```java
CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new)).get(60, TimeUnit.SECONDS);
```

Add imports:

```java
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
```

Remove:

```java
import io.smallrye.mutiny.Uni;
```

- [ ] **Step 3: Decide benchmark gating**

If `BenchmarkIntakeTest` remains part of normal `test`, add a class-level JUnit condition so heavy tests only run with `-Dbenchmark.intake=true`:

```java
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

@EnabledIfSystemProperty(named = "benchmark.intake", matches = "true")
public class BenchmarkIntakeTest {
```

Then normal correctness runs are not coupled to local throughput hardware. Verify whether CI currently expects these tests before applying this gate.

- [ ] **Step 4: Verify no Mutiny remains in tests**

Run:

```bash
rg "Mutiny|Uni|io.smallrye.mutiny" src/test/java
```

Expected: no service-owned test references remain.

---

## Task 4: Verify or Disable Mutiny Proto Generation

**Files:**
- Modify: `build.gradle`

- [ ] **Step 1: Try disabling Mutiny generation**

Change:

```groovy
generateMutiny = true
```

to:

```groovy
generateMutiny = false
```

- [ ] **Step 2: Run compile and tests**

Run:

```bash
./gradlew clean compileJava compileTestJava
```

Expected: compile succeeds if no service code depends on generated Mutiny classes.

- [ ] **Step 3: If compile fails due platform/plugin requirements, keep generation but document owned-code policy**

If compile fails because platform tooling requires generated Mutiny classes, revert only the `generateMutiny` value and add this comment above it:

```groovy
    // Generated for platform compatibility only. connector-intake owned
    // application and test code uses standard gRPC stubs, not Mutiny APIs.
    generateMutiny = true
```

Then rerun:

```bash
./gradlew compileJava compileTestJava
```

Expected: compile succeeds, and `rg "Mutiny|Uni|io.smallrye.mutiny" src/main/java src/test/java` returns no owned-code usage.

---

## Task 5: Add Focused PipeDoc Acceptance Tests

**Files:**
- Create: `src/test/java/ai/pipeline/connector/intake/pipedoc/PipeDocAcceptanceServiceTest.java`

- [ ] **Step 1: Add a plain unit test with injectable collaborators**

Create the test class in the same package so package-private fields can be set directly:

```java
package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.ingress.IntakeIngressProducer;
import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.connector.intake.v1.PipeDocItem;
import ai.pipestream.connector.intake.v1.StreamContext;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import ai.pipestream.engine.v1.IntakeHandoffRequest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PipeDocAcceptanceServiceTest {

    @Test
    void enqueueAndMaybePersistEnqueuesInlineDocumentAndSchedulesReplayWhenConfigured() {
        PipeDocAcceptanceService service = newService();
        RecordingIngressProducer ingress = new RecordingIngressProducer();
        RecordingReplayPersister replay = new RecordingReplayPersister();
        service.intakeIngressProducer = ingress;
        service.replayPersister = replay;

        PipeDoc doc = PipeDoc.newBuilder().setDocId("doc-1").build();
        var response = service.enqueueAndMaybePersist(doc, resolved(true), "crawl-1", System.nanoTime());

        assertTrue(response.getSuccess());
        assertEquals("doc-1", response.getDocId());
        assertEquals(1, ingress.requests.size());
        assertEquals("doc-1", ingress.sourceDocIds.get(0));
        assertTrue(ingress.requests.get(0).getStream().hasDocument());
        assertEquals("crawl-1", ingress.requests.get(0).getStream().getMetadata().getCrawlId());
        assertEquals(1, replay.docs.size());
    }

    @Test
    void enqueueAndMaybePersistDoesNotScheduleReplayWhenPersistenceDisabled() {
        PipeDocAcceptanceService service = newService();
        RecordingIngressProducer ingress = new RecordingIngressProducer();
        RecordingReplayPersister replay = new RecordingReplayPersister();
        service.intakeIngressProducer = ingress;
        service.replayPersister = replay;

        var response = service.enqueueAndMaybePersist(
                PipeDoc.newBuilder().setDocId("doc-2").build(),
                resolved(false),
                "",
                System.nanoTime());

        assertTrue(response.getSuccess());
        assertEquals(1, ingress.requests.size());
        assertTrue(replay.docs.isEmpty());
    }

    private static PipeDocAcceptanceService newService() {
        PipeDocAcceptanceService service = new PipeDocAcceptanceService();
        service.docIdDeriver = new PipeDocIdDeriver();
        return service;
    }

    private static ConfigResolutionService.ResolvedConfig resolved(boolean persist) {
        DataSourceConfig.Builder config = DataSourceConfig.newBuilder()
                .setAccountId("acct-1")
                .setDatasourceId("ds-1")
                .setConnectorId("connector-1");
        if (persist) {
            config.getGlobalConfigBuilder()
                    .getPersistenceConfigBuilder()
                    .setPersistPipedoc(true);
        }
        return new ConfigResolutionService.ResolvedConfig(config.build(), IngestionConfig.getDefaultInstance());
    }

    private static final class RecordingIngressProducer implements IntakeIngressProducer {
        final List<IntakeHandoffRequest> requests = new ArrayList<>();
        final List<String> sourceDocIds = new ArrayList<>();

        @Override
        public EnqueueResult enqueue(IntakeHandoffRequest request, String sourceDocId) {
            requests.add(request);
            sourceDocIds.add(sourceDocId);
            return new EnqueueResult("redis-1");
        }
    }

    private static final class RecordingReplayPersister extends IntakeReplayPersister {
        final List<PipeDoc> docs = new ArrayList<>();

        @Override
        public void persistAsync(PipeDoc pipeDoc, String accountId, String datasourceId) {
            docs.add(pipeDoc);
        }
    }
}
```

- [ ] **Step 2: Add streaming item test cases**

Add tests for accepted item and missing deterministic doc id:

```java
@Test
void acceptStreamingItemDerivesDocIdAndReturnsAck() {
    PipeDocAcceptanceService service = newService();
    RecordingIngressProducer ingress = new RecordingIngressProducer();
    service.intakeIngressProducer = ingress;
    service.replayPersister = new RecordingReplayPersister();

    var response = service.acceptStreamingItem(
            PipeDocItem.newBuilder()
                    .setSourceDocId("source-1")
                    .setPipeDoc(PipeDoc.getDefaultInstance())
                    .build(),
            StreamContext.newBuilder()
                    .setDatasourceId("ds-1")
                    .setCrawlId("crawl-1")
                    .build(),
            resolved(false));

    assertTrue(response.getSuccess());
    assertEquals("source-1", response.getRef().getSourceDocId());
    assertEquals("ds-1:source-1", response.getRef().getDocId());
    assertEquals(1, ingress.requests.size());
}

@Test
void acceptStreamingItemRejectsWhenDocIdCannotBeDerived() {
    PipeDocAcceptanceService service = newService();
    service.intakeIngressProducer = new RecordingIngressProducer();
    service.replayPersister = new RecordingReplayPersister();

    var response = service.acceptStreamingItem(
            PipeDocItem.newBuilder().setPipeDoc(PipeDoc.getDefaultInstance()).build(),
            StreamContext.newBuilder().setDatasourceId("ds-1").build(),
            resolved(false));

    assertFalse(response.getSuccess());
    assertFalse(response.getRetryable());
    assertTrue(response.getMessage().contains("cannot determine doc_id"));
}
```

- [ ] **Step 3: Run focused test**

Run:

```bash
./gradlew test --tests ai.pipeline.connector.intake.pipedoc.PipeDocAcceptanceServiceTest
```

Expected: all tests pass.

---

## Task 6: Add Engine Handoff Stream Tests

**Files:**
- Create: `src/test/java/ai/pipeline/connector/intake/service/IntakeHandoffStreamClientTest.java`

- [ ] **Step 1: Add Quarkus test using fake engine state**

Create:

```java
package ai.pipeline.connector.intake.service;

import ai.pipeline.connector.intake.mock.MockEngineState;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class IntakeHandoffStreamClientTest {

    @Inject
    IntakeHandoffStreamClient client;

    @BeforeEach
    void reset() {
        MockEngineState.reset();
    }

    @Test
    void intakeHandoffReturnsAcceptedAck() {
        var response = client.intakeHandoff(EngineClient.buildHandoffRequest(
                PipeDoc.newBuilder().setDocId("doc-1").build(),
                "ds-1",
                "acct-1",
                IngestionConfig.getDefaultInstance(),
                "crawl-1"));

        assertTrue(response.getAccepted());
        assertEquals("mock-engine-stream", response.getAssignedStreamId());
        assertEquals(1, MockEngineState.received().size());
    }

    @Test
    void retryableAckMapsToUnavailable() {
        MockEngineState.mode(MockEngineState.Mode.RETRYABLE_REJECT);

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
                client.intakeHandoff(EngineClient.buildHandoffRequest(
                        PipeDoc.newBuilder().setDocId("doc-retry").build(),
                        "ds-1",
                        "acct-1",
                        IngestionConfig.getDefaultInstance(),
                        null)));

        assertEquals(Status.Code.UNAVAILABLE, ex.getStatus().getCode());
    }

    @Test
    void permanentAckMapsToFailedPrecondition() {
        MockEngineState.mode(MockEngineState.Mode.PERMANENT_REJECT);

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
                client.intakeHandoff(EngineClient.buildHandoffRequest(
                        PipeDoc.newBuilder().setDocId("doc-permanent").build(),
                        "ds-1",
                        "acct-1",
                        IngestionConfig.getDefaultInstance(),
                        null)));

        assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    }
}
```

- [ ] **Step 2: Add timeout case with short test timeout**

Use the `%test.pipestream.intake.engine.handoff-stream-ack-timeout-ms=1000` property from Task 1:

```java
@Test
void missingAckMapsToDeadlineExceeded() {
    MockEngineState.mode(MockEngineState.Mode.NO_ACK);

    StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () ->
            client.intakeHandoff(EngineClient.buildHandoffRequest(
                    PipeDoc.newBuilder().setDocId("doc-timeout").build(),
                    "ds-1",
                    "acct-1",
                    IngestionConfig.getDefaultInstance(),
                    null)));

    assertEquals(Status.Code.DEADLINE_EXCEEDED, ex.getStatus().getCode());
}
```

- [ ] **Step 3: Run focused test**

Run:

```bash
./gradlew test --tests ai.pipeline.connector.intake.service.IntakeHandoffStreamClientTest
```

Expected: accepted, retryable, permanent, and timeout cases pass.

---

## Task 7: Add Blob Upload and Replay Tests

**Files:**
- Create: `src/test/java/ai/pipeline/connector/intake/pipedoc/BlobUploadHandoffServiceTest.java`
- Create: `src/test/java/ai/pipeline/connector/intake/pipedoc/IntakeReplayPersisterTest.java`

- [ ] **Step 1: Add blob upload integration tests against fake repository and fake engine**

Create `BlobUploadHandoffServiceTest` as a `@QuarkusTest`:

```java
package ai.pipeline.connector.intake.pipedoc;

import ai.pipeline.connector.intake.mock.MockEngineState;
import ai.pipeline.connector.intake.service.ConfigResolutionService;
import ai.pipestream.connector.intake.v1.DataSourceConfig;
import ai.pipestream.data.v1.IngestionConfig;
import ai.pipestream.data.v1.PipeDoc;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class BlobUploadHandoffServiceTest {

    @Inject
    BlobUploadHandoffService service;

    @BeforeEach
    void reset() {
        MockEngineState.reset();
    }

    @Test
    void persistBlobAndHandoffReturnsSuccessWhenEngineAccepts() {
        var response = service.persistBlobAndHandoff(
                PipeDoc.newBuilder().setDocId("blob-1").build(),
                resolved(),
                System.nanoTime(),
                "crawl-1");

        assertTrue(response.getSuccess());
        assertTrue(response.getDocId().startsWith("mock-doc-"));
        assertEquals(1, MockEngineState.received().size());
    }

    @Test
    void persistBlobAndHandoffReturnsFailureWhenEngineRejects() {
        MockEngineState.mode(MockEngineState.Mode.PERMANENT_REJECT);

        var response = service.persistBlobAndHandoff(
                PipeDoc.newBuilder().setDocId("blob-2").build(),
                resolved(),
                System.nanoTime(),
                "crawl-1");

        assertFalse(response.getSuccess());
        assertTrue(response.getMessage().contains("FAILED_PRECONDITION")
                || response.getMessage().contains("permanently rejected"));
    }

    private static ConfigResolutionService.ResolvedConfig resolved() {
        return new ConfigResolutionService.ResolvedConfig(
                DataSourceConfig.newBuilder()
                        .setAccountId("acct-1")
                        .setDatasourceId("ds-1")
                        .setConnectorId("connector-1")
                        .build(),
                IngestionConfig.getDefaultInstance());
    }
}
```

If the second test currently throws instead of returning a failure response, update `BlobUploadHandoffService.persistBlobAndHandoff` to catch `StatusRuntimeException` from `engineClient.handoffReferenceToEngine` and return `UploadBlobResponse.success=false` with the status message.

- [ ] **Step 2: Add replay persister test seam if needed**

If `IntakeReplayPersister` cannot be tested without a real channel, add a package-private constructor accepting a `NodeUploadServiceGrpc.NodeUploadServiceStub` and `ExecutorService`, while keeping the CDI no-arg path:

```java
IntakeReplayPersister(NodeUploadServiceGrpc.NodeUploadServiceStub repositoryStub, ExecutorService executor) {
    this.repositoryStub = repositoryStub;
    this.executor = executor;
}
```

If the current `executor` field is `final`, change it to package-private initialized in the default constructor path:

```java
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
```

- [ ] **Step 3: Add replay persistence tests**

Create `IntakeReplayPersisterTest` using a fake in-process gRPC service if constructor injection is not practical. Cover:

```java
@Test
void persistAsyncReturnsImmediatelyAndDoesNotThrowOnRepositoryFailure() {
    IntakeReplayPersister persister = new IntakeReplayPersister();
    persister.repositoryChannel = failingInProcessChannel();
    persister.buildStub();

    assertDoesNotThrow(() -> persister.persistAsync(
            PipeDoc.newBuilder().setDocId("doc-1").build(),
            "acct-1",
            "ds-1"));
}
```

Prefer a deterministic fake stub/channel over sleeping. If an async assertion is needed, use `CountDownLatch` with a 2 second timeout.

- [ ] **Step 4: Run focused tests**

Run:

```bash
./gradlew test --tests ai.pipeline.connector.intake.pipedoc.BlobUploadHandoffServiceTest --tests ai.pipeline.connector.intake.pipedoc.IntakeReplayPersisterTest
```

Expected: all tests pass.

---

## Task 8: Add Streaming Observer Tests

**Files:**
- Create: `src/test/java/ai/pipeline/connector/intake/pipedoc/streaming/StreamingPipeDocObserverTest.java`

- [ ] **Step 1: Add direct observer tests with recording response observer**

Create a test similar to existing `ConnectorIntakeStreamingTest`, but instantiate `StreamingPipeDocObserver` directly with fake `ConfigResolutionService` and `PipeDocAcceptanceService` test doubles. Cover:

```java
@Test
void itemBeforeContextReturnsNonRetryableFailure() throws Exception {
    RecordingResponseObserver responses = new RecordingResponseObserver();
    StreamingPipeDocObserver observer = new StreamingPipeDocObserver(
            responses,
            new FakeConfigResolutionService(),
            new AcceptingPipeDocAcceptanceService());

    observer.onNext(UploadPipeDocStreamRequest.newBuilder()
            .setItem(PipeDocItem.newBuilder()
                    .setSourceDocId("source-1")
                    .setPipeDoc(PipeDoc.newBuilder().setDocId("doc-1").build())
                    .build())
            .build());

    UploadPipeDocStreamResponse response = responses.awaitCount(1).get(0);
    assertFalse(response.getSuccess());
    assertFalse(response.getRetryable());
    assertTrue(response.getMessage().contains("first message must be StreamContext"));
}
```

- [ ] **Step 2: Add context failure and delete-ref tests**

Add:

```java
@Test
void contextResolutionFailureReturnsNonRetryableAck() throws Exception {
    RecordingResponseObserver responses = new RecordingResponseObserver();
    StreamingPipeDocObserver observer = new StreamingPipeDocObserver(
            responses,
            new RejectingConfigResolutionService(),
            new AcceptingPipeDocAcceptanceService());

    observer.onNext(UploadPipeDocStreamRequest.newBuilder()
            .setContext(StreamContext.newBuilder()
                    .setDatasourceId("bad-ds")
                    .setApiKey("bad-key")
                    .build())
            .build());

    UploadPipeDocStreamResponse response = responses.awaitCount(1).get(0);
    assertFalse(response.getSuccess());
    assertFalse(response.getRetryable());
    assertTrue(response.getMessage().contains("stream context rejected"));
}
```

- [ ] **Step 3: Run focused tests**

Run:

```bash
./gradlew test --tests ai.pipeline.connector.intake.pipedoc.streaming.StreamingPipeDocObserverTest
```

Expected: all observer behavior tests pass.

---

## Task 9: Add Operator E2E Script Library

**Files:**
- Create: `scripts/e2e/lib/common.sh`

- [ ] **Step 1: Add strict common shell helpers**

Create:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
E2E_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

info() {
  echo "INFO: $*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but was not found on PATH"
}

mask_secret() {
  local value="${1:-}"
  if [[ -z "$value" ]]; then
    echo ""
  elif [[ ${#value} -le 8 ]]; then
    echo "********"
  else
    echo "${value:0:4}********${value: -4}"
  fi
}

json_get() {
  local json="$1"
  local path="$2"
  jq -r "$path // empty" <<<"$json"
}

write_secret_env() {
  local file="$1"
  shift
  umask 077
  : > "$file"
  chmod 600 "$file"
  for line in "$@"; do
    printf '%s\n' "$line" >> "$file"
  done
}

load_env_file() {
  local file="$1"
  [[ -f "$file" ]] || die "env file not found: $file"
  [[ ! -L "$file" ]] || die "refusing to source symlinked env file: $file"
  # shellcheck disable=SC1090
  set -a; source "$file"; set +a
}
```

- [ ] **Step 2: Make executable**

Run:

```bash
chmod +x scripts/e2e/lib/common.sh
```

Expected: no output.

---

## Task 10: Add S3 Seed and Datasource Scripts

**Files:**
- Create: `scripts/e2e/seed-s3-documents.sh`
- Create: `scripts/e2e/create-datasource.sh`

- [ ] **Step 1: Add S3 seed script**

Create `scripts/e2e/seed-s3-documents.sh`:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/e2e/lib/common.sh"

require_cmd aws

S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX="${S3_PREFIX:-intake-e2e/$(date +%Y%m%d-%H%M%S)}"
S3_ENDPOINT="${S3_ENDPOINT:-}"
S3_REGION="${S3_REGION:-us-east-1}"

[[ -n "$S3_BUCKET" ]] || die "S3_BUCKET is required"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

printf 'hello from connector intake e2e\n' > "$tmp_dir/hello.txt"
printf '{"source":"connector-intake-e2e","ok":true}\n' > "$tmp_dir/sample.json"
mkdir -p "$tmp_dir/nested"
printf 'nested document\n' > "$tmp_dir/nested/readme.md"

aws_args=(--region "$S3_REGION")
if [[ -n "$S3_ENDPOINT" ]]; then
  aws_args+=(--endpoint-url "$S3_ENDPOINT")
fi

info "Uploading sample documents to s3://$S3_BUCKET/$S3_PREFIX"
aws "${aws_args[@]}" s3 cp "$tmp_dir/hello.txt" "s3://$S3_BUCKET/$S3_PREFIX/hello.txt"
aws "${aws_args[@]}" s3 cp "$tmp_dir/sample.json" "s3://$S3_BUCKET/$S3_PREFIX/sample.json"
aws "${aws_args[@]}" s3 cp "$tmp_dir/nested/readme.md" "s3://$S3_BUCKET/$S3_PREFIX/nested/readme.md"

cat <<EOF
S3_BUCKET=$S3_BUCKET
S3_PREFIX=$S3_PREFIX
S3_ENDPOINT=$S3_ENDPOINT
S3_REGION=$S3_REGION
EOF
```

- [ ] **Step 2: Add datasource creation script with masked secrets**

Create `scripts/e2e/create-datasource.sh` with:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/e2e/lib/common.sh"

require_cmd grpcurl
require_cmd jq

SERVICE_HOST="${SERVICE_HOST:-localhost}"
ACCOUNT_PORT="${ACCOUNT_PORT:-18105}"
REPO_PORT="${REPO_PORT:-18102}"
ADMIN_PORT="${ADMIN_PORT:-18107}"
ACCOUNT_ID="${ACCOUNT_ID:-default}"
DRIVE_NAME="${DRIVE_NAME:-default-drive}"
LOCAL_BUCKET="${LOCAL_BUCKET:-pipestream}"
DATASOURCE_NAME="${DATASOURCE_NAME:-s3-crawl}"
S3_CONNECTOR_ID="${S3_CONNECTOR_ID:-a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"
OUT_FILE="${OUT_FILE:-.intake-e2e.env}"

info "Creating account $ACCOUNT_ID"
grpcurl -plaintext -d "{
  \"account_id\": \"$ACCOUNT_ID\",
  \"name\": \"$ACCOUNT_ID\",
  \"description\": \"Created by connector-intake E2E script\",
  \"create_default_drive\": true,
  \"default_drive_config\": {
    \"drive_name\": \"$DRIVE_NAME\",
    \"bucket_name\": \"$LOCAL_BUCKET\",
    \"create_bucket\": true
  }
}" "$SERVICE_HOST:$ACCOUNT_PORT" ai.pipestream.repository.account.v1.AccountService/CreateAccount >/dev/null || true

info "Ensuring repository bucket $LOCAL_BUCKET"
grpcurl -plaintext -d "{\"bucket_name\":\"$LOCAL_BUCKET\"}" \
  "$SERVICE_HOST:$REPO_PORT" ai.pipestream.repository.filesystem.v1.FilesystemService/CreateBucket >/dev/null || true

info "Creating datasource $DATASOURCE_NAME"
response="$(grpcurl -plaintext -d "{
  \"account_id\": \"$ACCOUNT_ID\",
  \"connector_id\": \"$S3_CONNECTOR_ID\",
  \"name\": \"$DATASOURCE_NAME\",
  \"drive_name\": \"$DRIVE_NAME\"
}" "$SERVICE_HOST:$ADMIN_PORT" ai.pipestream.connector.intake.v1.DataSourceAdminService/CreateDataSource)"

api_key="$(json_get "$response" '.apiKey')"
datasource_id="$(json_get "$response" '.datasourceId')"
[[ -n "$api_key" ]] || die "CreateDataSource did not return apiKey"
[[ -n "$datasource_id" ]] || die "CreateDataSource did not return datasourceId"

write_secret_env "$OUT_FILE" \
  "DS_ID=$datasource_id" \
  "API_KEY=$api_key" \
  "ACCOUNT_ID=$ACCOUNT_ID" \
  "DRIVE_NAME=$DRIVE_NAME"

echo "Datasource ID: $datasource_id"
echo "API Key:       $(mask_secret "$api_key")"
echo "Env file:      $OUT_FILE"
```

- [ ] **Step 3: Make scripts executable and dry-run dependency checks**

Run:

```bash
chmod +x scripts/e2e/seed-s3-documents.sh scripts/e2e/create-datasource.sh
bash -n scripts/e2e/seed-s3-documents.sh
bash -n scripts/e2e/create-datasource.sh
```

Expected: no syntax errors.

---

## Task 11: Add Direct Upload and Crawl Scripts

**Files:**
- Create: `scripts/e2e/upload-intake-docs.sh`
- Create: `scripts/e2e/run-s3-crawl.sh`
- Create: `scripts/e2e/verify-intake.sh`
- Modify: `scripts/setup-s3-crawl.sh`

- [ ] **Step 1: Add direct intake upload script**

Create `scripts/e2e/upload-intake-docs.sh`:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/e2e/lib/common.sh"

require_cmd curl
require_cmd grpcurl
require_cmd jq

ENV_FILE="${ENV_FILE:-.intake-e2e.env}"
load_env_file "$ENV_FILE"

SERVICE_HOST="${SERVICE_HOST:-localhost}"
INTAKE_HTTP_PORT="${INTAKE_HTTP_PORT:-18108}"
INTAKE_GRPC_PORT="${INTAKE_GRPC_PORT:-18108}"
DOC_FILE="${DOC_FILE:-}"

if [[ -z "$DOC_FILE" ]]; then
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' EXIT
  DOC_FILE="$tmp_dir/direct-upload.txt"
  printf 'direct connector-intake upload\n' > "$DOC_FILE"
fi

[[ -n "${DS_ID:-}" ]] || die "DS_ID missing; run create-datasource.sh first"
[[ -n "${API_KEY:-}" ]] || die "API_KEY missing; run create-datasource.sh first"

source_path="/e2e/$(basename "$DOC_FILE")"
echo "HTTP raw upload:"
curl -fsS \
  -H "x-datasource-id: $DS_ID" \
  -H "x-api-key: $API_KEY" \
  -H "x-source-path: $source_path" \
  -H "x-filename: $(basename "$DOC_FILE")" \
  --data-binary "@$DOC_FILE" \
  "http://$SERVICE_HOST:$INTAKE_HTTP_PORT/uploads/raw"
echo

echo "gRPC UploadPipeDoc:"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DS_ID\",
  \"api_key\": \"$API_KEY\",
  \"source_doc_id\": \"grpc-pipedoc-$(date +%s)\",
  \"pipe_doc\": {
    \"search_metadata\": {
      \"source_path\": \"$source_path\"
    }
  }
}" "$SERVICE_HOST:$INTAKE_GRPC_PORT" ai.pipestream.connector.intake.v1.ConnectorIntakeService/UploadPipeDoc

echo "gRPC UploadBlob:"
content_b64="$(base64 -w0 "$DOC_FILE")"
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DS_ID\",
  \"api_key\": \"$API_KEY\",
  \"source_doc_id\": \"grpc-blob-$(date +%s)\",
  \"filename\": \"$(basename "$DOC_FILE")\",
  \"mime_type\": \"text/plain\",
  \"path\": \"$source_path\",
  \"content\": \"$content_b64\"
}" "$SERVICE_HOST:$INTAKE_GRPC_PORT" ai.pipestream.connector.intake.v1.ConnectorIntakeService/UploadBlob
```

- [ ] **Step 2: Add S3 crawl script**

Create `scripts/e2e/run-s3-crawl.sh` based on the current `scripts/setup-s3-crawl.sh`, but load `.intake-e2e.env`, mask `API_KEY`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY`, and do not print a dry-run command containing raw secrets. The crawl call should remain:

```bash
grpcurl -plaintext \
  -H "x-api-key: $API_KEY" \
  -H "x-datasource-id: $DS_ID" \
  -d "{
    \"datasource_id\": \"$DS_ID\",
    \"bucket\": \"$S3_BUCKET\",
    \"prefix\": \"$S3_PREFIX\",
    \"connection_config\": {
      \"endpoint_override\": \"$S3_ENDPOINT\",
      \"region\": \"$S3_REGION\",
      \"credentials_type\": \"$S3_CREDENTIALS_TYPE\",
      \"access_key_id\": \"$S3_ACCESS_KEY\",
      \"secret_access_key\": \"$S3_SECRET_KEY\",
      \"path_style_access\": $S3_PATH_STYLE
    }
  }" "$SERVICE_HOST:$CONNECTOR_PORT" ai.pipestream.connector.s3.v1.S3ConnectorControlService/StartCrawl
```

- [ ] **Step 3: Add verification script**

Create `scripts/e2e/verify-intake.sh`:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/e2e/lib/common.sh"

require_cmd grpcurl

SERVICE_HOST="${SERVICE_HOST:-localhost}"
INTAKE_GRPC_PORT="${INTAKE_GRPC_PORT:-18108}"

info "Checking connector-intake gRPC health"
grpcurl -plaintext "$SERVICE_HOST:$INTAKE_GRPC_PORT" list ai.pipestream.connector.intake.v1.ConnectorIntakeService >/dev/null
echo "connector-intake gRPC service is reachable"

if command -v redis-cli >/dev/null 2>&1 && [[ -n "${REDIS_URL:-}" ]]; then
  info "Checking Redis ingress stream length"
  redis-cli -u "$REDIS_URL" XLEN "${INTAKE_REDIS_STREAM:-pipestream:intake:ingress}"
fi
```

- [ ] **Step 4: Replace legacy script with compatibility wrapper**

Modify `scripts/setup-s3-crawl.sh` to call:

```bash
#!/usr/bin/env bash
set -Eeuo pipefail
echo "scripts/setup-s3-crawl.sh is kept for compatibility; use scripts/e2e/run-s3-crawl.sh for new workflows." >&2
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/e2e/run-s3-crawl.sh" "$@"
```

- [ ] **Step 5: Validate shell syntax**

Run:

```bash
chmod +x scripts/e2e/upload-intake-docs.sh scripts/e2e/run-s3-crawl.sh scripts/e2e/verify-intake.sh scripts/setup-s3-crawl.sh
bash -n scripts/e2e/upload-intake-docs.sh
bash -n scripts/e2e/run-s3-crawl.sh
bash -n scripts/e2e/verify-intake.sh
bash -n scripts/setup-s3-crawl.sh
```

Expected: no syntax errors.

---

## Task 12: Update README Operator Workflows

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add direct upload quickstart**

Add after the HTTP API section:

```markdown
## Upload A Document

Set these once for local testing:

```bash
export INTAKE_HTTP_PORT=18108
export INTAKE_GRPC_PORT=18108
export SERVICE_HOST=localhost
export DS_ID=<datasource-id>
export API_KEY=<connector-api-key>
```

Raw HTTP upload:

```bash
printf 'hello intake\n' > /tmp/intake-example.txt
curl -fsS \
  -H "x-datasource-id: $DS_ID" \
  -H "x-api-key: $API_KEY" \
  -H "x-source-path: /examples/intake-example.txt" \
  -H "x-filename: intake-example.txt" \
  --data-binary @/tmp/intake-example.txt \
  "http://$SERVICE_HOST:$INTAKE_HTTP_PORT/uploads/raw"
```

Structured PipeDoc upload:

```bash
grpcurl -plaintext -d "{
  \"datasource_id\": \"$DS_ID\",
  \"api_key\": \"$API_KEY\",
  \"source_doc_id\": \"example-pipedoc-1\",
  \"pipe_doc\": {
    \"search_metadata\": {
      \"source_path\": \"/examples/example-pipedoc-1.txt\"
    }
  }
}" "$SERVICE_HOST:$INTAKE_GRPC_PORT" ai.pipestream.connector.intake.v1.ConnectorIntakeService/UploadPipeDoc
```
```

- [ ] **Step 2: Add S3 E2E workflow**

Add:

```markdown
## S3-Compatible E2E Scripts

The scripts in `scripts/e2e/` are intended for QA/operator workflows against a local or shared Pipestream environment. They support AWS S3 and S3-compatible endpoints such as MinIO.

```bash
export S3_ENDPOINT=http://localhost:9000
export S3_BUCKET=pipestream-e2e
export S3_ACCESS_KEY=<access-key>
export S3_SECRET_KEY=<secret-key>
export S3_REGION=us-east-1
export S3_PATH_STYLE=true

scripts/e2e/seed-s3-documents.sh
scripts/e2e/create-datasource.sh
ENV_FILE=.intake-e2e.env scripts/e2e/upload-intake-docs.sh
ENV_FILE=.intake-e2e.env scripts/e2e/run-s3-crawl.sh
scripts/e2e/verify-intake.sh
```

The scripts mask API keys and S3 secrets in output. Generated env files are written with `0600` permissions.
```

- [ ] **Step 3: Add performance note**

Add:

```markdown
## Performance Checks

Connector intake can exceed 1000 MB/s on capable local hardware. Heavy throughput checks are intentionally separate from normal correctness tests so CI does not depend on workstation-class I/O. Run benchmark-style checks explicitly, for example:

```bash
./gradlew test --tests ai.pipeline.connector.intake.BenchmarkIntakeTest -Dbenchmark.intake=true
```
```

- [ ] **Step 4: Review README command safety**

Run:

```bash
rg "SECRET|API_KEY|set -x|password|secret_access_key" README.md scripts/e2e scripts/setup-s3-crawl.sh
```

Expected: no committed literal secrets; examples use placeholders and masking.

---

## Task 13: Full Verification

**Files:**
- All modified files

- [ ] **Step 1: Check remaining Mutiny usage in owned code**

Run:

```bash
rg "Mutiny|Uni|io.smallrye.mutiny" src/main/java src/test/java build.gradle
```

Expected: either no owned Java references, or only the documented `generateMutiny = true` compatibility comment in `build.gradle`.

- [ ] **Step 2: Run normal tests**

Run:

```bash
./gradlew test
```

Expected: all non-benchmark tests pass. If benchmark gating is added, benchmark tests should be skipped unless `-Dbenchmark.intake=true`.

- [ ] **Step 3: Run script syntax checks**

Run:

```bash
bash -n scripts/e2e/lib/common.sh
bash -n scripts/e2e/seed-s3-documents.sh
bash -n scripts/e2e/create-datasource.sh
bash -n scripts/e2e/upload-intake-docs.sh
bash -n scripts/e2e/run-s3-crawl.sh
bash -n scripts/e2e/verify-intake.sh
bash -n scripts/setup-s3-crawl.sh
```

Expected: no syntax errors.

- [ ] **Step 4: Run lints/diagnostics for touched files**

Use IDE diagnostics on:

```text
src/test/java/ai/pipeline/connector/intake/mock/MockEngineService.java
src/test/java/ai/pipeline/connector/intake/mock/MockEngineState.java
src/test/java/ai/pipeline/connector/intake/mock/MockRepositoryService.java
src/test/java/ai/pipeline/connector/intake/ConnectorIntakeIntegrationTest.java
src/test/java/ai/pipeline/connector/intake/BenchmarkIntakeTest.java
README.md
```

Expected: no new compile/linter errors.

- [ ] **Step 5: Optional local E2E smoke**

When local platform dependencies are running:

```bash
scripts/e2e/seed-s3-documents.sh
scripts/e2e/create-datasource.sh
ENV_FILE=.intake-e2e.env scripts/e2e/upload-intake-docs.sh
ENV_FILE=.intake-e2e.env scripts/e2e/run-s3-crawl.sh
scripts/e2e/verify-intake.sh
```

Expected: direct upload commands return successful responses; crawl start returns accepted/started response; verification confirms intake gRPC reachability and optional Redis stream evidence.

---

## Self-Review

- Spec coverage: The plan covers Mutiny-free service/test code, deterministic tests for current architecture, S3/direct-upload operator scripts, README updates, security controls for secrets, and performance separation.
- Scope: The plan stays inside this Gradle root and does not create a full platform orchestrator.
- Type consistency: Test fakes use standard generated gRPC base classes and `StreamObserver`, matching existing production code style.
- Placeholder scan: There are no `TBD`, `TODO`, or unresolved placeholder tasks. README/script examples use angle-bracket placeholders only for user-provided secrets.
