# Connector Intake Hardening and E2E Design

## Context

`connector-intake-service` is the front door for documents entering the Pipestream pipeline. It validates datasource API keys, derives deterministic document IDs, accepts raw/gRPC document uploads, optionally persists replay copies through repository-service, and hands documents toward engine processing through Redis ingress or the engine handoff stream.

This service is being prepared for open source release and a near-term security review. It also needs to be easy for QA and operators to exercise: someone should be able to upload a document directly to intake, seed an S3-compatible bucket, trigger an S3 crawl, and verify that documents were accepted without reading implementation code.

Current branch state is already mid-refactor:

- Production upload paths are mostly Mutiny-free and use standard gRPC `StreamObserver` callbacks, `CompletableFuture`, and virtual threads.
- Some tests and generated-client usage still depend on Mutiny.
- The current `./gradlew test` run fails in blob upload success tests because the new engine bidi handoff stream is not represented by a deterministic test double. One failure panics through Stork service discovery, and another waits for the full 120 second ack timeout.
- Existing documentation describes the service, but it does not provide a polished "how to upload a document" workflow.
- `scripts/setup-s3-crawl.sh` exists, but it is not yet suitable as a security-review-quality operator script.

Performance is a first-class requirement. On this machine, connector intake is capable of transfer rates above 1000 MB/s. The hardening work must preserve that characteristic: correctness and security tests should not introduce hot-path abstractions that reduce throughput, and performance checks should make regressions visible without making the normal unit suite slow or flaky.

## Goals

1. Make application and test code Mutiny-free for this service's owned code paths.
2. Replace stale tests with deterministic coverage for current architecture:
   - unary gRPC `UploadPipeDoc`
   - bidi gRPC `UploadPipeDocStream`
   - gRPC `UploadBlob`
   - HTTP `POST /uploads/raw`
   - Redis ingress envelope/enqueue behavior
   - fire-and-forget replay persistence
   - engine handoff stream ack, rejection, reset, and timeout behavior
3. Add operator-ready scripts for:
   - seeding an S3-compatible bucket with sample documents
   - creating/configuring a datasource/API key
   - uploading documents directly into intake
   - triggering an S3 connector crawl
   - verifying accepted handoff signals
4. Improve README/operator documentation so QA can upload a document without service-internal knowledge.
5. Preserve high-throughput intake behavior and keep performance checks explicit.

## Non-Goals

- Do not redesign the pipeline architecture or move responsibilities across services.
- Do not implement a full local platform orchestrator in this pass.
- Do not add new persistence products or shared modules.
- Do not make the normal test suite dependent on real AWS S3.
- Do not print or persist secrets in script output.

## Recommended Approach

Use a focused hardening pass that keeps production behavior stable while replacing weak test and operator surfaces.

Alternative approaches considered:

- Minimal patch: fix the current failing tests and add one upload command to the README. This is too small for the security-review and open source goals.
- Full demo harness: one command to stand up all dependencies, seed S3, create accounts/datasources, upload docs, and verify downstream outputs. Useful later, but too broad for this pass and likely to mix intake hardening with platform orchestration.
- Recommended middle path: harden intake boundaries and provide S3/operator E2E scripts that can run against existing local or shared environments. This gives QA a professional workflow without coupling this repository to every platform service lifecycle.

## Architecture

### Upload Boundaries

Keep the service boundaries already implied by the refactor:

- `ConnectorIntakeServiceImpl` remains a thin gRPC adapter.
- `UnaryPipeDocUploadService` validates and derives doc identity for unary PipeDoc uploads.
- `StreamingPipeDocObserver` serializes inbound stream events through its mailbox and delegates accepted items to `PipeDocAcceptanceService`.
- `PipeDocAcceptanceService` routes hydrated gRPC PipeDocs to Redis ingress by default, supports explicit direct-engine compatibility modes, and optionally schedules replay persistence.
- `GrpcBlobUploadService` builds a PipeDoc around raw blob bytes.
- `BlobUploadHandoffService` persists gRPC blob uploads to repository-service, then hands off a reference to engine through `EngineClient`.
- `RawUploadResource` remains a blocking virtual-thread HTTP proxy to repository-service for large raw uploads.
- `IntakeHandoffStreamClient` owns the long-lived engine bidi stream.

The design should not add a new generic intake orchestrator. These boundaries are already small enough and map well to behavior that needs independent tests.

### Mutiny-Free Direction

Owned code should use standard gRPC generated classes and Java concurrency primitives:

- standard `*Grpc.*Stub` and `*ImplBase`
- `StreamObserver`
- `CompletableFuture`
- virtual threads for blocking waits at RPC boundaries
- explicit timeouts where an upstream/downstream call can hang

Remove or avoid:

- `MutinyConnectorIntakeServiceGrpc` in tests
- `MutinyNodeUploadServiceGrpc` in test mocks
- `Uni` in benchmark/tests
- `generateMutiny = true` if proto-toolchain and downstream generated code allow it in this repository

If platform tooling still requires Mutiny generation for external compatibility, document that generation remains enabled but service-owned code does not use it. The implementation should verify this before changing generation flags.

### Test Design

Split tests by boundary rather than by transport accident.

Unit/component tests:

- `PipeDocAcceptanceServiceTest`
  - enqueues inline handoff request to `IntakeIngressProducer`
  - stamps `INGRESS_MODE_GRPC_INLINE`
  - schedules replay persistence only when `shouldPersist()` is true
  - maps retryable enqueue failures for streaming responses
- `IntakeReplayPersisterTest`
  - successful repository response logs/returns without surfacing to caller
  - repository rejection is logged and swallowed
  - transport error is logged and swallowed
  - executor shuts down on destroy
- `BlobUploadHandoffServiceTest`
  - repository success plus engine accepted returns success
  - repository rejection returns failure without engine handoff
  - engine permanent/retryable rejection returns failure with useful message
  - repository call interruption restores interrupt flag and maps to `CANCELLED`
- `IntakeHandoffStreamClientTest`
  - accepted ack maps to `IntakeHandoffResponse`
  - retryable/permanent ack maps to the expected gRPC status
  - stream error fails in-flight calls and allows lazy reopen
  - ack timeout removes in-flight state
- `StreamingPipeDocObserverTest`
  - first message must be context
  - context validation failure is a non-retryable ack
  - item ack includes source doc id and derived doc id
  - cancellation stops writes
  - mailbox overflow maps to `RESOURCE_EXHAUSTED`
- `IntakeIngressEnvelopeTest`
  - inline/reference payload modes
  - required fields
  - malformed base64/missing request handling

Quarkus integration tests:

- unary PipeDoc accepts through the configured inline handoff mode
- streaming PipeDoc accepts through the configured inline handoff mode
- gRPC blob upload uses fake repository and fake engine stream
- raw HTTP upload proxies to fake repository HTTP server
- auth failures remain explicit and do not touch downstream systems

The fake engine must implement the bidi stream RPC and send deterministic acks. Blob upload tests should not connect to Stork/Consul unless the test explicitly verifies service discovery.

Performance tests:

- Keep heavy throughput tests separate from normal `test`, either behind a Gradle property or a dedicated task.
- Preserve a smoke benchmark that can validate the >1000 MB/s class of throughput on capable hardware without making CI flaky.
- Measure the hot path directly and avoid using Mutiny in the benchmark harness.

### Operator Scripts

Replace the single ad hoc script with a small `scripts/e2e/` suite:

- `scripts/e2e/lib/common.sh`
  - strict mode
  - dependency checks
  - JSON helpers using `jq`
  - secret masking
  - temp dir handling and cleanup
- `scripts/e2e/seed-s3-documents.sh`
  - supports AWS S3 and S3-compatible endpoints such as MinIO
  - uploads small text, JSON, PDF/doc fixture where available, and nested-prefix examples
  - never echoes secret keys
- `scripts/e2e/create-datasource.sh`
  - creates or reuses account/datasource
  - stores API key in a local env file with restrictive permissions
  - masks secrets in stdout
- `scripts/e2e/upload-intake-docs.sh`
  - demonstrates HTTP raw upload
  - demonstrates gRPC `UploadPipeDoc`
  - demonstrates gRPC `UploadBlob`
  - prints the exact doc ids accepted
- `scripts/e2e/run-s3-crawl.sh`
  - starts the S3 connector crawl for the configured bucket/prefix
  - supports dry-run output without exposing secrets
- `scripts/e2e/verify-intake.sh`
  - checks for accepted responses and, where available, Redis ingress or downstream handoff evidence

Script constraints:

- Use `bash`, `grpcurl`, `jq`, and AWS CLI-compatible commands.
- Do not source arbitrary config files without validating ownership/permissions or clearly documenting the trust boundary.
- Do not print API keys or S3 secrets.
- Prefer `mktemp` and `chmod 600` for generated credential files.
- Support `--dry-run` and `--verbose`, with verbose still masking secrets.
- Exit non-zero with actionable error messages.

### Documentation

Update `README.md` with a clear operator section:

- prerequisites
- service ports
- how to upload one raw document with `curl`
- how to upload a PipeDoc with `grpcurl`
- how to upload a blob with `grpcurl`
- how to seed S3 and run the E2E scripts
- expected success responses
- common failure messages and fixes

Add examples that are safe to paste into a terminal. Use placeholders for secrets and avoid committing environment files.

## Error Handling

Use explicit, testable failure mapping:

- validation failures return failure responses for current RPC behavior
- auth failures preserve `UNAUTHENTICATED`/`PERMISSION_DENIED`
- retryable downstream failures use retryable statuses in streaming acks
- engine handoff timeout is bounded and removes in-flight state
- replay persistence failures never block user-facing acceptance, but are visible in logs
- operator scripts fail fast with clear messages and no leaked credentials

Where current behavior returns success/failure protobuf responses instead of gRPC errors, tests should encode that contract. Avoid silently changing public behavior during hardening.

## Security Considerations

- API keys and S3 secrets must not appear in logs, scripts, dry-run output, committed examples, or test artifacts.
- Raw upload error JSON should continue escaping response messages.
- Tests should verify invalid API keys and inactive/missing accounts do not call repository, Redis ingress, or engine.
- Generated credential files should be opt-in, local-only, and mode `0600`.
- Scripts should avoid `set -x` around secrets and should mask known secret values.
- README should clearly distinguish local/dev plaintext endpoints from production TLS expectations.
- Redis production configuration should document authentication/TLS expectations; local dev may remain unauthenticated.

## Verification Plan

Normal development verification:

- `./gradlew test`
- targeted test classes for upload boundaries
- script shell lint where available
- dry-run script execution with sample env files

Optional/local E2E verification:

- start platform dependencies
- seed S3-compatible bucket
- create datasource/API key
- upload direct intake docs
- trigger S3 crawl
- verify accepted docs and handoff evidence

Performance verification:

- run dedicated throughput smoke benchmark on a capable local machine
- record throughput and payload shape
- avoid gating regular CI on the 1000 MB/s target unless CI hardware is known to support it

## Implementation Sequence

1. Fix test infrastructure by adding deterministic fake engine stream behavior and removing Stork-dependent engine calls from ordinary tests.
2. Convert remaining test clients/mocks/benchmarks off Mutiny.
3. Add focused tests for current upload, replay, Redis ingress, and engine stream behavior.
4. Add E2E scripts with strict secret handling.
5. Update README with direct upload and S3 crawl workflows.
6. Run normal tests, script dry-runs, and a local throughput smoke where practical.

## Open Decisions

- Whether `generateMutiny = true` can be disabled safely in this repository without disrupting platform proto-toolchain conventions.
- Whether E2E verification should inspect Redis ingress directly, engine acknowledgements, repository state, or all of the above in the first pass.
- Which sample document set should be canonical for open source examples: generated text/JSON only, or fixtures from `ai.pipestream:test-documents` copied through a helper.
