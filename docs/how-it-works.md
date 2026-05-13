# How Connector Intake Works

`connector-intake-service` is the entry point for documents coming from connectors. A connector may be a crawler, an S3 reader, a JDBC extractor, a filesystem scanner, or a custom integration. Intake does not decide how a document moves through a graph. Its job is to admit the document, give it a stable identity, apply service-level policy, and hand it to the next part of the platform.

The service is intentionally graph-agnostic. It understands the datasource and the account. The engine understands graph routing, entry nodes, and graph-level overrides.

## The Short Version

A connector sends a document to intake over gRPC or HTTP. Intake validates the datasource API key, checks that the owning account is active, derives a deterministic `doc_id`, and chooses the right handoff path for the upload type.

Small structured `PipeDoc` uploads normally go into the Redis intake stream. For compatibility deployments, the same inline `PipeDoc` request can be sent directly to engine instead. Large raw uploads and blob uploads are staged through `repository-service`, then handed to the engine as document references. Crawl sessions are tracked in PostgreSQL so callers can start a crawl, send heartbeats, and close the crawl with a clear status.

## What Intake Owns

Intake owns the boundary between connector clients and the platform. That includes request validation, datasource authentication, document identity, upload admission, crawl-session state, and the first handoff into processing.

It does not own graph selection, graph routing, parser selection, chunking, embedding, indexing, or downstream retry policy. Those belong to the engine and pipeline modules.

This separation keeps connector-facing behavior predictable. A connector only needs to know its datasource id, API key, and document metadata. It does not need graph internals.

## Upload Paths

There are three main upload paths.

`UploadPipeDoc` is the structured gRPC path. The client sends a `PipeDoc`, and intake derives or confirms its `doc_id`. By default, intake writes an engine handoff request to the Redis ingress stream. The engine sidecar drains that stream and forwards the document into engine admission. Operators can set `pipestream.intake.inline-handoff-mode=engine-unary` for older engine deployments, or `engine-stream` for direct stream handoff.

`UploadBlob` is the gRPC binary path. Intake wraps the bytes in a `PipeDoc`, stores the document through `repository-service`, and sends a reference to the engine. This avoids pushing large blobs through Redis.

`POST /uploads/raw` is the HTTP streaming path for large files. Intake validates headers, derives the document id, and streams the request body directly to `repository-service`. This path is intended for callers that have a file stream rather than a prebuilt protobuf message.

## Validation and Identity

Every upload is tied to a datasource. Intake validates the datasource id and API key by calling datasource-admin, then checks the owning account through account-manager. The result is a Tier 1 configuration snapshot for the request.

Documents need stable IDs because retries and repeated crawls should refer to the same source document. Intake derives `doc_id` in a fixed order:

1. Use the client-provided `doc_id` when present.
2. Otherwise use `source_doc_id`, scoped by datasource.
3. Otherwise use a canonical `source_uri`, scoped by datasource.
4. Otherwise use a normalized `source_path`, scoped by datasource.

If none of those inputs are available, intake rejects the upload. Guessing a document id would make deduplication and audit behavior harder to reason about.

## Redis Ingress

For structured PipeDoc uploads, intake writes an `IntakeIngressEnvelope` to the Redis stream configured as `pipestream:intake:ingress` when `pipestream.intake.inline-handoff-mode=redis`, which is the default. The envelope carries the serialized engine handoff request along with searchable fields such as datasource id, account id, crawl id, doc id, stream id, payload mode, retry count, and accepted timestamp.

Redis is used here as a queue between connector-facing intake and engine admission. Intake returns once the request has been accepted into that queue. The engine sidecar is responsible for draining it and calling engine.

## Repository Persistence

Repository persistence appears in three places, each with a different reason.

Raw HTTP uploads are always streamed to repository because the body may be large and should not be buffered inside intake.

gRPC blob uploads are also persisted first, then handed to engine as references. This keeps large byte payloads out of Redis and out of engine handoff messages.

Structured PipeDoc uploads can optionally schedule a replay copy. That copy is fire-and-forget: it is for later replay, not for admitting the document into the engine path. A replay persistence failure is logged, but it does not change whether the document was accepted by the selected inline handoff path.

## Engine Handoff

When intake needs to call engine directly, it uses a long-lived bidi gRPC stream through `IntakeHandoffStreamClient`. Each request gets a handoff id, and the client waits for the matching engine ack. Accepted acks return normally. Retryable and permanent rejections are mapped to gRPC status errors for callers that need to make a decision.

The Redis path is different. For the default `UploadPipeDoc` mode, intake does not call engine directly. It writes to Redis and lets the sidecar do the engine handoff. The compatibility modes bypass Redis for inline `PipeDoc` uploads and call engine directly.

## Crawl Sessions

Crawl sessions give connectors a way to report crawl lifecycle. A connector can start a session, send heartbeats while it is running, and end it as completed or failed. Intake stores that state in PostgreSQL.

The session APIs are deliberately separate from document upload. A connector can upload documents with or without a crawl id, but when a crawl id is present the platform has a stable handle for progress reporting and cleanup.

## Failure Behavior

Most validation failures are returned as ordinary failure responses on the upload RPCs. Authentication and permission failures keep their gRPC status semantics internally and are surfaced in the response message where the public RPC contract expects response objects.

Streaming uploads return per-item acknowledgements. A bad stream context is non-retryable. A transient downstream issue can be marked retryable on the item ack. Client cancellation is treated as a normal end condition; intake stops writing responses to a closed stream.

For background replay persistence, failures are logged and swallowed by design. That path must not delay or change the primary engine admission path.

## Operational Notes

The service discovers platform dependencies through Stork and Consul in normal deployments. Tests use `pipestream-wiremock-server` and local test doubles for gRPC services that need streaming behavior.

PostgreSQL stores crawl-session state. Redis stores intake ingress messages. Kafka carries repository intake events back to intake for reference handoff. The service itself remains stateless for upload admission apart from those external systems.

For a visual overview, see `docs/architecture-c4.md`.
