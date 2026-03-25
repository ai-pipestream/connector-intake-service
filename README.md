# connector-intake-service

The front door for documents entering the pipeline. Connectors (JDBC, web crawlers, file system scanners, etc.) send documents here. This service validates the connector's API key, derives a deterministic document ID, optionally persists the document to repository-service, and hands it off to the engine for pipeline processing.

Intake is deliberately **graph-agnostic** -- it handles Tier 1 (service-level) configuration only. The engine handles Tier 2 (graph-level) config resolution, routing, and entry-node selection.

## How It Fits in the Pipeline

```
Connector  --(gRPC/HTTP)--> connector-intake-service --(gRPC)--> engine
                                    |
                                    +--(gRPC)--> repository-service  (optional persist)
                                    +--(gRPC)--> connector-admin     (API key validation)
                                    +--(gRPC)--> account-manager     (account active check)
```

1. Connector sends a document (via gRPC `UploadPipeDoc`, gRPC `UploadBlob`, or HTTP `POST /uploads/raw`).
2. Intake validates `datasource_id` + `api_key` by calling `connector-admin` (datasource-admin) and `account-manager`.
3. Intake derives a deterministic `doc_id` from one of: client-provided ID, `source_doc_id`, `source_uri`, or `source_path`.
4. Based on Tier 1 config, either:
   - **Persist path**: Save to repository-service, then hand off a document reference to the engine.
   - **Inline path**: Hand off the full document directly to the engine.
5. Intake also manages **crawl sessions** -- connectors can start/end sessions and send heartbeats, with session state tracked in PostgreSQL.

## gRPC API

Service: `ConnectorIntakeService` (proto package: `ai.pipestream.connector.intake.v1`)

| RPC | Description |
|-----|-------------|
| `UploadPipeDoc` | Upload a structured PipeDoc. Requires `datasource_id`, `api_key`, and a deterministically derivable `doc_id`. |
| `UploadBlob` | Upload raw binary content. Builds a PipeDoc with a `BlobBag` internally. Always persists to repository. |
| `StartCrawlSession` | Begin a crawl session. Tracks documents found/processed/failed/skipped. |
| `EndCrawlSession` | End a crawl session. Sets state to `COMPLETED` or `FAILED`. |
| `Heartbeat` | Keep a crawl session alive. Updates `last_heartbeat` timestamp. |

## HTTP API

### `POST /uploads/raw`

Binary upload endpoint for large files. Validates headers, derives `doc_id`, then streams the body to repository-service.

Required headers:
- `x-datasource-id` -- datasource identifier
- `x-api-key` -- connector API key
- `Content-Length` -- body size in bytes

For doc_id derivation, provide at least one of (checked in priority order):
- `x-doc-id` -- client-provided document ID
- `x-source-doc-id` -- source system document ID
- `x-source-uri` -- source URI (canonicalized)
- `x-source-path` -- source file path (normalized)

## Doc ID Derivation

Documents need a deterministic ID for deduplication. Intake tries these in order:

1. **Client-provided `doc_id`** -- used as-is
2. **`source_doc_id`** -- prefixed with `{datasource_id}:`
3. **`source_uri`** from `SearchMetadata` -- canonicalized, then prefixed with `{datasource_id}:`
4. **`source_path`** from `SearchMetadata` -- normalized, then prefixed with `{datasource_id}:`

If none are available, the request is rejected.

## Key Classes

| Class | Role |
|-------|------|
| `ConnectorIntakeServiceImpl` | gRPC service implementation. Entry point for all gRPC calls. |
| `EngineClient` | Wraps documents in `PipeStream` and calls the engine's `IntakeHandoff` RPC. |
| `ConfigResolutionService` | Validates API key via `ConnectorValidationService`, returns Tier 1 config. |
| `ConnectorValidationService` | Calls `connector-admin` to validate API keys, calls `account-manager` to check account is active. Results are cached. |
| `SessionManager` | CRUD for crawl sessions in PostgreSQL. |
| `RawUploadResource` | HTTP endpoint for binary uploads. Proxies to repository-service. |
| `IntakeRepoEventConsumer` | Kafka consumer for `IntakeRepoEvent` messages. Hands off persisted documents to engine. |

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `quarkus.http.port` / `%dev.quarkus.http.port` | `18108` | HTTP port |
| `connector-intake.default-rate-limit-per-minute` | `1000` | Default rate limit per connector |
| `connector-intake.max-concurrent-streams` | `100` | Max concurrent gRPC streams |
| `connector-intake.session-timeout-minutes` | `60` | Crawl session timeout |
| `connector-intake.max-document-size-mb` | `10240` | Max document size (10 GB) |

### Service Discovery

Uses Stork with Consul for discovering `connector-admin`, `account-manager`, `repository`, and `engine`.

### Database

PostgreSQL, managed by Flyway migrations. DevServices provisions a container automatically in dev/test.

## Build and Run

Requires: Java 21, Docker (for DevServices).

```bash
# Build
./gradlew build

# Run in dev mode (DevServices starts PostgreSQL, Kafka, Consul)
./gradlew quarkusDev

# Dev mode port: 18108
```

## Test

```bash
./gradlew test
```

Tests use the `pipestream-wiremock-server` container for mock gRPC services (engine, repository, connector-admin, account-manager).
