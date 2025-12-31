# Service Responsibilities - Datasource Configuration Ownership

This document clarifies the ownership and responsibilities of each service in the 2-tier datasource configuration architecture.

## Service Ownership Split

### datasource-admin (service-level)

**Owns**:
- **DatasourceDefinition**: Connector types/templates and their capabilities
  - Connector registration metadata (display name, owner, tags, documentation)
  - Connector capabilities and configuration schemas
- **Tier 1 Configuration** (Global/Default):
  - Connector defaults (strongly typed + custom_config defaults)
  - DataSource instance-level Tier 1 overrides
  - Custom config schemas (Tier 1 and Tier 2 node schemas)
- **DataSource Entity**: Service-level datasource instances
  - References connector type
  - References drive_name (storage abstraction)
  - Contains Tier 1 config overrides
- **Validation API**: Provides datasource validation and config resolution for intake service

**Does NOT Own**:
- ❌ Tier 2 Configuration (graph-level) - owned by Engine
- ❌ Graph topology/routing - owned by Engine
- ❌ Storage details (S3, credentials) - owned by repository-service

### pipestream-engine (graph-level)

**Owns**:
- **Graph Topology**: Nodes, edges, and graph structure
- **DatasourceInstance** (graph-level):
  - Graph-versioned instance of a datasource
  - Contains Tier 2 config only (`node_config`)
  - Binds `datasource_id` → `entry_node_id`
  - References `datasource_id` from datasource-admin (does NOT duplicate Tier 1 config)
- **Tier 2 Configuration** (Per-Node):
  - Node-specific config overrides
  - Output hints (desired_collection, routing hints)
  - Node-specific custom config
- **Routing Logic**: Resolves active graph, routes documents to entry nodes
- **Graph Activation**: Controls which graph version is active per cluster

**Does NOT Own**:
- ❌ Tier 1 Configuration (service-level) - owned by datasource-admin
- ❌ DatasourceDefinition - owned by datasource-admin
- ❌ Connector registration - owned by datasource-admin

### connector-intake-service (orchestration)

**Owns**:
- **Tier 1 Configuration Resolution**: Resolves Tier 1 config from datasource-admin only
- **Persistence Decision**: Applies Tier 1 config to decide when to persist documents
- **Ingestion Orchestration**: Coordinates between datasource-admin, repository-service, and engine

**Does NOT Own**:
- ❌ Tier 1 Configuration storage - reads from datasource-admin
- ❌ Tier 2 Configuration resolution - Engine handles this during IntakeHandoff
- ❌ Graph topology - Intake is graph-agnostic
- ❌ Graph routing - Engine handles all routing based on datasource_id

### repository-service (storage abstraction)

**Owns**:
- **Drive Entity**: Physical storage configuration
  - S3 bucket details
  - Credentials (stored in Infisical)
  - KMS encryption keys
  - Lifecycle policies
- **Storage Operations**: Save, retrieve, hydrate blobs

**Does NOT Own**:
- ❌ Datasource configuration - datasources reference drives by name only

## Configuration Resolution Flow

```
1. Intake receives request with datasource_id
   ↓
2. Query datasource-admin for Tier 1 config:
   - Load Connector entity (defaults)
   - Load DataSource entity (Tier 1 overrides)
   - Merge: Connector defaults + DataSource overrides = Tier 1 config
   ↓
3. Apply Tier 1 config:
   - Persistence decision: Check Tier 1 persistence_config.persist_pipedoc
   - Persist to repository if needed (based on Tier 1 config)
   - Build base IngestionConfig with Tier 1 hydration_config
   ↓
4. Forward to engine:
   - Pass datasource_id, account_id, and Tier 1 IngestionConfig
   - Engine resolves DatasourceInstance(s) for datasource_id in active graphs
   - Engine merges Tier 2 config (from DatasourceInstance.node_config) into IngestionConfig
   - Engine routes to entry_node_id(s) from DatasourceInstance(s)
```

**Key Design**: Intake is graph-agnostic and only uses Tier 1 config. Engine handles all Tier 2 resolution and graph routing internally.

## Key Design Principles

1. **Service-Level vs Graph-Level**: 
   - Tier 1 (service-level) = "What is this datasource?" (owned by datasource-admin)
   - Tier 2 (graph-level) = "How is this datasource used in this graph?" (owned by engine)

2. **Single Source of Truth**:
   - Tier 1 config: datasource-admin is authoritative
   - Tier 2 config: Engine (graph) is authoritative
   - No duplication: Engine references datasource_id, doesn't duplicate Tier 1 config

3. **Separation of Concerns**:
   - datasource-admin: Manages datasource definitions and service-level config
   - Engine: Manages graph topology and graph-level datasource usage
   - Intake: Orchestrates configuration resolution and ingestion decisions

## Naming: connector-admin = datasource-admin

**Note**: The service is named `connector-admin` in the codebase but is referred to as `datasource-admin` in design documents. These are the **same service** - no rename is planned.

The name `connector-admin` remains because:
- It manages both **connectors** (DatasourceDefinition) and **datasources** (DataSource entity)
- Renaming would require updating multiple repositories, CI configs, and service references
- The gRPC service name (`DataSourceAdminService`) already reflects the datasource focus

When reading documentation:
- `datasource-admin` = `connector-admin` (same service)
- Both terms may be used interchangeably

The service owns:
- ✅ Service-level datasource management (Tier 1)
- ✅ Connector/DatasourceDefinition registration
- ✅ DataSource entity lifecycle

The service does NOT own:
- ❌ Graph-level configuration (Tier 2) - Engine owns this
- ❌ Graph topology/routing - Engine owns this

## Clarification: Engine Does NOT Own Datasource Config

**Question**: Is Engine in charge of datasource config?

**Answer**: **No** - Engine only owns **graph-level** (Tier 2) configuration:
- Engine owns: How a datasource is used in a specific graph (entry node, output hints, node-specific overrides)
- Engine does NOT own: What the datasource is, its service-level defaults, or connector definitions

This split makes sense because:
- **Tier 1** (datasource-admin): Service-level, reusable across graphs
- **Tier 2** (engine): Graph-specific, versioned with the graph

Just like modules:
- ModuleDefinition (service-level) is owned by platform-registration
- Node config (graph-level) is owned by engine

Datasources follow the same pattern:
- DatasourceDefinition + Tier 1 config (service-level) owned by datasource-admin
- DatasourceInstance + Tier 2 config (graph-level) owned by engine

