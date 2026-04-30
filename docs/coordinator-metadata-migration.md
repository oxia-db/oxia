# Coordinator Metadata Migration Spec

## Problem

The current `oxiad/coordinator/metadata.Metadata` interface still exposes raw
configuration and status objects:

- desired config through methods like `LoadConfig`, `NamespaceConfig`, `Node`
- runtime status through methods like `LoadStatus`, `UpdateStatus`,
  `ApplyStatusChanges`

This has a few problems:

- callers know too much about the internal storage shape
- admin APIs and reconcilers do not share one domain API
- desired config ownership is ambiguous
- small refactors are hard because many call sites depend on generic methods

The goal is to move toward a metadata API that exposes domain operations such
as namespace and dataserver CRUD/read methods instead of raw config/status
documents.

## Goals

- Hide raw config/status storage details from most callers
- Provide one domain API for:
  - coordinator logic
  - admin API
  - external reconciler
- Support two desired-config ownership modes:
  - `Managed`
  - `Reconciled`
- Keep status persistence and desired-config ownership separate
- Migrate in small PRs, ideally one new method at a time
- Eventually remove the existing generic metadata methods

## Non-Goals

- Do not redesign shard-controller logic in one step
- Do not replace all metadata methods in one PR
- Do not force reconciled mode to persist desired config
- Do not merge desired config and runtime status into one physical file/object

## Design Decisions

### 1. Desired Config and Runtime Status Are Separate

They have different ownership and write patterns:

- desired config:
  - namespaces
  - dataservers
  - server metadata
  - load balancer settings
- runtime status:
  - shard metadata
  - leader/ensemble state
  - split progress
  - instance id

Status is always metadata-owned and persisted.

Config is mode-dependent:

- `Managed` mode:
  - config is persisted somewhere and mutated through metadata APIs
- `Reconciled` mode:
  - config is supplied externally by a reconciler
  - metadata should not persist or directly watch the config file/configmap

### 2. Reconciler and Admin Must Use the Same Domain API

The reconciler should not update raw config structures directly.
It should call the same metadata methods that an admin API would call.

Examples:

- `CreateNamespace`
- `PatchNamespace`
- `DeleteNamespace`
- `CreateDataServer`
- `PatchDataServer`
- `DeleteDataServer`

### 3. Only One Writer Owns Desired Config

Desired config must have one source of truth:

- `Managed` mode:
  - admin API can perform mutable config operations
- `Reconciled` mode:
  - reconciler performs mutable config operations
  - admin API is read-only for desired config

Admin reads are always allowed.

### 4. Provider Should Not Be The Final Public API

The public abstraction for callers should remain `metadata.Metadata`, not
`provider.Provider`.

Internally we may end up with:

- a status persistence backend
- a managed config persistence backend
- an in-memory reconciled config backend

But callers should use domain methods on `Metadata`.

## Target Shape

### Desired State API

These are examples of the intended direction:

- namespaces
  - `ListNamespaces()`
  - `GetNamespace(name string)`
  - `CreateNamespace(...)`
  - `PatchNamespace(...)`
  - `DeleteNamespace(name string)`

- dataservers
  - `ListDataServers()`
  - `GetDataServer(id string)`
  - `CreateDataServer(...)`
  - `PatchDataServer(...)`
  - `DeleteDataServer(id string)`

### Runtime State API

These should be migrated later:

- shard reads
  - `GetShard(namespace, shard)`
  - `ListNamespaceShards(namespace)`

- shard/status mutations
  - narrow shard mutation methods
  - controller-owned status updates

## Storage Model

### Status

Status is always persisted.

For file mode, this should map to a status file.

### Config

Config depends on mode.

#### Managed Mode

Config must be stored somewhere.

For file mode, use a separate config file from status.

For configmap mode, use a separate logical object/key from status.

#### Reconciled Mode

Config should not be stored by metadata.

Instead:

- an external reconciler watches file/configmap/etc.
- reconciler loads desired config
- reconciler updates metadata's in-memory config state through domain methods

## Reconciler

The file/configmap watcher should move out of `metadata`.

Responsibilities:

- watch external config source
- parse/validate desired config
- compare against current desired state if needed
- apply changes through metadata domain methods

Metadata should become passive and stop owning configuration watch logic.

## Migration Strategy

The migration should be done in small PRs.

### Phase 0: Freeze The Old API

Rule:

- do not add new callers to:
  - `LoadConfig`
  - `LoadStatus`
  - `NamespaceConfig`
  - `Node`
  - `Nodes`
  - `NodesWithMetadata`
  - `GetDataServerInfo`
  - `ApplyStatusChanges`

New work should prefer newly introduced narrow methods.

### Phase 1: Foundations

These PRs are small structural changes to make later one-method PRs possible.

#### PR 1: Introduce Config Ownership Mode

Add an internal mode concept:

- `Managed`
- `Reconciled`

No behavior change beyond plumbing.

#### PR 2: Split Metadata Internals Into Config And Status Paths

Keep the public `Metadata` interface unchanged for now.

Internally separate:

- status persistence
- desired config state

No caller migration yet.

#### PR 3: Move File/ConfigMap Watch Logic Out Of Metadata

Introduce reconciler-owned watch/apply flow.

Metadata no longer directly watches config sources.

### Phase 2: Add Narrow Read APIs

Do reads first because they are lowest risk.

Suggested order:

#### PR 4: `ListNamespaces()`

Migrate one simple admin caller.

#### PR 5: `GetNamespace(name)`

Migrate targeted namespace lookups.

#### PR 6: `ListDataServers()`

Migrate admin dataserver listing.

#### PR 7: `GetDataServer(id)`

Migrate `GetDataServerInfo` and simple `Node(...)`-style reads.

### Phase 3: Add Narrow Desired-State Write APIs

Each write method should be gated by config ownership mode.

In `Reconciled` mode:

- admin write calls should fail
- reconciler remains the writer

Suggested order:

#### PR 8: `CreateNamespace(...)`
#### PR 9: `PatchNamespace(...)`
#### PR 10: `DeleteNamespace(name)`
#### PR 11: `CreateDataServer(...)`
#### PR 12: `PatchDataServer(...)`
#### PR 13: `DeleteDataServer(id)`

### Phase 4: Migrate Runtime/Status APIs

Only after desired state is mostly migrated.

Examples:

- `GetShard`
- `ListNamespaceShards`
- narrow shard mutation methods

### Phase 5: Delete Old Generic Methods

Once the last caller is gone, remove:

- `LoadConfig`
- `NamespaceConfig`
- `Node`
- `Nodes`
- `NodesWithMetadata`
- `GetDataServerInfo`

Later, after status callers are migrated:

- `LoadStatus`
- `UpdateStatus`
- `ApplyStatusChanges`
- `StatusChangeNotify`

## First Recommended PR

The best next PR is:

- introduce config ownership mode

Why:

- small
- low risk
- needed for both managed and reconciled behavior
- does not force immediate caller migration

## Open Questions

1. What is the exact persisted backend for managed config in each provider mode?
   - file
   - configmap
   - raft
   - memory

2. Should managed config writes be implemented directly as CRUD methods, or
   should there be an internal replace/patch layer first?

3. How should metadata readiness behave in reconciled mode before the first
   desired config has been applied?

4. Do we want one `Metadata` interface long term, or separate desired/runtime
   interfaces after the migration is complete?
