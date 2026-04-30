<!--
Copyright 2023-2026 The Oxia Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Coordinator Metadata Domain API Spec

## Problem

The current coordinator metadata layer is still storage-shaped instead of
domain-shaped.

Today the `metadata` package exposes a mixed surface that includes:

- low-level persistence operations such as loading and replacing protobuf
  documents
- cached configuration indexes and watch mechanics
- domain mutations that are partly implemented in `metadata`, partly in
  coordinator, and partly in utility functions

This leads to a few problems:

- callers manipulate `ClusterConfiguration` and `ClusterStatus` directly
  instead of using resource-level operations
- declarative configuration handling and imperative management are not clearly
  separated
- coordinator invariants are spread across multiple packages
- it is difficult to expose a stable admin API because the internal API is not
  yet resource-oriented
- multi-document writes are hard to optimize because the mutation boundary is
  not explicit

## Goals

- make coordinator metadata operations domain-level
- expose resource-shaped APIs centered on `Namespace`, `DataServer`,
  `ShardMetadata`, and cluster-scoped settings
- route all mutating operations through coordinator domain logic
- support both declarative management and imperative admin management through
  the same coordinator domain service
- convert configuration watching into reconciler-driven behavior
- phase out the existing storage-shaped metadata API
- enable future internal batch mutation support for metadata persistence
- break the migration into many small PRs, ideally one domain API per PR

## Non-Goals

- redesign `metadata.proto` in the first phase
- introduce hybrid field ownership or Kubernetes-style server-side apply
- allow both declarative and imperative writers to be authoritative at the
  same time without explicit mode selection
- expose raw metadata batching or transaction semantics as public API
- introduce a public snapshot aggregate API

## Design Decisions

### Coordinator owns all mutations

All domain mutations must go through coordinator.

The flow is:

- declarative reconciler -> coordinator domain service
- admin API -> coordinator domain service
- coordinator domain service -> metadata store

The flow is not:

- declarative reconciler -> metadata directly
- admin API -> metadata directly

### Metadata becomes an internal store boundary

The metadata package should stop being treated as the public domain surface.

Its long-term role is:

- persistence
- cached reads needed by coordinator internals
- internal batch mutation support

Its long-term role is not:

- public resource mutation API

### Declarative and imperative management are both supported

Oxia must support:

- declarative management from config
- imperative management from admin APIs

These two management styles share the same coordinator domain service, but they
cannot both be authoritative at the same time without conflict handling.

The first phase therefore defines explicit management modes:

- `declarative`
- `imperative`

In `declarative` mode:

- desired state comes from the reconciler input
- admin mutating APIs should be rejected with a clear error

In `imperative` mode:

- desired state comes from internal persisted resources
- declarative reconciler is disabled

Hybrid ownership is explicitly out of scope for the first phase.

### No public snapshot API

The target domain API is resource-oriented.

We do not expose a public aggregate such as `Snapshot()` because that would
recreate the same storage-shaped pattern at a different level.

### HierarchyPolicies belongs to Namespace

`HierarchyPolicies` is part of namespace configuration and should not become a
separate top-level resource.

It is managed through namespace operations, especially `PatchNamespace`.

### Metadata protobuf refactoring comes later

The protobuf document shape should follow the domain API, not lead it.

The first phase keeps `ClusterConfiguration` and `ClusterStatus` as internal
persistence documents while the public coordinator API becomes resource-shaped.

Only after the domain API is stable should we decide whether
`metadata.proto` needs another structural refactor.

The one explicit exception is the data server naming cleanup described below,
because the current names make the domain rollout harder to read.

### Rename DataServerInfo to DataServer

The current protobuf naming is backward for a domain API:

- `DataServer` is currently the nested identity/address message
- `DataServerInfo` is currently the actual resource-shaped object

Before the domain API rollout, the spec renames these types to make the domain
surface read naturally:

```proto
message DataServerIdentity {
  optional string name = 1;
  string public = 2;
  string internal = 3;
}

message DataServer {
  DataServerIdentity identity = 1;
  DataServerMetadata metadata = 2;
}
```

This is a targeted rename, not a broader metadata protobuf redesign.

## Target Architecture

### Coordinator Domain API

Coordinator itself exposes the domain API that owns all metadata mutations.

The initial resource groups are:

- namespaces
- data servers
- shards
- cluster-scoped settings

### Declarative Reconciler

Configuration watch logic is replaced by a reconciler-style component.

The reconciler:

- reads desired state from config input
- reads current desired state through coordinator domain APIs
- computes resource-level differences
- calls coordinator domain operations such as create, patch, and delete

The reconciler does not write metadata directly.

### Admin API

Admin RPCs become thin wrappers over coordinator domain APIs.

They do not manipulate protobuf metadata documents directly.

### Metadata Store

The metadata store persists internal protobuf documents and eventually supports
internal batched mutation to reduce document modification overhead.

Batching is an internal optimization, not a public API contract.

## Domain API Shape

The public target is resource-oriented CRUD-style APIs.

### Namespace

```go
CreateNamespace(ns *proto.Namespace) error
GetNamespace(name string) (*proto.Namespace, bool)
ListNamespaces() []*proto.Namespace
PatchNamespace(name string, fn func(*proto.Namespace) error) (*proto.Namespace, error)
DeleteNamespace(name string) error
```

Namespace ownership rules:

- `name` is immutable
- `initialShardCount` is immutable
- `replicationFactor` is patchable
- `notificationsEnabled` is patchable
- `keySorting` is patchable
- `policy` is patchable

Create behavior:

- persists namespace desired state
- allocates initial shard metadata in status

Delete behavior:

- removes the namespace from desired state
- marks owned shards for deletion in status

### Data Server

```go
CreateDataServer(dataServer *proto.DataServer) error
GetDataServer(id string) (*proto.DataServer, bool)
ListDataServers() []*proto.DataServer
PatchDataServer(id string, fn func(*proto.DataServer) error) (*proto.DataServer, error)
DeleteDataServer(id string) error
```

Data server ownership rules:

- identity is `GetNameOrDefault()`
- `name` is immutable after create
- `internal` is immutable after create
- `public` is patchable
- metadata labels are patchable

Delete behavior:

- removes the data server from desired state
- coordinator handles draining and rebalance behavior

### Shard

```go
GetShard(namespace string, shardID int64) (*proto.ShardMetadata, bool)
ListShards(namespace string) map[int64]*proto.ShardMetadata
PatchShard(namespace string, shardID int64, fn func(*proto.ShardMetadata) error) (*proto.ShardMetadata, error)
DeleteShard(namespace string, shardID int64) error
```

Notes:

- shard creation is namespace-driven in the first phase
- there is no public `CreateShard` API in the first phase
- shard APIs are primarily for runtime metadata mutation and read access

### Cluster-Scoped Settings

```go
GetLoadBalancer() *proto.LoadBalancer
PatchLoadBalancer(fn func(*proto.LoadBalancer) error) (*proto.LoadBalancer, error)

GetAllowExtraAuthorities() []string
PatchAllowExtraAuthorities(fn func([]string) ([]string, error)) ([]string, error)
```

These remain cluster-scoped configuration, not independent resources.

## Coordinator Responsibilities

Coordinator domain operations own:

- validation
- immutability enforcement
- defaulting where required by domain semantics
- coupled config/status updates
- transition semantics such as draining, shard allocation, and namespace shard
  deletion behavior

Callers should not reimplement these rules.

## Metadata Responsibilities

Metadata internals remain responsible for:

- loading persisted documents
- storing persisted documents
- watches and caches needed by coordinator internals
- internal batched persistence when introduced

Metadata internals should stop exposing resource mutation as raw document
operations once coordinator domain APIs are in place.

## Reconciler Model

The declarative reconciler replaces the existing config-watch direct mutation
path.

Its steps are:

1. load desired state from config input
2. read current desired state through domain list/get APIs
3. calculate resource-level differences
4. call domain create/patch/delete operations
5. rely on coordinator and metadata internals to persist the resulting state

The reconciler should be able to reconcile:

- namespaces
- data servers
- cluster-scoped settings

Shard resources are managed indirectly through namespace reconciliation in the
first phase.

## Admin API Model

The admin API becomes an imperative frontend to the same coordinator domain
service.

The admin API should eventually expose:

- namespace get/list/create/patch/delete
- data server get/list/create/patch/delete
- shard get/list/patch/delete as needed
- load balancer get/patch
- allow-extra-authorities get/patch

In `declarative` mode, mutating admin APIs must be rejected.

## Internal Batch Mutation

Many domain operations modify both desired state and runtime status.

Examples:

- `CreateNamespace`
- `DeleteNamespace`
- `DeleteDataServer`

To keep document modification cost under control, metadata internals should
eventually support batching.

This batching is:

- internal
- coordinator-owned
- invisible to external callers

The expected shape is one domain operation causing one internal batched
read-modify-write sequence.

## Deprecation Strategy

The current storage-shaped metadata surface should be deprecated gradually.

Examples of methods expected to disappear from public usage:

- `LoadStatus`
- `UpdateStatus`
- `UpdateShardMetadata`
- `DeleteShardMetadata`
- `LoadConfig`
- `ConfigWatch`
- `Nodes`
- `NodesWithMetadata`
- `Namespace`
- `Node`
- `GetDataServerInfo`
- `ApplyStatusChanges`

These may remain temporarily as adapters while domain APIs are introduced one
by one.

## Detailed PR Plan

The migration should be split into many small PRs.

Rule for each PR:

- one new domain API
- the minimum internal metadata/store changes required for that API
- tests for that API
- migrate one real caller if there is a natural first caller
- no unrelated protobuf redesign

PR1 is the one explicit exception to this rule because it is a naming cleanup
that makes the subsequent domain APIs easier to understand.

### Naming Preparation

PR1:

- rename protobuf messages:
  - `DataServer` -> `DataServerIdentity`
  - `DataServerInfo` -> `DataServer`
- rename the nested field:
  - `dataServer` -> `identity`
- update existing callers and compatibility codecs
- no behavior change beyond naming

### Namespace APIs

PR2:

- add `GetNamespace`

PR3:

- add `ListNamespaces`

PR4:

- add `CreateNamespace`
- move initial shard allocation behavior behind this API

PR5:

- add `PatchNamespace`
- include `HierarchyPolicies` updates here

PR6:

- add `DeleteNamespace`
- move namespace deletion and shard deletion marking behind this API

### Data Server APIs

PR7:

- add `GetDataServer`

PR8:

- add `ListDataServers`

PR9:

- add `CreateDataServer`

PR10:

- add `PatchDataServer`

PR11:

- add `DeleteDataServer`

### Shard APIs

PR12:

- add `GetShard`

PR13:

- add `ListShards`

PR14:

- add `PatchShard`

PR15:

- add `DeleteShard`

### Cluster-Scoped Settings APIs

PR16:

- add `GetLoadBalancer`

PR17:

- add `PatchLoadBalancer`

PR18:

- add `GetAllowExtraAuthorities`

PR19:

- add `PatchAllowExtraAuthorities`

### Declarative Reconciler

PR20:

- extract reconciler skeleton from current config watch path

PR21:

- reconciler uses namespace get/list APIs

PR22:

- reconciler uses namespace create/patch/delete APIs

PR23:

- reconciler uses data server create/patch/delete APIs

PR24:

- reconciler uses cluster-scoped patch APIs

### Management Mode

PR25:

- add explicit management mode
- `declarative` or `imperative`

PR26:

- reject mutating admin calls in declarative mode

### Admin API Exposure

PR27:

- expose namespace read APIs

PR28:

- expose namespace create API

PR29:

- expose namespace patch API

PR30:

- expose namespace delete API

PR31:

- expose data server read and write APIs

PR32:

- expose cluster-scoped management APIs

### Internal Batch Optimization

PR33:

- introduce internal metadata batch mutation support

PR34:

- move namespace and data server operations to use internal batch mutation

### Cleanup

PR35 and later:

- deprecate old metadata public APIs one by one
- remove adapters once no callers remain

Final cleanup PR:

- remove the old storage-shaped metadata public surface

## Open Questions

- what is the exact coordinator-internal package name for the new domain
  service
- whether `Patch` should remain callback-based or later become explicit patch
  request types
- whether shard delete should stay public or remain internal-only in the first
  admin API cut
- whether `CreateDataServer` should continue accepting `DataServer` directly or
  move to a more explicit request type after the domain surface stabilizes
