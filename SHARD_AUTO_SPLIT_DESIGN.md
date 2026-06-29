# Shard Auto-Split Design

## Overview

Extend the coordinator to automatically split shards when they exceed configurable
size or throughput thresholds. This complements the existing manual split API
(`oxia admin split-shard`) by allowing the cluster to self-manage shard granularity.

Auto-splitting is **disabled by default** and must be explicitly enabled in the
cluster configuration.

## Goals

1. Automatically detect when a shard needs splitting based on DB size and/or throughput
2. Split one shard at a time per cluster to limit blast radius
3. Keep the manual split API fully functional and independent
4. Provide a stable, conservative default that operators can tune

## Non-Goals

- Shard merging (reverse of split)
- Cross-namespace split coordination
- Automatic split-point optimization (always use hash-range midpoint)

---

## Configuration

Add a cluster-wide `shard_management` section to `ClusterConfiguration` in
`common/proto/metadata.proto`. This is intentionally cluster-wide (not per-namespace)
for simplicity. The section is designed to also accommodate future auto-merge settings.

```protobuf
message AutoSplitConfig {
  bool enabled = 1;                    // default: false
  uint32 max_shard_size_mb = 2;        // split when DB disk usage exceeds this (default: 1024 = 1 GiB)
  uint32 max_throughput_ops = 3;       // combined reads + writes threshold (default: 10000)
  string stabilization_period = 4;     // how long a threshold must be exceeded (default: "1m")
  string cooldown_period = 5;          // minimum time between consecutive splits (default: "5m")
}

message ShardManagement {
  uint32 max_shards_per_namespace = 1; // safety guard-rail (default: 64)
  AutoSplitConfig auto_split = 2;
  // reserved for future: auto_merge = 3;
}

message ClusterConfiguration {
  repeated Namespace namespaces = 1;
  repeated DataServerIdentity servers = 2;
  repeated string allow_extra_authorities = 3;
  map<string, DataServerMetadata> server_metadata = 4;
  LoadBalancer load_balancer = 5;
  ShardManagement shard_management = 6;  // new
}
```

### Threshold semantics

- **`max_shard_size_mb`**: Compared against Pebble `DiskSpaceUsage()` on the shard leader.
  A shard is a split candidate when its size exceeds this value for the full
  `stabilization_period`. Default: `1024` (1 GiB).
- **`max_throughput_ops`**: Combined read + write operations per second. Computed from
  the delta of per-shard cumulative operation counters
  (`gets + lists + range_scans + puts + deletes + delete_ranges`) sampled over the
  collection interval. A value of `0` disables this threshold. Default: `10000`.
- **`max_shards_per_namespace`**: Per-namespace guard-rail. Auto-split will not split a shard if
  the namespace already has this many shards. Default: `64`. This also applies to
  manual splits via the admin API.
- A shard becomes a split candidate if **any** enabled threshold is exceeded for the
  stabilization period.

### Hot-reload

The `shard_management` config is re-read from the cluster config on every evaluation
cycle via `metadata.GetConfig()`. Changes take effect immediately on the next tick —
no coordinator restart required. This is critical for being able to disable auto-split
quickly if it misbehaves.

---

## Data Collection: Extending `GetStatus` RPC

The coordinator needs per-shard size and operation counts from data server leaders.
Rather than scraping Prometheus, extend the existing internal `GetStatus` RPC in
`common/proto/replication.proto`:

```protobuf
message GetStatusResponse {
  int64 term = 1;
  ServingStatus status = 2;

  int64 head_offset = 3;
  int64 commit_offset = 4;

  // New field for auto-split
  ShardStats shard_stats = 5;
}

message ShardStats {
  uint64 db_size_bytes = 1;      // pebble DiskSpaceUsage()
  uint64 read_ops_total = 2;     // cumulative reads (gets + lists + range_scans)
  uint64 write_ops_total = 3;    // cumulative writes (puts + deletes + delete_ranges)
}
```

The data server leader controller (`oxiad/dataserver/controller/lead/leader_controller.go`)
already has access to the DB instance via `lc.db`; populating these fields in `GetStatus`
is straightforward. The follower controller returns nil for `shard_stats` (the coordinator
only collects stats from leaders).

**Why extend GetStatus instead of a new RPC?**
- The coordinator already calls `GetStatus` in periodic health checks (shard controller)
  and during splits (split controller)
- Avoids adding a new RPC surface
- The extra fields are small and always available

---

## Coordinator Component: `AutoSplitMonitor`

A new component that runs inside the coordinator runtime
(`oxiad/coordinator/runtime/`), alongside the existing `LoadBalancer`.
It follows a similar pattern: background goroutine with a periodic ticker.

### Lifecycle

```
runtime.New()
  ├── loadBalancer.Start()
  └── autoSplitMonitor.start()       // new background goroutine via wg.Go

runtime.Close()
  ├── ctxCancel()                    // signals all background goroutines
  ├── splitControllers close
  ├── shardControllers close
  └── wg.Wait()                      // waits for autoSplitMonitor + action worker
```

### Structure

```go
// In oxiad/coordinator/runtime/autosplit/monitor.go

type Monitor struct {
    ctx    context.Context

    metadata   coordmetadata.Metadata
    rpc        rpc.Provider
    splitter   controller.ShardSplitter

    // Track per-shard stats over time
    shardStats map[shardKey]*shardStatsTracker
    mu         sync.Mutex

    // Track cooldown
    lastSplitCompleted time.Time

    collectionInterval time.Duration   // how often to poll leaders (default: 30s)
}

type shardKey struct {
    namespace string
    shardId   int64
}

type shardStatsTracker struct {
    // Latest sample
    lastSample      *proto.ShardStats
    lastSampleTime  time.Time

    // Computed rate (combined reads + writes)
    throughputOps   float64  // ops/sec

    // How long the shard has continuously exceeded a threshold
    exceedingSince  time.Time   // zero value = not exceeding
}
```

### Main Loop

```
every collectionInterval:
  1. config := metadata.GetConfig() → get shard_management.auto_split settings
  2. If not enabled, clear trackers, skip
  3. status := metadata.ListNamespaceStatus()
  4. For each namespace, for each shard in SteadyState with a leader:
     a. Call rpc.GetStatus on the leader → get ShardStats
     b. Update shardStatsTracker (compute throughput rate from ops_total delta)
     c. Evaluate thresholds (size and/or throughput)
  5. If any shard exceeds thresholds for >= stabilizationPeriod:
     a. Pick the "worst offender" (highest overshoot ratio)
     b. Check cooldown (time since last completed split)
     c. Check whether a split is already in progress (any shard has SplitMetadata)
     d. Check maxShardsPerNamespace guard-rail for the namespace
     e. If clear: initiate split via splitter.InitiateSplit()
  6. Clean up trackers for shards that no longer exist
```

### Split candidate selection

When multiple shards exceed thresholds simultaneously, pick the one with the
highest **overshoot ratio**:

```
overshootRatio = max(
    sizeMB / maxShardSizeMB,                // if maxShardSizeMB > 0
    throughputOps / maxThroughputOps,        // if maxThroughputOps > 0
)
```

This prioritizes the shard that is furthest past its limits.

### Interaction with manual splits

- Before initiating an auto-split, the monitor checks whether any shard in the
  cluster status has `SplitMetadata` set — if so, a split (auto or manual) is
  already in progress and the monitor waits.
- Manual splits via the admin API bypass threshold checks but prevent auto-split
  from starting a concurrent one.

### Interaction with leader balancer

- Auto-split should not trigger during leader rebalancing. Before initiating a split,
  check that the target shard is in `SteadyState` with no pending ensemble changes.
  `InitiateSplit` already validates this, so a rejection is safe (retry next cycle).

---

## Observability

New metrics exposed by the coordinator:

| Metric | Type | Description |
|--------|------|-------------|
| `oxia_coordinator_autosplit_evaluations_total` | Counter | Number of threshold evaluation cycles |
| `oxia_coordinator_autosplit_splits_initiated_total` | Counter | Auto-splits started |
| `oxia_coordinator_autosplit_splits_completed_total` | Counter | Auto-splits completed successfully |
| `oxia_coordinator_autosplit_splits_aborted_total` | Counter | Auto-splits that timed out or failed |
| `oxia_coordinator_autosplit_shard_size_bytes` | Gauge (per shard) | Last observed DB size |
| `oxia_coordinator_autosplit_shard_throughput_ops` | Gauge (per shard) | Last computed throughput (reads + writes) |

All log lines are tagged with `slog.String("component", "auto-split-monitor")`.
Log at `Info` level when a split is initiated, completed, or aborted. Log at `Debug`
level for each evaluation cycle with per-shard stats.

---

## Failure Modes & Safety

| Scenario | Behavior |
|----------|----------|
| Leader unreachable during stats collection | Skip shard this cycle, retain previous tracker state |
| Split fails / times out | SplitController aborts; cooldown applies before retry |
| Coordinator failover mid-split | Existing `restartInProgressSplits()` recovers the split; auto-split monitor restarts fresh (no persistent state needed — thresholds re-evaluated from live data) |
| Shard in election | Skip — only evaluate shards in `SteadyState` |
| Config change disables auto-split | Monitor stops evaluating; in-progress split runs to completion |
| Rapid growth exceeding threshold immediately after split | Cooldown period prevents re-splitting the children too quickly; children need to exceed thresholds independently for a full stabilization period |
| Namespace hits maxShardsPerNamespace | Auto-split skips the namespace; logs a warning so operators can raise the limit or investigate |

---

## Implementation Plan

### Phase 1: Data collection infrastructure
1. Add `ShardStats` message to `common/proto/replication.proto`
2. Add `shard_stats` field to `GetStatusResponse`
3. Populate `ShardStats` in the leader controller's `GetStatus` handler
   (`oxiad/dataserver/controller/lead/leader_controller.go`)
4. Regenerate proto (`make proto`)

### Phase 2: Configuration
5. Add `AutoSplitConfig` and `ShardManagement` messages to `common/proto/metadata.proto`
6. Add `shard_management` field to `ClusterConfiguration`
7. Regenerate proto (`make proto`)
8. Add helper functions for parsing duration fields and applying defaults

### Phase 3: Auto-split monitor
9. Create `oxiad/coordinator/runtime/autosplit/monitor.go` with stats collection loop
10. Implement threshold evaluation and stabilization tracking
11. Wire into runtime lifecycle (`runtime.New()` — start as background goroutine via `wg.Go`)

### Phase 4: Split coordination
12. Connect auto-split monitor to `ShardSplitter.InitiateSplit()`
13. Add split-in-progress check (scan cluster status for any active `SplitMetadata`)
14. Add `maxShardsPerNamespace` guard-rail check in `InitiateSplit`

### Phase 5: Observability & testing
15. Add metrics
16. Unit tests for threshold evaluation, stabilization, cooldown, candidate selection
17. Integration test: configure auto-split, write data until threshold, verify split happens

---

## Resolved Design Decisions

1. **Hot-reloadable config** — Yes. The monitor re-reads `shard_management` from
   cluster config every cycle via `metadata.GetConfig()`. Operators can disable
   auto-split at runtime without restarting the coordinator.

2. **Admin API for auto-split status** — Not now. Component-tagged structured logs
   (`component=auto-split-monitor`) plus the per-shard gauge metrics provide sufficient
   observability. Can be revisited later if needed.

3. **Max shard count guard-rail** — Yes. `max_shards_per_namespace: 64` (per namespace, default).
   Enforced for both auto and manual splits.

4. **Split-in-progress detection** — Scan cluster status for any shard with active
   `SplitMetadata` rather than maintaining a separate flag. This is naturally correct
   across coordinator failovers and manual splits.
