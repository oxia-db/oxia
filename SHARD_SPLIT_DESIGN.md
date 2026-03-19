# Shard Split Design

## Overview

This document describes the mechanism to split shards in Oxia without downtime.
A shard split takes a parent shard and produces two child shards, each covering
half of the parent's hash range. The parent continues serving traffic until the
children are fully caught up, at which point traffic is atomically cut over.

## Terminology

| Term | Definition |
|------|-----------|
| **Parent shard** | The shard being split |
| **Child shards** | The two new shards created by the split (left/right) |
| **Split point** | The hash boundary dividing parent's range into two halves |
| **Catch-up phase** | Period where children follow the parent leader's WAL |
| **Cutover** | Atomic switch of traffic from parent to children |

## Split Trigger

Initially, splits are triggered manually via a new admin RPC:

```protobuf
// In replication.proto, on OxiaCoordination service
rpc SplitShard(SplitShardRequest) returns (SplitShardResponse);

message SplitShardRequest {
  string namespace = 1;
  int64 shard = 2;
  // Optional: explicit split point. If omitted, split at midpoint.
  optional uint32 split_point = 3;
}

message SplitShardResponse {
  int64 left_child_shard = 1;
  int64 right_child_shard = 2;
}
```

The coordinator exposes this RPC. The `oxia admin` CLI gets a new `split-shard`
subcommand.

Later, auto-splitting can be added based on shard size or load metrics.

## No New Shard Status

The split lifecycle is **not** tracked via a new `ShardStatus`. The existing
status enum (`SteadyState`, `Election`, `Deleting`) remains unchanged.

**Rationale**: The parent shard must remain free to transition between
`SteadyState` and `Election` during the split process, which may take a
significant amount of time. If the parent leader crashes mid-split, the
coordinator must be able to run a normal leader election for it. A `Splitting`
status would conflict with the `Election` status and require special-casing the
entire election code path.

Instead, the split state is tracked through a separate `SplitMetadata` field on
`ShardMetadata`, orthogonal to the operational status. The parent shard keeps
its normal status and the shard controller continues to manage elections,
ensemble changes, etc. as usual.

## Cluster Status Extension

Add split metadata to `ShardMetadata` to track the relationship between parent
and children:

```go
// model/cluster_status.go

type SplitPhase int

const (
    SplitPhaseInit       SplitPhase = iota
    SplitPhaseBootstrap              // Sending snapshots to children
    SplitPhaseCatchUp                // Children following parent WAL
    SplitPhaseCutover                // Fencing parent, electing children
    SplitPhaseCleanup                // Deleting parent shard
)

type SplitMetadata struct {
    Phase SplitPhase `json:"phase"`

    // ParentShardId is set on child shards, pointing to the parent they
    // were split from.
    ParentShardId int64 `json:"parentShardId"`

    // ChildShardIds is set on the parent shard, pointing to the children.
    ChildShardIds []int64 `json:"childShardIds,omitempty"`

    // SplitPoint is the hash boundary. Left child gets [min, splitPoint],
    // right child gets [splitPoint+1, max].
    SplitPoint uint32 `json:"splitPoint"`

    // SnapshotOffset is the parent WAL offset at which the snapshot was
    // taken. Children catch up from this point.
    SnapshotOffset int64 `json:"snapshotOffset,omitempty"`
}

type ShardMetadata struct {
    Status         ShardStatus    `json:"status" yaml:"status"`
    Term           int64          `json:"term" yaml:"term"`
    Leader         *Server        `json:"leader" yaml:"leader"`
    Ensemble       []Server       `json:"ensemble" yaml:"ensemble"`
    RemovedNodes   []Server       `json:"removedNodes" yaml:"removedNodes"`
    // ... existing fields ...
    Int32HashRange Int32HashRange `json:"int32HashRange" yaml:"int32HashRange"`

    // Non-nil only when this shard is involved in an active split
    // (either as parent or as child).
    Split *SplitMetadata `json:"split,omitempty" yaml:"split,omitempty"`
}
```

The coordinator's shard controller checks `Split != nil` to know whether
split-related work is pending, independent of the shard's operational status.

## Split Phases

The split proceeds through these phases, orchestrated by the coordinator:

```
┌──────────┐    ┌──────────────┐    ┌──────────────┐    ┌─────────┐    ┌─────────┐
│ 1. INIT  │───>│ 2. BOOTSTRAP │───>│ 3. CATCH-UP  │───>│4.CUTOVER│───>│5.CLEANUP│
└──────────┘    └──────────────┘    └──────────────┘    └─────────┘    └─────────┘
```

Parent shard status throughout phases 1-3: `SteadyState` (or `Election` if a
leader failure occurs - handled normally by the existing shard controller).

### Phase 1: Initialization

**Coordinator actions:**

1. Validate the split request:
   - Parent shard exists and is in `SteadyState`
   - Parent shard does not already have `Split != nil`
   - Hash range has room to split (max - min >= 1)
2. Compute the split point (midpoint of parent's hash range, or use explicit
   value)
3. Allocate two new shard IDs from `ShardIdGenerator`
4. Assign ensembles for both children (use the existing placement policy)
5. Update `ClusterStatus`:
   - Set parent's `Split` with `Phase: SplitPhaseInit` and `ChildShardIds`
   - Create two new child shard entries with:
     - Status: `SteadyState` (they don't serve traffic yet - no assignment
       is published for them until cutover)
     - Hash ranges: `[parent.min, splitPoint]` and `[splitPoint+1, parent.max]`
     - `Split.ParentShardId` pointing to parent
6. Persist the updated cluster status

**Important**: At this point, the parent shard continues serving all traffic
normally. No client shard assignments change. The children exist only in
`ClusterStatus` - they are not included in `ShardAssignments` broadcasts yet.

### Phase 2 & 3: Bootstrap and Catch-up via Observer Followers

The parent leader should not need to know about splits. Instead, the
coordinator treats the two children leaders as **observer followers** of the
parent leader. The parent's existing `FollowerCursor` machinery handles
everything: snapshot transfer, WAL tailing, and acknowledgment tracking.

**Why this works**: The existing `FollowerCursor` already:
- Detects that a new follower has `ackOffset == InvalidOffset` and
  automatically sends a full snapshot (`shouldSendSnapshot()`)
- After the snapshot, tails the WAL from the snapshot offset forward
- Streams `Append` messages with entries and commit offset
- Handles reconnection on stream failures

The children leaders are just regular followers from the parent leader's
perspective. The only differences are:
1. They are not part of the parent's replication quorum (their acks don't
   count toward commit)
2. The child filters keys by hash range when applying entries to its DB

**Required change on the parent leader**: The `AddFollower` method currently
rejects followers beyond `replicationFactor - 1`:

```go
// leader_controller.go:459
if len(lc.followers) == int(lc.replicationFactor)-1 {
    return nil, errors.New("all followers are already attached")
}
```

This needs to be relaxed to allow non-voting **observer** followers. The
implementation:

- Add an `observer` flag to `AddFollowerRequest`
- Track observers in a separate map (`lc.observers`) from voting followers
  (`lc.followers`). The `replicationFactor - 1` check only applies to the
  voting followers map
- Observers get a `FollowerCursor` just like voting followers — same snapshot
  and WAL tailing machinery — but their `CursorAcker` does **not** participate
  in the `QuorumAckTracker`. Observer acks are tracked only so the coordinator
  can monitor catch-up progress
- When the leader is fenced (new term), observer cursors are closed along with
  all other cursors. The coordinator re-adds them on the new leader if needed

**Proto change:**

```protobuf
message AddFollowerRequest {
  string namespace = 1;
  int64 shard = 2;
  int64 term = 3;
  string follower_name = 4;
  EntryId follower_head_entry_id = 5;

  // NEW: if true, this follower is an observer that does not participate
  // in the replication quorum. Used for shard split catch-up.
  bool observer = 6;

  // NEW: the target child shard ID. Allows the parent leader to track
  // observers by target shard (key: "follower:targetShardId") to avoid
  // collisions when both children's observers land on the same node.
  int64 target_shard = 7;

  // NEW: the hash range assigned to the target child shard. The parent
  // leader's observer cursor propagates this to the child follower via
  // gRPC stream metadata, enabling receiver-side key filtering.
  optional io.oxia.proto.v1.Int32HashRange split_hash_range = 8;
}
```

**Hash range propagation**: The coordinator includes the child's hash range in
`AddFollowerRequest.split_hash_range`. The parent leader stores this on the
observer `FollowerCursor`. When the cursor sends a snapshot or streams WAL
entries, it injects the hash range as gRPC outgoing metadata
(`split-hash-range-min` / `split-hash-range-max`). On the receiving child
follower, `SetSplitHashRange()` activates receiver-side filtering: the child
applies `FilterDBForSplit()` after snapshot install and
`ApplyLogEntryWithSplitFilter()` during WAL replay, both of which skip keys
whose hash falls outside the child's range. This approach means the parent
leader streams data unmodified while each child filters independently.

**Abort cleanup (observers)**: The coordinator removes observer cursors
from the parent leader via the `RemoveObserver` RPC without fencing the
parent. This closes the observer's gRPC stream and removes it from the
`lc.observers` map. The parent continues serving traffic uninterrupted.
Observer cursors are also automatically closed if the parent is fenced
via `NewTerm` (e.g., during parent leader election or cutover).

**Coordinator actions (phase 2 - bootstrap):**

1. Ensure parent shard is in `SteadyState` with a healthy leader
2. For each child shard, run `NewTerm` on the child's ensemble to fence them
3. For each child leader, send `AddFollower` to the parent leader with
   `observer = true` and `follower_head_entry_id = {term: -1, offset: -1}`
   (indicating empty follower)
4. The parent leader's `FollowerCursor` detects the empty follower and
   automatically sends a full Pebble snapshot via `SendSnapshot`
5. Update `SplitMetadata.Phase` to `SplitPhaseBootstrap`

**Child leader actions (on receiving snapshot):**

1. Install the snapshot via the existing `InstallSnapshot` flow (clear WAL,
   close DB, load snapshot chunks, reopen DB)
2. **New step: filter keys by hash range.** After the DB is opened from
   the snapshot, iterate all keys and handle each category as described in
   the Key Classification section below.
3. After filtering, the child has a clean DB with only the keys it owns.
4. Send `SnapshotResponse` to the parent with the ack offset — this tells
   the parent's `FollowerCursor` to transition to WAL tailing.

**Important sequencing**: The child leader must **not** start replicating to
its own followers until the snapshot is fully received and filtered, and the
parent has been fenced. The coordinator enforces this by only running
`BecomeLeader` on the child (which attaches its own followers) during the
cutover phase, after the parent is fenced and children have consumed all WAL
entries. The sequence is:

1. Coordinator sends `NewTerm` to all child ensemble members (fencing them)
2. Coordinator adds child leader as observer on parent (with `split_hash_range`)
   → parent sends snapshot
3. Child follower receives snapshot, hash range metadata activates
   `FilterDBForSplit()` which filters keys by hash range, then acks back
4. Parent's `FollowerCursor` transitions to WAL tailing for the child
5. Child follower applies WAL entries with `ApplyLogEntryWithSplitFilter()`,
   skipping keys outside its hash range
6. During catch-up, the child tails the parent WAL while the parent serves
   traffic normally
7. At cutover, the parent is fenced and children consume final entries
8. **Only now** does the coordinator send `BecomeLeader` to the child leader,
   which attaches the child's own voting followers
9. The child's followers catch up from the child leader via normal
   `FollowerCursor` (snapshot + WAL), receiving already-filtered data
10. Coordinator waits for child quorum commit before proceeding to cleanup

This ensures the child's followers never see unfiltered data and a quorum
has acknowledged all entries before the parent is deleted.

**Coordinator actions (phase 3 - catch-up):**

1. After verifying child leaders have received and acked the snapshot, update
   `SplitMetadata.Phase` to `SplitPhaseCatchUp`
2. The parent leader continues streaming WAL entries to the children via
   the same `FollowerCursor` — no new RPCs, no new streams. The existing
   `Replicate` stream handles everything.
3. The child leader receives WAL entries and applies only those relevant to
   its hash range (filtering at the DB commit layer, not the WAL layer)
4. Coordinator monitors child offsets via `GetStatus` calls to child leaders.
   When both children are close to the parent's head offset, proceed to cutover.

**Note**: `BecomeLeader` is **not** sent to children during catch-up. The
children operate as standalone followers of the parent until cutover. Only
during cutover (after the parent is fenced and children reach the final offset)
does the coordinator elect child leaders via `BecomeLeader`, which attaches
the children's own voting followers. This ensures children's followers only
ever receive already-filtered data.

**Why full snapshot with receiver-side filtering:**
- Leverages the existing snapshot infrastructure with zero changes to the
  snapshot creation/transfer code path on the parent
- SST files are already compressed; re-creating a filtered DB would lose this
  and require re-compaction
- Filtering after load is a simple iterate-and-delete pass on the child,
  which Pebble handles efficiently (tombstones, compacted away later)
- Avoids adding CPU load on the parent leader (which is serving traffic)
- Hash range propagation via gRPC metadata means the parent leader code needs
  minimal changes — it just stores and forwards the hash range

**Catch-up completion detection**: The coordinator polls `GetStatus` on each
child leader, checking `HeadOffset` against the parent leader's `HeadOffset`.
When both children have reached the parent's current head offset, the
controller transitions to the cutover phase.

**Leader election during catch-up**: If the parent leader fails during
catch-up, the parent's shard controller runs a normal election (parent status
goes to `Election` then back to `SteadyState`). The observer follower cursors
are closed when the old leader is fenced. After the new parent leader is
elected, the coordinator re-sends `AddFollower(observer=true)` for each child.
The new leader's `FollowerCursor` resumes from the child's current ack offset
— sending a snapshot only if the child is behind the WAL's first offset,
otherwise tailing from where the child left off. The `SplitMetadata` on the
cluster status is unaffected.

### Phase 4: Cutover

This is the critical phase that must be atomic from the client's perspective.

**Sequence:**

1. **Fence the parent**: The coordinator increments the parent's term and sends
   `NewTerm` to the parent leader. This causes the parent to:
   - Stop accepting new writes (return `FENCED` errors)
   - Close all follower cursors (including observer cursors to children)
   - The parent's current clients will get errors and re-fetch shard assignments

2. **Final catch-up**: After fencing, the parent's WAL is frozen. The
   coordinator re-adds the children as observers on the fenced parent (or on
   the new term). The children consume the remaining WAL entries up to the
   final offset. This is a short wait since children were already close.

3. **Elect child leaders**: The coordinator runs a full election for both child
   shards — fencing the ensemble with a new term (`NewTerm`), then sending
   `BecomeLeader` to the child's current leader (the node that received the
   snapshot). `BecomeLeader` attaches the child's voting followers, which start
   replicating from the child leader.

4. **Wait for child quorum commit**: After electing child leaders, the
   coordinator waits for each child's `CommitOffset` to reach the parent's
   final offset. The `CommitOffset` only advances when a quorum of followers
   has acknowledged the data. This ensures that even if a child leader fails
   immediately after cutover, a quorum of followers has the data and can elect
   a new leader without data loss. Without this step, a child leader failure
   during cleanup could lose data if followers hadn't yet caught up.

5. **Update shard assignments**: The coordinator broadcasts new
   `ShardAssignments` to all nodes and clients:
   - Remove the parent shard assignment
   - Add both child shard assignments with their respective hash ranges

6. **Client behavior**: When clients receive the new shard assignments, the
   existing overlap detection in `shard_manager.go` handles the transition:
   ```go
   // This existing code already handles split correctly:
   // New child shards overlap the old parent shard, so the parent
   // is automatically removed from the client's shard map.
   for shardId, existing := range s.shards {
       if overlap(update.HashRange, existing.HashRange) {
           delete(s.shards, shardId)
       }
   }
   ```

**Downtime window**: Between fencing the parent and child leaders becoming
available, clients will receive transient errors. This window is bounded by:
- Time for children to consume remaining parent WAL entries (small, since
  children are already nearly caught up)
- Time for child leader election (typically < 1 second)
- Time for child followers to replicate and commit (quorum acknowledgement)
- Time for shard assignment propagation to clients

This is not a full outage - only the keys in the split shard's hash range are
temporarily unavailable. All other shards continue serving normally.

To minimize this window further (future optimization): the coordinator can
pre-elect child leaders and only fence the parent as the very last step, doing
the final WAL tail catch-up with an extremely short fence.

### Phase 5: Cleanup

After cutover and quorum commit confirmation, the parent shard is no longer
needed and can safely be deleted (a quorum of child followers has the data):

1. Coordinator sets parent shard status to `Deleting`
2. Sends `DeleteShard` to all parent ensemble members
3. Data servers delete parent's WAL and DB files
4. Coordinator removes parent shard entry from `ClusterStatus`
5. Coordinator clears `SplitMetadata` from both child shards

## Data Flow Diagram

```
                    ┌───────────────────────────────────────────┐
                    │             COORDINATOR                    │
                    │                                            │
                    │  1. Validate & allocate child IDs          │
                    │  2. Update ClusterStatus                   │
                    │  3. AddFollower(observer, split_hash_range)│
                    │  4. Monitor catch-up progress              │
                    │  5. Fence parent, elect children           │
                    │  6. Wait for child quorum commit           │
                    │  7. Broadcast new assignments              │
                    └──────────┬──────────┬────────────────────┘
                               │          │
               ┌───────────────┘          └──────────────────┐
               ▼                                             ▼
   ┌────────────────────┐                      ┌────────────────────┐
   │   PARENT SHARD     │                      │   PARENT SHARD     │
   │   (Leader)         │                      │   (Followers)      │
   │                    │                      │                    │
   │ Status: SteadyState│  normal replication  │ - Replicate as     │
   │ (unaware of split) ├─────────────────────>│   usual            │
   │                    │                      │                    │
   │ - Serves traffic   │                      └────────────────────┘
   │ - FollowerCursor   │
   │   per child (keyed │
   │   by targetShardId)│
   │ - Observer, not in │
   │   quorum           │
   │ - Propagates hash  │
   │   range via gRPC   │
   │   stream metadata  │
   └──────┬────────┬────┘
          │        │
          │        │  SendSnapshot + Replicate (existing RPCs)
          │        │  + gRPC metadata: split-hash-range-min/max
          ▼        ▼
┌──────────────┐  ┌──────────────┐
│ LEFT CHILD   │  │ RIGHT CHILD  │
│ [min, split] │  │ [split+1,max]│
│  (follower)  │  │  (follower)  │
│              │  │              │
│ - Recv snap  │  │ - Recv snap  │
│ - Filter DB  │  │ - Filter DB  │
│   by hash    │  │   by hash    │
│   (receiver  │  │   (receiver  │
│    side)     │  │    side)     │
│ - Recv WAL   │  │ - Recv WAL   │
│   entries    │  │   entries    │
│ - Filter at  │  │ - Filter at  │
│   apply time │  │   apply time │
│              │  │              │
│ After cutover│  │ After cutover│
│ - Elected    │  │ - Elected    │
│   leader     │  │   leader     │
│ - Replicate  │  │ - Replicate  │
│   to own     │  │   to own     │
│   followers  │  │   followers  │
│ - Quorum     │  │ - Quorum     │
│   commit     │  │   commit     │
│   verified   │  │   verified   │
└──────────────┘  └──────────────┘
```

## Proto Changes Summary

### replication.proto changes

```protobuf
// Existing message, extended with observer + split fields:
message AddFollowerRequest {
  string namespace = 1;
  int64 shard = 2;
  int64 term = 3;
  string follower_name = 4;
  EntryId follower_head_entry_id = 5;

  // Observer follower does not participate in replication quorum.
  // Used for shard split catch-up.
  bool observer = 6;

  // Target child shard ID, used for observer map keying.
  int64 target_shard = 7;

  // Hash range for the target child, propagated to child follower via
  // gRPC metadata to enable receiver-side key filtering.
  optional io.oxia.proto.v1.Int32HashRange split_hash_range = 8;
}

// NEW: admin trigger for shard split
rpc SplitShard(SplitShardRequest) returns (SplitShardResponse);

message SplitShardRequest {
  string namespace = 1;
  int64 shard = 2;
  optional uint32 split_point = 3;
}

message SplitShardResponse {
  int64 left_child_shard = 1;
  int64 right_child_shard = 2;
}
```

No new RPCs for snapshot transfer or WAL streaming — the existing
`SendSnapshot` and `Replicate` RPCs are reused as-is via the observer
`FollowerCursor`. Hash range propagation uses gRPC stream metadata
(`split-hash-range-min` / `split-hash-range-max` constants) rather than
new message fields on the replication stream.

## Coordinator Split Controller

The coordinator manages the split via a `SplitController` that drives the state
machine. It runs alongside the parent's `ShardController`, not replacing it.

```go
type SplitController struct {
    namespace      string
    parentShardId  int64
    leftChildId    int64
    rightChildId   int64
    splitPoint     uint32
    snapshotOffset int64
    phase          SplitPhase

    rpcProvider    RpcProvider   // for GetStatus, NewTerm, BecomeLeader, etc.
    clusterStatus  ClusterStatus // shared cluster state
    eventListener  SplitEventListener // notifies coordinator on completion
}
```

The `SplitController` is created by the coordinator when a split is requested.
Its phase is persisted in `ClusterStatus` via `SplitMetadata`. On coordinator
restart, in-progress splits are resumed from the persisted phase.

Key methods:
- `waitForChildOffset(childId, targetOffset)` — polls child leader's
  `HeadOffset` via `GetStatus` until it reaches the target
- `waitForChildCommit(childId, targetOffset)` — polls child leader's
  `CommitOffset` (quorum-acknowledged offset) via `GetStatus` until it reaches
  the target. This ensures a majority of followers have the data.
- `electChild(childId)` — fences child ensemble with `NewTerm`, then sends
  `BecomeLeader` to the child's current leader

The parent's `ShardController` continues to operate normally. If it needs to
run a leader election while a split is in progress, it does so without any
special handling. The `SplitController` adapts to the new leader by re-adding
the children as observer followers on the new leader.

## Key Implementation Considerations

### 1. Full Snapshot + Post-Load Filtering (Receiver-Side)

The child receives the full Pebble checkpoint via the existing `SendSnapshot`
infrastructure (raw SST files streamed as chunks). Filtering happens entirely
on the **receiver** (child follower), triggered by the hash range propagated
via gRPC stream metadata. When `SetSplitHashRange()` is called on the child
follower, it activates `FilterDBForSplit()` after snapshot install.

The filter iterates **all** keys in the DB and handles each category
appropriately. All keys (user data, sessions, indexes, notifications,
metadata) live in the same Pebble keyspace — the `__oxia/` prefix is just a
convention, not a separate column family.

```go
func FilterDBForSplit(db database.DB, hashRange Int32HashRange) error {
    // Iterate ALL keys, including internal ones
    iter := db.RawIterator()  // no filtering
    defer iter.Close()

    batch := db.NewWriteBatch()
    for iter.First(); iter.Valid(); iter.Next() {
        key := iter.Key()
        action := classifyKeyForSplit(key, iter.Value(), hashRange)
        switch action {
        case deleteKey:
            batch.Delete(key)
        case keepKey:
            // no-op
        case resetKey:
            batch.Set(key, resetValue)
        }
    }
    return batch.Commit()
}
```

The key classification rules are detailed in the following sections.

Pebble handles deletes efficiently as tombstones that are compacted away in
the background.

**Abort cleanup (snapshot filtering)**: `FilterDBForSplit()` is destructive
and irreversible — keys deleted from the child's DB cannot be recovered.
This is safe for abort because the child shard has never served traffic.
The coordinator aborts by calling `RemoveObserver` on the parent leader
(stops data streaming), then sending `DeleteShard` to all child ensemble
members (deletes DB+WAL). No "unfilter" operation is needed.

### 2. Key Classification During Post-Load Filtering

Every key in the snapshot falls into one of these categories:

#### User data keys (no `__oxia/` prefix)

- Deserialize the `StorageEntry` value to check for `partition_key`
  (`StorageEntry.partition_key`, field 8 in `storage.proto`)
- If `partition_key` is set, compute `hash.Xxh332(partitionKey)`;
  otherwise compute `hash.Xxh332(key)`
- **Delete** if the hash falls outside the child's `Int32HashRange`
- **Keep** if inside the range

#### `__oxia/last-version-id`

- **Keep in both children.** This is the global monotonic version ID counter.
  Both children must start from at least the parent's value to ensure version
  IDs remain globally unique and monotonically increasing. Each child continues
  incrementing independently from this value.

#### `__oxia/commit-offset`, `__oxia/term`, `__oxia/term-options`

- **Keep in both children.** These are inherited from the parent snapshot. They
  will be overwritten when the child runs its own leader election and starts
  committing its own WAL entries.

#### `__oxia/checksum`

- **Reset to zero / delete.** The parent's incremental checksum is invalid
  after filtering removes keys. Each child should start checksumming fresh
  from its own filtered dataset.

#### `__oxia/notifications/{offset}`

- **Filter by key hash.** Deserialize the `NotificationBatch` protobuf, iterate
  the `notifications` map, and remove entries whose key hashes fall outside the
  child's hash range. If the batch becomes empty after filtering, delete the
  entire entry. `ReadNextNotifications` uses RangeScan so offset gaps in the
  notification keyspace are fine.
- Children inherit the parent's offset numbering, allowing clients to resume
  from their last seen offset after re-subscribing to the child shards.

#### `__oxia/session/{session-id}` (session metadata)

- **Duplicate to both children.** A session might own ephemeral keys that
  land in either child's hash range. The session metadata itself is small
  (timeout + identity). It is simpler to copy to both children and let the
  session expire naturally on the child that ends up with no keys for it.
- Alternatively, during the filter pass, track which children have shadow keys
  for each session and only keep the session metadata if at least one shadow
  key survives. This is an optimization for later.

#### `__oxia/session/{session-id}/{user-key}` (session shadow keys)

- These are per-key entries that track which user keys belong to a session.
  The `{user-key}` portion is URL-escaped.
- **Filter by the referenced user key's shard hash.** Decode the user key
  from the shadow key, then determine its shard hash: look up the user key's
  `StorageEntry` — if `partition_key` is set, hash that; otherwise hash the
  key itself. Delete the shadow key if the hash is outside range.

#### `__oxia/idx/{index-name}/{secondary-key}\x01{primary-key}` (secondary indexes)

- **Filter by the primary key's shard hash.** The secondary index entry should
  only exist on the child that owns the primary key. Parse the primary key
  from the index key (after the `\x01` separator, URL-decoded), then determine
  its shard hash the same way: look up the primary key's `StorageEntry` — if
  `partition_key` is set, hash that; otherwise hash the key itself. Delete the
  index entry if outside range.
- Note: since we process user data keys in the same pass, we could also simply
  check whether the primary key still exists in the DB after the user-data
  filtering. But hashing is more robust and doesn't depend on iteration order.

### 3. Key Classification Summary

| Key pattern | Action | Basis |
|-------------|--------|-------|
| User data keys | Filter by hash | `partition_key` or record key hash |
| `__oxia/last-version-id` | Keep | Both children need monotonic IDs |
| `__oxia/commit-offset` | Keep | Overwritten by child leader election |
| `__oxia/term` | Keep | Overwritten by child leader election |
| `__oxia/term-options` | Keep | Overwritten by child leader election |
| `__oxia/checksum` | Delete/reset | Invalid after filtering |
| `__oxia/notifications/*` | Filter by key hash | Remove out-of-range keys; delete empty batches |
| `__oxia/session/{id}` | Keep (both) | Session may own keys in either child |
| `__oxia/session/{id}/{key}` | Filter by key hash | Same as the user key it references |
| `__oxia/idx/{idx}/{sec}\x01{pri}` | Filter by primary key hash | Follows the primary key's ownership |

### 4. WAL Entry Filtering During Catch-up (Receiver-Side)

During catch-up (phase 3), the parent streams all WAL entries unmodified.
The child follower, having received the hash range via gRPC metadata, applies
entries through `ApplyLogEntryWithSplitFilter()` which deserializes each
`LogEntryValue` -> `WriteRequests` -> individual `WriteRequest` and skips
puts/deletes whose key hash falls outside the child's range.

For `delete_range` operations: the child applies the delete range as-is since
it operates on key ranges (not hash ranges) and the child's DB only contains
keys in its hash range after the post-snapshot filter. Keys outside the range
have already been deleted.

**Abort cleanup (WAL filtering)**: `ApplyLogEntryWithSplitFilter()` only
affects the child's DB — the parent's WAL entries are streamed unmodified
and the parent's data is never touched. On abort, deleting the child shard
discards all filtered state.

### 5. Handling Writes During Split

During phases 2-3, the parent continues accepting writes normally. New writes
go through the parent's WAL and are replicated to both the parent's followers
AND streamed to the children. This ensures no data loss.

### 6. Partition Key Handling

Keys with explicit `partition_key` must be routed based on the partition key's
hash, not the record key's hash. Both the post-snapshot filter and the WAL
entry filter use `StorageEntry.partition_key` (for DB entries) or
`PutRequest.partition_key` (for WAL entries) when present.

### 7. Sessions and Ephemeral Keys

Sessions are shard-scoped (created with a specific shard ID). After the split:

- **Session metadata** (`__oxia/session/{id}`) is duplicated to both children
  during the snapshot filter. This is safe because session metadata is small
  (just timeout + identity).
- **Session shadow keys** (`__oxia/session/{id}/{key}`) are filtered by the
  hash of the referenced user key — they follow the same child as the user
  data they point to.
- After cutover, each child independently manages session expiry. A session
  that has no remaining shadow keys on a given child will naturally expire
  with no side effects (the timeout fires, finds no keys to delete).
- **Client sessions**: Clients with active sessions on the parent will get
  errors on the next heartbeat. They must create new sessions on the
  appropriate child shard(s). The client library's session logic already
  handles reconnection.

### 8. Notifications

Notification offset = commit offset = WAL offset — they are all the same value.
Each `ProcessWrite()` call creates a notification batch at the commit offset.
Children preserve the parent's offset numbering so that clients can resume from
where they left off.

**Snapshot filtering**: For each `__oxia/notifications/{offset}` entry,
deserialize the `NotificationBatch`, remove keys outside the child's hash range.
Delete the entire entry if the batch becomes empty after filtering.
`ReadNextNotifications` uses RangeScan so offset gaps are fine.

**WAL tailing**: WAL entries are appended to the child's WAL unmodified
(contiguous offsets preserved at the WAL level). Filtering happens at state
machine apply time only. When `ProcessWrite()` runs with a filtered
`WriteRequest`, the resulting notification naturally only contains in-range keys.
If all keys were filtered out, no notification is stored (offset gap is OK).

**Client transition**: When shard assignments update (children replace parent
via overlap detection in `shard_manager.go`), clients subscribed to the parent
get a stream termination. They re-subscribe to the children using the last seen
parent offset as `start_offset_exclusive` — this works because offset numbering
is inherited from the parent.

### 9. Failure Handling and Rollback

**Abort is supported in phases 1-3 (Init, Bootstrap, CatchUp).** Once the
parent is fenced during Cutover (phase 4), the split is past the point of
no return and must complete.

**Before cutover (phases 1-3) — data server state at each phase:**

| Phase | Parent leader state | Child data server state |
|-------|---|---|
| **Init** | Unchanged | Shards in ClusterStatus only — no data on disk |
| **Bootstrap** | Observer `FollowerCursor` per child in `lc.observers`; snapshot streaming | Snapshot received/installed; `FilterDBForSplit()` may have run |
| **CatchUp** | Observer cursors tailing WAL | Filtered DB + WAL entries applied via `ApplyLogEntryWithSplitFilter()` |

**Abort sequence (coordinator-driven):**
1. Cancel the `SplitController` context (stops the state machine goroutine)
2. Send `RemoveObserver` RPC to the parent leader for each child — closes
   observer cursors and stops data streaming without fencing the parent
3. Send `DeleteShard` RPC to all child ensemble members — deletes DB+WAL
4. Remove child shards from `ClusterStatus`
5. Clear parent's `SplitMetadata` (`Split = nil`)
6. Parent continues serving normally — its data was never modified

**Parent leader failure during split:**
- Parent shard controller runs a normal election (unaware of split)
- After new leader is elected, `SplitController` detects the change
- Re-adds children as observers on the new parent leader
- Split progress may be lost (children may need to re-bootstrap), but
  parent availability is not affected

**During cutover (phase 4):**
- Once the parent is fenced, we must complete the cutover
- If a child fails election, retry
- If a child node is down, the standard ensemble change mechanism handles it

**After cutover (phase 5):**
- Cleanup failures are non-critical; retry in background
- Parent data can remain on disk temporarily without impact

### 10. Coordinator Restart During Split

The split state is persisted in `ClusterStatus`. On coordinator restart:
- `SplitMetadata` on shards indicates an in-progress split
- The coordinator reconstructs the `SplitController` and resumes from the
  appropriate phase
- Each phase is idempotent and can be safely retried
- The parent shard's own `ShardController` starts independently and handles
  elections normally

## Implementation Plan

### Milestone 1: Core Infrastructure
1. Add `SplitMetadata` and `SplitPhase` to cluster status model
2. Add `SplitShard` admin RPC to coordinator
3. Add `split-shard` CLI command
4. Add `SplitController` skeleton to coordinator

### Milestone 2: Observer Followers
5. Add `observer` flag to `AddFollowerRequest` proto
6. Modify `AddFollower` / `FollowerCursor` to support observer followers that
   don't participate in the `QuorumAckTracker`
7. Track observers in a separate map from quorum followers (skip the
   `replicationFactor - 1` check for observers)

### Milestone 3: Snapshot Filtering + Bootstrap
8. Implement post-load key filtering on child (iterate all keys, classify and
   delete by hash range per the key classification rules)
9. Wire up bootstrap phase in `SplitController` (coordinator adds children as
   observers on parent leader, existing `FollowerCursor` auto-sends snapshot)

### Milestone 4: Catch-up
10. Implement WAL entry filtering on child leader (filter puts/deletes by hash
    range when applying committed entries to DB)
11. Add catch-up monitoring to `SplitController`

### Milestone 5: Cutover
12. Implement the cutover sequence in `SplitController` (fence parent, final
    catch-up, elect children)
13. Update shard assignment broadcast to include children and exclude parent
14. Verify client overlap handling works correctly

### Milestone 6: Cleanup + Hardening
15. Parent shard deletion after cutover
16. Coordinator restart recovery for in-progress splits
17. Integration tests for the full split lifecycle
18. Failure injection tests (node failures during each phase)

## Future Work

- **Auto-split**: Trigger splits based on shard size (Pebble DB size) or
  request rate metrics
- **Merge shards**: Reverse operation to combine underutilized shards
- **N-way split**: Split into more than 2 children in a single operation
