# Shard Splitting Plan

Status: Draft  
Tracking PR: [#1271](https://github.com/oxia-db/oxia/pull/1271)

## Objective

Make shard splitting a controller-owned, resumable operation that preserves all
committed data and does not overwrite concurrent coordinator metadata changes.
The parent shard controller owns the operation from request admission through
parent deletion.

## Scope

This plan covers:

- split request serialization
- parent and child metadata creation
- Bootstrap, CatchUp, and Cutover lifecycle phases
- coordinator restart and controller shutdown behavior
- rollback before the parent fence
- forward-only recovery after the parent fence
- unit, race, integration, and fault-injection verification

This plan does not change the hash-range model, placement algorithm, replication
protocol, or public split command.

## Required Invariants

1. The parent shard controller is the only owner of an active split.
2. Split initiation is serialized with parent elections, ensemble changes, and
   deletion on the parent controller event loop.
3. Metadata writes modify the latest cluster status and never replace an
   unrelated shard update with a stale namespace snapshot.
4. The parent and both children have consistent persisted split phases.
5. Child controllers do not begin normal balancing or elections until their
   split metadata is cleared.
6. A split can roll back before the parent is fenced.
7. A split must only move forward after the parent is fenced.
8. Coordinator shutdown must not leave a running split goroutine or create new
   child controllers after runtime closure begins.

## Ownership Model

The request path is:

1. The coordinator runtime finds the parent shard controller.
2. The runtime submits a `SplitAction` to the parent controller.
3. The controller handles the action on its event loop.
4. `Splitter` validates placement inputs and prepares parent and child metadata.
5. `Metadata.ShardSplit` compares the expected parent with the latest parent
   under the metadata lock and atomically creates the split records.
6. `Splitting` runs under the parent controller context and drives persisted
   phases until completion or an allowed rollback.

The runtime only manages controller registration and assignment recomputation.
It does not own a separate split state machine.

## Lifecycle

### Bootstrap

- Fence and elect leaders for both child shards.
- Add each child leader as an observer of the parent.
- Persist the parent term and child leader identities used by the observers.
- Atomically advance the parent and both children to `CatchUp`.
- Return to `Bootstrap` if a parent or child election invalidates an observer.

### CatchUp

- Read the parent's committed offset.
- Wait for both children to commit through that offset.
- Repeat in bounded rounds while the parent continues receiving writes.
- Atomically advance all split records to `Cutover` when both children catch up.

### Cutover

- Freeze parent writes without fencing the parent.
- Drain both observers through the parent's final head offset.
- Unfreeze and return to `Bootstrap` if an observer becomes stale before the
  parent fence.
- Fence the parent only after both children hold the final tail.
- Re-elect both children in independent terms so their own quorums commit the
  tail.
- Clear child split metadata, mark the parent for deletion, clear the parent
  split metadata, and recompute assignments.

The parent fence is the point of no return. Recovery after that step must never
restore the parent as writable.

## Metadata Operations

`UpdateNamespaceStatus` keeps its existing API. Split-specific atomic changes
use narrow metadata operations:

| Operation | Responsibility |
| --- | --- |
| `ShardSplit` | Compare the expected parent with the latest parent, attach split metadata, and create both children in one store update. |
| `UpdateShardSplitPhase` | Validate the parent and both children, then update all three phases in one store update. |

Both operations must be idempotent because the metadata provider can retry a
store operation. An identical already-persisted split is success; conflicting
metadata is an error.

## Work Plan

| Stage | Deliverable | Status |
| --- | --- | --- |
| 1 | Route split requests through the parent shard controller event loop. | Implemented in PR #1271 |
| 2 | Move split context and lifecycle ownership into the parent controller. | Implemented in PR #1271 |
| 3 | Add atomic `ShardSplit` and `UpdateShardSplitPhase` metadata operations. | Implemented in PR #1271 |
| 4 | Reject stale parent snapshots and incomplete split metadata. | Implemented in PR #1271 |
| 5 | Make controller and runtime shutdown ordering race-free. | Implemented in PR #1271 |
| 6 | Verify restart behavior in every pre-fence phase. | Pending fault-injection coverage |
| 7 | Define and verify forward-only recovery for every post-fence interruption point. | Pending design audit |
| 8 | Run sustained writes, elections, and coordinator restarts during splits. | Pending end-to-end coverage |
| 9 | Confirm logs and metrics identify namespace, parent, children, phase, retries, and abort reason. | Pending observability audit |

## Failure Policy

| Failure point | Required behavior |
| --- | --- |
| Before metadata creation | Return an error without creating children. |
| Bootstrap | Retry; abort and remove children if the split timeout expires. |
| CatchUp | Retry in bounded rounds; reset to Bootstrap if an observer is stale. |
| Cutover before parent fence | Unfreeze the parent, then retry or abort safely. |
| Cutover after parent fence | Resume forward; never abort to the parent. |
| Coordinator restart | Recreate controllers from persisted metadata and resume the parent-owned state machine. |
| Runtime shutdown | Cancel the split, stop the event loop, then release the final split state. |
| Missing or conflicting metadata | Stop the transition and return a specific error without a partial phase update. |

## Verification Plan

### Local Gates

- `make lint`
- `go test ./oxiad/...`
- `go test -race ./oxiad/coordinator/runtime/controller/shard -count=1`
- `git diff --check`

### Required Regression Cases

- a concurrent update to an unrelated shard survives split creation
- a parent term, leader, ensemble, status, or range change rejects split creation
- two split requests for one parent are serialized
- a split action for the wrong shard is rejected
- missing parent or child split metadata cannot produce a partial phase update
- shutdown during split initialization does not race or retain `currentSplitting`
- a persisted split without configured dependencies does not start
- a parent or child election during CatchUp returns the split to Bootstrap
- cancellation before the parent fence removes observers and child metadata

### Fault-Injection Matrix

Restart the coordinator after each persisted or externally visible step:

1. Parent and children are created in `Bootstrap`.
2. One child is fenced and elected.
3. Both observers are installed.
4. The phase advances to `CatchUp`.
5. The phase advances to `Cutover`.
6. The parent is frozen but not fenced.
7. The children receive the final parent tail.
8. The parent is fenced.
9. One child is re-elected.
10. Both children are re-elected.
11. Child split metadata is partially cleared.
12. The parent is marked deleting but still has split metadata.
13. The parent split metadata is cleared but physical deletion is incomplete.

For each point, verify data integrity, assignment visibility, retry behavior,
and whether recovery correctly chooses rollback or forward-only completion.

## Remaining Design Decisions

### Post-Fence Progress

The current Cutover cleanup uses several shard metadata updates after the parent
is fenced. We must prove that each intermediate state is sufficient to resume
forward. If it is not, add a persisted cutover checkpoint or an atomic
`CompleteShardSplit` metadata operation before declaring post-fence recovery
complete.

### Abort Atomicity

Abort currently removes child metadata and clears the parent split metadata in
separate operations. Verify restart behavior between those operations. Add a
single metadata transaction if partial cleanup cannot be recovered safely.

### Operational Visibility

Decide whether structured logs are sufficient for the first release. If not,
add counters and duration metrics for starts, phase transitions, retries,
aborts, completions, and recovery resumes.

## Completion Criteria

Shard splitting is ready when:

1. All local and CI gates pass.
2. Every fault-injection point has a deterministic recovery result.
3. No acknowledged write is lost during split, election, or restart tests.
4. Clients discover both children only after successful Cutover.
5. The parent cannot become writable after the point of no return.
6. Metadata remains consistent after timeout, cancellation, and shutdown.
7. Reviewers agree whether post-fence and abort atomicity are handled in PR
   #1271 or tracked as explicit blocking follow-up work.
