# KV sequence tests

This suite describes KV operations and their expected results in text files. A
file is one stateful scenario: commands execute in order against the same
`oxia.SyncClient`. Each file gets fresh storage and dynamic ports, using either
a standalone server or a coordinated three-server RF3 cluster. The operations
go through the real client, gRPC, shard controllers, WAL, and Pebble; there is no
mock KV implementation.

## Why this approach

The upstream etcd test suite provides two useful references:

- `tests/robustness/model/describe.go` renders histories as `put(...)`, `get(...)`,
  and `range(...)` with results. These descriptions are output for inspection,
  rather than executable text input. Its revision, transaction, and lease model
  cannot serve as an Oxia correctness oracle without adapting the semantics.
- `pkg/expect` and `tests/framework/e2e` start processes and wait for terminal
  output. This is useful for CLI/interactive tests, but KV behavior tests need
  access to returned records, versions, and errors rather than CLI presentation.

This suite uses [datadriven](https://github.com/cockroachdb/datadriven) for parsing
and expected-result diffs, with a small Oxia-specific command adapter. The pinned
version is already used by Oxia's Pebble dependency. The dependency and adapter
are confined to the tests module; no production CLI or server changes are needed.

## Writing a scenario

Each block consists of a command with named arguments, `----`, and the exact
expected output. Blank lines separate blocks; `#` introduces comments. Arguments
accept plain strings or Go double-quoted strings, including escapes and empty
values. Datadriven requires parentheses around an argument containing spaces,
for example `value=("two words")`. A comma or parenthesis inside a quoted string
can be encoded as `\x2c`, `\x28`, or `\x29` to avoid datadriven's grouping syntax.

```text
put key=a value="one" create-only save=first
----
ok key="a" modifications=0

get key=a
----
key="a" value="one" modifications=0

put key=a value="two" expected-version=@first
----
ok key="a" modifications=1

put key=a value="rejected" expected-version=@first
----
error: unexpected version id

range start=a end=z
----
key="a" value="two" modifications=1
```

The failed conditional write is followed by a read to verify that the stored
value did not change. Similar sequences can reproduce a regression by adding
only a text file, without writing more Go test scaffolding.

## Commands

| Command | Required arguments | Optional arguments | API |
| --- | --- | --- | --- |
| `put` | `key`, `value` | `create-only`, `expected-version`, `save`, `save-record`, `partition` | `Put` |
| `get` | `key` | `comparison`, `include-value`, `persistent`, `save`, `save-record`, `partition` | `Get` |
| `delete` | `key` | `expected-version`, `partition` | `Delete` |
| `range` | `start`, `end` | `partition`, `unordered` | `RangeScan` |
| `list` | `start`, `end` | `partition`, `unordered` | `List` |
| `delete-range` | `start`, `end` | `partition` | `DeleteRange` |
| `placement` | `keys` | `partition`, `min-shards`, `require-override` | Independent shard reads |
| `assert-record` | `key` | `unchanged`, `metadata`, `updated`, `missing`, `persistent`, `partition` | `Get` and record assertion |
| `client` | `name` | `namespace` | Create and select an independent `SyncClient` |
| `use-client` | `name` | — | Select an existing client |
| `stop-server` | `role`, `key`, `save` | `partition` | RF3 server stop and election barrier |
| `restart-server` | `saved` | — | RF3 server restart and topology barrier |
| `wait-replicated` | — | — | RF3 replica application barrier |

- `create-only` is a bare flag, mapped to `ExpectedRecordNotExists()`.
- `save=name` stores the successful put/get result's version ID. Use
  `expected-version=@name` in later writes or deletes. A nonnegative numeric ID
  is also accepted, but aliases avoid assumptions about assigned version IDs.
  Saving to the same name replaces its previous value; failed operations do not
  update aliases.
- `save-record=name` on `put` saves the supplied value and the returned key/version,
  without issuing another read; on `get` it saves the complete returned record.
  `assert-record key=a unchanged=@name` requires every field to match, while
  `metadata=@name` compares the key and all version fields without comparing values.
  `updated=@name` requires a different opaque version ID, unchanged creation time,
  and a modification count increment of one. It does not compare timestamp order.
  `missing` requires a not-found result. Only one relation can be selected.
  `persistent` checks `Ephemeral=false`, `SessionId=0`, and empty `ClientIdentity`;
  it can stand alone or accompany a relation other than `missing`.
  Record aliases are separate from version aliases, and neither absolute IDs nor
  timestamps enter expected output. Failed operations do not replace record aliases.
- `client name=observer` creates and selects another client with the same endpoint,
  request timeout, and discovery resolver. `use-client name=default` selects the
  original client. Clients share script aliases but have independent SDK state;
  all are closed during cleanup. Optional `namespace=alpha` selects a configured
  namespace. Namespace fixtures configure `alpha`, `beta`, and the default namespace.
  Placement and cluster control commands select records in the default namespace;
  namespace fixtures use only the KV and client commands.
- `comparison` accepts `equal` (the default), `floor`, `ceiling`, `lower`, and
  `higher`.
- `get ... include-value=false` maps to `IncludeValue(false)` and asserts that no
  value bytes were returned. Key and version metadata remain available; metadata
  aliases can check their equality with a full read. `true` explicitly requests
  the value. `get ... persistent` also checks the returned version's persistent fields.
- `partition` maps to `PartitionKey()`. Supply it consistently on writes and
  reads for keys whose routing is overridden. It selects a shard; it does not
  create a separate namespace or provide tenant isolation.
- Range bounds are inclusive `start` and exclusive `end`. `end=""` means no
  upper bound, following the Oxia API.
- `list ... unordered` checks the returned key set by sorting it in byte order
  before comparison. Use this for multi-shard lists, whose order is not stable.
  Without this flag, list output is compared in the original returned order.
- `range ... unordered` sorts the formatted record lines before comparison,
  retaining duplicates. This checks complete results without requiring an order.
  The public `RangeScan` ordering guarantee applies when a partition key is supplied
  and the records were written with that partition key. Global scans should use
  `unordered` when verifying that contract; their current merge order is not an
  additional API guarantee.
- `placement keys=(a,c,e) min-shards=2` reads those keys from every assigned shard
  using an independent gRPC connection and explicit shard IDs. Every key must
  exist on exactly one shard and match the advertised XXHASH3 routing. The
  optional `partition` checks the overridden route, including an empty string.
  `require-override` requires `partition` and verifies that at least one key was
  placed differently from its default route. Shard IDs appear only in diagnostics.
- `stop-server role=leader key=a save=old` selects the current leader of the
  key's shard, closes that server, and waits for a different leader at a higher
  term. `role=follower` selects a follower instead. `partition` changes shard
  selection exactly as it does for KV commands. Stopping a server can affect
  several shards; the barrier checks every shard and its surviving replicas.
- `restart-server saved=@old` restarts the saved server with the same identity,
  network addresses, WAL, and database directories. It waits for all three
  members to have a serving leader and followers ready at the current terms.
  An idle follower can remain fenced until its first append in a new term;
  `wait-replicated` additionally requires that append to have been accepted.
  The test resolver removes a seed after stopping it and readmits it only
  after restart readiness, keeping surviving seeds ahead of restored ones.
  The existing client and saved versions/records remain in use across both
  commands.
- `wait-replicated` requires all three servers to be running. It snapshots each
  leader's quorum commit offset, sends an empty write to advertise that commit
  to followers, and waits for every replica's database-applied offset to reach
  the snapshot at the same term. The empty writes create no user records. This
  verifies application progress; it does not directly compare follower values.
  A term change or an unmet prerequisite fails the command.

Put results include the returned key and modification count. Get/range results
also include the quoted value. This preserves spaces and empty values in the
assertions. List emits one quoted key per line; an empty range/list emits
`(empty)`. Delete operations emit `ok` on success.

The adapter omits timestamps, session IDs, and absolute version IDs from output,
so fixtures stay deterministic. Range and list output keep the API's returned
order unless `unordered` is explicitly requested. Known domain
errors (`key not found`, `unexpected version id`, `invalid options`) have stable
error output; unexpected transport failures
and timeouts fail the test even when rewriting expected results. Malformed
commands, duplicate/unsupported arguments, and unknown version aliases also fail.
Placement failures, record assertion failures, and cluster barrier failures are
fatal even with `-rewrite`. KV commands have a five-second deadline. Cluster
commands use a thirty-second context for RPCs and readiness/replication waits;
synchronous server startup and close are bounded by the overall test timeout.
Only topology and replication observations are polled; KV failures are never
retried by the test adapter.

## Running and extending

From the repository root:

```sh
make test-kv
go test -v -timeout 10m ./tests/kvscript/...
go test -v ./tests/kvscript/... -run 'TestKVScripts/natural/shards-4/cas$'
go test -v ./tests/kvscript/... -run 'TestKVClusterScripts/natural/shards-4/follower_catchup$'
```

`make test-kv` enables race detection. The suite is also included in the existing
`make test` and CI through `./tests/...`.

Files in `testdata/common` run with both hierarchical and natural sorting, each
with one and four shards. Sorting-specific fixtures live in
`testdata/hierarchical` and `testdata/natural`. Every file gets its own server and
client; only commands within that file share state. The common fixtures cover
CRUD, empty values, deletion/recreation, conditional writes/deletes, comparison
queries, range boundaries, explicit partition routing, string/binary values,
and invalid UTF-8 keys. Sorting fixtures
check the differences between the two key orders, including merged shard reads.

Files in `testdata/multishard/common` and `testdata/multishard/<sorting>` run only
with four shards. They verify actual shard placement before checking default
and partition routing, empty/colliding/wrong partition keys, comparison candidates
on different shards, query completeness, and global or partition range deletion.
The sorting-specific cases cover leading, trailing and repeated slashes and path
depth boundaries. Common failure scenarios compare complete saved records after
rejected writes/deletes and invalid UTF-8 deletion bounds.

`TestKVScripts` uses one standalone server with replication factor one.
`TestKVClusterScripts` reruns the same fixtures with RF3, using a real coordinator
runtime and reconciler plus three data servers, each with separate public/internal
gRPC endpoints and storage. All components run inside the same Go test process.
The coordinator uses in-memory metadata; coordinator restart is outside this suite.
The test resolver supplies ready server addresses through `WithDialResolver`,
removing a node after its controlled stop and restoring it after readiness.
This keeps the same client's assignment stream connected to available seeds
while restarted servers initialize. Advertised leader addresses determine KV
routing; the test resolver only controls discovery endpoints. The harness keeps
live-address service discovery current; recovery through static or stale
bootstrap addresses requires separate SDK tests.

Files in `testdata/cluster/common` add leader failover, follower catch-up, CAS
across elections, and deletion/recreation across elections, under both sortings
and one/four shards. `testdata/cluster/multishard` adds complete cross-shard reads
and scoped deletion across a server stop/restart, with four shards. Every file
gets its own cluster. Together the two runners execute 246 scenarios: 112 RF1
and 134 RF3. Two acknowledged-write fixtures stop the leader immediately after a
successful Put or Delete, without `wait-replicated` or an intervening KV read.
They check the returned Put version after election, retain CAS eligibility, and
verify that deletion and recreation survive another election and node restart.

`TestKVNamespaceScripts` adds four RF3 executions with three configured namespaces,
under both sortings and one/four shards. Identical record and partition keys remain
isolated during conditional updates, scoped and global range deletions, and recreation.
Standalone exposes only the default namespace, so namespace isolation uses a real
coordinator-backed configuration. The three script runners execute 250 scenarios.

`TestKVConcurrentCAS` uses independent clients and start barriers for Put/Put,
Put/Delete, and create-only competitions. Both clients first observe the same
version or absence; exactly one operation must succeed, and the other must report
a version conflict. Both read the winning record and reject stale versions after
recreation. RF1/RF3, both sortings, one/four shards, default/partition routing, and
three rounds per combination give 144 competitions. These bounded races complement
the sequential fixtures; they are not a general concurrent-history checker.

Multishard fixtures assert actual placement rather than assuming that four
configured shards are enough. The probe refreshes assignments and reads each
shard's advertised leader independently. Server stops are graceful, and replica
barriers observe database application, so these checks do not establish hard-crash
or power-loss durability. Cross-shard operations should not be interpreted as a
global atomic transaction or snapshot. Quorum loss, arbitrary network partitions,
and concurrent linearizability are outside this suite.

## Core KV contract coverage

Each row connects a public behavior to executable checks. Scenarios assume a
healthy configured namespace and consistent routing options unless they explicitly
stop a replica. Sequential reads start after the preceding mutation completes;
range checks use a stable dataset rather than asserting a concurrent snapshot.

| Behavior | Observable guarantee checked | Representative checks |
| --- | --- | --- |
| Record lifecycle | Exact key/value, overwrite without duplicates, missing delete, recreation resets modifications | `crud`, `etcd_overwrite_same_value`, `etcd_recreate_cycles` |
| Key/value representation | Empty keys/values, valid Unicode and NUL, distinct Unicode spellings, arbitrary value bytes | `strings`, `utf8_and_empty_keys`, `record_metadata` |
| Conditional mutations | Valid versions succeed; rejected mutations preserve records; deleted versions stay stale | `cas`, `partition_cas_failure_preserves_record` |
| Returned metadata | Put/Get full version equality, updates preserve creation time and increment modifications, persistent fields | `record_metadata`, `partition_record_metadata` |
| Client visibility | Completed writes/deletes are visible to another client using the same namespace and route | `cross_client_visibility`, `partition_cross_client_visibility` |
| Conditional competition | Exactly one contender succeeds with a shared version or create-only condition | `TestKVConcurrentCAS` |
| Comparison reads | Equal/floor/ceiling/lower/higher select the expected key; omitting values retains metadata and CAS usability | `comparison`, `include_value_comparison`, partition variants |
| Range boundaries | Inclusive start, exclusive end, unbounded end, completeness and scoped deletion | `range`, `global_range`, `partition_delete_range`, sorting fixtures |
| Partition routing | Explicit route selects a shard and co-locates records; it is not namespace isolation | `partition_override`, `partition_collision`, `wrong_partition` |
| Namespace isolation | Same record/partition keys in different namespaces remain independent through mutation and range deletion | `namespace/isolation` |
| Internal key boundary | Default scans hide internal records; mixed user/internal deletion bounds are rejected without modifying users | `internal_key_boundaries` |
| RF3 recovery | Successfully returned records/versions and deletions survive controlled election and restart | `acknowledged_write_failover_*`, existing cluster fixtures |

Version IDs are opaque and meaningful for a given key; tests do not require a global
revision sequence or compare IDs across records or namespaces. Timestamp checks use
field equality across API results and preservation on updates, without assuming
that successive mutations must occur in different milliseconds.

The no-change assertions concern definitive version conflicts and invalid write
inputs in the listed scenarios. A timeout or connection failure can leave a mutation's
outcome unknown; this suite fails on those errors rather than assuming no mutation
occurred. Cross-shard operations do not acquire an atomic global snapshot or transaction.
The shared scripts currently run through the Go SDK; Java SDK conformance needs an
adapter and its own execution results.

Add a file under the appropriate directory to introduce another scenario. On a
mismatch, datadriven reports the file, line, command, and expected/actual diff.
Expected outputs should normally be written by hand. For an intentional output
change, datadriven can rewrite a selected scenario:

```sh
go test ./tests/kvscript/... -run 'TestKVScripts/natural/shards-1/cas$' -args -rewrite
git diff -- tests/kvscript/testdata
```

Review rewritten expectations before accepting them: recorded output alone is
not evidence that the behavior is correct.

## Adapted etcd scenarios

Representative scenarios are adapted from the public upstream etcd
[common KV tests][etcd-common-kv] and [MVCC tests][etcd-mvcc-kv]. The table below
identifies the source functions. Files under `testdata/common` run with both key
sortings and one/four shards; the prefix scenario runs only with natural sorting,
with one/four shards. Their expectations are written by hand using Oxia's behavior.

| Fixture | Directory | etcd reference |
| --- | --- | --- |
| `etcd_overwrite_same_value` | `common` | [MVCC tests][etcd-mvcc-kv]: `TestKVPutMultipleTimes` |
| `etcd_delete_exact_path` | `common` | [Common KV tests][etcd-common-kv]: `TestKVDelete`, exact-key case |
| `etcd_delete_missing` | `common` | [Common KV tests][etcd-common-kv]: `TestKVDelete`, missing-key case |
| `etcd_full_user_range` | `common` | [Common KV tests][etcd-common-kv]: `TestKVGet` and `TestKVDelete`, all-key cases |
| `etcd_delete_empty_range` | `common` | [MVCC tests][etcd-mvcc-kv]: `TestKVDeleteRange`, no-match case |
| `etcd_recreate_cycles` | `common` | [MVCC tests][etcd-mvcc-kv]: `TestKVOperationInSequence` |
| `etcd_prefix_and_from_key` | `natural` | [Common KV tests][etcd-common-kv]: `TestKVGet` and `TestKVDelete`, prefix/from-key cases |

The overwrite scenario also borrows the duplicate-key check from
[etcd's CLI KV tests][etcd-cli-kv], `getCountOnlyTest`: repeated puts must not add
another record. It checks that a same-value put increments the modification
count and invalidates the saved old version, while neighboring records remain
unchanged. The original etcd lease/revision assertions are outside this port.

Exact deletion covers both directions: deleting `c` keeps `c/abc`, and deleting
`c/abc` keeps the recreated `c`. Missing/repeated deletion checks the complete
remaining key set, each surviving value and modification count, and successful
CAS using versions captured before the failed operations. Full-range deletion
includes keys below `a`, above `z`, and Unicode, then verifies reads, recreation,
and conditional updates after repeated cleanup.

The no-match range deletion runs against a populated store, including a record
at the excluded upper bound. It checks the complete key set, values, modification
counts, and saved versions after deleting `[foo3,foo8)`. An equal-bound deletion
`[foo1,foo1)` is additional Oxia coverage, not a copied etcd delete-range case.

The recreation scenario spells out five rounds of put/range/delete/range, adapted
from etcd's ten-round loop. Each round must start with a missing record, create
only one current value with modification count zero, and leave no record after
deletion. Saved versions from earlier rounds must not update or delete a later
incarnation; the neighbor's version must stay valid. These CAS assertions are
Oxia additions, rather than historical etcd revision checks.

The natural prefix scenario contrasts `[foo,fop)` with `[foo,"")` using `foo`,
`foo1`, `foo/abc`, and adjacent records. Prefix deletion must preserve `fop` and
later keys, while from-key deletion removes them; both must preserve `fo` below
the lower bound. It cannot run with hierarchical sorting: those bounds no longer
select all records sharing the byte prefix, because path depth affects the order.

The key adaptations are:

- A missing single-key delete returns `key not found` in Oxia; etcd returns a
  successful response with zero deleted records.
- Oxia's delete-range returns a status rather than a deleted count. The scripts
  check the complete remaining records using list/range and individual gets.
- The all-user-key range uses `start="" end=""`; this is not a direct copy of
  etcd's raw RangeEnd sentinel. Subsequent writes check that cleanup leaves the
  server usable.
- Absolute etcd revisions are replaced with modification counts and saved
  Oxia version aliases. Multi-shard lists use explicit `unordered` assertions.

## Follow-on work

Useful additions are secondary-index and sequential-key options, ephemeral-record
lifecycle checks through named clients, and process-level crash/recovery scenarios.
Each addition needs explicit output and lifecycle rules.

The scripts verify sequential KV behavior and the Go tests add bounded CAS races. Concurrent histories,
linearizability, network partitions, watch ordering, and interactive terminal
editing still require their own test mechanisms; etcd's history model and
process-expect framework are references for those separate layers.

[etcd-common-kv]: https://github.com/etcd-io/etcd/blob/main/tests/common/kv_test.go
[etcd-mvcc-kv]: https://github.com/etcd-io/etcd/blob/main/server/storage/mvcc/kv_test.go
[etcd-cli-kv]: https://github.com/etcd-io/etcd/blob/main/tests/e2e/ctl_v3_kv_test.go
