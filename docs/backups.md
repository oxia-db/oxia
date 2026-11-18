# Shard backups to object storage

Oxia backs up every shard to object storage on a schedule, keeps the backups
for a retention period, and restores a namespace as of a point in time
covered by the backups. This document is the reference for the feature: the
policy, what each component does, the storage layout, and the operations.
The proposal it implements is oxia-db/oxia discussion #1488.

## Policy

Backups are configured once for the cluster, in the cluster configuration,
next to `autoSplit`:

```yaml
backup:
  enabled: true
  destination: s3://my-bucket/oxia-prod
  interval: 1h               # target maximum age of a shard's newest backup
  retention: 7d              # how far back a restore can go
  maxConcurrentPerServer: 2  # backup and cleanup jobs running at once on one data server
  scheduleInterval: 30s      # how often the coordinator checks what is due
  cleanupInterval: 24h       # how often expired backups are deleted
```

Durations take the `time.ParseDuration` syntax, plus a `d` suffix for days.
`retention` must be longer than `interval`.

A namespace opts out of the policy in its own entry, next to `keySorting`:

```yaml
namespaces:
  - name: sessions
    initialShardCount: 8
    replicationFactor: 3
    backup: false   # the policy doesn't cover this namespace
```

Its shards take no backups, forced or not, and report no backup age; its
cleanup still runs, so the backups taken before it opted out expire.

Destinations:

- `s3://<bucket>/<prefix>[?region=<r>&endpoint=<url>&path-style=true]`:
  credentials come from the environment (IAM role, workload identity, the
  usual `AWS_*` variables), never from the configuration. `endpoint` and
  `path-style` are for S3-compatible stores such as MinIO or RustFS.
- `mem://<name>`: an in-process store, for tests only.

Every data server needs write access to the destination. The coordinator
never reads or writes object storage: listings and restore planning go
through a data server.

Nothing else may delete objects under the destination. A bucket lifecycle
rule that expires old objects would delete SST files that newer backups still
share.

## Who does what

The subsystem is split in three packages that talk only through the
`OxiaBackup` RPC service (`common/proto/backup.proto`), which the data servers
serve on their internal endpoint:

| `oxiad/coordinator/runtime/backup` (scheduler) | `oxiad/dataserver/backup` (executor) | `oxiad/backup` (core) |
|---|---|---|
| Tracks when each shard is due | Takes a checkpoint of the shard | Blob store interface, S3 and in-memory stores |
| Assigns each shard a backup replica, and keeps it while it can | Uploads the new SSTs and writes the backup manifest | Storage layout and backup IDs |
| Holds back shards in an election, or split children not serving yet | Expires old backups and deletes unreferenced SSTs | Manifest format |
| Orders the requests most overdue first | Bounds the jobs running at once on the server | Retention rule |
| Schedules the cleanup of each namespace | Lists the backups of a namespace | Restore planning |
| Plans restores | Downloads and installs a shard's database for a restore | |

Every request to a data server is declarative and idempotent: `BackupShard`
asks the replica to make the shard's backup satisfy the policy, and returns
the state of that work (`NOT_NEEDED` with the last backup, `STARTED`,
`RUNNING`, `BUSY` when the server has no free slot, `DONE`, or `FAILED` with
the error). The coordinator re-sends it on every check until the shard is up
to date, so a coordinator failover loses nothing: the new coordinator asks
every shard's replica and rebuilds its due times from the answers. A job's
result is reported once, then the next request starts the next job.

## Storage layout

```
<destination>/<namespace>/shard-<id>/
  sst/<lineage>-<file-number>.sst         # each SST uploaded once, shared by every backup that lists it
  backups/<backup-id>/                     # the small per-backup files: MANIFEST, OPTIONS, markers
  backups/<backup-id>.json                 # the backup manifest, written last
```

- A **backup ID** is the UTC start time followed by the lineage, e.g.
  `2026-10-03T14-25-07Z-5f3a9c1e2b7d4e60`. Listings come back in time order,
  and a date prefix narrows a listing to one day.
- A **lineage** identifies one database of one replica. Pebble file numbers
  are only unique within one database, so two replicas, or one replica before
  and after a snapshot install, can each have a `000123.sst` with different
  contents. A replica generates a random lineage whenever it creates or
  replaces a shard's database, or takes a backup that can't share the SSTs
  of its last one, and keeps it in `oxia-backup.json` next to the database,
  outside the replicated keyspace, with the destination the last backup went
  to. A snapshot install replaces the database directory, so it replaces the
  lineage too. An SST key is written once: no backup uploads over a key.
- The **manifest** records the shard ID and hash range, the namespace
  configuration needed to recreate it, the term, the commit offset and the
  time, the lineage, and the list of SSTs and other files with their sizes
  and CRC32C checksums. A backup exists only once its manifest is written: an
  interrupted backup leaves orphaned SSTs, never a partial backup.
- The **commit offset** and the **term** in the manifest are read from the
  files of the checkpoint, where every batch wrote them with its data: the
  backup holds every entry up to the commit offset, and none past it.

## Taking a backup

1. The replica flushes the database and takes a Pebble checkpoint, which
   hard-links the SSTs: compaction can't delete them while they upload. The
   checkpoint is taken under the controller's read lock, so a change of term
   waits for the flush. The commit offset and the term of the backup are
   read from the files of the checkpoint.
2. It compares the checkpoint's SSTs with the SSTs of its last backup, and
   uploads only the missing ones. It reuses the SSTs of its last backup only
   if that backup is younger than `retention` minus a safety margin (the
   cleanup grace period, at most a quarter of the retention), because cleanup
   never expires a backup younger than `retention`: the reused SSTs are still
   in the bucket. Otherwise, or when the record is of another destination, it
   uploads every SST under a new lineage.
3. It uploads the other checkpoint files under `backups/<backup-id>/`, then
   writes `backups/<backup-id>.json`.
4. It updates its local record of the last backup (ID, time, commit offset,
   term, SST list, destination), deletes the checkpoint, and reports the
   result. The record is read and written under the controller's read lock,
   and only while the database is the one the checkpoint was taken from: a
   snapshot install during the upload replaces the database and wipes the
   record, the backup stays valid, and the next one starts a new lineage.

A request is skipped when the record is younger than `interval`, and when
the commit offset hasn't moved since the record and the record is younger
than `retention - interval`. Past that age an unchanged shard gets a new
backup anyway, which uploads no SSTs, so every shard always has a backup
inside the retention window.

Deduplication only works within a lineage: replicas compact independently,
so their SSTs differ even when they hold the same data. The first backup of
a lineage uploads the whole shard, which is why each shard keeps the same
backup replica.

## Backup replica

The coordinator assigns each shard a backup replica, stored in the shard's
metadata, and keeps it whether it is the leader or a follower. It prefers
followers, and spreads the role evenly across the data servers. The role
moves only when the replica leaves the ensemble, or when it couldn't take a
due backup for half an interval because it is down or not caught up.

A shard is eligible when it is in steady state with a leader, and its backup
replica is the leader, or a member of the ensemble whose commit offset is at
least the leader's commit offset read just before (a follower reports
`FENCED` until the first entry after the election, with a database as
complete as a `FOLLOWER`'s). A split parent stays eligible until it is
fenced; the children become eligible once the split completes, and inherit
the parent's due time: a split never leaves a hash range without a backup
for longer than an interval.

A follower only learns that an entry is committed from the next entry the
leader sends, so an idle follower used to leave the last committed entry
unapplied. With `FEATURE_COMMIT_ADVERTISEMENT` pinned by the term, the leader
advertises a commit offset advance to its followers with an entry-less
`Append`, as it already did to split observers: the backups taken from a
follower then hold every committed entry.

## Retention and cleanup

For each hash range, every backup taken within `retention` is kept, plus the
newest one taken before that, which a restore to the start of the window
needs. A backup expires once a newer backup, or a set of newer backups,
covering its whole hash range is also older than `retention`. Split parents
are covered by their children.

Expiring a backup deletes its manifest and its `backups/<backup-id>/` files.
A sweep then deletes the SSTs of the shard that no remaining manifest
references and that are older than a grace period of one day, which
protects the SSTs of a backup that hasn't written its manifest yet.

Cleanup lists and reads manifests, which is slow and costly on object
storage, so it runs once per `cleanupInterval` per namespace, on one data
server that the coordinator picks, not after every backup. The backups of a
namespace that no longer exists are kept: nothing takes new ones, and their
last backup stays restorable until an operator deletes the prefix.

## Restore

```
oxia admin restore --namespace orders --as-of 2026-10-03T12:00:00Z --to-namespace orders-restored
```

1. The coordinator lists the backups of the source namespace through a data
   server, and for each part of the hash range picks the newest backup
   started at or before `--as-of` (by the start time recorded in the
   manifest, which is more precise than the second of the backup ID). Parent and child backups cover overlapping ranges: a
   restore to a time before a split picks the parent, and a restore to a time
   where only one child has a backup restores the other half from the parent,
   filtered to that half.
2. It creates the target namespace's status with those hash ranges, each
   shard marked with the backup to restore from, then adds the namespace to
   the configuration with the settings recorded in the manifest.
3. Each shard controller, before its first election, asks every member of the
   ensemble to restore the backup: the member downloads the files, verifies
   their checksums, renames the SSTs back to their file numbers, installs the
   database through the snapshot-install path, and filters it to the shard's
   range when the backup covers more. The controller then clears the restore
   mark and elects a leader as usual.
4. Sessions restored with the data have no clients: they expire after their
   timeout and their ephemeral records are deleted, as after any failover.

Restoring into a new namespace leaves the source untouched. The restore is
idempotent: run again after a coordinator crash, it completes the step it
stopped at.

## CLI

- `oxia admin backup [--namespace ns] [--shard id]`: make the selected
  shards due now, e.g. before an upgrade.
- `oxia admin backup list [--namespace ns] [--shard id]`: the backups per
  shard, with their time, commit offset and size.
- `oxia admin restore --namespace ns --as-of <time> --to-namespace ns2
  [--from <destination>]`.

## Observability

- `oxia_coordinator_backup_age`: per shard, the seconds since the shard's
  state was last captured (-1 until known). Alert above about twice the
  interval: that catches what scheduling can't fix, such as a shard without
  a leader.
- `oxia_coordinator_backup_requests`, `oxia_coordinator_backups_completed`,
  `oxia_coordinator_backup_failures`, `oxia_coordinator_backup_replica_moves`.
- `oxia_server_backups_completed`, `oxia_server_backups_failed`,
  `oxia_server_backup_uploaded` (bytes), `oxia_server_backup_latency`,
  `oxia_server_backups_expired`, `oxia_server_backup_deleted` (bytes),
  `oxia_server_backup_cleanups_failed`, `oxia_server_restores_completed`,
  `oxia_server_restores_failed`.

## Limitations

- Restoring between two backups needs WAL archiving, which is future work:
  a restore loses the writes made since the shard's last backup before
  `--as-of`, up to one interval.
- The restored sessions have no clients, and their ephemeral records go
  when they expire.
- The backups of a namespace that no longer exists are never expired.
- A split child taken mid-split is backed up with the records of its parent
  it still holds; a restore filters them out.

## Costs and trade-offs

- Data loss window: up to one interval, plus any time the shard was not
  eligible. Restoring between two backups needs WAL archiving, which is
  future work.
- A new lineage costs a full upload, and the bucket holds about two copies of
  the shard for one retention period.
- The SSTs are uploaded as Pebble stores them: uncompressed at L0, Snappy
  below. A second compression at upload would spend the CPU of the data
  server on data that is compressed already, for little gain.
- Encryption relies on the bucket's server-side encryption.
