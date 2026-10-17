// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/backup"
	"github.com/oxia-db/oxia/oxiad/backup/blob"

	// The S3 destinations, registered with package blob.
	_ "github.com/oxia-db/oxia/oxiad/backup/blob/s3"
	"github.com/oxia-db/oxia/oxiad/dataserver/database"
)

const (
	// DefaultGracePeriod is how old a file no backup references must be for
	// the cleanup to delete it: well above the longest a backup can take, so
	// that the files of a backup that hasn't written its manifest yet stay.
	DefaultGracePeriod = 24 * time.Hour
	// DefaultUploadWorkers is how many files a backup uploads at once.
	DefaultUploadWorkers = 4
)

// Host is what the executor needs of the data server: the shards it hosts.
type Host interface {
	// Checkpoint takes a checkpoint of the database of a shard hosted here.
	Checkpoint(shardId int64) (*database.Checkpoint, error)
	// CommitOffset returns the last entry applied to the database of a shard
	// hosted here.
	CommitOffset(shardId int64) (int64, error)
	// WithDatabase runs fn with the directory of the database of a shard
	// hosted here while it is still the database a checkpoint of the given
	// generation was taken from, under the lock that a replacement of the
	// database takes; it returns database.ErrDatabaseReplaced otherwise.
	WithDatabase(shardId int64, generation uint64, fn func(dir string) error) error
	// DatabaseDir returns the directory of the database of a shard, where the
	// executor keeps its record.
	DatabaseDir(namespace string, shardId int64) string
}

// Options configures an Executor. Only Host is required.
type Options struct {
	Host Host
	// Clock is the time source, for the tests.
	Clock func() time.Time
	// GracePeriod is DefaultGracePeriod unless set.
	GracePeriod time.Duration
	// UploadWorkers is DefaultUploadWorkers unless set.
	UploadWorkers int
	Logger        *slog.Logger
}

// Executor serves the OxiaBackup requests on a data server. It runs the
// backup and cleanup jobs on their own goroutines, at most the policy's
// maxConcurrentPerServer at once, and reports their state.
//
// The jobs are shared state of the executor: the request handlers and the
// job goroutines access them under mu. A job stays in the map until the
// request that follows its end reads its result.
type Executor struct {
	host          Host
	clock         func() time.Time
	grace         time.Duration
	uploadWorkers int
	log           *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	jobs    map[jobKey]*job
	running int
	stores  map[string]blob.Store

	backupsCompleted metric.Counter
	backupsFailed    metric.Counter
	uploadedBytes    metric.Counter
	backupLatency    metric.LatencyHistogram
	backupsExpired   metric.Counter
	deletedBytes     metric.Counter
	cleanupsFailed   metric.Counter
}

type jobKind int

const (
	backupJobKind jobKind = iota
	cleanupJobKind
)

type jobKey struct {
	kind      jobKind
	namespace string
	shard     int64
}

// job is a running or finished job. done is closed when it finishes, with
// result or err set.
type job struct {
	done   chan struct{}
	result any
	err    error
}

// New returns an executor, which Close stops.
func New(options Options) *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Executor{
		host:          options.Host,
		clock:         options.Clock,
		grace:         options.GracePeriod,
		uploadWorkers: options.UploadWorkers,
		log:           options.Logger,
		ctx:           ctx,
		cancel:        cancel,
		jobs:          map[jobKey]*job{},
		stores:        map[string]blob.Store{},
	}
	if e.clock == nil {
		e.clock = time.Now
	}
	if e.grace <= 0 {
		e.grace = DefaultGracePeriod
	}
	if e.uploadWorkers <= 0 {
		e.uploadWorkers = DefaultUploadWorkers
	}
	if e.log == nil {
		e.log = slog.With(slog.String("component", "backup-executor"))
	}
	labels := map[string]any{}
	e.backupsCompleted = metric.NewCounter("oxia_server_backups_completed",
		"The number of shard backups completed", "count", labels)
	e.backupsFailed = metric.NewCounter("oxia_server_backups_failed",
		"The number of shard backups failed", "count", labels)
	e.uploadedBytes = metric.NewCounter("oxia_server_backup_uploaded",
		"The bytes the shard backups uploaded", metric.Bytes, labels)
	e.backupLatency = metric.NewLatencyHistogram("oxia_server_backup_latency",
		"The time a shard backup takes", labels)
	e.backupsExpired = metric.NewCounter("oxia_server_backups_expired",
		"The number of backups the cleanups expired", "count", labels)
	e.deletedBytes = metric.NewCounter("oxia_server_backup_deleted",
		"The bytes the cleanups deleted", metric.Bytes, labels)
	e.cleanupsFailed = metric.NewCounter("oxia_server_backup_cleanups_failed",
		"The number of backup cleanups failed", "count", labels)
	return e
}

// Close stops the jobs and waits for them.
func (e *Executor) Close() error {
	// The cancellation goes under mu: start admits a job and adds it to the
	// group under mu, so no job is admitted once the wait begins
	e.mu.Lock()
	e.cancel()
	e.mu.Unlock()
	e.wg.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	var err error
	for destination, store := range e.stores {
		err = multierr.Append(err, store.Close())
		delete(e.stores, destination)
	}
	return err
}

// policy is a BackupPolicy, parsed.
type policy struct {
	destination   string
	interval      time.Duration
	retention     time.Duration
	maxConcurrent int
	compression   string
}

func parsePolicy(p *proto.BackupPolicy) (policy, error) {
	parsed := policy{
		destination:   p.GetDestination(),
		interval:      time.Duration(p.GetIntervalMillis()) * time.Millisecond,
		retention:     time.Duration(p.GetRetentionMillis()) * time.Millisecond,
		maxConcurrent: int(p.GetMaxConcurrentPerServer()),
	}
	if p.GetCompression() {
		parsed.compression = backup.CompressionZstd
	}
	switch {
	case parsed.destination == "":
		return policy{}, errors.New("the backup policy has no destination")
	case parsed.interval <= 0 || parsed.retention <= parsed.interval:
		return policy{}, fmt.Errorf("the backup policy needs a retention longer than its interval: %s and %s",
			parsed.retention, parsed.interval)
	case parsed.maxConcurrent <= 0:
		return policy{}, errors.New("the backup policy must allow at least one job per server")
	}
	return parsed, nil
}

// reuseWindow is how old the last backup of a lineage can be for the next
// one to share its SSTs: the cleanup never expires a backup younger than the
// retention, and the margin covers the duration of the backup and the clock
// differences between the data servers.
func (p policy) reuseWindow(grace time.Duration) time.Duration {
	return p.retention - min(grace, p.retention/4)
}

// backupNeeded reports whether the policy wants a new backup of a shard
// whose last backup is last (nil for none) and whose database is at
// commitOffset: when the last backup is older than the interval, unless the
// shard didn't change since and the last backup is younger than the
// retention minus the interval.
func backupNeeded(last *LastBackup, commitOffset int64, p policy, now time.Time) bool {
	if last == nil {
		return true
	}
	age := now.Sub(last.Time)
	if age < p.interval {
		return false
	}
	if commitOffset != last.CommitOffset {
		return true
	}
	return age >= p.retention-p.interval
}

// validateShard rejects a request without a namespace, or with a negative
// shard, before it reaches the host: its paths need both.
func validateShard(namespace string, shard int64) error {
	if namespace == "" || shard < 0 {
		return fmt.Errorf("the request needs a namespace and a shard, got %q and %d", namespace, shard)
	}
	return nil
}

// BackupShard serves the OxiaBackup request of the same name.
func (e *Executor) BackupShard(_ context.Context, req *proto.BackupShardRequest) (*proto.BackupShardResponse, error) {
	if err := validateShard(req.GetNamespace(), req.GetShard()); err != nil {
		return nil, err
	}
	p, err := parsePolicy(req.GetPolicy())
	if err != nil {
		return nil, err
	}
	key := jobKey{kind: backupJobKind, namespace: req.GetNamespace(), shard: req.GetShard()}
	dir := e.host.DatabaseDir(req.GetNamespace(), req.GetShard())
	if state, finished := e.consume(key); state != proto.BackupJobState_BACKUP_JOB_STATE_UNKNOWN {
		return backupResponse(state, finished, dir)
	}
	// This record decides whether a backup is due and answers the request;
	// the job reads its own, bound to the database it checkpoints
	record, err := loadRecord(dir, p.destination)
	if err != nil {
		return nil, fmt.Errorf("read the backup record: %w", err)
	}
	commitOffset, err := e.host.CommitOffset(req.GetShard())
	if err != nil {
		return nil, err
	}
	if !req.GetForce() && !backupNeeded(record.LastBackup, commitOffset, p, e.clock()) {
		return &proto.BackupShardResponse{
			State:      proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED,
			LastBackup: recordProto(record),
		}, nil
	}
	namespaceConfig, err := protojson.Marshal(req.GetNamespaceConfig())
	if err != nil {
		return nil, fmt.Errorf("encode the namespace configuration: %w", err)
	}
	state := e.start(key, p.maxConcurrent, func(ctx context.Context) (any, error) {
		return e.runBackup(ctx, req, p, namespaceConfig)
	})
	return &proto.BackupShardResponse{State: state, LastBackup: recordProto(record)}, nil
}

func backupResponse(state proto.BackupJobState, finished *job, dir string) (*proto.BackupShardResponse, error) {
	record, _, err := readRecord(dir)
	if err != nil {
		return nil, fmt.Errorf("read the backup record: %w", err)
	}
	res := &proto.BackupShardResponse{State: state, LastBackup: recordProto(record)}
	if finished != nil && finished.err != nil {
		res.Error = finished.err.Error()
	}
	return res, nil
}

func (e *Executor) runBackup(ctx context.Context, req *proto.BackupShardRequest, p policy,
	namespaceConfig []byte) (any, error) {
	shard := req.GetShard()
	log := e.log.With(slog.String("namespace", req.GetNamespace()), slog.Int64("shard", shard))
	timer := e.backupLatency.Timer()
	store, err := e.store(p.destination)
	if err != nil {
		e.backupsFailed.Inc()
		return nil, err
	}
	checkpoint, err := e.host.Checkpoint(shard)
	if err != nil {
		e.backupsFailed.Inc()
		return nil, fmt.Errorf("take the checkpoint: %w", err)
	}
	defer func() { _ = checkpoint.Close() }()
	// The record is read from the database the checkpoint was taken from,
	// and written back to that database alone: a snapshot install in between
	// replaces the database, and the lineage of the old one, whose SSTs the
	// next backup would share by name, must not carry over to the new one
	var record *Record
	err = e.host.WithDatabase(shard, checkpoint.Generation, func(dir string) error {
		var err error
		record, err = loadRecord(dir, p.destination)
		return err
	})
	if err != nil {
		e.backupsFailed.Inc()
		return nil, fmt.Errorf("read the backup record: %w", err)
	}
	log = log.With(slog.String("lineage", record.Lineage))
	log.Info("Starting backup")
	j := &backupJob{
		store:           store,
		path:            backup.ShardPath{Namespace: req.GetNamespace(), Shard: req.GetShard()},
		checkpoint:      checkpoint,
		record:          record,
		policy:          p,
		hashRange:       backup.Range{Min: req.GetHashRange().GetMin(), Max: req.GetHashRange().GetMax()},
		namespaceConfig: namespaceConfig,
		now:             e.clock(),
		clock:           e.clock,
		reuseWindow:     p.reuseWindow(e.grace),
		uploadWorkers:   e.uploadWorkers,
	}
	last, err := j.run(ctx)
	if err != nil {
		e.backupsFailed.Inc()
		log.Warn("Backup failed", slog.Any("error", err))
		return nil, err
	}
	record.LastBackup = last
	err = e.host.WithDatabase(shard, checkpoint.Generation, func(dir string) error {
		return writeRecord(dir, record)
	})
	if errors.Is(err, database.ErrDatabaseReplaced) {
		// The backup of the old database is complete and valid; the next
		// backup of the new one starts a lineage of its own
		log.Warn("Backup record not kept: the database was replaced during the backup")
	} else if err != nil {
		e.backupsFailed.Inc()
		return nil, fmt.Errorf("write the backup record: %w", err)
	}
	timer.Done()
	e.backupsCompleted.Inc()
	e.uploadedBytes.Add(int(last.UploadedBytes))
	log.Info("Backup completed", slog.String("backup-id", last.ID), slog.Int64("commit-offset", last.CommitOffset),
		slog.Int64("size-bytes", last.SizeBytes), slog.Int64("uploaded-bytes", last.UploadedBytes),
		slog.Int64("duration-ms", last.DurationMillis))
	return last, nil
}

// ExpireBackups serves the OxiaBackup request of the same name.
func (e *Executor) ExpireBackups(_ context.Context, req *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error) {
	if req.GetNamespace() == "" {
		return nil, errors.New("the request has no namespace")
	}
	p, err := parsePolicy(req.GetPolicy())
	if err != nil {
		return nil, err
	}
	key := jobKey{kind: cleanupJobKind, namespace: req.GetNamespace(), shard: -1}
	if state, finished := e.consume(key); state != proto.BackupJobState_BACKUP_JOB_STATE_UNKNOWN {
		res := &proto.ExpireBackupsResponse{State: state}
		if finished != nil {
			if result, ok := finished.result.(cleanupResult); ok {
				res.BackupsExpired = uint32(result.backupsExpired)
				res.FilesDeleted = uint32(result.filesDeleted)
				res.BytesDeleted = uint64(result.bytesDeleted)
			}
			if finished.err != nil {
				res.Error = finished.err.Error()
			}
		}
		return res, nil
	}
	state := e.start(key, p.maxConcurrent, func(ctx context.Context) (any, error) {
		return e.runCleanup(ctx, req.GetNamespace(), p)
	})
	return &proto.ExpireBackupsResponse{State: state}, nil
}

func (e *Executor) runCleanup(ctx context.Context, namespace string, p policy) (any, error) {
	log := e.log.With(slog.String("namespace", namespace))
	log.Info("Starting backup cleanup")
	store, err := e.store(p.destination)
	if err != nil {
		e.cleanupsFailed.Inc()
		return nil, err
	}
	j := &cleanupJob{store: store, namespace: namespace, retention: p.retention, grace: e.grace, now: e.clock(), log: log}
	result, err := j.run(ctx)
	if err != nil {
		e.cleanupsFailed.Inc()
		log.Warn("Backup cleanup failed", slog.Any("error", err))
		return nil, err
	}
	e.backupsExpired.Add(result.backupsExpired)
	e.deletedBytes.Add(int(result.bytesDeleted))
	log.Info("Backup cleanup completed", slog.Int("backups-expired", result.backupsExpired),
		slog.Int("files-deleted", result.filesDeleted), slog.Int64("bytes-deleted", result.bytesDeleted))
	return result, nil
}

// ListBackups serves the OxiaBackup request of the same name.
func (e *Executor) ListBackups(ctx context.Context, req *proto.ListBackupsRequest) (*proto.ListBackupsResponse, error) {
	if req.GetDestination() == "" {
		return nil, errors.New("the request has no destination")
	}
	if req.GetNamespace() == "" {
		return nil, errors.New("the request has no namespace")
	}
	store, err := e.store(req.GetDestination())
	if err != nil {
		return nil, err
	}
	summaries, err := listBackups(ctx, store, req.GetNamespace(), req.Shard)
	if err != nil {
		return nil, err
	}
	res := &proto.ListBackupsResponse{Backups: make([]*proto.BackupInfo, 0, len(summaries))}
	for _, s := range summaries {
		namespaceConfig := &proto.Namespace{}
		if len(s.NamespaceConfig) > 0 {
			if err := protojson.Unmarshal(s.NamespaceConfig, namespaceConfig); err != nil {
				return nil, fmt.Errorf("%w: namespace configuration of %s: %w", backup.ErrInvalidManifest,
					s.ID, err)
			}
		}
		res.Backups = append(res.Backups, &proto.BackupInfo{
			NamespaceConfig: namespaceConfig,
			Shard:           s.Shard,
			BackupId:        s.ID.String(),
			TimestampMillis: s.StartedAt().UnixMilli(),
			CommitOffset:    s.CommitOffset,
			Term:            s.Term,
			HashRange:       &proto.HashRange{Min: s.Range.Min, Max: s.Range.Max},
			SizeBytes:       uint64(s.SizeBytes),
			SstCount:        uint32(s.SSTCount),
		})
	}
	return res, nil
}

// listBackups returns the backups of a namespace, or of one of its shards,
// oldest first by shard.
func listBackups(ctx context.Context, store blob.Store, namespace string, shard *int64) ([]backup.Summary, error) {
	var shards []backup.ShardPath
	if shard != nil {
		shards = []backup.ShardPath{{Namespace: namespace, Shard: *shard}}
	} else {
		var err error
		if shards, err = backup.ListShards(ctx, store, namespace); err != nil {
			return nil, err
		}
	}
	var all []backup.Summary
	for _, p := range shards {
		summaries, err := backup.ListManifests(ctx, store, p)
		if err != nil {
			return nil, err
		}
		all = append(all, summaries...)
	}
	return all, nil
}

// consume returns the state of the job at key: RUNNING, or DONE or FAILED
// with the finished job, which it forgets, or UNKNOWN when there is none.
func (e *Executor) consume(key jobKey) (proto.BackupJobState, *job) {
	e.mu.Lock()
	defer e.mu.Unlock()
	j, ok := e.jobs[key]
	if !ok {
		return proto.BackupJobState_BACKUP_JOB_STATE_UNKNOWN, nil
	}
	select {
	case <-j.done:
		delete(e.jobs, key)
		if j.err != nil {
			return proto.BackupJobState_BACKUP_JOB_STATE_FAILED, j
		}
		return proto.BackupJobState_BACKUP_JOB_STATE_DONE, j
	default:
		return proto.BackupJobState_BACKUP_JOB_STATE_RUNNING, nil
	}
}

// start runs fn as the job at key, unless limit jobs run already (no limit
// when it is 0), and returns STARTED or BUSY. It returns RUNNING when a job
// is at key already, running or waiting for the request that reports it:
// two requests that found no job at key may both come here, and one job at
// a time runs for a key.
func (e *Executor) start(key jobKey, limit int, fn func(ctx context.Context) (any, error)) proto.BackupJobState {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.jobs[key]; ok {
		return proto.BackupJobState_BACKUP_JOB_STATE_RUNNING
	}
	if e.ctx.Err() != nil || (limit > 0 && e.running >= limit) {
		return proto.BackupJobState_BACKUP_JOB_STATE_BUSY
	}
	j := &job{done: make(chan struct{})}
	e.jobs[key] = j
	e.running++
	e.wg.Go(func() {
		result, err := fn(e.ctx)
		e.mu.Lock()
		j.result, j.err = result, err
		e.running--
		e.mu.Unlock()
		close(j.done)
	})
	return proto.BackupJobState_BACKUP_JOB_STATE_STARTED
}

// store returns the store of a destination, opened once.
func (e *Executor) store(destination string) (blob.Store, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if store, ok := e.stores[destination]; ok {
		return store, nil
	}
	store, err := blob.Open(e.ctx, destination)
	if err != nil {
		return nil, err
	}
	e.stores[destination] = store
	return store, nil
}

func recordProto(record *Record) *proto.BackupRecord {
	if record == nil || record.LastBackup == nil {
		return nil
	}
	last := record.LastBackup
	return &proto.BackupRecord{
		BackupId:        last.ID,
		Lineage:         record.Lineage,
		TimestampMillis: last.Time.UnixMilli(),
		CommitOffset:    last.CommitOffset,
		Term:            last.Term,
		SizeBytes:       uint64(last.SizeBytes),
		UploadedBytes:   uint64(last.UploadedBytes),
		DurationMillis:  last.DurationMillis,
	}
}
