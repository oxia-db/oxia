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

package database

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxiad/common/crc"
	featurepkg "github.com/oxia-db/oxia/oxiad/common/feature"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/oxiad/dataserver/wal"

	"github.com/oxia-db/oxia/common/constant"
	time2 "github.com/oxia-db/oxia/common/time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
)

var (
	ErrBadVersionId          = errors.New("oxia: bad version id")
	ErrMissingPartitionKey   = errors.New("oxia: sequential key operation requires partition key")
	ErrMissingSequenceDeltas = errors.New("oxia: sequential key operation missing some sequence deltas")
	ErrSequenceDeltaIsZero   = errors.New("oxia: sequential key operation requires first delta do be > 0")
	ErrSequenceOverflow      = errors.New("oxia: sequential key operation overflows the sequence")
	ErrInvalidSequenceKey    = errors.New("oxia: sequential key operation found a key that is not part of the sequence")
	ErrNotificationRecord    = errors.New("oxia: write request reaches a notification record")
	ErrNotificationsDisabled = errors.New("oxia: notifications disabled")

	// ErrWriteRejected marks a write request that ProcessWrite rejected
	// because it can't be applied to the database: the request has no effect,
	// and its log entry is applied.
	ErrWriteRejected = errors.New("oxia: write request rejected")
)

const (
	commitOffsetKey        = constant.InternalKeyPrefix + "commit-offset"
	commitLastVersionIdKey = constant.InternalKeyPrefix + "last-version-id"
	commitChecksumKey      = constant.InternalKeyPrefix + "checksum"
	featureFlagKeyPrefix   = constant.InternalKeyPrefix + "features"
	termKey                = constant.InternalKeyPrefix + "term"
	termOptionsKey         = termKey + "-options"
	splitFilterKey         = constant.InternalKeyPrefix + "split-filter"
)

type UpdateOperationCallback interface {
	// ValidatePut must not mutate the request or database state.
	ValidatePut(req *proto.PutRequest, features featurepkg.Checker) proto.Status
	OnPut(batch kvstore.WriteBatch, notifications *Notifications, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error)
	OnDeleteWithEntry(batch kvstore.WriteBatch, notifications *Notifications, key string, value *proto.StorageEntry, features featurepkg.Checker) error
}

type RangeScanIterator interface {
	io.Closer

	Valid() bool
	Value() (*proto.GetResponse, error)
	Next() bool
}

type TermOptions struct {
	NotificationsEnabled bool
	KeySorting           proto.KeySortingType

	// Features is the feature set negotiated for the term, pinned for its
	// whole duration. Kept sorted so the persisted form is deterministic.
	Features []proto.Feature
}

type Meta struct {
	Checksum *crc.Checksum
}

type DB interface {
	io.Closer

	EnableNotifications(enable bool)

	EnableFeature(f proto.Feature)
	IsFeatureEnabled(f proto.Feature) bool
	EnabledFeatures() []proto.Feature
	ReadChecksum() crc.Checksum
	ResetChecksum()

	ProcessWrite(b *proto.WriteRequest, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error)
	ProcessControlRequest(controlRequest *proto.ControlRequest, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*Meta, error)

	Get(request *proto.GetRequest) (*proto.GetResponse, error)
	List(request *proto.ListRequest) (kvstore.KeyIterator, error)
	RangeScan(request *proto.RangeScanRequest) (RangeScanIterator, error)

	// KeyPrefixIterator returns an iterator over the keys that start with
	// prefix, internal keys included, to position with SeekGE or SeekLT
	KeyPrefixIterator(prefix string) (kvstore.KeyIterator, error)

	// ListPrefix returns the iterator of KeyPrefixIterator, and counts it as a
	// list, like List
	ListPrefix(prefix string) (kvstore.KeyIterator, error)

	// CompareKeys compares two keys in the order the shard sorts them
	CompareKeys(a, b string) int

	ReadCommitOffset() (int64, error)

	ReadNextNotifications(ctx context.Context, startOffset int64) ([]proto.EncodedNotificationBatch, error)
	GetSequenceUpdates(prefixKey string) (SequenceWaiter, error)

	UpdateTerm(newTerm int64, options TermOptions) error
	ReadTerm() (term int64, options TermOptions, err error)

	// SetSplitFilter records the filter of a split child, and flushes the
	// database: the writes that precede it become durable as well.
	SetSplitFilter(filter *SplitFilter) error
	// SplitFilter returns the filter recorded by SetSplitFilter, or nil.
	SplitFilter() *SplitFilter

	Snapshot() (kvstore.Snapshot, error)

	// RawKV returns the underlying key-value store. Used for operations that
	// need direct access to the storage layer, such as split filtering.
	RawKV() kvstore.KV

	// Delete and close the database and all its files
	Delete() error

	// Stats returns cumulative shard statistics for auto-split monitoring.
	Stats() *proto.ShardStats
}

func NewDB(namespace string, shardId int64, factory kvstore.Factory,
	keySorting proto.KeySortingType,
	notificationRetentionTime time.Duration,
	clock time2.Clock,
) (DB, error) {
	kv, err := factory.NewKV(namespace, shardId, keySorting)
	if err != nil {
		return nil, err
	}

	labels := metric.LabelsForShard(namespace, shardId)
	db := &db{
		kv:                    kv,
		shardId:               shardId,
		enabledFeatures:       sync.Map{},
		sequenceWaiterTracker: NewSequencesWaitTracker(),
		log: slog.With(
			slog.String("component", "db"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
		),

		batchWriteLatencyHisto: metric.NewLatencyHistogram("oxia_server_db_batch_write_latency",
			"The time it takes to write a batch in the db", labels),
		getLatencyHisto: metric.NewLatencyHistogram("oxia_server_db_get_latency",
			"The time it takes to get from the db", labels),
		listLatencyHisto: metric.NewLatencyHistogram("oxia_server_db_list_latency",
			"The time it takes to read a list from the db", labels),
		putCounter: metric.NewCounter("oxia_server_db_puts",
			"The total number of put operations", "count", labels),
		deleteCounter: metric.NewCounter("oxia_server_db_deletes",
			"The total number of delete operations", "count", labels),
		deleteRangesCounter: metric.NewCounter("oxia_server_db_delete_ranges",
			"The total number of delete ranges operations", "count", labels),
		getCounter: metric.NewCounter("oxia_server_db_gets",
			"The total number of get operations", "count", labels),
		getSequenceUpdatesCounter: metric.NewCounter("oxia_server_db_get_sequence_updates",
			"The total number of get sequence updates operations", "count", labels),
		listCounter: metric.NewCounter("oxia_server_db_lists",
			"The total number of list operations", "count", labels),
		rangeScanCounter: metric.NewCounter("oxia_server_db_range_scans",
			"The total number of range-scan operations", "count", labels),
	}
	db.notificationsEnabled.Store(true)

	// Close the kv on any failure from here on: a leaked store would keep
	// the Pebble lock and fail every later open of the shard
	lastVersionId, err := db.readLastVersionId()
	if err != nil {
		return nil, multierr.Append(err, kv.Close())
	}
	db.committedVersionId.Store(lastVersionId)

	// init the DB checksum
	lastChecksum, err := db.readLastChecksum()
	if err != nil {
		return nil, multierr.Append(err, kv.Close())
	}
	if !lastChecksum.IsZero() {
		db.enabledFeatures.Store(proto.Feature_FEATURE_DB_CHECKSUM, true)
	}
	db.committedChecksum.Store(&lastChecksum)

	if err := db.recoverFeatureFlags(); err != nil {
		return nil, multierr.Append(err, kv.Close())
	}

	if err := db.recoverSplitFilter(); err != nil {
		return nil, multierr.Append(err, kv.Close())
	}

	lastNotificationOffset, err := db.readLastNotificationOffset()
	if err != nil {
		return nil, multierr.Append(errors.Wrap(err, "failed to read last notification offset"), kv.Close())
	}

	db.notificationsTracker = newNotificationsTracker(namespace, shardId, lastNotificationOffset, kv, notificationRetentionTime, clock)
	return db, nil
}

type db struct {
	kv                    kvstore.KV
	shardId               int64
	committedVersionId    atomic.Int64
	committedChecksum     atomic.Pointer[crc.Checksum]
	notificationsTracker  *notificationsTracker
	log                   *slog.Logger
	notificationsEnabled  atomic.Bool
	enabledFeatures       sync.Map
	splitFilter           atomic.Pointer[SplitFilter]
	sequenceWaiterTracker SequenceWaiterTracker

	putCounter                metric.Counter
	deleteCounter             metric.Counter
	deleteRangesCounter       metric.Counter
	getCounter                metric.Counter
	getSequenceUpdatesCounter metric.Counter
	listCounter               metric.Counter
	rangeScanCounter          metric.Counter

	readOpsTotal  atomic.Uint64
	writeOpsTotal atomic.Uint64

	batchWriteLatencyHisto metric.LatencyHistogram
	getLatencyHisto        metric.LatencyHistogram
	listLatencyHisto       metric.LatencyHistogram
}

func (d *db) Snapshot() (kvstore.Snapshot, error) {
	return d.kv.Snapshot()
}

func (d *db) RawKV() kvstore.KV {
	return d.kv
}

func (d *db) EnableNotifications(enabled bool) {
	d.notificationsEnabled.Store(enabled)
}
func (d *db) ReadChecksum() crc.Checksum {
	return *d.committedChecksum.Load()
}

func (d *db) ResetChecksum() {
	var zero crc.Checksum
	d.committedChecksum.Store(&zero)
}

func (d *db) Close() error {
	return multierr.Combine(
		d.sequenceWaiterTracker.Close(),
		d.notificationsTracker.Close(),
		d.kv.Close(),
	)
}

func (d *db) Stats() *proto.ShardStats {
	return &proto.ShardStats{
		DbSizeBytes:   d.kv.DiskSpaceUsage(),
		ReadOpsTotal:  d.readOpsTotal.Load(),
		WriteOpsTotal: d.writeOpsTotal.Load(),
	}
}

func (d *db) Delete() error {
	return multierr.Combine(
		d.sequenceWaiterTracker.Close(),
		d.notificationsTracker.Close(),
		d.kv.Delete(),
	)
}

func now() uint64 {
	return uint64(time.Now().UnixMilli())
}

// sequenceUpdate is the new last key of a sequence, for the waiters to be
// notified once the batch that creates it is committed.
type sequenceUpdate struct {
	prefixKey string
	key       string
}

func (d *db) applyWriteRequest(b *proto.WriteRequest, batch kvstore.WriteBatch,
	baseVersionId *atomic.Int64, commitOffset int64, timestamp uint64,
	updateOperationCallback UpdateOperationCallback) (*Notifications, *proto.WriteResponse, []sequenceUpdate, error) {
	res := &proto.WriteResponse{}
	var notifications *Notifications
	if d.notificationsEnabled.Load() {
		notifications = newNotifications(d.shardId, commitOffset, timestamp)
		// Room for one notification per operation, the common case
		notifications.reserve(len(b.Puts) + len(b.Deletes) + len(b.DeleteRanges))
	}
	var sequenceUpdates []sequenceUpdate

	d.putCounter.Add(len(b.Puts))
	d.deleteCounter.Add(len(b.Deletes))
	d.deleteRangesCounter.Add(len(b.DeleteRanges))

	nextWriteOp := nextWriteOpByType
	if d.IsFeatureEnabled(proto.Feature_FEATURE_ORDERED_WRITES) {
		nextWriteOp = nextWriteOpByOpIndex
	}
	var p, dl, dr int
	for p < len(b.Puts) || dl < len(b.Deletes) || dr < len(b.DeleteRanges) {
		switch nextWriteOp(b, p, dl, dr) {
		case writeOpPut:
			// A sequential put replaces the request key with the generated one
			prefixKey := b.Puts[p].Key
			pr, err := d.applyPut(batch, baseVersionId, notifications, b.Puts[p], timestamp, updateOperationCallback, false)
			if err != nil {
				return nil, nil, nil, err
			}
			if pr.Key != nil {
				sequenceUpdates = append(sequenceUpdates, sequenceUpdate{prefixKey: prefixKey, key: *pr.Key})
			}
			res.Puts = append(res.Puts, pr)
			p++
		case writeOpDelete:
			delRes, err := d.applyDelete(batch, notifications, b.Deletes[dl], updateOperationCallback)
			if err != nil {
				return nil, nil, nil, err
			}
			res.Deletes = append(res.Deletes, delRes)
			dl++
		default: // writeOpDeleteRange
			delRangeRes, err := d.applyDeleteRange(batch, notifications, b.DeleteRanges[dr], updateOperationCallback)
			if err != nil {
				return nil, nil, nil, err
			}
			res.DeleteRanges = append(res.DeleteRanges, delRangeRes)
			dr++
		}
	}

	d.writeOpsTotal.Add(uint64(len(b.Puts) + len(b.Deletes) + len(b.DeleteRanges)))

	return notifications, res, sequenceUpdates, nil
}

type writeOp int

const (
	writeOpPut writeOp = iota
	writeOpDelete
	writeOpDeleteRange
)

// nextWriteOpByType returns the batch that holds the next request to apply,
// given the position reached in each batch, in the legacy order: all the
// puts, then the deletes, then the delete ranges.
func nextWriteOpByType(b *proto.WriteRequest, p, d, _ int) writeOp {
	switch {
	case p < len(b.Puts):
		return writeOpPut
	case d < len(b.Deletes):
		return writeOpDelete
	default:
		return writeOpDeleteRange
	}
}

// nextWriteOpByOpIndex returns the batch that holds the next request to
// apply, given the position reached in each batch: the one whose next request
// has the lowest op_index, with ties going to puts, then deletes, then delete
// ranges. Requests without op_index all tie, so they keep the legacy order.
func nextWriteOpByOpIndex(b *proto.WriteRequest, p, d, r int) writeOp {
	next, found, opIndex := writeOpPut, false, uint32(0)
	if p < len(b.Puts) {
		next, found, opIndex = writeOpPut, true, b.Puts[p].OpIndex
	}
	if d < len(b.Deletes) && (!found || b.Deletes[d].OpIndex < opIndex) {
		next, found, opIndex = writeOpDelete, true, b.Deletes[d].OpIndex
	}
	if r < len(b.DeleteRanges) && (!found || b.DeleteRanges[r].OpIndex < opIndex) {
		next = writeOpDeleteRange
	}
	return next
}

func (d *db) IsFeatureEnabled(f proto.Feature) bool {
	_, ok := d.enabledFeatures.Load(f)
	return ok
}

func (d *db) EnableFeature(f proto.Feature) {
	d.enabledFeatures.Store(f, true)
}

func (d *db) EnabledFeatures() []proto.Feature {
	var features []proto.Feature
	d.enabledFeatures.Range(func(key, _ any) bool {
		features = append(features, key.(proto.Feature))
		return true
	})
	slices.Sort(features)
	return features
}

func featureFlagKey(f proto.Feature) string {
	return fmt.Sprintf("%s/%010d", featureFlagKeyPrefix, f)
}

func (d *db) ProcessControlRequest(cmd *proto.ControlRequest, commitOffset int64, timestamp uint64, _ UpdateOperationCallback) (*Meta, error) {
	meta := &Meta{
		Checksum: new(d.ReadChecksum()),
	}

	var featuresToEnable []proto.Feature
	controlValue := cmd.GetValue()
	switch v := controlValue.(type) {
	case *proto.ControlRequest_FeatureEnable:
		featuresToEnable = v.FeatureEnable.GetFeatures()
		if unsupported := featurepkg.Unsupported(featuresToEnable); len(unsupported) > 0 {
			// Applying entries after this one with the feature off would
			// silently diverge from the replicas that do support it.
			return nil, errors.Wrapf(constant.ErrUnsupportedFeatures,
				"refusing to enable features %v not supported by this binary", unsupported)
		}
	case *proto.ControlRequest_RecordChecksum:
		// Recognized no-op. Checksum is already in meta.
	default:
		return nil, errors.Errorf("unknown control request type %T", controlValue)
	}

	batch := d.kv.NewWriteBatch()
	defer batch.Close()

	if err := d.addASCIILong(commitOffsetKey, commitOffset, batch, timestamp); err != nil {
		return nil, err
	}
	for _, f := range featuresToEnable {
		if err := d.addASCIILong(featureFlagKey(f), int64(f), batch, timestamp); err != nil {
			return nil, err
		}
	}
	if err := batch.Commit(); err != nil {
		return nil, err
	}

	for _, f := range featuresToEnable {
		d.enabledFeatures.Store(f, true)
	}

	return meta, nil
}

func (d *db) ProcessWrite(b *proto.WriteRequest, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error) {
	timer := d.batchWriteLatencyHisto.Timer()
	defer timer.Done()

	baseVersionId := &atomic.Int64{}
	baseVersionId.Store(d.committedVersionId.Load())

	batch := d.kv.NewWriteBatch()
	defer batch.Close()

	notifications, res, sequenceUpdates, err := d.applyWriteRequest(b, batch, baseVersionId, commitOffset, timestamp,
		updateOperationCallback)
	if isRejectedWrite(err) {
		return nil, d.rejectWrite(err, commitOffset, timestamp)
	}
	if err != nil {
		return nil, err
	}

	if err := d.addASCIILong(commitOffsetKey, commitOffset, batch, timestamp); err != nil {
		return nil, err
	}

	uncommitedVersionId := baseVersionId.Load()
	if err := d.addASCIILong(commitLastVersionIdKey, uncommitedVersionId, batch, timestamp); err != nil {
		return nil, err
	}

	if notifications != nil {
		// Add the notifications to the batch as well
		if err := d.addNotifications(batch, notifications); err != nil {
			return nil, err
		}
	}

	previousChecksum := *d.committedChecksum.Load()
	committedChecksum := previousChecksum
	if !previousChecksum.IsZero() || d.IsFeatureEnabled(proto.Feature_FEATURE_DB_CHECKSUM) {
		committedChecksum = batch.Checksum(previousChecksum)
		if err := d.addASCIILong(commitChecksumKey, int64(committedChecksum), batch, timestamp); err != nil {
			return nil, err
		}
	}

	if err := batch.Commit(); err != nil {
		return nil, err
	}
	// update the db local cache of version_id after commit success
	d.committedVersionId.Store(uncommitedVersionId)
	// update the db local cache of committed_checksum after commit success
	d.committedChecksum.Store(&committedChecksum)

	if notifications != nil {
		d.notificationsTracker.Committed(notifications)
	}

	// Only once committed: the waiters must never see a key that doesn't
	// exist, e.g. because the request failed after the sequential put
	for _, update := range sequenceUpdates {
		d.sequenceWaiterTracker.SequenceUpdated(update.prefixKey, update.key)
	}

	return res, nil
}

// isRejectedWrite reports whether err rejects a write request for its content,
// or for the database state it applies to, rather than for a storage failure:
// retrying the request can't succeed, and every replica applying it to the
// same state rejects it the same way.
func isRejectedWrite(err error) bool {
	return errors.Is(err, ErrMissingPartitionKey) ||
		errors.Is(err, ErrMissingSequenceDeltas) ||
		errors.Is(err, ErrSequenceDeltaIsZero) ||
		errors.Is(err, ErrSequenceOverflow) ||
		errors.Is(err, ErrInvalidSequenceKey) ||
		// E.g. a DeleteRange without an end, with the natural key sorting.
		// Each replica trims the notification records on its own: the
		// request is only rejected where some are left.
		errors.Is(err, ErrNotificationRecord)
}

// deserializeFailure returns the failure to deserialize the value of key,
// err, along with closeErr, the failure to release the read. It rejects the
// request when key is a notification record, which is not a storage entry,
// and the read was released: any other value that fails to deserialize is
// damaged, and a read that fails to be released is a storage failure.
func deserializeFailure(key string, err error, closeErr error) error {
	switch {
	case closeErr != nil:
		return multierr.Append(err, closeErr)
	case strings.HasPrefix(key, notificationsPrefix+"/"):
		return fmt.Errorf("%w: %w", ErrNotificationRecord, err)
	default:
		return err
	}
}

// rejectWrite records the write request at commitOffset as applied, with no
// effect. The leader answers the client with the error and moves on: failing
// the log entry instead would leave the followers retrying it forever, and no
// other replica could become leader.
//
// Only the commit offset is written, and the checksum is left as it is: the
// data, the notifications and the checksum stay the same as on a replica that
// dropped the request without recording it, like the leaders of the previous
// versions.
func (d *db) rejectWrite(cause error, commitOffset int64, timestamp uint64) error {
	batch := d.kv.NewWriteBatch()
	defer batch.Close()

	if err := d.addASCIILong(commitOffsetKey, commitOffset, batch, timestamp); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}

	d.log.Warn(
		"Rejected write request, it has no effect",
		slog.Int64("offset", commitOffset),
		slog.Any("error", cause),
	)
	return fmt.Errorf("%w: %w", ErrWriteRejected, cause)
}

func (d *db) addNotifications(batch kvstore.WriteBatch, notifications *Notifications) error {
	// seal() sorts the entries by key, which makes the generated marshal
	// deterministic — required because the value feeds the replicated batch
	// checksum.
	nb := notifications.seal()
	if !d.notificationsTracker.Caching() {
		// The bytes land directly in the batch arena
		return batch.PutMarshalable(notificationKey(nb.Offset), nb)
	}
	// The notifications cache keeps the bytes, which the batch copies
	var err error
	if notifications.encoded, err = nb.MarshalVT(); err != nil {
		return err
	}
	return batch.Put(notificationKey(nb.Offset), notifications.encoded)
}

func (d *db) addASCIILong(key string, value int64, batch kvstore.WriteBatch, timestamp uint64) error {
	asciiValue := []byte(fmt.Sprintf("%d", value))
	_, err := d.applyPut(batch, nil, nil, &proto.PutRequest{
		Key:               key,
		Value:             asciiValue,
		ExpectedVersionId: nil,
	}, timestamp, NoOpCallback, true)
	return err
}

func (d *db) Get(request *proto.GetRequest) (*proto.GetResponse, error) {
	timer := d.getLatencyHisto.Timer()
	defer timer.Done()

	d.getCounter.Add(1)
	d.readOpsTotal.Add(1)
	return applyGet(d.kv, request)
}

func (d *db) GetSequenceUpdates(prefixKey string) (SequenceWaiter, error) {
	d.getSequenceUpdatesCounter.Add(1)

	sw := d.sequenceWaiterTracker.AddSequenceWaiter(prefixKey)

	// First read last key in the sequence
	it, err := d.kv.KeyRangeScanReverse(
		fmt.Sprintf("%s-%020d", prefixKey, 0),
		fmt.Sprintf("%s-%020d", prefixKey, math.MaxInt64),
		kvstore.NoInternalKeys)
	if err != nil {
		err = multierr.Append(err, sw.Close())
		return nil, err
	} else if it.Valid() {
		sw.och.WriteLast(it.Key())
	}

	// The iterator is also invalid when the read failed: Close returns the error
	if err := it.Close(); err != nil {
		return nil, multierr.Append(errors.Wrap(err, "failed to read the last key of the sequence"), sw.Close())
	}
	return sw, nil
}

type listIterator struct {
	kvstore.KeyIterator
	timer metric.Timer
}

func (it *listIterator) Close() error {
	it.timer.Done()
	return it.KeyIterator.Close()
}

func (d *db) List(request *proto.ListRequest) (kvstore.KeyIterator, error) {
	d.listCounter.Add(1)
	d.readOpsTotal.Add(1)

	it, err := d.kv.KeyRangeScan(request.StartInclusive, request.EndExclusive,
		kvstore.IteratorOpts{IncludeInternalKeys: request.IncludeInternalKeys})
	if err != nil {
		return nil, err
	}

	return &listIterator{
		KeyIterator: it,
		timer:       d.listLatencyHisto.Timer(),
	}, nil
}

type rangeScanIterator struct {
	kvstore.KeyValueIterator
	timer metric.Timer
}

func (it *rangeScanIterator) Value() (*proto.GetResponse, error) {
	value, err := it.KeyValueIterator.Value()
	if err != nil {
		return nil, err
	}

	se := &proto.StorageEntry{}

	// Key() decodes the key on every call: do it once per entry
	key := it.Key()

	// Notifications are not using the stats headers so they would
	// fail to deserialize. We just provide the content without the
	// version object
	if strings.HasPrefix(key, notificationsPrefix) {
		se.Value = value
	} else if err = Deserialize(value, se); err != nil {
		return nil, err
	}

	res := &proto.GetResponse{
		Key:    &key,
		Value:  se.Value,
		Status: proto.Status_OK,
		Version: &proto.Version{
			VersionId:          se.VersionId,
			ModificationsCount: se.ModificationsCount,
			CreatedTimestamp:   se.CreationTimestamp,
			ModifiedTimestamp:  se.ModificationTimestamp,
			SessionId:          se.SessionId,
			ClientIdentity:     se.ClientIdentity,
		},
	}

	return res, nil
}

func (it *rangeScanIterator) Close() error {
	it.timer.Done()
	return it.KeyValueIterator.Close()
}

func (d *db) RangeScan(request *proto.RangeScanRequest) (RangeScanIterator, error) {
	d.rangeScanCounter.Add(1)
	d.readOpsTotal.Add(1)

	it, err := d.kv.RangeScan(request.StartInclusive, request.EndExclusive,
		kvstore.IteratorOpts{IncludeInternalKeys: request.IncludeInternalKeys})
	if err != nil {
		return nil, err
	}

	return &rangeScanIterator{
		KeyValueIterator: it,
		timer:            d.listLatencyHisto.Timer(),
	}, nil
}

func (d *db) KeyPrefixIterator(prefix string) (kvstore.KeyIterator, error) {
	return d.kv.KeyPrefixIterator(prefix)
}

func (d *db) ListPrefix(prefix string) (kvstore.KeyIterator, error) {
	d.listCounter.Add(1)
	d.readOpsTotal.Add(1)

	it, err := d.kv.KeyPrefixIterator(prefix)
	if err != nil {
		return nil, err
	}

	return &listIterator{
		KeyIterator: it,
		timer:       d.listLatencyHisto.Timer(),
	}, nil
}

func (d *db) CompareKeys(a, b string) int {
	return d.kv.CompareKeys(a, b)
}

func (d *db) ReadCommitOffset() (int64, error) {
	return d.readASCIILongOrDefault(commitOffsetKey, constant.I64NegativeOne)
}

func (d *db) readLastVersionId() (int64, error) {
	return d.readASCIILongOrDefault(commitLastVersionIdKey, constant.I64NegativeOne)
}
func (d *db) readLastChecksum() (crc.Checksum, error) {
	cs, err := d.readASCIILongOrDefault(commitChecksumKey, constant.I64Zero)
	if err != nil {
		return 0, err
	}
	return crc.Checksum(uint32(cs)), nil
}

func (d *db) recoverFeatureFlags() error {
	// Use a prefix break instead of an upper bound because key ordering can be
	// natural or hierarchical, and the safe upper bound differs between them.
	it, err := d.kv.KeyRangeScan(featureFlagKeyPrefix+"/", "", kvstore.ShowInternalKeys)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Valid() {
		key := it.Key()
		if !strings.HasPrefix(key, featureFlagKeyPrefix+"/") {
			break
		}

		featureValue, err := strconv.ParseInt(strings.TrimPrefix(key, featureFlagKeyPrefix+"/"), 10, 32)
		if err != nil {
			return errors.Wrapf(err, "invalid feature flag key %q", key)
		}
		if featureValue < 0 {
			return errors.Errorf("invalid feature flag key %q", key)
		}

		f := proto.Feature(featureValue)
		if key != featureFlagKey(f) {
			return errors.Errorf("invalid feature flag key %q", key)
		}
		if !featurepkg.IsSupported(f) {
			// This covers both a rolled-back binary reopening a database with
			// newer features enabled and a snapshot carrying such flags:
			// serving the shard would apply entries with the feature off and
			// silently diverge from the rest of the ensemble.
			return errors.Wrapf(constant.ErrUnsupportedFeatures,
				"feature %s is enabled in the database but not supported by this binary; refusing to serve the shard", f)
		}
		d.enabledFeatures.Store(f, true)

		it.Next()
	}

	// The iteration also stops when a read fails
	if err := it.Error(); err != nil {
		return errors.Wrap(err, "failed to read the feature flags")
	}
	return nil
}

func (d *db) readLastNotificationOffset() (int64, error) {
	key, _, closer, err := d.kv.Get(lastNotificationKey, kvstore.ComparisonFloor, kvstore.ShowInternalKeys)
	if errors.Is(err, kvstore.ErrKeyNotFound) {
		d.log.Debug("No notification offset found")
		return constant.I64NegativeOne, nil
	}
	if err != nil {
		return constant.I64NegativeOne, err
	}
	defer closer.Close()

	if !strings.HasPrefix(key, notificationsPrefix+"/") {
		d.log.Debug("No notification offset found", slog.String("floor-key", key))
		return constant.I64NegativeOne, nil
	}

	return parseNotificationKey(key)
}

func (d *db) readASCIILongOrDefault(key string, defaultValue int64) (int64, error) {
	kv := d.kv

	getReq := &proto.GetRequest{
		Key:          key,
		IncludeValue: true,
	}
	gr, err := applyGet(kv, getReq)
	if err != nil {
		return 0, err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return defaultValue, nil
	}

	var res int64
	if _, err = fmt.Sscanf(string(gr.Value), "%d", &res); err != nil {
		return 0, err
	}
	return res, nil
}

func (d *db) UpdateTerm(newTerm int64, options TermOptions) error {
	batch := d.kv.NewWriteBatch()
	defer batch.Close()

	if _, err := d.applyPut(batch, nil, nil, &proto.PutRequest{
		Key:   termKey,
		Value: []byte(fmt.Sprintf("%d", newTerm)),
	}, now(), NoOpCallback, true); err != nil {
		return err
	}

	serOptions, err := json.Marshal(options)
	if err != nil {
		return err
	}
	if _, err := d.applyPut(batch, nil, nil, &proto.PutRequest{
		Key:   termOptionsKey,
		Value: serOptions,
	}, now(), NoOpCallback, true); err != nil {
		return err
	}

	if err := batch.Commit(); err != nil {
		return err
	}

	// Since the term change is not stored in the WAL, we must force
	// the database to flush, in order to ensure the term change is durable
	return d.kv.Flush()
}

func (d *db) ReadTerm() (term int64, options TermOptions, err error) {
	getReq := &proto.GetRequest{
		Key:          termKey,
		IncludeValue: true,
	}
	gr, err := applyGet(d.kv, getReq)
	if err != nil {
		return wal.InvalidTerm, TermOptions{}, err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return wal.InvalidTerm, TermOptions{}, nil
	}

	if _, err = fmt.Sscanf(string(gr.Value), "%d", &term); err != nil {
		return wal.InvalidTerm, TermOptions{}, err
	}

	if gr, err = applyGet(d.kv, &proto.GetRequest{Key: termOptionsKey, IncludeValue: true}); err != nil {
		return wal.InvalidTerm, TermOptions{}, err
	}

	if gr.Status == proto.Status_KEY_NOT_FOUND {
		options = TermOptions{}
	} else {
		if err := json.Unmarshal(gr.Value, &options); err != nil {
			return wal.InvalidTerm, TermOptions{}, err
		}
	}

	return term, options, nil
}

//nolint:revive
func (d *db) applyPut(batch kvstore.WriteBatch, baseVersionId *atomic.Int64, notifications *Notifications,
	putReq *proto.PutRequest, timestamp uint64,
	updateOperationCallback UpdateOperationCallback, internal bool) (*proto.PutResponse, error) {
	if status := updateOperationCallback.ValidatePut(putReq, d); status != proto.Status_OK {
		return &proto.PutResponse{Status: status}, nil
	}

	var se *proto.StorageEntry
	var err error
	var newKey string
	if len(putReq.GetSequenceKeyDelta()) > 0 {
		if newKey, err = generateUniqueKeyFromSequences(batch, putReq, d); err == nil {
			putReq.Key = newKey
		}
	} else if !internal {
		se, err = checkExpectedVersionId(batch, putReq.Key, putReq.ExpectedVersionId)
	}

	switch {
	case errors.Is(err, ErrBadVersionId):
		return &proto.PutResponse{
			Status: proto.Status_UNEXPECTED_VERSION_ID,
		}, nil
	case isInvalidSequentialPut(err) && d.IsFeatureEnabled(proto.Feature_FEATURE_SEQUENCE_KEY_VALIDATION):
		// Only this put fails, instead of the whole write request with its
		// other operations
		return &proto.PutResponse{
			Status: proto.Status_INVALID_ARGUMENT,
		}, nil
	case err != nil:
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}

	// No version conflict.
	// The closure returns whichever entry is current on exit: se is nil for a
	// new key and replaced with a pooled entry below, and the callback paths
	// can return early before that happens.
	defer func() { se.ReturnToVTPool() }()

	versionId := wal.InvalidOffset
	if !internal {
		status, err := updateOperationCallback.OnPut(batch, notifications, putReq, se)
		if err != nil {
			return nil, err
		}
		if status != proto.Status_OK {
			return &proto.PutResponse{
				Status: status,
			}, nil
		}
		if putReq.OverrideVersionId != nil {
			versionId = *putReq.OverrideVersionId
		} else {
			versionId = baseVersionId.Add(1)
		}
	}

	if se == nil {
		se = proto.StorageEntryFromVTPool()
		se.VersionId = versionId
		se.ModificationsCount = 0
		se.Value = putReq.Value
		se.CreationTimestamp = timestamp
		se.ModificationTimestamp = timestamp
		se.SessionId = putReq.SessionId
		se.ClientIdentity = putReq.ClientIdentity
		se.PartitionKey = putReq.PartitionKey
	} else {
		se.VersionId = versionId
		se.ModificationsCount++
		se.Value = putReq.Value
		se.ModificationTimestamp = timestamp
		se.SessionId = putReq.SessionId
		se.ClientIdentity = putReq.ClientIdentity
		se.PartitionKey = putReq.PartitionKey
	}

	if putReq.OverrideModificationsCount != nil {
		se.ModificationsCount = *putReq.OverrideModificationsCount
	}

	se.SecondaryIndexes = putReq.SecondaryIndexes

	// Marshal the entry directly into the batch arena: marshal-then-Put would
	// allocate an intermediate buffer and copy the entry twice
	err = batch.PutMarshalable(putReq.Key, se)
	// The entry borrowed the request's buffers (Value, SecondaryIndexes), and
	// the marshal above was their last reader. Detach them before the pool
	// return: ResetVT keeps the Value capacity and the SecondaryIndexes slice
	// for reuse, and a later Deserialize into this pooled entry appends into
	// that capacity — the apply path decodes requests with UnmarshalVTUnsafe,
	// so the borrowed buffer is the WAL entry payload, still aliased by every
	// other operation of the same entry.
	se.Value = nil
	se.SecondaryIndexes = nil
	if err != nil {
		return nil, err
	}

	if notifications != nil {
		notifications.Modified(putReq.Key, se.VersionId, se.ModificationsCount)
	}

	version := &proto.Version{
		VersionId:          se.VersionId,
		ModificationsCount: se.ModificationsCount,
		CreatedTimestamp:   se.CreationTimestamp,
		ModifiedTimestamp:  se.ModificationTimestamp,
		SessionId:          se.SessionId,
		ClientIdentity:     se.ClientIdentity,
	}

	if d.log.Enabled(context.Background(), slog.LevelDebug) {
		d.log.Debug(
			"Applied put operation",
			slog.String("key", putReq.Key),
			slog.Any("version", version),
		)
	}

	pr := &proto.PutResponse{Version: version}
	if newKey != "" {
		// Return the address of a copy: the address of newKey would move it
		// to the heap on every put, while only the sequential ones return it
		key := newKey
		pr.Key = &key
	}
	return pr, nil
}

func (d *db) applyDelete(batch kvstore.WriteBatch, notifications *Notifications, delReq *proto.DeleteRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteResponse, error) {
	se, err := checkExpectedVersionId(batch, delReq.Key, delReq.ExpectedVersionId)
	if se != nil {
		defer se.ReturnToVTPool()
	}

	switch {
	case errors.Is(err, ErrBadVersionId):
		return &proto.DeleteResponse{Status: proto.Status_UNEXPECTED_VERSION_ID}, nil
	case err != nil:
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	case se == nil:
		return &proto.DeleteResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	default:
		err = updateOperationCallback.OnDeleteWithEntry(batch, notifications, delReq.Key, se, d)
		if err != nil {
			return nil, err
		}

		if err = batch.Delete(delReq.Key); err != nil {
			return &proto.DeleteResponse{}, err
		}

		if notifications != nil {
			notifications.Deleted(delReq.Key)
		}

		if d.log.Enabled(context.Background(), slog.LevelDebug) {
			d.log.Debug(
				"Applied delete operation",
				slog.String("key", delReq.Key),
			)
		}
		return &proto.DeleteResponse{Status: proto.Status_OK}, nil
	}
}

const DeleteRangeThreshold = 100

func (d *db) applyDeleteRange(batch kvstore.WriteBatch, notifications *Notifications, delReq *proto.DeleteRangeRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteRangeResponse, error) {
	// With the feature, a delete range covers either regular keys or internal
	// keys. One that covers both, e.g. from a regular key to an internal one,
	// gets the INVALID_ARGUMENT status, before the callbacks run and before its
	// notification.
	//
	// In a range over the internal keys, the notification records are deleted
	// without being read: they are not storage entries. Each replica trims them
	// on its own schedule, so the ones in the range differ between replicas:
	// the range is deleted with a range deletion, whatever its size, as
	// deleting the records one by one would make the batch, and the DB
	// checksum, differ between replicas.
	endExclusive, internalKeys, regularKeys := d.deleteRangeEnd(batch, delReq)
	if internalKeys && regularKeys {
		return &proto.DeleteRangeResponse{Status: proto.Status_INVALID_ARGUMENT}, nil
	}

	if notifications != nil {
		// The notification keeps the end of the request, even when it is
		// empty: the clients read that as all the records from the start on,
		// which is what the range deletes. The end of the scan, "__oxia/",
		// would read as a different range: the clients don't sort the internal
		// keys after all the others.
		notifications.DeletedRange(delReq.StartInclusive, delReq.EndExclusive)
	}

	it, err := batch.RangeScan(delReq.StartInclusive, endExclusive)
	if err != nil {
		return nil, err
	}
	var validKeys []string
	var validKeysNum = 0
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if internalKeys && strings.HasPrefix(key, notificationsPrefix+"/") {
			// No session or secondary index entry to clean up: the range
			// deletion below removes the record, which can't stay cached
			d.notificationsTracker.RecordsDeleted()
			continue
		}
		validKeysNum++
		if validKeysNum <= DeleteRangeThreshold {
			validKeys = append(validKeys, key)
		}
		value, err := it.Value()
		if err != nil {
			return nil, errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to get value on delete range")
		}
		se := proto.StorageEntryFromVTPool()
		if err = DeserializeMetadata(value, se); err != nil {
			se.ReturnToVTPool()
			return nil, errors.Wrap(deserializeFailure(key, err, it.Close()),
				"oxia db: failed to deserialize value on delete range")
		}
		if err = updateOperationCallback.OnDeleteWithEntry(batch, notifications, key, se, d); err != nil {
			se.ReturnToVTPool()
			return nil, errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to callback on delete range")
		}
		se.ReturnToVTPool()
	}
	if err := it.Close(); err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to close iterator on delete range")
	}
	if internalKeys || validKeysNum > DeleteRangeThreshold {
		err = batch.DeleteRange(delReq.StartInclusive, endExclusive)
	} else {
		err = deleteKeys(batch, validKeys)
	}
	if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to delete range")
	}

	if d.log.Enabled(context.Background(), slog.LevelDebug) {
		d.log.Debug(
			"Applied delete range operation",
			slog.String("key-start", delReq.StartInclusive),
			slog.String("key-end", endExclusive),
		)
	}
	return &proto.DeleteRangeResponse{Status: proto.Status_OK}, nil
}

// deleteRangeEnd returns the end of the keys that a delete range covers, and
// whether they include internal keys and regular keys, which it reports with
// the feature only.
func (d *db) deleteRangeEnd(batch kvstore.WriteBatch, delReq *proto.DeleteRangeRequest) (
	endExclusive string, internalKeys, regularKeys bool) {
	endExclusive = delReq.EndExclusive
	if !d.IsFeatureEnabled(proto.Feature_FEATURE_DELETE_RANGE_NOTIFICATION_RECORDS) {
		return endExclusive, false, false
	}
	if endExclusive == "" {
		// A range without an end stops before the internal keys, which sort at
		// or after their prefix with either key encoder. Every regular key
		// sorts before it, but for the natural keys that start with 0xff
		// bytes, which are not valid UTF-8.
		endExclusive = constant.InternalKeyPrefix
	}
	internalKeys, regularKeys = batch.RangeOverlaps(delReq.StartInclusive, endExclusive)
	return endExclusive, internalKeys, regularKeys
}

func deleteKeys(batch kvstore.WriteBatch, keys []string) error {
	for _, key := range keys {
		if err := batch.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func applyGet(kv kvstore.KV, getReq *proto.GetRequest) (*proto.GetResponse, error) {
	key, value, closer, err := kv.Get(getReq.Key, kvstore.ComparisonType(getReq.GetComparisonType()), kvstore.NoInternalKeys)

	if errors.Is(err, kvstore.ErrKeyNotFound) {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}

	var se *proto.StorageEntry
	var deserializeErr error
	if getReq.IncludeValue {
		// If we need to return the value we cannot pool the objects, because
		// the Value slice would be returned to pool
		se = &proto.StorageEntry{}
		deserializeErr = Deserialize(value, se)
	} else {
		// Metadata-only read: skip copying the value that would be dropped
		se = proto.StorageEntryFromVTPool()
		defer se.ReturnToVTPool()
		deserializeErr = DeserializeMetadata(value, se)
	}

	if err = multierr.Append(deserializeErr, closer.Close()); err != nil {
		return nil, err
	}

	res := &proto.GetResponse{
		Value: se.Value,
		Version: &proto.Version{
			VersionId:          se.VersionId,
			ModificationsCount: se.ModificationsCount,
			CreatedTimestamp:   se.CreationTimestamp,
			ModifiedTimestamp:  se.ModificationTimestamp,
			SessionId:          se.SessionId,
			ClientIdentity:     se.ClientIdentity,
		},
	}

	if getReq.ComparisonType != proto.KeyComparisonType_EQUAL {
		// Return the address of a copy: the address of key would move it to
		// the heap on every get, while the EQUAL ones don't return it
		foundKey := key
		res.Key = &foundKey
	}

	return res, nil
}

// GetStorageEntryMetadata reads the storage entry for key, skipping the value
// payload: the returned entry has Value nil and owns the rest of its fields.
// All the consumers of an existing entry (version checks, session shadows,
// secondary-index cleanup) only need the metadata, and copying the old value
// just to discard it costs a full memcpy per overwrite — and pins
// max-value-sized buffers in the entry pool.
func GetStorageEntryMetadata(batch kvstore.WriteBatch, key string) (*proto.StorageEntry, error) {
	value, closer, err := batch.Get(key)
	if err != nil {
		return nil, err
	}

	se := proto.StorageEntryFromVTPool()

	if err = DeserializeMetadata(value, se); err != nil {
		se.ReturnToVTPool()
		return nil, deserializeFailure(key, err, closer.Close())
	}
	if err = closer.Close(); err != nil {
		se.ReturnToVTPool()
		return nil, err
	}
	return se, nil
}

func checkExpectedVersionId(batch kvstore.WriteBatch, key string, expectedVersionId *int64) (*proto.StorageEntry, error) {
	se, err := GetStorageEntryMetadata(batch, key)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			if expectedVersionId == nil || *expectedVersionId == -1 {
				// OK, we were checking that the key was not there, and it's indeed not there
				return nil, nil //nolint:nilnil
			}

			return nil, ErrBadVersionId
		}
		return nil, err
	}

	if expectedVersionId != nil && se.VersionId != *expectedVersionId {
		se.ReturnToVTPool()
		return nil, ErrBadVersionId
	}

	return se, nil
}

// DeserializeMetadata fills se from buf without copying the value payload:
// the unmarshal aliases buf, then every field that must outlive buf is copied
// out and Value is dropped.
func DeserializeMetadata(buf []byte, se *proto.StorageEntry) error {
	if err := se.UnmarshalVTUnsafe(buf); err != nil {
		// The unmarshal can fail after the value: drop it too, as the pool
		// keeps the Value capacity
		se.Value = nil
		return errors.Wrap(err, "failed to Deserialize storage entry")
	}

	se.Value = nil
	if se.ClientIdentity != nil {
		ci := strings.Clone(*se.ClientIdentity)
		se.ClientIdentity = &ci
	}
	if se.PartitionKey != nil {
		pk := strings.Clone(*se.PartitionKey)
		se.PartitionKey = &pk
	}
	for _, si := range se.SecondaryIndexes {
		si.IndexName = strings.Clone(si.IndexName)
		si.SecondaryKey = strings.Clone(si.SecondaryKey)
	}
	return nil
}

func Deserialize(value []byte, se *proto.StorageEntry) error {
	if err := se.UnmarshalVT(value); err != nil {
		return errors.Wrap(err, "failed to Deserialize storage entry")
	}

	return nil
}

func (d *db) ReadNextNotifications(ctx context.Context, startOffset int64) ([]proto.EncodedNotificationBatch, error) {
	if !d.notificationsEnabled.Load() {
		return nil, ErrNotificationsDisabled
	}
	return d.notificationsTracker.ReadNextNotifications(ctx, startOffset)
}

func ToDbOption(opt *proto.NewTermOptions) TermOptions {
	to := TermOptions{NotificationsEnabled: true}
	if opt != nil {
		to.NotificationsEnabled = opt.EnableNotifications
		if len(opt.Features) > 0 {
			to.Features = slices.Clone(opt.Features)
			slices.Sort(to.Features)
		}
	}

	return to
}
