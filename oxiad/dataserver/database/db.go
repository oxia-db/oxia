// Copyright 2023-2025 The Oxia Authors
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
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	pb "google.golang.org/protobuf/proto"

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
	ErrNotificationsDisabled = errors.New("oxia: notifications disabled")
)

const (
	commitOffsetKey        = constant.InternalKeyPrefix + "commit-offset"
	commitLastVersionIdKey = constant.InternalKeyPrefix + "last-version-id"
	termKey                = constant.InternalKeyPrefix + "term"
	termOptionsKey         = termKey + "-options"
)

type UpdateOperationCallback interface {
	OnPut(batch kvstore.WriteBatch, notifications *Notifications, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error)
	OnDelete(batch kvstore.WriteBatch, notifications *Notifications, key string) error
	OnDeleteWithEntry(batch kvstore.WriteBatch, notifications *Notifications, key string, value *proto.StorageEntry) error
	OnDeleteRange(batch kvstore.WriteBatch, notifications *Notifications, keyStartInclusive string, keyEndExclusive string) error
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
}

type DB interface {
	io.Closer

	EnableNotifications(enable bool)

	ProcessWrite(b *proto.WriteRequest, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error)
	Get(request *proto.GetRequest) (*proto.GetResponse, error)
	List(request *proto.ListRequest) (kvstore.KeyIterator, error)
	RangeScan(request *proto.RangeScanRequest) (RangeScanIterator, error)
	KeyIterator(includeInternalKeys bool) (kvstore.KeyIterator, error)

	ReadCommitOffset() (int64, error)

	ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error)
	GetSequenceUpdates(prefixKey string) (SequenceWaiter, error)

	UpdateTerm(newTerm int64, options TermOptions) error
	ReadTerm() (term int64, options TermOptions, err error)

	Snapshot() (kvstore.Snapshot, error)

	// Delete and close the database and all its files
	Delete() error
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
		notificationsEnabled:  true,
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

	commitOffset, err := db.ReadCommitOffset()
	if err != nil {
		return nil, err
	}

	lastVersionId, err := db.readLastVersionId()
	if err != nil {
		return nil, err
	}
	db.committedVersionId.Store(lastVersionId)

	db.notificationsTracker = newNotificationsTracker(namespace, shardId, commitOffset, kv, notificationRetentionTime, clock)
	return db, nil
}

type db struct {
	kv                    kvstore.KV
	shardId               int64
	committedVersionId    atomic.Int64
	notificationsTracker  *notificationsTracker
	log                   *slog.Logger
	notificationsEnabled  bool
	sequenceWaiterTracker SequenceWaiterTracker

	putCounter                metric.Counter
	deleteCounter             metric.Counter
	deleteRangesCounter       metric.Counter
	getCounter                metric.Counter
	getSequenceUpdatesCounter metric.Counter
	listCounter               metric.Counter
	rangeScanCounter          metric.Counter

	batchWriteLatencyHisto metric.LatencyHistogram
	getLatencyHisto        metric.LatencyHistogram
	listLatencyHisto       metric.LatencyHistogram
}

func (d *db) Snapshot() (kvstore.Snapshot, error) {
	return d.kv.Snapshot()
}

func (d *db) EnableNotifications(enabled bool) {
	d.notificationsEnabled = enabled
}

func (d *db) Close() error {
	return multierr.Combine(
		d.sequenceWaiterTracker.Close(),
		d.notificationsTracker.Close(),
		d.kv.Close(),
	)
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

func (d *db) applyWriteRequest(b *proto.WriteRequest, batch kvstore.WriteBatch,
	baseVersionId *atomic.Int64, commitOffset int64, timestamp uint64,
	updateOperationCallback UpdateOperationCallback) (*Notifications, *proto.WriteResponse, error) {
	res := &proto.WriteResponse{}
	var notifications *Notifications
	if d.notificationsEnabled {
		notifications = newNotifications(d.shardId, commitOffset, timestamp)
	}

	d.putCounter.Add(len(b.Puts))
	for _, putReq := range b.Puts {
		pr, err := d.applyPut(batch, baseVersionId, notifications, putReq, timestamp, updateOperationCallback, false)
		if err != nil {
			return nil, nil, err
		}
		res.Puts = append(res.Puts, pr)
	}

	d.deleteCounter.Add(len(b.Deletes))
	for _, delReq := range b.Deletes {
		dr, err := d.applyDelete(batch, notifications, delReq, updateOperationCallback)
		if err != nil {
			return nil, nil, err
		}

		res.Deletes = append(res.Deletes, dr)
	}

	d.deleteRangesCounter.Add(len(b.DeleteRanges))
	for _, delRangeReq := range b.DeleteRanges {
		dr, err := d.applyDeleteRange(batch, notifications, delRangeReq, updateOperationCallback)
		if err != nil {
			return nil, nil, err
		}

		res.DeleteRanges = append(res.DeleteRanges, dr)
	}

	return notifications, res, nil
}

func (d *db) ProcessWrite(b *proto.WriteRequest, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error) {
	timer := d.batchWriteLatencyHisto.Timer()
	defer timer.Done()

	baseVersionId := &atomic.Int64{}
	baseVersionId.Store(d.committedVersionId.Load())

	batch := d.kv.NewWriteBatch()
	notifications, res, err := d.applyWriteRequest(b, batch, baseVersionId, commitOffset, timestamp, updateOperationCallback)
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

	if err := batch.Commit(); err != nil {
		return nil, err
	}
	// update the db local cache of version_id after commit success
	d.committedVersionId.Store(uncommitedVersionId)

	if notifications != nil {
		d.notificationsTracker.UpdatedCommitOffset(commitOffset)
	}

	if err := batch.Close(); err != nil {
		return nil, err
	}

	return res, nil
}

func (*db) addNotifications(batch kvstore.WriteBatch, notifications *Notifications) error {
	value, err := notifications.batch.MarshalVT()
	if err != nil {
		return err
	}

	return batch.Put(notificationKey(notifications.batch.Offset), value)
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

	_ = it.Close()
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

	// Notifications are not using the stats headers so they would
	// fail to deserialize. We just provide the content without the
	// version object
	if strings.HasPrefix(it.Key(), notificationsPrefix) {
		se.Value = value
	} else if err = Deserialize(value, se); err != nil {
		return nil, err
	}

	res := &proto.GetResponse{
		Key:    pb.String(it.Key()),
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

func (d *db) KeyIterator(includeInternalKeys bool) (kvstore.KeyIterator, error) {
	return d.kv.KeyIterator(kvstore.IteratorOpts{IncludeInternalKeys: includeInternalKeys})
}

func (d *db) ReadCommitOffset() (int64, error) {
	return d.readASCIILong(commitOffsetKey)
}

func (d *db) readLastVersionId() (int64, error) {
	return d.readASCIILong(commitLastVersionIdKey)
}

func (d *db) readASCIILong(key string) (int64, error) {
	kv := d.kv

	getReq := &proto.GetRequest{
		Key:          key,
		IncludeValue: true,
	}
	gr, err := applyGet(kv, getReq)
	if err != nil {
		return wal.InvalidOffset, err
	}
	if gr.Status == proto.Status_KEY_NOT_FOUND {
		return wal.InvalidOffset, nil
	}

	var res int64
	if _, err = fmt.Sscanf(string(gr.Value), "%d", &res); err != nil {
		return wal.InvalidOffset, err
	}
	return res, nil
}

func (d *db) UpdateTerm(newTerm int64, options TermOptions) error {
	batch := d.kv.NewWriteBatch()

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

	if err := batch.Close(); err != nil {
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
	var se *proto.StorageEntry
	var err error
	var newKey string
	if len(putReq.GetSequenceKeyDelta()) > 0 {
		prefixKey := putReq.Key
		newKey, err = generateUniqueKeyFromSequences(batch, putReq)
		putReq.Key = newKey
		d.sequenceWaiterTracker.SequenceUpdated(prefixKey, newKey)
	} else if !internal {
		se, err = checkExpectedVersionId(batch, putReq.Key, putReq.ExpectedVersionId)
	}

	switch {
	case errors.Is(err, ErrBadVersionId):
		return &proto.PutResponse{
			Status: proto.Status_UNEXPECTED_VERSION_ID,
		}, nil
	case err != nil:
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}

	// No version conflict
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

	defer se.ReturnToVTPool()

	ser, err := se.MarshalVT()
	if err != nil {
		return nil, err
	}

	if err = batch.Put(putReq.Key, ser); err != nil {
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

	d.log.Debug(
		"Applied put operation",
		slog.String("key", putReq.Key),
		slog.Any("version", version),
	)

	pr := &proto.PutResponse{Version: version}
	if newKey != "" {
		pr.Key = &newKey
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
		err = updateOperationCallback.OnDelete(batch, notifications, delReq.Key)
		if err != nil {
			return nil, err
		}

		if err = batch.Delete(delReq.Key); err != nil {
			return &proto.DeleteResponse{}, err
		}

		if notifications != nil {
			notifications.Deleted(delReq.Key)
		}

		d.log.Debug(
			"Applied delete operation",
			slog.String("key", delReq.Key),
		)
		return &proto.DeleteResponse{Status: proto.Status_OK}, nil
	}
}

const DeleteRangeThreshold = 100

func (d *db) applyDeleteRange(batch kvstore.WriteBatch, notifications *Notifications, delReq *proto.DeleteRangeRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteRangeResponse, error) {
	if notifications != nil {
		notifications.DeletedRange(delReq.StartInclusive, delReq.EndExclusive)
	}

	it, err := batch.RangeScan(delReq.StartInclusive, delReq.EndExclusive)
	if err != nil {
		return nil, err
	}
	var validKeys []string
	var validKeysNum = 0
	for ; it.Valid(); it.Next() {
		validKeysNum++
		key := it.Key()
		if validKeysNum <= DeleteRangeThreshold {
			validKeys = append(validKeys, key)
		}
		value, err := it.Value()
		if err != nil {
			return nil, errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to get value on delete range")
		}
		se := proto.StorageEntryFromVTPool()
		if err = Deserialize(value, se); err != nil {
			se.ReturnToVTPool()
			return nil, err
		}
		if err = updateOperationCallback.OnDeleteWithEntry(batch, notifications, key, se); err != nil {
			se.ReturnToVTPool()
			return nil, errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to callback on delete range")
		}
		se.ReturnToVTPool()
	}
	if err := it.Close(); err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to close iterator on delete range")
	}
	if validKeysNum > DeleteRangeThreshold {
		if err := batch.DeleteRange(delReq.StartInclusive, delReq.EndExclusive); err != nil {
			return nil, errors.Wrap(err, "oxia db: failed to delete range")
		}
	} else {
		for _, key := range validKeys {
			if err := batch.Delete(key); err != nil {
				return nil, errors.Wrap(err, "oxia db: failed to delete range")
			}
		}
	}

	d.log.Debug(
		"Applied delete range operation",
		slog.String("key-start", delReq.StartInclusive),
		slog.String("key-end", delReq.EndExclusive),
	)
	return &proto.DeleteRangeResponse{Status: proto.Status_OK}, nil
}

func applyGet(kv kvstore.KV, getReq *proto.GetRequest) (*proto.GetResponse, error) {
	key, value, closer, err := kv.Get(getReq.Key, kvstore.ComparisonType(getReq.GetComparisonType()), kvstore.NoInternalKeys)

	if errors.Is(err, kvstore.ErrKeyNotFound) {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}

	var se *proto.StorageEntry
	if getReq.IncludeValue {
		// If we need to return the value we cannot pool the objects, because
		// the Value slice would be returned to pool
		se = &proto.StorageEntry{}
	} else {
		se = proto.StorageEntryFromVTPool()
		defer se.ReturnToVTPool()
	}

	if err = multierr.Append(
		Deserialize(value, se),
		closer.Close(),
	); err != nil {
		return nil, err
	}

	resValue := se.Value
	if !getReq.IncludeValue {
		resValue = nil
	}

	res := &proto.GetResponse{
		Value: resValue,
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
		res.Key = &key
	}

	return res, nil
}

func GetStorageEntry(batch kvstore.WriteBatch, key string) (*proto.StorageEntry, error) {
	value, closer, err := batch.Get(key)
	if err != nil {
		return nil, err
	}

	se := proto.StorageEntryFromVTPool()

	if err = multierr.Append(
		Deserialize(value, se),
		closer.Close(),
	); err != nil {
		se.ReturnToVTPool()
		return nil, err
	}
	return se, nil
}

func checkExpectedVersionId(batch kvstore.WriteBatch, key string, expectedVersionId *int64) (*proto.StorageEntry, error) {
	se, err := GetStorageEntry(batch, key)
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

func Deserialize(value []byte, se *proto.StorageEntry) error {
	if err := se.UnmarshalVT(value); err != nil {
		return errors.Wrap(err, "failed to Deserialize storage entry")
	}

	return nil
}

func (d *db) ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error) {
	if !d.notificationsEnabled {
		return nil, ErrNotificationsDisabled
	}
	return d.notificationsTracker.ReadNextNotifications(ctx, startOffset)
}

type noopCallback struct{}

func (*noopCallback) OnDeleteWithEntry(kvstore.WriteBatch, *Notifications, string, *proto.StorageEntry) error {
	return nil
}

func (*noopCallback) OnPut(_ kvstore.WriteBatch, _ *Notifications, _ *proto.PutRequest, _ *proto.StorageEntry) (proto.Status, error) {
	return proto.Status_OK, nil
}

func (*noopCallback) OnDelete(_ kvstore.WriteBatch, _ *Notifications, _ string) error {
	return nil
}

func (*noopCallback) OnDeleteRange(_ kvstore.WriteBatch, _ *Notifications, _ string, _ string) error {
	return nil
}

var NoOpCallback UpdateOperationCallback = &noopCallback{}

func ToDbOption(opt *proto.NewTermOptions) TermOptions {
	to := TermOptions{NotificationsEnabled: true}
	if opt != nil {
		to.NotificationsEnabled = opt.EnableNotifications
	}

	return to
}
