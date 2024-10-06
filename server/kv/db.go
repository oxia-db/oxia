// Copyright 2023 StreamNative, Inc.
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

package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	pb "google.golang.org/protobuf/proto"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/metrics"
	"github.com/streamnative/oxia/proto"
	"github.com/streamnative/oxia/server/wal"
)

var (
	ErrBadVersionId          = errors.New("oxia: bad version id")
	ErrMissingPartitionKey   = errors.New("oxia: sequential key operation requires partition key")
	ErrMissingSequenceDeltas = errors.New("oxia: sequential key operation missing some sequence deltas")
	ErrSequenceDeltaIsZero   = errors.New("oxia: sequential key operation requires first delta do be > 0")
	ErrNotificationsDisabled = errors.New("oxia: notifications disabled")
)

const (
	commitOffsetKey        = common.InternalKeyPrefix + "commit-offset"
	commitLastVersionIdKey = common.InternalKeyPrefix + "last-version-id"
	termKey                = common.InternalKeyPrefix + "term"
	termOptionsKey         = termKey + "-options"
)

type UpdateOperationCallback interface {
	OnPut(batch WriteBatch, req *proto.PutRequest, se *proto.StorageEntry) (proto.Status, error)
	OnDelete(batch WriteBatch, key string) error
	OnDeleteRange(batch WriteBatch, keyStartInclusive string, keyEndExclusive string) error
}

type RangeScanIterator interface {
	io.Closer

	Valid() bool
	Value() (*proto.GetResponse, error)
	Next() bool
}

type TermOptions struct {
	NotificationsEnabled bool
}

type DB interface {
	io.Closer

	EnableNotifications(enable bool)

	ProcessWrite(b *proto.WriteRequest, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*proto.WriteResponse, error)
	Get(request *proto.GetRequest) (*proto.GetResponse, error)
	List(request *proto.ListRequest) (KeyIterator, error)
	RangeScan(request *proto.RangeScanRequest) (RangeScanIterator, error)
	ReadCommitOffset() (int64, error)

	ReadNextNotifications(ctx context.Context, startOffset int64) ([]*proto.NotificationBatch, error)

	UpdateTerm(newTerm int64, options TermOptions) error
	ReadTerm() (term int64, options TermOptions, err error)

	Snapshot() (Snapshot, error)

	// Delete and close the database and all its files
	Delete() error
}

func NewDB(namespace string, shardId int64, factory Factory, notificationRetentionTime time.Duration, clock common.Clock) (DB, error) {
	kv, err := factory.NewKV(namespace, shardId)
	if err != nil {
		return nil, err
	}

	labels := metrics.LabelsForShard(namespace, shardId)
	db := &db{
		kv:                   kv,
		shardId:              shardId,
		notificationsEnabled: true,
		log: slog.With(
			slog.String("component", "db"),
			slog.String("namespace", namespace),
			slog.Int64("shard", shardId),
		),

		batchWriteLatencyHisto: metrics.NewLatencyHistogram("oxia_server_db_batch_write_latency",
			"The time it takes to write a batch in the db", labels),
		getLatencyHisto: metrics.NewLatencyHistogram("oxia_server_db_get_latency",
			"The time it takes to get from the db", labels),
		listLatencyHisto: metrics.NewLatencyHistogram("oxia_server_db_list_latency",
			"The time it takes to read a list from the db", labels),
		putCounter: metrics.NewCounter("oxia_server_db_puts",
			"The total number of put operations", "count", labels),
		deleteCounter: metrics.NewCounter("oxia_server_db_deletes",
			"The total number of delete operations", "count", labels),
		deleteRangesCounter: metrics.NewCounter("oxia_server_db_delete_ranges",
			"The total number of delete ranges operations", "count", labels),
		getCounter: metrics.NewCounter("oxia_server_db_gets",
			"The total number of get operations", "count", labels),
		listCounter: metrics.NewCounter("oxia_server_db_lists",
			"The total number of list operations", "count", labels),
		rangeScanCounter: metrics.NewCounter("oxia_server_db_range_scans",
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
	db.versionIdTracker.Store(lastVersionId)

	db.notificationsTracker = newNotificationsTracker(namespace, shardId, commitOffset, kv, notificationRetentionTime, clock)
	return db, nil
}

type db struct {
	kv                   KV
	shardId              int64
	versionIdTracker     atomic.Int64
	notificationsTracker *notificationsTracker
	log                  *slog.Logger
	notificationsEnabled bool

	putCounter          metrics.Counter
	deleteCounter       metrics.Counter
	deleteRangesCounter metrics.Counter
	getCounter          metrics.Counter
	listCounter         metrics.Counter
	rangeScanCounter    metrics.Counter

	batchWriteLatencyHisto metrics.LatencyHistogram
	getLatencyHisto        metrics.LatencyHistogram
	listLatencyHisto       metrics.LatencyHistogram
}

func (d *db) Snapshot() (Snapshot, error) {
	return d.kv.Snapshot()
}

func (d *db) EnableNotifications(enabled bool) {
	d.notificationsEnabled = enabled
}

func (d *db) Close() error {
	return multierr.Combine(
		d.notificationsTracker.Close(),
		d.kv.Close(),
	)
}

func (d *db) Delete() error {
	return multierr.Combine(
		d.notificationsTracker.Close(),
		d.kv.Delete(),
	)
}

func now() uint64 {
	return uint64(time.Now().UnixMilli())
}

func (d *db) applyWriteRequest(b *proto.WriteRequest, batch WriteBatch, commitOffset int64, timestamp uint64, updateOperationCallback UpdateOperationCallback) (*notifications, *proto.WriteResponse, error) {
	res := &proto.WriteResponse{}
	var notifications *notifications
	if d.notificationsEnabled {
		notifications = newNotifications(d.shardId, commitOffset, timestamp)
	}

	d.putCounter.Add(len(b.Puts))
	for _, putReq := range b.Puts {
		pr, err := d.applyPut(batch, notifications, putReq, timestamp, updateOperationCallback, false)
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

	batch := d.kv.NewWriteBatch()
	notifications, res, err := d.applyWriteRequest(b, batch, commitOffset, timestamp, updateOperationCallback)
	if err != nil {
		return nil, err
	}

	if err := d.addASCIILong(commitOffsetKey, commitOffset, batch, timestamp); err != nil {
		return nil, err
	}

	if err := d.addASCIILong(commitLastVersionIdKey, d.versionIdTracker.Load(), batch, timestamp); err != nil {
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

	if notifications != nil {
		d.notificationsTracker.UpdatedCommitOffset(commitOffset)
	}

	if err := batch.Close(); err != nil {
		return nil, err
	}

	return res, nil
}

func (*db) addNotifications(batch WriteBatch, notifications *notifications) error {
	value, err := notifications.batch.MarshalVT()
	if err != nil {
		return err
	}

	return batch.Put(notificationKey(notifications.batch.Offset), value)
}

func (d *db) addASCIILong(key string, value int64, batch WriteBatch, timestamp uint64) error {
	asciiValue := []byte(fmt.Sprintf("%d", value))
	_, err := d.applyPut(batch, nil, &proto.PutRequest{
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

type listIterator struct {
	KeyIterator
	timer metrics.Timer
}

func (it *listIterator) Close() error {
	it.timer.Done()
	return it.KeyIterator.Close()
}

func (d *db) List(request *proto.ListRequest) (KeyIterator, error) {
	d.listCounter.Add(1)

	it, err := d.kv.KeyRangeScan(request.StartInclusive, request.EndExclusive)
	if err != nil {
		return nil, err
	}

	return &listIterator{
		KeyIterator: it,
		timer:       d.listLatencyHisto.Timer(),
	}, nil
}

type rangeScanIterator struct {
	KeyValueIterator
	timer metrics.Timer
}

func (it *rangeScanIterator) Value() (*proto.GetResponse, error) {
	value, err := it.KeyValueIterator.Value()
	if err != nil {
		return nil, err
	}

	se := &proto.StorageEntry{}
	if err = Deserialize(value, se); err != nil {
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

	it, err := d.kv.RangeScan(request.StartInclusive, request.EndExclusive)
	if err != nil {
		return nil, err
	}

	return &rangeScanIterator{
		KeyValueIterator: it,
		timer:            d.listLatencyHisto.Timer(),
	}, nil
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

	if _, err := d.applyPut(batch, nil, &proto.PutRequest{
		Key:   termKey,
		Value: []byte(fmt.Sprintf("%d", newTerm)),
	}, now(), NoOpCallback, true); err != nil {
		return err
	}

	serOptions, err := json.Marshal(options)
	if err != nil {
		return err
	}
	if _, err := d.applyPut(batch, nil, &proto.PutRequest{
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

func (d *db) applyPut(batch WriteBatch, notifications *notifications, putReq *proto.PutRequest, timestamp uint64, updateOperationCallback UpdateOperationCallback, internal bool) (*proto.PutResponse, error) { //nolint:revive
	var se *proto.StorageEntry
	var err error
	var newKey string
	if len(putReq.GetSequenceKeyDelta()) > 0 {
		newKey, err = generateUniqueKeyFromSequences(batch, putReq)
		putReq.Key = newKey
	} else {
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
	status, err := updateOperationCallback.OnPut(batch, putReq, se)
	if err != nil {
		return nil, err
	}
	if status != proto.Status_OK {
		return &proto.PutResponse{
			Status: status,
		}, nil
	}

	var versionId int64
	if internal {
		versionId = wal.InvalidOffset
	} else {
		versionId = d.versionIdTracker.Add(1)
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

func (d *db) applyDelete(batch WriteBatch, notifications *notifications, delReq *proto.DeleteRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteResponse, error) {
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
		err = updateOperationCallback.OnDelete(batch, delReq.Key)
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

func (*db) applyDeleteRangeNotifications(batch WriteBatch, notifications *notifications, delReq *proto.DeleteRangeRequest, updateOperationCallback UpdateOperationCallback) error {
	if notifications != nil {
		notifications.DeletedRange(delReq.StartInclusive, delReq.EndExclusive)
	}

	if updateOperationCallback != NoOpCallback {
		it, err := batch.KeyRangeScan(delReq.StartInclusive, delReq.EndExclusive)
		if err != nil {
			return err
		}

		for it.Next() {
			err := updateOperationCallback.OnDelete(batch, it.Key())
			if err != nil {
				return errors.Wrap(multierr.Combine(err, it.Close()), "oxia db: failed to delete range")
			}
		}

		if err := it.Close(); err != nil {
			return errors.Wrap(err, "oxia db: failed to delete range")
		}
	}

	return nil
}

func (d *db) applyDeleteRange(batch WriteBatch, notifications *notifications, delReq *proto.DeleteRangeRequest, updateOperationCallback UpdateOperationCallback) (*proto.DeleteRangeResponse, error) {
	if err := d.applyDeleteRangeNotifications(batch, notifications, delReq, updateOperationCallback); err != nil {
		return nil, err
	}

	if err := updateOperationCallback.OnDeleteRange(batch, delReq.StartInclusive, delReq.EndExclusive); err != nil {
		return nil, err
	}

	if err := batch.DeleteRange(delReq.StartInclusive, delReq.EndExclusive); err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to delete range")
	}

	d.log.Debug(
		"Applied delete range operation",
		slog.String("key-start", delReq.StartInclusive),
		slog.String("key-end", delReq.EndExclusive),
	)
	return &proto.DeleteRangeResponse{Status: proto.Status_OK}, nil
}

func applyGet(kv KV, getReq *proto.GetRequest) (*proto.GetResponse, error) {
	key, value, closer, err := kv.Get(getReq.Key, ComparisonType(getReq.GetComparisonType()))

	if errors.Is(err, ErrKeyNotFound) {
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

func GetStorageEntry(batch WriteBatch, key string) (*proto.StorageEntry, error) {
	value, closer, err := batch.Get(key)
	if err != nil {
		return nil, err
	}

	se := proto.StorageEntryFromVTPool()

	if err = multierr.Append(
		Deserialize(value, se),
		closer.Close(),
	); err != nil {
		return nil, err
	}
	return se, nil
}

func checkExpectedVersionId(batch WriteBatch, key string, expectedVersionId *int64) (*proto.StorageEntry, error) {
	se, err := GetStorageEntry(batch, key)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
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

func (*noopCallback) OnPut(_ WriteBatch, _ *proto.PutRequest, _ *proto.StorageEntry) (proto.Status, error) {
	return proto.Status_OK, nil
}

func (*noopCallback) OnDelete(_ WriteBatch, _ string) error {
	return nil
}

func (*noopCallback) OnDeleteRange(_ WriteBatch, _ string, _ string) error {
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
