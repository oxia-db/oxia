package kv

import (
	"github.com/pkg/errors"
	"io"
	"oxia/proto"
	"time"

	pb "google.golang.org/protobuf/proto"
)

var (
	ErrorBadVersion = errors.New("oxia: bad version")
)

type DB interface {
	io.Closer

	ProcessWrite(b *proto.WriteRequest) (*proto.WriteResponse, error)
	ProcessRead(b *proto.ReadRequest) (*proto.ReadResponse, error)
}

func NewDB(shardId uint32, factory KVFactory) (DB, error) {
	kv, err := factory.NewKV(shardId)
	if err != nil {
		return nil, err
	}

	return &db{
		kv: kv,
	}, nil
}

type db struct {
	kv KV
}

func (d *db) Close() error {
	return d.kv.Close()
}

func (d *db) ProcessWrite(b *proto.WriteRequest) (*proto.WriteResponse, error) {
	res := &proto.WriteResponse{}

	batch := d.kv.NewWriteBatch()

	for _, putReq := range b.Puts {
		if pr, err := applyPut(batch, putReq); err != nil {
			return nil, err
		} else {
			res.Puts = append(res.Puts, pr)
		}
	}

	for _, delReq := range b.Deletes {
		if dr, err := applyDelete(batch, delReq); err != nil {
			return nil, err
		} else {
			res.Deletes = append(res.Deletes, dr)
		}
	}

	for _, delRangeReq := range b.DeleteRanges {
		if dr, err := applyDeleteRange(batch, delRangeReq); err != nil {
			return nil, err
		} else {
			res.DeleteRanges = append(res.DeleteRanges, dr)
		}
	}

	if err := batch.Commit(); err != nil {
		return nil, err
	}

	if err := batch.Close(); err != nil {
		return nil, err
	}

	return res, nil
}

func (d *db) ProcessRead(b *proto.ReadRequest) (*proto.ReadResponse, error) {
	res := &proto.ReadResponse{}

	//TODO Implement a ReadBatch to guarantee read consistency
	kv := d.kv

	for _, getReq := range b.Gets {
		if gr, err := applyGet(kv, getReq); err != nil {
			return nil, err
		} else {
			res.Gets = append(res.Gets, gr)
		}
	}

	for _, getRangeReq := range b.GetRanges {
		if gr, err := applyGetRange(kv, getRangeReq); err != nil {
			return nil, err
		} else {
			res.GetRanges = append(res.GetRanges, gr)
		}
	}

	return res, nil
}

func applyPut(batch WriteBatch, putReq *proto.PutRequest) (*proto.PutResponse, error) {
	se, err := checkExpectedVersion(batch, putReq.Key, putReq.ExpectedVersion)
	if errors.Is(err, ErrorBadVersion) {
		return &proto.PutResponse{
			Status: proto.Status_BAD_VERSION,
		}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	} else {
		now := uint64(time.Now().UnixMilli())

		// No version conflict
		if se == nil {
			se = &proto.StorageEntry{
				Version:               0,
				Payload:               putReq.Payload,
				CreationTimestamp:     now,
				ModificationTimestamp: now,
			}
		} else {
			se.Version += 1
			se.Payload = putReq.Payload
			se.ModificationTimestamp = now
		}

		ser, err := pb.Marshal(se)
		if err != nil {
			return nil, err
		}

		if err = batch.Put(putReq.Key, ser); err != nil {
			return nil, err
		}

		return &proto.PutResponse{
			Stat: &proto.Stat{
				Version:           se.Version,
				CreatedTimestamp:  se.CreationTimestamp,
				ModifiedTimestamp: se.ModificationTimestamp,
			},
		}, nil
	}
}

func applyDelete(batch WriteBatch, delReq *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	se, err := checkExpectedVersion(batch, delReq.Key, delReq.ExpectedVersion)

	if errors.Is(err, ErrorBadVersion) {
		return &proto.DeleteResponse{Status: proto.Status_BAD_VERSION}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	} else if se == nil {
		return &proto.DeleteResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	} else {
		if err = batch.Delete(delReq.Key); err != nil {
			return &proto.DeleteResponse{}, err
		}
		return &proto.DeleteResponse{Status: proto.Status_OK}, nil
	}
}

func applyDeleteRange(batch WriteBatch, delReq *proto.DeleteRangeRequest) (*proto.DeleteRangeResponse, error) {
	if err := batch.DeleteRange(delReq.StartInclusive, delReq.EndExclusive); err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to delete range")
	}

	return &proto.DeleteRangeResponse{Status: proto.Status_OK}, nil
}

func applyGet(kv KV, getReq *proto.GetRequest) (*proto.GetResponse, error) {
	payload, closer, err := kv.Get(getReq.Key)
	if errors.Is(err, ErrorKeyNotFound) {
		return &proto.GetResponse{Status: proto.Status_KEY_NOT_FOUND}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "oxia db: failed to apply batch")
	}

	se, err := deserialize(payload, closer)
	if err != nil {
		return nil, err
	}

	resPayload := se.Payload
	if !getReq.IncludePayload {
		resPayload = nil
	}

	return &proto.GetResponse{
		Payload: resPayload,
		Stat: &proto.Stat{
			Version:           se.Version,
			CreatedTimestamp:  se.CreationTimestamp,
			ModifiedTimestamp: se.ModificationTimestamp,
		},
	}, nil
}

func applyGetRange(kv KV, getRangeReq *proto.GetRangeRequest) (*proto.GetRangeResponse, error) {
	it := kv.KeyRangeScan(getRangeReq.StartInclusive, getRangeReq.EndExclusive)

	res := &proto.GetRangeResponse{}

	for ; it.Valid(); it.Next() {
		res.Keys = append(res.Keys, it.Key())
	}

	if err := it.Close(); err != nil {
		return nil, err
	}

	return res, nil
}

func checkExpectedVersion(batch WriteBatch, key string, expectedVersion *int64) (*proto.StorageEntry, error) {
	payload, closer, err := batch.Get(key)
	if err != nil {
		if errors.Is(err, ErrorKeyNotFound) {
			if expectedVersion == nil || *expectedVersion == -1 {
				// OK, we were checking that the key was not there, and it's indeed not there
				return nil, nil
			} else {
				return nil, ErrorBadVersion
			}
		}
		return nil, err
	}

	se, err := deserialize(payload, closer)
	if err != nil {
		return nil, err
	}

	if expectedVersion != nil && se.Version != *expectedVersion {
		return nil, ErrorBadVersion
	}

	return se, nil
}

func deserialize(payload []byte, closer io.Closer) (*proto.StorageEntry, error) {
	se := &proto.StorageEntry{}
	if err := pb.Unmarshal(payload, se); err != nil {
		return nil, errors.Wrap(err, "failed to deserialize storage entry")
	}

	if err := closer.Close(); err != nil {
		return nil, err
	}
	return se, nil
}
