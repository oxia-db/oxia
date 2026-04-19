package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

const (
	logKeyFormat = "log-%020d"
)

type PebbleRaftStore struct {
	factory kvstore.Factory
	kv      kvstore.KV
}

func NewPebbleRaftStore(path string) (store *PebbleRaftStore, err error) {
	store = &PebbleRaftStore{}
	if store.factory, err = kvstore.NewPebbleKVFactory(&kvstore.FactoryOptions{
		DataDir:     path,
		CacheSizeMB: 1,
		UseWAL:      true,
		SyncData:    true,
	}); err != nil {
		return nil, err
	}

	if store.kv, err = store.factory.NewKV("raft", 0, proto.KeySortingType_NATURAL); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *PebbleRaftStore) Close() error {
	return multierr.Combine(
		s.kv.Close(),
		s.factory.Close(),
	)
}

var (
	logKeyMin = fmt.Sprintf(logKeyFormat, 0)
	logKeyMax = fmt.Sprintf(logKeyFormat, uint64(math.MaxUint64))
)

func (s *PebbleRaftStore) FirstIndex() (uint64, error) {
	storedKey, _, closer, err := s.kv.Get(logKeyMin, kvstore.ComparisonCeiling, kvstore.NoInternalKeys)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	if err = closer.Close(); err != nil {
		return 0, err
	}

	var idx uint64
	_, _ = fmt.Sscanf(storedKey, logKeyFormat, &idx)
	return idx, err
}

func (s *PebbleRaftStore) LastIndex() (idx uint64, err error) {
	storedKey, _, closer, err := s.kv.Get(logKeyMax, kvstore.ComparisonFloor, kvstore.NoInternalKeys)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	_, _ = fmt.Sscanf(storedKey, logKeyFormat, &idx)
	return idx, closer.Close()
}

func (s *PebbleRaftStore) GetLog(index uint64, log *hashicorpraft.Log) error {
	key := fmt.Sprintf(logKeyFormat, index)
	_, value, closer, err := s.kv.Get(key, kvstore.ComparisonEqual, kvstore.NoInternalKeys)
	if err != nil {
		return err
	}

	return multierr.Combine(
		json.Unmarshal(value, log),
		closer.Close(),
	)
}

func (s *PebbleRaftStore) StoreLog(log *hashicorpraft.Log) error {
	return s.StoreLogs([]*hashicorpraft.Log{log})
}

func (s *PebbleRaftStore) StoreLogs(logs []*hashicorpraft.Log) error {
	wb := s.kv.NewWriteBatch()
	for _, log := range logs {
		key := fmt.Sprintf(logKeyFormat, log.Index)

		value, err := json.Marshal(log)
		if err != nil {
			return err
		}

		if err := wb.Put(key, value); err != nil {
			return err
		}
	}

	return multierr.Combine(
		wb.Commit(),
		wb.Close(),
	)
}

func (s *PebbleRaftStore) DeleteRange(minInclusive, maxInclusive uint64) error {
	minKeyInclusive := fmt.Sprintf(logKeyFormat, minInclusive)
	maxKeyExclusive := fmt.Sprintf(logKeyFormat, maxInclusive+1)
	wb := s.kv.NewWriteBatch()

	return multierr.Combine(
		wb.DeleteRange(minKeyInclusive, maxKeyExclusive),
		wb.Commit(),
		wb.Close(),
	)
}

func (s *PebbleRaftStore) Set(key []byte, value []byte) error {
	wb := s.kv.NewWriteBatch()

	return multierr.Combine(
		wb.Put(string(key), value),
		wb.Commit(),
		wb.Close(),
	)
}

func (s *PebbleRaftStore) Get(key []byte) ([]byte, error) {
	_, value, closer, err := s.kv.Get(string(key), kvstore.ComparisonEqual, kvstore.NoInternalKeys)
	if err != nil {
		if errors.Is(err, kvstore.ErrKeyNotFound) {
			return []byte{}, nil
		}
		return nil, err
	}

	copiedValue := make([]byte, len(value))
	copy(copiedValue, value)
	return copiedValue, closer.Close()
}

func (s *PebbleRaftStore) SetUint64(key []byte, n uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, n)
	return s.Set(key, value)
}

func (s *PebbleRaftStore) GetUint64(key []byte) (uint64, error) {
	value, err := s.Get(key)
	if err != nil || len(value) == 0 {
		return 0, err
	}

	return binary.BigEndian.Uint64(value), nil
}
