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

package metadata

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/hashicorp/raft"
	"github.com/oxia-db/oxia/common/compare"
	"github.com/oxia-db/oxia/server/kv"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

type kvRaftStoreInterface interface {
	raft.StableStore
	raft.LogStore
}

type kvRaftStore struct {
	factory kv.Factory
	kv      kv.KV
}

func newKVRaftStore(path string) (store *kvRaftStore, err error) {
	store = &kvRaftStore{}
	if store.factory, err = kv.NewPebbleKVFactory(&kv.FactoryOptions{
		DataDir:     path,
		CacheSizeMB: 1,
		InMemory:    false,
	}); err != nil {
		return nil, err
	}

	if store.kv, err = store.factory.NewKV("raft", 0, compare.EncoderNatural); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *kvRaftStore) Close() error {
	return multierr.Combine(
		s.kv.Close(),
		s.factory.Close(),
	)
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// LogStore methods

const logKeyFormat = "log-%020d"

var (
	logKeyMin = fmt.Sprintf(logKeyFormat, 0)
	logKeyMax = fmt.Sprintf(logKeyFormat, uint64(math.MaxUint64))
)

func (s *kvRaftStore) FirstIndex() (uint64, error) {
	storedKey, _, closer, err := s.kv.Get(logKeyMin, kv.ComparisonCeiling)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	if err = closer.Close(); err != nil {
		return 0, err
	}

	var idx uint64
	_, err = fmt.Sscanf(storedKey, logKeyFormat, &idx)
	return idx, err
}

func (s *kvRaftStore) LastIndex() (idx uint64, err error) {
	storedKey, _, closer, err := s.kv.Get(logKeyMax, kv.ComparisonFloor)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	_, err = fmt.Sscanf(storedKey, logKeyFormat, &idx)

	err = multierr.Combine(err, closer.Close())
	return idx, err
}

func (s *kvRaftStore) GetLog(index uint64, log *raft.Log) error {
	key := fmt.Sprintf(logKeyFormat, index)
	_, value, closer, err := s.kv.Get(key, kv.ComparisonEqual)
	if err != nil {
		return err
	}

	return multierr.Combine(
		json.Unmarshal(value, log),
		closer.Close(),
	)
}

func (s *kvRaftStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

func (s *kvRaftStore) StoreLogs(logs []*raft.Log) error {
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
		s.kv.Flush(),
	)
}

func (s *kvRaftStore) DeleteRange(minInclusive, maxInclusive uint64) error {
	minKeyInclusive := fmt.Sprintf(logKeyFormat, minInclusive)
	maxKeyExclusive := fmt.Sprintf(logKeyFormat, maxInclusive+1)
	wb := s.kv.NewWriteBatch()

	return multierr.Combine(
		wb.DeleteRange(minKeyInclusive, maxKeyExclusive),
		wb.Commit(),
		wb.Close(),
		s.kv.Flush(),
	)
}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// StableStore methods

const stableKeyFormat = "store-%s"

func (s *kvRaftStore) Set(key []byte, value []byte) error {
	stableKey := fmt.Sprintf(stableKeyFormat, key)
	wb := s.kv.NewWriteBatch()

	return multierr.Combine(
		wb.Put(stableKey, value),
		wb.Commit(),
		wb.Close(),
		s.kv.Flush(),
	)
}

func (s *kvRaftStore) Get(key []byte) ([]byte, error) {
	stableKey := fmt.Sprintf(stableKeyFormat, key)
	_, value, closer, err := s.kv.Get(stableKey, kv.ComparisonEqual)
	if err != nil {
		if errors.Is(err, kv.ErrKeyNotFound) {
			return []byte{}, nil
		}
		return nil, err
	}

	copiedValue := make([]byte, len(value))
	copy(copiedValue, value)
	return copiedValue, closer.Close()
}

func (s *kvRaftStore) SetUint64(key []byte, n uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, n)
	return s.Set(key, value)
}

func (s *kvRaftStore) GetUint64(key []byte) (uint64, error) {
	value, err := s.Get(key)
	if err != nil || len(value) == 0 {
		return 0, err
	}

	return binary.BigEndian.Uint64(value), nil
}
