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

package metadata

import (
	"errors"
	"io"
	"math"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/crc"
	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"
)

func TestKVRaftStoreGetLogMissingReturnsErrLogNotFound(t *testing.T) {
	store, err := newKVRaftStore(filepath.Join(t.TempDir(), "raft-store"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	err = store.GetLog(1, &raft.Log{})
	assert.ErrorIs(t, err, raft.ErrLogNotFound)
}

func TestKVRaftStoreDeleteRangeHandlesMaxUint64(t *testing.T) {
	store, err := newKVRaftStore(filepath.Join(t.TempDir(), "raft-store"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	require.NoError(t, store.StoreLogs([]*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogCommand, Data: []byte("one")},
		{Index: 2, Term: 1, Type: raft.LogCommand, Data: []byte("two")},
	}))

	require.NoError(t, store.DeleteRange(1, math.MaxUint64))

	assert.ErrorIs(t, store.GetLog(1, &raft.Log{}), raft.ErrLogNotFound)
	assert.ErrorIs(t, store.GetLog(2, &raft.Log{}), raft.ErrLogNotFound)
}

func TestKVRaftStoreStoreLogsReturnsCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	wb := &fakeWriteBatch{closeErr: closeErr}
	store := &kvRaftStore{kv: &fakeKV{wb: wb}}

	err := store.StoreLog(&raft.Log{Index: 1, Term: 1})

	assert.ErrorIs(t, err, closeErr)
	assert.True(t, wb.closed)
}

type fakeFactory struct {
	closed bool
}

func (f *fakeFactory) Close() error {
	f.closed = true
	return nil
}

func (*fakeFactory) NewSnapshotLoader(string, int64) (kvstore.SnapshotLoader, error) {
	return nil, nil
}

func (*fakeFactory) NewKV(string, int64, proto.KeySortingType) (kvstore.KV, error) {
	return nil, errors.New("new kv failed")
}

func TestKVRaftStoreClosesFactoryWhenNewKVFails(t *testing.T) {
	factory := &fakeFactory{}

	_, err := newKVRaftStoreWithFactory(t.TempDir(), func(*kvstore.FactoryOptions) (kvstore.Factory, error) {
		return factory, nil
	})

	require.EqualError(t, err, "new kv failed")
	assert.True(t, factory.closed)
}

type fakeKV struct {
	wb kvstore.WriteBatch
}

func (f *fakeKV) NewWriteBatch() kvstore.WriteBatch {
	return f.wb
}

func (*fakeKV) Close() error {
	return nil
}

func (*fakeKV) Get(string, kvstore.ComparisonType, kvstore.IteratorOpts) (string, []byte, io.Closer, error) {
	panic("not implemented")
}

func (*fakeKV) KeyRangeScan(string, string, kvstore.IteratorOpts) (kvstore.KeyIterator, error) {
	panic("not implemented")
}

func (*fakeKV) KeyRangeScanReverse(string, string, kvstore.IteratorOpts) (kvstore.ReverseKeyIterator, error) {
	panic("not implemented")
}

func (*fakeKV) KeyIterator(kvstore.IteratorOpts) (kvstore.KeyIterator, error) {
	panic("not implemented")
}

func (*fakeKV) RangeScan(string, string, kvstore.IteratorOpts) (kvstore.KeyValueIterator, error) {
	panic("not implemented")
}

func (*fakeKV) Snapshot() (kvstore.Snapshot, error) {
	panic("not implemented")
}

func (*fakeKV) Flush() error {
	panic("not implemented")
}

func (*fakeKV) Delete() error {
	panic("not implemented")
}

type fakeWriteBatch struct {
	closeErr error
	closed   bool
}

func (*fakeWriteBatch) Put(string, []byte) error {
	return nil
}

func (*fakeWriteBatch) Delete(string) error {
	panic("not implemented")
}

func (*fakeWriteBatch) Get(string) ([]byte, io.Closer, error) {
	panic("not implemented")
}

func (*fakeWriteBatch) FindLower(string) (string, error) {
	panic("not implemented")
}

func (*fakeWriteBatch) DeleteRange(string, string) error {
	panic("not implemented")
}

func (*fakeWriteBatch) KeyRangeScan(string, string) (kvstore.KeyIterator, error) {
	panic("not implemented")
}

func (*fakeWriteBatch) RangeScan(string, string) (kvstore.KeyValueIterator, error) {
	panic("not implemented")
}

func (*fakeWriteBatch) Count() int {
	return 0
}

func (*fakeWriteBatch) Size() int {
	return 0
}

func (*fakeWriteBatch) Commit() error {
	return nil
}

func (*fakeWriteBatch) Checksum(init crc.Checksum) crc.Checksum {
	return init
}

func (f *fakeWriteBatch) Close() error {
	f.closed = true
	return f.closeErr
}
