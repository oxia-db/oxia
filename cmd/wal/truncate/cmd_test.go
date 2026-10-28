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

package truncate

import (
	"math"
	"strconv"
	"testing"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

func TestExecTruncatesAfterLastSafeEntry(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t, 5)

	require.NoError(t, execute("--last-safe-entry", "2"))

	assert.EqualValues(t, 2, lastOffset(t))
}

func TestExecRejectsLastSafeEntryAfterLastEntry(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t, 5)

	assert.ErrorIs(t, execute("--last-safe-entry", "10"), errs.ErrOffsetOverflow)

	assert.EqualValues(t, 4, lastOffset(t))
}

func TestExecTruncatesLastEntry(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t, 5)

	require.NoError(t, execute("--truncate-last-entry"))

	assert.EqualValues(t, 3, lastOffset(t))
}

func TestExecWithoutOperationKeepsLog(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t, 5)

	require.NoError(t, execute())

	assert.EqualValues(t, 4, lastOffset(t))
}

func TestExecRejectsBothOperations(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")

	err := execute("--last-safe-entry", "2", "--truncate-last-entry")

	assert.ErrorContains(t, err, "were all set")
}

func TestExecPanicsOnInvalidNamespace(t *testing.T) {
	setWalOption(t, t.TempDir(), "../ns")

	assert.PanicsWithError(t, `namespace "../ns" contains invalid path traversal sequence`, func() {
		_ = execute()
	})
}

func setWalOption(t *testing.T, dir string, namespace string) {
	t.Helper()
	previous := common.WalOption
	common.WalOption = common.WalOptions{Namespace: namespace, Shard: 1, WalDir: dir}
	t.Cleanup(func() { common.WalOption = previous })
}

func newWal(t *testing.T) (wal.Factory, wal.Wal) {
	t.Helper()
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  common.WalOption.WalDir,
		Retention:   math.MaxInt64,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
	})
	w, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	require.NoError(t, err)
	return factory, w
}

func writeWal(t *testing.T, count int64) {
	t.Helper()
	factory, w := newWal(t)
	for i := int64(0); i < count; i++ {
		require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: i, Value: []byte(strconv.FormatInt(i, 10))}))
	}
	require.NoError(t, w.Close())
	require.NoError(t, factory.Close())
}

func lastOffset(t *testing.T) int64 {
	t.Helper()
	factory, w := newWal(t)
	offset := w.LastOffset()
	require.NoError(t, w.Close())
	require.NoError(t, factory.Close())
	return offset
}

func execute(args ...string) error {
	for _, name := range []string{"last-safe-entry", "truncate-last-entry"} {
		flag := Cmd.Flags().Lookup(name)
		_ = flag.Value.Set(flag.DefValue)
		flag.Changed = false
	}
	Cmd.SetArgs(args)
	return Cmd.Execute()
}
