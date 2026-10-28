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

package scan

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/wal/common"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/dataserver/wal"
)

func TestExecPrintsOperationsAsLogs(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t, &proto.LogEntry{Term: 1, Offset: 0, Timestamp: 1000, Value: writeRequestsValue(t, &proto.WriteRequest{
		Puts: []*proto.PutRequest{
			{Key: "a", Value: []byte("1")},
			{Key: "b", Value: []byte("2"), ExpectedVersionId: ptr(int64(3)), ClientIdentity: ptr("me"),
				SessionId: ptr(int64(7))},
		},
		Deletes: []*proto.DeleteRequest{
			{Key: "c"},
			{Key: "d", ExpectedVersionId: ptr(int64(4))},
		},
		DeleteRanges: []*proto.DeleteRangeRequest{{StartInclusive: "e", EndExclusive: "f"}},
	})})

	logs := captureLogs(t)
	stdout, err := runScan(t)

	require.NoError(t, err)
	assert.Empty(t, stdout)
	records := decodeLogs(t, logs)
	require.Len(t, records, 5)
	assert.Equal(t, map[string]any{"op": "put", "key": "a", "offset": 0.0, "term": 1.0}, pick(records[0]))
	assert.Equal(t, 3.0, records[1]["expected-version-id"])
	assert.Equal(t, "me", records[1]["client-identity"])
	assert.Equal(t, 7.0, records[1]["session-id"])
	assert.Equal(t, map[string]any{"op": "delete", "key": "c", "offset": 0.0, "term": 1.0}, pick(records[2]))
	assert.Equal(t, map[string]any{"op": "delete", "key": "d", "offset": 0.0, "term": 1.0}, pick(records[3]))
	assert.Equal(t, 4.0, records[3]["expected-version-id"])
	assert.Equal(t, "delete-range", records[4]["op"])
	assert.Equal(t, "e", records[4]["key-start"])
	assert.Equal(t, "f", records[4]["key-end"])
}

func TestExecPrintsEntriesAsJSON(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t,
		&proto.LogEntry{Term: 2, Offset: 0, Timestamp: 1000, Value: writeRequestsValue(t, &proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: "a", Value: []byte("1")}},
		})},
		&proto.LogEntry{Term: 2, Offset: 1, Timestamp: 2000, Value: writeRequestsValue(t, &proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: "b", Value: []byte("2")}},
		})},
	)

	stdout, err := runScan(t, "--json")

	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	require.Len(t, lines, 2)
	for i, line := range lines {
		var entry LogEntry
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		assert.EqualValues(t, 2, entry.Term)
		assert.EqualValues(t, i, entry.Offset)
		assert.EqualValues(t, (i+1)*1000, entry.Timestamp.UnixMilli())
		require.Len(t, entry.WriteRequests.GetWrites(), 1)
	}
}

func TestExecReadsOffsetRange(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	var entries []*proto.LogEntry
	for i := int64(0); i < 5; i++ {
		entries = append(entries, &proto.LogEntry{Term: 1, Offset: i, Value: writeRequestsValue(t, &proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: "k"}},
		})})
	}
	writeWal(t, entries...)

	stdout, err := runScan(t, "--json", "--offset-first", "1", "--offset-last", "4")

	require.NoError(t, err)
	var offsets []int64
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		var entry LogEntry
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		offsets = append(offsets, entry.Offset)
	}
	assert.Equal(t, []int64{2, 3}, offsets)
}

func TestExecRejectsInvalidNamespace(t *testing.T) {
	setWalOption(t, t.TempDir(), "../ns")

	_, err := runScan(t)

	assert.ErrorContains(t, err, "invalid path traversal sequence")
}

func TestExecRejectsOffsetBeforeFirstEntry(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t, &proto.LogEntry{Term: 1, Offset: 5, Value: writeRequestsValue(t, &proto.WriteRequest{})})

	_, err := runScan(t, "--offset-first", "0")

	assert.ErrorIs(t, err, errs.ErrNotFound)
}

func TestExecRejectsUndecodableEntry(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t, &proto.LogEntry{Term: 1, Offset: 0, Value: []byte{0xff}})

	_, err := runScan(t)

	assert.Error(t, err)
}

func TestExecRejectsCorruptedEntry(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	// A small segment size rolls the first entries into a read-only segment, whose entries in the middle are only
	// validated when they are read
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  common.WalOption.WalDir,
		Retention:   math.MaxInt64,
		SegmentSize: 256,
	})
	w, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	require.NoError(t, err)
	for offset := int64(0); offset < 8; offset++ {
		require.NoError(t, w.Append(&proto.LogEntry{Term: 1, Offset: offset, Value: writeRequestsValue(t,
			&proto.WriteRequest{Puts: []*proto.PutRequest{{Key: "key", Value: []byte("value")}}})}))
	}
	require.NoError(t, w.Close())
	require.NoError(t, factory.Close())
	segment, err := os.OpenFile(filepath.Join(common.WalOption.WalDir, "default", "shard-1", "0.txnx"), os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = segment.WriteAt([]byte{0xaa, 0xbb}, 120)
	require.NoError(t, err)
	require.NoError(t, segment.Close())

	logs := captureLogs(t)
	_, err = runScan(t)

	assert.ErrorIs(t, err, errs.ErrDataCorrupted)
	// The entries before the corrupted one were printed
	assert.NotEmpty(t, decodeLogs(t, logs))
}

func TestExecReturnsStdoutError(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	writeWal(t, &proto.LogEntry{Term: 1, Offset: 0, Value: writeRequestsValue(t, &proto.WriteRequest{})})
	path := filepath.Join(t.TempDir(), "stdout")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	readOnly, err := os.Open(path)
	require.NoError(t, err)
	stdout := os.Stdout
	os.Stdout = readOnly
	t.Cleanup(func() {
		os.Stdout = stdout
		_ = readOnly.Close()
	})

	err = execute("--json")

	assert.Error(t, err)
}

func TestExecRejectsTimestampOutOfJSONRange(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	timestamp := time.Date(10001, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	writeWal(t, &proto.LogEntry{Term: 1, Offset: 0, Timestamp: uint64(timestamp),
		Value: writeRequestsValue(t, &proto.WriteRequest{})})

	stdout, err := runScan(t, "--json")

	assert.ErrorContains(t, err, "year outside of range")
	assert.Empty(t, stdout)
}

func TestExecSkipsControlRequests(t *testing.T) {
	setWalOption(t, t.TempDir(), "default")
	controlRequest, err := (&proto.LogEntryValue{Value: &proto.LogEntryValue_ControlRequest{
		ControlRequest: &proto.ControlRequest{Value: &proto.ControlRequest_FeatureEnable{
			FeatureEnable: &proto.FeatureEnableRequest{},
		}},
	}}).MarshalVT()
	require.NoError(t, err)
	writeWal(t,
		&proto.LogEntry{Term: 1, Offset: 0, Value: controlRequest},
		&proto.LogEntry{Term: 1, Offset: 1, Value: writeRequestsValue(t, &proto.WriteRequest{
			Puts: []*proto.PutRequest{{Key: "key", Value: []byte("value")}},
		})},
	)

	logs := captureLogs(t)
	_, err = runScan(t)

	require.NoError(t, err)
	records := decodeLogs(t, logs)
	require.Len(t, records, 1)
	assert.Equal(t, map[string]any{"op": "put", "key": "key", "offset": 1.0, "term": 1.0}, pick(records[0]))
}

func setWalOption(t *testing.T, dir string, namespace string) {
	t.Helper()
	previous := common.WalOption
	common.WalOption = common.WalOptions{Namespace: namespace, Shard: 1, WalDir: dir}
	t.Cleanup(func() { common.WalOption = previous })
}

func writeWal(t *testing.T, entries ...*proto.LogEntry) {
	t.Helper()
	factory := wal.NewWalFactory(&wal.FactoryOptions{
		BaseWalDir:  common.WalOption.WalDir,
		Retention:   math.MaxInt64,
		SegmentSize: wal.DefaultFactoryOptions.SegmentSize,
	})
	w, err := factory.NewWal(common.WalOption.Namespace, common.WalOption.Shard, nil)
	require.NoError(t, err)
	for _, entry := range entries {
		require.NoError(t, w.Append(entry))
	}
	require.NoError(t, w.Close())
	require.NoError(t, factory.Close())
}

func writeRequestsValue(t *testing.T, write *proto.WriteRequest) []byte {
	t.Helper()
	value, err := (&proto.LogEntryValue{Value: &proto.LogEntryValue_Requests{
		Requests: &proto.WriteRequests{Writes: []*proto.WriteRequest{write}},
	}}).MarshalVT()
	require.NoError(t, err)
	return value
}

func execute(args ...string) error {
	Cmd.SetArgs(append([]string{"--json=false", "--offset-first=-1", "--offset-last=-1"}, args...))
	return Cmd.Execute()
}

// runScan runs the command and returns what it printed on stdout.
func runScan(t *testing.T, args ...string) (string, error) {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	stdout := os.Stdout
	os.Stdout = writer
	output := make(chan string)
	go func() {
		data, _ := io.ReadAll(reader)
		output <- string(data)
	}()

	err = execute(args...)

	os.Stdout = stdout
	require.NoError(t, writer.Close())
	result := <-output
	require.NoError(t, reader.Close())
	return result, err
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buffer := new(bytes.Buffer)
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buffer, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buffer
}

func decodeLogs(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		record := map[string]any{}
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if _, ok := record["op"]; ok {
			records = append(records, record)
		}
	}
	return records
}

func pick(record map[string]any) map[string]any {
	picked := map[string]any{}
	for _, key := range []string{"op", "key", "offset", "term"} {
		picked[key] = record[key]
	}
	return picked
}

func ptr[T any](value T) *T {
	return &value
}
