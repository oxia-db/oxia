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

package main

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestLogLevelErrorError(t *testing.T) {
	require.EqualError(t, LogLevelError("verbose"), "unknown log level (verbose)")
}

func TestConfigureLogLevelAppliesLevel(t *testing.T) {
	restoreLogging(t)

	logLevelStr = "debug"
	require.NoError(t, configureLogLevel(nil, nil))
	require.Equal(t, slog.LevelDebug, logging.LogLevel)
}

func TestConfigureLogLevelRejectsUnknownLevel(t *testing.T) {
	restoreLogging(t)

	logLevelStr = "verbose"
	require.ErrorIs(t, configureLogLevel(nil, nil), LogLevelError("verbose"))
}

func TestHandleInitSkipsMessagesBeforeInit(t *testing.T) {
	previousNode, previousNodes := thisNode, allNodes
	t.Cleanup(func() { thisNode, allNodes = previousNode, previousNodes })
	lines := captureStdout(t)

	scanner := bufio.NewScanner(strings.NewReader(strings.Join([]string{
		`{"src":"c1","dest":"n2","body":{"type":"read","msg_id":1,"key":1}}`,
		`{"src":"c0","dest":"n2","body":{"type":"init","msg_id":2,"node_id":"n2","node_ids":["n1","n2","n3"]}}`,
	}, "\n")))
	require.NoError(t, handleInit(scanner))

	require.Equal(t, "n2", thisNode)
	require.Equal(t, []string{"n1", "n2", "n3"}, allNodes)
	msgType, msg, _ := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgType("init_ok"), msgType)
	initOk := msg.(*Message[BaseMessageBody])
	require.Equal(t, "n2", initOk.Src)
	require.Equal(t, "c0", initOk.Dest)
	require.Equal(t, int64(2), *initOk.Body.InReplyTo)
}

func TestRunCoordinatorRejectsTooFewDataServers(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	_, err := runCoordinator(d, memoryOptions(), []*proto.DataServerIdentity{{Public: "n2", Internal: "n2"}})
	require.ErrorContains(t, err, "failed to seed coordinator namespaces")
}

func TestRunCoordinatorRejectsDuplicateDataServers(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	name := "n2"
	_, err := runCoordinator(d, memoryOptions(), []*proto.DataServerIdentity{
		{Name: &name, Public: "n2", Internal: "n2"},
		{Name: &name, Public: "n2", Internal: "n2"},
	})
	require.ErrorContains(t, err, "failed to seed coordinator data servers")
}

func TestRunCoordinatorFailsOnUnknownMetadataProvider(t *testing.T) {
	d := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	options := memoryOptions()
	options.Metadata.ProviderName = "unknown"

	_, err := runCoordinator(d, options, dataServers("n2", "n3", "n4"))

	require.ErrorContains(t, err, "failed to create coordinator metadata factory")
}

func TestCoordinatorStartFailsOnMetadataError(t *testing.T) {
	errMetadata := errors.New("metadata failed")
	tests := []struct {
		name     string
		metadata func(*fakeMetadata)
		wantErr  string
	}{
		{"leadership", func(m *fakeMetadata) {
			m.waitToBecomeLeader = func() (<-chan struct{}, error) { return nil, errMetadata }
		}, "failed to wait for coordinator metadata leadership"},
		{"runtime", func(m *fakeMetadata) {
			m.listNamespaceStatus = func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
				return nil, errMetadata
			}
		}, "failed to create coordinator"},
		{"reconciler", func(m *fakeMetadata) {
			m.allocateShardIDs = func(uint32) (int64, error) { return 0, errMetadata }
		}, "failed to reconcile the cluster configuration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			captureStdout(t)
			ctx, cancel := context.WithCancel(t.Context())
			c := &coordinator{cancel: cancel}
			defer func() { require.NoError(t, c.Close()) }()
			metadata := newFakeMetadata(t)
			tt.metadata(metadata)

			err := c.start(ctx, newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider()),
				metadata, dataServers("n2", "n3", "n4"))

			require.ErrorIs(t, err, errMetadata)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestConfigureLoggerWritesJSON(t *testing.T) {
	restoreLogging(t)

	configureLogger()

	require.IsType(t, &slog.JSONHandler{}, slog.Default().Handler())
}

func TestRunFailsWithoutInit(t *testing.T) {
	restoreNodes(t)

	err := run(newScanner(`{"src":"c1","dest":"n2","body":{"type":"read","msg_id":1,"key":1}}`), t.TempDir())

	require.ErrorIs(t, err, errNoInit)
}

func TestRunServesDataServerUntilInputEnds(t *testing.T) {
	restoreNodes(t)
	lines := captureStdout(t)

	err := run(newScanner(
		initMessage("n2"),
		`{"src":"c1","dest":"n2","body":{"type":"read","msg_id":5,"key":1}}`,
	), t.TempDir())

	require.NoError(t, err)
	received := receiveLines(t, lines, 2)
	msgType, _, _ := parseLine(t, received[0])
	require.Equal(t, MsgType("init_ok"), msgType)
	msgType, msg, _ := parseLine(t, received[1])
	require.Equal(t, MsgTypeError, msgType)
	require.Equal(t, int64(5), *msg.(*Message[ErrorResponse]).Body.InReplyTo)
}

func TestRunServesCoordinatorUntilInputEnds(t *testing.T) {
	restoreNodes(t)
	lines := captureStdout(t)

	require.NoError(t, run(newScanner(initMessage("n1")), t.TempDir()))

	msgType, _, _ := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgType("init_ok"), msgType)
}

func TestRunFailsWhenCoordinatorCantStart(t *testing.T) {
	restoreNodes(t)
	captureStdout(t)

	err := run(newScanner(
		`{"src":"c0","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1","n2"]}}`,
	), t.TempDir())

	require.ErrorContains(t, err, "failed to start coordinator")
}

func TestRunFailsWhenDataServerCantStart(t *testing.T) {
	restoreNodes(t)
	captureStdout(t)
	dataDir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(dataDir, nil, 0o600))

	err := run(newScanner(initMessage("n2")), dataDir)

	require.ErrorContains(t, err, "failed to create dataserver manifest")
}

func TestRunFailsWhenDataServerWalDirIsDataDir(t *testing.T) {
	restoreNodes(t)
	captureStdout(t)
	dataDir := t.TempDir()
	nodeDir := filepath.Join(dataDir, "n2")
	require.NoError(t, os.MkdirAll(filepath.Join(nodeDir, "db"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join(nodeDir, "db"), filepath.Join(nodeDir, "wal")))

	err := run(newScanner(initMessage("n2")), dataDir)

	require.ErrorContains(t, err, "failed to start dataserver")
}

func TestRunFailsOnMalformedMessage(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
	}{
		{"before init", []string{`not json`, initMessage("n2")}},
		{"after init", []string{initMessage("n2"), `not json`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreNodes(t)
			captureStdout(t)

			err := run(newScanner(tt.lines...), t.TempDir())

			require.ErrorContains(t, err, `failed to unmarshal "not json"`)
		})
	}
}

func TestRunFailsOnInvalidResponseId(t *testing.T) {
	restoreNodes(t)
	captureStdout(t)

	err := run(newScanner(
		initMessage("n2"),
		`{"src":"n1","dest":"n2","body":{"type":"term-resp","in_reply_to":99,"m":{}}}`,
	), t.TempDir())

	require.ErrorContains(t, err, "invalid response id 99")
}

// restoreLogging restores the logging configuration that the test changes.
func restoreLogging(t *testing.T) {
	t.Helper()
	previousLevelStr, previousLevel, previousLogger := logLevelStr, logging.LogLevel, slog.Default()
	t.Cleanup(func() {
		logLevelStr, logging.LogLevel = previousLevelStr, previousLevel
		slog.SetDefault(previousLogger)
	})
}

// restoreNodes restores the node ids that the init message sets.
func restoreNodes(t *testing.T) {
	t.Helper()
	previousNode, previousNodes := thisNode, allNodes
	t.Cleanup(func() { thisNode, allNodes = previousNode, previousNodes })
}

func newScanner(lines ...string) *bufio.Scanner {
	return bufio.NewScanner(strings.NewReader(strings.Join(lines, "\n")))
}

func initMessage(node string) string {
	return `{"src":"c0","dest":"` + node + `","body":{"type":"init","msg_id":1,"node_id":"` + node +
		`","node_ids":["n1","n2","n3","n4"]}}`
}

func memoryOptions() *coordoption.Options {
	return &coordoption.Options{Metadata: coordoption.MetadataOptions{
		ProviderOptions: coordoption.ProviderOptions{ProviderName: metadatacommon.NameMemory},
	}}
}

func dataServers(nodes ...string) []*proto.DataServerIdentity {
	servers := make([]*proto.DataServerIdentity, 0, len(nodes))
	for _, node := range nodes {
		servers = append(servers, &proto.DataServerIdentity{Public: node, Internal: node})
	}
	return servers
}

// fakeMetadata is a memory metadata whose hooks, when set, replace its
// methods.
type fakeMetadata struct {
	coordmetadata.Metadata

	waitToBecomeLeader  func() (<-chan struct{}, error)
	listNamespaceStatus func() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error)
	allocateShardIDs    func(count uint32) (int64, error)
}

func newFakeMetadata(t *testing.T) *fakeMetadata {
	t.Helper()
	factory, err := coordmetadata.New(t.Context(), memoryOptions())
	require.NoError(t, err)
	metadata := factory.CreateMetadata(t.Context())
	t.Cleanup(func() {
		require.NoError(t, metadata.Close())
		require.NoError(t, factory.Close())
	})
	return &fakeMetadata{Metadata: metadata}
}

func (m *fakeMetadata) WaitToBecomeLeader() (<-chan struct{}, error) {
	if m.waitToBecomeLeader != nil {
		return m.waitToBecomeLeader()
	}
	return m.Metadata.WaitToBecomeLeader()
}

func (m *fakeMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	if m.listNamespaceStatus != nil {
		return m.listNamespaceStatus()
	}
	return m.Metadata.ListNamespaceStatus()
}

func (m *fakeMetadata) AllocateShardIDs(count uint32) (int64, error) {
	if m.allocateShardIDs != nil {
		return m.allocateShardIDs(count)
	}
	return m.Metadata.AllocateShardIDs(count)
}
