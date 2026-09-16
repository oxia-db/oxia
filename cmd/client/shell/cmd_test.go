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

package shell

import (
	"bytes"
	"context"
	"strings"
	"testing"

	prompt "github.com/c-bata/go-prompt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	admincommons "github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

func runShell(t *testing.T, input string) (stdout string, stderr string, err error) {
	t.Helper()

	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	Cmd.SetIn(bytes.NewBufferString(input))
	Cmd.SetOut(out)
	Cmd.SetErr(errOut)
	Cmd.SetArgs(nil)

	err = Cmd.Execute()
	Config.Reset()
	return out.String(), errOut.String(), err
}

func TestShellExecutesClientCommands(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })

	var emptyPutOptions []oxia.PutOption
	var emptyGetOptions []oxia.GetOption
	var emptyDeleteOptions []oxia.DeleteOption
	var emptyDeleteRangeOptions []oxia.DeleteRangeOption
	var emptyListOptions []oxia.ListOption
	var emptyRangeScanOptions []oxia.RangeScanOption

	rangeResults := make(chan oxia.GetResult, 2)
	rangeResults <- oxia.GetResult{Key: "a", Value: []byte("value-a")}
	rangeResults <- oxia.GetResult{Key: "b", Value: []byte("value-b")}
	close(rangeResults)

	common.MockedClient.On("Put", "k", []byte("hello world"), emptyPutOptions).Return("k", oxia.Version{}, nil)
	common.MockedClient.On("Get", "k", emptyGetOptions).Return("k", []byte("hello world"), oxia.Version{}, nil)
	common.MockedClient.On("Delete", "k", emptyDeleteOptions).Return(nil)
	common.MockedClient.On("DeleteRange", "a", "z", emptyDeleteRangeOptions).Return(nil)
	common.MockedClient.On("List", "a", "z", emptyListOptions).Return([]string{"a", "b"}, nil)
	common.MockedClient.On("RangeScan", "a", "z", emptyRangeScanOptions).Return(rangeResults)
	common.MockedClient.On("Close").Return(nil)

	stdout, stderr, err := runShell(t, strings.Join([]string{
		`put k "hello world"`,
		`get k`,
		`delete k`,
		`delete-range a z`,
		`list a z`,
		`range-scan a z`,
		`exit`,
	}, "\n"))

	require.NoError(t, err)
	assert.Empty(t, stderr)
	assert.Contains(t, stdout, `"key":"k"`)
	assert.Contains(t, stdout, "hello world\n")
	assert.Contains(t, stdout, "OK\n")
	assert.Contains(t, stdout, "a\nb\n")
	assert.Contains(t, stdout, "a\tvalue-a\nb\tvalue-b\n")
	common.MockedClient.AssertExpectations(t)
}

func TestBufferedShellReturnsCommandError(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })

	stdout, stderr, err := runShell(t, "unknown\nget k\n")

	require.EqualError(t, err, `unknown client command "unknown"`)
	assert.Empty(t, stdout)
	assert.Equal(t, "Error: unknown client command \"unknown\"\n", stderr)
	common.MockedClient.AssertExpectations(t)
}

func TestShellRejectsClientPrefix(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })

	stdout, stderr, err := runShell(t, "client get k\n")

	require.EqualError(t, err, `unknown client command "client"`)
	assert.Empty(t, stdout)
	assert.Equal(t, "Error: unknown client command \"client\"\n", stderr)
	common.MockedClient.AssertExpectations(t)
}

func TestInteractiveShellContinuesAfterCommandError(t *testing.T) {
	mockClient := common.NewMockClient()

	var emptyGetOptions []oxia.GetOption
	mockClient.On("Get", "k", emptyGetOptions).Return("k", []byte("value"), oxia.Version{}, nil)

	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	session := &repl{
		ctx:         context.Background(),
		client:      mockClient,
		out:         out,
		errOut:      errOut,
		interactive: true,
	}

	require.NoError(t, session.executeLine("unknown"))
	require.NoError(t, session.executeLine("get k"))
	assert.Equal(t, "value\n", out.String())
	assert.Equal(t, "Error: unknown client command \"unknown\"\n", errOut.String())
	mockClient.AssertExpectations(t)
}

func TestInteractiveShellContinuesAfterParseError(t *testing.T) {
	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	session := &repl{
		ctx:         context.Background(),
		out:         out,
		errOut:      errOut,
		interactive: true,
	}

	require.NoError(t, session.executeLine(`put key "unterminated`))
	assert.Empty(t, out.String())
	assert.Equal(t, "Error: unterminated quoted string\n", errOut.String())
}

func TestShellExecutesClientCommandsWithOptions(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })

	getOptions := []oxia.GetOption{
		oxia.PartitionKey("pk"),
		oxia.UseIndex("idx"),
		oxia.ComparisonFloor(),
	}
	putOptions := []oxia.PutOption{
		oxia.ExpectedRecordNotExists(),
		oxia.Ephemeral(),
		oxia.PartitionKey("pk"),
		oxia.SecondaryIndex("idx", "secondary"),
	}
	deleteOptions := []oxia.DeleteOption{
		oxia.ExpectedVersionId(2),
		oxia.PartitionKey("pk"),
	}
	deleteRangeOptions := []oxia.DeleteRangeOption{
		oxia.PartitionKey("pk"),
	}
	listOptions := []oxia.ListOption{
		oxia.PartitionKey("pk"),
		oxia.UseIndex("idx"),
		oxia.ShowInternalKeys(true),
	}
	rangeScanOptions := []oxia.RangeScanOption{
		oxia.PartitionKey("pk"),
		oxia.UseIndex("idx"),
		oxia.ShowInternalKeys(true),
	}

	rangeResults := make(chan oxia.GetResult, 1)
	rangeResults <- oxia.GetResult{Key: "a", Value: []byte("value-a")}
	close(rangeResults)

	common.MockedClient.On("Get", "k", getOptions).Return("stored-k", []byte("value"), oxia.Version{}, nil)
	common.MockedClient.On("Put", "k", []byte("value with spaces"), putOptions).Return("k", oxia.Version{}, nil)
	common.MockedClient.On("Delete", "k", deleteOptions).Return(nil)
	common.MockedClient.On("DeleteRange", "a", "z", deleteRangeOptions).Return(nil)
	common.MockedClient.On("List", "a", "z", listOptions).Return([]string{"a"}, nil)
	common.MockedClient.On("RangeScan", "a", "z", rangeScanOptions).Return(rangeResults)
	common.MockedClient.On("Close").Return(nil)

	stdout, stderr, err := runShell(t, strings.Join([]string{
		`get --include-version --partition-key pk --index idx --comparison-type floor k`,
		`put --create-only --ephemeral --partition-key pk --index idx=secondary k value with spaces`,
		`delete --expected-version 2 --partition-key pk k`,
		`delete-range --key-min a --key-max z --partition-key pk`,
		`list --key-min a --key-max z --partition-key pk --index idx --internal-keys`,
		`range-scan -s a -e z --include-version --partition-key pk --index idx --internal-keys`,
		`exit`,
	}, "\n"))

	require.NoError(t, err)
	assert.Empty(t, stderr)
	assert.Contains(t, stdout, "value\n---\n")
	assert.Contains(t, stdout, `"key":"k"`)
	assert.Contains(t, stdout, "OK\n")
	assert.Contains(t, stdout, "a\n")
	assert.Contains(t, stdout, "a\tvalue-a\n---\n")
	common.MockedClient.AssertExpectations(t)
}

func TestShellExecutesAdminCommands(t *testing.T) {
	admincommons.MockedAdminClient = admincommons.NewMockAdminClient()
	t.Cleanup(func() {
		admincommons.MockedAdminClient = nil
	})

	admincommons.MockedAdminClient.On("ListNamespaces").Return([]*proto.NamespaceView{
		namespaceView("ns-1", 2, 3),
	}, nil)
	admincommons.MockedAdminClient.On("GetNamespace", "ns-1").Return(namespaceView("ns-1", 2, 3), nil)
	admincommons.MockedAdminClient.On("CreateNamespace", mock.MatchedBy(func(namespace *proto.Namespace) bool {
		return namespace.GetName() == "ns-2" &&
			namespace.GetInitialShardCount() == 4 &&
			namespace.GetReplicationFactor() == 3 &&
			namespace.GetNotificationsEnabled() &&
			namespace.GetKeySorting() == "natural"
	})).Return(namespaceConfig("ns-2", 4, 3), nil)
	admincommons.MockedAdminClient.On("PatchNamespace", mock.MatchedBy(func(namespace *proto.Namespace) bool {
		return namespace.GetName() == "ns-2" &&
			namespace.GetReplicationFactor() == 5 &&
			namespace.NotificationsEnabled == nil
	})).Return(namespaceConfig("ns-2", 4, 5), nil)
	admincommons.MockedAdminClient.On("DeleteNamespace", "ns-2").Return(namespaceConfig("ns-2", 4, 5), nil)

	admincommons.MockedAdminClient.On("ListDataServers").Return([]*proto.DataServerView{
		dataServerView("server-1", "server-1:6648", "server-1:6649"),
	}, nil)
	admincommons.MockedAdminClient.On("GetDataServer", "server-1").Return(dataServerView("server-1", "server-1:6648", "server-1:6649"), nil)
	admincommons.MockedAdminClient.On("CreateDataServer", mock.MatchedBy(func(dataServer *proto.DataServer) bool {
		return dataServer.GetNameOrDefault() == "server-2" &&
			dataServer.GetIdentity().GetPublic() == "server-2:6648" &&
			dataServer.GetIdentity().GetInternal() == "server-2:6649" &&
			dataServer.GetMetadata().GetLabels()["zone"] == "a"
	})).Return(dataServerConfig("server-2", "server-2:6648", "server-2:6649", map[string]string{"zone": "a"}), nil)
	admincommons.MockedAdminClient.On("PatchDataServer", mock.MatchedBy(func(dataServer *proto.DataServer) bool {
		return dataServer.GetNameOrDefault() == "server-2" &&
			dataServer.GetIdentity().GetPublic() == "server-2-new:6648" &&
			dataServer.GetIdentity().GetInternal() == "" &&
			dataServer.GetMetadata() == nil
	})).Return(dataServerConfig("server-2", "server-2-new:6648", "server-2:6649", nil), nil)
	admincommons.MockedAdminClient.On("DeleteDataServer", "server-2").Return(dataServerConfig("server-2", "server-2-new:6648", "server-2:6649", nil), nil)

	splitPoint := uint32(42)
	admincommons.MockedAdminClient.On("SplitShard", "ns-1", int64(7), &splitPoint).Return(&oxia.SplitShardResult{
		LeftChildShardId:  8,
		RightChildShardId: 9,
	})
	admincommons.MockedAdminClient.On("Close").Return(nil)

	stdout, stderr, err := runShell(t, strings.Join([]string{
		`admin namespace list`,
		`admin namespace get ns-1`,
		`admin namespace create --initial-shards 4 --replication-factor 3 --key-sorting natural ns-2`,
		`admin namespace patch --replication-factor 5 ns-2`,
		`admin namespace delete ns-2`,
		`admin dataserver list`,
		`admin dataserver get server-1`,
		`admin dataserver create --public server-2:6648 --internal server-2:6649 --label zone=a server-2`,
		`admin dataserver patch --public server-2-new:6648 server-2`,
		`admin dataserver delete server-2`,
		`admin shard split --namespace ns-1 --shard 7 --split-point 42`,
		`exit`,
	}, "\n"))

	require.NoError(t, err)
	assert.Empty(t, stderr)
	assert.Contains(t, stdout, "ns-1")
	assert.Contains(t, stdout, "server-1")
	assert.Contains(t, stdout, `"LeftChildShardId":8`)
	admincommons.MockedAdminClient.AssertExpectations(t)
}

func TestRootShellConfiguresClientAndAdminFlags(t *testing.T) {
	oldCommonConfig := common.Config
	oldConfig := Config
	t.Cleanup(func() {
		common.Config = oldCommonConfig
		Config = oldConfig
		common.MockedClient = nil
	})

	out := new(bytes.Buffer)
	errOut := new(bytes.Buffer)
	Cmd.SetIn(bytes.NewBufferString("exit\n"))
	Cmd.SetOut(out)
	Cmd.SetErr(errOut)
	Cmd.SetArgs([]string{
		"--service-address", "client.example:6648",
		"--namespace", "tenant-a",
		"--auth-token", "client-token",
		"--admin-address", "admin.example:6651",
		"--admin-auth-token", "admin-token",
		"--prompt", "",
	})

	err := Cmd.Execute()

	require.NoError(t, err)
	assert.Empty(t, out.String())
	assert.Empty(t, errOut.String())
	assert.Equal(t, "client.example:6648", common.Config.ServiceAddr)
	assert.Equal(t, "tenant-a", common.Config.Namespace)
	assert.Equal(t, "client-token", common.Config.Auth.Token)
	assert.Equal(t, "admin.example:6651", Config.adminAddress)
	assert.Equal(t, "admin-token", adminClientConfig().Auth.Token)
}

func TestAdminClientConfigUsesClientAuthAsDefault(t *testing.T) {
	Config.Reset()
	common.Config.Auth.Token = "shared-token"
	t.Cleanup(func() {
		Config.Reset()
		common.Config.Auth.Token = ""
	})

	config := adminClientConfig()

	assert.Equal(t, "shared-token", config.Auth.Token)
}

func TestAdminClientConfigUsesAdminAuthOverride(t *testing.T) {
	Config.Reset()
	common.Config.Auth.Token = "client-token"
	Config.adminAuth.Token = "admin-token"
	t.Cleanup(func() {
		Config.Reset()
		common.Config.Auth.Token = ""
	})

	config := adminClientConfig()

	assert.Equal(t, "admin-token", config.Auth.Token)
}

func TestShellParsesQuotedArguments(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })

	var emptyPutOptions []oxia.PutOption
	common.MockedClient.On("Put", "key", []byte("hello world and more escaped space"), emptyPutOptions).Return("key", oxia.Version{}, nil)
	common.MockedClient.On("Close").Return(nil)

	_, stderr, err := runShell(t, "put key \"hello world\" 'and more' escaped\\ space\nexit\n")

	require.NoError(t, err)
	assert.Empty(t, stderr)
	common.MockedClient.AssertExpectations(t)
}

func TestShellParsesEmptyQuotedArgument(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })

	var emptyPutOptions []oxia.PutOption
	common.MockedClient.On("Put", "key", []byte(""), emptyPutOptions).Return("key", oxia.Version{}, nil)
	common.MockedClient.On("Close").Return(nil)

	_, stderr, err := runShell(t, "put key \"\"\nexit\n")

	require.NoError(t, err)
	assert.Empty(t, stderr)
	common.MockedClient.AssertExpectations(t)
}

func TestShellRejectsUnterminatedQuote(t *testing.T) {
	common.MockedClient = common.NewMockClient()
	t.Cleanup(func() { common.MockedClient = nil })

	_, stderr, err := runShell(t, "put key \"unterminated\nexit\n")

	require.EqualError(t, err, "unterminated quoted string")
	assert.Contains(t, stderr, "Error: unterminated quoted string\n")
	common.MockedClient.AssertExpectations(t)
}

func TestPromptSuggestions(t *testing.T) {
	assertSuggestionTexts(t, suggestionsFor(nil), "admin", "get", "put", "delete", "delete-range", "list", "range-scan", "help", "exit", "quit")
	assertSuggestionTexts(t, suggestionsFor([]string{"admin"}), "namespace", "dataserver", "shard")
	assertSuggestionTexts(t, suggestionsFor([]string{"admin", "namespace"}), "list", "get", "create", "patch", "delete")
	assertSuggestionTexts(t, suggestionsFor([]string{"admin", "shard"}), "split")
	assertSuggestionTexts(t, suggestionsFor([]string{"get"}), "--hex", "--include-version", "--partition-key", "--index", "--comparison-type")
	assertSuggestionTexts(t, suggestionsFor([]string{"delete-range"}), "--key-min", "--key-max", "--partition-key")
	assertSuggestionTexts(t, suggestionsFor([]string{"list"}), "--key-min", "--key-max", "--partition-key", "--index", "--internal-keys")
	assertSuggestionTexts(t, suggestionsFor([]string{"range-scan"}), "--key-min", "--key-max", "--hex", "--include-version", "--partition-key", "--index", "--internal-keys")
	assertSuggestionTexts(t, suggestionsFor([]string{"admin", "shard", "split"}), "--namespace", "--shard", "--split-point")
}

func assertSuggestionTexts(t *testing.T, suggestions []prompt.Suggest, expected ...string) {
	t.Helper()

	texts := make([]string, 0, len(suggestions))
	for _, suggestion := range suggestions {
		texts = append(texts, suggestion.Text)
	}
	assert.Equal(t, expected, texts)
}

func namespaceView(name string, initialShards uint32, replicationFactor uint32) *proto.NamespaceView {
	return &proto.NamespaceView{
		Namespace: namespaceConfig(name, initialShards, replicationFactor),
	}
}

func namespaceConfig(name string, initialShards uint32, replicationFactor uint32) *proto.Namespace {
	return &proto.Namespace{
		Name:              name,
		InitialShardCount: initialShards,
		ReplicationFactor: replicationFactor,
	}
}

func dataServerView(name string, public string, internal string) *proto.DataServerView {
	return &proto.DataServerView{
		DataServer:       dataServerConfig(name, public, internal, nil),
		DataServerStatus: &proto.DataServerStatus{State: proto.DataServerState_DATA_SERVER_STATE_RUNNING},
	}
}

func dataServerConfig(name string, public string, internal string, labels map[string]string) *proto.DataServer {
	return &proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &name,
			Public:   public,
			Internal: internal,
		},
		Metadata: &proto.DataServerMetadata{
			Labels: labels,
		},
	}
}
