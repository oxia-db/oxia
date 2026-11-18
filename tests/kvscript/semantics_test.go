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

package kvscript

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxia"
)

type namedClient struct {
	client    oxia.SyncClient
	namespace string
}

func (r *runner) newClient(t *testing.T, options ...oxia.ClientOption) oxia.SyncClient {
	t.Helper()
	options = append(slices.Clone(r.clientOptions), options...)
	client, err := oxia.NewSyncClient(r.address, options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client
}

func (r *runner) clientCommand(c command) string {
	c.t.Helper()
	if c.data.Cmd == "use-client" {
		c.checkArgs("name")
		client, exists := r.clients[c.arg("name")]
		if !exists {
			c.t.Fatalf("%s: unknown client %q", c.data.Pos, c.arg("name"))
		}
		r.client, r.namespace = client.client, client.namespace
		return "ok\n"
	}
	c.checkArgs("name", "namespace")
	name := c.arg("name")
	if name == "" || r.clients[name].client != nil {
		c.t.Fatalf("%s: client name must be nonempty and unique: %q", c.data.Pos, name)
	}
	var options []oxia.ClientOption
	namespace := constant.DefaultNamespace
	if c.data.HasArg("namespace") {
		namespace = c.arg("namespace")
		options = append(options, oxia.WithNamespace(namespace))
	}
	r.client = r.newClient(c.t, options...)
	r.namespace = namespace
	r.clients[name] = namedClient{client: r.client, namespace: namespace}
	return "ok\n"
}

func (r *runner) requireDefaultNamespace(c command) {
	c.t.Helper()
	if r.namespace != constant.DefaultNamespace {
		c.t.Fatalf("%s: %s requires a client in namespace %q; selected namespace is %q",
			c.data.Pos, c.data.Cmd, constant.DefaultNamespace, r.namespace)
	}
}

func (c command) boolArg(name string) bool {
	c.t.Helper()
	value := c.arg(name)
	if value != "true" && value != "false" {
		c.t.Fatalf("%s: %s must be true or false", c.data.Pos, name)
	}
	return value == "true"
}

func (r *runner) saveRecord(c command, key string, value []byte, version oxia.Version) {
	c.t.Helper()
	if c.data.HasArg("save-record") {
		r.records[c.arg("save-record")] = oxia.GetResult{Key: key, Value: bytes.Clone(value), Version: version}
	}
}

func (r *runner) savedRecord(c command, argument string) oxia.GetResult {
	c.t.Helper()
	alias, ok := strings.CutPrefix(c.arg(argument), "@")
	if !ok {
		c.t.Fatalf("%s: %s must reference a saved record with @name", c.data.Pos, argument)
	}
	saved, exists := r.records[alias]
	if !exists {
		c.t.Fatalf("%s: unknown saved record %q", c.data.Pos, alias)
	}
	return saved
}

func (r *runner) assertRecordRelations(c command) {
	c.t.Helper()
	relation := recordRelation(c)
	result := r.readRecord(c)
	if relation == "missing" {
		require.ErrorIs(c.t, result.Err, oxia.ErrKeyNotFound, "%s: record must be missing", c.data.Pos)
		return
	}
	require.NoError(c.t, result.Err, "%s: cannot read record for assertion", c.data.Pos)
	if c.data.HasArg("persistent") {
		assertPersistentVersion(c, result.Version)
	}
	if relation == "" {
		return
	}
	saved := r.savedRecord(c, relation)
	require.Equal(c.t, saved.Key, result.Key, "%s: record key changed", c.data.Pos)
	if relation == "updated" {
		assertUpdatedVersion(c, saved.Version, result.Version)
		return
	}
	require.Equal(c.t, saved.Version, result.Version, "%s: record version metadata changed", c.data.Pos)
	if relation == "unchanged" {
		require.True(c.t, bytes.Equal(saved.Value, result.Value), "%s: record value changed", c.data.Pos)
	}
}

func recordRelation(c command) string {
	c.t.Helper()
	var selected string
	for _, argument := range []string{"unchanged", "metadata", "updated", "missing"} {
		if !c.data.HasArg(argument) {
			continue
		}
		if selected != "" {
			c.t.Fatalf("%s: record assertions accept only one relation", c.data.Pos)
		}
		selected = argument
	}
	if selected == "missing" && c.data.HasArg("persistent") {
		c.t.Fatalf("%s: missing and persistent cannot be combined", c.data.Pos)
	}
	if selected == "" && !c.data.HasArg("persistent") {
		c.t.Fatalf("%s: record assertion requires a relation or persistent", c.data.Pos)
	}
	return selected
}

func assertUpdatedVersion(c command, previous, current oxia.Version) {
	c.t.Helper()
	// Version IDs are opaque: equality and CAS eligibility are the contract,
	// not their arithmetic progression or wall-clock timestamp ordering.
	require.NotEqual(c.t, previous.VersionId, current.VersionId, "%s: update reused the version", c.data.Pos)
	require.Equal(c.t, previous.CreatedTimestamp, current.CreatedTimestamp, "%s: update changed creation time", c.data.Pos)
	require.Equal(c.t, previous.ModificationsCount+1, current.ModificationsCount,
		"%s: update must increment the modification count", c.data.Pos)
}

func assertPersistentVersion(c command, version oxia.Version) {
	c.t.Helper()
	require.False(c.t, version.Ephemeral, "%s: record must be persistent", c.data.Pos)
	require.Zero(c.t, version.SessionId, "%s: persistent record has a session", c.data.Pos)
	require.Empty(c.t, version.ClientIdentity, "%s: persistent record has a client identity", c.data.Pos)
}
