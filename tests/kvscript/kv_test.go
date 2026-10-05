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
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

const (
	requestTimeout = 5 * time.Second
	clusterTimeout = 30 * time.Second
)

type scriptTopology uint8

const (
	standaloneTopology scriptTopology = iota
	replicatedTopology
)

func TestKVScripts(t *testing.T) {
	runScripts(t, standaloneTopology)
}

func TestKVClusterScripts(t *testing.T) {
	runScripts(t, replicatedTopology)
}

func runScripts(t *testing.T, topology scriptTopology) {
	t.Helper()
	for _, sorting := range []string{"hierarchical", "natural"} {
		for _, shards := range []uint32{1, 4} {
			t.Run(fmt.Sprintf("%s/shards-%d", sorting, shards), func(t *testing.T) {
				keySorting, err := proto.ParseKeySortingType(sorting)
				require.NoError(t, err)
				dirs := []string{"common", sorting}
				if shards > 1 {
					dirs = append(dirs, filepath.Join("multishard", "common"), filepath.Join("multishard", sorting))
				}
				if topology == replicatedTopology {
					dirs = append(dirs, filepath.Join("cluster", "common"))
					if shards > 1 {
						dirs = append(dirs, filepath.Join("cluster", "multishard"))
					}
				}
				for _, dir := range dirs {
					datadriven.Walk(t, filepath.Join("testdata", dir), func(t *testing.T, path string) {
						t.Helper()
						runner := newRunner(t, shards, keySorting, topology)
						datadriven.RunTest(t, path, runner.run)
					})
				}
			})
		}
	}
}

type runner struct {
	client   oxia.SyncClient
	address  string
	cluster  *scriptCluster
	servers  map[string]string
	versions map[string]int64
	records  map[string]oxia.GetResult
}

func newRunner(t *testing.T, shards uint32, sorting proto.KeySortingType, topology scriptTopology) *runner {
	t.Helper()
	r := &runner{
		versions: make(map[string]int64), records: make(map[string]oxia.GetResult), servers: make(map[string]string),
	}
	options := []oxia.ClientOption{oxia.WithRequestTimeout(requestTimeout)}
	if topology == replicatedTopology {
		r.cluster, r.address = newScriptCluster(t, shards, sorting)
		resolver := &seedResolver{addresses: r.cluster.seedAddresses()}
		r.cluster.updateSeeds = resolver.update
		options = append(options, oxia.WithDialResolver(resolver))
		r.address = "kvscript:///" + r.address
	} else {
		r.address = newStandalone(t, shards, sorting)
	}
	client, err := oxia.NewSyncClient(r.address, options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	r.client = client
	return r
}

func newStandalone(t *testing.T, shards uint32, sorting proto.KeySortingType) string {
	t.Helper()
	config := dataserver.NewTestConfig(t.TempDir())
	config.NumShards = shards
	config.KeySorting = sorting
	server, err := dataserver.NewStandalone(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	return server.ServiceAddr()
}

// The harness removes stopped seeds and only readmits fully initialized ones.
// Advertised per-shard leader addresses still control all KV operations.
type seedResolver struct {
	mu        sync.Mutex
	addresses []string
	updaters  []oxia.AddressUpdater
}

func (*seedResolver) Scheme() string { return "kvscript" }

func (r *seedResolver) Resolve(_ string, updater oxia.AddressUpdater) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updaters = append(r.updaters, updater)
	_ = updater(slices.Clone(r.addresses))
}

func (r *seedResolver) update(addresses []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Keep surviving seeds ahead of restarted ones so discovery need not leave
	// its healthy connection just because an earlier seed has recovered.
	next := make([]string, 0, len(addresses))
	for _, address := range r.addresses {
		if slices.Contains(addresses, address) {
			next = append(next, address)
		}
	}
	for _, address := range addresses {
		if !slices.Contains(next, address) {
			next = append(next, address)
		}
	}
	r.addresses = next
	for _, updater := range r.updaters {
		_ = updater(slices.Clone(next))
	}
}

type command struct {
	t    *testing.T
	data *datadriven.TestData
	ctx  context.Context
}

func (r *runner) run(t *testing.T, data *datadriven.TestData) string {
	t.Helper()
	timeout := requestTimeout
	if slices.Contains([]string{"stop-server", "restart-server", "wait-replicated"}, data.Cmd) {
		timeout = clusterTimeout
	}
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	c := command{t: t, data: data, ctx: ctx}
	switch data.Cmd {
	case "put":
		return r.put(c)
	case "get":
		return r.get(c)
	case "delete":
		return r.delete(c)
	case "range":
		return r.scan(c)
	case "list":
		return r.list(c)
	case "delete-range":
		return r.deleteRange(c)
	case "placement":
		return r.placement(c)
	case "assert-record":
		return r.assertRecord(c)
	case "stop-server", "restart-server", "wait-replicated":
		return r.clusterCommand(c)
	default:
		t.Fatalf("%s: unknown command %q", data.Pos, data.Cmd)
		return ""
	}
}

func (r *runner) clusterCommand(c command) string {
	c.t.Helper()
	if r.cluster == nil {
		c.t.Fatalf("%s: %s requires the RF3 cluster runner", c.data.Pos, c.data.Cmd)
	}
	switch c.data.Cmd {
	case "stop-server":
		c.checkArgs("role", "key", "partition", "save")
		alias := c.arg("save")
		if _, exists := r.servers[alias]; exists {
			c.t.Fatalf("%s: server alias %q already exists", c.data.Pos, alias)
		}
		var partition *string
		if c.data.HasArg("partition") {
			value := c.arg("partition")
			partition = &value
		}
		r.servers[alias] = r.cluster.stop(c, c.arg("role"), c.arg("key"), partition)
	case "restart-server":
		c.checkArgs("saved")
		alias, ok := strings.CutPrefix(c.arg("saved"), "@")
		name, exists := r.servers[alias]
		if !ok || !exists {
			c.t.Fatalf("%s: saved must reference a stopped server with @name", c.data.Pos)
		}
		r.cluster.restart(c, name)
	case "wait-replicated":
		c.checkArgs()
		r.cluster.waitReplicated(c)
	default:
		c.t.Fatalf("%s: unknown cluster command %q", c.data.Pos, c.data.Cmd)
	}
	return "ok\n"
}

func (c command) checkArgs(allowed ...string) {
	c.t.Helper()
	if c.data.Input != "" {
		c.t.Fatalf("%s: commands take named arguments, not an input body", c.data.Pos)
	}
	seen := make(map[string]bool)
	for _, arg := range c.data.CmdArgs {
		if !slices.Contains(allowed, arg.Key) || seen[arg.Key] {
			c.t.Fatalf("%s: unknown or duplicate argument %q", c.data.Pos, arg.Key)
		}
		seen[arg.Key] = true
		switch arg.Key {
		case "create-only", "unordered", "require-override", "missing":
			arg.ExpectNumVals(c.t, 0)
		case "keys":
			if len(arg.Vals) == 0 {
				c.t.Fatalf("%s: keys requires at least one value", c.data.Pos)
			}
		default:
			arg.ExpectNumVals(c.t, 1)
		}
	}
}

func (c command) arg(name string) string {
	c.t.Helper()
	var value string
	c.data.ScanArgs(c.t, name, &value)
	return c.decodeArg(name, value)
}

func (c command) args(name string) []string {
	c.t.Helper()
	for _, arg := range c.data.CmdArgs {
		if arg.Key == name {
			values := make([]string, 0, len(arg.Vals))
			for _, value := range arg.Vals {
				values = append(values, c.decodeArg(name, value))
			}
			return values
		}
	}
	c.t.Fatalf("%s: missing argument %q", c.data.Pos, name)
	return nil
}

func (c command) decodeArg(name, value string) string {
	c.t.Helper()
	if strings.HasPrefix(value, "\"") {
		unquoted, err := strconv.Unquote(value)
		if err != nil {
			c.t.Fatalf("%s: invalid quoted %s: %v", c.data.Pos, name, err)
		}
		return unquoted
	}
	return value
}

func (c command) partition() oxia.BaseOption {
	c.t.Helper()
	if c.data.HasArg("partition") {
		return oxia.PartitionKey(c.arg("partition"))
	}
	return nil
}

func (r *runner) expectedVersion(c command) int64 {
	c.t.Helper()
	value := c.arg("expected-version")
	if alias, ok := strings.CutPrefix(value, "@"); ok {
		version, exists := r.versions[alias]
		if !exists {
			c.t.Fatalf("%s: unknown saved version %q", c.data.Pos, alias)
		}
		return version
	}
	version, err := strconv.ParseInt(value, 10, 64)
	if err != nil || version < 0 {
		c.t.Fatalf("%s: expected-version must be a nonnegative integer or @alias", c.data.Pos)
	}
	return version
}

func (r *runner) saveVersion(c command, version oxia.Version) {
	c.t.Helper()
	if c.data.HasArg("save") {
		r.versions[c.arg("save")] = version.VersionId
	}
}

func (r *runner) put(c command) string {
	c.t.Helper()
	c.checkArgs("key", "value", "partition", "create-only", "expected-version", "save")
	key, value := c.arg("key"), c.arg("value")
	var options []oxia.PutOption
	if partition := c.partition(); partition != nil {
		options = append(options, partition)
	}
	if c.data.HasArg("create-only") {
		if c.data.HasArg("expected-version") {
			c.t.Fatalf("%s: create-only and expected-version cannot be combined", c.data.Pos)
		}
		options = append(options, oxia.ExpectedRecordNotExists())
	}
	if c.data.HasArg("expected-version") {
		options = append(options, oxia.ExpectedVersionId(r.expectedVersion(c)))
	}
	storedKey, version, err := r.client.Put(c.ctx, key, []byte(value), options...)
	if err != nil {
		return c.domainError(err)
	}
	r.saveVersion(c, version)
	return fmt.Sprintf("ok key=%q modifications=%d\n", storedKey, version.ModificationsCount)
}

func (r *runner) get(c command) string {
	c.t.Helper()
	c.checkArgs("key", "partition", "comparison", "save", "save-record")
	key := c.arg("key")
	var options []oxia.GetOption
	if partition := c.partition(); partition != nil {
		options = append(options, partition)
	}
	if c.data.HasArg("comparison") {
		comparisons := map[string]oxia.GetOption{
			"equal": oxia.ComparisonEqual(), "floor": oxia.ComparisonFloor(),
			"ceiling": oxia.ComparisonCeiling(), "lower": oxia.ComparisonLower(),
			"higher": oxia.ComparisonHigher(),
		}
		comparison, ok := comparisons[c.arg("comparison")]
		if !ok {
			c.t.Fatalf("%s: unknown comparison %q", c.data.Pos, c.arg("comparison"))
		}
		options = append(options, comparison)
	}
	storedKey, value, version, err := r.client.Get(c.ctx, key, options...)
	if err != nil {
		return c.domainError(err)
	}
	r.saveVersion(c, version)
	if c.data.HasArg("save-record") {
		r.records[c.arg("save-record")] = oxia.GetResult{Key: storedKey, Value: bytes.Clone(value), Version: version}
	}
	return formatRecord(storedKey, value, version)
}

func (r *runner) assertRecord(c command) string {
	c.t.Helper()
	c.checkArgs("key", "partition", "unchanged", "missing")
	if c.data.HasArg("missing") {
		if c.data.HasArg("unchanged") {
			c.t.Fatalf("%s: missing and unchanged cannot be combined", c.data.Pos)
		}
		result := r.readRecord(c)
		require.ErrorIs(c.t, result.Err, oxia.ErrKeyNotFound, "%s: record must be missing", c.data.Pos)
		return "ok\n"
	}
	alias, ok := strings.CutPrefix(c.arg("unchanged"), "@")
	if !ok {
		c.t.Fatalf("%s: unchanged must reference a saved record with @name", c.data.Pos)
	}
	saved, exists := r.records[alias]
	if !exists {
		c.t.Fatalf("%s: unknown saved record %q", c.data.Pos, alias)
	}
	result := r.readRecord(c)
	require.NoError(c.t, result.Err, "%s: cannot read record for unchanged assertion", c.data.Pos)
	require.Equal(c.t, saved.Key, result.Key, "%s: record key changed", c.data.Pos)
	require.Equal(c.t, saved.Value, result.Value, "%s: record value changed", c.data.Pos)
	require.Equal(c.t, saved.Version, result.Version, "%s: record version metadata changed", c.data.Pos)
	return "ok\n"
}

func (r *runner) readRecord(c command) oxia.GetResult {
	c.t.Helper()
	var options []oxia.GetOption
	if partition := c.partition(); partition != nil {
		options = append(options, partition)
	}
	key, value, version, err := r.client.Get(c.ctx, c.arg("key"), options...)
	return oxia.GetResult{Key: key, Value: value, Version: version, Err: err}
}

func (r *runner) delete(c command) string {
	c.t.Helper()
	c.checkArgs("key", "partition", "expected-version")
	key := c.arg("key")
	var options []oxia.DeleteOption
	if partition := c.partition(); partition != nil {
		options = append(options, partition)
	}
	if c.data.HasArg("expected-version") {
		options = append(options, oxia.ExpectedVersionId(r.expectedVersion(c)))
	}
	if err := r.client.Delete(c.ctx, key, options...); err != nil {
		return c.domainError(err)
	}
	return "ok\n"
}

func (r *runner) scan(c command) string {
	c.t.Helper()
	c.checkArgs("start", "end", "partition", "unordered")
	start, end := c.arg("start"), c.arg("end")
	var options []oxia.RangeScanOption
	if partition := c.partition(); partition != nil {
		options = append(options, partition)
	}
	results := r.client.RangeScan(c.ctx, start, end, options...)
	var records []string
	for {
		select {
		case <-c.ctx.Done():
			c.t.Fatalf("%s: range did not complete: %v", c.data.Pos, c.ctx.Err())
		case result, ok := <-results:
			if !ok {
				if err := c.ctx.Err(); err != nil {
					c.t.Fatalf("%s: range did not complete: %v", c.data.Pos, err)
				}
				if c.data.HasArg("unordered") {
					slices.Sort(records)
				}
				return nonemptyOutput(strings.Join(records, ""))
			}
			if result.Err != nil {
				return c.domainError(result.Err)
			}
			records = append(records, formatRecord(result.Key, result.Value, result.Version))
		}
	}
}

func (r *runner) list(c command) string {
	c.t.Helper()
	c.checkArgs("start", "end", "partition", "unordered")
	start, end := c.arg("start"), c.arg("end")
	var options []oxia.ListOption
	if partition := c.partition(); partition != nil {
		options = append(options, partition)
	}
	keys, err := r.client.List(c.ctx, start, end, options...)
	if err != nil {
		return c.domainError(err)
	}
	if c.data.HasArg("unordered") {
		slices.Sort(keys)
	}
	var output strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&output, "key=%q\n", key)
	}
	return nonemptyOutput(output.String())
}

func (r *runner) deleteRange(c command) string {
	c.t.Helper()
	c.checkArgs("start", "end", "partition")
	start, end := c.arg("start"), c.arg("end")
	var options []oxia.DeleteRangeOption
	if partition := c.partition(); partition != nil {
		options = append(options, partition)
	}
	if err := r.client.DeleteRange(c.ctx, start, end, options...); err != nil {
		return c.domainError(err)
	}
	return "ok\n"
}

func (c command) domainError(err error) string {
	c.t.Helper()
	for _, expected := range []error{oxia.ErrKeyNotFound, oxia.ErrUnexpectedVersionId, oxia.ErrInvalidOptions} {
		if errors.Is(err, expected) {
			return fmt.Sprintf("error: %s\n", expected)
		}
	}
	c.t.Fatalf("%s: unexpected request failure: %v", c.data.Pos, err)
	return ""
}

func formatRecord(key string, value []byte, version oxia.Version) string {
	return fmt.Sprintf("key=%q value=%q modifications=%d\n", key, value, version.ModificationsCount)
}

func nonemptyOutput(output string) string {
	if output == "" {
		return "(empty)\n"
	}
	return output
}
