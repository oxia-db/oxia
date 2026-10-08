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
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

const requestTimeout = 5 * time.Second

func TestKVScripts(t *testing.T) {
	for _, sorting := range []string{"hierarchical", "natural"} {
		for _, shards := range []uint32{1, 4} {
			t.Run(fmt.Sprintf("%s/shards-%d", sorting, shards), func(t *testing.T) {
				keySorting, err := proto.ParseKeySortingType(sorting)
				require.NoError(t, err)
				for _, dir := range []string{"common", sorting} {
					datadriven.Walk(t, filepath.Join("testdata", dir), func(t *testing.T, path string) {
						t.Helper()
						runner := newRunner(t, shards, keySorting)
						datadriven.RunTest(t, path, runner.run)
					})
				}
			})
		}
	}
}

type runner struct {
	client   oxia.SyncClient
	versions map[string]int64
}

func newRunner(t *testing.T, shards uint32, sorting proto.KeySortingType) *runner {
	t.Helper()
	config := dataserver.NewTestConfig(t.TempDir())
	config.NumShards = shards
	config.KeySorting = sorting
	server, err := dataserver.NewStandalone(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	client, err := oxia.NewSyncClient(server.ServiceAddr(), oxia.WithRequestTimeout(requestTimeout))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return &runner{client: client, versions: make(map[string]int64)}
}

type command struct {
	t    *testing.T
	data *datadriven.TestData
	ctx  context.Context
}

func (r *runner) run(t *testing.T, data *datadriven.TestData) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), requestTimeout)
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
	default:
		t.Fatalf("%s: unknown command %q", data.Pos, data.Cmd)
		return ""
	}
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
		if arg.Key == "create-only" || arg.Key == "unordered" {
			arg.ExpectNumVals(c.t, 0)
		} else {
			arg.ExpectNumVals(c.t, 1)
		}
	}
}

func (c command) arg(name string) string {
	c.t.Helper()
	var value string
	c.data.ScanArgs(c.t, name, &value)
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
	c.checkArgs("key", "partition", "comparison", "save")
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
	return formatRecord(storedKey, value, version)
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
	c.checkArgs("start", "end", "partition")
	start, end := c.arg("start"), c.arg("end")
	var options []oxia.RangeScanOption
	if partition := c.partition(); partition != nil {
		options = append(options, partition)
	}
	results := r.client.RangeScan(c.ctx, start, end, options...)
	var output strings.Builder
	for {
		select {
		case <-c.ctx.Done():
			c.t.Fatalf("%s: range did not complete: %v", c.data.Pos, c.ctx.Err())
		case result, ok := <-results:
			if !ok {
				if err := c.ctx.Err(); err != nil {
					c.t.Fatalf("%s: range did not complete: %v", c.data.Pos, err)
				}
				return nonemptyOutput(output.String())
			}
			if result.Err != nil {
				return c.domainError(result.Err)
			}
			output.WriteString(formatRecord(result.Key, result.Value, result.Version))
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
