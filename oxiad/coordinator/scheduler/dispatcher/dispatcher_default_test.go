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

package dispatcher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

// drain moves the replica of shard 1 from the removed r onto n1.
func drain() *action.ChangeEnsembleAction {
	return action.NewChangeEnsembleAction(1, identity("r"), identity("n1"))
}

// transfer moves the leadership of shard 2 from n0 to n1.
func transfer() *action.ElectionAction {
	return action.NewLeaderTransferAction(2, "n1")
}

func TestDispatcherDispatchChangesTheEnsemble(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	ac := drain()
	go func() { (<-actions).Done(nil) }()

	assert.Equal(t, []model.Result{{Action: ac}}, d.Dispatch(newStats(), []action.Action{ac}))
}

func TestDispatcherDispatchTransfersTheLeader(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	ac := transfer()
	go func() { (<-actions).Done("n1") }()

	assert.Equal(t, []model.Result{{Action: ac}}, d.Dispatch(newStats(), []action.Action{ac}))
}

func TestDispatcherDispatchSplitsTheShard(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	ac := action.NewSplitAction("b", 3)
	go func() { (<-actions).Done([]int64{4, 5}) }()

	assert.Equal(t, []model.Result{{Action: ac}}, d.Dispatch(newStats(), []action.Action{ac}))
}

func TestDispatcherDispatchFailsATransferToAnotherLeader(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	ac := transfer()
	go func() { (<-actions).Done("n0") }()

	results := d.Dispatch(newStats(), []action.Action{ac})

	assert.ErrorIs(t, errorOf(results, ac), errs.ErrFailedPrecondition)
}

func TestDispatcherDispatchReportsAFailedAction(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	ac := drain()
	failure := errors.New("not ready")
	go func() { (<-actions).Error(failure) }()

	results := d.Dispatch(newStats(), []action.Action{ac})

	assert.ErrorIs(t, errorOf(results, ac), failure)
}

func TestDispatcherDispatchWaitsForEveryAction(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	first, second := drain(), transfer()
	go func() {
		a, b := <-actions, <-actions
		b.Done("n1")
		a.Done(nil)
	}()

	results := d.Dispatch(newStats(), []action.Action{first, second})

	assert.Equal(t, []model.Result{{Action: first}, {Action: second}}, results)
}

func TestDispatcherDispatchChecksTheDataServersAreRunning(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action func() action.Action
		down   string
		err    error
	}{
		{"target down", func() action.Action { return drain() }, "n1", errs.ErrUnavailable},
		{"member down", func() action.Action { return drain() }, "n0", errs.ErrUnavailable},
		{"removed source down", func() action.Action { return drain() }, "r", nil},
		{"source down", func() action.Action {
			return action.NewChangeEnsembleAction(3, identity("n2"), identity("n1"))
		}, "n2", errs.ErrUnavailable},
		{"transfer target down", func() action.Action { return transfer() }, "n1", errs.ErrUnavailable},
		{"leader down", func() action.Action { return transfer() }, "n0", errs.ErrUnavailable},
		{"split member down", func() action.Action { return action.NewSplitAction("b", 3) }, "n2",
			errs.ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actions := make(chan action.Action, 1)
			d := New(t.Context(), func(name string) bool { return name != tc.down }, actions)
			ac := tc.action()
			if tc.err == nil {
				go func() { (<-actions).Done(nil) }()
			}

			results := d.Dispatch(newStats(), []action.Action{ac})

			if tc.err == nil {
				require.NoError(t, errorOf(results, ac))
				return
			}
			assert.ErrorIs(t, errorOf(results, ac), tc.err)
			assert.Empty(t, actions)
		})
	}
}

func TestDispatcherDispatchCancelsTheActionsAfterADataServerDown(t *testing.T) {
	actions := make(chan action.Action, 2)
	d := New(t.Context(), func(name string) bool { return name != "n2" }, actions)
	go func() { (<-actions).Done(nil) }()
	first, down, last := drain(), action.NewChangeEnsembleAction(3, identity("n0"), identity("n2")), transfer()

	results := d.Dispatch(newStats(), []action.Action{first, down, last})

	require.Len(t, results, 2)
	require.NoError(t, errorOf(results, first))
	assert.ErrorIs(t, errorOf(results, down), errs.ErrUnavailable)
	assert.Empty(t, actions)
}

func TestDispatcherDispatchGivesUpTheSendOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	d := New(ctx, running, make(chan action.Action))

	assert.Empty(t, d.Dispatch(newStats(), []action.Action{drain()}))
}

func TestDispatcherDispatchGivesUpTheWaitOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	d, actions := newDispatcher(ctx)
	ac := drain()
	go func() {
		<-actions
		cancel()
	}()

	results := d.Dispatch(newStats(), []action.Action{ac})

	assert.ErrorIs(t, errorOf(results, ac), context.Canceled)
}

// newDispatcher returns a dispatcher with every data server running, and its
// actions.
func newDispatcher(ctx context.Context) (Dispatcher, chan action.Action) {
	actions := make(chan action.Action, 10)
	return New(ctx, running, actions), actions
}

func running(string) bool {
	return true
}

// snapshot has a shard of namespace a on the removed data server r and on n0,
// and two shards of namespace b that n0 leads.
func newStats() *stats.ClusterStats {
	node := func(name string, removed bool) *stats.DataServerStats {
		return &stats.DataServerStats{DataServer: &proto.DataServer{Identity: identity(name)}, Removed: removed}
	}
	return &stats.ClusterStats{
		DataServers: map[string]*stats.DataServerStats{"r": node("r", true), "n0": node("n0", false), "n1": node("n1", false),
			"n2": node("n2", false)},
		Namespaces: map[string]*stats.NamespaceStats{"a": {Shards: map[int64]*stats.ShardStats{
			1: {Ensemble: []string{"r", "n0"}},
		}},
			"b": {Shards: map[int64]*stats.ShardStats{
				2: {Ensemble: []string{"n0", "n1"}, Leader: "n0"},
				3: {Ensemble: []string{"n0", "n2"}, Leader: "n0"},
			}}},
	}
}

// errorOf returns the error of ac in results.
func errorOf(results []model.Result, ac action.Action) error {
	for _, r := range results {
		if r.Action == ac {
			return r.Err
		}
	}
	panic("no result for the action")
}

func identity(name string) *proto.DataServerIdentity {
	return &proto.DataServerIdentity{Internal: name}
}
