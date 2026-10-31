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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

var (
	drain    = model.Op{Namespace: "a", Shard: 1, From: "r", To: "n1"}
	transfer = model.Op{Kind: model.TransferLeader, Namespace: "b", Shard: 2, From: "n0", To: "n1"}
)

func TestDispatcherStartSendsTheChangeOfEnsemble(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	s := snapshot()

	require.NoError(t, d.Start(s, drain))

	change := (<-actions).(*action.ChangeEnsembleAction)
	assert.Equal(t, int64(1), change.Shard)
	assert.Equal(t, s.DataServers["r"].Identity, change.From)
	assert.Equal(t, s.DataServers["n1"].Identity, change.To)
	assert.True(t, d.Busy(1))
	assert.False(t, d.Busy(2))
}

func TestDispatcherStartTransfersTheLeader(t *testing.T) {
	d, actions := newDispatcher(t.Context())

	require.NoError(t, d.Start(snapshot(), transfer))

	election := (<-actions).(*action.ElectionAction)
	assert.Equal(t, int64(2), election.Shard)
	assert.Equal(t, "n1", election.PreferredLeader)
	go election.Done("n1")
	d.Finish(<-d.Results(), defaults())
	assert.False(t, d.Busy(2))
}

func TestDispatcherFailsATransferToAnotherLeader(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	require.NoError(t, d.Start(snapshot(), transfer))

	go (<-actions).Done("n0")
	f := <-d.Results()

	assert.ErrorIs(t, f.err, errs.ErrFailedPrecondition)
	d.Finish(f, defaults())
	assert.True(t, d.Busy(2))
}

func TestDispatcherStartRejectsAnInvalidOp(t *testing.T) {
	d, actions := newDispatcher(t.Context())

	err := d.Start(snapshot(), model.Op{Namespace: "a", Shard: 1, From: "r", To: "n0"})

	assert.ErrorIs(t, err, errs.ErrInvalidArgument)
	assert.False(t, d.Busy(1))
	assert.Empty(t, actions)
}

func TestDispatcherStartGivesUpTheSendOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	d := New(ctx, defaultPlanner(), make(chan action.Action))

	require.NoError(t, d.Start(snapshot(), drain))

	assert.True(t, d.Busy(1))
}

func TestDispatcherFinishClearsASuccessfulOp(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	require.NoError(t, d.Start(snapshot(), drain))

	go (<-actions).Done(nil)
	d.Finish(<-d.Results(), defaults())

	assert.False(t, d.Busy(1))
}

func TestDispatcherFinishMakesAFailedShardWait(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	require.NoError(t, d.Start(snapshot(), drain))

	go (<-actions).Error(errors.New("not ready"))
	d.Finish(<-d.Results(), defaults())

	assert.Empty(t, d.(*dispatcher).flights)
	assert.True(t, d.Busy(1))
}

func TestDispatcherFinishIgnoresAFlightThatTimedOut(t *testing.T) {
	d, actions := newDispatcher(t.Context())
	require.NoError(t, d.Start(snapshot(), drain))
	first := <-actions

	d.Expire(policy.Policy{MoveTimeout: time.Nanosecond, MoveBackoff: time.Nanosecond})
	assert.Empty(t, d.(*dispatcher).flights)
	require.Eventually(t, func() bool { return !d.Busy(1) }, time.Second, time.Millisecond)
	require.NoError(t, d.Start(snapshot(), drain))
	go first.Done(nil)
	d.Finish(<-d.Results(), defaults())

	// The late outcome of the first op leaves the second one in flight
	assert.Len(t, d.(*dispatcher).flights, 1)
}

func TestDispatcherInFlight(t *testing.T) {
	d, _ := newDispatcher(t.Context())
	assert.Empty(t, d.InFlight())

	require.NoError(t, d.Start(snapshot(), drain))

	assert.Equal(t, []model.Op{drain}, d.InFlight())
}

func TestDispatcherReportsNothingAfterClose(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	d, actions := newDispatcher(ctx)
	require.NoError(t, d.Start(snapshot(), drain))

	cancel()
	(<-actions).Done(nil)

	assert.Len(t, d.(*dispatcher).flights, 1)
}

// newDispatcher returns a dispatcher with the default planner, and its
// actions.
func newDispatcher(ctx context.Context) (Dispatcher, chan action.Action) {
	actions := make(chan action.Action, 10)
	return New(ctx, defaultPlanner(), actions), actions
}

func defaultPlanner() planner.Planner {
	return planner.New(filter.Defaults(), tier.Defaults())
}

func defaults() policy.Policy {
	return policy.New(nil)
}

// snapshot has a shard of namespace a on the removed data server r and on n0,
// and two shards of namespace b that n0 leads.
func snapshot() *model.Snapshot {
	node := func(name string, removed bool) *model.DataServer {
		return &model.DataServer{Identity: &proto.DataServerIdentity{Internal: name}, Removed: removed, Running: true,
			Stable: !removed}
	}
	return &model.Snapshot{
		DataServers: map[string]*model.DataServer{"r": node("r", true), "n0": node("n0", false), "n1": node("n1", false),
			"n2": node("n2", false)},
		Namespaces: map[string]*model.Namespace{"a": {Shards: map[int64]*model.Shard{
			1: {Ensemble: []string{"r", "n0"}},
		}},
			"b": {Shards: map[int64]*model.Shard{
				2: {Ensemble: []string{"n0", "n1"}, Leader: "n0"},
				3: {Ensemble: []string{"n0", "n2"}, Leader: "n0"},
			}}},
	}
}
