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

package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	dataservercontroller "github.com/oxia-db/oxia/oxiad/coordinator/controller/dataserver"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/collector"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/dispatcher"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/filter"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/tier"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

// actionsBuffer leaves room for the ops of a few rounds while the runtime
// dispatches them.
const actionsBuffer = 100

var _ Scheduler = &scheduler{}

type scheduler struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	logger     *slog.Logger
	metadata   coordmetadata.Metadata
	actions    chan action.Action
	planner    planner.Planner
	dispatcher dispatcher.Dispatcher
	collector  *collector.Collector
	splits     metric.Counter
}

// New starts a scheduler. The owner reads its Actions before New, since the
// first round may send some right away.
//
// status returns the status of the controller of a data server, or false
// without one: the plan moves nothing that needs a data server that is down,
// and the dispatch checks again before it sends an op. provider reads the stats of the shards from their leaders,
// for the auto-split.
func New(ctx context.Context, metadata coordmetadata.Metadata,
	status func(name string) (dataservercontroller.Status, bool), provider rpc.ControllerRPCProvider) Scheduler {
	ctx, cancel := context.WithCancel(ctx)
	p := planner.New(filter.Defaults(), tier.Defaults())
	actions := make(chan action.Action, actionsBuffer)
	s := &scheduler{
		ctx:        ctx,
		cancel:     cancel,
		logger:     slog.With(slog.String("component", "scheduler")),
		metadata:   metadata,
		actions:    actions,
		planner:    p,
		dispatcher: dispatcher.New(ctx, func(name string) bool { return collector.Running(status(name)) }, actions),
		collector:  collector.New(ctx, metadata, status, provider),
		splits: metric.NewCounter("oxia_coordinator_autosplit_splits_initiated_total",
			"Auto-splits started", "count", map[string]any{}),
	}
	s.wg.Go(func() {
		process.DoWithLabels(s.ctx, map[string]string{"component": "scheduler"}, s.run)
	})
	return s
}

func (s *scheduler) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}

func (s *scheduler) GetActions() <-chan action.Action {
	return s.actions
}

func (s *scheduler) run() {
	subscription := s.metadata.SubscribeConfig()
	defer subscription.Close()
	changed := subscription.Changed()
	// The first round runs right away; each round waits the interval of its
	// policy until the next one
	interval := time.Duration(0)
	for {
		select {
		case <-time.After(interval):
		case _, ok := <-changed:
			if !ok {
				// The metadata closed: this round runs, and the interval drives
				// the next ones
				changed = nil
			}
		case <-s.ctx.Done():
			return
		}

		config, err := s.metadata.GetConfig()
		if err != nil {
			s.logger.Warn("Failed to read the scheduler policy, retrying next round", slog.Any("error", err))
			interval = policy.New(nil).Interval
			continue
		}
		p := policy.New(config.UnsafeBorrow())
		interval = p.GetRoundInterval()
		now := time.Now()
		snap, err := s.collector.Collect()
		if err != nil {
			s.logger.Warn("Failed to read the cluster, retrying next round", slog.Any("error", err))
			continue
		}
		actions, blocked := s.planner.Plan(snap)
		if len(actions) == 0 {
			// The splits come last: sample the stats only once nothing else
			// moves
			s.collector.Sample(snap, p.Split, now)
			actions, blocked = s.planner.Plan(snap)
		}
		for _, replica := range blocked {
			s.logger.Warn("A replica on a removed data server is blocked", slog.Any("replica", replica))
		}
		// The next round plans on the snapshot that these actions leave, right
		// away if all of them succeeded: a failed one waits the interval, or
		// the next round would retry it in a busy loop
		if s.settle(s.dispatcher.Dispatch(snap, actions), now) && len(actions) > 0 {
			interval = 0
		}
	}
}

// settle logs the results of the actions of a round. It reports whether all
// of them succeeded.
func (s *scheduler) settle(results []model.Result, now time.Time) bool {
	succeeded := true
	for _, result := range results {
		if result.Err != nil {
			s.logger.Warn("An action failed", slog.Any("action", result.Action), slog.Any("error", result.Err))
			succeeded = false
			continue
		}
		if _, ok := result.Action.(*action.SplitAction); ok {
			s.collector.SetLastSplit(now)
			s.splits.Inc()
		}
	}
	return succeeded
}
