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

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
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
	options    Options
	actions    chan action.Action
	planner    planner.Planner
	dispatcher dispatcher.Dispatcher
}

func New(options Options) Scheduler {
	ctx, cancel := context.WithCancel(options.Context)
	p := planner.New(filter.Defaults(), tier.Defaults())
	actions := make(chan action.Action, actionsBuffer)
	return &scheduler{
		ctx:        ctx,
		cancel:     cancel,
		logger:     slog.With(slog.String("component", "scheduler")),
		options:    options,
		actions:    actions,
		planner:    p,
		dispatcher: dispatcher.New(ctx, p, actions),
	}
}

func (s *scheduler) Start() {
	s.wg.Go(func() {
		process.DoWithLabels(s.ctx, map[string]string{"component": "scheduler"}, s.run)
	})
}

func (s *scheduler) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}

func (s *scheduler) Actions() <-chan action.Action {
	return s.actions
}

func (s *scheduler) run() {
	subscription := s.options.Metadata.SubscribeConfig()
	defer subscription.Close()
	changed := subscription.Changed()
	ticker := time.NewTicker(s.round(nil))
	defer ticker.Stop()
	for {
		var finished *dispatcher.Flight
		select {
		case <-ticker.C:
		case _, ok := <-changed:
			if !ok {
				// The metadata closed: the interval still drives the rounds
				changed = nil
				continue
			}
		case finished = <-s.dispatcher.Results():
		case <-s.ctx.Done():
			return
		}
		ticker.Reset(s.round(finished))
	}
}

// round records the flight that finished, if any, plans and starts the ops,
// and returns the interval until the next round.
func (s *scheduler) round(finished *dispatcher.Flight) time.Duration {
	p, err := s.policy()
	if finished != nil {
		s.dispatcher.Finish(finished, p)
	}
	if err != nil {
		s.logger.Warn("Failed to read the scheduler policy, retrying next round", slog.Any("error", err))
		return p.Interval
	}
	s.dispatcher.Expire(p)
	snapshot, err := s.observe(p, s.dispatcher.Busy)
	if err != nil {
		s.logger.Warn("Failed to observe the cluster, retrying next round", slog.Any("error", err))
		return p.Interval
	}
	ops, stuck := s.planner.Plan(snapshot, p, s.dispatcher.InFlight())
	for _, st := range stuck {
		s.logger.Warn("A replica on a removed data server can't be moved", slog.Any("replica", st))
	}
	for _, op := range ops {
		// Plan only returns valid ops, and the dispatcher logs a rejection
		_ = s.dispatcher.Start(snapshot, op)
	}
	return p.Interval
}

// observe builds the snapshot of the cluster from the metadata and the health
// of the data servers: a data server is stable once it has been running for
// p.StableTime. busy reports the shards that the dispatcher keeps frozen.
func (s *scheduler) observe(p policy.Policy, busy func(int64) bool) (*model.Snapshot, error) {
	dataServers, err := s.options.Metadata.ListDataServer()
	if err != nil {
		return nil, err
	}
	namespaces, err := s.options.Metadata.ListNamespace()
	if err != nil {
		return nil, err
	}
	statuses, err := s.options.Metadata.ListNamespaceStatus()
	if err != nil {
		return nil, err
	}
	snapshot := model.NewSnapshot(dataServers, namespaces, statuses)
	for name, dataServer := range snapshot.DataServers {
		running, since := s.options.Health(name)
		dataServer.Running = running
		dataServer.Stable = running && time.Since(since) >= p.StableTime
	}
	for _, ns := range snapshot.Namespaces {
		for id, shard := range ns.Shards {
			shard.Frozen = shard.Frozen || busy(id)
		}
	}
	return snapshot, nil
}

// policy returns the scheduler policy of the configuration, or the default
// one with the error that kept it from being read.
func (s *scheduler) policy() (policy.Policy, error) {
	config, err := s.options.Metadata.GetConfig()
	if err != nil {
		return policy.New(nil), err
	}
	return policy.New(config.UnsafeBorrow().GetSchedulerPolicy()), nil
}
