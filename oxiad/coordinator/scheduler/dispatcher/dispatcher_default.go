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
	"fmt"
	"log/slog"
	"time"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/oxiad/common/cooldown"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

var _ Dispatcher = &dispatcher{}

type dispatcher struct {
	ctx     context.Context
	logger  *slog.Logger
	planner planner.Planner
	actions chan<- action.Action
	results chan *Flight
	// flights holds the ops in flight, by shard.
	flights  map[int64]*Flight
	cooldown *cooldown.Cooldown[int64]
}

// New returns a Dispatcher that validates the ops with p and sends their
// actions on actions, until ctx is done.
func New(ctx context.Context, p planner.Planner, actions chan<- action.Action) Dispatcher {
	return &dispatcher{
		ctx:      ctx,
		logger:   slog.With(slog.String("component", "dispatcher")),
		planner:  p,
		actions:  actions,
		results:  make(chan *Flight),
		flights:  make(map[int64]*Flight),
		cooldown: cooldown.New[int64](),
	}
}

func (d *dispatcher) Results() <-chan *Flight {
	return d.results
}

func (d *dispatcher) Busy(shard int64) bool {
	_, ok := d.flights[shard]
	return ok || d.cooldown.Waiting(shard)
}

func (d *dispatcher) InFlight() []model.Op {
	ops := make([]model.Op, 0, len(d.flights))
	for _, f := range d.flights {
		ops = append(ops, f.op)
	}
	return ops
}

func (d *dispatcher) Start(s *model.Snapshot, op model.Op) error {
	if err := d.planner.Validate(s, op); err != nil {
		d.logger.Warn("Rejected an op", slog.Any("op", op), slog.Any("error", err))
		return err
	}
	f := &Flight{op: op, started: time.Now()}
	d.flights[op.Shard] = f
	d.logger.Info("Started an op", slog.Any("op", op))
	report := func(err error) {
		f.err = err
		select {
		case d.results <- f:
		case <-d.ctx.Done():
		}
	}
	var ac action.Action
	if op.Kind == model.TransferLeader {
		// The election elects To only if it has every committed entry
		ac = action.NewLeaderTransferAction(op.Shard, op.To, concurrent.NewOnce(func(leader any) {
			if leader != op.To {
				report(fmt.Errorf("%w: the election chose %v over %q", errs.ErrFailedPrecondition, leader, op.To))
				return
			}
			report(nil)
		}, report))
	} else {
		ac = action.NewChangeEnsembleActionWithCallback(op.Shard, s.DataServers[op.From].Identity,
			s.DataServers[op.To].Identity, concurrent.NewOnce(func(any) { report(nil) }, report))
	}
	select {
	case d.actions <- ac:
	case <-d.ctx.Done():
	}
	return nil
}

func (d *dispatcher) Finish(f *Flight, p policy.Policy) {
	if d.flights[f.op.Shard] != f {
		return
	}
	delete(d.flights, f.op.Shard)
	if f.err != nil {
		d.logger.Warn("An op failed", slog.Any("op", f.op), slog.Any("error", f.err))
		d.cooldown.Fail(f.op.Shard, p.MoveBackoff, p.MoveTimeout)
		return
	}
	d.logger.Info("An op finished", slog.Any("op", f.op))
	d.cooldown.Succeed(f.op.Shard)
}

func (d *dispatcher) Expire(p policy.Policy) {
	for shard, f := range d.flights {
		if time.Since(f.started) >= p.MoveTimeout {
			d.logger.Warn("An op timed out", slog.Any("op", f.op))
			delete(d.flights, shard)
			d.cooldown.Fail(shard, p.MoveBackoff, p.MoveTimeout)
		}
	}
	d.cooldown.Prune(p.MoveTimeout)
}
