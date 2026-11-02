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
	"slices"

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/action"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
)

var _ Dispatcher = &dispatcher{}

type dispatcher struct {
	ctx     context.Context
	logger  *slog.Logger
	running func(name string) bool
	actions chan<- action.Action
}

// New returns a Dispatcher that checks with running that the data servers of
// an action are running, and sends it on actions, until ctx is done.
func New(ctx context.Context, running func(name string) bool, actions chan<- action.Action) Dispatcher {
	return &dispatcher{
		ctx:     ctx,
		logger:  slog.With(slog.String("component", "dispatcher")),
		running: running,
		actions: actions,
	}
}

func (d *dispatcher) Dispatch(s *stats.ClusterStats, actions []action.Action) []model.Result {
	results := make([]model.Result, len(actions))
	var sent []int
	for i, ac := range actions {
		results[i].Action = ac
		if err := d.precheck(s, ac); err != nil {
			results[i].Err = err
			continue
		}
		select {
		case d.actions <- ac:
			d.logger.Info("Sent an action", slog.Any("action", ac))
			sent = append(sent, i)
		case <-d.ctx.Done():
			return nil
		}
	}
	for _, i := range sent {
		result, err := actions[i].Wait(d.ctx)
		// The election elects the target only if it has every committed entry
		if election, ok := actions[i].(*action.ElectionAction); ok && err == nil &&
			result != election.PreferredLeader {
			err = fmt.Errorf("%w: the election chose %v over %q", errs.ErrFailedPrecondition, result,
				election.PreferredLeader)
		}
		results[i].Err = err
	}
	return results
}

// precheck returns errs.ErrUnavailable if a data server that ac needs isn't
// running: its target, if it has one, or a member of its shard but a removed
// source, which is drained even when it is down.
func (d *dispatcher) precheck(s *stats.ClusterStats, ac action.Action) error {
	needed := slices.Clone(s.GetShard(ac.GetShard()).Ensemble)
	switch a := ac.(type) {
	case *action.ChangeEnsembleAction:
		needed = slices.DeleteFunc(needed, func(member string) bool {
			return member == a.From.GetNameOrDefault() && s.DataServers[member].Removed
		})
		needed = append(needed, a.To.GetNameOrDefault())
	case *action.ElectionAction:
		needed = append(needed, a.PreferredLeader)
	default:
	}
	if i := slices.IndexFunc(needed, func(name string) bool { return !d.running(name) }); i >= 0 {
		return fmt.Errorf("%w: data server %q of the action is down", errs.ErrUnavailable, needed[i])
	}
	return nil
}
