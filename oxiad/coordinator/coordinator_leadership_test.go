// Copyright 2023-2025 The Oxia Authors
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

package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type leadershipLostDuringStartupProvider struct {
	leadershipLostCh chan struct{}
}

func newLeadershipLostDuringStartupProvider() *leadershipLostDuringStartupProvider {
	return &leadershipLostDuringStartupProvider{
		leadershipLostCh: make(chan struct{}, 1),
	}
}

func (*leadershipLostDuringStartupProvider) Close() error {
	return nil
}

func (p *leadershipLostDuringStartupProvider) Get() (*model.ClusterStatus, metadata.Version, error) {
	return nil, metadata.NotExists, errors.New("not leader")
}

func (*leadershipLostDuringStartupProvider) Store(*model.ClusterStatus, metadata.Version) (metadata.Version, error) {
	return metadata.NotExists, errors.New("not leader")
}

func (p *leadershipLostDuringStartupProvider) WaitToBecomeLeader() error {
	p.leadershipLostCh <- struct{}{}
	return nil
}

func (p *leadershipLostDuringStartupProvider) LeadershipLost() <-chan struct{} {
	return p.leadershipLostCh
}

func TestNewCoordinatorStopsStartupWhenLeadershipIsAlreadyLost(t *testing.T) {
	meta := newLeadershipLostDuringStartupProvider()

	resultCh := make(chan error, 1)
	go func() {
		_, err := NewCoordinator(meta, func() (model.ClusterConfig, error) {
			return model.ClusterConfig{}, nil
		}, nil, nil)
		resultCh <- err
	}()

	select {
	case err := <-resultCh:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for coordinator startup to stop after leadership loss")
	}
}
