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

package metadata

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/coordinator/model"
)

type testRaftClusterProvider struct {
	providers []Provider
	leader    Provider
}

func (t testRaftClusterProvider) Close() error {
	for _, p := range t.providers {
		if err := p.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (t testRaftClusterProvider) Get() (cs *model.ClusterStatus, version Version, err error) {
	return t.leader.Get()
}

func (t testRaftClusterProvider) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	return t.leader.Store(cs, expectedVersion)
}

func (t testRaftClusterProvider) WaitToBecomeLeader() error {
	return t.leader.WaitToBecomeLeader()
}

func newTestRaftClusterProvider(t *testing.T) Provider {
	t.Helper()

	// start a cluster
	trc := &testRaftClusterProvider{}
	baseDir := t.TempDir()
	bootstrapServers := []string{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}

	for i := 0; i < 3; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 9000+i)
		dataDir := filepath.Join(baseDir, fmt.Sprintf("data-%d", i))
		p, err := NewMetadataProviderRaft(addr, bootstrapServers, dataDir)
		assert.NoError(t, err)

		trc.providers = append(trc.providers, p)
	}

	leaderFuture := concurrent.NewFuture[Provider]()

	for i := 0; i < 3; i++ {
		idx := i
		go func() {
			p := trc.providers[idx]
			err := p.WaitToBecomeLeader()
			assert.NoError(t, err)
			leaderFuture.Complete(p)
		}()
	}

	var err error
	trc.leader, err = leaderFuture.Wait(context.Background())
	assert.NoError(t, err)
	return trc
}
