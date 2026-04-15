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

package resource

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type failingStoreMetadataProvider struct {
	storeCalls atomic.Int32
}

func (*failingStoreMetadataProvider) Close() error {
	return nil
}

func (*failingStoreMetadataProvider) Get() (*model.ClusterStatus, metadata.Version, error) {
	return model.NewClusterStatus(), metadata.NotExists, nil
}

func (p *failingStoreMetadataProvider) Store(*model.ClusterStatus, metadata.Version) (metadata.Version, error) {
	p.storeCalls.Add(1)
	return metadata.NotExists, errors.New("not leader")
}

func (*failingStoreMetadataProvider) WaitToBecomeLeader() error {
	return nil
}

func TestStatusResourceUpdateStopsRetryingWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	provider := &failingStoreMetadataProvider{}
	statusResource, err := NewStatusResourceWithErrorContext(ctx, provider)
	require.NoError(t, err)

	cancel()
	start := time.Now()
	err = statusResource.Update(model.NewClusterStatus())

	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
	assert.EqualValues(t, 1, provider.storeCalls.Load())
}
