// Copyright 2023 StreamNative, Inc.
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

package batch

import (
	"context"
	"sync"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/oxia/batch"
)

func NewManager(ctx context.Context, batcherFactory func(context.Context, *int64) batch.Batcher) *Manager {
	return &Manager{
		ctx:            ctx,
		batcherFactory: batcherFactory,
		batchers:       make(map[int64]batch.Batcher),
	}
}

type Manager struct {
	sync.RWMutex
	ctx            context.Context
	batcherFactory func(context.Context, *int64) batch.Batcher
	batchers       map[int64]batch.Batcher
}

func (m *Manager) Get(shardId int64) batch.Batcher {
	m.RLock()
	batcher, ok := m.batchers[shardId]
	m.RUnlock()

	if ok {
		return batcher
	}

	// Fallback on write-lock
	m.Lock()
	defer m.Unlock()

	if batcher, ok = m.batchers[shardId]; !ok {
		batcher = m.batcherFactory(m.ctx, &shardId)
		m.batchers[shardId] = batcher
	}
	return batcher
}

func (m *Manager) Close() error {
	m.Lock()
	defer m.Unlock()

	var err error
	for id, batcher := range m.batchers {
		delete(m.batchers, id)
		err = multierr.Append(err, batcher.Close())
	}

	return err
}
