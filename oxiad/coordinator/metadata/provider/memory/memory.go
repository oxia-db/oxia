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

package memory

import (
	"fmt"
	"reflect"
	"sync"

	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	metadatawatch "github.com/oxia-db/oxia/oxiad/coordinator/metadata/watch"
)

var _ provider.StatusProvider = (*Provider[*proto.ClusterStatus])(nil)
var _ provider.ConfigProvider = (*Provider[*proto.ClusterConfiguration])(nil)

type Provider[T gproto.Message] struct {
	sync.Mutex

	value   T
	version provider.Version
}

func (*Provider[T]) WaitToBecomeLeader() error {
	return nil
}

func NewProvider[T gproto.Message]() provider.Provider[T] {
	return &Provider[T]{
		version: provider.NotExists,
	}
}

func (*Provider[T]) Close() error {
	return nil
}

func (m *Provider[T]) Get() (value T, version provider.Version, err error) {
	m.Lock()
	defer m.Unlock()

	return cloneMessage(m.value), m.version, nil
}

func (m *Provider[T]) Store(value T, expectedVersion provider.Version) (newVersion provider.Version, err error) {
	m.Lock()
	defer m.Unlock()

	if expectedVersion != m.version {
		panic(provider.ErrBadVersion)
	}

	m.value = cloneMessage(value)
	m.version = provider.NextVersion(m.version)
	return m.version, nil
}

func (*Provider[T]) Watch() (*metadatawatch.Receiver[T], error) {
	return nil, provider.ErrWatchUnsupported
}

func cloneMessage[T gproto.Message](value T) T {
	var zero T
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return zero
	}
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return zero
		}
	}
	cloned, ok := gproto.Clone(value).(T)
	if !ok {
		panic(fmt.Sprintf("failed to clone metadata value of type %T", value))
	}
	return cloned
}
