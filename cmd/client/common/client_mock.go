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

//revive:disable-next-line:var-naming
package common

import (
	"context"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"

	"github.com/oxia-db/oxia/oxia"
)

type MockClient struct {
	mock.Mock
}

func NewMockClient() *MockClient {
	return &MockClient{}
}

func (m *MockClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

func (m *MockClient) Put(_ context.Context, key string, value []byte, options ...oxia.PutOption) (insertedKey string, version oxia.Version, err error) {
	args := m.MethodCalled("Put", key, value, options)
	return args.String(0), oxia.Version{}, args.Error(2)
}

func (m *MockClient) Delete(_ context.Context, key string, options ...oxia.DeleteOption) error {
	args := m.MethodCalled("Delete", key, options)
	return args.Error(0)
}

func (m *MockClient) DeleteRange(_ context.Context, minKeyInclusive string, maxKeyExclusive string, options ...oxia.DeleteRangeOption) error {
	args := m.MethodCalled("DeleteRange", minKeyInclusive, maxKeyExclusive, options)
	return args.Error(0)
}

func (m *MockClient) Get(_ context.Context, key string, options ...oxia.GetOption) (storedKey string, value []byte, version oxia.Version, err error) {
	args := m.MethodCalled("Get", key, options)
	arg1, ok := args.Get(1).([]byte)
	if !ok {
		panic("cast failed")
	}
	return args.String(0), arg1, oxia.Version{}, args.Error(3)
}

func (m *MockClient) List(_ context.Context, minKeyInclusive string, maxKeyExclusive string, options ...oxia.ListOption) (keys []string, err error) {
	args := m.MethodCalled("List", minKeyInclusive, maxKeyExclusive, options)
	arg0, ok := args.Get(0).([]string)
	if !ok {
		panic("cast failed")
	}
	return arg0, args.Error(1)
}

func (m *MockClient) RangeScan(_ context.Context, minKeyInclusive string, maxKeyExclusive string, options ...oxia.RangeScanOption) <-chan oxia.GetResult {
	args := m.MethodCalled("RangeScan", minKeyInclusive, maxKeyExclusive, options)
	arg0, ok := args.Get(0).(chan oxia.GetResult)
	if !ok {
		panic("cast failed")
	}
	return arg0
}

func (*MockClient) GetNotifications() (oxia.Notifications, error) {
	return nil, errors.New("not implemented in mock")
}

func (*MockClient) GetSequenceUpdates(context.Context, string, ...oxia.GetSequenceUpdatesOption) (<-chan string, error) {
	return nil, errors.New("not implemented in mock")
}
