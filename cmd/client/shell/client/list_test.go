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

package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

func TestExecutorListUsesPositionalMinKey(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.On("RangeScan", "b", "", listOptions).Return(scanResults(oxia.GetResult{Key: "b"}, oxia.GetResult{Key: "c"}))
	executor, out := newTestExecutor(mockClient)

	require.NoError(t, executor.Execute([]string{"list", "b"}))

	assert.Equal(t, "b\nc\n", out.String())
	mockClient.AssertExpectations(t)
}

func TestExecutorListReturnsClientError(t *testing.T) {
	mockClient := common.NewMockClient()
	mockClient.On("RangeScan", "a", "z", listOptions).Return(
		scanResults(oxia.GetResult{Key: "a"}, oxia.GetResult{Err: errTest}))
	executor, out := newTestExecutor(mockClient)

	assert.ErrorIs(t, executor.Execute([]string{"list", "a", "z"}), errTest)
	assert.Equal(t, "a\n", out.String())
}

func TestExecutorListRejectsInvalidArgs(t *testing.T) {
	executor, out := newTestExecutor(common.NewMockClient())

	for _, args := range [][]string{
		{"list", "--unknown"},
		{"list", "--key-min", "a", "b"},
		{"list", "a", "b", "c"},
	} {
		assert.EqualError(t, executor.Execute(args), "usage: list [flags] [MIN_KEY [MAX_KEY]]", "args: %v", args)
	}
	assert.Empty(t, out.String())
}

var listOptions = []oxia.RangeScanOption{oxia.IncludeValue(false)}
