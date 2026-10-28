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
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

var errTest = errors.New("test failure")

func TestExecutorExecuteRejectsEmptyArgs(t *testing.T) {
	executor, _ := newTestExecutor(common.NewMockClient())

	assert.EqualError(t, executor.Execute(nil), "usage: get|put|delete|delete-range|list|range-scan")
}

func TestExecutorExecuteReturnsClientError(t *testing.T) {
	for _, args := range [][]string{
		{"get", "k"},
		{"put", "k", "v"},
		{"delete", "k"},
		{"delete-range", "a", "z"},
		{"list"},
		{"range-scan"},
	} {
		t.Run(args[0], func(t *testing.T) {
			executor := Executor{
				Ctx:    context.Background(),
				Client: func() (oxia.SyncClient, error) { return nil, errTest },
				Out:    new(bytes.Buffer),
			}

			assert.ErrorIs(t, executor.Execute(args), errTest)
		})
	}
}

func TestExecutorExecuteAcceptsAliases(t *testing.T) {
	mockClient := common.NewMockClient()
	var emptyDeleteOptions []oxia.DeleteOption
	var emptyRangeScanOptions []oxia.RangeScanOption
	results := make(chan oxia.GetResult)
	close(results)
	mockClient.On("Delete", "k", emptyDeleteOptions).Return(nil)
	mockClient.On("RangeScan", "", "", []oxia.RangeScanOption{oxia.IncludeValue(false)}).
		Return(scanResults(oxia.GetResult{Key: "a"}))
	mockClient.On("RangeScan", "", "", emptyRangeScanOptions).Return(results)
	executor, out := newTestExecutor(mockClient)

	assert.NoError(t, executor.Execute([]string{"del", "k"}))
	assert.NoError(t, executor.Execute([]string{"ls"}))
	assert.NoError(t, executor.Execute([]string{"scan"}))

	assert.Equal(t, "OK\na\n", out.String())
	mockClient.AssertExpectations(t)
}

func newTestExecutor(syncClient oxia.SyncClient) (Executor, *bytes.Buffer) {
	out := new(bytes.Buffer)
	return Executor{
		Ctx:    context.Background(),
		Client: func() (oxia.SyncClient, error) { return syncClient, nil },
		Out:    out,
	}, out
}

// failingWriter accepts the first okWrites writes and fails all the following ones.
type failingWriter struct {
	okWrites int
	writes   int
}

var _ io.Writer = (*failingWriter)(nil)

func (w *failingWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > w.okWrites {
		return 0, errTest
	}
	return len(p), nil
}
