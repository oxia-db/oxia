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

package channel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/entity"
	"github.com/stretchr/testify/assert"
)

func TestReadAll(t *testing.T) {
	type testItem struct {
		value int
	}

	errUnknown := errors.New("error unknown")
	tests := []struct {
		name        string
		setupFunc   func() (context.Context, chan *entity.TWithError[testItem])
		expected    []testItem
		expectedErr error
	}{
		{
			name: "ReadAll completes successfully",
			setupFunc: func() (context.Context, chan *entity.TWithError[testItem]) {
				ch := make(chan *entity.TWithError[testItem], 3)
				ch <- &entity.TWithError[testItem]{T: testItem{value: 1}}
				ch <- &entity.TWithError[testItem]{T: testItem{value: 2}}
				ch <- &entity.TWithError[testItem]{T: testItem{value: 3}}
				close(ch)
				return context.Background(), ch
			},
			expected:    []testItem{{1}, {2}, {3}},
			expectedErr: nil,
		},
		{
			name: "Channel is empty and closed",
			setupFunc: func() (context.Context, chan *entity.TWithError[testItem]) {
				ch := make(chan *entity.TWithError[testItem])
				close(ch)
				return context.Background(), ch
			},
			expected:    []testItem{},
			expectedErr: nil,
		},
		{
			name: "Context is canceled before reading",
			setupFunc: func() (context.Context, chan *entity.TWithError[testItem]) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				ch := make(chan *entity.TWithError[testItem])
				return ctx, ch
			},
			expected:    nil,
			expectedErr: context.Canceled,
		},
		{
			name: "Context times out during read",
			setupFunc: func() (context.Context, chan *entity.TWithError[testItem]) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				time.Sleep(20 * time.Millisecond)
				cancel()
				ch := make(chan *entity.TWithError[testItem])
				return ctx, ch
			},
			expected:    nil,
			expectedErr: context.DeadlineExceeded,
		},
		{
			name: "Read until error in channel",
			setupFunc: func() (context.Context, chan *entity.TWithError[testItem]) {
				ch := make(chan *entity.TWithError[testItem], 3)
				ch <- &entity.TWithError[testItem]{T: testItem{value: 1}}
				ch <- &entity.TWithError[testItem]{T: testItem{value: 2}, Err: errUnknown}
				close(ch)
				return context.Background(), ch
			},
			expected:    nil,
			expectedErr: errUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, ch := tt.setupFunc()
			result, err := ReadAll(ctx, ch)
			assert.ErrorIs(t, err, tt.expectedErr)
			assert.Equal(t, len(result), len(tt.expected))
			for i, v := range result {
				assert.Equal(t, v, tt.expected[i])
			}
		})
	}
}
