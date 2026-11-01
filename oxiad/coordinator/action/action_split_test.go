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

package action

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitActionWait(t *testing.T) {
	done := NewSplitAction("a", 1)
	assert.Equal(t, int64(1), done.GetShard())
	assert.Equal(t, "a", done.Namespace)
	done.Done([]int64{2, 3})
	done.Error(errors.New("late"))
	children, err := done.Wait(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []int64{2, 3}, children)

	failed := NewSplitAction("a", 1)
	failed.Error(errors.New("unavailable"))
	_, err = failed.Wait(context.Background())
	assert.Error(t, err)
}
