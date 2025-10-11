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

package object

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRc struct {
	closeFunc func()
}

func (t *testRc) Close() error {
	t.closeFunc()
	return nil
}

func TestRefCount(t *testing.T) {
	var done bool
	rc := NewRefCount[*testRc](&testRc{
		closeFunc: func() {
			done = true
		},
	})

	assert.EqualValues(t, 1, rc.RefCnt())

	rc2 := rc.Acquire()
	assert.EqualValues(t, 2, rc.RefCnt())
	assert.EqualValues(t, 2, rc2.RefCnt())

	assert.NoError(t, rc.Close())
	assert.EqualValues(t, 1, rc.RefCnt())
	assert.EqualValues(t, 1, rc2.RefCnt())
	assert.False(t, done)

	assert.NoError(t, rc.Close())
	assert.EqualValues(t, 0, rc.RefCnt())
	assert.EqualValues(t, 0, rc2.RefCnt())
	assert.True(t, done)
}
