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

package oxia

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptionalPresent(t *testing.T) {
	o := optionalOf(1)
	assert.True(t, o.Present())
	assert.False(t, o.Empty())
	v, ok := o.Get()
	assert.True(t, ok)
	assert.Equal(t, 1, v)
}

func TestOptionalEmpty(t *testing.T) {
	e := empty[string]()
	assert.False(t, e.Present())
	assert.True(t, e.Empty())
	es, ok := e.Get()
	assert.False(t, ok)
	assert.Equal(t, "", es)
}
