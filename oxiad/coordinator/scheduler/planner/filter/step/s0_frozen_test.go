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

package step

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/common/errs"
)

func TestFrozenName(t *testing.T) {
	assert.Equal(t, "frozen", Frozen{}.GetName())
}

func TestFrozenRejectsAFrozenShard(t *testing.T) {
	s := newStats()
	assert.NoError(t, Frozen{}.Filter(s, move("n0", "n2")))

	s.Namespaces["a"].Shards[1].Changing = true
	assert.ErrorIs(t, Frozen{}.Filter(s, move("n0", "n2")), errs.ErrFailedPrecondition)

	s.Namespaces["a"].Shards[1].Changing, s.Namespaces["a"].Shards[1].Splitting = false, true
	assert.ErrorIs(t, Frozen{}.Filter(s, move("n0", "n2")), errs.ErrFailedPrecondition)
}
