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

package tier

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/planner/load"
)

func TestDrainImprovesOnlyFromARemovedDataServer(t *testing.T) {
	s := snapshot(map[string][][]string{"a": {{"r", "n0"}}})
	l := load.New(s)

	delta, enough := Drain{}.Delta(s, l, model.Op{Namespace: "a", From: "r", To: "n1"}, 0)
	assert.Equal(t, -1, delta)
	assert.True(t, enough)

	delta, enough = Drain{}.Delta(s, l, model.Op{Namespace: "a", From: "n0", To: "n1"}, 0)
	assert.Zero(t, delta)
	assert.False(t, enough)

	delta, enough = Drain{}.Delta(s, l, model.Op{Kind: model.TransferLeader, Namespace: "a", From: "r", To: "n0"}, 0)
	assert.Zero(t, delta)
	assert.False(t, enough)
}
