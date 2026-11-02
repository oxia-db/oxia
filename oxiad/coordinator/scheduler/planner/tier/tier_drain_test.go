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
)

func TestDrainImprovesOnlyFromARemovedDataServer(t *testing.T) {
	s := newStats(map[string][][]string{"a": {{"r", "n0"}}})
	l := NewLoad(s)

	delta := Drain{}.Delta(s, l, model.Candidate{Namespace: "a", From: "r", To: "n1"})
	assert.Equal(t, -1, delta)

	delta = Drain{}.Delta(s, l, model.Candidate{Namespace: "a", From: "n0", To: "n1"})
	assert.Zero(t, delta)
}
