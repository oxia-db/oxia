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

func TestOvershootImprovesOnlyOverTheThreshold(t *testing.T) {
	s := newStats(map[string][][]string{"a": {{"n0"}}})
	shard := s.Namespaces["a"].Shards[1]
	split := model.Candidate{Kind: model.Split, Namespace: "a", Shard: 1}

	shard.Overshoot = 1
	delta := Overshoot{}.Delta(s, NewLoad(s), split)
	assert.Zero(t, delta)

	shard.Overshoot = 2.5
	delta = Overshoot{}.Delta(s, NewLoad(s), split)
	assert.Equal(t, -250, delta)
}
