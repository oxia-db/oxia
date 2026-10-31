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

package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model"
)

func TestTargetNeedsAStableDataServer(t *testing.T) {
	s := snapshot()
	assert.NoError(t, Target{}.Filter(s, move("n0", "n2")))
	assert.ErrorIs(t, Target{}.Filter(s, move("n0", "x")), errs.ErrUnavailable)

	s.DataServers["n2"].Stable = false
	assert.ErrorIs(t, Target{}.Filter(s, move("n0", "n2")), errs.ErrUnavailable)

	s.DataServers["n2"] = &model.DataServer{Removed: true, Running: true, Stable: true}
	assert.ErrorIs(t, Target{}.Filter(s, move("n0", "n2")), errs.ErrUnavailable)
}
