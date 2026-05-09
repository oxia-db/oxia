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

package constant

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/status"
)

func TestWithLeaderHint(t *testing.T) {
	err := WithLeaderHint(status.Convert(ErrNodeIsNotLeader), 1, "leader:6648")
	hint := GetLeaderHint(err)
	assert.NotNil(t, hint)
	assert.Equal(t, int64(1), hint.Shard)
	assert.Equal(t, "leader:6648", hint.LeaderAddress)
}
