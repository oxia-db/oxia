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

package common_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common"
)

func TestMust(t *testing.T) {
	assert.Equal(t, "value", common.Must("value", true))
	assert.PanicsWithValue(t, "unexpected missing value", func() {
		common.Must("", false)
	})
	assert.PanicsWithValue(t, "missing namespace=default shard=7", func() {
		common.Must("", false, "missing namespace=", "default", " shard=", int64(7))
	})
}
