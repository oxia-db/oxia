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

package feature

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestIsSupported(t *testing.T) {
	assert.True(t, IsSupported(proto.Feature_FEATURE_DB_CHECKSUM))
	assert.False(t, IsSupported(proto.Feature_FEATURE_UNKNOWN))
	assert.False(t, IsSupported(proto.Feature(999)))
}

func TestUnsupported(t *testing.T) {
	assert.Empty(t, Unsupported(nil))
	assert.Empty(t, Unsupported([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM}))
	assert.Equal(t, []proto.Feature{proto.Feature(999)},
		Unsupported([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM, proto.Feature(999)}))
}

func TestMissing(t *testing.T) {
	checksum := proto.Feature_FEATURE_DB_CHECKSUM
	other := proto.Feature(999)

	assert.Empty(t, Missing(nil, nil))
	assert.Empty(t, Missing(nil, []proto.Feature{checksum}))
	assert.Empty(t, Missing([]proto.Feature{checksum}, []proto.Feature{checksum, other}))
	assert.Equal(t, []proto.Feature{checksum}, Missing([]proto.Feature{checksum}, nil))
	assert.Equal(t, []proto.Feature{other}, Missing([]proto.Feature{checksum, other}, []proto.Feature{checksum}))
}
