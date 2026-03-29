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
	"github.com/stretchr/testify/require"
)

type mockResolver struct {
	scheme string
}

func (r *mockResolver) Scheme() string         { return r.scheme }
func (r *mockResolver) Resolve(_ string, _ AddressUpdater) {}

func TestWithDialResolver(t *testing.T) {
	r := &mockResolver{scheme: "test"}

	options, err := newClientOptions("localhost:6648",
		WithDialResolver(r),
	)
	require.NoError(t, err)
	assert.Equal(t, r, options.resolver)
}

func TestWithDialResolver_NotSet(t *testing.T) {
	options, err := newClientOptions("localhost:6648")
	require.NoError(t, err)
	assert.Nil(t, options.resolver)
}
