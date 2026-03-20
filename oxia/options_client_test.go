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
func (r *mockResolver) Start(_ AddressUpdater) {}
func (r *mockResolver) ResolveNow()            {}
func (r *mockResolver) Close()                 {}

func TestWithDialOption(t *testing.T) {
	r1 := &mockResolver{scheme: "test1"}
	r2 := &mockResolver{scheme: "test2"}

	options, err := newClientOptions("localhost:6648",
		WithDialOption(WithResolver(r1), WithResolver(r2)),
	)
	require.NoError(t, err)
	assert.Len(t, options.dialOptions, 2)
}

func TestWithDialOption_Multiple(t *testing.T) {
	r1 := &mockResolver{scheme: "test1"}
	r2 := &mockResolver{scheme: "test2"}

	options, err := newClientOptions("localhost:6648",
		WithDialOption(WithResolver(r1)),
		WithDialOption(WithResolver(r2)),
	)
	require.NoError(t, err)
	assert.Len(t, options.dialOptions, 2)
}

func TestWithDialOption_Empty(t *testing.T) {
	options, err := newClientOptions("localhost:6648")
	require.NoError(t, err)
	assert.Empty(t, options.dialOptions)
}
