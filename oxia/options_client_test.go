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
	"google.golang.org/grpc"
)

func TestWithDialOptions(t *testing.T) {
	opt1 := grpc.WithAuthority("test-authority")
	opt2 := grpc.WithUserAgent("test-agent")

	options, err := newClientOptions("localhost:6648", WithDialOptions(opt1, opt2))
	require.NoError(t, err)
	assert.Len(t, options.dialOptions, 2)
}

func TestWithDialOptions_Multiple(t *testing.T) {
	opt1 := grpc.WithAuthority("test-authority")
	opt2 := grpc.WithUserAgent("test-agent")

	options, err := newClientOptions("localhost:6648",
		WithDialOptions(opt1),
		WithDialOptions(opt2),
	)
	require.NoError(t, err)
	assert.Len(t, options.dialOptions, 2)
}

func TestWithDialOptions_Empty(t *testing.T) {
	options, err := newClientOptions("localhost:6648")
	require.NoError(t, err)
	assert.Empty(t, options.dialOptions)
}
