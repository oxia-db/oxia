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

package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func TestGetAuthority(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		":authority", "host:6648",
		"content-type", "application/grpc",
	))
	authority, err := GetAuthority(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "host:6648", authority)

	_, err = GetAuthority(metadata.NewIncomingContext(context.Background(), metadata.Pairs(":authority", "tls://host:6648")))
	assert.Error(t, err)

	_, err = GetAuthority(metadata.NewIncomingContext(context.Background(), metadata.Pairs("content-type", "application/grpc")))
	assert.Error(t, err)

	_, err = GetAuthority(context.Background())
	assert.Error(t, err)
}
