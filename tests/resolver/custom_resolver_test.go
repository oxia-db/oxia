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

package resolver

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

// staticResolver is a simple ServiceResolver that always resolves to a
// pre-configured address. It demonstrates how users can plug in custom
// service discovery (e.g. Consul, Kubernetes, DNS SRV) using the Oxia
// ServiceResolver interface — no gRPC imports required.
type staticResolver struct {
	scheme string
	target string
}

func (r *staticResolver) Scheme() string { return r.scheme }

func (r *staticResolver) Start(updater oxia.AddressUpdater) {
	updater([]string{r.target})
}

func (r *staticResolver) ResolveNow() {}

func (r *staticResolver) Close() {}

func newStandaloneServer(t *testing.T) *dataserver.Standalone {
	t.Helper()
	dir := t.TempDir()
	config := dataserver.NewTestConfig(dir)
	s, err := dataserver.NewStandalone(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestCustomResolver(t *testing.T) {
	server := newStandaloneServer(t)
	actualAddr := server.ServiceAddr()

	// Create a custom resolver that maps the custom scheme to the actual
	// server address. The authority (path) in the URI must be the real
	// address so the standalone server returns it as the shard leader.
	sr := &staticResolver{
		scheme: fmt.Sprintf("oxia-test-%d", os.Getpid()),
		target: actualAddr,
	}

	// Use the custom resolver via WithDialResolver.
	client, err := oxia.NewSyncClient(
		fmt.Sprintf("%s:///%s", sr.scheme, actualAddr),
		oxia.WithDialResolver(sr),
		oxia.WithRequestTimeout(10*time.Second),
	)
	require.NoError(t, err)
	defer client.Close()

	// Verify the client works end-to-end through the custom resolver.
	ctx := context.Background()

	_, _, err = client.Put(ctx, "/resolver-test", []byte("hello"))
	require.NoError(t, err)

	key, value, _, err := client.Get(ctx, "/resolver-test")
	require.NoError(t, err)
	assert.Equal(t, "/resolver-test", key)
	assert.Equal(t, []byte("hello"), value)
}
