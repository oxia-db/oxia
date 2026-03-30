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
	"sync"
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

func (r *staticResolver) Resolve(_ string, updater oxia.AddressUpdater) {
	_ = updater([]string{r.target})
}

// dynamicResolver demonstrates a resolver that can update its target
// address after creation. This simulates service-discovery systems where
// the set of backends changes over time (e.g. rolling deploys, failovers).
// It tracks updaters per endpoint so each connection gets its own updates.
type dynamicResolver struct {
	scheme   string
	mu       sync.Mutex
	targets  []string
	updaters map[string]oxia.AddressUpdater
}

func (r *dynamicResolver) Scheme() string { return r.scheme }

func (r *dynamicResolver) Resolve(endpoint string, updater oxia.AddressUpdater) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.updaters == nil {
		r.updaters = make(map[string]oxia.AddressUpdater)
	}
	r.updaters[endpoint] = updater
	_ = updater(r.targets)
}

// UpdateTargets pushes a new set of addresses to all connections.
func (r *dynamicResolver) UpdateTargets(targets []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targets = targets
	for _, updater := range r.updaters {
		_ = updater(targets)
	}
}

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
	// server address. In the URI form used below ("%s:///%s"), the path
	// component after the "///" encodes the real address so the standalone
	// server can return it as the shard leader.
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

func TestDynamicResolver(t *testing.T) {
	// Start two servers to simulate a failover scenario.
	server1 := newStandaloneServer(t)
	server2 := newStandaloneServer(t)
	addr1 := server1.ServiceAddr()
	addr2 := server2.ServiceAddr()

	dr := &dynamicResolver{
		scheme:  fmt.Sprintf("oxia-dyn-%d", os.Getpid()),
		targets: []string{addr1},
	}

	// Client initially connects to server1 via the dynamic resolver.
	client, err := oxia.NewSyncClient(
		fmt.Sprintf("%s:///%s", dr.scheme, addr1),
		oxia.WithDialResolver(dr),
		oxia.WithRequestTimeout(10*time.Second),
	)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Write through server1.
	_, _, err = client.Put(ctx, "/dyn-test", []byte("from-server1"))
	require.NoError(t, err)

	key, value, _, err := client.Get(ctx, "/dyn-test")
	require.NoError(t, err)
	assert.Equal(t, "/dyn-test", key)
	assert.Equal(t, []byte("from-server1"), value)

	// Simulate failover: update the resolver to point to server2.
	dr.UpdateTargets([]string{addr2})

	// Write through server2.
	_, _, err = client.Put(ctx, "/dyn-test-2", []byte("from-server2"))
	require.NoError(t, err)

	key, value, _, err = client.Get(ctx, "/dyn-test-2")
	require.NoError(t, err)
	assert.Equal(t, "/dyn-test-2", key)
	assert.Equal(t, []byte("from-server2"), value)
}
