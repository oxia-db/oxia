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
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"

	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

// staticResolver is a simple gRPC resolver that always resolves to a
// pre-configured address. It demonstrates how users can plug in custom
// service discovery (e.g. Consul, Kubernetes, DNS SRV).
type staticResolver struct {
	target string
	cc     resolver.ClientConn
}

func (r *staticResolver) ResolveNow(_ resolver.ResolveNowOptions) {}

func (r *staticResolver) Close() {}

func (r *staticResolver) start() {
	r.cc.UpdateState(resolver.State{
		Addresses: []resolver.Address{{Addr: r.target}},
	})
}

// staticResolverBuilder builds staticResolver instances for a given scheme.
type staticResolverBuilder struct {
	scheme string
	target string
}

func (b *staticResolverBuilder) Build(
	_ resolver.Target,
	cc resolver.ClientConn,
	_ resolver.BuildOptions,
) (resolver.Resolver, error) {
	r := &staticResolver{target: b.target, cc: cc}
	r.start()
	return r, nil
}

func (b *staticResolverBuilder) Scheme() string {
	return b.scheme
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

	// Create a custom resolver that maps "oxia-test:///service" to the actual
	// server address.
	rb := &staticResolverBuilder{
		scheme: fmt.Sprintf("oxia-test-%d", os.Getpid()),
		target: actualAddr,
	}

	// Use the custom resolver via WithDialOptions + grpc.WithResolvers.
	client, err := oxia.NewSyncClient(
		fmt.Sprintf("%s:///service", rb.scheme),
		oxia.WithDialOptions(grpc.WithResolvers(rb)),
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
