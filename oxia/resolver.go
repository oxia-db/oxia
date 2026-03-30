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
	"google.golang.org/grpc/resolver"
)

// ServiceResolver resolves a service name to a set of network addresses.
//
// Resolve may be called multiple times with different endpoints and
// concurrently. Implementations must be safe for concurrent use.
// The caller owns the resolver's lifecycle.
type ServiceResolver interface {
	// Scheme returns the URI scheme handled by this resolver (e.g. "dns", "tls").
	Scheme() string

	// Resolve resolves the given endpoint and calls updater with the addresses.
	// The updater must be called at least once, and again whenever addresses change.
	// Resolve must return promptly; launch long-running work in a goroutine.
	Resolve(endpoint string, updater AddressUpdater)
}

// AddressUpdater is a callback that the ServiceResolver uses to push
// updated addresses to the underlying transport.
//
// If an error is returned, the resolver should try to resolve the
// target again. The resolver should use a backoff timer to prevent
// overloading the server with requests. If a resolver is certain that
// reresolving will not change the result, e.g. because it is
// a watch-based resolver, returned errors can be ignored.
type AddressUpdater func(addresses []string) error

// WithDialResolver configures the client to use a custom ServiceResolver
// for discovering server addresses.
func WithDialResolver(sr ServiceResolver) ClientOption {
	return clientOptionFunc(func(options clientOptions) (clientOptions, error) {
		options.resolver = sr
		return options, nil
	})
}

// grpcResolverBuilder adapts a ServiceResolver to the gRPC
// resolver.Builder interface.
type grpcResolverBuilder struct {
	sr ServiceResolver
}

func (b *grpcResolverBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	_ resolver.BuildOptions,
) (resolver.Resolver, error) {
	b.sr.Resolve(target.Endpoint(), func(addresses []string) error {
		addrs := make([]resolver.Address, len(addresses))
		for i, addr := range addresses {
			addrs[i] = resolver.Address{Addr: addr}
		}
		return cc.UpdateState(resolver.State{Addresses: addrs})
	})
	return &grpcResolver{}, nil
}

func (b *grpcResolverBuilder) Scheme() string {
	return b.sr.Scheme()
}

// grpcResolver is a no-op resolver that satisfies the gRPC interface.
// All address updates are pushed by the ServiceResolver via the updater
// callback set up in Build.
type grpcResolver struct{}

func (*grpcResolver) ResolveNow(_ resolver.ResolveNowOptions) {}

func (*grpcResolver) Close() {}
