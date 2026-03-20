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
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

// ServiceResolver resolves a service name to a set of network addresses.
// Implementations can integrate with service-discovery systems such as
// Kubernetes, Consul, or DNS SRV records.
//
// The resolver is given an AddressUpdater when it is started. It should
// call the updater whenever the set of addresses changes.
type ServiceResolver interface {
	// Scheme returns the URI scheme handled by this resolver (e.g.
	// "k8s", "consul"). The service address passed to NewSyncClient /
	// NewAsyncClient must use this scheme (e.g. "k8s:///my-service").
	Scheme() string

	// Start begins resolving. The updater function must be called with
	// the current set of addresses whenever the resolved set changes.
	// Start should call updater at least once before returning.
	Start(updater AddressUpdater)

	// ResolveNow is a hint that the caller would like the resolver to
	// re-resolve immediately. It may be ignored by the implementation.
	ResolveNow()

	// Close stops the resolver and releases any resources.
	Close()
}

// AddressUpdater is a callback that the ServiceResolver uses to push
// updated addresses to the underlying transport.
type AddressUpdater func(addresses []string)

// WithResolver configures the client to use a custom ServiceResolver
// for discovering server addresses. The service address passed to
// NewSyncClient / NewAsyncClient must use the scheme returned by
// ServiceResolver.Scheme().
//
// Example:
//
//	resolver := &myResolver{scheme: "k8s", ...}
//	client, err := oxia.NewSyncClient(
//	    "k8s:///my-oxia-service",
//	    oxia.WithDialOption(oxia.WithResolver(resolver)),
//	)
func WithResolver(sr ServiceResolver) DialOption {
	return &resolverDialOption{resolver: sr}
}

type resolverDialOption struct {
	resolver ServiceResolver
}

func (o *resolverDialOption) toGrpcDialOption() grpc.DialOption {
	return grpc.WithResolvers(&grpcResolverBuilder{sr: o.resolver})
}

// grpcResolverBuilder adapts a ServiceResolver to the gRPC
// resolver.Builder interface.
type grpcResolverBuilder struct {
	sr ServiceResolver
}

func (b *grpcResolverBuilder) Build(
	_ resolver.Target,
	cc resolver.ClientConn,
	_ resolver.BuildOptions,
) (resolver.Resolver, error) {
	r := &grpcResolver{sr: b.sr, cc: cc}
	r.start()
	return r, nil
}

func (b *grpcResolverBuilder) Scheme() string {
	return b.sr.Scheme()
}

type grpcResolver struct {
	sr ServiceResolver
	cc resolver.ClientConn
}

func (r *grpcResolver) start() {
	r.sr.Start(func(addresses []string) {
		addrs := make([]resolver.Address, len(addresses))
		for i, addr := range addresses {
			addrs[i] = resolver.Address{Addr: addr}
		}
		_ = r.cc.UpdateState(resolver.State{Addresses: addrs})
	})
}

func (r *grpcResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	r.sr.ResolveNow()
}

func (r *grpcResolver) Close() {
	r.sr.Close()
}
