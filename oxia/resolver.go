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
	"context"

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
	//
	// The context is cancelled when the resolver is closed; long-running
	// operations (DNS watches, API calls) should respect it.
	Start(ctx context.Context, updater AddressUpdater)

	// ResolveNow is a hint that the caller would like the resolver to
	// re-resolve immediately. It may be ignored by the implementation.
	ResolveNow()

	// Close stops the resolver and releases any resources.
	Close()
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
// for discovering server addresses. The service address passed to
// NewSyncClient / NewAsyncClient must use the scheme returned by
// ServiceResolver.Scheme().
//
// Example:
//
//	resolver := &myResolver{scheme: "k8s", ...}
//	client, err := oxia.NewSyncClient(
//	    "k8s:///my-oxia-service",
//	    oxia.WithDialResolver(resolver),
//	)
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
	sr     ServiceResolver
	cc     resolver.ClientConn
	cancel context.CancelFunc
}

func (r *grpcResolver) start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.sr.Start(ctx, func(addresses []string) error {
		addrs := make([]resolver.Address, len(addresses))
		for i, addr := range addresses {
			addrs[i] = resolver.Address{Addr: addr}
		}
		return r.cc.UpdateState(resolver.State{Addresses: addrs})
	})
}

func (r *grpcResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	r.sr.ResolveNow()
}

func (r *grpcResolver) Close() {
	r.cancel()
	r.sr.Close()
}
