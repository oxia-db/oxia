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

package rpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type MetadataValueSupplier = func() map[string]string

func MetadataInjectionDialOptions(valuesSupplier MetadataValueSupplier) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(withOutgoingMetadata(ctx, valuesSupplier), method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(withOutgoingMetadata(ctx, valuesSupplier), desc, cc, method, opts...)
		}),
	}
}

func withOutgoingMetadata(ctx context.Context, valuesSupplier MetadataValueSupplier) context.Context {
	merged, _ := metadata.FromOutgoingContext(ctx)
	if merged == nil {
		merged = metadata.MD{}
	}
	for key, value := range valuesSupplier() {
		merged.Set(key, value)
	}
	if len(merged) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, merged)
}
