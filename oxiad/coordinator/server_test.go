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

package coordinator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/health"

	"github.com/oxia-db/oxia/common/rpc"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

type closeOrder []string

func (o *closeOrder) closer(name string) func() error {
	return func() error {
		*o = append(*o, name)
		return nil
	}
}

type orderedGrpcServer struct {
	rpc2.GrpcServer
	close func() error
}

func (s *orderedGrpcServer) Close() error { return s.close() }

type orderedCoordinator struct {
	Coordinator
	close func() error
}

func (c *orderedCoordinator) Close() error { return c.close() }

type orderedMetadataProvider struct {
	metadata.Provider
	close func() error
}

func (p *orderedMetadataProvider) Close() error { return p.close() }

type orderedClientPool struct {
	rpc.ClientPool
	close func() error
}

func (p *orderedClientPool) Close() error { return p.close() }

func TestGrpcServerCloseReleasesMetadataAfterCoordinatorAndBeforeClientPool(t *testing.T) {
	var order closeOrder
	ctx, cancel := context.WithCancel(context.Background())
	s := &GrpcServer{
		ctx:              ctx,
		ctxCancel:        cancel,
		healthServer:     health.NewServer(),
		grpcServer:       &orderedGrpcServer{close: order.closer("grpc-server")},
		adminServer:      &orderedGrpcServer{close: order.closer("admin-server")},
		coordinator:      &orderedCoordinator{close: order.closer("coordinator")},
		metadataProvider: &orderedMetadataProvider{close: order.closer("metadata-provider")},
		clientPool:       &orderedClientPool{close: order.closer("client-pool")},
	}

	assert.NoError(t, s.Close())
	assert.Equal(t, closeOrder{"grpc-server", "admin-server", "coordinator", "metadata-provider", "client-pool"}, order)
}
