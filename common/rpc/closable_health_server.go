// Copyright 2025 StreamNative, Inc.
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
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type HealthServer interface {
	io.Closer
	grpc_health_v1.HealthServer

	SetServingStatus(service string, servingStatus grpc_health_v1.HealthCheckResponse_ServingStatus)

	Shutdown()

	Resume()
}

var _ HealthServer = &ClosableHeathServer{}

type ClosableHeathServer struct {
	sync.WaitGroup
	*health.Server

	ctx    context.Context
	cancel context.CancelFunc
}

// Watch support to monitor the context of server to allow close health server manually.
func (c *ClosableHeathServer) Watch(request *grpc_health_v1.HealthCheckRequest, g grpc.ServerStreamingServer[grpc_health_v1.HealthCheckResponse]) error {
	finishCh := make(chan error, 1)
	c.Add(1)
	go func() {
		finishCh <- c.Server.Watch(request, g)
		c.Done()
	}()

	for {
		select {
		case err := <-finishCh:
			return err
		case <-c.ctx.Done():
			return nil
		}
	}
}

func (c *ClosableHeathServer) Close() error {
	c.Shutdown()

	c.cancel()
	c.Wait()

	return nil
}

func NewCancelableHeathServer(ctx context.Context) HealthServer {
	ctx, cancelFunc := context.WithCancel(ctx)
	return &ClosableHeathServer{
		ctx:    ctx,
		cancel: cancelFunc,
		Server: health.NewServer(),
	}
}
