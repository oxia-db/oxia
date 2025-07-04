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

var _ HealthServer = &CancelableHeathServer{}

type CancelableHeathServer struct {
	sync.WaitGroup
	*health.Server

	ctx    context.Context
	cancel context.CancelFunc
}

// Watch support to monitor the context of server to allow close health server manually
func (c *CancelableHeathServer) Watch(request *grpc_health_v1.HealthCheckRequest, g grpc.ServerStreamingServer[grpc_health_v1.HealthCheckResponse]) error {
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
			return c.ctx.Err()
		}
	}
}

func (c *CancelableHeathServer) Close() error {
	c.cancel()
	c.Wait()

	return nil
}

func NewCancelableHeathServer(ctx context.Context) HealthServer {
	ctx, cancelFunc := context.WithCancel(ctx)
	return &CancelableHeathServer{
		ctx:    ctx,
		cancel: cancelFunc,
		Server: health.NewServer(),
	}
}
