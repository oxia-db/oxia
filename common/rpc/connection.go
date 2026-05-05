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
	"crypto/tls"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/oxia-db/oxia/common/auth"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/process"
)

var (
	defaultGrpcClientHealthProbeInterval = defaultGrpcClientKeepAliveTime
	defaultGrpcClientHealthProbeTimeout  = defaultGrpcClientKeepAliveTimeout
)

type connection struct {
	io.Closer
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	status atomic.Uint32
	logger slog.Logger

	target         string
	conn           *grpc.ClientConn
	healthClient   grpc_health_v1.HealthClient
	healthListener connectionHealthListener
}

type connectionHealthListener interface {
	onNotHealthy(cnx *connection)
}

type connectionStatus uint32

const (
	connectionStatusOpen connectionStatus = iota
	connectionStatusClosed
)

func newConnection(
	ctx context.Context,
	target string,
	tlsOptions *tls.Config,
	auth auth.Authentication,
	extraOptions []grpc.DialOption,
	healthListener connectionHealthListener,
) (*connection, error) {
	logger := *slog.With("server_address", target)
	logger.Debug("Creating new GRPC connection", slog.String("server_address", target))

	actualAddress := target
	tcs := insecure.NewCredentials()
	if strings.HasPrefix(target, AddressSchemaTLS) {
		actualAddress, _ = strings.CutPrefix(target, AddressSchemaTLS)
		tcs = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	if tlsOptions != nil {
		tcs = credentials.NewTLS(tlsOptions)
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(tcs),
		grpc.WithChainStreamInterceptor(grpcprometheus.StreamClientInterceptor),
		grpc.WithChainUnaryInterceptor(grpcprometheus.UnaryClientInterceptor),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			PermitWithoutStream: defaultGrpcClientPermitWithoutStream,
			Time:                defaultGrpcClientKeepAliveTime,
			Timeout:             defaultGrpcClientKeepAliveTimeout,
		}),
	}
	if auth != nil {
		options = append(options, grpc.WithPerRPCCredentials(auth))
	}
	options = append(options, extraOptions...)

	client, err := grpc.NewClient(actualAddress, options...)
	if err != nil {
		return nil, errors.Wrapf(err, "error connecting to %s", target)
	}
	ctx, cancel := context.WithCancel(ctx)
	cnx := &connection{
		Closer:         client,
		ctx:            ctx,
		cancel:         cancel,
		wg:             sync.WaitGroup{},
		logger:         logger,
		target:         target,
		conn:           client,
		healthClient:   grpc_health_v1.NewHealthClient(client),
		healthListener: healthListener,
	}
	cnx.wg.Go(func() {
		process.DoWithLabels(
			cnx.ctx,
			map[string]string{"oxia": "connection-health-ping"},
			cnx.monitorHealth,
		)
	})
	return cnx, nil
}

func (c *connection) monitorHealth() {
	var failureStart time.Time
	ticker := time.NewTicker(defaultGrpcClientHealthProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			checkStartedAt := time.Now()
			ctx, cancel := context.WithTimeout(c.ctx, defaultGrpcClientHealthProbeTimeout)
			response, err := c.healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
			cancel()

			if err == nil && response.Status != grpc_health_v1.HealthCheckResponse_SERVING {
				err = errors.Errorf("health status is %s", response.Status)
			}

			switch {
			case err == nil:
				failureStart = time.Time{}
			case grpcstatus.Code(err) == codes.Unimplemented:
				c.logger.Info("GRPC health service is not available; skipping connection health monitoring", slog.Any("error", err))
				return
			default:
				if failureStart.IsZero() {
					failureStart = checkStartedAt
				}
				if time.Since(failureStart) >= defaultGrpcClientHealthProbeTimeout {
					c.logger.Warn("GRPC health monitor failed", slog.Any("error", err))
					if err := c.closeInner(); err != nil {
						c.logger.Warn("Failed to close GRPC connection", slog.Any("error", err))
					}
					if c.healthListener != nil {
						c.healthListener.onNotHealthy(c)
					}
					return
				}
			}
		}
	}
}

func (c *connection) isOpen() bool {
	return connectionStatus(c.status.Load()) == connectionStatusOpen && c.ctx.Err() == nil
}

func (c *connection) closeInner() error {
	if !c.status.CompareAndSwap(uint32(connectionStatusOpen), uint32(connectionStatusClosed)) {
		c.cancel()
		return nil
	}
	c.cancel()
	return c.conn.Close()
}

func (c *connection) Close() error {
	err := c.closeInner()
	c.wg.Wait()
	return err
}
