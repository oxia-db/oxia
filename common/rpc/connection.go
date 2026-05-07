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
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/auth"
	"github.com/oxia-db/oxia/common/process"
)

const (
	defaultGrpcClientKeepAliveTime       = 10 * time.Second
	defaultGrpcClientKeepAliveTimeout    = 3 * time.Second
	defaultGrpcClientPermitWithoutStream = true
)

type connectionFailureCallback func(target string)

type connection struct {
	*grpc.ClientConn

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	target            string
	healthClient      grpc_health_v1.HealthClient
	onFailure         connectionFailureCallback
	healthPingTimeout time.Duration
	healthInterval    time.Duration
	closeOnce         sync.Once
	disconnected      atomic.Bool
	log               *slog.Logger
}

var _ io.Closer = (*connection)(nil)
var _ grpc.ClientConnInterface = (*connection)(nil)

func newConnection(
	target string,
	tlsConf *tls.Config,
	authentication auth.Authentication,
	dialOptions []grpc.DialOption,
	onFailure connectionFailureCallback,
) (*connection, error) {
	return newConnectionWithHealthConfig(
		target,
		tlsConf,
		authentication,
		dialOptions,
		onFailure,
		defaultGrpcClientKeepAliveTime,
		defaultGrpcClientKeepAliveTimeout,
	)
}

func newConnectionWithHealthConfig(
	target string,
	tlsConf *tls.Config,
	authentication auth.Authentication,
	dialOptions []grpc.DialOption,
	onFailure connectionFailureCallback,
	healthInterval time.Duration,
	healthPingTimeout time.Duration,
) (*connection, error) {
	log := slog.With(slog.String("component", "rpc-connection"), slog.String("target", target))
	log.Debug("Creating new GRPC connection")

	actualAddress := target
	transportCredential := insecure.NewCredentials()
	if strings.HasPrefix(target, AddressSchemaTLS) {
		actualAddress, _ = strings.CutPrefix(target, AddressSchemaTLS)
		transportCredential = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	if tlsConf != nil {
		transportCredential = credentials.NewTLS(tlsConf)
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredential),
		grpc.WithChainStreamInterceptor(grpcprometheus.StreamClientInterceptor),
		grpc.WithChainUnaryInterceptor(grpcprometheus.UnaryClientInterceptor),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			PermitWithoutStream: defaultGrpcClientPermitWithoutStream,
			Time:                defaultGrpcClientKeepAliveTime,
			Timeout:             defaultGrpcClientKeepAliveTimeout,
		}),
	}
	if authentication != nil {
		options = append(options, grpc.WithPerRPCCredentials(authentication))
	}
	options = append(options, dialOptions...)

	clientConn, err := grpc.NewClient(actualAddress, options...)
	if err != nil {
		return nil, errors.Wrapf(err, "error connecting to %s", target)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &connection{
		ClientConn:        clientConn,
		target:            target,
		healthClient:      grpc_health_v1.NewHealthClient(clientConn),
		onFailure:         onFailure,
		healthPingTimeout: healthPingTimeout,
		healthInterval:    healthInterval,
		ctx:               ctx,
		cancel:            cancel,
		log:               log,
	}
	c.wg.Go(func() {
		process.DoWithLabels(
			c.ctx,
			map[string]string{
				"component": "rpc-connection-health-ping",
				"target":    c.target,
			},
			c.healthCheckLoop,
		)
	})
	return c, nil
}

func (c *connection) healthCheckLoop() {
	ticker := time.NewTicker(c.healthInterval)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(c.ctx, c.healthPingTimeout)
		response, err := c.healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: ""})
		cancel()

		if status.Code(err) == codes.Unimplemented {
			c.log.Debug("Connection health ping is unsupported")
			return
		}

		if err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
			if err != nil {
				c.log.Warn("Connection health ping failed", slog.Any("error", err))
			} else {
				c.log.Warn("Connection health ping returned unhealthy status", slog.Any("status", response.GetStatus()))
			}
			if c.disconnected.CompareAndSwap(false, true) {
				c.notifyDisconnect()
				return
			}
		}

		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *connection) notifyDisconnect() {
	go process.DoWithLabels(
		context.Background(),
		map[string]string{
			"component": "rpc-connection-failure-callback",
			"target":    c.target,
		},
		func() {
			c.onFailure(c.target)
		},
	)
}

func (c *connection) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
		err = c.ClientConn.Close()
	})
	return err
}
