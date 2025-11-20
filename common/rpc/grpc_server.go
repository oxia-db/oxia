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
	"net"
	"os"
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/oxia-db/oxia/common/process"

	auth "github.com/oxia-db/oxia/common/auth/server"
)

const (
	maxGrpcFrameSize                         = 256 * 1024 * 1024
	defaultGrpcServerKeepAliveMinTime        = 5 * time.Second
	defaultGrpcServerKeepPermitWithoutStream = true
	ReadinessProbeService                    = "oxia-readiness"
)

type GrpcServer interface {
	io.Closer

	Port() int
}

type GrpcProvider interface {
	StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar), tlsConf *tls.Config, options *auth.Options) (GrpcServer, error)
}

var Default = &defaultProvider{}

type defaultProvider struct {
}

func (*defaultProvider) StartGrpcServer(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar), tlsConf *tls.Config, options *auth.Options) (GrpcServer, error) {
	return newDefaultGrpcProvider(name, bindAddress, registerFunc, tlsConf, options)
}

type defaultGrpcServer struct {
	io.Closer
	server *grpc.Server
	port   int
	log    *slog.Logger
}

func newDefaultGrpcProvider(name, bindAddress string, registerFunc func(grpc.ServiceRegistrar),
	tlsConf *tls.Config, authOptions *auth.Options) (GrpcServer, error) {
	tcs := insecure.NewCredentials()
	if tlsConf != nil {
		tcs = credentials.NewTLS(tlsConf)
	}
	streamInterceptors := []grpc.StreamServerInterceptor{
		grpcprometheus.StreamServerInterceptor,
	}
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		grpcprometheus.UnaryServerInterceptor,
	}
	if authOptions.IsEnabled() {
		provider, err := auth.NewAuthenticationProvider(context.Background(), *authOptions)
		if err != nil {
			slog.Error("Failed to init authentication provider",
				slog.Any("authOptions", *authOptions),
				slog.Any("error", err))
			return nil, err
		}
		delegator, err := auth.NewGrpcAuthenticationDelegator(provider)
		if err != nil {
			slog.Error("Failed to init grpc authentication delegator",
				slog.Any("authOptions", *authOptions),
				slog.Any("error", err))
			return nil, err
		}
		unaryInterceptors = append(unaryInterceptors, delegator.GetUnaryInterceptor())
		streamInterceptors = append(streamInterceptors, delegator.GetStreamInterceptor())
	}

	c := &defaultGrpcServer{
		server: grpc.NewServer(
			grpc.Creds(tcs),
			grpc.ChainStreamInterceptor(streamInterceptors...),
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
			grpc.MaxRecvMsgSize(maxGrpcFrameSize),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				MinTime:             defaultGrpcServerKeepAliveMinTime,
				PermitWithoutStream: defaultGrpcServerKeepPermitWithoutStream,
			}),
		),
	}
	registerFunc(c.server)
	grpcprometheus.Register(c.server)

	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", bindAddress)
	if err != nil {
		return nil, err
	}

	c.port = listener.Addr().(*net.TCPAddr).Port

	c.log = slog.With(
		slog.String("grpc-server", name),
		slog.String("bindAddress", listener.Addr().String()),
	)

	go process.DoWithLabels(
		context.Background(),
		map[string]string{
			"oxia": name,
			"bind": listener.Addr().String(),
		},
		func() {
			if err := c.server.Serve(listener); err != nil {
				c.log.Error(
					"Failed to start serving",
					slog.Any("error", err),
				)
				os.Exit(1)
			}
		},
	)

	c.log.Info("Started Grpc server")

	return c, nil
}

func (c *defaultGrpcServer) Port() int {
	return c.port
}

func (c *defaultGrpcServer) Close() error {
	c.server.GracefulStop()
	c.log.Info("Stopped Grpc server")
	return nil
}
