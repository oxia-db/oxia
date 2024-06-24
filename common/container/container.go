// Copyright 2023 StreamNative, Inc.
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

package container

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"os"

	"github.com/streamnative/oxia/server/auth"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"

	"github.com/streamnative/oxia/common"
)

const (
	maxGrpcFrameSize = 256 * 1024 * 1024

	ReadinessProbeService = "oxia-readiness"
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
		),
	}
	registerFunc(c.server)
	grpcprometheus.Register(c.server)

	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return nil, err
	}

	c.port = listener.Addr().(*net.TCPAddr).Port

	c.log = slog.With(
		slog.String("grpc-server", name),
		slog.String("bindAddress", listener.Addr().String()),
	)

	go common.DoWithLabels(
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
