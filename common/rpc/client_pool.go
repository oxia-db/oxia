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
	"time"

	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/oxia-db/oxia/common/auth"
	"github.com/oxia-db/oxia/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
)

const DefaultRpcTimeout = 30 * time.Second
const AddressSchemaTLS = "tls://"
const (
	defaultGrpcClientKeepAliveTime       = time.Second * 10
	defaultGrpcClientKeepAliveTimeout    = time.Second * 5
	defaultGrpcClientPermitWithoutStream = true
)

type ClientPool interface {
	io.Closer
	GetClientRpc(target string) (proto.OxiaClientClient, error)
	GetHealthRpc(target string) (grpc_health_v1.HealthClient, io.Closer, error)
	GetCoordinationRpc(target string) (proto.OxiaCoordinationClient, error)
	GetReplicationRpc(target string) (proto.OxiaLogReplicationClient, error)
	GetAminRpc(target string) (proto.OxiaAdminClient, error)

	// Clear all the pooled client instances for the given target
	Clear(target string)
}

type clientPool struct {
	sync.RWMutex
	connections map[string]*grpc.ClientConn

	tls            *tls.Config
	authentication auth.Authentication
	log            *slog.Logger
}

func (cp *clientPool) GetAminRpc(target string) (proto.OxiaAdminClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}
	return proto.NewOxiaAdminClient(cnx), nil
}

func NewClientPool(tlsConf *tls.Config, authentication auth.Authentication) ClientPool {
	return &clientPool{
		connections:    make(map[string]*grpc.ClientConn),
		tls:            tlsConf,
		authentication: authentication,
		log: slog.With(
			slog.String("component", "client-pool"),
		),
	}
}

func (cp *clientPool) Close() error {
	cp.Lock()
	defer cp.Unlock()

	for target, cnx := range cp.connections {
		err := cnx.Close()
		if err != nil {
			cp.log.Warn(
				"Failed to close GRPC connection",
				slog.String("server_address", target),
				slog.Any("error", err),
			)
		}
	}
	return nil
}

func (cp *clientPool) GetHealthRpc(target string) (grpc_health_v1.HealthClient, io.Closer, error) {
	// Skip the pooling for health-checks
	cnx, err := cp.newConnection(target)
	if err != nil {
		return nil, nil, err
	}

	return grpc_health_v1.NewHealthClient(cnx), cnx, nil
}

func (cp *clientPool) GetClientRpc(target string) (proto.OxiaClientClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}

	return &loggingClientRpc{target, proto.NewOxiaClientClient(cnx)}, nil
}

func (cp *clientPool) GetCoordinationRpc(target string) (proto.OxiaCoordinationClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}

	return proto.NewOxiaCoordinationClient(cnx), nil
}

func (cp *clientPool) GetReplicationRpc(target string) (proto.OxiaLogReplicationClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}

	return proto.NewOxiaLogReplicationClient(cnx), nil
}

func (cp *clientPool) Clear(target string) {
	cp.Lock()
	defer cp.Unlock()

	if cnx, ok := cp.connections[target]; ok {
		if err := cnx.Close(); err != nil {
			cp.log.Warn(
				"Failed to close GRPC connection",
				slog.String("server_address", target),
				slog.Any("error", err),
			)
		}

		delete(cp.connections, target)
	}
}

func (cp *clientPool) getConnectionFromPool(target string) (grpc.ClientConnInterface, error) {
	cp.RLock()
	cnx, ok := cp.connections[target]
	cp.RUnlock()
	if ok {
		return cnx, nil
	}

	cp.Lock()
	defer cp.Unlock()

	cnx, ok = cp.connections[target]
	if ok {
		return cnx, nil
	}

	cnx, err := cp.newConnection(target)
	if err != nil {
		return nil, err
	}
	cp.connections[target] = cnx
	return cnx, nil
}

func (cp *clientPool) newConnection(target string) (*grpc.ClientConn, error) {
	cp.log.Debug(
		"Creating new GRPC connection",
		slog.String("server_address", target),
	)

	tcs := cp.getTransportCredential(target)

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(tcs),
		grpc.WithStreamInterceptor(grpcprometheus.StreamClientInterceptor),
		grpc.WithUnaryInterceptor(grpcprometheus.UnaryClientInterceptor),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			PermitWithoutStream: defaultGrpcClientPermitWithoutStream,
			Time:                defaultGrpcClientKeepAliveTime,
			Timeout:             defaultGrpcClientKeepAliveTimeout,
		}),
	}
	if cp.authentication != nil {
		options = append(options, grpc.WithPerRPCCredentials(cp.authentication))
	}
	cnx, err := grpc.NewClient(cp.getActualAddress(target), options...)
	if err != nil {
		return nil, errors.Wrapf(err, "error connecting to %s", target)
	}

	return cnx, nil
}

func (*clientPool) getActualAddress(target string) string {
	if strings.HasPrefix(target, AddressSchemaTLS) {
		after, _ := strings.CutPrefix(target, AddressSchemaTLS)
		return after
	}
	return target
}

//nolint:gosec
func (cp *clientPool) getTransportCredential(target string) credentials.TransportCredentials {
	tcs := insecure.NewCredentials()
	if strings.HasPrefix(target, AddressSchemaTLS) {
		tcs = credentials.NewTLS(&tls.Config{})
	}
	if cp.tls != nil {
		tcs = credentials.NewTLS(cp.tls)
	}
	return tcs
}

func GetPeer(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	return p.Addr.String()
}
