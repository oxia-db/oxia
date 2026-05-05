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
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"

	"github.com/oxia-db/oxia/common/auth"
	"github.com/oxia-db/oxia/common/proto"
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
	GetHealthRpc(target string) (grpc_health_v1.HealthClient, error)
	GetCoordinationRpc(target string) (proto.OxiaCoordinationClient, error)
	GetReplicationRpc(target string) (proto.OxiaLogReplicationClient, error)
	GetAminRpc(target string) (proto.OxiaAdminClient, error)

	// Clear all the pooled client instances for the given target
	Clear(target string)
}

type clientPool struct {
	sync.RWMutex
	connections map[string]*connection

	tls            *tls.Config
	authentication auth.Authentication
	dialOptions    []grpc.DialOption
	log            *slog.Logger
}

func (cp *clientPool) GetAminRpc(target string) (proto.OxiaAdminClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}
	return proto.NewOxiaAdminClient(cnx), nil
}

func NewClientPool(tlsConf *tls.Config, authentication auth.Authentication, dialOptions ...grpc.DialOption) ClientPool {
	return &clientPool{
		connections:    make(map[string]*connection),
		tls:            tlsConf,
		authentication: authentication,
		dialOptions:    dialOptions,
		log: slog.With(
			slog.String("component", "client-pool"),
		),
	}
}

func (cp *clientPool) Close() error {
	cp.Lock()
	connections := cp.connections
	cp.connections = make(map[string]*connection)
	cp.Unlock()
	for target, cnx := range connections {
		cp.closeConnection(target, cnx)
	}
	return nil
}

func (cp *clientPool) GetHealthRpc(target string) (grpc_health_v1.HealthClient, error) {
	cnx, err := cp.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}

	return grpc_health_v1.NewHealthClient(cnx), nil
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
	cp.removeAndCloseConnection(target)
}

func (cp *clientPool) onNotHealthy(cnx *connection) {
	cp.Lock()
	defer cp.Unlock()

	if cp.connections[cnx.target] == cnx {
		delete(cp.connections, cnx.target)
	}
}

func (cp *clientPool) removeAndCloseConnection(target string) {
	cp.Lock()
	var conn *connection
	var ok bool
	if conn, ok = cp.connections[target]; !ok {
		cp.Unlock()
		return
	}
	delete(cp.connections, target)
	cp.Unlock()

	cp.closeConnection(target, conn)
}

func (cp *clientPool) closeConnection(target string, conn *connection) {
	if err := conn.Close(); err != nil {
		cp.log.Warn(
			"Failed to close the stale GRPC connection",
			slog.String("server_address", target),
			slog.Any("error", err),
		)
	}
}

func (cp *clientPool) getConnectionFromPool(target string) (grpc.ClientConnInterface, error) {
	cp.RLock()
	cnx, ok := cp.connections[target]
	cp.RUnlock()
	if ok && cnx.isOpen() {
		return cnx.conn, nil
	}

	cp.Lock()

	cnx, ok = cp.connections[target]
	if ok && cnx.isOpen() {
		cp.Unlock()
		return cnx.conn, nil
	}
	var staleConnection *connection
	if ok {
		staleConnection = cnx
		delete(cp.connections, target)
	}

	cnx, err := newConnection(context.Background(), target, cp.tls, cp.authentication, cp.dialOptions, cp)
	if err == nil {
		cp.connections[target] = cnx
	}
	cp.Unlock()

	if staleConnection != nil {
		cp.closeConnection(target, staleConnection)
	}
	if err != nil {
		return nil, err
	}
	return cnx.conn, nil
}

func GetPeer(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}

	return p.Addr.String()
}
