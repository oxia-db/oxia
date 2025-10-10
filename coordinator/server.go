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

package coordinator

import (
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/coordinator/metadata"
	"github.com/oxia-db/oxia/coordinator/model"
	coordinatorrpc "github.com/oxia-db/oxia/coordinator/rpc"

	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/server/auth"
)

type Config struct {
	InternalServiceAddr              string
	InternalSecureServiceAddr        string
	PeerTLS                          *tls.Config `json:"-"`
	ServerTLS                        *tls.Config `json:"-"`
	MetricsServiceAddr               string
	MetadataProviderName             string
	K8SMetadataNamespace             string
	K8SMetadataConfigMapName         string
	FileMetadataPath                 string
	ClusterConfigProvider            func() (model.ClusterConfig, error) `json:"-"`
	ClusterConfigChangeNotifications chan any                            `json:"-"`
}

func NewConfig() Config {
	return Config{
		InternalServiceAddr:  fmt.Sprintf("localhost:%d", constant.DefaultInternalPort),
		MetricsServiceAddr:   fmt.Sprintf("localhost:%d", constant.DefaultMetricsPort),
		MetadataProviderName: metadata.ProviderNameFile,
	}
}

type GrpcServer struct {
	grpcServer   rpc.GrpcServer
	healthServer *health.Server
	coordinator  Coordinator
	clientPool   rpc.ClientPool
	metrics      *metric.PrometheusMetrics
}

func NewGrpcServer(config Config) (*GrpcServer, error) {
	slog.Info("Starting Oxia coordinator", slog.Any("config", config))

	var metadataProvider metadata.Provider
	switch config.MetadataProviderName {
	case metadata.ProviderNameMemory:
		metadataProvider = metadata.NewMetadataProviderMemory()
	case metadata.ProviderNameFile:
		metadataProvider = metadata.NewMetadataProviderFile(config.FileMetadataPath)
	case metadata.ProviderNameConfigmap:
		k8sConfig := metadata.NewK8SClientConfig()
		metadataProvider = metadata.NewMetadataProviderConfigMap(metadata.NewK8SClientset(k8sConfig),
			config.K8SMetadataNamespace, config.K8SMetadataConfigMapName)
	default:
		return nil, errors.New(`must be one of "memory", "configmap" or "file"`)
	}

	clientPool := rpc.NewClientPool(config.PeerTLS, nil)
	rpcClient := coordinatorrpc.NewRpcProvider(clientPool)

	coordinatorInstance, err := NewCoordinator(metadataProvider, config.ClusterConfigProvider, config.ClusterConfigChangeNotifications, rpcClient)
	if err != nil {
		return nil, err
	}

	healthServer := health.NewServer()

	grpcServer, err := rpc.Default.StartGrpcServer("coordinator", config.InternalServiceAddr, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, healthServer)
	}, config.ServerTLS, &auth.Disabled)
	if err != nil {
		return nil, err
	}
	metrics, err := metric.Start(config.MetricsServiceAddr)
	if err != nil {
		return nil, err
	}

	return &GrpcServer{
		grpcServer:   grpcServer,
		healthServer: healthServer,
		clientPool:   clientPool,
		coordinator:  coordinatorInstance,
		metrics:      metrics,
	}, nil
}

func (s *GrpcServer) Close() error {
	s.healthServer.Shutdown()
	return multierr.Combine(
		s.clientPool.Close(),
		s.grpcServer.Close(),
		s.coordinator.Close(),
		s.metrics.Close(),
	)
}
