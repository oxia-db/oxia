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
	"log/slog"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/mitchellh/mapstructure"
	"github.com/oxia-db/oxia/oxiad/common/entity"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/oxiad/common/metric"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordinatorrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/rpc"
)

type GrpcServer struct {
	grpcServer   rpc2.GrpcServer
	adminServer  rpc2.GrpcServer
	healthServer *health.Server
	coordinator  Coordinator
	clientPool   rpc.ClientPool
	metrics      *metric.PrometheusMetrics
}

func setConfigPath(cluster *option.ClusterOptions, v *viper.Viper) error {
	v.SetConfigType("yaml")
	configPath := cluster.ConfigPath

	if strings.HasPrefix(configPath, "configmap:") {
		err := v.AddRemoteProvider("configmap", "endpoint", configPath)
		if err != nil {
			slog.Error("Failed to add remote provider", slog.Any("error", err))
			return err
		}
		return v.WatchRemoteConfigOnChannel()
	}
	if configPath == "" {
		v.AddConfigPath("/oxia/conf")
		v.AddConfigPath(".")
	}

	v.SetConfigFile(configPath)
	v.WatchConfig()
	return nil
}

func loadClusterConfig(cluster *option.ClusterOptions, v *viper.Viper) (model.ClusterConfig, error) {
	cc := model.ClusterConfig{}

	var err error

	if strings.HasPrefix(cluster.ConfigPath, "configmap:") {
		err = v.ReadRemoteConfig()
	} else {
		err = v.ReadInConfig()
	}

	if err != nil {
		return cc, err
	}

	if err := v.Unmarshal(&cc, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		entity.OptBooleanViperHook(),
		mapstructure.StringToTimeDurationHookFunc(), // default hook
		mapstructure.StringToSliceHookFunc(","),     // default hook
	))); err != nil {
		return cc, errors.Wrap(err, "failed to load cluster config")
	}

	return cc, nil
}

func NewGrpcServer(options *option.Options) (*GrpcServer, error) {
	slog.Info("Starting Oxia coordinator", slog.Any("options", options))
	v := viper.New()

	clusterConfigChangeNotifications := make(chan any)
	v.OnConfigChange(func(_ fsnotify.Event) {
		clusterConfigChangeNotifications <- nil
	})

	clusterConfigProvider := func() (model.ClusterConfig, error) {
		return loadClusterConfig(&options.Cluster, v)
	}

	if err := setConfigPath(&options.Cluster, v); err != nil {
		return nil, err
	}

	if _, err := loadClusterConfig(&options.Cluster, v); err != nil {
		return nil, err
	}

	meta := &options.Metadata

	var metadataProvider metadata.Provider
	switch meta.ProviderName {
	case metadata.ProviderNameMemory:
		metadataProvider = metadata.NewMetadataProviderMemory()
	case metadata.ProviderNameFile:
		metadataProvider = metadata.NewMetadataProviderFile(meta.File.Path)
	case metadata.ProviderNameConfigmap:
		k8sConfig := metadata.NewK8SClientConfig()
		metadataProvider = metadata.NewMetadataProviderConfigMap(metadata.NewK8SClientset(k8sConfig),
			meta.Kubernetes.Namespace, meta.Kubernetes.ConfigMapName)
	case metadata.ProviderNameRaft:
		var err error
		metadataProvider, err = metadata.NewMetadataProviderRaft(
			meta.Raft.Address, meta.Raft.BootstrapNodes, meta.Raft.DataDir)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create raft metadata provider")
		}
	default:
		return nil, errors.New(`must be one of "memory", "configmap" or "file"`)
	}

	controller := &options.Controller
	controllerTls, err := controller.TLS.MakeClientTLSConf()
	if err != nil {
		return nil, err
	}
	clientPool := rpc.NewClientPool(controllerTls, nil)
	rpcClient := coordinatorrpc.NewRpcProvider(clientPool)

	coordinatorInstance, err := NewCoordinator(metadataProvider, clusterConfigProvider, clusterConfigChangeNotifications, rpcClient)
	if err != nil {
		return nil, err
	}

	healthServer := health.NewServer()

	internalServer := options.Server.Internal
	internalServerTls, err := internalServer.TLS.MakeServerTLSConf()
	if err != nil {
		return nil, err
	}
	grpcServer, err := rpc2.Default.StartGrpcServer("coordinator", internalServer.BindAddress, func(registrar grpc.ServiceRegistrar) {
		grpc_health_v1.RegisterHealthServer(registrar, healthServer)
	}, internalServerTls, &auth.Disabled)
	if err != nil {
		return nil, err
	}

	adminSv := options.Server.Admin
	adminSvTls, err := adminSv.TLS.MakeServerTLSConf()
	if err != nil {
		return nil, err
	}
	admin := newAdminServer(coordinatorInstance.StatusResource(), clusterConfigProvider)
	adminGrpcServer, err := rpc2.Default.StartGrpcServer("admin", adminSv.BindAddress, func(registrar grpc.ServiceRegistrar) {
		proto.RegisterOxiaAdminServer(registrar, admin)
	}, adminSvTls, &auth.Disabled)
	if err != nil {
		return nil, err
	}

	metrics := options.Observability.Metric

	var metricsServer *metric.PrometheusMetrics
	if metrics.IsEnabled() {
		metricsServer, err = metric.Start(metrics.BindAddress)
		if err != nil {
			return nil, err
		}
	}

	return &GrpcServer{
		grpcServer:   grpcServer,
		adminServer:  adminGrpcServer,
		healthServer: healthServer,
		clientPool:   clientPool,
		coordinator:  coordinatorInstance,
		metrics:      metricsServer,
	}, nil
}

func (s *GrpcServer) Close() error {
	var err error
	s.healthServer.Shutdown()
	err = multierr.Combine(
		s.clientPool.Close(),
		s.grpcServer.Close(),
		s.adminServer.Close(),
		s.coordinator.Close(),
	)
	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}
	return err
}
