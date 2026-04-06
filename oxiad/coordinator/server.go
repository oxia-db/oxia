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
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/multierr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"

	"github.com/oxia-db/oxia/common/entity"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"

	"github.com/oxia-db/oxia/oxiad/common/metric"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"

	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordinatorrpc "github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/common/rpc"
)

type GrpcServer struct {
	// concurrent control
	ctx              context.Context
	ctxCancel        context.CancelFunc
	wg               sync.WaitGroup
	logger           *slog.Logger
	watchableOptions *commonoption.Watch[*option.Options]

	grpcServer   rpc2.GrpcServer
	adminServer  rpc2.GrpcServer
	healthServer *health.Server
	clientPool   rpc.ClientPool
	metrics      *metric.PrometheusMetrics

	// coordinatorMu protects coordinator, which is
	// swapped by monitorLease and read by Close.
	coordinatorMu  sync.RWMutex
	coordinator    Coordinator
	leaseWatch     *concurrent.Watch[metadata.LeaseStatus]
	electionDoneCh <-chan struct{}

	metadataProvider                 metadata.Provider
	clusterConfigProvider            func() (model.ClusterConfig, error)
	clusterConfigChangeNotifications chan any
	rpcProvider                      coordinatorrpc.Provider
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

	if err := cc.Validate(); err != nil {
		return cc, err
	}

	return cc, nil
}

func NewGrpcServer(parent context.Context, watchableOptions *commonoption.Watch[*option.Options]) (*GrpcServer, error) {
	options, _ := watchableOptions.Load()
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

	healthServer := health.NewServer()

	internalServer := options.Server.Internal
	internalServerTLS, err := internalServer.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	grpcServer, err := rpc2.Default.StartGrpcServer("coordinator", internalServer.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
		grpc_health_v1.RegisterHealthServer(registrar, healthServer)
	}, internalServerTLS, &auth.Disabled)
	if err != nil {
		return nil, err
	}

	controller := &options.Controller
	controllerTLS, err := controller.TLS.TryIntoClientTLSConf()
	if err != nil {
		return nil, err
	}
	clientPool := rpc.NewClientPool(controllerTLS, nil)
	rpcClient := coordinatorrpc.NewRpcProvider(clientPool)

	leaseWatch, electionDoneCh := metadataProvider.RunElection(parent)
	for leaseWatch.Get() != metadata.LeaseStatusAcquired {
		<-leaseWatch.Changed()
	}

	coordinatorInstance, err := NewCoordinator(metadataProvider, leaseWatch, clusterConfigProvider, clusterConfigChangeNotifications, rpcClient) //nolint:contextcheck
	if err != nil {
		return nil, err
	}

	adminSv := options.Server.Admin
	adminSvTLS, err := adminSv.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	admin := newAdminServer(coordinatorInstance.StatusResource(), clusterConfigProvider, coordinatorInstance)
	adminGrpcServer, err := rpc2.Default.StartGrpcServer("admin", adminSv.BindAddress, func(registrar grpc.ServiceRegistrar) { //nolint:contextcheck
		proto.RegisterOxiaAdminServer(registrar, admin)
	}, adminSvTLS, &auth.Disabled)
	if err != nil {
		return nil, err
	}

	metrics := options.Observability.Metric

	var metricsServer *metric.PrometheusMetrics
	if metrics.IsEnabled() {
		metricsServer, err = metric.Start(metrics.BindAddress) //nolint:contextcheck
		if err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(parent)
	server := GrpcServer{
		ctx:              ctx,
		ctxCancel:        cancel,
		wg:               sync.WaitGroup{},
		logger:           slog.With(slog.String("component", "grpc-server")),
		watchableOptions: watchableOptions,
		grpcServer:       grpcServer,
		adminServer:      adminGrpcServer,
		healthServer:     healthServer,
		clientPool:       clientPool,
		coordinator:      coordinatorInstance,
		metrics:          metricsServer,

		metadataProvider:                 metadataProvider,
		clusterConfigProvider:            clusterConfigProvider,
		clusterConfigChangeNotifications: clusterConfigChangeNotifications,
		rpcProvider:                      rpcClient,
		leaseWatch:                       leaseWatch,
		electionDoneCh:                   electionDoneCh,
	}
	server.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "configuration-watcher",
		}, server.backgroundHandleConfChange)
	})
	server.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "lease-monitor",
		}, server.monitorLease)
	})

	return &server, nil
}

func (s *GrpcServer) backgroundHandleConfChange() {
	var coordinatorOptions *option.Options
	var ver uint64
	var err error
	for {
		coordinatorOptions, ver, err = s.watchableOptions.Wait(s.ctx, ver)
		if err != nil {
			s.logger.Warn("exit background configuration watch goroutine due to an error", slog.Any("error", err))
			return
		}

		s.logger.Info("configuration options has changed. processing the dynamic updates.")
		logOptions := &coordinatorOptions.Observability.Log
		if logging.ReconfigureLogger(logOptions) {
			s.logger.Info("reconfigured log options", slog.Any("options", logOptions))
		}
	}
}

func (s *GrpcServer) monitorLease() {
	w := s.leaseWatch
	for {
		select {
		case <-w.Changed():
		case <-s.ctx.Done():
			return
		}

		status := w.Get()
		switch status {
		case metadata.LeaseStatusNotAcquired:
			s.logger.Info("Lease lost, closing coordinator")
			s.coordinatorMu.Lock()
			if s.coordinator != nil {
				if err := s.coordinator.Close(); err != nil {
					s.logger.Warn("Failed to close coordinator", slog.Any("error", err))
				}
				s.coordinator = nil
			}
			s.coordinatorMu.Unlock()

		case metadata.LeaseStatusAcquired:
			s.coordinatorMu.RLock()
			hasCoordinator := s.coordinator != nil
			s.coordinatorMu.RUnlock()
			if hasCoordinator {
				continue
			}

			s.logger.Info("Lease acquired, creating coordinator")
			coordinatorInstance, err := NewCoordinator(
				s.metadataProvider,
				w,
				s.clusterConfigProvider,
				s.clusterConfigChangeNotifications,
				s.rpcProvider,
			)
			if err != nil {
				s.logger.Error("Failed to create coordinator", slog.Any("error", err))
				continue
			}

			s.coordinatorMu.Lock()
			s.coordinator = coordinatorInstance
			s.coordinatorMu.Unlock()
		}
	}
}

func (s *GrpcServer) Close() error {
	// Cancel context to signal goroutines to stop.
	s.ctxCancel()
	s.wg.Wait()

	// Wait for the election goroutine to finish.
	<-s.electionDoneCh

	var err error
	s.healthServer.Shutdown()

	s.coordinatorMu.RLock()
	coord := s.coordinator
	s.coordinatorMu.RUnlock()

	err = multierr.Combine(
		s.clientPool.Close(),
		s.grpcServer.Close(),
		s.adminServer.Close(),
		s.metadataProvider.Close(),
	)
	if coord != nil {
		err = multierr.Append(err, coord.Close())
	}
	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}
	return err
}
