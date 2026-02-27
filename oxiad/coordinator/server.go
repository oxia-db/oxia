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

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/common/logging"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"

	"github.com/oxia-db/oxia/oxiad/common/entity"
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
	coordinator  Coordinator
	coordinatorMu sync.RWMutex
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

// createMetadataProvider creates a metadata provider based on the configured provider name.
// For raft providers, the provider is expensive to create and should be reused across lifecycle iterations.
func createMetadataProvider(meta *option.MetadataOptions) (metadata.Provider, error) {
	switch meta.ProviderName {
	case metadata.ProviderNameMemory:
		return metadata.NewMetadataProviderMemory(), nil
	case metadata.ProviderNameFile:
		return metadata.NewMetadataProviderFile(meta.File.Path), nil
	case metadata.ProviderNameConfigmap:
		k8sConfig := metadata.NewK8SClientConfig()
		return metadata.NewMetadataProviderConfigMap(metadata.NewK8SClientset(k8sConfig),
			meta.Kubernetes.Namespace, meta.Kubernetes.ConfigMapName), nil
	case metadata.ProviderNameRaft:
		return metadata.NewMetadataProviderRaft(
			meta.Raft.Address, meta.Raft.BootstrapNodes, meta.Raft.DataDir)
	default:
		return nil, errors.New(`must be one of "memory", "configmap", "file" or "raft"`)
	}
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

	controller := &options.Controller
	controllerTLS, err := controller.TLS.TryIntoClientTLSConf()
	if err != nil {
		return nil, err
	}
	clientPool := rpc.NewClientPool(controllerTLS, nil)
	rpcClient := coordinatorrpc.NewRpcProvider(clientPool)

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

	adminSv := options.Server.Admin
	adminSvTLS, err := adminSv.TLS.TryIntoServerTLSConf()
	if err != nil {
		return nil, err
	}
	admin := newAdminServer(clusterConfigProvider)
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
	server := &GrpcServer{
		ctx:              ctx,
		ctxCancel:        cancel,
		wg:               sync.WaitGroup{},
		logger:           slog.With(slog.String("component", "grpc-server")),
		watchableOptions: watchableOptions,
		grpcServer:       grpcServer,
		adminServer:      adminGrpcServer,
		healthServer:     healthServer,
		clientPool:       clientPool,
		metrics:          metricsServer,
	}

	// Start in NOT_SERVING — the lifecycle loop will transition to SERVING once leader
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// Start the coordinator lifecycle loop in the background
	server.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "coordinator-lifecycle",
		}, func() {
			server.coordinatorLifecycleLoop(&options.Metadata, clusterConfigProvider, clusterConfigChangeNotifications, rpcClient)
		})
	})

	server.wg.Go(func() {
		process.DoWithLabels(ctx, map[string]string{
			"component": "configuration-watcher",
		}, server.backgroundHandleConfChange)
	})

	return server, nil
}

func (s *GrpcServer) coordinatorLifecycleLoop(
	meta *option.MetadataOptions,
	clusterConfigProvider func() (model.ClusterConfig, error),
	clusterConfigNotificationsCh chan any,
	rpcClient coordinatorrpc.Provider,
) {
	// For raft, create the provider once and reuse it
	var raftProvider metadata.Provider
	if meta.ProviderName == metadata.ProviderNameRaft {
		var err error
		raftProvider, err = createMetadataProvider(meta)
		if err != nil {
			s.logger.Error("Failed to create raft metadata provider", slog.Any("error", err))
			return
		}
		defer func() {
			if err := raftProvider.Close(); err != nil {
				s.logger.Warn("Failed to close raft metadata provider", slog.Any("error", err))
			}
		}()
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// 1. Create or reuse metadata provider
		var metadataProvider metadata.Provider
		if meta.ProviderName == metadata.ProviderNameRaft {
			metadataProvider = raftProvider
		} else {
			var err error
			metadataProvider, err = createMetadataProvider(meta)
			if err != nil {
				s.logger.Error("Failed to create metadata provider", slog.Any("error", err))
				return
			}
		}

		// 2. Set health to NOT_SERVING
		s.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		// 3. Wait to become leader (blocks for standby, cancellable)
		s.logger.Info("Waiting to become leader")
		if err := metadataProvider.WaitToBecomeLeader(s.ctx); err != nil {
			s.logger.Info("WaitToBecomeLeader interrupted", slog.Any("error", err))
			if meta.ProviderName != metadata.ProviderNameRaft {
				_ = metadataProvider.Close()
			}
			return
		}
		s.logger.Info("This coordinator is now leader")

		// 4. Create coordinator (now instant — no WaitToBecomeLeader inside)
		coordinatorInstance, err := NewCoordinator(metadataProvider, clusterConfigProvider, clusterConfigNotificationsCh, rpcClient) //nolint:contextcheck
		if err != nil {
			s.logger.Error("Failed to create coordinator", slog.Any("error", err))
			if meta.ProviderName != metadata.ProviderNameRaft {
				_ = metadataProvider.Close()
			}
			continue
		}

		// 5. Set coordinator reference
		s.coordinatorMu.Lock()
		s.coordinator = coordinatorInstance
		s.coordinatorMu.Unlock()

		// 6. Set health to SERVING
		s.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

		// 7. Wait for coordinator to finish (fatal error or context cancellation)
		select {
		case <-coordinatorInstance.Done():
			s.logger.Warn("Coordinator reported fatal error, restarting", slog.Any("error", coordinatorInstance.Err()))
		case <-s.ctx.Done():
		}

		// 8. Set health to NOT_SERVING
		s.healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		// 9. Close coordinator, clear reference
		if err := coordinatorInstance.Close(); err != nil {
			s.logger.Warn("Failed to close coordinator", slog.Any("error", err))
		}
		s.coordinatorMu.Lock()
		s.coordinator = nil
		s.coordinatorMu.Unlock()

		// 10. Close metadata provider (except raft)
		if meta.ProviderName != metadata.ProviderNameRaft {
			if err := metadataProvider.Close(); err != nil {
				s.logger.Warn("Failed to close metadata provider", slog.Any("error", err))
			}
		}

		// 11. If ctx.Done → return, else → loop
		select {
		case <-s.ctx.Done():
			return
		default:
		}
	}
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

func (s *GrpcServer) Close() error {
	// sync close the background task first
	s.ctxCancel()
	s.wg.Wait()

	var err error
	s.healthServer.Shutdown()

	s.coordinatorMu.RLock()
	coord := s.coordinator
	s.coordinatorMu.RUnlock()

	err = multierr.Combine(
		s.clientPool.Close(),
		s.grpcServer.Close(),
		s.adminServer.Close(),
	)
	if coord != nil {
		err = multierr.Append(err, coord.Close())
	}
	if s.metrics != nil {
		err = multierr.Append(err, s.metrics.Close())
	}
	return err
}
