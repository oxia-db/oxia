// Copyright 2023-2026 The Oxia Authors
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

package metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	gproto "google.golang.org/protobuf/proto"

	commonobject "github.com/oxia-db/oxia/common/object"
	commonproto "github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type Metadata interface {
	io.Closer

	WaitToBecomeLeader() (lost <-chan struct{}, err error)
	GetSelf() (*commonproto.Coordinator, error)
	GetLeader() (*commonproto.Coordinator, error)
	GetInstanceID() string

	// ReserveShardIDs and the status Create/Update/Delete methods retry until
	// the write succeeds or the metadata context is canceled. When they give
	// up, they return an error, or report that nothing was created or deleted:
	// the caller must not act as if the write was persisted. The status
	// updates also fail with ErrNotFound when the namespace or shard is gone.
	ReserveShardIDs(count uint32) (int64, error)

	CreateNamespaceStatus(name string, status *commonproto.NamespaceStatus) bool
	ListNamespaceStatus() map[string]commonobject.Borrowed[*commonproto.NamespaceStatus]
	GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool)
	UpdateNamespaceStatus(name string, status *commonproto.NamespaceStatus) error
	DeleteNamespaceStatus(name string) commonobject.Borrowed[*commonproto.NamespaceStatus]

	GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*commonproto.ShardMetadata], bool)
	UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) error
	DeleteShardStatus(namespace string, shard int64) error
	// InitShardSplit records the start of a shard split: the parent is marked
	// as splitting and both children are created in the same update, so a
	// reader never sees one without the other. It fails with
	// ErrFailedPrecondition when the parent cannot be split or the split point
	// and child ids are not usable.
	InitShardSplit(namespace string, parent, left, right int64, splitPoint uint32,
		leftEnsemble, rightEnsemble []*commonproto.DataServerIdentity) error

	GetConfig() commonobject.Borrowed[*commonproto.ClusterConfiguration]
	SubscribeConfig() *commonwatch.Receiver[provider.Versioned[*commonproto.ClusterConfiguration]]
	GetLoadBalancer() commonobject.Borrowed[*commonproto.LoadBalancer]

	CreateNamespace(namespace *commonproto.Namespace) error
	PatchNamespace(namespace *commonproto.Namespace) (*commonproto.Namespace, error)
	DeleteNamespace(name string) (*commonproto.Namespace, error)
	ListNamespace() map[string]commonobject.Borrowed[*commonproto.Namespace]
	GetNamespace(namespace string) (commonobject.Borrowed[*commonproto.Namespace], bool)

	CreateDataServer(dataServer *commonproto.DataServer) error
	PatchDataServer(dataServer *commonproto.DataServer) (*commonproto.DataServer, error)
	DeleteDataServer(name string) (*commonproto.DataServer, error)
	ListDataServer() map[string]commonobject.Borrowed[*commonproto.DataServer]
	GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool)
}

type EnsembleSupplier func(
	namespaceConfig *commonproto.Namespace,
	status *commonproto.ClusterStatus,
) ([]*commonproto.DataServerIdentity, error)

type coordinatorMetadata struct {
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	statusProvider provider.Provider[*commonproto.ClusterStatus]
	statusLock     sync.Mutex

	configProvider provider.Provider[*commonproto.ClusterConfiguration]
	configLock     sync.Mutex
	name           string
}

func newMetadata(
	ctx context.Context,
	statusProvider provider.Provider[*commonproto.ClusterStatus],
	configProvider provider.Provider[*commonproto.ClusterConfiguration],
	name string,
) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		logger:         slog.With(slog.String("component", "coordinator-metadata")),
		ctx:            metadataCtx,
		cancel:         cancel,
		statusProvider: statusProvider,
		configProvider: configProvider,
		name:           name,
	}
	return m
}

func (m *coordinatorMetadata) computeStatus(fn func(*commonproto.ClusterStatus, metadatacommon.Version) (*commonproto.ClusterStatus, bool)) error {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	current := m.statusProvider.Watch().Load()
	next, changed := fn(metadatacodec.ClusterStatusCodec.Clone(current.Value), current.Version)
	if !changed {
		return nil
	}

	_, err := m.statusProvider.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   next,
		Version: current.Version,
	})
	if errors.Is(err, metadatacommon.ErrBadVersion) {
		panic(err)
	}
	return err
}

func (m *coordinatorMetadata) computeConfig(fn func(*commonproto.ClusterConfiguration, metadatacommon.Version) (*commonproto.ClusterConfiguration, error)) error {
	m.configLock.Lock()
	defer m.configLock.Unlock()

	current := m.configProvider.Watch().Load()
	next, err := fn(metadatacodec.ClusterConfigCodec.Clone(current.Value), current.Version)
	if err != nil {
		return err
	}

	if err := next.Validate(); err != nil {
		return err
	}

	_, err = m.configProvider.Store(provider.Versioned[*commonproto.ClusterConfiguration]{
		Value:   next,
		Version: current.Version,
	})
	return err
}

func (m *coordinatorMetadata) Close() error {
	m.cancel()
	m.wg.Wait()
	return nil
}

func (m *coordinatorMetadata) doStatusRecovery() {
	status := m.statusProvider.Watch().Load().Value
	if status.GetInstanceId() == "" {
		_ = backoff.RetryNotify(func() error {
			return m.computeStatus(func(status *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
				if status.GetInstanceId() != "" {
					return status, false
				}
				status.InstanceId = uuid.NewString()
				return status, true
			})
		}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
			m.logger.Warn(
				"failed to initialize instance id",
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
		})
	}
}

func (m *coordinatorMetadata) GetInstanceID() string {
	return m.statusProvider.Watch().Load().Value.GetInstanceId()
}

func (m *coordinatorMetadata) GetSelf() (*commonproto.Coordinator, error) {
	coordinator, ok := m.configProvider.Watch().Load().Value.GetCoordinator(m.name)
	if !ok {
		return nil, fmt.Errorf("coordinator %q not found in cluster configuration", m.name)
	}
	return coordinator.CloneVT(), nil
}

func (m *coordinatorMetadata) GetLeader() (*commonproto.Coordinator, error) {
	name, err := m.statusProvider.GetLeaderName()
	if err != nil {
		return nil, err
	}
	coordinator, ok := m.configProvider.Watch().Load().Value.GetCoordinator(name)
	if !ok {
		return nil, fmt.Errorf("coordinator %q not found in cluster configuration", name)
	}
	return coordinator.CloneVT(), nil
}

func (m *coordinatorMetadata) WaitToBecomeLeader() (<-chan struct{}, error) {
	m.logger.Info("Waiting to become leader")
	leadershipLost, err := m.statusProvider.WaitToBecomeLeader()
	if err != nil {
		return nil, fmt.Errorf("failed to wait in becoming leader: %w", err)
	}
	m.logger.Info("This coordinator is now leader")
	m.doStatusRecovery()
	return leadershipLost, nil
}

func (m *coordinatorMetadata) ReserveShardIDs(count uint32) (int64, error) {
	var base int64
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(status *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			base = status.GetShardIdGenerator()
			status.ShardIdGenerator += int64(count)
			return status, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to reserve shard ids",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return 0, err
	}
	return base, nil
}

func (m *coordinatorMetadata) CreateNamespaceStatus(name string, status *commonproto.NamespaceStatus) bool {
	created := false
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			if clusterStatus.Namespaces == nil {
				clusterStatus.Namespaces = map[string]*commonproto.NamespaceStatus{}
			}
			if _, exists := clusterStatus.Namespaces[name]; exists {
				return clusterStatus, false
			}
			clusterStatus.Namespaces[name] = status
			created = true
			return clusterStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to create namespace status",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	return created && err == nil
}

func (m *coordinatorMetadata) ListNamespaceStatus() map[string]commonobject.Borrowed[*commonproto.NamespaceStatus] {
	status := m.statusProvider.Watch().Load().Value
	namespaces := make(map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], len(status.GetNamespaces()))
	for name, status := range status.GetNamespaces() {
		namespaces[name] = commonobject.Borrow(status)
	}
	return namespaces
}

func (m *coordinatorMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool) {
	status := m.statusProvider.Watch().Load().Value
	namespaceStatus, exists := status.GetNamespaces()[namespace]
	if !exists {
		return commonobject.Borrowed[*commonproto.NamespaceStatus]{}, false
	}
	return commonobject.Borrow(namespaceStatus), true
}

func (m *coordinatorMetadata) UpdateNamespaceStatus(name string, namespaceStatus *commonproto.NamespaceStatus) error {
	namespaceExists := true
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			if _, exists := clusterStatus.Namespaces[name]; !exists {
				namespaceExists = false
				return clusterStatus, false
			}
			clusterStatus.Namespaces[name] = namespaceStatus
			return clusterStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to update namespace status",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return err
	}
	if !namespaceExists {
		return fmt.Errorf("%w: namespace %q", metadatacommon.ErrNotFound, name)
	}
	return nil
}

func (m *coordinatorMetadata) DeleteNamespaceStatus(name string) commonobject.Borrowed[*commonproto.NamespaceStatus] {
	var namespaceStatus *commonproto.NamespaceStatus
	changed := false
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			ns, exists := clusterStatus.Namespaces[name]
			if !exists {
				return clusterStatus, false
			}
			namespaceStatus = ns
			for shardID, shardMetadata := range ns.Shards {
				if shardMetadata.Status != commonproto.ShardStatusDeleting {
					shardMetadata.Status = commonproto.ShardStatusDeleting
					ns.Shards[shardID] = shardMetadata
					changed = true
				}
			}
			return clusterStatus, changed
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to mark namespace deleting",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return commonobject.Borrowed[*commonproto.NamespaceStatus]{}
	}
	return commonobject.Borrow(namespaceStatus)
}

func (m *coordinatorMetadata) GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*commonproto.ShardMetadata], bool) {
	namespaceStatus, exists := m.GetNamespaceStatus(namespace)
	if !exists {
		return commonobject.Borrowed[*commonproto.ShardMetadata]{}, false
	}
	shardStatus, exists := namespaceStatus.UnsafeBorrow().GetShards()[shard]
	if !exists {
		return commonobject.Borrowed[*commonproto.ShardMetadata]{}, false
	}
	return commonobject.Borrow(shardStatus), true
}

func (m *coordinatorMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) error {
	if shardMetadata != nil {
		shardMetadata = gproto.Clone(shardMetadata).(*commonproto.ShardMetadata) //nolint:revive
	}
	shardExists := true
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			ns, exist := clusterStatus.Namespaces[namespace]
			if !exist {
				shardExists = false
				return clusterStatus, false
			}
			if _, exist = ns.Shards[shard]; !exist {
				shardExists = false
				return clusterStatus, false
			}
			ns.Shards[shard] = shardMetadata
			return clusterStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to update shard metadata",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return err
	}
	if !shardExists {
		return fmt.Errorf("%w: shard %d of namespace %q", metadatacommon.ErrNotFound, shard, namespace)
	}
	return nil
}

func (m *coordinatorMetadata) DeleteShardStatus(namespace string, shard int64) error {
	return backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			ns, exist := clusterStatus.Namespaces[namespace]
			if !exist {
				return clusterStatus, false
			}
			if _, exists := ns.Shards[shard]; !exists {
				return clusterStatus, false
			}
			delete(ns.Shards, shard)
			if len(ns.Shards) == 0 {
				delete(clusterStatus.Namespaces, namespace)
			}
			return clusterStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to delete shard metadata",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

// InitShardSplit atomically records the start of a shard split. The parent is
// marked as splitting and both child shards are created within a single update,
// so no reader ever observes a parent that claims children that do not exist,
// or children whose parent is not splitting.
//
// Only the values that cannot be derived from the parent are taken from the
// caller: the reserved child ids, the split point and the children's ensembles.
// The children's hash ranges are cut from the parent's current range, and the
// rest of their metadata is built here, so a child can never disagree with the
// parent it was split from.
func (m *coordinatorMetadata) InitShardSplit(namespace string, parent, left, right int64, splitPoint uint32,
	leftEnsemble, rightEnsemble []*commonproto.DataServerIdentity) error {
	var precondition error
	err := backoff.RetryNotify(func() error {
		precondition = nil
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			namespaceStatus, exist := clusterStatus.Namespaces[namespace]
			if !exist {
				precondition = fmt.Errorf("%w: namespace %q", metadatacommon.ErrNotFound, namespace)
				return clusterStatus, false
			}
			parentMetadata, exist := namespaceStatus.Shards[parent]
			if !exist {
				precondition = fmt.Errorf("%w: shard %d of namespace %q", metadatacommon.ErrNotFound, parent, namespace)
				return clusterStatus, false
			}
			if precondition = validateShardSplit(namespaceStatus, parentMetadata, parent, left, right,
				splitPoint, leftEnsemble, rightEnsemble); precondition != nil {
				return clusterStatus, false
			}

			hashRange := parentMetadata.GetInt32HashRange()
			parentMetadata.Split = &commonproto.SplitMetadata{
				Phase:         commonproto.SplitPhaseBootstrap,
				ChildShardIds: []int64{left, right},
				SplitPoint:    splitPoint,
			}
			namespaceStatus.Shards[left] = newChildShardMetadata(parent, splitPoint,
				leftEnsemble, hashRange.GetMin(), splitPoint)
			namespaceStatus.Shards[right] = newChildShardMetadata(parent, splitPoint,
				rightEnsemble, splitPoint+1, hashRange.GetMax())
			return clusterStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to initiate the shard split",
			slog.String("namespace", namespace),
			slog.Int64("shard", parent),
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return err
	}
	return precondition
}

// validateShardSplit checks, against the freshly loaded status, that the parent
// is in a state where a split can start and that the caller's child ids, split
// point and ensembles can be used to build the children.
func validateShardSplit(namespaceStatus *commonproto.NamespaceStatus, parentMetadata *commonproto.ShardMetadata,
	parent, left, right int64, splitPoint uint32,
	leftEnsemble, rightEnsemble []*commonproto.DataServerIdentity) error {
	if status := parentMetadata.GetStatusOrDefault(); status != commonproto.ShardStatusSteadyState {
		return fmt.Errorf("%w: shard %d is not in steady state (status=%s)",
			metadatacommon.ErrFailedPrecondition, parent, status)
	}
	if parentMetadata.Split != nil {
		return fmt.Errorf("%w: shard %d already has an active split",
			metadatacommon.ErrFailedPrecondition, parent)
	}
	if len(parentMetadata.PendingDeleteShardNodes) > 0 {
		return fmt.Errorf("%w: shard %d has pending ensemble changes",
			metadatacommon.ErrFailedPrecondition, parent)
	}
	if left == right {
		return fmt.Errorf("%w: the two children of shard %d must have distinct ids, got %d twice",
			metadatacommon.ErrFailedPrecondition, parent, left)
	}
	for _, child := range []int64{left, right} {
		if _, exist := namespaceStatus.Shards[child]; exist {
			return fmt.Errorf("%w: child shard %d is already in use",
				metadatacommon.ErrFailedPrecondition, child)
		}
	}
	if len(leftEnsemble) == 0 || len(rightEnsemble) == 0 {
		return fmt.Errorf("%w: both children of shard %d need an ensemble",
			metadatacommon.ErrFailedPrecondition, parent)
	}
	hashRange := parentMetadata.GetInt32HashRange()
	if hashRange.GetMax()-hashRange.GetMin() < 1 {
		return fmt.Errorf("%w: shard %d hash range is too small to split",
			metadatacommon.ErrFailedPrecondition, parent)
	}
	if splitPoint < hashRange.GetMin() || splitPoint >= hashRange.GetMax() {
		return fmt.Errorf("%w: split point %d is outside the hash range [%d, %d] of shard %d",
			metadatacommon.ErrFailedPrecondition, splitPoint, hashRange.GetMin(), hashRange.GetMax(), parent)
	}
	return nil
}

// newChildShardMetadata builds a child shard as it looks at birth: it serves
// its slice of the parent's hash range, and carries the split metadata that
// marks it as a child until the split completes.
func newChildShardMetadata(parent int64, splitPoint uint32,
	ensemble []*commonproto.DataServerIdentity, minHash, maxHash uint32) *commonproto.ShardMetadata {
	clonedEnsemble := make([]*commonproto.DataServerIdentity, len(ensemble))
	for idx, dataServer := range ensemble {
		clonedEnsemble[idx] = gproto.CloneOf(dataServer)
	}
	return &commonproto.ShardMetadata{
		Status:   commonproto.ShardStatusSteadyState,
		Term:     0,
		Ensemble: clonedEnsemble,
		Int32HashRange: &commonproto.HashRange{
			Min: minHash,
			Max: maxHash,
		},
		Split: &commonproto.SplitMetadata{
			Phase:         commonproto.SplitPhaseBootstrap,
			ParentShardId: parent,
			SplitPoint:    splitPoint,
		},
	}
}

func (m *coordinatorMetadata) GetConfig() commonobject.Borrowed[*commonproto.ClusterConfiguration] {
	return commonobject.Borrow(m.configProvider.Watch().Load().Value)
}

func (m *coordinatorMetadata) SubscribeConfig() *commonwatch.Receiver[provider.Versioned[*commonproto.ClusterConfiguration]] {
	return m.configProvider.Watch().Subscribe()
}

func (m *coordinatorMetadata) GetLoadBalancer() commonobject.Borrowed[*commonproto.LoadBalancer] {
	return commonobject.Borrow(m.GetConfig().UnsafeBorrow().GetLoadBalancerWithDefaults())
}

func (m *coordinatorMetadata) CreateNamespace(namespace *commonproto.Namespace) error {
	name := namespace.GetName()

	return m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for _, existing := range config.GetNamespaces() {
			if existing.GetName() == name {
				return nil, metadatacommon.ErrAlreadyExists
			}
		}
		if namespace.GetReplicationFactor() > uint32(len(config.GetServers())) {
			return nil, fmt.Errorf("%w: namespace %q has replicationFactor=%d but only %d servers are configured",
				metadatacommon.ErrFailedPrecondition, name, namespace.GetReplicationFactor(), len(config.GetServers()))
		}

		config.Namespaces = append(config.Namespaces, namespace)
		return config, nil
	})
}

func (m *coordinatorMetadata) PatchNamespace(desiredNamespace *commonproto.Namespace) (*commonproto.Namespace, error) {
	var updated *commonproto.Namespace
	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for _, namespace := range config.GetNamespaces() {
			if namespace.GetName() != desiredNamespace.GetName() {
				continue
			}
			if replicationFactor := desiredNamespace.GetReplicationFactor(); replicationFactor != 0 {
				if replicationFactor > uint32(len(config.GetServers())) {
					return nil, fmt.Errorf("%w: namespace %q has replicationFactor=%d but only %d servers are configured",
						metadatacommon.ErrFailedPrecondition, namespace.GetName(), replicationFactor, len(config.GetServers()))
				}
				namespace.ReplicationFactor = replicationFactor
			}
			if desiredNamespace.NotificationsEnabled != nil {
				notificationsEnabled := desiredNamespace.GetNotificationsEnabled()
				namespace.NotificationsEnabled = &notificationsEnabled
			}

			updated = namespace
			return config, nil
		}
		return nil, metadatacommon.ErrNotFound
	}); err != nil {
		return nil, err
	}

	return updated, nil
}

func (m *coordinatorMetadata) DeleteNamespace(name string) (*commonproto.Namespace, error) {
	var deleted *commonproto.Namespace
	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for i, namespace := range config.GetNamespaces() {
			if namespace.GetName() != name {
				continue
			}

			deleted = namespace
			config.Namespaces = append(config.Namespaces[:i], config.Namespaces[i+1:]...)
			return config, nil
		}
		return nil, metadatacommon.ErrNotFound
	}); err != nil {
		return nil, err
	}

	return deleted, nil
}

func (m *coordinatorMetadata) CreateDataServer(dataServer *commonproto.DataServer) error {
	name := dataServer.GetIdentity().GetName()

	return m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		if _, exists := config.GetDataServer(name); exists {
			return nil, metadatacommon.ErrAlreadyExists
		}

		config.Servers = append(config.Servers, dataServer.GetIdentity())
		if metadata := dataServer.GetMetadata(); metadata != nil {
			if config.ServerMetadata == nil {
				config.ServerMetadata = map[string]*commonproto.DataServerMetadata{}
			}
			config.ServerMetadata[name] = metadata
		}

		return config, nil
	})
}

func (m *coordinatorMetadata) PatchDataServer(desireDataServer *commonproto.DataServer) (*commonproto.DataServer, error) {
	var updated *commonproto.DataServer
	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for _, existID := range config.GetServers() {
			if existID.GetNameOrDefault() != desireDataServer.GetNameOrDefault() {
				continue
			}
			if public := desireDataServer.GetIdentity().GetPublic(); public != "" {
				existID.Public = public
			}
			if internal := desireDataServer.GetIdentity().GetInternal(); internal != "" {
				existID.Internal = internal
			}
			if config.ServerMetadata == nil {
				config.ServerMetadata = map[string]*commonproto.DataServerMetadata{}
			}
			var dsMeta *commonproto.DataServerMetadata
			var ok bool
			if dsMeta, ok = config.ServerMetadata[existID.GetNameOrDefault()]; !ok {
				dsMeta = &commonproto.DataServerMetadata{}
			}
			if desireDataServer.Metadata != nil {
				if desireDataServer.Metadata.Labels != nil {
					dsMeta.Labels = desireDataServer.Metadata.Labels
				}
				config.ServerMetadata[existID.GetNameOrDefault()] = dsMeta
			}
			updated = &commonproto.DataServer{
				Identity: existID,
				Metadata: dsMeta,
			}
			return config, nil
		}
		return nil, metadatacommon.ErrNotFound
	}); err != nil {
		return nil, err
	}

	return updated, nil
}

func (m *coordinatorMetadata) DeleteDataServer(name string) (*commonproto.DataServer, error) {
	var deleted *commonproto.DataServer
	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for i, identity := range config.GetServers() {
			if identity.GetNameOrDefault() != name {
				continue
			}

			dataServer, _ := config.GetDataServer(name)
			deleted = dataServer
			remainingServerCount := len(config.GetServers()) - 1
			for _, namespace := range config.GetNamespaces() {
				if uint64(namespace.GetReplicationFactor()) > uint64(remainingServerCount) {
					return nil, fmt.Errorf("%w: cannot delete data server %q because namespace %q replicationFactor=%d exceeds remaining data servers=%d",
						metadatacommon.ErrFailedPrecondition,
						name,
						namespace.GetName(),
						namespace.GetReplicationFactor(),
						remainingServerCount)
				}
			}
			config.Servers = append(config.Servers[:i], config.Servers[i+1:]...)
			if config.ServerMetadata != nil {
				delete(config.ServerMetadata, name)
			}
			return config, nil
		}
		return nil, metadatacommon.ErrNotFound
	}); err != nil {
		return nil, err
	}

	return deleted, nil
}

func (m *coordinatorMetadata) ListDataServer() map[string]commonobject.Borrowed[*commonproto.DataServer] {
	config := m.GetConfig().UnsafeBorrow()
	dataServers := make(map[string]commonobject.Borrowed[*commonproto.DataServer], len(config.GetServers()))
	for _, server := range config.GetServers() {
		name := server.GetNameOrDefault()
		identity := server
		if server.GetName() == "" {
			identity = &commonproto.DataServerIdentity{
				Name:     &name,
				Public:   server.GetPublic(),
				Internal: server.GetInternal(),
			}
		}
		dataServer := &commonproto.DataServer{
			Identity: identity,
			Metadata: &commonproto.DataServerMetadata{},
		}
		if value, found := config.GetServerMetadata()[name]; found {
			dataServer.Metadata = value
		}
		dataServers[name] = commonobject.Borrow(dataServer)
	}
	return dataServers
}

func (m *coordinatorMetadata) GetNamespace(namespace string) (commonobject.Borrowed[*commonproto.Namespace], bool) {
	ns, exists := m.ListNamespace()[namespace]
	return ns, exists
}

func (m *coordinatorMetadata) ListNamespace() map[string]commonobject.Borrowed[*commonproto.Namespace] {
	configNamespaces := m.GetConfig().UnsafeBorrow().GetNamespaces()
	namespaces := make(map[string]commonobject.Borrowed[*commonproto.Namespace], len(configNamespaces))
	for _, namespace := range configNamespaces {
		namespaces[namespace.GetName()] = commonobject.Borrow(namespace)
	}
	return namespaces
}

func (m *coordinatorMetadata) GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool) {
	value, ok := m.GetConfig().UnsafeBorrow().GetDataServer(name)
	if !ok {
		return commonobject.Borrowed[*commonproto.DataServer]{}, false
	}
	return commonobject.Borrow(value), true
}
