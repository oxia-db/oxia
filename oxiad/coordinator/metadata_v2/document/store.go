package document

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/emirpasic/gods/v2/sets/hashset"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonactor "github.com/oxia-db/oxia/oxiad/common/actor"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
)

var _ metadata_v2.Store = (*Store)(nil)

type Backend interface {
	io.Closer

	LeaseWatch() *commonoption.Watch[metadatapb.LeaseState]
	RevalidateLease() error

	LoadConfig() (*metadatapb.Cluster, error)
	CommitConfig(*metadatapb.Cluster) error

	LoadStatus() (*metadatapb.ClusterState, error)
	CommitStatus(*metadatapb.ClusterState) error
}

type Store struct {
	ctx    context.Context
	cancel context.CancelFunc

	wg      sync.WaitGroup
	backend Backend

	leaseWatch *commonoption.Watch[metadatapb.LeaseState]

	configMutator *Mutator[*metadatapb.Cluster]
	statusMutator *Mutator[*metadatapb.ClusterState]

	config atomic.Pointer[metadatapb.Cluster]
	status atomic.Pointer[metadatapb.ClusterState]
}

func NewStore(ctx context.Context, backend Backend) *Store {
	storeCtx, cancel := context.WithCancel(ctx)
	s := &Store{
		ctx:        storeCtx,
		cancel:     cancel,
		backend:    backend,
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_UNHELD),
	}

	s.configMutator = NewMutator[*metadatapb.Cluster](s.ctx, "metadata-document-config", actorErrors(), Hooks[*metadatapb.Cluster]{
		RequireLease: s.requireLease,
		Load:         s.loadConfig,
		Commit:       s.commitConfigMutationState,
		OnBadVersion: s.handleConfigBadVersion,
	})
	s.statusMutator = NewMutator[*metadatapb.ClusterState](s.ctx, "metadata-document-status", actorErrors(), Hooks[*metadatapb.ClusterState]{
		RequireLease: s.requireLease,
		Load:         s.loadStatus,
		Commit:       s.commitStatusMutationState,
		OnBadVersion: s.handleStatusBadVersion,
	})

	state, _ := s.backend.LeaseWatch().Load()
	s.leaseWatch = commonoption.NewWatch(state)
	s.applyLeaseState(state)

	s.wg.Go(func() {
		process.DoWithLabels(s.ctx, map[string]string{
			"oxia": "metadata-v2-document-lease-observer",
		}, s.observeLeaseLoop)
	})

	return s
}

func (s *Store) Close() error {
	s.cancel()
	closeErr := errors.Join(
		s.configMutator.Close(),
		s.statusMutator.Close(),
		s.backend.Close(),
	)
	s.wg.Wait()
	return closeErr
}

func (s *Store) LeaseWatch() *commonoption.Watch[metadatapb.LeaseState] {
	return s.leaseWatch
}

func (s *Store) GetAllowedAuthorities() []string {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil
	}
	return slices.Clone(cfg.AllowedExtraAuthorities)
}

func (s *Store) DeleteExtraAllowedAuthorities(authorities []string) error {
	toDelete := hashset.New(authorities...)
	toDelete.Remove("")
	return s.mutateConfig(func(cfg *metadatapb.Cluster) {
		filtered := make([]string, 0, len(cfg.AllowedExtraAuthorities))
		for _, authority := range cfg.AllowedExtraAuthorities {
			if !toDelete.Contains(authority) {
				filtered = append(filtered, authority)
			}
		}
		cfg.AllowedExtraAuthorities = filtered
	})
}

func (s *Store) AddExtraAllowedAuthorities(authorities []string) error {
	return s.mutateConfig(func(cfg *metadatapb.Cluster) {
		existing := hashset.New(cfg.AllowedExtraAuthorities...)
		for _, authority := range authorities {
			if authority == "" || existing.Contains(authority) {
				continue
			}
			cfg.AllowedExtraAuthorities = append(cfg.AllowedExtraAuthorities, authority)
			existing.Add(authority)
		}
	})
}

func (s *Store) PatchAllowedAuthorities(authorities []string) error {
	return s.mutateConfig(func(cfg *metadatapb.Cluster) {
		cfg.AllowedExtraAuthorities = slices.Clone(authorities)
	})
}

func (s *Store) GetLoadBalancerPolicies() *metadatapb.LoadBalancerPolicies {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil
	}
	return gproto.CloneOf(cfg.LoadBalancer)
}

func (s *Store) PatchLoadBalancerPolicies(policies *metadatapb.LoadBalancerPolicies) (*metadatapb.LoadBalancerPolicies, error) {
	var updated *metadatapb.LoadBalancerPolicies
	err := s.mutateConfig(func(cfg *metadatapb.Cluster) {
		cfg.LoadBalancer = gproto.CloneOf(policies)
		updated = gproto.CloneOf(cfg.LoadBalancer)
	})
	return updated, err
}

func (s *Store) GetClusterHierarchyPolicies() *metadatapb.HierarchyPolicies {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil
	}
	return gproto.CloneOf(cfg.Policies)
}

func (s *Store) PatchClusterHierarchyPolicies(policies *metadatapb.HierarchyPolicies) (*metadatapb.HierarchyPolicies, error) {
	var updated *metadatapb.HierarchyPolicies
	err := s.mutateConfig(func(cfg *metadatapb.Cluster) {
		cfg.Policies = gproto.CloneOf(policies)
		updated = gproto.CloneOf(cfg.Policies)
	})
	return updated, err
}

func (s *Store) GetDataServer(name string) (*metadatapb.DataServer, error) {
	if name == "" {
		return nil, metadata_v2.ErrInvalidInput
	}

	cfg, err := s.loadConfig()
	if err != nil {
		return nil, err
	}

	dataServer, ok := cfg.DataServers[name]
	if !ok || dataServer == nil {
		return nil, metadata_v2.ErrNotFound
	}
	return gproto.CloneOf(dataServer), nil
}

func (s *Store) DeleteDataServers(names []string) error {
	toDelete := hashset.New(names...)
	toDelete.Remove("")
	return s.mutateConfig(func(cfg *metadatapb.Cluster) {
		if toDelete.Empty() || cfg.DataServers == nil {
			return
		}
		for _, name := range toDelete.Values() {
			delete(cfg.DataServers, name)
		}
	})
}

func (s *Store) CreateDataServers(dataServers []*metadatapb.DataServer) error {
	return s.updateConfig(func(cfg *metadatapb.Cluster, fail func(error)) {
		if cfg.DataServers == nil {
			cfg.DataServers = make(map[string]*metadatapb.DataServer)
		}

		for _, dataServer := range dataServers {
			if dataServer == nil || dataServer.Name == "" {
				fail(metadata_v2.ErrInvalidInput)
				return
			}
			if _, ok := cfg.DataServers[dataServer.Name]; ok {
				fail(fmt.Errorf("%w: data server %q", metadata_v2.ErrAlreadyExists, dataServer.Name))
				return
			}
			cfg.DataServers[dataServer.Name] = gproto.CloneOf(dataServer)
		}
	})
}

func (s *Store) ListDataServer() ([]*metadatapb.DataServer, error) {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil, err
	}

	dataServers := make([]*metadatapb.DataServer, 0, len(cfg.DataServers))
	for _, dataServer := range cfg.DataServers {
		dataServers = append(dataServers, gproto.CloneOf(dataServer))
	}
	return dataServers, nil
}

func (s *Store) PatchDataServer(dataServer *metadatapb.DataServer) (*metadatapb.DataServer, error) {
	var updated *metadatapb.DataServer
	err := s.mutateConfig(func(cfg *metadatapb.Cluster) {
		if cfg.DataServers == nil {
			cfg.DataServers = make(map[string]*metadatapb.DataServer)
		}
		cloned := gproto.CloneOf(dataServer)
		cfg.DataServers[dataServer.Name] = cloned
		updated = gproto.CloneOf(cloned)
	})
	return updated, err
}

func (s *Store) GetNamespace(name string) (*metadatapb.Namespace, error) {
	if name == "" {
		return nil, metadata_v2.ErrInvalidInput
	}

	cfg, err := s.loadConfig()
	if err != nil {
		return nil, err
	}

	namespace, ok := cfg.Namespaces[name]
	if !ok || namespace == nil {
		return nil, metadata_v2.ErrNotFound
	}
	return gproto.CloneOf(namespace), nil
}

func (s *Store) DeleteNamespaces(names []string) error {
	toDelete := hashset.New(names...)
	toDelete.Remove("")
	return s.mutateConfig(func(cfg *metadatapb.Cluster) {
		if toDelete.Empty() || cfg.Namespaces == nil {
			return
		}
		for _, name := range toDelete.Values() {
			delete(cfg.Namespaces, name)
		}
	})
}

func (s *Store) CreateNamespaces(namespaces []*metadatapb.Namespace) error {
	return s.updateConfig(func(cfg *metadatapb.Cluster, fail func(error)) {
		if cfg.Namespaces == nil {
			cfg.Namespaces = make(map[string]*metadatapb.Namespace)
		}

		for _, namespace := range namespaces {
			if namespace == nil || namespace.Name == "" {
				fail(metadata_v2.ErrInvalidInput)
				return
			}
			if _, ok := cfg.Namespaces[namespace.Name]; ok {
				fail(fmt.Errorf("%w: namespace %q", metadata_v2.ErrAlreadyExists, namespace.Name))
				return
			}
			cfg.Namespaces[namespace.Name] = gproto.CloneOf(namespace)
		}
	})
}

func (s *Store) ListNamespace() ([]*metadatapb.Namespace, error) {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil, err
	}

	namespaces := make([]*metadatapb.Namespace, 0, len(cfg.Namespaces))
	for _, namespace := range cfg.Namespaces {
		namespaces = append(namespaces, gproto.CloneOf(namespace))
	}
	return namespaces, nil
}

func (s *Store) PatchNamespace(namespace *metadatapb.Namespace) (*metadatapb.Namespace, error) {
	var updated *metadatapb.Namespace
	err := s.mutateConfig(func(cfg *metadatapb.Cluster) {
		if cfg.Namespaces == nil {
			cfg.Namespaces = make(map[string]*metadatapb.Namespace)
		}
		cloned := gproto.CloneOf(namespace)
		cfg.Namespaces[namespace.Name] = cloned
		updated = gproto.CloneOf(cloned)
	})
	return updated, err
}

func (s *Store) GetNamespaceHierarchyPolicies(name string) *metadatapb.HierarchyPolicies {
	if name == "" {
		return nil
	}

	cfg, err := s.loadConfig()
	if err != nil {
		return nil
	}

	namespace, ok := cfg.Namespaces[name]
	if !ok || namespace == nil {
		return nil
	}
	return gproto.CloneOf(namespace.Policies)
}

func (s *Store) PatchNamespaceHierarchyPolicies(name string, policy *metadatapb.HierarchyPolicies) (*metadatapb.HierarchyPolicies, error) {
	if name == "" {
		return nil, metadata_v2.ErrInvalidInput
	}

	var updated *metadatapb.HierarchyPolicies
	err := s.updateConfig(func(cfg *metadatapb.Cluster, fail func(error)) {
		namespace, ok := cfg.Namespaces[name]
		if !ok || namespace == nil {
			fail(metadata_v2.ErrNotFound)
			return
		}
		namespace.Policies = gproto.CloneOf(policy)
		if namespace.Policies != nil {
			updated = gproto.CloneOf(namespace.Policies)
		}
	})
	return updated, err
}

func (s *Store) GetNamespaceState(name string) (*metadatapb.NamespaceState, error) {
	if name == "" {
		return nil, metadata_v2.ErrInvalidInput
	}

	status, err := s.loadStatus()
	if err != nil {
		return nil, err
	}

	namespace, ok := status.Namespaces[name]
	if !ok || namespace == nil {
		return nil, metadata_v2.ErrNotFound
	}
	return gproto.CloneOf(namespace), nil
}

func (s *Store) DeleteNamespaceStates(names []string) error {
	toDelete := hashset.New(names...)
	toDelete.Remove("")
	return s.mutateStatus(func(status *metadatapb.ClusterState) {
		if toDelete.Empty() || status.Namespaces == nil {
			return
		}
		for _, name := range toDelete.Values() {
			delete(status.Namespaces, name)
		}
	})
}

func (s *Store) CreateNamespaceStates(namespaces map[string]*metadatapb.NamespaceState) error {
	return s.updateStatus(func(status *metadatapb.ClusterState, fail func(error)) {
		if status.Namespaces == nil {
			status.Namespaces = make(map[string]*metadatapb.NamespaceState)
		}
		for name, namespace := range namespaces {
			if name == "" || namespace == nil {
				fail(metadata_v2.ErrInvalidInput)
				return
			}
			if _, ok := status.Namespaces[name]; ok {
				fail(fmt.Errorf("%w: namespace state %q", metadata_v2.ErrAlreadyExists, name))
				return
			}
			status.Namespaces[name] = gproto.CloneOf(namespace)
		}
	})
}

func (s *Store) PatchNamespaceState(name string, namespace *metadatapb.NamespaceState) (*metadatapb.NamespaceState, error) {
	if name == "" || namespace == nil {
		return nil, metadata_v2.ErrInvalidInput
	}

	var updated *metadatapb.NamespaceState
	err := s.mutateStatus(func(status *metadatapb.ClusterState) {
		if status.Namespaces == nil {
			status.Namespaces = make(map[string]*metadatapb.NamespaceState)
		}
		cloned := gproto.CloneOf(namespace)
		status.Namespaces[name] = cloned
		updated = gproto.CloneOf(cloned)
	})
	return updated, err
}

func (s *Store) GetShardState(namespace string, shardID int64) (*metadatapb.ShardState, error) {
	if namespace == "" {
		return nil, metadata_v2.ErrInvalidInput
	}

	status, err := s.loadStatus()
	if err != nil {
		return nil, err
	}

	namespaceState, ok := status.Namespaces[namespace]
	if !ok || namespaceState == nil {
		return nil, metadata_v2.ErrNotFound
	}
	shard, ok := namespaceState.Shards[shardID]
	if !ok || shard == nil {
		return nil, metadata_v2.ErrNotFound
	}
	return gproto.CloneOf(shard), nil
}

func (s *Store) PatchShardState(namespace string, shardID int64, shard *metadatapb.ShardState) (*metadatapb.ShardState, error) {
	if namespace == "" || shard == nil {
		return nil, metadata_v2.ErrInvalidInput
	}

	var updated *metadatapb.ShardState
	err := s.updateStatus(func(status *metadatapb.ClusterState, fail func(error)) {
		namespaceState, ok := status.Namespaces[namespace]
		if !ok || namespaceState == nil {
			fail(metadata_v2.ErrNotFound)
			return
		}
		if namespaceState.Shards == nil {
			namespaceState.Shards = make(map[int64]*metadatapb.ShardState)
		}
		cloned := gproto.CloneOf(shard)
		namespaceState.Shards[shardID] = cloned
		updated = gproto.CloneOf(cloned)
	})
	return updated, err
}

func (s *Store) observeLeaseLoop() {
	watch := s.backend.LeaseWatch()
	_, version := watch.Load()
	for {
		state, nextVersion, err := watch.Wait(s.ctx, version)
		if err != nil {
			return
		}
		s.applyLeaseState(state)
		version = nextVersion
	}
}

func (s *Store) applyLeaseState(state metadatapb.LeaseState) {
	switch state {
	case metadatapb.LeaseState_LEASE_STATE_HELD:
		_ = s.configMutator.Resume()
		_ = s.statusMutator.Resume()
	default:
		_ = s.configMutator.Pause()
		_ = s.statusMutator.Pause()
	}

	current, _ := s.leaseWatch.Load()
	if current != state {
		s.leaseWatch.Notify(state)
	}
}

func (s *Store) requireLease() error {
	state, _ := s.leaseWatch.Load()
	if state != metadatapb.LeaseState_LEASE_STATE_HELD {
		return metadata_v2.ErrLeaseNotHeld
	}
	return nil
}

func (s *Store) loadConfig() (*metadatapb.Cluster, error) {
	if config := s.config.Load(); config != nil {
		return gproto.CloneOf(config), nil
	}

	cluster, err := s.backend.LoadConfig()
	if err != nil {
		return nil, err
	}
	if cluster == nil {
		cluster = &metadatapb.Cluster{}
	}
	s.config.CompareAndSwap(nil, gproto.CloneOf(cluster))
	return cluster, nil
}

func (s *Store) commitConfigMutationState(config *metadatapb.Cluster) error {
	if err := s.backend.CommitConfig(config); err != nil {
		return err
	}
	s.config.Store(gproto.CloneOf(config))
	return nil
}

func (s *Store) handleConfigBadVersion() error {
	if err := s.revalidateLease(); err != nil {
		return err
	}

	cluster, err := s.backend.LoadConfig()
	if err != nil {
		return err
	}
	if cluster == nil {
		cluster = &metadatapb.Cluster{}
	}
	s.config.Store(gproto.CloneOf(cluster))
	return nil
}

func (s *Store) mutateConfig(apply func(*metadatapb.Cluster)) error {
	return s.configMutator.Submit(NewOperation(apply))
}

func (s *Store) updateConfig(apply func(*metadatapb.Cluster, func(error))) error {
	return s.configMutator.Submit(NewErrorOperation(apply))
}

func (s *Store) loadStatus() (*metadatapb.ClusterState, error) {
	if status := s.status.Load(); status != nil {
		return gproto.CloneOf(status), nil
	}

	clusterState, err := s.backend.LoadStatus()
	if err != nil {
		return nil, err
	}
	if clusterState == nil {
		clusterState = &metadatapb.ClusterState{}
	}
	s.status.CompareAndSwap(nil, gproto.CloneOf(clusterState))
	return clusterState, nil
}

func (s *Store) commitStatusMutationState(status *metadatapb.ClusterState) error {
	if err := s.backend.CommitStatus(status); err != nil {
		return err
	}
	s.status.Store(gproto.CloneOf(status))
	return nil
}

func (s *Store) handleStatusBadVersion() error {
	if err := s.revalidateLease(); err != nil {
		return err
	}

	status, err := s.backend.LoadStatus()
	if err != nil {
		return err
	}
	if status == nil {
		status = &metadatapb.ClusterState{}
	}
	s.status.Store(gproto.CloneOf(status))
	return nil
}

func (s *Store) mutateStatus(apply func(*metadatapb.ClusterState)) error {
	return s.statusMutator.Submit(NewOperation(apply))
}

func (s *Store) updateStatus(apply func(*metadatapb.ClusterState, func(error))) error {
	return s.statusMutator.Submit(NewErrorOperation(apply))
}

func (s *Store) revalidateLease() error {
	if err := s.backend.RevalidateLease(); err != nil {
		s.applyLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return err
	}
	return nil
}

func actorErrors() commonactor.Errors {
	return commonactor.Errors{
		Pause:    metadata_v2.ErrLeaseNotHeld,
		Shutdown: metadata_v2.ErrLeaseNotHeld,
	}
}
