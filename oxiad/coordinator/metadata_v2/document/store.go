package document

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/emirpasic/gods/v2/sets/hashset"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/backend"
	metadataerr "github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/error"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
)

type Store struct {
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	backend backend.Backend

	configExecutor *Executor[*metadatapb.Cluster]
	statusExecutor *Executor[*metadatapb.ClusterState]

	config backend.MetaRecord[*metadatapb.Cluster]
	status backend.MetaRecord[*metadatapb.ClusterState]
}

func NewStore(ctx context.Context, backendInstance backend.Backend) *Store {
	storeCtx, cancel := context.WithCancel(ctx)
	s := &Store{
		ctx:     storeCtx,
		cancel:  cancel,
		backend: backendInstance,
		config:  backend.NewLazyMetaRecord[*metadatapb.Cluster](backendInstance, backend.ConfigRecordName),
		status:  backend.NewLazyMetaRecord[*metadatapb.ClusterState](backendInstance, backend.StatusRecordName),
	}
	s.configExecutor = NewExecutor(s.ctx, "config_executor", &s.config)
	s.statusExecutor = NewExecutor(s.ctx, "status_executor", &s.status)

	s.wg.Go(func() {
		process.DoWithLabels(s.ctx, map[string]string{
			"oxia": "metadata-v2-document-lease-observer",
		}, s.onLeaseChanged)
	})

	return s
}

func (s *Store) Close() error {
	s.cancel()
	closeErr := errors.Join(
		s.configExecutor.Close(),
		s.statusExecutor.Close(),
		s.backend.Close(),
	)
	s.wg.Wait()
	return closeErr
}

func (s *Store) LeaseWatch() *commonoption.Watch[metadatapb.LeaseState] {
	return s.backend.LeaseWatch()
}

func (s *Store) GetAllowedAuthorities() []string {
	cfg := s.config.Load()
	return slices.Clone(cfg.AllowedExtraAuthorities)
}

func (s *Store) DeleteExtraAllowedAuthorities(authorities []string) error {
	toDelete := hashset.New(authorities...)
	toDelete.Remove("")
	return s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		filtered := make([]string, 0, len(cfg.AllowedExtraAuthorities))
		for _, authority := range cfg.AllowedExtraAuthorities {
			if !toDelete.Contains(authority) {
				filtered = append(filtered, authority)
			}
		}
		cfg.AllowedExtraAuthorities = filtered
		return nil
	}))
}

func (s *Store) AddExtraAllowedAuthorities(authorities []string) error {
	return s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		existing := hashset.New(cfg.AllowedExtraAuthorities...)
		for _, authority := range authorities {
			if authority == "" || existing.Contains(authority) {
				continue
			}
			cfg.AllowedExtraAuthorities = append(cfg.AllowedExtraAuthorities, authority)
			existing.Add(authority)
		}
		return nil
	}))
}

func (s *Store) PatchAllowedAuthorities(authorities []string) error {
	return s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		cfg.AllowedExtraAuthorities = slices.Clone(authorities)
		return nil
	}))
}

func (s *Store) GetLoadBalancerPolicies() *metadatapb.LoadBalancerPolicies {
	cfg := s.config.Load()
	return cfg.LoadBalancer
}

func (s *Store) PatchLoadBalancerPolicies(policies *metadatapb.LoadBalancerPolicies) (*metadatapb.LoadBalancerPolicies, error) {
	var updated *metadatapb.LoadBalancerPolicies
	err := s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		switch {
		case cfg.LoadBalancer == nil:
			cfg.LoadBalancer = gproto.CloneOf(policies)
		case policies != nil:
			gproto.Merge(cfg.LoadBalancer, policies)
		}
		updated = gproto.CloneOf(cfg.LoadBalancer)
		return nil
	}))
	return updated, err
}

func (s *Store) GetClusterHierarchyPolicies() *metadatapb.HierarchyPolicies {
	cfg := s.config.Load()
	return cfg.Policies
}

func (s *Store) PatchClusterHierarchyPolicies(policies *metadatapb.HierarchyPolicies) (*metadatapb.HierarchyPolicies, error) {
	var updated *metadatapb.HierarchyPolicies
	err := s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		switch {
		case cfg.Policies == nil:
			cfg.Policies = gproto.CloneOf(policies)
		case policies != nil:
			gproto.Merge(cfg.Policies, policies)
		}
		updated = gproto.CloneOf(cfg.Policies)
		return nil
	}))
	return updated, err
}

func (s *Store) GetDataServer(name string) (*metadatapb.DataServer, error) {
	cfg := s.config.Load()
	dataServer, ok := cfg.DataServers[name]
	if !ok || dataServer == nil {
		return nil, metadataerr.ErrNotFound
	}
	return dataServer, nil
}

func (s *Store) DeleteDataServers(names []string) error {
	toDelete := hashset.New(names...)
	toDelete.Remove("")
	return s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		if toDelete.Empty() || cfg.DataServers == nil {
			return nil
		}
		for _, name := range toDelete.Values() {
			delete(cfg.DataServers, name)
		}
		return nil
	}))
}

func (s *Store) CreateDataServers(dataServers []*metadatapb.DataServer) error {
	return s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		if cfg.DataServers == nil {
			cfg.DataServers = make(map[string]*metadatapb.DataServer)
		}
		for _, dataServer := range dataServers {
			if dataServer == nil || dataServer.Name == "" {
				return metadataerr.ErrInvalidInput
			}
			if _, ok := cfg.DataServers[dataServer.Name]; ok {
				return fmt.Errorf("%w: data server %q", metadataerr.ErrAlreadyExists, dataServer.Name)
			}
			cfg.DataServers[dataServer.Name] = gproto.CloneOf(dataServer)
		}
		return nil
	}))
}

func (s *Store) ListDataServer() ([]*metadatapb.DataServer, error) {
	cfg := s.config.Load()
	dataServers := make([]*metadatapb.DataServer, 0, len(cfg.DataServers))
	for _, dataServer := range cfg.DataServers {
		dataServers = append(dataServers, dataServer)
	}
	return dataServers, nil
}

func (s *Store) PatchDataServer(dataServer *metadatapb.DataServer) (*metadatapb.DataServer, error) {
	var updated *metadatapb.DataServer
	err := s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		if cfg.DataServers == nil {
			cfg.DataServers = make(map[string]*metadatapb.DataServer)
		}
		cloned := gproto.CloneOf(dataServer)
		cfg.DataServers[dataServer.Name] = cloned
		updated = gproto.CloneOf(cloned)
		return nil
	}))
	return updated, err
}

func (s *Store) GetNamespace(name string) (*metadatapb.Namespace, error) {
	cfg := s.config.Load()
	namespace, ok := cfg.Namespaces[name]
	if !ok || namespace == nil {
		return nil, metadataerr.ErrNotFound
	}
	return namespace, nil
}

func (s *Store) DeleteNamespaces(names []string) error {
	toDelete := hashset.New(names...)
	toDelete.Remove("")
	return s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		if toDelete.Empty() || cfg.Namespaces == nil {
			return nil
		}
		for _, name := range toDelete.Values() {
			delete(cfg.Namespaces, name)
		}
		return nil
	}))
}

func (s *Store) CreateNamespaces(namespaces []*metadatapb.Namespace) error {
	return s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		if cfg.Namespaces == nil {
			cfg.Namespaces = make(map[string]*metadatapb.Namespace)
		}

		for _, namespace := range namespaces {
			if namespace == nil || namespace.Name == "" {
				return metadataerr.ErrInvalidInput
			}
			if _, ok := cfg.Namespaces[namespace.Name]; ok {
				return fmt.Errorf("%w: namespace %q", metadataerr.ErrAlreadyExists, namespace.Name)
			}
			cfg.Namespaces[namespace.Name] = gproto.CloneOf(namespace)
		}
		return nil
	}))
}

func (s *Store) ListNamespace() ([]*metadatapb.Namespace, error) {
	cfg := s.config.Load()
	namespaces := make([]*metadatapb.Namespace, 0, len(cfg.Namespaces))
	for _, namespace := range cfg.Namespaces {
		namespaces = append(namespaces, namespace)
	}
	return namespaces, nil
}

func (s *Store) PatchNamespace(namespace *metadatapb.Namespace) (*metadatapb.Namespace, error) {
	var updated *metadatapb.Namespace
	err := s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		if cfg.Namespaces == nil {
			cfg.Namespaces = make(map[string]*metadatapb.Namespace)
		}
		cloned := gproto.CloneOf(namespace)
		cfg.Namespaces[namespace.Name] = cloned
		updated = gproto.CloneOf(cloned)
		return nil
	}))
	return updated, err
}

func (s *Store) GetNamespaceHierarchyPolicies(name string) *metadatapb.HierarchyPolicies {
	cfg := s.config.Load()
	namespace, ok := cfg.Namespaces[name]
	if !ok || namespace == nil {
		return nil
	}
	return namespace.Policies
}

func (s *Store) PatchNamespaceHierarchyPolicies(name string, policy *metadatapb.HierarchyPolicies) (*metadatapb.HierarchyPolicies, error) {
	var updated *metadatapb.HierarchyPolicies
	err := s.configExecutor.Execute(NewInflight(func(cfg *metadatapb.Cluster) error {
		namespace, ok := cfg.Namespaces[name]
		if !ok || namespace == nil {
			return metadataerr.ErrNotFound
		}
		switch {
		case namespace.Policies == nil:
			namespace.Policies = gproto.CloneOf(policy)
		case policy != nil:
			gproto.Merge(namespace.Policies, policy)
		}
		if namespace.Policies != nil {
			updated = gproto.CloneOf(namespace.Policies)
		}
		return nil
	}))
	return updated, err
}

func (s *Store) GetNamespaceState(name string) (*metadatapb.NamespaceState, error) {
	if name == "" {
		return nil, metadataerr.ErrInvalidInput
	}

	status := s.status.Load()
	namespace, ok := status.Namespaces[name]
	if !ok || namespace == nil {
		return nil, metadataerr.ErrNotFound
	}
	return namespace, nil
}

func (s *Store) DeleteNamespaceStates(names []string) error {
	toDelete := hashset.New(names...)
	toDelete.Remove("")
	return s.statusExecutor.Execute(NewInflight(func(status *metadatapb.ClusterState) error {
		if toDelete.Empty() || status.Namespaces == nil {
			return nil
		}
		for _, name := range toDelete.Values() {
			delete(status.Namespaces, name)
		}
		return nil
	}))
}

func (s *Store) CreateNamespaceStates(namespaces map[string]*metadatapb.NamespaceState) error {
	return s.statusExecutor.Execute(NewInflight(func(status *metadatapb.ClusterState) error {
		if status.Namespaces == nil {
			status.Namespaces = make(map[string]*metadatapb.NamespaceState)
		}
		for name, namespace := range namespaces {
			if name == "" || namespace == nil {
				return metadataerr.ErrInvalidInput
			}
			if _, ok := status.Namespaces[name]; ok {
				return fmt.Errorf("%w: namespace state %q", metadataerr.ErrAlreadyExists, name)
			}
			status.Namespaces[name] = gproto.CloneOf(namespace)
		}
		return nil
	}))
}

func (s *Store) PatchNamespaceState(name string, namespace *metadatapb.NamespaceState) (*metadatapb.NamespaceState, error) {

	var updated *metadatapb.NamespaceState
	err := s.statusExecutor.Execute(NewInflight(func(status *metadatapb.ClusterState) error {
		if status.Namespaces == nil {
			status.Namespaces = make(map[string]*metadatapb.NamespaceState)
		}
		cloned := gproto.CloneOf(namespace)
		status.Namespaces[name] = cloned
		updated = gproto.CloneOf(cloned)
		return nil
	}))
	return updated, err
}

func (s *Store) GetShardState(namespace string, shardID int64) (*metadatapb.ShardState, error) {
	status := s.status.Load()
	namespaceState, ok := status.Namespaces[namespace]
	if !ok || namespaceState == nil {
		return nil, metadataerr.ErrNotFound
	}
	shard, ok := namespaceState.Shards[shardID]
	if !ok || shard == nil {
		return nil, metadataerr.ErrNotFound
	}
	return shard, nil
}

func (s *Store) PatchShardState(namespace string, shardID int64, shard *metadatapb.ShardState) (*metadatapb.ShardState, error) {
	var updated *metadatapb.ShardState
	err := s.statusExecutor.Execute(NewInflight(func(status *metadatapb.ClusterState) error {
		namespaceState, ok := status.Namespaces[namespace]
		if !ok || namespaceState == nil {
			return metadataerr.ErrNotFound
		}
		if namespaceState.Shards == nil {
			namespaceState.Shards = make(map[int64]*metadatapb.ShardState)
		}
		cloned := gproto.CloneOf(shard)
		namespaceState.Shards[shardID] = cloned
		updated = gproto.CloneOf(cloned)
		return nil
	}))
	return updated, err
}

func (s *Store) onLeaseChanged() {
	watch := s.backend.LeaseWatch()
	// init apply
	state, version := watch.Load()
	s.applyLeaseState(state)
	// watching
	for {
		state, nextVersion, err := watch.Wait(s.ctx, version)
		if err != nil {
			// CODEX: add logs here
			return
		}
		s.applyLeaseState(state)
		version = nextVersion
	}
}

func (s *Store) applyLeaseState(state metadatapb.LeaseState) {
	switch state {
	case metadatapb.LeaseState_LEASE_STATE_HELD:
		_ = s.configExecutor.Resume()
		_ = s.statusExecutor.Resume()
	default:
		_ = s.configExecutor.Pause()
		_ = s.statusExecutor.Pause()
	}
}
