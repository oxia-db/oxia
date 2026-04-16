package file

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/juju/fslock"
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
)

var _ metadata_v2.Store = &Store{}

var (
	ErrAlreadyExists = errors.New("metadata_v2 file store resource already exists")
	ErrInvalidInput  = errors.New("metadata_v2 file store invalid input")
	ErrLeaseNotHeld  = errors.New("metadata_v2 file store lease not held")
	ErrNotFound      = errors.New("metadata_v2 file store resource not found")
)

const leaseRetryInterval = time.Second

type Store struct {
	ctx context.Context

	configPath string
	statePath  string

	leaderLockPath string
	leaderLock     *fslock.Lock
	leaseWatch     *commonoption.Watch[metadatapb.LeaseState]

	configMu sync.Mutex
	stateMu  sync.Mutex
}

type Batch struct {
	config *metadatapb.Cluster
	status *metadatapb.ClusterState
}

func (b *Batch) Config() *metadatapb.Cluster {
	return b.config
}

func (b *Batch) Status() *metadatapb.ClusterState {
	return b.status
}

func NewStore(ctx context.Context, configPath string, statePath string) *Store {
	if ctx == nil {
		ctx = context.Background()
	}

	s := &Store{
		ctx:            ctx,
		configPath:     configPath,
		statePath:      statePath,
		leaderLockPath: lockPath(configPath, statePath),
		leaseWatch:     commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_UNHELD),
	}
	s.leaderLock = fslock.New(s.leaderLockPath)

	go s.runLeaseLoop()

	return s
}

func (s *Store) Batch(fn func(*Batch) error) error {
	if fn == nil {
		return ErrInvalidInput
	}
	if err := s.requireLease(); err != nil {
		return err
	}

	s.configMu.Lock()
	defer s.configMu.Unlock()
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	config, err := s.loadConfigLocked()
	if err != nil {
		return err
	}
	status, err := s.loadStatusLocked()
	if err != nil {
		return err
	}

	originalConfig := cloneProto(config)
	originalStatus := cloneProto(status)

	if err := fn(&Batch{
		config: config,
		status: status,
	}); err != nil {
		return err
	}

	if !gproto.Equal(originalConfig, config) {
		if err := writeProtoFile(s.configPath, config); err != nil {
			return err
		}
	}
	if !gproto.Equal(originalStatus, status) {
		if err := writeProtoFile(s.statePath, status); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) LeaseWatch() *commonoption.Watch[metadatapb.LeaseState] {
	return s.leaseWatch
}

func (s *Store) GetAllowedAuthorities() []string {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil
	}
	return cloneStrings(cfg.AllowedExtraAuthorities)
}

func (s *Store) DeleteExtraAllowedAuthorities(authorities []string) error {
	toDelete := makeStringSet(authorities)
	return s.updateConfig(func(cfg *metadatapb.Cluster) error {
		if len(toDelete) == 0 {
			return nil
		}
		filtered := make([]string, 0, len(cfg.AllowedExtraAuthorities))
		for _, authority := range cfg.AllowedExtraAuthorities {
			if !toDelete[authority] {
				filtered = append(filtered, authority)
			}
		}
		cfg.AllowedExtraAuthorities = filtered
		return nil
	})
}

func (s *Store) AddExtraAllowedAuthorities(authorities []string) error {
	return s.updateConfig(func(cfg *metadatapb.Cluster) error {
		existing := makeStringSet(cfg.AllowedExtraAuthorities)
		for _, authority := range authorities {
			if authority == "" || existing[authority] {
				continue
			}
			cfg.AllowedExtraAuthorities = append(cfg.AllowedExtraAuthorities, authority)
			existing[authority] = true
		}
		return nil
	})
}

func (s *Store) UpdateAllowedAuthorities(authorities []string) error {
	return s.updateConfig(func(cfg *metadatapb.Cluster) error {
		cfg.AllowedExtraAuthorities = cloneStrings(authorities)
		return nil
	})
}

func (s *Store) GetLoadBalancerPolicies() *metadatapb.LoadBalancerPolicies {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil
	}
	return cloneProto(cfg.LoadBalancer)
}

func (s *Store) PatchLoadBalancerPolicies(policies *metadatapb.LoadBalancerPolicies) (*metadatapb.LoadBalancerPolicies, error) {
	var updated *metadatapb.LoadBalancerPolicies
	err := s.updateConfig(func(cfg *metadatapb.Cluster) error {
		cfg.LoadBalancer = cloneProto(policies)
		updated = cloneProto(cfg.LoadBalancer)
		return nil
	})
	return updated, err
}

func (s *Store) GetClusterHierarchyPolicies() *metadatapb.HierarchyPolicies {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil
	}
	return cloneProto(cfg.Policies)
}

func (s *Store) PatchClusterHierarchyPolicies(policies *metadatapb.HierarchyPolicies) (*metadatapb.HierarchyPolicies, error) {
	var updated *metadatapb.HierarchyPolicies
	err := s.updateConfig(func(cfg *metadatapb.Cluster) error {
		cfg.Policies = cloneProto(policies)
		updated = cloneProto(cfg.Policies)
		return nil
	})
	return updated, err
}

func (s *Store) GetDataServer(name string) (*metadatapb.DataServer, error) {
	if name == "" {
		return nil, ErrInvalidInput
	}

	cfg, err := s.loadConfig()
	if err != nil {
		return nil, err
	}

	idx := findDataServer(cfg, name)
	if idx < 0 {
		return nil, ErrNotFound
	}
	return cloneProto(cfg.DataServers[idx]), nil
}

func (s *Store) DeleteDataServers(names []string) error {
	toDelete := makeStringSet(names)
	return s.updateConfig(func(cfg *metadatapb.Cluster) error {
		if len(toDelete) == 0 {
			return nil
		}
		filtered := make([]*metadatapb.DataServer, 0, len(cfg.DataServers))
		for _, dataServer := range cfg.DataServers {
			if dataServer == nil || !toDelete[dataServer.Name] {
				filtered = append(filtered, dataServer)
			}
		}
		cfg.DataServers = filtered
		return nil
	})
}

func (s *Store) CreateDataServers(dataServers []*metadatapb.DataServer) error {
	return s.updateConfig(func(cfg *metadatapb.Cluster) error {
		existing := make(map[string]struct{}, len(cfg.DataServers))
		for _, dataServer := range cfg.DataServers {
			if dataServer != nil {
				existing[dataServer.Name] = struct{}{}
			}
		}

		for _, dataServer := range dataServers {
			if dataServer == nil || dataServer.Name == "" {
				return ErrInvalidInput
			}
			if _, ok := existing[dataServer.Name]; ok {
				return fmt.Errorf("%w: data server %q", ErrAlreadyExists, dataServer.Name)
			}
			cfg.DataServers = append(cfg.DataServers, cloneProto(dataServer))
			existing[dataServer.Name] = struct{}{}
		}
		return nil
	})
}

func (s *Store) ListDataServer() ([]*metadatapb.DataServer, error) {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil, err
	}

	dataServers := make([]*metadatapb.DataServer, 0, len(cfg.DataServers))
	for _, dataServer := range cfg.DataServers {
		dataServers = append(dataServers, cloneProto(dataServer))
	}
	return dataServers, nil
}

func (s *Store) PatchDataServer(dataServer *metadatapb.DataServer) (metadatapb.DataServer, error) {
	if dataServer == nil || dataServer.Name == "" {
		return metadatapb.DataServer{}, ErrInvalidInput
	}

	var updated metadatapb.DataServer
	err := s.updateConfig(func(cfg *metadatapb.Cluster) error {
		cloned := cloneProto(dataServer)
		idx := findDataServer(cfg, dataServer.Name)
		if idx < 0 {
			cfg.DataServers = append(cfg.DataServers, cloned)
		} else {
			cfg.DataServers[idx] = cloned
		}
		updated = *cloneProto(cloned)
		return nil
	})
	return updated, err
}

func (s *Store) GetNamespace(name string) (*metadatapb.Namespace, error) {
	if name == "" {
		return nil, ErrInvalidInput
	}

	cfg, err := s.loadConfig()
	if err != nil {
		return nil, err
	}

	idx := findNamespace(cfg, name)
	if idx < 0 {
		return nil, ErrNotFound
	}
	return cloneProto(cfg.Namespaces[idx]), nil
}

func (s *Store) DeleteNamespaces(names []string) error {
	toDelete := makeStringSet(names)
	return s.updateConfig(func(cfg *metadatapb.Cluster) error {
		if len(toDelete) == 0 {
			return nil
		}
		filtered := make([]*metadatapb.Namespace, 0, len(cfg.Namespaces))
		for _, namespace := range cfg.Namespaces {
			if namespace == nil || !toDelete[namespace.Name] {
				filtered = append(filtered, namespace)
			}
		}
		cfg.Namespaces = filtered
		return nil
	})
}

func (s *Store) CreateNamespaces(namespaces []*metadatapb.Namespace) error {
	return s.updateConfig(func(cfg *metadatapb.Cluster) error {
		existing := make(map[string]struct{}, len(cfg.Namespaces))
		for _, namespace := range cfg.Namespaces {
			if namespace != nil {
				existing[namespace.Name] = struct{}{}
			}
		}

		for _, namespace := range namespaces {
			if namespace == nil || namespace.Name == "" {
				return ErrInvalidInput
			}
			if _, ok := existing[namespace.Name]; ok {
				return fmt.Errorf("%w: namespace %q", ErrAlreadyExists, namespace.Name)
			}
			cfg.Namespaces = append(cfg.Namespaces, cloneProto(namespace))
			existing[namespace.Name] = struct{}{}
		}
		return nil
	})
}

func (s *Store) ListNamespace() ([]*metadatapb.Namespace, error) {
	cfg, err := s.loadConfig()
	if err != nil {
		return nil, err
	}

	namespaces := make([]*metadatapb.Namespace, 0, len(cfg.Namespaces))
	for _, namespace := range cfg.Namespaces {
		namespaces = append(namespaces, cloneProto(namespace))
	}
	return namespaces, nil
}

func (s *Store) PatchNamespace(namespace *metadatapb.Namespace) (metadatapb.Namespace, error) {
	if namespace == nil || namespace.Name == "" {
		return metadatapb.Namespace{}, ErrInvalidInput
	}

	var updated metadatapb.Namespace
	err := s.updateConfig(func(cfg *metadatapb.Cluster) error {
		cloned := cloneProto(namespace)
		idx := findNamespace(cfg, namespace.Name)
		if idx < 0 {
			cfg.Namespaces = append(cfg.Namespaces, cloned)
		} else {
			cfg.Namespaces[idx] = cloned
		}
		updated = *cloneProto(cloned)
		return nil
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

	idx := findNamespace(cfg, name)
	if idx < 0 {
		return nil
	}
	return cloneProto(cfg.Namespaces[idx].Policies)
}

func (s *Store) PatchNamespaceHierarchyPolicies(name string, policy *metadatapb.HierarchyPolicies) (metadatapb.HierarchyPolicies, error) {
	if name == "" {
		return metadatapb.HierarchyPolicies{}, ErrInvalidInput
	}

	var updated metadatapb.HierarchyPolicies
	err := s.updateConfig(func(cfg *metadatapb.Cluster) error {
		idx := findNamespace(cfg, name)
		if idx < 0 {
			return ErrNotFound
		}
		cfg.Namespaces[idx].Policies = cloneProto(policy)
		if cfg.Namespaces[idx].Policies != nil {
			updated = *cloneProto(cfg.Namespaces[idx].Policies)
		}
		return nil
	})
	return updated, err
}

func (s *Store) GetNamespaceState(name string) (*metadatapb.NamespaceState, error) {
	if name == "" {
		return nil, ErrInvalidInput
	}

	status, err := s.loadStatus()
	if err != nil {
		return nil, err
	}

	namespace, ok := status.Namespaces[name]
	if !ok || namespace == nil {
		return nil, ErrNotFound
	}
	return cloneProto(namespace), nil
}

func (s *Store) DeleteNamespaceStates(names []string) error {
	toDelete := makeStringSet(names)
	return s.updateStatus(func(status *metadatapb.ClusterState) error {
		if len(toDelete) == 0 || status.Namespaces == nil {
			return nil
		}
		for name := range toDelete {
			delete(status.Namespaces, name)
		}
		return nil
	})
}

func (s *Store) CreateNamespaceStates(namespaces map[string]*metadatapb.NamespaceState) error {
	return s.updateStatus(func(status *metadatapb.ClusterState) error {
		if status.Namespaces == nil {
			status.Namespaces = make(map[string]*metadatapb.NamespaceState)
		}

		for name, namespace := range namespaces {
			if name == "" || namespace == nil {
				return ErrInvalidInput
			}
			if _, ok := status.Namespaces[name]; ok {
				return fmt.Errorf("%w: namespace state %q", ErrAlreadyExists, name)
			}
			status.Namespaces[name] = cloneProto(namespace)
		}
		return nil
	})
}

func (s *Store) PatchNamespaceState(name string, namespace *metadatapb.NamespaceState) (metadatapb.NamespaceState, error) {
	if name == "" || namespace == nil {
		return metadatapb.NamespaceState{}, ErrInvalidInput
	}

	var updated metadatapb.NamespaceState
	err := s.updateStatus(func(status *metadatapb.ClusterState) error {
		if status.Namespaces == nil {
			status.Namespaces = make(map[string]*metadatapb.NamespaceState)
		}
		cloned := cloneProto(namespace)
		status.Namespaces[name] = cloned
		updated = *cloneProto(cloned)
		return nil
	})
	return updated, err
}

func (s *Store) GetShardState(namespace string, shardID int64) (*metadatapb.ShardState, error) {
	if namespace == "" {
		return nil, ErrInvalidInput
	}

	status, err := s.loadStatus()
	if err != nil {
		return nil, err
	}

	namespaceState, ok := status.Namespaces[namespace]
	if !ok || namespaceState == nil {
		return nil, ErrNotFound
	}
	shard, ok := namespaceState.Shards[shardID]
	if !ok || shard == nil {
		return nil, ErrNotFound
	}
	return cloneProto(shard), nil
}

func (s *Store) PatchShardState(namespace string, shardID int64, shard *metadatapb.ShardState) (metadatapb.ShardState, error) {
	if namespace == "" || shard == nil {
		return metadatapb.ShardState{}, ErrInvalidInput
	}

	var updated metadatapb.ShardState
	err := s.updateStatus(func(status *metadatapb.ClusterState) error {
		namespaceState, ok := status.Namespaces[namespace]
		if !ok || namespaceState == nil {
			return ErrNotFound
		}
		if namespaceState.Shards == nil {
			namespaceState.Shards = make(map[int64]*metadatapb.ShardState)
		}
		cloned := cloneProto(shard)
		namespaceState.Shards[shardID] = cloned
		updated = *cloneProto(cloned)
		return nil
	})
	return updated, err
}

func (s *Store) runLeaseLoop() {
	for {
		if s.ctx.Err() != nil {
			s.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			return
		}

		if err := ensureParentDirectoryExists(s.leaderLockPath); err != nil {
			s.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		} else if err := s.leaderLock.TryLock(); err == nil {
			s.setLeaseState(metadatapb.LeaseState_LEASE_STATE_HELD)
			<-s.ctx.Done()
			s.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			_ = s.leaderLock.Unlock()
			return
		} else {
			s.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		}

		select {
		case <-s.ctx.Done():
			s.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			return
		case <-time.After(leaseRetryInterval):
		}
	}
}

func (s *Store) setLeaseState(state metadatapb.LeaseState) {
	current, _ := s.leaseWatch.Load()
	if current != state {
		s.leaseWatch.Notify(state)
	}
}

func (s *Store) requireLease() error {
	state, _ := s.leaseWatch.Load()
	if state != metadatapb.LeaseState_LEASE_STATE_HELD {
		return ErrLeaseNotHeld
	}
	return nil
}

func (s *Store) loadConfig() (*metadatapb.Cluster, error) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	return s.loadConfigLocked()
}

func (s *Store) loadConfigLocked() (*metadatapb.Cluster, error) {
	cluster := &metadatapb.Cluster{}
	if err := readProtoFile(s.configPath, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (s *Store) updateConfig(fn func(*metadatapb.Cluster) error) error {
	if err := s.requireLease(); err != nil {
		return err
	}

	s.configMu.Lock()
	defer s.configMu.Unlock()

	cluster, err := s.loadConfigLocked()
	if err != nil {
		return err
	}
	if err := fn(cluster); err != nil {
		return err
	}
	return writeProtoFile(s.configPath, cluster)
}

func (s *Store) loadStatus() (*metadatapb.ClusterState, error) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.loadStatusLocked()
}

func (s *Store) loadStatusLocked() (*metadatapb.ClusterState, error) {
	status := &metadatapb.ClusterState{}
	if err := readProtoFile(s.statePath, status); err != nil {
		return nil, err
	}
	return status, nil
}

func (s *Store) updateStatus(fn func(*metadatapb.ClusterState) error) error {
	if err := s.requireLease(); err != nil {
		return err
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	status, err := s.loadStatusLocked()
	if err != nil {
		return err
	}
	if err := fn(status); err != nil {
		return err
	}
	return writeProtoFile(s.statePath, status)
}

func lockPath(configPath string, statePath string) string {
	switch {
	case configPath != "":
		return configPath + ".leader.lock"
	case statePath != "":
		return statePath + ".leader.lock"
	default:
		return "metadata_v2.leader.lock"
	}
}

func findDataServer(cluster *metadatapb.Cluster, name string) int {
	for i, dataServer := range cluster.DataServers {
		if dataServer != nil && dataServer.Name == name {
			return i
		}
	}
	return -1
}

func findNamespace(cluster *metadatapb.Cluster, name string) int {
	for i, namespace := range cluster.Namespaces {
		if namespace != nil && namespace.Name == name {
			return i
		}
	}
	return -1
}

func readProtoFile(path string, msg gproto.Message) error {
	if path == "" {
		return ErrInvalidInput
	}

	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(bytes.TrimSpace(content)) == 0 {
		return nil
	}

	return protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}.Unmarshal(content, msg)
}

func writeProtoFile(path string, msg gproto.Message) error {
	if path == "" {
		return ErrInvalidInput
	}
	if err := ensureParentDirectoryExists(path); err != nil {
		return err
	}

	content, err := protojson.MarshalOptions{
		Indent:        "  ",
		Multiline:     true,
		UseProtoNames: true,
	}.Marshal(msg)
	if err != nil {
		return err
	}
	content = append(content, '\n')

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}

	tmpName := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
	}()

	if _, err := tmpFile.Write(content); err != nil {
		return err
	}
	if err := tmpFile.Chmod(0o600); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return nil
}

func ensureParentDirectoryExists(path string) error {
	parentDir := filepath.Dir(path)
	if _, err := os.Stat(parentDir); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		if err := os.MkdirAll(parentDir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func makeStringSet(values []string) map[string]bool {
	res := make(map[string]bool, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		res[value] = true
	}
	return res
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func cloneProto[T gproto.Message](msg T) T {
	if any(msg) == nil {
		var zero T
		return zero
	}
	return gproto.Clone(msg).(T)
}
