package raft

import (
	"context"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

type Store struct {
}

func (s Store) Close() error {
	//TODO implement me
	panic("implement me")
}

func (s Store) LeaseWatch() *commonoption.Watch[metadatapb.LeaseState] {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetAllowedAuthorities() []string {
	//TODO implement me
	panic("implement me")
}

func (s Store) DeleteExtraAllowedAuthorities(authorities []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) AddExtraAllowedAuthorities(authorities []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchAllowedAuthorities(authorities []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetLoadBalancerPolicies() *metadatapb.LoadBalancerPolicies {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchLoadBalancerPolicies(policies *metadatapb.LoadBalancerPolicies) (*metadatapb.LoadBalancerPolicies, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetClusterHierarchyPolicies() *metadatapb.HierarchyPolicies {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchClusterHierarchyPolicies(policies *metadatapb.HierarchyPolicies) (*metadatapb.HierarchyPolicies, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetDataServer(name string) (*metadatapb.DataServer, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) DeleteDataServers(names []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) CreateDataServers(dataServers []*metadatapb.DataServer) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) ListDataServer() ([]*metadatapb.DataServer, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchDataServer(dataServer *metadatapb.DataServer) (*metadatapb.DataServer, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetNamespace(name string) (*metadatapb.Namespace, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) DeleteNamespaces(names []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) CreateNamespaces(namespaces []*metadatapb.Namespace) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) ListNamespace() ([]*metadatapb.Namespace, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchNamespace(namespace *metadatapb.Namespace) (*metadatapb.Namespace, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetNamespaceHierarchyPolicies(name string) *metadatapb.HierarchyPolicies {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchNamespaceHierarchyPolicies(name string, policy *metadatapb.HierarchyPolicies) (*metadatapb.HierarchyPolicies, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetNamespaceState(name string) (*metadatapb.NamespaceState, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) DeleteNamespaceStates(names []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) CreateNamespaceStates(namespace map[string]*metadatapb.NamespaceState) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchNamespaceState(name string, namespace *metadatapb.NamespaceState) (*metadatapb.NamespaceState, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetShardState(namespace string, shardID int64) (*metadatapb.ShardState, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchShardState(namespace string, shardID int64, shard *metadatapb.ShardState) (*metadatapb.ShardState, error) {
	//TODO implement me
	panic("implement me")
}

func NewStore(ctx context.Context, options option.RaftMetadata) *Store {
	return nil
}
