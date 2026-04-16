package file

import (
	"github.com/oxia-db/oxia/common/proto"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
)

var _ metadata_v2.Store = &Store{}

type Store struct {
}

func (s Store) LeaseWatch() commonoption.Watch[proto.LeaseState] {
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

func (s Store) UpdateAllowedAuthorities(authorities []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetLoadBalancerPolicies() *proto.LoadBalancerPolicies {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchLoadBalancerPolicies(policies *proto.LoadBalancerPolicies) (*proto.LoadBalancerPolicies, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetClusterHierarchyPolicies(name string) *proto.HierarchyPolicies {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchClusterHierarchyPolicies(policies *proto.HierarchyPolicies) (*proto.HierarchyPolicies, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetDataServer(name string) (*proto.DataServer, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) DeleteDataServers(names []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) CreateDataServers(dataServers []*proto.DataServer) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) ListDataServer() ([]*proto.DataServer, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchDataServer(dataServer *proto.DataServer) (proto.DataServer, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetNamespace(name string) (*proto.Namespace, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) DeleteNamespaces(names []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) CreateNamespaces(namespaces []*proto.Namespace) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) ListNamespace() ([]*proto.Namespace, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchNamespace(namespace *proto.Namespace) (proto.Namespace, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetNamespaceHierarchyPolicies(name string) *proto.HierarchyPolicies {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchNamespaceHierarchyPolicies(policy *proto.HierarchyPolicies) (proto.HierarchyPolicies, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetNamespaceState(name string) {
	//TODO implement me
	panic("implement me")
}

func (s Store) DeleteNamespaceStates(names []string) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) CreateNamespaceStates(namespace []*proto.NamespaceState) error {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchNamespaceState(namespace *proto.NamespaceState) (proto.NamespaceState, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) GetShardState(name string) (*proto.ShardState, error) {
	//TODO implement me
	panic("implement me")
}

func (s Store) PatchShardState(shard *proto.ShardState) (proto.ShardState, error) {
	//TODO implement me
	panic("implement me")
}
