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

package tls

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	commonoption "github.com/oxia-db/oxia/oxiad/common/option"

	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	rpc2 "github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/dataserver"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/rpc"

	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxia"
)

func getPeerTLSOption() (*security.TLSOptions, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	parentDir := filepath.Dir(pwd)
	caCertPath := filepath.Join(parentDir, "certs", "ca.crt")
	peerCertPath := filepath.Join(parentDir, "certs", "peer.crt")
	peerKeyPath := filepath.Join(parentDir, "certs", "peer.key")

	peerOption := security.TLSOptions{
		CertFile:      peerCertPath,
		KeyFile:       peerKeyPath,
		TrustedCaFile: caCertPath,
	}
	return &peerOption, nil
}

func getClientTLSOption() (*security.TLSOptions, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	parentDir := filepath.Dir(pwd)
	caCertPath := filepath.Join(parentDir, "certs", "ca.crt")
	peerCertPath := filepath.Join(parentDir, "certs", "client.crt")
	peerKeyPath := filepath.Join(parentDir, "certs", "client.key")

	clientOption := security.TLSOptions{
		CertFile:      peerCertPath,
		KeyFile:       peerKeyPath,
		TrustedCaFile: caCertPath,
	}
	return &clientOption, nil
}

func newTLSServer(t *testing.T) (s *dataserver.Server, addr model.Server) {
	t.Helper()
	return newTLSServerWithInterceptor(t, func(config *dataserveroption.Options) {
	})
}

func newTLSServerWithInterceptor(t *testing.T, interceptor func(config *dataserveroption.Options)) (s *dataserver.Server, addr model.Server) {
	t.Helper()
	option, err := getPeerTLSOption()
	assert.NoError(t, err)

	dataServerOption := dataserveroption.NewDefaultOptions()
	dataServerOption.Server.Public.BindAddress = "localhost:0"
	dataServerOption.Server.Public.TLS = *option
	dataServerOption.Server.Internal.BindAddress = "localhost:0"
	dataServerOption.Server.Internal.TLS = *option
	dataServerOption.Observability.Metric.BindAddress = "localhost:0"
	dataServerOption.Replication.TLS = *option
	dataServerOption.Storage.Database.Dir = t.TempDir()
	dataServerOption.Storage.WAL.Dir = t.TempDir()

	interceptor(dataServerOption)

	s, err = dataserver.New(t.Context(), commonoption.NewWatch(dataServerOption))

	assert.NoError(t, err)

	addr = model.Server{
		Public:   fmt.Sprintf("localhost:%d", s.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", s.InternalPort()),
	}

	return s, addr
}

func TestClusterHandshakeSuccess(t *testing.T) {
	s1, sa1 := newTLSServer(t)
	defer s1.Close()
	s2, sa2 := newTLSServer(t)
	defer s2.Close()
	s3, sa3 := newTLSServer(t)
	defer s3.Close()

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.TryIntoClientTLSConf()
	assert.NoError(t, err)

	clientPool := rpc.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()
}

func TestClientHandshakeFailByNoTlsConfig(t *testing.T) {
	s1, sa1 := newTLSServer(t)
	defer s1.Close()
	s2, sa2 := newTLSServer(t)
	defer s2.Close()
	s3, sa3 := newTLSServer(t)
	defer s3.Close()

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.TryIntoClientTLSConf()
	assert.NoError(t, err)

	clientPool := rpc.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithRequestTimeout(1*time.Second))
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestClientHandshakeByAuthFail(t *testing.T) {
	s1, sa1 := newTLSServer(t)
	defer s1.Close()
	s2, sa2 := newTLSServer(t)
	defer s2.Close()
	s3, sa3 := newTLSServer(t)
	defer s3.Close()

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.TryIntoClientTLSConf()
	assert.NoError(t, err)

	clientPool := rpc.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	tlsOption, err := getClientTLSOption()
	// clear the CA file
	tlsOption.TrustedCaFile = ""
	assert.NoError(t, err)
	tlsConf, err = tlsOption.TryIntoClientTLSConf()
	assert.NoError(t, err)
	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithTLS(tlsConf), oxia.WithRequestTimeout(1*time.Second))
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestClientHandshakeWithInsecure(t *testing.T) {
	s1, sa1 := newTLSServer(t)
	defer s1.Close()
	s2, sa2 := newTLSServer(t)
	defer s2.Close()
	s3, sa3 := newTLSServer(t)
	defer s3.Close()

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.TryIntoClientTLSConf()
	assert.NoError(t, err)

	clientPool := rpc.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	tlsOption, err := getClientTLSOption()
	// clear the CA file
	tlsOption.TrustedCaFile = ""
	tlsOption.InsecureSkipVerify = true
	assert.NoError(t, err)
	tlsConf, err = tlsOption.TryIntoClientTLSConf()
	assert.NoError(t, err)
	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithTLS(tlsConf))
	assert.NoError(t, err)
	client.Close()
}

func TestClientHandshakeSuccess(t *testing.T) {
	s1, sa1 := newTLSServer(t)
	defer s1.Close()
	s2, sa2 := newTLSServer(t)
	defer s2.Close()
	s3, sa3 := newTLSServer(t)
	defer s3.Close()

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.TryIntoClientTLSConf()
	assert.NoError(t, err)

	clientPool := rpc.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	tlsOption, err := getClientTLSOption()
	assert.NoError(t, err)
	tlsConf, err = tlsOption.TryIntoClientTLSConf()
	assert.NoError(t, err)
	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithTLS(tlsConf))
	assert.NoError(t, err)
	client.Close()
}

func TestOnlyEnablePublicTls(t *testing.T) {
	disableInternalTLS := func(config *dataserveroption.Options) {
		config.Server.Internal.TLS.Enabled = &constant.FlagFalse
		config.Replication.TLS.Enabled = &constant.FlagFalse
	}
	s1, sa1 := newTLSServerWithInterceptor(t, disableInternalTLS)
	defer s1.Close()
	s2, sa2 := newTLSServerWithInterceptor(t, disableInternalTLS)
	defer s2.Close()
	s3, sa3 := newTLSServerWithInterceptor(t, disableInternalTLS)
	defer s3.Close()

	metadataProvider := metadata.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.Server{sa1, sa2, sa3},
	}
	clientPool := rpc.NewClientPool(nil, nil)
	defer clientPool.Close()

	coordinatorInstance, err := coordinator.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, rpc2.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinatorInstance.Close()

	// failed without cert

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithRequestTimeout(1*time.Second))
	assert.Error(t, err)
	assert.Nil(t, client)

	// success with cert
	tlsOption, err := getClientTLSOption()
	assert.NoError(t, err)
	tlsConf, err := tlsOption.TryIntoClientTLSConf()
	assert.NoError(t, err)
	client, err = oxia.NewSyncClient(sa1.Public, oxia.WithTLS(tlsConf))
	assert.NoError(t, err)
	client.Close()
}
