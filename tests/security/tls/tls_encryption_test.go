// Copyright 2023 StreamNative, Inc.
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

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/common/security"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/oxia"
	"github.com/streamnative/oxia/server"
)

func getPeerTLSOption() (*security.TLSOption, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	parentDir := filepath.Dir(pwd)
	caCertPath := filepath.Join(parentDir, "certs", "ca.crt")
	peerCertPath := filepath.Join(parentDir, "certs", "peer.crt")
	peerKeyPath := filepath.Join(parentDir, "certs", "peer.key")

	peerOption := security.TLSOption{
		CertFile:      peerCertPath,
		KeyFile:       peerKeyPath,
		TrustedCaFile: caCertPath,
	}
	return &peerOption, nil
}

func getClientTLSOption() (*security.TLSOption, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	parentDir := filepath.Dir(pwd)
	caCertPath := filepath.Join(parentDir, "certs", "ca.crt")
	peerCertPath := filepath.Join(parentDir, "certs", "client.crt")
	peerKeyPath := filepath.Join(parentDir, "certs", "client.key")

	clientOption := security.TLSOption{
		CertFile:      peerCertPath,
		KeyFile:       peerKeyPath,
		TrustedCaFile: caCertPath,
	}
	return &clientOption, nil
}

func newTLSServer(t *testing.T) (s *server.Server, addr model.ServerAddress) {
	t.Helper()
	return newTLSServerWithInterceptor(t, func(config *server.Config) {

	})
}

func newTLSServerWithInterceptor(t *testing.T, interceptor func(config *server.Config)) (s *server.Server, addr model.ServerAddress) {
	t.Helper()
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	serverTLSConf, err := option.MakeServerTLSConf()
	assert.NoError(t, err)

	peerTLSConf, err := option.MakeClientTLSConf()
	assert.NoError(t, err)

	config := server.Config{
		PublicServiceAddr:          "localhost:0",
		InternalServiceAddr:        "localhost:0",
		MetricsServiceAddr:         "", // Disable metrics to avoid conflict
		DataDir:                    t.TempDir(),
		WalDir:                     t.TempDir(),
		NotificationsRetentionTime: 1 * time.Minute,
		PeerTLS:                    peerTLSConf,
		ServerTLS:                  serverTLSConf,
		InternalServerTLS:          serverTLSConf,
	}

	interceptor(&config)

	s, err = server.New(config)

	assert.NoError(t, err)

	addr = model.ServerAddress{
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

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTLSConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()
}

func TestClientHandshakeFailByNoTlsConfig(t *testing.T) {
	s1, sa1 := newTLSServer(t)
	defer s1.Close()
	s2, sa2 := newTLSServer(t)
	defer s2.Close()
	s3, sa3 := newTLSServer(t)
	defer s3.Close()

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTLSConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

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

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTLSConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	tlsOption, err := getClientTLSOption()
	// clear the CA file
	tlsOption.TrustedCaFile = ""
	assert.NoError(t, err)
	tlsConf, err = tlsOption.MakeClientTLSConf()
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

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTLSConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	tlsOption, err := getClientTLSOption()
	// clear the CA file
	tlsOption.TrustedCaFile = ""
	tlsOption.InsecureSkipVerify = true
	assert.NoError(t, err)
	tlsConf, err = tlsOption.MakeClientTLSConf()
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

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	option, err := getPeerTLSOption()
	assert.NoError(t, err)
	tlsConf, err := option.MakeClientTLSConf()
	assert.NoError(t, err)

	clientPool := common.NewClientPool(tlsConf, nil)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	tlsOption, err := getClientTLSOption()
	assert.NoError(t, err)
	tlsConf, err = tlsOption.MakeClientTLSConf()
	assert.NoError(t, err)
	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithTLS(tlsConf))
	assert.NoError(t, err)
	client.Close()
}

func TestOnlyEnablePublicTls(t *testing.T) {
	disableInternalTLS := func(config *server.Config) {
		config.InternalServerTLS = nil
		config.PeerTLS = nil
	}
	s1, sa1 := newTLSServerWithInterceptor(t, disableInternalTLS)
	defer s1.Close()
	s2, sa2 := newTLSServerWithInterceptor(t, disableInternalTLS)
	defer s2.Close()
	s3, sa3 := newTLSServerWithInterceptor(t, disableInternalTLS)
	defer s3.Close()

	metadataProvider := impl.NewMetadataProviderMemory()
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              common.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: []model.ServerAddress{sa1, sa2, sa3},
	}
	clientPool := common.NewClientPool(nil, nil)
	defer clientPool.Close()

	coordinator, err := impl.NewCoordinator(metadataProvider, func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil, impl.NewRpcProvider(clientPool))
	assert.NoError(t, err)
	defer coordinator.Close()

	// failed without cert

	client, err := oxia.NewSyncClient(sa1.Public, oxia.WithRequestTimeout(1*time.Second))
	assert.Error(t, err)
	assert.Nil(t, client)

	// success with cert
	tlsOption, err := getClientTLSOption()
	assert.NoError(t, err)
	tlsConf, err := tlsOption.MakeClientTLSConf()
	assert.NoError(t, err)
	client, err = oxia.NewSyncClient(sa1.Public, oxia.WithTLS(tlsConf))
	assert.NoError(t, err)
	client.Close()
}
