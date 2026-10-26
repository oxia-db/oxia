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

package option

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestPublicServerOptionsBindAddress(t *testing.T) {
	opts := readOptions(t, `
server:
  public:
    bindAddress: coordinator.example.com:7000
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "coordinator.example.com:7000", opts.Server.Public.BindAddress)
}

func TestPublicServerOptionsUnmarshalYAMLRejectsInvalidNode(t *testing.T) {
	opts := &Options{}
	err := yaml.Unmarshal([]byte("server:\n  public: [a, b]\n"), opts)
	require.Error(t, err)
}

func TestServerOptionsUnmarshalYAMLAcceptsDeprecatedAdmin(t *testing.T) {
	opts := readOptions(t, `
server:
  admin:
    bindAddress: 127.0.0.1:7000
    tls:
      certFile: /certs/admin.pem
  internal:
    bindAddress: 127.0.0.1:7001
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, "127.0.0.1:7000", opts.Server.Public.BindAddress)
	require.Equal(t, "/certs/admin.pem", opts.Server.Public.TLS.CertFile)
	require.Equal(t, "127.0.0.1:7001", opts.Server.Internal.BindAddress)
}

func TestServerOptionsUnmarshalYAMLKeepsUnsetFields(t *testing.T) {
	opts := &Options{Server: ServerOptions{Internal: InternalServerOptions{BindAddress: "127.0.0.1:7001"}}}
	require.NoError(t, yaml.Unmarshal([]byte("server:\n  public:\n    bindAddress: 127.0.0.1:7000\n"), opts))

	require.Equal(t, "127.0.0.1:7000", opts.Server.Public.BindAddress)
	require.Equal(t, "127.0.0.1:7001", opts.Server.Internal.BindAddress)
}

func TestServerOptionsUnmarshalYAMLRejectsPublicAndAdmin(t *testing.T) {
	opts := &Options{}
	err := yaml.Unmarshal([]byte(`
server:
  public:
    bindAddress: 127.0.0.1:7000
  admin:
    bindAddress: 127.0.0.1:7002
`), opts)
	require.ErrorContains(t, err, "server.public and deprecated server.admin are both set")
}

func TestServerOptionsUnmarshalYAMLRejectsInvalidNode(t *testing.T) {
	opts := &Options{}
	require.Error(t, yaml.Unmarshal([]byte("server: [a, b]\n"), opts))
}
