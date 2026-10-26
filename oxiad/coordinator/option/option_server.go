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
	"errors"
	"fmt"

	"go.uber.org/multierr"
	"gopkg.in/yaml.v3"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
)

// ServerOptions configures the two gRPC servers of the coordinator: the public
// server, which serves the admin API, and the internal server.
type ServerOptions struct {
	// Public configures the public server. The deprecated admin key is still
	// accepted in its place, but not together with it.
	Public PublicServerOptions `yaml:"public" json:"public" jsonschema:"description=Public server\\, which serves the admin API. The deprecated admin key is still accepted in its place"`
	// Internal configures the internal server.
	Internal InternalServerOptions `yaml:"internal" json:"internal" jsonschema:"description=Internal server\\, which serves the health checks"`
}

func (so *ServerOptions) UnmarshalYAML(value *yaml.Node) error {
	type Fields ServerOptions
	decoded := struct {
		Fields `yaml:",inline"`
		Admin  *PublicServerOptions `yaml:"admin"`
	}{Fields: Fields(*so)}
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	*so = ServerOptions(decoded.Fields)
	if decoded.Admin == nil {
		return nil
	}
	for i := 0; i < len(value.Content); i += 2 {
		if value.Content[i].Value == "public" {
			return errors.New("server.public and deprecated server.admin are both set")
		}
	}
	so.Public = *decoded.Admin
	return nil
}

func (so *ServerOptions) WithDefault() {
	so.Public.WithDefault()
	so.Internal.WithDefault()
}

func (so *ServerOptions) Validate() error {
	return multierr.Combine(so.Public.Validate(), so.Internal.Validate())
}

// InternalServerOptions configures the internal gRPC server of the
// coordinator, which serves its health checks.
type InternalServerOptions struct {
	// BindAddress is the host:port that the server listens on. A port of 0
	// picks a free port. It defaults to 0.0.0.0:6649.
	BindAddress string `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=host:port that the internal server listens on,default=0.0.0.0:6649,example=0.0.0.0:6649,format=hostname"`
	// Auth authenticates the clients of the server. It is disabled by default:
	// it is enabled when a provider is set, or when enabled is true.
	Auth auth.Options `yaml:"auth,omitempty" json:"auth,omitempty" jsonschema:"description=Authentication of the clients of the internal server. Disabled by default"`
	// TLS secures the connections to the server. It is disabled by default: it
	// is enabled when enabled is true, or, when enabled is unset, when a
	// certificate file is set.
	TLS security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=TLS of the internal server. Disabled by default"`
}

func (iso *InternalServerOptions) WithDefault() {
	if iso.BindAddress == "" {
		iso.BindAddress = fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort)
	}
	iso.Auth.WithDefault()
	iso.TLS.WithDefault()
}

func (iso *InternalServerOptions) Validate() error {
	return multierr.Combine(
		iso.Auth.Validate(),
		iso.TLS.Validate(),
	)
}

// PublicServerOptions configures the public gRPC server of the coordinator,
// which serves the admin API, e.g. to the oxia admin commands, and its health
// checks.
type PublicServerOptions struct {
	// BindAddress is the host:port that the server listens on. A port of 0
	// picks a free port. It defaults to 0.0.0.0:6651.
	BindAddress string `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=host:port that the public server listens on,default=0.0.0.0:6651,example=0.0.0.0:6651,format=hostname"`
	// Auth authenticates the clients of the server. It is disabled by default:
	// it is enabled when a provider is set, or when enabled is true.
	Auth auth.Options `yaml:"auth,omitempty" json:"auth,omitempty" jsonschema:"description=Authentication of the clients of the public server. Disabled by default"`
	// TLS secures the connections to the server. It is disabled by default: it
	// is enabled when enabled is true, or, when enabled is unset, when a
	// certificate file is set.
	TLS security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=TLS of the public server. Disabled by default"`
}

func (pso *PublicServerOptions) WithDefault() {
	defaultBindAddress := fmt.Sprintf("0.0.0.0:%d", constant.DefaultAdminPort)
	if pso.BindAddress == "" {
		pso.BindAddress = defaultBindAddress
	}
	pso.Auth.WithDefault()
	pso.TLS.WithDefault()
}

func (pso *PublicServerOptions) UnmarshalYAML(value *yaml.Node) error {
	type publicServerOptions PublicServerOptions
	var opts publicServerOptions
	if err := value.Decode(&opts); err != nil {
		return err
	}
	*pso = PublicServerOptions(opts)
	return nil
}

func (pso *PublicServerOptions) Validate() error {
	return multierr.Combine(
		pso.Auth.Validate(),
		pso.TLS.Validate(),
	)
}
