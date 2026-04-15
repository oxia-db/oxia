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

package model

import (
	"errors"
	"fmt"
)

var (
	ErrDataServerNotFound     = errors.New("data server not found")
	ErrInvalidDataServerPatch = errors.New("invalid data server patch")
)

type Server struct {
	// Name is the unique identification for clusters
	Name *string `json:"name" yaml:"name"`

	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`

	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

type ServerMetadata struct {
	// Labels represents a key-value map to store metadata associated with a server.
	Labels map[string]string `json:"labels" yaml:"labels"`
}

type DataServerPatch struct {
	PublicAddress   *string
	InternalAddress *string
	Metadata        *ServerMetadata
}

func (sv *Server) GetIdentifier() string {
	if sv.Name == nil || *sv.Name == "" {
		return sv.Internal
	}
	return *sv.Name
}

func (sm *ServerMetadata) Clone() ServerMetadata {
	if sm == nil {
		return ServerMetadata{}
	}

	labels := make(map[string]string, len(sm.Labels))
	for key, value := range sm.Labels {
		labels[key] = value
	}
	return ServerMetadata{Labels: labels}
}

func (cc *ClusterConfig) PatchDataServer(dataServer string, patch DataServerPatch) (*Server, error) {
	if dataServer == "" {
		return nil, fmt.Errorf("%w: data server must not be empty", ErrInvalidDataServerPatch)
	}
	if patch.PublicAddress == nil && patch.InternalAddress == nil && patch.Metadata == nil {
		return nil, fmt.Errorf("%w: at least one data server patch field must be provided", ErrInvalidDataServerPatch)
	}

	for i := range cc.Servers {
		server := &cc.Servers[i]
		if server.GetIdentifier() != dataServer {
			continue
		}

		if patch.PublicAddress != nil {
			if *patch.PublicAddress == "" {
				return nil, fmt.Errorf("%w: public address must not be empty", ErrInvalidDataServerPatch)
			}
			server.Public = *patch.PublicAddress
		}

		if patch.InternalAddress != nil {
			if *patch.InternalAddress == "" {
				return nil, fmt.Errorf("%w: internal address must not be empty", ErrInvalidDataServerPatch)
			}
			if server.Name == nil || *server.Name == "" {
				return nil, fmt.Errorf("%w: cannot patch internal address for unnamed data server %q", ErrInvalidDataServerPatch, dataServer)
			}
			server.Internal = *patch.InternalAddress
		}

		if patch.Metadata != nil {
			identifier := server.GetIdentifier()
			if len(patch.Metadata.Labels) == 0 {
				delete(cc.ServerMetadata, identifier)
			} else {
				if cc.ServerMetadata == nil {
					cc.ServerMetadata = make(map[string]ServerMetadata)
				}
				cc.ServerMetadata[identifier] = patch.Metadata.Clone()
			}
		}

		return server, nil
	}

	return nil, fmt.Errorf("%w: %q", ErrDataServerNotFound, dataServer)
}
