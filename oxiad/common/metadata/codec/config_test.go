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

package codec

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

func TestConfigCodecMarshalYAMLWhileSharedSubMessageIsFormatted(t *testing.T) {
	server := &commonproto.DataServerIdentity{Public: "localhost:6648", Internal: "localhost:6649"}
	config := &commonproto.ClusterConfiguration{
		Servers: []*commonproto.DataServerIdentity{server},
	}

	// Formatting a message stores its reflection state the first time.
	formatted := make(chan struct{})
	go func() {
		defer close(formatted)
		_ = server.String()
	}()
	data, err := ClusterConfigCodec.MarshalYAML(config)
	<-formatted

	require.NoError(t, err)
	require.Contains(t, string(data), "public: localhost:6648")
}

func TestConfigCodecUnmarshalYAMLRejectsInvalidYAML(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalYAML([]byte("servers: [unterminated"))
	require.Error(t, err)
	require.Nil(t, config)
}

func TestConfigCodecUnmarshalJSONRejectsInvalidJSON(t *testing.T) {
	config, err := ClusterConfigCodec.UnmarshalJSON([]byte(`{"servers": "not a list"}`))
	require.Error(t, err)
	require.Nil(t, config)
}
