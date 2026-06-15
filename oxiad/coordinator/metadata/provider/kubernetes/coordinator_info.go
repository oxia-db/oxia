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

package kubernetes

import (
	"encoding/base64"
	"errors"
	"strings"

	gproto "google.golang.org/protobuf/proto"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

// coordinatorInfoLeaseIdentityPrefix distinguishes encoded coordinator info from
// legacy plain-text lease identities during rolling upgrades.
const coordinatorInfoLeaseIdentityPrefix = "oxia-ci:"

func EncodeCoordinatorInfo(info *commonproto.CoordinatorInfo) (string, error) {
	if info == nil {
		return "", errors.New("coordinator info must not be nil")
	}

	data, err := gproto.Marshal(info)
	if err != nil {
		return "", err
	}
	return coordinatorInfoLeaseIdentityPrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

func DecodeCoordinatorInfo(identity string) (*commonproto.CoordinatorInfo, error) {
	if !strings.HasPrefix(identity, coordinatorInfoLeaseIdentityPrefix) {
		return &commonproto.CoordinatorInfo{Identity: identity}, nil
	}

	encoded := strings.TrimPrefix(identity, coordinatorInfoLeaseIdentityPrefix)
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}

	info := &commonproto.CoordinatorInfo{}
	if err = gproto.Unmarshal(data, info); err != nil {
		return nil, err
	}
	return info, nil
}
