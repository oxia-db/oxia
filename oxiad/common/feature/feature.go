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

// Package feature provides feature negotiation capabilities for Oxia.
// It enables safe rolling upgrades where new and old nodes can coexist
// by negotiating which features are supported by all members of a quorum.
package feature

import (
	"github.com/oxia-db/oxia/common/proto"
)

func SupportedFeatures() []proto.Feature {
	return []proto.Feature{
		proto.Feature_FEATURE_DB_CHECKSUM,
	}
}
