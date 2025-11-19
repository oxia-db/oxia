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

module github.com/oxia-db/oxia/common

go 1.25.2

require (
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/cockroachdb/pebble/v2 v2.1.0
	github.com/coreos/go-oidc/v3 v3.14.1
	github.com/mitchellh/mapstructure v1.5.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.22.0
	github.com/stretchr/testify v1.10.0
	golang.org/x/exp v0.0.0-20250506013437-ce4c2cf36ca6
	google.golang.org/grpc v1.72.0
	google.golang.org/protobuf v1.36.6
)