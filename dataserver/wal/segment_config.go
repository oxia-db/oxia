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

package wal

import (
	"github.com/oxia-db/oxia/dataserver/wal/codec"
)

type segmentConfig struct {
	codec         codec.Codec
	segmentExists bool
	txnPath       string
	idxPath       string
	baseOffset    int64
}

func newSegmentConfig(basePath string, baseOffset int64) (*segmentConfig, error) {
	_codec, segmentExists, err := codec.GetOrCreate(segmentPath(basePath, baseOffset))
	if err != nil {
		return nil, err
	}

	return &segmentConfig{
		codec:         _codec,
		segmentExists: segmentExists,
		txnPath:       segmentPath(basePath, baseOffset) + _codec.GetTxnExtension(),
		idxPath:       segmentPath(basePath, baseOffset) + _codec.GetIdxExtension(),
		baseOffset:    baseOffset,
	}, nil
}
