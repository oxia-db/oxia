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

package oxia

import "github.com/oxia-db/oxia/common/proto"

type getOptions struct {
	baseOptions
	listOptions
	comparisonType proto.KeyComparisonType
	includeValue   bool
}

// GetOption represents an option for the [SyncClient.Get] operation.
type GetOption interface {
	applyGet(opts *getOptions)
}

func newGetOptions(opts []GetOption) *getOptions {
	getOpts := &getOptions{
		includeValue: true,
	}
	for _, opt := range opts {
		opt.applyGet(getOpts)
	}
	return getOpts
}

type getComparisonType struct {
	comparisonType proto.KeyComparisonType
}

func (t *getComparisonType) applyGet(opts *getOptions) {
	opts.comparisonType = t.comparisonType
}

// ComparisonEqual sets the Get() operation to compare the stored key for equality.
func ComparisonEqual() GetOption {
	return &getComparisonType{proto.KeyComparisonType_EQUAL}
}

// ComparisonFloor option will make the get operation to search for the record whose
// key is the highest key <= to the supplied key.
func ComparisonFloor() GetOption {
	return &getComparisonType{proto.KeyComparisonType_FLOOR}
}

// ComparisonCeiling option will make the get operation to search for the record whose
// key is the lowest key >= to the supplied key.
func ComparisonCeiling() GetOption {
	return &getComparisonType{proto.KeyComparisonType_CEILING}
}

// ComparisonLower option will make the get operation to search for the record whose
// key is strictly < to the supplied key.
func ComparisonLower() GetOption {
	return &getComparisonType{proto.KeyComparisonType_LOWER}
}

// ComparisonHigher option will make the get operation to search for the record whose
// key is strictly > to the supplied key.
func ComparisonHigher() GetOption {
	return &getComparisonType{proto.KeyComparisonType_HIGHER}
}

type includeValue struct {
	includeValue bool
}

func (t *includeValue) applyGet(opts *getOptions) {
	opts.includeValue = t.includeValue
}

func (t *includeValue) applyRangeScan(opts *rangeScanOptions) {
	opts.includeValue = t.includeValue
}

// ReadOption is an option of the operations that read records: Get and RangeScan.
type ReadOption interface {
	GetOption
	RangeScanOption
}

// IncludeValue includes or excludes the values of the records that Get and RangeScan return. Without
// the values, they return the keys and the versions. Older servers send the values of a RangeScan
// anyway, and the client drops them.
func IncludeValue(include bool) ReadOption {
	return &includeValue{
		includeValue: include,
	}
}
