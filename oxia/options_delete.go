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

package oxia

type deleteOptions struct {
	baseOptions
	expectedVersion *int64
}

// DeleteOption represents an option for the [SyncClient.Delete] operation.
type DeleteOption interface {
	PutOption
	applyDelete(opts *deleteOptions)
}

func newDeleteOptions(opts []DeleteOption) *deleteOptions {
	deleteOpts := &deleteOptions{}
	for _, opt := range opts {
		opt.applyDelete(deleteOpts)
	}
	return deleteOpts
}

// ExpectedVersionId Marks that the operation should only be successful
// if the versionId of the record stored in the server matches the expected one.
func ExpectedVersionId(versionId int64) DeleteOption {
	return &expectedVersionId{versionId}
}

type expectedVersionId struct {
	versionId int64
}

func (e *expectedVersionId) applyPut(opts *putOptions) {
	opts.expectedVersion = &e.versionId
}

func (e *expectedVersionId) applyDelete(opts *deleteOptions) {
	opts.expectedVersion = &e.versionId
}
