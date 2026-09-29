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

package validation

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var validNamespacePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.\-]*$`)

// A data server keeps these names for itself, in the same directory as the
// directories of the namespaces, so no namespace can take one. File systems
// can be case-insensitive, so they are reserved in any letter case.
const (
	// KeywordNamespaceManifest is the file a data server keeps its manifest in.
	KeywordNamespaceManifest = "MANIFEST"
	// KeywordNamespaceSnapshots is the directory a data server keeps the
	// database snapshots in.
	KeywordNamespaceSnapshots = "snapshots"
)

func ValidateNamespace(namespace string) error {
	if namespace == "" {
		return errors.New("namespace must not be empty")
	}
	if strings.Contains(namespace, "..") {
		return errors.Errorf("namespace %q contains invalid path traversal sequence", namespace)
	}
	if strings.ContainsAny(namespace, `/\`) {
		return errors.Errorf("namespace %q contains invalid path separator", namespace)
	}
	if !validNamespacePattern.MatchString(namespace) {
		return errors.Errorf("namespace %q contains invalid characters", namespace)
	}
	if strings.EqualFold(namespace, KeywordNamespaceManifest) {
		return errors.Errorf("namespace %q is reserved: it collides with the data server manifest file", namespace)
	}
	if strings.EqualFold(namespace, KeywordNamespaceSnapshots) {
		return errors.Errorf("namespace %q is reserved: it collides with the data server snapshots directory", namespace)
	}
	return nil
}
