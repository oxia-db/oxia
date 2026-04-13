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

package validation

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

var validNamespacePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.\-]*$`)

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
	return nil
}
