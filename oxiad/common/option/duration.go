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

package option

import (
	"time"

	"github.com/invopop/jsonschema"
	"go.yaml.in/yaml/v3"
)

// Duration is a wrapper around time.Duration that supports YAML unmarshaling from string format (e.g., "1h", "30m").
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}

	duration, err := time.ParseDuration(s)
	if err != nil {
		return err
	}

	*d = Duration(duration)
	return nil
}
func (d *Duration) MarshalYAML() (any, error) {
	return time.Duration(*d).String(), nil
}

func (d *Duration) ToDuration() time.Duration {
	return time.Duration(*d)
}

// JSONSchema implements the jsonschema.ReflectType interface.
// Notice: We have no choice but to use the value receiver here.
func (Duration) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:        "string",
		Title:       "Duration",
		Description: "Duration string in Go duration format (e.g., \"1h\", \"30m\", \"5s\")",
		Pattern:     "^[0-9]+[smh]$",
		Examples: []any{
			"1h",
			"30m",
			"5s",
		},
	}
}
