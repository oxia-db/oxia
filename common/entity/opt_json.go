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

package entity

import (
	"encoding/json"
	"reflect"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type OptBooleanDefaultTrue struct {
	val *bool
}

func Bool(val bool) OptBooleanDefaultTrue {
	return OptBooleanDefaultTrue{&val}
}

func (o *OptBooleanDefaultTrue) Get() bool {
	if o.val != nil {
		return *o.val
	}

	return true
}

func (o *OptBooleanDefaultTrue) MarshalJSON() ([]byte, error) {
	return json.Marshal(o.val)
}

func (o OptBooleanDefaultTrue) MarshalYAML() (any, error) {
	return o.val, nil
}

var trueVal = true
var falseVal = false

func (o *OptBooleanDefaultTrue) UnmarshalJSON(data []byte) error {
	s := string(data)
	if s == "null" || s == "" || s == `""` {
		o.val = &trueVal
		return nil
	}

	if s == "true" {
		o.val = &trueVal
		return nil
	}

	if s == "false" {
		o.val = &falseVal
		return nil
	}

	return errors.New("invalid boolean value: " + s)
}

func OptBooleanViperHook() mapstructure.DecodeHookFuncType {
	return func(_ reflect.Type, t reflect.Type, data any) (any, error) {
		if t == reflect.TypeOf(OptBooleanDefaultTrue{}) {
			b, ok := data.(bool)
			if !ok {
				return nil, errors.Errorf("invalid boolean value: '%v'", data)
			}

			return Bool(b), nil
		}

		return data, nil
	}
}
