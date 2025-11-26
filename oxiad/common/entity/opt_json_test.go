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
	"testing"

	"github.com/stretchr/testify/assert"
)

type myStruct struct {
	Name    string                `json:"name"`
	Enabled OptBooleanDefaultTrue `json:"enabled"`
}

func TestOptBoolean_MarshallTrue(t *testing.T) {
	ms := &myStruct{
		Name:    "hello",
		Enabled: Bool(true),
	}
	ser, err := json.Marshal(ms)
	assert.NoError(t, err)

	assert.Equal(t, `{"name":"hello","enabled":true}`, string(ser))
}

func TestOptBoolean_MarshallFalse(t *testing.T) {
	ms := &myStruct{
		Name:    "hello",
		Enabled: Bool(false),
	}
	ser, err := json.Marshal(ms)
	assert.NoError(t, err)

	assert.Equal(t, `{"name":"hello","enabled":false}`, string(ser))
}

func TestOptBoolean_UnmarshallTrue(t *testing.T) {
	data := []byte(`{"name":"hello", "enabled":true}`)

	ms := &myStruct{}
	err := json.Unmarshal(data, ms)
	assert.NoError(t, err)

	assert.Equal(t, "hello", ms.Name)
	assert.True(t, ms.Enabled.Get())
}

func TestOptBoolean_UnmarshallFalse(t *testing.T) {
	data := []byte(`{"name":"hello", "enabled":false}`)

	ms := &myStruct{}
	err := json.Unmarshal(data, ms)
	assert.NoError(t, err)

	assert.Equal(t, "hello", ms.Name)
	assert.False(t, ms.Enabled.Get())
}

func TestOptBoolean_UnmarshallMissing(t *testing.T) {
	data := []byte(`{"name":"hello"}`)

	ms := &myStruct{}
	err := json.Unmarshal(data, ms)
	assert.NoError(t, err)

	assert.Equal(t, "hello", ms.Name)
	assert.True(t, ms.Enabled.Get())
}

func TestOptBoolean_UnmarshallNull(t *testing.T) {
	data := []byte(`{"name":"hello", "enabled":null}`)

	ms := &myStruct{}
	err := json.Unmarshal(data, ms)
	assert.NoError(t, err)

	assert.Equal(t, "hello", ms.Name)
	assert.True(t, ms.Enabled.Get())
}

func TestOptBoolean_UnmarshallError(t *testing.T) {
	ms := &myStruct{}

	err := json.Unmarshal([]byte(`{"name":"hello", "enabled":5}`), ms)
	assert.Error(t, err)

	err = json.Unmarshal([]byte(`{"name":"hello", "enabled":"invalid"}`), ms)
	assert.Error(t, err)
}
