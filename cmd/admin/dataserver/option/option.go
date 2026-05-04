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
	"strings"

	"github.com/spf13/pflag"
)

const (
	PublicFlagName   = "public"
	InternalFlagName = "internal"
	LabelFlagName    = "label"
)

type DataServerFields struct {
	PublicAddress   *string
	InternalAddress *string
	Labels          *[]string
}

func (f *DataServerFields) AddFlags(flagSet *pflag.FlagSet) {
	flagSet.Var(&optionalStringValue{target: &f.PublicAddress}, PublicFlagName, "Public address for the data server")
	flagSet.Var(&optionalStringValue{target: &f.InternalAddress}, InternalFlagName, "Internal address for the data server")
	flagSet.Var(&optionalStringArrayValue{target: &f.Labels}, LabelFlagName, "Label to attach to the data server in key=value form")
}

func (f *DataServerFields) Reset() {
	f.PublicAddress = nil
	f.InternalAddress = nil
	f.Labels = nil
}

type optionalStringValue struct {
	target **string
}

func (v *optionalStringValue) String() string {
	if *v.target == nil {
		return ""
	}
	return **v.target
}

func (v *optionalStringValue) Set(value string) error {
	copied := value
	*v.target = &copied
	return nil
}

func (*optionalStringValue) Type() string {
	return "string"
}

type optionalStringArrayValue struct {
	target **[]string
}

func (v *optionalStringArrayValue) String() string {
	if *v.target == nil {
		return ""
	}
	return strings.Join(**v.target, ",")
}

func (v *optionalStringArrayValue) Set(value string) error {
	if *v.target == nil {
		values := []string{value}
		*v.target = &values
		return nil
	}

	values := append(**v.target, value)
	*v.target = &values
	return nil
}

func (*optionalStringArrayValue) Type() string {
	return "stringArray"
}
