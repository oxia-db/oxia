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

import "github.com/spf13/pflag"

const (
	PublicFlagName   = "public"
	InternalFlagName = "internal"
	LabelFlagName    = "label"
)

type DataServerFields struct {
	PublicAddress   *string
	InternalAddress *string
	Labels          []string
}

func (f *DataServerFields) AddFlags(flagSet *pflag.FlagSet) {
	f.PublicAddress = flagSet.String(PublicFlagName, "", "Public address for the data server")
	f.InternalAddress = flagSet.String(InternalFlagName, "", "Internal address for the data server")
	flagSet.StringArrayVar(&f.Labels, LabelFlagName, nil, "Label to attach to the data server in key=value form")
}

func (f *DataServerFields) Reset() {
	f.PublicAddress = nil
	f.InternalAddress = nil
	f.Labels = nil
}
