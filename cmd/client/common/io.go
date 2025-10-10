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

package common

import (
	"encoding/hex"
	"encoding/json"
	"io"
)

func WriteOutput(out io.Writer, value any) {
	if sl, ok := value.([]string); ok {
		writeStringSlice(out, sl)
		return
	} else if b, ok := value.([]byte); ok {
		writeByteSlice(out, b)
		return
	}

	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	_, err = out.Write(append(b, "\n"...))
	if err != nil {
		panic(err)
	}
}

func writeStringSlice(out io.Writer, sl []string) {
	for _, s := range sl {
		if _, err := out.Write([]byte(s)); err != nil {
			panic(err)
		}
		if _, err := out.Write([]byte("\n")); err != nil {
			panic(err)
		}
	}
}

func writeByteSlice(out io.Writer, sl []byte) {
	if _, err := out.Write(sl); err != nil {
		panic(err)
	}
	if _, err := out.Write([]byte("\n")); err != nil {
		panic(err)
	}
}

func WriteHexDump(out io.Writer, buffer []byte) {
	dump := hex.Dumper(out)
	if _, err := dump.Write(buffer); err != nil {
		panic(err)
	}
	if err := dump.Close(); err != nil {
		panic(err)
	}
}
