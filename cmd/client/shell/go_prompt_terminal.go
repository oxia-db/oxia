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

package shell

import (
	"os"

	prompt "github.com/c-bata/go-prompt"
	"golang.org/x/term"
)

// goPromptTerminal is the terminal of go-prompt, which completes the commands.
type goPromptTerminal struct{}

func (goPromptTerminal) Run(prefix string, execute func(line string) (exit bool)) {
	// coverage-ignore: go-prompt opens /dev/tty before it applies the options
	fd := int(os.Stdin.Fd())
	if state, err := term.GetState(fd); err == nil {
		// coverage-ignore: stdin is a terminal
		defer func() { _ = term.Restore(fd, state) }()
	}
	// coverage-ignore: go-prompt opens /dev/tty before it applies the options
	exit := false
	prompt.New(
		func(line string) {
			// coverage-ignore: go-prompt calls it on the input of a terminal
			exit = execute(line)
		},
		complete,
		prompt.OptionPrefix(prefix),
		prompt.OptionMaxSuggestion(promptMaxSuggestions),
		prompt.OptionSetExitCheckerOnInput(func(_ string, breakline bool) bool {
			// coverage-ignore: go-prompt calls it on the input of a terminal
			return breakline && exit
		}),
	).Run()
}
