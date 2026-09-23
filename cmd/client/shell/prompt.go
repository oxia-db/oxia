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
	"errors"
	"strings"

	prompt "github.com/c-bata/go-prompt"
)

const promptMaxSuggestions = 12

var (
	topLevelSuggestions = []prompt.Suggest{
		{Text: commandAdmin, Description: "Run an admin command"},
		{Text: "get", Description: "Read a key"},
		{Text: "put", Description: "Write a key"},
		{Text: "delete", Description: "Delete a key"},
		{Text: "delete-range", Description: "Delete a key range"},
		{Text: "list", Description: "List keys"},
		{Text: "range-scan", Description: "Scan keys and values"},
		{Text: "help", Description: "Show available commands"},
		{Text: "exit", Description: "Exit the shell"},
		{Text: "quit", Description: "Exit the shell"},
	}
	adminCommandSuggestions = []prompt.Suggest{
		{Text: "namespace", Description: "Manage namespaces"},
		{Text: "dataserver", Description: "Manage data servers"},
		{Text: "shard", Description: "Manage shards"},
	}
	adminResourceSuggestions = []prompt.Suggest{
		{Text: "list", Description: "List resources"},
		{Text: "get", Description: "Show one resource"},
		{Text: "create", Description: "Create a resource"},
		{Text: "patch", Description: "Patch a resource"},
		{Text: "delete", Description: "Delete a resource"},
	}
	shardCommandSuggestions = []prompt.Suggest{
		{Text: "split", Description: "Split a shard"},
	}
	clientFlagSuggestions = map[string][]prompt.Suggest{
		"get": {
			{Text: "--hex", Description: "Print value as hex dump"},
			{Text: "--include-version", Description: "Include version metadata"},
			{Text: "--partition-key", Description: "Partition routing key"},
			{Text: "--index", Description: "Secondary index name"},
			{Text: "--comparison-type", Description: "equal, floor, ceiling, lower, or higher"},
		},
		"put": {
			{Text: "--expected-version", Description: "Conditional write version"},
			{Text: "--create-only", Description: "Require record to be absent"},
			{Text: "--ephemeral", Description: "Create an ephemeral record"},
			{Text: "--partition-key", Description: "Partition routing key"},
			{Text: "--sequence-keys-deltas", Description: "Comma-separated sequence deltas"},
			{Text: "--index", Description: "Secondary index as NAME=KEY"},
		},
		"delete": {
			{Text: "--expected-version", Description: "Conditional delete version"},
			{Text: "--partition-key", Description: "Partition routing key"},
		},
		"delete-range": {
			{Text: "--key-min", Description: "Key range minimum"},
			{Text: "--key-max", Description: "Key range maximum"},
			{Text: "--partition-key", Description: "Partition routing key"},
		},
		"list": {
			{Text: "--key-min", Description: "Key range minimum"},
			{Text: "--key-max", Description: "Key range maximum"},
			{Text: "--partition-key", Description: "Partition routing key"},
			{Text: "--index", Description: "Secondary index name"},
			{Text: "--internal-keys", Description: "Include internal keys"},
		},
		"range-scan": {
			{Text: "--key-min", Description: "Key range minimum"},
			{Text: "--key-max", Description: "Key range maximum"},
			{Text: "--hex", Description: "Print values as hex dumps"},
			{Text: "--include-version", Description: "Include version metadata"},
			{Text: "--partition-key", Description: "Partition routing key"},
			{Text: "--index", Description: "Secondary index name"},
			{Text: "--internal-keys", Description: "Include internal keys"},
		},
	}
	namespaceFlagSuggestions = map[string][]prompt.Suggest{
		"list": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
		},
		"get": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
		},
		"delete": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
		},
		"create": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
			{Text: "--initial-shards", Description: "Namespace initial shard count"},
			{Text: "--replication-factor", Description: "Namespace replication factor"},
			{Text: "--notifications", Description: "Enable namespace notifications"},
			{Text: "--key-sorting", Description: "natural or hierarchical"},
		},
		"patch": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
			{Text: "--replication-factor", Description: "Namespace replication factor"},
			{Text: "--notifications", Description: "Enable namespace notifications"},
		},
	}
	dataServerFlagSuggestions = map[string][]prompt.Suggest{
		"list": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
		},
		"get": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
		},
		"delete": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
		},
		"create": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
			{Text: "--public", Description: "Data server public address"},
			{Text: "--internal", Description: "Data server internal address"},
			{Text: "--label", Description: "Data server label as KEY=VALUE"},
		},
		"patch": {
			{Text: "--output", Description: "Output format: json, yaml, or table"},
			{Text: "--public", Description: "Data server public address"},
			{Text: "--internal", Description: "Data server internal address"},
			{Text: "--label", Description: "Data server label as KEY=VALUE"},
		},
	}
	shardFlagSuggestions = []prompt.Suggest{
		{Text: "--namespace", Description: "Namespace name"},
		{Text: "--shard", Description: "Shard ID"},
		{Text: "--split-point", Description: "Split hash point"},
	}
)

func (r *repl) runPrompt() error {
	var (
		exitRequested bool
		runErr        error
	)

	p := prompt.New(
		func(line string) {
			err := r.executeLine(line)
			switch {
			case errors.Is(err, errExit):
				exitRequested = true
			case err != nil:
				runErr = err
			default:
			}
		},
		complete,
		prompt.OptionPrefix(r.prompt),
		prompt.OptionMaxSuggestion(promptMaxSuggestions),
		prompt.OptionSetExitCheckerOnInput(func(_ string, breakline bool) bool {
			return breakline && (exitRequested || runErr != nil)
		}),
	)
	p.Run()
	return runErr
}

func complete(doc prompt.Document) []prompt.Suggest {
	line := doc.TextBeforeCursor()
	word := doc.GetWordBeforeCursor()
	completedWords := strings.Fields(line)
	if word != "" && !strings.HasSuffix(line, " ") {
		completedWords = completedWords[:len(completedWords)-1]
	}

	suggestions := suggestionsFor(completedWords)
	return prompt.FilterHasPrefix(suggestions, word, true)
}

func suggestionsFor(args []string) []prompt.Suggest {
	switch len(args) {
	case 0:
		return topLevelSuggestions
	case 1:
		switch args[0] {
		case commandAdmin:
			return adminCommandSuggestions
		default:
			return clientFlagSuggestions[args[0]]
		}
	case 2:
		switch args[0] {
		case commandAdmin:
			switch args[1] {
			case "namespace", "namespaces", "dataserver", "dataservers", "data-server", "data-servers":
				return adminResourceSuggestions
			case "shard", "shards":
				return shardCommandSuggestions
			default:
				return nil
			}
		default:
			return nil
		}
	case 3:
		if args[0] == commandAdmin {
			return adminFlagSuggestions(args[1], args[2])
		}
	default:
		return nil
	}
	return nil
}

func adminFlagSuggestions(resource string, command string) []prompt.Suggest {
	switch resource {
	case "namespace", "namespaces":
		return namespaceFlagSuggestions[command]
	case "dataserver", "dataservers", "data-server", "data-servers":
		return dataServerFlagSuggestions[command]
	case "shard", "shards":
		if command == "split" {
			return shardFlagSuggestions
		}
	default:
		return nil
	}
	return nil
}
