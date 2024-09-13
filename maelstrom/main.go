// Copyright 2023 StreamNative, Inc.
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

package main

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/impl"
	"github.com/streamnative/oxia/coordinator/model"
	"github.com/streamnative/oxia/server"
)

var (
	logLevelStr string
	rootCmd     = &cobra.Command{
		Use:               "oxia-maelstrom",
		Short:             "Run oxia in Maelstrom mode",
		Long:              `Run oxia in Maelstrom mode`,
		PersistentPreRunE: configureLogLevel,
	}
)

type LogLevelError string

func (l LogLevelError) Error() string {
	return fmt.Sprintf("unknown log level (%s)", string(l))
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&logLevelStr, "log-level", "l", common.DefaultLogLevel.String(), "Set logging level [debug|info|warn|error]")
	rootCmd.PersistentFlags().BoolVarP(&common.LogJSON, "log-json", "j", false, "Print logs in JSON format")
}

func configureLogLevel(_ *cobra.Command, _ []string) error {
	logLevel, err := common.ParseLogLevel(logLevelStr)
	if err != nil {
		return LogLevelError(logLevelStr)
	}
	common.LogLevel = logLevel
	common.ConfigureLogger()
	return nil
}

var thisNode string
var allNodes []string

func handleInit(scanner *bufio.Scanner) {
	for {
		if err := receiveInit(scanner); err != nil {
			continue
		}

		return
	}
}

func receiveInit(scanner *bufio.Scanner) error {
	if !scanner.Scan() {
		slog.Error("no init received")
		os.Exit(1)
	}

	line := scanner.Text()
	slog.Info(
		"Got line",
		slog.Any("line", []byte(line)),
	)
	reqType, req, _ := parseRequest(line)
	if reqType != MsgTypeInit {
		slog.Error(
			"Unexpected request while waiting for init",
			slog.Any("req", req),
		)
		return errors.New("invalid message type")
	}

	init := req.(*Message[Init])

	thisNode = init.Body.NodeId
	allNodes = init.Body.NodesIDs

	slog.Info(
		"Received init request",
		slog.String("this-node", thisNode),
		slog.Any("all-nodes", allNodes),
	)

	sendResponse(Message[EmptyResponse]{
		Src:  thisNode,
		Dest: init.Src,
		Body: EmptyResponse{BaseMessageBody{
			Type:      "init_ok",
			MsgId:     msgIdGenerator.Add(1),
			InReplyTo: &init.Body.MsgId,
		}},
	})

	return nil
}

func main() {
	common.ConfigureLogger()
	// NOTE: we must change the default logger to use Stderr for output,
	// because stdout is used as communication channel, see `sendErrorWithCode` for
	// more details.
	slog.SetDefault(slog.New(slog.NewJSONHandler(
		os.Stderr,
		&slog.HandlerOptions{},
	)))

	path, _ := os.Getwd()
	slog.Info(
		"Starting Oxia in Maelstrom mode",
		slog.String("PWD", path),
	)
	scanner := bufio.NewScanner(os.Stdin)
	handleInit(scanner)

	// Start event loop to handle requests
	grpcProvider := newMaelstromGrpcProvider()
	replicationGrpcProvider := newMaelstromReplicationRpcProvider()
	dispatcher := newDispatcher(grpcProvider, replicationGrpcProvider)

	var servers []model.ServerAddress
	for _, node := range allNodes {
		if node != thisNode {
			servers = append(servers, model.ServerAddress{
				Public:   node,
				Internal: node,
			})
		}
	}

	dataDir, err := os.MkdirTemp("", "oxia-maelstrom")
	if err != nil {
		slog.Error(
			"failed to create data dir",
			slog.Any("error", err),
		)
		os.Exit(1)
	}

	if thisNode == "n1" {
		// First node is going to be the "coordinator"
		clusterConfig := model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:              common.DefaultNamespace,
				ReplicationFactor: 3,
				InitialShardCount: 1,
			}},
			Servers: servers,
		}

		_, err := impl.NewCoordinator(
			impl.NewMetadataProviderFile(filepath.Join(dataDir, "cluster-status.json")),
			func() (model.ClusterConfig, error) { return clusterConfig, nil }, nil,
			newRpcProvider(dispatcher))
		if err != nil {
			slog.Error(
				"failed to create coordinator",
				slog.Any("error", err),
			)
			os.Exit(1)
		}
	} else {
		// Any other node will be a storage node
		_, err := server.NewWithGrpcProvider(server.Config{
			MetricsServiceAddr: "",
			DataDir:            filepath.Join(dataDir, thisNode, "db"),
			WalDir:             filepath.Join(dataDir, thisNode, "wal"),
		}, grpcProvider, replicationGrpcProvider)
		if err != nil {
			return
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		rt, req, protoMsg := parseRequest(line)

		dispatcher.ReceivedMessage(rt, req, protoMsg)
	}
}
