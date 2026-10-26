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

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/multierr"

	commonproto "github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	coordoption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	coordreconciler "github.com/oxia-db/oxia/oxiad/coordinator/reconciler"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"

	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/oxiad/common/logging"
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
	rootCmd.PersistentFlags().StringVarP(&logLevelStr, "log-level", "l", logging.DefaultLogLevel.String(), "Set logging level [debug|info|warn|error]")
	rootCmd.PersistentFlags().BoolVarP(&logging.LogJSON, "log-json", "j", false, "Print logs in JSON format")
}

func configureLogLevel(_ *cobra.Command, _ []string) error {
	logLevel, err := logging.ParseLogLevel(logLevelStr)
	if err != nil {
		return LogLevelError(logLevelStr)
	}
	logging.LogLevel = logLevel
	logging.ConfigureLogger()
	return nil
}

var thisNode string
var allNodes []string

var errNoInit = errors.New("no init received")

var errNotInitMessage = errors.New("invalid message type")

func handleInit(scanner *bufio.Scanner) error {
	for {
		err := receiveInit(scanner)
		if errors.Is(err, errNotInitMessage) {
			continue
		}
		return err
	}
}

func receiveInit(scanner *bufio.Scanner) error {
	if !scanner.Scan() {
		return errNoInit
	}

	line := scanner.Text()
	slog.Info(
		"Got line",
		slog.Any("line", []byte(line)),
	)
	reqType, req, _, err := parseRequest(line)
	if err != nil {
		return err
	}
	if reqType != MsgTypeInit {
		slog.Error(
			"Unexpected request while waiting for init",
			slog.Any("req", req),
		)
		return errNotInitMessage
	}

	init := req.(*Message[Init])

	thisNode = init.Body.NodeId
	allNodes = init.Body.NodesIDs

	slog.Info(
		"Received init request",
		slog.String("this-node", thisNode),
		slog.Any("all-nodes", allNodes),
	)

	return sendResponse(Message[EmptyResponse]{
		Src:  thisNode,
		Dest: init.Src,
		Body: EmptyResponse{BaseMessageBody{
			Type:      "init_ok",
			MsgId:     msgIdGenerator.Add(1),
			InReplyTo: &init.Body.MsgId,
		}},
	})
}

// configureLogger writes the logs to stderr, because stdout is the channel of
// the Maelstrom messages, see sendErrorWithCode.
func configureLogger() {
	logging.ConfigureLogger()
	slog.SetDefault(slog.New(slog.NewJSONHandler(
		os.Stderr,
		&slog.HandlerOptions{},
	)))
}

// run serves the Maelstrom messages read by scanner until its input ends, and
// fails on a message it can't handle, e.g. a malformed one. The node n1 runs
// the coordinator, and the others run a data server that keeps its data in a
// subdirectory of dataDir, per node id.
func run(scanner *bufio.Scanner, dataDir string) error {
	slog.Info(
		"Starting Oxia in Maelstrom mode",
		slog.String("data-dir", dataDir),
	)
	if err := handleInit(scanner); err != nil {
		return err
	}

	// Start event loop to handle requests
	grpcProvider := newMaelstromGrpcProvider()
	replicationGrpcProvider := newMaelstromReplicationRpcProvider()
	dispatcher := newDispatcher(grpcProvider, replicationGrpcProvider)

	var servers []*commonproto.DataServerIdentity
	for _, node := range allNodes {
		if node != thisNode {
			servers = append(servers, &commonproto.DataServerIdentity{
				Public:   node,
				Internal: node,
			})
		}
	}

	var node io.Closer
	var err error
	if thisNode == "n1" {
		options := &coordoption.Options{Metadata: coordoption.MetadataOptions{
			ProviderOptions: coordoption.ProviderOptions{ProviderName: metadatacommon.NameMemory},
		}}
		if node, err = runCoordinator(dispatcher, options, servers); err != nil {
			return errors.Wrap(err, "failed to start coordinator")
		}
	} else if node, err = runDataServer(grpcProvider, replicationGrpcProvider, dataDir); err != nil {
		return err
	}

	return multierr.Append(serve(scanner, dispatcher), node.Close())
}

func serve(scanner *bufio.Scanner, dispatcher *dispatcher) error {
	for scanner.Scan() {
		rt, req, protoMsg, err := parseRequest(scanner.Text())
		if err != nil {
			return err
		}
		if err := dispatcher.ReceivedMessage(rt, req, protoMsg); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func runDataServer(grpcProvider *maelstromGrpcProvider, replicationGrpcProvider *maelstromReplicationRpcProvider,
	dataDir string) (*dataserver.Server, error) {
	slog.Info(
		"Using data dir",
		slog.String("data-dir", dataDir),
		slog.String("node-id", thisNode),
	)

	dataServerOption := option.NewDefaultOptions()
	dataServerOption.FeatureFlags.AuthorityValidation = new(bool)
	dataServerOption.Observability.Metric.Enabled = &constant.FlagFalse
	dataServerOption.Storage.Database.Dir = filepath.Join(dataDir, thisNode, "db")
	dataServerOption.Storage.WAL.Dir = filepath.Join(dataDir, thisNode, "wal")
	manifest, err := manifestpkg.NewManifest(dataServerOption.Storage.Database.Dir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create dataserver manifest")
	}
	server, err := dataserver.NewWithGrpcProvider(
		context.Background(),
		commonwatch.New(dataServerOption),
		grpcProvider,
		replicationGrpcProvider,
		manifest,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start dataserver")
	}
	return server, nil
}

// coordinator closes what runCoordinator started, in the reverse order.
type coordinator struct {
	cancel  context.CancelFunc
	closers []io.Closer
}

func (c *coordinator) Close() error {
	c.cancel()
	var err error
	for i := len(c.closers) - 1; i >= 0; i-- {
		err = multierr.Append(err, c.closers[i].Close())
	}
	return err
}

// runCoordinator starts a coordinator that keeps its metadata with the
// provider of options.
func runCoordinator(dispatcher *dispatcher, options *coordoption.Options,
	servers []*commonproto.DataServerIdentity) (_ io.Closer, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &coordinator{cancel: cancel}
	defer func() {
		if err != nil {
			err = multierr.Append(err, c.Close())
		}
	}()

	metadataFactory, err := coordmetadata.New(ctx, options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create coordinator metadata factory")
	}
	c.closers = append(c.closers, metadataFactory)

	metadata := metadataFactory.CreateMetadata(ctx)
	c.closers = append(c.closers, metadata)
	if err := c.start(ctx, dispatcher, metadata, servers); err != nil {
		return nil, err
	}
	return c, nil
}

// start seeds metadata with the data servers and the default namespace, and
// coordinates them. The caller closes c, also when start fails.
func (c *coordinator) start(ctx context.Context, dispatcher *dispatcher, metadata coordmetadata.Metadata,
	servers []*commonproto.DataServerIdentity) error {
	clusterConfig := &commonproto.ClusterConfiguration{
		Namespaces: []*commonproto.Namespace{{
			Name:              constant.DefaultNamespace,
			ReplicationFactor: 3,
			InitialShardCount: 1,
		}},
		Servers: servers,
	}

	if _, err := metadata.WaitToBecomeLeader(); err != nil {
		return errors.Wrap(err, "failed to wait for coordinator metadata leadership")
	}
	for _, server := range clusterConfig.GetServers() {
		if err := metadata.CreateDataServer(&commonproto.DataServer{Identity: server}); err != nil {
			return errors.Wrap(err, "failed to seed coordinator data servers")
		}
	}
	for _, namespace := range clusterConfig.GetNamespaces() {
		if err := metadata.CreateNamespace(namespace); err != nil {
			return errors.Wrap(err, "failed to seed coordinator namespaces")
		}
	}

	coordinatorRuntime, err := coordruntime.New( //nolint:contextcheck
		metadata,
		func(instanceID string) rpc.ControllerRPCProvider {
			return newRpcProvider(dispatcher)
		},
	)
	if err != nil {
		return errors.Wrap(err, "failed to create coordinator")
	}
	c.closers = append(c.closers, coordinatorRuntime)

	reconciler, err := coordreconciler.New(ctx, coordinatorRuntime)
	if err != nil {
		return errors.Wrap(err, "failed to reconcile the cluster configuration")
	}
	c.closers = append(c.closers, reconciler)
	return nil
}
