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

package oxia

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/oxia-db/oxia/node"
)

var (
	exampleServerAddr = "localhost:6648"
)

func initExampleServer(serviceAddr string) *node.Standalone {
	dir, _ := os.MkdirTemp(os.TempDir(), "oxia-test-*")
	config := node.NewTestConfig(dir)
	config.PublicServiceAddr = serviceAddr
	standaloneServer, err := node.NewStandalone(config)
	if err != nil {
		log.Fatal(err)
	}

	return standaloneServer
}

func Example() {
	standaloneServer := initExampleServer(exampleServerAddr)
	defer standaloneServer.Close()

	// Creates a client instance
	// Once created, a client instance will be valid until it's explicitly closed, and it can
	// be used from different go-routines.
	client, err := NewSyncClient(exampleServerAddr, WithRequestTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Write a record to Oxia with the specified key and value, and with the expectation
	// that the record does not already exist.
	_, res1, err := client.Put(context.Background(), "/my-key", []byte("value-1"), ExpectedRecordNotExists())
	if err != nil {
		log.Fatal(err)
	}

	// Write a record with the expectation that it has not changed since the previous write.
	// If there was any change, the operation will fail
	_, _, err = client.Put(context.Background(), "/my-key", []byte("value-2"), ExpectedVersionId(res1.VersionId))
	if err != nil {
		log.Fatal(err)
	}

	// Read the value of a record
	key, value, version, err := client.Get(context.Background(), "/my-key")
	if err != nil {
		log.Fatal(err)
	}
	_ = client.Close()

	fmt.Printf("Result: key: %s - Value: %s - Version: %#v\n", key, string(value), version.VersionId)
	// Output: Result: key: /my-key - Value: value-2 - Version: 1
}

func ExampleAsyncClient() {
	standaloneServer := initExampleServer(exampleServerAddr)
	defer standaloneServer.Close()

	// Creates a client instance
	// Once created, a client instance will be valid until it's explicitly closed, and it can
	// be used from different go-routines.
	client, err := NewAsyncClient(exampleServerAddr, WithRequestTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}

	// Write a record to Oxia with the specified key and value, and with the expectation
	// that the record does not already exist.
	// The client library will try to batch multiple operations into a single request, to
	// achieve much better efficiency and performance
	c1 := client.Put("/my-key-1", []byte("value-1"))
	c2 := client.Put("/my-key-2", []byte("value-2"))
	c3 := client.Put("/my-key-3", []byte("value-3"))

	// Wait for the async operations to complete
	r1 := <-c1
	fmt.Printf("First operation complete: version: %#v - error: %#v\n", r1.Version.VersionId, r1.Err)

	r2 := <-c2
	fmt.Printf("First operation complete: version: %#v - error: %#v\n", r2.Version.VersionId, r2.Err)

	r3 := <-c3
	fmt.Printf("First operation complete: version: %#v - error: %#v\n", r3.Version.VersionId, r3.Err)

	_ = client.Close()

	// Output:
	// First operation complete: version: 0 - error: <nil>
	// First operation complete: version: 1 - error: <nil>
	// First operation complete: version: 2 - error: <nil>
}

func ExampleNotifications() {
	standaloneServer := initExampleServer(exampleServerAddr)
	defer standaloneServer.Close()

	client, err := NewSyncClient(exampleServerAddr)
	if err != nil {
		log.Fatal(err)
	}

	notifications, err := client.GetNotifications()
	if err != nil {
		log.Fatal(err)
	}

	_, _, err = client.Put(context.Background(), "/my-key", []byte("value-1"), ExpectedRecordNotExists())
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Go(func() {
		time.Sleep(1 * time.Second)
		_ = notifications.Close()
	})

	// Receive all the notification from the server
	for notification := range notifications.Ch() {
		fmt.Printf("Type %#v - Key: %s - VersionId: %d\n",
			notification.Type, notification.Key, notification.VersionId)
	}

	_ = client.Close()
	wg.Wait()

	// Output:
	// Type 0 - Key: /my-key - VersionId: 0
}
