// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command datastore-testproxy runs the DatastoreFixture gRPC test proxy wrapping cloud.google.com/go/datastore.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	dsproxy "github.com/googleapis/gapic-showcase/testproxies/datastore"
	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 0, "TCP port to listen on (0 allocates an ephemeral port)")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	proxy := dsproxy.NewServer()
	defer proxy.Close()

	srv := grpc.NewServer()
	fixturepb.RegisterDatastoreFixtureServer(srv, proxy)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		srv.GracefulStop()
	}()

	log.Printf("Datastore Go test proxy listening on %s", lis.Addr().String())
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
