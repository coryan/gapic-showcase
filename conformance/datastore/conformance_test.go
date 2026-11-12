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

package datastore

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	dssrv "github.com/googleapis/gapic-showcase/testproxies/datastore"
	"google.golang.org/grpc"
)

// knownGoDivergences lists conformance scenarios where cloud.google.com/go/datastore v1.28.1
// is known to diverge from the Cloud Datastore Client Library Specification.
var knownGoDivergences = []string{
	"LookupExposesEntityMetadata",
	"MutationExposesMutationResultMetadata",
	// knownGoDivergencesEnd
}

func startInProcessFixture(t *testing.T, srv fixturepb.DatastoreFixtureServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	fixturepb.RegisterDatastoreFixtureServer(gs, srv)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = gs.Serve(lis)
	}()

	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
		<-done
	})
	return lis.Addr().String()
}

func TestGoDatastoreProxyConformance(t *testing.T) {
	srv := dssrv.NewServer()
	defer srv.Close()
	addr := startInProcessFixture(t, srv)

	cfg := SuiteConfig{
		FixtureAddr: addr,
		Transports:  []fixturepb.Transport{fixturepb.Transport_TRANSPORT_GRPC},
		Timeout:     15 * time.Second,
	}
	if len(knownGoDivergences) > 0 {
		cfg.SkipPattern = "^(" + strings.Join(knownGoDivergences, "|") + ")$"
	}

	report, err := RunSuite(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunSuite failed: %v", err)
	}

	var buf strings.Builder
	_ = FormatReport(&buf, report, false)
	t.Log("\n" + buf.String())

	if report.Failed != 0 {
		t.Fatalf("expected 0 failures when skipping known Go divergences, got %d", report.Failed)
	}
	if got, want := report.Passed+report.Skipped, len(AllScenarios()); got != want {
		t.Fatalf("expected passed+skipped=%d, got %d", want, got)
	}
	if len(AllScenarios()) >= 60 && report.Passed < 40 {
		t.Fatalf("expected at least 40 passing scenarios on Go proxy, got %d", report.Passed)
	}
}

func TestGoKnownDivergencesDetected(t *testing.T) {
	if len(knownGoDivergences) == 0 {
		t.Skip("no known Go divergences registered yet")
	}
	srv := dssrv.NewServer()
	defer srv.Close()
	addr := startInProcessFixture(t, srv)

	report, err := RunSuite(context.Background(), SuiteConfig{
		FixtureAddr: addr,
		Transports:  []fixturepb.Transport{fixturepb.Transport_TRANSPORT_GRPC},
		RunPattern:  "^(" + strings.Join(knownGoDivergences, "|") + ")$",
		Timeout:     15 * time.Second,
	})
	if err != nil {
		t.Fatalf("RunSuite failed: %v", err)
	}

	var buf strings.Builder
	_ = FormatReport(&buf, report, false)
	t.Log("\n" + buf.String())

	if report.Passed != 0 || report.Skipped != 0 || report.Failed != len(knownGoDivergences) {
		t.Fatalf("expected all %d known divergences to fail (0 pass, 0 skip); got passed=%d failed=%d skipped=%d",
			len(knownGoDivergences), report.Passed, report.Failed, report.Skipped)
	}
}

func TestReferenceConformanceAllScenarios(t *testing.T) {
	srv := newReferenceFixtureServer()
	defer srv.Close()
	addr := startInProcessFixture(t, srv)

	report, err := RunSuite(context.Background(), SuiteConfig{
		FixtureAddr: addr,
		Transports:  []fixturepb.Transport{fixturepb.Transport_TRANSPORT_GRPC, fixturepb.Transport_TRANSPORT_HTTP_JSON},
		Strict:      true,
		Timeout:     15 * time.Second,
	})
	if err != nil {
		t.Fatalf("RunSuite failed: %v", err)
	}

	var buf strings.Builder
	_ = FormatReport(&buf, report, false)
	t.Log("\n" + buf.String())

	wantTotal := 2 * len(AllScenarios())
	if report.Failed != 0 || report.Skipped != 0 || report.Passed != wantTotal {
		t.Fatalf("expected %d passed, 0 failed, 0 skipped across gRPC+REST under --strict; got passed=%d failed=%d skipped=%d",
			wantTotal, report.Passed, report.Failed, report.Skipped)
	}
}
