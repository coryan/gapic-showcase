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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ds "github.com/googleapis/gapic-showcase/conformance/datastore"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	dsproxy "github.com/googleapis/gapic-showcase/testproxies/datastore"
	"google.golang.org/grpc"
)

const goSkipRegex = "^(" +
	"ValueRejectDirectlyNestedArray|" +
	"LookupExposesEntityMetadata|" +
	"MutationExposesMutationResultMetadata|" +
	"QueryProjectionKeylessEntity|" +
	"TxBeginLaterParallelReadsSerialized|" +
	"TxReadOnlyRejectsWrite|" +
	"RetryNoRetryBeginTransactionDeadlineExceeded" +
	")$"

func startTestProxyGRPC(t *testing.T) string {
	t.Helper()
	srv := dsproxy.NewServer()
	t.Cleanup(srv.Close)

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

func TestCLIDatastoreFixtureAddr(t *testing.T) {
	addr := startTestProxyGRPC(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. JSON report skipping known Go divergences succeeds with 0 failures.
	var jsonBuf bytes.Buffer
	err := runCLI(ctx, []string{
		"--json", "datastore",
		"--fixture-addr", addr,
		"--transports=grpc",
		"--skip", goSkipRegex,
	}, &jsonBuf)
	if err != nil {
		t.Fatalf("runCLI JSON failed: %v\noutput:\n%s", err, jsonBuf.String())
	}

	var report ds.SuiteReport
	if err := json.Unmarshal(jsonBuf.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON report: %v\nraw:\n%s", err, jsonBuf.String())
	}
	if report.Failed != 0 {
		t.Fatalf("expected 0 failed, got %d", report.Failed)
	}
	if report.Passed < 40 {
		t.Fatalf("expected >=40 passed, got %d", report.Passed)
	}

	// 2. Human-readable text summary.
	var textBuf bytes.Buffer
	err = runCLI(ctx, []string{
		"datastore",
		"--fixture-addr", addr,
		"--run", "^HeaderDefaultDatabase$",
	}, &textBuf)
	if err != nil {
		t.Fatalf("runCLI text failed: %v\noutput:\n%s", err, textBuf.String())
	}
	if !strings.Contains(textBuf.String(), "Summary: 1 passed, 0 failed, 0 skipped") {
		t.Fatalf("unexpected text summary:\n%s", textBuf.String())
	}

	// 3. Running an un-skipped divergence exits with error.
	var divBuf bytes.Buffer
	err = runCLI(ctx, []string{
		"datastore",
		"--fixture-addr", addr,
		"--run", "^ValueRejectDirectlyNestedArray$",
	}, &divBuf)
	if err == nil {
		t.Fatalf("expected non-nil error for known divergence; output:\n%s", divBuf.String())
	}

	// 4. --strict turns FEATURE_NOT_IMPLEMENTED skip into failure.
	var strictBuf bytes.Buffer
	err = runCLI(ctx, []string{
		"datastore",
		"--fixture-addr", addr,
		"--run", "^HeaderClientDefaultNamespace$",
		"--strict",
	}, &strictBuf)
	if err == nil {
		t.Fatalf("expected non-nil error under --strict for unimplemented feature; output:\n%s", strictBuf.String())
	}
}

func TestCLIDatastoreFixtureCmd(t *testing.T) {
	binPath := filepath.Join(t.TempDir(), "datastore-proxy")
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	buildCmd := exec.Command("go", "build", "-o", binPath, "./testproxies/datastore/cmd")
	buildCmd.Dir = repoRoot
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build datastore testproxy failed: %v\n%s", err, string(out))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var jsonBuf bytes.Buffer
	if err := runCLI(ctx, []string{
		"--json", "datastore",
		"--fixture-cmd", binPath,
		"--skip", goSkipRegex,
	}, &jsonBuf); err != nil {
		t.Fatalf("runCLI --fixture-cmd failed: %v\noutput:\n%s", err, jsonBuf.String())
	}

	var report ds.SuiteReport
	if err := json.Unmarshal(jsonBuf.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON report: %v\nraw:\n%s", err, jsonBuf.String())
	}
	if report.Failed != 0 || report.Passed < 40 {
		t.Fatalf("unexpected fixture-cmd report: passed=%d failed=%d skipped=%d",
			report.Passed, report.Failed, report.Skipped)
	}

	// Verify wrapper commands like "go run" (which spawn child processes) shut down cleanly without hanging.
	t.Run("go_run_wrapper", func(t *testing.T) {
		wrapCtx, wrapCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer wrapCancel()

		var wrapBuf bytes.Buffer
		wrapperCmd := "go -C " + repoRoot + " run ./testproxies/datastore/cmd"
		if err := runCLI(wrapCtx, []string{
			"--json", "datastore",
			"--fixture-cmd", wrapperCmd,
			"--run", "^HeaderDefaultDatabase$",
		}, &wrapBuf); err != nil {
			t.Fatalf("runCLI --fixture-cmd go run wrapper failed: %v\noutput:\n%s", err, wrapBuf.String())
		}
		var wrapReport ds.SuiteReport
		if err := json.Unmarshal(wrapBuf.Bytes(), &wrapReport); err != nil {
			t.Fatalf("invalid wrapper JSON report: %v\nraw:\n%s", err, wrapBuf.String())
		}
		if wrapReport.Passed != 1 || wrapReport.Failed != 0 {
			t.Fatalf("unexpected wrapper report: %+v", wrapReport)
		}
	})
}
