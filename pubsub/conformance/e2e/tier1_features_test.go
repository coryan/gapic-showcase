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

package e2e_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// --- Local Unexported Helpers (stdlib-only, zero collision with helpers_test.go / infra_test.go) ---

type t1ListedScenario struct {
	ID       string
	Category string
	Rest     string
}

func t1RawString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

func t1MustScenario(t *testing.T, rep *ParsedReport, id string) *ParsedScenarioResult {
	t.Helper()
	if rep == nil {
		t.Fatalf("t1MustScenario(%q): nil report", id)
	}
	sc, ok := rep.FindScenario(id)
	if !ok {
		t.Fatalf("scenario %q not found in root report (total=%d)", id, len(rep.Scenarios))
	}
	return sc
}

func t1FindTargetScenario(t *testing.T, tr *ParsedTargetReport, id string) *ParsedScenarioResult {
	t.Helper()
	if tr == nil {
		t.Fatalf("t1FindTargetScenario(%q): nil target report", id)
	}
	for i := range tr.Scenarios {
		if tr.Scenarios[i].ID == id {
			return &tr.Scenarios[i]
		}
	}
	t.Fatalf("scenario %q not found in target %q report (total=%d)", id, tr.TargetName, len(tr.Scenarios))
	return nil
}

func t1FindDiff(t *testing.T, rep *ParsedReport, kind, idOrField string) *ParsedDiffEntry {
	t.Helper()
	if rep == nil {
		t.Fatalf("t1FindDiff(%q, %q): nil report", kind, idOrField)
	}
	for i := range rep.Differences {
		d := &rep.Differences[i]
		if d.Kind == kind && (d.ScenarioID == idOrField || d.Field == idOrField) {
			return d
		}
	}
	t.Fatalf("difference kind=%q target=%q not found (total diffs=%d)", kind, idOrField, len(rep.Differences))
	return nil
}

func t1HasDiff(rep *ParsedReport, kind, idOrField string) bool {
	if rep == nil {
		return false
	}
	for i := range rep.Differences {
		d := &rep.Differences[i]
		if d.Kind == kind && (d.ScenarioID == idOrField || d.Field == idOrField) {
			return true
		}
	}
	return false
}

func t1AssertJSONV1Envelope(t *testing.T, rep *ParsedReport, wantMode string) {
	t.Helper()
	if rep == nil {
		t.Fatalf("nil JSON report")
	}
	if got := t1RawString(rep.Raw, "schema_version"); got != "1.0" {
		t.Errorf("schema_version = %q, want %q", got, "1.0")
	}
	genAt := t1RawString(rep.Raw, "generated_at")
	if genAt == "" {
		t.Errorf("generated_at missing or empty")
	} else if _, err := time.Parse(time.RFC3339Nano, genAt); err != nil {
		t.Errorf("generated_at %q invalid RFC3339 timestamp: %v", genAt, err)
	}
	if rep.Mode != wantMode {
		t.Errorf("report mode = %q, want %q", rep.Mode, wantMode)
	}
	if rep.InfraError != "" {
		t.Errorf("unexpected infra_error = %q", rep.InfraError)
	}
	if rep.TimedOut {
		t.Errorf("unexpected report timed_out = true")
	}
	rawDiffs, hasDiffs := rep.Raw["differences"]
	if !hasDiffs || rawDiffs == nil {
		t.Errorf("differences JSON key missing or null (expected non-nil JSON array)")
	} else if _, ok := rawDiffs.([]any); !ok {
		t.Errorf("differences JSON value has type %T, want array", rawDiffs)
	}
}

func t1RunCLIWithReports(t *testing.T, timeout time.Duration, proxyArgs []string, extraCLIArgs ...string) (CLIResult, *ParsedReport, *ParsedMarkdownReport) {
	t.Helper()
	wrapper, _ := writeProxyWrapper(t, proxyArgs...)
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "report.json")
	mdPath := filepath.Join(dir, "report.md")

	args := make([]string, 0, 5+len(extraCLIArgs))
	args = append(args, "--proxy-cmd="+wrapper, "--report-json="+jsonPath, "--report-md="+mdPath)
	args = append(args, extraCLIArgs...)

	res := runCLI(t, timeout, args...)
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stdout, res.Stderr)
	}

	rep := ReadJSONReport(t, jsonPath)
	AssertSummaryMatchesScenarios(t, rep)
	md := ReadMarkdownReport(t, mdPath)
	return res, rep, md
}

func t1ParseCLIList(t *testing.T, stdout string) []t1ListedScenario {
	t.Helper()
	var out []t1ListedScenario
	for _, line := range strings.Split(strings.ReplaceAll(stdout, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) < 3 {
			t.Fatalf("--list line %q has fewer than 3 fields", line)
		}
		// Preserve remainder after the first two fields.
		afterID := strings.TrimSpace(strings.TrimPrefix(trimmed, fields[0]))
		rest := strings.TrimSpace(strings.TrimPrefix(afterID, fields[1]))
		out = append(out, t1ListedScenario{
			ID:       fields[0],
			Category: fields[1],
			Rest:     rest,
		})
	}
	return out
}

func t1AssertExactIDSet(t *testing.T, label string, gotIDs, wantIDs []string) {
	t.Helper()
	got := make(map[string]int, len(gotIDs))
	for _, id := range gotIDs {
		got[id]++
	}
	want := make(map[string]struct{}, len(wantIDs))
	for _, id := range wantIDs {
		want[id] = struct{}{}
	}
	for id, cnt := range got {
		if cnt != 1 {
			t.Errorf("%s: duplicate ID %q (%d times)", label, id, cnt)
		}
		if _, ok := want[id]; !ok {
			t.Errorf("%s: unexpected ID %q", label, id)
		}
	}
	for id := range want {
		if got[id] == 0 {
			t.Errorf("%s: missing expected ID %q", label, id)
		}
	}
}

// --- E2E-F1: Proxy Subprocess Lifecycle & Protocol (5 tests) ---

func TestT1_F1_01_ProxyCmdHandshakeAndCapabilities(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^(PUB-01|PULL-01)$",
	)
	_ = res

	t1AssertJSONV1Envelope(t, rep, "single")
	if rep.Target != "default" {
		t.Errorf("Target = %q, want %q", rep.Target, "default")
	}
	if rep.Capabilities == nil {
		t.Fatalf("Capabilities is nil")
	}
	if rep.Capabilities.ClientLanguage != "go" {
		t.Errorf("ClientLanguage = %q, want %q", rep.Capabilities.ClientLanguage, "go")
	}
	if rep.Capabilities.ClientLibrary == "" {
		t.Errorf("ClientLibrary is empty")
	}
	if rep.Capabilities.ClientVersion == "" {
		t.Errorf("ClientVersion is empty")
	}
	if rep.Capabilities.SupportsMonitoring == nil || !*rep.Capabilities.SupportsMonitoring {
		t.Errorf("SupportsMonitoring = %v, want true", rep.Capabilities.SupportsMonitoring)
	}

	wantSum := ParsedSummary{Total: 2, Pass: 2}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}

	for _, id := range []string{"PUB-01", "PULL-01"} {
		sc := t1MustScenario(t, rep, id)
		if sc.Status != StatusPass || sc.TraceStatus != StatusPass {
			t.Errorf("%s: status=%q trace_status=%q, want PASS/PASS", id, sc.Status, sc.TraceStatus)
		}
	}

	if !md.HasHeadingContaining("Pub/Sub Conformance Report: default") {
		t.Errorf("Markdown missing default report heading; got %v", md.Headings)
	}
	if len(md.Tables) < 3 {
		t.Errorf("Markdown tables count = %d, want >= 3", len(md.Tables))
	}
	if !md.HasTableRowContaining("PUB-01", StatusPass) {
		t.Errorf("Markdown missing PUB-01 PASS row")
	}
	if !md.HasTableRowContaining("PULL-01", StatusPass) {
		t.Errorf("Markdown missing PULL-01 PASS row")
	}
	if md.TimeoutCount != 0 || md.ErrorCount != 0 {
		t.Errorf("Markdown breakdown counts timeout=%d error=%d, want 0/0", md.TimeoutCount, md.ErrorCount)
	}
}

func TestT1_F1_02_ProxyTargetNamedLaunch(t *testing.T) {
	t.Parallel()

	wrapper, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "report.json")
	mdPath := filepath.Join(dir, "report.md")

	res := runCLI(
		t, 30*time.Second,
		"--proxy-target=go-ref="+wrapper,
		"--filter=^PUB-01$",
		"--report-json="+jsonPath,
		"--report-md="+mdPath,
	)
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "[go-ref] PUB-01: PASS") {
		t.Errorf("stderr missing named target progress line; got:\n%s", res.Stderr)
	}

	rep := ReadJSONReport(t, jsonPath)
	AssertSummaryMatchesScenarios(t, rep)
	t1AssertJSONV1Envelope(t, rep, "single")

	if rep.Target != "go-ref" {
		t.Errorf("Target = %q, want %q", rep.Target, "go-ref")
	}
	tr, ok := rep.Targets["go-ref"]
	if !ok || tr == nil {
		t.Fatalf("Targets[%q] missing from parsed report; keys=%v", "go-ref", rep.Targets)
	}
	if tr.TargetError != "" || tr.TimedOut {
		t.Errorf("target go-ref unexpected error=%q timedOut=%v", tr.TargetError, tr.TimedOut)
	}
	if t1RawString(tr.Raw, "command") == "" {
		t.Errorf("target go-ref command missing in JSON")
	}

	sc := t1MustScenario(t, rep, "PUB-01")
	if sc.Status != StatusPass || sc.TraceStatus != StatusPass {
		t.Errorf("root PUB-01 status=%q trace=%q, want PASS/PASS", sc.Status, sc.TraceStatus)
	}
	tsc := t1FindTargetScenario(t, tr, "PUB-01")
	if tsc.Status != StatusPass || tsc.TraceStatus != StatusPass {
		t.Errorf("target PUB-01 status=%q trace=%q, want PASS/PASS", tsc.Status, tsc.TraceStatus)
	}

	md := ReadMarkdownReport(t, mdPath)
	if !md.HasHeadingContaining("Pub/Sub Conformance Report: go-ref") {
		t.Errorf("Markdown missing target heading for go-ref; got %v", md.Headings)
	}
	if !md.ContainsTokens("go-ref") {
		t.Errorf("Markdown missing token go-ref")
	}
	if !md.HasTableRowContaining("go-ref", "go", "true") {
		t.Errorf("Markdown missing capabilities row for go-ref")
	}
	if !md.HasTableRowContaining("PUB-01", StatusPass) {
		t.Errorf("Markdown missing PUB-01 PASS row")
	}
}

func TestT1_F1_03_ProxyArgsForwarding(t *testing.T) {
	t.Parallel()

	t.Run("wrapper_argv_order_and_commas", func(t *testing.T) {
		dir := t.TempDir()
		marker := newRunMarker(t)
		argvLog := filepath.Join(dir, "argv.log")
		jsonPath := filepath.Join(dir, "report.json")
		wrapper := filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))

		script := fmt.Sprintf(
			"#!/bin/sh\nfor a in \"$@\"; do\n  printf '%%s\\n' \"$a\" >> %s\ndone\nexec %s %s '--mode=conformant-go' '--fast-timings' \"$@\"\n",
			shellQuote(argvLog),
			shellQuote(proxyBinaryPath),
			shellQuote("--marker="+marker),
		)
		writeExecutableFile(t, wrapper, []byte(script))

		res := runCLI(
			t, 30*time.Second,
			"--proxy-cmd="+wrapper,
			"--proxy-arg=--language=arg-first",
			"--proxy-arg=--language=arg-last",
			"--proxy-arg=--unsupported=hedging,eod",
			"--filter=^PUB-01$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}

		rawArgv, err := os.ReadFile(argvLog)
		if err != nil {
			t.Fatalf("read argv.log: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(rawArgv)), "\n")
		idxFirst, idxLast, idxComma := -1, -1, -1
		hasPort := false
		for i, ln := range lines {
			switch ln {
			case "--language=arg-first":
				idxFirst = i
			case "--language=arg-last":
				idxLast = i
			case "--unsupported=hedging,eod":
				idxComma = i
			}
			if strings.HasPrefix(ln, "--port=") {
				hasPort = true
			}
		}
		if idxFirst < 0 || idxLast < 0 || idxFirst >= idxLast {
			t.Errorf("forwarded arg order invalid (first=%d last=%d); lines=%q", idxFirst, idxLast, lines)
		}
		if idxComma < 0 {
			t.Errorf("comma-containing arg --unsupported=hedging,eod missing or split; lines=%q", lines)
		}
		if !hasPort {
			t.Errorf("expected --port= flag forwarded to proxy; lines=%q", lines)
		}

		rep := ReadJSONReport(t, jsonPath)
		AssertSummaryMatchesScenarios(t, rep)
		if rep.Capabilities == nil {
			t.Fatalf("Capabilities nil")
		}
		if rep.Capabilities.ClientLanguage != "arg-last" {
			t.Errorf("ClientLanguage = %q, want %q", rep.Capabilities.ClientLanguage, "arg-last")
		}
		if rep.Capabilities.SupportsPublishHedging {
			t.Errorf("SupportsPublishHedging = true, want false from --unsupported=hedging,eod")
		}
		if rep.Capabilities.SupportsExactlyOnceDelivery {
			t.Errorf("SupportsExactlyOnceDelivery = true, want false from --unsupported=hedging,eod")
		}
		if sc := t1MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
			t.Errorf("PUB-01 status = %q, want PASS", sc.Status)
		}
	})

	t.Run("direct_binary_without_wrapper", func(t *testing.T) {
		dir := t.TempDir()
		marker := newRunMarker(t)
		jsonPath := filepath.Join(dir, "report.json")

		res := runCLI(
			t, 30*time.Second,
			"--proxy-cmd="+proxyBinaryPath,
			"--proxy-arg=--marker="+marker,
			"--proxy-arg=--mode=conformant-go",
			"--proxy-arg=--fast-timings",
			"--proxy-arg=--unsupported=hedging,eod",
			"--filter=^PUB-01$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}

		rep := ReadJSONReport(t, jsonPath)
		AssertSummaryMatchesScenarios(t, rep)
		if rep.Capabilities == nil {
			t.Fatalf("Capabilities nil")
		}
		if rep.Capabilities.SupportsPublishHedging || rep.Capabilities.SupportsExactlyOnceDelivery {
			t.Errorf("expected hedging=false and eod=false, got hedging=%v eod=%v",
				rep.Capabilities.SupportsPublishHedging, rep.Capabilities.SupportsExactlyOnceDelivery)
		}
		if sc := t1MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
			t.Errorf("PUB-01 status = %q, want PASS", sc.Status)
		}
	})
}

func TestT1_F1_04_GracefulShutdownNoOrphans(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	marker := newRunMarker(t)
	shutdownLog := filepath.Join(dir, "shutdown.log")
	wrapper := filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))

	// With --fault=ignore-sigterm, e2e-test-proxy only exits cleanly with rc=0
	// if pubsub-conformance shuts it down via ShutdownProxy RPC rather than SIGKILL.
	script := fmt.Sprintf(
		"#!/bin/sh\n%s %s '--mode=conformant-go' '--fast-timings' '--fault=ignore-sigterm' \"$@\" &\npid=$!\nwait \"$pid\"\nrc=$?\nprintf '%%s %%s\\n' \"$pid\" \"$rc\" >> %s\nexit \"$rc\"\n",
		shellQuote(proxyBinaryPath),
		shellQuote("--marker="+marker),
		shellQuote(shutdownLog),
	)
	writeExecutableFile(t, wrapper, []byte(script))

	res := runCLI(t, 20*time.Second, "--proxy-cmd="+wrapper, "--filter=^PUB-01$")
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}
	if res.Duration >= 5*time.Second {
		t.Errorf("shutdown took %v, expected prompt graceful ShutdownProxy < 5s", res.Duration)
	}

	raw, err := os.ReadFile(shutdownLog)
	if err != nil {
		t.Fatalf("read shutdown.log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 shutdown record, got %d: %q", len(lines), lines)
	}
	parts := strings.Fields(lines[0])
	if len(parts) != 2 {
		t.Fatalf("malformed shutdown record %q", lines[0])
	}
	pid, err := strconv.Atoi(parts[0])
	if err != nil || pid <= 0 {
		t.Errorf("invalid child pid in shutdown.log: %q", parts[0])
	}
	rc, err := strconv.Atoi(parts[1])
	if err != nil || rc != 0 {
		t.Errorf("child exit code = %q, want 0 via ShutdownProxy RPC", parts[1])
	}
}

func TestT1_F1_05_MultipleScenarioProxyReuseOrIsolation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	marker := newRunMarker(t)
	launchesLog := filepath.Join(dir, "launches.log")
	jsonPath := filepath.Join(dir, "report.json")
	wrapper := filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))

	script := fmt.Sprintf(
		"#!/bin/sh\nprintf '%%s|%%s\\n' \"$$\" \"${PUBSUB_EMULATOR_HOST:-}\" >> %s\nexec %s %s '--mode=conformant-go' '--fast-timings' \"$@\"\n",
		shellQuote(launchesLog),
		shellQuote(proxyBinaryPath),
		shellQuote("--marker="+marker),
	)
	writeExecutableFile(t, wrapper, []byte(script))

	// Pass stale PUBSUB_EMULATOR_HOST via child-only env to prove harness strips it
	// on non-emulator scenarios and scopes emulator host strictly to CFG-01.
	res := runCLIWithEnv(
		t, 45*time.Second,
		[]string{"PUBSUB_EMULATOR_HOST=stale.invalid:9999"},
		"--proxy-cmd="+wrapper,
		"--filter=^(PUB-01|PORD-02|PUB-10|TRAC-01b|CFG-01|CFG-02|CFG-03|CFG-04)$",
		"--report-json="+jsonPath,
	)
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}

	rep := ReadJSONReport(t, jsonPath)
	AssertSummaryMatchesScenarios(t, rep)
	wantSum := ParsedSummary{Total: 8, Pass: 8}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}

	raw, err := os.ReadFile(launchesLog)
	if err != nil {
		t.Fatalf("read launches.log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	// Expected 6 launches:
	// 0: shared (PUB-01 + PORD-02)
	// 1: isolated (PUB-10)
	// 2: isolated (TRAC-01b)
	// 3: isolated+emulator (CFG-01)
	// 4: isolated (CFG-02)
	// 5: shared (CFG-03 + CFG-04)
	if len(lines) != 6 {
		t.Fatalf("expected 6 proxy launches across 8 scenarios, got %d: %q", len(lines), lines)
	}

	seenPIDs := make(map[int]bool, len(lines))
	emuNonEmptyCount := 0
	for i, ln := range lines {
		pidStr, emuHost, ok := strings.Cut(ln, "|")
		if !ok {
			t.Fatalf("launch[%d] malformed line %q", i, ln)
		}
		pid, err := strconv.Atoi(pidStr)
		if err != nil || pid <= 0 {
			t.Errorf("launch[%d] invalid PID %q", i, pidStr)
		}
		if seenPIDs[pid] {
			t.Errorf("launch[%d] reused duplicate PID %d", i, pid)
		}
		seenPIDs[pid] = true

		if emuHost == "stale.invalid:9999" {
			t.Errorf("launch[%d] leaked stale inherited PUBSUB_EMULATOR_HOST", i)
		}
		if i == 3 {
			if emuHost == "" {
				t.Errorf("launch[3] (CFG-01) expected non-empty PUBSUB_EMULATOR_HOST")
			} else {
				host, portStr, splitErr := net.SplitHostPort(emuHost)
				portNum, pErr := strconv.Atoi(portStr)
				if splitErr != nil || host != "127.0.0.1" || pErr != nil || portNum <= 0 {
					t.Errorf("launch[3] (CFG-01) invalid PUBSUB_EMULATOR_HOST %q", emuHost)
				}
			}
			emuNonEmptyCount++
		} else if emuHost != "" {
			emuNonEmptyCount++
			t.Errorf("launch[%d] unexpected PUBSUB_EMULATOR_HOST=%q outside CFG-01", i, emuHost)
		}
	}
	if emuNonEmptyCount != 1 {
		t.Errorf("non-empty PUBSUB_EMULATOR_HOST count = %d, want 1", emuNonEmptyCount)
	}
}

// --- E2E-F2: Fake Pub/Sub RPC Validation (5 tests) ---

func TestT1_F2_01_PublishBasicPasses(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^PUB-01$",
	)
	if !strings.Contains(res.Stderr, "[default] PUB-01: PASS") {
		t.Errorf("stderr missing [default] PUB-01: PASS:\n%s", res.Stderr)
	}
	if !strings.Contains(res.Stdout, "# Pub/Sub Conformance Report: default") {
		t.Errorf("stdout missing Markdown heading")
	}

	t1AssertJSONV1Envelope(t, rep, "single")
	if rep.Target != "default" {
		t.Errorf("Target = %q, want default", rep.Target)
	}
	wantSum := ParsedSummary{Total: 1, Pass: 1}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	sc := t1MustScenario(t, rep, "PUB-01")
	if sc.Status != StatusPass || sc.TraceStatus != StatusPass || sc.Category != "publish" || sc.ProxyRestarted || sc.Error != "" {
		t.Errorf("PUB-01 unexpected result: %+v", *sc)
	}
	if !md.HasTableRowContaining("PUB-01", "publish", StatusPass) {
		t.Errorf("Markdown missing PUB-01 publish PASS row")
	}
}

func TestT1_F2_02_PublishOrderingFIFOPasses(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^PORD-02$",
	)
	if !strings.Contains(res.Stderr, "[default] PORD-02: PASS") {
		t.Errorf("stderr missing [default] PORD-02: PASS:\n%s", res.Stderr)
	}
	wantSum := ParsedSummary{Total: 1, Pass: 1}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	sc := t1MustScenario(t, rep, "PORD-02")
	if sc.Status != StatusPass || sc.TraceStatus != StatusPass || sc.Category != "ordering" || sc.Feature != "PORD-02" || sc.ProxyRestarted || sc.Error != "" {
		t.Errorf("PORD-02 unexpected result: %+v", *sc)
	}
	if !md.HasTableRowContaining("PORD-02", "ordering", StatusPass) {
		t.Errorf("Markdown missing PORD-02 ordering PASS row")
	}
}

func TestT1_F2_03_StreamingPullAckNackPasses(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^(PULL-01|LEASE-01)$",
	)
	for _, wantLine := range []string{"[default] PULL-01: PASS", "[default] LEASE-01: PASS"} {
		if !strings.Contains(res.Stderr, wantLine) {
			t.Errorf("stderr missing %q:\n%s", wantLine, res.Stderr)
		}
	}
	wantSum := ParsedSummary{Total: 2, Pass: 2}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}

	pull := t1MustScenario(t, rep, "PULL-01")
	if pull.Status != StatusPass || pull.TraceStatus != StatusPass || pull.Category != "pull" {
		t.Errorf("PULL-01 unexpected result: %+v", *pull)
	}
	lease := t1MustScenario(t, rep, "LEASE-01")
	if lease.Status != StatusPass || lease.TraceStatus != StatusPass || lease.Category != "lease" {
		t.Errorf("LEASE-01 unexpected result: %+v", *lease)
	}
	if !md.HasTableRowContaining("PULL-01", "pull", StatusPass) {
		t.Errorf("Markdown missing PULL-01 row")
	}
	if !md.HasTableRowContaining("LEASE-01", "lease", StatusPass) {
		t.Errorf("Markdown missing LEASE-01 row")
	}
}

func TestT1_F2_04_LeaseExtensionAndEODPasses(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^(LEASE-02|EOD-01|EOD-02)$",
	)
	for _, id := range []string{"LEASE-02", "EOD-01", "EOD-02"} {
		if !strings.Contains(res.Stderr, fmt.Sprintf("[default] %s: PASS", id)) {
			t.Errorf("stderr missing PASS line for %s:\n%s", id, res.Stderr)
		}
	}
	wantSum := ParsedSummary{Total: 3, Pass: 3}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}

	wantCats := map[string]string{"LEASE-02": "lease", "EOD-01": "eod", "EOD-02": "eod"}
	for id, cat := range wantCats {
		sc := t1MustScenario(t, rep, id)
		if sc.Status != StatusPass || sc.TraceStatus != StatusPass || sc.Category != cat || sc.ProxyRestarted || sc.Error != "" {
			t.Errorf("%s unexpected result: %+v", id, *sc)
		}
		if sc.DurationMs >= 5000 {
			t.Errorf("%s took %dms with --fast-timings, expected < 5000ms", id, sc.DurationMs)
		}
		if !md.HasTableRowContaining(id, cat, StatusPass) {
			t.Errorf("Markdown missing %s (%s) PASS row", id, cat)
		}
	}
}

func TestT1_F2_05_UnaryPullPasses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mode string
		lang string
	}{
		{name: "go", mode: "conformant-go", lang: "go"},
		{name: "cpp", mode: "conformant-cpp-partial", lang: "cpp"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, rep, md := t1RunCLIWithReports(
				t, 30*time.Second,
				[]string{"--mode=" + tc.mode, "--fast-timings"},
				"--filter=^PULL-04$",
			)
			if !strings.Contains(res.Stderr, "[default] PULL-04: PASS") {
				t.Errorf("stderr missing [default] PULL-04: PASS:\n%s", res.Stderr)
			}
			if rep.Capabilities == nil {
				t.Fatalf("Capabilities nil")
			}
			if rep.Capabilities.ClientLanguage != tc.lang || !rep.Capabilities.SupportsUnaryPull {
				t.Errorf("caps = {lang=%q unaryPull=%v}, want {%q, true}",
					rep.Capabilities.ClientLanguage, rep.Capabilities.SupportsUnaryPull, tc.lang)
			}
			wantSum := ParsedSummary{Total: 1, Pass: 1}
			if rep.Summary == nil || *rep.Summary != wantSum {
				t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
			}
			sc := t1MustScenario(t, rep, "PULL-04")
			if sc.Status != StatusPass || sc.TraceStatus != StatusPass || sc.Category != "pull" {
				t.Errorf("PULL-04 unexpected result: %+v", *sc)
			}
			if !md.HasTableRowContaining("PULL-04", "pull", StatusPass) {
				t.Errorf("Markdown missing PULL-04 row")
			}
		})
	}
}

// --- E2E-F3: Optional Client-Side OTel Trace Verification (5 tests) ---

func TestT1_F3_01_MonitoringEnabledPublishSpansPass(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^TRAC-01$",
	)
	if !strings.Contains(res.Stderr, "[default] TRAC-01: PASS") {
		t.Errorf("stderr missing [default] TRAC-01: PASS:\n%s", res.Stderr)
	}
	if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || !*rep.Capabilities.SupportsMonitoring {
		t.Errorf("SupportsMonitoring = %v, want true", rep.Capabilities)
	}
	wantSum := ParsedSummary{Total: 1, Pass: 1}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	sc := t1MustScenario(t, rep, "TRAC-01")
	if sc.Status != StatusPass || sc.TraceStatus != StatusPass || sc.Category != "traces" || sc.ProxyRestarted || sc.Error != "" {
		t.Errorf("TRAC-01 unexpected result: %+v", *sc)
	}
	if !md.HasTableRowContaining("TRAC-01", "traces", StatusPass) {
		t.Errorf("Markdown missing TRAC-01 traces PASS row")
	}
}

func TestT1_F3_02_MonitoringEnabledSubscribeSpansPass(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^TRAC-04$",
	)
	if !strings.Contains(res.Stderr, "[default] TRAC-04: PASS") {
		t.Errorf("stderr missing [default] TRAC-04: PASS:\n%s", res.Stderr)
	}
	if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || !*rep.Capabilities.SupportsMonitoring {
		t.Errorf("SupportsMonitoring = %v, want true", rep.Capabilities)
	}
	wantSum := ParsedSummary{Total: 1, Pass: 1}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	sc := t1MustScenario(t, rep, "TRAC-04")
	if sc.Status != StatusPass || sc.TraceStatus != StatusPass || sc.Category != "traces" || sc.Error != "" {
		t.Errorf("TRAC-04 unexpected result: %+v", *sc)
	}
	if !md.HasTableRowContaining("TRAC-04", "traces", StatusPass) {
		t.Errorf("Markdown missing TRAC-04 traces PASS row")
	}
}

func TestT1_F3_03_MonitoringEnabledContextPropagationPass(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^TRAC-03$",
	)
	if !strings.Contains(res.Stderr, "[default] TRAC-03: PASS") {
		t.Errorf("stderr missing [default] TRAC-03: PASS:\n%s", res.Stderr)
	}
	if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || !*rep.Capabilities.SupportsMonitoring {
		t.Errorf("SupportsMonitoring = %v, want true", rep.Capabilities)
	}
	wantSum := ParsedSummary{Total: 1, Pass: 1}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	sc := t1MustScenario(t, rep, "TRAC-03")
	if sc.Status != StatusPass || sc.TraceStatus != StatusPass || sc.Category != "traces" || sc.Error != "" {
		t.Errorf("TRAC-03 unexpected result: %+v", *sc)
	}
	if !md.HasTableRowContaining("TRAC-03", "traces", StatusPass) {
		t.Errorf("Markdown missing TRAC-03 traces PASS row")
	}
}

func TestT1_F3_04_MonitoringDisabledSkipsTraceAssertions(t *testing.T) {
	t.Parallel()

	res, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-rust-nomon", "--fast-timings"},
		"--filter=^(TRAC-01|TRAC-03)$",
	)
	if !strings.Contains(res.Stderr, "[default] TRAC-01: SKIPPED_NO_MONITORING") {
		t.Errorf("stderr missing TRAC-01 SKIPPED_NO_MONITORING:\n%s", res.Stderr)
	}
	if !strings.Contains(res.Stderr, "[default] TRAC-03: PASS") {
		t.Errorf("stderr missing TRAC-03 PASS:\n%s", res.Stderr)
	}
	if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || *rep.Capabilities.SupportsMonitoring {
		t.Errorf("SupportsMonitoring = %v, want false", rep.Capabilities)
	}
	wantSum := ParsedSummary{Total: 2, Pass: 1, SkippedNoMonitoring: 1}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}

	t01 := t1MustScenario(t, rep, "TRAC-01")
	if t01.Status != StatusSkippedNoMonitoring || t01.TraceStatus != StatusSkippedNoMonitoring ||
		t01.Reason != "client reports supports_monitoring=false" || t01.DurationMs != 0 {
		t.Errorf("TRAC-01 unexpected result: %+v", *t01)
	}
	t03 := t1MustScenario(t, rep, "TRAC-03")
	if t03.Status != StatusPass || t03.TraceStatus != StatusSkippedNoMonitoring || t03.Duration == "" {
		t.Errorf("TRAC-03 unexpected result: %+v", *t03)
	}

	if !md.HasTableRowContaining("TRAC-01", "traces", StatusSkippedNoMonitoring, "client reports supports_monitoring=false") {
		t.Errorf("Markdown missing TRAC-01 SKIPPED_NO_MONITORING row")
	}
	if !md.HasTableRowContaining("TRAC-03", "traces", StatusPass) {
		t.Errorf("Markdown missing TRAC-03 PASS row")
	}
}

func TestT1_F3_05_ResetAndDrainTracesIsolation(t *testing.T) {
	t.Parallel()

	_, rep, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^(TRAC-01|TRAC-01b|TRAC-03)$",
	)
	wantSum := ParsedSummary{Total: 3, Pass: 3}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	if len(rep.Scenarios) != 3 {
		t.Fatalf("len(Scenarios) = %d, want 3", len(rep.Scenarios))
	}
	// Registry order places TRAC-01 & TRAC-03 sequentially on the shared proxy
	// (verifying ResetTraces isolation), followed by isolated TRAC-01b.
	wantOrder := []string{"TRAC-01", "TRAC-03", "TRAC-01b"}
	for i, wantID := range wantOrder {
		sc := rep.Scenarios[i]
		if sc.ID != wantID {
			t.Errorf("Scenarios[%d].ID = %q, want %q", i, sc.ID, wantID)
		}
		if sc.Status != StatusPass || sc.TraceStatus != StatusPass || sc.ProxyRestarted || sc.Error != "" {
			t.Errorf("%s unexpected result: %+v", sc.ID, sc)
		}
		if !md.HasTableRowContaining(wantID, "traces", StatusPass) {
			t.Errorf("Markdown missing %s PASS row", wantID)
		}
	}
}

// --- E2E-F4: Scenario Filtering & Bounded Timeouts (5 tests) ---

func TestT1_F4_01_FilterByExactScenarioID(t *testing.T) {
	t.Parallel()

	wrapper, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")

	t.Run("exact_single_id", func(t *testing.T) {
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")
		mdPath := filepath.Join(dir, "report.md")
		res := runCLI(
			t, 30*time.Second,
			"--proxy-cmd="+wrapper,
			"--filter=^PUB-01$",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d err=%v\nstderr:\n%s", res.ExitCode, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		AssertSummaryMatchesScenarios(t, rep)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 1, Pass: 1}) {
			t.Errorf("Summary = %+v, want Total=1 Pass=1", rep.Summary)
		}
		if len(rep.Scenarios) != 1 || rep.Scenarios[0].ID != "PUB-01" || rep.Scenarios[0].Status != StatusPass {
			t.Errorf("unexpected scenarios: %+v", rep.Scenarios)
		}
		md := ReadMarkdownReport(t, mdPath)
		if !md.HasTableRowContaining("PUB-01", StatusPass) {
			t.Errorf("Markdown missing PUB-01 PASS row")
		}
	})

	t.Run("anchored_multi_id", func(t *testing.T) {
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")
		res := runCLI(
			t, 30*time.Second,
			"--proxy-cmd="+wrapper,
			"--filter=^(PUB-01|PULL-01)$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d err=%v\nstderr:\n%s", res.ExitCode, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		AssertSummaryMatchesScenarios(t, rep)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 2, Pass: 2}) {
			t.Errorf("Summary = %+v, want Total=2 Pass=2", rep.Summary)
		}
		if sc := t1MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
			t.Errorf("PUB-01 status = %q, want PASS", sc.Status)
		}
		if sc := t1MustScenario(t, rep, "PULL-01"); sc.Status != StatusPass {
			t.Errorf("PULL-01 status = %q, want PASS", sc.Status)
		}
	})

	t.Run("list_anchor_vs_unanchored_and_empty", func(t *testing.T) {
		exact := runCLI(t, 15*time.Second, "--list", "--filter=^TRAC-01$")
		if exact.ExitCode != 0 || exact.TimedOut || exact.Err != nil {
			t.Fatalf("--list ^TRAC-01$ failed: exit=%d err=%v", exact.ExitCode, exact.Err)
		}
		exactItems := t1ParseCLIList(t, exact.Stdout)
		if len(exactItems) != 1 || exactItems[0].ID != "TRAC-01" {
			t.Errorf("--list ^TRAC-01$ = %+v, want [TRAC-01]", exactItems)
		}

		prefix := runCLI(t, 15*time.Second, "--list", "--filter=TRAC-01")
		if prefix.ExitCode != 0 || prefix.TimedOut || prefix.Err != nil {
			t.Fatalf("--list TRAC-01 failed: exit=%d err=%v", prefix.ExitCode, prefix.Err)
		}
		prefixItems := t1ParseCLIList(t, prefix.Stdout)
		if len(prefixItems) < 2 {
			t.Errorf("--list TRAC-01 returned %d items, want >= 2 (TRAC-01 & TRAC-01b)", len(prefixItems))
		}

		empty := runCLI(t, 15*time.Second, "--list", "--filter=^NONEXISTENT-SCENARIO-999$")
		if empty.ExitCode != 0 || empty.TimedOut || empty.Err != nil {
			t.Fatalf("--list zero match failed: exit=%d err=%v", empty.ExitCode, empty.Err)
		}
		if strings.TrimSpace(empty.Stdout) != "" {
			t.Errorf("--list zero match stdout = %q, want empty", empty.Stdout)
		}
		if !strings.Contains(empty.Stderr, "No scenarios matched filter") {
			t.Errorf("--list zero match stderr = %q, want 'No scenarios matched filter'", empty.Stderr)
		}
	})
}

func TestT1_F4_02_FilterByCategoryRegex(t *testing.T) {
	t.Parallel()

	wantCats := map[string]int{
		"publish":      12,
		"ordering":     6,
		"pull":         8,
		"traces":       5,
		"flow_control": 7,
		"compression":  1,
		"hedging":      4,
		"retry":        3,
		"lease":        5,
		"dlq":          1,
		"eod":          6,
		"lifecycle":    3,
		"config":       4,
	}

	t.Run("list_13_categories_and_no_subscribe", func(t *testing.T) {
		res := runCLI(t, 15*time.Second, "--list")
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("--list failed: exit=%d err=%v", res.ExitCode, res.Err)
		}
		items := t1ParseCLIList(t, res.Stdout)
		if len(items) != 65 {
			t.Fatalf("--list total = %d, want 65", len(items))
		}
		gotCats := make(map[string]int)
		for _, it := range items {
			gotCats[it.Category]++
		}
		if len(gotCats) != len(wantCats) {
			t.Errorf("category count = %d (%v), want %d (%v)", len(gotCats), gotCats, len(wantCats), wantCats)
		}
		for cat, wantCnt := range wantCats {
			if gotCats[cat] != wantCnt {
				t.Errorf("category %q count = %d, want %d", cat, gotCats[cat], wantCnt)
			}
		}

		subRes := runCLI(t, 15*time.Second, "--list", "--filter=^subscribe$")
		if subRes.ExitCode != 0 || subRes.TimedOut || subRes.Err != nil {
			t.Fatalf("--list ^subscribe$ failed: exit=%d err=%v", subRes.ExitCode, subRes.Err)
		}
		if len(t1ParseCLIList(t, subRes.Stdout)) != 0 || !strings.Contains(subRes.Stderr, "No scenarios matched filter") {
			t.Errorf("expected 0 matches for ^subscribe$; stdout=%q stderr=%q", subRes.Stdout, subRes.Stderr)
		}

		unanchored := runCLI(t, 15*time.Second, "--list", "--filter=publish")
		anchored := runCLI(t, 15*time.Second, "--list", "--filter=^publish$")
		if unanchored.ExitCode != 0 || anchored.ExitCode != 0 {
			t.Fatalf("--list publish filters failed: unanchored=%d anchored=%d", unanchored.ExitCode, anchored.ExitCode)
		}
		if len(t1ParseCLIList(t, anchored.Stdout)) != 12 {
			t.Errorf("--list ^publish$ count = %d, want 12", len(t1ParseCLIList(t, anchored.Stdout)))
		}
		if len(t1ParseCLIList(t, unanchored.Stdout)) <= 12 {
			t.Errorf("--list unanchored publish count = %d, want > 12", len(t1ParseCLIList(t, unanchored.Stdout)))
		}
	})

	t.Run("execute_core_categories", func(t *testing.T) {
		wrapper, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		for _, cat := range []string{"publish", "pull", "lease", "eod"} {
			wantN := wantCats[cat]
			dir := t.TempDir()
			jsonPath := filepath.Join(dir, cat+".json")
			mdPath := filepath.Join(dir, cat+".md")
			res := runCLI(
				t, 45*time.Second,
				"--proxy-cmd="+wrapper,
				"--filter=^"+cat+"$",
				"--report-json="+jsonPath,
				"--report-md="+mdPath,
			)
			if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
				t.Fatalf("category %q run failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
					cat, res.ExitCode, res.TimedOut, res.Err, res.Stderr)
			}
			rep := ReadJSONReport(t, jsonPath)
			AssertSummaryMatchesScenarios(t, rep)
			wantSum := ParsedSummary{Total: wantN, Pass: wantN}
			if rep.Summary == nil || *rep.Summary != wantSum {
				t.Errorf("category %q summary = %+v, want %+v", cat, rep.Summary, wantSum)
			}
			for _, sc := range rep.Scenarios {
				if sc.Category != cat || sc.Status != StatusPass {
					t.Errorf("category %q scenario %s: cat=%q status=%q", cat, sc.ID, sc.Category, sc.Status)
				}
			}
			md := ReadMarkdownReport(t, mdPath)
			if !md.HasTableRowContaining(strconv.Itoa(wantN), "0") {
				t.Errorf("category %q Markdown summary row mismatch", cat)
			}
		}
	})
}

func TestT1_F4_03_ScenarioTimeoutAllowsFastRun(t *testing.T) {
	t.Parallel()

	_, rep, md := t1RunCLIWithReports(
		t, 25*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings", "--delay=20ms"},
		"--filter=^(PUB-01|PULL-01)$",
		"--scenario-timeout=10s",
	)
	if rep.TimedOut {
		t.Errorf("report TimedOut = true, want false")
	}
	wantSum := ParsedSummary{Total: 2, Pass: 2}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	for _, id := range []string{"PUB-01", "PULL-01"} {
		if sc := t1MustScenario(t, rep, id); sc.Status != StatusPass {
			t.Errorf("%s status = %q, want PASS", id, sc.Status)
		}
	}
	if md.TimeoutCount != 0 {
		t.Errorf("Markdown TimeoutCount = %d, want 0", md.TimeoutCount)
	}
}

func TestT1_F4_04_SuiteTimeoutAllowsCompleteRun(t *testing.T) {
	t.Parallel()

	_, rep, md := t1RunCLIWithReports(
		t, 45*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^publish$",
		"--suite-timeout=25s",
	)
	if rep.TimedOut || rep.InfraError != "" {
		t.Errorf("unexpected timeout/infraError: timedOut=%v infra=%q", rep.TimedOut, rep.InfraError)
	}
	wantSum := ParsedSummary{Total: 12, Pass: 12}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	if !md.HasTableRowContaining("PUB-01", StatusPass) || md.TimeoutCount != 0 {
		t.Errorf("Markdown check failed: timeoutCount=%d", md.TimeoutCount)
	}
}

func TestT1_F4_05_TimeoutFlagsAcceptGoDurationFormats(t *testing.T) {
	t.Parallel()

	wrapper, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	cases := []struct {
		name            string
		scenarioTimeout string
		suiteTimeout    string
	}{
		{name: "500ms_and_5s", scenarioTimeout: "500ms", suiteTimeout: "5s"},
		{name: "5s_and_1m", scenarioTimeout: "5s", suiteTimeout: "1m"},
		{name: "1m_and_1m", scenarioTimeout: "1m", suiteTimeout: "1m"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			jsonPath := filepath.Join(dir, "report.json")
			res := runCLI(
				t, 25*time.Second,
				"--proxy-cmd="+wrapper,
				"--filter=^PUB-01$",
				"--scenario-timeout="+tc.scenarioTimeout,
				"--suite-timeout="+tc.suiteTimeout,
				"--report-json="+jsonPath,
			)
			if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
				t.Fatalf("CLI failed (%s): exit=%d timedOut=%v err=%v\nstderr:\n%s",
					tc.name, res.ExitCode, res.TimedOut, res.Err, res.Stderr)
			}
			rep := ReadJSONReport(t, jsonPath)
			AssertSummaryMatchesScenarios(t, rep)
			wantSum := ParsedSummary{Total: 1, Pass: 1}
			if rep.Summary == nil || *rep.Summary != wantSum {
				t.Errorf("%s summary = %+v, want %+v", tc.name, rep.Summary, wantSum)
			}
			if sc := t1MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
				t.Errorf("%s PUB-01 status = %q, want PASS", tc.name, sc.Status)
			}
		})
	}
}

// --- E2E-F5: Reporting, Status Semantics & Strictness (5 tests) ---

func TestT1_F5_01_ReportJSONValidStructure(t *testing.T) {
	t.Parallel()

	_, rep, _ := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^(PUB-01|PORD-02|TRAC-01)$",
	)
	t1AssertJSONV1Envelope(t, rep, "single")
	if rep.Target != "default" {
		t.Errorf("Target = %q, want default", rep.Target)
	}
	rawDiffs, ok := rep.Raw["differences"].([]any)
	if !ok || len(rawDiffs) != 0 {
		t.Errorf("raw differences = %#v, want empty non-nil array", rep.Raw["differences"])
	}
	if rep.Capabilities == nil || rep.Capabilities.ClientLanguage != "go" ||
		rep.Capabilities.SupportsMonitoring == nil || !*rep.Capabilities.SupportsMonitoring ||
		rep.Capabilities.Defaults == nil {
		t.Errorf("unexpected capabilities block: %+v", rep.Capabilities)
	}
	if len(rep.Targets) != 1 || rep.Targets["default"] == nil {
		t.Errorf("expected single target 'default' in rep.Targets, got %v", rep.Targets)
	}
	if rawTargets, ok := rep.Raw["targets"].([]any); !ok || len(rawTargets) != 1 {
		t.Errorf("expected raw targets array len 1, got %T (%v)", rep.Raw["targets"], rep.Raw["targets"])
	}

	wantSum := ParsedSummary{Total: 3, Pass: 3}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	if len(rep.Scenarios) != 3 {
		t.Fatalf("len(Scenarios) = %d, want 3", len(rep.Scenarios))
	}
	for _, sc := range rep.Scenarios {
		if sc.ID == "" || sc.Name == "" || sc.Category == "" || sc.Feature == "" || sc.Duration == "" {
			t.Errorf("scenario missing required metadata fields: %+v", sc)
		}
		if sc.Status != StatusPass || sc.TraceStatus != StatusPass {
			t.Errorf("%s status=%q trace=%q, want PASS/PASS", sc.ID, sc.Status, sc.TraceStatus)
		}
	}
}

func TestT1_F5_02_ReportMarkdownReadable(t *testing.T) {
	t.Parallel()

	_, _, md := t1RunCLIWithReports(
		t, 30*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
		"--filter=^(PUB-01|PORD-02|TRAC-01)$",
	)
	for _, h := range []string{"Pub/Sub Conformance Report: default", "Target & Capabilities", "Summary", "Scenario Results"} {
		if !md.HasHeadingContaining(h) {
			t.Errorf("Markdown missing heading %q; got %v", h, md.Headings)
		}
	}
	if len(md.Tables) != 3 {
		t.Fatalf("Markdown tables count = %d, want 3", len(md.Tables))
	}
	if md.HasHeadingContaining("Diagnostics") || md.HasFailureBreakdown {
		t.Errorf("passing run should not include Diagnostics or failure breakdown")
	}

	wantHeaders := [][]string{
		{"Target", "Language", "Library", "Version", "Supports Monitoring"},
		{"Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client"},
		{"Scenario", "Category", "Status", "Duration", "Details"},
	}
	for i, wantHdr := range wantHeaders {
		if !slices.Equal(md.Tables[i].Headers, wantHdr) {
			t.Errorf("Markdown table[%d] headers = %v, want %v", i, md.Tables[i].Headers, wantHdr)
		}
	}

	checks := [][]string{
		{"default", "go", "cloud.google.com/go/pubsub/v2", "true"},
		{"3", "3", "0", "0", "0"},
		{"PUB-01", "publish", StatusPass},
		{"PORD-02", "ordering", StatusPass},
		{"TRAC-01", "traces", StatusPass},
	}
	for _, toks := range checks {
		if !md.HasTableRowContaining(toks...) {
			t.Errorf("Markdown missing table row matching %v", toks)
		}
	}
}

func TestT1_F5_03_UnsupportedReportedNonStrictExitZero(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		proxyArgs []string
	}{
		{
			name:      "rust_nomon",
			proxyArgs: []string{"--mode=conformant-rust-nomon", "--fast-timings"},
		},
		{
			name:      "go_explicit_unsupported",
			proxyArgs: []string{"--mode=conformant-go", "--unsupported=unary-pull,blocking-publish", "--fast-timings"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, rep, md := t1RunCLIWithReports(
				t, 30*time.Second,
				tc.proxyArgs,
				"--filter=^(PUB-01|PULL-04|PUB-09)$",
			)
			for _, wantLine := range []string{
				"[default] PUB-01: PASS",
				"[default] PULL-04: UNSUPPORTED_BY_CLIENT",
				"[default] PUB-09: UNSUPPORTED_BY_CLIENT",
			} {
				if !strings.Contains(res.Stderr, wantLine) {
					t.Errorf("stderr missing %q:\n%s", wantLine, res.Stderr)
				}
			}

			wantSum := ParsedSummary{Total: 3, Pass: 1, UnsupportedByClient: 2}
			if rep.Summary == nil || *rep.Summary != wantSum {
				t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
			}
			if sc := t1MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
				t.Errorf("PUB-01 status = %q, want PASS", sc.Status)
			}
			pull04 := t1MustScenario(t, rep, "PULL-04")
			if pull04.Status != StatusUnsupportedByClient || pull04.DurationMs != 0 ||
				pull04.Reason != "missing required capabilities: unary_pull" {
				t.Errorf("PULL-04 unexpected result: %+v", *pull04)
			}
			pub09 := t1MustScenario(t, rep, "PUB-09")
			if pub09.Status != StatusUnsupportedByClient || pub09.DurationMs != 0 ||
				pub09.Reason != "missing required capabilities: blocking_publish" {
				t.Errorf("PUB-09 unexpected result: %+v", *pub09)
			}

			if !md.HasTableRowContaining("3", "1", "0", "0", "2") {
				t.Errorf("Markdown missing summary row 3/1/0/0/2")
			}
			if !md.HasTableRowContaining("PULL-04", "pull", StatusUnsupportedByClient, "missing required capabilities: unary_pull") {
				t.Errorf("Markdown missing PULL-04 UNSUPPORTED_BY_CLIENT row")
			}
			if !md.HasTableRowContaining("PUB-09", "publish", StatusUnsupportedByClient, "missing required capabilities: blocking_publish") {
				t.Errorf("Markdown missing PUB-09 UNSUPPORTED_BY_CLIENT row")
			}
		})
	}
}

func TestT1_F5_04_SkippedNoMonitoringStatus(t *testing.T) {
	t.Parallel()

	t.Run("rust_nomon", func(t *testing.T) {
		_, rep, md := t1RunCLIWithReports(
			t, 30*time.Second,
			[]string{"--mode=conformant-rust-nomon", "--fast-timings"},
			"--filter=^TRAC-",
		)
		wantSum := ParsedSummary{Total: 5, Pass: 1, SkippedNoMonitoring: 4}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, id := range []string{"TRAC-01", "TRAC-01b", "TRAC-02", "TRAC-04"} {
			sc := t1MustScenario(t, rep, id)
			if sc.Status != StatusSkippedNoMonitoring || sc.TraceStatus != StatusSkippedNoMonitoring ||
				sc.Reason != "client reports supports_monitoring=false" {
				t.Errorf("%s unexpected result: %+v", id, *sc)
			}
			if !md.HasTableRowContaining(id, "traces", StatusSkippedNoMonitoring) {
				t.Errorf("Markdown missing %s SKIPPED_NO_MONITORING row", id)
			}
		}
		t03 := t1MustScenario(t, rep, "TRAC-03")
		if t03.Status != StatusPass || t03.TraceStatus != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-03 unexpected result: %+v", *t03)
		}
		if !md.HasTableRowContaining("TRAC-03", "traces", StatusPass) {
			t.Errorf("Markdown missing TRAC-03 PASS row")
		}
	})

	t.Run("cpp_nomon_capability_precedence", func(t *testing.T) {
		_, rep, _ := t1RunCLIWithReports(
			t, 30*time.Second,
			[]string{"--mode=conformant-cpp-partial", "--monitoring=false", "--fast-timings"},
			"--filter=^TRAC-",
		)
		wantSum := ParsedSummary{Total: 5, Pass: 1, SkippedNoMonitoring: 3, UnsupportedByClient: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, id := range []string{"TRAC-01", "TRAC-01b", "TRAC-04"} {
			sc := t1MustScenario(t, rep, id)
			if sc.Status != StatusSkippedNoMonitoring || sc.TraceStatus != StatusSkippedNoMonitoring {
				t.Errorf("%s unexpected result: %+v", id, *sc)
			}
		}
		t03 := t1MustScenario(t, rep, "TRAC-03")
		if t03.Status != StatusPass || t03.TraceStatus != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-03 unexpected result: %+v", *t03)
		}
		t02 := t1MustScenario(t, rep, "TRAC-02")
		if t02.Status != StatusUnsupportedByClient || t02.TraceStatus != "" ||
			t02.Reason != "missing required capabilities: publish_hedging" {
			t.Errorf("TRAC-02 capability precedence failed: %+v", *t02)
		}
	})
}

func TestT1_F5_05_StdoutSummaryAndExitCodeZero(t *testing.T) {
	t.Parallel()

	wrapper, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	res := runCLI(
		t, 30*time.Second,
		"--proxy-cmd="+wrapper,
		"--filter=^(PUB-01|PORD-02|PULL-01)$",
	)
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stdout, res.Stderr)
	}
	for _, wantLine := range []string{
		"[default] PUB-01: PASS",
		"[default] PORD-02: PASS",
		"[default] PULL-01: PASS",
	} {
		if !strings.Contains(res.Stderr, wantLine) {
			t.Errorf("stderr missing %q:\n%s", wantLine, res.Stderr)
		}
	}

	md, err := ParseMarkdownReport(res.Stdout)
	if err != nil {
		t.Fatalf("ParseMarkdownReport(stdout) failed: %v\nstdout:\n%s", err, res.Stdout)
	}
	for _, h := range []string{"Pub/Sub Conformance Report: default", "Target & Capabilities", "Summary", "Scenario Results"} {
		if !md.HasHeadingContaining(h) {
			t.Errorf("stdout Markdown missing heading %q", h)
		}
	}
	if md.HasHeadingContaining("Diagnostics") {
		t.Errorf("passing stdout Markdown should not contain Diagnostics heading")
	}
	if !md.HasTableRowContaining("3", "3", "0", "0", "0") {
		t.Errorf("stdout Markdown missing 3/3/0/0/0 summary row")
	}
	if !md.HasTableRowContaining("PUB-01", "publish", StatusPass) ||
		!md.HasTableRowContaining("PORD-02", "ordering", StatusPass) ||
		!md.HasTableRowContaining("PULL-01", "pull", StatusPass) {
		t.Errorf("stdout Markdown missing expected scenario PASS rows")
	}
}

// --- E2E-F6: Multi-Client Execution & Diff Comparison (5 tests) ---

func TestT1_F6_01_MultiProxyTargetRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "compare.json")
	mdPath := filepath.Join(dir, "compare.md")
	mGo := newRunMarker(t)
	mCpp := newRunMarker(t)

	targetGo := fmt.Sprintf("go=%s --marker=%s --mode=conformant-go --fast-timings", shellQuote(proxyBinaryPath), mGo)
	targetCpp := fmt.Sprintf("cpp=%s --marker=%s --mode=conformant-cpp-partial --fast-timings", shellQuote(proxyBinaryPath), mCpp)

	res := runCLI(
		t, 45*time.Second,
		"--proxy-target="+targetGo,
		"--proxy-target="+targetCpp,
		"--filter=^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-02)$",
		"--report-json="+jsonPath,
		"--report-md="+mdPath,
	)
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "[go] PUB-01: PASS") || !strings.Contains(res.Stderr, "[cpp] TRAC-02: UNSUPPORTED_BY_CLIENT") {
		t.Errorf("stderr missing expected target progress lines:\n%s", res.Stderr)
	}

	rep := ReadJSONReport(t, jsonPath)
	AssertSummaryMatchesScenarios(t, rep)
	t1AssertJSONV1Envelope(t, rep, "compare")
	if len(rep.Targets) != 2 {
		t.Fatalf("len(Targets) = %d, want 2", len(rep.Targets))
	}
	wantGo := ParsedSummary{Total: 5, Pass: 5}
	if rep.Targets["go"].Summary == nil || *rep.Targets["go"].Summary != wantGo {
		t.Errorf("go summary = %+v, want %+v", rep.Targets["go"].Summary, wantGo)
	}
	wantCpp := ParsedSummary{Total: 5, Pass: 4, UnsupportedByClient: 1}
	if rep.Targets["cpp"].Summary == nil || *rep.Targets["cpp"].Summary != wantCpp {
		t.Errorf("cpp summary = %+v, want %+v", rep.Targets["cpp"].Summary, wantCpp)
	}

	dTrac := t1FindDiff(t, rep, "scenario_verdict", "TRAC-02")
	if dTrac.Verdicts["go"] != StatusPass || dTrac.Verdicts["cpp"] != StatusUnsupportedByClient {
		t.Errorf("TRAC-02 diff verdicts = %v, want go=PASS cpp=UNSUPPORTED_BY_CLIENT", dTrac.Verdicts)
	}
	if !t1HasDiff(rep, "capability", "supports_publish_hedging") {
		t.Errorf("expected capability diff on supports_publish_hedging")
	}
	if !t1HasDiff(rep, "client_default", "defaults.publish_batch_byte_threshold") {
		t.Errorf("expected client_default diff on defaults.publish_batch_byte_threshold")
	}

	md := ReadMarkdownReport(t, mdPath)
	for _, h := range []string{
		"Pub/Sub Cross-Client Conformance Comparison Report",
		"Summary",
		"Capabilities & Defaults",
		"Scenario Matrix",
		"Behavioral Differences",
	} {
		if !md.HasHeadingContaining(h) {
			t.Errorf("Markdown missing compare heading %q", h)
		}
	}
	if !md.HasTableRowContaining("TRAC-02", "traces", StatusPass, StatusUnsupportedByClient) {
		t.Errorf("Markdown missing TRAC-02 PASS vs UNSUPPORTED_BY_CLIENT matrix row")
	}
}

func TestT1_F6_02_ThreeWayProxyTargetRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "compare3.json")
	mdPath := filepath.Join(dir, "compare3.md")
	mGo := newRunMarker(t)
	mCpp := newRunMarker(t)
	mRust := newRunMarker(t)

	targetGo := fmt.Sprintf("go=%s --marker=%s --mode=conformant-go --fast-timings", shellQuote(proxyBinaryPath), mGo)
	targetCpp := fmt.Sprintf("cpp=%s --marker=%s --mode=conformant-cpp-partial --fast-timings", shellQuote(proxyBinaryPath), mCpp)
	targetRust := fmt.Sprintf("rust=%s --marker=%s --mode=conformant-rust-nomon --fast-timings", shellQuote(proxyBinaryPath), mRust)

	res := runCLI(
		t, 60*time.Second,
		"--proxy-target="+targetGo,
		"--proxy-target="+targetCpp,
		"--proxy-target="+targetRust,
		"--filter=^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-01|TRAC-02|TRAC-03|LEASE-02|EOD-01)$",
		"--report-json="+jsonPath,
		"--report-md="+mdPath,
	)
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}

	rep := ReadJSONReport(t, jsonPath)
	AssertSummaryMatchesScenarios(t, rep)
	t1AssertJSONV1Envelope(t, rep, "compare")
	if len(rep.Targets) != 3 {
		t.Fatalf("len(Targets) = %d, want 3", len(rep.Targets))
	}

	wantSums := map[string]ParsedSummary{
		"go":   {Total: 9, Pass: 9},
		"cpp":  {Total: 9, Pass: 8, UnsupportedByClient: 1},
		"rust": {Total: 9, Pass: 6, SkippedNoMonitoring: 2, UnsupportedByClient: 1},
	}
	for name, want := range wantSums {
		tr := rep.Targets[name]
		if tr == nil || tr.Summary == nil || *tr.Summary != want {
			t.Errorf("target %q summary = %+v, want %+v", name, tr, want)
		}
	}

	for _, capField := range []string{"supports_monitoring", "supports_publish_hedging", "supports_unary_pull"} {
		if !t1HasDiff(rep, "capability", capField) {
			t.Errorf("missing capability diff for %q", capField)
		}
	}
	if !t1HasDiff(rep, "client_default", "defaults.publish_total_timeout_ms") {
		t.Errorf("missing client_default diff for defaults.publish_total_timeout_ms")
	}

	dTrac02 := t1FindDiff(t, rep, "scenario_verdict", "TRAC-02")
	if dTrac02.Verdicts["go"] != StatusPass ||
		dTrac02.Verdicts["cpp"] != StatusUnsupportedByClient ||
		dTrac02.Verdicts["rust"] != StatusSkippedNoMonitoring {
		t.Errorf("TRAC-02 3-way verdicts = %v", dTrac02.Verdicts)
	}
	dPull04 := t1FindDiff(t, rep, "scenario_verdict", "PULL-04")
	if dPull04.Verdicts["go"] != StatusPass || dPull04.Verdicts["cpp"] != StatusPass || dPull04.Verdicts["rust"] != StatusUnsupportedByClient {
		t.Errorf("PULL-04 3-way verdicts = %v", dPull04.Verdicts)
	}
	dPub01 := t1FindDiff(t, rep, "scenario_trace_delta", "PUB-01")
	if dPub01.Verdicts["go"] != StatusPass || dPub01.Verdicts["cpp"] != StatusPass || dPub01.Verdicts["rust"] != StatusSkippedNoMonitoring {
		t.Errorf("PUB-01 trace_delta = %v", dPub01.Verdicts)
	}

	md := ReadMarkdownReport(t, mdPath)
	if !md.HasTableRowContaining("go", "go", "true", "9", "9", "0") ||
		!md.HasTableRowContaining("cpp", "cpp", "true", "9", "8", "1") ||
		!md.HasTableRowContaining("rust", "rust", "false", "9", "6", "2", "1") {
		t.Errorf("Markdown missing 3-way target summary rows")
	}
	if !md.HasTableRowContaining("TRAC-02", "traces", StatusPass, StatusUnsupportedByClient, StatusSkippedNoMonitoring) {
		t.Errorf("Markdown missing TRAC-02 3-way matrix row")
	}
}

func TestT1_F6_03_CompareFromJSONReports(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	goJSON := filepath.Join(dir, "go_cmp.json")
	rustJSON := filepath.Join(dir, "rust_cmp.json")
	diffJSON := filepath.Join(dir, "diff.json")
	diffMD := filepath.Join(dir, "diff.md")

	wGo, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

	rGo := runCLI(t, 30*time.Second, "--proxy-cmd="+wGo, "--filter=^(PUB-01|PORD-01|PULL-03)$", "--report-json="+goJSON)
	if rGo.ExitCode != 0 || rGo.TimedOut || rGo.Err != nil {
		t.Fatalf("go run failed: exit=%d err=%v\n%s", rGo.ExitCode, rGo.Err, rGo.Stderr)
	}
	rRust := runCLI(t, 30*time.Second, "--proxy-cmd="+wRust, "--filter=^(PUB-01|PORD-01|PULL-03)$", "--report-json="+rustJSON)
	if rRust.ExitCode != 0 || rRust.TimedOut || rRust.Err != nil {
		t.Fatalf("rust run failed: exit=%d err=%v\n%s", rRust.ExitCode, rRust.Err, rRust.Stderr)
	}

	cmpRes := runCLI(
		t, 15*time.Second,
		"--compare="+goJSON+","+rustJSON,
		"--report-json="+diffJSON,
		"--report-md="+diffMD,
	)
	if cmpRes.ExitCode != 0 || cmpRes.TimedOut || cmpRes.Err != nil {
		t.Fatalf("compare failed: exit=%d err=%v\nstderr:\n%s", cmpRes.ExitCode, cmpRes.Err, cmpRes.Stderr)
	}
	if cmpRes.Stderr != "" {
		t.Errorf("expected empty stderr in offline --compare, got %q", cmpRes.Stderr)
	}
	if !strings.Contains(cmpRes.Stdout, "# Pub/Sub Cross-Client Conformance Comparison Report") {
		t.Errorf("compare stdout missing report title")
	}

	rep := ReadJSONReport(t, diffJSON)
	AssertSummaryMatchesScenarios(t, rep)
	t1AssertJSONV1Envelope(t, rep, "compare")
	if len(rep.Targets) != 2 || rep.Targets["go_cmp"] == nil || rep.Targets["rust_cmp"] == nil {
		t.Fatalf("expected targets go_cmp and rust_cmp derived from filenames, got %v", rep.Targets)
	}
	if *rep.Targets["go_cmp"].Summary != (ParsedSummary{Total: 3, Pass: 3}) {
		t.Errorf("go_cmp summary = %+v", *rep.Targets["go_cmp"].Summary)
	}
	if *rep.Targets["rust_cmp"].Summary != (ParsedSummary{Total: 3, Pass: 2, UnsupportedByClient: 1}) {
		t.Errorf("rust_cmp summary = %+v", *rep.Targets["rust_cmp"].Summary)
	}

	dPord := t1FindDiff(t, rep, "scenario_verdict", "PORD-01")
	if dPord.Verdicts["go_cmp"] != StatusPass || dPord.Verdicts["rust_cmp"] != StatusUnsupportedByClient {
		t.Errorf("PORD-01 diff verdicts = %v", dPord.Verdicts)
	}
	for _, scID := range []string{"PUB-01", "PULL-03"} {
		dTrace := t1FindDiff(t, rep, "scenario_trace_delta", scID)
		if dTrace.Verdicts["go_cmp"] != StatusPass || dTrace.Verdicts["rust_cmp"] != StatusSkippedNoMonitoring {
			t.Errorf("%s trace delta = %v", scID, dTrace.Verdicts)
		}
	}

	md := ReadMarkdownReport(t, diffMD)
	if !md.HasTableRowContaining("PORD-01", "ordering", StatusPass, StatusUnsupportedByClient) {
		t.Errorf("compare MD missing PORD-01 row")
	}
}

func TestT1_F6_04_CompareHighlightsDivergentVerdicts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	goJSON := filepath.Join(dir, "go_cmp.json")
	divJSON := filepath.Join(dir, "div_cmp.json")
	diffJSON := filepath.Join(dir, "diff.json")
	diffMD := filepath.Join(dir, "diff.md")

	wGo, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	wDiv, _ := writeProxyWrapper(t, "--mode=divergent-client", "--fast-timings")

	rGo := runCLI(t, 30*time.Second, "--proxy-cmd="+wGo, "--filter=^(PUB-01|PORD-01|PULL-03)$", "--report-json="+goJSON)
	if rGo.ExitCode != 0 || rGo.TimedOut || rGo.Err != nil {
		t.Fatalf("go run failed: exit=%d err=%v\n%s", rGo.ExitCode, rGo.Err, rGo.Stderr)
	}
	// divergent-client intentionally fails PORD-01 live (exit code 1).
	rDiv := runCLI(t, 30*time.Second, "--proxy-cmd="+wDiv, "--filter=^(PUB-01|PORD-01|PULL-03)$", "--report-json="+divJSON)
	if rDiv.ExitCode != 1 || rDiv.TimedOut {
		t.Fatalf("divergent-client expected exit=1 (live fail on PORD-01), got exit=%d timedOut=%v\n%s",
			rDiv.ExitCode, rDiv.TimedOut, rDiv.Stderr)
	}

	cmpRes := runCLI(
		t, 15*time.Second,
		"--compare="+goJSON+","+divJSON,
		"--report-json="+diffJSON,
		"--report-md="+diffMD,
	)
	if cmpRes.ExitCode != 0 || cmpRes.TimedOut || cmpRes.Err != nil {
		t.Fatalf("offline compare failed: exit=%d err=%v\nstderr:\n%s", cmpRes.ExitCode, cmpRes.Err, cmpRes.Stderr)
	}
	if cmpRes.Stderr != "" {
		t.Errorf("expected empty stderr on offline compare, got %q", cmpRes.Stderr)
	}

	rep := ReadJSONReport(t, diffJSON)
	AssertSummaryMatchesScenarios(t, rep)
	t1AssertJSONV1Envelope(t, rep, "compare")

	if *rep.Targets["go_cmp"].Summary != (ParsedSummary{Total: 3, Pass: 3}) {
		t.Errorf("go_cmp summary = %+v", *rep.Targets["go_cmp"].Summary)
	}
	if *rep.Targets["div_cmp"].Summary != (ParsedSummary{Total: 3, Pass: 1, Fail: 1, UnsupportedByClient: 1}) {
		t.Errorf("div_cmp summary = %+v", *rep.Targets["div_cmp"].Summary)
	}

	dPord := t1FindDiff(t, rep, "scenario_verdict", "PORD-01")
	if dPord.Verdicts["go_cmp"] != StatusPass || dPord.Verdicts["div_cmp"] != StatusFail {
		t.Errorf("PORD-01 verdicts = %v, want go_cmp=PASS div_cmp=FAIL", dPord.Verdicts)
	}
	if !strings.Contains(dPord.Details["div_cmp"], "ordering_key") {
		t.Errorf("PORD-01 div_cmp details = %q, expected mention of ordering_key failure", dPord.Details["div_cmp"])
	}

	dPull := t1FindDiff(t, rep, "scenario_verdict", "PULL-03")
	if dPull.Verdicts["go_cmp"] != StatusPass || dPull.Verdicts["div_cmp"] != StatusUnsupportedByClient {
		t.Errorf("PULL-03 verdicts = %v, want go_cmp=PASS div_cmp=UNSUPPORTED_BY_CLIENT", dPull.Verdicts)
	}
	if !strings.Contains(dPull.Details["div_cmp"], "keepalive") {
		t.Errorf("PULL-03 div_cmp details = %q, expected keepalive capability reason", dPull.Details["div_cmp"])
	}

	md := ReadMarkdownReport(t, diffMD)
	if !md.HasTableRowContaining("PORD-01", "ordering", StatusPass, StatusFail) {
		t.Errorf("Markdown missing PORD-01 PASS vs FAIL row")
	}
	if !md.HasTableRowContaining("PULL-03", "pull", StatusPass, StatusUnsupportedByClient) {
		t.Errorf("Markdown missing PULL-03 PASS vs UNSUPPORTED_BY_CLIENT row")
	}
	if !md.HasHeadingContaining("Diagnostics") {
		t.Errorf("Markdown missing Diagnostics section for FAIL target")
	}
	foundDiag := false
	for _, b := range md.DiagnosticsBullets {
		if strings.Contains(b, "div_cmp/PORD-01") {
			foundDiag = true
			break
		}
	}
	if !foundDiag {
		t.Errorf("Markdown Diagnostics bullets missing div_cmp/PORD-01; got %v", md.DiagnosticsBullets)
	}
}

func TestT1_F6_05_CompareHighlightsMonitoringCapabilityDelta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	goJSON := filepath.Join(dir, "go_mon.json")
	rustJSON := filepath.Join(dir, "rust_nomon.json")
	diffJSON := filepath.Join(dir, "diff.json")
	diffMD := filepath.Join(dir, "diff.md")

	wGo, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

	rGo := runCLI(t, 30*time.Second, "--proxy-cmd="+wGo, "--filter=^(PUB-01|TRAC-01|TRAC-03)$", "--report-json="+goJSON)
	if rGo.ExitCode != 0 || rGo.TimedOut || rGo.Err != nil {
		t.Fatalf("go run failed: exit=%d err=%v\n%s", rGo.ExitCode, rGo.Err, rGo.Stderr)
	}
	rRust := runCLI(t, 30*time.Second, "--proxy-cmd="+wRust, "--filter=^(PUB-01|TRAC-01|TRAC-03)$", "--report-json="+rustJSON)
	if rRust.ExitCode != 0 || rRust.TimedOut || rRust.Err != nil {
		t.Fatalf("rust run failed: exit=%d err=%v\n%s", rRust.ExitCode, rRust.Err, rRust.Stderr)
	}

	cmpRes := runCLI(
		t, 15*time.Second,
		"--compare="+goJSON+","+rustJSON,
		"--report-json="+diffJSON,
		"--report-md="+diffMD,
	)
	if cmpRes.ExitCode != 0 || cmpRes.TimedOut || cmpRes.Err != nil {
		t.Fatalf("compare failed: exit=%d err=%v\nstderr:\n%s", cmpRes.ExitCode, cmpRes.Err, cmpRes.Stderr)
	}
	if cmpRes.Stderr != "" {
		t.Errorf("expected empty stderr on offline compare, got %q", cmpRes.Stderr)
	}

	rep := ReadJSONReport(t, diffJSON)
	AssertSummaryMatchesScenarios(t, rep)
	t1AssertJSONV1Envelope(t, rep, "compare")

	if *rep.Targets["go_mon"].Summary != (ParsedSummary{Total: 3, Pass: 3}) {
		t.Errorf("go_mon summary = %+v", *rep.Targets["go_mon"].Summary)
	}
	if *rep.Targets["rust_nomon"].Summary != (ParsedSummary{Total: 3, Pass: 2, SkippedNoMonitoring: 1}) {
		t.Errorf("rust_nomon summary = %+v", *rep.Targets["rust_nomon"].Summary)
	}

	dMon := t1FindDiff(t, rep, "capability", "supports_monitoring")
	if dMon.Verdicts["go_mon"] != "true" || dMon.Verdicts["rust_nomon"] != "false" {
		t.Errorf("supports_monitoring verdicts = %v, want true/false", dMon.Verdicts)
	}

	dPub := t1FindDiff(t, rep, "scenario_trace_delta", "PUB-01")
	if dPub.Verdicts["go_mon"] != StatusPass || dPub.Verdicts["rust_nomon"] != StatusSkippedNoMonitoring {
		t.Errorf("PUB-01 trace_delta = %v", dPub.Verdicts)
	}

	dTrac01 := t1FindDiff(t, rep, "scenario_verdict", "TRAC-01")
	if dTrac01.Verdicts["go_mon"] != StatusPass || dTrac01.Verdicts["rust_nomon"] != StatusSkippedNoMonitoring {
		t.Errorf("TRAC-01 verdicts = %v", dTrac01.Verdicts)
	}
	if dTrac01.Details["rust_nomon"] != "client reports supports_monitoring=false" {
		t.Errorf("TRAC-01 rust_nomon details = %q", dTrac01.Details["rust_nomon"])
	}

	dTrac03 := t1FindDiff(t, rep, "scenario_trace_delta", "TRAC-03")
	if dTrac03.Verdicts["go_mon"] != StatusPass || dTrac03.Verdicts["rust_nomon"] != StatusSkippedNoMonitoring {
		t.Errorf("TRAC-03 trace_delta = %v", dTrac03.Verdicts)
	}

	md := ReadMarkdownReport(t, diffMD)
	if !md.HasTableRowContaining("supports_monitoring", "true", "false") {
		t.Errorf("Markdown missing supports_monitoring true vs false row")
	}
	if !md.HasTableRowContaining("TRAC-01", "traces", StatusPass, StatusSkippedNoMonitoring) {
		t.Errorf("Markdown missing TRAC-01 PASS vs SKIPPED_NO_MONITORING matrix row")
	}
	if !md.HasTableRowContaining("TRAC-03", "traces", StatusPass, StatusPass) {
		t.Errorf("Markdown missing TRAC-03 PASS vs PASS matrix row")
	}
	if !md.HasTableRowContaining("scenario_trace_delta", "TRAC-03", StatusPass, StatusSkippedNoMonitoring) {
		t.Errorf("Markdown missing Behavioral Differences row for TRAC-03 trace delta")
	}
}

// --- E2E-F7: Conformance Coverage & Proxy Execution (6 tests) ---

func TestT1_F7_01_GoProfileFullSuiteHappyPath(t *testing.T) {
	t.Parallel()

	_, rep, md := t1RunCLIWithReports(
		t, 90*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
	)
	t1AssertJSONV1Envelope(t, rep, "single")

	c := rep.Capabilities
	if c == nil {
		t.Fatalf("Capabilities is nil")
	}
	if c.ClientLanguage != "go" || c.ClientLibrary != "cloud.google.com/go/pubsub/v2" {
		t.Errorf("identity = (%q, %q), want (go, cloud.google.com/go/pubsub/v2)", c.ClientLanguage, c.ClientLibrary)
	}
	if c.SupportsMonitoring == nil || !*c.SupportsMonitoring {
		t.Errorf("SupportsMonitoring = %v, want true", c.SupportsMonitoring)
	}
	if c.SupportsAutoNackOnDrop {
		t.Errorf("SupportsAutoNackOnDrop = true, want false for Go")
	}
	if !c.SupportsUnaryPull || !c.SupportsBlockingPublish || !c.SupportsPublishFlowControl ||
		!c.SupportsPublishCompression || !c.SupportsPublishHedging || !c.SupportsOrderingOptIn ||
		!c.SupportsSubscriberOrdering || !c.SupportsKeepalive || !c.SupportsEodReceiptModack ||
		!c.SupportsAckWithResult || !c.SupportsCustomRetryCodes || !c.SupportsExactlyOnceDelivery ||
		!c.SupportsPerStreamFlowControl || !c.SupportsResumePublish || !c.SupportsShutdownBehavior ||
		!c.SupportsShutdownTimeout {
		t.Errorf("expected remaining 16 Go capabilities true, got %+v", *c)
	}
	if c.Defaults == nil {
		t.Fatalf("Capabilities.Defaults nil")
	}
	if c.Defaults.SubscriberMinAckExtensionMs != 250 ||
		c.Defaults.SubscriberMaxExtensionMs != 350 ||
		c.Defaults.RetryInitialDelayMs != 20 ||
		c.Defaults.KeepalivePingIntervalMs != 120 ||
		c.Defaults.KeepalivePongTimeoutMs != 70 {
		t.Errorf("Go --fast-timings defaults unexpected: %+v", *c.Defaults)
	}

	wantSum := ParsedSummary{Total: 65, Pass: 65}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	if len(rep.Scenarios) != 65 {
		t.Fatalf("len(Scenarios) = %d, want 65", len(rep.Scenarios))
	}
	for _, sc := range rep.Scenarios {
		if sc.Status != StatusPass || sc.TraceStatus != StatusPass {
			t.Errorf("%s status=%q trace=%q, want PASS/PASS (err=%q reason=%q)",
				sc.ID, sc.Status, sc.TraceStatus, sc.Error, sc.Reason)
		}
	}

	if !md.HasHeadingContaining("Pub/Sub Conformance Report") {
		t.Errorf("Markdown missing title heading")
	}
	if !md.HasTableRowContaining("default", "go", "cloud.google.com/go/pubsub/v2", "true") {
		t.Errorf("Markdown missing Go target row")
	}
	if !md.HasTableRowContaining("65", "65", "0", "0", "0") {
		t.Errorf("Markdown missing 65/65/0/0/0 summary row")
	}
}

func TestT1_F7_02_RustProfileNoMonSuiteRun(t *testing.T) {
	t.Parallel()

	_, rep, md := t1RunCLIWithReports(
		t, 90*time.Second,
		[]string{"--mode=conformant-rust-nomon", "--fast-timings"},
	)
	t1AssertJSONV1Envelope(t, rep, "single")

	c := rep.Capabilities
	if c == nil {
		t.Fatalf("Capabilities nil")
	}
	if c.ClientLanguage != "rust" || c.ClientLibrary != "google-cloud-pubsub" {
		t.Errorf("identity = (%q, %q)", c.ClientLanguage, c.ClientLibrary)
	}
	if c.SupportsMonitoring == nil || *c.SupportsMonitoring {
		t.Errorf("SupportsMonitoring = %v, want false", c.SupportsMonitoring)
	}

	wantSum := ParsedSummary{Total: 65, Pass: 48, SkippedNoMonitoring: 4, UnsupportedByClient: 13}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}

	var gotSkipped, gotUnsup []string
	for _, sc := range rep.Scenarios {
		switch sc.Status {
		case StatusSkippedNoMonitoring:
			gotSkipped = append(gotSkipped, sc.ID)
			if sc.TraceStatus != StatusSkippedNoMonitoring || !strings.Contains(sc.Reason, "supports_monitoring=false") {
				t.Errorf("%s unexpected skipped fields: %+v", sc.ID, sc)
			}
		case StatusUnsupportedByClient:
			gotUnsup = append(gotUnsup, sc.ID)
			if !strings.Contains(sc.Reason, "missing required capabilities:") {
				t.Errorf("%s unexpected unsupported reason: %q", sc.ID, sc.Reason)
			}
		case StatusPass:
		default:
			t.Errorf("%s unexpected status %q (err=%q)", sc.ID, sc.Status, sc.Error)
		}
	}

	wantSkipped := []string{"TRAC-01", "TRAC-01b", "TRAC-02", "TRAC-04"}
	wantUnsup := []string{
		"PULL-04", "PUB-09", "PORD-01",
		"PFC-01", "PFC-02", "PFC-03", "PFC-03b",
		"CMP-01", "PRET-01b",
		"SUBF-01b", "SUBF-02", "SUBF-02b", "EOD-04",
	}
	t1AssertExactIDSet(t, "rust SKIPPED_NO_MONITORING", gotSkipped, wantSkipped)
	t1AssertExactIDSet(t, "rust UNSUPPORTED_BY_CLIENT", gotUnsup, wantUnsup)

	t03 := t1MustScenario(t, rep, "TRAC-03")
	if t03.Status != StatusPass || t03.TraceStatus != StatusSkippedNoMonitoring {
		t.Errorf("TRAC-03 = %+v, want status=PASS trace_status=SKIPPED_NO_MONITORING", *t03)
	}

	if !md.HasTableRowContaining("65", "48", "0", "4", "13") {
		t.Errorf("Markdown missing 65/48/0/4/13 summary row")
	}
	if !md.HasTableRowContaining("TRAC-03", "traces", StatusPass) {
		t.Errorf("Markdown missing TRAC-03 PASS row")
	}
}

func TestT1_F7_03_CppProfilePartialSuiteRun(t *testing.T) {
	t.Parallel()

	_, rep, md := t1RunCLIWithReports(
		t, 90*time.Second,
		[]string{"--mode=conformant-cpp-partial", "--fast-timings"},
	)
	t1AssertJSONV1Envelope(t, rep, "single")

	c := rep.Capabilities
	if c == nil {
		t.Fatalf("Capabilities nil")
	}
	if c.ClientLanguage != "cpp" || c.ClientLibrary != "google-cloud-cpp/pubsub" {
		t.Errorf("identity = (%q, %q)", c.ClientLanguage, c.ClientLibrary)
	}
	if c.SupportsMonitoring == nil || !*c.SupportsMonitoring {
		t.Errorf("SupportsMonitoring = %v, want true", c.SupportsMonitoring)
	}
	if c.SupportsPublishHedging || c.SupportsKeepalive || c.SupportsEodReceiptModack || c.SupportsPerStreamFlowControl {
		t.Errorf("expected C++ unsupported capabilities false, got %+v", *c)
	}
	if !c.SupportsAutoNackOnDrop || !c.SupportsUnaryPull || !c.SupportsBlockingPublish {
		t.Errorf("expected C++ supported capabilities true, got %+v", *c)
	}

	wantSum := ParsedSummary{Total: 65, Pass: 56, UnsupportedByClient: 9}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}

	var gotUnsup []string
	for _, sc := range rep.Scenarios {
		switch sc.Status {
		case StatusUnsupportedByClient:
			gotUnsup = append(gotUnsup, sc.ID)
		case StatusPass:
		default:
			t.Errorf("%s unexpected status %q (err=%q)", sc.ID, sc.Status, sc.Error)
		}
	}
	wantUnsup := []string{"HDG-01", "HDG-02", "HDG-03", "HDG-04", "PULL-03", "PULL-03b", "SUBF-01b", "EOD-04", "TRAC-02"}
	t1AssertExactIDSet(t, "cpp UNSUPPORTED_BY_CLIENT", gotUnsup, wantUnsup)

	t02 := t1MustScenario(t, rep, "TRAC-02")
	if t02.Status != StatusUnsupportedByClient || t02.TraceStatus != "" || !strings.Contains(t02.Reason, "publish_hedging") {
		t.Errorf("TRAC-02 = %+v, want UNSUPPORTED_BY_CLIENT with publish_hedging reason", *t02)
	}
	for _, id := range []string{"TRAC-01", "TRAC-01b", "TRAC-03", "TRAC-04"} {
		sc := t1MustScenario(t, rep, id)
		if sc.Status != StatusPass || sc.TraceStatus != StatusPass {
			t.Errorf("%s = %+v, want PASS/PASS", id, *sc)
		}
	}

	if !md.HasTableRowContaining("65", "56", "0", "0", "9") {
		t.Errorf("Markdown missing 65/56/0/0/9 summary row")
	}
	if !md.HasTableRowContaining("TRAC-02", "traces", StatusUnsupportedByClient) {
		t.Errorf("Markdown missing TRAC-02 UNSUPPORTED_BY_CLIENT row")
	}
}

func TestT1_F7_04_SuiteCoversCoreDataPlaneCategories(t *testing.T) {
	t.Parallel()

	_, rep, _ := t1RunCLIWithReports(
		t, 90*time.Second,
		[]string{"--mode=conformant-go", "--fast-timings"},
	)

	wantCats := map[string]int{
		"publish":      12,
		"ordering":     6,
		"pull":         8,
		"traces":       5,
		"flow_control": 7,
		"compression":  1,
		"hedging":      4,
		"retry":        3,
		"lease":        5,
		"dlq":          1,
		"eod":          6,
		"lifecycle":    3,
		"config":       4,
	}

	if len(rep.Scenarios) != 65 {
		t.Fatalf("len(Scenarios) = %d, want 65", len(rep.Scenarios))
	}
	catCounts := make(map[string]int, len(wantCats))
	seenIDs := make(map[string]bool, 65)
	for _, sc := range rep.Scenarios {
		if seenIDs[sc.ID] {
			t.Errorf("duplicate scenario ID %q", sc.ID)
		}
		seenIDs[sc.ID] = true
		catCounts[sc.Category]++
	}
	if len(seenIDs) != 65 {
		t.Errorf("unique scenario count = %d, want 65", len(seenIDs))
	}
	if len(catCounts) != len(wantCats) {
		t.Errorf("category count = %d (%v), want %d (%v)", len(catCounts), catCounts, len(wantCats), wantCats)
	}
	for cat, wantCnt := range wantCats {
		if catCounts[cat] != wantCnt {
			t.Errorf("category %q count = %d, want %d", cat, catCounts[cat], wantCnt)
		}
	}

	// Verify SUBF-02 / SUBF-02b remain categorized under "ordering".
	for _, id := range []string{"SUBF-02", "SUBF-02b"} {
		if sc := t1MustScenario(t, rep, id); sc.Category != "ordering" {
			t.Errorf("%s category = %q, want ordering", id, sc.Category)
		}
	}

	// Verify --list parity in exact registry order.
	listRes := runCLI(t, 15*time.Second, "--list")
	if listRes.ExitCode != 0 || listRes.TimedOut || listRes.Err != nil {
		t.Fatalf("--list failed: exit=%d err=%v", listRes.ExitCode, listRes.Err)
	}
	listed := t1ParseCLIList(t, listRes.Stdout)
	if len(listed) != len(rep.Scenarios) {
		t.Fatalf("--list len=%d != report len=%d", len(listed), len(rep.Scenarios))
	}
	for i := range listed {
		if listed[i].ID != rep.Scenarios[i].ID || listed[i].Category != rep.Scenarios[i].Category {
			t.Errorf("index %d mismatch: --list=(%q,%q) report=(%q,%q)",
				i, listed[i].ID, listed[i].Category, rep.Scenarios[i].ID, rep.Scenarios[i].Category)
		}
	}
}

func TestT1_F7_05_RepeatedDeterministicRuns(t *testing.T) {
	t.Parallel()

	wrapper, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")
	dir := t.TempDir()
	run1Path := filepath.Join(dir, "run1.json")
	run2Path := filepath.Join(dir, "run2.json")

	r1 := runCLI(t, 90*time.Second, "--proxy-cmd="+wrapper, "--report-json="+run1Path)
	if r1.ExitCode != 0 || r1.TimedOut || r1.Err != nil {
		t.Fatalf("run1 failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			r1.ExitCode, r1.TimedOut, r1.Err, r1.Stderr)
	}
	r2 := runCLI(t, 90*time.Second, "--proxy-cmd="+wrapper, "--report-json="+run2Path)
	if r2.ExitCode != 0 || r2.TimedOut || r2.Err != nil {
		t.Fatalf("run2 failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			r2.ExitCode, r2.TimedOut, r2.Err, r2.Stderr)
	}

	rep1 := ReadJSONReport(t, run1Path)
	rep2 := ReadJSONReport(t, run2Path)
	AssertSummaryMatchesScenarios(t, rep1)
	AssertSummaryMatchesScenarios(t, rep2)

	wantSum := ParsedSummary{Total: 65, Pass: 48, SkippedNoMonitoring: 4, UnsupportedByClient: 13}
	if rep1.Summary == nil || *rep1.Summary != wantSum {
		t.Fatalf("run1 summary = %+v, want %+v", rep1.Summary, wantSum)
	}
	if rep2.Summary == nil || *rep2.Summary != wantSum {
		t.Fatalf("run2 summary = %+v, want %+v", rep2.Summary, wantSum)
	}
	if len(rep1.Scenarios) != 65 || len(rep2.Scenarios) != 65 {
		t.Fatalf("scenario counts (%d, %d), want (65, 65)", len(rep1.Scenarios), len(rep2.Scenarios))
	}

	for i := range rep1.Scenarios {
		a, b := rep1.Scenarios[i], rep2.Scenarios[i]
		if a.ID != b.ID || a.Name != b.Name || a.Category != b.Category || a.Feature != b.Feature ||
			a.Status != b.Status || a.StatusDetail != b.StatusDetail || a.TraceStatus != b.TraceStatus || a.Reason != b.Reason {
			t.Errorf("scenario[%d] drift: run1=%+v vs run2=%+v", i, a, b)
		}
	}
}

func TestT1_F7_06_DefaultTimingsWithoutFastFlag(t *testing.T) {
	t.Parallel()

	// Create wrappers WITHOUT --fast-timings so unmodified defaults are reported.
	wGo, _ := writeProxyWrapper(t, "--mode=conformant-go")
	wCpp, _ := writeProxyWrapper(t, "--mode=conformant-cpp-partial")
	wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon")

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "defaults.json")

	res := runCLI(
		t, 30*time.Second,
		"--proxy-target=go="+wGo,
		"--proxy-target=cpp="+wCpp,
		"--proxy-target=rust="+wRust,
		"--filter=^(PUB-01|PULL-01)$",
		"--report-json="+jsonPath,
	)
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}

	rep := ReadJSONReport(t, jsonPath)
	AssertSummaryMatchesScenarios(t, rep)
	t1AssertJSONV1Envelope(t, rep, "compare")

	if len(rep.Targets) != 3 {
		t.Fatalf("len(Targets) = %d, want 3", len(rep.Targets))
	}
	for _, name := range []string{"go", "cpp", "rust"} {
		tr := rep.Targets[name]
		if tr == nil || tr.Summary == nil || *tr.Summary != (ParsedSummary{Total: 2, Pass: 2}) {
			t.Fatalf("target %q summary unexpected: %+v", name, tr)
		}
		for _, id := range []string{"PUB-01", "PULL-01"} {
			if sc := t1FindTargetScenario(t, tr, id); sc.Status != StatusPass {
				t.Errorf("%s/%s status = %q, want PASS", name, id, sc.Status)
			}
		}
		if tr.Capabilities.Defaults == nil {
			t.Fatalf("target %q Defaults nil", name)
		}
	}

	dGo := rep.Targets["go"].Capabilities.Defaults
	if dGo.SubscriberMinAckExtensionMs != 10_000 ||
		dGo.SubscriberMaxExtensionMs != 3_600_000 ||
		dGo.SubscriberMaxAckExtensionMs != 600_000 ||
		!dGo.SubscriberAdaptiveAckExtension ||
		dGo.KeepalivePingIntervalMs != 30_000 ||
		dGo.KeepalivePongTimeoutMs != 15_000 ||
		dGo.RetryInitialDelayMs != 100 ||
		dGo.RetryMaxDelayMs != 60_000 ||
		dGo.RetryDelayMultiplier != 4.0 ||
		dGo.PublishTotalTimeoutMs != 60_000 {
		t.Errorf("Go unmodified defaults mismatch: %+v", *dGo)
	}

	dCpp := rep.Targets["cpp"].Capabilities.Defaults
	if dCpp.SubscriberMinAckExtensionMs != 60_000 ||
		dCpp.SubscriberMaxExtensionMs != 0 ||
		dCpp.KeepalivePingIntervalMs != 0 ||
		dCpp.KeepalivePongTimeoutMs != 0 ||
		dCpp.RetryInitialDelayMs != 100 ||
		dCpp.RetryDelayMultiplier != 2.0 ||
		dCpp.PublishBatchByteThreshold != 1_048_576 {
		t.Errorf("C++ unmodified defaults mismatch: %+v", *dCpp)
	}

	dRust := rep.Targets["rust"].Capabilities.Defaults
	if dRust.SubscriberMinAckExtensionMs != 60_000 ||
		dRust.SubscriberMaxExtensionMs != 3_600_000 ||
		dRust.KeepalivePingIntervalMs != 30_000 ||
		dRust.KeepalivePongTimeoutMs != 15_000 ||
		dRust.RetryInitialDelayMs != 100 ||
		dRust.PublishTotalTimeoutMs != 600_000 ||
		dRust.PublishFlowControlMaxMessages != -1 {
		t.Errorf("Rust unmodified defaults mismatch: %+v", *dRust)
	}
}
