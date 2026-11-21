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

/*
Tier 4 — Real-World End-to-End Application Workflows (`TestT4_01` .. `TestT4_07`)

Workflow Coverage:
  1. TestT4_01_FullMatrixGoCppRustMultiTargetCompare
     Full-65 baseline runs (Go/C++/Rust profiles) -> offline `--compare` ->
     live 3-target `--proxy-target` matrix -> strict JSON & MD parity check.
  2. TestT4_02_CICDGatePermissiveToStrictUnsupportedCompare
     Permissive Rust CI run (exit 0) -> strict `--strict-unsupported` gate (exit 1) ->
     golden Go baseline + 2-way `--compare` behavioral diff triage.
  3. TestT4_03_RegressionDetectionWorkflow
     Golden Go vs `wrong-rpc` vs `invalid-traces` full-65 triage + targeted
     `scenario_diagnostic` (`FAIL` vs `TIMEOUT`) and `scenario_trace_delta` checks.
  4. TestT4_04_BoundedTimeoutWorkflowUnderHungClient
     Multi-target bounded `--suite-timeout` with hung child PGID (`sleep 60` +
     ignored `SIGTERM`) + unstarted target sentinel + `--compare` ingestion +
     single-target `--scenario-timeout` restart & isolated `CFG-01` recovery.
  5. TestT4_05_MonitoringRolloutMigrationWorkflow
     Full-65 `--monitoring=false` -> `--monitoring=true` -> `--compare` diff
     validating `supports_monitoring`, 4 verdict flips, and 61 trace deltas.
  6. TestT4_06_CategoryFilteredProgressiveRollout
     `--list` 65/13 catalog audit -> 3 disjoint category-filtered `--filter` stages
     (`26 + 24 + 15 = 65`) -> final full-65 green sweep.
  7. TestT4_07_MidSuiteCrashRecoveryAndCleanTeardown
     Full-65 `--fault=crash-after=6,ignore-shutdown` with poisoned parent
     `PUBSUB_EMULATOR_HOST`, verifying exact crash/restart checkpoints, isolated
     `CFG-01` recovery, and clean zero-orphan teardown.

Concurrency Discipline:
  - TestT4_01 stays sequential at top level (no `t.Parallel()`) to prevent 2-CPU
    oversubscription alongside heavy Tier 1/3 65-scenario runs.
  - TestT4_02..TestT4_07 use top-level `t.Parallel()` with strictly sequential
    `t.Run(...)` subtests.
*/

package e2e_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// --- Local Unexported Constants, Types & Helpers (stdlib-only, t4* prefix) ---

var t4All13Categories = []string{
	"publish",
	"ordering",
	"flow_control",
	"compression",
	"hedging",
	"retry",
	"pull",
	"lease",
	"dlq",
	"eod",
	"lifecycle",
	"traces",
	"config",
}

type t4ListedScenario struct {
	ID       string
	Category string
	Rest     string
}

func t4RawString(m map[string]any, key string) string {
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

func t4RawBool(m map[string]any, key string) (bool, bool) {
	if m == nil {
		return false, false
	}
	v, ok := m[key]
	if !ok || v == nil {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

func t4AssertFileNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected file %q not to exist, got stat err=%v", path, err)
	}
}

func t4MustScenario(t *testing.T, rep *ParsedReport, id string) *ParsedScenarioResult {
	t.Helper()
	if rep == nil {
		t.Fatalf("t4MustScenario(%q): nil report", id)
	}
	sc, ok := rep.FindScenario(id)
	if !ok {
		t.Fatalf("scenario %q not found in root report (total=%d)", id, len(rep.Scenarios))
	}
	return sc
}

func t4MustTarget(t *testing.T, rep *ParsedReport, name string) *ParsedTargetReport {
	t.Helper()
	if rep == nil {
		t.Fatalf("t4MustTarget(%q): nil report", name)
	}
	tr, ok := rep.Targets[name]
	if !ok || tr == nil {
		t.Fatalf("target %q not found in report (targets=%d)", name, len(rep.Targets))
	}
	return tr
}

func t4MustTargetScenario(t *testing.T, tr *ParsedTargetReport, id string) *ParsedScenarioResult {
	t.Helper()
	if tr == nil {
		t.Fatalf("t4MustTargetScenario(%q): nil target report", id)
	}
	for i := range tr.Scenarios {
		if tr.Scenarios[i].ID == id {
			return &tr.Scenarios[i]
		}
	}
	t.Fatalf("scenario %q not found in target %q report (total=%d)", id, tr.TargetName, len(tr.Scenarios))
	return nil
}

func t4MustDiff(t *testing.T, rep *ParsedReport, kind, idOrField string) *ParsedDiffEntry {
	t.Helper()
	if rep == nil {
		t.Fatalf("t4MustDiff(%q, %q): nil report", kind, idOrField)
	}
	for i := range rep.Differences {
		d := &rep.Differences[i]
		if d.Kind != kind {
			continue
		}
		if idOrField == "" || d.ScenarioID == idOrField || d.Field == idOrField {
			return d
		}
	}
	t.Fatalf("difference kind=%q target=%q not found (total diffs=%d)", kind, idOrField, len(rep.Differences))
	return nil
}

func t4AssertJSONV1Base(t *testing.T, rep *ParsedReport, wantMode string, wantTimedOut bool) {
	t.Helper()
	if rep == nil {
		t.Fatalf("nil JSON report")
	}
	if got := t4RawString(rep.Raw, "schema_version"); got != "1.0" {
		t.Errorf("schema_version = %q (raw=%#v), want %q", got, rep.Raw["schema_version"], "1.0")
	}
	genAt := t4RawString(rep.Raw, "generated_at")
	if genAt == "" {
		t.Errorf("generated_at missing or empty")
	} else if _, err := time.Parse(time.RFC3339Nano, genAt); err != nil {
		t.Errorf("generated_at %q invalid RFC3339 timestamp: %v", genAt, err)
	}
	if rep.Mode != wantMode || t4RawString(rep.Raw, "mode") != wantMode {
		t.Errorf("report mode = (%q, raw=%q), want %q", rep.Mode, t4RawString(rep.Raw, "mode"), wantMode)
	}
	if rep.TimedOut != wantTimedOut {
		t.Errorf("report timed_out = %v, want %v", rep.TimedOut, wantTimedOut)
	}
	if wantTimedOut {
		if b, ok := t4RawBool(rep.Raw, "timed_out"); !ok || !b {
			t.Errorf("raw timed_out = %#v, want true", rep.Raw["timed_out"])
		}
	}
	rawDiffs, hasDiffs := rep.Raw["differences"]
	if !hasDiffs || rawDiffs == nil {
		t.Errorf("differences JSON key missing or null")
	} else if _, ok := rawDiffs.([]any); !ok {
		t.Errorf("differences JSON value has type %T, want array", rawDiffs)
	}
	AssertSummaryMatchesScenarios(t, rep)
}

func t4FindMDTable(t *testing.T, md *ParsedMarkdownReport, wantHeaders ...string) *MarkdownTable {
	t.Helper()
	if md == nil {
		t.Fatalf("t4FindMDTable: nil Markdown report")
	}
	for i := range md.Tables {
		if slices.Equal(md.Tables[i].Headers, wantHeaders) {
			return &md.Tables[i]
		}
	}
	var got [][]string
	for _, tbl := range md.Tables {
		got = append(got, tbl.Headers)
	}
	t.Fatalf("Markdown table with headers %v not found; got tables=%v", wantHeaders, got)
	return nil
}

func t4FindMDTableRow(t *testing.T, tbl *MarkdownTable, match map[string]string) map[string]string {
	t.Helper()
	if tbl == nil {
		t.Fatalf("t4FindMDTableRow: nil table")
	}
	for _, row := range tbl.Rows {
		matched := true
		for k, want := range match {
			if row[k] != want {
				matched = false
				break
			}
		}
		if matched {
			return row
		}
	}
	t.Fatalf("Markdown table %v missing row matching %v; rows=%v", tbl.Headers, match, tbl.Rows)
	return nil
}

func t4AssertAll13Categories(t *testing.T, scope string, scenarios []ParsedScenarioResult) {
	t.Helper()
	allowed := make(map[string]bool, len(t4All13Categories))
	for _, cat := range t4All13Categories {
		allowed[cat] = true
	}
	seen := make(map[string]int, len(t4All13Categories))
	for _, sc := range scenarios {
		if !allowed[sc.Category] {
			t.Errorf("%s: scenario %q has unexpected category %q", scope, sc.ID, sc.Category)
			continue
		}
		seen[sc.Category]++
	}
	for _, cat := range t4All13Categories {
		if seen[cat] == 0 {
			t.Errorf("%s: missing scenario category %q (seen=%v)", scope, cat, seen)
		}
	}
}

func t4AssertExactIDSet(t *testing.T, label string, gotIDs, wantIDs []string) {
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

func t4CountDiffKinds(diffs []ParsedDiffEntry) map[string]int {
	counts := make(map[string]int)
	for _, d := range diffs {
		counts[d.Kind]++
	}
	return counts
}

func t4ParseCLIList(t *testing.T, stdout string) []t4ListedScenario {
	t.Helper()
	var out []t4ListedScenario
	for _, line := range strings.Split(strings.ReplaceAll(stdout, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) < 3 {
			t.Fatalf("--list line %q has fewer than 3 fields", line)
		}
		afterID := strings.TrimSpace(strings.TrimPrefix(trimmed, fields[0]))
		rest := strings.TrimSpace(strings.TrimPrefix(afterID, fields[1]))
		out = append(out, t4ListedScenario{
			ID:       fields[0],
			Category: fields[1],
			Rest:     rest,
		})
	}
	return out
}

func t4WaitPIDDead(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	if pid <= 0 {
		t.Fatalf("t4WaitPIDDead: invalid pid %d", pid)
	}
	deadline := time.Now().Add(timeout)
	for {
		statRaw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if os.IsNotExist(err) {
			return
		}
		if err == nil {
			if st, _, ok := parseProcStat(string(statRaw)); ok && (st == "Z" || st == "X") {
				return
			}
		}
		if killErr := syscall.Kill(pid, 0); killErr == syscall.ESRCH {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("pid %d still alive after %v", pid, timeout)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func t4RunSingleSavedReports(t *testing.T, timeout time.Duration, wantExit int, jsonPath, mdPath string, proxyArgs []string, extraCLIArgs ...string) (CLIResult, *ParsedReport, *ParsedMarkdownReport, string) {
	t.Helper()
	wrapper, marker := writeProxyWrapper(t, proxyArgs...)
	args := make([]string, 0, 4+len(extraCLIArgs))
	args = append(args, "--proxy-cmd="+wrapper, "--report-json="+jsonPath, "--report-md="+mdPath)
	args = append(args, extraCLIArgs...)

	res := runCLI(t, timeout, args...)
	if res.ExitCode != wantExit || res.TimedOut {
		t.Fatalf("CLI exit=%d timedOut=%v err=%v (wantExit=%d TimedOut=false)\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, wantExit, res.Stdout, res.Stderr)
	}
	var rep *ParsedReport
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(e2eFormatFailureDiagnostics(rep, res.Stderr))
		}
	})
	assertNoOrphanProxy(t, marker)

	rep = ReadJSONReport(t, jsonPath)
	md := ReadMarkdownReport(t, mdPath)
	return res, rep, md, marker
}

// --- Workflow 1: Full Matrix Go/C++/Rust Multi-Target & Offline Compare Parity ---

func TestT4_01_FullMatrixGoCppRustMultiTargetCompare(t *testing.T) {
	// Sequential at top level to avoid 2-CPU contention with concurrent 65-scenario suites.
	dir := t.TempDir()

	wGo, mGo := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	wCpp, mCpp := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
	wRust, mRust := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

	goJSON := filepath.Join(dir, "go.json")
	goMD := filepath.Join(dir, "go.md")
	cppJSON := filepath.Join(dir, "cpp.json")
	cppMD := filepath.Join(dir, "cpp.md")
	rustJSON := filepath.Join(dir, "rust.json")
	rustMD := filepath.Join(dir, "rust.md")
	cmpJSON := filepath.Join(dir, "cmp3.json")
	cmpMD := filepath.Join(dir, "cmp3.md")
	liveJSON := filepath.Join(dir, "live3.json")
	liveMD := filepath.Join(dir, "live3.md")

	wantCppUnsupported := []string{"HDG-01", "HDG-02", "HDG-03", "HDG-04", "PULL-03", "PULL-03b", "SUBF-01b", "EOD-04", "TRAC-02"}
	wantRustSkipped := []string{"TRAC-01", "TRAC-01b", "TRAC-02", "TRAC-04"}
	wantRustUnsupported := []string{"PULL-04", "PUB-09", "PORD-01", "PFC-01", "PFC-02", "PFC-03", "PFC-03b", "CMP-01", "PRET-01b", "SUBF-01b", "SUBF-02", "SUBF-02b", "EOD-04"}

	t.Run("BaselineArchives", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			wrapper string
			marker  string
			jPath   string
			mPath   string
		}{
			{name: "go", wrapper: wGo, marker: mGo, jPath: goJSON, mPath: goMD},
			{name: "cpp", wrapper: wCpp, marker: mCpp, jPath: cppJSON, mPath: cppMD},
			{name: "rust", wrapper: wRust, marker: mRust, jPath: rustJSON, mPath: rustMD},
		} {
			res := runCLI(
				t, 150*time.Second,
				"--proxy-cmd="+tc.wrapper,
				"--suite-timeout=120s",
				"--report-json="+tc.jPath,
				"--report-md="+tc.mPath,
			)
			if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
				t.Fatalf("%s full-65 baseline failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
					tc.name, res.ExitCode, res.TimedOut, res.Err, res.Stderr)
			}
			assertNoOrphanProxy(t, tc.marker)
		}

		// Go baseline assertions.
		repGo := ReadJSONReport(t, goJSON)
		t4AssertJSONV1Base(t, repGo, "single", false)
		wantGoSum := ParsedSummary{Total: 65, Pass: 65}
		if repGo.Summary == nil || *repGo.Summary != wantGoSum {
			t.Fatalf("go Summary = %+v, want %+v", repGo.Summary, wantGoSum)
		}
		t4AssertAll13Categories(t, "go-65", repGo.Scenarios)
		for _, sc := range repGo.Scenarios {
			if sc.Status != StatusPass || sc.TraceStatus != StatusPass || t4RawString(sc.Raw, "trace_status") != StatusPass {
				t.Errorf("go/%s expected PASS with trace_status=PASS, got status=%q trace=%q raw=%v",
					sc.ID, sc.Status, sc.TraceStatus, sc.Raw)
			}
			if len(sc.Deviations) != 0 {
				t.Errorf("go/%s unexpected deviations: %+v", sc.ID, sc.Deviations)
			}
		}

		// C++ partial baseline assertions.
		repCpp := ReadJSONReport(t, cppJSON)
		t4AssertJSONV1Base(t, repCpp, "single", false)
		wantCppSum := ParsedSummary{Total: 65, Pass: 56, UnsupportedByClient: 9}
		if repCpp.Summary == nil || *repCpp.Summary != wantCppSum {
			t.Fatalf("cpp Summary = %+v, want %+v", repCpp.Summary, wantCppSum)
		}
		t4AssertAll13Categories(t, "cpp-65", repCpp.Scenarios)
		var gotCppUnsupported []string
		cppTracePass := 0
		for _, sc := range repCpp.Scenarios {
			switch sc.Status {
			case StatusUnsupportedByClient:
				gotCppUnsupported = append(gotCppUnsupported, sc.ID)
			case StatusPass:
				if sc.TraceStatus == StatusPass {
					cppTracePass++
				}
			}
		}
		t4AssertExactIDSet(t, "cpp unsupported IDs", gotCppUnsupported, wantCppUnsupported)
		if cppTracePass != 56 {
			t.Errorf("cpp trace_status=PASS count = %d, want 56", cppTracePass)
		}
		for _, devID := range []string{"TRAC-01", "TRAC-03", "TRAC-04", "CFG-01"} {
			if sc := t4MustScenario(t, repCpp, devID); sc.Status != StatusPass || len(sc.Deviations) == 0 {
				t.Errorf("cpp/%s expected PASS with C++ deviations, got status=%q devs=%d", devID, sc.Status, len(sc.Deviations))
			}
		}

		// Rust non-monitoring baseline assertions.
		repRust := ReadJSONReport(t, rustJSON)
		t4AssertJSONV1Base(t, repRust, "single", false)
		wantRustSum := ParsedSummary{Total: 65, Pass: 48, SkippedNoMonitoring: 4, UnsupportedByClient: 13}
		if repRust.Summary == nil || *repRust.Summary != wantRustSum {
			t.Fatalf("rust Summary = %+v, want %+v", repRust.Summary, wantRustSum)
		}
		t4AssertAll13Categories(t, "rust-65", repRust.Scenarios)
		rawCaps, okCaps := repRust.Raw["capabilities"].(map[string]any)
		if !okCaps {
			t.Fatalf("rust raw capabilities missing: %#v", repRust.Raw["capabilities"])
		}
		if mon, okMon := t4RawBool(rawCaps, "supports_monitoring"); !okMon || mon {
			t.Errorf("rust raw supports_monitoring = (%v, %v), want false", mon, okMon)
		}
		var gotRustSkipped, gotRustUnsupported []string
		rustTraceSkipped := 0
		for _, sc := range repRust.Scenarios {
			switch sc.Status {
			case StatusSkippedNoMonitoring:
				gotRustSkipped = append(gotRustSkipped, sc.ID)
			case StatusUnsupportedByClient:
				gotRustUnsupported = append(gotRustUnsupported, sc.ID)
			}
			if sc.TraceStatus == StatusSkippedNoMonitoring {
				rustTraceSkipped++
			}
		}
		t4AssertExactIDSet(t, "rust skipped IDs", gotRustSkipped, wantRustSkipped)
		t4AssertExactIDSet(t, "rust unsupported IDs", gotRustUnsupported, wantRustUnsupported)
		if rustTraceSkipped != 52 {
			t.Errorf("rust trace_status=SKIPPED_NO_MONITORING count = %d, want 52", rustTraceSkipped)
		}
		if trac03 := t4MustScenario(t, repRust, "TRAC-03"); trac03.Status != StatusPass || trac03.TraceStatus != StatusSkippedNoMonitoring {
			t.Errorf("rust/TRAC-03 expected PASS with trace_status=SKIPPED_NO_MONITORING, got %+v", *trac03)
		}

		ReadMarkdownReport(t, goMD)
		ReadMarkdownReport(t, cppMD)
		ReadMarkdownReport(t, rustMD)
	})

	var cmpRep *ParsedReport
	var cmpMDRep *ParsedMarkdownReport
	t.Run("OfflineCompare", func(t *testing.T) {
		res := runCLI(
			t, 30*time.Second,
			"--compare="+strings.Join([]string{goJSON, cppJSON, rustJSON}, ","),
			"--report-json="+cmpJSON,
			"--report-md="+cmpMD,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("offline 3-way --compare failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		cmpRep = ReadJSONReport(t, cmpJSON)
		t4AssertJSONV1Base(t, cmpRep, "compare", false)
		if len(cmpRep.Targets) != 3 {
			t.Fatalf("cmp3 targets len = %d, want 3", len(cmpRep.Targets))
		}
		for _, name := range []string{"go", "cpp", "rust"} {
			tr := t4MustTarget(t, cmpRep, name)
			if tr.TargetError != "" || tr.TimedOut {
				t.Errorf("cmp3 target %q unexpected error/timeout: %+v", name, tr)
			}
			AssertTargetSummaryMatchesScenarios(t, *tr)
		}
		cmpMDRep = ReadMarkdownReport(t, cmpMD)
	})

	t.Run("LiveMultiTargetAndParity", func(t *testing.T) {
		res := runCLI(
			t, 180*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--suite-timeout=150s",
			"--report-json="+liveJSON,
			"--report-md="+liveMD,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("live 3-way --proxy-target failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, mGo)
		assertNoOrphanProxy(t, mCpp)
		assertNoOrphanProxy(t, mRust)

		liveRep := ReadJSONReport(t, liveJSON)
		t4AssertJSONV1Base(t, liveRep, "compare", false)
		if len(liveRep.Targets) != 3 {
			t.Fatalf("live3 targets len = %d, want 3", len(liveRep.Targets))
		}
		for _, name := range []string{"go", "cpp", "rust"} {
			tr := t4MustTarget(t, liveRep, name)
			if tr.TargetError != "" || tr.TimedOut {
				t.Errorf("live3 target %q unexpected error/timeout: %+v", name, tr)
			}
			AssertTargetSummaryMatchesScenarios(t, *tr)
		}

		wantCounts := map[string]int{
			"capability":           13,
			"client_default":       15,
			"scenario_trace_delta": 42,
			"scenario_verdict":     23,
		}
		for label, rep := range map[string]*ParsedReport{"cmp3": cmpRep, "live3": liveRep} {
			if len(rep.Differences) != 93 {
				t.Errorf("%s total differences = %d, want 93", label, len(rep.Differences))
			}
			gotCounts := t4CountDiffKinds(rep.Differences)
			if !reflect.DeepEqual(gotCounts, wantCounts) {
				t.Errorf("%s diff kind counts = %v, want %v", label, gotCounts, wantCounts)
			}
		}

		if !reflect.DeepEqual(cmpRep.Raw["differences"], liveRep.Raw["differences"]) {
			t.Errorf("offline cmp3.differences != live3.differences")
		}

		liveMDRep := ReadMarkdownReport(t, liveMD)
		for label, md := range map[string]*ParsedMarkdownReport{"cmp3": cmpMDRep, "live3": liveMDRep} {
			if !md.HasHeadingContaining("Pub/Sub Cross-Client Conformance Comparison Report") {
				t.Errorf("%s missing comparison heading", label)
			}
			sumTbl := t4FindMDTable(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
			t4FindMDTableRow(t, sumTbl, map[string]string{"Target": "go", "Language": "go", "Monitoring": "true", "Total": "65", "Pass": "65", "Fail": "0", "Skipped No Monitoring": "0", "Unsupported By Client": "0"})
			t4FindMDTableRow(t, sumTbl, map[string]string{"Target": "cpp", "Language": "cpp", "Monitoring": "true", "Total": "65", "Pass": "56", "Fail": "0", "Skipped No Monitoring": "0", "Unsupported By Client": "9"})
			t4FindMDTableRow(t, sumTbl, map[string]string{"Target": "rust", "Language": "rust", "Monitoring": "false", "Total": "65", "Pass": "48", "Fail": "0", "Skipped No Monitoring": "4", "Unsupported By Client": "13"})

			capTbl := t4FindMDTable(t, md, "Field", "go", "cpp", "rust")
			t4FindMDTableRow(t, capTbl, map[string]string{"Field": "supports_monitoring", "go": "true", "cpp": "true", "rust": "false"})

			matTbl := t4FindMDTable(t, md, "Scenario", "Category", "go", "cpp", "rust")
			if len(matTbl.Rows) != 65 {
				t.Errorf("%s Scenario Matrix rows = %d, want 65", label, len(matTbl.Rows))
			}
			t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-01", "go": StatusPass, "cpp": StatusPass, "rust": StatusSkippedNoMonitoring})
			t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-03", "go": StatusPass, "cpp": StatusPass, "rust": StatusPass})
			t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "HDG-01", "go": StatusPass, "cpp": StatusUnsupportedByClient, "rust": StatusPass})

			diffTbl := t4FindMDTable(t, md, "Kind", "Scenario / Field", "go", "cpp", "rust", "Description")
			if len(diffTbl.Rows) != 93 {
				t.Errorf("%s Behavioral Differences rows = %d, want 93", label, len(diffTbl.Rows))
			}
		}
		if cmpMDRep.Raw != liveMDRep.Raw {
			t.Errorf("offline cmp3.md differs from live3.md")
		}
	})
}

// --- Workflow 2: CI/CD Permissive vs Strict Unsupported Gate & Golden Compare ---

func TestT4_02_CICDGatePermissiveToStrictUnsupportedCompare(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	rustJSON := filepath.Join(dir, "rust.json")
	rustPermMD := filepath.Join(dir, "rust_perm.md")
	rustStrictJSON := filepath.Join(dir, "rust_strict.json")
	rustStrictMD := filepath.Join(dir, "rust_strict.md")
	goJSON := filepath.Join(dir, "go.json")
	goMD := filepath.Join(dir, "go.md")
	cmpJSON := filepath.Join(dir, "cmp_go_rust.json")
	cmpMD := filepath.Join(dir, "cmp_go_rust.md")

	wantRustUnsupported := []string{"PULL-04", "PUB-09", "PORD-01", "PFC-01", "PFC-02", "PFC-03", "PFC-03b", "CMP-01", "PRET-01b", "SUBF-01b", "SUBF-02", "SUBF-02b", "EOD-04"}
	wantRustSum := ParsedSummary{Total: 65, Pass: 48, Fail: 0, SkippedNoMonitoring: 4, UnsupportedByClient: 13}

	wRust, mRust := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

	t.Run("PermissiveGate", func(t *testing.T) {
		res := runCLI(
			t, 150*time.Second,
			"--proxy-cmd="+wRust,
			"--suite-timeout=120s",
			"--report-json="+rustJSON,
			"--report-md="+rustPermMD,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("permissive gate expected exit=0, got exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, mRust)

		rep := ReadJSONReport(t, rustJSON)
		t4AssertJSONV1Base(t, rep, "single", false)
		if rep.Summary == nil || *rep.Summary != wantRustSum {
			t.Fatalf("permissive rust Summary = %+v, want %+v", rep.Summary, wantRustSum)
		}
	})

	t.Run("StrictUnsupportedGate", func(t *testing.T) {
		res := runCLI(
			t, 150*time.Second,
			"--proxy-cmd="+wRust,
			"--strict-unsupported",
			"--suite-timeout=120s",
			"--report-json="+rustStrictJSON,
			"--report-md="+rustStrictMD,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("strict gate expected exit=1 timedOut=false, got exit=%d timedOut=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Stderr)
		}
		assertNoOrphanProxy(t, mRust)

		rep := ReadJSONReport(t, rustStrictJSON)
		t4AssertJSONV1Base(t, rep, "single", false)
		if rep.Summary == nil || *rep.Summary != wantRustSum {
			t.Fatalf("strict rust Summary = %+v, want %+v", rep.Summary, wantRustSum)
		}

		md := ReadMarkdownReport(t, rustStrictMD)
		sumTbl := t4FindMDTable(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t4FindMDTableRow(t, sumTbl, map[string]string{
			"Total":                 "65",
			"Pass":                  "48",
			"Fail":                  "0",
			"Skipped No Monitoring": "4",
			"Unsupported By Client": "13",
		})
		scenTbl := t4FindMDTable(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		if len(scenTbl.Rows) != 65 {
			t.Fatalf("strict MD Scenario Results rows = %d, want 65", len(scenTbl.Rows))
		}
		for _, id := range wantRustUnsupported {
			t4FindMDTableRow(t, scenTbl, map[string]string{"Scenario": id, "Status": StatusUnsupportedByClient})
		}
	})

	t.Run("GoldenGoBaselineAndDiffTriage", func(t *testing.T) {
		_, repGo, _, _ := t4RunSingleSavedReports(
			t, 150*time.Second, 0, goJSON, goMD,
			[]string{"--mode=conformant-go", "--fast-timings"},
			"--suite-timeout=120s",
		)
		t4AssertJSONV1Base(t, repGo, "single", false)
		if repGo.Summary == nil || *repGo.Summary != (ParsedSummary{Total: 65, Pass: 65}) {
			t.Fatalf("golden go Summary = %+v, want 65/65 PASS", repGo.Summary)
		}

		res := runCLI(
			t, 30*time.Second,
			"--compare="+goJSON+","+rustJSON,
			"--report-json="+cmpJSON,
			"--report-md="+cmpMD,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("--compare go vs rust failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}

		rep := ReadJSONReport(t, cmpJSON)
		t4AssertJSONV1Base(t, rep, "compare", false)
		if len(rep.Differences) != 83 {
			t.Errorf("go vs rust differences len = %d, want 83", len(rep.Differences))
		}
		wantDiffCounts := map[string]int{
			"capability":           11,
			"client_default":       7,
			"scenario_verdict":     17,
			"scenario_trace_delta": 48,
		}
		if gotCounts := t4CountDiffKinds(rep.Differences); !reflect.DeepEqual(gotCounts, wantDiffCounts) {
			t.Errorf("go vs rust diff counts = %v, want %v", gotCounts, wantDiffCounts)
		}

		for _, id := range []string{"PUB-01", "TRAC-03"} {
			d := t4MustDiff(t, rep, "scenario_trace_delta", id)
			if d.Verdicts["go"] != StatusPass || d.Verdicts["rust"] != StatusSkippedNoMonitoring {
				t.Errorf("%s scenario_trace_delta = %+v, want go=PASS rust=SKIPPED_NO_MONITORING", id, *d)
			}
		}
		for _, id := range []string{"TRAC-01", "TRAC-01b", "TRAC-02", "TRAC-04"} {
			d := t4MustDiff(t, rep, "scenario_verdict", id)
			if d.Verdicts["go"] != StatusPass || d.Verdicts["rust"] != StatusSkippedNoMonitoring {
				t.Errorf("%s scenario_verdict = %+v, want go=PASS rust=SKIPPED_NO_MONITORING", id, *d)
			}
		}
		for _, id := range wantRustUnsupported {
			d := t4MustDiff(t, rep, "scenario_verdict", id)
			if d.Verdicts["go"] != StatusPass || d.Verdicts["rust"] != StatusUnsupportedByClient {
				t.Errorf("%s scenario_verdict = %+v, want go=PASS rust=UNSUPPORTED_BY_CLIENT", id, *d)
			}
		}

		md := ReadMarkdownReport(t, cmpMD)
		matTbl := t4FindMDTable(t, md, "Scenario", "Category", "go", "rust")
		if len(matTbl.Rows) != 65 {
			t.Errorf("Scenario Matrix rows = %d, want 65", len(matTbl.Rows))
		}
		t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "PUB-01", "go": StatusPass, "rust": StatusPass})
		t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-03", "go": StatusPass, "rust": StatusPass})
		t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-01", "go": StatusPass, "rust": StatusSkippedNoMonitoring})
		t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "PULL-04", "go": StatusPass, "rust": StatusUnsupportedByClient})

		diffTbl := t4FindMDTable(t, md, "Kind", "Scenario / Field", "go", "rust", "Description")
		if len(diffTbl.Rows) != 83 {
			t.Errorf("Behavioral Differences rows = %d, want 83", len(diffTbl.Rows))
		}
		t4FindMDTableRow(t, diffTbl, map[string]string{"Kind": "capability", "Scenario / Field": "supports_monitoring", "go": "true", "rust": "false"})
		t4FindMDTableRow(t, diffTbl, map[string]string{"Kind": "scenario_trace_delta", "Scenario / Field": "TRAC-03", "go": StatusPass, "rust": StatusSkippedNoMonitoring})
	})
}

// --- Workflow 3: Fault Regression Detection, Triage & Diagnostic/Trace Deltas ---

func TestT4_03_RegressionDetectionWorkflow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	goJSON := filepath.Join(dir, "go.json")
	goMD := filepath.Join(dir, "go.md")
	wrongJSON := filepath.Join(dir, "wrong.json")
	wrongMD := filepath.Join(dir, "wrong.md")
	invtrJSON := filepath.Join(dir, "invtr.json")
	invtrMD := filepath.Join(dir, "invtr.md")
	cmpJSON := filepath.Join(dir, "regress_cmp.json")
	cmpMD := filepath.Join(dir, "regress_cmp.md")

	var repWrong *ParsedReport
	var repInvtr *ParsedReport

	t.Run("GoldenBaseline", func(t *testing.T) {
		_, rep, _, _ := t4RunSingleSavedReports(
			t, 150*time.Second, 0, goJSON, goMD,
			[]string{"--mode=conformant-go", "--fast-timings"},
			"--suite-timeout=120s",
		)
		t4AssertJSONV1Base(t, rep, "single", false)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 65, Pass: 65}) {
			t.Fatalf("golden go Summary = %+v, want 65/65 PASS", rep.Summary)
		}
	})

	t.Run("CandidateWrongRPC", func(t *testing.T) {
		_, rep, _, _ := t4RunSingleSavedReports(
			t, 150*time.Second, 1, wrongJSON, wrongMD,
			[]string{"--mode=wrong-rpc", "--fast-timings"},
			"--suite-timeout=120s",
		)
		t4AssertJSONV1Base(t, rep, "single", false)
		wantSum := ParsedSummary{Total: 65, Pass: 33, Fail: 32}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("wrong-rpc Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, healthyID := range []string{"PULL-01", "PULL-04", "LEASE-01", "EOD-01", "EOD-02", "LIFE-01"} {
			if sc := t4MustScenario(t, rep, healthyID); sc.Status != StatusPass {
				t.Errorf("wrong-rpc/%s expected healthy PASS, got %+v", healthyID, *sc)
			}
		}
		for _, regID := range []string{"PUB-01", "PORD-02", "TRAC-01", "TRAC-03", "CFG-01"} {
			sc := t4MustScenario(t, rep, regID)
			if sc.Status != StatusFail || sc.Error == "" {
				t.Errorf("wrong-rpc/%s expected FAIL with diagnostic error, got %+v", regID, *sc)
			}
		}
		repWrong = rep
	})

	t.Run("CandidateInvalidTraces", func(t *testing.T) {
		_, rep, _, _ := t4RunSingleSavedReports(
			t, 150*time.Second, 1, invtrJSON, invtrMD,
			[]string{"--mode=invalid-traces", "--fault=bad-span-kind,missing-links", "--fast-timings"},
			"--suite-timeout=120s",
		)
		t4AssertJSONV1Base(t, rep, "single", false)
		wantSum := ParsedSummary{Total: 65, Pass: 60, Fail: 5}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("invalid-traces Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		wantFailed := []string{"TRAC-01", "TRAC-03", "TRAC-02", "TRAC-04", "CFG-01"}
		var gotFailed []string
		for _, sc := range rep.Scenarios {
			if sc.Status == StatusFail {
				gotFailed = append(gotFailed, sc.ID)
				if sc.TraceStatus != StatusFail || t4RawString(sc.Raw, "trace_status") != StatusFail {
					t.Errorf("invtr/%s expected trace_status=FAIL, got %q raw=%v", sc.ID, sc.TraceStatus, sc.Raw)
				}
				if !strings.Contains(sc.Error, "WRONG_SPAN_KIND") || !strings.Contains(sc.Error, "MISSING_LINK") {
					t.Errorf("invtr/%s error %q expected WRONG_SPAN_KIND and MISSING_LINK", sc.ID, sc.Error)
				}
			}
		}
		t4AssertExactIDSet(t, "invalid-traces failed IDs", gotFailed, wantFailed)
		if sc := t4MustScenario(t, rep, "TRAC-01b"); sc.Status != StatusPass || sc.TraceStatus != StatusPass {
			t.Errorf("invtr/TRAC-01b expected PASS/PASS, got %+v", *sc)
		}
		repInvtr = rep
	})

	t.Run("FullCompareRegressionTriage", func(t *testing.T) {
		res := runCLI(
			t, 30*time.Second,
			"--compare="+strings.Join([]string{goJSON, wrongJSON, invtrJSON}, ","),
			"--report-json="+cmpJSON,
			"--report-md="+cmpMD,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("regression --compare failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}

		rep := ReadJSONReport(t, cmpJSON)
		t4AssertJSONV1Base(t, rep, "compare", false)
		if len(rep.Targets) != 3 {
			t.Fatalf("Targets len = %d, want 3", len(rep.Targets))
		}
		if len(rep.Differences) != 33 {
			t.Errorf("regression diff len = %d, want 33", len(rep.Differences))
		}
		wantDiffCounts := map[string]int{"scenario_verdict": 33}
		if gotCounts := t4CountDiffKinds(rep.Differences); !reflect.DeepEqual(gotCounts, wantDiffCounts) {
			t.Errorf("regression diff counts = %v, want %v", gotCounts, wantDiffCounts)
		}

		// Every failed ID across wrong-rpc and invalid-traces must appear in scenario_verdict.
		expectedRegressed := make(map[string]struct{})
		for _, sc := range repWrong.Scenarios {
			if sc.Status == StatusFail {
				expectedRegressed[sc.ID] = struct{}{}
			}
		}
		for _, sc := range repInvtr.Scenarios {
			if sc.Status == StatusFail {
				expectedRegressed[sc.ID] = struct{}{}
			}
		}
		if len(expectedRegressed) != 33 {
			t.Fatalf("expected 33 union regressed IDs, got %d", len(expectedRegressed))
		}
		for id := range expectedRegressed {
			t4MustDiff(t, rep, "scenario_verdict", id)
		}
		dTrac04 := t4MustDiff(t, rep, "scenario_verdict", "TRAC-04")
		if dTrac04.Verdicts["go"] != StatusPass || dTrac04.Verdicts["wrong"] != StatusPass || dTrac04.Verdicts["invtr"] != StatusFail {
			t.Errorf("TRAC-04 verdicts unexpected: %+v", *dTrac04)
		}
		dPub01 := t4MustDiff(t, rep, "scenario_verdict", "PUB-01")
		if dPub01.Verdicts["go"] != StatusPass || dPub01.Verdicts["wrong"] != StatusFail || dPub01.Verdicts["invtr"] != StatusPass {
			t.Errorf("PUB-01 verdicts unexpected: %+v", *dPub01)
		}

		md := ReadMarkdownReport(t, cmpMD)
		sumTbl := t4FindMDTable(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t4FindMDTableRow(t, sumTbl, map[string]string{"Target": "go", "Pass": "65", "Fail": "0"})
		t4FindMDTableRow(t, sumTbl, map[string]string{"Target": "wrong", "Pass": "33", "Fail": "32"})
		t4FindMDTableRow(t, sumTbl, map[string]string{"Target": "invtr", "Pass": "60", "Fail": "5"})
		diffTbl := t4FindMDTable(t, md, "Kind", "Scenario / Field", "go", "wrong", "invtr", "Description")
		if len(diffTbl.Rows) != 33 {
			t.Errorf("Behavioral Differences rows = %d, want 33", len(diffTbl.Rows))
		}
		t4FindMDTableRow(t, diffTbl, map[string]string{"Kind": "scenario_verdict", "Scenario / Field": "TRAC-04", "go": StatusPass, "wrong": StatusPass, "invtr": StatusFail})
	})

	t.Run("DiagnosticAndTraceDeltaChecks", func(t *testing.T) {
		hungJSON := filepath.Join(dir, "hung_f.json")
		hungMD := filepath.Join(dir, "hung_f.md")
		_, repHung, _, _ := t4RunSingleSavedReports(
			t, 45*time.Second, 1, hungJSON, hungMD,
			[]string{"--mode=conformant-go", "--fault=hang-publish", "--fast-timings"},
			"--filter=^PUB-01$",
			"--scenario-timeout=400ms",
			"--suite-timeout=20s",
		)
		if sc := t4MustScenario(t, repHung, "PUB-01"); sc.Status != StatusFail || sc.StatusDetail != StatusTimeout {
			t.Fatalf("hung_f/PUB-01 expected FAIL (TIMEOUT), got %+v", *sc)
		}

		diagJSON := filepath.Join(dir, "diag_cmp.json")
		diagMD := filepath.Join(dir, "diag_cmp.md")
		rDiag := runCLI(t, 25*time.Second, "--compare="+wrongJSON+","+hungJSON, "--report-json="+diagJSON, "--report-md="+diagMD)
		if rDiag.ExitCode != 0 || rDiag.TimedOut || rDiag.Err != nil {
			t.Fatalf("diagnostic --compare failed: exit=%d err=%v\nstderr:\n%s", rDiag.ExitCode, rDiag.Err, rDiag.Stderr)
		}
		repDiag := ReadJSONReport(t, diagJSON)
		t4AssertJSONV1Base(t, repDiag, "compare", false)
		dDiag := t4MustDiff(t, repDiag, "scenario_diagnostic", "PUB-01")
		if dDiag.Verdicts["wrong"] != StatusFail || dDiag.Verdicts["hung_f"] != StatusFail ||
			!strings.Contains(dDiag.Details["hung_f"], "TIMEOUT:") || dDiag.Details["wrong"] == "" {
			t.Errorf("PUB-01 scenario_diagnostic unexpected: %+v", *dDiag)
		}
		mdDiag := ReadMarkdownReport(t, diagMD)
		diffDiagTbl := t4FindMDTable(t, mdDiag, "Kind", "Scenario / Field", "wrong", "hung_f", "Description")
		t4FindMDTableRow(t, diffDiagTbl, map[string]string{"Kind": "scenario_diagnostic", "Scenario / Field": "PUB-01", "wrong": StatusFail, "hung_f": StatusFail})

		// Targeted trace_delta check (go_f vs go_nomon_f).
		goFJSON := filepath.Join(dir, "go_f.json")
		goFMD := filepath.Join(dir, "go_f.md")
		t4RunSingleSavedReports(
			t, 45*time.Second, 0, goFJSON, goFMD,
			[]string{"--mode=conformant-go", "--fast-timings"},
			"--filter=^(PUB-01|PULL-01)$",
		)
		goNoMonJSON := filepath.Join(dir, "go_nomon_f.json")
		goNoMonMD := filepath.Join(dir, "go_nomon_f.md")
		t4RunSingleSavedReports(
			t, 45*time.Second, 0, goNoMonJSON, goNoMonMD,
			[]string{"--mode=conformant-go", "--monitoring=false", "--fast-timings"},
			"--filter=^(PUB-01|PULL-01)$",
		)

		tdJSON := filepath.Join(dir, "trace_delta_cmp.json")
		tdMD := filepath.Join(dir, "trace_delta_cmp.md")
		rTD := runCLI(t, 25*time.Second, "--compare="+goFJSON+","+goNoMonJSON, "--report-json="+tdJSON, "--report-md="+tdMD)
		if rTD.ExitCode != 0 || rTD.TimedOut || rTD.Err != nil {
			t.Fatalf("trace_delta --compare failed: exit=%d err=%v\nstderr:\n%s", rTD.ExitCode, rTD.Err, rTD.Stderr)
		}
		repTD := ReadJSONReport(t, tdJSON)
		t4AssertJSONV1Base(t, repTD, "compare", false)
		for _, id := range []string{"PUB-01", "PULL-01"} {
			d := t4MustDiff(t, repTD, "scenario_trace_delta", id)
			if d.Verdicts["go_f"] != StatusPass || d.Verdicts["go_nomon_f"] != StatusSkippedNoMonitoring {
				t.Errorf("%s trace_delta unexpected: %+v", id, *d)
			}
		}
	})
}

// --- Workflow 4: Bounded Timeouts, PGID SIGKILL Cleanup & Restart Recovery ---

func TestT4_04_BoundedTimeoutWorkflowUnderHungClient(t *testing.T) {
	t.Parallel()

	t.Run("SuiteTimeoutPGIDKillAndUnstartedCompare", func(t *testing.T) {
		dir := t.TempDir()
		pidFile := filepath.Join(dir, "hung_sleep.pid")
		unstartedSentinel := filepath.Join(dir, "unstarted.sentinel")
		outJSON := filepath.Join(dir, "t404.json")
		outMD := filepath.Join(dir, "t404.md")
		baseJSON := filepath.Join(dir, "baseline.json")
		baseMD := filepath.Join(dir, "baseline.md")
		cmpJSON := filepath.Join(dir, "t404_cmp.json")
		cmpMD := filepath.Join(dir, "t404_cmp.md")

		wHealthy, mHealthy := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")

		// Hung wrapper ignores SIGTERM, spawns background `sleep 60` in same PGID,
		// and execs e2e-test-proxy with hang-publish + ignore-shutdown + ignore-sigterm.
		mHung := newRunMarker(t)
		wHung := filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", mHung))
		hungScript := fmt.Sprintf("#!/bin/sh\ntrap '' TERM\nsleep 60 </dev/null >/dev/null 2>&1 &\nprintf '%%s\\n' \"$!\" > %s\nexec %s %s --mode=conformant-go --fast-timings --fault=hang-publish,ignore-shutdown,ignore-sigterm \"$@\"\n",
			shellQuote(pidFile), shellQuote(proxyBinaryPath), shellQuote("--marker="+mHung))
		writeExecutableFile(t, wHung, []byte(hungScript))

		// Unstarted wrapper writes a sentinel file if invoked at all.
		mUnstarted := newRunMarker(t)
		wUnstarted := filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", mUnstarted))
		unstartedScript := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$$\" > %s\nexec %s %s --mode=conformant-go --fast-timings \"$@\"\n",
			shellQuote(unstartedSentinel), shellQuote(proxyBinaryPath), shellQuote("--marker="+mUnstarted))
		writeExecutableFile(t, wUnstarted, []byte(unstartedScript))

		res := runCLI(
			t, 45*time.Second,
			"--proxy-target=healthy="+wHealthy,
			"--proxy-target=hung="+wHung,
			"--proxy-target=unstarted="+wUnstarted,
			"--filter=^(PUB-01|EOD-01)$",
			"--scenario-timeout=400ms",
			"--suite-timeout=2500ms",
			"--report-json="+outJSON,
			"--report-md="+outMD,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, mHealthy)
		assertNoOrphanProxy(t, mHung)
		assertNoOrphanProxy(t, mUnstarted)
		t4AssertFileNotExist(t, unstartedSentinel)

		pidRaw, err := os.ReadFile(pidFile)
		if err != nil {
			t.Fatalf("read hung sleep pidFile %q: %v", pidFile, err)
		}
		sleepPID, err := strconv.Atoi(strings.TrimSpace(string(pidRaw)))
		if err != nil || sleepPID <= 0 {
			t.Fatalf("invalid sleep PID %q in %q: %v", string(pidRaw), pidFile, err)
		}
		t4WaitPIDDead(t, sleepPID, 3*time.Second)

		rep := ReadJSONReport(t, outJSON)
		t4AssertJSONV1Base(t, rep, "compare", true)

		trHealthy := t4MustTarget(t, rep, "healthy")
		if trHealthy.TimedOut || trHealthy.TargetError != "" ||
			trHealthy.Summary == nil || *trHealthy.Summary != (ParsedSummary{Total: 2, Pass: 2}) {
			t.Errorf("healthy target unexpected: %+v", trHealthy)
		}
		if _, hasRawTO := t4RawBool(trHealthy.Raw, "timed_out"); hasRawTO {
			t.Errorf("healthy raw timed_out should be omitted when false, got %#v", trHealthy.Raw["timed_out"])
		}
		for _, id := range []string{"PUB-01", "EOD-01"} {
			if sc := t4MustTargetScenario(t, trHealthy, id); sc.Status != StatusPass {
				t.Errorf("healthy/%s status = %q, want PASS", id, sc.Status)
			}
		}

		trHung := t4MustTarget(t, rep, "hung")
		if !trHung.TimedOut || !strings.Contains(trHung.TargetError, "suite timed out") ||
			trHung.Summary == nil || trHung.Summary.TimeoutCount != 1 || trHung.Summary.Fail != 1 {
			t.Errorf("hung target unexpected: %+v", trHung)
		}
		if b, ok := t4RawBool(trHung.Raw, "timed_out"); !ok || !b {
			t.Errorf("hung raw timed_out = %#v, want true", trHung.Raw["timed_out"])
		}
		hungPub01 := t4MustTargetScenario(t, trHung, "PUB-01")
		if hungPub01.Status != StatusFail || hungPub01.StatusDetail != StatusTimeout ||
			t4RawString(hungPub01.Raw, "status_detail") != StatusTimeout {
			t.Errorf("hung/PUB-01 expected FAIL (TIMEOUT), got %+v raw=%v", *hungPub01, hungPub01.Raw)
		}

		trUnstarted := t4MustTarget(t, rep, "unstarted")
		wantUnstartedErr := "suite timed out before starting target: context deadline exceeded"
		if !trUnstarted.TimedOut || trUnstarted.TargetError != wantUnstartedErr ||
			t4RawString(trUnstarted.Raw, "target_error") != wantUnstartedErr ||
			len(trUnstarted.Scenarios) != 0 || trUnstarted.Summary == nil || trUnstarted.Summary.Total != 0 {
			t.Errorf("unstarted target unexpected: %+v raw=%v", trUnstarted, trUnstarted.Raw)
		}

		dErr := t4MustDiff(t, rep, "target_error", "")
		if dErr.Verdicts["healthy"] != "OK" || dErr.Verdicts["hung"] != StatusError || dErr.Verdicts["unstarted"] != StatusError {
			t.Errorf("t404 target_error verdicts unexpected: %+v", *dErr)
		}
		dPub := t4MustDiff(t, rep, "scenario_verdict", "PUB-01")
		if dPub.Verdicts["healthy"] != StatusPass || dPub.Verdicts["hung"] != StatusFail || dPub.Verdicts["unstarted"] != "MISSING" {
			t.Errorf("t404 PUB-01 verdicts unexpected: %+v", *dPub)
		}

		md := ReadMarkdownReport(t, outMD)
		matTbl := t4FindMDTable(t, md, "Scenario", "Category", "healthy", "hung", "unstarted")
		t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "PUB-01", "healthy": StatusPass, "hung": "FAIL (TIMEOUT)", "unstarted": "MISSING"})

		// Compare ingestion of timed-out report against clean baseline.
		t4RunSingleSavedReports(
			t, 45*time.Second, 0, baseJSON, baseMD,
			[]string{"--mode=conformant-go", "--fast-timings"},
			"--filter=^(PUB-01|EOD-01)$",
		)
		rCmp := runCLI(
			t, 25*time.Second,
			"--compare="+baseJSON+","+outJSON,
			"--report-json="+cmpJSON,
			"--report-md="+cmpMD,
		)
		if rCmp.ExitCode != 0 || rCmp.TimedOut || rCmp.Err != nil {
			t.Fatalf("--compare with timed-out report failed: exit=%d err=%v\nstderr:\n%s",
				rCmp.ExitCode, rCmp.Err, rCmp.Stderr)
		}
		cmpRep := ReadJSONReport(t, cmpJSON)
		t4AssertJSONV1Base(t, cmpRep, "compare", true)
		if len(cmpRep.Targets) != 4 {
			t.Fatalf("merged compare targets len = %d, want 4", len(cmpRep.Targets))
		}
		dCmpErr := t4MustDiff(t, cmpRep, "target_error", "")
		if dCmpErr.Verdicts["healthy"] != "OK" || dCmpErr.Verdicts["hung"] != StatusError || dCmpErr.Verdicts["unstarted"] != StatusError {
			t.Errorf("merged target_error verdicts unexpected: %+v", *dCmpErr)
		}
		dCmpPub := t4MustDiff(t, cmpRep, "scenario_verdict", "PUB-01")
		if dCmpPub.Verdicts["healthy"] != StatusPass || dCmpPub.Verdicts["hung"] != StatusFail || dCmpPub.Verdicts["unstarted"] != "MISSING" {
			t.Errorf("merged PUB-01 verdicts unexpected: %+v", *dCmpPub)
		}
		ReadMarkdownReport(t, cmpMD)
	})

	t.Run("ScenarioTimeoutPGIDKillAndRestartRecovery", func(t *testing.T) {
		dir := t.TempDir()
		shotSentinel := filepath.Join(dir, "shot1.done")
		pidFile := filepath.Join(dir, "shot1_sleep.pid")
		outJSON := filepath.Join(dir, "oneshot.json")
		outMD := filepath.Join(dir, "oneshot.md")

		marker := newRunMarker(t)
		wOneShot := filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))
		// First invocation traps TERM, spawns background sleep 60, and runs hang-publish;
		// restarted invocation runs clean conformant-go.
		oneShotScript := fmt.Sprintf("#!/bin/sh\nif [ ! -f %s ]; then\n  : > %s\n  trap '' TERM\n  sleep 60 </dev/null >/dev/null 2>&1 &\n  printf '%%s\\n' \"$!\" > %s\n  exec %s %s --mode=conformant-go --fast-timings --fault=hang-publish,ignore-shutdown,ignore-sigterm \"$@\"\nfi\nexec %s %s --mode=conformant-go --fast-timings \"$@\"\n",
			shellQuote(shotSentinel), shellQuote(shotSentinel), shellQuote(pidFile),
			shellQuote(proxyBinaryPath), shellQuote("--marker="+marker),
			shellQuote(proxyBinaryPath), shellQuote("--marker="+marker))
		writeExecutableFile(t, wOneShot, []byte(oneShotScript))

		res := runCLIWithEnv(
			t, 45*time.Second,
			[]string{"PUBSUB_EMULATOR_HOST=poison.invalid:9999"},
			"--proxy-cmd="+wOneShot,
			"--filter=^(PUB-01|CFG-01)$",
			"--scenario-timeout=400ms",
			"--suite-timeout=25s",
			"--report-json="+outJSON,
			"--report-md="+outMD,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, marker)

		pidRaw, err := os.ReadFile(pidFile)
		if err != nil {
			t.Fatalf("read sleep pidFile %q: %v", pidFile, err)
		}
		sleepPID, err := strconv.Atoi(strings.TrimSpace(string(pidRaw)))
		if err != nil || sleepPID <= 0 {
			t.Fatalf("invalid sleep PID %q: %v", string(pidRaw), err)
		}
		t4WaitPIDDead(t, sleepPID, 3*time.Second)

		rep := ReadJSONReport(t, outJSON)
		t4AssertJSONV1Base(t, rep, "single", false)
		wantSum := ParsedSummary{Total: 2, Pass: 1, Fail: 1, TimeoutCount: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("oneshot Summary = %+v, want %+v", rep.Summary, wantSum)
		}

		pub01 := t4MustScenario(t, rep, "PUB-01")
		if pub01.Status != StatusFail || pub01.StatusDetail != StatusTimeout ||
			t4RawString(pub01.Raw, "status_detail") != StatusTimeout || pub01.ProxyRestarted {
			t.Errorf("PUB-01 unexpected: %+v raw=%v", *pub01, pub01.Raw)
		}
		cfg01 := t4MustScenario(t, rep, "CFG-01")
		if cfg01.Status != StatusPass || !cfg01.ProxyRestarted || cfg01.Raw["proxy_restarted"] != true {
			t.Errorf("CFG-01 expected PASS with proxy_restarted=true, got %+v raw=%v", *cfg01, cfg01.Raw)
		}

		md := ReadMarkdownReport(t, outMD)
		if !md.HasFailureBreakdown || md.TimeoutCount != 1 || md.ErrorCount != 0 {
			t.Errorf("MD failure breakdown unexpected: has=%v to=%d err=%d", md.HasFailureBreakdown, md.TimeoutCount, md.ErrorCount)
		}
		scenTbl := t4FindMDTable(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		t4FindMDTableRow(t, scenTbl, map[string]string{"Scenario": "PUB-01", "Status": "FAIL (TIMEOUT)"})
		t4FindMDTableRow(t, scenTbl, map[string]string{"Scenario": "CFG-01", "Status": StatusPass})
	})
}

// --- Workflow 5: Monitoring Rollout Migration (false -> true) ---

func TestT4_05_MonitoringRolloutMigrationWorkflow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	beforeJSON := filepath.Join(dir, "before.json")
	beforeMD := filepath.Join(dir, "before.md")
	afterJSON := filepath.Join(dir, "after.json")
	afterMD := filepath.Join(dir, "after.md")
	cmpJSON := filepath.Join(dir, "migration_cmp.json")
	cmpMD := filepath.Join(dir, "migration_cmp.md")

	wantSkipped := []string{"TRAC-01", "TRAC-01b", "TRAC-02", "TRAC-04"}
	var beforePassIDs []string

	t.Run("BeforeMonitoringFalse", func(t *testing.T) {
		_, rep, _, _ := t4RunSingleSavedReports(
			t, 150*time.Second, 0, beforeJSON, beforeMD,
			[]string{"--mode=conformant-go", "--monitoring=false", "--fast-timings"},
			"--suite-timeout=120s",
		)
		t4AssertJSONV1Base(t, rep, "single", false)
		wantSum := ParsedSummary{Total: 65, Pass: 61, SkippedNoMonitoring: 4}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("before Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || *rep.Capabilities.SupportsMonitoring {
			t.Errorf("before expected SupportsMonitoring=false, got %+v", rep.Capabilities)
		}
		var gotSkipped []string
		for _, sc := range rep.Scenarios {
			if sc.TraceStatus != StatusSkippedNoMonitoring {
				t.Errorf("before/%s trace_status = %q, want SKIPPED_NO_MONITORING", sc.ID, sc.TraceStatus)
			}
			switch sc.Status {
			case StatusSkippedNoMonitoring:
				gotSkipped = append(gotSkipped, sc.ID)
			case StatusPass:
				beforePassIDs = append(beforePassIDs, sc.ID)
			}
		}
		t4AssertExactIDSet(t, "before skipped IDs", gotSkipped, wantSkipped)
		if trac03 := t4MustScenario(t, rep, "TRAC-03"); trac03.Status != StatusPass || trac03.TraceStatus != StatusSkippedNoMonitoring {
			t.Errorf("before/TRAC-03 unexpected: %+v", *trac03)
		}
	})

	t.Run("AfterMonitoringTrue", func(t *testing.T) {
		_, rep, _, _ := t4RunSingleSavedReports(
			t, 150*time.Second, 0, afterJSON, afterMD,
			[]string{"--mode=conformant-go", "--fast-timings"},
			"--suite-timeout=120s",
		)
		t4AssertJSONV1Base(t, rep, "single", false)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 65, Pass: 65}) {
			t.Fatalf("after Summary = %+v, want 65/65 PASS", rep.Summary)
		}
		for _, sc := range rep.Scenarios {
			if sc.Status != StatusPass || sc.TraceStatus != StatusPass {
				t.Errorf("after/%s expected PASS/PASS, got status=%q trace=%q", sc.ID, sc.Status, sc.TraceStatus)
			}
		}
	})

	t.Run("CompareRolloutDelta", func(t *testing.T) {
		res := runCLI(
			t, 30*time.Second,
			"--compare="+beforeJSON+","+afterJSON,
			"--report-json="+cmpJSON,
			"--report-md="+cmpMD,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("monitoring migration --compare failed: exit=%d err=%v\nstderr:\n%s",
				res.ExitCode, res.Err, res.Stderr)
		}

		rep := ReadJSONReport(t, cmpJSON)
		t4AssertJSONV1Base(t, rep, "compare", false)
		if len(rep.Differences) != 66 {
			t.Errorf("migration diff len = %d, want 66", len(rep.Differences))
		}
		wantCounts := map[string]int{
			"capability":           1,
			"scenario_verdict":     4,
			"scenario_trace_delta": 61,
		}
		if gotCounts := t4CountDiffKinds(rep.Differences); !reflect.DeepEqual(gotCounts, wantCounts) {
			t.Errorf("migration diff counts = %v, want %v", gotCounts, wantCounts)
		}

		dMon := t4MustDiff(t, rep, "capability", "supports_monitoring")
		if dMon.Verdicts["before"] != "false" || dMon.Verdicts["after"] != "true" {
			t.Errorf("supports_monitoring verdicts = %v, want before=false after=true", dMon.Verdicts)
		}
		for _, id := range wantSkipped {
			d := t4MustDiff(t, rep, "scenario_verdict", id)
			if d.Verdicts["before"] != StatusSkippedNoMonitoring || d.Verdicts["after"] != StatusPass {
				t.Errorf("%s verdict diff = %+v, want SKIPPED_NO_MONITORING -> PASS", id, *d)
			}
		}
		for _, id := range beforePassIDs {
			d := t4MustDiff(t, rep, "scenario_trace_delta", id)
			if d.Verdicts["before"] != StatusSkippedNoMonitoring || d.Verdicts["after"] != StatusPass {
				t.Errorf("%s trace_delta = %+v, want SKIPPED_NO_MONITORING -> PASS", id, *d)
			}
		}

		md := ReadMarkdownReport(t, cmpMD)
		capTbl := t4FindMDTable(t, md, "Field", "before", "after")
		if len(capTbl.Rows) != 1 {
			t.Errorf("Capabilities & Defaults rows = %d, want 1 (supports_monitoring only)", len(capTbl.Rows))
		}
		t4FindMDTableRow(t, capTbl, map[string]string{"Field": "supports_monitoring", "before": "false", "after": "true"})

		matTbl := t4FindMDTable(t, md, "Scenario", "Category", "before", "after")
		if len(matTbl.Rows) != 65 {
			t.Errorf("Scenario Matrix rows = %d, want 65", len(matTbl.Rows))
		}
		t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-01", "before": StatusSkippedNoMonitoring, "after": StatusPass})
		t4FindMDTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-03", "before": StatusPass, "after": StatusPass})

		diffTbl := t4FindMDTable(t, md, "Kind", "Scenario / Field", "before", "after", "Description")
		if len(diffTbl.Rows) != 66 {
			t.Errorf("Behavioral Differences rows = %d, want 66", len(diffTbl.Rows))
		}
	})
}

// --- Workflow 6: Category-Filtered Progressive Rollout & 100% Union Verification ---

func TestT4_06_CategoryFilteredProgressiveRollout(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	wGo, mGo := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")

	var allCatalogIDs []string
	var stage1IDs, stage2IDs, stage3IDs []string

	t.Run("ValidateCatalog65And13Categories", func(t *testing.T) {
		res := runCLI(t, 20*time.Second, "--list")
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("--list failed: exit=%d err=%v\nstderr:\n%s", res.ExitCode, res.Err, res.Stderr)
		}
		listed := t4ParseCLIList(t, res.Stdout)
		if len(listed) != 65 {
			t.Fatalf("--list returned %d scenarios, want 65", len(listed))
		}
		catCounts := make(map[string]int, len(t4All13Categories))
		for _, sc := range listed {
			catCounts[sc.Category]++
			allCatalogIDs = append(allCatalogIDs, sc.ID)
		}
		for _, cat := range t4All13Categories {
			if catCounts[cat] == 0 {
				t.Errorf("--list missing category %q", cat)
			}
		}
		if len(catCounts) != 13 {
			t.Errorf("--list distinct category count = %d, want 13 (%v)", len(catCounts), catCounts)
		}
	})

	runStage := func(t *testing.T, stageName, filter string, wantCount int, wantCats map[string]int) []string {
		t.Helper()
		listRes := runCLI(t, 20*time.Second, "--list", "--filter="+filter)
		if listRes.ExitCode != 0 || listRes.TimedOut || listRes.Err != nil {
			t.Fatalf("%s --list failed: exit=%d err=%v stderr=%s", stageName, listRes.ExitCode, listRes.Err, listRes.Stderr)
		}
		listed := t4ParseCLIList(t, listRes.Stdout)
		if len(listed) != wantCount {
			t.Fatalf("%s --list count = %d, want %d", stageName, len(listed), wantCount)
		}

		jPath := filepath.Join(dir, stageName+".json")
		mPath := filepath.Join(dir, stageName+".md")
		runRes := runCLI(
			t, 75*time.Second,
			"--proxy-cmd="+wGo,
			"--filter="+filter,
			"--suite-timeout=60s",
			"--report-json="+jPath,
			"--report-md="+mPath,
		)
		if runRes.ExitCode != 0 || runRes.TimedOut || runRes.Err != nil {
			t.Fatalf("%s CLI run failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				stageName, runRes.ExitCode, runRes.TimedOut, runRes.Err, runRes.Stderr)
		}
		assertNoOrphanProxy(t, mGo)

		rep := ReadJSONReport(t, jPath)
		t4AssertJSONV1Base(t, rep, "single", false)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: wantCount, Pass: wantCount}) {
			t.Fatalf("%s Summary = %+v, want %d PASS", stageName, rep.Summary, wantCount)
		}
		gotCats := make(map[string]int)
		var executedIDs []string
		for _, sc := range rep.Scenarios {
			if sc.Status != StatusPass {
				t.Errorf("%s/%s status = %q, want PASS", stageName, sc.ID, sc.Status)
			}
			gotCats[sc.Category]++
			executedIDs = append(executedIDs, sc.ID)
		}
		if !reflect.DeepEqual(gotCats, wantCats) {
			t.Errorf("%s category distribution = %v, want %v", stageName, gotCats, wantCats)
		}
		var listedIDs []string
		for _, item := range listed {
			listedIDs = append(listedIDs, item.ID)
		}
		t4AssertExactIDSet(t, stageName+" listed vs executed", executedIDs, listedIDs)
		return executedIDs
	}

	t.Run("Stage1CoreDataPlane", func(t *testing.T) {
		stage1IDs = runStage(t, "stage1", "^(publish|ordering|pull)$", 26, map[string]int{
			"publish":  12,
			"ordering": 6,
			"pull":     8,
		})
	})

	t.Run("Stage2ResilienceLeasingLifecycle", func(t *testing.T) {
		stage2IDs = runStage(t, "stage2", "^(flow_control|retry|lease|eod|lifecycle)$", 24, map[string]int{
			"flow_control": 7,
			"retry":        3,
			"lease":        5,
			"eod":          6,
			"lifecycle":    3,
		})
	})

	t.Run("Stage3AdvancedTracingConfig", func(t *testing.T) {
		stage3IDs = runStage(t, "stage3", "^(compression|hedging|dlq|traces|config)$", 15, map[string]int{
			"compression": 1,
			"hedging":     4,
			"dlq":         1,
			"traces":      5,
			"config":      4,
		})
	})

	t.Run("VerifyDisjointUnionAndFullSweep", func(t *testing.T) {
		var combined []string
		combined = append(combined, stage1IDs...)
		combined = append(combined, stage2IDs...)
		combined = append(combined, stage3IDs...)
		if len(combined) != 65 {
			t.Fatalf("combined stage count = %d, want 65 (26+24+15)", len(combined))
		}
		t4AssertExactIDSet(t, "disjoint stage union vs 65 catalog", combined, allCatalogIDs)

		fullJSON := filepath.Join(dir, "full_sweep.json")
		fullMD := filepath.Join(dir, "full_sweep.md")
		res := runCLI(
			t, 150*time.Second,
			"--proxy-cmd="+wGo,
			"--suite-timeout=120s",
			"--report-json="+fullJSON,
			"--report-md="+fullMD,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("final full-65 sweep failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, mGo)

		rep := ReadJSONReport(t, fullJSON)
		t4AssertJSONV1Base(t, rep, "single", false)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 65, Pass: 65}) {
			t.Fatalf("full sweep Summary = %+v, want 65/65 PASS", rep.Summary)
		}
		t4AssertAll13Categories(t, "full-sweep", rep.Scenarios)

		md := ReadMarkdownReport(t, fullMD)
		sumTbl := t4FindMDTable(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t4FindMDTableRow(t, sumTbl, map[string]string{"Total": "65", "Pass": "65", "Fail": "0"})
		scenTbl := t4FindMDTable(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		if len(scenTbl.Rows) != 65 {
			t.Errorf("full sweep MD Scenario Results rows = %d, want 65", len(scenTbl.Rows))
		}
	})
}

// --- Workflow 7: Mid-Suite Crash Recovery, Env Isolation & Clean Teardown ---

func TestT4_07_MidSuiteCrashRecoveryAndCleanTeardown(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	outJSON := filepath.Join(dir, "crash6.json")
	outMD := filepath.Join(dir, "crash6.md")

	wCrash, marker := writeProxyWrapper(t, "--mode=conformant-go", "--fault=crash-after=6,ignore-shutdown", "--fast-timings")
	res := runCLIWithEnv(
		t, 150*time.Second,
		[]string{"PUBSUB_EMULATOR_HOST=poison.invalid:9999"},
		"--proxy-cmd="+wCrash,
		"--suite-timeout=120s",
		"--report-json="+outJSON,
		"--report-md="+outMD,
	)
	if res.ExitCode != 1 || res.TimedOut {
		t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v err=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}
	assertNoOrphanProxy(t, marker)

	rep := ReadJSONReport(t, outJSON)
	t4AssertJSONV1Base(t, rep, "single", false)
	wantSum := ParsedSummary{
		Total:      65,
		Pass:       39,
		Fail:       26,
		ErrorCount: 26,
	}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Fatalf("crash6 Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	t4AssertAll13Categories(t, "crash6", rep.Scenarios)

	// Initial crash & immediate recovery checkpoints.
	if sc := t4MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass || sc.ProxyRestarted {
		t.Errorf("PUB-01 expected PASS with proxy_restarted=false, got %+v", *sc)
	}
	pord02 := t4MustScenario(t, rep, "PORD-02")
	if pord02.Status != StatusFail || pord02.StatusDetail != StatusError ||
		t4RawString(pord02.Raw, "status_detail") != StatusError || pord02.ProxyRestarted ||
		!strings.Contains(pord02.Error, "simulated crash-after=6 at RPC #6") ||
		!strings.Contains(pord02.Error, "exit status 2") {
		t.Errorf("PORD-02 expected crash-after=6 exit status 2 ERROR without restart, got %+v raw=%v", *pord02, pord02.Raw)
	}
	if sc := t4MustScenario(t, rep, "PULL-01"); sc.Status != StatusPass || !sc.ProxyRestarted || sc.Raw["proxy_restarted"] != true {
		t.Errorf("PULL-01 expected PASS with proxy_restarted=true, got %+v raw=%v", *sc, sc.Raw)
	}

	// Exact 26 ERROR IDs and exact 23 proxy_restarted=true IDs.
	wantErrorIDs := []string{
		"PORD-02", "TRAC-01", "PUB-02", "PUB-03", "PUB-05", "PUB-06b", "PUB-08",
		"PORD-03", "PORD-04", "PFC-01", "PFC-02", "PFC-03", "PFC-03b", "CMP-01",
		"HDG-01", "HDG-02", "HDG-03", "PRET-01", "PRET-02", "LEASE-01", "LEASE-05",
		"SUBF-01b", "EOD-01", "LIFE-01", "TRAC-02", "CFG-03",
	}
	wantRestartedIDs := []string{
		"PULL-01", "TRAC-03", "PUB-02b", "PUB-04", "PUB-06a", "PUB-07", "PUB-09",
		"PORD-04", "PFC-01", "PFC-02", "PFC-03", "PFC-03b", "CMP-01", "HDG-01",
		"HDG-04", "PRET-01b", "PULL-02", "LEASE-02", "DLQ-01", "SUBF-02", "EOD-01b",
		"LIFE-02", "CFG-04",
	}
	var gotErrorIDs, gotRestartedIDs []string
	for _, sc := range rep.Scenarios {
		if sc.Status == StatusFail {
			if sc.StatusDetail != StatusError {
				t.Errorf("crash6/%s expected StatusDetail=ERROR, got %+v", sc.ID, sc)
			}
			gotErrorIDs = append(gotErrorIDs, sc.ID)
		}
		if sc.ProxyRestarted {
			gotRestartedIDs = append(gotRestartedIDs, sc.ID)
		}
	}
	t4AssertExactIDSet(t, "crash6 ERROR IDs", gotErrorIDs, wantErrorIDs)
	t4AssertExactIDSet(t, "crash6 proxy_restarted IDs", gotRestartedIDs, wantRestartedIDs)

	// Dual crash-after-restart checkpoints (both ERROR and proxy_restarted=true).
	for _, dualID := range []string{"PORD-04", "PFC-01", "PFC-02", "PFC-03", "PFC-03b", "CMP-01", "HDG-01"} {
		sc := t4MustScenario(t, rep, dualID)
		if sc.Status != StatusFail || sc.StatusDetail != StatusError || !sc.ProxyRestarted {
			t.Errorf("%s expected dual ERROR + proxy_restarted=true, got %+v", dualID, *sc)
		}
	}

	// Tail isolated recovery & poisoned PUBSUB_EMULATOR_HOST isolation (#60..#65).
	if sc := t4MustScenario(t, rep, "TRAC-02"); sc.Status != StatusFail || sc.StatusDetail != StatusError {
		t.Errorf("TRAC-02 unexpected: %+v", *sc)
	}
	for _, id := range []string{"TRAC-04", "CFG-01", "CFG-02"} {
		if sc := t4MustScenario(t, rep, id); sc.Status != StatusPass || sc.ProxyRestarted {
			t.Errorf("%s expected PASS with proxy_restarted=false, got %+v", id, *sc)
		}
	}
	if sc := t4MustScenario(t, rep, "CFG-03"); sc.Status != StatusFail || sc.StatusDetail != StatusError || sc.ProxyRestarted {
		t.Errorf("CFG-03 unexpected: %+v", *sc)
	}
	if sc := t4MustScenario(t, rep, "CFG-04"); sc.Status != StatusPass || !sc.ProxyRestarted {
		t.Errorf("CFG-04 expected PASS with proxy_restarted=true, got %+v", *sc)
	}

	md := ReadMarkdownReport(t, outMD)
	sumTbl := t4FindMDTable(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
	t4FindMDTableRow(t, sumTbl, map[string]string{"Total": "65", "Pass": "39", "Fail": "26"})
	if !md.HasFailureBreakdown || md.ErrorCount != 26 || md.TimeoutCount != 0 {
		t.Errorf("MD failure breakdown unexpected: has=%v err=%d to=%d", md.HasFailureBreakdown, md.ErrorCount, md.TimeoutCount)
	}
	scenTbl := t4FindMDTable(t, md, "Scenario", "Category", "Status", "Duration", "Details")
	if len(scenTbl.Rows) != 65 {
		t.Errorf("MD Scenario Results rows = %d, want 65", len(scenTbl.Rows))
	}
	t4FindMDTableRow(t, scenTbl, map[string]string{"Scenario": "PORD-02", "Status": "FAIL (ERROR)"})
	t4FindMDTableRow(t, scenTbl, map[string]string{"Scenario": "CFG-01", "Status": StatusPass})
	t4FindMDTableRow(t, scenTbl, map[string]string{"Scenario": "CFG-04", "Status": StatusPass})
	if len(md.DiagnosticsBullets) != 26 {
		t.Errorf("MD DiagnosticsBullets len = %d, want 26", len(md.DiagnosticsBullets))
	}
}
