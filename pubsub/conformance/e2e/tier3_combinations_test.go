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
Tier 3 — Cross-Feature Pairwise Combinatorial Tests (`TestT3_01` .. `TestT3_14`)

Pairwise coverage matrix across all 7 user-facing E2E features (`E2E-F1` .. `E2E-F7`),
covering ALL C(7,2) = 21 distinct unordered feature pairs with dual-feature non-vacuity:

	Feature Legend:
	  F1: Proxy Subprocess Lifecycle & Protocol Handshake
	  F2: Fake Pub/Sub Server RPC Validation & Fault Detection
	  F3: Optional Client-Side OTel Trace Verification
	  F4: Scenario Filtering & Bounded Execution Timeouts
	  F5: Structured Reporting, Status Semantics & Strictness Flags
	  F6: Multi-Client Execution & Cross-Client Diff Comparison
	  F7: Conformance Suite Coverage & Real Client Proxy Execution

	Test-to-Feature Assignment (14 Catalog Tests):
	  01. TestT3_01_Filter_MonitoringOn_Reports                        {F1, F2, F3, F4, F5}
	  02. TestT3_02_Filter_MonitoringOff_StrictUnsupported             {F1, F2, F3, F4, F5}
	  03. TestT3_03_ScenarioTimeout_HungProxy_CleanReportAndNoOrphans  {F1, F4, F5}
	  04. TestT3_04_SuiteTimeout_MultiTarget_NoOrphans                 {F1, F4, F6}
	  05. TestT3_05_WrongRPC_MonitoringDisabled_StillFailsWithReport   {F2, F3, F5}
	  06. TestT3_06_MissingTraces_ValidRPC_FailsWithDiagnostics        {F2, F3, F5}
	  07. TestT3_07_MultiTarget_MixedMonitoring_CompareDiff            {F1, F3, F5, F6, F7}
	  08. TestT3_08_MultiTarget_PartialUnsupported_StrictVsPermissive  {F5, F6, F7}
	  09. TestT3_09_Filter_MultiTarget_CompareSubset                   {F4, F6, F7}
	  10. TestT3_10_CompareOfflineJSON_FaultyVsConformant              {F2, F3, F5, F6}
	  11. TestT3_11_ParallelOrSequentialScenarios_Timeout_FaultIsolation {F1, F2, F4, F7}
	  12. TestT3_12_OutOfOrderRPC_MultiTarget_DiffPinpointsBug         {F2, F6, F7}
	  13. TestT3_13_CrashMidSuite_PartialReportAndCleanup              {F1, F4, F5, F7}
	  14. TestT3_14_Compare_SuiteTimeout_BoundedExecution              {F4, F6, F7}

	Complete 21 / 21 Unordered Pair Mapping:
	  (F1, F2) -> TestT3_01, TestT3_02, TestT3_11
	  (F1, F3) -> TestT3_01, TestT3_02, TestT3_07
	  (F1, F4) -> TestT3_01, TestT3_02, TestT3_03, TestT3_04, TestT3_11, TestT3_13
	  (F1, F5) -> TestT3_01, TestT3_02, TestT3_03, TestT3_07, TestT3_13
	  (F1, F6) -> TestT3_04, TestT3_07
	  (F1, F7) -> TestT3_07, TestT3_11, TestT3_13
	  (F2, F3) -> TestT3_01, TestT3_02, TestT3_05, TestT3_06, TestT3_10
	  (F2, F4) -> TestT3_01, TestT3_02, TestT3_11
	  (F2, F5) -> TestT3_01, TestT3_02, TestT3_05, TestT3_06, TestT3_10
	  (F2, F6) -> TestT3_10, TestT3_12
	  (F2, F7) -> TestT3_11, TestT3_12
	  (F3, F4) -> TestT3_01, TestT3_02
	  (F3, F5) -> TestT3_01, TestT3_02, TestT3_05, TestT3_06, TestT3_07, TestT3_10
	  (F3, F6) -> TestT3_07, TestT3_10
	  (F3, F7) -> TestT3_07
	  (F4, F5) -> TestT3_01, TestT3_02, TestT3_03, TestT3_13
	  (F4, F6) -> TestT3_04, TestT3_09, TestT3_14
	  (F4, F7) -> TestT3_09, TestT3_11, TestT3_13, TestT3_14
	  (F5, F6) -> TestT3_07, TestT3_08, TestT3_10
	  (F5, F7) -> TestT3_07, TestT3_08, TestT3_13
	  (F6, F7) -> TestT3_07, TestT3_08, TestT3_09, TestT3_12, TestT3_14
*/

package e2e_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// --- Local Unexported Constants & Helpers (stdlib-only, t3* prefix for zero collision) ---

const t3All13Filter = "^(PUB-01|PORD-01|PFC-01|CMP-01|HDG-01|PRET-01b|PULL-03|SUBF-01b|EOD-04|LIFE-01|DLQ-01|TRAC-02|CFG-01|LEASE-01)$"

var t3All13Categories = []string{
	"publish",
	"ordering",
	"pull",
	"traces",
	"flow_control",
	"compression",
	"hedging",
	"retry",
	"lease",
	"dlq",
	"eod",
	"lifecycle",
	"config",
}

func t3RawString(m map[string]any, key string) string {
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

func t3RawBool(m map[string]any, key string) (bool, bool) {
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

func t3AssertFileNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected file %q not to exist, got stat err=%v", path, err)
	}
}

func t3MustScenario(t *testing.T, rep *ParsedReport, id string) *ParsedScenarioResult {
	t.Helper()
	if rep == nil {
		t.Fatalf("t3MustScenario(%q): nil report", id)
	}
	sc, ok := rep.FindScenario(id)
	if !ok {
		t.Fatalf("scenario %q not found in root report (total=%d)", id, len(rep.Scenarios))
	}
	return sc
}

func t3MustTarget(t *testing.T, rep *ParsedReport, name string) *ParsedTargetReport {
	t.Helper()
	if rep == nil {
		t.Fatalf("t3MustTarget(%q): nil report", name)
	}
	tr, ok := rep.Targets[name]
	if !ok || tr == nil {
		t.Fatalf("target %q not found in report (targets=%d)", name, len(rep.Targets))
	}
	return tr
}

func t3MustTargetScenario(t *testing.T, tr *ParsedTargetReport, id string) *ParsedScenarioResult {
	t.Helper()
	if tr == nil {
		t.Fatalf("t3MustTargetScenario(%q): nil target report", id)
	}
	for i := range tr.Scenarios {
		if tr.Scenarios[i].ID == id {
			return &tr.Scenarios[i]
		}
	}
	t.Fatalf("scenario %q not found in target %q report (total=%d)", id, tr.TargetName, len(tr.Scenarios))
	return nil
}

func t3MustDiff(t *testing.T, rep *ParsedReport, kind, idOrField string) *ParsedDiffEntry {
	t.Helper()
	if rep == nil {
		t.Fatalf("t3MustDiff(%q, %q): nil report", kind, idOrField)
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

func t3HasDiff(rep *ParsedReport, kind, idOrField string) bool {
	if rep == nil {
		return false
	}
	for i := range rep.Differences {
		d := &rep.Differences[i]
		if d.Kind != kind {
			continue
		}
		if idOrField == "" || d.ScenarioID == idOrField || d.Field == idOrField {
			return true
		}
	}
	return false
}

func t3AssertJSONV1Base(t *testing.T, rep *ParsedReport, wantMode string, wantTimedOut bool) {
	t.Helper()
	if rep == nil {
		t.Fatalf("nil JSON report")
	}
	if got := t3RawString(rep.Raw, "schema_version"); got != "1.0" {
		t.Errorf("schema_version = %q (raw=%#v), want %q", got, rep.Raw["schema_version"], "1.0")
	}
	genAt := t3RawString(rep.Raw, "generated_at")
	if genAt == "" {
		t.Errorf("generated_at missing or empty")
	} else if _, err := time.Parse(time.RFC3339Nano, genAt); err != nil {
		t.Errorf("generated_at %q invalid RFC3339 timestamp: %v", genAt, err)
	}
	if rep.Mode != wantMode || t3RawString(rep.Raw, "mode") != wantMode {
		t.Errorf("report mode = (%q, raw=%q), want %q", rep.Mode, t3RawString(rep.Raw, "mode"), wantMode)
	}
	if rep.TimedOut != wantTimedOut {
		t.Errorf("report timed_out = %v, want %v", rep.TimedOut, wantTimedOut)
	}
	if wantTimedOut {
		if b, ok := t3RawBool(rep.Raw, "timed_out"); !ok || !b {
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

func t3FindTableByHeaders(t *testing.T, md *ParsedMarkdownReport, wantHeaders ...string) *MarkdownTable {
	t.Helper()
	if md == nil {
		t.Fatalf("t3FindTableByHeaders: nil Markdown report")
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

func t3FindTableRow(t *testing.T, tbl *MarkdownTable, match map[string]string) map[string]string {
	t.Helper()
	if tbl == nil {
		t.Fatalf("t3FindTableRow: nil table")
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

func t3WriteLaunchSentinelWrapper(t *testing.T, sentinelPath string, proxyArgs ...string) (wrapperPath string, marker string) {
	t.Helper()
	if proxyBinaryPath == "" {
		t.Fatalf("t3WriteLaunchSentinelWrapper: proxyBinaryPath not initialized")
	}
	marker = newRunMarker(t)
	dir := t.TempDir()
	wrapperPath = filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))

	var b strings.Builder
	b.WriteString("#!/bin/sh\nprintf '%s\\n' \"$$\" >> ")
	b.WriteString(shellQuote(sentinelPath))
	b.WriteString("\nexec ")
	b.WriteString(shellQuote(proxyBinaryPath))
	b.WriteString(" ")
	b.WriteString(shellQuote("--marker=" + marker))
	for _, arg := range proxyArgs {
		b.WriteString(" ")
		b.WriteString(shellQuote(arg))
	}
	b.WriteString(" \"$@\"\n")

	writeExecutableFile(t, wrapperPath, []byte(b.String()))
	return wrapperPath, marker
}

func t3WrapIgnoreTermProxy(t *testing.T, pidsFile string, proxyArgs ...string) (wrapperPath string, marker string) {
	t.Helper()
	if proxyBinaryPath == "" {
		t.Fatalf("t3WrapIgnoreTermProxy: proxyBinaryPath not initialized")
	}
	marker = newRunMarker(t)
	dir := t.TempDir()
	wrapperPath = filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))

	var b strings.Builder
	b.WriteString("#!/bin/sh\ntrap '' TERM\n")
	b.WriteString(shellQuote(proxyBinaryPath))
	b.WriteString(" ")
	b.WriteString(shellQuote("--marker=" + marker))
	for _, arg := range proxyArgs {
		b.WriteString(" ")
		b.WriteString(shellQuote(arg))
	}
	b.WriteString(" \"$@\" &\nproxy_pid=$!\nprintf '%s %s\\n' \"$$\" \"$proxy_pid\" > ")
	b.WriteString(shellQuote(pidsFile))
	b.WriteString("\nwait \"$proxy_pid\"\n")

	writeExecutableFile(t, wrapperPath, []byte(b.String()))
	return wrapperPath, marker
}

func t3WaitPIDDead(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(750 * time.Millisecond)
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
			t.Errorf("pid %d still alive after shutdown cleanup", pid)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func t3AssertCategoriesCovered(t *testing.T, scope string, scenarios []ParsedScenarioResult, wantCategories []string) {
	t.Helper()
	seen := make(map[string]bool, len(scenarios))
	for _, sc := range scenarios {
		if sc.Category != "" {
			seen[sc.Category] = true
		}
	}
	for _, cat := range wantCategories {
		if !seen[cat] {
			t.Errorf("%s: missing scenario category %q (seen=%v)", scope, cat, seen)
		}
	}
}

func t3RunSingleWithReports(t *testing.T, timeout time.Duration, wantExit int, proxyArgs []string, extraCLIArgs ...string) (CLIResult, *ParsedReport, *ParsedMarkdownReport, string) {
	t.Helper()
	wrapper, marker := writeProxyWrapper(t, proxyArgs...)
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "report.json")
	mdPath := filepath.Join(dir, "report.md")

	args := make([]string, 0, 5+len(extraCLIArgs))
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

// --- Tier 3 Pairwise Combination Tests (14 Tests Covering 21/21 Pairs) ---

func TestT3_01_Filter_MonitoringOn_Reports(t *testing.T) {
	t.Parallel()

	t.Run("ConformantFiltered", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 0,
			[]string{"--mode=conformant-go", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|TRAC-01|TRAC-01b|TRAC-03|TRAC-04|EOD-01)$",
			"--scenario-timeout=15s",
			"--suite-timeout=45s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 7, Pass: 7}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || !*rep.Capabilities.SupportsMonitoring {
			t.Errorf("Capabilities.SupportsMonitoring unexpected: %+v", rep.Capabilities)
		}
		if rawTargets, ok := rep.Raw["targets"].([]any); !ok || len(rawTargets) != 1 {
			t.Errorf("rep.Raw[targets] unexpected: %#v", rep.Raw["targets"])
		}
		defTarget := t3MustTarget(t, rep, "default")
		if defTarget.Capabilities.SupportsMonitoring == nil || !*defTarget.Capabilities.SupportsMonitoring {
			t.Errorf("default target SupportsMonitoring unexpected: %+v", defTarget.Capabilities)
		}

		wantIDs := []string{"PUB-01", "PORD-02", "TRAC-01", "TRAC-01b", "TRAC-03", "TRAC-04", "EOD-01"}
		for _, id := range wantIDs {
			sc := t3MustScenario(t, rep, id)
			if sc.Status != StatusPass || sc.TraceStatus != StatusPass ||
				t3RawString(sc.Raw, "status") != StatusPass || t3RawString(sc.Raw, "trace_status") != StatusPass {
				t.Errorf("%s expected PASS + trace_status=PASS, got status=%q trace=%q raw=%v", id, sc.Status, sc.TraceStatus, sc.Raw)
			}
		}
		for _, excluded := range []string{"PULL-01", "HDG-01", "DLQ-01"} {
			if sc, found := rep.FindScenario(excluded); found {
				t.Errorf("excluded scenario %s unexpectedly present: %+v", excluded, *sc)
			}
		}

		capTbl := t3FindTableByHeaders(t, md, "Target", "Language", "Library", "Version", "Supports Monitoring")
		t3FindTableRow(t, capTbl, map[string]string{"Target": "default", "Language": "go", "Supports Monitoring": "true"})
		sumTbl := t3FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Total": "7", "Pass": "7", "Fail": "0", "Skipped No Monitoring": "0", "Unsupported By Client": "0"})
		scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		if len(scenTbl.Rows) != 7 {
			t.Errorf("Scenario Results row count = %d, want 7", len(scenTbl.Rows))
		}
		for _, id := range wantIDs {
			t3FindTableRow(t, scenTbl, map[string]string{"Scenario": id, "Status": StatusPass})
		}
	})

	t.Run("InvalidTracesFiltered", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=invalid-traces", "--fault=bad-span-kind,missing-links", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|TRAC-01|TRAC-03|TRAC-04|CFG-01)$",
			"--scenario-timeout=15s",
			"--suite-timeout=45s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 6, Pass: 2, Fail: 4}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, id := range []string{"PUB-01", "PORD-02"} {
			sc := t3MustScenario(t, rep, id)
			if sc.Status != StatusPass || sc.TraceStatus != StatusPass {
				t.Errorf("%s expected PASS/PASS, got status=%q trace=%q", id, sc.Status, sc.TraceStatus)
			}
		}
		for _, id := range []string{"TRAC-01", "TRAC-03", "TRAC-04", "CFG-01"} {
			sc := t3MustScenario(t, rep, id)
			if sc.Status != StatusFail || sc.TraceStatus != StatusFail || t3RawString(sc.Raw, "trace_status") != StatusFail {
				t.Errorf("%s expected FAIL with trace_status=FAIL, got status=%q trace=%q raw=%v", id, sc.Status, sc.TraceStatus, sc.Raw)
			}
			if len(sc.Diagnostics) == 0 || sc.Raw["diagnostics"] == nil {
				t.Errorf("%s missing non-empty diagnostics: %+v raw=%v", id, sc.Diagnostics, sc.Raw["diagnostics"])
			}
			if !strings.Contains(sc.Error, "WRONG_SPAN_KIND") || !strings.Contains(sc.Error, "MISSING_LINK") {
				t.Errorf("%s error %q expected WRONG_SPAN_KIND and MISSING_LINK", id, sc.Error)
			}
		}

		sumTbl := t3FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Total": "6", "Pass": "2", "Fail": "4"})
		scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		for _, id := range []string{"PUB-01", "PORD-02"} {
			t3FindTableRow(t, scenTbl, map[string]string{"Scenario": id, "Status": StatusPass})
		}
		for _, id := range []string{"TRAC-01", "TRAC-03", "TRAC-04", "CFG-01"} {
			t3FindTableRow(t, scenTbl, map[string]string{"Scenario": id, "Status": StatusFail})
		}
		if len(md.DiagnosticsBullets) != 4 {
			t.Errorf("DiagnosticsBullets len = %d, want 4: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
		for _, id := range []string{"TRAC-01", "TRAC-03", "TRAC-04", "CFG-01"} {
			found := slices.ContainsFunc(md.DiagnosticsBullets, func(b string) bool {
				return strings.Contains(b, id)
			})
			if !found {
				t.Errorf("DiagnosticsBullets missing %s: %v", id, md.DiagnosticsBullets)
			}
		}
	})

	t.Run("TraceDrainTimeoutUnderFilter", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=conformant-go", "--fault=hang-drain", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|TRAC-01)$",
			"--scenario-timeout=500ms",
			"--suite-timeout=20s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 3, Fail: 3, TimeoutCount: 3}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}

		pub01 := t3MustScenario(t, rep, "PUB-01")
		if pub01.Status != StatusFail || pub01.StatusDetail != StatusTimeout ||
			t3RawString(pub01.Raw, "status_detail") != StatusTimeout ||
			!strings.Contains(pub01.Error, "TIMEOUT:") || pub01.ProxyRestarted {
			t.Errorf("PUB-01 unexpected: %+v raw=%v", *pub01, pub01.Raw)
		}
		for _, id := range []string{"PORD-02", "TRAC-01"} {
			sc := t3MustScenario(t, rep, id)
			if sc.Status != StatusFail || sc.StatusDetail != StatusTimeout ||
				t3RawString(sc.Raw, "status_detail") != StatusTimeout ||
				!sc.ProxyRestarted || sc.Raw["proxy_restarted"] != true {
				t.Errorf("%s expected FAIL/TIMEOUT with ProxyRestarted=true, got %+v raw=%v", id, *sc, sc.Raw)
			}
		}

		if !md.HasFailureBreakdown || md.TimeoutCount != 3 || md.ErrorCount != 0 {
			t.Errorf("MD failure breakdown unexpected: has=%v timeout=%d err=%d raw=%q",
				md.HasFailureBreakdown, md.TimeoutCount, md.ErrorCount, md.FailureBreakdown)
		}
		scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		for _, id := range []string{"PUB-01", "PORD-02", "TRAC-01"} {
			t3FindTableRow(t, scenTbl, map[string]string{"Scenario": id, "Status": "FAIL (TIMEOUT)"})
		}
	})

	t.Run("WrongRPCWithMonitoringOn", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=wrong-rpc", "--fast-timings"},
			"--filter=^(PUB-01|TRAC-03)$",
			"--scenario-timeout=15s",
			"--suite-timeout=45s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 2, Fail: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, id := range []string{"PUB-01", "TRAC-03"} {
			sc := t3MustScenario(t, rep, id)
			if sc.Status != StatusFail || !strings.Contains(sc.Error, "message[0].data mismatch") {
				t.Errorf("%s expected RPC data mismatch failure, got %+v", id, *sc)
			}
		}
		sumTbl := t3FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Total": "2", "Pass": "0", "Fail": "2"})
		if len(md.DiagnosticsBullets) != 2 {
			t.Errorf("DiagnosticsBullets len = %d, want 2: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
	})
}

func TestT3_02_Filter_MonitoringOff_StrictUnsupported(t *testing.T) {
	t.Parallel()

	t.Run("PermissiveVsStrictFiltered", func(t *testing.T) {
		w, marker := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")
		dir := t.TempDir()
		filter := "^(PUB-01|PULL-04|TRAC-01|TRAC-03|PFC-01|CMP-01|CFG-01)$"

		permJSON := filepath.Join(dir, "perm.json")
		permMD := filepath.Join(dir, "perm.md")
		permRes := runCLI(
			t, 45*time.Second,
			"--proxy-cmd="+w,
			"--filter="+filter,
			"--scenario-timeout=10s",
			"--suite-timeout=30s",
			"--report-json="+permJSON,
			"--report-md="+permMD,
		)
		if permRes.ExitCode != 0 || permRes.TimedOut || permRes.Err != nil {
			t.Fatalf("permissive run failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				permRes.ExitCode, permRes.TimedOut, permRes.Err, permRes.Stderr)
		}
		assertNoOrphanProxy(t, marker)

		strictJSON := filepath.Join(dir, "strict.json")
		strictMD := filepath.Join(dir, "strict.md")
		strictRes := runCLI(
			t, 45*time.Second,
			"--proxy-cmd="+w,
			"--filter="+filter,
			"--scenario-timeout=10s",
			"--suite-timeout=30s",
			"--strict-unsupported",
			"--report-json="+strictJSON,
			"--report-md="+strictMD,
		)
		if strictRes.ExitCode != 1 || strictRes.TimedOut {
			t.Fatalf("strict run expected exit=1 timedOut=false, got exit=%d timedOut=%v\nstderr:\n%s",
				strictRes.ExitCode, strictRes.TimedOut, strictRes.Stderr)
		}
		assertNoOrphanProxy(t, marker)

		wantSum := ParsedSummary{
			Total:               7,
			Pass:                3,
			Fail:                0,
			SkippedNoMonitoring: 1,
			UnsupportedByClient: 3,
		}
		for _, item := range []struct {
			name string
			j    string
			m    string
		}{
			{name: "permissive", j: permJSON, m: permMD},
			{name: "strict", j: strictJSON, m: strictMD},
		} {
			rep := ReadJSONReport(t, item.j)
			t3AssertJSONV1Base(t, rep, "single", false)
			if rep.Summary == nil || *rep.Summary != wantSum {
				t.Fatalf("%s Summary = %+v, want %+v", item.name, rep.Summary, wantSum)
			}
			if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || *rep.Capabilities.SupportsMonitoring {
				t.Errorf("%s expected SupportsMonitoring=false, got %+v", item.name, rep.Capabilities)
			}
			for _, passID := range []string{"PUB-01", "TRAC-03", "CFG-01"} {
				sc := t3MustScenario(t, rep, passID)
				if sc.Status != StatusPass || sc.TraceStatus != StatusSkippedNoMonitoring ||
					t3RawString(sc.Raw, "trace_status") != StatusSkippedNoMonitoring {
					t.Errorf("%s %s expected PASS + trace_status=SKIPPED_NO_MONITORING, got %+v raw=%v",
						item.name, passID, *sc, sc.Raw)
				}
			}
			trac01 := t3MustScenario(t, rep, "TRAC-01")
			if trac01.Status != StatusSkippedNoMonitoring || trac01.TraceStatus != StatusSkippedNoMonitoring ||
				trac01.Reason == "" || t3RawString(trac01.Raw, "trace_status") != StatusSkippedNoMonitoring {
				t.Errorf("%s TRAC-01 unexpected: %+v raw=%v", item.name, *trac01, trac01.Raw)
			}
			for _, unimplID := range []string{"PULL-04", "PFC-01", "CMP-01"} {
				sc := t3MustScenario(t, rep, unimplID)
				if sc.Status != StatusUnsupportedByClient || !strings.Contains(sc.Reason, "missing required capabilities") {
					t.Errorf("%s %s expected UNSUPPORTED_BY_CLIENT with reason, got %+v", item.name, unimplID, *sc)
				}
			}
			if _, found := rep.FindScenario("EOD-02"); found {
				t.Errorf("%s excluded scenario EOD-02 unexpectedly present", item.name)
			}

			md := ReadMarkdownReport(t, item.m)
			capTbl := t3FindTableByHeaders(t, md, "Target", "Language", "Library", "Version", "Supports Monitoring")
			t3FindTableRow(t, capTbl, map[string]string{"Target": "default", "Language": "rust", "Supports Monitoring": "false"})
			sumTbl := t3FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
			t3FindTableRow(t, sumTbl, map[string]string{
				"Total":                 "7",
				"Pass":                  "3",
				"Fail":                  "0",
				"Skipped No Monitoring": "1",
				"Unsupported By Client": "3",
			})
			scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
			t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "TRAC-01", "Status": StatusSkippedNoMonitoring})
			t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "TRAC-03", "Status": StatusPass})
			t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "CFG-01", "Status": StatusPass})
			for _, unimplID := range []string{"PULL-04", "PFC-01", "CMP-01"} {
				t3FindTableRow(t, scenTbl, map[string]string{"Scenario": unimplID, "Status": StatusUnsupportedByClient})
			}
		}
	})

	t.Run("WrongPayloadWithMonitoringOffAndFilter", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=conformant-rust-nomon", "--fault=wrong-payload", "--fast-timings"},
			"--filter=^(PUB-01|PULL-04|TRAC-01|TRAC-03)$",
			"--scenario-timeout=10s",
			"--suite-timeout=30s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{
			Total:               4,
			Pass:                0,
			Fail:                2,
			SkippedNoMonitoring: 1,
			UnsupportedByClient: 1,
		}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, failID := range []string{"PUB-01", "TRAC-03"} {
			sc := t3MustScenario(t, rep, failID)
			if sc.Status != StatusFail || !strings.Contains(sc.Error, "message[0].data mismatch") {
				t.Errorf("%s expected FAIL with data mismatch, got %+v", failID, *sc)
			}
		}
		if sc := t3MustScenario(t, rep, "TRAC-01"); sc.Status != StatusSkippedNoMonitoring || sc.TraceStatus != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-01 expected SKIPPED_NO_MONITORING, got %+v", *sc)
		}
		if sc := t3MustScenario(t, rep, "PULL-04"); sc.Status != StatusUnsupportedByClient {
			t.Errorf("PULL-04 expected UNSUPPORTED_BY_CLIENT, got %+v", *sc)
		}

		sumTbl := t3FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{
			"Total":                 "4",
			"Pass":                  "0",
			"Fail":                  "2",
			"Skipped No Monitoring": "1",
			"Unsupported By Client": "1",
		})
		if len(md.DiagnosticsBullets) != 2 {
			t.Errorf("DiagnosticsBullets len = %d, want 2: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
	})
}

func TestT3_03_ScenarioTimeout_HungProxy_CleanReportAndNoOrphans(t *testing.T) {
	t.Parallel()

	res, rep, md, _ := t3RunSingleWithReports(
		t, 45*time.Second, 1,
		[]string{"--mode=conformant-go", "--fault=hang-publish,ignore-shutdown", "--fast-timings"},
		"--filter=^(PUB-01|PULL-01|EOD-02)$",
		"--scenario-timeout=600ms",
		"--suite-timeout=25s",
	)
	if res.Duration >= 20*time.Second {
		t.Errorf("elapsed %v too slow for 600ms scenario timeout", res.Duration)
	}
	t3AssertJSONV1Base(t, rep, "single", false)

	wantSum := ParsedSummary{Total: 3, Pass: 2, Fail: 1, TimeoutCount: 1, ErrorCount: 0}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	defTarget := t3MustTarget(t, rep, "default")
	if defTarget.Summary == nil || *defTarget.Summary != wantSum {
		t.Errorf("Targets[default].Summary = %+v, want %+v", defTarget.Summary, wantSum)
	}

	pub01 := t3MustScenario(t, rep, "PUB-01")
	if pub01.Status != StatusFail || pub01.StatusDetail != StatusTimeout ||
		t3RawString(pub01.Raw, "status_detail") != StatusTimeout ||
		!strings.Contains(pub01.Error, "TIMEOUT: scenario exceeded timeout (600ms)") ||
		pub01.ProxyRestarted {
		t.Errorf("PUB-01 unexpected: %+v raw=%v", *pub01, pub01.Raw)
	}
	pull01 := t3MustScenario(t, rep, "PULL-01")
	if pull01.Status != StatusPass || !pull01.ProxyRestarted || pull01.Raw["proxy_restarted"] != true {
		t.Errorf("PULL-01 expected PASS with ProxyRestarted=true, got %+v raw=%v", *pull01, pull01.Raw)
	}
	eod02 := t3MustScenario(t, rep, "EOD-02")
	if eod02.Status != StatusPass || eod02.ProxyRestarted {
		t.Errorf("EOD-02 expected PASS with ProxyRestarted=false, got %+v", *eod02)
	}

	if !md.HasFailureBreakdown || md.TimeoutCount != 1 || md.ErrorCount != 0 {
		t.Errorf("MD failure breakdown unexpected: has=%v timeout=%d err=%d raw=%q",
			md.HasFailureBreakdown, md.TimeoutCount, md.ErrorCount, md.FailureBreakdown)
	}
	scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
	pubRow := t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "PUB-01", "Status": "FAIL (TIMEOUT)"})
	if !strings.Contains(pubRow["Details"], "TIMEOUT: scenario exceeded timeout (600ms)") {
		t.Errorf("MD PUB-01 Details = %q, missing timeout message", pubRow["Details"])
	}
	t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "PULL-01", "Status": StatusPass})
	t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "EOD-02", "Status": StatusPass})
	if len(md.DiagnosticsBullets) != 1 || !strings.Contains(md.DiagnosticsBullets[0], "PUB-01") {
		t.Errorf("DiagnosticsBullets unexpected: %v", md.DiagnosticsBullets)
	}
}

func TestT3_04_SuiteTimeout_MultiTarget_NoOrphans(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	pidsFile := filepath.Join(dir, "t2_pids.txt")
	t3Sentinel := filepath.Join(dir, "t3_launched.log")
	jsonPath := filepath.Join(dir, "compare.json")
	mdPath := filepath.Join(dir, "compare.md")

	w1, m1 := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	w2, m2 := t3WrapIgnoreTermProxy(
		t, pidsFile,
		"--mode=conformant-cpp-partial",
		"--fault=hang-publish,ignore-shutdown,ignore-sigterm",
		"--fast-timings",
	)
	w3, m3 := t3WriteLaunchSentinelWrapper(t, t3Sentinel, "--mode=conformant-rust-nomon", "--fast-timings")

	res := runCLI(
		t, 45*time.Second,
		"--proxy-target=t1="+w1,
		"--proxy-target=t2="+w2,
		"--proxy-target=t3="+w3,
		"--filter=^(PUB-01|PORD-02)$",
		"--scenario-timeout=10s",
		"--suite-timeout=2500ms",
		"--report-json="+jsonPath,
		"--report-md="+mdPath,
	)
	if res.ExitCode != 1 || res.TimedOut {
		t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v err=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "Target t2 error: suite timed out during scenario PUB-01") {
		t.Errorf("stderr missing t2 timeout line:\n%s", res.Stderr)
	}
	if !strings.Contains(res.Stderr, "Target t3 error: suite timed out before starting target") {
		t.Errorf("stderr missing t3 pre-start timeout line:\n%s", res.Stderr)
	}

	t3AssertFileNotExist(t, t3Sentinel)
	assertNoOrphanProxy(t, m1)
	assertNoOrphanProxy(t, m2)
	assertNoOrphanProxy(t, m3)

	pidsRaw, err := os.ReadFile(pidsFile)
	if err != nil {
		t.Fatalf("read t2 pids file %q: %v", pidsFile, err)
	}
	pidFields := strings.Fields(string(pidsRaw))
	if len(pidFields) != 2 {
		t.Fatalf("expected 2 PIDs (sh + proxy) in %q, got %q", pidsFile, string(pidsRaw))
	}
	for _, f := range pidFields {
		pid, pErr := strconv.Atoi(f)
		if pErr != nil || pid <= 0 {
			t.Fatalf("invalid PID %q in %q", f, string(pidsRaw))
		}
		t3WaitPIDDead(t, pid)
	}

	rep := ReadJSONReport(t, jsonPath)
	t3AssertJSONV1Base(t, rep, "compare", true)

	t1 := t3MustTarget(t, rep, "t1")
	if t1.TimedOut || t1.TargetError != "" || t1.Summary == nil || *t1.Summary != (ParsedSummary{Total: 2, Pass: 2}) {
		t.Errorf("t1 unexpected: %+v", t1)
	}
	if sc := t3MustTargetScenario(t, t1, "PUB-01"); sc.Status != StatusPass {
		t.Errorf("t1/PUB-01 status = %q, want PASS", sc.Status)
	}
	if sc := t3MustTargetScenario(t, t1, "PORD-02"); sc.Status != StatusPass {
		t.Errorf("t1/PORD-02 status = %q, want PASS", sc.Status)
	}

	t2 := t3MustTarget(t, rep, "t2")
	if !t2.TimedOut || t2.TargetError != "suite timed out during scenario PUB-01" ||
		t3RawString(t2.Raw, "target_error") != "suite timed out during scenario PUB-01" ||
		t2.Summary == nil || *t2.Summary != (ParsedSummary{Total: 1, Fail: 1, TimeoutCount: 1}) {
		t.Errorf("t2 unexpected: %+v raw=%v", t2, t2.Raw)
	}
	t2Pub01 := t3MustTargetScenario(t, t2, "PUB-01")
	if t2Pub01.Status != StatusFail || t2Pub01.StatusDetail != StatusTimeout ||
		!strings.Contains(t2Pub01.Error, "TIMEOUT: suite deadline exceeded after") {
		t.Errorf("t2/PUB-01 unexpected: %+v", *t2Pub01)
	}

	t3 := t3MustTarget(t, rep, "t3")
	if !t3.TimedOut || !strings.Contains(t3.TargetError, "suite timed out before starting target") ||
		!strings.Contains(t3RawString(t3.Raw, "target_error"), "suite timed out before starting target") ||
		t3.Summary == nil || *t3.Summary != (ParsedSummary{}) || len(t3.Scenarios) != 0 || t3.Raw["capabilities"] != nil {
		t.Errorf("t3 unexpected: %+v rawCaps=%v", t3, t3.Raw["capabilities"])
	}

	dErr := t3MustDiff(t, rep, "target_error", "")
	if dErr.Verdicts["t1"] != "OK" || dErr.Verdicts["t2"] != StatusError || dErr.Verdicts["t3"] != StatusError ||
		dErr.Details["t2"] == "" || dErr.Details["t3"] == "" || t3RawString(dErr.Raw, "kind") != "target_error" {
		t.Errorf("target_error diff unexpected: %+v raw=%v", *dErr, dErr.Raw)
	}
	if rawV, ok := dErr.Raw["verdicts"].(map[string]any); !ok || t3RawString(rawV, "t1") != "OK" || t3RawString(rawV, "t2") != StatusError || t3RawString(rawV, "t3") != StatusError {
		t.Errorf("raw target_error verdicts unexpected: %#v", dErr.Raw["verdicts"])
	}
	dPub := t3MustDiff(t, rep, "scenario_verdict", "PUB-01")
	if dPub.Verdicts["t1"] != StatusPass || dPub.Verdicts["t2"] != StatusFail || dPub.Verdicts["t3"] != "MISSING" {
		t.Errorf("PUB-01 diff unexpected: %+v", *dPub)
	}
	dPord := t3MustDiff(t, rep, "scenario_verdict", "PORD-02")
	if dPord.Verdicts["t1"] != StatusPass || dPord.Verdicts["t2"] != "MISSING" || dPord.Verdicts["t3"] != "MISSING" {
		t.Errorf("PORD-02 diff unexpected: %+v", *dPord)
	}

	md := ReadMarkdownReport(t, mdPath)
	sumTbl := t3FindTableByHeaders(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
	t3FindTableRow(t, sumTbl, map[string]string{"Target": "t1", "Total": "2", "Pass": "2", "Fail": "0"})
	t3FindTableRow(t, sumTbl, map[string]string{"Target": "t2", "Total": "1", "Pass": "0", "Fail": "1"})
	t3FindTableRow(t, sumTbl, map[string]string{"Target": "t3", "Monitoring": "unknown", "Total": "0", "Pass": "0", "Fail": "0"})

	matTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "t1", "t2", "t3")
	t3FindTableRow(t, matTbl, map[string]string{"Scenario": "PUB-01", "t1": StatusPass, "t2": "FAIL (TIMEOUT)", "t3": "MISSING"})
	t3FindTableRow(t, matTbl, map[string]string{"Scenario": "PORD-02", "t1": StatusPass, "t2": "MISSING", "t3": "MISSING"})

	diffTbl := t3FindTableByHeaders(t, md, "Kind", "Scenario / Field", "t1", "t2", "t3", "Description")
	t3FindTableRow(t, diffTbl, map[string]string{"Kind": "target_error", "t1": "OK", "t2": StatusError, "t3": StatusError})
}

func TestT3_05_WrongRPC_MonitoringDisabled_StillFailsWithReport(t *testing.T) {
	t.Parallel()

	t.Run("WrongRPC_MonitoringOff", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=wrong-rpc", "--monitoring=false", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|TRAC-01|TRAC-03|PULL-01)$",
			"--scenario-timeout=15s",
			"--suite-timeout=30s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || *rep.Capabilities.SupportsMonitoring {
			t.Errorf("expected SupportsMonitoring=false, got %+v", rep.Capabilities)
		}
		wantSum := ParsedSummary{Total: 5, Pass: 1, Fail: 3, SkippedNoMonitoring: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		trac01 := t3MustScenario(t, rep, "TRAC-01")
		if trac01.Status != StatusSkippedNoMonitoring || trac01.TraceStatus != StatusSkippedNoMonitoring ||
			t3RawString(trac01.Raw, "trace_status") != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-01 expected SKIPPED_NO_MONITORING, got %+v raw=%v", *trac01, trac01.Raw)
		}
		pull01 := t3MustScenario(t, rep, "PULL-01")
		if pull01.Status != StatusPass || pull01.TraceStatus != StatusSkippedNoMonitoring {
			t.Errorf("PULL-01 expected PASS + SKIPPED_NO_MONITORING trace, got %+v", *pull01)
		}
		for _, failID := range []string{"PUB-01", "PORD-02", "TRAC-03"} {
			sc := t3MustScenario(t, rep, failID)
			if sc.Status != StatusFail || sc.Error == "" {
				t.Errorf("%s expected FAIL with non-empty Error, got %+v", failID, *sc)
			}
			if !strings.Contains(sc.Error, "mismatch") && !strings.Contains(sc.Error, "verification failure") {
				t.Errorf("%s error %q expected RPC verification/mismatch message", failID, sc.Error)
			}
		}

		sumTbl := t3FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{
			"Total":                 "5",
			"Pass":                  "1",
			"Fail":                  "3",
			"Skipped No Monitoring": "1",
			"Unsupported By Client": "0",
		})
		scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "PULL-01", "Status": StatusPass})
		t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "TRAC-01", "Status": StatusSkippedNoMonitoring})
		for _, failID := range []string{"PUB-01", "PORD-02", "TRAC-03"} {
			t3FindTableRow(t, scenTbl, map[string]string{"Scenario": failID, "Status": StatusFail})
		}
		if len(md.DiagnosticsBullets) != 3 {
			t.Errorf("DiagnosticsBullets len = %d, want 3: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
	})

	t.Run("MissingRPC_MonitoringOff", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=missing-rpc", "--monitoring=false", "--fast-timings"},
			"--filter=^(PUB-01|PULL-04|TRAC-01|TRAC-03)$",
			"--scenario-timeout=15s",
			"--suite-timeout=30s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 4, Pass: 0, Fail: 3, SkippedNoMonitoring: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t3MustScenario(t, rep, "TRAC-01"); sc.Status != StatusSkippedNoMonitoring || sc.TraceStatus != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-01 expected SKIPPED_NO_MONITORING, got %+v", *sc)
		}
		for _, failID := range []string{"PUB-01", "PULL-04", "TRAC-03"} {
			sc := t3MustScenario(t, rep, failID)
			if sc.Status != StatusFail || sc.Error == "" {
				t.Errorf("%s expected FAIL with error, got %+v", failID, *sc)
			}
		}
		if len(md.DiagnosticsBullets) != 3 {
			t.Errorf("DiagnosticsBullets len = %d, want 3: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
	})
}

func TestT3_06_MissingTraces_ValidRPC_FailsWithDiagnostics(t *testing.T) {
	t.Parallel()

	t.Run("MissingTraces_EmptyWireAndSpans", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=missing-traces", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|TRAC-01|TRAC-01b|TRAC-03|TRAC-04|EOD-01)$",
			"--scenario-timeout=15s",
			"--suite-timeout=45s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 7, Pass: 4, Fail: 3}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, passID := range []string{"PUB-01", "PORD-02", "TRAC-01b", "EOD-01"} {
			sc := t3MustScenario(t, rep, passID)
			if sc.Status != StatusPass {
				t.Errorf("%s expected PASS, got %+v", passID, *sc)
			}
		}
		for _, tpFailID := range []string{"TRAC-01", "TRAC-03"} {
			sc := t3MustScenario(t, rep, tpFailID)
			if sc.Status != StatusFail || sc.TraceStatus != "" || sc.Raw["trace_status"] != nil ||
				!strings.Contains(sc.Error, `missing "googclient_traceparent"`) {
				t.Errorf("%s expected wire traceparent failure without trace_status, got %+v raw=%v",
					tpFailID, *sc, sc.Raw)
			}
		}
		trac04 := t3MustScenario(t, rep, "TRAC-04")
		if trac04.Status != StatusFail || trac04.TraceStatus != "" || trac04.Raw["trace_status"] != nil ||
			!strings.Contains(trac04.Error, "traceparent is empty") {
			t.Errorf("TRAC-04 expected wire traceparent empty error, got %+v raw=%v", *trac04, trac04.Raw)
		}

		sumTbl := t3FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Total": "7", "Pass": "4", "Fail": "3"})
		if len(md.DiagnosticsBullets) != 3 {
			t.Errorf("DiagnosticsBullets len = %d, want 3: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
	})

	t.Run("InvalidTraces_SemanticSpanViolations", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=invalid-traces", "--fault=bad-span-kind,missing-links", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|TRAC-01|TRAC-03|TRAC-04)$",
			"--scenario-timeout=15s",
			"--suite-timeout=45s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 5, Pass: 2, Fail: 3}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, passID := range []string{"PUB-01", "PORD-02"} {
			if sc := t3MustScenario(t, rep, passID); sc.Status != StatusPass || sc.TraceStatus != StatusPass {
				t.Errorf("%s expected PASS/PASS, got %+v", passID, *sc)
			}
		}
		for _, failID := range []string{"TRAC-01", "TRAC-03", "TRAC-04"} {
			sc := t3MustScenario(t, rep, failID)
			if sc.Status != StatusFail || sc.TraceStatus != StatusFail ||
				t3RawString(sc.Raw, "trace_status") != StatusFail ||
				len(sc.Diagnostics) == 0 || sc.Raw["diagnostics"] == nil ||
				!strings.Contains(sc.Error, "Status=FAIL SupportsMonitoring=true") ||
				!strings.Contains(sc.Error, "WRONG_SPAN_KIND") ||
				!strings.Contains(sc.Error, "MISSING_LINK") {
				t.Errorf("%s expected trace semantic failure with diagnostics, got %+v raw=%v",
					failID, *sc, sc.Raw)
			}
		}

		scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		for _, failID := range []string{"TRAC-01", "TRAC-03", "TRAC-04"} {
			row := t3FindTableRow(t, scenTbl, map[string]string{"Scenario": failID, "Status": StatusFail})
			if !strings.Contains(row["Details"], "WRONG_SPAN_KIND") {
				t.Errorf("%s Markdown Details = %q, missing WRONG_SPAN_KIND", failID, row["Details"])
			}
		}
		if len(md.DiagnosticsBullets) != 3 {
			t.Errorf("DiagnosticsBullets len = %d, want 3: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
	})
}

func TestT3_07_MultiTarget_MixedMonitoring_CompareDiff(t *testing.T) {
	t.Parallel()

	t.Run("Full65Scenarios_GoCppRust_BehavioralMatrix", func(t *testing.T) {
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "full65.json")
		mdPath := filepath.Join(dir, "full65.md")

		wGo, mGo := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		wCpp, mCpp := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
		wRust, mRust := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

		res := runCLI(
			t, 210*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--suite-timeout=180s",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("full 65x3 CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, mGo)
		assertNoOrphanProxy(t, mCpp)
		assertNoOrphanProxy(t, mRust)

		rep := ReadJSONReport(t, jsonPath)
		t3AssertJSONV1Base(t, rep, "compare", false)

		trGo := t3MustTarget(t, rep, "go")
		trCpp := t3MustTarget(t, rep, "cpp")
		trRust := t3MustTarget(t, rep, "rust")

		if trGo.Capabilities.SupportsMonitoring == nil || !*trGo.Capabilities.SupportsMonitoring {
			t.Errorf("go SupportsMonitoring unexpected: %+v", trGo.Capabilities)
		}
		if trCpp.Capabilities.SupportsMonitoring == nil || !*trCpp.Capabilities.SupportsMonitoring {
			t.Errorf("cpp SupportsMonitoring unexpected: %+v", trCpp.Capabilities)
		}
		if trRust.Capabilities.SupportsMonitoring == nil || *trRust.Capabilities.SupportsMonitoring {
			t.Errorf("rust SupportsMonitoring unexpected: %+v", trRust.Capabilities)
		}

		wantGoSum := ParsedSummary{Total: 65, Pass: 65}
		wantCppSum := ParsedSummary{Total: 65, Pass: 56, UnsupportedByClient: 9}
		wantRustSum := ParsedSummary{Total: 65, Pass: 48, SkippedNoMonitoring: 4, UnsupportedByClient: 13}
		if trGo.Summary == nil || *trGo.Summary != wantGoSum {
			t.Errorf("go Summary = %+v, want %+v", trGo.Summary, wantGoSum)
		}
		if trCpp.Summary == nil || *trCpp.Summary != wantCppSum {
			t.Errorf("cpp Summary = %+v, want %+v", trCpp.Summary, wantCppSum)
		}
		if trRust.Summary == nil || *trRust.Summary != wantRustSum {
			t.Errorf("rust Summary = %+v, want %+v", trRust.Summary, wantRustSum)
		}

		t3AssertCategoriesCovered(t, "go-65", trGo.Scenarios, t3All13Categories)
		for _, sc := range trGo.Scenarios {
			if len(sc.Deviations) != 0 {
				t.Errorf("go/%s unexpected deviations: %+v", sc.ID, sc.Deviations)
			}
		}

		for _, id := range []string{"TRAC-01", "TRAC-03", "TRAC-04", "CFG-01"} {
			sc := t3MustTargetScenario(t, trCpp, id)
			if sc.Status != StatusPass || len(sc.Deviations) == 0 {
				t.Errorf("cpp/%s expected PASS with C++ trace deviations, got status=%q devs=%d",
					id, sc.Status, len(sc.Deviations))
			}
		}

		rustSkippedTraceCount := 0
		for _, sc := range trRust.Scenarios {
			if sc.TraceStatus == StatusSkippedNoMonitoring {
				rustSkippedTraceCount++
			}
		}
		if rustSkippedTraceCount != 52 {
			t.Errorf("rust trace_status=SKIPPED_NO_MONITORING count = %d, want 52", rustSkippedTraceCount)
		}

		dMon := t3MustDiff(t, rep, "capability", "supports_monitoring")
		if dMon.Verdicts["go"] != "true" || dMon.Verdicts["cpp"] != "true" || dMon.Verdicts["rust"] != "false" {
			t.Errorf("supports_monitoring diff unexpected: %+v", *dMon)
		}
		dDef := t3MustDiff(t, rep, "client_default", "defaults.publish_batch_byte_threshold")
		if dDef.Verdicts["go"] == dDef.Verdicts["cpp"] {
			t.Errorf("publish_batch_byte_threshold diff should contrast Go vs C++: %+v", *dDef)
		}
		t3MustDiff(t, rep, "client_default", "defaults.publish_compression_algorithm")

		for _, id := range []string{"TRAC-01", "TRAC-01b", "TRAC-04"} {
			d := t3MustDiff(t, rep, "scenario_verdict", id)
			if d.Verdicts["go"] != StatusPass || d.Verdicts["cpp"] != StatusPass || d.Verdicts["rust"] != StatusSkippedNoMonitoring {
				t.Errorf("%s verdict diff unexpected: %+v", id, *d)
			}
		}
		dTrac02 := t3MustDiff(t, rep, "scenario_verdict", "TRAC-02")
		if dTrac02.Verdicts["go"] != StatusPass || dTrac02.Verdicts["cpp"] != StatusUnsupportedByClient || dTrac02.Verdicts["rust"] != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-02 verdict diff unexpected: %+v", *dTrac02)
		}
		dHdg01 := t3MustDiff(t, rep, "scenario_verdict", "HDG-01")
		if dHdg01.Verdicts["go"] != StatusPass || dHdg01.Verdicts["cpp"] != StatusUnsupportedByClient || dHdg01.Verdicts["rust"] != StatusPass {
			t.Errorf("HDG-01 verdict diff unexpected: %+v", *dHdg01)
		}
		for _, id := range []string{"PUB-01", "PORD-02", "TRAC-03", "EOD-01", "CFG-01"} {
			d := t3MustDiff(t, rep, "scenario_trace_delta", id)
			if d.Verdicts["go"] != StatusPass || d.Verdicts["cpp"] != StatusPass || d.Verdicts["rust"] != StatusSkippedNoMonitoring {
				t.Errorf("%s scenario_trace_delta unexpected: %+v", id, *d)
			}
		}

		md := ReadMarkdownReport(t, mdPath)
		sumTbl := t3FindTableByHeaders(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "go", "Monitoring": "true", "Total": "65", "Pass": "65", "Unsupported By Client": "0"})
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "cpp", "Monitoring": "true", "Total": "65", "Pass": "56", "Unsupported By Client": "9"})
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "rust", "Monitoring": "false", "Total": "65", "Pass": "48", "Skipped No Monitoring": "4", "Unsupported By Client": "13"})

		capTbl := t3FindTableByHeaders(t, md, "Field", "go", "cpp", "rust")
		t3FindTableRow(t, capTbl, map[string]string{"Field": "supports_monitoring", "go": "true", "cpp": "true", "rust": "false"})

		matTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "go", "cpp", "rust")
		if len(matTbl.Rows) != 65 {
			t.Errorf("Scenario Matrix row count = %d, want 65", len(matTbl.Rows))
		}
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-01", "go": StatusPass, "cpp": StatusPass, "rust": StatusSkippedNoMonitoring})
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-02", "go": StatusPass, "cpp": StatusUnsupportedByClient, "rust": StatusSkippedNoMonitoring})

		diffTbl := t3FindTableByHeaders(t, md, "Kind", "Scenario / Field", "go", "cpp", "rust", "Description")
		t3FindTableRow(t, diffTbl, map[string]string{"Kind": "capability", "Scenario / Field": "supports_monitoring"})
		t3FindTableRow(t, diffTbl, map[string]string{"Kind": "scenario_verdict", "Scenario / Field": "TRAC-01"})
		t3FindTableRow(t, diffTbl, map[string]string{"Kind": "scenario_trace_delta", "Scenario / Field": "CFG-01"})
	})

	t.Run("TraceFault_MixedMonitoring_MultiCategory", func(t *testing.T) {
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "mixed.json")
		mdPath := filepath.Join(dir, "mixed.md")

		wGoBad, mGoBad := writeProxyWrapper(t, "--mode=invalid-traces", "--fault=bad-span-kind", "--fast-timings")
		wCpp, mCpp := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
		wRust, mRust := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

		res := runCLI(
			t, 75*time.Second,
			"--proxy-target=go_bad="+wGoBad,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter=^(PUB-01|TRAC-01|TRAC-02|TRAC-03|TRAC-04|CFG-01|EOD-04)$",
			"--suite-timeout=60s",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Stderr)
		}
		assertNoOrphanProxy(t, mGoBad)
		assertNoOrphanProxy(t, mCpp)
		assertNoOrphanProxy(t, mRust)

		rep := ReadJSONReport(t, jsonPath)
		t3AssertJSONV1Base(t, rep, "compare", false)

		trGoBad := t3MustTarget(t, rep, "go_bad")
		trCpp := t3MustTarget(t, rep, "cpp")
		trRust := t3MustTarget(t, rep, "rust")

		if trGoBad.Summary == nil || *trGoBad.Summary != (ParsedSummary{Total: 7, Pass: 2, Fail: 5}) {
			t.Errorf("go_bad Summary = %+v, want 2P/5F", trGoBad.Summary)
		}
		for _, id := range []string{"PUB-01", "EOD-04"} {
			if sc := t3MustTargetScenario(t, trGoBad, id); sc.Status != StatusPass {
				t.Errorf("go_bad/%s expected PASS, got %+v", id, *sc)
			}
		}
		for _, id := range []string{"TRAC-01", "TRAC-02", "TRAC-03", "TRAC-04", "CFG-01"} {
			sc := t3MustTargetScenario(t, trGoBad, id)
			if sc.Status != StatusFail || sc.TraceStatus != StatusFail || !strings.Contains(sc.Error, "WRONG_SPAN_KIND") {
				t.Errorf("go_bad/%s expected FAIL/trace_status=FAIL with WRONG_SPAN_KIND, got %+v", id, *sc)
			}
		}

		for _, id := range []string{"TRAC-01", "TRAC-03", "TRAC-04", "CFG-01"} {
			sc := t3MustTargetScenario(t, trCpp, id)
			if sc.Status != StatusPass || len(sc.Deviations) == 0 {
				t.Errorf("cpp/%s expected PASS with deviations, got %+v", id, *sc)
			}
		}
		for _, id := range []string{"TRAC-02", "EOD-04"} {
			if sc := t3MustTargetScenario(t, trCpp, id); sc.Status != StatusUnsupportedByClient {
				t.Errorf("cpp/%s expected UNSUPPORTED_BY_CLIENT, got %+v", id, *sc)
			}
		}

		for _, id := range []string{"PUB-01", "TRAC-03", "CFG-01"} {
			sc := t3MustTargetScenario(t, trRust, id)
			if sc.Status != StatusPass || sc.TraceStatus != StatusSkippedNoMonitoring {
				t.Errorf("rust/%s expected PASS + SKIPPED_NO_MONITORING trace, got %+v", id, *sc)
			}
		}
		for _, id := range []string{"TRAC-01", "TRAC-02", "TRAC-04"} {
			if sc := t3MustTargetScenario(t, trRust, id); sc.Status != StatusSkippedNoMonitoring {
				t.Errorf("rust/%s expected SKIPPED_NO_MONITORING, got %+v", id, *sc)
			}
		}
		if sc := t3MustTargetScenario(t, trRust, "EOD-04"); sc.Status != StatusUnsupportedByClient {
			t.Errorf("rust/EOD-04 expected UNSUPPORTED_BY_CLIENT, got %+v", *sc)
		}

		dCfg := t3MustDiff(t, rep, "scenario_verdict", "CFG-01")
		if dCfg.Verdicts["go_bad"] != StatusFail || dCfg.Verdicts["cpp"] != StatusPass || dCfg.Verdicts["rust"] != StatusPass {
			t.Errorf("CFG-01 diff unexpected: %+v", *dCfg)
		}
		dTrac02 := t3MustDiff(t, rep, "scenario_verdict", "TRAC-02")
		if dTrac02.Verdicts["go_bad"] != StatusFail || dTrac02.Verdicts["cpp"] != StatusUnsupportedByClient || dTrac02.Verdicts["rust"] != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-02 diff unexpected: %+v", *dTrac02)
		}
		dPub := t3MustDiff(t, rep, "scenario_trace_delta", "PUB-01")
		if dPub.Verdicts["go_bad"] != StatusPass || dPub.Verdicts["cpp"] != StatusPass || dPub.Verdicts["rust"] != StatusSkippedNoMonitoring {
			t.Errorf("PUB-01 scenario_trace_delta unexpected: %+v", *dPub)
		}

		md := ReadMarkdownReport(t, mdPath)
		diffTbl := t3FindTableByHeaders(t, md, "Kind", "Scenario / Field", "go_bad", "cpp", "rust", "Description")
		t3FindTableRow(t, diffTbl, map[string]string{"Kind": "scenario_verdict", "Scenario / Field": "CFG-01", "go_bad": StatusFail, "cpp": StatusPass, "rust": StatusPass})
		t3FindTableRow(t, diffTbl, map[string]string{"Kind": "scenario_trace_delta", "Scenario / Field": "PUB-01", "go_bad": StatusPass, "cpp": StatusPass, "rust": StatusSkippedNoMonitoring})
	})
}

func TestT3_08_MultiTarget_PartialUnsupported_StrictVsPermissive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	wGo, mGo := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	wCpp, mCpp := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
	wRust, mRust := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

	permJSON := filepath.Join(dir, "perm.json")
	permMD := filepath.Join(dir, "perm.md")
	permRes := runCLI(
		t, 75*time.Second,
		"--proxy-target=go="+wGo,
		"--proxy-target=cpp="+wCpp,
		"--proxy-target=rust="+wRust,
		"--filter="+t3All13Filter,
		"--suite-timeout=60s",
		"--report-json="+permJSON,
		"--report-md="+permMD,
	)
	if permRes.ExitCode != 0 || permRes.TimedOut || permRes.Err != nil {
		t.Fatalf("permissive multi-target failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			permRes.ExitCode, permRes.TimedOut, permRes.Err, permRes.Stderr)
	}
	assertNoOrphanProxy(t, mGo)
	assertNoOrphanProxy(t, mCpp)
	assertNoOrphanProxy(t, mRust)

	strictJSON := filepath.Join(dir, "strict.json")
	strictMD := filepath.Join(dir, "strict.md")
	strictRes := runCLI(
		t, 75*time.Second,
		"--proxy-target=go="+wGo,
		"--proxy-target=cpp="+wCpp,
		"--proxy-target=rust="+wRust,
		"--filter="+t3All13Filter,
		"--strict-unsupported",
		"--suite-timeout=60s",
		"--report-json="+strictJSON,
		"--report-md="+strictMD,
	)
	if strictRes.ExitCode != 1 || strictRes.TimedOut {
		t.Fatalf("strict multi-target expected exit=1 timedOut=false, got exit=%d timedOut=%v\nstderr:\n%s",
			strictRes.ExitCode, strictRes.TimedOut, strictRes.Stderr)
	}
	assertNoOrphanProxy(t, mGo)
	assertNoOrphanProxy(t, mCpp)
	assertNoOrphanProxy(t, mRust)

	wantGo := ParsedSummary{Total: 14, Pass: 14}
	wantCpp := ParsedSummary{Total: 14, Pass: 9, UnsupportedByClient: 5}
	wantRust := ParsedSummary{Total: 14, Pass: 7, SkippedNoMonitoring: 1, UnsupportedByClient: 6}

	for _, run := range []struct {
		name string
		j, m string
	}{
		{name: "permissive", j: permJSON, m: permMD},
		{name: "strict", j: strictJSON, m: strictMD},
	} {
		rep := ReadJSONReport(t, run.j)
		t3AssertJSONV1Base(t, rep, "compare", false)

		trGo := t3MustTarget(t, rep, "go")
		trCpp := t3MustTarget(t, rep, "cpp")
		trRust := t3MustTarget(t, rep, "rust")
		if trGo.Summary == nil || *trGo.Summary != wantGo {
			t.Errorf("%s go Summary = %+v, want %+v", run.name, trGo.Summary, wantGo)
		}
		if trCpp.Summary == nil || *trCpp.Summary != wantCpp {
			t.Errorf("%s cpp Summary = %+v, want %+v", run.name, trCpp.Summary, wantCpp)
		}
		if trRust.Summary == nil || *trRust.Summary != wantRust {
			t.Errorf("%s rust Summary = %+v, want %+v", run.name, trRust.Summary, wantRust)
		}
		t3AssertCategoriesCovered(t, run.name+"-go", trGo.Scenarios, t3All13Categories)

		for _, id := range []string{"PORD-01", "PFC-01", "CMP-01", "HDG-01", "PRET-01b", "PULL-03", "SUBF-01b", "EOD-04", "TRAC-02"} {
			t3MustDiff(t, rep, "scenario_verdict", id)
		}
		for _, id := range []string{"PUB-01", "LEASE-01", "DLQ-01", "LIFE-01", "CFG-01"} {
			d := t3MustDiff(t, rep, "scenario_trace_delta", id)
			if d.Verdicts["go"] != StatusPass || d.Verdicts["cpp"] != StatusPass || d.Verdicts["rust"] != StatusSkippedNoMonitoring {
				t.Errorf("%s %s trace_delta unexpected: %+v", run.name, id, *d)
			}
		}

		md := ReadMarkdownReport(t, run.m)
		sumTbl := t3FindTableByHeaders(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "go", "Pass": "14", "Unsupported By Client": "0"})
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "cpp", "Pass": "9", "Unsupported By Client": "5"})
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "rust", "Pass": "7", "Skipped No Monitoring": "1", "Unsupported By Client": "6"})

		matTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "go", "cpp", "rust")
		if len(matTbl.Rows) != 14 {
			t.Errorf("%s Scenario Matrix rows = %d, want 14", run.name, len(matTbl.Rows))
		}
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "HDG-01", "go": StatusPass, "cpp": StatusUnsupportedByClient, "rust": StatusPass})
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "CMP-01", "go": StatusPass, "cpp": StatusPass, "rust": StatusUnsupportedByClient})
	}
}

func TestT3_09_Filter_MultiTarget_CompareSubset(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	wGo, mGo := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	wCpp, mCpp := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
	wRust, mRust := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

	t.Run("MatchingFilter_MultiTarget", func(t *testing.T) {
		jsonPath := filepath.Join(dir, "match.json")
		mdPath := filepath.Join(dir, "match.md")
		res := runCLI(
			t, 75*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter=^(ordering|eod|traces)$",
			"--suite-timeout=60s",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("filtered multi-target failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, mGo)
		assertNoOrphanProxy(t, mCpp)
		assertNoOrphanProxy(t, mRust)

		rep := ReadJSONReport(t, jsonPath)
		t3AssertJSONV1Base(t, rep, "compare", false)

		trGo := t3MustTarget(t, rep, "go")
		trCpp := t3MustTarget(t, rep, "cpp")
		trRust := t3MustTarget(t, rep, "rust")
		if trGo.Summary == nil || *trGo.Summary != (ParsedSummary{Total: 17, Pass: 17}) {
			t.Errorf("go Summary = %+v, want 17/17 PASS", trGo.Summary)
		}
		if trCpp.Summary == nil || *trCpp.Summary != (ParsedSummary{Total: 17, Pass: 15, UnsupportedByClient: 2}) {
			t.Errorf("cpp Summary = %+v, want 15P/2U", trCpp.Summary)
		}
		if trRust.Summary == nil || *trRust.Summary != (ParsedSummary{Total: 17, Pass: 9, SkippedNoMonitoring: 4, UnsupportedByClient: 4}) {
			t.Errorf("rust Summary = %+v, want 9P/4S/4U", trRust.Summary)
		}
		allowedCats := map[string]bool{"ordering": true, "eod": true, "traces": true}
		for _, tr := range []*ParsedTargetReport{trGo, trCpp, trRust} {
			for _, sc := range tr.Scenarios {
				if !allowedCats[sc.Category] {
					t.Errorf("%s/%s unexpected category %q outside ^(ordering|eod|traces)$", tr.TargetName, sc.ID, sc.Category)
				}
			}
		}
		matTbl := t3FindTableByHeaders(t, ReadMarkdownReport(t, mdPath), "Scenario", "Category", "go", "cpp", "rust")
		if len(matTbl.Rows) != 17 {
			t.Errorf("Scenario Matrix row count = %d, want 17", len(matTbl.Rows))
		}
	})

	jGo := filepath.Join(dir, "go_subset.json")
	jRust := filepath.Join(dir, "rust_subset.json")

	t.Run("AsymmetricFiltered_OfflineCompare", func(t *testing.T) {
		r1 := runCLI(t, 45*time.Second, "--proxy-target=go="+wGo, "--filter=^(PUB-01|PORD-02|EOD-02|TRAC-03)$", "--report-json="+jGo)
		if r1.ExitCode != 0 || r1.TimedOut || r1.Err != nil {
			t.Fatalf("go subset failed: exit=%d err=%v stderr=%s", r1.ExitCode, r1.Err, r1.Stderr)
		}
		r2 := runCLI(t, 45*time.Second, "--proxy-target=rust="+wRust, "--filter=^(PULL-01|TRAC-01|TRAC-03|CFG-01)$", "--report-json="+jRust)
		if r2.ExitCode != 0 || r2.TimedOut || r2.Err != nil {
			t.Fatalf("rust subset failed: exit=%d err=%v stderr=%s", r2.ExitCode, r2.Err, r2.Stderr)
		}

		cmpJSON := filepath.Join(dir, "asym_cmp.json")
		cmpMD := filepath.Join(dir, "asym_cmp.md")
		rCmp := runCLI(t, 25*time.Second, "--compare="+jGo+","+jRust, "--report-json="+cmpJSON, "--report-md="+cmpMD)
		if rCmp.ExitCode != 0 || rCmp.TimedOut || rCmp.Err != nil {
			t.Fatalf("asymmetric --compare failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				rCmp.ExitCode, rCmp.TimedOut, rCmp.Err, rCmp.Stderr)
		}

		rep := ReadJSONReport(t, cmpJSON)
		t3AssertJSONV1Base(t, rep, "compare", false)
		if len(rep.Targets) != 2 {
			t.Fatalf("expected 2 targets, got %d", len(rep.Targets))
		}

		for _, tc := range []struct {
			id, wantGo, wantRust string
		}{
			{id: "PUB-01", wantGo: StatusPass, wantRust: "MISSING"},
			{id: "PORD-02", wantGo: StatusPass, wantRust: "MISSING"},
			{id: "EOD-02", wantGo: StatusPass, wantRust: "MISSING"},
			{id: "PULL-01", wantGo: "MISSING", wantRust: StatusPass},
			{id: "CFG-01", wantGo: "MISSING", wantRust: StatusPass},
			{id: "TRAC-01", wantGo: "MISSING", wantRust: StatusSkippedNoMonitoring},
		} {
			d := t3MustDiff(t, rep, "scenario_verdict", tc.id)
			if d.Verdicts["go"] != tc.wantGo || d.Verdicts["rust"] != tc.wantRust {
				t.Errorf("%s verdicts = %v, want go=%q rust=%q", tc.id, d.Verdicts, tc.wantGo, tc.wantRust)
			}
		}
		dTrac03 := t3MustDiff(t, rep, "scenario_trace_delta", "TRAC-03")
		if dTrac03.Verdicts["go"] != StatusPass || dTrac03.Verdicts["rust"] != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-03 trace_delta = %+v", *dTrac03)
		}

		md := ReadMarkdownReport(t, cmpMD)
		matTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "go", "rust")
		if len(matTbl.Rows) != 7 {
			t.Errorf("union Scenario Matrix rows = %d, want 7", len(matTbl.Rows))
		}
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "PUB-01", "go": StatusPass, "rust": "MISSING"})
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "PULL-01", "go": "MISSING", "rust": StatusPass})
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-01", "go": "MISSING", "rust": StatusSkippedNoMonitoring})
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-03", "go": StatusPass, "rust": StatusPass})
	})

	t.Run("ComparePlusFilterRejected", func(t *testing.T) {
		outJSON := filepath.Join(dir, "rejected.json")
		res := runCLI(t, 15*time.Second, "--compare="+jGo+","+jRust, "--filter=PUB-01", "--report-json="+outJSON)
		if res.ExitCode != 1 || res.TimedOut || !strings.Contains(res.Stderr, "--compare cannot be combined with") {
			t.Errorf("expected CLI conflict exit=1, got exit=%d timedOut=%v stderr=%q",
				res.ExitCode, res.TimedOut, res.Stderr)
		}
		t3AssertFileNotExist(t, outJSON)
	})
}

func TestT3_10_CompareOfflineJSON_FaultyVsConformant(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	filter := "^(PUB-01|PORD-02|TRAC-01|TRAC-03|CFG-02)$"

	wGo, mGo := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	wWrong, mWrong := writeProxyWrapper(t, "--mode=wrong-rpc", "--fast-timings")
	wMiss, mMiss := writeProxyWrapper(t, "--mode=missing-traces", "--fast-timings")
	wRust, mRust := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

	jGo := filepath.Join(dir, "go.json")
	jWrong := filepath.Join(dir, "wrong.json")
	jMiss := filepath.Join(dir, "missing.json")
	jRust := filepath.Join(dir, "rust.json")

	for _, prod := range []struct {
		name     string
		args     []string
		wantExit int
	}{
		{
			name:     "go_default",
			args:     []string{"--proxy-cmd=" + wGo, "--filter=" + filter, "--report-json=" + jGo},
			wantExit: 0,
		},
		{
			name:     "wrong_rpc",
			args:     []string{"--proxy-target=wrong=" + wWrong, "--filter=" + filter, "--report-json=" + jWrong},
			wantExit: 1,
		},
		{
			name:     "missing_traces",
			args:     []string{"--proxy-target=missing=" + wMiss, "--filter=" + filter, "--report-json=" + jMiss},
			wantExit: 1,
		},
		{
			name:     "rust_nomon",
			args:     []string{"--proxy-target=rust=" + wRust, "--filter=" + filter, "--report-json=" + jRust},
			wantExit: 0,
		},
	} {
		r := runCLI(t, 45*time.Second, prod.args...)
		if r.ExitCode != prod.wantExit || r.TimedOut {
			t.Fatalf("producer %s exit=%d timedOut=%v (wantExit=%d)\nstderr:\n%s",
				prod.name, r.ExitCode, r.TimedOut, prod.wantExit, r.Stderr)
		}
	}
	assertNoOrphanProxy(t, mGo)
	assertNoOrphanProxy(t, mWrong)
	assertNoOrphanProxy(t, mMiss)
	assertNoOrphanProxy(t, mRust)

	t.Run("FourWayOfflineCompare_MixedResults", func(t *testing.T) {
		cmpJSON := filepath.Join(dir, "compare_4way.json")
		cmpMD := filepath.Join(dir, "compare_4way.md")
		res := runCLI(
			t, 25*time.Second,
			"--compare="+jGo,
			"--compare="+jWrong,
			"--compare="+jMiss+","+jRust,
			"--report-json="+cmpJSON,
			"--report-md="+cmpMD,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("offline 4-way --compare failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}

		rep := ReadJSONReport(t, cmpJSON)
		t3AssertJSONV1Base(t, rep, "compare", false)
		if len(rep.Targets) != 4 {
			t.Fatalf("Targets len = %d, want 4", len(rep.Targets))
		}

		dPub := t3MustDiff(t, rep, "scenario_verdict", "PUB-01")
		if dPub.Verdicts["default"] != StatusPass || dPub.Verdicts["wrong"] != StatusFail ||
			dPub.Verdicts["missing"] != StatusPass || dPub.Verdicts["rust"] != StatusPass {
			t.Errorf("PUB-01 verdicts unexpected: %+v", *dPub)
		}
		if !strings.Contains(dPub.Details["wrong"], "mismatch") {
			t.Errorf("PUB-01 Details[wrong] = %q, expected mismatch diagnostic", dPub.Details["wrong"])
		}

		dTrac01 := t3MustDiff(t, rep, "scenario_verdict", "TRAC-01")
		if dTrac01.Verdicts["default"] != StatusPass || dTrac01.Verdicts["wrong"] != StatusFail ||
			dTrac01.Verdicts["missing"] != StatusFail || dTrac01.Verdicts["rust"] != StatusSkippedNoMonitoring {
			t.Errorf("TRAC-01 verdicts unexpected: %+v", *dTrac01)
		}
		dTrac03 := t3MustDiff(t, rep, "scenario_verdict", "TRAC-03")
		if dTrac03.Verdicts["default"] != StatusPass || dTrac03.Verdicts["wrong"] != StatusFail ||
			dTrac03.Verdicts["missing"] != StatusFail || dTrac03.Verdicts["rust"] != StatusPass {
			t.Errorf("TRAC-03 verdicts unexpected: %+v", *dTrac03)
		}
		dCfg02 := t3MustDiff(t, rep, "scenario_trace_delta", "CFG-02")
		if dCfg02.Verdicts["default"] != StatusPass || dCfg02.Verdicts["wrong"] != StatusPass ||
			dCfg02.Verdicts["missing"] != StatusPass || dCfg02.Verdicts["rust"] != StatusSkippedNoMonitoring {
			t.Errorf("CFG-02 trace_delta unexpected: %+v", *dCfg02)
		}
		dCap := t3MustDiff(t, rep, "capability", "supports_monitoring")
		if dCap.Verdicts["default"] != "true" || dCap.Verdicts["wrong"] != "true" ||
			dCap.Verdicts["missing"] != "true" || dCap.Verdicts["rust"] != "false" {
			t.Errorf("supports_monitoring diff unexpected: %+v", *dCap)
		}

		md := ReadMarkdownReport(t, cmpMD)
		sumTbl := t3FindTableByHeaders(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "default", "Pass": "5", "Fail": "0"})
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "wrong", "Pass": "1", "Fail": "4"})
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "missing", "Pass": "3", "Fail": "2"})
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "rust", "Pass": "4", "Fail": "0", "Skipped No Monitoring": "1"})

		matTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "default", "wrong", "missing", "rust")
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "PUB-01", "default": StatusPass, "wrong": StatusFail, "missing": StatusPass, "rust": StatusPass})
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-01", "default": StatusPass, "wrong": StatusFail, "missing": StatusFail, "rust": StatusSkippedNoMonitoring})

		diffTbl := t3FindTableByHeaders(t, md, "Kind", "Scenario / Field", "default", "wrong", "missing", "rust", "Description")
		t3FindTableRow(t, diffTbl, map[string]string{"Kind": "scenario_verdict", "Scenario / Field": "PUB-01"})
		t3FindTableRow(t, diffTbl, map[string]string{"Kind": "scenario_trace_delta", "Scenario / Field": "CFG-02"})
	})

	t.Run("OfflineCompare_DiagnosticDelta", func(t *testing.T) {
		wTimeout, mTimeout := writeProxyWrapper(t, "--mode=conformant-go", "--fault=hang-publish", "--fast-timings")
		jTimeout := filepath.Join(dir, "timeout.json")
		rTo := runCLI(
			t, 45*time.Second,
			"--proxy-target=to="+wTimeout,
			"--filter=^PUB-01$",
			"--scenario-timeout=400ms",
			"--report-json="+jTimeout,
		)
		if rTo.ExitCode != 1 || rTo.TimedOut {
			t.Fatalf("timeout producer expected exit=1, got exit=%d timedOut=%v", rTo.ExitCode, rTo.TimedOut)
		}
		assertNoOrphanProxy(t, mTimeout)

		diagJSON := filepath.Join(dir, "diag_cmp.json")
		diagMD := filepath.Join(dir, "diag_cmp.md")
		rCmp := runCLI(t, 25*time.Second, "--compare="+jWrong+","+jTimeout, "--report-json="+diagJSON, "--report-md="+diagMD)
		if rCmp.ExitCode != 0 || rCmp.TimedOut || rCmp.Err != nil {
			t.Fatalf("diagnostic compare failed: exit=%d err=%v\nstderr:\n%s", rCmp.ExitCode, rCmp.Err, rCmp.Stderr)
		}

		rep := ReadJSONReport(t, diagJSON)
		t3AssertJSONV1Base(t, rep, "compare", false)
		dDiag := t3MustDiff(t, rep, "scenario_diagnostic", "PUB-01")
		if dDiag.Verdicts["wrong"] != StatusFail || dDiag.Verdicts["to"] != StatusFail ||
			t3RawString(dDiag.Raw, "kind") != "scenario_diagnostic" ||
			!strings.Contains(dDiag.Description, "wrong=FAIL") || !strings.Contains(dDiag.Description, "to=TIMEOUT") ||
			dDiag.Details["wrong"] == "" || !strings.Contains(dDiag.Details["to"], "TIMEOUT:") {
			t.Errorf("PUB-01 scenario_diagnostic unexpected: %+v raw=%v", *dDiag, dDiag.Raw)
		}
		if rawV, ok := dDiag.Raw["verdicts"].(map[string]any); !ok || t3RawString(rawV, "wrong") != StatusFail || t3RawString(rawV, "to") != StatusFail {
			t.Errorf("raw scenario_diagnostic verdicts unexpected: %#v", dDiag.Raw["verdicts"])
		}

		md := ReadMarkdownReport(t, diagMD)
		diffTbl := t3FindTableByHeaders(t, md, "Kind", "Scenario / Field", "wrong", "to", "Description")
		t3FindTableRow(t, diffTbl, map[string]string{"Kind": "scenario_diagnostic", "Scenario / Field": "PUB-01"})
	})
}

func TestT3_11_ParallelOrSequentialScenarios_Timeout_FaultIsolation(t *testing.T) {
	t.Parallel()

	t.Run("HangPublishRestartAndIsolatedRecovery", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=conformant-go", "--fault=hang-publish", "--fast-timings"},
			"--filter=^(PUB-01|PULL-01|LEASE-01|EOD-02|LIFE-01)$",
			"--scenario-timeout=500ms",
			"--suite-timeout=30s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 5, Pass: 4, Fail: 1, TimeoutCount: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		t3AssertCategoriesCovered(t, "hang-recovery", rep.Scenarios, []string{"publish", "pull", "lease", "eod", "lifecycle"})

		pub01 := t3MustScenario(t, rep, "PUB-01")
		if pub01.Status != StatusFail || pub01.StatusDetail != StatusTimeout || pub01.ProxyRestarted {
			t.Errorf("PUB-01 unexpected: %+v", *pub01)
		}
		pull01 := t3MustScenario(t, rep, "PULL-01")
		if pull01.Status != StatusPass || !pull01.ProxyRestarted {
			t.Errorf("PULL-01 expected PASS + ProxyRestarted=true, got %+v", *pull01)
		}
		for _, id := range []string{"LEASE-01", "EOD-02", "LIFE-01"} {
			sc := t3MustScenario(t, rep, id)
			if sc.Status != StatusPass || sc.ProxyRestarted {
				t.Errorf("%s expected PASS + ProxyRestarted=false, got %+v", id, *sc)
			}
		}

		scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "PUB-01", "Status": "FAIL (TIMEOUT)"})
		t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "LIFE-01", "Status": StatusPass})
	})

	t.Run("MidScenarioCrashRestartBeforeIsolatedEmulator", func(t *testing.T) {
		_, rep, _, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=conformant-go", "--fault=crash-after=4", "--unsupported=unary_pull", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|PULL-01|CFG-01)$",
			"--scenario-timeout=10s",
			"--suite-timeout=30s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 4, Pass: 3, Fail: 1, ErrorCount: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t3MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass || sc.ProxyRestarted {
			t.Errorf("PUB-01 unexpected: %+v", *sc)
		}
		if sc := t3MustScenario(t, rep, "PORD-02"); sc.Status != StatusFail || sc.StatusDetail != StatusError || sc.ProxyRestarted {
			t.Errorf("PORD-02 unexpected: %+v", *sc)
		}
		if sc := t3MustScenario(t, rep, "PULL-01"); sc.Status != StatusPass || !sc.ProxyRestarted {
			t.Errorf("PULL-01 expected PASS + ProxyRestarted=true, got %+v", *sc)
		}
		if sc := t3MustScenario(t, rep, "CFG-01"); sc.Status != StatusPass || sc.ProxyRestarted {
			t.Errorf("CFG-01 expected isolated PASS + ProxyRestarted=false, got %+v", *sc)
		}
	})

	t.Run("ConcurrentCLIInvocations_FaultIsolation", func(t *testing.T) {
		buildConformanceCLIBinary(t)

		dirA := t.TempDir()
		dirB := t.TempDir()
		wHung, mHung := writeProxyWrapper(t, "--mode=conformant-go", "--fault=hang-publish", "--fast-timings")
		wHealthy, mHealthy := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")

		jA := filepath.Join(dirA, "hung.json")
		jB := filepath.Join(dirB, "healthy.json")

		var resA, resB CLIResult
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			resA = runCLI(
				t, 45*time.Second,
				"--proxy-cmd="+wHung,
				"--filter=^(PUB-01|PULL-01)$",
				"--scenario-timeout=500ms",
				"--suite-timeout=20s",
				"--report-json="+jA,
			)
		}()
		go func() {
			defer wg.Done()
			resB = runCLI(
				t, 45*time.Second,
				"--proxy-cmd="+wHealthy,
				"--filter=^(PUB-01|PORD-02|PULL-01|EOD-02|LIFE-01)$",
				"--scenario-timeout=10s",
				"--suite-timeout=30s",
				"--report-json="+jB,
			)
		}()
		wg.Wait()

		assertNoOrphanProxy(t, mHung)
		assertNoOrphanProxy(t, mHealthy)

		if resA.ExitCode != 1 || resA.TimedOut {
			t.Errorf("concurrent hung CLI expected exit=1 timedOut=false, got exit=%d timedOut=%v\nstderr:\n%s",
				resA.ExitCode, resA.TimedOut, resA.Stderr)
		}
		if resB.ExitCode != 0 || resB.TimedOut || resB.Err != nil {
			t.Errorf("concurrent healthy CLI expected exit=0, got exit=%d timedOut=%v err=%v\nstderr:\n%s",
				resB.ExitCode, resB.TimedOut, resB.Err, resB.Stderr)
		}

		repA := ReadJSONReport(t, jA)
		t3AssertJSONV1Base(t, repA, "single", false)
		if repA.Summary == nil || *repA.Summary != (ParsedSummary{Total: 2, Pass: 1, Fail: 1, TimeoutCount: 1}) {
			t.Errorf("repA Summary = %+v, want 2/1P/1F(1T)", repA.Summary)
		}
		if sc := t3MustScenario(t, repA, "PUB-01"); sc.Status != StatusFail || sc.StatusDetail != StatusTimeout {
			t.Errorf("repA/PUB-01 unexpected: %+v", *sc)
		}
		if sc := t3MustScenario(t, repA, "PULL-01"); sc.Status != StatusPass || !sc.ProxyRestarted {
			t.Errorf("repA/PULL-01 unexpected: %+v", *sc)
		}

		repB := ReadJSONReport(t, jB)
		t3AssertJSONV1Base(t, repB, "single", false)
		if repB.Summary == nil || *repB.Summary != (ParsedSummary{Total: 5, Pass: 5}) {
			t.Errorf("repB Summary = %+v, want 5/5 PASS", repB.Summary)
		}
	})
}

func TestT3_12_OutOfOrderRPC_MultiTarget_DiffPinpointsBug(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "ooo_compare.json")
	mdPath := filepath.Join(dir, "ooo_compare.md")

	wGood, mGood := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
	wBuggy, mBuggy := writeProxyWrapper(t, "--mode=out-of-order-rpc", "--fast-timings")

	res := runCLI(
		t, 45*time.Second,
		"--proxy-target=good="+wGood,
		"--proxy-target=buggy="+wBuggy,
		"--filter=^(PUB-01|PORD-02|PULL-01|SUBF-02|EOD-02)$",
		"--suite-timeout=30s",
		"--report-json="+jsonPath,
		"--report-md="+mdPath,
	)
	if res.ExitCode != 1 || res.TimedOut {
		t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Stderr)
	}
	assertNoOrphanProxy(t, mGood)
	assertNoOrphanProxy(t, mBuggy)

	rep := ReadJSONReport(t, jsonPath)
	t3AssertJSONV1Base(t, rep, "compare", false)

	trGood := t3MustTarget(t, rep, "good")
	trBuggy := t3MustTarget(t, rep, "buggy")
	if trGood.Summary == nil || *trGood.Summary != (ParsedSummary{Total: 5, Pass: 5}) {
		t.Errorf("good Summary = %+v, want 5/5 PASS", trGood.Summary)
	}
	if trBuggy.Summary == nil || *trBuggy.Summary != (ParsedSummary{Total: 5, Pass: 1, Fail: 4}) {
		t.Errorf("buggy Summary = %+v, want 1P/4F", trBuggy.Summary)
	}
	if sc := t3MustTargetScenario(t, trBuggy, "PUB-01"); sc.Status != StatusPass {
		t.Errorf("buggy/PUB-01 expected PASS, got %+v", *sc)
	}

	wantErrs := map[string]string{
		"PORD-02": "ordered Publish FIFO mismatch",
		"PULL-01": "premature ack observed before SettleMessageCommand",
		"SUBF-02": "out of order",
		"EOD-02":  `SettlementResultEvent("deliv-3") mismatch`,
	}
	for id, wantSub := range wantErrs {
		sc := t3MustTargetScenario(t, trBuggy, id)
		if sc.Status != StatusFail || !strings.Contains(sc.Error, wantSub) {
			t.Errorf("buggy/%s expected FAIL containing %q, got %+v", id, wantSub, *sc)
		}
	}

	if len(rep.Differences) != 4 {
		t.Errorf("expected exact 4 scenario_verdict diffs with 0 capability/default/PUB-01 noise, got %d: %+v",
			len(rep.Differences), rep.Differences)
	}
	for _, d := range rep.Differences {
		if d.Kind != "scenario_verdict" {
			t.Errorf("unexpected non-verdict diff: %+v", d)
		}
	}
	if t3HasDiff(rep, "scenario_verdict", "PUB-01") {
		t.Errorf("PUB-01 passed on both targets and must not appear in differences")
	}
	for id, wantSub := range wantErrs {
		d := t3MustDiff(t, rep, "scenario_verdict", id)
		if d.Verdicts["good"] != StatusPass || d.Verdicts["buggy"] != StatusFail ||
			!strings.Contains(d.Details["buggy"], wantSub) ||
			t3RawString(d.Raw, "kind") != "scenario_verdict" {
			t.Errorf("%s diff unexpected: %+v raw=%v", id, *d, d.Raw)
		}
	}

	md := ReadMarkdownReport(t, mdPath)
	if len(md.Tables) != 3 {
		t.Errorf("expected 3 Markdown tables (Capabilities & Defaults omitted for identical capabilities), got %d", len(md.Tables))
	}
	sumTbl := t3FindTableByHeaders(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
	t3FindTableRow(t, sumTbl, map[string]string{"Target": "good", "Pass": "5", "Fail": "0"})
	t3FindTableRow(t, sumTbl, map[string]string{"Target": "buggy", "Pass": "1", "Fail": "4"})

	matTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "good", "buggy")
	t3FindTableRow(t, matTbl, map[string]string{"Scenario": "PUB-01", "good": StatusPass, "buggy": StatusPass})
	for id := range wantErrs {
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": id, "good": StatusPass, "buggy": StatusFail})
	}

	diffTbl := t3FindTableByHeaders(t, md, "Kind", "Scenario / Field", "good", "buggy", "Description")
	if len(diffTbl.Rows) != 4 {
		t.Errorf("Behavioral Differences rows = %d, want 4", len(diffTbl.Rows))
	}
	if len(md.DiagnosticsBullets) != 4 {
		t.Errorf("DiagnosticsBullets len = %d, want 4: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
	}
}

func TestT3_13_CrashMidSuite_PartialReportAndCleanup(t *testing.T) {
	t.Parallel()

	t.Run("CrashMidSuite_RecoverAndContinueFiltered", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=conformant-go", "--fault=crash-after=4", "--delay=80ms", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|PULL-01|EOD-01b|LIFE-02)$",
			"--scenario-timeout=10s",
			"--suite-timeout=30s",
		)
		t3AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 5, Pass: 4, Fail: 1, ErrorCount: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		t3AssertCategoriesCovered(t, "crash-recovery", rep.Scenarios, []string{"publish", "ordering", "pull", "eod", "lifecycle"})

		if sc := t3MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass || sc.ProxyRestarted {
			t.Errorf("PUB-01 unexpected: %+v", *sc)
		}
		pord02 := t3MustScenario(t, rep, "PORD-02")
		if pord02.Status != StatusFail || pord02.StatusDetail != StatusError ||
			t3RawString(pord02.Raw, "status_detail") != StatusError ||
			!strings.Contains(pord02.Error, "crash-after=4") {
			t.Errorf("PORD-02 expected crash-after=4 ERROR, got %+v raw=%v", *pord02, pord02.Raw)
		}
		if sc := t3MustScenario(t, rep, "PULL-01"); sc.Status != StatusPass || !sc.ProxyRestarted {
			t.Errorf("PULL-01 expected PASS + ProxyRestarted=true, got %+v", *sc)
		}
		for _, id := range []string{"EOD-01b", "LIFE-02"} {
			if sc := t3MustScenario(t, rep, id); sc.Status != StatusPass || sc.ProxyRestarted {
				t.Errorf("%s expected PASS + ProxyRestarted=false, got %+v", id, *sc)
			}
		}

		if !md.HasFailureBreakdown || md.ErrorCount != 1 || md.TimeoutCount != 0 {
			t.Errorf("MD failure breakdown unexpected: has=%v timeout=%d err=%d",
				md.HasFailureBreakdown, md.TimeoutCount, md.ErrorCount)
		}
		scenTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "PORD-02", "Status": "FAIL (ERROR)"})
		t3FindTableRow(t, scenTbl, map[string]string{"Scenario": "LIFE-02", "Status": StatusPass})
		if len(md.DiagnosticsBullets) != 1 || !strings.Contains(md.DiagnosticsBullets[0], "PORD-02") {
			t.Errorf("DiagnosticsBullets unexpected: %v", md.DiagnosticsBullets)
		}
	})

	t.Run("CrashPlusTightSuiteTimeout_PartialReport", func(t *testing.T) {
		_, rep, md, _ := t3RunSingleWithReports(
			t, 45*time.Second, 1,
			[]string{"--mode=conformant-go", "--delay=250ms", "--fault=crash-after=4", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|PULL-01|LEASE-03|LIFE-02)$",
			"--scenario-timeout=5s",
			"--suite-timeout=1500ms",
		)
		t3AssertJSONV1Base(t, rep, "single", true)

		if rep.Summary == nil || rep.Summary.Total < 1 || rep.Summary.Total >= 5 {
			t.Fatalf("expected truncated partial summary 1 <= Total < 5, got %+v", rep.Summary)
		}
		defTarget := t3MustTarget(t, rep, "default")
		if !defTarget.TimedOut || !strings.Contains(defTarget.TargetError, "suite timed out") {
			t.Errorf("Targets[default] expected TimedOut + suite timed out error, got %+v", defTarget)
		}
		if rep.Summary.Total >= 2 {
			pord02 := t3MustScenario(t, rep, "PORD-02")
			if pord02.Status != StatusFail || pord02.StatusDetail != StatusError || rep.Summary.ErrorCount < 1 {
				t.Errorf("PORD-02 expected crash ERROR with ErrorCount >= 1, got sc=%+v sum=%+v", *pord02, rep.Summary)
			}
		}

		sumTbl := t3FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Total": strconv.Itoa(rep.Summary.Total)})
		if rep.Summary.ErrorCount > 0 || rep.Summary.TimeoutCount > 0 {
			if !md.HasFailureBreakdown || md.ErrorCount != rep.Summary.ErrorCount || md.TimeoutCount != rep.Summary.TimeoutCount {
				t.Errorf("MD failure breakdown mismatch: md=(has=%v err=%d to=%d) json=(err=%d to=%d)",
					md.HasFailureBreakdown, md.ErrorCount, md.TimeoutCount, rep.Summary.ErrorCount, rep.Summary.TimeoutCount)
			}
		}
		if len(md.DiagnosticsBullets) == 0 {
			t.Errorf("expected non-empty Markdown DiagnosticsBullets on timed-out crash run")
		}
	})
}

func TestT3_14_Compare_SuiteTimeout_BoundedExecution(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	jBounded := filepath.Join(dir, "bounded_13cat.json")
	mBounded := filepath.Join(dir, "bounded_13cat.md")
	jTrunc := filepath.Join(dir, "truncated_3target.json")
	mTrunc := filepath.Join(dir, "truncated_3target.md")

	t.Run("BoundedMultiTargetExecution_All13Categories", func(t *testing.T) {
		wGo, mGo := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		wCpp, mCpp := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
		wRust, mRust := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")

		res := runCLI(
			t, 75*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter="+t3All13Filter,
			"--scenario-timeout=15s",
			"--suite-timeout=45s",
			"--report-json="+jBounded,
			"--report-md="+mBounded,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("bounded 13-category compare failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, mGo)
		assertNoOrphanProxy(t, mCpp)
		assertNoOrphanProxy(t, mRust)

		rep := ReadJSONReport(t, jBounded)
		t3AssertJSONV1Base(t, rep, "compare", false)
		for _, name := range []string{"go", "cpp", "rust"} {
			tr := t3MustTarget(t, rep, name)
			if tr.TimedOut || tr.TargetError != "" || tr.Summary == nil || tr.Summary.Total != 14 {
				t.Errorf("%s target unexpected: %+v", name, tr)
			}
			t3AssertCategoriesCovered(t, "bounded-"+name, tr.Scenarios, t3All13Categories)
		}
		t3MustDiff(t, rep, "capability", "supports_monitoring")
		t3MustDiff(t, rep, "scenario_verdict", "HDG-01")
		ReadMarkdownReport(t, mBounded)
	})

	t.Run("TightSuiteTimeout_MultiTarget_TruncatesAndCompares", func(t *testing.T) {
		t3Sentinel := filepath.Join(dir, "t3_never_started.log")
		w1, m1 := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		w2, m2 := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--delay=450ms", "--fast-timings")
		w3, m3 := t3WriteLaunchSentinelWrapper(t, t3Sentinel, "--mode=conformant-rust-nomon", "--fast-timings")

		res := runCLI(
			t, 45*time.Second,
			"--proxy-target=t1="+w1,
			"--proxy-target=t2="+w2,
			"--proxy-target=t3="+w3,
			"--filter=^(PUB-01|PORD-02|PULL-01|LEASE-01|EOD-02)$",
			"--scenario-timeout=10s",
			"--suite-timeout=2200ms",
			"--report-json="+jTrunc,
			"--report-md="+mTrunc,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v\nstderr:\n%s",
				res.ExitCode, res.TimedOut, res.Stderr)
		}
		t3AssertFileNotExist(t, t3Sentinel)
		assertNoOrphanProxy(t, m1)
		assertNoOrphanProxy(t, m2)
		assertNoOrphanProxy(t, m3)

		rep := ReadJSONReport(t, jTrunc)
		t3AssertJSONV1Base(t, rep, "compare", true)

		t1 := t3MustTarget(t, rep, "t1")
		if t1.TimedOut || t1.TargetError != "" || t1.Summary == nil || *t1.Summary != (ParsedSummary{Total: 5, Pass: 5}) {
			t.Errorf("t1 unexpected: %+v", t1)
		}
		t2 := t3MustTarget(t, rep, "t2")
		if !t2.TimedOut || !strings.Contains(t2.TargetError, "suite timed out") ||
			len(t2.Scenarios) < 1 || len(t2.Scenarios) >= 5 {
			t.Errorf("t2 expected truncated 1 <= len(Scenarios) < 5 with suite timeout, got len=%d target=%+v",
				len(t2.Scenarios), t2)
		}
		t3 := t3MustTarget(t, rep, "t3")
		if !t3.TimedOut || !strings.Contains(t3.TargetError, "suite timed out before starting target") ||
			len(t3.Scenarios) != 0 || t3.Summary == nil || t3.Summary.Total != 0 {
			t.Errorf("t3 expected unstarted timeout stub, got %+v", t3)
		}

		dErr := t3MustDiff(t, rep, "target_error", "")
		if dErr.Verdicts["t1"] != "OK" || dErr.Verdicts["t2"] != StatusError || dErr.Verdicts["t3"] != StatusError {
			t.Errorf("target_error diff unexpected: %+v", *dErr)
		}
		dEod := t3MustDiff(t, rep, "scenario_verdict", "EOD-02")
		if dEod.Verdicts["t1"] != StatusPass || dEod.Verdicts["t2"] != "MISSING" || dEod.Verdicts["t3"] != "MISSING" {
			t.Errorf("EOD-02 diff unexpected: %+v", *dEod)
		}

		md := ReadMarkdownReport(t, mTrunc)
		sumTbl := t3FindTableByHeaders(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "t1", "Total": "5", "Pass": "5"})
		t3FindTableRow(t, sumTbl, map[string]string{"Target": "t3", "Monitoring": "unknown", "Total": "0"})
		matTbl := t3FindTableByHeaders(t, md, "Scenario", "Category", "t1", "t2", "t3")
		t3FindTableRow(t, matTbl, map[string]string{"Scenario": "EOD-02", "t1": StatusPass, "t2": "MISSING", "t3": "MISSING"})
	})

	t.Run("OfflineCompare_WithSuiteTimeoutContract", func(t *testing.T) {
		cmpJSON := filepath.Join(dir, "offline_timeout_cmp.json")
		cmpMD := filepath.Join(dir, "offline_timeout_cmp.md")
		rOK := runCLI(
			t, 25*time.Second,
			"--compare="+jBounded+","+jTrunc,
			"--suite-timeout=10s",
			"--scenario-timeout=5s",
			"--report-json="+cmpJSON,
			"--report-md="+cmpMD,
		)
		if rOK.ExitCode != 0 || rOK.TimedOut || rOK.Err != nil {
			t.Fatalf("offline --compare with valid timeouts failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
				rOK.ExitCode, rOK.TimedOut, rOK.Err, rOK.Stderr)
		}
		rep := ReadJSONReport(t, cmpJSON)
		t3AssertJSONV1Base(t, rep, "compare", true)
		if len(rep.Targets) != 6 {
			t.Fatalf("expected 6 merged targets (go,cpp,rust,t1,t2,t3), got %d", len(rep.Targets))
		}
		dErr := t3MustDiff(t, rep, "target_error", "")
		if dErr.Verdicts["go"] != "OK" || dErr.Verdicts["t1"] != "OK" || dErr.Verdicts["t2"] != StatusError || dErr.Verdicts["t3"] != StatusError {
			t.Errorf("offline compare target_error verdicts unexpected: %+v", *dErr)
		}
		dHdg := t3MustDiff(t, rep, "scenario_verdict", "HDG-01")
		if dHdg.Verdicts["go"] != StatusPass || dHdg.Verdicts["t3"] != "MISSING" {
			t.Errorf("offline compare HDG-01 verdicts unexpected: %+v", *dHdg)
		}
		ReadMarkdownReport(t, cmpMD)

		badSuiteOut := filepath.Join(dir, "bad_suite_to.json")
		rBadSuite := runCLI(t, 15*time.Second, "--compare="+jBounded+","+jTrunc, "--suite-timeout=0s", "--report-json="+badSuiteOut)
		if rBadSuite.ExitCode != 1 || rBadSuite.TimedOut || !strings.Contains(rBadSuite.Stderr, "--suite-timeout must be > 0 (got 0s)") {
			t.Errorf("expected --suite-timeout=0s rejection, got exit=%d timedOut=%v stderr=%q",
				rBadSuite.ExitCode, rBadSuite.TimedOut, rBadSuite.Stderr)
		}
		t3AssertFileNotExist(t, badSuiteOut)

		badScenOut := filepath.Join(dir, "bad_scen_to.json")
		rBadScen := runCLI(t, 15*time.Second, "--compare="+jBounded+","+jTrunc, "--scenario-timeout=-1s", "--report-json="+badScenOut)
		if rBadScen.ExitCode != 1 || rBadScen.TimedOut || !strings.Contains(rBadScen.Stderr, "--scenario-timeout must be > 0 (got -1s)") {
			t.Errorf("expected --scenario-timeout=-1s rejection, got exit=%d timedOut=%v stderr=%q",
				rBadScen.ExitCode, rBadScen.TimedOut, rBadScen.Stderr)
		}
		t3AssertFileNotExist(t, badScenOut)
	})
}
