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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// --- Local Unexported Helpers (stdlib-only, t2* prefix for zero collision) ---

func t2RawString(m map[string]any, key string) string {
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

func t2AssertErrorExit(t *testing.T, res CLIResult, wantStderrSub string) {
	t.Helper()
	if res.ExitCode != 1 || res.TimedOut {
		t.Errorf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v err=%v\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stdout, res.Stderr)
	}
	if res.Stdout != "" {
		t.Errorf("expected empty stdout on validation/compare failure, got %q", res.Stdout)
	}
	if wantStderrSub != "" && !strings.Contains(res.Stderr, wantStderrSub) {
		t.Errorf("stderr missing %q; got:\n%s", wantStderrSub, res.Stderr)
	}
}

func t2AssertFileNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected file %q not to exist, got stat err=%v", path, err)
	}
}

func t2MustScenario(t *testing.T, rep *ParsedReport, id string) *ParsedScenarioResult {
	t.Helper()
	if rep == nil {
		t.Fatalf("t2MustScenario(%q): nil report", id)
	}
	sc, ok := rep.FindScenario(id)
	if !ok {
		t.Fatalf("scenario %q not found in root report (total=%d)", id, len(rep.Scenarios))
	}
	return sc
}

func t2MustTargetScenario(t *testing.T, tr *ParsedTargetReport, id string) *ParsedScenarioResult {
	t.Helper()
	if tr == nil {
		t.Fatalf("t2MustTargetScenario(%q): nil target report", id)
	}
	for i := range tr.Scenarios {
		if tr.Scenarios[i].ID == id {
			return &tr.Scenarios[i]
		}
	}
	t.Fatalf("scenario %q not found in target %q report (total=%d)", id, tr.TargetName, len(tr.Scenarios))
	return nil
}

func t2MustDiff(t *testing.T, rep *ParsedReport, kind, idOrField string) *ParsedDiffEntry {
	t.Helper()
	if rep == nil {
		t.Fatalf("t2MustDiff(%q, %q): nil report", kind, idOrField)
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

func t2AssertJSONV1Base(t *testing.T, rep *ParsedReport, wantMode string, wantTimedOut bool) {
	t.Helper()
	if rep == nil {
		t.Fatalf("nil JSON report")
	}
	if got := t2RawString(rep.Raw, "schema_version"); got != "1.0" {
		t.Errorf("schema_version = %q (raw=%#v), want %q", got, rep.Raw["schema_version"], "1.0")
	}
	genAt := t2RawString(rep.Raw, "generated_at")
	if genAt == "" {
		t.Errorf("generated_at missing or empty")
	} else if _, err := time.Parse(time.RFC3339Nano, genAt); err != nil {
		t.Errorf("generated_at %q invalid RFC3339 timestamp: %v", genAt, err)
	}
	if rep.Mode != wantMode || t2RawString(rep.Raw, "mode") != wantMode {
		t.Errorf("report mode = (%q, raw=%q), want %q", rep.Mode, t2RawString(rep.Raw, "mode"), wantMode)
	}
	if rep.TimedOut != wantTimedOut {
		t.Errorf("report timed_out = %v, want %v", rep.TimedOut, wantTimedOut)
	}
	rawDiffs, hasDiffs := rep.Raw["differences"]
	if !hasDiffs || rawDiffs == nil {
		t.Errorf("differences JSON key missing or null")
	} else if _, ok := rawDiffs.([]any); !ok {
		t.Errorf("differences JSON value has type %T, want array", rawDiffs)
	}
	AssertSummaryMatchesScenarios(t, rep)
}

func t2FindTableByHeaders(t *testing.T, md *ParsedMarkdownReport, wantHeaders ...string) *MarkdownTable {
	t.Helper()
	if md == nil {
		t.Fatalf("t2FindTableByHeaders: nil Markdown report")
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

func t2FindTableRow(t *testing.T, tbl *MarkdownTable, match map[string]string) map[string]string {
	t.Helper()
	if tbl == nil {
		t.Fatalf("t2FindTableRow: nil table")
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

func t2WriteMarkerScript(t *testing.T, scriptBody string) (wrapperPath string, marker string) {
	t.Helper()
	marker = newRunMarker(t)
	dir := t.TempDir()
	wrapperPath = filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))
	writeExecutableFile(t, wrapperPath, []byte(scriptBody))
	return wrapperPath, marker
}

func t2WriteLaunchSentinelWrapper(t *testing.T, sentinelPath string, proxyArgs ...string) (wrapperPath string, marker string) {
	t.Helper()
	if proxyBinaryPath == "" {
		t.Fatalf("t2WriteLaunchSentinelWrapper: proxyBinaryPath not initialized")
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

func e2eFormatFailureDiagnostics(rep *ParsedReport, stderr string) string {
	var b strings.Builder
	b.WriteString("=== E2E failure diagnostics ===")
	if rep != nil {
		fmt.Fprintf(&b, "\nInfraError: %q\nTimedOut: %v", rep.InfraError, rep.TimedOut)
		if rep.Summary != nil {
			fmt.Fprintf(&b, "\nSummary: Total=%d Pass=%d Fail=%d", rep.Summary.Total, rep.Summary.Pass, rep.Summary.Fail)
		}
		names := make([]string, 0, len(rep.Targets))
		for k := range rep.Targets {
			names = append(names, k)
		}
		slices.Sort(names)
		for _, k := range names {
			tr := rep.Targets[k]
			fmt.Fprintf(&b, "\nTarget[%q].TargetError: %q (TimedOut=%v)", k, tr.TargetError, tr.TimedOut)
		}
	}
	fmt.Fprintf(&b, "\nStderr:\n%s", stderr)
	return b.String()
}

func t2ValidateScenarioFailureReport(rep *ParsedReport, stderr string) error {
	if rep == nil || rep.Summary == nil || rep.Summary.Total <= 0 {
		return fmt.Errorf("expected scenario-level failure with Summary.Total > 0\n%s", e2eFormatFailureDiagnostics(rep, stderr))
	}
	if rep.InfraError != "" {
		return fmt.Errorf("unexpected InfraError %q on scenario failure\n%s", rep.InfraError, e2eFormatFailureDiagnostics(rep, stderr))
	}
	if tr := rep.Targets["default"]; tr != nil && (tr.TargetError != "" || tr.TimedOut) {
		return fmt.Errorf("unexpected target-level error/timeout on scenario failure (TargetError=%q TimedOut=%v)\n%s",
			tr.TargetError, tr.TimedOut, e2eFormatFailureDiagnostics(rep, stderr))
	}
	return nil
}

func t2RunCLIExpectExit1(t *testing.T, timeout time.Duration, proxyArgs []string, extraCLIArgs ...string) (CLIResult, *ParsedReport, *ParsedMarkdownReport) {
	t.Helper()
	wrapper, _ := writeProxyWrapper(t, proxyArgs...)
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "report.json")
	mdPath := filepath.Join(dir, "report.md")

	args := make([]string, 0, 5+len(extraCLIArgs))
	args = append(args, "--proxy-cmd="+wrapper, "--report-json="+jsonPath, "--report-md="+mdPath)
	args = append(args, extraCLIArgs...)

	res := runCLI(t, timeout, args...)
	if res.ExitCode != 1 || res.TimedOut {
		t.Fatalf("expected failure ExitCode=1 TimedOut=false, got exit=%d timedOut=%v err=%v\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.TimedOut, res.Err, res.Stdout, res.Stderr)
	}
	var rep *ParsedReport
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(e2eFormatFailureDiagnostics(rep, res.Stderr))
		}
	})
	rep = ReadJSONReport(t, jsonPath)
	AssertSummaryMatchesScenarios(t, rep)
	md := ReadMarkdownReport(t, mdPath)
	return res, rep, md
}

func t2RunExpectFailure(t *testing.T, timeout time.Duration, proxyArgs []string, extraCLIArgs ...string) (CLIResult, *ParsedReport, *ParsedMarkdownReport) {
	t.Helper()
	res, rep, md := t2RunCLIExpectExit1(t, timeout, proxyArgs, extraCLIArgs...)
	if err := t2ValidateScenarioFailureReport(rep, res.Stderr); err != nil {
		t.Fatalf("t2RunExpectFailure: %v", err)
	}
	return res, rep, md
}

func t2RunExpectStartupFailure(t *testing.T, timeout time.Duration, proxyArgs []string, extraCLIArgs ...string) (CLIResult, *ParsedReport, *ParsedMarkdownReport) {
	t.Helper()
	res, rep, md := t2RunCLIExpectExit1(t, timeout, proxyArgs, extraCLIArgs...)
	if rep.Summary == nil || rep.Summary.Total != 0 {
		t.Fatalf("t2RunExpectStartupFailure: expected Summary.Total == 0\n%s", e2eFormatFailureDiagnostics(rep, res.Stderr))
	}
	tr := rep.Targets["default"]
	if (tr == nil || tr.TargetError == "") && rep.InfraError == "" {
		t.Fatalf("t2RunExpectStartupFailure: expected non-empty TargetError or InfraError\n%s", e2eFormatFailureDiagnostics(rep, res.Stderr))
	}
	return res, rep, md
}

func t2RunExpectSuiteTimeoutFailure(t *testing.T, timeout time.Duration, proxyArgs []string, extraCLIArgs ...string) (CLIResult, *ParsedReport, *ParsedMarkdownReport) {
	t.Helper()
	res, rep, md := t2RunCLIExpectExit1(t, timeout, proxyArgs, extraCLIArgs...)
	if rep.Summary == nil || rep.Summary.Total <= 0 {
		t.Fatalf("t2RunExpectSuiteTimeoutFailure: expected Summary.Total > 0\n%s", e2eFormatFailureDiagnostics(rep, res.Stderr))
	}
	if rep.InfraError != "" {
		t.Fatalf("t2RunExpectSuiteTimeoutFailure: unexpected InfraError\n%s", e2eFormatFailureDiagnostics(rep, res.Stderr))
	}
	tr := rep.Targets["default"]
	if tr == nil || !tr.TimedOut || tr.TargetError == "" {
		t.Fatalf("t2RunExpectSuiteTimeoutFailure: expected TimedOut=true and non-empty TargetError\n%s", e2eFormatFailureDiagnostics(rep, res.Stderr))
	}
	return res, rep, md
}

func t2RunValidJSONForCompare(t *testing.T, dir, name string, proxyArgs []string, filter string) string {
	t.Helper()
	wrapper, _ := writeProxyWrapper(t, proxyArgs...)
	jsonPath := filepath.Join(dir, name)
	res := runCLI(t, 25*time.Second, "--proxy-cmd="+wrapper, "--filter="+filter, "--report-json="+jsonPath)
	if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
		t.Fatalf("t2RunValidJSONForCompare(%s) failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			name, res.ExitCode, res.TimedOut, res.Err, res.Stderr)
	}
	return jsonPath
}

func t2WaitPIDDead(t *testing.T, pid int) {
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

// --- E2E-F1 Negative/Boundary (6 tests) ---

func TestT2_F1_01_MissingProxyCmdFailsFast(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sentinel := filepath.Join(dir, "launched.log")
	wrapper, _ := t2WriteLaunchSentinelWrapper(t, sentinel, "--mode=conformant-go", "--fast-timings")

	cases := []struct {
		name      string
		args      []string
		wantErr   string
		reportOut string
	}{
		{
			name:      "no_mode_flags",
			args:      []string{"--filter=^PUB-01$", "--report-json=" + filepath.Join(dir, "no_mode.json")},
			wantErr:   "specify --proxy-cmd, --proxy-target, --compare, or --list",
			reportOut: filepath.Join(dir, "no_mode.json"),
		},
		{
			name:      "empty_proxy_cmd",
			args:      []string{"--proxy-cmd=", "--filter=^PUB-01$", "--report-json=" + filepath.Join(dir, "empty.json")},
			wantErr:   "--proxy-cmd cannot be empty",
			reportOut: filepath.Join(dir, "empty.json"),
		},
		{
			name:    "whitespace_proxy_cmd",
			args:    []string{"--proxy-cmd=   ", "--filter=^PUB-01$"},
			wantErr: "--proxy-cmd cannot be empty",
		},
		{
			name:    "orphan_proxy_arg_without_proxy_cmd",
			args:    []string{"--proxy-arg=--mode=conformant-go", "--filter=^PUB-01$"},
			wantErr: "--proxy-arg requires --proxy-cmd",
		},
		{
			name:      "combine_proxy_cmd_and_proxy_target",
			args:      []string{"--proxy-cmd=" + wrapper, "--proxy-target=go=" + wrapper, "--filter=^PUB-01$", "--report-json=" + filepath.Join(dir, "combine.json")},
			wantErr:   "cannot combine --proxy-cmd and --proxy-target",
			reportOut: filepath.Join(dir, "combine.json"),
		},
		{
			name:    "unterminated_quote_proxy_cmd",
			args:    []string{`--proxy-cmd="unterminated`, "--filter=^PUB-01$"},
			wantErr: "unterminated quote",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runCLI(t, 15*time.Second, tc.args...)
			t2AssertErrorExit(t, res, tc.wantErr)
			if res.Duration >= 3*time.Second {
				t.Errorf("%s took %v, expected fast pre-run rejection", tc.name, res.Duration)
			}
			if tc.reportOut != "" {
				t2AssertFileNotExist(t, tc.reportOut)
			}
		})
	}
	t2AssertFileNotExist(t, sentinel)
}

func TestT2_F1_02_NonExistentProxyBinaryFails(t *testing.T) {
	t.Parallel()

	missingBin := filepath.Join(t.TempDir(), "does-not-exist-e2e-proxy-xyz")

	t.Run("via_proxy_cmd", func(t *testing.T) {
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")
		mdPath := filepath.Join(dir, "report.md")

		res := runCLI(
			t, 15*time.Second,
			"--proxy-cmd="+missingBin,
			"--filter=^PUB-01$",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v err=%v", res.ExitCode, res.TimedOut, res.Err)
		}
		if !strings.Contains(res.Stderr, "Target default error:") || !strings.Contains(res.Stderr, "no such file or directory") {
			t.Errorf("stderr missing expected target error; got:\n%s", res.Stderr)
		}

		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "single", false)
		if rep.Target != "default" {
			t.Errorf("Target = %q, want default", rep.Target)
		}
		if rep.InfraError != "" || rep.Raw["infra_error"] != nil {
			t.Errorf("expected empty/nil infra_error for target startup failure, got %q (%#v)", rep.InfraError, rep.Raw["infra_error"])
		}
		if rep.Capabilities != nil || len(rep.Scenarios) != 0 {
			t.Errorf("expected nil capabilities and 0 scenarios, got caps=%v scenarios=%d", rep.Capabilities, len(rep.Scenarios))
		}
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{}) {
			t.Errorf("Summary = %+v, want all zeros", rep.Summary)
		}
		tr := rep.Targets["default"]
		if tr == nil || !strings.Contains(tr.TargetError, "no such file or directory") {
			t.Fatalf("Targets[default].TargetError unexpected: %+v", tr)
		}

		md := ReadMarkdownReport(t, mdPath)
		if res.Stdout != md.Raw {
			t.Errorf("stdout Markdown differs from --report-md output")
		}
		if len(md.Tables) != 2 {
			t.Errorf("startup-failure Markdown tables = %d, want 2 (Target & Capabilities omitted)", len(md.Tables))
		}
		sumTbl := t2FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t2FindTableRow(t, sumTbl, map[string]string{"Total": "0", "Pass": "0", "Fail": "0"})
		scenTbl := t2FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		if len(scenTbl.Rows) != 0 {
			t.Errorf("expected 0 Scenario Results rows, got %d", len(scenTbl.Rows))
		}
		if len(md.DiagnosticsBullets) != 1 || !strings.Contains(md.DiagnosticsBullets[0], "Target default Error") || !strings.Contains(md.DiagnosticsBullets[0], "no such file or directory") {
			t.Errorf("DiagnosticsBullets unexpected: %v", md.DiagnosticsBullets)
		}
	})

	t.Run("via_proxy_target", func(t *testing.T) {
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")
		mdPath := filepath.Join(dir, "report.md")

		res := runCLI(
			t, 15*time.Second,
			"--proxy-target=bad="+missingBin,
			"--filter=^PUB-01$",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v", res.ExitCode, res.TimedOut)
		}
		if !strings.Contains(res.Stderr, "Target bad error:") {
			t.Errorf("stderr missing Target bad error:\n%s", res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "single", false)
		if rep.Target != "bad" || rep.Summary == nil || rep.Summary.Total != 0 || len(rep.Scenarios) != 0 {
			t.Errorf("unexpected report state: target=%q summary=%+v scenarios=%d", rep.Target, rep.Summary, len(rep.Scenarios))
		}
		if tr := rep.Targets["bad"]; tr == nil || !strings.Contains(tr.TargetError, "no such file or directory") {
			t.Errorf("Targets[bad] unexpected: %+v", tr)
		}
	})
}

func TestT2_F1_03_ProxyCrashOnStartupDetected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		proxyArgs      []string
		wantErrSubs    []string
		wantAnyErrSubs []string
	}{
		{
			name:        "crash_startup",
			proxyArgs:   []string{"--mode=crash-startup"},
			wantErrSubs: []string{"exit_code=2", "simulated startup crash", "before READY: EOF"},
		},
		{
			name:           "crash_phase_ready",
			proxyArgs:      []string{"--crash-phase=ready"},
			wantErrSubs:    []string{"exit_code=2", "simulated post-ready crash"},
			wantAnyErrSubs: []string{"GetCapabilities probe failed", "proxy exited immediately after READY"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, rep, md := t2RunExpectStartupFailure(t, 15*time.Second, tc.proxyArgs, "--filter=^PUB-01$")
			t2AssertJSONV1Base(t, rep, "single", false)

			if !strings.Contains(res.Stderr, "Target default error:") || !strings.Contains(res.Stderr, "exit_code=2") {
				t.Errorf("stderr missing expected crash details:\n%s", res.Stderr)
			}
			if rep.InfraError != "" || rep.Raw["infra_error"] != nil {
				t.Errorf("expected nil infra_error, got %q", rep.InfraError)
			}
			if rep.Summary == nil || *rep.Summary != (ParsedSummary{}) || len(rep.Scenarios) != 0 {
				t.Errorf("expected 0 scenarios and zero summary, got sum=%+v len=%d", rep.Summary, len(rep.Scenarios))
			}
			tr := rep.Targets["default"]
			if tr == nil {
				t.Fatalf("Targets[default] missing")
			}
			for _, sub := range tc.wantErrSubs {
				if !strings.Contains(tr.TargetError, sub) {
					t.Errorf("TargetError %q missing %q", tr.TargetError, sub)
				}
			}
			if len(tc.wantAnyErrSubs) > 0 && !slices.ContainsFunc(tc.wantAnyErrSubs, func(sub string) bool {
				return strings.Contains(tr.TargetError, sub)
			}) {
				t.Errorf("TargetError %q missing any of %v", tr.TargetError, tc.wantAnyErrSubs)
			}

			if len(md.Tables) != 2 {
				t.Errorf("expected 2 Markdown tables on startup crash, got %d", len(md.Tables))
			}
			sumTbl := t2FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
			t2FindTableRow(t, sumTbl, map[string]string{"Total": "0", "Pass": "0", "Fail": "0"})
			if len(md.DiagnosticsBullets) != 1 || !strings.Contains(md.DiagnosticsBullets[0], "Target default Error") {
				t.Errorf("DiagnosticsBullets unexpected: %v", md.DiagnosticsBullets)
			}
		})
	}
}

func TestT2_F1_04_MalformedReadyLineRejected(t *testing.T) {
	t.Parallel()

	payloadDir := t.TempDir()
	longLineFile := filepath.Join(payloadDir, "long_ready.txt")
	if err := os.WriteFile(longLineFile, []byte(strings.Repeat("X", 9000)+"\n"), 0o644); err != nil {
		t.Fatalf("write long line payload: %v", err)
	}

	makeScript := func(t *testing.T, body string) string {
		t.Helper()
		w, _ := t2WriteMarkerScript(t, body)
		return w
	}

	cases := []struct {
		name       string
		wrapperFn  func(t *testing.T) string
		wantErrSub string
	}{
		{
			name: "bad_ready_host_port",
			wrapperFn: func(t *testing.T) string {
				w, _ := writeProxyWrapper(t, "--mode=bad-ready")
				return w
			},
			wantErrSub: `malformed READY endpoint "READY not-a-host-port"`,
		},
		{
			name: "silent_exit_zero_before_ready",
			wrapperFn: func(t *testing.T) string {
				w, _ := writeProxyWrapper(t, "--mode=malformed-ready", "--fault=silent-exit")
				return w
			},
			wantErrSub: "before READY: EOF",
		},
		{
			name: "invalid_port_99999_with_sleep",
			wrapperFn: func(t *testing.T) string {
				return makeScript(t, "#!/bin/sh\nprintf 'READY 127.0.0.1:99999\\n'\nsleep 30\n")
			},
			wantErrSub: `malformed READY endpoint "READY 127.0.0.1:99999"`,
		},
		{
			name: "port_zero_with_sleep",
			wrapperFn: func(t *testing.T) string {
				return makeScript(t, "#!/bin/sh\nprintf 'READY 127.0.0.1:0\\n'\nsleep 30\n")
			},
			wantErrSub: `malformed READY endpoint "READY 127.0.0.1:0"`,
		},
		{
			name: "pre_ready_line_flood_exceeds_64",
			wrapperFn: func(t *testing.T) string {
				return makeScript(t, "#!/bin/sh\ni=0\nwhile [ \"$i\" -lt 70 ]; do\n  printf 'noise %d\\n' \"$i\"\n  i=$((i+1))\ndone\nsleep 30\n")
			},
			wantErrSub: "exceeded 64 pre-READY stdout lines",
		},
		{
			name: "oversized_pre_ready_line_9000B",
			wrapperFn: func(t *testing.T) string {
				return makeScript(t, fmt.Sprintf("#!/bin/sh\ncat %s\nsleep 30\n", shellQuote(longLineFile)))
			},
			wantErrSub: "exceeded 8192 bytes before READY",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := tc.wrapperFn(t)
			dir := t.TempDir()
			jsonPath := filepath.Join(dir, "report.json")
			res := runCLI(t, 15*time.Second, "--proxy-cmd="+w, "--filter=^PUB-01$", "--report-json="+jsonPath)
			if res.ExitCode != 1 || res.TimedOut {
				t.Fatalf("%s expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v\nstderr:\n%s",
					tc.name, res.ExitCode, res.TimedOut, res.Stderr)
			}
			if res.Duration >= 5*time.Second {
				t.Errorf("%s took %v, expected prompt rejection < 5s", tc.name, res.Duration)
			}
			rep := ReadJSONReport(t, jsonPath)
			t2AssertJSONV1Base(t, rep, "single", false)
			if rep.Summary == nil || rep.Summary.Total != 0 {
				t.Errorf("%s expected Summary.Total=0, got %+v", tc.name, rep.Summary)
			}
			tr := rep.Targets["default"]
			if tr == nil || !strings.Contains(tr.TargetError, tc.wantErrSub) {
				t.Errorf("%s TargetError = %+v, want substring %q", tc.name, tr, tc.wantErrSub)
			}
		})
	}
}

func TestT2_F1_05_ProxyIgnoresShutdownKilledCleanly(t *testing.T) {
	t.Parallel()

	t.Run("ignores_shutdown_rpc_exits_on_sigterm", func(t *testing.T) {
		w, _ := writeProxyWrapper(t, "--mode=ignore-shutdown", "--fast-timings")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")
		res := runCLI(t, 20*time.Second, "--proxy-cmd="+w, "--filter=^PUB-01$", "--report-json="+jsonPath)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		if res.Duration < 900*time.Millisecond || res.Duration >= 5*time.Second {
			t.Errorf("duration = %v, expected waitGone(1s)+SIGTERM in [900ms, 5s)", res.Duration)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "single", false)
		if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
			t.Errorf("PUB-01 status = %q, want PASS", sc.Status)
		}
	})

	t.Run("ignores_shutdown_and_sigterm_force_killed", func(t *testing.T) {
		dir := t.TempDir()
		marker := newRunMarker(t)
		pidsFile := filepath.Join(dir, "pids.txt")
		stderrFile := filepath.Join(dir, "proxy.stderr")
		jsonPath := filepath.Join(dir, "report.json")
		wrapper := filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))

		script := fmt.Sprintf(
			"#!/bin/sh\ntrap '' TERM\nsleep 60 &\nsleep_pid=$!\n%s %s '--mode=ignore-shutdown' '--fault=ignore-sigterm' '--fast-timings' \"$@\" 2>%s &\nproxy_pid=$!\nprintf '%%s %%s %%s\\n' \"$$\" \"$proxy_pid\" \"$sleep_pid\" > %s\nwait \"$proxy_pid\"\n",
			shellQuote(proxyBinaryPath),
			shellQuote("--marker="+marker),
			shellQuote(stderrFile),
			shellQuote(pidsFile),
		)
		writeExecutableFile(t, wrapper, []byte(script))

		res := runCLI(t, 30*time.Second, "--proxy-cmd="+wrapper, "--filter=^PUB-01$", "--report-json="+jsonPath)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		if res.Duration < 5500*time.Millisecond {
			t.Errorf("duration = %v, expected >= 5.5s (1s waitGone + 5s DefaultShutdownTimeout -> SIGKILL)", res.Duration)
		}

		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "single", false)
		if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
			t.Errorf("PUB-01 status = %q, want PASS", sc.Status)
		}

		proxyStderr, err := os.ReadFile(stderrFile)
		if err != nil {
			t.Fatalf("read proxy stderr file: %v", err)
		}
		if !strings.Contains(string(proxyStderr), "ignoring SIGTERM due to fault") {
			t.Errorf("proxy stderr missing ignore-sigterm proof; got %q", string(proxyStderr))
		}

		pidsRaw, err := os.ReadFile(pidsFile)
		if err != nil {
			t.Fatalf("read pids file: %v", err)
		}
		fields := strings.Fields(string(pidsRaw))
		if len(fields) != 3 {
			t.Fatalf("expected 3 PIDs in %q, got %q", pidsFile, string(pidsRaw))
		}
		for _, f := range fields {
			pid, pErr := strconv.Atoi(f)
			if pErr != nil || pid <= 0 {
				t.Fatalf("invalid PID %q in %q", f, string(pidsRaw))
			}
			t2WaitPIDDead(t, pid)
		}
	})
}

func TestT2_F1_06_MidScenarioProxyCrashDetectedAndRestarted(t *testing.T) {
	t.Parallel()

	t.Run("crash_phase_scenario", func(t *testing.T) {
		res, rep, md := t2RunExpectFailure(
			t, 30*time.Second,
			[]string{"--mode=conformant-go", "--crash-phase=scenario", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|PULL-01)$",
		)
		t2AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 3, Fail: 3, ErrorCount: 3}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if !strings.Contains(res.Stderr, "[default] PUB-01: ERROR") {
			t.Errorf("stderr missing [default] PUB-01: ERROR:\n%s", res.Stderr)
		}

		pub01 := t2MustScenario(t, rep, "PUB-01")
		if pub01.Status != StatusFail || pub01.StatusDetail != StatusError || pub01.ProxyRestarted {
			t.Errorf("PUB-01 unexpected: %+v", *pub01)
		}
		if !strings.Contains(pub01.Error, "simulated scenario crash at RPC #1") {
			t.Errorf("PUB-01 Error %q missing simulated crash text", pub01.Error)
		}
		for _, id := range []string{"PORD-02", "PULL-01"} {
			sc := t2MustScenario(t, rep, id)
			if sc.Status != StatusFail || sc.StatusDetail != StatusError || !sc.ProxyRestarted {
				t.Errorf("%s expected FAIL/ERROR with ProxyRestarted=true, got %+v", id, *sc)
			}
		}

		if !md.HasFailureBreakdown || md.ErrorCount != 3 || md.TimeoutCount != 0 {
			t.Errorf("MD failure breakdown unexpected: has=%v timeout=%d err=%d raw=%q",
				md.HasFailureBreakdown, md.TimeoutCount, md.ErrorCount, md.FailureBreakdown)
		}
		scenTbl := t2FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		for _, id := range []string{"PUB-01", "PORD-02", "PULL-01"} {
			t2FindTableRow(t, scenTbl, map[string]string{"Scenario": id, "Status": "FAIL (ERROR)"})
		}
		if len(md.DiagnosticsBullets) != 3 {
			t.Errorf("DiagnosticsBullets len = %d, want 3: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
	})

	t.Run("crash_after_2_recovers_subsequent_scenario", func(t *testing.T) {
		_, rep, md := t2RunExpectFailure(
			t, 30*time.Second,
			[]string{"--mode=conformant-go", "--fault=crash-after=2", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02|PULL-01)$",
		)
		t2AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 3, Pass: 1, Fail: 2, ErrorCount: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}

		pub01 := t2MustScenario(t, rep, "PUB-01")
		pord02 := t2MustScenario(t, rep, "PORD-02")
		pull01 := t2MustScenario(t, rep, "PULL-01")
		if pub01.Status != StatusFail || pub01.StatusDetail != StatusError || pub01.ProxyRestarted {
			t.Errorf("PUB-01 unexpected: %+v", *pub01)
		}
		if !strings.Contains(pub01.Error, "simulated crash-after=2") {
			t.Errorf("PUB-01 Error %q missing simulated crash-after=2", pub01.Error)
		}
		if pord02.Status != StatusFail || pord02.StatusDetail != StatusError || !pord02.ProxyRestarted {
			t.Errorf("PORD-02 expected FAIL/ERROR with ProxyRestarted=true, got %+v", *pord02)
		}
		if pull01.Status != StatusPass || !pull01.ProxyRestarted {
			t.Errorf("PULL-01 expected PASS with ProxyRestarted=true, got %+v", *pull01)
		}

		sumTbl := t2FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t2FindTableRow(t, sumTbl, map[string]string{"Total": "3", "Pass": "1", "Fail": "2"})
		if !md.HasFailureBreakdown || md.ErrorCount != 2 || md.TimeoutCount != 0 {
			t.Errorf("MD failure breakdown unexpected: has=%v timeout=%d err=%d",
				md.HasFailureBreakdown, md.TimeoutCount, md.ErrorCount)
		}
		scenTbl := t2FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PUB-01", "Status": "FAIL (ERROR)"})
		t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PORD-02", "Status": "FAIL (ERROR)"})
		t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PULL-01", "Status": StatusPass})
		if len(md.DiagnosticsBullets) != 2 {
			t.Errorf("DiagnosticsBullets len = %d, want 2: %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
		}
	})
}

// --- E2E-F2 Negative/Boundary (6 tests) ---

func TestT2_F2_01_MissingPubSubRPCFailsScenario(t *testing.T) {
	t.Parallel()

	t.Run("fail_immediate", func(t *testing.T) {
		_, rep, md := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=missing-rpc", "--fast-timings"},
			"--filter=^(PUB-01|PULL-04)$",
			"--scenario-timeout=5s",
		)
		wantSum := ParsedSummary{Total: 2, Fail: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		pub01 := t2MustScenario(t, rep, "PUB-01")
		if pub01.Status != StatusFail || !strings.Contains(pub01.Error, "call count mismatch: want 1, got 0") {
			t.Errorf("PUB-01 unexpected: %+v", *pub01)
		}
		pull04 := t2MustScenario(t, rep, "PULL-04")
		if pull04.Status != StatusFail || !strings.Contains(pull04.Error, "UnaryPull returned 0 messages, want 1") {
			t.Errorf("PULL-04 unexpected: %+v", *pull04)
		}
		scenTbl := t2FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PUB-01", "Status": StatusFail})
		t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PULL-04", "Status": StatusFail})
	})

	t.Run("timeout_stream", func(t *testing.T) {
		_, rep, md := t2RunExpectFailure(
			t, 20*time.Second,
			[]string{"--mode=missing-rpc", "--fast-timings"},
			"--filter=^PULL-01$",
			"--scenario-timeout=400ms",
		)
		wantSum := ParsedSummary{Total: 1, Fail: 1, TimeoutCount: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		pull01 := t2MustScenario(t, rep, "PULL-01")
		if pull01.Status != StatusFail || pull01.StatusDetail != StatusTimeout || !strings.Contains(pull01.Error, "TIMEOUT:") {
			t.Errorf("PULL-01 unexpected: %+v", *pull01)
		}
		if !md.HasFailureBreakdown || md.TimeoutCount != 1 {
			t.Errorf("MD timeout breakdown unexpected: has=%v to=%d", md.HasFailureBreakdown, md.TimeoutCount)
		}
		scenTbl := t2FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PULL-01", "Status": "FAIL (TIMEOUT)"})
	})
}

func TestT2_F2_02_WrongPayloadOrAttrsFailsScenario(t *testing.T) {
	t.Parallel()

	t.Run("bare_wrong_rpc", func(t *testing.T) {
		_, rep, md := t2RunExpectFailure(
			t, 30*time.Second,
			[]string{"--mode=wrong-rpc", "--fast-timings"},
			"--filter=^(PUB-01|PULL-01|PULL-04|TRAC-01|TRAC-03)$",
		)
		wantSum := ParsedSummary{Total: 5, Pass: 2, Fail: 3}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, id := range []string{"PULL-01", "PULL-04"} {
			if sc := t2MustScenario(t, rep, id); sc.Status != StatusPass {
				t.Errorf("%s expected PASS under wrong-rpc, got %+v", id, *sc)
			}
		}
		for _, id := range []string{"PUB-01", "TRAC-01", "TRAC-03"} {
			sc := t2MustScenario(t, rep, id)
			if sc.Status != StatusFail || !strings.Contains(sc.Error, "data mismatch") || !strings.Contains(sc.Error, "__e2e_corrupted_attr__") {
				t.Errorf("%s expected data+attr mismatch, got %+v", id, *sc)
			}
		}
		if len(md.DiagnosticsBullets) != 3 {
			t.Errorf("DiagnosticsBullets len = %d, want 3", len(md.DiagnosticsBullets))
		}
	})

	t.Run("wrong_payload_only", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 20*time.Second,
			[]string{"--mode=conformant-go", "--fault=wrong-payload", "--fast-timings"},
			"--filter=^PUB-01$",
		)
		sc := t2MustScenario(t, rep, "PUB-01")
		if sc.Status != StatusFail || !strings.Contains(sc.Error, "message[0].data mismatch") || strings.Contains(sc.Error, "__e2e_corrupted_attr__") {
			t.Errorf("PUB-01 wrong-payload unexpected: %+v", *sc)
		}
	})

	t.Run("wrong_attrs_only", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 20*time.Second,
			[]string{"--mode=conformant-go", "--fault=wrong-attrs", "--fast-timings"},
			"--filter=^PUB-01$",
		)
		sc := t2MustScenario(t, rep, "PUB-01")
		if sc.Status != StatusFail || !strings.Contains(sc.Error, "message[0].attributes mismatch") ||
			!strings.Contains(sc.Error, "__e2e_corrupted_attr__") || strings.Contains(sc.Error, "data mismatch") {
			t.Errorf("PUB-01 wrong-attrs unexpected: %+v", *sc)
		}
	})
}

func TestT2_F2_03_WrongOrderingKeyOrMetadataFails(t *testing.T) {
	t.Parallel()

	t.Run("wrong_ordering_key", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=wrong-rpc", "--fault=wrong-ordering-key", "--fast-timings"},
			"--filter=^(PUB-01|PORD-02)$",
			"--scenario-timeout=600ms",
		)
		wantSum := ParsedSummary{Total: 2, Pass: 1, Fail: 1, TimeoutCount: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
			t.Errorf("PUB-01 unexpected: %+v", *sc)
		}
		pord02 := t2MustScenario(t, rep, "PORD-02")
		if pord02.Status != StatusFail || pord02.StatusDetail != StatusTimeout || !strings.Contains(pord02.Error, "TIMEOUT:") {
			t.Errorf("PORD-02 unexpected: %+v", *pord02)
		}
	})

	t.Run("allow_unordered_key_and_divergent_client", func(t *testing.T) {
		for _, proxyArgs := range [][]string{
			{"--mode=conformant-go", "--fault=allow-unordered-key", "--fast-timings"},
			{"--mode=divergent-client", "--fast-timings"},
		} {
			_, rep, _ := t2RunExpectFailure(t, 20*time.Second, proxyArgs, "--filter=^PORD-01$")
			sc := t2MustScenario(t, rep, "PORD-01")
			if sc.Status != StatusFail || !strings.Contains(sc.Error, "expected ordering_key publish with enable_message_ordering=false to fail") {
				t.Errorf("PORD-01 (%v) unexpected: %+v", proxyArgs, *sc)
			}
		}
	})

	t.Run("missing_routing_header", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=wrong-rpc", "--fault=missing-routing-header", "--fast-timings"},
			"--filter=^(PUB-01|PULL-01|PULL-04)$",
		)
		wantSum := ParsedSummary{Total: 3, Fail: 3}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, id := range []string{"PUB-01", "PULL-01", "PULL-04"} {
			sc := t2MustScenario(t, rep, id)
			if sc.Status != StatusFail || !strings.Contains(sc.Error, "x-goog-request-params mismatch") || !strings.Contains(sc.Error, "got []") {
				t.Errorf("%s expected missing x-goog-request-params, got %+v", id, *sc)
			}
		}
	})

	t.Run("missing_custom_headers", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=wrong-rpc", "--fault=missing-custom-headers", "--fast-timings"},
			"--filter=^(PUB-01|CFG-02)$",
		)
		wantSum := ParsedSummary{Total: 2, Pass: 1, Fail: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass {
			t.Errorf("PUB-01 unexpected: %+v", *sc)
		}
		cfg02 := t2MustScenario(t, rep, "CFG-02")
		if cfg02.Status != StatusFail || !strings.Contains(cfg02.Error, `custom header "x-custom-test" = [], want "m3e-val"`) {
			t.Errorf("CFG-02 unexpected: %+v", *cfg02)
		}
	})
}

func TestT2_F2_04_OutOfOrderPublishFailsScenario(t *testing.T) {
	t.Parallel()

	t.Run("bare_out_of_order_rpc", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=out-of-order-rpc", "--fast-timings"},
			"--filter=^(PORD-02|PULL-01|LEASE-01)$",
		)
		wantSum := ParsedSummary{Total: 3, Fail: 3}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t2MustScenario(t, rep, "PORD-02"); sc.Status != StatusFail || !strings.Contains(sc.Error, "ordered Publish FIFO mismatch") {
			t.Errorf("PORD-02 unexpected: %+v", *sc)
		}
		if sc := t2MustScenario(t, rep, "PULL-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "premature ack observed before SettleMessageCommand") {
			t.Errorf("PULL-01 unexpected: %+v", *sc)
		}
		if sc := t2MustScenario(t, rep, "LEASE-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "delivered metadata mismatch") {
			t.Errorf("LEASE-01 unexpected: %+v", *sc)
		}
	})

	t.Run("out_of_order_publish_only", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 20*time.Second,
			[]string{"--mode=conformant-go", "--fault=out-of-order-publish", "--fast-timings"},
			"--filter=^PORD-02$",
		)
		if sc := t2MustScenario(t, rep, "PORD-02"); sc.Status != StatusFail || !strings.Contains(sc.Error, "ordered Publish FIFO mismatch") {
			t.Errorf("PORD-02 unexpected: %+v", *sc)
		}
	})

	t.Run("out_of_order_ack_only", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=conformant-go", "--fault=out-of-order-ack", "--fast-timings"},
			"--filter=^(PULL-01|LEASE-01)$",
		)
		if sc := t2MustScenario(t, rep, "PULL-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "premature ack observed before SettleMessageCommand") {
			t.Errorf("PULL-01 unexpected: %+v", *sc)
		}
		if sc := t2MustScenario(t, rep, "LEASE-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "delivered metadata mismatch") {
			t.Errorf("LEASE-01 unexpected: %+v", *sc)
		}
	})
}

func TestT2_F2_05_UnexpectedExtraOrWrongRPCFails(t *testing.T) {
	t.Parallel()

	t.Run("wrong_rpc_method_immediate", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=wrong-rpc", "--fault=wrong-rpc-method", "--fast-timings"},
			"--filter=^(PUB-01|PULL-04)$",
		)
		wantSum := ParsedSummary{Total: 2, Fail: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "topic mismatch") || !strings.Contains(sc.Error, "-wrong") {
			t.Errorf("PUB-01 unexpected: %+v", *sc)
		}
		if sc := t2MustScenario(t, rep, "PULL-04"); sc.Status != StatusFail || !strings.Contains(sc.Error, "subscription mismatch") || !strings.Contains(sc.Error, "-wrong") {
			t.Errorf("PULL-04 unexpected: %+v", *sc)
		}
	})

	t.Run("wrong_rpc_method_streaming_timeout", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 20*time.Second,
			[]string{"--mode=wrong-rpc", "--fault=wrong-rpc-method", "--fast-timings"},
			"--filter=^PULL-01$",
			"--scenario-timeout=600ms",
		)
		if sc := t2MustScenario(t, rep, "PULL-01"); sc.Status != StatusFail || sc.StatusDetail != StatusTimeout {
			t.Errorf("PULL-01 expected FAIL/TIMEOUT, got %+v", *sc)
		}
	})

	t.Run("extra_unexpected_rpc", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=wrong-rpc", "--fault=extra-unexpected-rpc", "--fast-timings"},
			"--filter=^(PUB-01|PULL-01|PULL-04)$",
		)
		wantSum := ParsedSummary{Total: 3, Pass: 1, Fail: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "call count mismatch: want 1, got 2") {
			t.Errorf("PUB-01 unexpected: %+v", *sc)
		}
		if sc := t2MustScenario(t, rep, "PULL-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "expected exactly 1 acknowledged ack_id occurrence, got 2") {
			t.Errorf("PULL-01 unexpected: %+v", *sc)
		}
		if sc := t2MustScenario(t, rep, "PULL-04"); sc.Status != StatusPass {
			t.Errorf("PULL-04 unexpected: %+v", *sc)
		}
	})
}

func TestT2_F2_06_BatchSplitLimitViolationFails(t *testing.T) {
	t.Parallel()

	t.Run("ack_batch_split_limit_violation", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=conformant-go", "--fault=extra-unexpected-rpc", "--fast-timings"},
			"--filter=^LEASE-01$",
		)
		sc := t2MustScenario(t, rep, "LEASE-01")
		if sc.Status != StatusFail ||
			!strings.Contains(sc.Error, "Acknowledge request size") ||
			!strings.Contains(sc.Error, "exceeds 524288 bytes") {
			t.Errorf("LEASE-01 batch split violation unexpected: %+v", *sc)
		}
	})

	t.Run("wrong_rpc_composes_header_faults", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=wrong-rpc", "--fault=missing-routing-header,missing-custom-headers", "--fast-timings"},
			"--filter=^(PUB-01|CFG-02)$",
		)
		wantSum := ParsedSummary{Total: 2, Fail: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "x-goog-request-params mismatch") {
			t.Errorf("PUB-01 unexpected: %+v", *sc)
		}
		if sc := t2MustScenario(t, rep, "CFG-02"); sc.Status != StatusFail ||
			!strings.Contains(sc.Error, "x-goog-request-params mismatch") ||
			!strings.Contains(sc.Error, `custom header "x-custom-test"`) {
			t.Errorf("CFG-02 unexpected: %+v", *sc)
		}
	})
}

// --- E2E-F3 Negative/Boundary (5 tests) ---

func TestT2_F3_01_MonitoringEnabledMissingTracesFails(t *testing.T) {
	t.Parallel()

	_, rep, md := t2RunExpectFailure(
		t, 30*time.Second,
		[]string{"--mode=missing-traces", "--fast-timings"},
		"--filter=^(PUB-01|TRAC-01|TRAC-01b|TRAC-03|TRAC-04)$",
	)
	if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || !*rep.Capabilities.SupportsMonitoring {
		t.Fatalf("expected SupportsMonitoring=true, got %+v", rep.Capabilities)
	}
	wantSum := ParsedSummary{Total: 5, Pass: 2, Fail: 3}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	for _, passID := range []string{"PUB-01", "TRAC-01b"} {
		sc := t2MustScenario(t, rep, passID)
		if sc.Status != StatusPass || sc.TraceStatus != StatusPass {
			t.Errorf("%s expected PASS/PASS, got %+v", passID, *sc)
		}
	}
	for _, failID := range []string{"TRAC-01", "TRAC-03", "TRAC-04"} {
		sc := t2MustScenario(t, rep, failID)
		if sc.Status != StatusFail || sc.TraceStatus != "" {
			t.Errorf("%s expected FAIL with empty TraceStatus (wire failure before DrainTraces), got %+v", failID, *sc)
		}
		if _, hasTS := sc.Raw["trace_status"]; hasTS {
			t.Errorf("%s unexpected trace_status key in Raw: %v", failID, sc.Raw["trace_status"])
		}
	}
	if !strings.Contains(t2MustScenario(t, rep, "TRAC-01").Error, `missing "googclient_traceparent"`) ||
		!strings.Contains(t2MustScenario(t, rep, "TRAC-03").Error, `missing "googclient_traceparent"`) ||
		!strings.Contains(t2MustScenario(t, rep, "TRAC-04").Error, "traceparent is empty") {
		t.Errorf("unexpected trace errors: TRAC-01=%q TRAC-03=%q TRAC-04=%q",
			t2MustScenario(t, rep, "TRAC-01").Error,
			t2MustScenario(t, rep, "TRAC-03").Error,
			t2MustScenario(t, rep, "TRAC-04").Error)
	}
	if len(md.DiagnosticsBullets) != 3 {
		t.Errorf("DiagnosticsBullets len = %d, want 3", len(md.DiagnosticsBullets))
	}
}

func TestT2_F3_02_MonitoringEnabledInvalidSpanKindOrNameFails(t *testing.T) {
	t.Parallel()

	t.Run("bare_invalid_traces", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=invalid-traces", "--fast-timings"},
			"--filter=^(PUB-01|TRAC-01|TRAC-03)$",
		)
		if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass || sc.TraceStatus != StatusPass {
			t.Errorf("PUB-01 unexpected: %+v", *sc)
		}
		for _, id := range []string{"TRAC-01", "TRAC-03"} {
			sc := t2MustScenario(t, rep, id)
			if sc.Status != StatusFail || sc.TraceStatus != "" || !strings.Contains(sc.Error, `missing "googclient_traceparent"`) {
				t.Errorf("%s bare invalid-traces unexpected: %+v", id, *sc)
			}
		}
	})

	cases := []struct {
		name      string
		proxyArgs []string
		wantTok   map[string]string
	}{
		{
			name:      "bad_span_kind",
			proxyArgs: []string{"--mode=invalid-traces", "--fault=bad-span-kind", "--fast-timings"},
			wantTok:   map[string]string{"TRAC-01": "WRONG_SPAN_KIND", "TRAC-03": "WRONG_SPAN_KIND", "TRAC-04": "WRONG_SPAN_KIND"},
		},
		{
			name:      "bad_span_name",
			proxyArgs: []string{"--mode=invalid-traces", "--fault=bad-span-name", "--fast-timings"},
			wantTok:   map[string]string{"TRAC-01": "MISSING_SPAN", "TRAC-03": "MISSING_SPAN", "TRAC-04": "CUSTOM_CHECK_FAILED"},
		},
		{
			name:      "missing_links",
			proxyArgs: []string{"--mode=invalid-traces", "--fault=missing-links", "--fast-timings"},
			wantTok:   map[string]string{"TRAC-01": "MISSING_LINK", "TRAC-03": "MISSING_LINK", "TRAC-04": "CUSTOM_CHECK_FAILED"},
		},
		{
			name:      "bad_status_code",
			proxyArgs: []string{"--mode=conformant-go", "--fault=bad-status-code", "--fast-timings"},
			wantTok:   map[string]string{"TRAC-01": "WRONG_STATUS_CODE", "TRAC-03": "WRONG_STATUS_CODE", "TRAC-04": "WRONG_STATUS_CODE"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rep, md := t2RunExpectFailure(
				t, 25*time.Second,
				tc.proxyArgs,
				"--filter=^(TRAC-01|TRAC-03|TRAC-04)$",
			)
			wantSum := ParsedSummary{Total: 3, Fail: 3}
			if rep.Summary == nil || *rep.Summary != wantSum {
				t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
			}
			for id, wantErrTok := range tc.wantTok {
				sc := t2MustScenario(t, rep, id)
				if sc.Status != StatusFail || sc.TraceStatus != StatusFail {
					t.Errorf("%s expected status=FAIL trace_status=FAIL, got %+v", id, *sc)
				}
				if !strings.Contains(sc.Error, wantErrTok) {
					t.Errorf("%s Error %q missing %q", id, sc.Error, wantErrTok)
				}
			}
			if len(md.DiagnosticsBullets) != 3 {
				t.Errorf("DiagnosticsBullets len = %d, want 3", len(md.DiagnosticsBullets))
			}
		})
	}
}

func TestT2_F3_03_MonitoringEnabledMissingTraceparentFails(t *testing.T) {
	t.Parallel()

	_, rep, _ := t2RunExpectFailure(
		t, 25*time.Second,
		[]string{"--mode=invalid-traces", "--fault=missing-traceparent", "--fast-timings"},
		"--filter=^(PUB-01|TRAC-01|TRAC-03|TRAC-04)$",
	)
	wantSum := ParsedSummary{Total: 4, Pass: 1, Fail: 3}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	if sc := t2MustScenario(t, rep, "PUB-01"); sc.Status != StatusPass || sc.TraceStatus != StatusPass {
		t.Errorf("PUB-01 unexpected: %+v", *sc)
	}
	for _, id := range []string{"TRAC-01", "TRAC-03"} {
		sc := t2MustScenario(t, rep, id)
		if sc.Status != StatusFail || sc.TraceStatus != "" || !strings.Contains(sc.Error, `missing "googclient_traceparent"`) {
			t.Errorf("%s unexpected: %+v", id, *sc)
		}
	}
	t04 := t2MustScenario(t, rep, "TRAC-04")
	if t04.Status != StatusFail || t04.TraceStatus != "" || !strings.Contains(t04.Error, "traceparent is empty") {
		t.Errorf("TRAC-04 unexpected: %+v", *t04)
	}
}

func TestT2_F3_04_MonitoringDisabledFailsWhenRPCWrong(t *testing.T) {
	t.Parallel()

	_, rep, md := t2RunExpectFailure(
		t, 25*time.Second,
		[]string{"--mode=conformant-rust-nomon", "--fault=wrong-rpc", "--fast-timings"},
		"--filter=^(PUB-01|TRAC-01|TRAC-03|TRAC-04)$",
	)
	if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || *rep.Capabilities.SupportsMonitoring {
		t.Fatalf("expected SupportsMonitoring=false, got %+v", rep.Capabilities)
	}
	wantSum := ParsedSummary{Total: 4, Fail: 2, SkippedNoMonitoring: 2}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}
	for _, id := range []string{"PUB-01", "TRAC-03"} {
		sc := t2MustScenario(t, rep, id)
		if sc.Status != StatusFail || sc.TraceStatus != "" ||
			!strings.Contains(sc.Error, "data mismatch") || !strings.Contains(sc.Error, "__e2e_corrupted_attr__") {
			t.Errorf("%s expected RPC failure unmasked by monitoring=false, got %+v", id, *sc)
		}
	}
	for _, id := range []string{"TRAC-01", "TRAC-04"} {
		sc := t2MustScenario(t, rep, id)
		if sc.Status != StatusSkippedNoMonitoring || sc.TraceStatus != StatusSkippedNoMonitoring ||
			sc.Reason != "client reports supports_monitoring=false" {
			t.Errorf("%s expected SKIPPED_NO_MONITORING, got %+v", id, *sc)
		}
	}
	if len(md.DiagnosticsBullets) != 2 {
		t.Errorf("DiagnosticsBullets len = %d, want 2 (only FAIL scenarios): %v", len(md.DiagnosticsBullets), md.DiagnosticsBullets)
	}
}

func TestT2_F3_05_MonitoringDisabledPassesWhenZeroTracesAndValidRPC(t *testing.T) {
	t.Parallel()

	t.Run("go_profile_overridden_monitoring_false", func(t *testing.T) {
		wrapper, _ := writeProxyWrapper(t, "--mode=conformant-go", "--monitoring=false", "--fast-timings")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")
		mdPath := filepath.Join(dir, "report.md")

		res := runCLI(
			t, 30*time.Second,
			"--proxy-cmd="+wrapper,
			"--filter=^(PUB-01|TRAC-01|TRAC-01b|TRAC-03|TRAC-04)$",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}

		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "single", false)
		if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || *rep.Capabilities.SupportsMonitoring {
			t.Fatalf("expected SupportsMonitoring=false after --monitoring=false override, got %+v", rep.Capabilities)
		}
		wantSum := ParsedSummary{Total: 5, Pass: 2, SkippedNoMonitoring: 3}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, id := range []string{"PUB-01", "TRAC-03"} {
			sc := t2MustScenario(t, rep, id)
			if sc.Status != StatusPass || sc.TraceStatus != StatusSkippedNoMonitoring {
				t.Errorf("%s expected PASS with TraceStatus=SKIPPED_NO_MONITORING, got %+v", id, *sc)
			}
		}
		for _, id := range []string{"TRAC-01", "TRAC-01b", "TRAC-04"} {
			sc := t2MustScenario(t, rep, id)
			if sc.Status != StatusSkippedNoMonitoring || sc.TraceStatus != StatusSkippedNoMonitoring ||
				sc.Reason != "client reports supports_monitoring=false" {
				t.Errorf("%s expected SKIPPED_NO_MONITORING, got %+v", id, *sc)
			}
		}
		md := ReadMarkdownReport(t, mdPath)
		sumTbl := t2FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t2FindTableRow(t, sumTbl, map[string]string{"Total": "5", "Pass": "2", "Fail": "0", "Skipped No Monitoring": "3"})
		if md.HasHeadingContaining("Diagnostics") {
			t.Errorf("unexpected Diagnostics section on clean passing run")
		}
	})

	t.Run("nomon_ignores_trace_graph_faults", func(t *testing.T) {
		wrapper, _ := writeProxyWrapper(t, "--mode=conformant-no-mon", "--fault=bad-span-kind,bad-span-name,missing-links", "--fast-timings")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")

		res := runCLI(
			t, 25*time.Second,
			"--proxy-cmd="+wrapper,
			"--filter=^(PUB-01|TRAC-01|TRAC-03|TRAC-04)$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "single", false)
		wantSum := ParsedSummary{Total: 4, Pass: 2, SkippedNoMonitoring: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
	})
}

// --- E2E-F4 Negative/Boundary (5 tests) ---

func TestT2_F4_01_FilterMatchesZeroScenarios(t *testing.T) {
	t.Parallel()

	t.Run("list_zero_match_exits_zero", func(t *testing.T) {
		res := runCLI(t, 15*time.Second, "--list", "--filter=^NONEXISTENT_SCENARIO_XYZ$")
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("--list zero match failed: exit=%d timedOut=%v err=%v", res.ExitCode, res.TimedOut, res.Err)
		}
		if res.Stdout != "" {
			t.Errorf("expected empty stdout, got %q", res.Stdout)
		}
		if !strings.Contains(res.Stderr, "No scenarios matched filter") {
			t.Errorf("stderr missing notice: %q", res.Stderr)
		}
	})

	t.Run("live_zero_match_exits_one_no_launch", func(t *testing.T) {
		dir := t.TempDir()
		sentinel := filepath.Join(dir, "launched.log")
		reportOut := filepath.Join(dir, "report.json")
		wrapper, _ := t2WriteLaunchSentinelWrapper(t, sentinel, "--mode=conformant-go", "--fast-timings")

		res := runCLI(
			t, 15*time.Second,
			"--proxy-cmd="+wrapper,
			"--filter=^NONEXISTENT_SCENARIO_XYZ$",
			"--report-json="+reportOut,
		)
		t2AssertErrorExit(t, res, "matched 0 scenarios")
		t2AssertFileNotExist(t, reportOut)
		t2AssertFileNotExist(t, sentinel)
	})
}

func TestT2_F4_02_InvalidRegexFilterFailsFast(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sentinel := filepath.Join(dir, "launched.log")
	wrapper, _ := t2WriteLaunchSentinelWrapper(t, sentinel, "--mode=conformant-go", "--fast-timings")

	t.Run("list_invalid_bracket", func(t *testing.T) {
		res := runCLI(t, 15*time.Second, "--list", "--filter=[")
		t2AssertErrorExit(t, res, "invalid --filter regex")
	})

	for _, tc := range []struct {
		name   string
		filter string
	}{
		{name: "live_invalid_bracket", filter: "["},
		{name: "live_unclosed_group", filter: "(unclosed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reportOut := filepath.Join(dir, tc.name+".json")
			res := runCLI(t, 15*time.Second, "--proxy-cmd="+wrapper, "--filter="+tc.filter, "--report-json="+reportOut)
			t2AssertErrorExit(t, res, "invalid --filter regex")
			if res.Duration >= 2*time.Second {
				t.Errorf("%s took %v, expected immediate regex validation error", tc.name, res.Duration)
			}
			t2AssertFileNotExist(t, reportOut)
		})
	}
	t2AssertFileNotExist(t, sentinel)
}

func TestT2_F4_03_ScenarioTimeoutKillsHungScenario(t *testing.T) {
	t.Parallel()

	t.Run("timeout_zero_killed_and_recovered", func(t *testing.T) {
		_, rep, md := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=conformant-go", "--fast-timings", "--fault=hang-publish"},
			"--filter=^(PUB-01|PULL-01)$",
			"--scenario-timeout=400ms",
		)
		t2AssertJSONV1Base(t, rep, "single", false)

		wantSum := ParsedSummary{Total: 2, Pass: 1, Fail: 1, TimeoutCount: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		pub01 := t2MustScenario(t, rep, "PUB-01")
		if pub01.Status != StatusFail || pub01.StatusDetail != StatusTimeout || pub01.ProxyRestarted ||
			!strings.Contains(pub01.Error, "TIMEOUT: scenario exceeded timeout (400ms)") {
			t.Errorf("PUB-01 unexpected: %+v", *pub01)
		}
		pull01 := t2MustScenario(t, rep, "PULL-01")
		if pull01.Status != StatusPass || !pull01.ProxyRestarted {
			t.Errorf("PULL-01 expected PASS with ProxyRestarted=true after hung proxy kill, got %+v", *pull01)
		}

		sumTbl := t2FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t2FindTableRow(t, sumTbl, map[string]string{"Total": "2", "Pass": "1", "Fail": "1"})
		if !md.HasFailureBreakdown || md.TimeoutCount != 1 || md.ErrorCount != 0 {
			t.Errorf("MD failure breakdown unexpected: has=%v to=%d err=%d", md.HasFailureBreakdown, md.TimeoutCount, md.ErrorCount)
		}
		scenTbl := t2FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
		t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PUB-01", "Status": "FAIL (TIMEOUT)"})
		t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PULL-01", "Status": StatusPass})
		if len(md.DiagnosticsBullets) != 1 || !strings.Contains(md.DiagnosticsBullets[0], "PUB-01") {
			t.Errorf("DiagnosticsBullets unexpected: %v", md.DiagnosticsBullets)
		}
	})

	t.Run("explicit_timeout_gt_zero_ignores_smaller_scenario_timeout", func(t *testing.T) {
		_, rep, _ := t2RunExpectSuiteTimeoutFailure(
			t, 25*time.Second,
			[]string{"--mode=hang-scenario"},
			"--filter=^EOD-01$",
			"--scenario-timeout=300ms",
			"--suite-timeout=1200ms",
		)
		t2AssertJSONV1Base(t, rep, "single", true)

		eod01 := t2MustScenario(t, rep, "EOD-01")
		if eod01.DurationMs < 800 {
			t.Errorf("EOD-01 DurationMs = %d, expected >= 800ms (must ignore smaller --scenario-timeout=300ms)", eod01.DurationMs)
		}
		if eod01.Status != StatusFail || eod01.StatusDetail != StatusTimeout || !strings.Contains(eod01.Error, "TIMEOUT: suite deadline exceeded") {
			t.Errorf("EOD-01 unexpected: %+v", *eod01)
		}
		if tr := rep.Targets["default"]; tr == nil || !tr.TimedOut || !strings.Contains(tr.TargetError, "suite timed out during scenario EOD-01") {
			t.Errorf("Targets[default] unexpected: %+v", tr)
		}
	})
}

func TestT2_F4_04_SuiteTimeoutBoundsHungSuite(t *testing.T) {
	t.Parallel()

	t.Run("single_target_multi_scenario", func(t *testing.T) {
		res, rep, md := t2RunExpectSuiteTimeoutFailure(
			t, 25*time.Second,
			[]string{"--mode=hang-scenario"},
			"--filter=^(PUB-01|PORD-02|EOD-01)$",
			"--suite-timeout=1200ms",
		)
		if res.Duration >= 8*time.Second {
			t.Errorf("duration = %v, expected suite-timeout to bound execution < 8s", res.Duration)
		}
		t2AssertJSONV1Base(t, rep, "single", true)

		wantSum := ParsedSummary{Total: 1, Fail: 1, TimeoutCount: 1}
		if rep.Summary == nil || *rep.Summary != wantSum || len(rep.Scenarios) != 1 {
			t.Fatalf("expected 1 completed scenario (PUB-01), got sum=%+v len=%d", rep.Summary, len(rep.Scenarios))
		}
		pub01 := t2MustScenario(t, rep, "PUB-01")
		if pub01.Status != StatusFail || pub01.StatusDetail != StatusTimeout || !strings.Contains(pub01.Error, "TIMEOUT: suite deadline exceeded") {
			t.Errorf("PUB-01 unexpected: %+v", *pub01)
		}
		tr := rep.Targets["default"]
		if tr == nil || !tr.TimedOut || !strings.Contains(tr.TargetError, "suite timed out during scenario PUB-01") {
			t.Errorf("Targets[default] unexpected: %+v", tr)
		}
		if len(md.DiagnosticsBullets) != 2 {
			t.Errorf("expected 2 DiagnosticsBullets (Target default Error + PUB-01), got %v", md.DiagnosticsBullets)
		}
	})

	t.Run("multi_target_skips_unstarted_second_target", func(t *testing.T) {
		dir := t.TempDir()
		t2Sentinel := filepath.Join(dir, "t2_launched.log")
		w1, _ := writeProxyWrapper(t, "--mode=hang-scenario")
		w2, _ := t2WriteLaunchSentinelWrapper(t, t2Sentinel, "--mode=conformant-go", "--fast-timings")
		jsonPath := filepath.Join(dir, "compare.json")

		res := runCLI(
			t, 25*time.Second,
			"--proxy-target=t1="+w1,
			"--proxy-target=t2="+w2,
			"--filter=^(PUB-01|PORD-02)$",
			"--suite-timeout=1000ms",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Stderr)
		}
		t2AssertFileNotExist(t, t2Sentinel)

		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "compare", true)

		t1 := rep.Targets["t1"]
		t2 := rep.Targets["t2"]
		if t1 == nil || !t1.TimedOut || !strings.Contains(t1.TargetError, "suite timed out during scenario PUB-01") {
			t.Errorf("t1 unexpected: %+v", t1)
		}
		if t2 == nil || !t2.TimedOut || t2.Summary == nil || t2.Summary.Total != 0 || len(t2.Scenarios) != 0 ||
			!strings.Contains(t2.TargetError, "suite timed out before starting target") {
			t.Errorf("t2 unexpected: %+v", t2)
		}

		dErr := t2MustDiff(t, rep, "target_error", "")
		if dErr.Verdicts["t1"] != StatusError || dErr.Verdicts["t2"] != StatusError {
			t.Errorf("target_error verdicts = %v, want both ERROR", dErr.Verdicts)
		}
		dSc := t2MustDiff(t, rep, "scenario_verdict", "PUB-01")
		if dSc.Verdicts["t1"] != StatusFail || dSc.Verdicts["t2"] != "MISSING" {
			t.Errorf("PUB-01 diff verdicts = %v, want t1=FAIL t2=MISSING", dSc.Verdicts)
		}
	})
}

func TestT2_F4_05_NegativeOrZeroTimeoutRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sentinel := filepath.Join(dir, "launched.log")
	wrapper, _ := t2WriteLaunchSentinelWrapper(t, sentinel, "--mode=conformant-go", "--fast-timings")

	cases := []struct {
		name    string
		flag    string
		wantErr string
	}{
		{name: "scenario_timeout_zero", flag: "--scenario-timeout=0s", wantErr: "--scenario-timeout must be > 0 (got 0s)"},
		{name: "scenario_timeout_negative", flag: "--scenario-timeout=-1s", wantErr: "--scenario-timeout must be > 0 (got -1s)"},
		{name: "suite_timeout_zero", flag: "--suite-timeout=0s", wantErr: "--suite-timeout must be > 0 (got 0s)"},
		{name: "suite_timeout_negative", flag: "--suite-timeout=-1s", wantErr: "--suite-timeout must be > 0 (got -1s)"},
		{name: "scenario_timeout_invalid_string", flag: "--scenario-timeout=abc", wantErr: `invalid argument "abc"`},
		{name: "suite_timeout_invalid_string", flag: "--suite-timeout=abc", wantErr: `invalid argument "abc"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reportOut := filepath.Join(dir, tc.name+".json")
			res := runCLI(t, 15*time.Second, "--proxy-cmd="+wrapper, tc.flag, "--filter=^PUB-01$", "--report-json="+reportOut)
			t2AssertErrorExit(t, res, tc.wantErr)
			t2AssertFileNotExist(t, reportOut)
		})
	}
	t2AssertFileNotExist(t, sentinel)
}

// --- E2E-F5 Negative/Boundary (6 tests) ---

func TestT2_F5_01_StrictUnsupportedFailsExitCode(t *testing.T) {
	t.Parallel()

	t.Run("no_strict_exits_zero", func(t *testing.T) {
		w, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")
		mdPath := filepath.Join(dir, "report.md")
		res := runCLI(t, 25*time.Second, "--proxy-cmd="+w, "--filter=^(PUB-01|PULL-04|TRAC-01)$", "--report-json="+jsonPath, "--report-md="+mdPath)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("expected exit 0, got exit=%d timedOut=%v err=%v", res.ExitCode, res.TimedOut, res.Err)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "single", false)
		wantSum := ParsedSummary{Total: 3, Pass: 1, SkippedNoMonitoring: 1, UnsupportedByClient: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		md := ReadMarkdownReport(t, mdPath)
		sumTbl := t2FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t2FindTableRow(t, sumTbl, map[string]string{"Total": "3", "Pass": "1", "Fail": "0", "Skipped No Monitoring": "1", "Unsupported By Client": "1"})
	})

	t.Run("strict_with_unsupported_exits_one", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=conformant-rust-nomon", "--fast-timings"},
			"--filter=^(PUB-01|PULL-04|TRAC-01)$",
			"--strict-unsupported",
		)
		wantSum := ParsedSummary{Total: 3, Pass: 1, SkippedNoMonitoring: 1, UnsupportedByClient: 1}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		if sc := t2MustScenario(t, rep, "PULL-04"); sc.Status != StatusUnsupportedByClient || sc.Reason == "" {
			t.Errorf("PULL-04 unexpected: %+v", *sc)
		}
	})

	t.Run("strict_zero_unsupported_exits_zero", func(t *testing.T) {
		w, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "report.json")
		res := runCLI(t, 25*time.Second, "--proxy-cmd="+w, "--filter=^(PUB-01|PULL-04|TRAC-01)$", "--strict-unsupported", "--report-json="+jsonPath)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("expected exit 0 when 0 unsupported, got exit=%d timedOut=%v err=%v", res.ExitCode, res.TimedOut, res.Err)
		}
		rep := ReadJSONReport(t, jsonPath)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 3, Pass: 3}) {
			t.Errorf("Summary = %+v, want 3/3 PASS", rep.Summary)
		}
	})

	t.Run("multi_target_strict_exits_one", func(t *testing.T) {
		wGo, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "compare.json")
		mdPath := filepath.Join(dir, "compare.md")
		res := runCLI(
			t, 30*time.Second,
			"--proxy-target=clean="+wGo,
			"--proxy-target=rust="+wRust,
			"--filter=^(PUB-01|PULL-04|TRAC-01)$",
			"--strict-unsupported",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected exit 1 on multi-target strict unsupported, got exit=%d timedOut=%v", res.ExitCode, res.TimedOut)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "compare", false)
		if rep.Targets["clean"].Summary.UnsupportedByClient != 0 || rep.Targets["rust"].Summary.UnsupportedByClient != 1 {
			t.Errorf("unexpected unsupported counts: clean=%+v rust=%+v", rep.Targets["clean"].Summary, rep.Targets["rust"].Summary)
		}
		md := ReadMarkdownReport(t, mdPath)
		sumTbl := t2FindTableByHeaders(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
		t2FindTableRow(t, sumTbl, map[string]string{"Target": "clean", "Unsupported By Client": "0"})
		t2FindTableRow(t, sumTbl, map[string]string{"Target": "rust", "Unsupported By Client": "1"})
	})
}

func TestT2_F5_02_UnwritableReportPathFailsCleanly(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sentinel := filepath.Join(dir, "launched.log")
	wSentinel, _ := t2WriteLaunchSentinelWrapper(t, sentinel, "--mode=conformant-go", "--fast-timings")
	existingDir := filepath.Join(dir, "existing_dir")
	if err := os.MkdirAll(existingDir, 0o755); err != nil {
		t.Fatalf("mkdir existingDir: %v", err)
	}

	t.Run("missing_parent_dir", func(t *testing.T) {
		r1 := runCLI(t, 15*time.Second, "--proxy-cmd="+wSentinel, "--filter=^PUB-01$", "--report-json="+filepath.Join(dir, "no_such_dir", "r.json"))
		t2AssertErrorExit(t, r1, "parent directory")
		r2 := runCLI(t, 15*time.Second, "--proxy-cmd="+wSentinel, "--filter=^PUB-01$", "--report-md="+filepath.Join(dir, "no_such_dir", "r.md"))
		t2AssertErrorExit(t, r2, "parent directory")
		t2AssertFileNotExist(t, sentinel)
	})

	t.Run("dir_as_report_path", func(t *testing.T) {
		r1 := runCLI(t, 15*time.Second, "--proxy-cmd="+wSentinel, "--filter=^PUB-01$", "--report-json="+existingDir)
		t2AssertErrorExit(t, r1, "is a directory")
		r2 := runCLI(t, 15*time.Second, "--proxy-cmd="+wSentinel, "--filter=^PUB-01$", "--report-md="+existingDir)
		t2AssertErrorExit(t, r2, "is a directory")
		t2AssertFileNotExist(t, sentinel)
	})

	t.Run("ro_0555_dir_write_time_failures", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Fatalf("test cannot verify 0555 directory permission enforcement when running as root (euid=0)")
		}
		roDir := filepath.Join(dir, "ro_dir")
		if err := os.MkdirAll(roDir, 0o755); err != nil {
			t.Fatalf("mkdir roDir: %v", err)
		}
		if err := os.Chmod(roDir, 0o555); err != nil {
			t.Fatalf("chmod 0555 failed: %v", err)
		}
		t.Cleanup(func() {
			_ = os.Chmod(roDir, 0o755)
		})

		okDir := filepath.Join(dir, "ok_dir")
		if err := os.MkdirAll(okDir, 0o755); err != nil {
			t.Fatalf("mkdir okDir: %v", err)
		}

		wLive, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")

		okMD := filepath.Join(okDir, "sibling.md")
		rJSONFail := runCLI(t, 25*time.Second, "--proxy-cmd="+wLive, "--filter=^PUB-01$", "--report-json="+filepath.Join(roDir, "out.json"), "--report-md="+okMD)
		if rJSONFail.ExitCode != 1 || rJSONFail.TimedOut || !strings.Contains(rJSONFail.Stderr, "permission denied") {
			t.Errorf("live unwritable --report-json unexpected: exit=%d stderr=%q", rJSONFail.ExitCode, rJSONFail.Stderr)
		}
		if !strings.Contains(rJSONFail.Stdout, "# Pub/Sub Conformance Report:") {
			t.Errorf("expected stdout Markdown preserved even when --report-json failed; got:\n%s", rJSONFail.Stdout)
		}
		mdSib := ReadMarkdownReport(t, okMD)
		if len(mdSib.Tables) < 3 {
			t.Errorf("sibling --report-md expected >= 3 tables, got %d", len(mdSib.Tables))
		}

		okJSON := filepath.Join(okDir, "sibling.json")
		rMDFail := runCLI(t, 25*time.Second, "--proxy-cmd="+wLive, "--filter=^PUB-01$", "--report-json="+okJSON, "--report-md="+filepath.Join(roDir, "out.md"))
		if rMDFail.ExitCode != 1 || rMDFail.TimedOut || !strings.Contains(rMDFail.Stderr, "permission denied") {
			t.Errorf("live unwritable --report-md unexpected: exit=%d stderr=%q", rMDFail.ExitCode, rMDFail.Stderr)
		}
		if !strings.Contains(rMDFail.Stdout, "# Pub/Sub Conformance Report:") {
			t.Errorf("expected stdout Markdown preserved even when --report-md failed")
		}
		repSib := ReadJSONReport(t, okJSON)
		t2AssertJSONV1Base(t, repSib, "single", false)

		cmpDir := filepath.Join(okDir, "cmp_inputs")
		if err := os.MkdirAll(cmpDir, 0o755); err != nil {
			t.Fatalf("mkdir cmpDir: %v", err)
		}
		j1 := t2RunValidJSONForCompare(t, cmpDir, "go1.json", []string{"--mode=conformant-go", "--fast-timings"}, "^PUB-01$")
		j2 := t2RunValidJSONForCompare(t, cmpDir, "rust1.json", []string{"--mode=conformant-rust-nomon", "--fast-timings"}, "^PUB-01$")
		rCmpFail := runCLI(t, 15*time.Second, "--compare="+j1+","+j2, "--report-md="+filepath.Join(roDir, "cmp.md"))
		if rCmpFail.ExitCode != 1 || rCmpFail.TimedOut || !strings.Contains(rCmpFail.Stderr, "permission denied") {
			t.Errorf("compare unwritable --report-md unexpected: exit=%d stderr=%q", rCmpFail.ExitCode, rCmpFail.Stderr)
		}
		if !strings.Contains(rCmpFail.Stdout, "# Pub/Sub Cross-Client Conformance Comparison Report") {
			t.Errorf("expected compare stdout Markdown preserved even when --report-md failed")
		}
	})
}

func TestT2_F5_03_FailureExitCodeNonZero(t *testing.T) {
	t.Parallel()

	t.Run("mixed_pass_fail", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=wrong-rpc", "--fast-timings"},
			"--filter=^(PUB-01|PULL-04)$",
		)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 2, Pass: 1, Fail: 1}) {
			t.Errorf("Summary = %+v, want Total=2 Pass=1 Fail=1", rep.Summary)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 20*time.Second,
			[]string{"--mode=missing-rpc", "--fast-timings"},
			"--filter=^PULL-01$",
			"--scenario-timeout=400ms",
		)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 1, Fail: 1, TimeoutCount: 1}) {
			t.Errorf("Summary = %+v, want Total=1 Fail=1 TimeoutCount=1", rep.Summary)
		}
	})

	t.Run("crash_error", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 20*time.Second,
			[]string{"--mode=conformant-go", "--crash-phase=scenario", "--fast-timings"},
			"--filter=^PUB-01$",
		)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 1, Fail: 1, ErrorCount: 1}) {
			t.Errorf("Summary = %+v, want Total=1 Fail=1 ErrorCount=1", rep.Summary)
		}
		if sc := t2MustScenario(t, rep, "PUB-01"); !strings.Contains(sc.Error, "simulated scenario crash at RPC #1") {
			t.Errorf("PUB-01 Error unexpected: %+v", *sc)
		}
	})

	t.Run("empty_filter", func(t *testing.T) {
		w, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		res := runCLI(t, 15*time.Second, "--proxy-cmd="+w, "--filter=^DOES_NOT_EXIST$")
		t2AssertErrorExit(t, res, "matched 0 scenarios")
	})
}

func TestT2_F5_04_ReportCountsMatchScenarioList(t *testing.T) {
	t.Parallel()

	// Single deterministic run producing all 6 distinct outcome states:
	// PORD-02 (FAIL/TIMEOUT), PULL-04 (UNSUPPORTED_BY_CLIENT), TRAC-01 (SKIPPED_NO_MONITORING),
	// CFG-02 (FAIL), CFG-03 (PASS), CFG-04 (FAIL/ERROR).
	_, rep, md := t2RunExpectFailure(
		t, 30*time.Second,
		[]string{
			"--mode=conformant-go",
			"--monitoring=false",
			"--unsupported=unary_pull",
			"--fault=missing-custom-headers,wrong-ordering-key,crash-after=6",
			"--fast-timings",
		},
		"--filter=^(PORD-02|PULL-04|TRAC-01|CFG-02|CFG-03|CFG-04)$",
		"--scenario-timeout=600ms",
	)
	t2AssertJSONV1Base(t, rep, "single", false)

	wantSum := ParsedSummary{
		Total:               6,
		Pass:                1,
		Fail:                3,
		SkippedNoMonitoring: 1,
		UnsupportedByClient: 1,
		TimeoutCount:        1,
		ErrorCount:          1,
	}
	if rep.Summary == nil || *rep.Summary != wantSum {
		t.Fatalf("Summary = %+v, want %+v", rep.Summary, wantSum)
	}

	rawSum, ok := rep.Raw["summary"].(map[string]any)
	if !ok {
		t.Fatalf("rep.Raw[summary] missing or invalid type %T", rep.Raw["summary"])
	}
	for k, wantVal := range map[string]float64{
		"total":                 6,
		"pass":                  1,
		"fail":                  3,
		"skipped_no_monitoring": 1,
		"unsupported_by_client": 1,
		"timeout_count":         1,
		"error_count":           1,
	} {
		if got, ok := rawSum[k].(float64); !ok || got != wantVal {
			t.Errorf("raw summary[%q] = %#v, want %v", k, rawSum[k], wantVal)
		}
	}

	pord02 := t2MustScenario(t, rep, "PORD-02")
	if pord02.Status != StatusFail || pord02.StatusDetail != StatusTimeout ||
		t2RawString(pord02.Raw, "status") != StatusFail || t2RawString(pord02.Raw, "status_detail") != StatusTimeout ||
		pord02.Raw["error"] == nil || pord02.Raw["reason"] != nil || pord02.Raw["trace_status"] != nil {
		t.Errorf("PORD-02 schema mismatch: %+v raw=%v", *pord02, pord02.Raw)
	}

	pull04 := t2MustScenario(t, rep, "PULL-04")
	if pull04.Status != StatusUnsupportedByClient || pull04.Raw["reason"] == nil ||
		pull04.Raw["error"] != nil || pull04.Raw["status_detail"] != nil || pull04.Raw["trace_status"] != nil {
		t.Errorf("PULL-04 schema mismatch: %+v raw=%v", *pull04, pull04.Raw)
	}

	trac01 := t2MustScenario(t, rep, "TRAC-01")
	if trac01.Status != StatusSkippedNoMonitoring || trac01.TraceStatus != StatusSkippedNoMonitoring ||
		trac01.Raw["reason"] == nil || t2RawString(trac01.Raw, "trace_status") != StatusSkippedNoMonitoring ||
		trac01.Raw["error"] != nil || trac01.Raw["status_detail"] != nil {
		t.Errorf("TRAC-01 schema mismatch: %+v raw=%v", *trac01, trac01.Raw)
	}

	cfg02 := t2MustScenario(t, rep, "CFG-02")
	if cfg02.Status != StatusFail || cfg02.StatusDetail != "" ||
		t2RawString(cfg02.Raw, "status") != StatusFail || cfg02.Raw["status_detail"] != nil || cfg02.Raw["error"] == nil {
		t.Errorf("CFG-02 schema mismatch: %+v raw=%v", *cfg02, cfg02.Raw)
	}

	cfg03 := t2MustScenario(t, rep, "CFG-03")
	if cfg03.Status != StatusPass || cfg03.TraceStatus != StatusSkippedNoMonitoring ||
		t2RawString(cfg03.Raw, "status") != StatusPass || cfg03.Raw["error"] != nil || cfg03.Raw["reason"] != nil || cfg03.Raw["status_detail"] != nil {
		t.Errorf("CFG-03 schema mismatch: %+v raw=%v", *cfg03, cfg03.Raw)
	}

	cfg04 := t2MustScenario(t, rep, "CFG-04")
	if cfg04.Status != StatusFail || cfg04.StatusDetail != StatusError ||
		t2RawString(cfg04.Raw, "status") != StatusFail || t2RawString(cfg04.Raw, "status_detail") != StatusError || cfg04.Raw["error"] == nil {
		t.Errorf("CFG-04 schema mismatch: %+v raw=%v", *cfg04, cfg04.Raw)
	}

	sumTbl := t2FindTableByHeaders(t, md, "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
	t2FindTableRow(t, sumTbl, map[string]string{
		"Total":                 "6",
		"Pass":                  "1",
		"Fail":                  "3",
		"Skipped No Monitoring": "1",
		"Unsupported By Client": "1",
	})
	if !md.HasFailureBreakdown || md.TimeoutCount != 1 || md.ErrorCount != 1 ||
		md.FailureBreakdown != "Failure breakdown: 1 timeout(s), 1 error(s)." ||
		!strings.Contains(md.Raw, "_Failure breakdown: 1 timeout(s), 1 error(s)._") {
		t.Errorf("MD failure breakdown unexpected: has=%v to=%d err=%d text=%q",
			md.HasFailureBreakdown, md.TimeoutCount, md.ErrorCount, md.FailureBreakdown)
	}

	scenTbl := t2FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
	t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PORD-02", "Status": "FAIL (TIMEOUT)"})
	t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "PULL-04", "Status": StatusUnsupportedByClient})
	t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "TRAC-01", "Status": StatusSkippedNoMonitoring})
	t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "CFG-02", "Status": StatusFail})
	t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "CFG-03", "Status": StatusPass})
	t2FindTableRow(t, scenTbl, map[string]string{"Scenario": "CFG-04", "Status": "FAIL (ERROR)"})
}

func TestT2_F5_05_ReportCapturesDiagnosticErrorText(t *testing.T) {
	t.Parallel()

	_, rep, md := t2RunExpectFailure(
		t, 30*time.Second,
		[]string{
			"--mode=conformant-go",
			"--monitoring=false",
			"--unsupported=unary_pull",
			"--fault=missing-custom-headers,wrong-ordering-key,crash-after=6",
			"--fast-timings",
		},
		"--filter=^(PORD-02|PULL-04|TRAC-01|CFG-02|CFG-03|CFG-04)$",
		"--scenario-timeout=600ms",
	)

	scenTbl := t2FindTableByHeaders(t, md, "Scenario", "Category", "Status", "Duration", "Details")
	for _, sc := range rep.Scenarios {
		row := t2FindTableRow(t, scenTbl, map[string]string{"Scenario": sc.ID})
		switch sc.Status {
		case StatusFail:
			if sc.Error == "" || sc.Error != t2RawString(sc.Raw, "error") {
				t.Errorf("%s failed scenario missing error text: err=%q raw=%v", sc.ID, sc.Error, sc.Raw["error"])
			}
			if row["Details"] == "" {
				t.Errorf("%s Markdown Details empty on FAIL row", sc.ID)
			}
		case StatusPass:
			if sc.Error != "" || sc.Raw["error"] != nil {
				t.Errorf("%s passing scenario has unexpected error %q", sc.ID, sc.Error)
			}
			if row["Details"] != "" {
				t.Errorf("%s Markdown Details expected blank on PASS row, got %q", sc.ID, row["Details"])
			}
		default:
			if sc.Error != "" || sc.Reason == "" || row["Details"] != sc.Reason {
				t.Errorf("%s (%s) Markdown Details=%q != Reason=%q", sc.ID, sc.Status, row["Details"], sc.Reason)
			}
		}
	}

	if len(md.DiagnosticsBullets) != 3 {
		t.Fatalf("expected 3 DiagnosticsBullets for PORD-02, CFG-02, CFG-04; got %v", md.DiagnosticsBullets)
	}
	wantBullets := map[string]string{
		"PORD-02": "**PORD-02** (`FAIL (TIMEOUT)`):",
		"CFG-02":  "**CFG-02** (`FAIL`):",
		"CFG-04":  "**CFG-04** (`FAIL (ERROR)`):",
	}
	for id, prefix := range wantBullets {
		found := false
		for _, b := range md.DiagnosticsBullets {
			if strings.HasPrefix(b, prefix) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("DiagnosticsBullets missing %s entry with prefix %q; got %v", id, prefix, md.DiagnosticsBullets)
		}
	}
	for _, nonFailID := range []string{"CFG-03", "PULL-04", "TRAC-01"} {
		for _, b := range md.DiagnosticsBullets {
			if strings.Contains(b, nonFailID) {
				t.Errorf("DiagnosticsBullets should omit non-failing %s, got %q", nonFailID, b)
			}
		}
	}
}

func TestT2_F5_06_ReportPathCollisionAndStrictValidation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sentinel := filepath.Join(dir, "launched.log")
	wSentinel, _ := t2WriteLaunchSentinelWrapper(t, sentinel, "--mode=conformant-go", "--fast-timings")
	subDir := filepath.Join(dir, "sub")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("mkdir subDir: %v", err)
	}

	samePath := filepath.Join(dir, "collide.out")
	normPath := filepath.Join(dir, "norm.out")
	normAlt := filepath.Join(subDir, "..", "norm.out")

	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "exact_same_path",
			args:    []string{"--proxy-cmd=" + wSentinel, "--filter=^PUB-01$", "--report-json=" + samePath, "--report-md=" + samePath},
			wantErr: "cannot resolve to the same file",
		},
		{
			name:    "normalized_parent_dir_collision",
			args:    []string{"--proxy-cmd=" + wSentinel, "--filter=^PUB-01$", "--report-json=" + normPath, "--report-md=" + normAlt},
			wantErr: "cannot resolve to the same file",
		},
		{
			name:    "empty_report_json",
			args:    []string{"--proxy-cmd=" + wSentinel, "--filter=^PUB-01$", "--report-json="},
			wantErr: "--report-json cannot be empty",
		},
		{
			name:    "empty_report_md",
			args:    []string{"--proxy-cmd=" + wSentinel, "--filter=^PUB-01$", "--report-md="},
			wantErr: "--report-md cannot be empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := runCLI(t, 15*time.Second, tc.args...)
			t2AssertErrorExit(t, res, tc.wantErr)
		})
	}
	t2AssertFileNotExist(t, sentinel)

	t.Run("symlink_and_devnull_boundary_contrast", func(t *testing.T) {
		wLive, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		targetFile := filepath.Join(dir, "sym_target.json")
		symlinkPath := filepath.Join(dir, "sym_alias.md")
		if err := os.WriteFile(targetFile, []byte("{}\n"), 0o644); err != nil {
			t.Fatalf("write targetFile: %v", err)
		}
		if err := os.Symlink(targetFile, symlinkPath); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		rSym := runCLI(t, 25*time.Second, "--proxy-cmd="+wLive, "--filter=^PUB-01$", "--report-json="+targetFile, "--report-md="+symlinkPath)
		if rSym.ExitCode != 0 || rSym.TimedOut || rSym.Err != nil {
			t.Errorf("expected filepath.Abs symlink contrast to exit 0, got exit=%d stderr=%q", rSym.ExitCode, rSym.Stderr)
		}
		rNull := runCLI(t, 25*time.Second, "--proxy-cmd="+wLive, "--filter=^PUB-01$", "--report-json=/dev/null", "--report-md=/dev/null")
		if rNull.ExitCode != 0 || rNull.TimedOut || rNull.Err != nil {
			t.Errorf("expected /dev/null exempt contrast to exit 0, got exit=%d stderr=%q", rNull.ExitCode, rNull.Stderr)
		}
	})
}

// --- E2E-F6 Negative/Boundary (6 tests) ---

func TestT2_F6_01_CompareMissingFileFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	valid := t2RunValidJSONForCompare(t, dir, "valid.json", []string{"--mode=conformant-go", "--fast-timings"}, "^PUB-01$")
	miss1 := filepath.Join(dir, "missing_1.json")
	miss2 := filepath.Join(dir, "missing_2.json")

	for _, tc := range []struct {
		name string
		arg  string
		out  string
	}{
		{name: "both_missing", arg: miss1 + "," + miss2, out: filepath.Join(dir, "out1.json")},
		{name: "one_valid_one_missing", arg: valid + "," + miss2, out: filepath.Join(dir, "out2.json")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := runCLI(t, 15*time.Second, "--compare="+tc.arg, "--report-json="+tc.out)
			t2AssertErrorExit(t, res, "reading JSON report")
			t2AssertFileNotExist(t, tc.out)
		})
	}
}

func TestT2_F6_02_CompareMalformedJSONFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	valid := t2RunValidJSONForCompare(t, dir, "valid.json", []string{"--mode=conformant-go", "--fast-timings"}, "^PUB-01$")
	validBytes, err := os.ReadFile(valid)
	if err != nil {
		t.Fatalf("read valid JSON: %v", err)
	}

	cases := []struct {
		name       string
		content    string
		standalone bool
		wantErr    string
	}{
		{name: "corrupt_syntax", content: "{not-json", wantErr: "invalid JSON report"},
		{name: "empty_file", content: "", wantErr: "empty JSON report"},
		{name: "null_json", content: "null", wantErr: "invalid JSON report"},
		{name: "array_json", content: "[]", wantErr: "invalid JSON report"},
		{name: "empty_object", content: "{}", wantErr: "missing both scenarios and targets"},
		{name: "trailing_json_tokens", content: string(validBytes) + "\n{}", wantErr: "unexpected trailing JSON content"},
		{name: "invalid_scenario_status", content: `{"scenarios":[{"id":"PUB-01","status":"BOGUS"}]}`, wantErr: "status invalid/missing"},
		{
			name:       "intra_file_duplicate_target",
			content:    `{"targets":[{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]},{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]}]}`,
			standalone: true,
			wantErr:    `duplicate target "go" in report`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			badFile := filepath.Join(dir, tc.name+".json")
			if err := os.WriteFile(badFile, []byte(tc.content), 0o644); err != nil {
				t.Fatalf("write %s: %v", badFile, err)
			}
			outFile := filepath.Join(dir, tc.name+"_out.json")
			cmpArg := valid + "," + badFile
			if tc.standalone {
				cmpArg = badFile
			}
			res := runCLI(t, 15*time.Second, "--compare="+cmpArg, "--report-json="+outFile)
			t2AssertErrorExit(t, res, tc.wantErr)
			t2AssertFileNotExist(t, outFile)
		})
	}
}

func TestT2_F6_03_CompareSingleInputErrorOrDegenerate(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	one := t2RunValidJSONForCompare(t, dir, "one.json", []string{"--mode=conformant-go", "--fast-timings"}, "^PUB-01$")
	twoA := t2RunValidJSONForCompare(t, dir, "twoA.json", []string{"--mode=conformant-go", "--fast-timings"}, "^PUB-01$")
	twoB := t2RunValidJSONForCompare(t, dir, "twoB.json", []string{"--mode=conformant-rust-nomon", "--fast-timings"}, "^PUB-01$")

	twoTargetFile := filepath.Join(dir, "two_targets.json")
	if mk := runCLI(t, 15*time.Second, "--compare="+twoA+","+twoB, "--report-json="+twoTargetFile); mk.ExitCode != 0 {
		t.Fatalf("failed building two_targets.json: exit=%d stderr=%q", mk.ExitCode, mk.Stderr)
	}

	for _, tc := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "single_target_file",
			args:    []string{"--compare=" + one},
			wantErr: "compare requires at least 2 target reports, got 1",
		},
		{
			name:    "empty_compare_arg",
			args:    []string{"--compare="},
			wantErr: "empty file path in --compare argument",
		},
		{
			name:    "trailing_comma",
			args:    []string{"--compare=" + one + ","},
			wantErr: "empty file path in --compare argument",
		},
		{
			name:    "empty_repeatable_compare",
			args:    []string{"--compare=" + one, "--compare=" + twoB, "--compare="},
			wantErr: "empty file path in --compare argument",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outFile := filepath.Join(dir, tc.name+"_out.json")
			args := append(slices.Clone(tc.args), "--report-json="+outFile)
			res := runCLI(t, 15*time.Second, args...)
			t2AssertErrorExit(t, res, tc.wantErr)
			t2AssertFileNotExist(t, outFile)
		})
	}

	t.Run("contrast_single_multi_target_file_succeeds", func(t *testing.T) {
		outFile := filepath.Join(dir, "multi_in_cmp.json")
		res := runCLI(t, 15*time.Second, "--compare="+twoTargetFile, "--report-json="+outFile)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("expected 2-target single JSON compare to succeed, got exit=%d stderr=%q", res.ExitCode, res.Stderr)
		}
		rep := ReadJSONReport(t, outFile)
		t2AssertJSONV1Base(t, rep, "compare", false)
		if len(rep.Targets) != 2 {
			t.Errorf("len(Targets) = %d, want 2", len(rep.Targets))
		}
	})

	t.Run("contrast_duplicate_file_auto_dedupes", func(t *testing.T) {
		outFile := filepath.Join(dir, "dup_dedup.json")
		res := runCLI(t, 15*time.Second, "--compare="+one+","+one, "--report-json="+outFile)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("expected duplicate file compare to auto-dedupe and succeed, got exit=%d stderr=%q", res.ExitCode, res.Stderr)
		}
		rep := ReadJSONReport(t, outFile)
		t2AssertJSONV1Base(t, rep, "compare", false)
		if len(rep.Targets) != 2 || rep.Targets["one"] == nil || rep.Targets["one-2"] == nil {
			t.Errorf("expected auto-deduped targets one and one-2, got %v", rep.Targets)
		}
	})
}

func TestT2_F6_04_MultiTargetOneCrashingProxyIsolated(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		deadFirst bool
		deadCmd   func(t *testing.T) string
		wantErr   string
	}{
		{
			name:      "dead_crash_startup_first_go_second",
			deadFirst: true,
			deadCmd: func(t *testing.T) string {
				w, _ := writeProxyWrapper(t, "--mode=crash-startup")
				return w
			},
			wantErr: "simulated startup crash",
		},
		{
			name:      "go_first_dead_missing_binary_second",
			deadFirst: false,
			deadCmd: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "missing-target-xyz")
			},
			wantErr: "no such file or directory",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wGo, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
			wDead := tc.deadCmd(t)
			dir := t.TempDir()
			jsonPath := filepath.Join(dir, "compare.json")
			mdPath := filepath.Join(dir, "compare.md")

			targets := []string{"--proxy-target=go=" + wGo, "--proxy-target=dead=" + wDead}
			if tc.deadFirst {
				targets = []string{"--proxy-target=dead=" + wDead, "--proxy-target=go=" + wGo}
			}
			args := append(targets, "--filter=^PUB-01$", "--report-json="+jsonPath, "--report-md="+mdPath)
			res := runCLI(t, 25*time.Second, args...)
			if res.ExitCode != 1 || res.TimedOut {
				t.Fatalf("expected ExitCode=1 TimedOut=false, got exit=%d timedOut=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Stderr)
			}
			if !strings.Contains(res.Stderr, "[go] PUB-01: PASS") || !strings.Contains(res.Stderr, "Target dead error:") {
				t.Errorf("stderr missing isolated progress/error lines:\n%s", res.Stderr)
			}

			rep := ReadJSONReport(t, jsonPath)
			t2AssertJSONV1Base(t, rep, "compare", false)

			tGo := rep.Targets["go"]
			tDead := rep.Targets["dead"]
			if tGo == nil || tGo.TargetError != "" || tGo.Summary == nil || *tGo.Summary != (ParsedSummary{Total: 1, Pass: 1}) {
				t.Fatalf("healthy go target unexpected: %+v", tGo)
			}
			if t2MustTargetScenario(t, tGo, "PUB-01").Status != StatusPass {
				t.Errorf("go/PUB-01 not PASS")
			}
			if tDead == nil || tDead.Summary == nil || *tDead.Summary != (ParsedSummary{}) || len(tDead.Scenarios) != 0 ||
				!strings.Contains(tDead.TargetError, tc.wantErr) {
				t.Fatalf("dead target unexpected: %+v", tDead)
			}

			dErr := t2MustDiff(t, rep, "target_error", "")
			if dErr.Verdicts["dead"] != StatusError || dErr.Verdicts["go"] != "OK" || dErr.Details["dead"] != tDead.TargetError {
				t.Errorf("target_error diff unexpected: %+v", *dErr)
			}
			if rawV, ok := dErr.Raw["verdicts"].(map[string]any); !ok || t2RawString(rawV, "dead") != StatusError || t2RawString(rawV, "go") != "OK" {
				t.Errorf("raw target_error verdicts unexpected: %#v", dErr.Raw["verdicts"])
			}
			dSc := t2MustDiff(t, rep, "scenario_verdict", "PUB-01")
			if dSc.Verdicts["dead"] != "MISSING" || dSc.Verdicts["go"] != StatusPass {
				t.Errorf("PUB-01 diff unexpected: %+v", *dSc)
			}

			md := ReadMarkdownReport(t, mdPath)
			if len(md.Tables) != 3 {
				t.Errorf("expected 3 Markdown tables (Capabilities & Defaults omitted), got %d", len(md.Tables))
			}
			sumTbl := t2FindTableByHeaders(t, md, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
			t2FindTableRow(t, sumTbl, map[string]string{"Target": "dead", "Monitoring": "unknown", "Total": "0", "Pass": "0", "Fail": "0"})
			t2FindTableRow(t, sumTbl, map[string]string{"Target": "go", "Language": "go", "Monitoring": "true", "Total": "1", "Pass": "1", "Fail": "0"})

			var matHeaders, diffHeaders []string
			if tc.deadFirst {
				matHeaders = []string{"Scenario", "Category", "dead", "go"}
				diffHeaders = []string{"Kind", "Scenario / Field", "dead", "go", "Description"}
			} else {
				matHeaders = []string{"Scenario", "Category", "go", "dead"}
				diffHeaders = []string{"Kind", "Scenario / Field", "go", "dead", "Description"}
			}
			matTbl := t2FindTableByHeaders(t, md, matHeaders...)
			t2FindTableRow(t, matTbl, map[string]string{"Scenario": "PUB-01", "dead": "MISSING", "go": StatusPass})
			diffTbl := t2FindTableByHeaders(t, md, diffHeaders...)
			t2FindTableRow(t, diffTbl, map[string]string{"Kind": "target_error", "dead": StatusError, "go": "OK"})
			t2FindTableRow(t, diffTbl, map[string]string{"Kind": "scenario_verdict", "Scenario / Field": "PUB-01", "dead": "MISSING", "go": StatusPass})

			if len(md.DiagnosticsBullets) != 1 || !strings.Contains(md.DiagnosticsBullets[0], "Target dead Error") {
				t.Errorf("DiagnosticsBullets unexpected: %v", md.DiagnosticsBullets)
			}
		})
	}
}

func TestT2_F6_05_DuplicateTargetNameRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sentinel := filepath.Join(dir, "launched.log")
	wrapper, _ := t2WriteLaunchSentinelWrapper(t, sentinel, "--mode=conformant-go", "--fast-timings")

	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "exact_duplicate",
			args:    []string{"--proxy-target=go=" + wrapper, "--proxy-target=go=" + wrapper, "--filter=^PUB-01$"},
			wantErr: `duplicate target name "go"`,
		},
		{
			name:    "trimmed_whitespace_duplicate",
			args:    []string{"--proxy-target=go=" + wrapper, "--proxy-target= go =" + wrapper, "--filter=^PUB-01$"},
			wantErr: `duplicate target name "go"`,
		},
		{
			name:    "missing_equals",
			args:    []string{"--proxy-target=noequals", "--filter=^PUB-01$"},
			wantErr: "must be in non-empty name=command format",
		},
		{
			name:    "empty_target_name",
			args:    []string{"--proxy-target==" + wrapper, "--filter=^PUB-01$"},
			wantErr: "must be in non-empty name=command format",
		},
		{
			name:    "empty_command",
			args:    []string{"--proxy-target=go=", "--filter=^PUB-01$"},
			wantErr: `target "go": command cannot be empty`,
		},
		{
			name:    "unterminated_quote",
			args:    []string{`--proxy-target=go="unterminated`, "--filter=^PUB-01$"},
			wantErr: "unterminated quote",
		},
		{
			name:    "trailing_backslash",
			args:    []string{`--proxy-target=go=trailing\`, "--filter=^PUB-01$"},
			wantErr: "trailing escape backslash",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runCLI(t, 15*time.Second, tc.args...)
			t2AssertErrorExit(t, res, tc.wantErr)
		})
	}
	t2AssertFileNotExist(t, sentinel)
}

func TestT2_F6_06_CompareFlagConflictsRejected(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sentinel := filepath.Join(dir, "launched.log")
	wrapper, _ := t2WriteLaunchSentinelWrapper(t, sentinel, "--mode=conformant-go", "--fast-timings")
	aJSON := t2RunValidJSONForCompare(t, dir, "a.json", []string{"--mode=conformant-go", "--fast-timings"}, "^PUB-01$")
	bJSON := t2RunValidJSONForCompare(t, dir, "b.json", []string{"--mode=conformant-rust-nomon", "--fast-timings"}, "^PUB-01$")

	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "compare_with_proxy_cmd",
			args:    []string{"--compare=" + aJSON + "," + bJSON, "--proxy-cmd=" + wrapper},
			wantErr: "--compare cannot be combined",
		},
		{
			name:    "compare_with_proxy_arg_only",
			args:    []string{"--compare=" + aJSON + "," + bJSON, "--proxy-arg=--x"},
			wantErr: "--proxy-arg requires --proxy-cmd",
		},
		{
			name:    "compare_with_proxy_cmd_and_arg",
			args:    []string{"--compare=" + aJSON + "," + bJSON, "--proxy-cmd=" + wrapper, "--proxy-arg=--x"},
			wantErr: "--compare cannot be combined",
		},
		{
			name:    "compare_with_proxy_target",
			args:    []string{"--compare=" + aJSON + "," + bJSON, "--proxy-target=go=" + wrapper},
			wantErr: "--compare cannot be combined",
		},
		{
			name:    "compare_with_filter",
			args:    []string{"--compare=" + aJSON + "," + bJSON, "--filter=^PUB-01$"},
			wantErr: "--compare cannot be combined",
		},
		{
			name:    "compare_with_strict_unsupported",
			args:    []string{"--compare=" + aJSON + "," + bJSON, "--strict-unsupported"},
			wantErr: "--compare cannot be combined",
		},
		{
			name:    "compare_with_list",
			args:    []string{"--compare=" + aJSON + "," + bJSON, "--list"},
			wantErr: "--list cannot be combined",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outFile := filepath.Join(dir, tc.name+"_out.json")
			args := append(slices.Clone(tc.args), "--report-json="+outFile)
			res := runCLI(t, 15*time.Second, args...)
			t2AssertErrorExit(t, res, tc.wantErr)
			t2AssertFileNotExist(t, outFile)
		})
	}
	t2AssertFileNotExist(t, sentinel)
}

// --- E2E-F7 Boundary, Negative & Corner-Case Conformance Scenarios (6 tests) ---

func TestT2_F7_01_LargePayload10MBBoundary(t *testing.T) {
	t.Parallel()

	t.Run("three_way_without_fast_timings", func(t *testing.T) {
		wGo, _ := writeProxyWrapper(t, "--mode=conformant-go")
		wCpp, _ := writeProxyWrapper(t, "--mode=conformant-cpp-partial")
		wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "compare.json")

		res := runCLI(
			t, 45*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter=^(PUB-06a|PUB-06b)$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "compare", false)

		for _, name := range []string{"go", "cpp", "rust"} {
			tr := rep.Targets[name]
			if tr == nil || tr.Summary == nil || *tr.Summary != (ParsedSummary{Total: 2, Pass: 2}) {
				t.Fatalf("target %q summary unexpected: %+v", name, tr)
			}
			wantTrace := StatusPass
			if name == "rust" {
				wantTrace = StatusSkippedNoMonitoring
			}
			for _, id := range []string{"PUB-06a", "PUB-06b"} {
				sc := t2MustTargetScenario(t, tr, id)
				if sc.Status != StatusPass || sc.TraceStatus != wantTrace || sc.ProxyRestarted {
					t.Errorf("%s/%s unexpected: %+v (want trace=%s)", name, id, *sc, wantTrace)
				}
			}
		}
	})

	t.Run("negative_wrong_rpc_mutated_payload", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=wrong-rpc"},
			"--filter=^PUB-06a$",
		)
		sc := t2MustScenario(t, rep, "PUB-06a")
		if sc.Status != StatusFail || !strings.Contains(sc.Error, "2 MiB payload length mismatch") {
			t.Errorf("PUB-06a wrong-rpc unexpected: %+v", *sc)
		}
	})
}

func TestT2_F7_02_HighBatchCountSplit1000Boundary(t *testing.T) {
	t.Parallel()

	t.Run("three_way_without_fast_timings", func(t *testing.T) {
		wGo, _ := writeProxyWrapper(t, "--mode=conformant-go")
		wCpp, _ := writeProxyWrapper(t, "--mode=conformant-cpp-partial")
		wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "compare.json")

		res := runCLI(
			t, 45*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter=^(PUB-03|LEASE-01)$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "compare", false)
		for _, name := range []string{"go", "cpp", "rust"} {
			tr := rep.Targets[name]
			if tr == nil || tr.Summary == nil || *tr.Summary != (ParsedSummary{Total: 2, Pass: 2}) {
				t.Fatalf("target %q summary unexpected: %+v", name, tr)
			}
			for _, id := range []string{"PUB-03", "LEASE-01"} {
				if sc := t2MustTargetScenario(t, tr, id); sc.Status != StatusPass {
					t.Errorf("%s/%s status = %q, want PASS", name, id, sc.Status)
				}
			}
		}
	})

	t.Run("negative_out_of_order_rpc", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=out-of-order-rpc"},
			"--filter=^(PUB-03|LEASE-01)$",
		)
		if rep.Summary == nil || *rep.Summary != (ParsedSummary{Total: 2, Fail: 2}) {
			t.Errorf("Summary = %+v, want Total=2 Fail=2", rep.Summary)
		}
		if sc := t2MustScenario(t, rep, "PUB-03"); sc.Status != StatusFail || !strings.Contains(sc.Error, "mismatch") {
			t.Errorf("PUB-03 unexpected: %+v", *sc)
		}
		if sc := t2MustScenario(t, rep, "LEASE-01"); sc.Status != StatusFail || !strings.Contains(sc.Error, "delivered metadata mismatch") {
			t.Errorf("LEASE-01 unexpected: %+v", *sc)
		}
	})
}

func TestT2_F7_03_PartialBatchEODErrorInfoBoundary(t *testing.T) {
	t.Parallel()

	t.Run("three_way_without_fast_timings", func(t *testing.T) {
		wGo, _ := writeProxyWrapper(t, "--mode=conformant-go")
		wCpp, _ := writeProxyWrapper(t, "--mode=conformant-cpp-partial")
		wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "compare.json")

		res := runCLI(
			t, 45*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter=^(EOD-03|EOD-03b)$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "compare", false)
		for _, name := range []string{"go", "cpp", "rust"} {
			tr := rep.Targets[name]
			if tr == nil || tr.Summary == nil || *tr.Summary != (ParsedSummary{Total: 2, Pass: 2}) {
				t.Fatalf("target %q summary unexpected: %+v", name, tr)
			}
			wantTrace := StatusPass
			if name == "rust" {
				wantTrace = StatusSkippedNoMonitoring
			}
			for _, id := range []string{"EOD-03", "EOD-03b"} {
				sc := t2MustTargetScenario(t, tr, id)
				if sc.Status != StatusPass || sc.TraceStatus != wantTrace {
					t.Errorf("%s/%s unexpected: %+v (want trace=%s)", name, id, *sc, wantTrace)
				}
			}
		}
	})

	t.Run("negative_unsupported_ack_with_result_strict", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=conformant-go", "--unsupported=ack_with_result"},
			"--filter=^(EOD-03|EOD-03b)$",
			"--strict-unsupported",
		)
		wantSum := ParsedSummary{Total: 2, UnsupportedByClient: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		for _, id := range []string{"EOD-03", "EOD-03b"} {
			sc := t2MustScenario(t, rep, id)
			if sc.Status != StatusUnsupportedByClient || !strings.Contains(sc.Reason, "missing required capabilities: ack_with_result") {
				t.Errorf("%s unexpected: %+v", id, *sc)
			}
		}
	})
}

func TestT2_F7_04_MidStreamTransientRecoveryBoundary(t *testing.T) {
	t.Parallel()

	t.Run("three_way_without_fast_timings", func(t *testing.T) {
		wGo, _ := writeProxyWrapper(t, "--mode=conformant-go")
		wCpp, _ := writeProxyWrapper(t, "--mode=conformant-cpp-partial")
		wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "compare.json")

		res := runCLI(
			t, 45*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter=^(PULL-02|PULL-02b|PULL-02c|PULL-02d)$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "compare", false)
		for _, name := range []string{"go", "cpp", "rust"} {
			tr := rep.Targets[name]
			if tr == nil || tr.Summary == nil || *tr.Summary != (ParsedSummary{Total: 4, Pass: 4}) {
				t.Fatalf("target %q summary unexpected: %+v", name, tr)
			}
			for _, id := range []string{"PULL-02", "PULL-02b", "PULL-02c", "PULL-02d"} {
				sc := t2MustTargetScenario(t, tr, id)
				if sc.Status != StatusPass {
					t.Errorf("%s/%s status = %q, want PASS", name, id, sc.Status)
				}
				if (id == "PULL-02b" || id == "PULL-02c") && sc.ProxyRestarted {
					t.Errorf("%s/%s expected ProxyRestarted=false across IsolateProxy boundary, got true", name, id)
				}
			}
		}
	})

	t.Run("negative_go_retry_resource_exhausted", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=conformant-go", "--fault=retry-resource-exhausted"},
			"--filter=^PULL-02c$",
		)
		sc := t2MustScenario(t, rep, "PULL-02c")
		if sc.Status != StatusFail || !strings.Contains(sc.Error, "retried non-retryable error") {
			t.Errorf("PULL-02c go retry divergence unexpected: %+v", *sc)
		}
	})

	t.Run("negative_cpp_fatal_resource_exhausted", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=conformant-cpp-partial", "--fault=fatal-resource-exhausted"},
			"--filter=^PULL-02c$",
		)
		sc := t2MustScenario(t, rep, "PULL-02c")
		if sc.Status != StatusFail || !strings.Contains(sc.Error, "reconnect failed") {
			t.Errorf("PULL-02c cpp fatal divergence unexpected: %+v", *sc)
		}
	})
}

func TestT2_F7_05_RapidShutdownNackImmediatelyBoundary(t *testing.T) {
	t.Parallel()

	t.Run("three_way_without_fast_timings", func(t *testing.T) {
		wGo, _ := writeProxyWrapper(t, "--mode=conformant-go")
		wCpp, _ := writeProxyWrapper(t, "--mode=conformant-cpp-partial")
		wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "compare.json")

		res := runCLI(
			t, 30*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter=^(LIFE-02|LIFE-03)$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "compare", false)
		for _, name := range []string{"go", "cpp", "rust"} {
			tr := rep.Targets[name]
			if tr == nil || tr.Summary == nil || *tr.Summary != (ParsedSummary{Total: 2, Pass: 2}) {
				t.Fatalf("target %q summary unexpected: %+v", name, tr)
			}
			for _, id := range []string{"LIFE-02", "LIFE-03"} {
				if sc := t2MustTargetScenario(t, tr, id); sc.Status != StatusPass || sc.ProxyRestarted {
					t.Errorf("%s/%s unexpected: %+v", name, id, *sc)
				}
			}
		}
	})

	t.Run("negative_missing_rpc_timeout_and_restart", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=missing-rpc"},
			"--filter=^(LIFE-02|LIFE-03)$",
			"--scenario-timeout=500ms",
		)
		wantSum := ParsedSummary{Total: 2, Fail: 2, TimeoutCount: 2}
		if rep.Summary == nil || *rep.Summary != wantSum {
			t.Errorf("Summary = %+v, want %+v", rep.Summary, wantSum)
		}
		life02 := t2MustScenario(t, rep, "LIFE-02")
		life03 := t2MustScenario(t, rep, "LIFE-03")
		if life02.Status != StatusFail || life02.StatusDetail != StatusTimeout || life02.ProxyRestarted {
			t.Errorf("LIFE-02 expected FAIL/TIMEOUT ProxyRestarted=false, got %+v", *life02)
		}
		if life03.Status != StatusFail || life03.StatusDetail != StatusTimeout || !life03.ProxyRestarted {
			t.Errorf("LIFE-03 expected FAIL/TIMEOUT ProxyRestarted=true (after LIFE-02 failDirty restart), got %+v", *life03)
		}
	})
}

func TestT2_F7_06_MaxExtensionFlowControlAndMismatchBoundary(t *testing.T) {
	t.Parallel()

	t.Run("three_way_with_fast_timings", func(t *testing.T) {
		wGo, _ := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		wCpp, _ := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
		wRust, _ := writeProxyWrapper(t, "--mode=conformant-rust-nomon", "--fast-timings")
		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "compare.json")

		res := runCLI(
			t, 45*time.Second,
			"--proxy-target=go="+wGo,
			"--proxy-target=cpp="+wCpp,
			"--proxy-target=rust="+wRust,
			"--filter=^(PUB-10|PFC-03|PFC-03b|LEASE-03)$",
			"--report-json="+jsonPath,
		)
		if res.ExitCode != 0 || res.TimedOut || res.Err != nil {
			t.Fatalf("CLI failed: exit=%d timedOut=%v err=%v\nstderr:\n%s", res.ExitCode, res.TimedOut, res.Err, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		t2AssertJSONV1Base(t, rep, "compare", false)

		for _, name := range []string{"go", "cpp"} {
			tr := rep.Targets[name]
			if tr == nil || tr.Summary == nil || *tr.Summary != (ParsedSummary{Total: 4, Pass: 4}) {
				t.Fatalf("target %q summary unexpected: %+v", name, tr)
			}
			for _, id := range []string{"PUB-10", "PFC-03", "PFC-03b", "LEASE-03"} {
				if sc := t2MustTargetScenario(t, tr, id); sc.Status != StatusPass {
					t.Errorf("%s/%s status = %q, want PASS", name, id, sc.Status)
				}
			}
		}

		tRust := rep.Targets["rust"]
		if tRust == nil || tRust.Summary == nil || *tRust.Summary != (ParsedSummary{Total: 4, Pass: 2, UnsupportedByClient: 2}) {
			t.Fatalf("rust summary unexpected: %+v", tRust)
		}
		for _, id := range []string{"PUB-10", "LEASE-03"} {
			if sc := t2MustTargetScenario(t, tRust, id); sc.Status != StatusPass {
				t.Errorf("rust/%s status = %q, want PASS", id, sc.Status)
			}
		}
		for _, id := range []string{"PFC-03", "PFC-03b"} {
			if sc := t2MustTargetScenario(t, tRust, id); sc.Status != StatusUnsupportedByClient {
				t.Errorf("rust/%s status = %q, want UNSUPPORTED_BY_CLIENT", id, sc.Status)
			}
		}
	})

	t.Run("negative_eod02_noneod_raw_status", func(t *testing.T) {
		_, rep, _ := t2RunExpectFailure(
			t, 25*time.Second,
			[]string{"--mode=conformant-go", "--fault=eod02-noneod-raw-status", "--fast-timings"},
			"--filter=^EOD-02$",
		)
		sc := t2MustScenario(t, rep, "EOD-02")
		if sc.Status != StatusFail ||
			!strings.Contains(sc.Error, "want status=ACK_STATUS_SUCCESS") ||
			!strings.Contains(sc.Error, "got status=ACK_STATUS_PERMISSION_DENIED") {
			t.Errorf("EOD-02 negative raw status unexpected: %+v", *sc)
		}
	})
}
