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
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestTier5_LiveOSSignalTermination_SIGTERMAndSIGINT(t *testing.T) {
	cliBin := buildConformanceCLIBinary(t)

	for _, tc := range []struct {
		name string
		sig  syscall.Signal
	}{
		{"SIGTERM", syscall.SIGTERM},
		{"SIGINT", syscall.SIGINT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "sleep.pid")
			faultEnteredFile := filepath.Join(dir, "hang_entered.sentinel")
			outJSON := filepath.Join(dir, "sig.json")
			outMD := filepath.Join(dir, "sig.md")

			marker := newRunMarker(t)
			t.Cleanup(func() { killProcessesWithMarker(marker) })

			wHung := filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))
			hungScript := fmt.Sprintf("#!/bin/sh\ntrap '' TERM INT\nsleep 60 </dev/null >/dev/null 2>&1 &\nprintf '%%s\\n' \"$!\" > %s\nexport E2E_PROXY_FAULT_ENTERED_FILE=%s\nexec %s %s --mode=conformant-go --fast-timings --fault=hang-publish,ignore-shutdown,ignore-sigterm \"$@\"\n",
				shellQuote(pidFile), shellQuote(faultEnteredFile), shellQuote(proxyBinaryPath), shellQuote("--marker="+marker))
			writeExecutableFile(t, wHung, []byte(hungScript))

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			cmd := exec.CommandContext(ctx, cliBin,
				"--proxy-cmd="+wHung,
				"--filter=^PUB-01$",
				"--scenario-timeout=15s",
				"--report-json="+outJSON,
				"--report-md="+outMD,
			)
			cmd.Dir = repoRootDir
			var stdoutBuf, stderrBuf syncBuffer
			cmd.Stdout = &stdoutBuf
			cmd.Stderr = &stderrBuf

			if err := cmd.Start(); err != nil {
				t.Fatalf("Start CLI: %v", err)
			}
			waitDone := make(chan error, 1)
			go func() { waitDone <- cmd.Wait() }()

			var sleepPID int
			sleepInProc := false
			proxyStarted := false
			deadline := time.Now().Add(8 * time.Second)
			for time.Now().Before(deadline) {
				if sleepPID == 0 {
					if raw, err := os.ReadFile(pidFile); err == nil {
						if p, pErr := strconv.Atoi(strings.TrimSpace(string(raw))); pErr == nil && p > 0 {
							sleepPID = p
							sp := p
							t.Cleanup(func() { _ = syscall.Kill(sp, syscall.SIGKILL) })
						}
					}
				}
				if sleepPID > 0 && !sleepInProc {
					if _, err := os.Stat(fmt.Sprintf("/proc/%d", sleepPID)); err == nil {
						sleepInProc = true
					}
				}
				if sleepInProc {
					if procs, _ := findProcessesWithMarker(marker); len(procs) > 0 {
						for _, p := range procs {
							if strings.Contains(p.Cmdline, "e2e-test-proxy") {
								proxyStarted = true
								break
							}
						}
					}
				}
				if sleepInProc && proxyStarted {
					break
				}
				select {
				case err := <-waitDone:
					t.Fatalf("CLI exited early before %s could be delivered: err=%v\nstdout:\n%s\nstderr:\n%s", tc.sig, err, stdoutBuf.String(), stderrBuf.String())
				case <-time.After(25 * time.Millisecond):
				}
			}
			if sleepPID <= 0 || !sleepInProc || !proxyStarted {
				_ = cmd.Process.Kill()
				<-waitDone
				t.Fatalf("timed out waiting for proxy/sleep start (sleepPID=%d sleepInProc=%v proxyStarted=%v)\nstderr:\n%s", sleepPID, sleepInProc, proxyStarted, stderrBuf.String())
			}

			// Deterministically wait until Publish enters blockIfFault("hang-publish") before signaling.
			faultEntered := false
			faultDeadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(faultDeadline) {
				if _, err := os.Stat(faultEnteredFile); err == nil {
					faultEntered = true
					break
				}
				select {
				case err := <-waitDone:
					t.Fatalf("CLI exited early before %s could be delivered: err=%v\nstdout:\n%s\nstderr:\n%s", tc.sig, err, stdoutBuf.String(), stderrBuf.String())
				case <-time.After(25 * time.Millisecond):
				}
			}
			if !faultEntered {
				_ = cmd.Process.Kill()
				<-waitDone
				t.Fatalf("timed out waiting for fault-entered sentinel %s\nstderr:\n%s", faultEnteredFile, stderrBuf.String())
			}

			if err := cmd.Process.Signal(tc.sig); err != nil {
				t.Fatalf("Signal(%s) failed: %v", tc.sig, err)
			}

			var waitErr error
			select {
			case waitErr = <-waitDone:
			case <-time.After(15 * time.Second):
				_ = cmd.Process.Kill()
				<-waitDone
				t.Fatalf("CLI did not terminate within 15s after %s\nstderr:\n%s", tc.sig, stderrBuf.String())
			}

			if ctx.Err() != nil {
				t.Fatalf("outer test context expired: %v", ctx.Err())
			}
			var exitErr *exec.ExitError
			if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("expected exit code 1 after %s, got err=%v\nstdout:\n%s\nstderr:\n%s", tc.sig, waitErr, stdoutBuf.String(), stderrBuf.String())
			}

			assertNoOrphanProxy(t, marker)
			t4WaitPIDDead(t, sleepPID, 3*time.Second)

			rep := ReadJSONReport(t, outJSON)
			sc := t4MustScenario(t, rep, "PUB-01")
			if sc.Status != StatusFail || (sc.StatusDetail != StatusTimeout && !strings.Contains(strings.ToLower(sc.Error), "canceled")) {
				t.Fatalf("PUB-01 after %s expected FAIL(TIMEOUT/canceled), got %+v", tc.sig, *sc)
			}
			md := ReadMarkdownReport(t, outMD)
			if !strings.Contains(md.Raw, "PUB-01") || !strings.Contains(md.Raw, "FAIL") {
				t.Fatalf("markdown report after %s missing PUB-01/FAIL:\n%s", tc.sig, md.Raw)
			}
		})
	}
}

func TestTier5_ListValidationAndNoArgs(t *testing.T) {
	t.Run("PositionalExtraArg", func(t *testing.T) {
		res := runCLI(t, 10*time.Second, "extra-arg")
		if res.ExitCode != 1 || !strings.Contains(res.Stderr, `unknown command "extra-arg"`) {
			t.Fatalf("expected unknown command error, got exit=%d stderr=%q", res.ExitCode, res.Stderr)
		}
	})

	t.Run("ListWithPositionalExtraArg", func(t *testing.T) {
		res := runCLI(t, 10*time.Second, "--list", "extra-arg")
		if res.ExitCode != 1 || !strings.Contains(res.Stderr, `unknown command "extra-arg"`) {
			t.Fatalf("expected unknown command error, got exit=%d stderr=%q", res.ExitCode, res.Stderr)
		}
	})

	t.Run("ListInvalidFilterRegex", func(t *testing.T) {
		res := runCLI(t, 10*time.Second, "--list", "--filter=[")
		if res.ExitCode != 1 || !strings.Contains(res.Stderr, "invalid --filter regex") {
			t.Fatalf("expected invalid --filter regex error, got exit=%d stderr=%q", res.ExitCode, res.Stderr)
		}
	})

	t.Run("ListConflictMatrix", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			args    []string
			wantSub string
		}{
			{"ProxyCmd", []string{"--list", "--proxy-cmd=/bin/true"}, "--list cannot be combined"},
			{"ProxyArgWithProxyCmd", []string{"--list", "--proxy-cmd=/bin/true", "--proxy-arg=--foo"}, "--list cannot be combined"},
			{"ProxyArgWithoutProxyCmd", []string{"--list", "--proxy-arg=--foo"}, "--proxy-arg requires --proxy-cmd"},
			{"ProxyTarget", []string{"--list", "--proxy-target=t=/bin/true"}, "--list cannot be combined"},
			{"Compare", []string{"--list", "--compare=a.json,b.json"}, "--list cannot be combined"},
			{"ReportJSON", []string{"--list", "--report-json=" + filepath.Join(t.TempDir(), "r.json")}, "--list cannot be combined"},
			{"ReportMD", []string{"--list", "--report-md=" + filepath.Join(t.TempDir(), "r.md")}, "--list cannot be combined"},
			{"StrictUnsupported", []string{"--list", "--strict-unsupported"}, "--list cannot be combined"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				res := runCLI(t, 10*time.Second, tc.args...)
				if res.ExitCode != 1 || !strings.Contains(res.Stderr, tc.wantSub) {
					t.Fatalf("%v expected %q, got exit=%d stderr=%q", tc.args, tc.wantSub, res.ExitCode, res.Stderr)
				}
			})
		}
	})
}

func TestTier5_CompareDeduplicationAndErrorEdges(t *testing.T) {
	dir := t.TempDir()
	d1JSON := filepath.Join(dir, "d1", "rep.json")
	d1MD := filepath.Join(dir, "d1", "rep.md")
	d2JSON := filepath.Join(dir, "d2", "rep.json")
	d2MD := filepath.Join(dir, "d2", "rep.md")
	for _, d := range []string{filepath.Dir(d1JSON), filepath.Dir(d2JSON)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	t4RunSingleSavedReports(t, 20*time.Second, 0, d1JSON, d1MD, []string{"--mode=conformant-go", "--fast-timings"}, "--filter=^PUB-01$")
	t4RunSingleSavedReports(t, 20*time.Second, 1, d2JSON, d2MD, []string{"--mode=conformant-go", "--fast-timings", "--fault=wrong-payload"}, "--filter=^PUB-01$")

	t.Run("CollidingDefaultStems", func(t *testing.T) {
		cmpJSON := filepath.Join(dir, "default_colliding.json")
		cmpMD := filepath.Join(dir, "default_colliding.md")
		res := runCLI(t, 15*time.Second, "--compare="+d1JSON+","+d2JSON, "--report-json="+cmpJSON, "--report-md="+cmpMD)
		if res.ExitCode != 0 {
			t.Fatalf("--compare failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
		}
		rep := ReadJSONReport(t, cmpJSON)
		tr1 := t4MustTarget(t, rep, "rep")
		tr2 := t4MustTarget(t, rep, "rep-2")
		if t4MustTargetScenario(t, tr1, "PUB-01").Status != StatusPass || t4MustTargetScenario(t, tr2, "PUB-01").Status != StatusFail {
			t.Fatalf("unexpected verdicts across deduplicated stems: tr1=%+v tr2=%+v", tr1.Scenarios, tr2.Scenarios)
		}
		diff := t4MustDiff(t, rep, "scenario_verdict", "PUB-01")
		if diff.Verdicts["rep"] != StatusPass || diff.Verdicts["rep-2"] != StatusFail {
			t.Fatalf("unexpected diff verdicts: %+v", diff.Verdicts)
		}
		md := ReadMarkdownReport(t, cmpMD)
		tbl := t4FindMDTable(t, md, "Scenario", "Category", "rep", "rep-2")
		t4FindMDTableRow(t, tbl, map[string]string{"Scenario": "PUB-01", "rep": StatusPass, "rep-2": StatusFail})
	})

	t.Run("CollidingExplicitTargetNames_PreservesTargets", func(t *testing.T) {
		wGo, mGo := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		wCpp, mCpp := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
		m1JSON := filepath.Join(dir, "m1", "report.json")
		m2JSON := filepath.Join(dir, "m2", "report.json")
		for _, p := range []string{m1JSON, m2JSON} {
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		if res := runCLI(t, 25*time.Second, "--proxy-target=go="+wGo, "--proxy-target=cpp="+wCpp, "--filter=^PUB-01$", "--report-json="+m1JSON); res.ExitCode != 0 {
			t.Fatalf("m1 live failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
		}
		assertNoOrphanProxy(t, mGo)
		assertNoOrphanProxy(t, mCpp)
		if res := runCLI(t, 25*time.Second, "--proxy-target=go="+wGo, "--proxy-target=cpp="+wCpp, "--filter=^PUB-01$", "--report-json="+m2JSON); res.ExitCode != 0 {
			t.Fatalf("m2 live failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
		}
		assertNoOrphanProxy(t, mGo)
		assertNoOrphanProxy(t, mCpp)

		cmpOut := filepath.Join(dir, "explicit_cmp.json")
		res := runCLI(t, 15*time.Second, "--compare="+m1JSON+","+m2JSON, "--report-json="+cmpOut)
		if res.ExitCode != 0 {
			t.Fatalf("explicit colliding --compare failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
		}
		rep := ReadJSONReport(t, cmpOut)
		if len(rep.Targets) != 4 {
			t.Fatalf("expected 4 targets, got %d (%v)", len(rep.Targets), rep.Targets)
		}
		t4MustTarget(t, rep, "go")
		t4MustTarget(t, rep, "cpp")
		for name := range rep.Targets {
			if name == "report" || name == "report-2" {
				t.Fatalf("explicit target name overwritten by file stem: %q", name)
			}
		}
	})

	t.Run("IdenticalReportTwice", func(t *testing.T) {
		res := runCLI(t, 15*time.Second, "--compare="+d1JSON+","+d1JSON)
		if res.ExitCode != 0 || !strings.Contains(res.Stdout, "No behavioral differences detected.") {
			t.Fatalf("identical --compare mismatch: exit=%d stdout=%q stderr=%q", res.ExitCode, res.Stdout, res.Stderr)
		}
	})

	t.Run("CompareDirectoryError", func(t *testing.T) {
		res := runCLI(t, 10*time.Second, "--compare="+t.TempDir())
		if res.ExitCode != 1 || !strings.Contains(res.Stderr, "is a directory") {
			t.Fatalf("expected is a directory error, got exit=%d stderr=%q", res.ExitCode, res.Stderr)
		}
	})

	t.Run("ProxyArgRequiresProxyCmdWithProxyTarget", func(t *testing.T) {
		res := runCLI(t, 10*time.Second, "--proxy-arg=--foo", "--proxy-target=t=/bin/true")
		if res.ExitCode != 1 || !strings.Contains(res.Stderr, "--proxy-arg requires --proxy-cmd") {
			t.Fatalf("expected --proxy-arg requires --proxy-cmd error, got exit=%d stderr=%q", res.ExitCode, res.Stderr)
		}
	})
}

func TestTier5_ProxyExecutableAndFaultBoundaries(t *testing.T) {
	t.Run("NonExecutableScript", func(t *testing.T) {
		script := filepath.Join(t.TempDir(), "nonexec.sh")
		if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		outJSON := filepath.Join(t.TempDir(), "nonexec.json")
		res := runCLI(t, 15*time.Second, "--proxy-cmd="+script, "--filter=^PUB-01$", "--report-json="+outJSON)
		if res.ExitCode != 1 || !strings.Contains(strings.ToLower(res.Stdout+res.Stderr), "permission denied") {
			t.Fatalf("expected permission denied, got exit=%d stdout=%q stderr=%q", res.ExitCode, res.Stdout, res.Stderr)
		}
	})

	t.Run("DirectoryAsProxyCmd", func(t *testing.T) {
		dirCmd := t.TempDir()
		res := runCLI(t, 15*time.Second, "--proxy-cmd="+dirCmd, "--filter=^PUB-01$")
		if res.ExitCode != 1 || !strings.Contains(strings.ToLower(res.Stdout+res.Stderr), "permission denied") {
			t.Fatalf("expected permission denied on directory, got exit=%d stdout=%q stderr=%q", res.ExitCode, res.Stdout, res.Stderr)
		}
	})

	t.Run("HangPull_PULL04_Timeout", func(t *testing.T) {
		w, marker := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings", "--fault=hang-pull")
		outJSON := filepath.Join(t.TempDir(), "hang_pull.json")
		res := runCLI(t, 25*time.Second, "--proxy-cmd="+w, "--filter=^PULL-04$", "--scenario-timeout=1s", "--report-json="+outJSON)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected exit=1 non-timedout, got exit=%d timedOut=%v stderr=%s", res.ExitCode, res.TimedOut, res.Stderr)
		}
		assertNoOrphanProxy(t, marker)
		sc := t4MustScenario(t, ReadJSONReport(t, outJSON), "PULL-04")
		if sc.Status != StatusFail || sc.StatusDetail != StatusTimeout {
			t.Fatalf("PULL-04 hang-pull expected FAIL(TIMEOUT), got %+v", *sc)
		}
	})

	t.Run("HangCreate_PUB01_Timeout", func(t *testing.T) {
		w, marker := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings", "--fault=hang-create")
		outJSON := filepath.Join(t.TempDir(), "hang_create.json")
		res := runCLI(t, 25*time.Second, "--proxy-cmd="+w, "--filter=^PUB-01$", "--scenario-timeout=1s", "--report-json="+outJSON)
		if res.ExitCode != 1 || res.TimedOut {
			t.Fatalf("expected exit=1 non-timedout, got exit=%d timedOut=%v stderr=%s", res.ExitCode, res.TimedOut, res.Stderr)
		}
		assertNoOrphanProxy(t, marker)
		sc := t4MustScenario(t, ReadJSONReport(t, outJSON), "PUB-01")
		if sc.Status != StatusFail || sc.StatusDetail != StatusTimeout {
			t.Fatalf("PUB-01 hang-create expected FAIL(TIMEOUT), got %+v", *sc)
		}
	})

	t.Run("CleanRecovery_PULL01_Pass", func(t *testing.T) {
		outJSON := filepath.Join(t.TempDir(), "clean_pull01.json")
		outMD := filepath.Join(t.TempDir(), "clean_pull01.md")
		_, rep, _, _ := t4RunSingleSavedReports(t, 20*time.Second, 0, outJSON, outMD, []string{"--mode=conformant-go", "--fast-timings"}, "--filter=^PULL-01$")
		if sc := t4MustScenario(t, rep, "PULL-01"); sc.Status != StatusPass {
			t.Fatalf("PULL-01 expected PASS, got %+v", *sc)
		}
	})

	t.Run("UnsupportedBogusFlag", func(t *testing.T) {
		w, marker := writeProxyWrapper(t, "--mode=conformant-go", "--unsupported=bogus-flag")
		outJSON := filepath.Join(t.TempDir(), "bogus.json")
		res := runCLI(t, 15*time.Second, "--proxy-cmd="+w, "--filter=^PUB-01$", "--report-json="+outJSON)
		combined := res.Stdout + res.Stderr
		if res.ExitCode != 1 || !strings.Contains(combined, "unknown unsupported feature") || !strings.Contains(combined, "bogus-flag") {
			t.Fatalf("expected unknown unsupported feature diagnostic, got exit=%d stdout=%q stderr=%q", res.ExitCode, res.Stdout, res.Stderr)
		}
		assertNoOrphanProxy(t, marker)
	})
}

func TestTier5_E2EDiagnosticHelperSurfacesTargetAndInfraErrors(t *testing.T) {
	t.Parallel()

	t.Run("NilReport", func(t *testing.T) {
		err := t2ValidateScenarioFailureReport(nil, "nil report stderr")
		if err == nil {
			t.Fatalf("expected error on nil report")
		}
		msg := err.Error()
		for _, want := range []string{"Summary.Total > 0", "=== E2E failure diagnostics ===", "Stderr:\nnil report stderr"} {
			if !strings.Contains(msg, want) {
				t.Errorf("message missing %q:\n%s", want, msg)
			}
		}
		if strings.Contains(msg, "InfraError:") || strings.Contains(msg, "Summary:") {
			t.Errorf("nil report should not format InfraError/Summary:\n%s", msg)
		}
	})

	t.Run("NilSummary", func(t *testing.T) {
		err := t2ValidateScenarioFailureReport(&ParsedReport{Summary: nil}, "nil summary stderr")
		if err == nil {
			t.Fatalf("expected error on nil Summary")
		}
		msg := err.Error()
		for _, want := range []string{"Summary.Total > 0", `InfraError: ""`, "TimedOut: false", "nil summary stderr"} {
			if !strings.Contains(msg, want) {
				t.Errorf("message missing %q:\n%s", want, msg)
			}
		}
		if strings.Contains(msg, "Summary: Total=") {
			t.Errorf("nil Summary should not format Summary line:\n%s", msg)
		}
	})

	t.Run("ZeroTotal_Isolated", func(t *testing.T) {
		// Empty InfraError and empty TargetError ensure Total <= 0 -> Total < 0 cannot mask via fallthrough.
		for _, rep := range []*ParsedReport{
			{Summary: &ParsedSummary{Total: 0}},
			{
				Summary: &ParsedSummary{Total: 0},
				Targets: map[string]*ParsedTargetReport{"default": {TargetName: "default"}},
			},
		} {
			err := t2ValidateScenarioFailureReport(rep, "zero total stderr")
			if err == nil {
				t.Fatalf("expected error when Summary.Total==0 (rep=%+v)", rep)
			}
			msg := err.Error()
			for _, want := range []string{"Summary.Total > 0", "Summary: Total=0 Pass=0 Fail=0", "zero total stderr"} {
				if !strings.Contains(msg, want) {
					t.Errorf("message missing %q:\n%s", want, msg)
				}
			}
		}
	})

	t.Run("NegativeTotal_Isolated", func(t *testing.T) {
		err := t2ValidateScenarioFailureReport(&ParsedReport{Summary: &ParsedSummary{Total: -1}}, "neg total stderr")
		if err == nil || !strings.Contains(err.Error(), "Summary.Total > 0") || !strings.Contains(err.Error(), "neg total stderr") {
			t.Fatalf("expected Summary.Total > 0 rejection for negative Total, got %v", err)
		}
	})

	t.Run("InfraErrorOnly_PositiveTotal", func(t *testing.T) {
		rep := &ParsedReport{
			InfraError: "infra boom",
			TimedOut:   true,
			Summary:    &ParsedSummary{Total: 1, Fail: 1},
		}
		err := t2ValidateScenarioFailureReport(rep, "infra only stderr")
		if err == nil {
			t.Fatalf("expected error on non-empty InfraError")
		}
		msg := err.Error()
		for _, want := range []string{"unexpected InfraError", "infra boom", "TimedOut: true", "Summary: Total=1 Pass=0 Fail=1", "infra only stderr"} {
			if !strings.Contains(msg, want) {
				t.Errorf("message missing %q:\n%s", want, msg)
			}
		}
	})

	t.Run("TargetErrorOnly_PositiveTotal", func(t *testing.T) {
		rep := &ParsedReport{
			Summary: &ParsedSummary{Total: 1, Fail: 1},
			Targets: map[string]*ParsedTargetReport{
				"default": {TargetName: "default", TargetError: "unexpected target failure"},
			},
		}
		err := t2ValidateScenarioFailureReport(rep, "target err stderr")
		if err == nil {
			t.Fatalf("expected error on non-empty TargetError")
		}
		msg := err.Error()
		for _, want := range []string{
			"unexpected target-level error/timeout",
			`Target["default"].TargetError: "unexpected target failure" (TimedOut=false)`,
			"target err stderr",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("message missing %q:\n%s", want, msg)
			}
		}
	})

	t.Run("TargetTimedOutOnly_PositiveTotal", func(t *testing.T) {
		rep := &ParsedReport{
			Summary: &ParsedSummary{Total: 1, Fail: 1},
			Targets: map[string]*ParsedTargetReport{
				"default": {TargetName: "default", TargetError: "", TimedOut: true},
			},
		}
		err := t2ValidateScenarioFailureReport(rep, "target timeout stderr")
		if err == nil {
			t.Fatalf("expected error on target TimedOut=true even with empty TargetError")
		}
		msg := err.Error()
		for _, want := range []string{
			"unexpected target-level error/timeout",
			`Target["default"].TargetError: "" (TimedOut=true)`,
			"target timeout stderr",
		} {
			if !strings.Contains(msg, want) {
				t.Errorf("message missing %q:\n%s", want, msg)
			}
		}
	})

	t.Run("ValidScenarioFailure_ReturnsNil", func(t *testing.T) {
		rep := &ParsedReport{
			Summary: &ParsedSummary{Total: 1, Fail: 1},
			Targets: map[string]*ParsedTargetReport{
				"default": {TargetName: "default"},
			},
		}
		if err := t2ValidateScenarioFailureReport(rep, "valid stderr"); err != nil {
			t.Fatalf("expected nil on valid scenario failure report, got %v", err)
		}
	})

	t.Run("FormatDiagnostics_CombinedAndSortedTargets", func(t *testing.T) {
		rep := &ParsedReport{
			InfraError: "infra boom",
			TimedOut:   true,
			Summary:    &ParsedSummary{Total: 2, Pass: 1, Fail: 1},
			Targets: map[string]*ParsedTargetReport{
				"zeta":  {TargetName: "zeta", TargetError: "z err"},
				"alpha": {TargetName: "alpha", TargetError: "a err", TimedOut: true},
			},
		}
		out := e2eFormatFailureDiagnostics(rep, "child stderr line")
		for _, want := range []string{
			`InfraError: "infra boom"`,
			"TimedOut: true",
			"Summary: Total=2 Pass=1 Fail=1",
			`Target["alpha"].TargetError: "a err" (TimedOut=true)`,
			`Target["zeta"].TargetError: "z err" (TimedOut=false)`,
			"Stderr:\nchild stderr line",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("formatted diagnostics missing %q:\n%s", want, out)
			}
		}
		if strings.Index(out, `Target["alpha"]`) > strings.Index(out, `Target["zeta"]`) {
			t.Errorf("expected sorted targets alpha before zeta:\n%s", out)
		}
		if nilOut := e2eFormatFailureDiagnostics(nil, "only stderr"); !strings.Contains(nilOut, "=== E2E failure diagnostics ===") || !strings.Contains(nilOut, "Stderr:\nonly stderr") {
			t.Errorf("nil rep format unexpected:\n%s", nilOut)
		}
	})
}
