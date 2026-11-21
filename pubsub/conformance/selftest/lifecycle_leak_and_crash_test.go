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

package selftest_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
)

func TestLifecycle_HungProxy_SIGKILLPGID_CLI(t *testing.T) {
	cases := []struct {
		name, fault, filter, wantID, wantErrSub string
		extraCLI                                []string
		wantSuiteTO                             bool
		maxDur                                  time.Duration
	}{
		{"ScenarioTimeout_HangPublish_IgnoreShutdownAndSIGTERM", "hang-publish,ignore-shutdown,ignore-sigterm", "^PUB-01$", "PUB-01", "scenario exceeded timeout", []string{"--scenario-timeout=350ms"}, false, 10 * time.Second},
		{"ScenarioTimeout_HangSubscribe_IgnoreShutdownAndSIGTERM", "hang-subscribe,ignore-shutdown,ignore-sigterm", "^PULL-01$", "PULL-01", "scenario exceeded timeout", []string{"--scenario-timeout=350ms"}, false, 10 * time.Second},
		{"SuiteTimeout_HangPublish_IgnoreShutdownAndSIGTERM", "hang-publish,ignore-shutdown,ignore-sigterm", "^(PUB-01|PULL-01)$", "PUB-01", "suite deadline exceeded", []string{"--scenario-timeout=10s", "--suite-timeout=1s"}, true, 12 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			pidLog, repJSON := filepath.Join(dir, "pids.log"), filepath.Join(dir, "hang.json")
			trackPIDLog(t, pidLog)

			args := append(proxyCmdArgs("--profile=go", "--fault="+tc.fault, "--pid-log="+pidLog, "--spawn-descendant"),
				append([]string{"--filter=" + tc.filter, "--report-json=" + repJSON}, tc.extraCLI...)...)
			start := time.Now()
			res := runCLI(t, 35*time.Second, nil, args...)
			if dt := time.Since(start); dt > tc.maxDur {
				t.Fatalf("%s took %v (>%v)", tc.name, dt, tc.maxDur)
			}
			if res.ExitCode != 1 {
				t.Fatalf("expected exit 1 on %s, got %d\nstdout:\n%s\nstderr:\n%s", tc.name, res.ExitCode, res.Stdout, res.Stderr)
			}
			rep, rawMap := mustLoadJSONReport(t, repJSON)
			if rep.TimedOut != tc.wantSuiteTO || !strings.Contains(res.Stdout, "FAIL (TIMEOUT)") || len(rep.Scenarios) != 1 || rep.Scenarios[0].ID != tc.wantID || rep.Scenarios[0].Status != harness.StatusTimeout || !strings.Contains(rep.Scenarios[0].Error, tc.wantErrSub) {
				t.Fatalf("%s unexpected report: timedOut=%t want=%t scenarios=%+v stdout=%s", tc.name, rep.TimedOut, tc.wantSuiteTO, rep.Scenarios, res.Stdout)
			}
			if rawSc := rawScenarioByID(t, rawMap, tc.wantID); rawSc["status"] != "FAIL" || rawSc["status_detail"] != "TIMEOUT" {
				t.Fatalf("expected JSON status=FAIL & status_detail=TIMEOUT, got %+v", rawSc)
			}
			if tc.wantSuiteTO && (len(rep.Targets) != 1 || rep.Targets[0].TargetError == "" || strings.Contains(res.Stdout, "PULL-01")) {
				t.Fatalf("suite-timeout expected TargetError and skipped PULL-01, got targets=%+v stdout=%s", rep.Targets, res.Stdout)
			}
			assertAllPIDsDead(t, pidLog, 2, true)
		})
	}

	t.Run("ExternalSIGTERM_ToCLIBinary_ReapsProxyAndDescendant", func(t *testing.T) {
		dir := t.TempDir()
		pidLog, startedFile := filepath.Join(dir, "pids.log"), filepath.Join(dir, "started.flag")
		trackPIDLog(t, pidLog)

		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()

		args := append(proxyCmdArgs("--profile=go", "--fault=hang-publish,ignore-shutdown,ignore-sigterm", "--pid-log="+pidLog, "--started-file="+startedFile, "--spawn-descendant"),
			"--filter=^PUB-01$", "--scenario-timeout=15s")
		cmd := exec.CommandContext(ctx, cliBinaryPath, args...)
		cmd.Env = envWith(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
		var outBuf, errBuf bytes.Buffer
		cmd.Stdout, cmd.Stderr = &outBuf, &errBuf
		if err := cmd.Start(); err != nil {
			t.Fatalf("start CLI: %v", err)
		}
		if !waitForFile(startedFile, 10*time.Second) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("proxy never reached Publish RPC\nstderr:\n%s", errBuf.String())
		}

		sigAt := time.Now()
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("send SIGTERM to CLI: %v", err)
		}
		waitErr := cmd.Wait()
		if dt := time.Since(sigAt); dt > 12*time.Second {
			t.Fatalf("CLI took %v after SIGTERM (>12s)", dt)
		}
		if ctx.Err() != nil || waitErr == nil {
			t.Fatalf("expected bounded non-zero exit after SIGTERM: ctxErr=%v waitErr=%v\nstderr:\n%s", ctx.Err(), waitErr, errBuf.String())
		}
		assertAllPIDsDead(t, pidLog, 2, true)
	})
}

func TestLifecycle_HungProxyAndRecovery_Runner(t *testing.T) {
	t.Run("ScenarioTimeout_DescendantReap_AndProxyRestarted", func(t *testing.T) {
		pidLog := filepath.Join(t.TempDir(), "pids.log")
		trackPIDLog(t, pidLog)

		target := helperTargetSpec(t, "hang-then-ok", pidLog, "--fault=hang-publish,ignore-shutdown,ignore-sigterm", "--spawn-descendant")
		target.ShutdownTimeout = 200 * time.Millisecond
		rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^(PUB-01|PULL-01)$", ScenarioTimeout: 300 * time.Millisecond}, target)
		byID := statusByID(rep.Scenarios)
		if byID["PUB-01"].Status != harness.StatusTimeout || byID["PULL-01"].Status != harness.StatusPass || !byID["PULL-01"].ProxyRestarted {
			t.Fatalf("expected PUB-01=TIMEOUT then PULL-01=PASS(ProxyRestarted=true), got %+v", rep.Scenarios)
		}
		assertAllPIDsDead(t, pidLog, 4, true)
	})

	t.Run("SuiteTimeout_And_ContextCancellation_ReapDescendants", func(t *testing.T) {
		for _, mode := range []string{"suite-timeout", "ctx-cancel"} {
			t.Run(mode, func(t *testing.T) {
				dir := t.TempDir()
				pidLog, startedFile := filepath.Join(dir, "pids.log"), filepath.Join(dir, "started.flag")
				trackPIDLog(t, pidLog)

				target := helperTargetSpec(t, mode, pidLog, "--fault=hang-publish,ignore-shutdown,ignore-sigterm", "--started-file="+startedFile, "--spawn-descendant")
				target.ShutdownTimeout = 200 * time.Millisecond
				cfg := harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^(PUB-01|PULL-01)$", ScenarioTimeout: 10 * time.Second}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				wantSub := "suite deadline exceeded"
				if mode == "suite-timeout" {
					cfg.SuiteTimeout = 1 * time.Second
				} else {
					wantSub = "suite canceled"
					go func() {
						waitForFile(startedFile, 5*time.Second)
						cancel()
					}()
				}
				rep := mustRunTarget(t, ctx, cfg, target)
				if !rep.TimedOut || rep.TargetError == "" || len(rep.Scenarios) != 1 || rep.Scenarios[0].ID != "PUB-01" || rep.Scenarios[0].Status != harness.StatusTimeout || !strings.Contains(rep.Scenarios[0].Error, wantSub) {
					t.Fatalf("%s expected TimedOut=true, TargetError, and single PUB-01 StatusTimeout(%q), got rep=%+v", mode, wantSub, rep)
				}
				assertAllPIDsDead(t, pidLog, 2, true)
			})
		}
	})
}

func TestLifecycle_StartupAndMidScenarioCrashes(t *testing.T) {
	t.Run("CLI_StartupFailures", func(t *testing.T) {
		for _, tc := range []struct {
			name, fault string
			maxDur      time.Duration
		}{
			{"CrashStartup_NonZeroExit", "crash-startup", 15 * time.Second},
			{"BadReady_ClosedStdout", "bad-ready", 15 * time.Second},
			{"BadReady_HangAfterBadLine", "bad-ready-hang", 3 * time.Second},
			{"CrashAfterReady", "crash-after-ready", 15 * time.Second},
		} {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				pidLog, repJSON := filepath.Join(dir, "pids.log"), filepath.Join(dir, "startup.json")
				trackPIDLog(t, pidLog)

				start := time.Now()
				args := append(proxyCmdArgs("--fault="+tc.fault, "--pid-log="+pidLog, "--spawn-descendant", "--ignore-sigterm"), "--filter=^PUB-01$", "--report-json="+repJSON)
				res := runCLI(t, 20*time.Second, nil, args...)
				if dt := time.Since(start); dt > tc.maxDur {
					t.Fatalf("%s took %v (>%v)", tc.name, dt, tc.maxDur)
				}
				if res.ExitCode != 1 {
					t.Fatalf("%s expected exit 1, got %d", tc.name, res.ExitCode)
				}
				rep, _ := mustLoadJSONReport(t, repJSON)
				if len(rep.Targets) != 1 || rep.Targets[0].TargetError == "" || len(rep.Scenarios) != 0 {
					t.Fatalf("%s expected TargetError and empty Scenarios, got %+v", tc.name, rep)
				}
				assertAllPIDsDead(t, pidLog, 2, true)
			})
		}
	})

	t.Run("Runner_StartupHangsAndSilentExit_WithDescendant", func(t *testing.T) {
		for _, tc := range []struct {
			name, fault string
			timeout     time.Duration
		}{
			{"SilentExit_NoReady", "silent-exit,ignore-sigterm", 2 * time.Second},
			{"CrashAfterReady_WithDescendant", "crash-after-ready,ignore-sigterm", 2 * time.Second},
			{"HangBeforeReady_WithDescendant", "hang-before-ready,ignore-sigterm", 500 * time.Millisecond},
			{"HangGetCapabilities_WithDescendant", "hang-caps,ignore-sigterm", 500 * time.Millisecond},
		} {
			t.Run(tc.name, func(t *testing.T) {
				pidLog := filepath.Join(t.TempDir(), "pids.log")
				trackPIDLog(t, pidLog)
				target := helperTargetSpec(t, tc.name, pidLog, "--fault="+tc.fault, "--spawn-descendant")
				target.StartupTimeout = tc.timeout
				rep, err := mustNewRunner(t, harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^PUB-01$"}).RunTarget(context.Background(), target)
				if err == nil || rep == nil || rep.TargetError == "" {
					t.Fatalf("%s expected startup error & TargetError, got err=%v rep=%+v", tc.name, err, rep)
				}
				if tc.name == "SilentExit_NoReady" && (!strings.Contains(rep.TargetError, "READY") || strings.Contains(rep.TargetError, "timed out")) {
					t.Fatalf("SilentExit_NoReady expected READY exit error without timeout, got %q", rep.TargetError)
				}
				assertAllPIDsDead(t, pidLog, 2, true)
			})
		}
	})

	t.Run("CLI_MidScenarioCrash_StatusError", func(t *testing.T) {
		dir := t.TempDir()
		pidLog, repJSON := filepath.Join(dir, "pids.log"), filepath.Join(dir, "midcrash.json")
		trackPIDLog(t, pidLog)

		args := append(proxyCmdArgs("--fault=crash-mid-scenario,ignore-sigterm", "--pid-log="+pidLog, "--spawn-descendant"),
			"--filter=^(PUB-01|PULL-01)$", "--report-json="+repJSON, "--scenario-timeout=5s")
		res := runCLI(t, 20*time.Second, nil, args...)
		if res.ExitCode != 1 {
			t.Fatalf("expected exit 1 on mid-scenario crash, got %d stderr=%s", res.ExitCode, res.Stderr)
		}
		rep, rawMap := mustLoadJSONReport(t, repJSON)
		if rep.Summary.ErrorCount != 2 || len(rep.Scenarios) != 2 {
			t.Fatalf("expected 2 StatusError results, got summary=%+v scenarios=%+v", rep.Summary, rep.Scenarios)
		}
		for _, id := range []string{"PUB-01", "PULL-01"} {
			if statusByID(rep.Scenarios)[id].Status != harness.StatusError {
				t.Fatalf("expected %s StatusError, got %+v", id, statusByID(rep.Scenarios)[id])
			}
			if rawSc := rawScenarioByID(t, rawMap, id); rawSc["status"] != "FAIL" || rawSc["status_detail"] != "ERROR" {
				t.Fatalf("expected JSON status=FAIL & status_detail=ERROR for %s, got %+v", id, rawSc)
			}
		}
		assertAllPIDsDead(t, pidLog, 4, true)
	})

	t.Run("Runner_CrashOnceRecovery_And_StopOnProxyCrash", func(t *testing.T) {
		dir := t.TempDir()
		pidLog, onceMarker := filepath.Join(dir, "pids.log"), filepath.Join(dir, "crashed.once")
		trackPIDLog(t, pidLog)

		target := helperTargetSpec(t, "crash-once", pidLog, "--fault=crash-once,ignore-sigterm", "--once-marker="+onceMarker, "--spawn-descendant")
		rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^(PUB-01|PULL-01)$", ScenarioTimeout: 5 * time.Second}, target)
		byID := statusByID(rep.Scenarios)
		if byID["PUB-01"].Status != harness.StatusError || byID["PULL-01"].Status != harness.StatusPass || !byID["PULL-01"].ProxyRestarted || rep.TargetError != "" {
			t.Fatalf("crash-once recovery mismatch: targetErr=%q scenarios=%+v", rep.TargetError, rep.Scenarios)
		}
		assertAllPIDsDead(t, pidLog, 4, true)

		pidStopLog := filepath.Join(dir, "pids-stop.log")
		trackPIDLog(t, pidStopLog)
		stopTarget := helperTargetSpec(t, "stop-on-crash", pidStopLog, "--fault=crash-mid-scenario,ignore-sigterm", "--spawn-descendant")
		sRep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{stopTarget}, Scenarios: scenarios.All(), Filter: "^(PUB-01|PULL-01)$", StopOnProxyCrash: true, ScenarioTimeout: 5 * time.Second}, stopTarget)
		if len(sRep.Scenarios) != 1 || sRep.Scenarios[0].Status != harness.StatusError || sRep.TargetError == "" {
			t.Fatalf("StopOnProxyCrash expected abort after 1 scenario with TargetError, got %+v", sRep)
		}
		assertAllPIDsDead(t, pidStopLog, 2, true)
	})
}
