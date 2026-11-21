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
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/protobuf/proto"
)

const smokeFilter = "^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-01|TRAC-03)$"

func assertMonitoringDisabledPass(t *testing.T, scens []harness.ScenarioResult, rpcLog string) {
	t.Helper()
	byID := statusByID(scens)
	if byID["TRAC-01"].Status != harness.StatusSkippedNoMonitoring || byID["TRAC-01"].TraceStatus != traces.StatusSkippedNoMonitoring {
		t.Fatalf("TRAC-01 expected SKIPPED_NO_MONITORING, got %+v", byID["TRAC-01"])
	}
	if byID["TRAC-03"].Status != harness.StatusPass || byID["TRAC-03"].TraceStatus != traces.StatusSkippedNoMonitoring {
		t.Fatalf("TRAC-03 expected PASS + SKIPPED_NO_MONITORING, got %+v", byID["TRAC-03"])
	}
	for _, id := range []string{"PUB-01", "PORD-02", "PULL-01"} {
		if byID[id].Status != harness.StatusPass {
			t.Fatalf("%s expected PASS, got %+v", id, byID[id])
		}
	}
	rpcText := mustReadString(t, rpcLog)
	if strings.Contains(rpcText, "trace_rpc:") || !strings.Contains(rpcText, "trace_rpcs=0") {
		t.Fatalf("expected 0 ResetTraces/DrainTraces RPCs when supports_monitoring=false, got:\n%s", rpcText)
	}
}

func TestPass_ConformantBaseline_CLIAndRunner(t *testing.T) {
	t.Run("CLI_6SmokeScenarios_AndReports", func(t *testing.T) {
		dir := t.TempDir()
		pidLog := filepath.Join(dir, "pids.log")
		repJSON := filepath.Join(dir, "report.json")
		repMD := filepath.Join(dir, "report.md")
		trackPIDLog(t, pidLog)

		args := append(proxyCmdArgs("--profile=go", "--pid-log="+pidLog, "--spawn-descendant"),
			"--filter="+smokeFilter, "--report-json="+repJSON, "--report-md="+repMD, "--scenario-timeout=10s")
		res := runCLI(t, 25*time.Second, nil, args...)
		if res.ExitCode != 0 {
			t.Fatalf("CLI conformant run failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
		}

		rep, rawMap := mustLoadJSONReport(t, repJSON)
		if rawMap["schema_version"] != harness.ReportSchemaVersion || rep.SchemaVersion != "1.0" || rep.Mode != "single" {
			t.Fatalf("unexpected report metadata: schema=%v mode=%q", rawMap["schema_version"], rep.Mode)
		}
		if rep.Summary == nil || rep.Summary.Total != 6 || rep.Summary.Pass != 6 || rep.Summary.Fail != 0 {
			t.Fatalf("unexpected summary: %+v", rep.Summary)
		}
		byID := statusByID(rep.Scenarios)
		for _, id := range []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"} {
			if byID[id].Status != harness.StatusPass {
				t.Fatalf("scenario %s status=%s err=%s", id, byID[id].Status, byID[id].Error)
			}
		}
		if byID["TRAC-01"].TraceStatus != traces.StatusPass || byID["TRAC-03"].TraceStatus != traces.StatusPass {
			t.Fatalf("expected TRAC-01 & TRAC-03 TraceStatus=PASS, got TRAC-01=%s TRAC-03=%s", byID["TRAC-01"].TraceStatus, byID["TRAC-03"].TraceStatus)
		}

		mdText := mustReadString(t, repMD)
		if mdText != res.Stdout {
			t.Fatalf("report.md does not match stdout")
		}
		assertStringsContainAll(t, mdText, "# Pub/Sub Conformance Report", "| Scenario | Category | Status |", "PUB-01", "PORD-02", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03", "PASS")
		assertAllPIDsDead(t, pidLog, 2, true)
	})

	t.Run("Runner_6SmokeScenarios", func(t *testing.T) {
		pidLog := filepath.Join(t.TempDir(), "pids.log")
		trackPIDLog(t, pidLog)
		target := helperTargetSpec(t, "go-runner", pidLog, "--profile=go", "--spawn-descendant")
		rep := mustRunTarget(t, context.Background(), harness.RunConfig{
			Targets:         []harness.TargetSpec{target},
			Scenarios:       scenarios.All(),
			Filter:          smokeFilter,
			ScenarioTimeout: 10 * time.Second,
		}, target)
		byID := statusByID(rep.Scenarios)
		if rep.Summary.Total != 6 || rep.Summary.Pass != 6 || byID["TRAC-01"].TraceStatus != traces.StatusPass || byID["TRAC-03"].TraceStatus != traces.StatusPass {
			t.Fatalf("unexpected Runner baseline: summary=%+v scenarios=%+v", rep.Summary, rep.Scenarios)
		}
		assertAllPIDsDead(t, pidLog, 2, true)
	})
}

func TestUseEmulatorEnv_Isolation_Runner(t *testing.T) {
	dir := t.TempDir()
	pidLog, envLog := filepath.Join(dir, "pids.log"), filepath.Join(dir, "env.log")
	trackPIDLog(t, pidLog)

	pubAndCheck := func(ctx context.Context, env *harness.ScenarioEnv, withMon bool) error {
		topic := env.TopicPath(env.UniqueName("emu-topic"))
		cResp, err := env.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: env.Endpoint(), Topic: topic, ProjectId: env.ProjectID,
			UseEmulatorEnv: env.Scenario.UseEmulatorEnv, EnableMonitoring: withMon, CountThreshold: proto.Int64(1),
		})
		if err != nil {
			return err
		}
		pubID := cResp.GetPublisherId()
		defer func() { _, _ = env.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubID}) }()
		tp := ""
		if withMon {
			tp = "00-11111111111111111111111111111111-2222222222222222-01"
		}
		pr, err := env.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("emu-isolation-payload"), ParentTraceparent: tp})
		if err != nil {
			return err
		}
		if pr.GetErrorCode() != 0 {
			return env.Failf("publish error %d: %s", pr.GetErrorCode(), pr.GetErrorMessage())
		}
		snap := env.Fake.Snapshot()
		if err := fake.VerifySequence(snap, fake.DefaultVerifyOptions(), fake.ExpectPublish("emu-pub", fake.PublishTopic(topic))); err != nil {
			return err
		}
		if withMon {
			wireTP, _ := traces.ExtractTraceparent(snap.PublishCalls()[0].PublishReq.GetMessages()[0].GetAttributes())
			return env.MustVerifyTraces(ctx, &traces.TraceExpectation{
				Topic: topic, ProjectID: env.ProjectID,
				Publish: &traces.PublishExpectation{MessageCount: 1, BatchCount: 1, ParentTraceparent: tp, MessageIDs: []string{pr.GetMessageId()}, WireTraceparents: []string{wireTP}, BodySizes: []int64{21}},
			})
		}
		return env.MustVerifyTraces(ctx, traces.ExpectNoTraces())
	}

	var emuEndpoint string
	customScenarios := []harness.Scenario{
		{ID: "EMU-PRE", Name: "Pre-Emulator Clean Env", Category: "config", Run: func(ctx context.Context, env *harness.ScenarioEnv) error { return pubAndCheck(ctx, env, false) }},
		{ID: "EMU-ON", Name: "Isolated Emulator Env", Category: "config", UseEmulatorEnv: true, Run: func(ctx context.Context, env *harness.ScenarioEnv) error {
			emuEndpoint = env.Endpoint()
			return pubAndCheck(ctx, env, false)
		}},
		{ID: "EMU-POST", Name: "Post-Emulator Monitoring Clean Env", Category: "config", Run: func(ctx context.Context, env *harness.ScenarioEnv) error { return pubAndCheck(ctx, env, true) }},
	}

	target := helperTargetSpec(t, "emu-target", pidLog, "--profile=go", "--enforce-emulator-env", "--env-log="+envLog)
	target.Env = append(target.Env, "PUBSUB_EMULATOR_HOST=127.0.0.1:19999")
	rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: customScenarios, ScenarioTimeout: 5 * time.Second}, target)
	byID := statusByID(rep.Scenarios)
	if rep.Summary.Pass != 3 || byID["EMU-POST"].TraceStatus != traces.StatusPass {
		t.Fatalf("expected all 3 scenarios PASS with EMU-POST TraceStatus=PASS, got %+v scenarios=%+v", rep.Summary, rep.Scenarios)
	}
	envText := mustReadString(t, envLog)
	lines := strings.Split(strings.TrimSpace(envText), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 proxy launches (initial/EMU-PRE, EMU-ON, EMU-POST), got %d:\n%s", len(lines), envText)
	}
	if !strings.Contains(lines[0], `emu=""`) || !strings.Contains(lines[1], `emu="`+emuEndpoint+`"`) || !strings.Contains(lines[2], `emu=""`) {
		t.Fatalf("PUBSUB_EMULATOR_HOST isolation mismatch (fakeAddr=%q):\n%s", emuEndpoint, envText)
	}
	if lines[0] == lines[1] || lines[1] == lines[2] || lines[0] == lines[2] {
		t.Fatalf("expected distinct proxy PIDs across emulator isolation transitions:\n%s", envText)
	}
	assertAllPIDsDead(t, pidLog, 3, false)

	t.Run("CLI_StripsInheritedEmulatorEnv", func(t *testing.T) {
		cDir := t.TempDir()
		cPIDLog, cEnvLog := filepath.Join(cDir, "pids.log"), filepath.Join(cDir, "env.log")
		trackPIDLog(t, cPIDLog)
		args := append(proxyCmdArgs("--profile=go", "--enforce-emulator-env", "--pid-log="+cPIDLog, "--env-log="+cEnvLog), "--filter=^PUB-01$")
		res := runCLI(t, 20*time.Second, []string{"PUBSUB_EMULATOR_HOST=127.0.0.1:19999"}, args...)
		if res.ExitCode != 0 {
			t.Fatalf("CLI failed with inherited PUBSUB_EMULATOR_HOST: exit=%d stderr=%s", res.ExitCode, res.Stderr)
		}
		logText := mustReadString(t, cEnvLog)
		if !strings.Contains(logText, `emu=""`) || strings.Contains(logText, "19999") {
			t.Fatalf("expected stripped PUBSUB_EMULATOR_HOST in child proxy env, log=%s", logText)
		}
		assertAllPIDsDead(t, cPIDLog, 1, false)
	})
}

func TestMonitoringDisabled_SkipAndFunctionalChecks(t *testing.T) {
	t.Run("CLI_SkipTraces_ZeroTraceRPCs_AndPassTRAC03", func(t *testing.T) {
		dir := t.TempDir()
		pidLog, rpcLog, repJSON := filepath.Join(dir, "pids.log"), filepath.Join(dir, "rpc.log"), filepath.Join(dir, "nomon.json")
		trackPIDLog(t, pidLog)

		args := append(proxyCmdArgs("--profile=go", "--monitoring=false", "--pid-log="+pidLog, "--rpc-count-log="+rpcLog),
			"--filter="+smokeFilter, "--report-json="+repJSON, "--scenario-timeout=10s")
		res := runCLI(t, 25*time.Second, nil, args...)
		if res.ExitCode != 0 {
			t.Fatalf("monitoring=false CLI failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
		}
		rep, _ := mustLoadJSONReport(t, repJSON)
		assertMonitoringDisabledPass(t, rep.Scenarios, rpcLog)
		if statusByID(rep.Scenarios)["PULL-04"].Status != harness.StatusPass {
			t.Fatalf("PULL-04 expected PASS, got %+v", statusByID(rep.Scenarios)["PULL-04"])
		}
		assertAllPIDsDead(t, pidLog, 1, false)
	})

	t.Run("CLI_MonitoringDisabled_StillFailsBadAndMissingFunctionalRPCs", func(t *testing.T) {
		for _, fault := range []string{"wrong-payload", "missing-rpc"} {
			t.Run(fault, func(t *testing.T) {
				dir := t.TempDir()
				pidLog, repJSON := filepath.Join(dir, "pids.log"), filepath.Join(dir, "fail.json")
				trackPIDLog(t, pidLog)

				args := append(proxyCmdArgs("--profile=go", "--monitoring=false", "--fault="+fault, "--pid-log="+pidLog),
					"--filter=^TRAC-03$", "--scenario-timeout=600ms", "--report-json="+repJSON)
				res := runCLI(t, 20*time.Second, nil, args...)
				if res.ExitCode == 0 {
					t.Fatalf("expected non-zero exit for monitoring=false + %s", fault)
				}
				rep, rawMap := mustLoadJSONReport(t, repJSON)
				byID := statusByID(rep.Scenarios)
				if byID["TRAC-03"].Status != harness.StatusFail {
					t.Fatalf("expected TRAC-03 strict FAIL under %s, got %+v", fault, byID["TRAC-03"])
				}
				if rawTRAC03 := rawScenarioByID(t, rawMap, "TRAC-03"); rawTRAC03["status"] != "FAIL" {
					t.Fatalf("expected raw JSON status FAIL, got %+v", rawTRAC03)
				}
				assertAllPIDsDead(t, pidLog, 1, false)
			})
		}
	})
}

func TestUnsupportedCapability_StrictAndDefault(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"CapabilityGate_SupportsUnaryPullFalse", []string{"--profile=partial"}},
		{"RuntimeUnimplemented_UnaryPull", []string{"--profile=go", "--fault=runtime-unimpl-unary-pull"}},
		{"InBandUnimplemented_UnaryPull", []string{"--profile=go", "--fault=inband-unimpl-unary-pull"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, strict := range []bool{false, true} {
				dir := t.TempDir()
				pidLog := filepath.Join(dir, "pids.log")
				repJSON := filepath.Join(dir, "rep.json")
				trackPIDLog(t, pidLog)

				proxyArgs := append([]string{"--pid-log=" + pidLog}, tc.args...)
				cliArgs := append(proxyCmdArgs(proxyArgs...), "--filter=^PULL-04$", "--report-json="+repJSON, "--scenario-timeout=5s")
				if strict {
					cliArgs = append(cliArgs, "--strict-unsupported")
				}
				res := runCLI(t, 20*time.Second, nil, cliArgs...)
				wantExit := 0
				if strict {
					wantExit = 1
				}
				if res.ExitCode != wantExit {
					t.Fatalf("strict=%t exit=%d want=%d stderr=%s", strict, res.ExitCode, wantExit, res.Stderr)
				}
				rep, _ := mustLoadJSONReport(t, repJSON)
				if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusUnsupportedByClient {
					t.Fatalf("expected UNSUPPORTED_BY_CLIENT, got %+v", rep.Scenarios)
				}
				assertAllPIDsDead(t, pidLog, 1, false)
			}
		})
	}
}

func TestPass_CppProfile_WithDeviations(t *testing.T) {
	dir := t.TempDir()
	pidLog := filepath.Join(dir, "pids.log")
	repJSON := filepath.Join(dir, "cpp.json")
	trackPIDLog(t, pidLog)

	args := append(proxyCmdArgs("--profile=cpp", "--pid-log="+pidLog), "--filter="+smokeFilter, "--report-json="+repJSON, "--scenario-timeout=10s")
	res := runCLI(t, 25*time.Second, nil, args...)
	if res.ExitCode != 0 {
		t.Fatalf("cpp CLI run failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
	}
	rep, _ := mustLoadJSONReport(t, repJSON)
	byID := statusByID(rep.Scenarios)
	if rep.Summary.Pass != 6 || byID["TRAC-01"].TraceStatus != traces.StatusPass || len(byID["TRAC-01"].Deviations) == 0 || len(byID["TRAC-03"].Deviations) == 0 {
		t.Fatalf("cpp profile unexpected: summary=%+v TRAC-01=%+v TRAC-03=%+v", rep.Summary, byID["TRAC-01"], byID["TRAC-03"])
	}
	assertAllPIDsDead(t, pidLog, 1, false)
}

func TestRunner_Parity_RunAllCompareAndMonitoringDisabled(t *testing.T) {
	dir := t.TempDir()
	pidLog, rpcLog := filepath.Join(dir, "pids.log"), filepath.Join(dir, "rpc.log")
	trackPIDLog(t, pidLog)
	goT := helperTargetSpec(t, "go", pidLog, "--profile=go", "--spawn-descendant", "--tag=go")
	rustT := helperTargetSpec(t, "rust", pidLog, "--profile=rust", "--rpc-count-log="+rpcLog, "--spawn-descendant", "--tag=rust")
	rep, err := mustNewRunner(t, harness.RunConfig{Targets: []harness.TargetSpec{goT, rustT}, Scenarios: scenarios.All(), Filter: smokeFilter, ScenarioTimeout: 10 * time.Second}).RunAll(context.Background())
	if err != nil || rep.Mode != "compare" || len(rep.Targets) != 2 || harness.ExitCode(rep, false) != 0 || harness.ExitCode(rep, true) != 1 {
		t.Fatalf("RunAll(go,rust) unexpected: err=%v rep=%+v", err, rep)
	}
	assertMonitoringDisabledPass(t, rep.Targets[1].Scenarios, rpcLog)
	if statusByID(rep.Targets[1].Scenarios)["PULL-04"].Status != harness.StatusUnsupportedByClient {
		t.Fatalf("rust PULL-04 expected UNSUPPORTED_BY_CLIENT, got %+v", statusByID(rep.Targets[1].Scenarios)["PULL-04"])
	}
	cmpRep, err := harness.CompareTargets(rep.Targets)
	if err != nil {
		t.Fatalf("CompareTargets: %v", err)
	}
	assertDifferencesContain(t, cmpRep.Differences, "capability", "client_default", "scenario_verdict", "scenario_trace_delta")
	md, err := harness.FormatMarkdownReport(cmpRep)
	if err != nil {
		t.Fatalf("FormatMarkdownReport(compare): %v", err)
	}
	assertStringsContainAll(t, md, "# Pub/Sub Cross-Client Conformance Comparison Report", "SKIPPED_NO_MONITORING", "UNSUPPORTED_BY_CLIENT")
	assertAllPIDsDead(t, pidLog, 4, true)

	pidLog2, rpcLog2 := filepath.Join(dir, "pids2.log"), filepath.Join(dir, "rpc2.log")
	trackPIDLog(t, pidLog2)
	failT := helperTargetSpec(t, "nomon-fail", pidLog2, "--profile=go", "--monitoring=false", "--fault=wrong-payload", "--rpc-count-log="+rpcLog2, "--spawn-descendant")
	failRep, err := mustNewRunner(t, harness.RunConfig{Targets: []harness.TargetSpec{failT}, Scenarios: scenarios.All(), Filter: "^(TRAC-01|TRAC-03)$", ScenarioTimeout: 5 * time.Second}).RunAll(context.Background())
	if err != nil || failRep.Mode != "single" || harness.ExitCode(failRep, false) != 1 {
		t.Fatalf("RunAll(nomon-fail) unexpected: err=%v rep=%+v", err, failRep)
	}
	byID := statusByID(failRep.Scenarios)
	if byID["TRAC-01"].Status != harness.StatusSkippedNoMonitoring || byID["TRAC-03"].Status != harness.StatusFail || !strings.Contains(byID["TRAC-03"].Error, "mismatch") {
		t.Fatalf("nomon-fail unexpected scenarios: %+v", failRep.Scenarios)
	}
	if rpcText := mustReadString(t, rpcLog2); strings.Contains(rpcText, "trace_rpc:") || !strings.Contains(rpcText, "trace_rpcs=0") {
		t.Fatalf("expected 0 trace RPCs on nomon-fail, got:\n%s", rpcText)
	}
	mdSingle, err := harness.FormatMarkdownReport(failRep)
	if err != nil {
		t.Fatalf("FormatMarkdownReport(single): %v", err)
	}
	assertStringsContainAll(t, mdSingle, "# Pub/Sub Conformance Report", "SKIPPED_NO_MONITORING", "TRAC-03", "FAIL")
	assertAllPIDsDead(t, pidLog2, 2, true)
}
