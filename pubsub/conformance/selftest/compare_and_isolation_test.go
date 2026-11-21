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
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
)

func TestCompare_LiveMultiTargetAndOfflineCompare_CLI(t *testing.T) {
	dir := t.TempDir()
	pidLog := filepath.Join(dir, "pids.log")
	trackPIDLog(t, pidLog)

	liveJSON := filepath.Join(dir, "live-compare.json")
	liveMD := filepath.Join(dir, "live-compare.md")
	res := runCLI(t, 30*time.Second, nil,
		proxyTargetFlag("go", "--profile=go", "--pid-log="+pidLog, "--tag=go"),
		proxyTargetFlag("rust", "--profile=rust", "--pid-log="+pidLog, "--tag=rust"),
		proxyTargetFlag("cpp", "--profile=cpp", "--pid-log="+pidLog, "--tag=cpp"),
		"--filter="+smokeFilter, "--report-json="+liveJSON, "--report-md="+liveMD, "--scenario-timeout=10s",
	)
	if res.ExitCode != 0 {
		t.Fatalf("multi-target --proxy-target failed: exit=%d stderr=%s", res.ExitCode, res.Stderr)
	}
	rep, rawMap := mustLoadJSONReport(t, liveJSON)
	if rawMap["schema_version"] != harness.ReportSchemaVersion || rep.Mode != "compare" || len(rep.Targets) != 3 {
		t.Fatalf("unexpected live compare report: schema=%v mode=%q targets=%d", rawMap["schema_version"], rep.Mode, len(rep.Targets))
	}
	assertDifferencesContain(t, rep.Differences, "capability", "client_default", "scenario_verdict", "scenario_trace_delta")

	mdText := mustReadString(t, liveMD)
	if mdText != res.Stdout {
		t.Fatalf("live compare markdown file differs from stdout")
	}
	assertStringsContainAll(t, mdText,
		"# Pub/Sub Cross-Client Conformance Comparison Report", "## Summary", "## Capabilities & Defaults",
		"## Scenario Matrix", "## Behavioral Differences", "SKIPPED_NO_MONITORING", "UNSUPPORTED_BY_CLIENT",
		"scenario_verdict", "scenario_trace_delta",
	)
	assertAllPIDsDead(t, pidLog, 3, false)

	for _, tc := range []struct {
		name, badName, badArg string
		wantDiffs, wantMD     []string
	}{
		{"fail", "bad", "--fault=wrong-payload", []string{"scenario_verdict"}, []string{"scenario_verdict", "bad/PUB-01"}},
		{"crash", "crashed", "--fault=crash-startup", []string{"target_error", "scenario_verdict"}, []string{"target_error", "Target crashed Error"}},
	} {
		pLog, jPath, mPath := filepath.Join(dir, tc.name+"-pids.log"), filepath.Join(dir, tc.name+".json"), filepath.Join(dir, tc.name+".md")
		trackPIDLog(t, pLog)
		r := runCLI(t, 20*time.Second, nil, proxyTargetFlag(tc.badName, tc.badArg, "--pid-log="+pLog), proxyTargetFlag("go", "--profile=go", "--pid-log="+pLog), "--filter=^PUB-01$", "--report-json="+jPath, "--report-md="+mPath)
		if r.ExitCode != 1 {
			t.Fatalf("live compare %s expected exit 1, got %d stderr=%s", tc.name, r.ExitCode, r.Stderr)
		}
		repFail, _ := mustLoadJSONReport(t, jPath)
		if repFail.Mode != "compare" || len(repFail.Targets) != 2 || repFail.Targets[1].Target != "go" || repFail.Targets[1].Summary == nil || repFail.Targets[1].Summary.Pass != 1 {
			t.Fatalf("live compare %s unexpected targets: %+v", tc.name, repFail.Targets)
		}
		assertDifferencesContain(t, repFail.Differences, tc.wantDiffs...)
		if mustReadString(t, mPath) != r.Stdout {
			t.Fatalf("live compare %s MD mismatch", tc.name)
		}
		assertStringsContainAll(t, r.Stdout, tc.wantMD...)
		assertAllPIDsDead(t, pLog, 2, false)
	}

	goJSON, rustJSON, crashJSON, failJSON, toJSON := filepath.Join(dir, "go.json"), filepath.Join(dir, "rust.json"), filepath.Join(dir, "crashed.json"), filepath.Join(dir, "fail.json"), filepath.Join(dir, "timeout.json")
	for _, sc := range []struct {
		path     string
		wantExit int
		args     []string
	}{
		{goJSON, 0, append(proxyCmdArgs("--profile=go", "--pid-log="+pidLog), "--filter="+smokeFilter)},
		{rustJSON, 0, append(proxyCmdArgs("--profile=rust", "--pid-log="+pidLog), "--filter="+smokeFilter)},
		{crashJSON, 1, append(proxyCmdArgs("--fault=crash-startup", "--pid-log="+pidLog), "--filter=^PUB-01$")},
		{failJSON, 1, append(proxyCmdArgs("--fault=wrong-payload", "--pid-log="+pidLog), "--filter=^PUB-01$")},
		{toJSON, 1, append(proxyCmdArgs("--fault=hang-publish", "--pid-log="+pidLog), "--filter=^PUB-01$", "--scenario-timeout=350ms")},
	} {
		if r := runCLI(t, 20*time.Second, nil, append(sc.args, "--report-json="+sc.path)...); r.ExitCode != sc.wantExit {
			t.Fatalf("single %s expected exit %d, got %d stderr=%s", filepath.Base(sc.path), sc.wantExit, r.ExitCode, r.Stderr)
		}
	}
	assertAllPIDsDead(t, pidLog, 8, false)

	cmpJSON, cmpMD := filepath.Join(dir, "offline-cmp.json"), filepath.Join(dir, "offline-cmp.md")
	cmpRes := runCLI(t, 15*time.Second, nil, "--compare="+goJSON+","+rustJSON, "--report-json="+cmpJSON, "--report-md="+cmpMD)
	if cmpRes.ExitCode != 0 {
		t.Fatalf("offline --compare(go,rust) failed: exit=%d stderr=%s", cmpRes.ExitCode, cmpRes.Stderr)
	}
	cmpRep, rawCmp := mustLoadJSONReport(t, cmpJSON)
	if rawCmp["schema_version"] != harness.ReportSchemaVersion || cmpRep.Mode != "compare" || len(cmpRep.Targets) != 2 {
		t.Fatalf("unexpected offline compare metadata: schema=%v mode=%q targets=%d", rawCmp["schema_version"], cmpRep.Mode, len(cmpRep.Targets))
	}
	assertDifferencesContain(t, cmpRep.Differences, "capability", "client_default", "scenario_verdict", "scenario_trace_delta")
	assertStringsContainAll(t, mustReadString(t, cmpMD), "## Behavioral Differences", "scenario_trace_delta", "go", "rust")

	cmpErrJSON := filepath.Join(dir, "offline-cmp-err.json")
	if errRes := runCLI(t, 15*time.Second, nil, "--compare="+goJSON+","+crashJSON, "--report-json="+cmpErrJSON); errRes.ExitCode != 0 {
		t.Fatalf("offline --compare(go,crashed) failed: exit=%d stderr=%s", errRes.ExitCode, errRes.Stderr)
	}
	cmpErrRep, _ := mustLoadJSONReport(t, cmpErrJSON)
	assertDifferencesContain(t, cmpErrRep.Differences, "target_error", "scenario_verdict")

	diagJSON := filepath.Join(dir, "offline-diag.json")
	if r := runCLI(t, 15*time.Second, nil, "--compare="+failJSON, "--compare="+toJSON, "--report-json="+diagJSON); r.ExitCode != 0 {
		t.Fatalf("repeatable --compare(fail,timeout) failed: exit=%d stderr=%s", r.ExitCode, r.Stderr)
	} else {
		diagRep, _ := mustLoadJSONReport(t, diagJSON)
		assertDifferencesContain(t, diagRep.Differences, "scenario_diagnostic")
		assertStringsContainAll(t, r.Stdout, "scenario_diagnostic", "failure mode differs")
	}

	zeroJSON, zeroMD := filepath.Join(dir, "offline-zero.json"), filepath.Join(dir, "offline-zero.md")
	if r := runCLI(t, 15*time.Second, nil, "--compare="+goJSON, "--compare="+goJSON, "--report-json="+zeroJSON, "--report-md="+zeroMD); r.ExitCode != 0 {
		t.Fatalf("repeatable --compare(go,go) failed: exit=%d stderr=%s", r.ExitCode, r.Stderr)
	} else {
		zeroRep, _ := mustLoadJSONReport(t, zeroJSON)
		if len(zeroRep.Differences) != 0 {
			t.Fatalf("identical compare expected 0 differences, got %+v", zeroRep.Differences)
		}
		if md := mustReadString(t, zeroMD); md != r.Stdout || !strings.Contains(md, "No behavioral differences detected.") {
			t.Fatalf("zero-diff MD mismatch: out=%s", r.Stdout)
		}
	}
}

func TestCLI_Hygiene_ListFilterAndPortZeroInjection(t *testing.T) {
	listAll := runCLI(t, 10*time.Second, nil, "--list")
	if listAll.ExitCode != 0 {
		t.Fatalf("--list failed: exit=%d stderr=%s", listAll.ExitCode, listAll.Stderr)
	}
	assertStringsContainAll(t, listAll.Stdout, "PUB-01", "PORD-02", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03")

	listFiltered := runCLI(t, 10*time.Second, nil, "--list", "--filter=^traces$")
	if listFiltered.ExitCode != 0 || !strings.Contains(listFiltered.Stdout, "TRAC-01") || !strings.Contains(listFiltered.Stdout, "TRAC-03") || strings.Contains(listFiltered.Stdout, "PUB-01") {
		t.Fatalf("--list --filter unexpected: exit=%d out=%s", listFiltered.ExitCode, listFiltered.Stdout)
	}

	dir := t.TempDir()
	pidLog := filepath.Join(dir, "pids.log")
	argsAuto := filepath.Join(dir, "args-auto.json")
	argsExplicit := filepath.Join(dir, "args-explicit.json")
	repJSON := filepath.Join(dir, "pub01.json")
	trackPIDLog(t, pidLog)

	res := runCLI(t, 20*time.Second, nil, append(proxyCmdArgs("--pid-log="+pidLog, "--args-log="+argsAuto), "--filter=^PUB-01$", "--report-json="+repJSON)...)
	if res.ExitCode != 0 {
		t.Fatalf("filtered PUB-01 failed: %d stderr=%s", res.ExitCode, res.Stderr)
	}
	rep, _ := mustLoadJSONReport(t, repJSON)
	if rep.Summary.Total != 1 || rep.Summary.Pass != 1 {
		t.Fatalf("expected 1 scenario executed/passed, got %+v", rep.Summary)
	}
	readArgs := func(path string) []string {
		var out []string
		if err := json.Unmarshal([]byte(mustReadString(t, path)), &out); err != nil {
			t.Fatalf("unmarshal args %s: %v", path, err)
		}
		return out
	}
	if gotAuto := readArgs(argsAuto); len(gotAuto) == 0 || gotAuto[len(gotAuto)-1] != "--port=0" {
		t.Fatalf("expected automatic --port=0 injection, got %v", gotAuto)
	}

	resExp := runCLI(t, 20*time.Second, nil, append(proxyCmdArgs("--pid-log="+pidLog, "--args-log="+argsExplicit, "--port=0"), "--filter=^PUB-01$")...)
	if resExp.ExitCode != 0 {
		t.Fatalf("explicit --port=0 failed: %d stderr=%s", resExp.ExitCode, resExp.Stderr)
	}
	gotExp := readArgs(argsExplicit)
	portCount := 0
	for _, a := range gotExp {
		if a == "--port=0" {
			portCount++
		}
	}
	if portCount != 1 {
		t.Fatalf("expected single --port=0 without duplicate injection, got %v", gotExp)
	}

	if bad := runCLI(t, 10*time.Second, nil, append(proxyCmdArgs(), "--filter=[bad")...); bad.ExitCode != 1 {
		t.Fatalf("invalid filter expected exit 1, got %d", bad.ExitCode)
	}
	if empty := runCLI(t, 10*time.Second, nil, append(proxyCmdArgs("--pid-log="+pidLog), "--filter=^NOMATCH$")...); empty.ExitCode != 1 {
		t.Fatalf("empty live filter expected exit 1, got %d", empty.ExitCode)
	}
	assertAllPIDsDead(t, pidLog, 2, false)
}

func TestSelftest_ASTIsolationAndBudget(t *testing.T) {
	selftestDir := filepath.Join(repoRootDir, "pubsub", "conformance", "selftest")
	entries, err := os.ReadDir(selftestDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", selftestDir, err)
	}
	var goFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			goFiles = append(goFiles, e.Name())
		}
	}
	if len(goFiles) == 0 || len(goFiles) > 10 {
		t.Fatalf("selftest .go file count %d out of [1, 10]: %v", len(goFiles), goFiles)
	}

	forbiddenImports := []string{
		"cloud.google.com/go/pubsub/conformanceproxy/proxypb",
		"go.opentelemetry.io/otel/sdk",
		"github.com/googleapis/gapic-showcase/pubsub/conformance/e2e",
	}
	fset := token.NewFileSet()
	totalLines := 0
	for _, name := range goFiles {
		fullPath := filepath.Join(selftestDir, name)
		src, err := os.ReadFile(fullPath)
		if err != nil {
			t.Fatalf("read %s: %v", fullPath, err)
		}
		lines := strings.Count(string(src), "\n")
		if len(src) > 0 && src[len(src)-1] != '\n' {
			lines++
		}
		totalLines += lines

		f, err := parser.ParseFile(fset, fullPath, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", fullPath, err)
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if slices.Contains(forbiddenImports, p) || strings.Contains(p, "conformanceproxy") || strings.Contains(p, "otel/sdk") || strings.HasSuffix(p, "/e2e") {
				t.Fatalf("file %s contains forbidden import %q", fullPath, p)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Skip", "SkipNow", "Skipf", "Setenv":
				t.Fatalf("file %s calls forbidden method %s at %s", fullPath, sel.Sel.Name, fset.Position(sel.Pos()))
			}
			return true
		})
	}
	if totalLines > 3000 {
		t.Fatalf("selftest total lines %d exceeds 3000 ceiling (files=%v)", totalLines, filepath.Join(selftestDir, "*.go"))
	}
}

func TestCompare_CollidingStemsAndScenarioEnvHelpers(t *testing.T) {
	dir := t.TempDir()
	pidLog := filepath.Join(dir, "pids.log")
	trackPIDLog(t, pidLog)
	r1, r2, outCmp := filepath.Join(dir, "d1", "report.json"), filepath.Join(dir, "d2", "report.json"), filepath.Join(dir, "colliding.json")
	for _, tc := range [2]struct {
		p, fault string
		want     int
	}{{r1, "", 0}, {r2, "wrong-payload", 1}} {
		_ = os.MkdirAll(filepath.Dir(tc.p), 0o755)
		if res := runCLI(t, 15*time.Second, nil, append(proxyCmdArgs("--pid-log="+pidLog, "--fault="+tc.fault), "--filter=^PUB-01$", "--report-json="+tc.p)...); res.ExitCode != tc.want {
			t.Fatalf("%s failed: %d", tc.p, res.ExitCode)
		}
	}
	if res := runCLI(t, 15*time.Second, nil, "--compare="+r1+","+r2, "--report-json="+outCmp); res.ExitCode != 0 {
		t.Fatalf("colliding --compare failed: %d", res.ExitCode)
	}
	if rep, _ := mustLoadJSONReport(t, outCmp); len(rep.Targets) != 2 || rep.Targets[0].Target != "report" || rep.Targets[1].Target != "report-2" {
		t.Fatalf("expected [report, report-2], got %+v", rep.Targets)
	}
	target := helperTargetSpec(t, "env", pidLog, "--profile=go", "--fault=emit-span-when-disabled")
	repEnv := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: []harness.Scenario{{ID: "ENV-DRAIN", Category: "traces", Run: func(ctx context.Context, env *harness.ScenarioEnv) error {
		_ = env.RequireCapability(harness.CapUnaryPull)
		pub, _ := env.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: env.Endpoint(), Topic: env.TopicPath("t")})
		_, _ = env.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pub.GetPublisherId(), Data: []byte("x")})
		if spans, drained, _ := env.DrainTraces(ctx); !drained {
			return env.Failf("not drained")
		} else {
			return env.VerifySpans(spans, traces.ExpectNoPubSubSpans()).Err()
		}
	}}}, ScenarioTimeout: 5 * time.Second}, target)
	if len(repEnv.Scenarios) != 1 || repEnv.Scenarios[0].Status != harness.StatusFail || !strings.Contains(repEnv.Scenarios[0].Error, "UNEXPECTED_SPAN") {
		t.Fatalf("DrainTraces+VerifySpans expected UNEXPECTED_SPAN, got %+v", repEnv.Scenarios)
	}
	assertAllPIDsDead(t, pidLog, 3, false)
}
