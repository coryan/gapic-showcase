// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
)

func TestReportAndCompare_JSONMarkdownAndDisambiguation(t *testing.T) {
	capsGo := CapabilitiesFromProto(&proxypb.GetCapabilitiesResponse{ClientLanguage: "go", ClientLibrary: "cloud.google.com/go/pubsub/v2", ClientVersion: "v2.0.0", SupportsMonitoring: true, Defaults: &proxypb.ClientDefaults{PublishBatchCountThreshold: 100, PublishFlowControlBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK}})
	capsRust := CapabilitiesFromProto(&proxypb.GetCapabilitiesResponse{ClientLanguage: "rust", ClientLibrary: "google-cloud-pubsub", ClientVersion: "0.1.0", Defaults: &proxypb.ClientDefaults{PublishBatchCountThreshold: 1000, SubscriberMaxExtensionMs: 600000, PublishFlowControlBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE}})
	single, err := BuildReport([]TargetReport{{Target: "go", Capabilities: capsGo, Scenarios: []ScenarioResult{
		{ID: "S1", Category: "pub", Status: StatusPass, TraceStatus: traces.StatusPass, DurationMs: 5},
		{ID: "S2", Category: "pub", Status: StatusFail, Error: "a|b\nc"}, {ID: "S3", Category: "trace", Status: StatusSkippedNoMonitoring},
		{ID: "S4", Category: "pull", Status: StatusUnsupportedByClient}, {ID: "S5", Category: "hang", Status: StatusTimeout, Error: "deadline"},
		{ID: "S6", Category: "crash", Status: StatusError, Error: "boom"},
	}}})
	must(t, err)
	dir := t.TempDir()
	jsonPath, mdPath := filepath.Join(dir, "single.json"), filepath.Join(dir, "single.md")
	must(t, WriteJSONReport(jsonPath, single))
	must(t, WriteMarkdownReport(mdPath, single))
	_, bErr := BuildReport(nil)
	assert(t, bErr != nil && WriteJSONReport(filepath.Join(dir, "missing", "r.json"), single) != nil && WriteMarkdownReport(dir, single) != nil && WriteJSONReport(jsonPath, nil) != nil && WriteMarkdownReport(mdPath, nil) != nil &&
		ExitCode(nil, false) == 1 && ExitCode(&Report{InfraError: "boom"}, false) == 1 && ExitCode(&Report{Targets: []TargetReport{{TargetError: "err"}}}, false) == 1 &&
		ExitCode(&Report{Targets: []TargetReport{{Summary: &Summary{UnsupportedByClient: 1}}}}, false) == 0 && ExitCode(&Report{Targets: []TargetReport{{Summary: &Summary{UnsupportedByClient: 1}}}}, true) == 1, "report validation/ExitCode check failed")
	rawJSON, _ := os.ReadFile(jsonPath)
	var m map[string]any
	_ = json.Unmarshal(rawJSON, &m)
	capsMap := m["capabilities"].(map[string]any)
	_, hasUnary := capsMap["supports_unary_pull"]
	assert(t, hasUnary && len(capsMap["defaults"].(map[string]any)) == 27 && m["differences"] != nil && m["scenarios"] != nil && m["targets"] != nil, "missing fields/non-nil slices: %v", m)
	assert(t, !slices.ContainsFunc(m["scenarios"].([]any), func(sc any) bool { st := sc.(map[string]any)["status"]; return st == "TIMEOUT" || st == "ERROR" }), "canonical JSON status must never be TIMEOUT/ERROR: %v", m["scenarios"])
	reloaded, err := LoadJSONReport(jsonPath)
	mdBytes, _ := os.ReadFile(mdPath)
	assert(t, err == nil && reloaded.Summary.Total == 6 && reloaded.Summary.Fail == 3 && reloaded.Scenarios[4].Status == StatusTimeout, "reloaded mismatch: %v %+v", err, reloaded)
	for _, tok := range []string{"FAIL (TIMEOUT)", "FAIL (ERROR)", `a\|b c`} {
		assert(t, strings.Contains(string(mdBytes), tok), "Markdown missing %q", tok)
	}
	rustJSON := filepath.Join(dir, "rust-run.json")
	rustRep, _ := BuildReport([]TargetReport{{Target: "go", Capabilities: capsRust, Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass, TraceStatus: traces.StatusSkippedNoMonitoring}, {ID: "S2", Status: StatusTimeout, Error: "slow"}}}})
	_ = WriteJSONReport(rustJSON, rustRep)
	cmp, err := CompareJSONFiles([]string{jsonPath, rustJSON})
	cmpMD, mdErr := FormatMarkdownReport(cmp)
	zeroCmp, zErr := CompareTargets([]TargetReport{{Target: "a", Capabilities: capsGo, Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}}, {Target: "b", Capabilities: capsGo, Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}}})
	zeroMD, _ := FormatMarkdownReport(zeroCmp)
	cmpJSON, _ := json.Marshal(cmp)
	var cmpMap map[string]any
	_ = json.Unmarshal(cmpJSON, &cmpMap)
	assert(t, err == nil && len(cmp.Targets) == 2 && cmp.Targets[0].Target != cmp.Targets[1].Target && mdErr == nil && strings.Contains(cmpMD, "MISSING") && strings.Contains(cmpMD, "## Diagnostics") && zErr == nil && len(zeroCmp.Differences) == 0 && strings.Contains(zeroMD, "No behavioral differences detected."), "CompareJSONFiles/Markdown failed: %v", err)
	for _, forbidden := range []string{"capabilities", "summary", "scenarios"} {
		_, exists := cmpMap[forbidden]
		assert(t, !exists, "compare root JSON must omit %q", forbidden)
	}
	for _, need := range []string{"capability", "client_default", "scenario_trace_delta", "scenario_diagnostic", "scenario_verdict"} {
		assert(t, slices.ContainsFunc(cmp.Differences, func(d Difference) bool { return d.Kind == need }), "missing diff kind %q", need)
	}
	traceOmit, _ := CompareTargets([]TargetReport{
		{Target: "a", Scenarios: []ScenarioResult{{ID: "T1", Status: StatusPass}, {ID: "T2", Status: StatusPass}}},
		{Target: "b", TimedOut: true, Scenarios: []ScenarioResult{{ID: "T1", Status: StatusPass, TraceStatus: traces.StatusSkippedNoMonitoring}, {ID: "T2", Status: StatusPass}}},
	})
	assert(t, len(traceOmit.Differences) == 2 && traceOmit.Differences[0].Kind == "target_error" && traceOmit.Differences[1].Kind == "scenario_trace_delta" && traceOmit.Differences[1].Verdicts["a"] == "PASS", "trace_status omission diff mismatch: %+v", traceOmit.Differences)
	var cmp3Paths []string
	for _, tc := range []struct{ sub, body string }{
		{"d1", `{"capabilities":{"language":"go","defaults":{"publish_flow_control_behavior":2}},"results":[{"scenario_id":"S1","verdict":"passed"}]}`},
		{"d2", `{"target":"go-2","scenarios":[{"id":"S1","status":"PASS"}]}`}, {"d3", `{"scenarios":[{"id":"S1","status":"PASS"}]}`},
	} {
		p := filepath.Join(dir, tc.sub, "go.json")
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		_ = os.WriteFile(p, []byte(tc.body), 0o644)
		cmp3Paths = append(cmp3Paths, p)
	}
	cmp3, err := CompareJSONFiles(cmp3Paths)
	mapRep, mapErr := ParseJSONReportBytes([]byte(`{"targets":{"t1":{"scenarios":[{"id":"S1","status":"PASS"}]},"t2":{"scenarios":[{"id":"S1","status":"FAIL"}]}}}`))
	_, lt2Err := CompareJSONFiles([]string{jsonPath})
	assert(t, err == nil && len(cmp3.Targets) == 3 && cmp3.Targets[0].Capabilities.Defaults.PublishFlowControlBehavior == "FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK" &&
		mapErr == nil && mapRep.Mode == "compare" && len(mapRep.Targets) == 2 && lt2Err != nil, "3-way/map/lt2 failed: %v / %v", err, mapErr)
	for _, bad := range []string{"", "null", "[]", `{}`, `{} {}`, `{"scenarios":null}`, `{"scenarios":[{"id":"x","status":"NOPE"}]}`} {
		_, err := ParseJSONReportBytes([]byte(bad))
		assert(t, err != nil, "ParseJSONReportBytes(%q) succeeded", bad)
	}
}

func TestFormatMarkdownReport_SingleEmptyTargetsEmitsDiagnostics(t *testing.T) {
	rep := &Report{
		Mode:   "single",
		Target: "go",
		Scenarios: []ScenarioResult{
			{ID: "s1", Category: "publish", Status: StatusFail, Error: "boom"},
			{ID: "s2", Category: "publish", Status: StatusPass},
		},
	}
	md, err := FormatMarkdownReport(rep)
	must(t, err)
	assert(t, strings.Contains(md, "## Diagnostics"), "missing ## Diagnostics in single-target report with empty Targets:\n%s", md)
	assert(t, strings.Contains(md, "- **s1** (`FAIL`): boom"), "missing s1 diagnostic bullet in:\n%s", md)
}

func TestCompareJSONFiles_PreservesTargetsAndStemEdges(t *testing.T) {
	dir := t.TempDir()
	writeFile := func(rel, content string) string {
		p := filepath.Join(dir, rel)
		must(t, os.MkdirAll(filepath.Dir(p), 0o755))
		must(t, os.WriteFile(p, []byte(content), 0o644))
		return p
	}

	// Case A: multi-target reports with colliding basenames preserve explicit target names.
	a1 := writeFile("run1/report.json", `{"mode":"compare","targets":[{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]},{"target":"cpp","scenarios":[{"id":"PUB-01","status":"FAIL","error":"cpp fail"}]}]}`)
	a2 := writeFile("run2/report.json", `{"mode":"compare","targets":[{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]},{"target":"cpp","scenarios":[{"id":"PUB-01","status":"PASS"}]}]}`)
	cmpA, err := CompareJSONFiles([]string{a1, a2})
	must(t, err)
	gotA := []string{cmpA.Targets[0].Target, cmpA.Targets[1].Target, cmpA.Targets[2].Target, cmpA.Targets[3].Target}
	assert(t, slices.Equal(gotA, []string{"go", "cpp", "go-2", "cpp-2"}), "Case A targets = %v, want [go cpp go-2 cpp-2]", gotA)

	// Case B: single-target reports with explicit target "go" and colliding basenames preserve "go" rather than clobbering to "report".
	b1 := writeFile("b1/report.json", `{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	b2 := writeFile("b2/report.json", `{"target":"go","scenarios":[{"id":"PUB-01","status":"FAIL","error":"regressed"}]}`)
	cmpB, err := CompareJSONFiles([]string{b1, b2})
	must(t, err)
	gotB := []string{cmpB.Targets[0].Target, cmpB.Targets[1].Target}
	assert(t, slices.Equal(gotB, []string{"go", "go-2"}), "Case B (colliding stem) targets = %v, want [go go-2]", gotB)

	// Case B2: single-target reports with explicit target "go" and distinct basenames use orig-base.
	b3 := writeFile("b3/alpha.json", `{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	b4 := writeFile("b4/beta.json", `{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	cmpB2, err := CompareJSONFiles([]string{b3, b4})
	must(t, err)
	gotB2 := []string{cmpB2.Targets[0].Target, cmpB2.Targets[1].Target}
	assert(t, slices.Equal(gotB2, []string{"go-alpha", "go-beta"}), "Case B (distinct stem) targets = %v, want [go-alpha go-beta]", gotB2)

	// Case C: generic "default" stem replacement never displaces an already-unique explicit target name.
	c1 := writeFile("c1/alpha.json", `{"target":"default","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	c2 := writeFile("c2/beta.json", `{"target":"default","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	c3 := writeFile("c3/gamma.json", `{"target":"alpha","scenarios":[{"id":"PUB-01","status":"FAIL","error":"x"}]}`)
	cmpC, err := CompareJSONFiles([]string{c1, c2, c3})
	must(t, err)
	gotC := []string{cmpC.Targets[0].Target, cmpC.Targets[1].Target, cmpC.Targets[2].Target}
	assert(t, slices.Equal(gotC, []string{"alpha-2", "beta", "alpha"}), "Case C targets = %v, want [alpha-2 beta alpha]", gotC)

	// Existing "default" stem deduplication behavior: d1/rep.json + d2/rep.json -> rep, rep-2.
	d1 := writeFile("d1/rep.json", `{"target":"default","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	d2 := writeFile("d2/rep.json", `{"target":"default","scenarios":[{"id":"PUB-01","status":"FAIL","error":"y"}]}`)
	cmpD, err := CompareJSONFiles([]string{d1, d2})
	must(t, err)
	gotD := []string{cmpD.Targets[0].Target, cmpD.Targets[1].Target}
	assert(t, slices.Equal(gotD, []string{"rep", "rep-2"}), "default stem dedup targets = %v, want [rep rep-2]", gotD)

	// Error paths in CompareJSONFiles / LoadJSONReport.
	_, errParse := CompareJSONFiles([]string{"a.json,"})
	assert(t, errParse != nil && strings.Contains(errParse.Error(), "empty file path"), "expected ParseCompareArgs error, got %v", errParse)
	_, errLoad := CompareJSONFiles([]string{d1, filepath.Join(dir, "does-not-exist.json")})
	assert(t, errLoad != nil && strings.Contains(errLoad.Error(), "reading JSON report"), "expected LoadJSONReport error, got %v", errLoad)
}

func TestCompareTargets_ValidationAndMixedCaps(t *testing.T) {
	_, errEmpty := CompareTargets([]TargetReport{
		{Target: "   ", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
		{Target: "go", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
	})
	assert(t, errEmpty != nil && strings.Contains(errEmpty.Error(), "target name cannot be empty"), "expected empty target error, got %v", errEmpty)

	_, errDupTarget := CompareTargets([]TargetReport{
		{Target: "go", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
		{Target: " go ", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
	})
	assert(t, errDupTarget != nil && strings.Contains(errDupTarget.Error(), "duplicate target name"), "expected duplicate target error, got %v", errDupTarget)

	_, errDupScen := CompareTargets([]TargetReport{
		{Target: "go", Scenarios: []ScenarioResult{{ID: "pub.basic", Status: StatusFail, Error: "first"}, {ID: "pub.basic", Status: StatusPass}}},
		{Target: "py", Scenarios: []ScenarioResult{{ID: "pub.basic", Status: StatusPass}}},
	})
	assert(t, errDupScen != nil && strings.Contains(errDupScen.Error(), `duplicate scenario ID "pub.basic" in target "go"`), "expected duplicate scenario ID error, got %v", errDupScen)

	rep, err := CompareTargets([]TargetReport{
		{Target: "go", Capabilities: &CapabilitiesReport{SupportsMonitoring: true}, Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
		{Target: "nilcap", Capabilities: nil, Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
		{Target: "rust", Capabilities: &CapabilitiesReport{SupportsMonitoring: false}, Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
	})
	must(t, err)
	var monDiff *Difference
	for i := range rep.Differences {
		if rep.Differences[i].Kind == "capability" && rep.Differences[i].Field == "supports_monitoring" {
			monDiff = &rep.Differences[i]
			break
		}
	}
	assert(t, monDiff != nil && monDiff.Verdicts["nilcap"] == "MISSING" && monDiff.Verdicts["go"] == "true" && monDiff.Verdicts["rust"] == "false", "mixed nil/non-nil capabilities diff unexpected: %+v", monDiff)
}

func TestParseJSONReportBytes_AliasesAndValidationEdges(t *testing.T) {
	t.Run("targets_array_non_object", func(t *testing.T) {
		_, err := ParseJSONReportBytes([]byte(`{"targets":["bad"]}`))
		assert(t, err != nil && strings.Contains(err.Error(), "targets[0] must be a JSON object"), "got %v", err)
	})
	t.Run("targets_array_blank_target_auto_named", func(t *testing.T) {
		rep, err := ParseJSONReportBytes([]byte(`{"targets":[{"scenarios":[{"id":"S1","status":"PASS"}]},{"target":"cpp","scenarios":[{"id":"S1","status":"PASS"}]}]}`))
		must(t, err)
		assert(t, len(rep.Targets) == 2 && rep.Targets[0].Target == "target-0", "targets=%+v", rep.Targets)
	})
	t.Run("targets_array_duplicate_target", func(t *testing.T) {
		_, err := ParseJSONReportBytes([]byte(`{"targets":[{"target":"go","scenarios":[]},{"target":"go","scenarios":[]}]}`))
		assert(t, err != nil && strings.Contains(err.Error(), `duplicate target "go" in report`), "got %v", err)
	})
	t.Run("targets_map_non_object", func(t *testing.T) {
		_, err := ParseJSONReportBytes([]byte(`{"targets":{"go":"bad"}}`))
		assert(t, err != nil && strings.Contains(err.Error(), `target "go" must be a JSON object`), "got %v", err)
	})
	t.Run("targets_invalid_nested_scenarios", func(t *testing.T) {
		_, err := ParseJSONReportBytes([]byte(`{"targets":[{"target":"go","scenarios":"not-array"}]}`))
		assert(t, err != nil && strings.Contains(err.Error(), "target go (index 0)") && strings.Contains(err.Error(), "scenarios block must be a JSON array"), "got %v", err)
	})
	t.Run("scenarios_array_non_object", func(t *testing.T) {
		_, err := ParseJSONReportBytes([]byte(`{"scenarios":["bad"]}`))
		assert(t, err != nil && strings.Contains(err.Error(), "scenario[0] must be a JSON object"), "got %v", err)
	})
	t.Run("scenario_error_object_promoted", func(t *testing.T) {
		rep, err := ParseJSONReportBytes([]byte(`{"scenarios":[{"id":"S1","status":"FAIL","error":{"msg":"boom"},"duration":123}]}`))
		must(t, err)
		assert(t, len(rep.Scenarios) == 1 && rep.Scenarios[0].Error == "boom" && rep.Scenarios[0].Duration == "123", "scenarios=%+v", rep.Scenarios)
	})
	t.Run("single_target_array_populates_top_level", func(t *testing.T) {
		rep, err := ParseJSONReportBytes([]byte(`{"targets":[{"target":"go","capabilities":{"language":"go"},"scenarios":[{"id":"S1","status":"PASS"}]}]}`))
		must(t, err)
		assert(t, rep.Mode == "single" && rep.Target == "go" && rep.Capabilities != nil && rep.Capabilities.ClientLanguage == "go" && len(rep.Scenarios) == 1 && rep.Scenarios[0].ID == "S1", "rep=%+v", rep)
	})
	t.Run("targets_invalid_type", func(t *testing.T) {
		_, err := ParseJSONReportBytes([]byte(`{"targets":42}`))
		assert(t, err != nil && strings.Contains(err.Error(), "targets must be an array or object"), "got %v", err)
	})
	t.Run("targets_array_whitespace_duplicate_target", func(t *testing.T) {
		_, err := ParseJSONReportBytes([]byte(`{"targets":[{"target":"go","scenarios":[]},{"target":" go ","scenarios":[]}]}`))
		assert(t, err != nil && strings.Contains(err.Error(), `duplicate target "go" in report`), "got %v", err)
	})
	t.Run("targets_map_empty_key_and_whitespace_duplicate", func(t *testing.T) {
		_, errEmpty := ParseJSONReportBytes([]byte(`{"mode":"compare","targets":{"":{"scenarios":[{"id":"S1","status":"PASS"}]},"cpp":{"scenarios":[{"id":"S1","status":"PASS"}]}}}`))
		assert(t, errEmpty != nil && strings.Contains(errEmpty.Error(), "target map key cannot be empty"), "got %v", errEmpty)
		_, errDup := ParseJSONReportBytes([]byte(`{"mode":"compare","targets":{"go":{"scenarios":[{"id":"S1","status":"PASS"}]}," go ":{"scenarios":[{"id":"S1","status":"PASS"}]}}}`))
		assert(t, errDup != nil && strings.Contains(errDup.Error(), `duplicate target "go" in report`), "got %v", errDup)
	})
	t.Run("duplicate_scenario_id_top_and_target", func(t *testing.T) {
		_, errTop := ParseJSONReportBytes([]byte(`{"target":"go","scenarios":[{"id":"S1","status":"PASS"},{"id":"S1","status":"FAIL"}]}`))
		assert(t, errTop != nil && strings.Contains(errTop.Error(), `duplicate scenario ID "S1"`), "got %v", errTop)
		_, errTarg := ParseJSONReportBytes([]byte(`{"mode":"compare","targets":[{"target":"go","scenarios":[{"id":"S1","status":"PASS"},{"id":"S1","status":"FAIL"}]},{"target":"cpp","scenarios":[{"id":"S1","status":"PASS"}]}]}`))
		assert(t, errTarg != nil && strings.Contains(errTarg.Error(), `duplicate scenario ID "S1"`), "got %v", errTarg)
	})
}

func TestFormatMarkdownReport_CompareValidationEdges(t *testing.T) {
	t.Run("duplicate_target_name", func(t *testing.T) {
		_, err := FormatMarkdownReport(&Report{
			Mode: "compare",
			Targets: []TargetReport{
				{Target: "go", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
				{Target: "go", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusFail}}},
			},
		})
		assert(t, err != nil && strings.Contains(err.Error(), `duplicate target name "go"`), "got %v", err)
	})
	t.Run("whitespace_duplicate_target_name", func(t *testing.T) {
		_, err := FormatMarkdownReport(&Report{
			Mode: "compare",
			Targets: []TargetReport{
				{Target: "go", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
				{Target: " go ", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusFail}}},
			},
		})
		assert(t, err != nil && strings.Contains(err.Error(), `duplicate target name "go"`), "got %v", err)
	})
	t.Run("duplicate_scenario_id_in_target", func(t *testing.T) {
		_, err := FormatMarkdownReport(&Report{
			Mode: "compare",
			Targets: []TargetReport{
				{Target: "go", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}, {ID: "S1", Status: StatusFail}}},
				{Target: "cpp", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
			},
		})
		assert(t, err != nil && strings.Contains(err.Error(), `duplicate scenario ID "S1" in target "go"`), "got %v", err)
	})
	t.Run("empty_compare_target", func(t *testing.T) {
		_, err := FormatMarkdownReport(&Report{
			Mode: "compare",
			Targets: []TargetReport{
				{Target: "   ", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
				{Target: "cpp", Scenarios: []ScenarioResult{{ID: "S1", Status: StatusPass}}},
			},
		})
		assert(t, err != nil && strings.Contains(err.Error(), "target name cannot be empty"), "got %v", err)
	})
}

func TestCompareJSONFiles_LanguageFallbackAndSingleLegacyDefault(t *testing.T) {
	dir := t.TempDir()
	writeFile := func(rel, content string) string {
		p := filepath.Join(dir, rel)
		must(t, os.MkdirAll(filepath.Dir(p), 0o755))
		must(t, os.WriteFile(p, []byte(content), 0o644))
		return p
	}

	t.Run("empty_target_uses_client_language", func(t *testing.T) {
		emptyTarget := writeFile("a/run1.json", `{"target":"","capabilities":{"client_language":"go"},"scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
		cpp := writeFile("a/run2.json", `{"target":"cpp","scenarios":[{"id":"PUB-01","status":"FAIL","error":"cpp fail"}]}`)
		cmp, err := CompareJSONFiles([]string{emptyTarget, cpp})
		must(t, err)
		got := []string{cmp.Targets[0].Target, cmp.Targets[1].Target}
		assert(t, slices.Equal(got, []string{"go", "cpp"}), "targets = %v, want [go cpp]", got)

		wsTarget := writeFile("b/run1.json", `{"target":"   ","capabilities":{"client_language":"go"},"scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
		cmpWS, err := CompareJSONFiles([]string{wsTarget, cpp})
		must(t, err)
		gotWS := []string{cmpWS.Targets[0].Target, cmpWS.Targets[1].Target}
		assert(t, slices.Equal(gotWS, []string{"go", "cpp"}), "ws targets = %v, want [go cpp]", gotWS)
	})

	t.Run("legacy_single_default_preserved", func(t *testing.T) {
		def := writeFile("legacy/report.json", `{"target":"default","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
		goRep := writeFile("legacy/go.json", `{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
		cppRep := writeFile("legacy/cpp.json", `{"target":"cpp","scenarios":[{"id":"PUB-01","status":"FAIL","error":"cpp fail"}]}`)
		cmp, err := CompareJSONFiles([]string{def, goRep, cppRep})
		must(t, err)
		got := []string{cmp.Targets[0].Target, cmp.Targets[1].Target, cmp.Targets[2].Target}
		assert(t, slices.Equal(got, []string{"default", "go", "cpp"}), "legacy targets = %v, want [default go cpp]", got)
	})
}

func TestTier5_WhitespaceTargetTrimAndMarkdownHeading(t *testing.T) {
	t.Run("ParseJSONReportBytes_trims_target_and_falls_back_to_client_language", func(t *testing.T) {
		rep, err := ParseJSONReportBytes([]byte(`{"target":"   ","capabilities":{"client_language":" go "},"scenarios":[{"id":"PUB-01","status":"PASS"}]}`))
		must(t, err)
		assert(t, rep.Target == "", "rep.Target = %q, want empty", rep.Target)
		assert(t, len(rep.Targets) == 1 && rep.Targets[0].Target == "go", "rep.Targets[0].Target = %q, want %q", rep.Targets[0].Target, "go")

		repNamed, err := ParseJSONReportBytes([]byte(`{"target":"  rust-client  ","scenarios":[{"id":"PUB-01","status":"PASS"}]}`))
		must(t, err)
		assert(t, repNamed.Target == "rust-client", "repNamed.Target = %q, want %q", repNamed.Target, "rust-client")
		assert(t, len(repNamed.Targets) == 1 && repNamed.Targets[0].Target == "rust-client", "repNamed.Targets[0].Target = %q, want %q", repNamed.Targets[0].Target, "rust-client")

		repTargetsWS, err := ParseJSONReportBytes([]byte(`{"target":"   ","targets":[{"target":"  go  ","scenarios":[{"id":"PUB-01","status":"PASS"}]}]}`))
		must(t, err)
		assert(t, repTargetsWS.Target == "go", "repTargetsWS.Target = %q, want %q", repTargetsWS.Target, "go")
		assert(t, len(repTargetsWS.Targets) == 1 && repTargetsWS.Targets[0].Target == "go", "repTargetsWS.Targets[0].Target = %q, want %q", repTargetsWS.Targets[0].Target, "go")

		repTargetsNamed, err := ParseJSONReportBytes([]byte(`{"target":"  my-go  ","targets":[{"target":"other","scenarios":[{"id":"PUB-01","status":"PASS"}]}]}`))
		must(t, err)
		assert(t, repTargetsNamed.Target == "my-go", "repTargetsNamed.Target = %q, want %q", repTargetsNamed.Target, "my-go")
	})

	t.Run("FormatMarkdownReport_single_whitespace_target_no_dangling_colon", func(t *testing.T) {
		md, err := FormatMarkdownReport(&Report{
			Mode:      "single",
			Target:    "   ",
			Targets:   nil,
			Scenarios: []ScenarioResult{{ID: "PUB-01", Category: "publish", Status: StatusPass, DurationMs: 5}},
		})
		must(t, err)
		firstLine, _, _ := strings.Cut(md, "\n")
		assert(t, firstLine == "# Pub/Sub Conformance Report", "heading = %q, want no dangling colon", firstLine)
		assert(t, !strings.Contains(firstLine, ":"), "unexpected colon in heading %q", firstLine)

		mdTrim, err := FormatMarkdownReport(&Report{
			Mode:         "single",
			Target:       "  cpp  ",
			Capabilities: &CapabilitiesReport{ClientLanguage: "cpp"},
			Scenarios:    []ScenarioResult{{ID: "PUB-01", Category: "publish", Status: StatusPass, DurationMs: 5}},
		})
		must(t, err)
		firstLineTrim, _, _ := strings.Cut(mdTrim, "\n")
		assert(t, firstLineTrim == "# Pub/Sub Conformance Report: cpp", "trimmed heading = %q", firstLineTrim)

		mdFromTarget, err := FormatMarkdownReport(&Report{
			Mode:   "single",
			Target: "   ",
			Targets: []TargetReport{{
				Target:       "  rust  ",
				Capabilities: &CapabilitiesReport{ClientLanguage: "rust"},
				Scenarios:    []ScenarioResult{{ID: "PUB-01", Category: "publish", Status: StatusPass, DurationMs: 5}},
			}},
		})
		must(t, err)
		firstLineTarget, _, _ := strings.Cut(mdFromTarget, "\n")
		assert(t, firstLineTarget == "# Pub/Sub Conformance Report: rust", "from-target heading = %q", firstLineTarget)
		assert(t, strings.Contains(mdFromTarget, "| rust | rust |"), "missing trimmed target table row in:\n%s", mdFromTarget)

		mdFromWSTarget, err := FormatMarkdownReport(&Report{
			Mode:   "single",
			Target: "   ",
			Targets: []TargetReport{{
				Target:    "   ",
				Scenarios: []ScenarioResult{{ID: "PUB-01", Category: "publish", Status: StatusPass, DurationMs: 5}},
			}},
		})
		must(t, err)
		firstLineWSTarget, _, _ := strings.Cut(mdFromWSTarget, "\n")
		assert(t, firstLineWSTarget == "# Pub/Sub Conformance Report", "ws-target heading = %q", firstLineWSTarget)
		assert(t, !strings.Contains(firstLineWSTarget, ":"), "unexpected colon in ws-target heading %q", firstLineWSTarget)
	})
}

func TestTier6_FormatMarkdownReport_InfraErrorDiagnostics(t *testing.T) {
	t.Run("single_target_infra_error_escaped_in_diagnostics", func(t *testing.T) {
		rep := &Report{
			Mode:       "single",
			Target:     "go",
			InfraError: "boom|1\nline2",
			Scenarios:  []ScenarioResult{{ID: "PUB-01", Category: "publish", Status: StatusPass, DurationMs: 5}},
		}
		md, err := FormatMarkdownReport(rep)
		must(t, err)
		assert(t, strings.Contains(md, "## Diagnostics"), "missing ## Diagnostics:\n%s", md)
		assert(t, strings.Contains(md, `- **Infra Error**: boom\|1 line2`), "missing escaped Infra Error line:\n%s", md)
	})

	t.Run("compare_mode_infra_error_escaped_in_diagnostics", func(t *testing.T) {
		cmpRep := &Report{
			Mode:       "compare",
			InfraError: "boom|1",
			Targets: []TargetReport{
				{Target: "go", Scenarios: []ScenarioResult{{ID: "PUB-01", Status: StatusPass}}},
				{Target: "cpp", Scenarios: []ScenarioResult{{ID: "PUB-01", Status: StatusPass}}},
			},
		}
		md, err := FormatMarkdownReport(cmpRep)
		must(t, err)
		assert(t, strings.Contains(md, "## Diagnostics"), "missing ## Diagnostics in compare mode:\n%s", md)
		assert(t, strings.Contains(md, `- **Infra Error**: boom\|1`), "missing escaped Infra Error line in compare mode:\n%s", md)
	})

	t.Run("empty_infra_error_omits_infra_error_bullet", func(t *testing.T) {
		rep := &Report{
			Mode:      "single",
			Target:    "go",
			Scenarios: []ScenarioResult{{ID: "PUB-01", Category: "publish", Status: StatusPass, DurationMs: 5}},
		}
		md, err := FormatMarkdownReport(rep)
		must(t, err)
		assert(t, !strings.Contains(md, "Infra Error"), "unexpected Infra Error line when InfraError is empty:\n%s", md)
	})
}

func TestTier7_ReportEmptyPathAndNilSummaryMarshal(t *testing.T) {
	t.Run("GAP-A7-H5_WriteReportEmptyAndWhitespacePathRejected", func(t *testing.T) {
		rep := &Report{
			Mode:      "single",
			Target:    "go",
			Scenarios: []ScenarioResult{{ID: "PUB-01", Status: StatusPass}},
		}
		for _, p := range []string{"", "   ", "\t\n "} {
			errMD := WriteMarkdownReport(p, rep)
			assert(t, errMD != nil && strings.Contains(errMD.Error(), "report output path cannot be empty"), "WriteMarkdownReport(%q) = %v", p, errMD)
			errJSON := WriteJSONReport(p, rep)
			assert(t, errJSON != nil && strings.Contains(errJSON.Error(), "report output path cannot be empty"), "WriteJSONReport(%q) = %v", p, errJSON)
		}
	})

	t.Run("GAP-A7-H6_MarshalJSONComputesNilSummaryInSingleMode", func(t *testing.T) {
		for _, mode := range []string{"single", ""} {
			rep := Report{
				Mode:   mode,
				Target: "go",
				Scenarios: []ScenarioResult{
					{ID: "S1", Status: StatusPass},
					{ID: "S2", Status: StatusFail, Error: "boom"},
				},
			}
			raw, err := json.Marshal(rep)
			must(t, err)
			var m map[string]any
			must(t, json.Unmarshal(raw, &m))
			sumObj, ok := m["summary"].(map[string]any)
			assert(t, ok && sumObj != nil && sumObj["total"] == float64(2) && sumObj["pass"] == float64(1) && sumObj["fail"] == float64(1), "mode=%q summary=%v raw=%s", mode, m["summary"], raw)
			var roundTrip Report
			must(t, json.Unmarshal(raw, &roundTrip))
			assert(t, roundTrip.Summary != nil && roundTrip.Summary.Total == 2 && roundTrip.Summary.Pass == 1 && roundTrip.Summary.Fail == 1, "roundTrip.Summary=%+v", roundTrip.Summary)
		}
	})
}
