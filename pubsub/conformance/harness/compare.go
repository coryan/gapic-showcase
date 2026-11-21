// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
)

type Difference struct {
	Kind        string            `json:"kind"`
	ScenarioID  string            `json:"scenario_id,omitempty"`
	Field       string            `json:"field,omitempty"`
	Description string            `json:"description"`
	Verdicts    map[string]string `json:"verdicts"`
	Details     map[string]string `json:"details,omitempty"`
}

func CompareTargets(targets []TargetReport) (*Report, error) {
	if len(targets) < 2 {
		return nil, fmt.Errorf("compare requires at least 2 target reports, got %d", len(targets))
	}
	if err := validateTargetCollection(targets); err != nil {
		return nil, err
	}
	norm, timedOut := make([]TargetReport, len(targets)), false
	for i, t := range targets {
		t.Target = strings.TrimSpace(t.Target)
		norm[i] = normalizeTargetReport(t)
		timedOut = timedOut || norm[i].TimedOut
	}
	diffs := append(diffTargetErrors(norm), diffStructFields("capability", "", norm, func(t TargetReport) any { return t.Capabilities }, 3)...)
	diffs = append(diffs, diffStructFields("client_default", "defaults.", norm, func(t TargetReport) any {
		if t.Capabilities != nil {
			return t.Capabilities.Defaults
		}
		return nil
	}, 0)...)
	return &Report{SchemaVersion: ReportSchemaVersion, Mode: "compare", GeneratedAt: time.Now().UTC(), TimedOut: timedOut, Targets: norm, Differences: append(diffs, diffScenarios(norm)...)}, nil
}
func validateTargetCollection(targets []TargetReport) error {
	seen := make(map[string]struct{}, len(targets))
	for i, t := range targets {
		name := strings.TrimSpace(t.Target)
		if name == "" {
			return fmt.Errorf("target[%d]: target name cannot be empty", i)
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("duplicate target name %q in comparison", name)
		}
		seen[name] = struct{}{}
		scSeen := make(map[string]struct{}, len(t.Scenarios))
		for _, sc := range t.Scenarios {
			if _, dup := scSeen[sc.ID]; dup {
				return fmt.Errorf("duplicate scenario ID %q in target %q", sc.ID, name)
			}
			scSeen[sc.ID] = struct{}{}
		}
	}
	return nil
}
func diffTargetErrors(targets []TargetReport) []Difference {
	hasErr, verdicts, details := false, make(map[string]string, len(targets)), make(map[string]string, len(targets))
	for _, t := range targets {
		if t.TargetError != "" || t.TimedOut {
			hasErr, verdicts[t.Target], details[t.Target] = true, map[bool]string{true: "ERROR", false: "TIMEOUT"}[t.TargetError != ""], firstNonEmpty(t.TargetError, "target timed out")
		} else {
			verdicts[t.Target] = "OK"
		}
	}
	if !hasErr {
		return nil
	}
	return []Difference{{Kind: "target_error", Description: "target reported startup/infra/timeout error", Verdicts: verdicts, Details: details}}
}
func diffStructFields(kind, prefix string, targets []TargetReport, extract func(TargetReport) any, start int) []Difference {
	objs, present, typ := make([]reflect.Value, len(targets)), 0, reflect.Type(nil)
	for i, t := range targets {
		if v := reflect.ValueOf(extract(t)); v.IsValid() && !v.IsNil() {
			objs[i], typ, present = v.Elem(), v.Elem().Type(), present+1
		}
	}
	if present < 2 || typ == nil {
		return nil
	}
	var diffs []Difference
	for f := start; f < typ.NumField(); f++ {
		sf := typ.Field(f)
		tag, _, _ := strings.Cut(sf.Tag.Get("json"), ",")
		if sf.Type.Kind() == reflect.Ptr || sf.PkgPath != "" || tag == "" || tag == "-" {
			continue
		}
		verdicts, distinct, parts := make(map[string]string, len(targets)), map[string]struct{}{}, []string(nil)
		for i, t := range targets {
			if !objs[i].IsValid() {
				verdicts[t.Target] = "MISSING"
				continue
			}
			val := fmt.Sprint(objs[i].Field(f).Interface())
			verdicts[t.Target], distinct[val], parts = val, struct{}{}, append(parts, t.Target+"="+val)
		}
		if len(distinct) > 1 {
			diffs = append(diffs, Difference{Kind: kind, Field: prefix + tag, Verdicts: verdicts, Description: fmt.Sprintf("%s %s%s differs (%s)", kind, prefix, tag, strings.Join(parts, ", "))})
		}
	}
	return diffs
}
func indexTargetScenarios(targets []TargetReport) ([]string, map[string]map[string]ScenarioResult) {
	seen, order, byTarget := map[string]struct{}{}, []string(nil), make(map[string]map[string]ScenarioResult, len(targets))
	for _, t := range targets {
		m := make(map[string]ScenarioResult, len(t.Scenarios))
		for _, sc := range t.Scenarios {
			m[sc.ID] = sc
			if _, ok := seen[sc.ID]; !ok {
				seen[sc.ID], order = struct{}{}, append(order, sc.ID)
			}
		}
		byTarget[t.Target] = m
	}
	return order, byTarget
}
func diffScenarios(targets []TargetReport) []Difference {
	order, byTarget := indexTargetScenarios(targets)
	var diffs []Difference
	for _, id := range order {
		verdicts, traceV, details := make(map[string]string, len(targets)), make(map[string]string, len(targets)), map[string]string{}
		canonSet, rawSet, traceSet := map[string]struct{}{}, map[string]struct{}{}, map[string]struct{}{}
		omitted, parts := false, []string(nil)
		for _, t := range targets {
			sr, ok := byTarget[t.Target][id]
			if !ok {
				omitted, verdicts[t.Target], parts = true, "MISSING", append(parts, t.Target+"=MISSING")
				continue
			}
			raw := effectiveRawStatus(sr)
			canon := string(raw.Canonical())
			verdicts[t.Target], canonSet[canon], rawSet[string(raw)], parts = canon, struct{}{}, struct{}{}, append(parts, t.Target+"="+string(raw))
			if d := firstNonEmpty(sr.Error, sr.Reason); d != "" {
				details[t.Target] = d
			}
			if ts := firstNonEmpty(string(sr.TraceStatus), map[bool]string{true: string(traces.StatusPass)}[canon == string(StatusPass)]); ts != "" {
				traceV[t.Target], traceSet[ts] = ts, struct{}{}
			}
		}
		if len(details) == 0 {
			details = nil
		}
		_, isFail := canonSet[string(StatusFail)]
		_, isPass := canonSet[string(StatusPass)]
		var kind, desc string
		v := verdicts
		switch {
		case omitted || len(canonSet) > 1:
			kind, desc = "scenario_verdict", fmt.Sprintf("scenario %s verdict differs (%s)", id, strings.Join(parts, ", "))
		case isFail && len(rawSet) > 1:
			kind, desc = "scenario_diagnostic", fmt.Sprintf("scenario %s failure mode differs (%s)", id, strings.Join(parts, ", "))
		case isPass && len(canonSet) == 1 && len(traceSet) > 1:
			tp := make([]string, len(targets))
			for i, t := range targets {
				tp[i] = t.Target + "=" + traceV[t.Target]
			}
			kind, v, desc = "scenario_trace_delta", traceV, fmt.Sprintf("scenario %s trace verification differs (%s)", id, strings.Join(tp, ", "))
		}
		if kind != "" {
			diffs = append(diffs, Difference{Kind: kind, ScenarioID: id, Description: desc, Verdicts: v, Details: details})
		}
	}
	return diffs
}
func effectiveRawStatus(sr ScenarioResult) Status {
	if sr.StatusDetail == StatusTimeout || sr.StatusDetail == StatusError {
		return sr.StatusDetail
	}
	return sr.Status
}
func LoadJSONReport(path string) (*Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading JSON report %q: %w", path, err)
	}
	return ParseJSONReportBytes(data)
}
func ParseJSONReportBytes(data []byte) (*Report, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty JSON report")
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()
	var root map[string]any
	if err := dec.Decode(&root); err != nil || root == nil {
		return nil, fmt.Errorf("invalid JSON report: expected JSON object (%v)", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("unexpected trailing JSON content")
	}
	promoteAliases(root, "capabilities:client_capabilities", "scenarios:results,scenario_results,cases", "targets:clients,reports,runs", "differences:diffs,divergences,behavioral_differences,comparisons", "target:target_name,name,client")
	if caps, ok := root["capabilities"].(map[string]any); ok {
		normalizeCapsAliases(caps)
	}
	rawScen, hasScen := root["scenarios"]
	rawTarg, hasTarg := root["targets"]
	hasTarg = hasTarg && rawTarg != nil
	if !hasScen && !hasTarg {
		return nil, fmt.Errorf("JSON report missing both scenarios and targets")
	}
	if err := normalizeScenarioArray(rawScen); hasScen && err != nil {
		return nil, err
	}
	if err := normalizeTargetsField(root, rawTarg); hasTarg && err != nil {
		return nil, err
	}
	rawNorm, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	var rep Report
	if err := json.Unmarshal(rawNorm, &rep); err != nil {
		return nil, fmt.Errorf("decoding normalized report: %w", err)
	}
	rep.SchemaVersion, rep.Targets, rep.Differences = firstNonEmpty(rep.SchemaVersion, ReportSchemaVersion), nonNilSlice(rep.Targets), nonNilSlice(rep.Differences)
	rep.Mode = firstNonEmpty(rep.Mode, map[bool]string{true: "compare", false: "single"}[len(rep.Targets) > 1])
	if len(rep.Targets) == 0 && hasScen {
		name := strings.TrimSpace(rep.Target)
		rep.Target = name
		if name == "" && rep.Capabilities != nil {
			name = strings.TrimSpace(rep.Capabilities.ClientLanguage)
		}
		rep.Targets = []TargetReport{{Target: name, TargetError: rep.InfraError, TimedOut: rep.TimedOut, Capabilities: rep.Capabilities, Scenarios: slices.Clone(rep.Scenarios)}}
	} else if len(rep.Targets) == 1 && !hasScen {
		rep.Scenarios, rep.Target = slices.Clone(rep.Targets[0].Scenarios), firstNonEmpty(strings.TrimSpace(rep.Target), strings.TrimSpace(rep.Targets[0].Target))
		if rep.Capabilities == nil {
			rep.Capabilities = rep.Targets[0].Capabilities
		}
	}
	rep.TimedOut = normalizeTargetReports(rep.Targets) || rep.TimedOut
	if rep.Mode != "compare" {
		rep.Scenarios, rep.Summary = nonNilSlice(rep.Scenarios), ComputeSummary(rep.Scenarios)
	}
	return &rep, nil
}
func CompareJSONFiles(paths []string) (*Report, error) {
	parsed, err := ParseCompareArgs(paths)
	if err != nil {
		return nil, err
	}
	type loadedTarget struct {
		report     TargetReport
		base       string
		orig       string
		wasEmpty   bool
		wasDefault bool
	}
	var loaded []loadedTarget
	explicitCounts := map[string]int{}
	explicitBaseCounts := map[string]map[string]int{}
	defaultCount := 0
	for _, p := range parsed {
		rep, err := LoadJSONReport(p)
		if err != nil {
			return nil, err
		}
		base := strings.TrimSuffix(filepath.Base(p), filepath.Ext(p))
		for _, t := range rep.Targets {
			orig := strings.TrimSpace(t.Target)
			wasDefault := orig == "default"
			if orig == "" && t.Capabilities != nil {
				orig = strings.TrimSpace(t.Capabilities.ClientLanguage)
			}
			wasEmpty := orig == ""
			if wasDefault {
				defaultCount++
			} else if !wasEmpty {
				explicitCounts[orig]++
				if explicitBaseCounts[orig] == nil {
					explicitBaseCounts[orig] = map[string]int{}
				}
				explicitBaseCounts[orig][base]++
			}
			loaded = append(loaded, loadedTarget{
				report:     t,
				base:       base,
				orig:       orig,
				wasEmpty:   wasEmpty,
				wasDefault: wasDefault,
			})
		}
	}
	used := make(map[string]int, len(loaded))
	assignUnique := func(i int, cand string) {
		cand = firstNonEmpty(cand, "target")
		for baseName, n := cand, 2; used[cand] > 0; n++ {
			cand = fmt.Sprintf("%s-%d", baseName, n)
		}
		used[cand] = 1
		loaded[i].report.Target = cand
	}
	resolved := make([]bool, len(loaded))
	for i, lt := range loaded {
		if !lt.wasEmpty && !lt.wasDefault && explicitCounts[lt.orig] == 1 {
			assignUnique(i, lt.orig)
			resolved[i] = true
		} else if lt.wasDefault && defaultCount == 1 && explicitCounts["default"] == 0 {
			assignUnique(i, "default")
			resolved[i] = true
		}
	}
	for i, lt := range loaded {
		if resolved[i] || lt.wasEmpty || lt.wasDefault {
			continue
		}
		cand := lt.orig
		if lt.base != "" && lt.base != lt.orig && explicitBaseCounts[lt.orig][lt.base] == 1 {
			cand = lt.orig + "-" + lt.base
		}
		assignUnique(i, cand)
		resolved[i] = true
	}
	for i, lt := range loaded {
		if resolved[i] {
			continue
		}
		cand := firstNonEmpty(lt.base, lt.orig, "default")
		assignUnique(i, cand)
		resolved[i] = true
	}
	targets := make([]TargetReport, len(loaded))
	for i, lt := range loaded {
		targets[i] = lt.report
	}
	return CompareTargets(targets)
}
func promoteAliases(m map[string]any, specs ...string) {
	for _, s := range specs {
		canon, rest, _ := strings.Cut(s, ":")
		if _, ok := m[canon]; ok {
			continue
		}
		for _, a := range strings.Split(rest, ",") {
			if v, ok := m[a]; ok {
				m[canon] = v
				break
			}
		}
	}
}
func normalizeCapsAliases(m map[string]any) {
	promoteAliases(m, "client_language:language,clientLanguage", "client_library:library,clientLibrary", "client_version:version,clientVersion", "supports_monitoring:supportsMonitoring,monitoring")
	if dm, ok := m["defaults"].(map[string]any); ok {
		for k, names := range map[string]map[int32]string{"publish_flow_control_behavior": proxypb.FlowControlLimitBehavior_name, "publish_compression_algorithm": proxypb.CompressionAlgorithm_name} {
			if num, ok := dm[k].(json.Number); ok {
				if n, err := num.Int64(); err == nil {
					dm[k] = firstNonEmpty(names[int32(n)], num.String())
				}
			}
		}
	}
}
func normalizeTargetsField(root map[string]any, raw any) error {
	var arr []any
	switch tv := raw.(type) {
	case []any:
		seen := map[string]struct{}{}
		for i, item := range tv {
			tm, ok := item.(map[string]any)
			if !ok {
				return fmt.Errorf("targets[%d] must be a JSON object", i)
			}
			promoteAliases(tm, "target:target_name,name,client,id")
			s, _ := tm["target"].(string)
			name := strings.TrimSpace(s)
			if name == "" {
				name = fmt.Sprintf("target-%d", i)
			}
			tm["target"] = name
			if _, dup := seen[name]; dup {
				return fmt.Errorf("duplicate target %q in report", name)
			}
			seen[name], arr = struct{}{}, append(arr, tm)
		}
	case map[string]any:
		seen := map[string]struct{}{}
		for _, k := range slices.Sorted(maps.Keys(tv)) {
			name := strings.TrimSpace(k)
			if name == "" {
				return fmt.Errorf("target map key cannot be empty")
			}
			if _, dup := seen[name]; dup {
				return fmt.Errorf("duplicate target %q in report", name)
			}
			tm, ok := tv[k].(map[string]any)
			if !ok {
				return fmt.Errorf("target %q must be a JSON object", k)
			}
			seen[name] = struct{}{}
			tm["target"] = name
			arr = append(arr, tm)
		}
	default:
		return fmt.Errorf("targets must be an array or object")
	}
	for i, item := range arr {
		tm := item.(map[string]any)
		promoteAliases(tm, "capabilities:client_capabilities", "scenarios:results,scenario_results,cases")
		if caps, ok := tm["capabilities"].(map[string]any); ok {
			normalizeCapsAliases(caps)
		}
		if err := normalizeScenarioArray(tm["scenarios"]); tm["scenarios"] != nil && err != nil {
			return fmt.Errorf("target %v (index %d): %w", tm["target"], i, err)
		}
	}
	root["targets"] = arr
	return nil
}
func normalizeScenarioArray(raw any) error {
	arr, ok := raw.([]any)
	if !ok {
		return fmt.Errorf("scenarios block must be a JSON array")
	}
	seenIDs := make(map[string]struct{}, len(arr))
	for i, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("scenario[%d] must be a JSON object", i)
		}
		promoteAliases(m, "id:scenario_id,name,scenario", "category:group,suite", "status:verdict,outcome,result,state", "error:err,error_message,message,details,failure")
		idStr, ok := m["id"].(string)
		if !ok && m["id"] != nil {
			idStr = fmt.Sprint(m["id"])
		}
		if _, dup := seenIDs[idStr]; dup {
			return fmt.Errorf("duplicate scenario ID %q", idStr)
		}
		seenIDs[idStr] = struct{}{}
		if em, ok := m["error"].(map[string]any); ok {
			promoteAliases(em, "message:msg,error,reason,detail")
			if em["message"] != nil {
				m["error"] = em["message"]
			}
		}
		for _, k := range []string{"error", "duration"} {
			if v := m[k]; v != nil {
				m[k] = fmt.Sprint(v)
			}
		}
		rawStatus, ok := m["status"].(string)
		st, err := parseStatusString(rawStatus)
		if !ok || err != nil {
			return fmt.Errorf("scenario[%d] (%v): status invalid/missing (%q)", i, m["id"], rawStatus)
		}
		m["status"] = string(st)
		if d, _ := m["status_detail"].(string); d != "" {
			if dst, derr := parseStatusString(d); derr == nil && (dst == StatusTimeout || dst == StatusError) {
				m["status_detail"] = string(dst)
			}
		}
	}
	return nil
}

var statusAliases = map[string]Status{"PASS": StatusPass, "PASSED": StatusPass, "FAIL": StatusFail, "FAILED": StatusFail, "ERROR": StatusError, "TIMEOUT": StatusTimeout, "SKIPPED_NO_MONITORING": StatusSkippedNoMonitoring, "SKIPPED": StatusSkippedNoMonitoring, "UNSUPPORTED_BY_CLIENT": StatusUnsupportedByClient, "UNSUPPORTED": StatusUnsupportedByClient}

func parseStatusString(raw string) (Status, error) {
	if st, ok := statusAliases[strings.ReplaceAll(strings.ToUpper(strings.TrimSpace(raw)), "-", "_")]; ok {
		return st, nil
	}
	return "", fmt.Errorf("unknown scenario status %q", raw)
}
