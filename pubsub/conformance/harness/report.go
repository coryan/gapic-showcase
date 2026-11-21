// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
)

const ReportSchemaVersion = "1.0"

type Report struct {
	SchemaVersion string              `json:"schema_version"`
	Mode          string              `json:"mode"`
	Target        string              `json:"target,omitempty"`
	GeneratedAt   time.Time           `json:"generated_at"`
	InfraError    string              `json:"infra_error,omitempty"`
	TimedOut      bool                `json:"timed_out,omitempty"`
	Capabilities  *CapabilitiesReport `json:"capabilities,omitempty"`
	Summary       *Summary            `json:"summary,omitempty"`
	Scenarios     []ScenarioResult    `json:"scenarios,omitempty"`
	Targets       []TargetReport      `json:"targets"`
	Differences   []Difference        `json:"differences"`
}

func (r Report) MarshalJSON() ([]byte, error) {
	type reportAlias Report
	w := struct {
		*reportAlias
		Scenarios *[]ScenarioResult `json:"scenarios,omitempty"`
	}{reportAlias: (*reportAlias)(&r)}
	w.SchemaVersion, w.Targets, w.Differences = firstNonEmpty(r.SchemaVersion, ReportSchemaVersion), nonNilSlice(r.Targets), nonNilSlice(r.Differences)
	w.Mode, w.TimedOut = firstNonEmpty(w.Mode, map[bool]string{true: "compare", false: "single"}[len(w.Targets) > 1]), normalizeTargetReports(w.Targets) || w.TimedOut
	if w.Mode == "compare" {
		w.Target, w.Capabilities, w.Summary, w.Scenarios = "", nil, nil, nil
	} else {
		sc := nonNilSlice(r.Scenarios)
		w.Scenarios = &sc
		if w.Summary == nil {
			w.Summary = ComputeSummary(sc)
		}
	}
	return json.Marshal(w)
}
func nonNilSlice[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return slices.Clone(s)
}

type TargetReport struct {
	Target       string              `json:"target"`
	Command      string              `json:"command,omitempty"`
	TargetError  string              `json:"target_error,omitempty"`
	TimedOut     bool                `json:"timed_out,omitempty"`
	Capabilities *CapabilitiesReport `json:"capabilities,omitempty"`
	Summary      *Summary            `json:"summary"`
	Scenarios    []ScenarioResult    `json:"scenarios"`
}
type Summary struct {
	Total               int `json:"total"`
	Pass                int `json:"pass"`
	Fail                int `json:"fail"`
	SkippedNoMonitoring int `json:"skipped_no_monitoring"`
	UnsupportedByClient int `json:"unsupported_by_client"`
	TimeoutCount        int `json:"timeout_count,omitempty"`
	ErrorCount          int `json:"error_count,omitempty"`
}
type ScenarioResult struct {
	ID             string             `json:"id"`
	Name           string             `json:"name,omitempty"`
	Category       string             `json:"category,omitempty"`
	Feature        string             `json:"feature,omitempty"`
	Status         Status             `json:"status"`
	StatusDetail   Status             `json:"status_detail,omitempty"`
	TraceStatus    traces.Status      `json:"trace_status,omitempty"`
	Reason         string             `json:"reason,omitempty"`
	Error          string             `json:"error,omitempty"`
	Duration       string             `json:"duration"`
	DurationMs     int64              `json:"duration_ms"`
	Diagnostics    []string           `json:"diagnostics,omitempty"`
	Deviations     []traces.Deviation `json:"deviations,omitempty"`
	ProxyRestarted bool               `json:"proxy_restarted,omitempty"`
}

func (s ScenarioResult) MarshalJSON() ([]byte, error) {
	type wire ScenarioResult
	w := wire(s)
	if raw := Status(firstNonEmpty(string(w.Status), string(w.StatusDetail))); raw.Canonical() != raw && raw != "" {
		w.Status, w.StatusDetail, w.Error = raw.Canonical(), raw, formatStatusError(raw, w.Error)
	}
	if w.Duration == "" {
		w.Duration = (time.Duration(w.DurationMs) * time.Millisecond).String()
	}
	return json.Marshal(w)
}
func (s *ScenarioResult) UnmarshalJSON(data []byte) error {
	type wire ScenarioResult
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	if w.StatusDetail == StatusTimeout || w.StatusDetail == StatusError {
		w.Status = w.StatusDetail
	}
	*s = ScenarioResult(w)
	return nil
}

type CapabilitiesReport struct {
	ClientLanguage               string                `json:"client_language"`
	ClientLibrary                string                `json:"client_library"`
	ClientVersion                string                `json:"client_version"`
	SupportsMonitoring           bool                  `json:"supports_monitoring"`
	SupportsUnaryPull            bool                  `json:"supports_unary_pull"`
	SupportsBlockingPublish      bool                  `json:"supports_blocking_publish"`
	SupportsPublishFlowControl   bool                  `json:"supports_publish_flow_control"`
	SupportsPublishCompression   bool                  `json:"supports_publish_compression"`
	SupportsPublishHedging       bool                  `json:"supports_publish_hedging"`
	SupportsOrderingOptIn        bool                  `json:"supports_ordering_opt_in"`
	SupportsSubscriberOrdering   bool                  `json:"supports_subscriber_ordering"`
	SupportsKeepalive            bool                  `json:"supports_keepalive"`
	SupportsEodReceiptModack     bool                  `json:"supports_eod_receipt_modack"`
	SupportsAutoNackOnDrop       bool                  `json:"supports_auto_nack_on_drop"`
	SupportsAckWithResult        bool                  `json:"supports_ack_with_result"`
	SupportsCustomRetryCodes     bool                  `json:"supports_custom_retry_codes"`
	SupportsExactlyOnceDelivery  bool                  `json:"supports_exactly_once_delivery"`
	SupportsPerStreamFlowControl bool                  `json:"supports_per_stream_flow_control"`
	SupportsResumePublish        bool                  `json:"supports_resume_publish"`
	SupportsShutdownBehavior     bool                  `json:"supports_shutdown_behavior"`
	SupportsShutdownTimeout      bool                  `json:"supports_shutdown_timeout"`
	Defaults                     *ClientDefaultsReport `json:"defaults,omitempty"`
}
type ClientDefaultsReport struct {
	PublishBatchCountThreshold                int64   `json:"publish_batch_count_threshold"`
	PublishBatchByteThreshold                 int64   `json:"publish_batch_byte_threshold"`
	PublishMaxRequestBytes                    int64   `json:"publish_max_request_bytes"`
	PublishBatchDelayMs                       int64   `json:"publish_batch_delay_ms"`
	PublishTotalTimeoutMs                     int64   `json:"publish_total_timeout_ms"`
	PublishFlowControlMaxMessages             int64   `json:"publish_flow_control_max_messages"`
	PublishFlowControlMaxBytes                int64   `json:"publish_flow_control_max_bytes"`
	PublishFlowControlBehavior                string  `json:"publish_flow_control_behavior"`
	PublishCompressionEnabledByDefault        bool    `json:"publish_compression_enabled_by_default"`
	PublishCompressionBytesThreshold          int64   `json:"publish_compression_bytes_threshold"`
	PublishCompressionAlgorithm               string  `json:"publish_compression_algorithm"`
	PublishHedgingDelayMs                     int64   `json:"publish_hedging_delay_ms"`
	PublishHedgingMaxTokens                   int64   `json:"publish_hedging_max_tokens"`
	PublishHedgingRefillRatio                 float64 `json:"publish_hedging_refill_ratio"`
	SubscriberMaxOutstandingMessages          int64   `json:"subscriber_max_outstanding_messages"`
	SubscriberMaxOutstandingBytes             int64   `json:"subscriber_max_outstanding_bytes"`
	SubscriberMaxExtensionMs                  int64   `json:"subscriber_max_extension_ms"`
	SubscriberMinAckExtensionMs               int64   `json:"subscriber_min_ack_extension_ms"`
	SubscriberMaxAckExtensionMs               int64   `json:"subscriber_max_ack_extension_ms"`
	SubscriberAdaptiveAckExtension            bool    `json:"subscriber_adaptive_ack_extension"`
	SubscriberPerStreamFlowControl            bool    `json:"subscriber_per_stream_flow_control"`
	SubscriberDefaultStreamAckDeadlineSeconds int32   `json:"subscriber_default_stream_ack_deadline_seconds"`
	KeepalivePingIntervalMs                   int64   `json:"keepalive_ping_interval_ms"`
	KeepalivePongTimeoutMs                    int64   `json:"keepalive_pong_timeout_ms"`
	RetryInitialDelayMs                       int64   `json:"retry_initial_delay_ms"`
	RetryMaxDelayMs                           int64   `json:"retry_max_delay_ms"`
	RetryDelayMultiplier                      float64 `json:"retry_delay_multiplier"`
}

func CapabilitiesFromProto(caps *proxypb.GetCapabilitiesResponse) *CapabilitiesReport {
	if caps == nil {
		return nil
	}
	var cr CapabilitiesReport
	copyMatchingStructFields(&cr, caps)
	if d := caps.GetDefaults(); d != nil {
		cr.Defaults = &ClientDefaultsReport{}
		copyMatchingStructFields(cr.Defaults, d)
	}
	return &cr
}
func copyMatchingStructFields(dstPtr, srcPtr any) {
	dv, sv := reflect.ValueOf(dstPtr).Elem(), reflect.ValueOf(srcPtr).Elem()
	for i := 0; i < dv.NumField(); i++ {
		sf, src := dv.Type().Field(i), sv.FieldByName(dv.Type().Field(i).Name)
		if sf.PkgPath != "" || sf.Type.Kind() == reflect.Ptr || !src.IsValid() {
			continue
		}
		if src.Type().AssignableTo(sf.Type) {
			dv.Field(i).Set(src)
		} else if str, ok := src.Interface().(fmt.Stringer); ok && sf.Type.Kind() == reflect.String {
			dv.Field(i).SetString(str.String())
		}
	}
}
func ComputeSummary(results []ScenarioResult) *Summary {
	s := &Summary{Total: len(results)}
	for _, r := range results {
		raw := effectiveRawStatus(r)
		s.TimeoutCount += map[bool]int{true: 1}[raw == StatusTimeout]
		s.ErrorCount += map[bool]int{true: 1}[raw == StatusError]
		switch raw.Canonical() {
		case StatusPass:
			s.Pass++
		case StatusFail:
			s.Fail++
		case StatusSkippedNoMonitoring:
			s.SkippedNoMonitoring++
		case StatusUnsupportedByClient:
			s.UnsupportedByClient++
		}
	}
	return s
}
func BuildReport(targets []TargetReport) (*Report, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("cannot build report with zero targets")
	}
	if len(targets) > 1 {
		return CompareTargets(targets)
	}
	t := normalizeTargetReport(targets[0])
	return &Report{SchemaVersion: ReportSchemaVersion, Mode: "single", Target: t.Target, GeneratedAt: time.Now().UTC(), TimedOut: t.TimedOut, Capabilities: t.Capabilities, Summary: t.Summary, Scenarios: slices.Clone(t.Scenarios), Targets: []TargetReport{t}, Differences: []Difference{}}, nil
}

func normalizeTargetReport(t TargetReport) TargetReport {
	t.Scenarios, t.Summary = nonNilSlice(t.Scenarios), ComputeSummary(t.Scenarios)
	return t
}
func normalizeTargetReports(targets []TargetReport) bool {
	timedOut := false
	for i := range targets {
		targets[i] = normalizeTargetReport(targets[i])
		timedOut = timedOut || targets[i].TimedOut
	}
	return timedOut
}
func WriteJSONReport(path string, r *Report) error {
	if r == nil {
		return fmt.Errorf("report cannot be nil")
	}
	data, err := json.MarshalIndent(r, "", "  ")
	return writeReportBytes(path, append(data, '\n'), err)
}
func WriteMarkdownReport(path string, r *Report) error {
	md, err := FormatMarkdownReport(r)
	return writeReportBytes(path, []byte(md), err)
}
func writeReportBytes(path string, data []byte, err error) error {
	if err != nil {
		return err
	}
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("report output path cannot be empty")
	}
	if info, serr := os.Stat(path); serr == nil && info.IsDir() {
		return fmt.Errorf("report output path %q is a directory", path)
	}
	return os.WriteFile(path, data, 0o644)
}
func FormatMarkdownReport(r *Report) (string, error) {
	if r == nil {
		return "", fmt.Errorf("report cannot be nil")
	}
	var b strings.Builder
	if r.Mode == "compare" || len(r.Targets) > 1 {
		if err := formatCompareMarkdown(&b, r); err != nil {
			return "", err
		}
	} else {
		formatSingleMarkdown(&b, r)
	}
	return b.String(), nil
}
func summaryCols(sum *Summary) []string {
	return []string{strconv.Itoa(sum.Total), strconv.Itoa(sum.Pass), strconv.Itoa(sum.Fail), strconv.Itoa(sum.SkippedNoMonitoring), strconv.Itoa(sum.UnsupportedByClient)}
}
func formatSingleMarkdown(b *strings.Builder, r *Report) {
	targetName, caps, scens := strings.TrimSpace(r.Target), r.Capabilities, r.Scenarios
	if len(r.Targets) > 0 {
		t0 := r.Targets[0]
		targetName = firstNonEmpty(targetName, strings.TrimSpace(t0.Target))
		if caps == nil {
			caps = t0.Capabilities
		}
		if scens == nil {
			scens = t0.Scenarios
		}
	}
	b.WriteString("# Pub/Sub Conformance Report" + map[bool]string{true: ": " + escapeMDCell(targetName)}[targetName != ""] + "\n\n")
	if caps != nil {
		writeMDSection(b, "Target & Capabilities", []string{"Target", "Language", "Library", "Version", "Supports Monitoring"}, [][]string{{targetName, caps.ClientLanguage, caps.ClientLibrary, caps.ClientVersion, strconv.FormatBool(caps.SupportsMonitoring)}})
	}
	sum := r.Summary
	if sum == nil {
		sum = ComputeSummary(scens)
	}
	writeMDSection(b, "Summary", []string{"Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client"}, [][]string{summaryCols(sum)})
	if sum.TimeoutCount > 0 || sum.ErrorCount > 0 {
		fmt.Fprintf(b, "_Failure breakdown: %d timeout(s), %d error(s)._\n\n", sum.TimeoutCount, sum.ErrorCount)
	}
	rows := make([][]string, len(scens))
	for i, sc := range scens {
		rows[i] = []string{sc.ID, sc.Category, formatMDStatus(sc.Status, sc.StatusDetail), firstNonEmpty(sc.Duration, fmt.Sprintf("%dms", sc.DurationMs)), firstNonEmpty(sc.Error, sc.Reason, strings.Join(sc.Diagnostics, "; "))}
	}
	writeMDSection(b, "Scenario Results", []string{"Scenario", "Category", "Status", "Duration", "Details"}, rows)
	appendDiagnosticsMarkdown(b, r, scens)
}
func formatCompareMarkdown(b *strings.Builder, r *Report) error {
	if err := validateTargetCollection(r.Targets); err != nil {
		return err
	}
	b.WriteString("# Pub/Sub Cross-Client Conformance Comparison Report\n\n")
	sumRows, targetCols := make([][]string, 0, len(r.Targets)), make([]string, len(r.Targets))
	for i, t := range r.Targets {
		targetCols[i] = t.Target
		lang, lib, mon, sum := "", "", "unknown", t.Summary
		if t.Capabilities != nil {
			lang, lib, mon = t.Capabilities.ClientLanguage, t.Capabilities.ClientLibrary, strconv.FormatBool(t.Capabilities.SupportsMonitoring)
		}
		if sum == nil {
			sum = ComputeSummary(t.Scenarios)
		}
		sumRows = append(sumRows, append([]string{t.Target, lang, lib, mon}, summaryCols(sum)...))
	}
	writeMDSection(b, "Summary", []string{"Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client"}, sumRows)
	var capRows, diffRows [][]string
	for _, d := range r.Differences {
		v := collectVerdicts(r.Targets, d.Verdicts)
		if d.Kind == "capability" || d.Kind == "client_default" {
			capRows = append(capRows, append([]string{d.Field}, v...))
		}
		diffRows = append(diffRows, append(append([]string{d.Kind, firstNonEmpty(d.ScenarioID, d.Field, "-")}, v...), d.Description))
	}
	if len(capRows) > 0 {
		writeMDSection(b, "Capabilities & Defaults", append([]string{"Field"}, targetCols...), capRows)
	}
	scenarioIDs, byTarget := indexTargetScenarios(r.Targets)
	matrixRows := make([][]string, 0, len(scenarioIDs))
	for _, id := range scenarioIDs {
		cat, row := "", []string{id, ""}
		for _, t := range r.Targets {
			if sr, ok := byTarget[t.Target][id]; ok {
				cat, row = firstNonEmpty(cat, sr.Category), append(row, formatMDStatus(sr.Status, sr.StatusDetail))
			} else {
				row = append(row, "MISSING")
			}
		}
		row[1] = cat
		matrixRows = append(matrixRows, row)
	}
	writeMDSection(b, "Scenario Matrix", append([]string{"Scenario", "Category"}, targetCols...), matrixRows)
	if len(diffRows) == 0 {
		b.WriteString("## Behavioral Differences\n\nNo behavioral differences detected.\n\n")
	} else {
		writeMDSection(b, "Behavioral Differences", append(append([]string{"Kind", "Scenario / Field"}, targetCols...), "Description"), diffRows)
	}
	appendDiagnosticsMarkdown(b, r, nil)
	return nil
}
func collectVerdicts(targets []TargetReport, verdicts map[string]string) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = verdicts[t.Target]
	}
	return out
}
func writeMDSection(b *strings.Builder, heading string, headers []string, rows [][]string) {
	fmt.Fprintf(b, "## %s\n\n", heading)
	for _, row := range append([][]string{headers, slices.Repeat([]string{"---"}, len(headers))}, rows...) {
		b.WriteByte('|')
		for _, c := range row {
			fmt.Fprintf(b, " %s |", escapeMDCell(c))
		}
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
}
func appendDiagnosticsMarkdown(b *strings.Builder, r *Report, singleScens []ScenarioResult) {
	var lines []string
	if r.InfraError != "" {
		lines = append(lines, "- **Infra Error**: "+escapeMDCell(r.InfraError))
	}
	if len(r.Targets) == 0 && len(singleScens) > 0 {
		for _, sc := range singleScens {
			if sc.Error != "" {
				lines = append(lines, fmt.Sprintf("- **%s** (`%s`): %s", escapeMDCell(sc.ID), formatMDStatus(sc.Status, sc.StatusDetail), escapeMDCell(sc.Error)))
			}
		}
	}
	for i, t := range r.Targets {
		if t.TargetError != "" {
			lines = append(lines, fmt.Sprintf("- **Target %s Error**: %s", escapeMDCell(t.Target), escapeMDCell(t.TargetError)))
		}
		scList := t.Scenarios
		if len(singleScens) > 0 {
			scList = map[bool][]ScenarioResult{true: singleScens}[i == 0]
		}
		for _, sc := range scList {
			if sc.Error != "" {
				pfx := map[bool]string{true: t.Target + "/" + sc.ID, false: sc.ID}[len(r.Targets) > 1]
				lines = append(lines, fmt.Sprintf("- **%s** (`%s`): %s", escapeMDCell(pfx), formatMDStatus(sc.Status, sc.StatusDetail), escapeMDCell(sc.Error)))
			}
		}
	}
	if len(lines) > 0 {
		b.WriteString("## Diagnostics\n\n" + strings.Join(lines, "\n") + "\n\n")
	}
}

func formatMDStatus(st, detail Status) string {
	if raw := effectiveRawStatus(ScenarioResult{Status: st, StatusDetail: detail}); raw == StatusTimeout || raw == StatusError {
		return "FAIL (" + string(raw) + ")"
	}
	return string(st)
}
func escapeMDCell(s string) string {
	return strings.TrimSpace(strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ", "|", `\|`).Replace(s))
}
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
func ExitCode(r *Report, strictUnsupported bool) int {
	hasFail := func(sum *Summary, scens []ScenarioResult) bool {
		return (sum != nil && (sum.Fail > 0 || (strictUnsupported && sum.UnsupportedByClient > 0))) ||
			slices.ContainsFunc(scens, func(sc ScenarioResult) bool { return sc.Status.IsFailure(strictUnsupported) })
	}
	if r == nil || r.InfraError != "" || r.TimedOut || hasFail(r.Summary, r.Scenarios) ||
		slices.ContainsFunc(r.Targets, func(t TargetReport) bool { return t.TargetError != "" || t.TimedOut || hasFail(t.Summary, t.Scenarios) }) {
		return 1
	}
	return 0
}
