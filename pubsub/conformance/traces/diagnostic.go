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

package traces

import (
	"fmt"
	"sort"
	"strings"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
)

// ViolationCode categorizes trace verification failures for programmatic checks
// and structured reports.
type ViolationCode string

const (
	CodeInvalidTraceID          ViolationCode = "INVALID_TRACE_ID"
	CodeInvalidSpanID           ViolationCode = "INVALID_SPAN_ID"
	CodeInvalidParentSpanID     ViolationCode = "INVALID_PARENT_SPAN_ID"
	CodeDuplicateSpanID         ViolationCode = "DUPLICATE_SPAN_ID"
	CodeInvalidTimestamp        ViolationCode = "INVALID_TIMESTAMP"
	CodeWrongScope              ViolationCode = "WRONG_SCOPE"
	CodeMissingSpan             ViolationCode = "MISSING_SPAN"
	CodeUnexpectedSpan          ViolationCode = "UNEXPECTED_SPAN"
	CodeWrongSpanKind           ViolationCode = "WRONG_SPAN_KIND"
	CodeWrongParent             ViolationCode = "WRONG_PARENT"
	CodeBrokenTraceparent       ViolationCode = "BROKEN_TRACEPARENT"
	CodeMissingLink             ViolationCode = "MISSING_LINK"
	CodeBrokenBidirectionalLink ViolationCode = "BROKEN_BIDIRECTIONAL_LINK"
	CodeMissingLinkAttr         ViolationCode = "MISSING_LINK_ATTR"
	CodeMissingAttribute        ViolationCode = "MISSING_ATTRIBUTE"
	CodeWrongAttributeType      ViolationCode = "WRONG_ATTRIBUTE_TYPE"
	CodeWrongAttributeValue     ViolationCode = "WRONG_ATTRIBUTE_VALUE"
	CodeUnexpectedAttribute     ViolationCode = "UNEXPECTED_ATTRIBUTE"
	CodeMissingEvent            ViolationCode = "MISSING_EVENT"
	CodeWrongStatusCode         ViolationCode = "WRONG_STATUS_CODE"
	CodeWrongStatusMessage      ViolationCode = "WRONG_STATUS_MESSAGE"
	CodeDrainError              ViolationCode = "DRAIN_ERROR"
	CodeCustomCheckFailed       ViolationCode = "CUSTOM_CHECK_FAILED"
)

// Violation describes a single trace verification failure.
type Violation struct {
	Code     ViolationCode `json:"code,omitempty"`
	Rule     string        `json:"rule"`
	Message  string        `json:"message"`
	Expected string        `json:"expected,omitempty"`
	Actual   string        `json:"actual,omitempty"`
	SpanID   string        `json:"span_id,omitempty"`
	TraceID  string        `json:"trace_id,omitempty"`
	SpanName string        `json:"span_name,omitempty"`
}

// SpanSummary is a compact JSON-serializable summary of a captured TraceSpan.
type SpanSummary struct {
	TraceID      string   `json:"trace_id"`
	SpanID       string   `json:"span_id"`
	ParentSpanID string   `json:"parent_span_id,omitempty"`
	Name         string   `json:"name"`
	Kind         string   `json:"kind"`
	Scope        string   `json:"scope,omitempty"`
	StatusCode   string   `json:"status_code"`
	StatusMsg    string   `json:"status_message,omitempty"`
	LinkTargets  []string `json:"link_targets,omitempty"`
	Events       []string `json:"events,omitempty"`
	AttrKeys     []string `json:"attr_keys,omitempty"`
}

// TraceResult holds the complete outcome of trace verification for a scenario.
type TraceResult struct {
	Status             Status        `json:"status"`
	SupportsMonitoring bool          `json:"supports_monitoring"`
	ClientLanguage     string        `json:"client_language,omitempty"`
	Profile            ProfileName   `json:"profile"`
	SpanCount          int           `json:"span_count"`
	Failures           []Violation   `json:"failures,omitempty"`
	Deviations         []Deviation   `json:"deviations,omitempty"`
	Spans              []SpanSummary `json:"spans,omitempty"`
}

// Passed reports whether trace verification passed or was cleanly skipped
// because the client does not support monitoring (R2).
func (r TraceResult) Passed() bool {
	return r.Status == StatusPass || r.Status == StatusSkippedNoMonitoring
}

// Failed reports whether trace verification failed.
func (r TraceResult) Failed() bool {
	return r.Status == StatusFail
}

// Err returns nil when Passed() is true, or a formatted diagnostic error
// describing all violations and captured spans when Failed() is true.
func (r TraceResult) Err() error {
	if r.Passed() {
		return nil
	}
	return fmt.Errorf("%s", r.Format())
}

// Format renders a deterministic human-readable diagnostic report for CLI/Markdown output.
func (r TraceResult) Format() string {
	return FormatReport(&r)
}

// FormatReport renders a deterministic, greppable multi-section report from r.
func FormatReport(r *TraceResult) string {
	if r == nil {
		return "<nil TraceResult>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Status=%s SupportsMonitoring=%t Profile=%s", r.Status, r.SupportsMonitoring, r.Profile)
	if r.ClientLanguage != "" {
		fmt.Fprintf(&b, " ClientLanguage=%s", r.ClientLanguage)
	}
	fmt.Fprintf(&b, " Spans=%d Violations=%d Deviations=%d", r.SpanCount, len(r.Failures), len(r.Deviations))

	if len(r.Failures) > 0 {
		b.WriteString("\nViolations:")
		sortedFailures := make([]Violation, len(r.Failures))
		copy(sortedFailures, r.Failures)
		sort.SliceStable(sortedFailures, func(i, j int) bool {
			if sortedFailures[i].Code != sortedFailures[j].Code {
				return sortedFailures[i].Code < sortedFailures[j].Code
			}
			if sortedFailures[i].SpanName != sortedFailures[j].SpanName {
				return sortedFailures[i].SpanName < sortedFailures[j].SpanName
			}
			if sortedFailures[i].Rule != sortedFailures[j].Rule {
				return sortedFailures[i].Rule < sortedFailures[j].Rule
			}
			return sortedFailures[i].Message < sortedFailures[j].Message
		})
		for i, v := range sortedFailures {
			tracePrefix := v.TraceID
			if len(tracePrefix) > 8 {
				tracePrefix = tracePrefix[:8]
			}
			loc := ""
			if v.SpanName != "" || v.SpanID != "" {
				loc = fmt.Sprintf(" span=%q (%s/%s)", v.SpanName, tracePrefix, v.SpanID)
			}
			code := string(v.Code)
			if code == "" {
				code = "VIOLATION"
			}
			fmt.Fprintf(&b, "\n  [%d] %s (rule=%s)%s: %s", i+1, code, v.Rule, loc, v.Message)
			if v.Expected != "" || v.Actual != "" {
				fmt.Fprintf(&b, "\n      expected: %s\n      actual:   %s", v.Expected, v.Actual)
			}
		}
	}

	if len(r.Deviations) > 0 {
		b.WriteString("\nDeviations:")
		for i, d := range r.Deviations {
			fmt.Fprintf(&b, "\n  [dev %d] %s (%s)", i+1, d.Rule, d.Category)
			if d.SpanName != "" {
				fmt.Fprintf(&b, " on %q", d.SpanName)
			}
			fmt.Fprintf(&b, ": canonical=%s, observed=%s — %s", d.ExpectedGo, d.Observed, d.Description)
		}
	}

	if len(r.Spans) > 0 {
		const maxUnboundedSpans = 200
		const edgeCount = 25
		b.WriteString("\nCaptured Spans:")
		if len(r.Spans) <= maxUnboundedSpans {
			for i, s := range r.Spans {
				fmt.Fprintf(&b, "\n  [%d] %s", i, formatSummaryLine(s))
			}
		} else {
			unmatched := unmatchedSpanIDs(r.Failures)
			printed := make(map[int]bool)
			var headLines, midLines, tailLines []string
			for i := 0; i < edgeCount && i < len(r.Spans); i++ {
				printed[i] = true
				prefix := ""
				if isFlaggedSpan(r.Spans[i], unmatched) {
					prefix = "(flagged) "
				}
				headLines = append(headLines, fmt.Sprintf("\n  [%d] %s%s", i, prefix, formatSummaryLine(r.Spans[i])))
			}
			tailStart := len(r.Spans) - edgeCount
			if tailStart < edgeCount {
				tailStart = edgeCount
			}
			for i := tailStart; i < len(r.Spans); i++ {
				if !printed[i] {
					printed[i] = true
					prefix := ""
					if isFlaggedSpan(r.Spans[i], unmatched) {
						prefix = "(flagged) "
					}
					tailLines = append(tailLines, fmt.Sprintf("\n  [%d] %s%s", i, prefix, formatSummaryLine(r.Spans[i])))
				}
			}
			for i := edgeCount; i < tailStart; i++ {
				s := r.Spans[i]
				if isFlaggedSpan(s, unmatched) && !printed[i] {
					printed[i] = true
					midLines = append(midLines, fmt.Sprintf("\n  [%d] (flagged) %s", i, formatSummaryLine(s)))
				}
			}
			for _, ln := range headLines {
				b.WriteString(ln)
			}
			for _, ln := range midLines {
				b.WriteString(ln)
			}
			omitted := len(r.Spans) - len(printed)
			if omitted > 0 {
				fmt.Fprintf(&b, "\n  ... (%d spans omitted; showing first %d, flagged, and last %d of %d) ...", omitted, edgeCount, edgeCount, len(r.Spans))
			}
			for _, ln := range tailLines {
				b.WriteString(ln)
			}
		}
	}

	return b.String()
}

type flaggedSpanSet struct {
	byKey    map[SpanKey]bool
	bySpanID map[string]bool
}

func unmatchedSpanIDs(failures []Violation) flaggedSpanSet {
	s := flaggedSpanSet{
		byKey:    make(map[SpanKey]bool),
		bySpanID: make(map[string]bool),
	}
	for _, f := range failures {
		if f.SpanID == "" {
			continue
		}
		if f.TraceID != "" {
			s.byKey[SpanKey{TraceID: NormalizeHex(f.TraceID), SpanID: NormalizeHex(f.SpanID)}] = true
		} else {
			s.bySpanID[NormalizeHex(f.SpanID)] = true
		}
	}
	return s
}

func isFlaggedSpan(sp SpanSummary, flagged flaggedSpanSet) bool {
	sid := NormalizeHex(sp.SpanID)
	if sid == "" {
		return false
	}
	if sp.TraceID != "" && flagged.byKey[SpanKey{TraceID: NormalizeHex(sp.TraceID), SpanID: sid}] {
		return true
	}
	return flagged.bySpanID[sid]
}

// SummarizeSpan converts a TraceSpan into a deterministic SpanSummary.
func SummarizeSpan(s *proxypb.TraceSpan) SpanSummary {
	if s == nil {
		return SpanSummary{}
	}
	parent := s.GetParentSpanId()
	links := make([]string, 0, len(s.GetLinks()))
	for _, l := range s.GetLinks() {
		if l == nil {
			continue
		}
		t := l.GetTraceId()
		if len(t) > 8 {
			t = t[:8]
		}
		links = append(links, fmt.Sprintf("%s/%s", t, l.GetSpanId()))
	}
	events := make([]string, 0, len(s.GetEvents()))
	for _, ev := range s.GetEvents() {
		if ev != nil {
			events = append(events, ev.GetName())
		}
	}
	attrKeys := make([]string, 0, len(s.GetAttributes()))
	for k := range s.GetAttributes() {
		attrKeys = append(attrKeys, k)
	}
	sort.Strings(attrKeys)

	return SpanSummary{
		TraceID:      s.GetTraceId(),
		SpanID:       s.GetSpanId(),
		ParentSpanID: parent,
		Name:         s.GetName(),
		Kind:         s.GetKind().String(),
		Scope:        s.GetInstrumentationScopeName(),
		StatusCode:   s.GetStatusCode().String(),
		StatusMsg:    s.GetStatusMessage(),
		LinkTargets:  links,
		Events:       events,
		AttrKeys:     attrKeys,
	}
}

// FormatSpanSummary renders a single TraceSpan in compact single-line diagnostic form.
func FormatSpanSummary(s *proxypb.TraceSpan) string {
	return formatSummaryLine(SummarizeSpan(s))
}

func formatSummaryLine(s SpanSummary) string {
	traceShort := s.TraceID
	if len(traceShort) > 8 {
		traceShort = traceShort[:8]
	}
	parent := s.ParentSpanID
	if parent == "" {
		parent = "ROOT"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "trace=%s span=%s parent=%s kind=%s name=%q status=%s",
		traceShort, s.SpanID, parent, s.Kind, s.Name, s.StatusCode)
	if s.StatusMsg != "" {
		fmt.Fprintf(&b, "(%q)", s.StatusMsg)
	}
	if s.Scope != "" {
		fmt.Fprintf(&b, " scope=%q", s.Scope)
	}
	if len(s.AttrKeys) > 0 {
		fmt.Fprintf(&b, " attrs=%v", s.AttrKeys)
	}
	if len(s.Events) > 0 {
		fmt.Fprintf(&b, " events=%v", s.Events)
	}
	if len(s.LinkTargets) > 0 {
		fmt.Fprintf(&b, " links=%v", s.LinkTargets)
	}
	return b.String()
}
