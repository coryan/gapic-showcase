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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
)

func TestDiagnostics_FormattingAndJSONAndTruncation(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
	spans, exp := buildCppStreamingE2EFixture(true, true, false)
	// Break one attribute on create so we get both Failures and deduplicated Deviations.
	delete(spans[0].Attributes, "messaging.message.id")

	res := Verify(cppCaps, spans, exp)
	if res.Status != StatusFail || res.Err() == nil {
		t.Fatalf("expected FAIL + non-nil Err(), got %+v", res)
	}

	report := res.Format()
	for _, want := range []string{
		"Status=FAIL",
		"Profile=cpp",
		string(CodeMissingAttribute),
		"Deviations:",
		"Captured Spans:",
		"scope.instrumentation_name",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report missing %q:\n%s", want, report)
		}
	}

	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("json.Marshal(TraceResult) error = %v", err)
	}
	var roundTrip TraceResult
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatalf("json.Unmarshal(TraceResult) error = %v", err)
	}
	if roundTrip.Status != res.Status || len(roundTrip.Failures) != len(res.Failures) || len(roundTrip.Deviations) != len(res.Deviations) {
		t.Fatalf("JSON round-trip mismatch: got %+v, want %+v", roundTrip, res)
	}

	t.Run("truncates >200 spans deterministically while keeping flagged spans and exact omitted count", func(t *testing.T) {
		var many []*proxypb.TraceSpan
		for i := 1; i <= 260; i++ {
			sp := goSpan(testTraceID(i), testSpanID(i), "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(i)*time.Millisecond)
			if i == 130 || i == 255 {
				sp.ParentSpanId = "0000000000000000" // flagged violations in middle and tail
			}
			many = append(many, sp)
		}
		resMany := Verify(&proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}, many, nil)
		if resMany.Status != StatusFail {
			t.Fatalf("expected FAIL for invalid parent span id, got %+v", resMany)
		}
		out := resMany.Format()
		if !strings.Contains(out, "209 spans omitted; showing first 25, flagged, and last 25 of 260") {
			t.Fatalf("expected exact 209 omitted count in report:\n%s", out)
		}
		if !strings.Contains(out, "(flagged)") {
			t.Fatalf("expected flagged span marker for middle span in truncated report:\n%s", out)
		}
		idxMid := strings.Index(out, "[129] (flagged)")
		idxEllipsis := strings.Index(out, "209 spans omitted")
		idxTail := strings.Index(out, "[254] (flagged)")
		if idxMid < 0 || idxEllipsis < 0 || idxTail < 0 || !(idxMid < idxEllipsis && idxEllipsis < idxTail) {
			t.Fatalf("expected middle flagged -> ellipsis -> tail flagged order in report:\n%s", out)
		}
	})

	t.Run("all-flagged >200 spans omits 0 spans omitted banner and shows all indices", func(t *testing.T) {
		var allBad []*proxypb.TraceSpan
		for i := 1; i <= 205; i++ {
			sp := goSpan(testTraceID(i), testSpanID(i), "0000000000000000", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(i)*time.Millisecond)
			allBad = append(allBad, sp)
		}
		resAll := Verify(&proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}, allBad, nil)
		if resAll.Status != StatusFail {
			t.Fatalf("expected FAIL for all invalid parent span ids, got %+v", resAll)
		}
		out := resAll.Format()
		if strings.Contains(out, "0 spans omitted") || strings.Contains(out, "spans omitted") {
			t.Fatalf("unexpected omitted spans banner when every span was printed:\n%s", out)
		}
		for i := 0; i < 205; i++ {
			idxTag := fmt.Sprintf("[%d]", i)
			if !strings.Contains(out, idxTag) {
				t.Fatalf("missing span index %s in report:\n%s", idxTag, out)
			}
		}
	})

	t.Run("FormatSpanSummary covers populated and nil spans", func(t *testing.T) {
		if got := FormatSpanSummary(nil); got == "" {
			t.Fatal("FormatSpanSummary(nil) returned empty string")
		}
		sum := FormatSpanSummary(spans[0])
		if !strings.Contains(sum, `name="t create"`) || !strings.Contains(sum, `scope="gl-cpp"`) {
			t.Fatalf("unexpected FormatSpanSummary output: %s", sum)
		}
	})

	t.Run("GAP-A7-T12_FormatReport_Nil_SortSpanName_EmptyCode_And_SpanIDOnlyFlagging", func(t *testing.T) {
		if got := FormatReport(nil); got != "<nil TraceResult>" {
			t.Fatalf("FormatReport(nil) = %q, want %q", got, "<nil TraceResult>")
		}

		// Same Violation.Code tie-breaks deterministically by SpanName, and empty Code renders "VIOLATION".
		res := &TraceResult{
			Status:             StatusFail,
			SupportsMonitoring: true,
			Profile:            ProfileGo,
			Failures: []Violation{
				{Code: CodeMissingAttribute, Rule: "r1", SpanName: "z-span", Message: "second"},
				{Code: CodeMissingAttribute, Rule: "r2", SpanName: "a-span", Message: "first"},
				{Code: "", Rule: "r-empty", Message: "no-code"},
			},
		}
		out := FormatReport(res)
		idxA := strings.Index(out, `span="a-span"`)
		idxZ := strings.Index(out, `span="z-span"`)
		if idxA < 0 || idxZ < 0 || idxA > idxZ {
			t.Fatalf("expected a-span before z-span on equal Code tie-break, got:\n%s", out)
		}
		if !strings.Contains(out, "VIOLATION (rule=r-empty): no-code") {
			t.Fatalf("expected default VIOLATION label for empty Code, got:\n%s", out)
		}

		// >200 spans with a Failure carrying SpanID != "" and TraceID == "" flags the middle span.
		many := make([]SpanSummary, 210)
		for i := range many {
			many[i] = SpanSummary{
				TraceID: testTraceID(i + 1),
				SpanID:  testSpanID(i + 1),
				Name:    "t create",
				Kind:    "SPAN_KIND_PRODUCER",
			}
		}
		resFlag := &TraceResult{
			Status:             StatusFail,
			SupportsMonitoring: true,
			Profile:            ProfileGo,
			SpanCount:          len(many),
			Spans:              many,
			Failures: []Violation{
				{
					Code:    CodeMissingSpan,
					Rule:    "custom.span_id_only",
					TraceID: "",
					SpanID:  many[100].SpanID,
					Message: "flagged by span_id only",
				},
			},
		}
		outFlag := FormatReport(resFlag)
		if !strings.Contains(outFlag, "[100] (flagged)") {
			t.Fatalf("expected middle span [100] to be flagged by SpanID alone when TraceID is empty:\n%s", outFlag)
		}
	})
}
