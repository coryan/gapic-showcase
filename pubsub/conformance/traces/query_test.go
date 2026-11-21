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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParseTraceparent(t *testing.T) {
	validTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	validSpanID := "00f067aa0ba902b7"

	t.Run("valid sampled and unsampled", func(t *testing.T) {
		tests := []struct {
			raw         string
			wantSampled bool
			wantFlags   byte
		}{
			{"00-" + validTraceID + "-" + validSpanID + "-01", true, 0x01},
			{"00-" + validTraceID + "-" + validSpanID + "-00", false, 0x00},
			{"00-" + validTraceID + "-" + validSpanID + "-02", false, 0x02},
			{"00-" + validTraceID + "-" + validSpanID + "-03", true, 0x03},
		}
		for _, tc := range tests {
			tp, err := ParseTraceparent(tc.raw)
			if err != nil {
				t.Fatalf("ParseTraceparent(%q) unexpected error: %v", tc.raw, err)
			}
			if tp.Version != 0 || tp.TraceID != validTraceID || tp.SpanID != validSpanID || tp.TraceFlags != tc.wantFlags {
				t.Fatalf("ParseTraceparent(%q) = %+v", tc.raw, tp)
			}
			if tp.IsSampled() != tc.wantSampled {
				t.Fatalf("ParseTraceparent(%q).IsSampled() = %v, want %v", tc.raw, tp.IsSampled(), tc.wantSampled)
			}
			if tp.String() != tc.raw {
				t.Fatalf("tp.String() = %q, want %q", tp.String(), tc.raw)
			}
		}
	})

	t.Run("invalid formats rejected", func(t *testing.T) {
		invalids := []string{
			"",
			"00-" + validTraceID + "-" + validSpanID,
			"00-" + validTraceID + "-" + validSpanID + "-01-extra",
			" 00-" + validTraceID + "-" + validSpanID + "-01",
			"00-" + validTraceID + "-" + validSpanID + "-01 ",
			"00-4BF92F3577B34DA6A3CE929D0E0E4736-" + validSpanID + "-01",
			"00-" + validTraceID + "-00F067AA0BA902B7-01",
			"01-" + validTraceID + "-" + validSpanID + "-01",
			"ff-" + validTraceID + "-" + validSpanID + "-01",
			"00-00000000000000000000000000000000-" + validSpanID + "-01",
			"00-" + validTraceID + "-0000000000000000-01",
			"00-" + validTraceID + "-" + validSpanID + "-zz",
			"00-" + validTraceID + "-" + validSpanID + "-04",
			"00-" + validTraceID + "-" + validSpanID + "-ff",
			"00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-" + validSpanID + "-01",
			"00-" + validTraceID + "-1234-01",
			"00-" + validTraceID + "-" + validSpanID + "01",
			"00-" + validTraceID + "-" + validSpanID + "-0-1",
			"0g-" + validTraceID + "-" + validSpanID + "-01",
			"00-" + validTraceID + "-" + validSpanID + "-0g",
		}
		for _, raw := range invalids {
			if _, err := ParseTraceparent(raw); err == nil {
				t.Errorf("ParseTraceparent(%q) succeeded, expected error", raw)
			}
		}
	})
}

func TestNormalizeAndExtractHelpers(t *testing.T) {
	if got := NormalizeHex("  0123ABCD "); got != "0123abcd" {
		t.Fatalf("NormalizeHex = %q, want %q", got, "0123abcd")
	}

	if tp, ok := ExtractTraceparent(nil); ok || tp != "" {
		t.Fatalf("ExtractTraceparent(nil) = (%q, %v)", tp, ok)
	}
	if tp, ok := ExtractTraceparent(map[string]string{"googclient_traceparent": ""}); ok || tp != "" {
		t.Fatalf("ExtractTraceparent(empty) = (%q, %v)", tp, ok)
	}
	wantTP := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if tp, ok := ExtractTraceparent(map[string]string{"googclient_traceparent": wantTP}); !ok || tp != wantTP {
		t.Fatalf("ExtractTraceparent = (%q, %v), want (%q, true)", tp, ok, wantTP)
	}

	p, s, f := ExtractResourceIDs("projects/my-proj/topics/my-topic", "topics", "")
	if p != "my-proj" || s != "my-topic" || f != "projects/my-proj/topics/my-topic" {
		t.Fatalf("ExtractResourceIDs full = (%q, %q, %q)", p, s, f)
	}
	p2, s2, f2 := ExtractResourceIDs("sub-short", "subscriptions", "override-proj")
	if p2 != "override-proj" || s2 != "sub-short" || f2 != "projects/override-proj/subscriptions/sub-short" {
		t.Fatalf("ExtractResourceIDs short = (%q, %q, %q)", p2, s2, f2)
	}
	p3, s3, f3 := ExtractResourceIDs("short-only", "topics", "")
	if p3 != "" || s3 != "short-only" || f3 != "" {
		t.Fatalf("ExtractResourceIDs short without project = (%q, %q, %q), want (\"\", \"short-only\", \"\")", p3, s3, f3)
	}
}

func TestBuildSpanIndexAndQueries(t *testing.T) {
	tid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	parent := &proxypb.TraceSpan{
		TraceId:        tid,
		SpanId:         "1111111111111111",
		Name:           "sub subscribe",
		Kind:           proxypb.SpanKind_SPAN_KIND_INTERNAL,
		ChildSpanCount: 0, // Intentionally 0 as in Go pubsub/v2 ContextWithSpanContext.
	}
	child := &proxypb.TraceSpan{
		TraceId:      tid,
		SpanId:       "2222222222222222",
		ParentSpanId: "1111111111111111",
		Name:         "sub process",
		Kind:         proxypb.SpanKind_SPAN_KIND_CONSUMER,
	}

	idx, violations := BuildSpanIndex([]*proxypb.TraceSpan{parent, child})
	if len(violations) != 0 {
		t.Fatalf("unexpected structural violations: %+v", violations)
	}
	children := idx.ChildrenOf(parent)
	if diff := cmp.Diff([]*proxypb.TraceSpan{child}, children, protocmp.Transform()); diff != "" {
		t.Fatalf("ChildrenOf mismatch (-want +got):\n%s", diff)
	}
	if got := FindChildSpans([]*proxypb.TraceSpan{parent, child}, parent); len(got) != 1 || got[0] != child {
		t.Fatalf("FindChildSpans = %v, want [child]", got)
	}
	if got := FindSpansByName([]*proxypb.TraceSpan{parent, child}, "sub process"); len(got) != 1 {
		t.Fatalf("FindSpansByName = %v", got)
	}
	if got := FindSpansByNameAndKind([]*proxypb.TraceSpan{parent, child}, "sub process", proxypb.SpanKind_SPAN_KIND_CONSUMER); len(got) != 1 {
		t.Fatalf("FindSpansByNameAndKind = %v", got)
	}

	t.Run("structural errors detected", func(t *testing.T) {
		now := time.Now()
		badSpans := []*proxypb.TraceSpan{
			{
				TraceId:      "00000000000000000000000000000000",
				SpanId:       "0000000000000000",
				ParentSpanId: "0000000000000000",
				Name:         "bad-root",
				StartTime:    timestamppb.New(now),
				EndTime:      timestamppb.New(now.Add(-time.Second)),
				Links: []*proxypb.SpanLink{
					{TraceId: "bad", SpanId: "0000000000000000"},
				},
			},
			{
				TraceId: tid,
				SpanId:  "1111111111111111",
				Name:    "dup-1",
			},
			{
				TraceId: tid,
				SpanId:  "1111111111111111",
				Name:    "dup-2",
			},
		}
		_, v := BuildSpanIndex(badSpans)
		gotCodes := make(map[ViolationCode]bool)
		for _, item := range v {
			gotCodes[item.Code] = true
		}
		for _, wantCode := range []ViolationCode{
			CodeInvalidTraceID,
			CodeInvalidSpanID,
			CodeInvalidParentSpanID,
			CodeInvalidTimestamp,
			CodeDuplicateSpanID,
		} {
			if !gotCodes[wantCode] {
				t.Errorf("expected violation code %s in %+v", wantCode, v)
			}
		}

		// Invalid non-nil timestamps (Nanos out of range) & invalid SpanId skipped from idx.ByKey.
		badTS := &proxypb.TraceSpan{
			TraceId:   tid,
			SpanId:    "0000000000000000",
			Name:      "bad-ts",
			StartTime: &timestamppb.Timestamp{Seconds: 100, Nanos: 2_000_000_000},
			EndTime:   &timestamppb.Timestamp{Seconds: 100, Nanos: 2_000_000_000},
		}
		badIdx, badV := BuildSpanIndex([]*proxypb.TraceSpan{badTS})
		if _, inMap := badIdx.ByKey[SpanKey{TraceID: tid, SpanID: "0000000000000000"}]; inMap {
			t.Fatal("expected invalid SpanId to be omitted from idx.ByKey")
		}
		tsViolations := 0
		for _, item := range badV {
			if item.Code == CodeInvalidTimestamp {
				tsViolations++
			}
		}
		if tsViolations < 2 {
			t.Fatalf("expected invalid start_time and end_time timestamp violations, got %+v", badV)
		}
	})

	t.Run("self-parent span rejected", func(t *testing.T) {
		selfParent := &proxypb.TraceSpan{
			TraceId:      "0123456789abcdef0123456789abcdef",
			SpanId:       "1111111111111111",
			ParentSpanId: "1111111111111111",
			Name:         "self-loop",
		}
		_, v := BuildSpanIndex([]*proxypb.TraceSpan{selfParent})
		found := false
		for _, item := range v {
			if item.Code == CodeWrongParent && item.Rule == "span.non_self_parent" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected CodeWrongParent / span.non_self_parent violation, got %+v", v)
		}
		caps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
		if res := Verify(caps, []*proxypb.TraceSpan{selfParent}, nil); res.Passed() {
			t.Fatalf("expected Verify to reject self-parented span, got %+v", res)
		}
		if idx.ChildrenOf(nil) != nil || (*SpanIndex)(nil).ChildrenOf(selfParent) != nil {
			t.Fatalf("ChildrenOf nil guard mismatch")
		}
		if FindChildSpans([]*proxypb.TraceSpan{selfParent}, nil) != nil || FindSpans([]*proxypb.TraceSpan{selfParent}, nil) != nil {
			t.Fatalf("Find* nil guard mismatch")
		}
	})
}

func TestTypedAttributeAccessors(t *testing.T) {
	attrs := map[string]*proxypb.AttributeValue{
		"s":  {Value: &proxypb.AttributeValue_StringValue{StringValue: "10"}},
		"i":  {Value: &proxypb.AttributeValue_Int64Value{Int64Value: 42}},
		"d":  {Value: &proxypb.AttributeValue_DoubleValue{DoubleValue: 3.5}},
		"b":  {Value: &proxypb.AttributeValue_BoolValue{BoolValue: true}},
		"sl": {Value: &proxypb.AttributeValue_StringList{StringList: &proxypb.StringList{Values: []string{"a", "b"}}}},
		"il": {Value: &proxypb.AttributeValue_Int64List{Int64List: &proxypb.Int64List{Values: []int64{1, 2}}}},
		"dl": {Value: &proxypb.AttributeValue_DoubleList{DoubleList: &proxypb.DoubleList{Values: []float64{1.25}}}},
		"bl": {Value: &proxypb.AttributeValue_BoolList{BoolList: &proxypb.BoolList{Values: []bool{true, false}}}},
	}

	if v, ok := AttrString(attrs, "s"); !ok || v != "10" {
		t.Fatalf("AttrString(s) = (%q, %v)", v, ok)
	}
	if _, ok := AttrInt64(attrs, "s"); ok {
		t.Fatal("AttrInt64(s) unexpectedly succeeded for StringValue")
	}
	if v, ok := AttrInt64(attrs, "i"); !ok || v != 42 {
		t.Fatalf("AttrInt64(i) = (%d, %v)", v, ok)
	}
	if _, ok := AttrString(attrs, "i"); ok {
		t.Fatal("AttrString(i) unexpectedly succeeded for Int64Value")
	}
	if v, ok := AttrDouble(attrs, "d"); !ok || v != 3.5 {
		t.Fatalf("AttrDouble(d) = (%v, %v)", v, ok)
	}
	if v, ok := AttrBool(attrs, "b"); !ok || !v {
		t.Fatalf("AttrBool(b) = (%v, %v)", v, ok)
	}
	if v, ok := AttrStringList(attrs, "sl"); !ok || len(v) != 2 {
		t.Fatalf("AttrStringList(sl) = (%v, %v)", v, ok)
	}
	if v, ok := AttrInt64List(attrs, "il"); !ok || len(v) != 2 {
		t.Fatalf("AttrInt64List(il) = (%v, %v)", v, ok)
	}
	if v, ok := AttrDoubleList(attrs, "dl"); !ok || len(v) != 1 {
		t.Fatalf("AttrDoubleList(dl) = (%v, %v)", v, ok)
	}
	if v, ok := AttrBoolList(attrs, "bl"); !ok || len(v) != 2 {
		t.Fatalf("AttrBoolList(bl) = (%v, %v)", v, ok)
	}
	if got := FormatAttrValue(nil); got != "<unset>" {
		t.Fatalf("FormatAttrValue(nil) = %q", got)
	}
	for k, wantPrefix := range map[string]string{
		"s":  "string(",
		"i":  "int64(",
		"d":  "double(",
		"b":  "bool(",
		"sl": "string_list(",
		"il": "int64_list(",
		"dl": "double_list(",
		"bl": "bool_list(",
	} {
		got := FormatAttrValue(attrs[k])
		if len(got) < len(wantPrefix) || got[:len(wantPrefix)] != wantPrefix {
			t.Errorf("FormatAttrValue(%q) = %q, want prefix %q", k, got, wantPrefix)
		}
	}

	// Negative / missing / nil-inner / wrong-type cases for AttrDouble, AttrBool, Attr*List.
	nilInner := map[string]*proxypb.AttributeValue{
		"sl":  {Value: &proxypb.AttributeValue_StringList{}},
		"il":  {Value: &proxypb.AttributeValue_Int64List{}},
		"dl":  {Value: &proxypb.AttributeValue_DoubleList{}},
		"bl":  {Value: &proxypb.AttributeValue_BoolList{}},
		"nil": nil,
	}
	if _, ok := AttrDouble(nil, "d"); ok {
		t.Fatal("AttrDouble(nil) succeeded")
	}
	if _, ok := AttrDouble(attrs, "s"); ok {
		t.Fatal("AttrDouble(wrong-type) succeeded")
	}
	if _, ok := AttrDouble(nilInner, "nil"); ok {
		t.Fatal("AttrDouble(nil-av) succeeded")
	}
	if _, ok := AttrBool(nil, "b"); ok {
		t.Fatal("AttrBool(nil) succeeded")
	}
	if _, ok := AttrBool(attrs, "s"); ok {
		t.Fatal("AttrBool(wrong-type) succeeded")
	}
	if _, ok := AttrStringList(nil, "sl"); ok {
		t.Fatal("AttrStringList(nil) succeeded")
	}
	if _, ok := AttrStringList(nilInner, "sl"); ok {
		t.Fatal("AttrStringList(nil-inner) succeeded")
	}
	if _, ok := AttrStringList(attrs, "s"); ok {
		t.Fatal("AttrStringList(wrong-type) succeeded")
	}
	if _, ok := AttrInt64List(nil, "il"); ok {
		t.Fatal("AttrInt64List(nil) succeeded")
	}
	if _, ok := AttrInt64List(nilInner, "il"); ok {
		t.Fatal("AttrInt64List(nil-inner) succeeded")
	}
	if _, ok := AttrDoubleList(nil, "dl"); ok {
		t.Fatal("AttrDoubleList(nil) succeeded")
	}
	if _, ok := AttrDoubleList(nilInner, "dl"); ok {
		t.Fatal("AttrDoubleList(nil-inner) succeeded")
	}
	if _, ok := AttrBoolList(nil, "bl"); ok {
		t.Fatal("AttrBoolList(nil) succeeded")
	}
	if _, ok := AttrBoolList(nilInner, "bl"); ok {
		t.Fatal("AttrBoolList(nil-inner) succeeded")
	}
}

func TestFindChildSpans_RejectsInvalidAllZeroParentIDs(t *testing.T) {
	validTID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	validSID := "1111111111111111"
	zeroTID := "00000000000000000000000000000000"
	zeroSID := "0000000000000000"

	zeroParent := &proxypb.TraceSpan{TraceId: zeroTID, SpanId: zeroSID, Name: "zero-parent"}
	zeroOrphan := &proxypb.TraceSpan{TraceId: zeroTID, SpanId: validSID, ParentSpanId: zeroSID, Name: "zero-orphan"}
	badTraceParent := &proxypb.TraceSpan{TraceId: "bad", SpanId: validSID, Name: "bad-trace-parent"}
	badTraceOrphan := &proxypb.TraceSpan{TraceId: "bad", SpanId: "2222222222222222", ParentSpanId: validSID, Name: "bad-trace-orphan"}
	zeroSpanParent := &proxypb.TraceSpan{TraceId: validTID, SpanId: zeroSID, Name: "zero-span-parent"}
	zeroSpanOrphan := &proxypb.TraceSpan{TraceId: validTID, SpanId: "3333333333333333", ParentSpanId: zeroSID, Name: "zero-span-orphan"}

	spans := []*proxypb.TraceSpan{zeroParent, zeroOrphan, badTraceParent, badTraceOrphan, zeroSpanParent, zeroSpanOrphan}
	for name, parent := range map[string]*proxypb.TraceSpan{
		"all_zero_trace_and_span": zeroParent,
		"invalid_trace_id":        badTraceParent,
		"all_zero_span_id":        zeroSpanParent,
	} {
		if got := FindChildSpans(spans, parent); len(got) != 0 {
			t.Fatalf("%s: FindChildSpans returned %v, want empty", name, got)
		}
	}
}
