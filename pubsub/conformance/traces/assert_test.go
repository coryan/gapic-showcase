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

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
)

func strAV(s string) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringValue{StringValue: s}}
}

func intAV(i int64) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: i}}
}

func boolAV(b bool) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: b}}
}

func dblAV(d float64) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_DoubleValue{DoubleValue: d}}
}

func strListAV(vals ...string) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringList{StringList: &proxypb.StringList{Values: vals}}}
}

func intListAV(vals ...int64) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64List{Int64List: &proxypb.Int64List{Values: vals}}}
}

func dblListAV(vals ...float64) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_DoubleList{DoubleList: &proxypb.DoubleList{Values: vals}}}
}

func boolListAV(vals ...bool) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolList{BoolList: &proxypb.BoolList{Values: vals}}}
}

func TestLowLevelAssertions(t *testing.T) {
	tid1 := "11111111111111111111111111111111"
	tid2 := "22222222222222222222222222222222"
	sid1 := "1111111111111111"
	sid2 := "2222222222222222"
	sid3 := "3333333333333333"

	root := &proxypb.TraceSpan{
		TraceId:    tid1,
		SpanId:     sid1,
		TraceFlags: 1,
		Name:       "t create",
		Kind:       proxypb.SpanKind_SPAN_KIND_PRODUCER,
		Attributes: map[string]*proxypb.AttributeValue{
			"messaging.message.body.size": intAV(12),
			"messaging.system":            strAV("gcp_pubsub"),
		},
		Events: []*proxypb.SpanEvent{
			{
				Name: "publish start",
				Attributes: map[string]*proxypb.AttributeValue{
					"messaging.batch.message_count": intAV(1),
				},
			},
			{
				Name: "publish start",
				Attributes: map[string]*proxypb.AttributeValue{
					"messaging.batch.message_count": intAV(5),
				},
			},
		},
		Links: []*proxypb.SpanLink{
			{
				TraceId: tid2,
				SpanId:  sid2,
				Attributes: map[string]*proxypb.AttributeValue{
					"messaging.operation.name": strAV("publish"),
				},
			},
		},
	}

	child := &proxypb.TraceSpan{
		TraceId:      tid1,
		SpanId:       sid3,
		ParentSpanId: sid1,
		Name:         "publisher batching",
		Kind:         proxypb.SpanKind_SPAN_KIND_INTERNAL,
	}

	pub := &proxypb.TraceSpan{
		TraceId:       tid2,
		SpanId:        sid2,
		Name:          "t publish",
		Kind:          proxypb.SpanKind_SPAN_KIND_INTERNAL,
		StatusCode:    proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
		StatusMessage: "boom: deadline exceeded",
		Links: []*proxypb.SpanLink{
			{TraceId: tid1, SpanId: sid1},
		},
	}

	if err := AssertRootSpan(root); err != nil {
		t.Fatalf("AssertRootSpan(root) = %v", err)
	}
	if err := AssertRootSpan(child); err == nil {
		t.Fatal("AssertRootSpan(child) unexpectedly passed")
	}
	if err := AssertRootSpan(&proxypb.TraceSpan{TraceId: tid1, SpanId: sid1, ParentSpanId: "0000000000000000"}); err == nil {
		t.Fatal("AssertRootSpan with all-zero parent unexpectedly passed")
	}
	if err := AssertChildOf(child, root); err != nil {
		t.Fatalf("AssertChildOf = %v", err)
	}
	if err := AssertChildOf(pub, root); err == nil {
		t.Fatal("AssertChildOf(pub, root) unexpectedly passed")
	}
	if err := AssertSpanKind(root, proxypb.SpanKind_SPAN_KIND_PRODUCER); err != nil {
		t.Fatalf("AssertSpanKind = %v", err)
	}
	if err := AssertSpanKind(root, proxypb.SpanKind_SPAN_KIND_CLIENT); err == nil {
		t.Fatal("AssertSpanKind mismatch unexpectedly passed")
	}

	// Bidirectional link + link attribute check (including WithLinkAttribute alias & duplicate link rejection).
	if err := AssertBidirectionalLink(root, pub, WithForwardLinkAttribute("messaging.operation.name", MatchString("publish"))); err != nil {
		t.Fatalf("AssertBidirectionalLink = %v", err)
	}
	if err := AssertBidirectionalLink(root, pub, WithLinkAttribute("messaging.operation.name", MatchString("publish"))); err != nil {
		t.Fatalf("AssertBidirectionalLink(WithLinkAttribute) = %v", err)
	}
	if err := AssertBidirectionalLink(root, pub, WithReverseLinkAttribute("messaging.operation.name", MatchString("publish"))); err == nil {
		t.Fatal("AssertBidirectionalLink unexpectedly passed with missing reverse attr")
	}
	if err := AssertBidirectionalLink(root, child); err == nil {
		t.Fatal("AssertBidirectionalLink unexpectedly passed when no links exist")
	}
	dupRoot := &proxypb.TraceSpan{
		TraceId: tid1,
		SpanId:  sid1,
		Name:    "t create",
		Links:   []*proxypb.SpanLink{root.Links[0], root.Links[0]},
	}
	if err := AssertBidirectionalLink(dupRoot, pub); err == nil {
		t.Fatal("AssertBidirectionalLink unexpectedly passed with duplicate forward links")
	}
	dupPub := &proxypb.TraceSpan{
		TraceId: tid2,
		SpanId:  sid2,
		Name:    "t publish",
		Links:   []*proxypb.SpanLink{pub.Links[0], pub.Links[0]},
	}
	if err := AssertBidirectionalLink(root, dupPub); err == nil {
		t.Fatal("AssertBidirectionalLink unexpectedly passed with duplicate reverse links")
	}

	// Type-safe attribute checks.
	if err := AssertSpanAttribute(root, "messaging.message.body.size", MatchInt64(12)); err != nil {
		t.Fatalf("AssertSpanAttribute = %v", err)
	}
	if err := AssertSpanAttribute(&proxypb.TraceSpan{
		Name:       "wrong-type",
		Attributes: map[string]*proxypb.AttributeValue{"messaging.message.body.size": strAV("12")},
	}, "messaging.message.body.size", MatchInt64(12)); err == nil {
		t.Fatal("AssertSpanAttribute unexpectedly accepted StringValue for MatchInt64")
	}
	if err := AssertAttributeAbsent(root, "nonexistent"); err != nil {
		t.Fatalf("AssertAttributeAbsent = %v", err)
	}
	if err := AssertAttributeAbsent(root, "messaging.system"); err == nil {
		t.Fatal("AssertAttributeAbsent unexpectedly passed for present attr")
	}

	// Event checks across repeated events.
	if err := AssertEvent(root, "publish start", CheckAttr("messaging.batch.message_count", MatchInt64(5))); err != nil {
		t.Fatalf("AssertEvent = %v", err)
	}
	if err := AssertEvent(root, "publish start", CheckAttr("messaging.batch.message_count", MatchInt64(99))); err == nil {
		t.Fatal("AssertEvent unexpectedly passed on mismatched batch count")
	}
	if err := AssertEvent(root, "publish end"); err == nil {
		t.Fatal("AssertEvent unexpectedly passed for missing event")
	}

	// Status & traceparent checks.
	if err := AssertStatus(pub, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "deadline exceeded"); err != nil {
		t.Fatalf("AssertStatus = %v", err)
	}
	if err := AssertStatus(pub, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "missing-substr"); err == nil {
		t.Fatal("AssertStatus unexpectedly passed with non-matching substring")
	}

	tp := "00-" + tid1 + "-" + sid1 + "-01"
	if err := AssertTraceparentMatchesSpan(tp, root); err != nil {
		t.Fatalf("AssertTraceparentMatchesSpan = %v", err)
	}
	unsampTP := "00-" + tid1 + "-" + sid1 + "-00"
	if err := AssertTraceparentMatchesSpan(unsampTP, root); err == nil {
		t.Fatal("AssertTraceparentMatchesSpan unexpectedly accepted -00 traceparent for TraceFlags=1 span")
	}
	if err := AssertParentFromTraceparent(child, tp); err != nil {
		t.Fatalf("AssertParentFromTraceparent = %v", err)
	}
	if err := AssertParentFromTraceparent(pub, tp); err == nil {
		t.Fatal("AssertParentFromTraceparent(pub, tp) unexpectedly passed")
	}

	// Exhaustive negative coverage for AssertValidSpanIDs, AssertChildOf, AssertSpanKind,
	// AssertBidirectionalLink, AssertStatus, AssertParentFromTraceparent, AssertTraceparentMatchesSpan.
	badSpan := &proxypb.TraceSpan{TraceId: "bad", SpanId: "bad", ParentSpanId: "bad"}
	for _, err := range []error{
		AssertValidSpanIDs(nil),
		AssertValidSpanIDs(&proxypb.TraceSpan{TraceId: "bad"}),
		AssertValidSpanIDs(&proxypb.TraceSpan{TraceId: tid1, SpanId: "bad"}),
		AssertValidSpanIDs(&proxypb.TraceSpan{TraceId: tid1, SpanId: sid1, ParentSpanId: "bad"}),
		AssertChildOf(child, badSpan),
		AssertChildOf(badSpan, root),
		AssertChildOf(&proxypb.TraceSpan{TraceId: tid1, SpanId: sid3, ParentSpanId: sid2}, root),
		AssertSpanKind(nil, proxypb.SpanKind_SPAN_KIND_INTERNAL),
		AssertBidirectionalLink(badSpan, pub),
		AssertBidirectionalLink(root, badSpan),
		AssertBidirectionalLink(root, pub, WithForwardLinkAttribute("missing", MatchString("x"))),
		AssertBidirectionalLink(&proxypb.TraceSpan{TraceId: tid1, SpanId: sid1, Links: []*proxypb.SpanLink{{TraceId: tid2, SpanId: sid2}}}, &proxypb.TraceSpan{TraceId: tid2, SpanId: sid2}),
		AssertAttribute(nil, "k", nil),
		AssertSpanAttribute(nil, "k", nil),
		AssertAttributeAbsent(nil, "k"),
		AssertEvent(nil, "e"),
		AssertStatus(nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, ""),
		AssertStatus(pub, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, ""),
		AssertParentFromTraceparent(badSpan, tp),
		AssertParentFromTraceparent(child, "bad-tp"),
		AssertTraceparentMatchesSpan(tp, nil),
		AssertTraceparentMatchesSpan("bad-tp", root),
		AssertTraceparentMatchesSpan("00-"+tid2+"-"+sid1+"-01", root),
		AssertTraceparentMatchesSpan("00-"+tid1+"-"+sid2+"-01", root),
	} {
		if err == nil {
			t.Errorf("expected non-nil error for negative assertion check")
		}
	}
}

func TestAttrMatchers_AllConstructors(t *testing.T) {
	attrs := map[string]*proxypb.AttributeValue{
		"s":  strAV("hello world"),
		"i":  intAV(7),
		"d":  dblAV(2.5),
		"b":  boolAV(true),
		"sl": strListAV("x", "y"),
		"il": intListAV(10, 20),
		"dl": dblListAV(1.5, 2.5),
		"bl": boolListAV(true, false),
	}

	checks := []struct {
		key     string
		matcher AttrMatcher
		wantOK  bool
	}{
		{"s", MatchPresent(), true},
		{"s", MatchStringContains("world"), true},
		{"s", MatchStringContains("nope"), false},
		{"s", MatchStringOneOf("a", "hello world"), true},
		{"s", MatchStringOneOf("a", "b"), false},
		{"i", MatchStringOneOf("7", "hello world"), false},
		{"s", MatchAnyString(), true},
		{"i", MatchAnyString(), false},
		{"d", MatchDouble(2.5), true},
		{"d", MatchDouble(9.9), false},
		{"sl", MatchStringList([]string{"x", "y"}), true},
		{"sl", MatchStringList([]string{"x"}), false},
		{"il", MatchInt64List([]int64{10, 20}), true},
		{"il", MatchInt64List([]int64{10}), false},
		{"dl", MatchDoubleList([]float64{1.5, 2.5}), true},
		{"dl", MatchDoubleList([]float64{1.5}), false},
		{"bl", MatchBoolList([]bool{true, false}), true},
		{"bl", MatchBoolList([]bool{false}), false},
	}

	for _, tc := range checks {
		err := AssertAttribute(attrs, tc.key, tc.matcher)
		if tc.wantOK && err != nil {
			t.Errorf("AssertAttribute(%q) unexpected error: %v", tc.key, err)
		}
		if !tc.wantOK && err == nil {
			t.Errorf("AssertAttribute(%q) unexpectedly succeeded", tc.key)
		}
	}
}
