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
	"slices"
	"strings"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
)

// AttrMatcher matches a typed proxypb.AttributeValue.
type AttrMatcher interface {
	Match(av *proxypb.AttributeValue) (ok bool, expected string, actual string)
}

type attrMatcherFunc struct {
	expected string
	fn       func(av *proxypb.AttributeValue) bool
}

func (m attrMatcherFunc) Match(av *proxypb.AttributeValue) (bool, string, string) {
	if av == nil || av.GetValue() == nil {
		return false, m.expected, "<missing>"
	}
	if m.fn(av) {
		return true, m.expected, FormatAttrValue(av)
	}
	return false, m.expected, FormatAttrValue(av)
}

// MatchPresent matches any non-nil attribute with a populated oneof value.
func MatchPresent() AttrMatcher {
	return attrMatcherFunc{
		expected: "present attribute value",
		fn: func(av *proxypb.AttributeValue) bool {
			return av != nil && av.GetValue() != nil
		},
	}
}

// MatchString matches a StringValue exactly equal to want.
func MatchString(want string) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("string(%q)", want),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_StringValue)
			return ok && v.StringValue == want
		},
	}
}

// MatchStringNonEmpty matches a StringValue with non-empty content.
func MatchStringNonEmpty() AttrMatcher {
	return attrMatcherFunc{
		expected: "non-empty string",
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_StringValue)
			return ok && v.StringValue != ""
		},
	}
}

// MatchStringContains matches a StringValue containing substr.
func MatchStringContains(substr string) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("string containing %q", substr),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_StringValue)
			return ok && strings.Contains(v.StringValue, substr)
		},
	}
}

// MatchStringOneOf matches a StringValue equal to one of wants.
func MatchStringOneOf(wants ...string) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("string one of %q", wants),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_StringValue)
			if !ok {
				return false
			}
			for _, w := range wants {
				if v.StringValue == w {
					return true
				}
			}
			return false
		},
	}
}

// MatchAnyString matches any StringValue attribute (including "").
func MatchAnyString() AttrMatcher {
	return attrMatcherFunc{
		expected: "any string",
		fn: func(av *proxypb.AttributeValue) bool {
			_, ok := av.GetValue().(*proxypb.AttributeValue_StringValue)
			return ok
		},
	}
}

// MatchInt64 matches an Int64Value equal to want.
func MatchInt64(want int64) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("int64(%d)", want),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_Int64Value)
			return ok && v.Int64Value == want
		},
	}
}

// MatchInt64Min matches an Int64Value >= minVal.
func MatchInt64Min(minVal int64) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("int64(>= %d)", minVal),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_Int64Value)
			return ok && v.Int64Value >= minVal
		},
	}
}

// MatchAnyInt64 matches any Int64Value attribute.
func MatchAnyInt64() AttrMatcher {
	return attrMatcherFunc{
		expected: "any int64",
		fn: func(av *proxypb.AttributeValue) bool {
			_, ok := av.GetValue().(*proxypb.AttributeValue_Int64Value)
			return ok
		},
	}
}

// MatchDouble matches a DoubleValue equal to want.
func MatchDouble(want float64) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("double(%g)", want),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_DoubleValue)
			return ok && v.DoubleValue == want
		},
	}
}

// MatchBool matches a BoolValue equal to want.
func MatchBool(want bool) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("bool(%t)", want),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_BoolValue)
			return ok && v.BoolValue == want
		},
	}
}

// MatchAnyBool matches any BoolValue attribute.
func MatchAnyBool() AttrMatcher {
	return attrMatcherFunc{
		expected: "any bool",
		fn: func(av *proxypb.AttributeValue) bool {
			_, ok := av.GetValue().(*proxypb.AttributeValue_BoolValue)
			return ok
		},
	}
}

// MatchStringList matches a StringList attribute equal to want.
func MatchStringList(want []string) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("string_list(%v)", want),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_StringList)
			return ok && v.StringList != nil && slices.Equal(v.StringList.GetValues(), want)
		},
	}
}

// MatchInt64List matches an Int64List attribute equal to want.
func MatchInt64List(want []int64) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("int64_list(%v)", want),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_Int64List)
			return ok && v.Int64List != nil && slices.Equal(v.Int64List.GetValues(), want)
		},
	}
}

// MatchDoubleList matches a DoubleList attribute equal to want.
func MatchDoubleList(want []float64) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("double_list(%v)", want),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_DoubleList)
			return ok && v.DoubleList != nil && slices.Equal(v.DoubleList.GetValues(), want)
		},
	}
}

// MatchBoolList matches a BoolList attribute equal to want.
func MatchBoolList(want []bool) AttrMatcher {
	return attrMatcherFunc{
		expected: fmt.Sprintf("bool_list(%v)", want),
		fn: func(av *proxypb.AttributeValue) bool {
			v, ok := av.GetValue().(*proxypb.AttributeValue_BoolList)
			return ok && v.BoolList != nil && slices.Equal(v.BoolList.GetValues(), want)
		},
	}
}

// AttrCheck pairs an attribute key with a typed AttrMatcher for event/link assertions.
type AttrCheck struct {
	Key     string
	Matcher AttrMatcher
}

// CheckAttr creates an AttrCheck for key using matcher.
func CheckAttr(key string, matcher AttrMatcher) AttrCheck {
	return AttrCheck{Key: key, Matcher: matcher}
}

type linkCheckConfig struct {
	forwardAttrs []AttrCheck
	reverseAttrs []AttrCheck
}

// LinkOption configures attribute checks on AssertBidirectionalLink.
type LinkOption func(*linkCheckConfig)

// WithForwardLinkAttribute requires a link from span a -> b to contain key matching matcher.
func WithForwardLinkAttribute(key string, matcher AttrMatcher) LinkOption {
	return func(cfg *linkCheckConfig) {
		cfg.forwardAttrs = append(cfg.forwardAttrs, AttrCheck{Key: key, Matcher: matcher})
	}
}

// WithLinkAttribute requires the link from span a -> b to contain key matching matcher.
func WithLinkAttribute(key string, matcher AttrMatcher) LinkOption {
	return WithForwardLinkAttribute(key, matcher)
}

// WithReverseLinkAttribute requires the link from span b -> a to contain key matching matcher.
func WithReverseLinkAttribute(key string, matcher AttrMatcher) LinkOption {
	return func(cfg *linkCheckConfig) {
		cfg.reverseAttrs = append(cfg.reverseAttrs, AttrCheck{Key: key, Matcher: matcher})
	}
}

// AssertValidSpanIDs verifies that span is non-nil and has valid trace_id,
// span_id, and (if non-empty) parent_span_id.
func AssertValidSpanIDs(span *proxypb.TraceSpan) error {
	if span == nil {
		return fmt.Errorf("span is nil")
	}
	if !IsValidTraceID(span.GetTraceId()) {
		return fmt.Errorf("span %q has invalid trace_id %q", span.GetName(), span.GetTraceId())
	}
	if !IsValidSpanID(span.GetSpanId()) {
		return fmt.Errorf("span %q has invalid span_id %q", span.GetName(), span.GetSpanId())
	}
	if p := span.GetParentSpanId(); p != "" && !IsValidSpanID(p) {
		return fmt.Errorf("span %q has invalid parent_span_id %q", span.GetName(), p)
	}
	return nil
}

// AssertRootSpan verifies that span has an empty ParentSpanId and valid trace/span IDs.
func AssertRootSpan(span *proxypb.TraceSpan) error {
	if err := AssertValidSpanIDs(span); err != nil {
		return err
	}
	if p := span.GetParentSpanId(); p != "" {
		return fmt.Errorf("expected span %q (%s) to be a root span (empty parent_span_id), got %q", span.GetName(), span.GetSpanId(), p)
	}
	return nil
}

// AssertChildOf verifies that child shares parent's trace_id and has
// parent_span_id equal to parent's span_id.
func AssertChildOf(child, parent *proxypb.TraceSpan) error {
	if err := AssertValidSpanIDs(parent); err != nil {
		return fmt.Errorf("invalid parent span: %w", err)
	}
	if err := AssertValidSpanIDs(child); err != nil {
		return fmt.Errorf("invalid child span: %w", err)
	}
	if NormalizeHex(child.GetTraceId()) != NormalizeHex(parent.GetTraceId()) {
		return fmt.Errorf("expected child span %q trace_id %q to match parent %q trace_id %q",
			child.GetName(), child.GetTraceId(), parent.GetName(), parent.GetTraceId())
	}
	if NormalizeHex(child.GetParentSpanId()) != NormalizeHex(parent.GetSpanId()) {
		return fmt.Errorf("expected child span %q parent_span_id %q to match parent %q span_id %q",
			child.GetName(), child.GetParentSpanId(), parent.GetName(), parent.GetSpanId())
	}
	return nil
}

// AssertSpanKind verifies that span.Kind equals want.
func AssertSpanKind(span *proxypb.TraceSpan, want proxypb.SpanKind) error {
	if span == nil {
		return fmt.Errorf("span is nil (want kind %s)", want)
	}
	if span.GetKind() != want {
		return fmt.Errorf("span %q has kind %s, want %s", span.GetName(), span.GetKind(), want)
	}
	return nil
}

func findMatchingLinks(from, target *proxypb.TraceSpan) []*proxypb.SpanLink {
	if from == nil || target == nil {
		return nil
	}
	tt := NormalizeHex(target.GetTraceId())
	ts := NormalizeHex(target.GetSpanId())
	var out []*proxypb.SpanLink
	for _, l := range from.GetLinks() {
		if l == nil {
			continue
		}
		if NormalizeHex(l.GetTraceId()) == tt && NormalizeHex(l.GetSpanId()) == ts {
			out = append(out, l)
		}
	}
	return out
}

func findMatchingLink(from, target *proxypb.TraceSpan) *proxypb.SpanLink {
	links := findMatchingLinks(from, target)
	if len(links) == 0 {
		return nil
	}
	return links[0]
}

// AssertBidirectionalLink verifies that span a links to span b AND span b links
// to span a (without duplicate links to the same target), and checks any
// configured forward (a->b) or reverse (b->a) link attribute requirements.
func AssertBidirectionalLink(a, b *proxypb.TraceSpan, opts ...LinkOption) error {
	if err := AssertValidSpanIDs(a); err != nil {
		return fmt.Errorf("invalid span a: %w", err)
	}
	if err := AssertValidSpanIDs(b); err != nil {
		return fmt.Errorf("invalid span b: %w", err)
	}
	var cfg linkCheckConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	fwdMatches := findMatchingLinks(a, b)
	if len(fwdMatches) == 0 {
		return fmt.Errorf("missing forward link from %q (%s/%s) to %q (%s/%s)",
			a.GetName(), a.GetTraceId(), a.GetSpanId(), b.GetName(), b.GetTraceId(), b.GetSpanId())
	}
	if len(fwdMatches) > 1 {
		return fmt.Errorf("duplicate forward links (%d) from %q (%s/%s) to %q (%s/%s)",
			len(fwdMatches), a.GetName(), a.GetTraceId(), a.GetSpanId(), b.GetName(), b.GetTraceId(), b.GetSpanId())
	}
	fwd := fwdMatches[0]
	for _, chk := range cfg.forwardAttrs {
		if err := AssertAttribute(fwd.GetAttributes(), chk.Key, chk.Matcher); err != nil {
			return fmt.Errorf("forward link %q -> %q: %w", a.GetName(), b.GetName(), err)
		}
	}

	revMatches := findMatchingLinks(b, a)
	if len(revMatches) == 0 {
		return fmt.Errorf("missing reverse link from %q (%s/%s) to %q (%s/%s)",
			b.GetName(), b.GetTraceId(), b.GetSpanId(), a.GetName(), a.GetTraceId(), a.GetSpanId())
	}
	if len(revMatches) > 1 {
		return fmt.Errorf("duplicate reverse links (%d) from %q (%s/%s) to %q (%s/%s)",
			len(revMatches), b.GetName(), b.GetTraceId(), b.GetSpanId(), a.GetName(), a.GetTraceId(), a.GetSpanId())
	}
	rev := revMatches[0]
	for _, chk := range cfg.reverseAttrs {
		if err := AssertAttribute(rev.GetAttributes(), chk.Key, chk.Matcher); err != nil {
			return fmt.Errorf("reverse link %q -> %q: %w", b.GetName(), a.GetName(), err)
		}
	}
	return nil
}

// AssertAttribute checks that key exists in attrs and satisfies matcher (or
// MatchPresent if matcher is nil).
func AssertAttribute(attrs map[string]*proxypb.AttributeValue, key string, matcher AttrMatcher) error {
	if matcher == nil {
		matcher = MatchPresent()
	}
	if attrs == nil {
		_, expected, _ := matcher.Match(nil)
		return fmt.Errorf("missing attribute %q (attributes map is nil; expected %s)", key, expected)
	}
	av, ok := attrs[key]
	if !ok || av == nil || av.GetValue() == nil {
		_, expected, _ := matcher.Match(nil)
		return fmt.Errorf("missing attribute %q (expected %s)", key, expected)
	}
	matched, expected, actual := matcher.Match(av)
	if !matched {
		return fmt.Errorf("attribute %q mismatch: expected %s, got %s", key, expected, actual)
	}
	return nil
}

// AssertSpanAttribute checks that span's attributes contain key matching matcher.
func AssertSpanAttribute(span *proxypb.TraceSpan, key string, matcher AttrMatcher) error {
	if span == nil {
		return fmt.Errorf("span is nil while checking attribute %q", key)
	}
	if err := AssertAttribute(span.GetAttributes(), key, matcher); err != nil {
		return fmt.Errorf("span %q: %w", span.GetName(), err)
	}
	return nil
}

// AssertAttributeAbsent verifies that key is NOT present in span.Attributes.
func AssertAttributeAbsent(span *proxypb.TraceSpan, key string) error {
	if span == nil {
		return fmt.Errorf("span is nil while checking attribute %q absence", key)
	}
	if attrs := span.GetAttributes(); attrs != nil {
		if av, ok := attrs[key]; ok {
			return fmt.Errorf("span %q unexpectedly contains attribute %q = %s", span.GetName(), key, FormatAttrValue(av))
		}
	}
	return nil
}

// AssertEvent verifies that span contains at least one SpanEvent named name
// satisfying all attrChecks.
func AssertEvent(span *proxypb.TraceSpan, name string, attrChecks ...AttrCheck) error {
	if span == nil {
		return fmt.Errorf("span is nil while checking event %q", name)
	}
	var candidates []*proxypb.SpanEvent
	var seenNames []string
	for _, ev := range span.GetEvents() {
		if ev == nil {
			continue
		}
		seenNames = append(seenNames, ev.GetName())
		if ev.GetName() == name {
			candidates = append(candidates, ev)
		}
	}
	if len(candidates) == 0 {
		return fmt.Errorf("span %q missing event %q (events present: %q)", span.GetName(), name, seenNames)
	}
	var firstErr error
	for _, ev := range candidates {
		var mismatch error
		for _, chk := range attrChecks {
			if err := AssertAttribute(ev.GetAttributes(), chk.Key, chk.Matcher); err != nil {
				mismatch = fmt.Errorf("span %q event %q: %w", span.GetName(), name, err)
				break
			}
		}
		if mismatch == nil {
			return nil
		}
		if firstErr == nil {
			firstErr = mismatch
		}
	}
	return firstErr
}

// AssertStatus verifies span.StatusCode equals wantCode and, when msgContains != "",
// checks that span.StatusMessage contains msgContains.
func AssertStatus(span *proxypb.TraceSpan, wantCode proxypb.SpanStatusCode, msgContains string) error {
	if span == nil {
		return fmt.Errorf("span is nil (want status %s)", wantCode)
	}
	if span.GetStatusCode() != wantCode {
		return fmt.Errorf("span %q status_code = %s (message %q), want %s",
			span.GetName(), span.GetStatusCode(), span.GetStatusMessage(), wantCode)
	}
	if msgContains != "" && !strings.Contains(span.GetStatusMessage(), msgContains) {
		return fmt.Errorf("span %q status_message %q does not contain %q",
			span.GetName(), span.GetStatusMessage(), msgContains)
	}
	return nil
}

// AssertParentFromTraceparent validates traceparent per W3C Trace Context and
// verifies that span's (trace_id, parent_span_id) matches the extracted
// (trace_id, span_id).
func AssertParentFromTraceparent(span *proxypb.TraceSpan, traceparent string) error {
	if err := AssertValidSpanIDs(span); err != nil {
		return err
	}
	tp, err := ParseTraceparent(traceparent)
	if err != nil {
		return fmt.Errorf("invalid traceparent: %w", err)
	}
	if NormalizeHex(span.GetTraceId()) != tp.TraceID {
		return fmt.Errorf("span %q trace_id %q does not match traceparent trace_id %q",
			span.GetName(), span.GetTraceId(), tp.TraceID)
	}
	if NormalizeHex(span.GetParentSpanId()) != tp.SpanID {
		return fmt.Errorf("span %q parent_span_id %q does not match traceparent span_id %q",
			span.GetName(), span.GetParentSpanId(), tp.SpanID)
	}
	return nil
}

// AssertTraceparentMatchesSpan validates traceparent per W3C Trace Context and
// verifies that its (trace_id, span_id) matches span's own (trace_id, span_id).
func AssertTraceparentMatchesSpan(traceparent string, span *proxypb.TraceSpan) error {
	if err := AssertValidSpanIDs(span); err != nil {
		return err
	}
	tp, err := ParseTraceparent(traceparent)
	if err != nil {
		return fmt.Errorf("invalid traceparent: %w", err)
	}
	if tp.TraceID != NormalizeHex(span.GetTraceId()) {
		return fmt.Errorf("traceparent trace_id %q does not match span %q trace_id %q",
			tp.TraceID, span.GetName(), span.GetTraceId())
	}
	if tp.SpanID != NormalizeHex(span.GetSpanId()) {
		return fmt.Errorf("traceparent span_id %q does not match span %q span_id %q",
			tp.SpanID, span.GetName(), span.GetSpanId())
	}
	if span.GetTraceFlags() != 0 && (tp.TraceFlags&0x01) != (byte(span.GetTraceFlags())&0x01) {
		return fmt.Errorf("traceparent sampled flag 0x%02x does not match span %q trace_flags 0x%02x",
			tp.TraceFlags&0x01, span.GetName(), span.GetTraceFlags())
	}
	return nil
}
