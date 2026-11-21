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
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
)

// Traceparent represents a parsed W3C Trace Context traceparent header
// (`00-<32-hex-trace-id>-<16-hex-span-id>-<2-hex-flags>`).
type Traceparent struct {
	Version    byte
	TraceID    string
	SpanID     string
	TraceFlags byte
}

// IsSampled reports whether the sampled flag (bit 0) is set.
func (tp Traceparent) IsSampled() bool {
	return (tp.TraceFlags & 0x01) != 0
}

// String formats tp in canonical W3C traceparent format.
func (tp Traceparent) String() string {
	return fmt.Sprintf("%02x-%s-%s-%02x", tp.Version, tp.TraceID, tp.SpanID, tp.TraceFlags)
}

// NormalizeHex trims surrounding whitespace and lowercases hex ID strings for
// case-tolerant map indexing. Validation helpers still enforce strict lowercase
// hex formatting where required by W3C Trace Context / OTel conventions.
func NormalizeHex(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func isAllZeroHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

// IsValidTraceID checks that id is exactly 32 lowercase hex characters and not all zero.
func IsValidTraceID(id string) bool {
	return len(id) == 32 && isLowerHex(id) && !isAllZeroHex(id)
}

// IsValidSpanID checks that id is exactly 16 lowercase hex characters and not all zero.
func IsValidSpanID(id string) bool {
	return len(id) == 16 && isLowerHex(id) && !isAllZeroHex(id)
}

// ParseTraceparent strictly validates a W3C version-00 traceparent string.
// It rejects leading/trailing whitespace, uppercase hex, unsupported versions,
// malformed part counts/lengths, and all-zero trace or span IDs.
func ParseTraceparent(raw string) (Traceparent, error) {
	if raw == "" {
		return Traceparent{}, fmt.Errorf("traceparent is empty")
	}
	if raw != strings.TrimSpace(raw) {
		return Traceparent{}, fmt.Errorf("traceparent %q contains leading or trailing whitespace", raw)
	}
	if len(raw) != 55 {
		return Traceparent{}, fmt.Errorf("traceparent %q has length %d, want 55", raw, len(raw))
	}
	parts := strings.Split(raw, "-")
	if len(parts) != 4 {
		return Traceparent{}, fmt.Errorf("traceparent %q has %d dash-separated parts, want 4", raw, len(parts))
	}
	if len(parts[0]) != 2 || !isLowerHex(parts[0]) {
		return Traceparent{}, fmt.Errorf("traceparent %q has invalid version %q", raw, parts[0])
	}
	if parts[0] != "00" {
		return Traceparent{}, fmt.Errorf("traceparent %q has unsupported version %q (want \"00\")", raw, parts[0])
	}
	if !IsValidTraceID(parts[1]) {
		return Traceparent{}, fmt.Errorf("traceparent %q has invalid trace_id %q", raw, parts[1])
	}
	if !IsValidSpanID(parts[2]) {
		return Traceparent{}, fmt.Errorf("traceparent %q has invalid span_id %q", raw, parts[2])
	}
	if len(parts[3]) != 2 || !isLowerHex(parts[3]) {
		return Traceparent{}, fmt.Errorf("traceparent %q has invalid trace_flags %q", raw, parts[3])
	}
	flagBytes, err := hex.DecodeString(parts[3])
	if err != nil || len(flagBytes) != 1 {
		return Traceparent{}, fmt.Errorf("traceparent %q failed decoding trace_flags %q: %w", raw, parts[3], err)
	}
	if flagBytes[0] > 0x03 {
		return Traceparent{}, fmt.Errorf("traceparent %q has invalid version-00 trace_flags 0x%02x (must be <= 0x03)", raw, flagBytes[0])
	}
	return Traceparent{
		Version:    0,
		TraceID:    parts[1],
		SpanID:     parts[2],
		TraceFlags: flagBytes[0],
	}, nil
}

// ExtractTraceparent extracts a non-empty "googclient_traceparent" attribute from
// a PubsubMessage attributes map.
func ExtractTraceparent(attrs map[string]string) (string, bool) {
	if len(attrs) == 0 {
		return "", false
	}
	v, ok := attrs["googclient_traceparent"]
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// ExtractResourceIDs extracts (projectID, shortID, fullName) from a full
// Pub/Sub resource path ("projects/{p}/topics/{t}" or
// "projects/{p}/subscriptions/{s}") or short ID.
func ExtractResourceIDs(resourceOrID, resourceType, projectOverride string) (projectID, shortID, fullName string) {
	projectID = strings.TrimSpace(projectOverride)
	trimmed := strings.TrimSpace(resourceOrID)
	if trimmed == "" {
		return projectID, "", ""
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 4 && parts[0] == "projects" && parts[2] == resourceType {
		if projectID == "" {
			projectID = parts[1]
		}
		shortID = parts[3]
		fullName = trimmed
		return projectID, shortID, fullName
	}
	if idx := strings.LastIndex(trimmed, "/"); idx >= 0 && idx+1 < len(trimmed) {
		shortID = trimmed[idx+1:]
	} else {
		shortID = trimmed
	}
	if projectID != "" && resourceType != "" {
		fullName = fmt.Sprintf("projects/%s/%s/%s", projectID, resourceType, shortID)
	}
	return projectID, shortID, fullName
}

// SpanKey uniquely identifies a span within a trace.
type SpanKey struct {
	TraceID string
	SpanID  string
}

// SpanIndex indexes TraceSpans by (trace_id, span_id), name, and parent-child
// relationships using parent_span_id (never child_span_count).
type SpanIndex struct {
	Spans    []*proxypb.TraceSpan
	ByKey    map[SpanKey]*proxypb.TraceSpan
	ByName   map[string][]*proxypb.TraceSpan
	Children map[SpanKey][]*proxypb.TraceSpan
}

// BuildSpanIndex indexes spans and returns any structural span ID, timestamp,
// link ID, or duplicate identity violations found across the slice.
func BuildSpanIndex(spans []*proxypb.TraceSpan) (*SpanIndex, []Violation) {
	idx := &SpanIndex{
		Spans:    make([]*proxypb.TraceSpan, 0, len(spans)),
		ByKey:    make(map[SpanKey]*proxypb.TraceSpan, len(spans)),
		ByName:   make(map[string][]*proxypb.TraceSpan),
		Children: make(map[SpanKey][]*proxypb.TraceSpan),
	}
	var violations []Violation

	for i, s := range spans {
		if s == nil {
			violations = append(violations, Violation{
				Code:    CodeInvalidSpanID,
				Rule:    "span.non_nil",
				Message: fmt.Sprintf("span at index %d is nil", i),
			})
			continue
		}
		idx.Spans = append(idx.Spans, s)
		idx.ByName[s.GetName()] = append(idx.ByName[s.GetName()], s)

		rawTrace := s.GetTraceId()
		rawSpan := s.GetSpanId()
		rawParent := s.GetParentSpanId()
		tid := NormalizeHex(rawTrace)
		sid := NormalizeHex(rawSpan)
		pid := NormalizeHex(rawParent)

		if !IsValidTraceID(rawTrace) {
			violations = append(violations, Violation{
				Code:     CodeInvalidTraceID,
				Rule:     "span.trace_id",
				Message:  fmt.Sprintf("span %q has invalid trace_id %q", s.GetName(), rawTrace),
				Expected: "32 lowercase non-zero hex chars",
				Actual:   fmt.Sprintf("%q", rawTrace),
				TraceID:  rawTrace,
				SpanID:   rawSpan,
				SpanName: s.GetName(),
			})
		}
		if !IsValidSpanID(rawSpan) {
			violations = append(violations, Violation{
				Code:     CodeInvalidSpanID,
				Rule:     "span.span_id",
				Message:  fmt.Sprintf("span %q has invalid span_id %q", s.GetName(), rawSpan),
				Expected: "16 lowercase non-zero hex chars",
				Actual:   fmt.Sprintf("%q", rawSpan),
				TraceID:  rawTrace,
				SpanID:   rawSpan,
				SpanName: s.GetName(),
			})
		}
		if rawParent != "" && !IsValidSpanID(rawParent) {
			violations = append(violations, Violation{
				Code:     CodeInvalidParentSpanID,
				Rule:     "span.parent_span_id",
				Message:  fmt.Sprintf("span %q has invalid non-empty parent_span_id %q", s.GetName(), rawParent),
				Expected: "empty string (root) or 16 lowercase non-zero hex chars",
				Actual:   fmt.Sprintf("%q", rawParent),
				TraceID:  rawTrace,
				SpanID:   rawSpan,
				SpanName: s.GetName(),
			})
		} else if sid != "" && sid == pid {
			violations = append(violations, Violation{
				Code:     CodeWrongParent,
				Rule:     "span.non_self_parent",
				Message:  fmt.Sprintf("span %q cannot have parent_span_id equal to its own span_id %q", s.GetName(), rawSpan),
				Expected: "parent_span_id != span_id",
				Actual:   fmt.Sprintf("%q", rawParent),
				TraceID:  rawTrace,
				SpanID:   rawSpan,
				SpanName: s.GetName(),
			})
		}

		st, et := s.GetStartTime(), s.GetEndTime()
		if st != nil && !st.IsValid() {
			violations = append(violations, Violation{
				Code:     CodeInvalidTimestamp,
				Rule:     "span.timestamps",
				Message:  fmt.Sprintf("span %q has invalid start_time", s.GetName()),
				Expected: "valid protobuf Timestamp",
				Actual:   fmt.Sprintf("%v", st),
				TraceID:  rawTrace,
				SpanID:   rawSpan,
				SpanName: s.GetName(),
			})
		}
		if et != nil && !et.IsValid() {
			violations = append(violations, Violation{
				Code:     CodeInvalidTimestamp,
				Rule:     "span.timestamps",
				Message:  fmt.Sprintf("span %q has invalid end_time", s.GetName()),
				Expected: "valid protobuf Timestamp",
				Actual:   fmt.Sprintf("%v", et),
				TraceID:  rawTrace,
				SpanID:   rawSpan,
				SpanName: s.GetName(),
			})
		}
		if st != nil && et != nil && st.IsValid() && et.IsValid() {
			if et.AsTime().Before(st.AsTime()) {
				violations = append(violations, Violation{
					Code:     CodeInvalidTimestamp,
					Rule:     "span.timestamps",
					Message:  fmt.Sprintf("span %q end_time (%s) precedes start_time (%s)", s.GetName(), et.AsTime(), st.AsTime()),
					Expected: "end_time >= start_time",
					Actual:   fmt.Sprintf("start=%s end=%s", st.AsTime(), et.AsTime()),
					TraceID:  rawTrace,
					SpanID:   rawSpan,
					SpanName: s.GetName(),
				})
			}
		}

		for li, link := range s.GetLinks() {
			if link == nil {
				continue
			}
			if !IsValidTraceID(link.GetTraceId()) {
				violations = append(violations, Violation{
					Code:     CodeInvalidTraceID,
					Rule:     "span.link.trace_id",
					Message:  fmt.Sprintf("span %q link[%d] has invalid trace_id %q", s.GetName(), li, link.GetTraceId()),
					Expected: "32 lowercase non-zero hex chars",
					Actual:   fmt.Sprintf("%q", link.GetTraceId()),
					TraceID:  rawTrace,
					SpanID:   rawSpan,
					SpanName: s.GetName(),
				})
			}
			if !IsValidSpanID(link.GetSpanId()) {
				violations = append(violations, Violation{
					Code:     CodeInvalidSpanID,
					Rule:     "span.link.span_id",
					Message:  fmt.Sprintf("span %q link[%d] has invalid span_id %q", s.GetName(), li, link.GetSpanId()),
					Expected: "16 lowercase non-zero hex chars",
					Actual:   fmt.Sprintf("%q", link.GetSpanId()),
					TraceID:  rawTrace,
					SpanID:   rawSpan,
					SpanName: s.GetName(),
				})
			}
		}

		if tid != "" && sid != "" {
			key := SpanKey{TraceID: tid, SpanID: sid}
			if prev, exists := idx.ByKey[key]; exists {
				violations = append(violations, Violation{
					Code:     CodeDuplicateSpanID,
					Rule:     "span.unique_id",
					Message:  fmt.Sprintf("duplicate span identity (%s, %s) on %q (previously seen on %q)", tid, sid, s.GetName(), prev.GetName()),
					Expected: "unique (trace_id, span_id)",
					Actual:   fmt.Sprintf("(%s, %s)", tid, sid),
					TraceID:  rawTrace,
					SpanID:   rawSpan,
					SpanName: s.GetName(),
				})
			} else if IsValidTraceID(rawTrace) && IsValidSpanID(rawSpan) {
				idx.ByKey[key] = s
			}
		}
		if IsValidTraceID(rawTrace) && IsValidSpanID(rawParent) {
			pkey := SpanKey{TraceID: tid, SpanID: pid}
			idx.Children[pkey] = append(idx.Children[pkey], s)
		}
	}

	return idx, violations
}

// Children returns all direct child spans of parent based on (trace_id, parent_span_id),
// independent of ChildSpanCount.
func (idx *SpanIndex) ChildrenOf(parent *proxypb.TraceSpan) []*proxypb.TraceSpan {
	if idx == nil || parent == nil {
		return nil
	}
	key := SpanKey{
		TraceID: NormalizeHex(parent.GetTraceId()),
		SpanID:  NormalizeHex(parent.GetSpanId()),
	}
	return idx.Children[key]
}

// FindSpans returns all spans in spans matching pred, preserving order.
func FindSpans(spans []*proxypb.TraceSpan, pred func(*proxypb.TraceSpan) bool) []*proxypb.TraceSpan {
	if pred == nil {
		return nil
	}
	var out []*proxypb.TraceSpan
	for _, s := range spans {
		if s != nil && pred(s) {
			out = append(out, s)
		}
	}
	return out
}

// FindSpansByName returns all spans whose Name equals name.
func FindSpansByName(spans []*proxypb.TraceSpan, name string) []*proxypb.TraceSpan {
	return FindSpans(spans, func(s *proxypb.TraceSpan) bool {
		return s.GetName() == name
	})
}

// FindSpansByNameAndKind returns all spans matching name and kind.
func FindSpansByNameAndKind(spans []*proxypb.TraceSpan, name string, kind proxypb.SpanKind) []*proxypb.TraceSpan {
	return FindSpans(spans, func(s *proxypb.TraceSpan) bool {
		return s.GetName() == name && s.GetKind() == kind
	})
}

// FindChildSpans returns all spans whose (trace_id, parent_span_id) matches
// parent's (trace_id, span_id).
func FindChildSpans(spans []*proxypb.TraceSpan, parent *proxypb.TraceSpan) []*proxypb.TraceSpan {
	if parent == nil {
		return nil
	}
	pt := NormalizeHex(parent.GetTraceId())
	ps := NormalizeHex(parent.GetSpanId())
	if !IsValidTraceID(pt) || !IsValidSpanID(ps) {
		return nil
	}
	return FindSpans(spans, func(s *proxypb.TraceSpan) bool {
		return NormalizeHex(s.GetTraceId()) == pt && NormalizeHex(s.GetParentSpanId()) == ps
	})
}

// SortSpansDeterministic sorts a copy of spans by (StartTime, TraceId, SpanId, Name).
func SortSpansDeterministic(spans []*proxypb.TraceSpan) []*proxypb.TraceSpan {
	out := make([]*proxypb.TraceSpan, 0, len(spans))
	for _, s := range spans {
		if s != nil {
			out = append(out, s)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return compareSpanOrder(out[i], out[j]) < 0
	})
	return out
}

func compareSpanOrder(a, b *proxypb.TraceSpan) int {
	at := a.GetStartTime()
	bt := b.GetStartTime()
	aValid := at != nil && at.IsValid()
	bValid := bt != nil && bt.IsValid()
	if aValid != bValid {
		if aValid {
			return -1
		}
		return 1
	}
	if aValid && bValid {
		atTime := at.AsTime()
		btTime := bt.AsTime()
		if atTime.Before(btTime) {
			return -1
		}
		if btTime.Before(atTime) {
			return 1
		}
	}
	if a.GetTraceId() != b.GetTraceId() {
		return strings.Compare(a.GetTraceId(), b.GetTraceId())
	}
	if a.GetSpanId() != b.GetSpanId() {
		return strings.Compare(a.GetSpanId(), b.GetSpanId())
	}
	return strings.Compare(a.GetName(), b.GetName())
}

// AttrString extracts a string attribute strictly checking the StringValue oneof variant.
func AttrString(attrs map[string]*proxypb.AttributeValue, key string) (string, bool) {
	if attrs == nil {
		return "", false
	}
	av, ok := attrs[key]
	if !ok || av == nil {
		return "", false
	}
	v, ok := av.GetValue().(*proxypb.AttributeValue_StringValue)
	if !ok {
		return "", false
	}
	return v.StringValue, true
}

// AttrInt64 extracts an int64 attribute strictly checking the Int64Value oneof variant.
func AttrInt64(attrs map[string]*proxypb.AttributeValue, key string) (int64, bool) {
	if attrs == nil {
		return 0, false
	}
	av, ok := attrs[key]
	if !ok || av == nil {
		return 0, false
	}
	v, ok := av.GetValue().(*proxypb.AttributeValue_Int64Value)
	if !ok {
		return 0, false
	}
	return v.Int64Value, true
}

// AttrDouble extracts a float64 attribute strictly checking the DoubleValue oneof variant.
func AttrDouble(attrs map[string]*proxypb.AttributeValue, key string) (float64, bool) {
	if attrs == nil {
		return 0, false
	}
	av, ok := attrs[key]
	if !ok || av == nil {
		return 0, false
	}
	v, ok := av.GetValue().(*proxypb.AttributeValue_DoubleValue)
	if !ok {
		return 0, false
	}
	return v.DoubleValue, true
}

// AttrBool extracts a bool attribute strictly checking the BoolValue oneof variant.
func AttrBool(attrs map[string]*proxypb.AttributeValue, key string) (bool, bool) {
	if attrs == nil {
		return false, false
	}
	av, ok := attrs[key]
	if !ok || av == nil {
		return false, false
	}
	v, ok := av.GetValue().(*proxypb.AttributeValue_BoolValue)
	if !ok {
		return false, false
	}
	return v.BoolValue, true
}

// AttrStringList extracts a []string attribute strictly checking the StringList oneof variant.
func AttrStringList(attrs map[string]*proxypb.AttributeValue, key string) ([]string, bool) {
	if attrs == nil {
		return nil, false
	}
	av, ok := attrs[key]
	if !ok || av == nil {
		return nil, false
	}
	v, ok := av.GetValue().(*proxypb.AttributeValue_StringList)
	if !ok || v.StringList == nil {
		return nil, false
	}
	return v.StringList.GetValues(), true
}

// AttrInt64List extracts an []int64 attribute strictly checking the Int64List oneof variant.
func AttrInt64List(attrs map[string]*proxypb.AttributeValue, key string) ([]int64, bool) {
	if attrs == nil {
		return nil, false
	}
	av, ok := attrs[key]
	if !ok || av == nil {
		return nil, false
	}
	v, ok := av.GetValue().(*proxypb.AttributeValue_Int64List)
	if !ok || v.Int64List == nil {
		return nil, false
	}
	return v.Int64List.GetValues(), true
}

// AttrDoubleList extracts a []float64 attribute strictly checking the DoubleList oneof variant.
func AttrDoubleList(attrs map[string]*proxypb.AttributeValue, key string) ([]float64, bool) {
	if attrs == nil {
		return nil, false
	}
	av, ok := attrs[key]
	if !ok || av == nil {
		return nil, false
	}
	v, ok := av.GetValue().(*proxypb.AttributeValue_DoubleList)
	if !ok || v.DoubleList == nil {
		return nil, false
	}
	return v.DoubleList.GetValues(), true
}

// AttrBoolList extracts a []bool attribute strictly checking the BoolList oneof variant.
func AttrBoolList(attrs map[string]*proxypb.AttributeValue, key string) ([]bool, bool) {
	if attrs == nil {
		return nil, false
	}
	av, ok := attrs[key]
	if !ok || av == nil {
		return nil, false
	}
	v, ok := av.GetValue().(*proxypb.AttributeValue_BoolList)
	if !ok || v.BoolList == nil {
		return nil, false
	}
	return v.BoolList.GetValues(), true
}

// FormatAttrValue returns a type-tagged representation of av for diagnostics.
func FormatAttrValue(av *proxypb.AttributeValue) string {
	if av == nil || av.GetValue() == nil {
		return "<unset>"
	}
	switch v := av.GetValue().(type) {
	case *proxypb.AttributeValue_StringValue:
		return fmt.Sprintf("string(%q)", v.StringValue)
	case *proxypb.AttributeValue_Int64Value:
		return fmt.Sprintf("int64(%d)", v.Int64Value)
	case *proxypb.AttributeValue_DoubleValue:
		return fmt.Sprintf("double(%g)", v.DoubleValue)
	case *proxypb.AttributeValue_BoolValue:
		return fmt.Sprintf("bool(%t)", v.BoolValue)
	case *proxypb.AttributeValue_StringList:
		return fmt.Sprintf("string_list(%v)", v.StringList.GetValues())
	case *proxypb.AttributeValue_Int64List:
		return fmt.Sprintf("int64_list(%v)", v.Int64List.GetValues())
	case *proxypb.AttributeValue_DoubleList:
		return fmt.Sprintf("double_list(%v)", v.DoubleList.GetValues())
	case *proxypb.AttributeValue_BoolList:
		return fmt.Sprintf("bool_list(%v)", v.BoolList.GetValues())
	default:
		return fmt.Sprintf("<unknown:%T>", av.GetValue())
	}
}
