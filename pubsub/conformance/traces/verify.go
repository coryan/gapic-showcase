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
	"math"
	"math/bits"
	"sort"
	"strconv"
	"strings"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
)

const (
	eodPartialBatchErrorMsg  = "some messages in the batch failed or require retry"
	publishRequireAttrsCap   = int64(20_000)
	subscribeSettleBonusCap  = int64(100_000)
	subscribeRequireAttrsCap = int64(8_000)
	unaryPullSettleBonusCap  = int64(400_000)
	unaryPullAckIDBonus      = int64(2_400_000)
)

func normalizeSettleBonus(raw, maxRaw, capVal int64) int64 {
	if raw <= 0 || maxRaw <= 0 || capVal <= 0 {
		return 0
	}
	if raw >= maxRaw {
		return capVal
	}
	hi, lo := bits.Mul64(uint64(raw), uint64(capVal))
	scaledU, _ := bits.Div64(hi, lo, uint64(maxRaw))
	if scaledU < 1 {
		return 1
	}
	return int64(scaledU)
}

func verifySpans(spans []*proxypb.TraceSpan, profile Profile, exp *TraceExpectation) TraceResult {
	res := TraceResult{
		Status:    StatusPass,
		Profile:   profile.Name,
		SpanCount: len(spans),
	}

	strictRaw := exp != nil && exp.StrictRawSpans
	filtered := filterSpansForProfile(spans, profile, strictRaw)
	idx, structViolations := BuildSpanIndex(filtered)

	var failures []Violation
	failures = append(failures, structViolations...)

	// Even when transport spans are ignored from trace matching, ensure they do
	// not carry malformed W3C hex IDs if present.
	if !strictRaw && len(filtered) != len(spans) {
		for _, s := range spans {
			if s != nil && profile.IsIgnoredScope(s.GetInstrumentationScopeName()) {
				if err := AssertValidSpanIDs(s); err != nil {
					failures = append(failures, Violation{
						Code:     CodeInvalidSpanID,
						Rule:     "span.transport_ids",
						Message:  err.Error(),
						TraceID:  s.GetTraceId(),
						SpanID:   s.GetSpanId(),
						SpanName: s.GetName(),
					})
				}
			}
		}
	}

	sortedSpans := SortSpansDeterministic(filtered)
	res.Spans = make([]SpanSummary, 0, len(sortedSpans))
	for _, s := range sortedSpans {
		res.Spans = append(res.Spans, SummarizeSpan(s))
	}

	devs := newDeviationCollector()
	matched := make(map[*proxypb.TraceSpan]bool)

	if exp != nil {
		if exp.ExpectNoSpans {
			// Every non-ignored span (including C++ gl-cpp spans) fails ExpectNoSpans.
			for _, s := range sortedSpans {
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "traces.expect_no_spans",
					Message:  fmt.Sprintf("expected zero spans when monitoring is disabled, but found %q (scope %q)", s.GetName(), s.GetInstrumentationScopeName()),
					Expected: "0 spans",
					Actual:   s.GetName(),
					TraceID:  s.GetTraceId(),
					SpanID:   s.GetSpanId(),
					SpanName: s.GetName(),
				})
			}
		}

		explicitProj := strings.TrimSpace(exp.ProjectID)
		projFromTopic, _, _ := ExtractResourceIDs(exp.Topic, "topics", explicitProj)
		projFromSub, _, _ := ExtractResourceIDs(exp.Subscription, "subscriptions", explicitProj)
		pubProjectID := explicitProj
		if pubProjectID == "" {
			if projFromTopic != "" {
				pubProjectID = projFromTopic
			} else {
				pubProjectID = projFromSub
			}
		}
		subProjectID := explicitProj
		if subProjectID == "" {
			if projFromSub != "" {
				subProjectID = projFromSub
			} else {
				subProjectID = projFromTopic
			}
		}
		_, topicID, fullTopic := ExtractResourceIDs(exp.Topic, "topics", pubProjectID)
		_, subID, fullSub := ExtractResourceIDs(exp.Subscription, "subscriptions", subProjectID)

		if exp.Publish != nil {
			f := verifyPublish(idx, sortedSpans, profile, pubProjectID, fullTopic, topicID, exp.Publish, !exp.AllowExtraPubSubSpans, matched, devs)
			failures = append(failures, f...)
		}

		if exp.Subscribe != nil {
			f := verifySubscribe(idx, sortedSpans, profile, subProjectID, fullSub, subID, exp.Subscribe, !exp.AllowExtraPubSubSpans, matched, devs)
			failures = append(failures, f...)
		}

		if exp.UnaryPull != nil {
			f := verifyUnaryPull(idx, sortedSpans, profile, subProjectID, fullSub, subID, topicID, exp.UnaryPull, matched, devs)
			failures = append(failures, f...)
		}

		if !exp.AllowExtraPubSubSpans && !exp.ExpectNoSpans && (exp.Publish != nil || exp.Subscribe != nil || exp.UnaryPull != nil) {
			for _, s := range sortedSpans {
				if matched[s] {
					continue
				}
				if profile.AllowBenignCppSpans && IsBenignCppLifecycleOrGRPCSpan(s) {
					devs.add(Deviation{
						Category:    "scope",
						Rule:        "spans.cpp_grpc_and_lifecycle",
						ExpectedGo:  "Pub/Sub semantic spans only",
						Observed:    s.GetName(),
						SpanName:    s.GetName(),
						Description: "C++ SDK emits gRPC stub and Flush/ResumePublish lifecycle spans",
					})
					continue
				}
				if exp.StrictRawSpans || isPubSubSpanOrScope(s, profile) {
					failures = append(failures, Violation{
						Code:     CodeUnexpectedSpan,
						Rule:     "traces.no_extra_pubsub_spans",
						Message:  fmt.Sprintf("unexpected extra Pub/Sub span %q (kind=%s, scope=%q)", s.GetName(), s.GetKind(), s.GetInstrumentationScopeName()),
						Expected: "no unexpected Pub/Sub spans",
						Actual:   s.GetName(),
						TraceID:  s.GetTraceId(),
						SpanID:   s.GetSpanId(),
						SpanName: s.GetName(),
					})
				}
			}
		}

		for _, chk := range exp.CustomChecks {
			if chk == nil {
				continue
			}
			customViolations := chk(filtered, profile)
			for i := range customViolations {
				if customViolations[i].Code == "" {
					customViolations[i].Code = CodeCustomCheckFailed
				}
				if customViolations[i].Rule == "" {
					customViolations[i].Rule = "custom_check"
				}
			}
			failures = append(failures, customViolations...)
		}
	}

	res.Deviations = devs.sorted()
	if len(failures) > 0 {
		res.Status = StatusFail
		res.Failures = failures
	}
	return res
}

func filterSpansForProfile(spans []*proxypb.TraceSpan, profile Profile, strictRaw bool) []*proxypb.TraceSpan {
	out := make([]*proxypb.TraceSpan, 0, len(spans))
	for _, s := range spans {
		if s == nil {
			out = append(out, nil)
			continue
		}
		if !strictRaw && profile.IsIgnoredScope(s.GetInstrumentationScopeName()) {
			continue
		}
		out = append(out, s)
	}
	return out
}

func isKnownPubSubScope(scope string, profile Profile) bool {
	return scope == GoPubSubScopeName || scope == CppScopeName || (profile.ExpectedScope != "" && scope == profile.ExpectedScope)
}

func isPubSubSpanOrScope(s *proxypb.TraceSpan, profile Profile) bool {
	if s == nil {
		return false
	}
	scope := s.GetInstrumentationScopeName()
	if isKnownPubSubScope(scope, profile) {
		return true
	}
	if scope == "" {
		return isPubSubSpanName(s.GetName())
	}
	return false
}

func isCandidatePubSubSpan(s *proxypb.TraceSpan, profile Profile) bool {
	if !isPubSubSpanOrScope(s, profile) {
		return false
	}
	if profile.AllowBenignCppSpans && IsBenignCppLifecycleOrGRPCSpan(s) {
		return false
	}
	return true
}

func isPubSubSpanName(name string) bool {
	switch name {
	case "publisher flow control",
		"publisher batching",
		"subscriber concurrency control",
		"subscribe scheduler",
		"subscriber scheduler":
		return true
	}
	if strings.HasPrefix(name, "publish #") {
		return true
	}
	for _, suffix := range []string{" create", " publish", " subscribe", " process", " ack", " nack", " modack", " receive"} {
		if strings.HasSuffix(name, suffix) && len(name) > len(suffix) {
			return true
		}
	}
	return false
}

func checkSpanScope(s *proxypb.TraceSpan, profile Profile, rule string, devs *deviationCollector) []Violation {
	if s == nil || profile.ExpectedScope == "" {
		return nil
	}
	got := s.GetInstrumentationScopeName()
	if got != profile.ExpectedScope {
		return []Violation{{
			Code:     CodeWrongScope,
			Rule:     rule,
			Message:  fmt.Sprintf("span %q has instrumentation_scope_name %q, want %q", s.GetName(), got, profile.ExpectedScope),
			Expected: profile.ExpectedScope,
			Actual:   got,
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		}}
	}
	if profile.Name == ProfileCPP && got != GoPubSubScopeName {
		devs.add(Deviation{
			Category:    "scope",
			Rule:        "scope.instrumentation_name",
			ExpectedGo:  GoPubSubScopeName,
			Observed:    got,
			Description: "C++ SDK uses instrumentation scope gl-cpp instead of cloud.google.com/go/pubsub/v2",
		})
	}
	return nil
}

func checkSpanKindV(s *proxypb.TraceSpan, want proxypb.SpanKind, rule string) []Violation {
	if err := AssertSpanKind(s, want); err != nil {
		return []Violation{{
			Code:     CodeWrongSpanKind,
			Rule:     rule,
			Message:  err.Error(),
			Expected: want.String(),
			Actual:   s.GetKind().String(),
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		}}
	}
	return nil
}

func checkRootSpanV(s *proxypb.TraceSpan, rule string) []Violation {
	if err := AssertRootSpan(s); err != nil {
		return []Violation{{
			Code:     CodeWrongParent,
			Rule:     rule,
			Message:  err.Error(),
			Expected: `"" (root span)`,
			Actual:   fmt.Sprintf("%q", s.GetParentSpanId()),
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		}}
	}
	return nil
}

func checkChildOfV(child, parent *proxypb.TraceSpan, rule string) []Violation {
	if err := AssertChildOf(child, parent); err != nil {
		return []Violation{{
			Code:     CodeWrongParent,
			Rule:     rule,
			Message:  err.Error(),
			Expected: fmt.Sprintf("%s/%s", parent.GetTraceId(), parent.GetSpanId()),
			Actual:   fmt.Sprintf("%s/%s", child.GetTraceId(), child.GetParentSpanId()),
			TraceID:  child.GetTraceId(),
			SpanID:   child.GetSpanId(),
			SpanName: child.GetName(),
		}}
	}
	return nil
}

func checkAttrV(s *proxypb.TraceSpan, key string, matcher AttrMatcher, rule string) []Violation {
	if s == nil {
		return nil
	}
	if matcher == nil {
		matcher = MatchPresent()
	}
	attrs := s.GetAttributes()
	if attrs == nil {
		_, expected, _ := matcher.Match(nil)
		return []Violation{{
			Code:     CodeMissingAttribute,
			Rule:     rule,
			Message:  fmt.Sprintf("span %q missing attribute %q", s.GetName(), key),
			Expected: expected,
			Actual:   "<missing>",
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		}}
	}
	av, ok := attrs[key]
	if !ok || av == nil || av.GetValue() == nil {
		_, expected, _ := matcher.Match(nil)
		return []Violation{{
			Code:     CodeMissingAttribute,
			Rule:     rule,
			Message:  fmt.Sprintf("span %q missing attribute %q", s.GetName(), key),
			Expected: expected,
			Actual:   "<missing>",
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		}}
	}
	matched, expected, actual := matcher.Match(av)
	if !matched {
		code := CodeWrongAttributeValue
		if !sameAttrType(expected, av) {
			code = CodeWrongAttributeType
		}
		return []Violation{{
			Code:     code,
			Rule:     rule,
			Message:  fmt.Sprintf("span %q attribute %q mismatch: expected %s, got %s", s.GetName(), key, expected, actual),
			Expected: expected,
			Actual:   actual,
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		}}
	}
	return nil
}

func sameAttrType(expectedDesc string, av *proxypb.AttributeValue) bool {
	if av == nil || av.GetValue() == nil {
		return false
	}
	if expectedDesc == "present attribute value" {
		return true
	}
	switch av.GetValue().(type) {
	case *proxypb.AttributeValue_StringValue:
		return strings.HasPrefix(expectedDesc, "string(") ||
			expectedDesc == "non-empty string" ||
			strings.HasPrefix(expectedDesc, "string containing ") ||
			strings.HasPrefix(expectedDesc, "string one of ") ||
			expectedDesc == "any string"
	case *proxypb.AttributeValue_Int64Value:
		return strings.HasPrefix(expectedDesc, "int64(") || expectedDesc == "any int64"
	case *proxypb.AttributeValue_DoubleValue:
		return strings.HasPrefix(expectedDesc, "double(")
	case *proxypb.AttributeValue_BoolValue:
		return strings.HasPrefix(expectedDesc, "bool(") || expectedDesc == "any bool"
	case *proxypb.AttributeValue_StringList:
		return strings.HasPrefix(expectedDesc, "string_list(")
	case *proxypb.AttributeValue_Int64List:
		return strings.HasPrefix(expectedDesc, "int64_list(")
	case *proxypb.AttributeValue_DoubleList:
		return strings.HasPrefix(expectedDesc, "double_list(")
	case *proxypb.AttributeValue_BoolList:
		return strings.HasPrefix(expectedDesc, "bool_list(")
	default:
		return false
	}
}

func checkAttrAbsentV(s *proxypb.TraceSpan, key string, rule string) []Violation {
	if err := AssertAttributeAbsent(s, key); err != nil {
		return []Violation{{
			Code:     CodeUnexpectedAttribute,
			Rule:     rule,
			Message:  err.Error(),
			Expected: fmt.Sprintf("attribute %q absent", key),
			Actual:   FormatAttrValue(s.GetAttributes()[key]),
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		}}
	}
	return nil
}

func checkEventV(s *proxypb.TraceSpan, name string, rule string, checks ...AttrCheck) []Violation {
	if err := AssertEvent(s, name, checks...); err != nil {
		return []Violation{{
			Code:     CodeMissingEvent,
			Rule:     rule,
			Message:  err.Error(),
			Expected: fmt.Sprintf("event %q", name),
			Actual:   fmt.Sprintf("%v", eventNames(s)),
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		}}
	}
	return nil
}

func checkEventAbsentV(s *proxypb.TraceSpan, name string, rule string) []Violation {
	for _, ev := range s.GetEvents() {
		if ev != nil && ev.GetName() == name {
			return []Violation{{
				Code:     CodeUnexpectedSpan,
				Rule:     rule,
				Message:  fmt.Sprintf("span %q unexpectedly contains event %q", s.GetName(), name),
				Expected: fmt.Sprintf("event %q absent", name),
				Actual:   name,
				TraceID:  s.GetTraceId(),
				SpanID:   s.GetSpanId(),
				SpanName: s.GetName(),
			}}
		}
	}
	return nil
}

func eventNames(s *proxypb.TraceSpan) []string {
	var out []string
	for _, ev := range s.GetEvents() {
		if ev != nil {
			out = append(out, ev.GetName())
		}
	}
	return out
}

func checkStatusV(s *proxypb.TraceSpan, wantCode proxypb.SpanStatusCode, requireNonEmptyMsg bool, msgContains string, rule string) []Violation {
	var out []Violation
	if s.GetStatusCode() != wantCode {
		out = append(out, Violation{
			Code:     CodeWrongStatusCode,
			Rule:     rule,
			Message:  fmt.Sprintf("span %q status_code = %s (message %q), want %s", s.GetName(), s.GetStatusCode(), s.GetStatusMessage(), wantCode),
			Expected: wantCode.String(),
			Actual:   s.GetStatusCode().String(),
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		})
	}
	if requireNonEmptyMsg && s.GetStatusMessage() == "" {
		out = append(out, Violation{
			Code:     CodeWrongStatusMessage,
			Rule:     rule + ".message",
			Message:  fmt.Sprintf("span %q has empty status_message on error span", s.GetName()),
			Expected: "non-empty status_message",
			Actual:   `""`,
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		})
	}
	if msgContains != "" && !strings.Contains(s.GetStatusMessage(), msgContains) {
		out = append(out, Violation{
			Code:     CodeWrongStatusMessage,
			Rule:     rule + ".message",
			Message:  fmt.Sprintf("span %q status_message %q does not contain %q", s.GetName(), s.GetStatusMessage(), msgContains),
			Expected: fmt.Sprintf("contains %q", msgContains),
			Actual:   fmt.Sprintf("%q", s.GetStatusMessage()),
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		})
	}
	return out
}

func checkCommonAttrs(s *proxypb.TraceSpan, profile Profile, projectID, fullResource, shortID string, devs *deviationCollector, rulePrefix string) []Violation {
	var out []Violation
	if projectID != "" {
		out = append(out, checkAttrV(s, "gcp.project_id", MatchString(projectID), rulePrefix+".attr.gcp_project_id")...)
	} else {
		out = append(out, checkAttrV(s, "gcp.project_id", MatchStringNonEmpty(), rulePrefix+".attr.gcp_project_id")...)
	}
	out = append(out, checkAttrV(s, "messaging.system", MatchString("gcp_pubsub"), rulePrefix+".attr.messaging_system")...)
	if shortID != "" {
		out = append(out, checkAttrV(s, "messaging.destination.name", MatchString(shortID), rulePrefix+".attr.destination_name")...)
	} else {
		out = append(out, checkAttrV(s, "messaging.destination.name", MatchStringNonEmpty(), rulePrefix+".attr.destination_name")...)
	}
	if profile.RequireGCPResourceName {
		if fullResource != "" {
			out = append(out, checkAttrV(s, "gcp.resource.name", MatchString(fullResource), rulePrefix+".attr.gcp_resource_name")...)
		} else {
			out = append(out, checkAttrV(s, "gcp.resource.name", MatchStringNonEmpty(), rulePrefix+".attr.gcp_resource_name")...)
		}
	} else if _, ok := AttrString(s.GetAttributes(), "gcp.resource.name"); !ok {
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.missing_gcp_resource_name",
			ExpectedGo:  "gcp.resource.name present",
			Observed:    "omitted",
			SpanName:    s.GetName(),
			Description: "C++ SDK omits gcp.resource.name on Pub/Sub spans",
		})
	}
	return out
}

func checkZeroAttributesV(s *proxypb.TraceSpan, rule string) []Violation {
	if len(s.GetAttributes()) == 0 {
		return nil
	}
	keys := make([]string, 0, len(s.GetAttributes()))
	for k := range s.GetAttributes() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return []Violation{{
		Code:     CodeUnexpectedAttribute,
		Rule:     rule,
		Message:  fmt.Sprintf("span %q expected 0 attributes, got %v", s.GetName(), keys),
		Expected: "0 attributes",
		Actual:   fmt.Sprintf("%v", keys),
		TraceID:  s.GetTraceId(),
		SpanID:   s.GetSpanId(),
		SpanName: s.GetName(),
	}}
}

func cppEnvelopeSize(data []byte, attrs map[string]string, orderingKey, msgID string) int64 {
	size := int64(20 + len(data) + len(msgID) + len(orderingKey))
	for k, v := range attrs {
		size += int64(len(k) + len(v))
	}
	return size
}

// ---------------------------------------------------------------------------
// Publish Flow Verification
// ---------------------------------------------------------------------------

func expandPublishMessages(pubExp *PublishExpectation) []PublishMessageExpectation {
	if len(pubExp.Messages) > 0 {
		out := make([]PublishMessageExpectation, len(pubExp.Messages))
		copy(out, pubExp.Messages)
		for i := range out {
			if out[i].OrderingKey == "" {
				out[i].OrderingKey = pubExp.OrderingKey
			}
			if out[i].ParentTraceparent == "" {
				out[i].ParentTraceparent = pubExp.ParentTraceparent
			}
			if !out[i].BlockingPublisher {
				out[i].BlockingPublisher = pubExp.BlockingPublisher
			}
			if !out[i].FlowControlIgnored {
				out[i].FlowControlIgnored = pubExp.FlowControlIgnored
			}
			if out[i].RequireFlowControlSpan == nil {
				out[i].RequireFlowControlSpan = pubExp.RequireFlowControlSpan
			}
			if out[i].RequireBatchingSpan == nil {
				out[i].RequireBatchingSpan = pubExp.RequireBatchingSpan
			}
			if !out[i].Hedged {
				out[i].Hedged = pubExp.Hedged
			}
			if out[i].MinHedgedAttempts == 0 {
				out[i].MinHedgedAttempts = pubExp.MinHedgedAttempts
			}
			if !out[i].ExpectError && pubExp.ExpectError {
				out[i].ExpectError = true
			}
			if out[i].ErrorKind == PublishErrorAuto {
				out[i].ErrorKind = pubExp.ErrorKind
			}
			if out[i].ErrorSubstring == "" {
				out[i].ErrorSubstring = pubExp.ErrorSubstring
			}
			if len(pubExp.RequireAttributes) > 0 {
				merged := make(map[string]AttrMatcher, len(pubExp.RequireAttributes)+len(out[i].RequireAttributes))
				for k, v := range pubExp.RequireAttributes {
					merged[k] = v
				}
				for k, v := range out[i].RequireAttributes {
					merged[k] = v
				}
				out[i].RequireAttributes = merged
			}
		}
		return out
	}

	n := pubExp.MessageCount
	if len(pubExp.MessageIDs) > n {
		n = len(pubExp.MessageIDs)
	}
	if len(pubExp.WireTraceparents) > n {
		n = len(pubExp.WireTraceparents)
	}
	if len(pubExp.BodySizes) > n {
		n = len(pubExp.BodySizes)
	}
	if n == 0 && len(pubExp.Batches) > 0 {
		total := 0
		for _, b := range pubExp.Batches {
			if b.MessageCount > 0 {
				total += b.MessageCount
			}
		}
		if total > 0 {
			n = total
		} else {
			n = len(pubExp.Batches)
		}
	}
	if n == 0 && (pubExp.ExpectError || pubExp.BatchCount > 0 || pubExp.Hedged || pubExp.MinHedgedAttempts > 0) {
		n = 1
	}
	if n == 0 {
		return nil
	}
	out := make([]PublishMessageExpectation, n)
	for i := 0; i < n; i++ {
		m := PublishMessageExpectation{
			OrderingKey:            pubExp.OrderingKey,
			ParentTraceparent:      pubExp.ParentTraceparent,
			RequireAttributes:      pubExp.RequireAttributes,
			BlockingPublisher:      pubExp.BlockingPublisher,
			FlowControlIgnored:     pubExp.FlowControlIgnored,
			RequireFlowControlSpan: pubExp.RequireFlowControlSpan,
			RequireBatchingSpan:    pubExp.RequireBatchingSpan,
			Hedged:                 pubExp.Hedged,
			MinHedgedAttempts:      pubExp.MinHedgedAttempts,
			ExpectError:            pubExp.ExpectError,
			ErrorKind:              pubExp.ErrorKind,
			ErrorSubstring:         pubExp.ErrorSubstring,
		}
		if i < len(pubExp.MessageIDs) {
			m.MessageID = pubExp.MessageIDs[i]
		}
		if i < len(pubExp.WireTraceparents) {
			m.WireTraceparent = pubExp.WireTraceparents[i]
		}
		if i < len(pubExp.BodySizes) {
			sz := pubExp.BodySizes[i]
			m.BodySize = &sz
		}
		out[i] = m
	}
	return out
}

func isOrphanFlowControlSpan(idx *SpanIndex, fc *proxypb.TraceSpan) bool {
	if fc == nil {
		return false
	}
	pid := NormalizeHex(fc.GetParentSpanId())
	if pid == "" {
		return true
	}
	_, parentExists := idx.ByKey[SpanKey{TraceID: NormalizeHex(fc.GetTraceId()), SpanID: pid}]
	return !parentExists
}

type pubCreateMeta struct {
	normTraceID         string
	normSpanID          string
	normParentSpanID    string
	rawParentEmpty      bool
	msgID               string
	hasMsgID            bool
	orderingKey         string
	hasOrderingKey      bool
	sizeVal             int64
	hasSize             bool
	isErr               bool
	isBlocking          bool
	hedgedStartCount    int
	hasValidHedgedStart bool
	hasHedgedEnd        bool
	hasFCChild          bool
	hasBatchChild       bool
	autoErrKind         PublishErrorKind
	statusMsg           string
	attrs               map[string]*proxypb.AttributeValue
}

func buildPubCreateMeta(idx *SpanIndex, profile Profile, c *proxypb.TraceSpan) pubCreateMeta {
	attrs := c.GetAttributes()
	msgID, hasMsgID := AttrString(attrs, "messaging.message.id")
	orderingKey, hasOrderingKey := AttrString(attrs, "messaging.gcp_pubsub.message.ordering_key")
	var sizeVal int64
	var hasSize bool
	if !profile.UseEnvelopeSize {
		sizeVal, hasSize = AttrInt64(attrs, "messaging.message.body.size")
	} else {
		sizeVal, hasSize = AttrInt64(attrs, "messaging.message.envelope.size")
	}
	var isBlocking bool
	if profile.AllowBlockingPublisher {
		fn, hasFn := AttrString(attrs, profile.CodeFunctionAttrKey)
		isBlocking = hasFn && fn == "pubsub::BlockingPublisher::Publish"
	}
	children := idx.ChildrenOf(c)
	hasFCChild := false
	hasBatchChild := false
	for _, ch := range children {
		if ch == nil {
			continue
		}
		switch ch.GetName() {
		case "publisher flow control":
			hasFCChild = true
		case "publisher batching":
			hasBatchChild = true
		}
	}
	hedgedStartCount := countEventsNamed(c, "publish start (hedged)")
	hasValidHedgedStart := hedgedStartCount > 0 &&
		AssertEvent(c, "publish start (hedged)", CheckAttr("messaging.batch.message_count", MatchInt64Min(1))) == nil
	return pubCreateMeta{
		normTraceID:         NormalizeHex(c.GetTraceId()),
		normSpanID:          NormalizeHex(c.GetSpanId()),
		normParentSpanID:    NormalizeHex(c.GetParentSpanId()),
		rawParentEmpty:      c.GetParentSpanId() == "",
		msgID:               msgID,
		hasMsgID:            hasMsgID,
		orderingKey:         orderingKey,
		hasOrderingKey:      hasOrderingKey,
		sizeVal:             sizeVal,
		hasSize:             hasSize,
		isErr:               c.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
		isBlocking:          isBlocking,
		hedgedStartCount:    hedgedStartCount,
		hasValidHedgedStart: hasValidHedgedStart,
		hasHedgedEnd:        AssertEvent(c, "publish end (hedged)") == nil,
		hasFCChild:          hasFCChild,
		hasBatchChild:       hasBatchChild,
		autoErrKind:         resolvePublishErrorKindFromMeta(c, hasFCChild, hasBatchChild, profile),
		statusMsg:           c.GetStatusMessage(),
		attrs:               attrs,
	}
}

func resolvePublishErrorKindFromMeta(cs *proxypb.TraceSpan, hasFC, hasBatch bool, profile Profile) PublishErrorKind {
	hasPubStart := countEventsNamed(cs, "publish start") > 0 || countEventsNamed(cs, "gl-cpp.publish_start") > 0
	if hasPubStart || len(cs.GetLinks()) > 0 {
		return PublishErrorRPC
	}
	if profile.Name == ProfileCPP {
		if hasBatch {
			return PublishErrorOrderingRejected
		}
		if hasFC {
			return PublishErrorFlowControl
		}
		return PublishErrorPreValidation
	}
	if hasBatch {
		return PublishErrorRPC
	}
	if hasFC {
		return PublishErrorOversized
	}
	return PublishErrorPreValidation
}

type pubMsgScoreMeta struct {
	msg            PublishMessageExpectation
	parsedPTP      Traceparent
	hasValidPTP    bool
	parsedWTP      Traceparent
	hasValidWTP    bool
	requiresWireTP bool
	wantSize       int64
	hasWantSize    bool
	exactSize      bool
}

func buildPubMsgScoreMeta(profile Profile, m PublishMessageExpectation) pubMsgScoreMeta {
	pm := pubMsgScoreMeta{
		msg:            m,
		requiresWireTP: m.WireTraceparent != "" && !(profile.AllowBlockingPublisher && m.BlockingPublisher),
	}
	if m.ParentTraceparent != "" {
		if ptp, err := ParseTraceparent(m.ParentTraceparent); err == nil {
			pm.parsedPTP = ptp
			pm.hasValidPTP = true
		}
	}
	if pm.requiresWireTP {
		if wtp, err := ParseTraceparent(m.WireTraceparent); err == nil {
			pm.parsedWTP = wtp
			pm.hasValidWTP = true
		}
	}
	if !profile.UseEnvelopeSize {
		if m.BodySize != nil {
			pm.wantSize = *m.BodySize
			pm.hasWantSize = true
			pm.exactSize = true
		} else if m.Data != nil {
			pm.wantSize = int64(len(m.Data))
			pm.hasWantSize = true
			pm.exactSize = true
		}
	} else {
		if m.EnvelopeSize != nil {
			pm.wantSize = *m.EnvelopeSize
			pm.hasWantSize = true
			pm.exactSize = true
		} else if m.Data != nil {
			pm.wantSize = cppEnvelopeSize(m.Data, m.Attributes, m.OrderingKey, "")
			pm.hasWantSize = true
		} else if m.BodySize != nil {
			pm.wantSize = 20 + *m.BodySize
			pm.hasWantSize = true
		}
	}
	return pm
}

func scorePublishCreateEdgeFast(profile Profile, pm pubMsgScoreMeta, cm pubCreateMeta) int64 {
	var score int64
	m := pm.msg

	if !m.ExpectError && m.MessageID != "" {
		if cm.hasMsgID && cm.msgID == m.MessageID {
			score += 200_000
		}
	}
	if cm.isErr == m.ExpectError {
		score += 35_000
	}
	if m.ExpectError && m.ErrorSubstring != "" && strings.Contains(cm.statusMsg, m.ErrorSubstring) {
		score += 25_000
	}
	if pm.hasValidPTP {
		if cm.normTraceID == pm.parsedPTP.TraceID && cm.normParentSpanID == pm.parsedPTP.SpanID {
			score += 25_000
		}
	}
	if m.ExpectError && m.ErrorKind != PublishErrorAuto && cm.isErr && cm.autoErrKind == m.ErrorKind {
		score += 20_000
	}
	if m.OrderingKey != "" {
		if cm.hasOrderingKey && cm.orderingKey == m.OrderingKey {
			score += 18_000
		}
	}
	if pm.hasWantSize && cm.hasSize {
		if pm.exactSize {
			if cm.sizeVal == pm.wantSize {
				score += 10_000
			}
		} else if cm.sizeVal >= pm.wantSize {
			score += 10_000
		}
	}
	if len(m.RequireAttributes) > 0 {
		var matchedAttrs int64
		for k, matcher := range m.RequireAttributes {
			if matcher == nil {
				matcher = MatchPresent()
			}
			if av, ok := cm.attrs[k]; ok && av != nil && av.GetValue() != nil {
				if matchedAttr, _, _ := matcher.Match(av); matchedAttr {
					matchedAttrs++
				}
			}
		}
		if int64(len(m.RequireAttributes))*15_000 <= publishRequireAttrsCap {
			score += matchedAttrs * 15_000
		} else {
			score += normalizeSettleBonus(matchedAttrs, int64(len(m.RequireAttributes)), publishRequireAttrsCap)
		}
	}
	if !m.ExpectError && profile.Name == ProfileGo {
		if m.Hedged || m.MinHedgedAttempts > 0 {
			if cm.hedgedStartCount > 0 {
				score += 1_000
				if cm.hasHedgedEnd {
					score += 4_000
				}
			}
			if cm.hasValidHedgedStart {
				score += 2_000
			}
		}
		if m.MinHedgedAttempts > 0 && cm.hedgedStartCount >= m.MinHedgedAttempts {
			score += 8_000
		}
	}
	if profile.AllowBlockingPublisher {
		if m.BlockingPublisher && cm.isBlocking {
			score += 8_000
		} else if !m.BlockingPublisher && !cm.isBlocking {
			score += 3_000
		}
	}
	if m.ParentTraceparent == "" && cm.rawParentEmpty {
		score += 5_000
	}
	if m.OrderingKey == "" {
		if (cm.hasOrderingKey && cm.orderingKey == "") || (!cm.hasOrderingKey && !profile.RequireUnconditionalOrderingKey) {
			score += 3_000
		}
	}

	resolvedKind := PublishErrorAuto
	if m.ExpectError {
		if m.ErrorKind != PublishErrorAuto {
			resolvedKind = m.ErrorKind
		} else {
			resolvedKind = cm.autoErrKind
		}
	}
	if expectFlowControlChild(m, resolvedKind, profile) == cm.hasFCChild {
		score += 3_000
	}
	if expectBatchingChild(m, resolvedKind, profile) == cm.hasBatchChild {
		score += 3_000
	}

	return score
}

func scorePublishOrphanFCEdge(profile Profile, m PublishMessageExpectation, fc *proxypb.TraceSpan) int64 {
	var score int64
	if fc.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
		score += 35_000
	}
	if m.ErrorSubstring != "" && strings.Contains(fc.GetStatusMessage(), m.ErrorSubstring) {
		score += 25_000
	}
	if m.ParentTraceparent != "" {
		if ptp, err := ParseTraceparent(m.ParentTraceparent); err == nil && NormalizeHex(fc.GetTraceId()) == ptp.TraceID {
			score += 25_000
		}
	}
	if fc.GetParentSpanId() != "" {
		score += 5_000
	}
	if fc.GetInstrumentationScopeName() == profile.ExpectedScope {
		score += 3_000
	}
	return score
}

func isStrictValidSampledCreateSpan(c *proxypb.TraceSpan, profile Profile, topicID string) bool {
	if c == nil {
		return false
	}
	if profile.ExpectedScope != "" {
		if c.GetInstrumentationScopeName() != profile.ExpectedScope {
			return false
		}
	} else if !isCandidatePubSubSpan(c, profile) {
		return false
	}
	if c.GetKind() != proxypb.SpanKind_SPAN_KIND_PRODUCER {
		return false
	}
	if (c.GetTraceFlags() & 0x01) == 0 {
		return false
	}
	msgSys, ok := AttrString(c.GetAttributes(), "messaging.system")
	if !ok || msgSys != "gcp_pubsub" {
		return false
	}
	dst, ok := AttrString(c.GetAttributes(), "messaging.destination.name")
	if !ok {
		return false
	}
	if topicID != "" {
		return dst == topicID
	}
	return dst != ""
}

func verifyPublish(
	idx *SpanIndex,
	sortedSpans []*proxypb.TraceSpan,
	profile Profile,
	projectID, fullTopic, topicID string,
	pubExp *PublishExpectation,
	strictExtra bool,
	matched map[*proxypb.TraceSpan]bool,
	devs *deviationCollector,
) []Violation {
	var failures []Violation
	msgs := expandPublishMessages(pubExp)

	createSpanName := topicID + " create"
	var createCandidates []*proxypb.TraceSpan
	if topicID != "" {
		createCandidates = SortSpansDeterministic(idx.ByName[createSpanName])
	} else {
		for _, s := range sortedSpans {
			if isCandidatePubSubSpan(s, profile) && strings.HasSuffix(s.GetName(), " create") && len(s.GetName()) > len(" create") {
				topicID = strings.TrimSuffix(s.GetName(), " create")
				createSpanName = topicID + " create"
				createCandidates = SortSpansDeterministic(idx.ByName[createSpanName])
				break
			}
		}
		if topicID == "" {
			for _, s := range sortedSpans {
				if isCandidatePubSubSpan(s, profile) && strings.HasSuffix(s.GetName(), " publish") && len(s.GetName()) > len(" publish") {
					topicID = strings.TrimSuffix(s.GetName(), " publish")
					createSpanName = topicID + " create"
					break
				}
			}
		}
	}

	publishSpanName := topicID + " publish"
	var pubSpans []*proxypb.TraceSpan
	if topicID != "" {
		pubSpans = SortSpansDeterministic(idx.ByName[publishSpanName])
	}

	fcSpans := SortSpansDeterministic(idx.ByName["publisher flow control"])
	batchingSpans := SortSpansDeterministic(idx.ByName["publisher batching"])

	usedCreates := make(map[*proxypb.TraceSpan]bool)
	usedFCs := make(map[*proxypb.TraceSpan]bool)

	type indexedPublishMsg struct {
		origIdx int
		msg     PublishMessageExpectation
	}
	var droppedMsgs, activeMsgs []indexedPublishMsg
	activeParentTraceIDs := make(map[string]bool)
	for i, m := range msgs {
		item := indexedPublishMsg{origIdx: i, msg: m}
		if m.ExpectDroppedUnsampled {
			droppedMsgs = append(droppedMsgs, item)
		} else {
			activeMsgs = append(activeMsgs, item)
			if m.ParentTraceparent != "" {
				if tp, err := ParseTraceparent(m.ParentTraceparent); err == nil {
					activeParentTraceIDs[tp.TraceID] = true
				}
			}
		}
	}

	anyDroppedUnsampled := len(droppedMsgs) > 0

	// Stage 1a: Pre-match explicit trace leaks from unsampled (-00) dropped messages.
	for _, item := range droppedMsgs {
		m := item.msg
		if m.ParentTraceparent != "" {
			if tp, err := ParseTraceparent(m.ParentTraceparent); err == nil {
				for _, cs := range createCandidates {
					if usedCreates[cs] {
						continue
					}
					if NormalizeHex(cs.GetTraceId()) == tp.TraceID &&
						(NormalizeHex(cs.GetParentSpanId()) == tp.SpanID || !activeParentTraceIDs[tp.TraceID]) {
						failures = append(failures, Violation{
							Code:     CodeUnexpectedSpan,
							Rule:     "publish.unsampled_dropped",
							Message:  fmt.Sprintf("unsampled parent traceparent %q should not export %q", m.ParentTraceparent, cs.GetName()),
							Expected: "create span dropped",
							Actual:   cs.GetSpanId(),
							TraceID:  cs.GetTraceId(),
							SpanID:   cs.GetSpanId(),
							SpanName: cs.GetName(),
						})
						usedCreates[cs] = true
						matched[cs] = true
					}
				}
			}
		}
	}

	var wireCreateSpans []*proxypb.TraceSpan
	anyWireRPCExpectError := false
	verifyMatchedCreate := func(m PublishMessageExpectation, cs *proxypb.TraceSpan) {
		errKind := resolvePublishErrorKind(idx, cs, m, profile)
		reachesWire := !m.BlockingPublisher &&
			(!m.ExpectError || errKind == PublishErrorRPC)

		effTopic := topicID
		if effTopic == "" && strings.HasSuffix(cs.GetName(), " create") {
			effTopic = strings.TrimSuffix(cs.GetName(), " create")
		}

		failures = append(failures, checkSpanScope(cs, profile, "publish.create.scope", devs)...)
		failures = append(failures, checkSpanKindV(cs, proxypb.SpanKind_SPAN_KIND_PRODUCER, "publish.create.span_kind")...)

		// Parent check.
		if m.ParentTraceparent != "" {
			if err := AssertParentFromTraceparent(cs, m.ParentTraceparent); err != nil {
				failures = append(failures, Violation{
					Code:     CodeBrokenTraceparent,
					Rule:     "publish.create.parent_traceparent",
					Message:  err.Error(),
					Expected: m.ParentTraceparent,
					Actual:   fmt.Sprintf("%s/%s", cs.GetTraceId(), cs.GetParentSpanId()),
					TraceID:  cs.GetTraceId(),
					SpanID:   cs.GetSpanId(),
					SpanName: cs.GetName(),
				})
			}
		} else {
			failures = append(failures, checkRootSpanV(cs, "publish.create.root")...)
		}

		// Outgoing wire traceparent check.
		if m.WireTraceparent != "" {
			if profile.AllowBlockingPublisher && m.BlockingPublisher {
				devs.add(Deviation{
					Category:    "propagation",
					Rule:        "publish.blocking_publisher_no_batch_or_injection",
					ExpectedGo:  "Injects googclient_traceparent and emits batching/publish spans",
					Observed:    "BlockingPublisher skips injection and batching/publish spans",
					SpanName:    cs.GetName(),
					Description: "C++ BlockingPublisher does not inject googclient_traceparent or create flow control/batching/publish spans",
				})
			} else if err := AssertTraceparentMatchesSpan(m.WireTraceparent, cs); err != nil {
				failures = append(failures, Violation{
					Code:     CodeBrokenTraceparent,
					Rule:     "publish.create.wire_traceparent",
					Message:  err.Error(),
					Expected: fmt.Sprintf("trace_id=%s span_id=%s", cs.GetTraceId(), cs.GetSpanId()),
					Actual:   m.WireTraceparent,
					TraceID:  cs.GetTraceId(),
					SpanID:   cs.GetSpanId(),
					SpanName: cs.GetName(),
				})
			}
		}

		// Attributes on create.
		failures = append(failures, checkCommonAttrs(cs, profile, projectID, fullTopic, effTopic, devs, "publish.create")...)
		if profile.Name == ProfileGo {
			failures = append(failures, checkAttrV(cs, "code.function", MatchString("Publish"), "publish.create.attr.code_function")...)
		} else {
			wantFn := "pubsub::PublisherConnection::Publish"
			if m.BlockingPublisher {
				wantFn = "pubsub::BlockingPublisher::Publish"
			}
			failures = append(failures, checkAttrV(cs, profile.CodeFunctionAttrKey, MatchString(wantFn), "publish.create.attr.code_function_name")...)
			devs.add(Deviation{
				Category:    "attribute",
				Rule:        "attr.code_function_key",
				ExpectedGo:  "code.function",
				Observed:    profile.CodeFunctionAttrKey,
				SpanName:    cs.GetName(),
				Description: "C++ uses semconv incubating code.function.name instead of code.function",
			})
			failures = append(failures, checkAttrV(cs, "messaging.operation.type", MatchString("create"), "publish.create.attr.operation_type")...)
			failures = append(failures, checkAttrV(cs, "gl-cpp.status_code", MatchStringNonEmpty(), "publish.create.attr.gl_cpp_status_code")...)
			devs.add(Deviation{
				Category:    "attribute",
				Rule:        "attr.cpp_extra_semconv",
				ExpectedGo:  "omits messaging.operation.type/gl-cpp.*/server.address/thread.id",
				Observed:    "sets messaging.operation.type and gl-cpp.status_code",
				SpanName:    cs.GetName(),
				Description: "C++ adds extra semconv and gl-cpp.* attributes on publish/subscribe spans",
			})
		}

		// Body size vs envelope size.
		if !profile.UseEnvelopeSize {
			wantBody := MatchAnyInt64()
			if m.BodySize != nil {
				wantBody = MatchInt64(*m.BodySize)
			} else if m.Data != nil {
				wantBody = MatchInt64(int64(len(m.Data)))
			}
			failures = append(failures, checkAttrV(cs, "messaging.message.body.size", wantBody, "publish.create.attr.body_size")...)
		} else {
			wantEnv := MatchAnyInt64()
			if m.EnvelopeSize != nil {
				wantEnv = MatchInt64(*m.EnvelopeSize)
			} else if m.Data != nil {
				wantEnv = MatchInt64Min(cppEnvelopeSize(m.Data, m.Attributes, m.OrderingKey, ""))
			} else if m.BodySize != nil {
				wantEnv = MatchInt64Min(20 + *m.BodySize)
			}
			failures = append(failures, checkAttrV(cs, "messaging.message.envelope.size", wantEnv, "publish.create.attr.envelope_size")...)
			devs.add(Deviation{
				Category:    "attribute",
				Rule:        "attr.envelope_size_vs_body_size",
				ExpectedGo:  "messaging.message.body.size",
				Observed:    "messaging.message.envelope.size",
				SpanName:    cs.GetName(),
				Description: "C++ records messaging.message.envelope.size instead of messaging.message.body.size",
			})
		}

		// Ordering key (unconditional in Go, optional when empty in C++).
		if profile.RequireUnconditionalOrderingKey || m.OrderingKey != "" {
			failures = append(failures, checkAttrV(cs, "messaging.gcp_pubsub.message.ordering_key", MatchString(m.OrderingKey), "publish.create.attr.ordering_key")...)
		} else if _, hasKey := AttrString(cs.GetAttributes(), "messaging.gcp_pubsub.message.ordering_key"); !hasKey {
			devs.add(Deviation{
				Category:    "attribute",
				Rule:        "attr.omits_empty_ordering_key",
				ExpectedGo:  `messaging.gcp_pubsub.message.ordering_key=""`,
				Observed:    "omitted when empty",
				SpanName:    cs.GetName(),
				Description: "C++ omits messaging.gcp_pubsub.message.ordering_key when ordering_key is empty",
			})
		}

		// Message ID (unconditional in Go, omitted on error in C++).
		if !m.ExpectError {
			wantID := MatchStringNonEmpty()
			if m.MessageID != "" {
				wantID = MatchString(m.MessageID)
			}
			failures = append(failures, checkAttrV(cs, "messaging.message.id", wantID, "publish.create.attr.message_id")...)
		} else if profile.RequireUnconditionalCreateMessageID {
			failures = append(failures, checkAttrV(cs, "messaging.message.id", MatchString(""), "publish.create.attr.message_id_on_error")...)
		} else if _, hasID := AttrString(cs.GetAttributes(), "messaging.message.id"); !hasID {
			devs.add(Deviation{
				Category:    "attribute",
				Rule:        "attr.omits_error_message_id",
				ExpectedGo:  `messaging.message.id="" on error`,
				Observed:    "omitted on error",
				SpanName:    cs.GetName(),
				Description: "C++ omits messaging.message.id when publish fails",
			})
		}

		for k, matcher := range m.RequireAttributes {
			failures = append(failures, checkAttrV(cs, k, matcher, "publish.create.attr.custom."+k)...)
		}

		// Status & events on create.
		if !m.ExpectError {
			if profile.Name == ProfileGo {
				failures = append(failures, checkStatusV(cs, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "publish.create.status")...)
				failures = append(failures, checkEventV(cs, "publish start", "publish.create.event.publish_start",
					CheckAttr("messaging.batch.message_count", MatchInt64Min(1)))...)
				failures = append(failures, checkEventV(cs, "publish end", "publish.create.event.publish_end")...)
				if m.Hedged || m.MinHedgedAttempts > 0 {
					failures = append(failures, checkEventV(cs, "publish start (hedged)", "publish.create.event.publish_start_hedged",
						CheckAttr("messaging.batch.message_count", MatchInt64Min(1)))...)
					failures = append(failures, checkEventV(cs, "publish end (hedged)", "publish.create.event.publish_end_hedged")...)
					if m.MinHedgedAttempts > 0 {
						gotHedged := countEventsNamed(cs, "publish start (hedged)")
						if gotHedged < m.MinHedgedAttempts {
							failures = append(failures, Violation{
								Code:     CodeMissingEvent,
								Rule:     "publish.create.event.min_hedged_attempts",
								Message:  fmt.Sprintf("span %q has %d %q events, want >= %d", cs.GetName(), gotHedged, "publish start (hedged)", m.MinHedgedAttempts),
								Expected: fmt.Sprintf(">= %d", m.MinHedgedAttempts),
								Actual:   strconv.Itoa(gotHedged),
								TraceID:  cs.GetTraceId(),
								SpanID:   cs.GetSpanId(),
								SpanName: cs.GetName(),
							})
						}
					}
				}
			} else {
				failures = append(failures, checkStatusV(cs, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, false, "", "publish.create.status")...)
				if !m.BlockingPublisher {
					failures = append(failures, checkEventV(cs, "gl-cpp.added_to_batch", "publish.create.event.cpp_added_to_batch")...)
					failures = append(failures, checkEventV(cs, "gl-cpp.publish_start", "publish.create.event.cpp_publish_start")...)
					failures = append(failures, checkEventV(cs, "gl-cpp.publish_end", "publish.create.event.cpp_publish_end")...)
					devs.add(Deviation{
						Category:    "event",
						Rule:        "events.cpp_publish_names",
						ExpectedGo:  `"publish start", "publish end"`,
						Observed:    `"gl-cpp.added_to_batch", "gl-cpp.publish_start", "gl-cpp.publish_end"`,
						SpanName:    cs.GetName(),
						Description: "C++ uses gl-cpp.* event names on <topic> create",
					})
				}
			}
		} else {
			failures = append(failures, checkStatusV(cs, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, m.ErrorSubstring, "publish.create.status")...)
			if profile.RequireExceptionEventOnError {
				failures = append(failures, checkEventV(cs, "exception", "publish.create.event.exception")...)
			}
			if profile.Name == ProfileGo && errKind == PublishErrorRPC {
				failures = append(failures, checkEventV(cs, "publish start", "publish.create.event.publish_start",
					CheckAttr("messaging.batch.message_count", MatchInt64Min(1)))...)
				failures = append(failures, checkEventAbsentV(cs, "publish end", "publish.create.event.no_publish_end_on_error")...)
			} else if profile.Name == ProfileCPP && errKind == PublishErrorRPC && !m.BlockingPublisher {
				failures = append(failures, checkEventV(cs, "gl-cpp.added_to_batch", "publish.create.event.cpp_added_to_batch")...)
				failures = append(failures, checkEventV(cs, "gl-cpp.publish_start", "publish.create.event.cpp_publish_start")...)
				failures = append(failures, checkEventV(cs, "gl-cpp.publish_end", "publish.create.event.cpp_publish_end")...)
				devs.add(Deviation{
					Category:    "event",
					Rule:        "events.cpp_publish_names",
					ExpectedGo:  `"publish start", "publish end"`,
					Observed:    `"gl-cpp.added_to_batch", "gl-cpp.publish_start", "gl-cpp.publish_end"`,
					SpanName:    cs.GetName(),
					Description: "C++ uses gl-cpp.* event names on <topic> create",
				})
			}
		}

		// Child spans of create ("publisher flow control" and "publisher batching").
		children := SortSpansDeterministic(idx.ChildrenOf(cs))
		fcChildren := filterByName(children, "publisher flow control")
		batchChildren := filterByName(children, "publisher batching")

		expectFC := expectFlowControlChild(m, errKind, profile)
		if expectFC {
			if len(fcChildren) == 0 {
				if profile.AllowOmittedFlowControlIfIgnored && m.FlowControlIgnored {
					devs.add(Deviation{
						Category:    "span",
						Rule:        "span.missing_flow_control_ignored",
						ExpectedGo:  `"publisher flow control" present`,
						Observed:    "omitted under FullPublisherAction::kIgnored",
						SpanName:    "publisher flow control",
						Description: "C++ omits flow control tracing decorator when FullPublisherAction::kIgnored is active",
					})
				} else {
					failures = append(failures, Violation{
						Code:     CodeMissingSpan,
						Rule:     "publish.flow_control.present",
						Message:  fmt.Sprintf("missing child span %q under %q (%s)", "publisher flow control", cs.GetName(), cs.GetSpanId()),
						Expected: "1 child span named \"publisher flow control\"",
						Actual:   "0",
						TraceID:  cs.GetTraceId(),
						SpanID:   cs.GetSpanId(),
						SpanName: cs.GetName(),
					})
				}
			} else {
				if len(fcChildren) > 1 {
					failures = append(failures, Violation{
						Code:     CodeUnexpectedSpan,
						Rule:     "publish.flow_control.unique",
						Message:  fmt.Sprintf("expected 1 %q child under %q, got %d", "publisher flow control", cs.GetName(), len(fcChildren)),
						Expected: "1",
						Actual:   strconv.Itoa(len(fcChildren)),
						TraceID:  cs.GetTraceId(),
						SpanID:   cs.GetSpanId(),
						SpanName: "publisher flow control",
					})
				}
				fc := fcChildren[0]
				matched[fc] = true
				usedFCs[fc] = true
				failures = append(failures, verifyFlowControlChild(fc, cs, profile, errKind == PublishErrorFlowControl, m.ErrorSubstring, devs)...)
			}
		} else if len(fcChildren) > 0 && !m.FlowControlIgnored {
			// If explicitly disallowed (e.g. BlockingPublisher or Go PreValidation), leave unmatched so extra-span guard flags it if strict.
		}

		expectBatch := expectBatchingChild(m, errKind, profile)
		if expectBatch {
			if len(batchChildren) == 0 {
				failures = append(failures, Violation{
					Code:     CodeMissingSpan,
					Rule:     "publish.batching.present",
					Message:  fmt.Sprintf("missing child span %q under %q (%s)", "publisher batching", cs.GetName(), cs.GetSpanId()),
					Expected: "1 child span named \"publisher batching\"",
					Actual:   "0",
					TraceID:  cs.GetTraceId(),
					SpanID:   cs.GetSpanId(),
					SpanName: cs.GetName(),
				})
			} else {
				if len(batchChildren) > 1 {
					failures = append(failures, Violation{
						Code:     CodeUnexpectedSpan,
						Rule:     "publish.batching.unique",
						Message:  fmt.Sprintf("expected 1 %q child under %q, got %d", "publisher batching", cs.GetName(), len(batchChildren)),
						Expected: "1",
						Actual:   strconv.Itoa(len(batchChildren)),
						TraceID:  cs.GetTraceId(),
						SpanID:   cs.GetSpanId(),
						SpanName: "publisher batching",
					})
				}
				bc := batchChildren[0]
				matched[bc] = true
				failures = append(failures, verifyBatchingChild(bc, cs, profile, devs)...)
			}
		} else if profile.Name == ProfileGo && errKind == PublishErrorOversized && len(batchChildren) > 0 {
			// Go leaks publisher batching on ErrOversizedMessage. Leave unmatched to flag if unexpectedly exported.
		}

		if m.BlockingPublisher && profile.AllowBlockingPublisher {
			devs.add(Deviation{
				Category:    "hierarchy",
				Rule:        "publish.blocking_publisher_no_batch_or_injection",
				ExpectedGo:  "Creates flow control, batching, and publish spans and injects googclient_traceparent",
				Observed:    "BlockingPublisher emits only <topic> create + gRPC span",
				SpanName:    cs.GetName(),
				Description: "C++ BlockingPublisher bypasses flow control, batching, BatchSink publish, and trace injection",
			})
		}

		if reachesWire {
			wireCreateSpans = append(wireCreateSpans, cs)
			if m.ExpectError {
				anyWireRPCExpectError = true
			}
		}
	}

	// Stage 2: Unified global maximum-cardinality maximum-weight bipartite matching
	// over activeMsgs -> (createCandidates + Go orphan "publisher flow control" spans).
	rightCreates := createCandidates
	var rightOrphanFCs []*proxypb.TraceSpan
	if profile.Name == ProfileGo {
		for _, fc := range fcSpans {
			if isOrphanFlowControlSpan(idx, fc) {
				rightOrphanFCs = append(rightOrphanFCs, fc)
			}
		}
	}

	createMetas := make([]pubCreateMeta, len(rightCreates))
	createIdxByKey := make(map[SpanKey][]int, len(rightCreates))
	for rj, cs := range rightCreates {
		cm := buildPubCreateMeta(idx, profile, cs)
		createMetas[rj] = cm
		if !usedCreates[cs] {
			sk := SpanKey{TraceID: cm.normTraceID, SpanID: cm.normSpanID}
			createIdxByKey[sk] = append(createIdxByKey[sk], rj)
		}
	}

	activeMsgMetas := make([]pubMsgScoreMeta, len(activeMsgs))
	for li, item := range activeMsgs {
		activeMsgMetas[li] = buildPubMsgScoreMeta(profile, item.msg)
	}

	var pubEdges []bipartiteEdge
	for li, pm := range activeMsgMetas {
		m := pm.msg
		if pm.requiresWireTP {
			if pm.hasValidWTP {
				sk := SpanKey{TraceID: pm.parsedWTP.TraceID, SpanID: pm.parsedWTP.SpanID}
				for _, rj := range createIdxByKey[sk] {
					pubEdges = append(pubEdges, bipartiteEdge{
						left:   li,
						right:  rj,
						weight: scorePublishCreateEdgeFast(profile, pm, createMetas[rj]),
					})
				}
			}
			continue
		}

		if !(profile.Name == ProfileGo && m.ExpectError && m.ErrorKind == PublishErrorFlowControl) {
			for rj, cs := range rightCreates {
				if usedCreates[cs] {
					continue
				}
				pubEdges = append(pubEdges, bipartiteEdge{
					left:   li,
					right:  rj,
					weight: scorePublishCreateEdgeFast(profile, pm, createMetas[rj]),
				})
			}
		}
		if profile.Name == ProfileGo && m.ExpectError && (m.ErrorKind == PublishErrorFlowControl || m.ErrorKind == PublishErrorAuto) {
			for fj, fc := range rightOrphanFCs {
				pubEdges = append(pubEdges, bipartiteEdge{
					left:   li,
					right:  len(rightCreates) + fj,
					weight: scorePublishOrphanFCEdge(profile, m, fc),
				})
			}
		}
	}

	pubAssign := solveBipartiteMatching(len(activeMsgs), len(rightCreates)+len(rightOrphanFCs), nil, nil, pubEdges)

	// Mark all matched create spans first so unmatched diagnostics accurately see unusedCreates.
	for li := range activeMsgs {
		if rj := pubAssign[li]; rj >= 0 && rj < len(rightCreates) {
			usedCreates[rightCreates[rj]] = true
		}
	}

	anyActiveMatchedCreate := false
	anyActiveUnmatchedCreate := false
	for li, item := range activeMsgs {
		m := item.msg
		rj := pubAssign[li]
		if rj >= 0 && rj < len(rightCreates) {
			cs := rightCreates[rj]
			matched[cs] = true
			anyActiveMatchedCreate = true
			verifyMatchedCreate(m, cs)
			continue
		}
		if rj >= len(rightCreates) {
			fc := rightOrphanFCs[rj-len(rightCreates)]
			matched[fc] = true
			usedFCs[fc] = true
			failures = append(failures, verifyGoFlowControlErrorSpan(fc, profile, m.ErrorSubstring, devs)...)
			continue
		}

		// Unmatched active message: preserve exact negative diagnostic codes.
		requiresWireTP := m.WireTraceparent != "" && !(profile.AllowBlockingPublisher && m.BlockingPublisher)
		if requiresWireTP {
			anyActiveUnmatchedCreate = true
			unusedCreates := 0
			for _, cs := range rightCreates {
				if !usedCreates[cs] {
					unusedCreates++
				}
			}
			tp, tpErr := ParseTraceparent(m.WireTraceparent)
			if tpErr != nil {
				failures = append(failures, Violation{
					Code:     CodeBrokenTraceparent,
					Rule:     "publish.create.wire_traceparent",
					Message:  fmt.Sprintf("wire googclient_traceparent %q is invalid W3C traceparent: %v", m.WireTraceparent, tpErr),
					Expected: "valid W3C traceparent",
					Actual:   m.WireTraceparent,
				})
			} else if unusedCreates > 0 {
				failures = append(failures, Violation{
					Code:     CodeBrokenTraceparent,
					Rule:     "publish.create.wire_traceparent",
					Message:  fmt.Sprintf("no <topic> create span matches wire googclient_traceparent %q (trace_id=%s span_id=%s)", m.WireTraceparent, tp.TraceID, tp.SpanID),
					Expected: fmt.Sprintf("%s/%s", tp.TraceID, tp.SpanID),
					Actual:   "<no matching create span>",
				})
			} else {
				failures = append(failures, Violation{
					Code:     CodeMissingSpan,
					Rule:     "publish.create.present",
					Message:  fmt.Sprintf("missing %q span for publish message[%d]", createSpanName, item.origIdx),
					Expected: createSpanName,
					Actual:   "<missing>",
					SpanName: createSpanName,
				})
			}
		} else if profile.Name == ProfileGo && m.ExpectError && m.ErrorKind == PublishErrorFlowControl {
			failures = append(failures, Violation{
				Code:     CodeMissingSpan,
				Rule:     "publish.flow_control.error_span",
				Message:  fmt.Sprintf("missing %q error span for flow control failure message[%d]", "publisher flow control", item.origIdx),
				Expected: `"publisher flow control" with SPAN_STATUS_CODE_ERROR`,
				Actual:   "<missing>",
				SpanName: "publisher flow control",
			})
		} else {
			anyActiveUnmatchedCreate = true
			failures = append(failures, Violation{
				Code:     CodeMissingSpan,
				Rule:     "publish.create.present",
				Message:  fmt.Sprintf("missing %q span for publish message[%d]", createSpanName, item.origIdx),
				Expected: createSpanName,
				Actual:   "<missing>",
				SpanName: createSpanName,
			})
		}
	}

	// Stage 1b: Catch any fresh-root <topic> create (and stray fc/batching) leaks when
	// unsampled (-00) dropped messages are expected (even if AllowExtraPubSubSpans is true).
	if anyDroppedUnsampled {
		for _, cs := range createCandidates {
			if !usedCreates[cs] {
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "publish.unsampled_dropped",
					Message:  fmt.Sprintf("unexpected %q span (%s/%s) when unsampled publish message should drop create subtree", cs.GetName(), cs.GetTraceId(), cs.GetSpanId()),
					Expected: "create span dropped",
					Actual:   cs.GetName(),
					TraceID:  cs.GetTraceId(),
					SpanID:   cs.GetSpanId(),
					SpanName: cs.GetName(),
				})
				usedCreates[cs] = true
				matched[cs] = true
			}
		}
		reportedFCOrBatchLeak := make(map[*proxypb.TraceSpan]bool)
		if !anyActiveMatchedCreate && !anyActiveUnmatchedCreate {
			for _, s := range sortedSpans {
				if matched[s] || !isCandidatePubSubSpan(s, profile) {
					continue
				}
				if s.GetName() == "publisher flow control" || s.GetName() == "publisher batching" {
					reportedFCOrBatchLeak[s] = true
					matched[s] = true
					failures = append(failures, Violation{
						Code:     CodeUnexpectedSpan,
						Rule:     "publish.unsampled_dropped",
						Message:  fmt.Sprintf("unexpected %q span (%s/%s) when unsampled publish message should drop create subtree", s.GetName(), s.GetTraceId(), s.GetSpanId()),
						Expected: "create subtree dropped",
						Actual:   s.GetName(),
						TraceID:  s.GetTraceId(),
						SpanID:   s.GetSpanId(),
						SpanName: s.GetName(),
					})
				}
			}
		}
		for _, item := range droppedMsgs {
			if item.msg.ParentTraceparent == "" {
				continue
			}
			tp, err := ParseTraceparent(item.msg.ParentTraceparent)
			if err != nil {
				continue
			}
			for _, s := range sortedSpans {
				if matched[s] || reportedFCOrBatchLeak[s] || !isCandidatePubSubSpan(s, profile) {
					continue
				}
				if NormalizeHex(s.GetTraceId()) == tp.TraceID &&
					(s.GetName() == "publisher flow control" || s.GetName() == "publisher batching") {
					reportedFCOrBatchLeak[s] = true
					matched[s] = true
					failures = append(failures, Violation{
						Code:     CodeUnexpectedSpan,
						Rule:     "publish.unsampled_dropped",
						Message:  fmt.Sprintf("unsampled parent traceparent %q should not export %q", item.msg.ParentTraceparent, s.GetName()),
						Expected: "create subtree dropped",
						Actual:   s.GetName(),
						TraceID:  s.GetTraceId(),
						SpanID:   s.GetSpanId(),
						SpanName: s.GetName(),
					})
				}
			}
		}
	}

	// Suppress unused warning when batchingSpans is inspected via ChildrenOf.
	_ = batchingSpans

	// Compute unsampled (-00) wire batch slot counts.
	droppedWireRequired := 0
	droppedWireOptional := 0
	for _, item := range droppedMsgs {
		m := item.msg
		if profile.AllowBlockingPublisher && m.BlockingPublisher {
			continue
		}
		if !m.ExpectError || m.ErrorKind == PublishErrorRPC {
			droppedWireRequired++
			if !m.BlockingPublisher && m.ExpectError {
				anyWireRPCExpectError = true
			}
		} else if m.ErrorKind == PublishErrorAuto {
			droppedWireOptional++
		}
	}
	droppedWireMax := droppedWireRequired + droppedWireOptional

	wireCreateSet := make(map[*proxypb.TraceSpan]bool, len(wireCreateSpans))
	for _, cs := range wireCreateSpans {
		wireCreateSet[cs] = true
	}

	// Verify <topicID> publish spans & links to wireCreateSpans + unsampled (-00) slots.
	allowExtraValidCreateLinks := !strictExtra && pubExp.BatchCount == 0 && len(pubExp.Batches) == 0 && !anyDroppedUnsampled
	validExtraSampledCreateByKey := make(map[SpanKey]*proxypb.TraceSpan)
	if allowExtraValidCreateLinks {
		for _, cs := range createCandidates {
			if wireCreateSet[cs] || usedCreates[cs] || !isStrictValidSampledCreateSpan(cs, profile, topicID) {
				continue
			}
			ck := SpanKey{TraceID: NormalizeHex(cs.GetTraceId()), SpanID: NormalizeHex(cs.GetSpanId())}
			if idx.ByKey[ck] == cs {
				validExtraSampledCreateByKey[ck] = cs
			}
		}
	}

	type createPubPair struct {
		cs     *proxypb.TraceSpan
		pubKey SpanKey
	}
	verifiedCreatePubPairs := make(map[createPubPair]bool)
	matchedPubKeys := make(map[SpanKey]bool)
	if len(wireCreateSpans) > 0 || droppedWireRequired > 0 || pubExp.BatchCount > 0 || len(pubExp.Batches) > 0 || len(pubSpans) > 0 {
		if len(pubSpans) == 0 {
			failures = append(failures, Violation{
				Code:     CodeMissingSpan,
				Rule:     "publish.batch.present",
				Message:  fmt.Sprintf("missing %q batch publish span(s) for %d message(s)", publishSpanName, len(wireCreateSpans)+droppedWireRequired),
				Expected: publishSpanName,
				Actual:   "<missing>",
				SpanName: publishSpanName,
			})
			return failures
		}
		if pubExp.BatchCount > 0 && len(pubSpans) != pubExp.BatchCount {
			failures = append(failures, Violation{
				Code:     CodeMissingSpan,
				Rule:     "publish.batch.count",
				Message:  fmt.Sprintf("expected %d %q spans, got %d", pubExp.BatchCount, publishSpanName, len(pubSpans)),
				Expected: strconv.Itoa(pubExp.BatchCount),
				Actual:   strconv.Itoa(len(pubSpans)),
				SpanName: publishSpanName,
			})
		}

		maxLinks := pubExp.MaxLinksPerSpan
		if maxLinks <= 0 {
			maxLinks = profile.DefaultMaxLinksPerSpan
			if maxLinks <= 0 {
				maxLinks = 128
			}
		}

		type pubBatchInfo struct {
			ps            *proxypb.TraceSpan
			linked        int
			linkedMatched int
			batchCount    int64
			hasBatchCount bool
		}
		batchInfos := make([]pubBatchInfo, 0, len(pubSpans))
		linkedCreateKeys := make(map[SpanKey]*proxypb.TraceSpan)

		for _, ps := range pubSpans {
			effTopic := topicID
			if effTopic == "" && strings.HasSuffix(ps.GetName(), " publish") {
				effTopic = strings.TrimSuffix(ps.GetName(), " publish")
			}

			failures = append(failures, checkSpanScope(ps, profile, "publish.batch.scope", devs)...)
			if ps.GetKind() != profile.PublishSpanKind {
				failures = append(failures, checkSpanKindV(ps, profile.PublishSpanKind, "publish.batch.span_kind")...)
			} else if profile.Name == ProfileCPP && ps.GetKind() != proxypb.SpanKind_SPAN_KIND_INTERNAL {
				devs.add(Deviation{
					Category:    "span_kind",
					Rule:        "span_kind.publish",
					ExpectedGo:  proxypb.SpanKind_SPAN_KIND_INTERNAL.String(),
					Observed:    ps.GetKind().String(),
					SpanName:    ps.GetName(),
					Description: "C++ <topic> publish span uses SPAN_KIND_CLIENT instead of SPAN_KIND_INTERNAL",
				})
			}

			failures = append(failures, checkRootSpanV(ps, "publish.batch.root")...)
			failures = append(failures, checkCommonAttrs(ps, profile, projectID, fullTopic, effTopic, devs, "publish.batch")...)
			failures = append(failures, checkAttrV(ps, "messaging.batch.message_count", MatchInt64Min(1), "publish.batch.attr.message_count")...)

			if profile.Name == ProfileGo {
				failures = append(failures, checkAttrV(ps, "code.function", MatchString("publishMessageBundle"), "publish.batch.attr.code_function")...)
				failures = append(failures, checkStatusV(ps, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "publish.batch.status")...)
			} else {
				failures = append(failures, checkAttrV(ps, profile.CodeFunctionAttrKey, MatchString("BatchSink::AsyncPublish"), "publish.batch.attr.code_function_name")...)
				devs.add(Deviation{
					Category:    "attribute",
					Rule:        "attr.code_function_key",
					ExpectedGo:  "code.function",
					Observed:    profile.CodeFunctionAttrKey,
					SpanName:    ps.GetName(),
					Description: "C++ uses semconv incubating code.function.name instead of code.function",
				})
				failures = append(failures, checkAttrV(ps, "messaging.operation.type", MatchString("publish"), "publish.batch.attr.operation_type")...)
				failures = append(failures, checkAttrV(ps, "gl-cpp.status_code", MatchStringNonEmpty(), "publish.batch.attr.gl_cpp_status_code")...)
				failures = append(failures, checkStatusV(ps, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, false, "", "publish.batch.status")...)
				psStatusCode, _ := AttrString(ps.GetAttributes(), "gl-cpp.status_code")
				if pubExp.ExpectError || anyWireRPCExpectError ||
					(droppedWireOptional > 0 && ((psStatusCode != "" && psStatusCode != "OK") || (len(wireCreateSpans) == 0 && droppedWireRequired == 0))) {
					devs.add(Deviation{
						Category:    "status",
						Rule:        "status.publish_ignores_rpc_error",
						ExpectedGo:  "SPAN_STATUS_CODE_UNSET (error on create)",
						Observed:    "SPAN_STATUS_CODE_OK (error on create and gRPC stub)",
						SpanName:    ps.GetName(),
						Description: "C++ BatchSink ends <topic> publish with OK even when Publish RPC fails",
					})
				}
			}

			batchCount, hasBatchCount := AttrInt64(ps.GetAttributes(), "messaging.batch.message_count")

			// Inspect forward links from ps (or C++ publish #N fan-out children).
			fanoutChildren := collectPublishFanoutChildren(idx.ChildrenOf(ps))
			var forwardLinks []*proxypb.SpanLink
			if len(fanoutChildren) > 0 {
				if !profile.AllowLinkFanoutChildren {
					failures = append(failures, Violation{
						Code:     CodeUnexpectedSpan,
						Rule:     "publish.batch.unexpected_fanout",
						Message:  fmt.Sprintf("Go canonical profile does not allow %q link fan-out children under %q", "publish #N", ps.GetName()),
						Expected: "direct links on <topic> publish",
						Actual:   fmt.Sprintf("%d fan-out children", len(fanoutChildren)),
						TraceID:  ps.GetTraceId(),
						SpanID:   ps.GetSpanId(),
						SpanName: ps.GetName(),
					})
				} else {
					if len(ps.GetLinks()) > 0 {
						failures = append(failures, Violation{
							Code:     CodeUnexpectedSpan,
							Rule:     "publish.batch.fanout_parent_links",
							Message:  fmt.Sprintf("%q must have 0 direct links when splitting into %d fan-out child spans", ps.GetName(), len(fanoutChildren)),
							Expected: "0 links on parent",
							Actual:   strconv.Itoa(len(ps.GetLinks())),
							TraceID:  ps.GetTraceId(),
							SpanID:   ps.GetSpanId(),
							SpanName: ps.GetName(),
						})
					}
					if hasBatchCount && batchCount <= int64(maxLinks) {
						failures = append(failures, Violation{
							Code:     CodeUnexpectedSpan,
							Rule:     "publish.batch.unexpected_fanout",
							Message:  fmt.Sprintf("%q with batch size %d <= %d must not emit %q fan-out child spans", ps.GetName(), batchCount, maxLinks, "publish #N"),
							Expected: "0 fan-out children",
							Actual:   strconv.Itoa(len(fanoutChildren)),
							TraceID:  ps.GetTraceId(),
							SpanID:   ps.GetSpanId(),
							SpanName: ps.GetName(),
						})
					} else if hasBatchCount && batchCount > int64(maxLinks) {
						wantFanout := int((batchCount + int64(maxLinks) - 1) / int64(maxLinks))
						if len(fanoutChildren) != wantFanout {
							code := CodeUnexpectedSpan
							if len(fanoutChildren) < wantFanout {
								code = CodeMissingSpan
							}
							failures = append(failures, Violation{
								Code:     code,
								Rule:     "publish.batch.fanout_count",
								Message:  fmt.Sprintf("%q with batch size %d expected %d %q fan-out children, got %d", ps.GetName(), batchCount, wantFanout, "publish #N", len(fanoutChildren)),
								Expected: strconv.Itoa(wantFanout),
								Actual:   strconv.Itoa(len(fanoutChildren)),
								TraceID:  ps.GetTraceId(),
								SpanID:   ps.GetSpanId(),
								SpanName: ps.GetName(),
							})
						}
					}
					failures = append(failures, verifyFanoutChildren(ps, fanoutChildren, maxLinks, batchCount, hasBatchCount, anyDroppedUnsampled, profile, matched, devs)...)
					for _, fc := range fanoutChildren {
						forwardLinks = append(forwardLinks, fc.GetLinks()...)
					}
					devs.add(Deviation{
						Category:    "link",
						Rule:        "links.publish_fanout_children",
						ExpectedGo:  "links directly on <topic> publish",
						Observed:    `child spans "publish #0", "publish #1", ...`,
						SpanName:    ps.GetName(),
						Description: "C++ splits >128 links across child spans named \"publish #N\"",
					})
				}
			} else {
				if profile.AllowLinkFanoutChildren && hasBatchCount && batchCount > int64(maxLinks) {
					failures = append(failures, Violation{
						Code:     CodeMissingSpan,
						Rule:     "publish.batch.fanout_required",
						Message:  fmt.Sprintf("%q has batch size %d > %d max links without %q fan-out child spans", ps.GetName(), batchCount, maxLinks, "publish #N"),
						Expected: `child spans "publish #0", "publish #1", ...`,
						Actual:   fmt.Sprintf("0 fan-out children (%d direct links, %d dropped)", len(ps.GetLinks()), ps.GetDroppedLinksCount()),
						TraceID:  ps.GetTraceId(),
						SpanID:   ps.GetSpanId(),
						SpanName: ps.GetName(),
					})
				}
				forwardLinks = ps.GetLinks()
			}

			pubKey := SpanKey{TraceID: NormalizeHex(ps.GetTraceId()), SpanID: NormalizeHex(ps.GetSpanId())}
			// Verify forward links and reverse link/fallback on cs -> ps (never suppress dangling links on -00!).
			linkedMatchedForBatch := 0
			linkedExtraForBatch := 0
			seenForward := make(map[SpanKey]bool)
			for li, link := range forwardLinks {
				if link == nil {
					continue
				}
				if len(link.GetAttributes()) > 0 {
					failures = append(failures, Violation{
						Code:     CodeUnexpectedAttribute,
						Rule:     "publish.batch.link_attrs_empty",
						Message:  fmt.Sprintf("forward link[%d] on %q -> create must have empty attributes", li, ps.GetName()),
						Expected: "0 attributes",
						Actual:   fmt.Sprintf("%d attributes", len(link.GetAttributes())),
						TraceID:  ps.GetTraceId(),
						SpanID:   ps.GetSpanId(),
						SpanName: ps.GetName(),
					})
				}
				ckey := SpanKey{TraceID: NormalizeHex(link.GetTraceId()), SpanID: NormalizeHex(link.GetSpanId())}
				if seenForward[ckey] {
					failures = append(failures, Violation{
						Code:     CodeBrokenBidirectionalLink,
						Rule:     "publish.batch.duplicate_link",
						Message:  fmt.Sprintf("duplicate forward link[%d] on %q targeting (%s/%s)", li, ps.GetName(), link.GetTraceId(), link.GetSpanId()),
						Expected: "unique forward link per create span",
						Actual:   fmt.Sprintf("%s/%s", link.GetTraceId(), link.GetSpanId()),
						TraceID:  ps.GetTraceId(),
						SpanID:   ps.GetSpanId(),
						SpanName: ps.GetName(),
					})
					continue
				}
				seenForward[ckey] = true
				if cs, ok := idx.ByKey[ckey]; ok && wireCreateSet[cs] {
					linkedMatchedForBatch++
					linkedCreateKeys[ckey] = cs
					verifiedCreatePubPairs[createPubPair{cs: cs, pubKey: pubKey}] = true
					failures = append(failures, verifyCreateToPublishLink(cs, ps, profile, devs)...)
				} else if allowExtraValidCreateLinks && validExtraSampledCreateByKey[ckey] != nil {
					linkedExtraForBatch++
				} else {
					expected := "link targeting matched wire <topic> create span"
					if allowExtraValidCreateLinks {
						expected = "link targeting valid sampled <topic> create span"
					}
					failures = append(failures, Violation{
						Code:     CodeMissingLink,
						Rule:     "publish.batch.dangling_link",
						Message:  fmt.Sprintf("forward link[%d] on %q targets unresolved or non-wire create span (%s/%s)", li, ps.GetName(), link.GetTraceId(), link.GetSpanId()),
						Expected: expected,
						Actual:   fmt.Sprintf("%s/%s", link.GetTraceId(), link.GetSpanId()),
						TraceID:  ps.GetTraceId(),
						SpanID:   ps.GetSpanId(),
						SpanName: ps.GetName(),
					})
				}
			}

			batchInfos = append(batchInfos, pubBatchInfo{
				ps:            ps,
				linked:        linkedMatchedForBatch + linkedExtraForBatch,
				linkedMatched: linkedMatchedForBatch,
				batchCount:    batchCount,
				hasBatchCount: hasBatchCount,
			})
		}

		// Two-pass slot-capacity accounting across pubSpans:
		// Pass 1 processes anchored batches (linked > 0); Pass 2 assigns 0-link batches against remaining dropped slots.
		remainingDropped := droppedWireMax
		droppedSlotsUsed := 0
		for _, info := range batchInfos {
			if info.linked == 0 {
				continue
			}
			matched[info.ps] = true
			if !info.hasBatchCount {
				continue
			}
			if info.batchCount < int64(info.linked) {
				failures = append(failures, Violation{
					Code:     CodeWrongAttributeValue,
					Rule:     "publish.batch.attr.message_count",
					Message:  fmt.Sprintf("%q messaging.batch.message_count = %d, want >= %d (linked create count)", info.ps.GetName(), info.batchCount, info.linked),
					Expected: fmt.Sprintf(">= %d", info.linked),
					Actual:   strconv.FormatInt(info.batchCount, 10),
					TraceID:  info.ps.GetTraceId(),
					SpanID:   info.ps.GetSpanId(),
					SpanName: info.ps.GetName(),
				})
				continue
			}
			excess := int(info.batchCount) - info.linked
			if excess > remainingDropped {
				expStr := strconv.Itoa(info.linked)
				if remainingDropped > 0 {
					expStr = fmt.Sprintf("<= %d", info.linked+remainingDropped)
				}
				failures = append(failures, Violation{
					Code:     CodeWrongAttributeValue,
					Rule:     "publish.batch.attr.message_count",
					Message:  fmt.Sprintf("%q messaging.batch.message_count = %d exceeds linked creates (%d) + remaining unsampled slots (%d)", info.ps.GetName(), info.batchCount, info.linked, remainingDropped),
					Expected: expStr,
					Actual:   strconv.FormatInt(info.batchCount, 10),
					TraceID:  info.ps.GetTraceId(),
					SpanID:   info.ps.GetSpanId(),
					SpanName: info.ps.GetName(),
				})
			}
			used := excess
			if used > remainingDropped {
				used = remainingDropped
			}
			remainingDropped -= used
			droppedSlotsUsed += used
		}

		for _, info := range batchInfos {
			if info.linked > 0 {
				continue
			}
			if remainingDropped <= 0 {
				// Leave matched[info.ps] = false so traces.no_extra_pubsub_spans also flags it when strict,
				// and emit batch_count mismatch so AllowExtraPubSubSpans=true also catches bogus 0-link publish spans.
				actualStr := "<missing>"
				if info.hasBatchCount {
					actualStr = strconv.FormatInt(info.batchCount, 10)
				}
				failures = append(failures, Violation{
					Code:     CodeWrongAttributeValue,
					Rule:     "publish.batch.attr.message_count",
					Message:  fmt.Sprintf("unexpected 0-link %q span with %d remaining unsampled publish slots", info.ps.GetName(), remainingDropped),
					Expected: "0 (no remaining unsampled slots)",
					Actual:   actualStr,
					TraceID:  info.ps.GetTraceId(),
					SpanID:   info.ps.GetSpanId(),
					SpanName: info.ps.GetName(),
				})
				continue
			}
			if !info.hasBatchCount {
				matched[info.ps] = true
				remainingDropped--
				droppedSlotsUsed++
				continue
			}
			if info.batchCount <= int64(remainingDropped) {
				matched[info.ps] = true
				if info.batchCount > 0 {
					remainingDropped -= int(info.batchCount)
					droppedSlotsUsed += int(info.batchCount)
				}
				continue
			}
			failures = append(failures, Violation{
				Code:     CodeWrongAttributeValue,
				Rule:     "publish.batch.attr.message_count",
				Message:  fmt.Sprintf("0-link %q messaging.batch.message_count = %d exceeds remaining unsampled slots (%d)", info.ps.GetName(), info.batchCount, remainingDropped),
				Expected: fmt.Sprintf("<= %d", remainingDropped),
				Actual:   strconv.FormatInt(info.batchCount, 10),
				TraceID:  info.ps.GetTraceId(),
				SpanID:   info.ps.GetSpanId(),
				SpanName: info.ps.GetName(),
			})
		}

		if droppedSlotsUsed < droppedWireRequired {
			failures = append(failures, Violation{
				Code:     CodeMissingSpan,
				Rule:     "publish.unsampled_batch_slot",
				Message:  fmt.Sprintf("only %d of %d unsampled publish message(s) accounted for in %q batch_count", droppedSlotsUsed, droppedWireRequired, publishSpanName),
				Expected: fmt.Sprintf(">= %d unsampled batch slot(s)", droppedWireRequired),
				Actual:   strconv.Itoa(droppedSlotsUsed),
				SpanName: publishSpanName,
			})
		}

		for _, info := range batchInfos {
			if matched[info.ps] && info.linkedMatched > 0 {
				matchedPubKeys[SpanKey{TraceID: NormalizeHex(info.ps.GetTraceId()), SpanID: NormalizeHex(info.ps.GetSpanId())}] = true
			}
		}

		// Ensure every create span that reached wire was linked by at least one publish span.
		for _, cs := range wireCreateSpans {
			ckey := SpanKey{TraceID: NormalizeHex(cs.GetTraceId()), SpanID: NormalizeHex(cs.GetSpanId())}
			if _, linked := linkedCreateKeys[ckey]; !linked {
				failures = append(failures, Violation{
					Code:     CodeMissingLink,
					Rule:     "publish.batch.forward_link",
					Message:  fmt.Sprintf("no %q span links to create span %q (%s/%s)", publishSpanName, cs.GetName(), cs.GetTraceId(), cs.GetSpanId()),
					Expected: fmt.Sprintf("forward link -> %s/%s", cs.GetTraceId(), cs.GetSpanId()),
					Actual:   "<missing link>",
					TraceID:  cs.GetTraceId(),
					SpanID:   cs.GetSpanId(),
					SpanName: cs.GetName(),
				})
			}
		}

		if len(pubExp.Batches) > 0 {
			failures = append(failures, verifyPublishBatches(pubSpans, pubExp.Batches, pubExp.RequireSequentialBatches)...)
		}
	}

	// Audit reverse links on all matched create spans: disallow dangling links to unresolved or unmatched publish spans.
	for _, cs := range createCandidates {
		if !usedCreates[cs] {
			continue
		}
		for li, link := range cs.GetLinks() {
			if link == nil {
				continue
			}
			pkey := SpanKey{TraceID: NormalizeHex(link.GetTraceId()), SpanID: NormalizeHex(link.GetSpanId())}
			if !wireCreateSet[cs] || !matchedPubKeys[pkey] || !verifiedCreatePubPairs[createPubPair{cs: cs, pubKey: pkey}] {
				failures = append(failures, Violation{
					Code:     CodeMissingLink,
					Rule:     "publish.create.dangling_link",
					Message:  fmt.Sprintf("reverse link[%d] on %q targets unresolved or unmatched publish span (%s/%s)", li, cs.GetName(), link.GetTraceId(), link.GetSpanId()),
					Expected: "link targeting matched <topic> publish span",
					Actual:   fmt.Sprintf("%s/%s", link.GetTraceId(), link.GetSpanId()),
					TraceID:  cs.GetTraceId(),
					SpanID:   cs.GetSpanId(),
					SpanName: cs.GetName(),
				})
			}
		}
	}

	return failures
}

func verifyGoFlowControlErrorSpan(
	fc *proxypb.TraceSpan,
	profile Profile,
	errSubstr string,
	devs *deviationCollector,
) []Violation {
	var v []Violation
	v = append(v, checkSpanScope(fc, profile, "publish.flow_control.scope", devs)...)
	v = append(v, checkSpanKindV(fc, proxypb.SpanKind_SPAN_KIND_INTERNAL, "publish.flow_control.span_kind")...)
	v = append(v, checkZeroAttributesV(fc, "publish.flow_control.zero_attrs")...)
	v = append(v, checkStatusV(fc, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, errSubstr, "publish.flow_control.error_status")...)
	v = append(v, checkEventV(fc, "exception", "publish.flow_control.event.exception")...)
	if fc.GetParentSpanId() == "" {
		v = append(v, Violation{
			Code:     CodeWrongParent,
			Rule:     "publish.flow_control.parent",
			Message:  fmt.Sprintf("%q must have non-empty parent_span_id pointing to leaked create span", fc.GetName()),
			Expected: "non-empty parent_span_id",
			Actual:   `""`,
			TraceID:  fc.GetTraceId(),
			SpanID:   fc.GetSpanId(),
			SpanName: fc.GetName(),
		})
	}
	return v
}

func resolvePublishErrorKind(idx *SpanIndex, cs *proxypb.TraceSpan, m PublishMessageExpectation, profile Profile) PublishErrorKind {
	if !m.ExpectError {
		return PublishErrorAuto
	}
	if m.ErrorKind != PublishErrorAuto {
		return m.ErrorKind
	}
	children := idx.ChildrenOf(cs)
	hasFC := len(filterByName(children, "publisher flow control")) > 0
	hasBatch := len(filterByName(children, "publisher batching")) > 0
	return resolvePublishErrorKindFromMeta(cs, hasFC, hasBatch, profile)
}

func expectFlowControlChild(m PublishMessageExpectation, errKind PublishErrorKind, profile Profile) bool {
	if m.RequireFlowControlSpan != nil {
		return *m.RequireFlowControlSpan
	}
	if m.BlockingPublisher && profile.AllowBlockingPublisher {
		return false
	}
	if m.ExpectError && (errKind == PublishErrorPreValidation || errKind == PublishErrorOrderingRejected) {
		return profile.Name == ProfileCPP
	}
	return true
}

func expectBatchingChild(m PublishMessageExpectation, errKind PublishErrorKind, profile Profile) bool {
	if m.RequireBatchingSpan != nil {
		return *m.RequireBatchingSpan
	}
	if m.BlockingPublisher && profile.AllowBlockingPublisher {
		return false
	}
	if m.ExpectError {
		if profile.Name == ProfileCPP {
			return errKind == PublishErrorRPC || errKind == PublishErrorPreValidation || errKind == PublishErrorOrderingRejected
		}
		if errKind == PublishErrorPreValidation || errKind == PublishErrorOrderingRejected || errKind == PublishErrorFlowControl || errKind == PublishErrorOversized {
			return false
		}
	}
	return true
}

func verifyFlowControlChild(fc, createSpan *proxypb.TraceSpan, profile Profile, expectError bool, errSubstr string, devs *deviationCollector) []Violation {
	var out []Violation
	out = append(out, checkSpanScope(fc, profile, "publish.flow_control.scope", devs)...)
	out = append(out, checkChildOfV(fc, createSpan, "publish.flow_control.parent")...)
	if fc.GetKind() != profile.FlowControlSpanKind {
		out = append(out, checkSpanKindV(fc, profile.FlowControlSpanKind, "publish.flow_control.span_kind")...)
	} else if profile.Name == ProfileCPP && fc.GetKind() != proxypb.SpanKind_SPAN_KIND_INTERNAL {
		devs.add(Deviation{
			Category:    "span_kind",
			Rule:        "span_kind.flow_control",
			ExpectedGo:  proxypb.SpanKind_SPAN_KIND_INTERNAL.String(),
			Observed:    fc.GetKind().String(),
			SpanName:    fc.GetName(),
			Description: "C++ publisher flow control span uses SPAN_KIND_CLIENT instead of SPAN_KIND_INTERNAL",
		})
	}

	if profile.Name == ProfileGo {
		out = append(out, checkZeroAttributesV(fc, "publish.flow_control.zero_attrs")...)
		if expectError {
			out = append(out, checkStatusV(fc, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, errSubstr, "publish.flow_control.status")...)
		} else {
			out = append(out, checkStatusV(fc, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "publish.flow_control.status")...)
		}
	} else {
		out = append(out, checkAttrV(fc, profile.CodeFunctionAttrKey, MatchString("pubsub::FlowControlledPublisherConnection::Publish"), "publish.flow_control.attr.code_function_name")...)
		out = append(out, checkStatusV(fc, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, false, "", "publish.flow_control.status")...)
		if expectError {
			devs.add(Deviation{
				Category:    "status",
				Rule:        "status.flow_control_sync_ok",
				ExpectedGo:  "SPAN_STATUS_CODE_ERROR on flow control rejection",
				Observed:    "SPAN_STATUS_CODE_OK (sync end, error on <topic> create)",
				SpanName:    fc.GetName(),
				Description: "C++ FlowControlledPublisherConnection ends flow control span synchronously with OK even when future fails",
			})
		}
	}
	return out
}

func verifyBatchingChild(bc, createSpan *proxypb.TraceSpan, profile Profile, devs *deviationCollector) []Violation {
	var out []Violation
	out = append(out, checkSpanScope(bc, profile, "publish.batching.scope", devs)...)
	out = append(out, checkChildOfV(bc, createSpan, "publish.batching.parent")...)
	if bc.GetKind() != profile.BatchingSpanKind {
		out = append(out, checkSpanKindV(bc, profile.BatchingSpanKind, "publish.batching.span_kind")...)
	} else if profile.Name == ProfileCPP && bc.GetKind() != proxypb.SpanKind_SPAN_KIND_INTERNAL {
		devs.add(Deviation{
			Category:    "span_kind",
			Rule:        "span_kind.batching",
			ExpectedGo:  proxypb.SpanKind_SPAN_KIND_INTERNAL.String(),
			Observed:    bc.GetKind().String(),
			SpanName:    bc.GetName(),
			Description: "C++ publisher batching span uses SPAN_KIND_CLIENT instead of SPAN_KIND_INTERNAL",
		})
	}
	if profile.Name == ProfileGo {
		out = append(out, checkZeroAttributesV(bc, "publish.batching.zero_attrs")...)
		out = append(out, checkStatusV(bc, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "publish.batching.status")...)
	} else {
		out = append(out, checkAttrV(bc, profile.CodeFunctionAttrKey, MatchString("pubsub::BatchingPublisherConnection::Publish"), "publish.batching.attr.code_function_name")...)
		out = append(out, checkStatusV(bc, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, false, "", "publish.batching.status")...)
	}
	return out
}

func collectPublishFanoutChildren(children []*proxypb.TraceSpan) []*proxypb.TraceSpan {
	var out []*proxypb.TraceSpan
	for _, c := range children {
		if strings.HasPrefix(c.GetName(), "publish #") {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		ni, ei := strconv.Atoi(strings.TrimPrefix(out[i].GetName(), "publish #"))
		nj, ej := strconv.Atoi(strings.TrimPrefix(out[j].GetName(), "publish #"))
		if ei == nil && ej == nil && ni != nj {
			return ni < nj
		}
		return out[i].GetName() < out[j].GetName()
	})
	return out
}

func verifyFanoutChildren(
	parent *proxypb.TraceSpan,
	children []*proxypb.TraceSpan,
	maxLinks int,
	batchCount int64,
	hasBatchCount bool,
	anyDroppedUnsampled bool,
	profile Profile,
	matched map[*proxypb.TraceSpan]bool,
	devs *deviationCollector,
) []Violation {
	var out []Violation
	for idxExpected, c := range children {
		matched[c] = true
		out = append(out, checkSpanScope(c, profile, "publish.batch.fanout_scope", devs)...)
		wantName := fmt.Sprintf("publish #%d", idxExpected)
		if c.GetName() != wantName {
			out = append(out, Violation{
				Code:     CodeUnexpectedSpan,
				Rule:     "publish.batch.fanout_contiguous_names",
				Message:  fmt.Sprintf("fan-out child[%d] named %q, want %q", idxExpected, c.GetName(), wantName),
				Expected: wantName,
				Actual:   c.GetName(),
				TraceID:  c.GetTraceId(),
				SpanID:   c.GetSpanId(),
				SpanName: c.GetName(),
			})
		}
		out = append(out, checkChildOfV(c, parent, "publish.batch.fanout_parent")...)
		out = append(out, checkSpanKindV(c, proxypb.SpanKind_SPAN_KIND_CLIENT, "publish.batch.fanout_kind")...)
		out = append(out, checkZeroAttributesV(c, "publish.batch.fanout_zero_attrs")...)

		chunkMax := maxLinks
		exactChunk := false
		if hasBatchCount && batchCount > int64(maxLinks) {
			rem := int(batchCount) - idxExpected*maxLinks
			if rem > 0 {
				if rem < chunkMax {
					chunkMax = rem
				}
				exactChunk = true
			}
		}
		linkCnt := len(c.GetLinks())
		if !anyDroppedUnsampled && exactChunk {
			if linkCnt != chunkMax {
				out = append(out, Violation{
					Code:     CodeMissingLink,
					Rule:     "publish.batch.fanout_link_bounds",
					Message:  fmt.Sprintf("fan-out child %q has %d links, want %d", c.GetName(), linkCnt, chunkMax),
					Expected: fmt.Sprintf("%d links", chunkMax),
					Actual:   strconv.Itoa(linkCnt),
					TraceID:  c.GetTraceId(),
					SpanID:   c.GetSpanId(),
					SpanName: c.GetName(),
				})
			}
		} else {
			minLinks := 1
			if anyDroppedUnsampled {
				minLinks = 0
			}
			if linkCnt < minLinks || linkCnt > chunkMax {
				out = append(out, Violation{
					Code:     CodeMissingLink,
					Rule:     "publish.batch.fanout_link_bounds",
					Message:  fmt.Sprintf("fan-out child %q has %d links, want %d..%d", c.GetName(), linkCnt, minLinks, chunkMax),
					Expected: fmt.Sprintf("%d..%d links", minLinks, chunkMax),
					Actual:   strconv.Itoa(linkCnt),
					TraceID:  c.GetTraceId(),
					SpanID:   c.GetSpanId(),
					SpanName: c.GetName(),
				})
			}
		}
	}
	return out
}

func verifyCreateToPublishLink(cs, ps *proxypb.TraceSpan, profile Profile, devs *deviationCollector) []Violation {
	var out []Violation
	revMatches := findMatchingLinks(cs, ps)
	if len(revMatches) > 0 {
		if len(revMatches) > 1 {
			out = append(out, Violation{
				Code:     CodeBrokenBidirectionalLink,
				Rule:     "publish.create.duplicate_reverse_link",
				Message:  fmt.Sprintf("duplicate reverse links (%d) from %q (%s) -> %q (%s)", len(revMatches), cs.GetName(), cs.GetSpanId(), ps.GetName(), ps.GetSpanId()),
				Expected: "at most 1 reverse SpanLink",
				Actual:   strconv.Itoa(len(revMatches)),
				TraceID:  cs.GetTraceId(),
				SpanID:   cs.GetSpanId(),
				SpanName: cs.GetName(),
			})
		}
		rev := revMatches[0]
		if profile.RequireReversePublishLinkOpName {
			if err := AssertAttribute(rev.GetAttributes(), "messaging.operation.name", MatchString("publish")); err != nil {
				out = append(out, Violation{
					Code:     CodeMissingLinkAttr,
					Rule:     "publish.create.reverse_link_attr",
					Message:  fmt.Sprintf("reverse link %q -> %q missing/invalid messaging.operation.name: %v", cs.GetName(), ps.GetName(), err),
					Expected: `messaging.operation.name="publish"`,
					Actual:   FormatAttrValue(rev.GetAttributes()["messaging.operation.name"]),
					TraceID:  cs.GetTraceId(),
					SpanID:   cs.GetSpanId(),
					SpanName: cs.GetName(),
				})
			}
		} else if len(rev.GetAttributes()) == 0 {
			devs.add(Deviation{
				Category:    "link",
				Rule:        "links.missing_operation_name",
				ExpectedGo:  `SpanLink with messaging.operation.name="publish"`,
				Observed:    "SpanLink with empty attributes (ABI v2)",
				SpanName:    cs.GetName(),
				Description: "C++ ABI >= 2 adds reverse SpanLink on <topic> create without messaging.operation.name",
			})
		}
		return out
	}

	if profile.AllowABIV1LinkAttrs {
		if hasEventWithAttrs(cs, "gl-cpp.publish_start", map[string]string{
			"gcp_pubsub.publish.trace_id": NormalizeHex(ps.GetTraceId()),
			"gcp_pubsub.publish.span_id":  NormalizeHex(ps.GetSpanId()),
		}) {
			devs.add(Deviation{
				Category:    "link",
				Rule:        "links.abi_v1_event_attrs_fallback",
				ExpectedGo:  "SpanLink on <topic> create -> <topic> publish",
				Observed:    `event "gl-cpp.publish_start" attributes gcp_pubsub.publish.{trace_id,span_id}`,
				SpanName:    cs.GetName(),
				Description: "C++ ABI v1 records publish trace/span IDs on gl-cpp.publish_start event instead of SpanLink",
			})
			return nil
		}
	}

	return []Violation{{
		Code:     CodeBrokenBidirectionalLink,
		Rule:     "publish.bidirectional_link",
		Message:  fmt.Sprintf("missing reverse link from %q (%s) -> %q (%s)", cs.GetName(), cs.GetSpanId(), ps.GetName(), ps.GetSpanId()),
		Expected: fmt.Sprintf("SpanLink -> %s/%s", ps.GetTraceId(), ps.GetSpanId()),
		Actual:   "<missing>",
		TraceID:  cs.GetTraceId(),
		SpanID:   cs.GetSpanId(),
		SpanName: cs.GetName(),
	}}
}

func hasEventWithAttrs(s *proxypb.TraceSpan, eventName string, wantHex map[string]string) bool {
	for _, ev := range s.GetEvents() {
		if ev == nil || ev.GetName() != eventName {
			continue
		}
		ok := true
		for k, want := range wantHex {
			got, exists := AttrString(ev.GetAttributes(), k)
			if !exists || NormalizeHex(got) != NormalizeHex(want) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func verifyPublishBatches(pubSpans []*proxypb.TraceSpan, wantBatches []PublishBatchExpectation, sequential bool) []Violation {
	if len(pubSpans) != len(wantBatches) {
		return []Violation{{
			Code:     CodeMissingSpan,
			Rule:     "publish.batches.count",
			Message:  fmt.Sprintf("expected %d publish batch spans, got %d", len(wantBatches), len(pubSpans)),
			Expected: strconv.Itoa(len(wantBatches)),
			Actual:   strconv.Itoa(len(pubSpans)),
		}}
	}
	if sequential {
		ordered := SortSpansDeterministic(pubSpans)
		var out []Violation
		for i, wb := range wantBatches {
			ps := ordered[i]
			if wb.MessageCount > 0 {
				out = append(out, checkAttrV(ps, "messaging.batch.message_count", MatchInt64(int64(wb.MessageCount)), "publish.batches.sequential.message_count")...)
			}
		}
		return out
	}

	// Global bipartite matching on MessageCount (weight=10 exact, weight=1 wildcard).
	ordered := SortSpansDeterministic(pubSpans)
	var batchEdges []bipartiteEdge
	if len(wantBatches)*len(ordered) > 256 {
		byCount := make(map[int64][]int, len(ordered))
		for j, ps := range ordered {
			if cnt, ok := AttrInt64(ps.GetAttributes(), "messaging.batch.message_count"); ok {
				byCount[cnt] = append(byCount[cnt], j)
			}
		}
		for i, wb := range wantBatches {
			if wb.MessageCount == 0 {
				for j := range ordered {
					batchEdges = append(batchEdges, bipartiteEdge{left: i, right: j, weight: 1})
				}
			} else {
				for _, j := range byCount[int64(wb.MessageCount)] {
					batchEdges = append(batchEdges, bipartiteEdge{left: i, right: j, weight: 10})
				}
			}
		}
	} else {
		for i, wb := range wantBatches {
			for j, ps := range ordered {
				cnt, ok := AttrInt64(ps.GetAttributes(), "messaging.batch.message_count")
				if wb.MessageCount == 0 {
					batchEdges = append(batchEdges, bipartiteEdge{left: i, right: j, weight: 1})
				} else if ok && cnt == int64(wb.MessageCount) {
					batchEdges = append(batchEdges, bipartiteEdge{left: i, right: j, weight: 10})
				}
			}
		}
	}
	assign := solveBipartiteMatching(len(wantBatches), len(ordered), nil, nil, batchEdges)
	var out []Violation
	for idxB, wb := range wantBatches {
		if assign[idxB] < 0 {
			out = append(out, Violation{
				Code:     CodeWrongAttributeValue,
				Rule:     "publish.batches.multiset",
				Message:  fmt.Sprintf("no unmatched %q span with messaging.batch.message_count=%d for expected batch[%d]", "publish", wb.MessageCount, idxB),
				Expected: strconv.Itoa(wb.MessageCount),
				Actual:   "<no matching batch span>",
			})
		}
	}
	return out
}

func filterByName(spans []*proxypb.TraceSpan, name string) []*proxypb.TraceSpan {
	var out []*proxypb.TraceSpan
	for _, s := range spans {
		if s != nil && s.GetName() == name {
			out = append(out, s)
		}
	}
	return out
}

func countEventsNamed(s *proxypb.TraceSpan, name string) int {
	n := 0
	for _, ev := range s.GetEvents() {
		if ev != nil && ev.GetName() == name {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Subscribe Flow Verification
// ---------------------------------------------------------------------------

func expandSubscribeMessages(subExp *SubscribeExpectation) []SubscribeMessageExpectation {
	if len(subExp.Messages) > 0 {
		out := make([]SubscribeMessageExpectation, len(subExp.Messages))
		copy(out, subExp.Messages)
		for i := range out {
			if out[i].OrderingKey == "" {
				out[i].OrderingKey = subExp.OrderingKey
			}
			if out[i].DeliveryAttempt == 0 {
				out[i].DeliveryAttempt = subExp.DeliveryAttempt
			}
			if !out[i].ExpectDroppedUnsampled {
				out[i].ExpectDroppedUnsampled = subExp.ExpectDroppedUnsampled
			}
			if out[i].ExactlyOnce == nil {
				out[i].ExactlyOnce = subExp.ExactlyOnce
			}
			if strings.TrimSpace(out[i].ExpectedOutcome) == "" {
				out[i].ExpectedOutcome = subExp.ExpectedOutcome
			}
			if len(out[i].Settlements) == 0 && len(subExp.ExpectedSettlements) > 0 {
				out[i].Settlements = append([]SettlementExpectation(nil), subExp.ExpectedSettlements...)
			}
			if out[i].BatchCount == 0 {
				out[i].BatchCount = subExp.BatchCount
			}
			if out[i].InterruptKind == SubscribeInterruptNone {
				out[i].InterruptKind = subExp.InterruptKind
			}
			if out[i].ExpectConcurrencyControlSpan == nil {
				out[i].ExpectConcurrencyControlSpan = subExp.ExpectConcurrencyControlSpan
			}
			if out[i].ExpectSchedulerSpan == nil {
				out[i].ExpectSchedulerSpan = subExp.ExpectSchedulerSpan
			}
			if out[i].ExpectProcessSpan == nil {
				out[i].ExpectProcessSpan = subExp.ExpectProcessSpan
			}
			if out[i].ExpectProcessSettlementEvent == nil {
				out[i].ExpectProcessSettlementEvent = subExp.ExpectProcessSettlementEvent
			}
		}
		return out
	}

	n := subExp.MessageCount
	if len(subExp.WireTraceparents) > n {
		n = len(subExp.WireTraceparents)
	}
	if len(subExp.CallbackTraceparents) > n {
		n = len(subExp.CallbackTraceparents)
	}
	if len(subExp.AckIDs) > n {
		n = len(subExp.AckIDs)
	}
	if len(subExp.MessageIDs) > n {
		n = len(subExp.MessageIDs)
	}
	if n == 0 && (len(subExp.ExpectedSettlements) > 0 || strings.TrimSpace(subExp.ExpectedOutcome) != "" || subExp.InterruptKind != SubscribeInterruptNone) {
		n = 1
	}
	if n == 0 {
		return nil
	}
	out := make([]SubscribeMessageExpectation, n)
	for i := 0; i < n; i++ {
		m := SubscribeMessageExpectation{
			OrderingKey:                  subExp.OrderingKey,
			DeliveryAttempt:              subExp.DeliveryAttempt,
			ExactlyOnce:                  subExp.ExactlyOnce,
			BatchCount:                   subExp.BatchCount,
			ExpectDroppedUnsampled:       subExp.ExpectDroppedUnsampled,
			ExpectProcessSpan:            subExp.ExpectProcessSpan,
			InterruptKind:                subExp.InterruptKind,
			ExpectConcurrencyControlSpan: subExp.ExpectConcurrencyControlSpan,
			ExpectSchedulerSpan:          subExp.ExpectSchedulerSpan,
			ExpectProcessSettlementEvent: subExp.ExpectProcessSettlementEvent,
			ExpectedOutcome:              subExp.ExpectedOutcome,
			Settlements:                  append([]SettlementExpectation(nil), subExp.ExpectedSettlements...),
		}
		if i < len(subExp.WireTraceparents) {
			m.WireTraceparent = subExp.WireTraceparents[i]
		}
		if i < len(subExp.CallbackTraceparents) {
			m.CallbackTraceparent = subExp.CallbackTraceparents[i]
		}
		if i < len(subExp.AckIDs) {
			m.AckID = subExp.AckIDs[i]
		}
		if i < len(subExp.MessageIDs) {
			m.MessageID = subExp.MessageIDs[i]
		}
		out[i] = m
	}
	return out
}

type mergedSettlement struct {
	expectations []SettlementExpectation
}

func normalizeSubscribeOutcome(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func isValidSubscribeOutcome(norm string) bool {
	switch norm {
	case "", "acked", "nacked", "expired", "eod_failed":
		return true
	default:
		return false
	}
}

func resolveIsEOD(m SubscribeMessageExpectation, subSpan *proxypb.TraceSpan, profile Profile) bool {
	if m.ExactlyOnce != nil {
		return *m.ExactlyOnce
	}
	if normalizeSubscribeOutcome(m.ExpectedOutcome) == "eod_failed" {
		return true
	}
	if subSpan != nil {
		if v, ok := AttrBool(subSpan.GetAttributes(), profile.EOSSubscribeAttrKey); ok {
			return v
		}
	}
	return false
}

type spanLinkPair struct {
	sub    *proxypb.TraceSpan
	settle *proxypb.TraceSpan
}

func verifySubscribeReverseLink(subSpan, chosen *proxypb.TraceSpan, op string, profile Profile, devs *deviationCollector) []Violation {
	revMatches := findMatchingLinks(subSpan, chosen)
	if profile.RequireBidirectionalSubscribeLinks {
		if len(revMatches) == 0 {
			return []Violation{{
				Code:     CodeBrokenBidirectionalLink,
				Rule:     "subscribe.settlement.reverse_link",
				Message:  fmt.Sprintf("missing reverse link from %q (%s) -> %q (%s)", subSpan.GetName(), subSpan.GetSpanId(), chosen.GetName(), chosen.GetSpanId()),
				Expected: fmt.Sprintf("SpanLink -> %s/%s", chosen.GetTraceId(), chosen.GetSpanId()),
				Actual:   "<missing>",
				TraceID:  subSpan.GetTraceId(),
				SpanID:   subSpan.GetSpanId(),
				SpanName: subSpan.GetName(),
			}}
		}
		var out []Violation
		if len(revMatches) > 1 {
			out = append(out, Violation{
				Code:     CodeBrokenBidirectionalLink,
				Rule:     "subscribe.settlement.duplicate_reverse_link",
				Message:  fmt.Sprintf("duplicate reverse links (%d) from %q (%s) -> %q (%s)", len(revMatches), subSpan.GetName(), subSpan.GetSpanId(), chosen.GetName(), chosen.GetSpanId()),
				Expected: "at most 1 reverse SpanLink",
				Actual:   strconv.Itoa(len(revMatches)),
				TraceID:  subSpan.GetTraceId(),
				SpanID:   subSpan.GetSpanId(),
				SpanName: subSpan.GetName(),
			})
		}
		if err := AssertAttribute(revMatches[0].GetAttributes(), "messaging.operation.name", MatchString(op)); err != nil {
			out = append(out, Violation{
				Code:     CodeMissingLinkAttr,
				Rule:     "subscribe.settlement.reverse_link_attr",
				Message:  fmt.Sprintf("reverse link %q -> %q missing/invalid messaging.operation.name: %v", subSpan.GetName(), chosen.GetName(), err),
				Expected: fmt.Sprintf("messaging.operation.name=%q", op),
				Actual:   FormatAttrValue(revMatches[0].GetAttributes()["messaging.operation.name"]),
				TraceID:  subSpan.GetTraceId(),
				SpanID:   subSpan.GetSpanId(),
				SpanName: subSpan.GetName(),
			})
		}
		return out
	}
	if len(revMatches) > 1 {
		return []Violation{{
			Code:     CodeBrokenBidirectionalLink,
			Rule:     "subscribe.settlement.duplicate_reverse_link",
			Message:  fmt.Sprintf("duplicate reverse links (%d) from %q (%s) -> %q (%s)", len(revMatches), subSpan.GetName(), subSpan.GetSpanId(), chosen.GetName(), chosen.GetSpanId()),
			Expected: "at most 1 reverse SpanLink",
			Actual:   strconv.Itoa(len(revMatches)),
			TraceID:  subSpan.GetTraceId(),
			SpanID:   subSpan.GetSpanId(),
			SpanName: subSpan.GetName(),
		}}
	}
	if len(revMatches) == 0 {
		devs.add(Deviation{
			Category:    "link",
			Rule:        "links.subscribe_unidirectional",
			ExpectedGo:  "Bidirectional links between subscribe and ack/nack/modack",
			Observed:    "Unidirectional link from ack/nack/modack -> subscribe only",
			SpanName:    subSpan.GetName(),
			Description: "C++ does not add reverse SpanLinks from <subID> subscribe back to ack/nack/modack",
		})
	}
	return nil
}

func hasExplicitTerminalSettlement(settlements []SettlementExpectation) bool {
	for _, se := range settlements {
		op := strings.ToLower(strings.TrimSpace(se.Operation))
		if op == "ack" || op == "nack" {
			return true
		}
	}
	return false
}

func sampledSubAllowsSettlementOp(m SubscribeMessageExpectation, op string, strictSettlementOps bool) bool {
	for _, se := range m.Settlements {
		if strings.ToLower(strings.TrimSpace(se.Operation)) == op {
			return true
		}
	}
	if strictSettlementOps {
		return false
	}
	if op == "modack" {
		return true
	}
	if !hasExplicitTerminalSettlement(m.Settlements) {
		switch normalizeSubscribeOutcome(m.ExpectedOutcome) {
		case "":
			return true
		case "acked":
			return op == "ack"
		case "nacked":
			return op == "nack"
		case "eod_failed":
			return op == "ack" || op == "nack"
		case "expired":
			return false
		}
	}
	return false
}

func analyzeSubSpanTerminalEvidence(subSpan *proxypb.TraceSpan, profile Profile) (hasAck, hasNack, hasExp bool) {
	if subSpan == nil {
		return false, false, false
	}
	for _, ev := range subSpan.GetEvents() {
		if ev == nil {
			continue
		}
		switch ev.GetName() {
		case "ack start", "ack end", "gl-cpp.ack_start", "gl-cpp.ack_end", "gl-cpp.message_ack":
			hasAck = true
		case "nack start", "nack end", "gl-cpp.nack_start", "gl-cpp.nack_end", "gl-cpp.message_nack":
			hasNack = true
		case "gl-cpp.expired":
			hasExp = true
		}
	}
	if rawRes, _ := AttrString(subSpan.GetAttributes(), "messaging.gcp_pubsub.result"); rawRes != "" {
		if res := strings.ToLower(strings.TrimSpace(rawRes)); res != "" {
			ackedVal := strings.ToLower(strings.TrimSpace(profile.ResultValues["acked"]))
			nackedVal := strings.ToLower(strings.TrimSpace(profile.ResultValues["nacked"]))
			expiredVal := strings.ToLower(strings.TrimSpace(profile.ResultValues["expired"]))
			switch {
			case res == "ack" || res == "acked" || (ackedVal != "" && res == ackedVal):
				hasAck = true
			case res == "nack" || res == "nacked" || (nackedVal != "" && res == nackedVal):
				hasNack = true
			case res == "expired" || (expiredVal != "" && res == expiredVal):
				hasExp = true
			}
		}
	}
	return hasAck, hasNack, hasExp
}

func sampledSubSpanAllowsSettlementOp(subSpan *proxypb.TraceSpan, m SubscribeMessageExpectation, op string, strictSettlementOps bool, profile Profile) bool {
	if !sampledSubAllowsSettlementOp(m, op, strictSettlementOps) {
		return false
	}
	if op == "modack" {
		return true
	}
	if !hasExplicitTerminalSettlement(m.Settlements) && subSpan != nil {
		norm := normalizeSubscribeOutcome(m.ExpectedOutcome)
		if norm == "" || norm == "eod_failed" {
			hasAck, hasNack, hasExp := analyzeSubSpanTerminalEvidence(subSpan, profile)
			if hasExp || (hasAck && hasNack) {
				return false
			}
			if hasAck {
				return op == "ack"
			}
			if hasNack {
				return op == "nack"
			}
		}
	}
	return true
}

type spanPresence uint8

const (
	spanRequired spanPresence = iota
	spanOptional
	spanForbidden
)

func overrideSpanPresence(base spanPresence, override *bool) spanPresence {
	if override == nil {
		return base
	}
	if *override {
		return spanRequired
	}
	return spanForbidden
}

func resolveSubscribeChildModes(m SubscribeMessageExpectation, outcome string) (ccMode, schedMode, procMode spanPresence) {
	switch m.InterruptKind {
	case SubscribeInterruptPreDispatchCancel:
		ccMode = spanOptional
		schedMode = spanForbidden
		procMode = spanForbidden
	case SubscribeInterruptForcedShutdownNack:
		ccMode = spanRequired
		schedMode = spanRequired
		procMode = spanOptional
	default:
		ccMode = spanRequired
		schedMode = spanRequired
		procMode = spanRequired
		if outcome == "expired" {
			procMode = spanOptional
		}
	}
	ccMode = overrideSpanPresence(ccMode, m.ExpectConcurrencyControlSpan)
	schedMode = overrideSpanPresence(schedMode, m.ExpectSchedulerSpan)
	procMode = overrideSpanPresence(procMode, m.ExpectProcessSpan)
	return ccMode, schedMode, procMode
}

type subSpanMeta struct {
	effSubID          string
	normTraceID       string
	normSpanID        string
	normParentSpanID  string
	rawParentEmpty    bool
	ackID             string
	hasAckID          bool
	msgID             string
	hasMsgID          bool
	orderingKey       string
	hasOrderingKey    bool
	sizeVal           int64
	hasSize           bool
	hasDA             bool
	da                int64
	hasDAAny          bool
	hasBC             bool
	bc                int64
	hasEOS            bool
	eos               bool
	hasResult         bool
	result            string
	isUnset           bool
	isErr             bool
	ccCount           int
	schedCount        int
	procCount         int
	procHasAckCalled  bool
	procHasNackCalled bool
	firstProcKey      SpanKey
	extraProcKeys     []SpanKey
	attrs             map[string]*proxypb.AttributeValue
}

func (sm *subSpanMeta) hasProcKey(traceID, spanID string) bool {
	if sm.procCount == 0 {
		return false
	}
	if sm.firstProcKey.TraceID == traceID && sm.firstProcKey.SpanID == spanID {
		return true
	}
	for _, pk := range sm.extraProcKeys {
		if pk.TraceID == traceID && pk.SpanID == spanID {
			return true
		}
	}
	return false
}

func buildSubSpanMeta(idx *SpanIndex, profile Profile, subID string, c *proxypb.TraceSpan) subSpanMeta {
	effSubID := subID
	if effSubID == "" && strings.HasSuffix(c.GetName(), " subscribe") {
		effSubID = strings.TrimSuffix(c.GetName(), " subscribe")
	}
	attrs := c.GetAttributes()
	ackID, hasAckID := AttrString(attrs, "messaging.gcp_pubsub.message.ack_id")
	msgID, hasMsgID := AttrString(attrs, "messaging.message.id")
	orderingKey, hasOrderingKey := AttrString(attrs, "messaging.gcp_pubsub.message.ordering_key")
	var sizeVal int64
	var hasSize bool
	if !profile.UseEnvelopeSize {
		sizeVal, hasSize = AttrInt64(attrs, "messaging.message.body.size")
	} else {
		sizeVal, hasSize = AttrInt64(attrs, "messaging.message.envelope.size")
	}
	da, hasDA := AttrInt64(attrs, "messaging.gcp_pubsub.message.delivery_attempt")
	_, hasDAAny := attrs["messaging.gcp_pubsub.message.delivery_attempt"]
	bc, hasBC := AttrInt64(attrs, "messaging.batch.message_count")
	eos, hasEOS := AttrBool(attrs, profile.EOSSubscribeAttrKey)
	result, hasResult := AttrString(attrs, "messaging.gcp_pubsub.result")

	procName := effSubID + " process"
	var ccCount, schedCount, procCount int
	var procHasAckCalled, procHasNackCalled bool
	var firstProcKey SpanKey
	var extraProcKeys []SpanKey
	for _, ch := range idx.ChildrenOf(c) {
		if ch == nil {
			continue
		}
		name := ch.GetName()
		if name == "subscriber concurrency control" {
			ccCount++
		} else if name == profile.SchedulerSpanName {
			schedCount++
		} else if name == procName {
			procCount++
			for _, ev := range ch.GetEvents() {
				if ev == nil {
					continue
				}
				switch ev.GetName() {
				case "ack called":
					procHasAckCalled = true
				case "nack called":
					procHasNackCalled = true
				}
			}
			pk := SpanKey{TraceID: NormalizeHex(ch.GetTraceId()), SpanID: NormalizeHex(ch.GetSpanId())}
			if procCount == 1 {
				firstProcKey = pk
			} else {
				extraProcKeys = append(extraProcKeys, pk)
			}
		}
	}

	return subSpanMeta{
		effSubID:          effSubID,
		normTraceID:       NormalizeHex(c.GetTraceId()),
		normSpanID:        NormalizeHex(c.GetSpanId()),
		normParentSpanID:  NormalizeHex(c.GetParentSpanId()),
		rawParentEmpty:    c.GetParentSpanId() == "",
		ackID:             ackID,
		hasAckID:          hasAckID,
		msgID:             msgID,
		hasMsgID:          hasMsgID,
		orderingKey:       orderingKey,
		hasOrderingKey:    hasOrderingKey,
		sizeVal:           sizeVal,
		hasSize:           hasSize,
		hasDA:             hasDA,
		da:                da,
		hasDAAny:          hasDAAny,
		hasBC:             hasBC,
		bc:                bc,
		hasEOS:            hasEOS,
		eos:               eos,
		hasResult:         hasResult,
		result:            result,
		isUnset:           c.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET,
		isErr:             c.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
		ccCount:           ccCount,
		schedCount:        schedCount,
		procCount:         procCount,
		procHasAckCalled:  procHasAckCalled,
		procHasNackCalled: procHasNackCalled,
		firstProcKey:      firstProcKey,
		extraProcKeys:     extraProcKeys,
		attrs:             attrs,
	}
}

type subSettleSpanMeta struct {
	isErr       bool
	statusMsg   string
	hasExcept   bool
	hasReceipt  bool
	isReceipt   bool
	hasDeadline bool
	deadline    int64
	hasBC       bool
	bc          int64
}

type subMsgScoreMeta struct {
	msg                SubscribeMessageExpectation
	parsedCBTP         Traceparent
	hasValidCBTP       bool
	parsedWTP          Traceparent
	hasValidWTP        bool
	expectsRoot        bool
	wantSize           int64
	hasWantSize        bool
	exactSize          bool
	wantOutcomeTrue    string
	wantOutcomeFalse   string
	wantValTrue        string
	wantValFalse       string
	ccModeTrue         spanPresence
	schedModeTrue      spanPresence
	procModeTrue       spanPresence
	ccModeFalse        spanPresence
	schedModeFalse     spanPresence
	procModeFalse      spanPresence
	expectSettleEvents bool
	expectsAckCalled   bool
	expectsNackCalled  bool
	expectedSettleOp   string
	opGroups           [][]SettlementExpectation
}

func buildSubMsgScoreMeta(profile Profile, m SubscribeMessageExpectation, opToIdx map[string]int) subMsgScoreMeta {
	expectSettleEvents := m.InterruptKind != SubscribeInterruptForcedShutdownNack && m.InterruptKind != SubscribeInterruptPreDispatchCancel
	if m.ExpectProcessSettlementEvent != nil {
		expectSettleEvents = *m.ExpectProcessSettlementEvent
	}
	sm := subMsgScoreMeta{
		msg:                m,
		expectsRoot:        true,
		expectSettleEvents: expectSettleEvents,
		expectedSettleOp:   expectedSettlementOp(m),
	}
	if m.CallbackTraceparent != "" {
		if cbtp, err := ParseTraceparent(m.CallbackTraceparent); err == nil {
			sm.parsedCBTP = cbtp
			sm.hasValidCBTP = true
		}
	}
	if m.WireTraceparent != "" {
		if wtp, err := ParseTraceparent(m.WireTraceparent); err == nil {
			if wtp.IsSampled() || !profile.RequireSampledParentOnSubscribe {
				sm.expectsRoot = false
				sm.parsedWTP = wtp
				sm.hasValidWTP = true
			}
		}
	}
	if !profile.UseEnvelopeSize {
		if m.BodySize != nil {
			sm.wantSize = *m.BodySize
			sm.hasWantSize = true
			sm.exactSize = true
		} else if m.Data != nil {
			sm.wantSize = int64(len(m.Data))
			sm.hasWantSize = true
			sm.exactSize = true
		}
	} else {
		if m.EnvelopeSize != nil {
			sm.wantSize = *m.EnvelopeSize
			sm.hasWantSize = true
			sm.exactSize = true
		} else if m.Data != nil {
			sm.wantSize = cppEnvelopeSize(m.Data, m.Attributes, m.OrderingKey, m.MessageID)
			sm.hasWantSize = true
		} else if m.BodySize != nil {
			sm.wantSize = 20 + *m.BodySize
			sm.hasWantSize = true
		}
	}
	sm.wantOutcomeTrue = expectedSubscribeOutcome(m, true)
	sm.wantOutcomeFalse = expectedSubscribeOutcome(m, false)
	if v := profile.ResultValues[sm.wantOutcomeTrue]; v != "" {
		sm.wantValTrue = v
	} else {
		sm.wantValTrue = sm.wantOutcomeTrue
	}
	if v := profile.ResultValues[sm.wantOutcomeFalse]; v != "" {
		sm.wantValFalse = v
	} else {
		sm.wantValFalse = sm.wantOutcomeFalse
	}
	sm.ccModeTrue, sm.schedModeTrue, sm.procModeTrue = resolveSubscribeChildModes(m, sm.wantOutcomeTrue)
	sm.ccModeFalse, sm.schedModeFalse, sm.procModeFalse = resolveSubscribeChildModes(m, sm.wantOutcomeFalse)

	if len(m.Settlements) > 0 {
		if len(opToIdx) > 0 {
			sm.opGroups = make([][]SettlementExpectation, len(opToIdx))
		}
		for _, se := range m.Settlements {
			op := strings.ToLower(strings.TrimSpace(se.Operation))
			if op == "" {
				continue
			}
			if op == "ack" {
				sm.expectsAckCalled = true
			} else if op == "nack" {
				sm.expectsNackCalled = true
			}
			if opIdx, ok := opToIdx[op]; ok && sm.opGroups != nil {
				sm.opGroups[opIdx] = append(sm.opGroups[opIdx], se)
			}
		}
	}
	return sm
}

// scoreSettleGroupDistinct computes the exact maximum-weight 1:1 distinct
// assignment bonus between expected settlements wantOp for a single operation
// and unique linked settlement candidate spans cands.
func scoreSettleGroupDistinct[W, C any](wantOp []W, cands []C, bonusFn func(W, C) int64) int64 {
	nw, nc := len(wantOp), len(cands)
	if nw == 0 || nc == 0 {
		return 0
	}
	if nw == 1 {
		w := wantOp[0]
		var best int64
		for _, c := range cands {
			if b := bonusFn(w, c); b > best {
				best = b
			}
		}
		return best
	}
	if nc == 1 {
		c := cands[0]
		var best int64
		for _, w := range wantOp {
			if b := bonusFn(w, c); b > best {
				best = b
			}
		}
		return best
	}
	edges := make([]bipartiteEdge, 0, nw*nc)
	for i, w := range wantOp {
		for j, c := range cands {
			edges = append(edges, bipartiteEdge{
				left:   i,
				right:  j,
				weight: bonusFn(w, c),
			})
		}
	}
	return sumBipartiteMatchedWeights(nw, nc, edges)
}

func sumBipartiteMatchedWeights(nw, nc int, edges []bipartiteEdge) int64 {
	assign := solveBipartiteMatching(nw, nc, nil, nil, edges)
	var total int64
	for i, j := range assign {
		if j >= 0 {
			total += edges[i*nc+j].weight
		}
	}
	return total
}

func scoreSubscribeEdgeFast(
	profile Profile,
	sm subMsgScoreMeta,
	cm subSpanMeta,
	linkedSettle [][]subSettleSpanMeta,
	rj int,
	numOps int,
) (baseScore int64, rawSettle int64) {
	var score int64
	m := sm.msg

	if m.AckID != "" && cm.hasAckID && cm.ackID == m.AckID {
		score += 500_000
	}
	if sm.hasValidCBTP && cm.hasProcKey(sm.parsedCBTP.TraceID, sm.parsedCBTP.SpanID) {
		score += 400_000
	}
	if sm.hasValidWTP && cm.normTraceID == sm.parsedWTP.TraceID && cm.normParentSpanID == sm.parsedWTP.SpanID {
		score += 300_000
	}
	if sm.expectsRoot && cm.rawParentEmpty {
		score += 10_000
	}
	if m.MessageID != "" && cm.hasMsgID && cm.msgID == m.MessageID {
		score += 200_000
	}

	var isEOD bool
	if m.ExactlyOnce != nil {
		isEOD = *m.ExactlyOnce
	} else {
		isEOD = cm.hasEOS && cm.eos
	}
	var wantOutcome, wantVal string
	var ccMode, schedMode, procMode spanPresence
	if isEOD {
		wantOutcome = sm.wantOutcomeTrue
		wantVal = sm.wantValTrue
		ccMode, schedMode, procMode = sm.ccModeTrue, sm.schedModeTrue, sm.procModeTrue
	} else {
		wantOutcome = sm.wantOutcomeFalse
		wantVal = sm.wantValFalse
		ccMode, schedMode, procMode = sm.ccModeFalse, sm.schedModeFalse, sm.procModeFalse
	}

	if wantOutcome == "acked" || wantOutcome == "nacked" || wantOutcome == "expired" {
		if cm.hasResult && cm.result == wantVal {
			score += 20_000
		}
		if wantOutcome == "expired" && cm.procCount == 0 {
			score += 5_000
		}
		if cm.isUnset {
			score += 3_000
		}
	} else if wantOutcome == "eod_failed" {
		if !profile.AllowSubscribeUnsetOnEODError {
			if cm.isErr {
				score += 18_000
				if !cm.hasResult {
					score += 2_000
				}
			}
			if cm.hasEOS && cm.eos {
				score += 6_000
			}
		} else {
			if cm.isUnset {
				score += 10_000
			}
			if cm.hasEOS && cm.eos {
				score += 12_000
			}
			if (sm.expectedSettleOp == "ack" || sm.expectedSettleOp == "nack") && cm.hasResult && cm.result == sm.expectedSettleOp {
				score += 4_000
			} else if sm.expectedSettleOp == "" && cm.hasResult && (cm.result == "ack" || cm.result == "nack") {
				score += 4_000
			}
		}
	} else if cm.isUnset {
		score += 3_000
	}

	var totalSettleBonus int64
	if cm.effSubID != "" && numOps > 0 && len(sm.opGroups) > 0 && len(linkedSettle) > 0 {
		rowBase := rj * numOps
		for opIdx, wantOp := range sm.opGroups {
			if len(wantOp) == 0 {
				continue
			}
			cands := linkedSettle[rowBase+opIdx]
			totalSettleBonus += scoreSettleGroupDistinct(wantOp, cands, func(se SettlementExpectation, spm subSettleSpanMeta) int64 {
				bonus := int64(200)
				if spm.isErr == se.ExpectError {
					bonus += 100
				}
				if se.ExpectError && se.ErrorSubstring != "" && strings.Contains(spm.statusMsg, se.ErrorSubstring) {
					bonus += 50
				}
				if se.ExpectError && se.EODPartialBatchFailure && strings.Contains(spm.statusMsg, eodPartialBatchErrorMsg) {
					bonus += 25
				}
				if profile.RequireExceptionEventOnError && se.ExpectError && !se.EODPartialBatchFailure && spm.hasExcept {
					bonus += 25
				}
				if se.IsReceiptModack != nil && profile.Name == ProfileGo {
					if spm.hasReceipt && spm.isReceipt == *se.IsReceiptModack {
						bonus += 50
					}
				}
				if se.AckDeadlineSeconds > 0 {
					if spm.hasDeadline && spm.deadline == int64(se.AckDeadlineSeconds) {
						bonus += 50
					}
				}
				if se.ExpectedBatchCount > 0 {
					if spm.hasBC && spm.bc == int64(se.ExpectedBatchCount) {
						bonus += 50
					}
				}
				return bonus
			})
		}
	}

	if m.DeliveryAttempt > 0 {
		if cm.hasDA && cm.da == int64(m.DeliveryAttempt) {
			score += 6_000
		}
	} else if profile.Name == ProfileGo {
		if !cm.hasDAAny {
			score += 2_000
		}
	}
	if m.OrderingKey != "" {
		if cm.hasOrderingKey && cm.orderingKey == m.OrderingKey {
			score += 6_000
		}
	} else {
		if (cm.hasOrderingKey && cm.orderingKey == "") || (!cm.hasOrderingKey && !profile.RequireUnconditionalOrderingKey) {
			score += 1_500
		}
	}

	if sm.hasWantSize && cm.hasSize {
		if sm.exactSize {
			if cm.sizeVal == sm.wantSize {
				score += 5_000
			}
		} else if cm.sizeVal >= sm.wantSize {
			score += 5_000
		}
	}

	if profile.Name == ProfileGo && m.BatchCount > 0 {
		if cm.hasBC && cm.bc == int64(m.BatchCount) {
			score += 3_000
		}
	}
	if m.ExactlyOnce != nil {
		if cm.hasEOS && cm.eos == *m.ExactlyOnce {
			score += 3_000
		}
	}
	if len(m.RequireAttributes) > 0 {
		var matchedAttrs int64
		for k, matcher := range m.RequireAttributes {
			if matcher == nil {
				matcher = MatchPresent()
			}
			if av, ok := cm.attrs[k]; ok && av != nil && av.GetValue() != nil {
				if matchedAttr, _, _ := matcher.Match(av); matchedAttr {
					matchedAttrs++
				}
			}
		}
		if int64(len(m.RequireAttributes))*2_000 <= subscribeRequireAttrsCap {
			score += matchedAttrs * 2_000
		} else {
			score += normalizeSettleBonus(matchedAttrs, int64(len(m.RequireAttributes)), subscribeRequireAttrsCap)
		}
	}
	switch ccMode {
	case spanRequired:
		if cm.ccCount > 0 {
			score += 2_000
		}
	case spanForbidden:
		if cm.ccCount == 0 {
			score += 2_000
		}
	}
	switch schedMode {
	case spanRequired:
		if cm.schedCount > 0 {
			score += 2_000
		}
	case spanForbidden:
		if cm.schedCount == 0 {
			score += 2_000
		}
	}
	switch procMode {
	case spanRequired:
		if cm.procCount > 0 {
			score += 2_000
		}
	case spanForbidden:
		if cm.procCount == 0 {
			score += 2_000
		}
	}
	if profile.ProcessEventsEnabled && cm.procCount > 0 && procMode != spanForbidden {
		if !sm.expectSettleEvents {
			if !cm.procHasAckCalled && !cm.procHasNackCalled {
				score += 10_000
			}
		} else if sm.expectsAckCalled || sm.expectsNackCalled {
			if (!sm.expectsAckCalled || cm.procHasAckCalled) && (!sm.expectsNackCalled || cm.procHasNackCalled) {
				score += 10_000
			}
		} else {
			switch wantOutcome {
			case "acked":
				if cm.procHasAckCalled {
					score += 5_000
				}
			case "nacked":
				if cm.procHasNackCalled {
					score += 5_000
				}
			case "":
				if cm.procHasAckCalled || cm.procHasNackCalled {
					score += 5_000
				}
			}
		}
	}

	return score, totalSettleBonus
}

type settleScoreMeta struct {
	hasReceipt  bool
	isReceipt   bool
	hasDeadline bool
	deadline    int64
	hasBC       bool
	bc          int64
	isErr       bool
	statusMsg   string
	hasExcept   bool
}

func buildSettleScoreMeta(sp *proxypb.TraceSpan) settleScoreMeta {
	attrs := sp.GetAttributes()
	isReceipt, hasReceipt := AttrBool(attrs, "messaging.gcp_pubsub.is_receipt_modack")
	deadline, hasDeadline := AttrInt64(attrs, "messaging.gcp_pubsub.message.ack_deadline")
	if !hasDeadline {
		deadline, hasDeadline = AttrInt64(attrs, "messaging.gcp_pubsub.message.ack_deadline_seconds")
	}
	bc, hasBC := AttrInt64(attrs, "messaging.batch.message_count")
	return settleScoreMeta{
		hasReceipt:  hasReceipt,
		isReceipt:   isReceipt,
		hasDeadline: hasDeadline,
		deadline:    deadline,
		hasBC:       hasBC,
		bc:          bc,
		isErr:       sp.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
		statusMsg:   sp.GetStatusMessage(),
		hasExcept:   AssertEvent(sp, "exception") == nil,
	}
}

func scoreSettlementEdgeFast(se SettlementExpectation, spm settleScoreMeta, profile Profile) int64 {
	var score int64
	if se.IsReceiptModack != nil && profile.Name == ProfileGo {
		if spm.hasReceipt && spm.isReceipt == *se.IsReceiptModack {
			score += 100_000
		}
	}
	if se.AckDeadlineSeconds > 0 {
		if spm.hasDeadline && spm.deadline == int64(se.AckDeadlineSeconds) {
			score += 80_000
		}
	}
	if se.ExpectedBatchCount > 0 {
		if spm.hasBC && spm.bc == int64(se.ExpectedBatchCount) {
			score += 60_000
		}
	}
	if spm.isErr == se.ExpectError {
		score += 40_000
	}
	if se.ExpectError && se.ErrorSubstring != "" && strings.Contains(spm.statusMsg, se.ErrorSubstring) {
		score += 30_000
	}
	if se.ExpectError && se.EODPartialBatchFailure && strings.Contains(spm.statusMsg, eodPartialBatchErrorMsg) {
		score += 20_000
	}
	if profile.RequireExceptionEventOnError && se.ExpectError && !se.EODPartialBatchFailure && spm.hasExcept {
		score += 10_000
	}
	return score
}

func scoreSettlementEdge(se SettlementExpectation, sp *proxypb.TraceSpan, profile Profile) int64 {
	return scoreSettlementEdgeFast(se, buildSettleScoreMeta(sp), profile)
}

type unreservedDroppedItem struct {
	droppedIdx int
	wantErr    bool
	allowAck   bool
	allowNack  bool
}

type unlinkedSettleCandidate struct {
	op      string
	sp      *proxypb.TraceSpan
	isErr   bool
	baseOK  int64
	baseErr int64
}

func phase46EdgeWeight(d unreservedDroppedItem, c unlinkedSettleCandidate) int64 {
	w := c.baseOK
	if d.wantErr {
		w = c.baseErr
	}
	if (c.op == "ack" && !d.allowNack) || (c.op == "nack" && !d.allowAck) {
		w++
	}
	if w < 0 {
		w = 0
	}
	return w
}

func matchPhase46Pass(items []unreservedDroppedItem, cands []unlinkedSettleCandidate, caps []int, statusMatchOnly bool) []int {
	return matchPhase46PassImpl(items, cands, caps, statusMatchOnly, 2500)
}

func matchPhase46PassImpl(items []unreservedDroppedItem, cands []unlinkedSettleCandidate, caps []int, statusMatchOnly bool, smallThreshold int) []int {
	assign := make([]int, len(items))
	for i := range assign {
		assign[i] = -1
	}
	if len(items) == 0 || len(cands) == 0 {
		return assign
	}

	var ltItems [6][]int
	var activeLTs []int
	for i, d := range items {
		if !d.allowAck && !d.allowNack {
			continue
		}
		opCode := 0 // 0: ack-only, 1: nack-only, 2: both
		if d.allowAck && d.allowNack {
			opCode = 2
		} else if d.allowNack {
			opCode = 1
		}
		errBit := 0
		if d.wantErr {
			errBit = 1
		}
		lt := opCode*2 + errBit
		if len(ltItems[lt]) == 0 {
			activeLTs = append(activeLTs, lt)
		}
		ltItems[lt] = append(ltItems[lt], i)
	}
	if len(activeLTs) == 0 {
		return assign
	}

	var rtCands [4][]int
	for j, c := range cands {
		if j >= len(caps) || caps[j] <= 0 {
			continue
		}
		rt := 0
		if c.op == "nack" {
			rt = 2
		}
		if c.isErr {
			rt++
		}
		rtCands[rt] = append(rtCands[rt], j)
	}

	ltCompatRT := func(lt, rt int) bool {
		opCode := lt / 2
		if (opCode == 0 && rt >= 2) || (opCode == 1 && rt < 2) {
			return false
		}
		return !statusMatchOnly || (lt%2 == rt%2)
	}

	solveHomogeneous := func(itemIdx []int, compatCands []int) {
		if len(compatCands) == 0 {
			return
		}
		type candChoice struct {
			origR  int
			capVal int
			weight int64
		}
		d0 := items[itemIdx[0]]
		ordered := make([]candChoice, len(compatCands))
		for idx, r := range compatCands {
			ordered[idx] = candChoice{
				origR:  r,
				capVal: caps[r],
				weight: phase46EdgeWeight(d0, cands[r]),
			}
		}
		sort.Slice(ordered, func(a, b int) bool {
			if ordered[a].weight != ordered[b].weight {
				return ordered[a].weight > ordered[b].weight
			}
			return ordered[a].origR < ordered[b].origR
		})
		rem := len(itemIdx)
		type pickedCand struct {
			origR int
			take  int
		}
		picked := make([]pickedCand, 0, len(ordered))
		for _, o := range ordered {
			take := o.capVal
			if take > rem {
				take = rem
			}
			picked = append(picked, pickedCand{origR: o.origR, take: take})
			rem -= take
			if rem == 0 {
				break
			}
		}
		sort.Slice(picked, func(a, b int) bool {
			return picked[a].origR < picked[b].origR
		})
		li := 0
		for _, p := range picked {
			for k := 0; k < p.take; k++ {
				assign[itemIdx[li]] = p.origR
				li++
			}
		}
	}

	if len(activeLTs) == 1 {
		lt := activeLTs[0]
		var compatCands []int
		for rt := 0; rt < 4; rt++ {
			if ltCompatRT(lt, rt) {
				compatCands = append(compatCands, rtCands[rt]...)
			}
		}
		solveHomogeneous(ltItems[lt], compatCands)
		return assign
	}

	var seenLT [6]bool
	var seenRT [4]bool
	for _, startLT := range activeLTs {
		if seenLT[startLT] {
			continue
		}
		seenLT[startLT] = true
		compLTs := []int{startLT}
		var compRTs []int
		lHead, rHead := 0, 0
		for lHead < len(compLTs) || rHead < len(compRTs) {
			for lHead < len(compLTs) {
				lt := compLTs[lHead]
				lHead++
				for rt := 0; rt < 4; rt++ {
					if !seenRT[rt] && len(rtCands[rt]) > 0 && ltCompatRT(lt, rt) {
						seenRT[rt] = true
						compRTs = append(compRTs, rt)
					}
				}
			}
			for rHead < len(compRTs) {
				rt := compRTs[rHead]
				rHead++
				for _, lt := range activeLTs {
					if !seenLT[lt] && ltCompatRT(lt, rt) {
						seenLT[lt] = true
						compLTs = append(compLTs, lt)
					}
				}
			}
		}
		if len(compLTs) == 1 {
			var compatCands []int
			for _, rt := range compRTs {
				compatCands = append(compatCands, rtCands[rt]...)
			}
			solveHomogeneous(ltItems[compLTs[0]], compatCands)
			continue
		}
		solveHetComponentExact(compLTs, compRTs, &ltItems, &rtCands, ltCompatRT, items, cands, caps, assign, smallThreshold)
	}

	return assign
}

type phase46SwapEntry struct {
	diff int64
	sid  int
}

func pushPhase46SwapHeap(h *[]phase46SwapEntry, e phase46SwapEntry) {
	*h = append(*h, e)
	idx := len(*h) - 1
	arr := *h
	for idx > 0 {
		p := (idx - 1) / 2
		if arr[p].diff >= arr[idx].diff {
			break
		}
		arr[p], arr[idx] = arr[idx], arr[p]
		idx = p
	}
}

func popPhase46SwapHeap(h *[]phase46SwapEntry) {
	arr := *h
	n := len(arr) - 1
	arr[0] = arr[n]
	arr = arr[:n]
	*h = arr
	i := 0
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		best := l
		r := l + 1
		if r < n && arr[r].diff > arr[l].diff {
			best = r
		}
		if arr[i].diff >= arr[best].diff {
			break
		}
		arr[i], arr[best] = arr[best], arr[i]
		i = best
	}
}

type phase46HetItem struct {
	origL int
	group int
	lReq  bool
}

type phase46HetCand struct {
	origR int
	cap   int
	lMask uint8
	rReq  bool
	tsig  int
}

func solveHetComponentExact(
	compLTs []int,
	compRTs []int,
	ltItems *[6][]int,
	rtCands *[4][]int,
	ltCompatRT func(int, int) bool,
	items []unreservedDroppedItem,
	cands []unlinkedSettleCandidate,
	caps []int,
	assign []int,
	smallThreshold int,
) {
	type candSig struct {
		rt      int
		baseOK  int64
		baseErr int64
	}
	type keptCand struct {
		origR int
		cap   int
		sid   int
	}

	K := len(compLTs)
	var eligRT [4]int
	for _, rt := range compRTs {
		elig := 0
		for _, lt := range compLTs {
			if ltCompatRT(lt, rt) {
				elig += len(ltItems[lt])
			}
		}
		eligRT[rt] = elig
	}

	sigMap := make(map[candSig]int)
	var sigCap []int
	var sigMask []uint8
	var repOrigR []int
	var kept []keptCand
	for _, rt := range compRTs {
		elig := eligRT[rt]
		for _, j := range rtCands[rt] {
			c := cands[j]
			sig := candSig{rt: rt, baseOK: c.baseOK, baseErr: c.baseErr}
			sid, ok := sigMap[sig]
			if !ok {
				sid = len(sigCap)
				sigMap[sig] = sid
				sigCap = append(sigCap, 0)
				repOrigR = append(repOrigR, j)
				var m uint8
				for li, lt := range compLTs {
					if ltCompatRT(lt, rt) {
						m |= 1 << uint(li)
					}
				}
				sigMask = append(sigMask, m)
			}
			used := sigCap[sid]
			if used >= elig {
				continue
			}
			take := caps[j]
			if take > elig-used {
				take = elig - used
			}
			sigCap[sid] = used + take
			kept = append(kept, keptCand{origR: j, cap: take, sid: sid})
		}
	}
	sort.Slice(kept, func(a, b int) bool {
		return kept[a].origR < kept[b].origR
	})

	S := len(sigCap)
	var supply [6]int
	sigW := make([]int64, K*S)
	sortedS := make([][]int, K)
	for li, lt := range compLTs {
		supply[li] = len(ltItems[lt])
		d0 := items[ltItems[lt][0]]
		for sid := 0; sid < S; sid++ {
			if (sigMask[sid] & (1 << uint(li))) != 0 {
				sigW[li*S+sid] = phase46EdgeWeight(d0, cands[repOrigR[sid]])
				sortedS[li] = append(sortedS[li], sid)
			}
		}
		sort.Slice(sortedS[li], func(a, b int) bool {
			wa, wb := sigW[li*S+sortedS[li][a]], sigW[li*S+sortedS[li][b]]
			if wa != wb {
				return wa > wb
			}
			return sortedS[li][a] < sortedS[li][b]
		})
	}

	// Stage 1: Exact <= 6 x |S| compressed (card, sumW) min-cost max-flow.
	f := make([]int, K*S)
	var rowUsed [6]int
	colUsed := make([]int, S)
	var freePtr [6]int
	swapHeaps := make([][]phase46SwapEntry, K*K)
	inSwapHeap := make([]bool, K*K*S)

	onFlowPositive := func(lb, sid int) {
		m := sigMask[sid]
		for la := 0; la < K; la++ {
			if la != lb && (m&(1<<uint(la))) != 0 {
				idx := la*K + lb
				if !inSwapHeap[idx*S+sid] {
					inSwapHeap[idx*S+sid] = true
					pushPhase46SwapHeap(&swapHeaps[idx], phase46SwapEntry{
						diff: sigW[la*S+sid] - sigW[lb*S+sid],
						sid:  sid,
					})
				}
			}
		}
	}

	for {
		var bestSinkW [6]int64
		var bestSinkSid [6]int
		for li := 0; li < K; li++ {
			bestSinkSid[li] = -1
			for freePtr[li] < len(sortedS[li]) {
				sid := sortedS[li][freePtr[li]]
				if colUsed[sid] < sigCap[sid] {
					bestSinkSid[li] = sid
					bestSinkW[li] = sigW[li*S+sid]
					break
				}
				freePtr[li]++
			}
		}

		var bestSwapDiff [36]int64
		var bestSwapSid [36]int
		for la := 0; la < K; la++ {
			for lb := 0; lb < K; lb++ {
				idx := la*K + lb
				bestSwapSid[idx] = -1
				if la == lb {
					continue
				}
				for len(swapHeaps[idx]) > 0 {
					top := swapHeaps[idx][0]
					if f[lb*S+top.sid] > 0 {
						bestSwapSid[idx] = top.sid
						bestSwapDiff[idx] = top.diff
						break
					}
					popPhase46SwapHeap(&swapHeaps[idx])
					inSwapHeap[idx*S+top.sid] = false
				}
			}
		}

		var distW [6]int64
		var distH [6]int
		var valid [6]bool
		var prevL [6]int
		var prevSid [6]int
		for li := 0; li < K; li++ {
			prevL[li] = -1
			prevSid[li] = -1
			if rowUsed[li] < supply[li] {
				valid[li] = true
			}
		}
		for round := 0; round < K-1; round++ {
			for la := 0; la < K; la++ {
				if !valid[la] {
					continue
				}
				for lb := 0; lb < K; lb++ {
					if la == lb {
						continue
					}
					s := bestSwapSid[la*K+lb]
					if s < 0 {
						continue
					}
					cw := distW[la] + bestSwapDiff[la*K+lb]
					ch := distH[la] + 1
					if !valid[lb] || cw > distW[lb] || (cw == distW[lb] && ch < distH[lb]) {
						valid[lb] = true
						distW[lb] = cw
						distH[lb] = ch
						prevL[lb] = la
						prevSid[lb] = s
					}
				}
			}
		}

		bestLastL := -1
		var bestTotalW int64
		bestTotalH := 0
		for li := 0; li < K; li++ {
			if !valid[li] || bestSinkSid[li] < 0 {
				continue
			}
			tw := distW[li] + bestSinkW[li]
			th := distH[li] + 1
			if bestLastL < 0 || tw > bestTotalW || (tw == bestTotalW && th < bestTotalH) {
				bestLastL = li
				bestTotalW = tw
				bestTotalH = th
			}
		}
		if bestLastL < 0 {
			break
		}

		sEnd := bestSinkSid[bestLastL]
		delta := sigCap[sEnd] - colUsed[sEnd]
		cur := bestLastL
		for prevL[cur] >= 0 {
			s := prevSid[cur]
			if f[cur*S+s] < delta {
				delta = f[cur*S+s]
			}
			cur = prevL[cur]
		}
		startL := cur
		if supply[startL]-rowUsed[startL] < delta {
			delta = supply[startL] - rowUsed[startL]
		}

		rowUsed[startL] += delta
		colUsed[sEnd] += delta
		wasZeroEnd := f[bestLastL*S+sEnd] == 0
		f[bestLastL*S+sEnd] += delta
		if wasZeroEnd {
			onFlowPositive(bestLastL, sEnd)
		}
		cur = bestLastL
		for prevL[cur] >= 0 {
			la := prevL[cur]
			s := prevSid[cur]
			f[cur*S+s] -= delta
			wasZero := f[la*S+s] == 0
			f[la*S+s] += delta
			if wasZero {
				onFlowPositive(la, s)
			}
			cur = la
		}
	}

	// Stage 1 Dual Recovery via (K + 2)-round Bellman-Ford.
	type dualPair struct {
		c int64
		w int64
	}
	var p [6]dualPair
	var pValid [6]bool
	for li := 0; li < K; li++ {
		if rowUsed[li] < supply[li] {
			pValid[li] = true
		}
	}
	q := make([]dualPair, S)
	for round := 0; round < K+2; round++ {
		for li := 0; li < K; li++ {
			for sid := 0; sid < S; sid++ {
				if f[li*S+sid] > 0 {
					candC := q[sid].c + 1
					candW := q[sid].w + sigW[li*S+sid]
					if !pValid[li] || candC < p[li].c || (candC == p[li].c && candW < p[li].w) {
						p[li] = dualPair{c: candC, w: candW}
						pValid[li] = true
					}
				}
			}
		}
		for sid := 0; sid < S; sid++ {
			m := sigMask[sid]
			for li := 0; li < K; li++ {
				if (m & (1 << uint(li))) == 0 {
					continue
				}
				candC := p[li].c - 1
				candW := p[li].w - sigW[li*S+sid]
				if candC < q[sid].c || (candC == q[sid].c && candW < q[sid].w) {
					q[sid] = dualPair{c: candC, w: candW}
				}
			}
		}
	}

	var lReq [6]bool
	for li := 0; li < K; li++ {
		lReq[li] = p[li].c > 0 || (p[li].c == 0 && p[li].w > 0)
	}
	sReq := make([]bool, S)
	for sid := 0; sid < S; sid++ {
		sReq[sid] = q[sid].c < 0 || (q[sid].c == 0 && q[sid].w < 0)
	}
	tight := make([]bool, K*S)
	for li := 0; li < K; li++ {
		for sid := 0; sid < S; sid++ {
			if (sigMask[sid]&(1<<uint(li))) != 0 && p[li].c-q[sid].c == 1 && p[li].w-q[sid].w == sigW[li*S+sid] {
				tight[li*S+sid] = true
			}
		}
	}

	// Stage 2: E_tight connected components, left-group merging, and Shortcuts A/B/C.
	var visL [6]bool
	visS := make([]bool, S)
	for startL := 0; startL < K; startL++ {
		if visL[startL] {
			continue
		}
		visL[startL] = true
		cLs := []int{startL}
		var cSs []int
		lH, sH := 0, 0
		for lH < len(cLs) || sH < len(cSs) {
			for lH < len(cLs) {
				li := cLs[lH]
				lH++
				for sid := 0; sid < S; sid++ {
					if !visS[sid] && tight[li*S+sid] {
						visS[sid] = true
						cSs = append(cSs, sid)
					}
				}
			}
			for sH < len(cSs) {
				sid := cSs[sH]
				sH++
				for li := 0; li < K; li++ {
					if !visL[li] && tight[li*S+sid] {
						visL[li] = true
						cLs = append(cLs, li)
					}
				}
			}
		}
		if len(cSs) == 0 {
			continue
		}

		// Merge left types with identical (lReq, tightNeighbors) into numG <= 6 groups.
		var liGroup [6]int
		var gRepLI [6]int
		var gLReq [6]bool
		numG := 0
		for _, li := range cLs {
			matchedG := -1
			for g := 0; g < numG; g++ {
				if gLReq[g] != lReq[li] {
					continue
				}
				rep := gRepLI[g]
				same := true
				for _, sid := range cSs {
					if tight[rep*S+sid] != tight[li*S+sid] {
						same = false
						break
					}
				}
				if same {
					matchedG = g
					break
				}
			}
			if matchedG < 0 {
				matchedG = numG
				gRepLI[numG] = li
				gLReq[numG] = lReq[li]
				numG++
			}
			liGroup[li] = matchedG
		}

		var cItems []phase46HetItem
		for _, li := range cLs {
			g := liGroup[li]
			req := lReq[li]
			for _, origL := range ltItems[compLTs[li]] {
				cItems = append(cItems, phase46HetItem{origL: origL, group: g, lReq: req})
			}
		}
		sort.Slice(cItems, func(a, b int) bool {
			return cItems[a].origL < cItems[b].origL
		})

		sInComp := make([]bool, S)
		sLMask := make([]uint8, S)
		for _, sid := range cSs {
			sInComp[sid] = true
			var m uint8
			for g := 0; g < numG; g++ {
				if tight[gRepLI[g]*S+sid] {
					m |= 1 << uint(g)
				}
			}
			sLMask[sid] = m
		}

		var cCands []phase46HetCand
		for _, kc := range kept {
			if sInComp[kc.sid] {
				cCands = append(cCands, phase46HetCand{
					origR: kc.origR,
					cap:   kc.cap,
					lMask: sLMask[kc.sid],
					rReq:  sReq[kc.sid],
				})
			}
		}

		// Shortcut C: remOptItems dominance pruning when allLReq == true.
		allLReq := true
		for g := 0; g < numG; g++ {
			if !gLReq[g] {
				allLReq = false
				break
			}
		}
		if allLReq {
			reqCapSum := 0
			for _, rc := range cCands {
				if rc.rReq {
					reqCapSum += rc.cap
				}
			}
			remOptItems := len(cItems) - reqCapSum
			var optCapByMask [64]int
			maxMask := 1 << uint(numG)
			pruned := cCands[:0]
			for _, rc := range cCands {
				if rc.rReq {
					pruned = append(pruned, rc)
					continue
				}
				supCap := 0
				for m := 0; m < maxMask; m++ {
					if (uint8(m) & rc.lMask) == rc.lMask {
						supCap += optCapByMask[m]
					}
				}
				if supCap >= remOptItems {
					continue
				}
				if rc.cap > remOptItems {
					rc.cap = remOptItems
				}
				optCapByMask[rc.lMask] += rc.cap
				pruned = append(pruned, rc)
			}
			cCands = pruned
		}

		// Shortcut A: numG == 1.
		if numG == 1 {
			ordered := make([]phase46HetCand, len(cCands))
			copy(ordered, cCands)
			sort.Slice(ordered, func(a, b int) bool {
				if ordered[a].rReq != ordered[b].rReq {
					return ordered[a].rReq
				}
				return ordered[a].origR < ordered[b].origR
			})
			type pickedSlot struct {
				origR int
				take  int
			}
			var picked []pickedSlot
			rem := len(cItems)
			for _, rc := range ordered {
				take := rc.cap
				if take > rem {
					take = rem
				}
				picked = append(picked, pickedSlot{origR: rc.origR, take: take})
				rem -= take
				if rem == 0 {
					break
				}
			}
			sort.Slice(picked, func(a, b int) bool {
				return picked[a].origR < picked[b].origR
			})
			li := 0
			for _, ps := range picked {
				for k := 0; k < ps.take; k++ {
					assign[cItems[li].origL] = ps.origR
					li++
				}
			}
			continue
		}

		// Assign tsig IDs by (lMask, rReq).
		type tsigKey struct {
			lMask uint8
			rReq  bool
		}
		tsigMap := make(map[tsigKey]int)
		totalCap := 0
		for idx := range cCands {
			tk := tsigKey{lMask: cCands[idx].lMask, rReq: cCands[idx].rReq}
			tid, ok := tsigMap[tk]
			if !ok {
				tid = len(tsigMap)
				tsigMap[tk] = tid
			}
			cCands[idx].tsig = tid
			totalCap += cCands[idx].cap
		}
		numTsigs := len(tsigMap)

		// Shortcut B: numTsigs == 1.
		if numTsigs == 1 {
			take := len(cItems)
			if totalCap < take {
				take = totalCap
			}
			numLReq := 0
			for _, it := range cItems {
				if it.lReq {
					numLReq++
				}
			}
			optBudget := take - numLReq
			ci, remCap := 0, cCands[0].cap
			for _, it := range cItems {
				if !it.lReq {
					if optBudget <= 0 {
						continue
					}
					optBudget--
				}
				for remCap == 0 {
					ci++
					remCap = cCands[ci].cap
				}
				assign[it.origL] = cCands[ci].origR
				remCap--
			}
			continue
		}

		// Stage 3 (small): delegate to solveBipartiteMatching for exact tie-breaking slices.Equal parity.
		if int64(len(cItems))*int64(len(cCands)) <= int64(smallThreshold) {
			tightCaps := make([]int, len(cands))
			for _, rc := range cCands {
				tightCaps[rc.origR] = rc.cap
			}
			tightEdges := make([]bipartiteEdge, 0, len(cItems)*len(cCands))
			for _, it := range cItems {
				d := items[it.origL]
				bit := uint8(1) << uint(it.group)
				for _, rc := range cCands {
					if (rc.lMask & bit) != 0 {
						tightEdges = append(tightEdges, bipartiteEdge{
							left:   it.origL,
							right:  rc.origR,
							weight: phase46EdgeWeight(d, cands[rc.origR]),
						})
					}
				}
			}
			subAssign := solveBipartiteMatching(len(items), len(cands), tightCaps, nil, tightEdges)
			for _, it := range cItems {
				if subAssign[it.origL] >= 0 {
					assign[it.origL] = subAssign[it.origL]
				}
			}
			continue
		}

		// Stage 3 (large): exact int64 capacity-unexpanded SSAP with M_0 dual-seeding & B=32 block hulls.
		solveTightComponentSSAP(len(items), len(cands), cItems, cCands, numG, numTsigs, assign)
	}
}

func solveTightComponentSSAP(
	globalL int,
	globalR int,
	cItems []phase46HetItem,
	cCands []phase46HetCand,
	numG int,
	numTsigs int,
	assign []int,
) {
	mL := len(cItems)
	mR := len(cCands)
	totalCap := 0
	for _, rc := range cCands {
		totalCap += rc.cap
	}
	maxCard := mL
	if totalCap < maxCard {
		maxCard = totalCap
	}
	maxTie := int64(globalL) * int64(globalR)
	B := int64(maxCard)*maxTie + 1

	tsigCols := make([][]int, numTsigs)
	tsigLMask := make([]uint8, numTsigs)
	tsigRReq := make([]bool, numTsigs)
	for c := 0; c < mR; c++ {
		tid := cCands[c].tsig
		tsigCols[tid] = append(tsigCols[tid], c)
		tsigLMask[tid] = cCands[c].lMask
		tsigRReq[tid] = cCands[c].rReq
	}

	const blkSize = 32
	type hullBlock struct {
		tid          int
		start        int
		end          int
		satCount     int
		dirty        bool
		hullCols     []int
		hullM        []int64
		hullQ        []int64
		openGen      int
		openDirty    bool
		openHullCols []int
		openHullM    []int64
		openHullQ    []int64
	}
	var blocks []hullBlock
	tsigBlocksStart := make([]int, numTsigs)
	tsigBlocksEnd := make([]int, numTsigs)
	colBlock := make([]int, mR)
	for tid := 0; tid < numTsigs; tid++ {
		tsigBlocksStart[tid] = len(blocks)
		cols := tsigCols[tid]
		for s := 0; s < len(cols); s += blkSize {
			e := s + blkSize
			if e > len(cols) {
				e = len(cols)
			}
			bid := len(blocks)
			blocks = append(blocks, hullBlock{
				tid:   tid,
				start: s,
				end:   e,
				dirty: true,
			})
			for k := s; k < e; k++ {
				colBlock[cols[k]] = bid
			}
		}
		tsigBlocksEnd[tid] = len(blocks)
	}

	edgeCost := func(r, c int) int64 {
		pri := int64(1)
		if cItems[r].lReq {
			pri++
		}
		if cCands[c].rReq {
			pri++
		}
		return -pri*B - int64(globalL-cItems[r].origL)*int64(globalR-cCands[c].origR)
	}

	colRows := make([][]int, mR)
	used := make([]int, mR)
	rowMatch := make([]int, mL)
	rowSlotInCol := make([]int, mL)
	for r := 0; r < mL; r++ {
		rowMatch[r] = -1
		rowSlotInCol[r] = -1
	}

	// Build initial M_0: exact (numG x numTsigs) maxPri flow + origL-ASC feasibility-guarded tie assignment (plus coarse chunk comparison).
	var gSupply [6]int
	var gLReq [6]bool
	for r := 0; r < mL; r++ {
		g := cItems[r].group
		gSupply[g]++
		gLReq[g] = cItems[r].lReq
	}
	tCap := make([]int, numTsigs)
	for c := 0; c < mR; c++ {
		tCap[cCands[c].tsig] += cCands[c].cap
	}
	priGT := func(g, tid int) int64 {
		p := int64(1)
		if gLReq[g] {
			p++
		}
		if tsigRReq[tid] {
			p++
		}
		return p
	}

	// Solve (numG x numTsigs) exact max-pri flow F via SPFA.
	F := make([]int, numG*numTsigs)
	unmatchedG := make([]int, numG)
	for g := 0; g < numG; g++ {
		unmatchedG[g] = gSupply[g]
	}
	unusedT := make([]int, numTsigs)
	copy(unusedT, tCap)
	{
		nV := numG + numTsigs + 2
		src := numG + numTsigs
		snk := src + 1
		dist := make([]int64, nV)
		inQ := make([]bool, nV)
		prevNode := make([]int, nV)
		qBuf := make([]int, 0, nV*4)
		for {
			for i := 0; i < nV; i++ {
				dist[i] = 1 << 60
				prevNode[i] = -1
				inQ[i] = false
			}
			dist[src] = 0
			qBuf = qBuf[:0]
			qBuf = append(qBuf, src)
			inQ[src] = true
			for qh := 0; qh < len(qBuf); qh++ {
				u := qBuf[qh]
				inQ[u] = false
				du := dist[u]
				if u == src {
					for g := 0; g < numG; g++ {
						if unmatchedG[g] > 0 && du < dist[g] {
							dist[g] = du
							prevNode[g] = src
							if !inQ[g] {
								qBuf = append(qBuf, g)
								inQ[g] = true
							}
						}
					}
				} else if u < numG {
					g := u
					bit := uint8(1) << uint(g)
					for tid := 0; tid < numTsigs; tid++ {
						if (tsigLMask[tid] & bit) == 0 {
							continue
						}
						cand := du - priGT(g, tid)
						tn := numG + tid
						if cand < dist[tn] {
							dist[tn] = cand
							prevNode[tn] = u
							if !inQ[tn] {
								qBuf = append(qBuf, tn)
								inQ[tn] = true
							}
						}
					}
				} else if u < src {
					tid := u - numG
					if unusedT[tid] > 0 && du < dist[snk] {
						dist[snk] = du
						prevNode[snk] = u
						if !inQ[snk] {
							qBuf = append(qBuf, snk)
							inQ[snk] = true
						}
					}
					for g := 0; g < numG; g++ {
						if F[g*numTsigs+tid] > 0 {
							cand := du + priGT(g, tid)
							if cand < dist[g] {
								dist[g] = cand
								prevNode[g] = u
								if !inQ[g] {
									qBuf = append(qBuf, g)
									inQ[g] = true
								}
							}
						}
					}
				}
			}
			if prevNode[snk] < 0 || dist[snk] >= 0 {
				break
			}
			delta := 1 << 30
			cur := snk
			for cur != src {
				pv := prevNode[cur]
				var capE int
				if pv == src {
					capE = unmatchedG[cur]
				} else if cur == snk {
					capE = unusedT[pv-numG]
				} else if pv < numG {
					capE = delta
				} else {
					capE = F[cur*numTsigs+(pv-numG)]
				}
				if capE < delta {
					delta = capE
				}
				cur = pv
			}
			cur = snk
			for cur != src {
				pv := prevNode[cur]
				if pv == src {
					unmatchedG[cur] -= delta
				} else if cur == snk {
					unusedT[pv-numG] -= delta
				} else if pv < numG {
					F[pv*numTsigs+(cur-numG)] += delta
				} else {
					F[cur*numTsigs+(pv-numG)] -= delta
				}
				cur = pv
			}
		}
	}

	// Recover exact max-pri potentials (pPri, qPri) on (numG x numTsigs).
	var pPri [6]int64
	var pPriValid [6]bool
	for g := 0; g < numG; g++ {
		if unmatchedG[g] > 0 {
			pPriValid[g] = true
		}
	}
	qPri := make([]int64, numTsigs)
	for round := 0; round < numG+2; round++ {
		for g := 0; g < numG; g++ {
			for tid := 0; tid < numTsigs; tid++ {
				if F[g*numTsigs+tid] > 0 {
					cand := qPri[tid] + priGT(g, tid)
					if !pPriValid[g] || cand < pPri[g] {
						pPri[g] = cand
						pPriValid[g] = true
					}
				}
			}
		}
		for tid := 0; tid < numTsigs; tid++ {
			m := tsigLMask[tid]
			for g := 0; g < numG; g++ {
				if (m & (1 << uint(g))) == 0 {
					continue
				}
				cand := pPri[g] - priGT(g, tid)
				if cand < qPri[tid] {
					qPri[tid] = cand
				}
			}
		}
	}

	// Build M_origL by iterating rows in origL ASC, picking smallest origR among pri-tight reroutable tsigs.
	{
		Fwork := make([]int, len(F))
		copy(Fwork, F)
		unmWork := make([]int, numG)
		copy(unmWork, unmatchedG)
		unuWork := make([]int, numTsigs)
		copy(unuWork, unusedT)

		tsigPtr := make([]int, numTsigs)
		tsigUsedHead := make([]int, numTsigs)
		candTIDs := make([]int, 0, numTsigs)

		nBFS := numTsigs + numG + 2
		srcBFS := numTsigs + numG
		snkBFS := srcBFS + 1
		visBFS := make([]int, nBFS)
		parNode := make([]int, nBFS)
		parType := make([]int, nBFS)
		bfsQ := make([]int, 0, nBFS)
		bfsGen := 0

		tryReroute := func(g, startTID int) bool {
			if Fwork[g*numTsigs+startTID] > 0 {
				Fwork[g*numTsigs+startTID]--
				return true
			}
			bfsGen++
			bfsQ = bfsQ[:0]
			visBFS[startTID] = bfsGen
			parNode[startTID] = -1
			bfsQ = append(bfsQ, startTID)

			applyEnd := func(cur int, endStep int) {
				if endStep == 1 {
					Fwork[g*numTsigs+cur]--
				} else {
					unmWork[g]--
				}
				for cur != startTID {
					pv := parNode[cur]
					pt := parType[cur]
					switch pt {
					case 1:
						Fwork[(cur-numTsigs)*numTsigs+pv]--
					case 2:
						unuWork[pv]--
					case 3:
						unuWork[cur]++
					case 4:
						Fwork[(pv-numTsigs)*numTsigs+cur]++
					case 5:
						unmWork[pv-numTsigs]++
					case 6:
						unmWork[cur-numTsigs]--
					}
					cur = pv
				}
			}

			for qh := 0; qh < len(bfsQ); qh++ {
				u := bfsQ[qh]
				if u < numTsigs {
					t := u
					if Fwork[g*numTsigs+t] > 0 {
						applyEnd(t, 1)
						return true
					}
					for gp := 0; gp < numG; gp++ {
						if gp != g && Fwork[gp*numTsigs+t] > 0 {
							gn := numTsigs + gp
							if visBFS[gn] != bfsGen {
								visBFS[gn] = bfsGen
								parNode[gn] = u
								parType[gn] = 1
								bfsQ = append(bfsQ, gn)
							}
						}
					}
					if unuWork[t] > 0 && qPri[t] == 0 {
						if visBFS[snkBFS] != bfsGen {
							visBFS[snkBFS] = bfsGen
							parNode[snkBFS] = u
							parType[snkBFS] = 2
							bfsQ = append(bfsQ, snkBFS)
						}
					}
				} else if u == snkBFS {
					for tp := 0; tp < numTsigs; tp++ {
						if qPri[tp] == 0 && visBFS[tp] != bfsGen {
							visBFS[tp] = bfsGen
							parNode[tp] = u
							parType[tp] = 3
							bfsQ = append(bfsQ, tp)
						}
					}
				} else if u == srcBFS {
					if unmWork[g] > 0 && pPri[g] == 0 {
						applyEnd(u, 2)
						return true
					}
					for gp := 0; gp < numG; gp++ {
						if gp != g && unmWork[gp] > 0 && pPri[gp] == 0 {
							gn := numTsigs + gp
							if visBFS[gn] != bfsGen {
								visBFS[gn] = bfsGen
								parNode[gn] = u
								parType[gn] = 6
								bfsQ = append(bfsQ, gn)
							}
						}
					}
				} else {
					gp := u - numTsigs
					bit := uint8(1) << uint(gp)
					for tp := 0; tp < numTsigs; tp++ {
						if (tsigLMask[tp]&bit) != 0 && pPri[gp]-qPri[tp] == priGT(gp, tp) && visBFS[tp] != bfsGen {
							visBFS[tp] = bfsGen
							parNode[tp] = u
							parType[tp] = 4
							bfsQ = append(bfsQ, tp)
						}
					}
					if pPri[gp] == 0 && visBFS[srcBFS] != bfsGen {
						visBFS[srcBFS] = bfsGen
						parNode[srcBFS] = u
						parType[srcBFS] = 5
						bfsQ = append(bfsQ, srcBFS)
					}
				}
			}
			return false
		}

		for r := 0; r < mL; r++ {
			g := cItems[r].group
			bit := uint8(1) << uint(g)
			candTIDs = candTIDs[:0]
			for tid := 0; tid < numTsigs; tid++ {
				if (tsigLMask[tid]&bit) == 0 || tsigPtr[tid] >= len(tsigCols[tid]) {
					continue
				}
				if pPri[g]-qPri[tid] == priGT(g, tid) {
					candTIDs = append(candTIDs, tid)
				}
			}
			sort.Slice(candTIDs, func(a, b int) bool {
				ca := tsigCols[candTIDs[a]][tsigPtr[candTIDs[a]]]
				cb := tsigCols[candTIDs[b]][tsigPtr[candTIDs[b]]]
				return cCands[ca].origR < cCands[cb].origR
			})
			chosenTID := -1
			for _, tid := range candTIDs {
				if tryReroute(g, tid) {
					chosenTID = tid
					break
				}
			}
			if chosenTID >= 0 {
				c := tsigCols[chosenTID][tsigPtr[chosenTID]]
				rowMatch[r] = c
				used[c]++
				tsigUsedHead[chosenTID]++
				if tsigUsedHead[chosenTID] == cCands[c].cap {
					tsigPtr[chosenTID]++
					tsigUsedHead[chosenTID] = 0
				}
			} else {
				unmWork[g]--
			}
		}
	}

	// Build coarse <= 48-target-chunk M_chunk via DAG-initialized O(V^2) Johnson Dijkstra; keep better (pri, tie).
	if mL >= 64 && numTsigs <= 48 && maxTie <= (1<<50)/int64(maxCard+1) {
		var gRows [6][]int
		for r := 0; r < mL; r++ {
			gRows[cItems[r].group] = append(gRows[cItems[r].group], r)
		}
		chunkL := (mL + 47) / 48
		type lChunk struct {
			g      int
			rows   []int
			avgOL2 int64
		}
		var lChunks []lChunk
		for g := 0; g < numG; g++ {
			rows := gRows[g]
			for s := 0; s < len(rows); s += chunkL {
				e := s + chunkL
				if e > len(rows) {
					e = len(rows)
				}
				sub := rows[s:e]
				var sumOL int64
				for _, r := range sub {
					sumOL += int64(cItems[r].origL)
				}
				lChunks = append(lChunks, lChunk{g: g, rows: sub, avgOL2: (2 * sumOL) / int64(len(sub))})
			}
		}

		chunkR := (totalCap + 47) / 48
		type rSlotRun struct {
			c   int
			cnt int
		}
		type rChunk struct {
			tid    int
			lMask  uint8
			rReq   bool
			cap    int
			runs   []rSlotRun
			avgOR2 int64
		}
		var rChunks []rChunk
		for tid := 0; tid < numTsigs; tid++ {
			cols := tsigCols[tid]
			ci := 0
			remInC := 0
			if len(cols) > 0 {
				remInC = cCands[cols[0]].cap
			}
			for ci < len(cols) {
				var runs []rSlotRun
				taken := 0
				var sumOR int64
				for ci < len(cols) && taken < chunkR {
					take := chunkR - taken
					if remInC < take {
						take = remInC
					}
					c := cols[ci]
					runs = append(runs, rSlotRun{c: c, cnt: take})
					sumOR += int64(take) * int64(cCands[c].origR)
					taken += take
					remInC -= take
					if remInC == 0 {
						ci++
						if ci < len(cols) {
							remInC = cCands[cols[ci]].cap
						}
					}
				}
				rChunks = append(rChunks, rChunk{
					tid:    tid,
					lMask:  tsigLMask[tid],
					rReq:   tsigRReq[tid],
					cap:    taken,
					runs:   runs,
					avgOR2: (2 * sumOR) / int64(taken),
				})
			}
		}

		if len(lChunks) <= 64 && len(rChunks) <= 104 {
			nLC := len(lChunks)
			nRC := len(rChunks)
			nV := nLC + nRC + 2
			src := nLC + nRC
			snk := src + 1
			type ckEdge struct {
				to, rev, cap int
				cost         int64
			}
			adj := make([][]ckEdge, nV)
			addE := func(u, v, cap int, cost int64) {
				adj[u] = append(adj[u], ckEdge{to: v, rev: len(adj[v]), cap: cap, cost: cost})
				adj[v] = append(adj[v], ckEdge{to: u, rev: len(adj[u]) - 1, cap: 0, cost: -cost})
			}
			Bck := int64(4)*int64(maxCard)*maxTie + 1
			for li, lc := range lChunks {
				addE(src, li, len(lc.rows), 0)
			}
			for ri, rc := range rChunks {
				addE(nLC+ri, snk, rc.cap, 0)
			}
			for li, lc := range lChunks {
				bit := uint8(1) << uint(lc.g)
				for ri, rc := range rChunks {
					if (rc.lMask & bit) == 0 {
						continue
					}
					pri := int64(1)
					if gLReq[lc.g] {
						pri++
					}
					if rc.rReq {
						pri++
					}
					tie4 := (2*int64(globalL) - lc.avgOL2) * (2*int64(globalR) - rc.avgOR2)
					addE(li, nLC+ri, min(len(lc.rows), rc.cap), -pri*Bck-tie4)
				}
			}
			const infPot = int64(1) << 61
			pot := make([]int64, nV)
			for i := nLC; i < nV; i++ {
				pot[i] = infPot
			}
			for li := 0; li < nLC; li++ {
				for _, e := range adj[li] {
					if e.cap > 0 && pot[li]+e.cost < pot[e.to] {
						pot[e.to] = pot[li] + e.cost
					}
				}
			}
			for ri := 0; ri < nRC; ri++ {
				if pot[nLC+ri] < pot[snk] {
					pot[snk] = pot[nLC+ri]
				}
			}
			for i := range pot {
				if pot[i] == infPot {
					pot[i] = 0
				}
			}
			dist := make([]int64, nV)
			vis := make([]bool, nV)
			pV := make([]int, nV)
			pE := make([]int, nV)
			for {
				for i := 0; i < nV; i++ {
					dist[i] = infPot
					vis[i] = false
					pV[i] = -1
				}
				dist[src] = 0
				for step := 0; step < nV; step++ {
					u := -1
					bestD := infPot
					for i := 0; i < nV; i++ {
						if !vis[i] && dist[i] < bestD {
							bestD = dist[i]
							u = i
						}
					}
					if u < 0 {
						break
					}
					vis[u] = true
					pu := pot[u]
					for ei, e := range adj[u] {
						if e.cap > 0 {
							nd := bestD + e.cost + pu - pot[e.to]
							if nd < dist[e.to] {
								dist[e.to] = nd
								pV[e.to] = u
								pE[e.to] = ei
							}
						}
					}
				}
				if pV[snk] < 0 || dist[snk]+pot[snk]-pot[src] >= 0 {
					break
				}
				for i := 0; i < nV; i++ {
					if dist[i] < infPot {
						pot[i] += dist[i]
					}
				}
				delta := 1 << 30
				for cur := snk; cur != src; cur = pV[cur] {
					if c := adj[pV[cur]][pE[cur]].cap; c < delta {
						delta = c
					}
				}
				for cur := snk; cur != src; cur = pV[cur] {
					e := &adj[pV[cur]][pE[cur]]
					e.cap -= delta
					adj[cur][e.rev].cap += delta
				}
			}
			ckMatch := make([]int, mL)
			for r := 0; r < mL; r++ {
				ckMatch[r] = -1
			}
			ckUsed := make([]int, mR)
			rPos := make([]int, nRC)
			rRunRem := make([]int, nRC)
			for ri, rc := range rChunks {
				if len(rc.runs) > 0 {
					rRunRem[ri] = rc.runs[0].cnt
				}
			}
			for li, lc := range lChunks {
				rowPos := 0
				for _, e := range adj[li] {
					if e.to >= nLC && e.to < src {
						ri := e.to - nLC
						revCap := adj[e.to][e.rev].cap
						for k := 0; k < revCap; k++ {
							r := lc.rows[rowPos]
							rowPos++
							for rRunRem[ri] == 0 {
								rPos[ri]++
								rRunRem[ri] = rChunks[ri].runs[rPos[ri]].cnt
							}
							c := rChunks[ri].runs[rPos[ri]].c
							rRunRem[ri]--
							ckMatch[r] = c
							ckUsed[c]++
						}
					}
				}
			}
			rePairLMaskMatch := func(rm []int, us []int) {
				var mRows [64][]int
				var mCols [64][]int
				for r := 0; r < mL; r++ {
					if c := rm[r]; c >= 0 {
						mRows[cCands[c].lMask] = append(mRows[cCands[c].lMask], r)
					}
				}
				for c := 0; c < mR; c++ {
					if us[c] > 0 {
						mCols[cCands[c].lMask] = append(mCols[cCands[c].lMask], c)
					}
				}
				for lm := 0; lm < 64; lm++ {
					rows := mRows[lm]
					if len(rows) == 0 {
						continue
					}
					cols := mCols[lm]
					cnts := make([]int, len(cols))
					for i, c := range cols {
						cnts[i] = us[c]
						us[c] = 0
					}
					ci := 0
					for _, r := range rows {
						for cnts[ci] == 0 {
							ci++
						}
						c := cols[ci]
						cnts[ci]--
						rm[r] = c
						us[c]++
					}
				}
			}
			rePairLMaskMatch(rowMatch, used)
			rePairLMaskMatch(ckMatch, ckUsed)
			var costOrig, costChunk int64
			for r := 0; r < mL; r++ {
				if c := rowMatch[r]; c >= 0 {
					costOrig += edgeCost(r, c)
				}
				if c := ckMatch[r]; c >= 0 {
					costChunk += edgeCost(r, c)
				}
			}
			if costChunk < costOrig {
				copy(rowMatch, ckMatch)
				copy(used, ckUsed)
			}
		}
	}

	// Property 4 (generalized to lMask): monotonically re-pair assigned items sharing lMask in (origL ASC, origR ASC).
	var maskRows [64][]int
	var maskUsedCols [64][]int
	for r := 0; r < mL; r++ {
		if c := rowMatch[r]; c >= 0 {
			maskRows[cCands[c].lMask] = append(maskRows[cCands[c].lMask], r)
		}
	}
	for c := 0; c < mR; c++ {
		if used[c] > 0 {
			maskUsedCols[cCands[c].lMask] = append(maskUsedCols[cCands[c].lMask], c)
		}
	}
	for lm := 0; lm < 64; lm++ {
		rows := maskRows[lm]
		if len(rows) == 0 {
			continue
		}
		cols := maskUsedCols[lm]
		cnts := make([]int, len(cols))
		for i, c := range cols {
			cnts[i] = used[c]
			used[c] = 0
		}
		ci := 0
		for _, r := range rows {
			for cnts[ci] == 0 {
				ci++
			}
			c := cols[ci]
			cnts[ci]--
			rowMatch[r] = c
			rowSlotInCol[r] = len(colRows[c])
			colRows[c] = append(colRows[c], r)
			used[c]++
		}
	}

	v := make([]int64, mR)
	colIsSat := make([]bool, mR)
	colClosedGen := make([]int, mR)
	tsigFreeHead := make([]int, numTsigs)

	refreshSatAndFree := func() {
		for bid := range blocks {
			blocks[bid].satCount = 0
			blocks[bid].dirty = true
		}
		for c := 0; c < mR; c++ {
			sat := used[c] == cCands[c].cap
			colIsSat[c] = sat
			if sat {
				blocks[colBlock[c]].satCount++
			}
		}
		for tid := 0; tid < numTsigs; tid++ {
			fh := -1
			for _, c := range tsigCols[tid] {
				if used[c] < cCands[c].cap {
					fh = c
					break
				}
			}
			tsigFreeHead[tid] = fh
		}
	}

	buildBlockHull := func(blk *hullBlock, closedGen int, dstCols *[]int, dstM, dstQ *[]int64) {
		*dstCols = (*dstCols)[:0]
		*dstM = (*dstM)[:0]
		*dstQ = (*dstQ)[:0]
		cols := tsigCols[blk.tid]
		for k := blk.start; k < blk.end; k++ {
			c := cols[k]
			if !colIsSat[c] || colClosedGen[c] == closedGen {
				continue
			}
			m := int64(cCands[c].origR)
			q := -v[c]
			for len(*dstCols) >= 2 {
				n := len(*dstCols)
				m1, q1 := (*dstM)[n-2], (*dstQ)[n-2]
				m2, q2 := (*dstM)[n-1], (*dstQ)[n-1]
				if (q1-q2)*(m-m2) < (q2-q)*(m2-m1) {
					*dstCols = (*dstCols)[:n-1]
					*dstM = (*dstM)[:n-1]
					*dstQ = (*dstQ)[:n-1]
				} else {
					break
				}
			}
			*dstCols = append(*dstCols, c)
			*dstM = append(*dstM, m)
			*dstQ = append(*dstQ, q)
		}
	}

	rebuildBlockHull := func(bid int) {
		blk := &blocks[bid]
		blk.dirty = false
		buildBlockHull(blk, -1, &blk.hullCols, &blk.hullM, &blk.hullQ)
	}

	queryHullBestIdx := func(hullM, hullQ []int64, x int64) (int, int64) {
		lo, hi := 0, len(hullM)-1
		for lo < hi {
			mid := (lo + hi) / 2
			if hullM[mid]*x+hullQ[mid] <= hullM[mid+1]*x+hullQ[mid+1] {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		return lo, hullM[lo]*x + hullQ[lo]
	}

	queryBlockBestIdx := func(bid int, x int64) (int, int64) {
		blk := &blocks[bid]
		if blk.dirty {
			rebuildBlockHull(bid)
		}
		return queryHullBestIdx(blk.hullM, blk.hullQ, x)
	}

	queryTsigBestCandCol := func(tid int, r int) (int, int64) {
		x := int64(globalL - cItems[r].origL)
		var bestCostMinusV int64
		bestCol := -1
		if fh := tsigFreeHead[tid]; fh >= 0 {
			bestCostMinusV = edgeCost(r, fh)
			bestCol = fh
		}
		for bid := tsigBlocksStart[tid]; bid < tsigBlocksEnd[tid]; bid++ {
			if blocks[bid].satCount == 0 {
				continue
			}
			lo, _ := queryBlockBestIdx(bid, x)
			c := blocks[bid].hullCols[lo]
			cand := edgeCost(r, c) - v[c]
			if bestCol < 0 || cand < bestCostMinusV {
				bestCostMinusV = cand
				bestCol = c
			}
		}
		return bestCol, bestCostMinusV
	}

	queryTsigBestCand := func(tid int, r int) int64 {
		_, val := queryTsigBestCandCol(tid, r)
		return val
	}

	// Seed v[c] via global-origR monotone chain (if minRowL non-decreasing) or per-tsig + Bellman-Ford shift.
	satCols := make([]int, 0, mR)
	for c := 0; c < mR; c++ {
		if used[c] == cCands[c].cap {
			satCols = append(satCols, c)
		}
	}
	runBFShift := func(vArr []int64) []int64 {
		copy(v, vArr)
		refreshSatAndFree()
		minDiff := make([]int64, numTsigs*numTsigs)
		hasDiff := make([]bool, numTsigs*numTsigs)
		for r := 0; r < mL; r++ {
			c1 := rowMatch[r]
			if c1 < 0 || used[c1] < cCands[c1].cap {
				continue
			}
			tid1 := cCands[c1].tsig
			base1 := edgeCost(r, c1) - v[c1]
			bit := uint8(1) << uint(cItems[r].group)
			for tid2 := 0; tid2 < numTsigs; tid2++ {
				if tid2 == tid1 || (tsigLMask[tid2]&bit) == 0 {
					continue
				}
				best2 := queryTsigBestCand(tid2, r)
				diff := best2 - base1
				idx := tid1*numTsigs + tid2
				if !hasDiff[idx] || diff < minDiff[idx] {
					minDiff[idx] = diff
					hasDiff[idx] = true
				}
			}
		}
		shift := make([]int64, numTsigs)
		for iter := 0; iter < numTsigs; iter++ {
			updated := false
			for tid1 := 0; tid1 < numTsigs; tid1++ {
				for tid2 := 0; tid2 < numTsigs; tid2++ {
					if tsigFreeHead[tid2] >= 0 || !hasDiff[tid1*numTsigs+tid2] {
						continue
					}
					cand := shift[tid1] + minDiff[tid1*numTsigs+tid2]
					if cand < shift[tid2] {
						shift[tid2] = cand
						updated = true
					}
				}
			}
			if !updated {
				break
			}
		}
		out := make([]int64, mR)
		copy(out, vArr)
		for tid := 0; tid < numTsigs; tid++ {
			if sh := shift[tid]; sh != 0 {
				for _, c := range tsigCols[tid] {
					if used[c] == cCands[c].cap {
						out[c] += sh
					}
				}
			}
		}
		return out
	}

	computeU := func() []int64 {
		refreshSatAndFree()
		uOut := make([]int64, mL)
		for r := 0; r < mL; r++ {
			best := int64(0)
			bit := uint8(1) << uint(cItems[r].group)
			for tid := 0; tid < numTsigs; tid++ {
				if (tsigLMask[tid] & bit) == 0 {
					continue
				}
				if cand := queryTsigBestCand(tid, r); cand < best {
					best = cand
				}
			}
			uOut[r] = best
		}
		return uOut
	}

	evalDual := func(vCand []int64) ([]int64, int) {
		copy(v, vCand)
		uCand := computeU()
		drop := 0
		for r := 0; r < mL; r++ {
			if c := rowMatch[r]; c >= 0 && edgeCost(r, c)-uCand[r]-v[c] != 0 {
				drop++
			}
		}
		return uCand, drop
	}

	relaxDualVector := func(vInit, uInit []int64, dropInit, maxPasses int) ([]int64, []int64, int) {
		bestV := make([]int64, mR)
		copy(bestV, vInit)
		bestU := uInit
		bestDrop := dropInit
		copy(v, vInit)
		curU := uInit
		for pass := 0; pass < maxPasses && bestDrop > 0; pass++ {
			changed := false
			for _, c := range satCols {
				slack := -v[c]
				for _, r := range colRows[c] {
					if s := edgeCost(r, c) - curU[r] - v[c]; s < slack {
						slack = s
					}
				}
				if slack > 0 {
					v[c] += slack
					changed = true
				}
			}
			if !changed {
				break
			}
			curU = computeU()
			drop := 0
			for r := 0; r < mL; r++ {
				if c := rowMatch[r]; c >= 0 && edgeCost(r, c)-curU[r]-v[c] != 0 {
					drop++
				}
			}
			if drop < bestDrop {
				bestDrop = drop
				copy(bestV, v)
				bestU = curU
			}
		}
		return bestV, bestU, bestDrop
	}

	swapRemoveRowFromCol := func(r, c int) {
		idx := rowSlotInCol[r]
		lastIdx := len(colRows[c]) - 1
		if idx != lastIdx {
			moved := colRows[c][lastIdx]
			colRows[c][idx] = moved
			rowSlotInCol[moved] = idx
		}
		colRows[c] = colRows[c][:lastIdx]
		rowMatch[r] = -1
		rowSlotInCol[r] = -1
		used[c]--
	}

	chainColsV := func(buckets [][]int) []int64 {
		out := make([]int64, mR)
		for _, cols := range buckets {
			if len(cols) == 0 {
				continue
			}
			var vTie int64
			var maxV int64
			vList := make([]int64, len(cols))
			for p := len(cols) - 1; p >= 0; p-- {
				c := cols[p]
				if p < len(cols)-1 {
					prevC := cols[p+1]
					minL2 := cItems[colRows[prevC][0]].origL
					vTie -= int64(globalL-minL2) * int64(cCands[prevC].origR-cCands[c].origR)
				}
				vVal := vTie
				if cCands[c].rReq {
					vVal -= B
				}
				vList[p] = vVal
				if p == len(cols)-1 || vVal > maxV {
					maxV = vVal
				}
			}
			for p, c := range cols {
				out[c] = vList[p] - maxV
			}
		}
		return out
	}

	seedDuals := func() ([]int64, int) {
		satCols = satCols[:0]
		for c := 0; c < mR; c++ {
			if used[c] == cCands[c].cap {
				satCols = append(satCols, c)
			}
		}
		if len(satCols) == 0 {
			for c := 0; c < mR; c++ {
				v[c] = 0
			}
			uNew := computeU()
			drop := 0
			for r := 0; r < mL; r++ {
				if c := rowMatch[r]; c >= 0 && edgeCost(r, c)-uNew[r]-v[c] != 0 {
					drop++
				}
			}
			return uNew, drop
		}
		mergedMono := true
		for p := 0; p+1 < len(satCols); p++ {
			if cItems[colRows[satCols[p]][0]].origL > cItems[colRows[satCols[p+1]][0]].origL {
				mergedMono = false
				break
			}
		}
		if mergedMono {
			vMono := runBFShift(chainColsV([][]int{satCols}))
			uMono, dropMono := evalDual(vMono)
			if dropMono == 0 {
				copy(v, vMono)
				return uMono, 0
			}
		}
		var maskSatCols [64][]int
		for _, c := range satCols {
			m := cCands[c].lMask
			maskSatCols[m] = append(maskSatCols[m], c)
		}
		vM := runBFShift(chainColsV(maskSatCols[:]))
		uM, dropM := evalDual(vM)
		bestV, bestU, bestDrop := vM, uM, dropM
		if bestDrop > 0 && B <= (1<<50) {
			var monoRuns [][]int
			curRun := []int{satCols[0]}
			for p := 1; p < len(satCols); p++ {
				cPrev, cCur := satCols[p-1], satCols[p]
				if cItems[colRows[cPrev][0]].origL > cItems[colRows[cCur][0]].origL {
					monoRuns = append(monoRuns, curRun)
					curRun = []int{cCur}
				} else {
					curRun = append(curRun, cCur)
				}
			}
			monoRuns = append(monoRuns, curRun)
			vR := runBFShift(chainColsV(monoRuns))
			uR, dropR := evalDual(vR)
			if dropR < bestDrop {
				bestV, bestU, bestDrop = vR, uR, dropR
			}
		}
		if bestDrop > 0 && B <= (1<<50) {
			var grpSatCols [6][]int
			for _, c := range satCols {
				g := cItems[colRows[c][0]].group
				grpSatCols[g] = append(grpSatCols[g], c)
			}
			vA := runBFShift(chainColsV(grpSatCols[:numG]))
			uA, dropA := evalDual(vA)
			if dropA < bestDrop {
				bestV, bestU, bestDrop = vA, uA, dropA
			}
		}
		if bestDrop > 0 && B <= (1<<50) {
			vB := make([]int64, mR)
			curMinL := cItems[colRows[satCols[len(satCols)-1]][0]].origL
			for p := len(satCols) - 2; p >= 0; p-- {
				c1, c2 := satCols[p], satCols[p+1]
				if l2 := cItems[colRows[c2][0]].origL; l2 < curMinL {
					curMinL = l2
				}
				vB[c1] = vB[c2] - int64(globalL-curMinL)*int64(cCands[c2].origR-cCands[c1].origR)
			}
			vB = runBFShift(vB)
			uB, dropB := evalDual(vB)
			if dropB < bestDrop {
				bestV, bestU, bestDrop = vB, uB, dropB
			}
		}
		if bestDrop > 0 && B <= (1<<50) {
			type dEdge struct {
				to int
				r  int
				w  int64
			}
			adjD := make([][]dEdge, mR)
			buildColEdges := func(c1 int) {
				adjD[c1] = adjD[c1][:0]
				var mask1 uint8
				for _, r := range colRows[c1] {
					mask1 |= uint8(1) << uint(cItems[r].group)
				}
				or1 := cCands[c1].origR
				for tid2 := 0; tid2 < numTsigs; tid2++ {
					if (tsigLMask[tid2] & mask1) == 0 {
						continue
					}
					cols2 := tsigCols[tid2]
					pos, hiS := 0, len(cols2)
					for pos < hiS {
						mid := int(uint(pos+hiS) >> 1)
						if cCands[cols2[mid]].origR >= or1 {
							hiS = mid
						} else {
							pos = mid + 1
						}
					}
					loIdx := pos - 1
					if loIdx < 0 {
						loIdx = 0
					}
					hiIdx := pos + 1
					if hiIdx >= len(cols2) {
						hiIdx = len(cols2) - 1
					}
					addColEdge := func(c2 int) {
						if c2 == c1 {
							return
						}
						var minW int64
						bestR := -1
						m2 := cCands[c2].lMask
						for _, r := range colRows[c1] {
							if (m2 & (1 << uint(cItems[r].group))) == 0 {
								continue
							}
							w := edgeCost(r, c2) - edgeCost(r, c1)
							if bestR < 0 || w < minW {
								minW = w
								bestR = r
							}
						}
						if bestR >= 0 {
							adjD[c1] = append(adjD[c1], dEdge{to: c2, r: bestR, w: minW})
						}
					}
					for k := loIdx; k <= hiIdx; k++ {
						addColEdge(cols2[k])
					}
					if fh := tsigFreeHead[tid2]; fh >= 0 && (fh < cols2[loIdx] || fh > cols2[hiIdx]) {
						addColEdge(fh)
					}
				}
			}
			for _, c1 := range satCols {
				buildColEdges(c1)
			}

			vS := make([]int64, mR)
			parCol := make([]int, mR)
			parRow := make([]int, mR)
			initNegOne := make([]int, mR)
			for i := 0; i < mR; i++ {
				initNegOne[i] = -1
			}
			pushCnt := make([]int, mR)
			inQ := make([]bool, mR)
			hasPopped := make([]bool, mR)
			capDQ := 1
			for capDQ < mR*2+16 {
				capDQ <<= 1
			}
			dqMask := capDQ - 1
			dq := make([]int, capDQ)
			colDirty := make([]bool, mR)
			seenCycle := make([]int, mR)
			cycGen := 0
			touchedPass := make([]int, mR)
			passGen := 0
			cancelsLeft := 2048
			var mRowsPass [64][]int
			var mColsPass [64][]int
			cntsBuf := make([]int, mR)
			passTouched := make([]int, 0, mR)

			swapAssignRow := func(r, oldC, newC int) {
				idx := rowSlotInCol[r]
				lastIdx := len(colRows[oldC]) - 1
				if idx != lastIdx {
					moved := colRows[oldC][lastIdx]
					colRows[oldC][idx] = moved
					rowSlotInCol[moved] = idx
				}
				colRows[oldC] = colRows[oldC][:lastIdx]
				rowMatch[r] = newC
				rowSlotInCol[r] = len(colRows[newC])
				colRows[newC] = append(colRows[newC], r)
				used[oldC]--
				used[newC]++
			}

			for cutRound := 0; cutRound < 6; cutRound++ {
				spfaConverged := false
				spfaMutated := false
				for pass := 0; pass < 80; pass++ {
					if cancelsLeft == 0 {
						break
					}
					passGen++
					dqHead, dqTail := 0, 0
					clear(vS)
					copy(parCol, initNegOne)
					copy(parRow, initNegOne)
					clear(pushCnt)
					clear(inQ)
					clear(hasPopped)
					for idx := len(satCols) - 1; idx >= 0; idx-- {
						c := satCols[idx]
						dq[dqTail&dqMask] = c
						dqTail++
						inQ[c] = true
					}
					passTouched = passTouched[:0]
					pops := 0
					maxQ := 6 * mR
					if maxQ < 64 {
						maxQ = 64
					}
					spfaAborted := false
					for dqHead != dqTail {
						if len(passTouched) > 0 && pops >= maxQ {
							break
						}
						pops++
						c1 := dq[dqHead&dqMask]
						dqHead++
						inQ[c1] = false
						hasPopped[c1] = true
						if touchedPass[c1] == passGen {
							continue
						}
						vc1 := vS[c1]
						edges := adjD[c1]
						for ei := 0; ei < len(edges); ei++ {
							ep := &edges[ei]
							c2 := ep.to
							cand := vc1 + ep.w
							if cand >= vS[c2] {
								continue
							}
							if touchedPass[c2] == passGen {
								continue
							}
							if used[c2] < cCands[c2].cap {
								if cancelsLeft > 0 {
									cycGen++
									cur := c1
									var pathCols []int
									var pathRows []int
									hitCycle := -1
									pathValid := true
									for cur >= 0 {
										if touchedPass[cur] == passGen {
											pathValid = false
											break
										}
										if seenCycle[cur] == cycGen {
											hitCycle = cur
											break
										}
										seenCycle[cur] = cycGen
										pathCols = append(pathCols, cur)
										pathRows = append(pathRows, parRow[cur])
										cur = parCol[cur]
									}
									if !pathValid {
										continue
									}
									if hitCycle >= 0 {
										sIdx := 0
										for pathCols[sIdx] != hitCycle {
											sIdx++
										}
										var cycSum int64
										for k := len(pathCols) - 1; k > sIdx; k-- {
											mvR := pathRows[k-1]
											cycSum += edgeCost(mvR, pathCols[k-1]) - edgeCost(mvR, pathCols[k])
										}
										closeR := pathRows[len(pathCols)-1]
										cycSum += edgeCost(closeR, pathCols[len(pathCols)-1]) - edgeCost(closeR, hitCycle)
										if cycSum < 0 {
											for k := len(pathCols) - 1; k > sIdx; k-- {
												swapAssignRow(pathRows[k-1], pathCols[k], pathCols[k-1])
											}
											swapAssignRow(closeR, hitCycle, pathCols[len(pathCols)-1])
											for k := sIdx; k < len(pathCols); k++ {
												tc := pathCols[k]
												touchedPass[tc] = passGen
												passTouched = append(passTouched, tc)
											}
											cancelsLeft--
											break
										}
									} else if len(pathCols) > 0 {
										var pathSum int64
										for k := len(pathCols) - 1; k > 0; k-- {
											mvR := pathRows[k-1]
											pathSum += edgeCost(mvR, pathCols[k-1]) - edgeCost(mvR, pathCols[k])
										}
										pathSum += edgeCost(ep.r, c2) - edgeCost(ep.r, c1)
										if pathSum < 0 {
											for k := len(pathCols) - 1; k > 0; k-- {
												swapAssignRow(pathRows[k-1], pathCols[k], pathCols[k-1])
											}
											swapAssignRow(ep.r, c1, c2)
											for _, touched := range pathCols {
												touchedPass[touched] = passGen
												passTouched = append(passTouched, touched)
											}
											touchedPass[c2] = passGen
											passTouched = append(passTouched, c2)
											cancelsLeft--
											break
										}
									}
								}
							} else {
								vS[c2] = cand
								parCol[c2] = c1
								parRow[c2] = ep.r
								hitCyc := false
								if hasPopped[c2] {
									if p1 := parCol[c1]; p1 == c2 {
										hitCyc = true
									} else if p1 >= 0 && touchedPass[p1] != passGen && parCol[p1] == c2 {
										hitCyc = true
									} else if pc2 := pushCnt[c2]; pc2 >= 1 {
										checkLimit := 16
										if pc2 >= 8 && (pc2&7) == 0 {
											checkLimit = mR + 2
										}
										cur := c1
										steps := 0
										for cur >= 0 && steps < checkLimit {
											if touchedPass[cur] == passGen {
												break
											}
											if cur == c2 {
												hitCyc = true
												break
											}
											cur = parCol[cur]
											steps++
										}
									}
								}
								if hitCyc && cancelsLeft > 0 {
									var revCols []int
									var revRows []int
									cur := c1
									revCols = append(revCols, c1)
									revRows = append(revRows, ep.r)
									for cur != c2 {
										pv := parCol[cur]
										pr := parRow[cur]
										revCols = append(revCols, pv)
										revRows = append(revRows, pr)
										cur = pv
									}
									var cycSum int64
									nC := len(revCols)
									for i := 0; i < nC; i++ {
										fromC := revCols[i]
										toC := c2
										if i > 0 {
											toC = revCols[i-1]
										}
										mvR := revRows[i]
										cycSum += edgeCost(mvR, toC) - edgeCost(mvR, fromC)
									}
									if cycSum < 0 {
										for i := 0; i < nC; i++ {
											fromC := revCols[i]
											toC := c2
											if i > 0 {
												toC = revCols[i-1]
											}
											swapAssignRow(revRows[i], fromC, toC)
										}
										for _, tc := range revCols {
											touchedPass[tc] = passGen
											passTouched = append(passTouched, tc)
										}
										cancelsLeft--
										break
									}
								}
								if !inQ[c2] {
									if pushCnt[c2] > mR+4 {
										spfaAborted = true
										break
									}
									if dqHead != dqTail && cand < vS[dq[dqHead&dqMask]] {
										dqHead--
										dq[dqHead&dqMask] = c2
									} else {
										dq[dqTail&dqMask] = c2
										dqTail++
									}
									inQ[c2] = true
									pushCnt[c2]++
								}
							}
						}
						if spfaAborted {
							break
						}
					}
					if len(passTouched) == 0 {
						if !spfaAborted {
							spfaConverged = true
						}
						break
					}
					spfaMutated = true
					fhChanged := false
					for tid := 0; tid < numTsigs; tid++ {
						fh := -1
						for _, c := range tsigCols[tid] {
							if used[c] < cCands[c].cap {
								fh = c
								break
							}
						}
						if fh != tsigFreeHead[tid] {
							tsigFreeHead[tid] = fh
							fhChanged = true
						}
					}
					clear(colDirty)
					var touchedLM [64]bool
					for _, tc := range passTouched {
						colDirty[tc] = true
						touchedLM[cCands[tc].lMask] = true
					}
					for lm := 0; lm < 64; lm++ {
						mRowsPass[lm] = mRowsPass[lm][:0]
						mColsPass[lm] = mColsPass[lm][:0]
					}
					for r := 0; r < mL; r++ {
						if c := rowMatch[r]; c >= 0 && touchedLM[cCands[c].lMask] {
							mRowsPass[cCands[c].lMask] = append(mRowsPass[cCands[c].lMask], r)
						}
					}
					for c := 0; c < mR; c++ {
						if used[c] > 0 && touchedLM[cCands[c].lMask] {
							mColsPass[cCands[c].lMask] = append(mColsPass[cCands[c].lMask], c)
						}
					}
					for lm := 0; lm < 64; lm++ {
						rows := mRowsPass[lm]
						if len(rows) == 0 {
							continue
						}
						cols := mColsPass[lm]
						cnts := cntsBuf[:len(cols)]
						for i, c := range cols {
							cnts[i] = used[c]
							colRows[c] = colRows[c][:0]
						}
						ci := 0
						for _, r := range rows {
							for cnts[ci] == 0 {
								ci++
							}
							c := cols[ci]
							cnts[ci]--
							if rowMatch[r] != c {
								if oldC := rowMatch[r]; oldC >= 0 {
									colDirty[oldC] = true
								}
								colDirty[c] = true
								rowMatch[r] = c
							}
							rowSlotInCol[r] = len(colRows[c])
							colRows[c] = append(colRows[c], r)
						}
					}
					satCols = satCols[:0]
					for c := 0; c < mR; c++ {
						if used[c] == cCands[c].cap {
							satCols = append(satCols, c)
							if fhChanged || colDirty[c] {
								buildColEdges(c)
							}
						}
					}
					if spfaAborted {
						break
					}
				}
				if !spfaConverged {
					if spfaMutated {
						bestU, bestDrop = evalDual(bestV)
					}
					break
				}
				uS, dropS := evalDual(vS)
				if cancelsLeft < 2048 || dropS < bestDrop {
					copy(bestV, vS)
					bestU, bestDrop = uS, dropS
				}
				if dropS == 0 {
					break
				}
				for r := 0; r < mL; r++ {
					c1 := rowMatch[r]
					if c1 < 0 || edgeCost(r, c1)-uS[r]-vS[c1] == 0 {
						continue
					}
					bit := uint8(1) << uint(cItems[r].group)
					bestCol := -1
					var bestVal int64
					for tid := 0; tid < numTsigs; tid++ {
						if (tsigLMask[tid] & bit) == 0 {
							continue
						}
						cc, val := queryTsigBestCandCol(tid, r)
						if cc >= 0 && (bestCol < 0 || val < bestVal) {
							bestVal = val
							bestCol = cc
						}
					}
					if bestCol >= 0 && bestCol != c1 {
						w := edgeCost(r, bestCol) - edgeCost(r, c1)
						adjD[c1] = append(adjD[c1], dEdge{to: bestCol, r: r, w: w})
					}
				}
			}
			if bestDrop > 0 && B <= (1<<50) {
				bestV, bestU, bestDrop = relaxDualVector(bestV, bestU, bestDrop, 16)
			}
		}
		copy(v, bestV)
		return bestU, bestDrop
	}

	u, dropInit := seedDuals()
	if dropInit > 0 {
		for r := 0; r < mL; r++ {
			if c := rowMatch[r]; c >= 0 && edgeCost(r, c)-u[r]-v[c] != 0 {
				swapRemoveRowFromCol(r, c)
			}
		}
		u, dropInit = seedDuals()
	}

	// Safe downward v-shift for saturated tsigs to raise u[unmatched_lOpt] toward 0.
	if dropInit == 0 {
		hasUnmNegU := false
		for r := 0; r < mL; r++ {
			if rowMatch[r] < 0 && u[r] < 0 {
				hasUnmNegU = true
				break
			}
		}
		if hasUnmNegU {
			refreshSatAndFree()
			raiseU := make([]int64, numTsigs)
			hasMatchedInTid := make([]bool, numTsigs)
			for tid := 0; tid < numTsigs; tid++ {
				if tsigFreeHead[tid] >= 0 {
					raiseU[tid] = 0
					hasMatchedInTid[tid] = true
					continue
				}
				minNeg := int64(1 << 60)
				anyR := false
				for _, c := range tsigCols[tid] {
					for _, r := range colRows[c] {
						anyR = true
						if -u[r] < minNeg {
							minNeg = -u[r]
						}
					}
				}
				if anyR {
					raiseU[tid] = minNeg
					hasMatchedInTid[tid] = true
				}
			}
			minSlack := make([]int64, numTsigs*numTsigs)
			hasSlack := make([]bool, numTsigs*numTsigs)
			for r := 0; r < mL; r++ {
				c1 := rowMatch[r]
				if c1 < 0 {
					continue
				}
				tid1 := cCands[c1].tsig
				if tsigFreeHead[tid1] >= 0 {
					continue
				}
				bit := uint8(1) << uint(cItems[r].group)
				for tid2 := 0; tid2 < numTsigs; tid2++ {
					if tid2 == tid1 || (tsigLMask[tid2]&bit) == 0 {
						continue
					}
					sl := queryTsigBestCand(tid2, r) - u[r]
					idx := tid1*numTsigs + tid2
					if !hasSlack[idx] || sl < minSlack[idx] {
						minSlack[idx] = sl
						hasSlack[idx] = true
					}
				}
			}
			for iter := 0; iter < numTsigs; iter++ {
				upd := false
				for tid1 := 0; tid1 < numTsigs; tid1++ {
					if tsigFreeHead[tid1] >= 0 || !hasMatchedInTid[tid1] {
						continue
					}
					for tid2 := 0; tid2 < numTsigs; tid2++ {
						idx := tid1*numTsigs + tid2
						if !hasSlack[idx] || !hasMatchedInTid[tid2] {
							continue
						}
						if cand := raiseU[tid2] + minSlack[idx]; cand < raiseU[tid1] {
							raiseU[tid1] = cand
							upd = true
						}
					}
				}
				if !upd {
					break
				}
			}
			shiftedAny := false
			for tid := 0; tid < numTsigs; tid++ {
				if sh := raiseU[tid]; sh > 0 {
					for _, c := range tsigCols[tid] {
						if used[c] == cCands[c].cap {
							v[c] -= sh
						}
					}
					shiftedAny = true
				}
			}
			if shiftedAny {
				u = computeU()
			}
		}
	}

	for r := 0; r < mL; r++ {
		if c := rowMatch[r]; c >= 0 && edgeCost(r, c)-u[r]-v[c] != 0 {
			swapRemoveRowFromCol(r, c)
		}
	}

	freeColQ := make([]int, 0, mR)
	for c := 0; c < mR; c++ {
		if used[c] < cCands[c].cap && v[c] != 0 {
			v[c] = 0
			freeColQ = append(freeColQ, c)
		}
	}
	for head := 0; head < len(freeColQ); head++ {
		c := freeColQ[head]
		mask := cCands[c].lMask
		for r := 0; r < mL; r++ {
			if (mask & (1 << uint(cItems[r].group))) == 0 {
				continue
			}
			ec := edgeCost(r, c)
			if u[r] > ec {
				u[r] = ec
				if oldC := rowMatch[r]; oldC >= 0 {
					swapRemoveRowFromCol(r, oldC)
					if used[oldC] < cCands[oldC].cap && v[oldC] != 0 {
						v[oldC] = 0
						freeColQ = append(freeColQ, oldC)
					}
				}
			}
		}
	}

	refreshSatAndFree()

	// Capacity-unexpanded SSAP Dijkstra with B=32 per-tsig block hull acceleration.
	rowDist := make([]int64, mL)
	colDist := make([]int64, mR)
	colSeenGen := make([]int, mR)
	parentRow := make([]int, mR)
	heapPos := make([]int, mR)
	for c := 0; c < mR; c++ {
		heapPos[c] = -1
	}
	var heapArr []int
	var closedRows []int
	var closedCols []int
	blkClosedGen := make([]int, len(blocks))
	blkClosedCount := make([]int, len(blocks))
	colPendingRows := make([][]int, mR)
	colPendingGen := make([]int, mR)
	gen := 0

	heapPushOrUpdate := func(col int) {
		idx := heapPos[col]
		if idx < 0 {
			idx = len(heapArr)
			heapArr = append(heapArr, col)
			heapPos[col] = idx
		}
		for idx > 0 {
			p := (idx - 1) / 2
			pc := heapArr[p]
			if colDist[col] >= colDist[pc] {
				break
			}
			heapArr[idx] = pc
			heapPos[pc] = idx
			idx = p
		}
		heapArr[idx] = col
		heapPos[col] = idx
	}

	heapPop := func() int {
		top := heapArr[0]
		heapPos[top] = -1
		n := len(heapArr) - 1
		if n > 0 {
			last := heapArr[n]
			heapArr[0] = last
			heapPos[last] = 0
		}
		heapArr = heapArr[:n]
		i := 0
		for {
			l := 2*i + 1
			if l >= n {
				break
			}
			best := l
			r := l + 1
			if r < n && colDist[heapArr[r]] < colDist[heapArr[l]] {
				best = r
			}
			if colDist[heapArr[best]] >= colDist[heapArr[i]] {
				break
			}
			heapArr[i], heapArr[best] = heapArr[best], heapArr[i]
			heapPos[heapArr[i]] = i
			heapPos[heapArr[best]] = best
			i = best
		}
		return top
	}

	tryRelax := func(r int, d int64, c int, curGen int) {
		cand := d + (edgeCost(r, c) - u[r] - v[c])
		if colSeenGen[c] != curGen || cand < colDist[c] {
			if colSeenGen[c] != curGen {
				colSeenGen[c] = curGen
				heapPos[c] = -1
			}
			colDist[c] = cand
			parentRow[c] = r
			heapPushOrUpdate(c)
		}
	}

	relaxBlockAllOpen := func(bid, r int, d int64, curGen int) {
		blk := &blocks[bid]
		tid := blk.tid
		cols := tsigCols[tid]
		pri := int64(1)
		if cItems[r].lReq {
			pri++
		}
		if tsigRReq[tid] {
			pri++
		}
		x := int64(globalL - cItems[r].origL)
		rBase := d - pri*B - x*int64(globalR) - u[r]
		for k := blk.start; k < blk.end; k++ {
			c := cols[k]
			if !colIsSat[c] || colClosedGen[c] == curGen {
				continue
			}
			cand := rBase + x*int64(cCands[c].origR) - v[c]
			if colSeenGen[c] != curGen || cand < colDist[c] {
				if colSeenGen[c] != curGen {
					colSeenGen[c] = curGen
					heapPos[c] = -1
				}
				colDist[c] = cand
				parentRow[c] = r
				heapPushOrUpdate(c)
			}
		}
	}

	rebuildOpenHull := func(bid, curGen int) {
		blk := &blocks[bid]
		blk.openGen = curGen
		blk.openDirty = false
		buildBlockHull(blk, curGen, &blk.openHullCols, &blk.openHullM, &blk.openHullQ)
	}

	relaxFromRow := func(r int, d int64, curGen int) {
		x := int64(globalL - cItems[r].origL)
		bit := uint8(1) << uint(cItems[r].group)
		for tid := 0; tid < numTsigs; tid++ {
			if (tsigLMask[tid] & bit) == 0 {
				continue
			}
			if fh := tsigFreeHead[tid]; fh >= 0 {
				tryRelax(r, d, fh, curGen)
			}
			for bid := tsigBlocksStart[tid]; bid < tsigBlocksEnd[tid]; bid++ {
				satCnt := blocks[bid].satCount
				if satCnt == 0 {
					continue
				}
				if blkClosedGen[bid] == curGen && blkClosedCount[bid] >= satCnt {
					continue
				}
				blk := &blocks[bid]
				var hCols []int
				var hM, hQ []int64
				if blk.openGen != curGen {
					if blk.dirty {
						rebuildBlockHull(bid)
					}
					hCols, hM, hQ = blk.hullCols, blk.hullM, blk.hullQ
				} else {
					if blk.openDirty {
						rebuildOpenHull(bid, curGen)
					}
					hCols, hM, hQ = blk.openHullCols, blk.openHullM, blk.openHullQ
				}
				lo, bestVal := queryHullBestIdx(hM, hQ, x)
				hcWin := hCols[lo]
				for idx := lo; idx < len(hCols) && hM[idx]*x+hQ[idx] == bestVal; idx++ {
					tryRelax(r, d, hCols[idx], curGen)
				}
				if colPendingGen[hcWin] != curGen {
					colPendingGen[hcWin] = curGen
					colPendingRows[hcWin] = colPendingRows[hcWin][:0]
				}
				colPendingRows[hcWin] = append(colPendingRows[hcWin], r)
			}
		}
	}

	for startRow := 0; startRow < mL; startRow++ {
		if rowMatch[startRow] >= 0 || u[startRow] == 0 {
			continue
		}
		gen++
		heapArr = heapArr[:0]
		closedRows = closedRows[:0]
		closedCols = closedCols[:0]

		rowDist[startRow] = 0
		closedRows = append(closedRows, startRow)
		bestDummyDist := -u[startRow]
		bestDummyRow := startRow

		relaxFromRow(startRow, 0, gen)

		augTargetCol := -1
		augDist := bestDummyDist
		for len(heapArr) > 0 {
			c := heapPop()
			d := colDist[c]
			if d >= bestDummyDist {
				break
			}
			colClosedGen[c] = gen
			closedCols = append(closedCols, c)
			if colIsSat[c] {
				bid := colBlock[c]
				if blkClosedGen[bid] != gen {
					blkClosedGen[bid] = gen
					blkClosedCount[bid] = 1
				} else {
					blkClosedCount[bid]++
				}
				blk := &blocks[bid]
				if blkClosedCount[bid] < blk.satCount {
					activeCols := blk.hullCols
					if blk.openGen == gen {
						activeCols = blk.openHullCols
					}
					needRebuild := blk.dirty || (blk.openGen == gen && blk.openDirty)
					if !needRebuild {
						for _, hc := range activeCols {
							if hc == c {
								needRebuild = true
								break
							}
						}
					}
					if needRebuild {
						blk.openGen = gen
						blk.openDirty = true
					}
				}
				if colPendingGen[c] == gen {
					if blkClosedCount[bid] < blk.satCount {
						for _, pr := range colPendingRows[c] {
							relaxBlockAllOpen(bid, pr, rowDist[pr], gen)
						}
					}
					colPendingRows[c] = colPendingRows[c][:0]
				}
			}
			if used[c] < cCands[c].cap {
				augTargetCol = c
				augDist = d
				break
			}
			for _, r0 := range colRows[c] {
				rowDist[r0] = d
				closedRows = append(closedRows, r0)
				if candDummy := d - u[r0]; candDummy < bestDummyDist {
					bestDummyDist = candDummy
					bestDummyRow = r0
				}
				relaxFromRow(r0, d, gen)
			}
		}
		if augTargetCol < 0 {
			augDist = bestDummyDist
		}

		for _, r := range closedRows {
			u[r] += augDist - rowDist[r]
		}
		for _, c := range closedCols {
			if deltaV := augDist - colDist[c]; deltaV != 0 {
				v[c] -= deltaV
				blocks[colBlock[c]].dirty = true
			}
		}

		c := -1
		vacSlot := -1
		if augTargetCol >= 0 {
			r := parentRow[augTargetCol]
			vacSlot = rowSlotInCol[r]
			c = rowMatch[r]
			rowSlotInCol[r] = len(colRows[augTargetCol])
			colRows[augTargetCol] = append(colRows[augTargetCol], r)
			rowMatch[r] = augTargetCol
			used[augTargetCol]++
			if used[augTargetCol] == cCands[augTargetCol].cap {
				colIsSat[augTargetCol] = true
				bid := colBlock[augTargetCol]
				blocks[bid].satCount++
				blocks[bid].dirty = true
				tid := cCands[augTargetCol].tsig
				fh := -1
				for _, tc := range tsigCols[tid] {
					if used[tc] < cCands[tc].cap {
						fh = tc
						break
					}
				}
				tsigFreeHead[tid] = fh
			}
		} else if bestDummyRow != startRow {
			vacSlot = rowSlotInCol[bestDummyRow]
			c = rowMatch[bestDummyRow]
			rowMatch[bestDummyRow] = -1
			rowSlotInCol[bestDummyRow] = -1
		}
		for c >= 0 {
			r := parentRow[c]
			nextSlot := rowSlotInCol[r]
			nextC := rowMatch[r]
			colRows[c][vacSlot] = r
			rowSlotInCol[r] = vacSlot
			rowMatch[r] = c
			vacSlot = nextSlot
			c = nextC
		}
	}

	for r := 0; r < mL; r++ {
		if c := rowMatch[r]; c >= 0 {
			assign[cItems[r].origL] = cCands[c].origR
		}
	}
}

func verifySubscribe(
	idx *SpanIndex,
	sortedSpans []*proxypb.TraceSpan,
	profile Profile,
	projectID, fullSub, subID string,
	subExp *SubscribeExpectation,
	strictExtra bool,
	matched map[*proxypb.TraceSpan]bool,
	devs *deviationCollector,
) []Violation {
	var failures []Violation
	msgs := expandSubscribeMessages(subExp)

	// Reject invalid ExpectedOutcome values and contradictory ExactlyOnce=false + ExpectedOutcome="eod_failed"
	// upfront before Phase 1-5 matching or Phase 4 ALO accounting executes.
	var validationErrs []Violation
	for i, m := range msgs {
		normOutcome := normalizeSubscribeOutcome(m.ExpectedOutcome)
		if !isValidSubscribeOutcome(normOutcome) {
			validationErrs = append(validationErrs, Violation{
				Code:     CodeCustomCheckFailed,
				Rule:     "subscribe.expectation.invalid_outcome",
				Message:  fmt.Sprintf("SubscribeExpectation message[%d] has invalid ExpectedOutcome %q", i, m.ExpectedOutcome),
				Expected: `"acked", "nacked", "expired", "eod_failed", or ""`,
				Actual:   fmt.Sprintf("%q", m.ExpectedOutcome),
			})
			continue
		}
		if m.ExactlyOnce != nil && !*m.ExactlyOnce && normOutcome == "eod_failed" {
			validationErrs = append(validationErrs, Violation{
				Code:     CodeCustomCheckFailed,
				Rule:     "subscribe.expectation.conflict",
				Message:  fmt.Sprintf("SubscribeExpectation message[%d] sets ExactlyOnce=false, which contradicts ExpectedOutcome=%q (eod_failed requires EOD)", i, m.ExpectedOutcome),
				Expected: `ExactlyOnce=true or nil when ExpectedOutcome="eod_failed"`,
				Actual:   fmt.Sprintf("ExactlyOnce=false, ExpectedOutcome=%q", m.ExpectedOutcome),
			})
		}
	}
	if len(validationErrs) == 0 {
		topOutcome := normalizeSubscribeOutcome(subExp.ExpectedOutcome)
		if !isValidSubscribeOutcome(topOutcome) {
			validationErrs = append(validationErrs, Violation{
				Code:     CodeCustomCheckFailed,
				Rule:     "subscribe.expectation.invalid_outcome",
				Message:  fmt.Sprintf("SubscribeExpectation has invalid ExpectedOutcome %q", subExp.ExpectedOutcome),
				Expected: `"acked", "nacked", "expired", "eod_failed", or ""`,
				Actual:   fmt.Sprintf("%q", subExp.ExpectedOutcome),
			})
		} else if subExp.ExactlyOnce != nil && !*subExp.ExactlyOnce && topOutcome == "eod_failed" {
			validationErrs = append(validationErrs, Violation{
				Code:     CodeCustomCheckFailed,
				Rule:     "subscribe.expectation.conflict",
				Message:  fmt.Sprintf("SubscribeExpectation sets ExactlyOnce=false, which contradicts ExpectedOutcome=%q (eod_failed requires EOD)", subExp.ExpectedOutcome),
				Expected: `ExactlyOnce=true or nil when ExpectedOutcome="eod_failed"`,
				Actual:   fmt.Sprintf("ExactlyOnce=false, ExpectedOutcome=%q", subExp.ExpectedOutcome),
			})
		}
	}
	if len(validationErrs) > 0 {
		return validationErrs
	}

	subSpanName := subID + " subscribe"
	var subCandidates []*proxypb.TraceSpan
	if subID != "" {
		subCandidates = SortSpansDeterministic(idx.ByName[subSpanName])
	} else {
		for _, s := range sortedSpans {
			if isCandidatePubSubSpan(s, profile) && strings.HasSuffix(s.GetName(), " subscribe") && len(s.GetName()) > len(" subscribe") {
				subID = strings.TrimSuffix(s.GetName(), " subscribe")
				subSpanName = subID + " subscribe"
				subCandidates = SortSpansDeterministic(idx.ByName[subSpanName])
				break
			}
		}
		if subID == "" {
			for _, s := range sortedSpans {
				if !isCandidatePubSubSpan(s, profile) {
					continue
				}
				for _, suffix := range []string{" ack", " nack", " modack"} {
					if strings.HasSuffix(s.GetName(), suffix) && len(s.GetName()) > len(suffix) {
						subID = strings.TrimSuffix(s.GetName(), suffix)
						subSpanName = subID + " subscribe"
						break
					}
				}
				if subID != "" {
					break
				}
			}
		}
	}

	type indexedSubMsg struct {
		origIdx int
		msg     SubscribeMessageExpectation
	}
	var sampledMsgs, droppedMsgs []indexedSubMsg
	for i, m := range msgs {
		item := indexedSubMsg{origIdx: i, msg: m}
		if m.ExpectDroppedUnsampled {
			droppedMsgs = append(droppedMsgs, item)
		} else {
			sampledMsgs = append(sampledMsgs, item)
		}
	}

	usedSub := make(map[*proxypb.TraceSpan]bool)
	uniqueSettleSpans := make(map[*proxypb.TraceSpan]*mergedSettlement)
	var settleOrder []*proxypb.TraceSpan
	var matchedSubs []*proxypb.TraceSpan
	matchedSubKeys := make(map[SpanKey]bool)
	matchedSubByKey := make(map[SpanKey]*proxypb.TraceSpan)
	matchedSubMsg := make(map[*proxypb.TraceSpan]SubscribeMessageExpectation)
	matchedSubForOrigIdx := make(map[int]*proxypb.TraceSpan)
	checkedReverseLinks := make(map[spanLinkPair]bool)
	unsampledUsedCount := make(map[*proxypb.TraceSpan]int)
	phase3ReservedAck := make(map[int]bool)
	phase3ReservedNack := make(map[int]bool)
	phase3ReservedTerminal := make(map[int]bool)
	phase2ModackForwardLinkFailed := false

	recordUniqueSettlement := func(sp *proxypb.TraceSpan, se SettlementExpectation) {
		if existing, ok := uniqueSettleSpans[sp]; ok {
			existing.expectations = append(existing.expectations, se)
			return
		}
		uniqueSettleSpans[sp] = &mergedSettlement{expectations: []SettlementExpectation{se}}
		settleOrder = append(settleOrder, sp)
	}

	// Phase 2: Precompute subscribe metadata + single-pass reverse link index, then run global bipartite matching.
	subMetas := make([]subSpanMeta, len(subCandidates))
	subKeyMap := make(map[SpanKey][]int, len(subCandidates))
	var effSubIDs []string
	seenEffSub := make(map[string]bool)
	for rj, sc := range subCandidates {
		sm := buildSubSpanMeta(idx, profile, subID, sc)
		subMetas[rj] = sm
		sk := SpanKey{TraceID: sm.normTraceID, SpanID: sm.normSpanID}
		subKeyMap[sk] = append(subKeyMap[sk], rj)
		if sm.effSubID != "" && !seenEffSub[sm.effSubID] {
			seenEffSub[sm.effSubID] = true
			effSubIDs = append(effSubIDs, sm.effSubID)
		}
	}

	var distinctOps []string
	opToIdx := make(map[string]int)
	for _, item := range sampledMsgs {
		for _, se := range item.msg.Settlements {
			op := strings.ToLower(strings.TrimSpace(se.Operation))
			if op == "" {
				continue
			}
			if _, seen := opToIdx[op]; !seen {
				opToIdx[op] = len(distinctOps)
				distinctOps = append(distinctOps, op)
			}
		}
	}
	numOps := len(distinctOps)
	sortedOpCache := make(map[string][]*proxypb.TraceSpan)
	getSortedOpSpans := func(name string) []*proxypb.TraceSpan {
		if s, ok := sortedOpCache[name]; ok {
			return s
		}
		s := SortSpansDeterministic(idx.ByName[name])
		sortedOpCache[name] = s
		return s
	}

	var linkedSettle [][]subSettleSpanMeta
	if numOps > 0 && len(subCandidates) > 0 && len(effSubIDs) > 0 {
		linkedSettle = make([][]subSettleSpanMeta, len(subCandidates)*numOps)
		seenRJGen := make([]int, len(subCandidates))
		spGen := 0
		for _, effSub := range effSubIDs {
			for opIdx, op := range distinctOps {
				for _, sp := range getSortedOpSpans(effSub + " " + op) {
					links := sp.GetLinks()
					if len(links) == 0 {
						continue
					}
					spAttrs := sp.GetAttributes()
					isReceipt, hasReceipt := AttrBool(spAttrs, "messaging.gcp_pubsub.is_receipt_modack")
					deadline, hasDeadline := AttrInt64(spAttrs, "messaging.gcp_pubsub.message.ack_deadline")
					if !hasDeadline {
						deadline, hasDeadline = AttrInt64(spAttrs, "messaging.gcp_pubsub.message.ack_deadline_seconds")
					}
					bc, hasBC := AttrInt64(spAttrs, "messaging.batch.message_count")
					spm := subSettleSpanMeta{
						isErr:       sp.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
						statusMsg:   sp.GetStatusMessage(),
						hasExcept:   AssertEvent(sp, "exception") == nil,
						hasReceipt:  hasReceipt,
						isReceipt:   isReceipt,
						hasDeadline: hasDeadline,
						deadline:    deadline,
						hasBC:       hasBC,
						bc:          bc,
					}
					spGen++
					for _, l := range links {
						if l == nil {
							continue
						}
						lk := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
						for _, rj := range subKeyMap[lk] {
							if subMetas[rj].effSubID == effSub && seenRJGen[rj] != spGen {
								seenRJGen[rj] = spGen
								slot := rj*numOps + opIdx
								linkedSettle[slot] = append(linkedSettle[slot], spm)
							}
						}
					}
				}
			}
		}
	}

	subEdges := make([]bipartiteEdge, 0, len(sampledMsgs)*len(subCandidates))
	var rawSettles []int64
	if numOps > 0 && len(sampledMsgs) > 0 && len(subCandidates) > 0 {
		rawSettles = make([]int64, 0, len(sampledMsgs)*len(subCandidates))
	}
	var maxRawSubSettle int64
	for li, item := range sampledMsgs {
		sm := buildSubMsgScoreMeta(profile, item.msg, opToIdx)
		for rj := range subCandidates {
			baseScore, rawSettle := scoreSubscribeEdgeFast(profile, sm, subMetas[rj], linkedSettle, rj, numOps)
			if rawSettle > maxRawSubSettle {
				maxRawSubSettle = rawSettle
			}
			subEdges = append(subEdges, bipartiteEdge{
				left:   li,
				right:  rj,
				weight: baseScore + rawSettle,
			})
			if rawSettles != nil {
				rawSettles = append(rawSettles, rawSettle)
			}
		}
	}
	if maxRawSubSettle > subscribeSettleBonusCap {
		for idx := range subEdges {
			raw := rawSettles[idx]
			if raw > 0 {
				subEdges[idx].weight = (subEdges[idx].weight - raw) + normalizeSettleBonus(raw, maxRawSubSettle, subscribeSettleBonusCap)
			}
		}
	}
	subAssign := solveBipartiteMatching(len(sampledMsgs), len(subCandidates), nil, nil, subEdges)

	linkedOpCache := make(map[string]map[*proxypb.TraceSpan][]*proxypb.TraceSpan)
	getOpSpansAndLinked := func(settleName string) ([]*proxypb.TraceSpan, map[*proxypb.TraceSpan][]*proxypb.TraceSpan) {
		sorted := getSortedOpSpans(settleName)
		if bySub, ok := linkedOpCache[settleName]; ok {
			return sorted, bySub
		}
		bySub := make(map[*proxypb.TraceSpan][]*proxypb.TraceSpan)
		seenSubGen := make(map[*proxypb.TraceSpan]int)
		spGen := 0
		for _, sp := range sorted {
			spGen++
			for _, l := range sp.GetLinks() {
				if l == nil {
					continue
				}
				lk := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
				for _, rj := range subKeyMap[lk] {
					sc := subCandidates[rj]
					if seenSubGen[sc] != spGen {
						seenSubGen[sc] = spGen
						bySub[sc] = append(bySub[sc], sp)
					}
				}
			}
		}
		linkedOpCache[settleName] = bySub
		return sorted, bySub
	}

	for li, item := range sampledMsgs {
		i := item.origIdx
		m := item.msg

		effName := subSpanName
		rj := subAssign[li]
		if rj < 0 {
			failures = append(failures, Violation{
				Code:     CodeMissingSpan,
				Rule:     "subscribe.present",
				Message:  fmt.Sprintf("missing %q span for delivered message[%d] (ack_id=%q msg_id=%q)", effName, i, m.AckID, m.MessageID),
				Expected: effName,
				Actual:   "<missing>",
				SpanName: effName,
			})
			continue
		}

		subSpan := subCandidates[rj]
		usedSub[subSpan] = true
		matched[subSpan] = true
		matchedSubs = append(matchedSubs, subSpan)
		sk := SpanKey{TraceID: NormalizeHex(subSpan.GetTraceId()), SpanID: NormalizeHex(subSpan.GetSpanId())}
		matchedSubKeys[sk] = true
		matchedSubByKey[sk] = subSpan
		matchedSubMsg[subSpan] = m
		matchedSubForOrigIdx[i] = subSpan

		effSubID := subID
		if effSubID == "" && strings.HasSuffix(subSpan.GetName(), " subscribe") {
			effSubID = strings.TrimSuffix(subSpan.GetName(), " subscribe")
		}
		isEOD := resolveIsEOD(m, subSpan, profile)

		failures = append(failures, checkSpanScope(subSpan, profile, "subscribe.scope", devs)...)
		if subSpan.GetKind() != profile.SubscribeSpanKind {
			failures = append(failures, checkSpanKindV(subSpan, profile.SubscribeSpanKind, "subscribe.span_kind")...)
		} else if profile.Name == ProfileCPP && subSpan.GetKind() != proxypb.SpanKind_SPAN_KIND_INTERNAL {
			devs.add(Deviation{
				Category:    "span_kind",
				Rule:        "span_kind.subscribe",
				ExpectedGo:  proxypb.SpanKind_SPAN_KIND_INTERNAL.String(),
				Observed:    subSpan.GetKind().String(),
				SpanName:    subSpan.GetName(),
				Description: "C++ <subID> subscribe uses SPAN_KIND_CONSUMER instead of SPAN_KIND_INTERNAL",
			})
		}

		// Traceparent parenting check.
		if m.WireTraceparent == "" {
			failures = append(failures, checkRootSpanV(subSpan, "subscribe.root_when_no_traceparent")...)
		} else if tp, err := ParseTraceparent(m.WireTraceparent); err != nil {
			failures = append(failures, checkRootSpanV(subSpan, "subscribe.root_when_invalid_traceparent")...)
		} else if tp.IsSampled() || !profile.RequireSampledParentOnSubscribe {
			if err := AssertParentFromTraceparent(subSpan, m.WireTraceparent); err != nil {
				failures = append(failures, Violation{
					Code:     CodeBrokenTraceparent,
					Rule:     "subscribe.parent_traceparent",
					Message:  err.Error(),
					Expected: fmt.Sprintf("%s/%s", tp.TraceID, tp.SpanID),
					Actual:   fmt.Sprintf("%s/%s", subSpan.GetTraceId(), subSpan.GetParentSpanId()),
					TraceID:  subSpan.GetTraceId(),
					SpanID:   subSpan.GetSpanId(),
					SpanName: subSpan.GetName(),
				})
			}
		} else {
			// C++ ignores valid unsampled traceparent -> root span.
			failures = append(failures, checkRootSpanV(subSpan, "subscribe.cpp_root_on_unsampled")...)
			devs.add(Deviation{
				Category:    "hierarchy",
				Rule:        "parent.requires_sampled_traceparent",
				ExpectedGo:  "Parents subscribe to valid traceparent regardless of sampled flag",
				Observed:    "Falls back to root when traceparent is unsampled (-00)",
				SpanName:    subSpan.GetName(),
				Description: "C++ TracingBatchCallback checks IsSampled() && IsValid() before parenting subscribe",
			})
		}

		// Subscribe attributes.
		failures = append(failures, verifySubscribeAttributes(subSpan, profile, projectID, fullSub, effSubID, m, isEOD, devs)...)

		// Child spans via parent_span_id (NEVER child_span_count!).
		children := SortSpansDeterministic(idx.ChildrenOf(subSpan))
		ccChildren := filterByName(children, "subscriber concurrency control")
		schedChildren := filterByName(children, profile.SchedulerSpanName)
		processChildren := filterByName(children, effSubID+" process")

		outcome := expectedSubscribeOutcome(m, isEOD)
		ccMode, schedMode, procMode := resolveSubscribeChildModes(m, outcome)

		if ccMode == spanForbidden {
			for _, cc := range ccChildren {
				matched[cc] = true
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "subscribe.concurrency_control.unexpected",
					Message:  fmt.Sprintf("unexpected child span %q under %q (%s) when concurrency control span is forbidden", cc.GetName(), subSpan.GetName(), subSpan.GetSpanId()),
					Expected: "0 child spans named \"subscriber concurrency control\"",
					Actual:   strconv.Itoa(len(ccChildren)),
					TraceID:  cc.GetTraceId(),
					SpanID:   cc.GetSpanId(),
					SpanName: cc.GetName(),
				})
			}
		} else if len(ccChildren) == 0 {
			if ccMode == spanRequired {
				failures = append(failures, Violation{
					Code:     CodeMissingSpan,
					Rule:     "subscribe.concurrency_control.present",
					Message:  fmt.Sprintf("missing child span %q under %q (%s)", "subscriber concurrency control", subSpan.GetName(), subSpan.GetSpanId()),
					Expected: "1 child span named \"subscriber concurrency control\"",
					Actual:   "0",
					TraceID:  subSpan.GetTraceId(),
					SpanID:   subSpan.GetSpanId(),
					SpanName: subSpan.GetName(),
				})
			}
		} else {
			if len(ccChildren) > 1 {
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "subscribe.concurrency_control.unique",
					Message:  fmt.Sprintf("expected 1 %q child under %q, got %d", "subscriber concurrency control", subSpan.GetName(), len(ccChildren)),
					Expected: "1",
					Actual:   strconv.Itoa(len(ccChildren)),
					TraceID:  subSpan.GetTraceId(),
					SpanID:   subSpan.GetSpanId(),
					SpanName: "subscriber concurrency control",
				})
			}
			cc := ccChildren[0]
			matched[cc] = true
			failures = append(failures, verifySubscribeChildControl(cc, subSpan, profile, "subscribe.concurrency_control", devs)...)
		}

		if schedMode == spanForbidden {
			for _, sc := range schedChildren {
				matched[sc] = true
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "subscribe.scheduler.unexpected",
					Message:  fmt.Sprintf("unexpected child span %q under %q (%s) when scheduler span is forbidden", sc.GetName(), subSpan.GetName(), subSpan.GetSpanId()),
					Expected: fmt.Sprintf("0 child spans named %q", profile.SchedulerSpanName),
					Actual:   strconv.Itoa(len(schedChildren)),
					TraceID:  sc.GetTraceId(),
					SpanID:   sc.GetSpanId(),
					SpanName: sc.GetName(),
				})
			}
		} else if len(schedChildren) == 0 {
			if schedMode == spanRequired {
				failures = append(failures, Violation{
					Code:     CodeMissingSpan,
					Rule:     "subscribe.scheduler.present",
					Message:  fmt.Sprintf("missing child span %q under %q (%s)", profile.SchedulerSpanName, subSpan.GetName(), subSpan.GetSpanId()),
					Expected: profile.SchedulerSpanName,
					Actual:   "0",
					TraceID:  subSpan.GetTraceId(),
					SpanID:   subSpan.GetSpanId(),
					SpanName: subSpan.GetName(),
				})
			}
		} else {
			if len(schedChildren) > 1 {
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "subscribe.scheduler.unique",
					Message:  fmt.Sprintf("expected 1 %q child under %q, got %d", profile.SchedulerSpanName, subSpan.GetName(), len(schedChildren)),
					Expected: "1",
					Actual:   strconv.Itoa(len(schedChildren)),
					TraceID:  subSpan.GetTraceId(),
					SpanID:   subSpan.GetSpanId(),
					SpanName: profile.SchedulerSpanName,
				})
			}
			sc := schedChildren[0]
			matched[sc] = true
			if profile.Name == ProfileCPP && sc.GetName() != "subscribe scheduler" {
				devs.add(Deviation{
					Category:    "span_name",
					Rule:        "span_name.subscriber_scheduler",
					ExpectedGo:  `"subscribe scheduler"`,
					Observed:    fmt.Sprintf("%q", sc.GetName()),
					SpanName:    sc.GetName(),
					Description: `C++ names the scheduler span "subscriber scheduler" instead of "subscribe scheduler"`,
				})
			}
			failures = append(failures, verifySubscribeChildControl(sc, subSpan, profile, "subscribe.scheduler", devs)...)
		}

		hasAckEv, hasNackEv, hasExpEv := analyzeSubSpanTerminalEvidence(subSpan, profile)
		normExplicitOutcome := normalizeSubscribeOutcome(m.ExpectedOutcome)
		hasExplicitTerminalSettle := hasExplicitTerminalSettlement(m.Settlements)
		var hasLinkedAck, hasLinkedNack bool
		var eodLinkedOp string
		if !hasExplicitTerminalSettle && (normExplicitOutcome == "eod_failed" || normExplicitOutcome == "") {
			_, ackBySub := getOpSpansAndLinked(effSubID + " ack")
			_, nackBySub := getOpSpansAndLinked(effSubID + " nack")
			linkedAck := len(ackBySub[subSpan]) > 0
			linkedNack := len(nackBySub[subSpan]) > 0
			if !hasExplicitTerminalSettle && normExplicitOutcome == "eod_failed" {
				hasLinkedAck = linkedAck
				hasLinkedNack = linkedNack
			}
			if linkedAck && !linkedNack {
				eodLinkedOp = "ack"
			} else if linkedNack && !linkedAck {
				eodLinkedOp = "nack"
			} else if !linkedAck && !linkedNack {
				if hasAckEv && !hasNackEv && !hasExpEv {
					eodLinkedOp = "ack"
				} else if hasNackEv && !hasAckEv && !hasExpEv {
					eodLinkedOp = "nack"
				}
			}
		}

		if procMode == spanForbidden {
			for _, ps := range processChildren {
				matched[ps] = true
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "subscribe.process.unexpected",
					Message:  fmt.Sprintf("unexpected child span %q under %q (%s) when process span is forbidden", ps.GetName(), subSpan.GetName(), subSpan.GetSpanId()),
					Expected: fmt.Sprintf("0 child spans named %q", effSubID+" process"),
					Actual:   strconv.Itoa(len(processChildren)),
					TraceID:  ps.GetTraceId(),
					SpanID:   ps.GetSpanId(),
					SpanName: ps.GetName(),
				})
			}
		} else {
			if len(processChildren) > 1 {
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "subscribe.process.unique",
					Message:  fmt.Sprintf("expected at most 1 %q child under %q, got %d", effSubID+" process", subSpan.GetName(), len(processChildren)),
					Expected: "1",
					Actual:   strconv.Itoa(len(processChildren)),
					TraceID:  subSpan.GetTraceId(),
					SpanID:   subSpan.GetSpanId(),
					SpanName: effSubID + " process",
				})
			}
			if len(processChildren) == 0 {
				if procMode == spanRequired {
					failures = append(failures, Violation{
						Code:     CodeMissingSpan,
						Rule:     "subscribe.process.present",
						Message:  fmt.Sprintf("missing child span %q under %q (%s)", effSubID+" process", subSpan.GetName(), subSpan.GetSpanId()),
						Expected: effSubID + " process",
						Actual:   "0",
						TraceID:  subSpan.GetTraceId(),
						SpanID:   subSpan.GetSpanId(),
						SpanName: subSpan.GetName(),
					})
				}
			} else {
				ps := processChildren[0]
				matched[ps] = true
				failures = append(failures, verifyProcessSpan(ps, subSpan, profile, projectID, fullSub, effSubID, m, eodLinkedOp, devs)...)
			}
		}

		if (hasAckEv && hasNackEv) || (hasExpEv && (hasAckEv || hasNackEv)) {
			failures = append(failures, Violation{
				Code:     CodeCustomCheckFailed,
				Rule:     "subscribe.settlement.conflicting_evidence",
				Message:  fmt.Sprintf("subscribe span %q (%s) contains conflicting terminal settlement evidence (ack=%v nack=%v expired=%v)", subSpan.GetName(), subSpan.GetSpanId(), hasAckEv, hasNackEv, hasExpEv),
				Expected: "mutually exclusive terminal settlement evidence on subscribe span",
				Actual:   fmt.Sprintf("ack=%v nack=%v expired=%v", hasAckEv, hasNackEv, hasExpEv),
				TraceID:  subSpan.GetTraceId(),
				SpanID:   subSpan.GetSpanId(),
				SpanName: subSpan.GetName(),
			})
		}

		// Settlements linked to this subscribe span: global bipartite matching per operation.
		if !hasExplicitTerminalSettle {
			switch normExplicitOutcome {
			case "acked":
				failures = append(failures, verifySubscribeSettlementEvents(subSpan, profile, SettlementExpectation{Operation: "ack"}, isEOD, devs)...)
			case "nacked":
				failures = append(failures, verifySubscribeSettlementEvents(subSpan, profile, SettlementExpectation{Operation: "nack"}, isEOD, devs)...)
			case "eod_failed":
				if hasLinkedAck {
					failures = append(failures, verifySubscribeSettlementEvents(subSpan, profile, SettlementExpectation{Operation: "ack"}, isEOD, devs)...)
				}
				if hasLinkedNack {
					failures = append(failures, verifySubscribeSettlementEvents(subSpan, profile, SettlementExpectation{Operation: "nack"}, isEOD, devs)...)
				}
				if !hasLinkedAck && !hasLinkedNack {
					if hasAckEv {
						failures = append(failures, verifySubscribeSettlementEvents(subSpan, profile, SettlementExpectation{Operation: "ack"}, isEOD, devs)...)
					} else if hasNackEv {
						failures = append(failures, verifySubscribeSettlementEvents(subSpan, profile, SettlementExpectation{Operation: "nack"}, isEOD, devs)...)
					} else {
						failures = append(failures, Violation{
							Code:     CodeMissingEvent,
							Rule:     "subscribe.event.settlement_events",
							Message:  fmt.Sprintf("subscribe span %q (%s) with ExpectedOutcome=%q is missing ack/nack start and end events", subSpan.GetName(), subSpan.GetSpanId(), m.ExpectedOutcome),
							Expected: "ack or nack start/end settlement events on subscribe span",
							Actual:   "<missing>",
							TraceID:  subSpan.GetTraceId(),
							SpanID:   subSpan.GetSpanId(),
							SpanName: subSpan.GetName(),
						})
					}
				}
			}
		}
		var opOrder []string
		opGroups := make(map[string][]SettlementExpectation)
		for _, settleExp := range m.Settlements {
			failures = append(failures, verifySubscribeSettlementEvents(subSpan, profile, settleExp, isEOD, devs)...)
			op := strings.ToLower(strings.TrimSpace(settleExp.Operation))
			if _, seen := opGroups[op]; !seen {
				opOrder = append(opOrder, op)
			}
			opGroups[op] = append(opGroups[op], settleExp)
		}
		for _, op := range opOrder {
			opItems := opGroups[op]
			settleName := effSubID + " " + op
			allOpSpans, bySub := getOpSpansAndLinked(settleName)
			linkedCandidates := bySub[subSpan]

			var opEdges []bipartiteEdge
			for si, settleExp := range opItems {
				for sj, sp := range linkedCandidates {
					opEdges = append(opEdges, bipartiteEdge{
						left:   si,
						right:  sj,
						weight: scoreSettlementEdge(settleExp, sp, profile),
					})
				}
			}
			opAssign := solveBipartiteMatching(len(opItems), len(linkedCandidates), nil, nil, opEdges)
			for si, settleExp := range opItems {
				rj := opAssign[si]
				if rj < 0 {
					if len(allOpSpans) > len(linkedCandidates) {
						if op == "modack" {
							phase2ModackForwardLinkFailed = true
						}
						failures = append(failures, Violation{
							Code:     CodeMissingLink,
							Rule:     "subscribe.settlement.forward_link",
							Message:  fmt.Sprintf("%q span exists but does not link to %q (%s/%s)", settleName, subSpan.GetName(), subSpan.GetTraceId(), subSpan.GetSpanId()),
							Expected: fmt.Sprintf("link -> %s/%s", subSpan.GetTraceId(), subSpan.GetSpanId()),
							Actual:   "<missing link>",
							TraceID:  subSpan.GetTraceId(),
							SpanID:   subSpan.GetSpanId(),
							SpanName: settleName,
						})
					} else {
						failures = append(failures, Violation{
							Code:     CodeMissingSpan,
							Rule:     "subscribe.settlement.present",
							Message:  fmt.Sprintf("missing %q span for %q (%s)", settleName, subSpan.GetName(), subSpan.GetSpanId()),
							Expected: settleName,
							Actual:   "<missing>",
							TraceID:  subSpan.GetTraceId(),
							SpanID:   subSpan.GetSpanId(),
							SpanName: settleName,
						})
					}
					continue
				}

				chosen := linkedCandidates[rj]
				matched[chosen] = true
				recordUniqueSettlement(chosen, settleExp)

				if profile.PerMessageSettle && (op == "ack" || op == "nack") {
					devs.add(Deviation{
						Category:    "cardinality",
						Rule:        "cardinality.per_message_settle",
						ExpectedGo:  "Batched <subID> ack/nack per RPC",
						Observed:    "One <subID> ack/nack span per message",
						SpanName:    chosen.GetName(),
						Description: "C++ TracingExactlyOnceAckHandler emits one ack/nack span per message",
					})
				}

				pair := spanLinkPair{sub: subSpan, settle: chosen}
				if !checkedReverseLinks[pair] {
					checkedReverseLinks[pair] = true
					failures = append(failures, verifySubscribeReverseLink(subSpan, chosen, op, profile, devs)...)
				}
			}
		}

		// Subscribe EOD failure status check.
		if isSubscribeEODFailed(m, isEOD) {
			if profile.AllowSubscribeUnsetOnEODError {
				if subSpan.GetStatusCode() != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
					failures = append(failures, checkStatusV(subSpan, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "subscribe.eod_error.cpp_status")...)
				} else {
					devs.add(Deviation{
						Category:    "status",
						Rule:        "status.subscribe_unset_on_eod_error",
						ExpectedGo:  "SPAN_STATUS_CODE_ERROR on <subID> subscribe",
						Observed:    "SPAN_STATUS_CODE_UNSET on subscribe (ERROR on <subID> ack/nack)",
						SpanName:    subSpan.GetName(),
						Description: "C++ subscribe_span->End() does not record EOD settlement failure status",
					})
				}
			} else {
				failures = append(failures, checkStatusV(subSpan, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, "", "subscribe.eod_error.status")...)
				failures = append(failures, checkEventV(subSpan, "exception", "subscribe.eod_error.exception")...)
			}
		} else {
			failures = append(failures, checkStatusV(subSpan, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "subscribe.status")...)
		}
	}

	// Collect all dropped (-00) trace IDs for Phase 3, Phase 4, and Phase 5 link audits.
	droppedTraceIDs := make(map[string]bool)
	for _, item := range droppedMsgs {
		if item.msg.WireTraceparent != "" {
			if tp, err := ParseTraceparent(item.msg.WireTraceparent); err == nil {
				droppedTraceIDs[tp.TraceID] = true
			}
		}
	}

	// Build the set of valid sampled <subID> subscribe span keys (excluding dropped -00 traces).
	allowExtraValidSubLinks := !strictExtra && !subExp.StrictSettlementOps
	validSampledSubByKey := make(map[SpanKey]*proxypb.TraceSpan, len(subCandidates))
	for _, sc := range subCandidates {
		if !isStrictValidSampledSubSpan(sc, profile, subID) {
			continue
		}
		sk := SpanKey{TraceID: NormalizeHex(sc.GetTraceId()), SpanID: NormalizeHex(sc.GetSpanId())}
		if idx.ByKey[sk] == sc && !droppedTraceIDs[sk.TraceID] {
			validSampledSubByKey[sk] = sc
		}
	}
	for sk, subSpan := range matchedSubByKey {
		if !droppedTraceIDs[sk.TraceID] {
			validSampledSubByKey[sk] = subSpan
		}
	}
	allowedExtraSubTargets := matchedSubByKey
	if allowExtraValidSubLinks {
		allowedExtraSubTargets = validSampledSubByKey
	}

	// Phase 3: Verify dropped unsampled (-00) subscribe messages AFTER sampled messages,
	// using capacitated leftGroup global bipartite matching across all dropped settlements.
	if len(droppedMsgs) > 0 {
		// Catch a buggy client that ignores unsampled traceparent and creates a fresh root subscribe subtree
		// (even when AllowExtraPubSubSpans is true).
		for _, sc := range subCandidates {
			if !usedSub[sc] {
				matched[sc] = true
				failures = append(failures, Violation{
					Code:     CodeUnexpectedSpan,
					Rule:     "subscribe.unsampled_dropped",
					Message:  fmt.Sprintf("unexpected %q span (%s/%s) when unsampled message should drop subscribe subtree", sc.GetName(), sc.GetTraceId(), sc.GetSpanId()),
					Expected: "subscribe subtree dropped",
					Actual:   sc.GetName(),
					TraceID:  sc.GetTraceId(),
					SpanID:   sc.GetSpanId(),
					SpanName: sc.GetName(),
				})
			}
		}
		if len(sampledMsgs) == 0 {
			for _, s := range sortedSpans {
				if matched[s] {
					continue
				}
				if isDroppedSubscribeSubtreeSpan(s, subID, profile) {
					name := s.GetName()
					matched[s] = true
					failures = append(failures, Violation{
						Code:     CodeUnexpectedSpan,
						Rule:     "subscribe.unsampled_dropped",
						Message:  fmt.Sprintf("unexpected %q span (%s/%s) when no sampled subscribe span is expected", name, s.GetTraceId(), s.GetSpanId()),
						Expected: "subscribe subtree dropped",
						Actual:   name,
						TraceID:  s.GetTraceId(),
						SpanID:   s.GetSpanId(),
						SpanName: name,
					})
				}
			}
		}

		reportedSubtreeLeak := make(map[*proxypb.TraceSpan]bool)
		for _, item := range droppedMsgs {
			if item.msg.WireTraceparent == "" {
				continue
			}
			tp, err := ParseTraceparent(item.msg.WireTraceparent)
			if err != nil {
				continue
			}
			for _, s := range sortedSpans {
				if NormalizeHex(s.GetTraceId()) == tp.TraceID && !matched[s] && !reportedSubtreeLeak[s] && isDroppedSubscribeSubtreeSpan(s, subID, profile) {
					reportedSubtreeLeak[s] = true
					matched[s] = true
					failures = append(failures, Violation{
						Code:     CodeUnexpectedSpan,
						Rule:     "subscribe.unsampled_dropped",
						Message:  fmt.Sprintf("unsampled wire traceparent %q should drop subscribe subtree, but found %q", item.msg.WireTraceparent, s.GetName()),
						Expected: "subscribe subtree dropped",
						Actual:   s.GetName(),
						TraceID:  s.GetTraceId(),
						SpanID:   s.GetSpanId(),
						SpanName: s.GetName(),
					})
				}
			}
		}

		type droppedSettleItem struct {
			droppedIdx int
			se         SettlementExpectation
		}
		var droppedOpOrder []string
		droppedByOp := make(map[string][]droppedSettleItem)
		for dIdx, item := range droppedMsgs {
			for _, se := range item.msg.Settlements {
				op := strings.ToLower(strings.TrimSpace(se.Operation))
				if _, seen := droppedByOp[op]; !seen {
					droppedOpOrder = append(droppedOpOrder, op)
				}
				droppedByOp[op] = append(droppedByOp[op], droppedSettleItem{droppedIdx: dIdx, se: se})
			}
		}

		checkDroppedSettlementCandidate := func(sp *proxypb.TraceSpan, op string) (bool, int, string, string) {
			seenSubKeys := make(map[SpanKey]bool)
			var rejectReason string
			for _, l := range sp.GetLinks() {
				if l == nil {
					continue
				}
				k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
				if droppedTraceIDs[k.TraceID] {
					return false, 0, "forbidden_link", k.TraceID
				}
				if len(sampledMsgs) == 0 {
					if rejectReason == "" {
						rejectReason = "dangling_link"
					}
					continue
				}
				if seenSubKeys[k] {
					if rejectReason == "" {
						rejectReason = "duplicate_link"
					}
					continue
				}
				seenSubKeys[k] = true
				subSpan, exists := matchedSubByKey[k]
				if !exists {
					if rejectReason == "" {
						rejectReason = "dangling_link"
					}
					continue
				}
				if profile.RequireBidirectionalSubscribeLinks {
					revMatches := findMatchingLinks(subSpan, sp)
					if len(revMatches) == 0 {
						if rejectReason == "" {
							rejectReason = "missing_reverse_link"
						}
						continue
					}
					if len(revMatches) > 1 {
						if rejectReason == "" {
							rejectReason = "duplicate_reverse_link"
						}
						continue
					}
					if AssertAttribute(revMatches[0].GetAttributes(), "messaging.operation.name", MatchString(op)) != nil {
						if rejectReason == "" {
							rejectReason = "bad_reverse_link_attr"
						}
						continue
					}
				}
				if !sampledSubSpanAllowsSettlementOp(subSpan, matchedSubMsg[subSpan], op, subExp.StrictSettlementOps, profile) {
					if rejectReason == "" {
						rejectReason = "incompatible_op"
					}
					continue
				}
			}
			if rejectReason != "" {
				return false, 0, rejectReason, ""
			}

			var slotCap int
			if batchCnt, hasBatch := AttrInt64(sp.GetAttributes(), "messaging.batch.message_count"); hasBatch {
				if batchCnt > 0 {
					used64 := int64(len(seenSubKeys)) + int64(unsampledUsedCount[sp])
					if batchCnt > used64 {
						slotCap = int(min(batchCnt-used64, int64(math.MaxInt)))
					}
				}
			} else if !matched[sp] && len(seenSubKeys) == 0 && unsampledUsedCount[sp] == 0 {
				slotCap = 1
			}
			if slotCap <= 0 {
				return false, 0, "no_capacity", ""
			}
			return true, slotCap, "", ""
		}

		for _, op := range droppedOpOrder {
			opItems := droppedByOp[op]
			settleName := subID + " " + op
			var allOpSpans []*proxypb.TraceSpan
			if subID != "" {
				allOpSpans = SortSpansDeterministic(idx.ByName[settleName])
			}

			var eligible []*proxypb.TraceSpan
			var rightCaps []int
			var sawDroppedTraceID string
			var unmatchedReason string
			var unmatchedSpan *proxypb.TraceSpan

			for _, sp := range allOpSpans {
				ok, capVal, reason, forbiddenTID := checkDroppedSettlementCandidate(sp, op)
				if ok {
					eligible = append(eligible, sp)
					rightCaps = append(rightCaps, capVal)
					continue
				}
				if !matched[sp] {
					if reason == "forbidden_link" && sawDroppedTraceID == "" {
						sawDroppedTraceID = forbiddenTID
					} else if unmatchedReason == "" && reason != "no_capacity" {
						unmatchedReason = reason
						unmatchedSpan = sp
					}
				}
			}

			leftGroup := make([]int, len(opItems))
			dEdges := make([]bipartiteEdge, 0, len(opItems)*len(eligible))
			if len(opItems)*len(eligible) > 64 {
				eligMetas := make([]settleScoreMeta, len(eligible))
				for j, sp := range eligible {
					eligMetas[j] = buildSettleScoreMeta(sp)
				}
				for i, di := range opItems {
					leftGroup[i] = di.droppedIdx
					for j := range eligible {
						dEdges = append(dEdges, bipartiteEdge{
							left:   i,
							right:  j,
							weight: scoreSettlementEdgeFast(di.se, eligMetas[j], profile),
						})
					}
				}
			} else {
				for i, di := range opItems {
					leftGroup[i] = di.droppedIdx
					for j, sp := range eligible {
						dEdges = append(dEdges, bipartiteEdge{
							left:   i,
							right:  j,
							weight: scoreSettlementEdge(di.se, sp, profile),
						})
					}
				}
			}
			dAssign := solveBipartiteMatching(len(opItems), len(eligible), rightCaps, leftGroup, dEdges)

			for i, di := range opItems {
				rj := dAssign[i]
				if rj >= 0 {
					chosen := eligible[rj]
					matched[chosen] = true
					unsampledUsedCount[chosen]++
					if op == "ack" {
						phase3ReservedAck[di.droppedIdx] = true
						phase3ReservedTerminal[di.droppedIdx] = true
					} else if op == "nack" {
						phase3ReservedNack[di.droppedIdx] = true
						phase3ReservedTerminal[di.droppedIdx] = true
					}
					recordUniqueSettlement(chosen, di.se)
					if profile.PerMessageSettle && (op == "ack" || op == "nack") {
						devs.add(Deviation{
							Category:    "cardinality",
							Rule:        "cardinality.per_message_settle",
							ExpectedGo:  "Batched <subID> ack/nack per RPC",
							Observed:    "One <subID> ack/nack span per message",
							SpanName:    chosen.GetName(),
							Description: "C++ TracingExactlyOnceAckHandler emits one ack/nack span per message",
						})
					}
					continue
				}

				if sawDroppedTraceID != "" {
					failures = append(failures, Violation{
						Code:     CodeBrokenTraceparent,
						Rule:     "subscribe.unsampled_settlement.forbidden_link",
						Message:  fmt.Sprintf("%q span links to dropped unsampled trace %s", settleName, sawDroppedTraceID),
						Expected: "no links to dropped trace " + sawDroppedTraceID,
						Actual:   "link targeting " + sawDroppedTraceID,
						SpanName: settleName,
					})
					continue
				}
				switch unmatchedReason {
				case "missing_reverse_link":
					failures = append(failures, Violation{
						Code:     CodeBrokenBidirectionalLink,
						Rule:     "subscribe.settlement.reverse_link",
						Message:  fmt.Sprintf("%q (%s) links to sampled subscribe span without reverse SpanLink", unmatchedSpan.GetName(), unmatchedSpan.GetSpanId()),
						Expected: fmt.Sprintf("bidirectional SpanLink <-> %s/%s", unmatchedSpan.GetTraceId(), unmatchedSpan.GetSpanId()),
						Actual:   "<missing reverse link>",
						TraceID:  unmatchedSpan.GetTraceId(),
						SpanID:   unmatchedSpan.GetSpanId(),
						SpanName: unmatchedSpan.GetName(),
					})
				case "duplicate_reverse_link":
					failures = append(failures, Violation{
						Code:     CodeBrokenBidirectionalLink,
						Rule:     "subscribe.settlement.duplicate_reverse_link",
						Message:  fmt.Sprintf("duplicate reverse SpanLink targeting %q (%s)", unmatchedSpan.GetName(), unmatchedSpan.GetSpanId()),
						Expected: "at most 1 reverse SpanLink",
						Actual:   "duplicate reverse link",
						TraceID:  unmatchedSpan.GetTraceId(),
						SpanID:   unmatchedSpan.GetSpanId(),
						SpanName: unmatchedSpan.GetName(),
					})
				case "bad_reverse_link_attr":
					failures = append(failures, Violation{
						Code:     CodeMissingLinkAttr,
						Rule:     "subscribe.settlement.reverse_link_attr",
						Message:  fmt.Sprintf("reverse link to %q (%s) missing/invalid messaging.operation.name", unmatchedSpan.GetName(), unmatchedSpan.GetSpanId()),
						Expected: fmt.Sprintf("messaging.operation.name=%q", op),
						Actual:   "<invalid>",
						TraceID:  unmatchedSpan.GetTraceId(),
						SpanID:   unmatchedSpan.GetSpanId(),
						SpanName: unmatchedSpan.GetName(),
					})
				case "duplicate_link":
					failures = append(failures, Violation{
						Code:     CodeBrokenBidirectionalLink,
						Rule:     "subscribe.settlement.duplicate_link",
						Message:  fmt.Sprintf("duplicate forward link on %q (%s)", unmatchedSpan.GetName(), unmatchedSpan.GetSpanId()),
						Expected: "unique forward link per subscribe span",
						Actual:   "duplicate link",
						TraceID:  unmatchedSpan.GetTraceId(),
						SpanID:   unmatchedSpan.GetSpanId(),
						SpanName: unmatchedSpan.GetName(),
					})
				case "dangling_link":
					failures = append(failures, Violation{
						Code:     CodeMissingLink,
						Rule:     "subscribe.settlement.dangling_link",
						Message:  fmt.Sprintf("%q (%s) contains dangling forward link to unresolved subscribe span", unmatchedSpan.GetName(), unmatchedSpan.GetSpanId()),
						Expected: "link targeting matched <subID> subscribe span",
						Actual:   "<dangling link>",
						TraceID:  unmatchedSpan.GetTraceId(),
						SpanID:   unmatchedSpan.GetSpanId(),
						SpanName: unmatchedSpan.GetName(),
					})
				default:
					failures = append(failures, Violation{
						Code:     CodeMissingSpan,
						Rule:     "subscribe.unsampled_settlement.present",
						Message:  fmt.Sprintf("missing root %q span (or remaining compatible batch slot) when subscribe span was dropped unsampled", settleName),
						Expected: settleName,
						Actual:   "<missing>",
						SpanName: settleName,
					})
				}
			}
		}
	}

	ackEligibleDropped := 0
	nackEligibleDropped := 0
	totalTerminalDropped := 0
	eodAckEligibleUnreserved := 0
	eodNackEligibleUnreserved := 0
	for dIdx, item := range droppedMsgs {
		allowAck := sampledSubAllowsSettlementOp(item.msg, "ack", subExp.StrictSettlementOps)
		allowNack := sampledSubAllowsSettlementOp(item.msg, "nack", subExp.StrictSettlementOps)
		if allowAck {
			ackEligibleDropped++
		}
		if allowNack {
			nackEligibleDropped++
		}
		if allowAck || allowNack {
			totalTerminalDropped++
		}
		if !phase3ReservedTerminal[dIdx] {
			isDroppedEOD := resolveIsEOD(item.msg, nil, profile) || (item.msg.ExactlyOnce == nil && profile.Name == ProfileGo)
			if isDroppedEOD {
				if allowAck && !phase3ReservedAck[dIdx] {
					eodAckEligibleUnreserved++
				}
				if allowNack && !phase3ReservedNack[dIdx] {
					eodNackEligibleUnreserved++
				}
			}
		}
	}
	unreservedAckRemaining := ackEligibleDropped - len(phase3ReservedAck)
	if unreservedAckRemaining < 0 {
		unreservedAckRemaining = 0
	}
	unreservedNackRemaining := nackEligibleDropped - len(phase3ReservedNack)
	if unreservedNackRemaining < 0 {
		unreservedNackRemaining = 0
	}
	unreservedTotalRemaining := totalTerminalDropped - len(phase3ReservedTerminal)
	if unreservedTotalRemaining < 0 {
		unreservedTotalRemaining = 0
	}

	// Phase 4: Validate <subID> modack spans (auto-matching only when StrictSettlementOps is false).
	anyDroppedUnsampled := len(droppedMsgs) > 0
	if subID != "" && len(msgs) > 0 {
		modackSpans := SortSpansDeterministic(idx.ByName[subID+" modack"])

		sampledALOCount := 0
		for _, item := range sampledMsgs {
			if !resolveIsEOD(item.msg, matchedSubForOrigIdx[item.origIdx], profile) {
				sampledALOCount++
			}
		}
		extraSampledALOCount := 0
		validExtraALOSubByKey := make(map[SpanKey]*proxypb.TraceSpan)
		if !strictExtra {
			for _, sc := range subCandidates {
				if usedSub[sc] || !isStrictValidSampledSubSpan(sc, profile, subID) {
					continue
				}
				sk := SpanKey{TraceID: NormalizeHex(sc.GetTraceId()), SpanID: NormalizeHex(sc.GetSpanId())}
				if idx.ByKey[sk] != sc || droppedTraceIDs[sk.TraceID] {
					continue
				}
				if eos, hasEOS := AttrBool(sc.GetAttributes(), profile.EOSSubscribeAttrKey); hasEOS && eos {
					continue
				}
				extraSampledALOCount++
				validExtraALOSubByKey[sk] = sc
			}
		}
		totalSampledALOCount := sampledALOCount + extraSampledALOCount
		allSampledAreEOD := len(sampledMsgs) > 0 && totalSampledALOCount == 0

		linkedALOReceiptSubs := make(map[*proxypb.TraceSpan]bool)
		for _, ms := range modackSpans {
			if len(ms.GetLinks()) == 0 {
				continue
			}
			isRec, hasRec := AttrBool(ms.GetAttributes(), "messaging.gcp_pubsub.is_receipt_modack")
			if hasRec && isRec {
				for _, l := range ms.GetLinks() {
					if l == nil {
						continue
					}
					k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
					if subSpan, ok := matchedSubByKey[k]; ok && !resolveIsEOD(matchedSubMsg[subSpan], subSpan, profile) {
						linkedALOReceiptSubs[subSpan] = true
					} else if !strictExtra {
						if extraSub := validExtraALOSubByKey[k]; extraSub != nil {
							linkedALOReceiptSubs[extraSub] = true
						}
					}
				}
			}
		}
		unlinkedALOAllowed := totalSampledALOCount - len(linkedALOReceiptSubs)
		if unlinkedALOAllowed < 0 {
			unlinkedALOAllowed = 0
		}
		unlinkedReceiptSlotsLeft := unlinkedALOAllowed
		remainingMixedReceiptSlots := len(droppedMsgs) + unlinkedALOAllowed
		linkedReceiptSlotsUsed := 0

		// Pass A: Linked auto-modack spans (len(links) > 0).
		// Process Phase 2/3 matched spans before unmatched auto spans so explicit matches
		// keep receipt ownership over duplicate spans.
		var linkedPhase23, linkedAuto []*proxypb.TraceSpan
		for _, ms := range modackSpans {
			if len(ms.GetLinks()) == 0 {
				continue
			}
			if matched[ms] {
				linkedPhase23 = append(linkedPhase23, ms)
			} else {
				linkedAuto = append(linkedAuto, ms)
			}
		}
		orderedLinked := make([]*proxypb.TraceSpan, 0, len(linkedPhase23)+len(linkedAuto))
		orderedLinked = append(orderedLinked, linkedPhase23...)
		orderedLinked = append(orderedLinked, linkedAuto...)

		seenLinkedReceiptSub := make(map[*proxypb.TraceSpan]bool)
		for _, ms := range orderedLinked {
			uniqueMatched := make(map[*proxypb.TraceSpan]bool)
			var matchedSubList []*proxypb.TraceSpan
			uniqueAllowedCount := 0
			seenAllowedKeys := make(map[SpanKey]bool)
			for _, l := range ms.GetLinks() {
				if l == nil {
					continue
				}
				k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
				if subSpan := matchedSubByKey[k]; subSpan != nil && !uniqueMatched[subSpan] {
					uniqueMatched[subSpan] = true
					matchedSubList = append(matchedSubList, subSpan)
				}
				if !seenAllowedKeys[k] && allowedExtraSubTargets[k] != nil {
					seenAllowedKeys[k] = true
					uniqueAllowedCount++
				}
			}
			linkedAny := len(uniqueMatched) > 0

			if linkedAny && profile.Name == ProfileGo {
				isRec, hasRec := AttrBool(ms.GetAttributes(), "messaging.gcp_pubsub.is_receipt_modack")
				bc, hasBC := AttrInt64(ms.GetAttributes(), "messaging.batch.message_count")
				if hasRec && isRec {
					var dupSubs []*proxypb.TraceSpan
					for _, subSpan := range matchedSubList {
						if seenLinkedReceiptSub[subSpan] {
							dupSubs = append(dupSubs, subSpan)
						}
					}
					if len(dupSubs) > 0 {
						firstDup := SortSpansDeterministic(dupSubs)[0]
						failures = append(failures, Violation{
							Code:     CodeUnexpectedSpan,
							Rule:     "subscribe.modack.duplicate_receipt",
							Message:  fmt.Sprintf("duplicate linked receipt %q span (%s/%s) targeting %q (%s/%s) which already has a linked is_receipt_modack=true modack", ms.GetName(), ms.GetTraceId(), ms.GetSpanId(), firstDup.GetName(), firstDup.GetTraceId(), firstDup.GetSpanId()),
							Expected: "at most 1 linked is_receipt_modack=true modack per <subID> subscribe span",
							Actual:   fmt.Sprintf("duplicate receipt modack %s/%s", ms.GetTraceId(), ms.GetSpanId()),
							TraceID:  ms.GetTraceId(),
							SpanID:   ms.GetSpanId(),
							SpanName: ms.GetName(),
						})
						continue
					}
					for _, subSpan := range matchedSubList {
						seenLinkedReceiptSub[subSpan] = true
					}
				}
				if !hasRec || isRec {
					excess := 0
					if hasBC && int(bc) > uniqueAllowedCount {
						excess = int(bc) - uniqueAllowedCount
					}
					if unsampledUsedCount[ms] > excess {
						excess = unsampledUsedCount[ms]
					}
					maxAvail := unlinkedReceiptSlotsLeft
					if anyDroppedUnsampled {
						maxAvail = len(droppedMsgs) + unlinkedALOAllowed - linkedReceiptSlotsUsed
					}
					if maxAvail < 0 {
						maxAvail = 0
					}
					if strictExtra && excess > maxAvail {
						actualStr := strconv.Itoa(len(uniqueMatched) + excess)
						if hasBC {
							actualStr = strconv.FormatInt(bc, 10)
						}
						failures = append(failures, Violation{
							Code:     CodeWrongAttributeValue,
							Rule:     "settlement.modack.attr.batch_count",
							Message:  fmt.Sprintf("linked receipt %q batch_count %s exceeds linked+available unlinked slots (%d + %d)", ms.GetName(), actualStr, len(uniqueMatched), maxAvail),
							Expected: fmt.Sprintf("<= %d", len(uniqueMatched)+maxAvail),
							Actual:   actualStr,
							TraceID:  ms.GetTraceId(),
							SpanID:   ms.GetSpanId(),
							SpanName: ms.GetName(),
						})
					}
					used := excess
					if used > maxAvail {
						used = maxAvail
					}
					linkedReceiptSlotsUsed += used
					if anyDroppedUnsampled {
						remainingMixedReceiptSlots -= used
						if remainingMixedReceiptSlots < 0 {
							remainingMixedReceiptSlots = 0
						}
					} else {
						unlinkedReceiptSlotsLeft -= used
						if unlinkedReceiptSlotsLeft < 0 {
							unlinkedReceiptSlotsLeft = 0
						}
					}
				} else if strictExtra && hasBC && int(bc) > len(uniqueMatched)+len(droppedMsgs) {
					failures = append(failures, Violation{
						Code:     CodeWrongAttributeValue,
						Rule:     "settlement.modack.attr.batch_count",
						Message:  fmt.Sprintf("linked periodic non-receipt %q batch_count %d exceeds linked+dropped slots (%d + %d)", ms.GetName(), bc, len(uniqueMatched), len(droppedMsgs)),
						Expected: fmt.Sprintf("<= %d", len(uniqueMatched)+len(droppedMsgs)),
						Actual:   strconv.FormatInt(bc, 10),
						TraceID:  ms.GetTraceId(),
						SpanID:   ms.GetSpanId(),
						SpanName: ms.GetName(),
					})
				}
			}

			if linkedAny {
				if !matched[ms] {
					if !subExp.StrictSettlementOps {
						matched[ms] = true
						recordUniqueSettlement(ms, SettlementExpectation{Operation: "modack"})
					} else {
						failures = append(failures, auditExtraSettlementLinks(ms, matchedSubByKey, droppedTraceIDs, false)...)
					}
				}
				continue
			}
			// 0 links target any matched sampled subscribe span: under AllowExtraPubSubSpans && !StrictSettlementOps,
			// permit links targeting valid unmatched sampled <subID> subscribe spans while still auditing bad/dropped/duplicate links.
			failures = append(failures, auditExtraSettlementLinks(ms, allowedExtraSubTargets, droppedTraceIDs, allowExtraValidSubLinks)...)
		}

		// Pass B: Unlinked auto-modack spans (len(links) == 0).
		// Process explicitly Phase-3 matched spans BEFORE unmatched spans so Phase 3 spans
		// receive slot priority and never double-count themselves against remainingMixedReceiptSlots.
		// Go-specific EOD synchronous receipt modack and is_receipt_modack capacity rules
		// are strictly gated on profile.Name == ProfileGo so C++ RefreshMessageLeases/AckEnd races never false-fail.
		var unlinkedPhase3, unlinkedAuto []*proxypb.TraceSpan
		for _, ms := range modackSpans {
			if len(ms.GetLinks()) > 0 {
				continue
			}
			if matched[ms] {
				unlinkedPhase3 = append(unlinkedPhase3, ms)
			} else {
				unlinkedAuto = append(unlinkedAuto, ms)
			}
		}
		orderedUnlinked := make([]*proxypb.TraceSpan, 0, len(unlinkedPhase3)+len(unlinkedAuto))
		orderedUnlinked = append(orderedUnlinked, unlinkedPhase3...)
		orderedUnlinked = append(orderedUnlinked, unlinkedAuto...)

		for _, ms := range orderedUnlinked {
			wasMatchedInPhase3 := matched[ms]
			isReceipt, hasIsReceipt := AttrBool(ms.GetAttributes(), "messaging.gcp_pubsub.is_receipt_modack")
			batchCount, hasBatchCount := AttrInt64(ms.GetAttributes(), "messaging.batch.message_count")

			if profile.Name == ProfileGo && !anyDroppedUnsampled {
				if allSampledAreEOD {
					if strictExtra && hasBatchCount && batchCount > int64(len(sampledMsgs)) {
						failures = append(failures, Violation{
							Code:     CodeWrongAttributeValue,
							Rule:     "settlement.modack.attr.batch_count",
							Message:  fmt.Sprintf("unlinked %q batch_count %d exceeds sampled message count (%d)", ms.GetName(), batchCount, len(sampledMsgs)),
							Expected: fmt.Sprintf("<= %d", len(sampledMsgs)),
							Actual:   strconv.FormatInt(batchCount, 10),
							TraceID:  ms.GetTraceId(),
							SpanID:   ms.GetSpanId(),
							SpanName: ms.GetName(),
						})
					}
					if !phase2ModackForwardLinkFailed {
						failures = append(failures, Violation{
							Code:     CodeMissingLink,
							Rule:     "subscribe.modack.missing_link",
							Message:  fmt.Sprintf("unlinked %q span (%s) invalid when all sampled messages use EOD (Go sends EOD receipt modack synchronously before callback)", ms.GetName(), ms.GetSpanId()),
							Expected: "SpanLink targeting <subID> subscribe",
							Actual:   "0 links",
							TraceID:  ms.GetTraceId(),
							SpanID:   ms.GetSpanId(),
							SpanName: ms.GetName(),
						})
					}
					continue
				}
				if unlinkedReceiptSlotsLeft <= 0 {
					if strictExtra && hasBatchCount && batchCount > int64(len(sampledMsgs)) {
						failures = append(failures, Violation{
							Code:     CodeWrongAttributeValue,
							Rule:     "settlement.modack.attr.batch_count",
							Message:  fmt.Sprintf("unlinked %q batch_count %d exceeds sampled message count (%d)", ms.GetName(), batchCount, len(sampledMsgs)),
							Expected: fmt.Sprintf("<= %d", len(sampledMsgs)),
							Actual:   strconv.FormatInt(batchCount, 10),
							TraceID:  ms.GetTraceId(),
							SpanID:   ms.GetSpanId(),
							SpanName: ms.GetName(),
						})
					}
					if !phase2ModackForwardLinkFailed {
						failures = append(failures, Violation{
							Code:     CodeMissingLink,
							Rule:     "subscribe.modack.missing_link",
							Message:  fmt.Sprintf("unlinked %q span (%s) invalid when no unlinked ALO receipt modack slots remain", ms.GetName(), ms.GetSpanId()),
							Expected: "SpanLink targeting <subID> subscribe",
							Actual:   "0 links",
							TraceID:  ms.GetTraceId(),
							SpanID:   ms.GetSpanId(),
							SpanName: ms.GetName(),
						})
					}
					continue
				}
				if strictExtra && (!hasIsReceipt || isReceipt || subExp.StrictSettlementOps) && hasBatchCount && batchCount > int64(unlinkedReceiptSlotsLeft) {
					failures = append(failures, Violation{
						Code:     CodeWrongAttributeValue,
						Rule:     "settlement.modack.attr.batch_count",
						Message:  fmt.Sprintf("unlinked receipt %q batch_count %d exceeds remaining unlinked ALO slots (%d)", ms.GetName(), batchCount, unlinkedReceiptSlotsLeft),
						Expected: fmt.Sprintf("<= %d", unlinkedReceiptSlotsLeft),
						Actual:   strconv.FormatInt(batchCount, 10),
						TraceID:  ms.GetTraceId(),
						SpanID:   ms.GetSpanId(),
						SpanName: ms.GetName(),
					})
					continue
				}
				usedSlots := 1
				if hasBatchCount && batchCount > 0 {
					if batchCount > int64(unlinkedReceiptSlotsLeft) {
						usedSlots = unlinkedReceiptSlotsLeft
					} else {
						usedSlots = int(batchCount)
					}
				}
				if usedSlots > unlinkedReceiptSlotsLeft {
					usedSlots = unlinkedReceiptSlotsLeft
				}
				unlinkedReceiptSlotsLeft -= usedSlots
			} else if profile.Name == ProfileGo && anyDroppedUnsampled {
				if hasIsReceipt && !isReceipt {
					if strictExtra && hasBatchCount && batchCount > int64(len(droppedMsgs)) {
						failures = append(failures, Violation{
							Code:     CodeWrongAttributeValue,
							Rule:     "settlement.modack.attr.batch_count",
							Message:  fmt.Sprintf("unlinked periodic non-receipt %q batch_count %d exceeds dropped message count (%d)", ms.GetName(), batchCount, len(droppedMsgs)),
							Expected: fmt.Sprintf("<= %d", len(droppedMsgs)),
							Actual:   strconv.FormatInt(batchCount, 10),
							TraceID:  ms.GetTraceId(),
							SpanID:   ms.GetSpanId(),
							SpanName: ms.GetName(),
						})
						if !wasMatchedInPhase3 && !phase2ModackForwardLinkFailed {
							failures = append(failures, Violation{
								Code:     CodeMissingLink,
								Rule:     "subscribe.modack.missing_link",
								Message:  fmt.Sprintf("unlinked periodic non-receipt %q batch_count %d exceeds dropped count (%d) without links to sampled subscribe spans", ms.GetName(), batchCount, len(droppedMsgs)),
								Expected: "SpanLink targeting <subID> subscribe",
								Actual:   "0 links",
								TraceID:  ms.GetTraceId(),
								SpanID:   ms.GetSpanId(),
								SpanName: ms.GetName(),
							})
						}
						continue
					}
				} else {
					slotsNeeded64 := int64(1)
					if hasBatchCount && batchCount > 0 {
						slotsNeeded64 = batchCount
					} else if int64(unsampledUsedCount[ms]) > slotsNeeded64 {
						slotsNeeded64 = int64(unsampledUsedCount[ms])
					}
					if remainingMixedReceiptSlots <= 0 && !wasMatchedInPhase3 {
						if !phase2ModackForwardLinkFailed {
							failures = append(failures, Violation{
								Code:     CodeMissingLink,
								Rule:     "subscribe.modack.missing_link",
								Message:  fmt.Sprintf("unlinked receipt %q span (%s) exceeds remaining dropped+ALO receipt slots", ms.GetName(), ms.GetSpanId()),
								Expected: "SpanLink targeting <subID> subscribe",
								Actual:   "0 links",
								TraceID:  ms.GetTraceId(),
								SpanID:   ms.GetSpanId(),
								SpanName: ms.GetName(),
							})
						}
						continue
					}
					if slotsNeeded64 > int64(remainingMixedReceiptSlots) {
						if strictExtra {
							failures = append(failures, Violation{
								Code:     CodeWrongAttributeValue,
								Rule:     "settlement.modack.attr.batch_count",
								Message:  fmt.Sprintf("unlinked receipt %q batch_count %d exceeds remaining dropped+ALO receipt slots (%d)", ms.GetName(), slotsNeeded64, remainingMixedReceiptSlots),
								Expected: fmt.Sprintf("<= %d", remainingMixedReceiptSlots),
								Actual:   strconv.FormatInt(slotsNeeded64, 10),
								TraceID:  ms.GetTraceId(),
								SpanID:   ms.GetSpanId(),
								SpanName: ms.GetName(),
							})
							remainingMixedReceiptSlots = 0
							continue
						}
						remainingMixedReceiptSlots = 0
					} else {
						remainingMixedReceiptSlots -= int(slotsNeeded64)
					}
				}
			}

			if !wasMatchedInPhase3 && !subExp.StrictSettlementOps {
				matched[ms] = true
				autoModackExp := SettlementExpectation{Operation: "modack"}
				if profile.Name == ProfileGo && !anyDroppedUnsampled {
					autoModackExp.IsReceiptModack = BoolPtr(true)
				}
				recordUniqueSettlement(ms, autoModackExp)
			}
		}

		// Audit unmatched linked ack/nack spans consistently with Phase 4 Pass A modack link auditing,
		// enforcing settlement operation compatibility and auto-recording legitimate omitted-settlement spans
		// when !subExp.StrictSettlementOps so Phase 5 verifies attributes, status, and reverse links.
		autoMatchedTerminalSub := make(map[*proxypb.TraceSpan]bool)
		if strictExtra {
			for _, op := range []string{"ack", "nack"} {
				for _, sp := range getSortedOpSpans(subID + " " + op) {
					if !matched[sp] {
						continue
					}
					for _, l := range sp.GetLinks() {
						if l == nil {
							continue
						}
						k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
						if subSpan := matchedSubByKey[k]; subSpan != nil {
							autoMatchedTerminalSub[subSpan] = true
						}
					}
				}
			}
		}
		for _, op := range []string{"ack", "nack"} {
			otherOp := "nack"
			if op == "nack" {
				otherOp = "ack"
			}
			opSpans := getSortedOpSpans(subID + " " + op)
			hasNonErrForSub := make(map[*proxypb.TraceSpan]bool)
			hasNonErrForExtra := make(map[*proxypb.TraceSpan]bool)
			hasNonErrOtherOpExtra := make(map[*proxypb.TraceSpan]bool)
			if !strictExtra {
				for _, sp := range opSpans {
					if sp.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
						continue
					}
					for _, l := range sp.GetLinks() {
						if l == nil {
							continue
						}
						k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
						if subSpan := matchedSubByKey[k]; subSpan != nil {
							if profile.Name != ProfileGo {
								hasNonErrForSub[subSpan] = true
							}
						} else if extraSub := validSampledSubByKey[k]; extraSub != nil {
							hasNonErrForExtra[extraSub] = true
						}
					}
				}
				for _, cand := range getSortedOpSpans(subID + " " + otherOp) {
					if cand.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
						continue
					}
					for _, l := range cand.GetLinks() {
						if l == nil {
							continue
						}
						k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
						if matchedSubByKey[k] == nil {
							if extraSub := validSampledSubByKey[k]; extraSub != nil {
								hasNonErrOtherOpExtra[extraSub] = true
							}
						}
					}
				}
			}
			unreservedOpRem := unreservedAckRemaining
			unreservedEODOpDropped := eodAckEligibleUnreserved
			if op == "nack" {
				unreservedOpRem = unreservedNackRemaining
				unreservedEODOpDropped = eodNackEligibleUnreserved
			}
			unreservedEODOpDropped = min(unreservedEODOpDropped, min(unreservedOpRem, unreservedTotalRemaining))
			for _, sp := range opSpans {
				wasMatched := matched[sp]
				if len(sp.GetLinks()) == 0 || (wasMatched && subExp.StrictSettlementOps) {
					continue
				}
				isErrSpan := sp.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				var linkAudits []Violation
				if !wasMatched {
					linkAudits = auditExtraSettlementLinks(sp, allowedExtraSubTargets, droppedTraceIDs, allowExtraValidSubLinks)
					failures = append(failures, linkAudits...)
				}

				seenTargets := make(map[SpanKey]bool)
				seenValidSubs := make(map[*proxypb.TraceSpan]bool)
				var linkedMatchedSubs []*proxypb.TraceSpan
				linkedMatchedSub := false
				hasIncompatibleOp := false
				anyUnclaimedMissingExplicit := false
				allUnclaimedMissingExplicit := true
				allMissingExplicit := true
				anyEODTarget := false
				anyEODWithExplicit := false
				anyEODFailedTarget := false
				anyRetrySuccessPartner := false
				anyCppRetrySuccessPartner := false
				anyUnverifiedExtraSub := false
				hasDisallowedBatchPartner := false

				for _, l := range sp.GetLinks() {
					if l == nil {
						continue
					}
					k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
					if seenTargets[k] {
						continue
					}
					seenTargets[k] = true
					subSpan := matchedSubByKey[k]
					if subSpan == nil {
						if !strictExtra {
							if extraSub := validSampledSubByKey[k]; extraSub != nil {
								seenValidSubs[extraSub] = true
								if !isErrSpan && extraSub.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
									anyEODFailedTarget = true
									hasDisallowedBatchPartner = true
								} else if resolveIsEOD(SubscribeMessageExpectation{}, extraSub, profile) {
									anyEODTarget = true
									resStr, _ := AttrString(extraSub.GetAttributes(), "messaging.gcp_pubsub.result")
									resNorm := strings.ToLower(strings.TrimSpace(resStr))
									resOtherOrExpired := resNorm == "expired" || resNorm == otherOp || resNorm == otherOp+"ed"
									haExtra, hnExtra, heExtra := analyzeSubSpanTerminalEvidence(extraSub, profile)
									hasConflictExtra := (haExtra && hnExtra) || (heExtra && (haExtra || hnExtra))
									hasOtherEv := (op == "ack" && hnExtra) || (op == "nack" && haExtra)
									if !hasConflictExtra && !heExtra && !resOtherOrExpired && !hasOtherEv {
										if extraSub.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
											goResultContradictsErr := profile.Name == ProfileGo && resNorm != ""
											hasErrExtraEv := (profile.Name == ProfileGo && countEventsNamed(extraSub, op+" start") > 0 && countEventsNamed(extraSub, op+" end") > 0) ||
												(profile.Name != ProfileGo && (countEventsNamed(extraSub, "gl-cpp."+op+"_start") > 0 || len(extraSub.GetEvents()) == 0))
											if !hasNonErrForExtra[extraSub] && !hasNonErrOtherOpExtra[extraSub] &&
												!goResultContradictsErr && hasErrExtraEv {
												anyEODFailedTarget = true
											} else {
												hasDisallowedBatchPartner = true
											}
										} else if !hasNonErrOtherOpExtra[extraSub] {
											if profile.Name == ProfileGo {
												starts := countEventsNamed(extraSub, op+" start")
												ends := countEventsNamed(extraSub, op+" end")
												noOtherEv := countEventsNamed(extraSub, otherOp+" start") == 0 && countEventsNamed(extraSub, otherOp+" end") == 0
												if starts > 0 && ends > starts && noOtherEv {
													anyRetrySuccessPartner = true
												} else if !hasNonErrForExtra[extraSub] && ((starts > 0 && ends == starts && noOtherEv) || len(extraSub.GetEvents()) == 0) {
													if len(extraSub.GetEvents()) == 0 {
														// Preserve synthetic Go fixtures whose extra EOD subscribe spans omit settlement events.
														anyUnverifiedExtraSub = true
													}
												} else {
													hasDisallowedBatchPartner = true
												}
											} else {
												cppStarts := countEventsNamed(extraSub, "gl-cpp."+op+"_start")
												cppEnds := countEventsNamed(extraSub, "gl-cpp."+op+"_end")
												hasCppOtherOrExpiredEv := countEventsNamed(extraSub, "gl-cpp."+otherOp+"_start") > 0 ||
													countEventsNamed(extraSub, "gl-cpp."+otherOp+"_end") > 0 ||
													countEventsNamed(extraSub, "gl-cpp.expired") > 0 ||
													countEventsNamed(extraSub, "gl-cpp.message_"+otherOp) > 0
												if !hasNonErrForExtra[extraSub] && !hasCppOtherOrExpiredEv && ((cppStarts > 0 && cppEnds > 0) || len(extraSub.GetEvents()) == 0) {
													anyUnverifiedExtraSub = true
												} else {
													hasDisallowedBatchPartner = true
												}
											}
										} else {
											hasDisallowedBatchPartner = true
										}
									} else {
										hasDisallowedBatchPartner = true
									}
								} else {
									hasDisallowedBatchPartner = true
								}
							} else {
								hasDisallowedBatchPartner = true
							}
						}
						continue
					}
					linkedMatchedSub = true
					linkedMatchedSubs = append(linkedMatchedSubs, subSpan)
					subMsg := matchedSubMsg[subSpan]
					if !sampledSubSpanAllowsSettlementOp(subSpan, subMsg, op, subExp.StrictSettlementOps, profile) {
						hasIncompatibleOp = true
						hasDisallowedBatchPartner = true
						if !wasMatched {
							failures = append(failures, Violation{
								Code:     CodeBrokenBidirectionalLink,
								Rule:     "subscribe.settlement.link_incompatible_op",
								Message:  fmt.Sprintf("%q (%s) links to %q (%s) which disallows settlement operation %q (expected_outcome=%q)", sp.GetName(), sp.GetSpanId(), subSpan.GetName(), subSpan.GetSpanId(), op, subMsg.ExpectedOutcome),
								Expected: fmt.Sprintf("links only to subscribe spans allowing %q", op),
								Actual:   fmt.Sprintf("link to %s/%s", subSpan.GetTraceId(), subSpan.GetSpanId()),
								TraceID:  sp.GetTraceId(),
								SpanID:   sp.GetSpanId(),
								SpanName: sp.GetName(),
							})
						}
						continue
					}
					seenValidSubs[subSpan] = true
					isEODTarget := resolveIsEOD(subMsg, subSpan, profile)
					if isEODTarget {
						anyEODTarget = true
					} else {
						hasDisallowedBatchPartner = true
					}
					hasExplicitTerminal := hasExplicitTerminalSettlement(subMsg.Settlements)
					if !hasExplicitTerminal {
						if !autoMatchedTerminalSub[subSpan] {
							anyUnclaimedMissingExplicit = true
						} else {
							allUnclaimedMissingExplicit = false
						}
					} else {
						allMissingExplicit = false
						if isEODTarget {
							anyEODWithExplicit = true
						}
					}
					if !hasExplicitTerminal || !wasMatched {
						if isSubscribeEODFailed(subMsg, isEODTarget) {
							anyEODFailedTarget = true
						} else if profile.Name == ProfileGo && isEODTarget &&
							countEventsNamed(subSpan, op+" start") > 0 &&
							countEventsNamed(subSpan, op+" end") > countEventsNamed(subSpan, op+" start") {
							anyRetrySuccessPartner = true
						} else if !strictExtra && profile.Name != ProfileGo && isEODTarget && hasNonErrForSub[subSpan] {
							anyCppRetrySuccessPartner = true
						}
					}
				}

				bc, hasBC := AttrInt64(sp.GetAttributes(), "messaging.batch.message_count")
				totalBatchSlots := int64(len(seenTargets))
				if hasBC && bc > totalBatchSlots {
					totalBatchSlots = bc
				}
				isMultiMsgBatch := (hasBC && bc > 1) || len(seenTargets) > 1
				if !isMultiMsgBatch && anyCppRetrySuccessPartner {
					anyRetrySuccessPartner = true
				}
				allowUnmatchedExplicitEOD := !wasMatched && !strictExtra && !isMultiMsgBatch && anyEODWithExplicit && !hasDisallowedBatchPartner
				hasUnverifiedBatchSlot := totalBatchSlots > int64(len(seenValidSubs))+int64(unsampledUsedCount[sp]) || anyUnverifiedExtraSub
				unverifiedPartialBatch := anyEODTarget && isMultiMsgBatch && hasUnverifiedBatchSlot && (!strictExtra || unreservedEODOpDropped > 0)
				isPartialBatchStatus := sp.GetStatusMessage() == eodPartialBatchErrorMsg
				expectErr := anyEODFailedTarget || (isErrSpan && !hasDisallowedBatchPartner && (anyRetrySuccessPartner || allowUnmatchedExplicitEOD || (unverifiedPartialBatch && isPartialBatchStatus)))
				partialBatchFailure := expectErr && (anyEODFailedTarget || anyRetrySuccessPartner || unverifiedPartialBatch) && isPartialBatchStatus

				if wasMatched {
					if expectErr {
						recordUniqueSettlement(sp, SettlementExpectation{
							Operation:              op,
							ExpectError:            true,
							EODPartialBatchFailure: partialBatchFailure,
						})
					}
					continue
				}

				if !subExp.StrictSettlementOps && linkedMatchedSub && len(linkAudits) == 0 && !hasIncompatibleOp {
					shouldAutoClaim := !strictExtra || (anyUnclaimedMissingExplicit && allUnclaimedMissingExplicit && allMissingExplicit)
					if shouldAutoClaim {
						if strictExtra {
							for _, subSpan := range linkedMatchedSubs {
								autoMatchedTerminalSub[subSpan] = true
							}
						}
						matched[sp] = true
						recordUniqueSettlement(sp, SettlementExpectation{
							Operation:              op,
							ExpectError:            expectErr,
							EODPartialBatchFailure: partialBatchFailure,
						})
					}
				}
			}
		}

	}

	// Phase 5a: Pre-audit forward/reverse links and precompute uniqueValidLinks across spans already in settleOrder
	// (including both matchedSubByKey and validSampledSubByKey links) before Phase 4.6 coalescing.
	prePhase46Order := SortSpansDeterministic(settleOrder)
	preUniqueValidLinks := make(map[*proxypb.TraceSpan]int, len(prePhase46Order))
	preUniqueTargetCount := make(map[*proxypb.TraceSpan]int, len(prePhase46Order))
	preLinkFailures := make(map[*proxypb.TraceSpan][]Violation)
	inferSettleOp := func(sp *proxypb.TraceSpan) string {
		return strings.ToLower(strings.TrimSpace(uniqueSettleSpans[sp].expectations[0].Operation))
	}
	for _, sp := range prePhase46Order {
		op := inferSettleOp(sp)
		seenTargets := make(map[SpanKey]bool)
		uniqueValidLinks := 0
		var linkFails []Violation
		for li, l := range sp.GetLinks() {
			if l == nil {
				continue
			}
			k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
			if seenTargets[k] {
				linkFails = append(linkFails, Violation{
					Code:     CodeBrokenBidirectionalLink,
					Rule:     "subscribe.settlement.duplicate_link",
					Message:  fmt.Sprintf("duplicate forward link[%d] on %q targeting (%s/%s)", li, sp.GetName(), l.GetTraceId(), l.GetSpanId()),
					Expected: "unique forward link per subscribe span",
					Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
					TraceID:  sp.GetTraceId(),
					SpanID:   sp.GetSpanId(),
					SpanName: sp.GetName(),
				})
				continue
			}
			seenTargets[k] = true
			subSpan, ok := matchedSubByKey[k]
			if !ok {
				if droppedTraceIDs[k.TraceID] {
					linkFails = append(linkFails, Violation{
						Code:     CodeBrokenTraceparent,
						Rule:     "subscribe.unsampled_settlement.forbidden_link",
						Message:  fmt.Sprintf("%q link[%d] targets dropped unsampled trace %s", sp.GetName(), li, k.TraceID),
						Expected: "no links to dropped trace " + k.TraceID,
						Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
						TraceID:  sp.GetTraceId(),
						SpanID:   sp.GetSpanId(),
						SpanName: sp.GetName(),
					})
				} else if allowExtraValidSubLinks && validSampledSubByKey[k] != nil {
					uniqueValidLinks++
				} else {
					expected := "link targeting matched <subID> subscribe span"
					if allowExtraValidSubLinks {
						expected = "link targeting valid sampled <subID> subscribe span"
					}
					linkFails = append(linkFails, Violation{
						Code:     CodeMissingLink,
						Rule:     "subscribe.settlement.dangling_link",
						Message:  fmt.Sprintf("%q link[%d] targets unresolved or non-subscribe span (%s/%s)", sp.GetName(), li, l.GetTraceId(), l.GetSpanId()),
						Expected: expected,
						Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
						TraceID:  sp.GetTraceId(),
						SpanID:   sp.GetSpanId(),
						SpanName: sp.GetName(),
					})
				}
				continue
			}
			pair := spanLinkPair{sub: subSpan, settle: sp}
			if !sampledSubSpanAllowsSettlementOp(subSpan, matchedSubMsg[subSpan], op, subExp.StrictSettlementOps, profile) {
				checkedReverseLinks[pair] = true
				subMsg := matchedSubMsg[subSpan]
				linkFails = append(linkFails, Violation{
					Code:     CodeBrokenBidirectionalLink,
					Rule:     "subscribe.settlement.link_incompatible_op",
					Message:  fmt.Sprintf("%q (%s) links to %q (%s) which disallows settlement operation %q (expected_outcome=%q)", sp.GetName(), sp.GetSpanId(), subSpan.GetName(), subSpan.GetSpanId(), op, subMsg.ExpectedOutcome),
					Expected: fmt.Sprintf("links only to subscribe spans allowing %q", op),
					Actual:   fmt.Sprintf("link to %s/%s", subSpan.GetTraceId(), subSpan.GetSpanId()),
					TraceID:  sp.GetTraceId(),
					SpanID:   sp.GetSpanId(),
					SpanName: sp.GetName(),
				})
				continue
			}
			uniqueValidLinks++
			if !checkedReverseLinks[pair] {
				checkedReverseLinks[pair] = true
				linkFails = append(linkFails, verifySubscribeReverseLink(subSpan, sp, op, profile, devs)...)
			}
		}
		preUniqueValidLinks[sp] = uniqueValidLinks
		preUniqueTargetCount[sp] = len(seenTargets)
		if len(linkFails) > 0 {
			preLinkFailures[sp] = linkFails
		}
	}

	// Phase 4.6: Jointly match unreserved non-terminal dropped (-00) messages across:
	// Pass 1a (0-link status-match) -> Pass 1b (>=1-link coalesced status-match) ->
	// Pass 2a (0-link status-mismatch) -> Pass 2b (>=1-link coalesced status-mismatch),
	// followed by deterministic CodeMissingSpan enforcement.
	if len(msgs) > 0 && len(droppedMsgs) > 0 {
		if unreservedTotalRemaining > 0 {
			var unreservedDropped []unreservedDroppedItem
			for dIdx, item := range droppedMsgs {
				if hasExplicitTerminalSettlement(item.msg.Settlements) || phase3ReservedTerminal[dIdx] {
					continue
				}
				allowAck := sampledSubAllowsSettlementOp(item.msg, "ack", subExp.StrictSettlementOps)
				allowNack := sampledSubAllowsSettlementOp(item.msg, "nack", subExp.StrictSettlementOps)
				if !allowAck && !allowNack {
					continue
				}
				isEOD := resolveIsEOD(item.msg, nil, profile) || (item.msg.ExactlyOnce == nil && profile.Name == ProfileGo)
				unreservedDropped = append(unreservedDropped, unreservedDroppedItem{
					droppedIdx: dIdx,
					wantErr:    isSubscribeEODFailed(item.msg, isEOD),
					allowAck:   allowAck,
					allowNack:  allowNack,
				})
			}

			if len(unreservedDropped) > 0 {
				var cands0 []unlinkedSettleCandidate
				var caps0 []int
				var cands0Invalid []unlinkedSettleCandidate
				var caps0Invalid []int
				for _, op := range []string{"ack", "nack"} {
					opAvail := unreservedAckRemaining
					if op == "nack" {
						opAvail = unreservedNackRemaining
					}
					if opAvail <= 0 {
						continue
					}
					for _, sp := range getSortedOpSpans(subID + " " + op) {
						if len(sp.GetLinks()) != 0 {
							continue
						}
						capVal := 1
						invalidBC := false
						if batchCnt, hasBatch := AttrInt64(sp.GetAttributes(), "messaging.batch.message_count"); hasBatch {
							if batchCnt <= 0 {
								if strictExtra || matched[sp] || unsampledUsedCount[sp] > 0 {
									continue
								}
								invalidBC = true
							} else {
								rem64 := batchCnt - int64(unsampledUsedCount[sp])
								if rem64 <= 0 {
									continue
								}
								capVal = int(min(rem64, int64(len(unreservedDropped))))
							}
						} else if matched[sp] || unsampledUsedCount[sp] > 0 {
							continue
						}
						meta := buildSettleScoreMeta(sp)
						var baseOK, baseErr int64
						if !invalidBC {
							baseOK = scoreSettlementEdgeFast(SettlementExpectation{Operation: op, ExpectError: false}, meta, profile)*2 + 10
							baseErr = scoreSettlementEdgeFast(SettlementExpectation{Operation: op, ExpectError: true}, meta, profile)*2 + 10
							cands0 = append(cands0, unlinkedSettleCandidate{
								op:      op,
								sp:      sp,
								isErr:   sp.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
								baseOK:  baseOK,
								baseErr: baseErr,
							})
							caps0 = append(caps0, capVal)
						} else {
							cands0Invalid = append(cands0Invalid, unlinkedSettleCandidate{
								op:      op,
								sp:      sp,
								isErr:   sp.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
								baseOK:  0,
								baseErr: 0,
							})
							caps0Invalid = append(caps0Invalid, capVal)
						}
					}
				}

				var candsCoal []unlinkedSettleCandidate
				var capsCoal []int
				for _, sp := range prePhase46Order {
					if len(sp.GetLinks()) == 0 || preUniqueValidLinks[sp] <= 0 {
						continue
					}
					op := inferSettleOp(sp)
					if op != "ack" && op != "nack" {
						continue
					}
					opAvail := unreservedAckRemaining
					if op == "nack" {
						opAvail = unreservedNackRemaining
					}
					if opAvail <= 0 {
						continue
					}
					batchCnt, hasBatch := AttrInt64(sp.GetAttributes(), "messaging.batch.message_count")
					if !hasBatch || batchCnt <= 0 {
						continue
					}
					rem64 := batchCnt - int64(preUniqueTargetCount[sp]) - int64(unsampledUsedCount[sp])
					if rem64 <= 0 {
						continue
					}
					capVal := int(min(rem64, int64(len(unreservedDropped))))
					meta := buildSettleScoreMeta(sp)
					candsCoal = append(candsCoal, unlinkedSettleCandidate{
						op:      op,
						sp:      sp,
						isErr:   sp.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
						baseOK:  scoreSettlementEdgeFast(SettlementExpectation{Operation: op, ExpectError: false}, meta, profile)*2 + 10,
						baseErr: scoreSettlementEdgeFast(SettlementExpectation{Operation: op, ExpectError: true}, meta, profile)*2 + 10,
					})
					capsCoal = append(capsCoal, capVal)
				}

				usedCap0 := make([]int, len(cands0))
				usedCap0Invalid := make([]int, len(cands0Invalid))
				usedCapCoal := make([]int, len(candsCoal))

				applyMatches := func(items []unreservedDroppedItem, assign []int, cands []unlinkedSettleCandidate, origIndices []int, usedCap []int) {
					for i, d := range items {
						rj := assign[i]
						if rj < 0 {
							continue
						}
						origJ := origIndices[rj]
						usedCap[origJ]++
						c := cands[rj]
						chosen := c.sp
						matched[chosen] = true
						unsampledUsedCount[chosen]++
						phase3ReservedTerminal[d.droppedIdx] = true
						if d.allowAck {
							phase3ReservedAck[d.droppedIdx] = true
							unreservedAckRemaining--
						}
						if d.allowNack {
							phase3ReservedNack[d.droppedIdx] = true
							unreservedNackRemaining--
						}
						unreservedTotalRemaining--
						recordUniqueSettlement(chosen, SettlementExpectation{
							Operation:              c.op,
							ExpectError:            d.wantErr,
							EODPartialBatchFailure: d.wantErr && chosen.GetStatusMessage() == eodPartialBatchErrorMsg,
						})
						if profile.PerMessageSettle {
							devs.add(Deviation{
								Category:    "cardinality",
								Rule:        "cardinality.per_message_settle",
								ExpectedGo:  "Batched <subID> ack/nack per RPC",
								Observed:    "One <subID> ack/nack span per message",
								SpanName:    chosen.GetName(),
								Description: "C++ TracingExactlyOnceAckHandler emits one ack/nack span per message",
							})
						}
					}
				}

				unmatchedDrops := unreservedDropped
				runSubPass := func(cands []unlinkedSettleCandidate, caps []int, usedCap []int, statusMatchOnly bool) {
					if len(unmatchedDrops) == 0 || unreservedTotalRemaining <= 0 || len(cands) == 0 {
						return
					}
					var subCands []unlinkedSettleCandidate
					var subCaps []int
					var origIdx []int
					for j, c := range cands {
						if (c.op == "ack" && unreservedAckRemaining <= 0) || (c.op == "nack" && unreservedNackRemaining <= 0) {
							continue
						}
						rem := caps[j] - usedCap[j]
						if rem <= 0 {
							continue
						}
						if rem > len(unmatchedDrops) {
							rem = len(unmatchedDrops)
						}
						subCands = append(subCands, c)
						subCaps = append(subCaps, rem)
						origIdx = append(origIdx, j)
					}
					if len(subCands) == 0 {
						return
					}
					assign := matchPhase46Pass(unmatchedDrops, subCands, subCaps, statusMatchOnly)
					applyMatches(unmatchedDrops, assign, subCands, origIdx, usedCap)
					var nextUnmatched []unreservedDroppedItem
					for i, d := range unmatchedDrops {
						if assign[i] < 0 {
							nextUnmatched = append(nextUnmatched, d)
						}
					}
					unmatchedDrops = nextUnmatched
				}

				runSubPass(cands0, caps0, usedCap0, true)
				runSubPass(candsCoal, capsCoal, usedCapCoal, true)
				runSubPass(cands0, caps0, usedCap0, false)
				runSubPass(candsCoal, capsCoal, usedCapCoal, false)
				if len(cands0Invalid) > 0 {
					runSubPass(cands0Invalid, caps0Invalid, usedCap0Invalid, true)
					runSubPass(cands0Invalid, caps0Invalid, usedCap0Invalid, false)
				}
			}
		}

		for dIdx, item := range droppedMsgs {
			if hasExplicitTerminalSettlement(item.msg.Settlements) || phase3ReservedTerminal[dIdx] {
				continue
			}
			normOutcome := normalizeSubscribeOutcome(item.msg.ExpectedOutcome)
			requiresSettlement := false
			if subExp.StrictSettlementOps {
				requiresSettlement = normOutcome == "acked" || normOutcome == "nacked" || normOutcome == "eod_failed"
			} else {
				requiresSettlement = normOutcome != "expired"
			}
			if !requiresSettlement {
				continue
			}
			spanLabel := "ack/nack"
			expectedDesc := "<subID> ack or <subID> nack"
			if subID != "" {
				spanLabel = subID + " ack"
				expectedDesc = subID + " ack or " + subID + " nack"
			}
			if normOutcome == "acked" {
				if subID != "" {
					spanLabel = subID + " ack"
				} else {
					spanLabel = "ack"
				}
				expectedDesc = spanLabel
			} else if normOutcome == "nacked" {
				if subID != "" {
					spanLabel = subID + " nack"
				} else {
					spanLabel = "nack"
				}
				expectedDesc = spanLabel
			}
			msg := fmt.Sprintf("missing terminal settlement span (%s) for dropped unsampled message[%d] (expected_outcome=%q)", expectedDesc, item.origIdx, item.msg.ExpectedOutcome)
			if subExp.StrictSettlementOps {
				msg = fmt.Sprintf("dropped unsampled message[%d] with ExpectedOutcome=%q and StrictSettlementOps=true requires explicit ack or nack SettlementExpectation", item.origIdx, item.msg.ExpectedOutcome)
				expectedDesc = "explicit ack or nack SettlementExpectation in Settlements"
			}
			failures = append(failures, Violation{
				Code:     CodeMissingSpan,
				Rule:     "subscribe.settlement.missing_span",
				Message:  msg,
				Expected: expectedDesc,
				Actual:   "<missing>",
				SpanName: spanLabel,
			})
		}
	}

	// Phase 5b: Unified link-audit replay, batch-capacity, and attribute audit across all unique settlement spans.
	phase5Order := SortSpansDeterministic(settleOrder)
	matchedSettleKeys := make(map[SpanKey]bool, len(phase5Order))
	matchedSettleByKey := make(map[SpanKey]*proxypb.TraceSpan, len(phase5Order))
	for _, sp := range phase5Order {
		sk := SpanKey{TraceID: NormalizeHex(sp.GetTraceId()), SpanID: NormalizeHex(sp.GetSpanId())}
		matchedSettleKeys[sk] = true
		matchedSettleByKey[sk] = sp

		ms := uniqueSettleSpans[sp]
		op := inferSettleOp(sp)
		if linkFails := preLinkFailures[sp]; len(linkFails) > 0 {
			failures = append(failures, linkFails...)
		}
		uniqueValidLinks := preUniqueValidLinks[sp]

		minRequired := uniqueValidLinks + unsampledUsedCount[sp]
		if batchCnt, hasBatch := AttrInt64(sp.GetAttributes(), "messaging.batch.message_count"); hasBatch {
			if minRequired > 0 && batchCnt < int64(minRequired) {
				failures = append(failures, Violation{
					Code:     CodeWrongAttributeValue,
					Rule:     "settlement." + op + ".attr.batch_count",
					Message:  fmt.Sprintf("%q messaging.batch.message_count = %d < %d (%d unique linked subscribe + %d unsampled slot(s))", sp.GetName(), batchCnt, minRequired, uniqueValidLinks, unsampledUsedCount[sp]),
					Expected: fmt.Sprintf("int64(>= %d)", minRequired),
					Actual:   fmt.Sprintf("int64(%d)", batchCnt),
					TraceID:  sp.GetTraceId(),
					SpanID:   sp.GetSpanId(),
					SpanName: sp.GetName(),
				})
			}
			if strictExtra && (op == "ack" || op == "nack") {
				unreservedOp := unreservedAckRemaining
				if op == "nack" {
					unreservedOp = unreservedNackRemaining
				}
				maxExtraAvail := unreservedOp
				if unreservedTotalRemaining < maxExtraAvail {
					maxExtraAvail = unreservedTotalRemaining
				}
				maxAllowed := minRequired + maxExtraAvail
				if batchCnt > int64(maxAllowed) {
					failures = append(failures, Violation{
						Code:     CodeWrongAttributeValue,
						Rule:     "settlement." + op + ".attr.batch_count",
						Message:  fmt.Sprintf("%q messaging.batch.message_count = %d > %d (%d unique linked subscribe + %d reserved + %d available dropped unsampled slot(s))", sp.GetName(), batchCnt, maxAllowed, uniqueValidLinks, unsampledUsedCount[sp], maxExtraAvail),
						Expected: fmt.Sprintf("int64(<= %d)", maxAllowed),
						Actual:   fmt.Sprintf("int64(%d)", batchCnt),
						TraceID:  sp.GetTraceId(),
						SpanID:   sp.GetSpanId(),
						SpanName: sp.GetName(),
					})
				}
				if batchCnt > int64(minRequired) {
					extraNeeded64 := batchCnt - int64(minRequired)
					usedExtra := maxExtraAvail
					if extraNeeded64 < int64(usedExtra) {
						usedExtra = int(extraNeeded64)
					}
					if op == "ack" {
						unreservedAckRemaining -= usedExtra
					} else {
						unreservedNackRemaining -= usedExtra
					}
					unreservedTotalRemaining -= usedExtra
				}
			}
		}

		failures = append(failures, verifySettlementSpan(sp, profile, projectID, fullSub, subID, ms.expectations, devs)...)
	}

	// Audit reverse links on every matched <subID> subscribe span for dangling, asymmetric, or unchecked duplicate links.
	for _, subSpan := range matchedSubs {
		seenSubRev := make(map[SpanKey]bool)
		reportedAsymFwd := make(map[spanLinkPair]bool)
		reportedDupRev := make(map[spanLinkPair]bool)
		for li, l := range subSpan.GetLinks() {
			if l == nil {
				continue
			}
			k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
			if settleSpan, ok := matchedSettleByKey[k]; !ok || !matchedSettleKeys[k] {
				failures = append(failures, Violation{
					Code:     CodeMissingLink,
					Rule:     "subscribe.settlement.dangling_reverse_link",
					Message:  fmt.Sprintf("reverse link[%d] on %q targets unresolved or unmatched settlement span (%s/%s)", li, subSpan.GetName(), l.GetTraceId(), l.GetSpanId()),
					Expected: "link targeting matched settlement span",
					Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
					TraceID:  subSpan.GetTraceId(),
					SpanID:   subSpan.GetSpanId(),
					SpanName: subSpan.GetName(),
				})
			} else {
				pair := spanLinkPair{sub: subSpan, settle: settleSpan}
				if !checkedReverseLinks[pair] {
					if !reportedAsymFwd[pair] {
						reportedAsymFwd[pair] = true
						failures = append(failures, Violation{
							Code:     CodeBrokenBidirectionalLink,
							Rule:     "subscribe.settlement.forward_link",
							Message:  fmt.Sprintf("%q (%s) carries reverse link[%d] to %q (%s) without matching forward SpanLink", subSpan.GetName(), subSpan.GetSpanId(), li, settleSpan.GetName(), settleSpan.GetSpanId()),
							Expected: fmt.Sprintf("forward SpanLink on %q -> %s/%s", settleSpan.GetName(), subSpan.GetTraceId(), subSpan.GetSpanId()),
							Actual:   "<missing forward link>",
							TraceID:  settleSpan.GetTraceId(),
							SpanID:   settleSpan.GetSpanId(),
							SpanName: settleSpan.GetName(),
						})
					}
					if seenSubRev[k] && !reportedDupRev[pair] {
						reportedDupRev[pair] = true
						failures = append(failures, Violation{
							Code:     CodeBrokenBidirectionalLink,
							Rule:     "subscribe.settlement.duplicate_reverse_link",
							Message:  fmt.Sprintf("duplicate reverse link[%d] on %q -> %q (%s)", li, subSpan.GetName(), settleSpan.GetName(), settleSpan.GetSpanId()),
							Expected: "at most 1 reverse SpanLink per settlement span",
							Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
							TraceID:  subSpan.GetTraceId(),
							SpanID:   subSpan.GetSpanId(),
							SpanName: subSpan.GetName(),
						})
					}
				}
			}
			seenSubRev[k] = true
		}
	}

	return failures
}

func auditExtraSettlementLinks(ms *proxypb.TraceSpan, allowedSubByKey map[SpanKey]*proxypb.TraceSpan, droppedTraceIDs map[string]bool, allowExtraValid bool) []Violation {
	var out []Violation
	seenTargets := make(map[SpanKey]bool)
	for li, l := range ms.GetLinks() {
		if l == nil {
			continue
		}
		k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
		if seenTargets[k] {
			out = append(out, Violation{
				Code:     CodeBrokenBidirectionalLink,
				Rule:     "subscribe.settlement.duplicate_link",
				Message:  fmt.Sprintf("duplicate forward link[%d] on %q targeting (%s/%s)", li, ms.GetName(), l.GetTraceId(), l.GetSpanId()),
				Expected: "unique forward link per subscribe span",
				Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
				TraceID:  ms.GetTraceId(),
				SpanID:   ms.GetSpanId(),
				SpanName: ms.GetName(),
			})
			continue
		}
		seenTargets[k] = true
		if _, ok := allowedSubByKey[k]; !ok {
			if droppedTraceIDs[k.TraceID] {
				out = append(out, Violation{
					Code:     CodeBrokenTraceparent,
					Rule:     "subscribe.unsampled_settlement.forbidden_link",
					Message:  fmt.Sprintf("%q link[%d] targets dropped unsampled trace %s", ms.GetName(), li, k.TraceID),
					Expected: "no links to dropped trace " + k.TraceID,
					Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
					TraceID:  ms.GetTraceId(),
					SpanID:   ms.GetSpanId(),
					SpanName: ms.GetName(),
				})
			} else {
				expected := "link targeting matched <subID> subscribe span"
				if allowExtraValid {
					expected = "link targeting valid sampled <subID> subscribe span"
				}
				out = append(out, Violation{
					Code:     CodeMissingLink,
					Rule:     "subscribe.settlement.dangling_link",
					Message:  fmt.Sprintf("%q link[%d] targets unresolved or non-subscribe span (%s/%s)", ms.GetName(), li, l.GetTraceId(), l.GetSpanId()),
					Expected: expected,
					Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
					TraceID:  ms.GetTraceId(),
					SpanID:   ms.GetSpanId(),
					SpanName: ms.GetName(),
				})
			}
		}
	}
	return out
}

func isStrictValidSampledSubSpan(sc *proxypb.TraceSpan, profile Profile, subID string) bool {
	if sc == nil {
		return false
	}
	if profile.ExpectedScope != "" {
		if sc.GetInstrumentationScopeName() != profile.ExpectedScope {
			return false
		}
	} else if !isCandidatePubSubSpan(sc, profile) {
		return false
	}
	if profile.SubscribeSpanKind != proxypb.SpanKind_SPAN_KIND_UNSPECIFIED && sc.GetKind() != profile.SubscribeSpanKind {
		return false
	}
	if (sc.GetTraceFlags() & 0x01) == 0 {
		return false
	}
	msgSys, ok := AttrString(sc.GetAttributes(), "messaging.system")
	if !ok || msgSys != "gcp_pubsub" {
		return false
	}
	dst, ok := AttrString(sc.GetAttributes(), "messaging.destination.name")
	if !ok {
		return false
	}
	if subID != "" {
		return dst == subID
	}
	return dst != ""
}

func isDroppedSubscribeSubtreeSpan(s *proxypb.TraceSpan, subID string, profile Profile) bool {
	if s == nil || !isCandidatePubSubSpan(s, profile) {
		return false
	}
	name := s.GetName()
	switch name {
	case "subscriber concurrency control", "subscribe scheduler", "subscriber scheduler":
		return true
	}
	if subID != "" {
		return name == subID+" subscribe" || name == subID+" process"
	}
	return (strings.HasSuffix(name, " subscribe") && len(name) > len(" subscribe")) ||
		(strings.HasSuffix(name, " process") && len(name) > len(" process"))
}

func expectedSettlementOp(m SubscribeMessageExpectation) string {
	for i := len(m.Settlements) - 1; i >= 0; i-- {
		op := strings.ToLower(strings.TrimSpace(m.Settlements[i].Operation))
		if op == "ack" || op == "nack" {
			return op
		}
	}
	return ""
}

func verifySubscribeAttributes(s *proxypb.TraceSpan, profile Profile, projectID, fullSub, subID string, m SubscribeMessageExpectation, isEOD bool, devs *deviationCollector) []Violation {
	var out []Violation
	out = append(out, checkCommonAttrs(s, profile, projectID, fullSub, subID, devs, "subscribe")...)

	wantMsgID := MatchStringNonEmpty()
	if m.MessageID != "" {
		wantMsgID = MatchString(m.MessageID)
	}
	out = append(out, checkAttrV(s, "messaging.message.id", wantMsgID, "subscribe.attr.message_id")...)

	wantAckID := MatchStringNonEmpty()
	if m.AckID != "" {
		wantAckID = MatchString(m.AckID)
	}
	out = append(out, checkAttrV(s, "messaging.gcp_pubsub.message.ack_id", wantAckID, "subscribe.attr.ack_id")...)

	if !profile.UseEnvelopeSize {
		wantBody := MatchAnyInt64()
		if m.BodySize != nil {
			wantBody = MatchInt64(*m.BodySize)
		} else if m.Data != nil {
			wantBody = MatchInt64(int64(len(m.Data)))
		}
		out = append(out, checkAttrV(s, "messaging.message.body.size", wantBody, "subscribe.attr.body_size")...)
	} else {
		wantEnv := MatchAnyInt64()
		if m.EnvelopeSize != nil {
			wantEnv = MatchInt64(*m.EnvelopeSize)
		} else if m.Data != nil {
			wantEnv = MatchInt64Min(cppEnvelopeSize(m.Data, m.Attributes, m.OrderingKey, m.MessageID))
		} else if m.BodySize != nil {
			wantEnv = MatchInt64Min(20 + *m.BodySize)
		}
		out = append(out, checkAttrV(s, "messaging.message.envelope.size", wantEnv, "subscribe.attr.envelope_size")...)
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.envelope_size_vs_body_size",
			ExpectedGo:  "messaging.message.body.size",
			Observed:    "messaging.message.envelope.size",
			SpanName:    s.GetName(),
			Description: "C++ records messaging.message.envelope.size instead of messaging.message.body.size",
		})
	}

	if profile.RequireUnconditionalOrderingKey || m.OrderingKey != "" {
		out = append(out, checkAttrV(s, "messaging.gcp_pubsub.message.ordering_key", MatchString(m.OrderingKey), "subscribe.attr.ordering_key")...)
	} else if _, ok := AttrString(s.GetAttributes(), "messaging.gcp_pubsub.message.ordering_key"); !ok {
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.omits_empty_ordering_key",
			ExpectedGo:  `messaging.gcp_pubsub.message.ordering_key=""`,
			Observed:    "omitted when empty",
			SpanName:    s.GetName(),
			Description: "C++ omits messaging.gcp_pubsub.message.ordering_key when ordering_key is empty",
		})
	}

	wantEOS := MatchAnyBool()
	if m.ExactlyOnce != nil {
		wantEOS = MatchBool(*m.ExactlyOnce)
	} else if normalizeSubscribeOutcome(m.ExpectedOutcome) == "eod_failed" {
		wantEOS = MatchBool(true)
	}
	out = append(out, checkAttrV(s, profile.EOSSubscribeAttrKey, wantEOS, "subscribe.attr.exactly_once_delivery")...)
	if profile.Name == ProfileCPP && profile.EOSSubscribeAttrKey != "messaging.gcp_pubsub.exactly_once_delivery" {
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.eos_key_name",
			ExpectedGo:  "messaging.gcp_pubsub.exactly_once_delivery",
			Observed:    profile.EOSSubscribeAttrKey,
			SpanName:    s.GetName(),
			Description: "C++ names the EOD boolean messaging.gcp_pubsub.subscription.exactly_once_delivery",
		})
	}

	if profile.Name == ProfileGo {
		wantBatch := MatchInt64Min(1)
		if m.BatchCount > 0 {
			wantBatch = MatchInt64(int64(m.BatchCount))
		}
		out = append(out, checkAttrV(s, "messaging.batch.message_count", wantBatch, "subscribe.attr.batch_count")...)
		out = append(out, checkAttrV(s, "code.function", MatchString("receive"), "subscribe.attr.code_function")...)
		if m.DeliveryAttempt > 0 {
			out = append(out, checkAttrV(s, "messaging.gcp_pubsub.message.delivery_attempt", MatchInt64(int64(m.DeliveryAttempt)), "subscribe.attr.delivery_attempt")...)
		} else {
			out = append(out, checkAttrAbsentV(s, "messaging.gcp_pubsub.message.delivery_attempt", "subscribe.attr.delivery_attempt_absent")...)
		}
	} else {
		out = append(out, checkAttrV(s, "messaging.operation.type", MatchString("subscribe"), "subscribe.attr.operation_type")...)
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.cpp_extra_semconv",
			ExpectedGo:  "omits messaging.operation.type on subscribe",
			Observed:    `messaging.operation.type="subscribe"`,
			SpanName:    s.GetName(),
			Description: "C++ sets messaging.operation.type=subscribe on <subID> subscribe",
		})
	}

	// Result outcome check.
	outcome := expectedSubscribeOutcome(m, isEOD)
	switch outcome {
	case "acked", "nacked", "expired":
		wantVal := profile.ResultValues[outcome]
		if wantVal == "" {
			wantVal = outcome
		}
		out = append(out, checkAttrV(s, "messaging.gcp_pubsub.result", MatchString(wantVal), "subscribe.attr.result")...)
		if profile.Name == ProfileCPP && wantVal != outcome {
			devs.add(Deviation{
				Category:    "attribute",
				Rule:        "attr.result_value_form",
				ExpectedGo:  fmt.Sprintf("%q", outcome),
				Observed:    fmt.Sprintf("%q", wantVal),
				SpanName:    s.GetName(),
				Description: `C++ sets messaging.gcp_pubsub.result to "ack"/"nack" instead of "acked"/"nacked"`,
			})
		}
	case "eod_failed":
		if profile.Name == ProfileGo {
			out = append(out, checkAttrAbsentV(s, "messaging.gcp_pubsub.result", "subscribe.attr.result_omitted_on_eod_fail")...)
		} else if profile.Name == ProfileCPP {
			// C++ TracingBatchCallback sets result="ack"/"nack" in AckEnd/NackEnd even on EOD failure.
			matcher := MatchStringOneOf("ack", "nack")
			observed := `"ack"/"nack"`
			if op := expectedSettlementOp(m); op == "ack" || op == "nack" {
				matcher = MatchString(op)
				observed = fmt.Sprintf("%q", op)
			}
			out = append(out, checkAttrV(s, "messaging.gcp_pubsub.result", matcher, "subscribe.attr.result")...)
			devs.add(Deviation{
				Category:    "attribute",
				Rule:        "attr.result_set_on_eod_fail",
				ExpectedGo:  "omits messaging.gcp_pubsub.result on EOD failure",
				Observed:    observed,
				SpanName:    s.GetName(),
				Description: "C++ TracingBatchCallback sets messaging.gcp_pubsub.result in AckEnd/NackEnd even when EOD settlement fails",
			})
		}
	}

	for k, matcher := range m.RequireAttributes {
		out = append(out, checkAttrV(s, k, matcher, "subscribe.attr.custom."+k)...)
	}

	return out
}

func expectedSubscribeOutcome(m SubscribeMessageExpectation, isEOD bool) string {
	if norm := normalizeSubscribeOutcome(m.ExpectedOutcome); norm != "" {
		return norm
	}
	for i := len(m.Settlements) - 1; i >= 0; i-- {
		se := m.Settlements[i]
		op := strings.ToLower(strings.TrimSpace(se.Operation))
		if op == "ack" || op == "nack" {
			if se.ExpectError && isEOD {
				return "eod_failed"
			}
			if op == "ack" {
				return "acked"
			}
			return "nacked"
		}
	}
	return ""
}

func isSubscribeEODFailed(m SubscribeMessageExpectation, isEOD bool) bool {
	return expectedSubscribeOutcome(m, isEOD) == "eod_failed"
}

func verifySubscribeChildControl(c, subSpan *proxypb.TraceSpan, profile Profile, rulePrefix string, devs *deviationCollector) []Violation {
	var out []Violation
	out = append(out, checkSpanScope(c, profile, rulePrefix+".scope", devs)...)
	out = append(out, checkChildOfV(c, subSpan, rulePrefix+".parent")...)
	out = append(out, checkSpanKindV(c, proxypb.SpanKind_SPAN_KIND_INTERNAL, rulePrefix+".span_kind")...)
	if profile.ExpectZeroChildAttrs {
		out = append(out, checkZeroAttributesV(c, rulePrefix+".zero_attrs")...)
	} else {
		out = append(out, checkAttrV(c, "messaging.system", MatchString("gcp_pubsub"), rulePrefix+".attr.messaging_system")...)
	}
	out = append(out, checkStatusV(c, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", rulePrefix+".status")...)
	return out
}

func verifyProcessSpan(ps, subSpan *proxypb.TraceSpan, profile Profile, projectID, fullSub, subID string, m SubscribeMessageExpectation, inferredOp string, devs *deviationCollector) []Violation {
	var out []Violation
	out = append(out, checkSpanScope(ps, profile, "subscribe.process.scope", devs)...)
	out = append(out, checkChildOfV(ps, subSpan, "subscribe.process.parent")...)

	if ps.GetKind() != profile.ProcessSpanKind {
		out = append(out, checkSpanKindV(ps, profile.ProcessSpanKind, "subscribe.process.span_kind")...)
	} else if profile.Name == ProfileCPP && ps.GetKind() != proxypb.SpanKind_SPAN_KIND_CONSUMER {
		devs.add(Deviation{
			Category:    "span_kind",
			Rule:        "span_kind.process",
			ExpectedGo:  proxypb.SpanKind_SPAN_KIND_CONSUMER.String(),
			Observed:    ps.GetKind().String(),
			SpanName:    ps.GetName(),
			Description: "C++ <subID> process uses SPAN_KIND_INTERNAL instead of SPAN_KIND_CONSUMER",
		})
	}

	if m.CallbackTraceparent != "" {
		if err := AssertTraceparentMatchesSpan(m.CallbackTraceparent, ps); err != nil {
			out = append(out, Violation{
				Code:     CodeBrokenTraceparent,
				Rule:     "subscribe.process.callback_traceparent",
				Message:  err.Error(),
				Expected: fmt.Sprintf("trace_id=%s span_id=%s", ps.GetTraceId(), ps.GetSpanId()),
				Actual:   m.CallbackTraceparent,
				TraceID:  ps.GetTraceId(),
				SpanID:   ps.GetSpanId(),
				SpanName: ps.GetName(),
			})
		}
	}

	if profile.ProcessMinimalAttributes {
		out = append(out, checkAttrV(ps, "messaging.system", MatchString("gcp_pubsub"), "subscribe.process.attr.messaging_system")...)
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.process_minimal",
			ExpectedGo:  "full process attributes + messaging.operation.type=process",
			Observed:    "only messaging.system=gcp_pubsub",
			SpanName:    ps.GetName(),
			Description: "C++ TracingMessageCallback sets only messaging.system on <subID> process",
		})
	} else {
		out = append(out, checkCommonAttrs(ps, profile, projectID, fullSub, subID, devs, "subscribe.process")...)
		wantMsgID := MatchStringNonEmpty()
		if m.MessageID != "" {
			wantMsgID = MatchString(m.MessageID)
		}
		out = append(out, checkAttrV(ps, "messaging.message.id", wantMsgID, "subscribe.process.attr.message_id")...)
		wantBody := MatchAnyInt64()
		if m.BodySize != nil {
			wantBody = MatchInt64(*m.BodySize)
		} else if m.Data != nil {
			wantBody = MatchInt64(int64(len(m.Data)))
		}
		out = append(out, checkAttrV(ps, "messaging.message.body.size", wantBody, "subscribe.process.attr.body_size")...)
		out = append(out, checkAttrV(ps, "messaging.gcp_pubsub.message.ordering_key", MatchString(m.OrderingKey), "subscribe.process.attr.ordering_key")...)
		if m.DeliveryAttempt > 0 {
			out = append(out, checkAttrV(ps, "messaging.gcp_pubsub.message.delivery_attempt", MatchInt64(int64(m.DeliveryAttempt)), "subscribe.process.attr.delivery_attempt")...)
		} else {
			out = append(out, checkAttrAbsentV(ps, "messaging.gcp_pubsub.message.delivery_attempt", "subscribe.process.attr.delivery_attempt_absent")...)
		}
		out = append(out, checkAttrV(ps, "messaging.operation.type", MatchString("process"), "subscribe.process.attr.operation_type")...)
	}

	expectSettleEvents := m.InterruptKind != SubscribeInterruptForcedShutdownNack && m.InterruptKind != SubscribeInterruptPreDispatchCancel
	if m.ExpectProcessSettlementEvent != nil {
		expectSettleEvents = *m.ExpectProcessSettlementEvent
	}
	if profile.ProcessEventsEnabled {
		if !expectSettleEvents {
			out = append(out, checkEventAbsentV(ps, "ack called", "subscribe.process.event.unexpected_ack_called")...)
			out = append(out, checkEventAbsentV(ps, "nack called", "subscribe.process.event.unexpected_nack_called")...)
		} else {
			wantAck := false
			wantNack := false
			for _, se := range m.Settlements {
				op := strings.ToLower(strings.TrimSpace(se.Operation))
				if op == "ack" {
					wantAck = true
				} else if op == "nack" {
					wantNack = true
				}
			}
			normOutcome := normalizeSubscribeOutcome(m.ExpectedOutcome)
			if !wantAck && !wantNack {
				switch normOutcome {
				case "acked":
					wantAck = true
				case "nacked":
					wantNack = true
				case "eod_failed":
					if inferredOp == "ack" {
						wantAck = true
					} else if inferredOp == "nack" {
						wantNack = true
					}
				}
			}
			if wantAck {
				out = append(out, checkEventV(ps, "ack called", "subscribe.process.event.ack_called")...)
				if !wantNack {
					out = append(out, checkEventAbsentV(ps, "nack called", "subscribe.process.event.unexpected_nack_called")...)
				}
			}
			if wantNack {
				out = append(out, checkEventV(ps, "nack called", "subscribe.process.event.nack_called")...)
				if !wantAck {
					out = append(out, checkEventAbsentV(ps, "ack called", "subscribe.process.event.unexpected_ack_called")...)
				}
			}
			if !wantAck && !wantNack {
				hasAckCalled := AssertEvent(ps, "ack called") == nil
				hasNackCalled := AssertEvent(ps, "nack called") == nil
				if inferredOp == "nack" && hasAckCalled {
					out = append(out, checkEventAbsentV(ps, "ack called", "subscribe.process.event.unexpected_ack_called")...)
				} else if (hasAckCalled && hasNackCalled) || (inferredOp == "ack" && hasNackCalled) {
					out = append(out, checkEventAbsentV(ps, "nack called", "subscribe.process.event.unexpected_nack_called")...)
				} else if normOutcome == "eod_failed" && !hasAckCalled && !hasNackCalled {
					out = append(out, Violation{
						Code:     CodeMissingEvent,
						Rule:     "subscribe.process.event.settlement_called",
						Message:  fmt.Sprintf("process span %q (%s) with ExpectedOutcome=%q is missing \"ack called\" or \"nack called\" event", ps.GetName(), ps.GetSpanId(), m.ExpectedOutcome),
						Expected: `"ack called" or "nack called"`,
						Actual:   "<missing>",
						TraceID:  ps.GetTraceId(),
						SpanID:   ps.GetSpanId(),
						SpanName: ps.GetName(),
					})
				}
			}
		}
	} else {
		devs.add(Deviation{
			Category:    "event",
			Rule:        "events.missing_ack_nack_called",
			ExpectedGo:  `"ack called" / "nack called" on <subID> process`,
			Observed:    "no events on <subID> process",
			SpanName:    ps.GetName(),
			Description: "C++ ends <subID> process on callback return without ack/nack called events",
		})
	}

	out = append(out, checkStatusV(ps, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "subscribe.process.status")...)
	return out
}

func verifySubscribeSettlementEvents(subSpan *proxypb.TraceSpan, profile Profile, se SettlementExpectation, isEOD bool, devs *deviationCollector) []Violation {
	_ = isEOD
	op := strings.ToLower(strings.TrimSpace(se.Operation))
	if op != "ack" && op != "nack" && op != "modack" {
		return nil
	}
	if profile.Name == ProfileGo {
		var out []Violation
		out = append(out, checkEventV(subSpan, op+" start", "subscribe.event."+op+"_start",
			CheckAttr("messaging.batch.message_count", MatchInt64Min(1)))...)
		out = append(out, checkEventV(subSpan, op+" end", "subscribe.event."+op+"_end")...)
		return out
	}

	// C++ events on subscribe.
	devs.add(Deviation{
		Category:    "event",
		Rule:        "events.cpp_subscribe_names",
		ExpectedGo:  fmt.Sprintf("%q / %q", op+" start", op+" end"),
		Observed:    fmt.Sprintf("%q / %q", "gl-cpp."+op+"_start", "gl-cpp."+op+"_end"),
		SpanName:    subSpan.GetName(),
		Description: "C++ uses gl-cpp.* event naming on <subID> subscribe",
	})
	var out []Violation
	if op == "ack" || op == "nack" {
		out = append(out, checkEventV(subSpan, "gl-cpp."+op+"_start", "subscribe.event.cpp_"+op+"_start")...)
		out = append(out, checkEventV(subSpan, "gl-cpp."+op+"_end", "subscribe.event.cpp_"+op+"_end")...)
	} else {
		out = append(out, checkEventV(subSpan, "gl-cpp.modack_start", "subscribe.event.cpp_modack_start")...)
		out = append(out, checkEventV(subSpan, "gl-cpp.modack_end", "subscribe.event.cpp_modack_end")...)
	}
	return out
}

func verifySettlementSpan(sp *proxypb.TraceSpan, profile Profile, projectID, fullSub, subID string, expectations []SettlementExpectation, devs *deviationCollector) []Violation {
	var out []Violation
	op := ""
	if len(expectations) > 0 {
		op = strings.ToLower(strings.TrimSpace(expectations[0].Operation))
	}
	if op == "" {
		switch {
		case strings.HasSuffix(sp.GetName(), " ack"):
			op = "ack"
		case strings.HasSuffix(sp.GetName(), " nack"):
			op = "nack"
		case strings.HasSuffix(sp.GetName(), " modack"):
			op = "modack"
		}
	}
	effSubID := subID
	if effSubID == "" && strings.HasSuffix(sp.GetName(), " "+op) {
		effSubID = strings.TrimSuffix(sp.GetName(), " "+op)
	}

	var batchCounts []int
	seenBatch := make(map[int]bool)
	var deadlines []int32
	seenDeadline := make(map[int32]bool)
	var receiptFlags []bool
	seenReceipt := make(map[bool]bool)
	var errSubstrings []string
	seenErrSub := make(map[string]bool)
	anyExpectError := false
	requireException := false

	for _, se := range expectations {
		if se.ExpectedBatchCount > 0 && !seenBatch[se.ExpectedBatchCount] {
			seenBatch[se.ExpectedBatchCount] = true
			batchCounts = append(batchCounts, se.ExpectedBatchCount)
		}
		if se.AckDeadlineSeconds > 0 && !seenDeadline[se.AckDeadlineSeconds] {
			seenDeadline[se.AckDeadlineSeconds] = true
			deadlines = append(deadlines, se.AckDeadlineSeconds)
		}
		if se.IsReceiptModack != nil && !seenReceipt[*se.IsReceiptModack] {
			seenReceipt[*se.IsReceiptModack] = true
			receiptFlags = append(receiptFlags, *se.IsReceiptModack)
		}
		if se.ExpectError {
			anyExpectError = true
			if !se.EODPartialBatchFailure {
				requireException = true
			}
			if se.ErrorSubstring != "" && !seenErrSub[se.ErrorSubstring] {
				seenErrSub[se.ErrorSubstring] = true
				errSubstrings = append(errSubstrings, se.ErrorSubstring)
			}
		}
	}

	out = append(out, checkSpanScope(sp, profile, "settlement."+op+".scope", devs)...)
	out = append(out, checkRootSpanV(sp, "settlement."+op+".root")...)

	wantKind := proxypb.SpanKind_SPAN_KIND_INTERNAL
	if op == "modack" {
		wantKind = profile.ModAckSpanKind
	}
	if sp.GetKind() != wantKind {
		out = append(out, checkSpanKindV(sp, wantKind, "settlement."+op+".span_kind")...)
	} else if profile.Name == ProfileCPP && op == "modack" && wantKind != proxypb.SpanKind_SPAN_KIND_INTERNAL {
		devs.add(Deviation{
			Category:    "span_kind",
			Rule:        "span_kind.modack",
			ExpectedGo:  proxypb.SpanKind_SPAN_KIND_INTERNAL.String(),
			Observed:    sp.GetKind().String(),
			SpanName:    sp.GetName(),
			Description: "C++ <subID> modack uses SPAN_KIND_CLIENT instead of SPAN_KIND_INTERNAL",
		})
	}

	out = append(out, checkCommonAttrs(sp, profile, projectID, fullSub, effSubID, devs, "settlement."+op)...)

	if profile.Name == ProfileGo {
		if len(batchCounts) == 0 {
			out = append(out, checkAttrV(sp, "messaging.batch.message_count", MatchInt64Min(1), "settlement."+op+".attr.batch_count")...)
		} else {
			for _, bc := range batchCounts {
				out = append(out, checkAttrV(sp, "messaging.batch.message_count", MatchInt64(int64(bc)), "settlement."+op+".attr.batch_count")...)
			}
		}
		switch op {
		case "ack":
			out = append(out, checkAttrV(sp, "code.function", MatchString("sendAck"), "settlement.ack.attr.code_function")...)
		case "modack":
			out = append(out, checkAttrV(sp, "code.function", MatchString("sendModAck"), "settlement.modack.attr.code_function")...)
			if len(deadlines) == 0 {
				out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.message.ack_deadline", MatchInt64Min(1), "settlement.modack.attr.ack_deadline")...)
			} else {
				for _, dl := range deadlines {
					out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.message.ack_deadline", MatchInt64(int64(dl)), "settlement.modack.attr.ack_deadline")...)
				}
			}
			if len(receiptFlags) == 0 {
				out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.is_receipt_modack", MatchAnyBool(), "settlement.modack.attr.is_receipt_modack")...)
			} else {
				for _, rf := range receiptFlags {
					out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.is_receipt_modack", MatchBool(rf), "settlement.modack.attr.is_receipt_modack")...)
				}
			}
		case "nack":
			out = append(out, checkAttrV(sp, "code.function", MatchString("sendModAck"), "settlement.nack.attr.code_function")...)
			out = append(out, checkAttrAbsentV(sp, "messaging.gcp_pubsub.message.ack_deadline", "settlement.nack.attr.ack_deadline_omitted")...)
			out = append(out, checkAttrAbsentV(sp, "messaging.gcp_pubsub.is_receipt_modack", "settlement.nack.attr.is_receipt_modack_omitted")...)
		}

		if anyExpectError {
			if len(errSubstrings) == 0 {
				out = append(out, checkStatusV(sp, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, "", "settlement."+op+".status")...)
			} else {
				for _, sub := range errSubstrings {
					out = append(out, checkStatusV(sp, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, sub, "settlement."+op+".status")...)
				}
			}
			if requireException {
				out = append(out, checkEventV(sp, "exception", "settlement."+op+".event.exception")...)
			}
		} else {
			out = append(out, checkStatusV(sp, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "settlement."+op+".status")...)
		}
		return out
	}

	// ProfileCpp settlement verification.
	switch op {
	case "ack", "nack":
		out = append(out, checkAttrV(sp, profile.CodeFunctionAttrKey, MatchString("pubsub::AckHandler::"+op), "settlement."+op+".attr.code_function_name")...)
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.code_function_key",
			ExpectedGo:  "code.function",
			Observed:    profile.CodeFunctionAttrKey,
			SpanName:    sp.GetName(),
			Description: "C++ uses code.function.name instead of code.function",
		})
		out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.message.ack_id", MatchStringNonEmpty(), "settlement."+op+".attr.ack_id")...)
		if fullSub != "" {
			out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.subscription.template", MatchString(fullSub), "settlement."+op+".attr.subscription_template")...)
		} else {
			out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.subscription.template", MatchStringNonEmpty(), "settlement."+op+".attr.subscription_template")...)
		}
		out = append(out, checkAttrV(sp, "messaging.operation.type", MatchString("settle"), "settlement."+op+".attr.operation_type")...)
		out = append(out, checkAttrV(sp, "gl-cpp.status_code", MatchStringNonEmpty(), "settlement."+op+".attr.gl_cpp_status_code")...)
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.cpp_extra_semconv",
			ExpectedGo:  "omits subscription.template and messaging.operation.type=settle",
			Observed:    "sets subscription.template, messaging.operation.type=settle, gl-cpp.status_code",
			SpanName:    sp.GetName(),
			Description: "C++ adds subscription.template, operation.type=settle, and gl-cpp.status_code on ack/nack",
		})
		if anyExpectError {
			if len(errSubstrings) == 0 {
				out = append(out, checkStatusV(sp, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, "", "settlement."+op+".status")...)
			} else {
				for _, sub := range errSubstrings {
					out = append(out, checkStatusV(sp, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, sub, "settlement."+op+".status")...)
				}
			}
		} else {
			out = append(out, checkStatusV(sp, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, false, "", "settlement."+op+".status")...)
		}
	case "modack":
		out = append(out, checkAttrV(sp, "messaging.operation.type", MatchString("extend"), "settlement.modack.attr.operation_type")...)
		if len(batchCounts) == 0 {
			out = append(out, checkAttrV(sp, "messaging.batch.message_count", MatchInt64Min(1), "settlement.modack.attr.batch_count")...)
		} else {
			for _, bc := range batchCounts {
				out = append(out, checkAttrV(sp, "messaging.batch.message_count", MatchInt64(int64(bc)), "settlement.modack.attr.batch_count")...)
			}
		}
		if len(deadlines) == 0 {
			out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.message.ack_deadline_seconds", MatchInt64Min(1), "settlement.modack.attr.ack_deadline_seconds")...)
		} else {
			for _, dl := range deadlines {
				out = append(out, checkAttrV(sp, "messaging.gcp_pubsub.message.ack_deadline_seconds", MatchInt64(int64(dl)), "settlement.modack.attr.ack_deadline_seconds")...)
			}
		}
		devs.add(Deviation{
			Category:    "attribute",
			Rule:        "attr.modack_deadline_seconds",
			ExpectedGo:  "messaging.gcp_pubsub.message.ack_deadline + is_receipt_modack",
			Observed:    "messaging.gcp_pubsub.message.ack_deadline_seconds (no is_receipt_modack)",
			SpanName:    sp.GetName(),
			Description: "C++ uses ack_deadline_seconds and omits is_receipt_modack on <subID> modack",
		})
		out = append(out, checkStatusV(sp, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, false, "", "settlement.modack.status")...)
		if anyExpectError && profile.AllowModAckUnsetOnError {
			devs.add(Deviation{
				Category:    "status",
				Rule:        "status.modack_unset",
				ExpectedGo:  "SPAN_STATUS_CODE_ERROR on modack failure",
				Observed:    "SPAN_STATUS_CODE_UNSET",
				SpanName:    sp.GetName(),
				Description: "C++ EndModackSpan ends <subID> modack with UNSET even when RPC fails",
			})
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Unary Pull Verification (C++ Only)
// ---------------------------------------------------------------------------

func normalizeUnaryPullSettlementOp(op string) string {
	return strings.ToLower(strings.TrimSpace(op))
}

func isValidUnaryPullSettlementOp(op string) bool {
	return op == "ack" || op == "nack" || op == "modack"
}

func unaryPullOpIndex(op string) int {
	switch op {
	case "ack":
		return 0
	case "nack":
		return 1
	case "modack":
		return 2
	default:
		return -1
	}
}

// collectUnaryPullMessageSettlements resolves expected settlements for message msgIdx.
// Rules:
// - Settlement != nil && len(Settlements) == 0 -> [*Settlement] (100% backwards-compatible)
// - Settlement == nil && len(Settlements) > 0  -> copy(Settlements)
// - Both set                                  -> Violation(CodeCustomCheckFailed, "unary_pull.settlement.conflict")
// - Any op not in {"ack","nack","modack"}     -> Violation(CodeCustomCheckFailed, "unary_pull.settlement.invalid_operation")
func collectUnaryPullMessageSettlements(msgIdx int, m UnaryPullMessageExpectation) ([]SettlementExpectation, []Violation) {
	if m.Settlement != nil && len(m.Settlements) > 0 {
		return nil, []Violation{{
			Code:     CodeCustomCheckFailed,
			Rule:     "unary_pull.settlement.conflict",
			Message:  fmt.Sprintf("UnaryPullExpectation.Messages[%d] sets both Settlement and Settlements; use Settlements for multiple settlements", msgIdx),
			Expected: "at most one of Settlement or Settlements",
			Actual:   fmt.Sprintf("Settlement=%+v, len(Settlements)=%d", *m.Settlement, len(m.Settlements)),
		}}
	}
	var raw []SettlementExpectation
	if len(m.Settlements) > 0 {
		raw = make([]SettlementExpectation, len(m.Settlements))
		copy(raw, m.Settlements)
	} else if m.Settlement != nil {
		raw = []SettlementExpectation{*m.Settlement}
	}
	var outErr []Violation
	for si, se := range raw {
		op := normalizeUnaryPullSettlementOp(se.Operation)
		if !isValidUnaryPullSettlementOp(op) {
			outErr = append(outErr, Violation{
				Code:     CodeCustomCheckFailed,
				Rule:     "unary_pull.settlement.invalid_operation",
				Message:  fmt.Sprintf("UnaryPullExpectation.Messages[%d] settlement[%d] has invalid Operation %q", msgIdx, si, se.Operation),
				Expected: `"ack", "nack", or "modack"`,
				Actual:   fmt.Sprintf("%q", se.Operation),
			})
		}
	}
	if len(outErr) > 0 {
		return nil, outErr
	}
	return raw, nil
}

type unaryPullRecvMeta struct {
	effSubID       string
	normTraceID    string
	normSpanID     string
	msgID          string
	hasMsgID       bool
	orderingKey    string
	hasOrderingKey bool
	envSize        int64
	hasEnvSize     bool
	isUnlinked     bool
	linkKeys       []SpanKey
	hasCreateAttr  bool
	createAttrKey  SpanKey
}

func (rm *unaryPullRecvMeta) hasCreateLink(traceID, spanID string) bool {
	for _, k := range rm.linkKeys {
		if k.TraceID == traceID && k.SpanID == spanID {
			return true
		}
	}
	return rm.hasCreateAttr && rm.createAttrKey.TraceID == traceID && rm.createAttrKey.SpanID == spanID
}

func buildUnaryPullRecvMeta(subID string, c *proxypb.TraceSpan) unaryPullRecvMeta {
	effSubID := subID
	if effSubID == "" && strings.HasSuffix(c.GetName(), " receive") {
		effSubID = strings.TrimSuffix(c.GetName(), " receive")
	}
	attrs := c.GetAttributes()
	msgID, hasMsgID := AttrString(attrs, "messaging.message.id")
	orderingKey, hasOrderingKey := AttrString(attrs, "messaging.gcp_pubsub.message.ordering_key")
	envSize, hasEnvSize := AttrInt64(attrs, "messaging.message.envelope.size")

	gotT, okT := AttrString(attrs, "gcp_pubsub.create.trace_id")
	gotS, okS := AttrString(attrs, "gcp_pubsub.create.span_id")
	links := c.GetLinks()
	var linkKeys []SpanKey
	if len(links) > 0 {
		linkKeys = make([]SpanKey, 0, len(links))
		for _, l := range links {
			if l != nil {
				linkKeys = append(linkKeys, SpanKey{
					TraceID: NormalizeHex(l.GetTraceId()),
					SpanID:  NormalizeHex(l.GetSpanId()),
				})
			}
		}
	}
	var createAttrKey SpanKey
	hasCreateAttr := okT && okS
	if hasCreateAttr {
		createAttrKey = SpanKey{TraceID: NormalizeHex(gotT), SpanID: NormalizeHex(gotS)}
	}
	return unaryPullRecvMeta{
		effSubID:       effSubID,
		normTraceID:    NormalizeHex(c.GetTraceId()),
		normSpanID:     NormalizeHex(c.GetSpanId()),
		msgID:          msgID,
		hasMsgID:       hasMsgID,
		orderingKey:    orderingKey,
		hasOrderingKey: hasOrderingKey,
		envSize:        envSize,
		hasEnvSize:     hasEnvSize,
		isUnlinked:     len(links) == 0 && !okT && !okS,
		linkKeys:       linkKeys,
		hasCreateAttr:  hasCreateAttr,
		createAttrKey:  createAttrKey,
	}
}

type unaryPullSettleMeta struct {
	ackID       string
	hasAckID    bool
	isErr       bool
	statusMsg   string
	deadline    int64
	hasDeadline bool
	hasDA       bool
	da          int64
}

type unaryPullMsgScoreMeta struct {
	msg           UnaryPullMessageExpectation
	wantSettles   []SettlementExpectation
	opGroups      [3][]SettlementExpectation
	hasWireTP     bool
	wireTPSampled bool
	parsedWTP     Traceparent
	wantEnv       int64
	hasWantEnv    bool
	exactEnv      bool
}

func buildUnaryPullMsgScoreMeta(m UnaryPullMessageExpectation, wantSettles []SettlementExpectation) unaryPullMsgScoreMeta {
	pm := unaryPullMsgScoreMeta{
		msg:         m,
		wantSettles: wantSettles,
		hasWireTP:   m.WireTraceparent != "",
	}
	for _, se := range wantSettles {
		if opIdx := unaryPullOpIndex(normalizeUnaryPullSettlementOp(se.Operation)); opIdx >= 0 {
			pm.opGroups[opIdx] = append(pm.opGroups[opIdx], se)
		}
	}
	if pm.hasWireTP {
		if tp, err := ParseTraceparent(m.WireTraceparent); err == nil && tp.IsSampled() {
			pm.wireTPSampled = true
			pm.parsedWTP = tp
		}
	}
	if m.EnvelopeSize != nil {
		pm.wantEnv = *m.EnvelopeSize
		pm.hasWantEnv = true
		pm.exactEnv = true
	} else if m.Data != nil {
		pm.wantEnv = cppEnvelopeSize(m.Data, m.Attributes, m.OrderingKey, m.MessageID)
		pm.hasWantEnv = true
	} else if m.BodySize != nil {
		pm.wantEnv = 20 + *m.BodySize
		pm.hasWantEnv = true
	}
	return pm
}

func scoreUnaryPullReceiveEdgeFast(
	pm unaryPullMsgScoreMeta,
	rm unaryPullRecvMeta,
	linkedSettle [][]unaryPullSettleMeta,
	rj int,
) (baseScore int64, rawSettle int64) {
	var score int64
	m := pm.msg

	if pm.hasWireTP {
		if pm.wireTPSampled {
			if rm.hasCreateLink(pm.parsedWTP.TraceID, pm.parsedWTP.SpanID) {
				score += 1_200_000
			}
		} else if rm.isUnlinked {
			score += 50_000
		}
	}
	if m.MessageID != "" && rm.hasMsgID && rm.msgID == m.MessageID {
		score += 600_000
	}
	var hasAckIDMatch bool
	var totalSettleBonus int64
	if len(pm.wantSettles) > 0 && rm.effSubID != "" && len(linkedSettle) > 0 {
		rowBase := rj * 3
		for opIdx := 0; opIdx < 3; opIdx++ {
			wantOp := pm.opGroups[opIdx]
			if len(wantOp) == 0 {
				continue
			}
			cands := linkedSettle[rowBase+opIdx]
			if m.AckID != "" && !hasAckIDMatch {
				for _, spm := range cands {
					if spm.hasAckID && spm.ackID == m.AckID {
						hasAckIDMatch = true
						break
					}
				}
			}
			totalSettleBonus += scoreSettleGroupDistinct(wantOp, cands, func(se SettlementExpectation, spm unaryPullSettleMeta) int64 {
				bonus := int64(400)
				if m.AckID != "" && spm.hasAckID && spm.ackID == m.AckID {
					bonus += 200
				}
				if spm.isErr == se.ExpectError {
					bonus += 100
				}
				if se.ExpectError && se.ErrorSubstring != "" && strings.Contains(spm.statusMsg, se.ErrorSubstring) {
					bonus += 100
				}
				if se.AckDeadlineSeconds > 0 && spm.hasDeadline && spm.deadline == int64(se.AckDeadlineSeconds) {
					bonus += 100
				}
				if m.DeliveryAttempt > 0 && (opIdx == 0 || opIdx == 1) && spm.hasDA && spm.da == int64(m.DeliveryAttempt) {
					bonus += 100
				}
				return bonus
			})
		}
	}
	if hasAckIDMatch {
		score += unaryPullAckIDBonus
	}
	if m.OrderingKey != "" && rm.hasOrderingKey && rm.orderingKey == m.OrderingKey {
		score += 40_000
	}
	if pm.hasWantEnv && rm.hasEnvSize {
		if pm.exactEnv {
			if rm.envSize == pm.wantEnv {
				score += 20_000
			}
		} else if rm.envSize >= pm.wantEnv {
			score += 20_000
		}
	}
	return score, totalSettleBonus
}

func verifyUnaryPull(
	idx *SpanIndex,
	sortedSpans []*proxypb.TraceSpan,
	profile Profile,
	projectID, fullSub, subID, topicID string,
	pullExp *UnaryPullExpectation,
	matched map[*proxypb.TraceSpan]bool,
	devs *deviationCollector,
) []Violation {
	if !profile.AllowUnaryPull {
		return []Violation{{
			Code:     CodeUnexpectedSpan,
			Rule:     "unary_pull.unsupported_in_profile",
			Message:  fmt.Sprintf("UnaryPull span verification is not supported under profile %q", profile.Name),
			Expected: "ProfileCpp",
			Actual:   string(profile.Name),
		}}
	}

	var failures []Violation
	recvName := subID + " receive"
	var recvCandidates []*proxypb.TraceSpan
	if subID != "" {
		recvCandidates = SortSpansDeterministic(idx.ByName[recvName])
	} else {
		for _, s := range sortedSpans {
			if isCandidatePubSubSpan(s, profile) && strings.HasSuffix(s.GetName(), " receive") && len(s.GetName()) > len(" receive") {
				subID = strings.TrimSuffix(s.GetName(), " receive")
				recvName = subID + " receive"
				recvCandidates = SortSpansDeterministic(idx.ByName[recvName])
				break
			}
		}
	}

	if pullExp.ExpectPullError {
		if len(recvCandidates) == 0 {
			return []Violation{{
				Code:     CodeMissingSpan,
				Rule:     "unary_pull.receive.error_present",
				Message:  fmt.Sprintf("missing %q span for failed UnaryPull", recvName),
				Expected: recvName,
				Actual:   "<missing>",
				SpanName: recvName,
			}}
		}
		rs := recvCandidates[0]
		matched[rs] = true
		effSubID := subID
		if effSubID == "" && strings.HasSuffix(rs.GetName(), " receive") {
			effSubID = strings.TrimSuffix(rs.GetName(), " receive")
		}
		failures = append(failures, checkSpanScope(rs, profile, "unary_pull.receive.scope", devs)...)
		failures = append(failures, checkSpanKindV(rs, proxypb.SpanKind_SPAN_KIND_CONSUMER, "unary_pull.receive.span_kind")...)
		failures = append(failures, checkCommonAttrs(rs, profile, projectID, fullSub, effSubID, devs, "unary_pull.receive")...)
		failures = append(failures, checkAttrV(rs, "messaging.operation.type", MatchString("receive"), "unary_pull.receive.attr.operation_type")...)
		failures = append(failures, checkAttrV(rs, profile.CodeFunctionAttrKey, MatchString("pubsub::SubscriberConnection::Pull"), "unary_pull.receive.attr.code_function_name")...)
		failures = append(failures, checkStatusV(rs, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, pullExp.ErrorSubstring, "unary_pull.receive.status")...)
		devs.add(Deviation{
			Category:    "unary_pull",
			Rule:        "spans.cpp_unary_pull",
			ExpectedGo:  "Unary Pull UNIMPLEMENTED on Go high-level Subscriber",
			Observed:    "<subID> receive + PullAckHandler/PullLeaseManager spans",
			SpanName:    rs.GetName(),
			Description: "C++ emits <subID> receive and pull ack/nack/modack spans on Unary Pull",
		})
		return failures
	}

	// Upfront validation of Settlement vs Settlements conflict and valid operations.
	msgSettlements := make([][]SettlementExpectation, len(pullExp.Messages))
	var validationErrs []Violation
	var usedOpFlags [3]bool
	for i, m := range pullExp.Messages {
		resolved, v := collectUnaryPullMessageSettlements(i, m)
		if len(v) > 0 {
			validationErrs = append(validationErrs, v...)
			continue
		}
		msgSettlements[i] = resolved
		for _, se := range resolved {
			if opIdx := unaryPullOpIndex(normalizeUnaryPullSettlementOp(se.Operation)); opIdx >= 0 {
				usedOpFlags[opIdx] = true
			}
		}
	}
	if len(validationErrs) > 0 {
		return validationErrs
	}

	if len(pullExp.Messages) > 0 && len(recvCandidates) == 0 {
		return []Violation{{
			Code:     CodeMissingSpan,
			Rule:     "unary_pull.receive.present",
			Message:  fmt.Sprintf("missing %q span(s) for %d pulled message(s)", recvName, len(pullExp.Messages)),
			Expected: recvName,
			Actual:   "<missing>",
			SpanName: recvName,
		}}
	}

	recvMetas := make([]unaryPullRecvMeta, len(recvCandidates))
	recvByKey := make(map[SpanKey][]int, len(recvCandidates))
	var effSubIDs []string
	seenEffSub := make(map[string]bool)
	for rj, rs := range recvCandidates {
		rm := buildUnaryPullRecvMeta(subID, rs)
		recvMetas[rj] = rm
		sk := SpanKey{TraceID: rm.normTraceID, SpanID: rm.normSpanID}
		recvByKey[sk] = append(recvByKey[sk], rj)
		if rm.effSubID != "" && !seenEffSub[rm.effSubID] {
			seenEffSub[rm.effSubID] = true
			effSubIDs = append(effSubIDs, rm.effSubID)
		}
	}

	sortedOpCache := make(map[string][]*proxypb.TraceSpan)
	getSortedOpSpans := func(name string) []*proxypb.TraceSpan {
		if s, ok := sortedOpCache[name]; ok {
			return s
		}
		s := SortSpansDeterministic(idx.ByName[name])
		sortedOpCache[name] = s
		return s
	}

	var linkedSettle [][]unaryPullSettleMeta
	if (usedOpFlags[0] || usedOpFlags[1] || usedOpFlags[2]) && len(recvCandidates) > 0 && len(effSubIDs) > 0 {
		linkedSettle = make([][]unaryPullSettleMeta, len(recvCandidates)*3)
		seenRJGen := make([]int, len(recvCandidates))
		spGen := 0
		opNames := [3]string{"ack", "nack", "modack"}
		for _, effSub := range effSubIDs {
			for opIdx := 0; opIdx < 3; opIdx++ {
				if !usedOpFlags[opIdx] {
					continue
				}
				for _, sp := range getSortedOpSpans(effSub + " " + opNames[opIdx]) {
					spAttrs := sp.GetAttributes()
					ackID, hasAckID := AttrString(spAttrs, "messaging.gcp_pubsub.message.ack_id")
					dl, hasDL := AttrInt64(spAttrs, "messaging.gcp_pubsub.message.ack_deadline_seconds")
					da, hasDA := AttrInt64(spAttrs, "messaging.gcp_pubsub.message.delivery_attempt")
					spm := unaryPullSettleMeta{
						ackID:       ackID,
						hasAckID:    hasAckID,
						isErr:       sp.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR,
						statusMsg:   sp.GetStatusMessage(),
						deadline:    dl,
						hasDeadline: hasDL,
						hasDA:       hasDA,
						da:          da,
					}
					spGen++
					for _, l := range sp.GetLinks() {
						if l == nil {
							continue
						}
						lk := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
						for _, rj := range recvByKey[lk] {
							if recvMetas[rj].effSubID == effSub && seenRJGen[rj] != spGen {
								seenRJGen[rj] = spGen
								slot := rj*3 + opIdx
								linkedSettle[slot] = append(linkedSettle[slot], spm)
							}
						}
					}
					gotT, okT := AttrString(spAttrs, "gcp_pubsub.receive.trace_id")
					gotS, okS := AttrString(spAttrs, "gcp_pubsub.receive.span_id")
					if okT && okS {
						ak := SpanKey{TraceID: NormalizeHex(gotT), SpanID: NormalizeHex(gotS)}
						for _, rj := range recvByKey[ak] {
							if recvMetas[rj].effSubID == effSub && seenRJGen[rj] != spGen {
								seenRJGen[rj] = spGen
								slot := rj*3 + opIdx
								linkedSettle[slot] = append(linkedSettle[slot], spm)
							}
						}
					}
				}
			}
		}
	}

	pullEdges := make([]bipartiteEdge, 0, len(pullExp.Messages)*len(recvCandidates))
	hasAnySettle := usedOpFlags[0] || usedOpFlags[1] || usedOpFlags[2]
	var rawSettles []int64
	if hasAnySettle && len(pullExp.Messages) > 0 && len(recvCandidates) > 0 {
		rawSettles = make([]int64, 0, len(pullExp.Messages)*len(recvCandidates))
	}
	var maxRawPullSettle int64
	for li, m := range pullExp.Messages {
		pm := buildUnaryPullMsgScoreMeta(m, msgSettlements[li])
		for rj := range recvCandidates {
			baseScore, rawSettle := scoreUnaryPullReceiveEdgeFast(pm, recvMetas[rj], linkedSettle, rj)
			if rawSettle > maxRawPullSettle {
				maxRawPullSettle = rawSettle
			}
			pullEdges = append(pullEdges, bipartiteEdge{
				left:   li,
				right:  rj,
				weight: baseScore + rawSettle,
			})
			if rawSettles != nil {
				rawSettles = append(rawSettles, rawSettle)
			}
		}
	}
	if maxRawPullSettle > unaryPullSettleBonusCap {
		for idx := range pullEdges {
			raw := rawSettles[idx]
			if raw > 0 {
				pullEdges[idx].weight = (pullEdges[idx].weight - raw) + normalizeSettleBonus(raw, maxRawPullSettle, unaryPullSettleBonusCap)
			}
		}
	}
	pullAssign := solveBipartiteMatching(len(pullExp.Messages), len(recvCandidates), nil, nil, pullEdges)

	type pendingUnarySettle struct {
		effSubID        string
		recvSpan        *proxypb.TraceSpan
		ackID           string
		deliveryAttempt int32
		se              SettlementExpectation
	}
	var settleOpOrder []string
	settleByOp := make(map[string][]pendingUnarySettle)

	for i, m := range pullExp.Messages {
		rj := pullAssign[i]
		if rj < 0 {
			failures = append(failures, Violation{
				Code:     CodeMissingSpan,
				Rule:     "unary_pull.receive.present",
				Message:  fmt.Sprintf("missing %q span for pulled message[%d]", recvName, i),
				Expected: recvName,
				Actual:   "<missing>",
				SpanName: recvName,
			})
			continue
		}
		rs := recvCandidates[rj]
		matched[rs] = true
		effSubID := subID
		if effSubID == "" && strings.HasSuffix(rs.GetName(), " receive") {
			effSubID = strings.TrimSuffix(rs.GetName(), " receive")
		}

		failures = append(failures, checkSpanScope(rs, profile, "unary_pull.receive.scope", devs)...)
		failures = append(failures, checkSpanKindV(rs, proxypb.SpanKind_SPAN_KIND_CONSUMER, "unary_pull.receive.span_kind")...)
		failures = append(failures, checkCommonAttrs(rs, profile, projectID, fullSub, effSubID, devs, "unary_pull.receive")...)
		failures = append(failures, checkAttrV(rs, "messaging.operation.type", MatchString("receive"), "unary_pull.receive.attr.operation_type")...)
		failures = append(failures, checkAttrV(rs, profile.CodeFunctionAttrKey, MatchString("pubsub::SubscriberConnection::Pull"), "unary_pull.receive.attr.code_function_name")...)
		wantMsgID := MatchStringNonEmpty()
		if m.MessageID != "" {
			wantMsgID = MatchString(m.MessageID)
		}
		failures = append(failures, checkAttrV(rs, "messaging.message.id", wantMsgID, "unary_pull.receive.attr.message_id")...)
		wantEnv := MatchAnyInt64()
		if m.EnvelopeSize != nil {
			wantEnv = MatchInt64(*m.EnvelopeSize)
		} else if m.Data != nil {
			wantEnv = MatchInt64Min(cppEnvelopeSize(m.Data, m.Attributes, m.OrderingKey, m.MessageID))
		} else if m.BodySize != nil {
			wantEnv = MatchInt64Min(20 + *m.BodySize)
		}
		failures = append(failures, checkAttrV(rs, "messaging.message.envelope.size", wantEnv, "unary_pull.receive.attr.envelope_size")...)
		if m.OrderingKey != "" {
			failures = append(failures, checkAttrV(rs, "messaging.gcp_pubsub.message.ordering_key", MatchString(m.OrderingKey), "unary_pull.receive.attr.ordering_key")...)
		}
		failures = append(failures, checkStatusV(rs, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, false, "", "unary_pull.receive.status")...)

		// Verify link/ABI-v1-attrs to create span from WireTraceparent when sampled & valid,
		// and forbid dangling/duplicate links on <subID> receive.
		if m.WireTraceparent != "" {
			if tp, err := ParseTraceparent(m.WireTraceparent); err == nil {
				if tp.IsSampled() {
					failures = append(failures, verifyCppABIV1OrV2Link(rs, tp.TraceID, tp.SpanID, "create", "unary_pull.receive.link_to_create", devs)...)
				} else {
					failures = append(failures, checkAttrAbsentV(rs, "gcp_pubsub.create.trace_id", "unary_pull.receive.attr.create_trace_id_absent")...)
					failures = append(failures, checkAttrAbsentV(rs, "gcp_pubsub.create.span_id", "unary_pull.receive.attr.create_span_id_absent")...)
					for li, l := range rs.GetLinks() {
						if l == nil {
							continue
						}
						if NormalizeHex(l.GetTraceId()) == tp.TraceID {
							failures = append(failures, Violation{
								Code:     CodeBrokenTraceparent,
								Rule:     "unary_pull.receive.forbidden_link",
								Message:  fmt.Sprintf("%q link[%d] targets unsampled (-00) trace %s", rs.GetName(), li, tp.TraceID),
								Expected: "0 links to unsampled create",
								Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
								TraceID:  rs.GetTraceId(),
								SpanID:   rs.GetSpanId(),
								SpanName: rs.GetName(),
							})
						} else {
							failures = append(failures, Violation{
								Code:     CodeMissingLink,
								Rule:     "unary_pull.receive.dangling_link",
								Message:  fmt.Sprintf("%q link[%d] unexpectedly targets (%s/%s)", rs.GetName(), li, l.GetTraceId(), l.GetSpanId()),
								Expected: "0 links when WireTraceparent is unsampled",
								Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
								TraceID:  rs.GetTraceId(),
								SpanID:   rs.GetSpanId(),
								SpanName: rs.GetName(),
							})
						}
					}
				}
			} else {
				failures = append(failures, checkAttrAbsentV(rs, "gcp_pubsub.create.trace_id", "unary_pull.receive.attr.create_trace_id_absent")...)
				failures = append(failures, checkAttrAbsentV(rs, "gcp_pubsub.create.span_id", "unary_pull.receive.attr.create_span_id_absent")...)
				for li, l := range rs.GetLinks() {
					if l == nil {
						continue
					}
					failures = append(failures, Violation{
						Code:     CodeBrokenTraceparent,
						Rule:     "unary_pull.receive.forbidden_link",
						Message:  fmt.Sprintf("%q link[%d] unexpectedly present when WireTraceparent %q is invalid (%v)", rs.GetName(), li, m.WireTraceparent, err),
						Expected: "0 links when WireTraceparent is invalid",
						Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
						TraceID:  rs.GetTraceId(),
						SpanID:   rs.GetSpanId(),
						SpanName: rs.GetName(),
					})
				}
			}
		} else {
			seenRecvLinks := make(map[SpanKey]bool)
			for li, l := range rs.GetLinks() {
				if l == nil {
					continue
				}
				k := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
				if seenRecvLinks[k] {
					failures = append(failures, Violation{
						Code:     CodeBrokenBidirectionalLink,
						Rule:     "unary_pull.receive.link_to_create.duplicate_link",
						Message:  fmt.Sprintf("duplicate link[%d] on %q targeting (%s/%s)", li, rs.GetName(), l.GetTraceId(), l.GetSpanId()),
						Expected: "at most 1 link",
						Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
						TraceID:  rs.GetTraceId(),
						SpanID:   rs.GetSpanId(),
						SpanName: rs.GetName(),
					})
					continue
				}
				seenRecvLinks[k] = true
				if len(seenRecvLinks) > 1 {
					failures = append(failures, Violation{
						Code:     CodeMissingLink,
						Rule:     "unary_pull.receive.link_to_create.dangling_link",
						Message:  fmt.Sprintf("unexpected extra link[%d] on single-message %q targeting (%s/%s)", li, rs.GetName(), l.GetTraceId(), l.GetSpanId()),
						Expected: "at most 1 link to create",
						Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
						TraceID:  rs.GetTraceId(),
						SpanID:   rs.GetSpanId(),
						SpanName: rs.GetName(),
					})
				}
			}
		}

		devs.add(Deviation{
			Category:    "unary_pull",
			Rule:        "spans.cpp_unary_pull",
			ExpectedGo:  "Unary Pull UNIMPLEMENTED on Go high-level Subscriber",
			Observed:    "<subID> receive + PullAckHandler/PullLeaseManager spans",
			SpanName:    rs.GetName(),
			Description: "C++ emits <subID> receive and pull ack/nack/modack spans on Unary Pull",
		})

		for _, se := range msgSettlements[i] {
			op := normalizeUnaryPullSettlementOp(se.Operation)
			if _, seen := settleByOp[op]; !seen {
				settleOpOrder = append(settleOpOrder, op)
			}
			settleByOp[op] = append(settleByOp[op], pendingUnarySettle{
				effSubID:        effSubID,
				recvSpan:        rs,
				ackID:           m.AckID,
				deliveryAttempt: m.DeliveryAttempt,
				se:              se,
			})
		}
	}

	// Global bipartite matching per settlement operation across all matched UnaryPull receive spans.
	for _, op := range settleOpOrder {
		opItems := settleByOp[op]
		effName := subID + " " + op
		if subID == "" && len(opItems) > 0 {
			effName = opItems[0].effSubID + " " + op
		}
		candidates := getSortedOpSpans(effName)

		itemsByRecvKey := make(map[SpanKey][]int, len(opItems))
		for si, item := range opItems {
			rk := SpanKey{
				TraceID: NormalizeHex(item.recvSpan.GetTraceId()),
				SpanID:  NormalizeHex(item.recvSpan.GetSpanId()),
			}
			itemsByRecvKey[rk] = append(itemsByRecvKey[rk], si)
		}

		var sEdges []bipartiteEdge
		seenSIGen := make([]int, len(opItems))
		candGen := 0
		for sj, c := range candidates {
			if matched[c] {
				continue
			}
			attrs := c.GetAttributes()
			gotAck, hasAck := AttrString(attrs, "messaging.gcp_pubsub.message.ack_id")
			isErr := c.GetStatusCode() == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			statusMsg := c.GetStatusMessage()
			dl, hasDL := AttrInt64(attrs, "messaging.gcp_pubsub.message.ack_deadline_seconds")
			da, hasDA := AttrInt64(attrs, "messaging.gcp_pubsub.message.delivery_attempt")

			candGen++
			emitForItem := func(si int) {
				if seenSIGen[si] == candGen {
					return
				}
				seenSIGen[si] = candGen
				item := opItems[si]
				var score int64
				if item.ackID != "" && hasAck && gotAck == item.ackID {
					score += 100_000
				}
				if isErr == item.se.ExpectError {
					score += 40_000
				}
				if item.se.ExpectError && item.se.ErrorSubstring != "" && strings.Contains(statusMsg, item.se.ErrorSubstring) {
					score += 30_000
				}
				if item.se.AckDeadlineSeconds > 0 && hasDL && dl == int64(item.se.AckDeadlineSeconds) {
					score += 20_000
				}
				if item.deliveryAttempt > 0 && (op == "ack" || op == "nack") && hasDA && da == int64(item.deliveryAttempt) {
					score += 15_000
				}
				sEdges = append(sEdges, bipartiteEdge{left: si, right: sj, weight: score})
			}

			for _, l := range c.GetLinks() {
				if l == nil {
					continue
				}
				lk := SpanKey{TraceID: NormalizeHex(l.GetTraceId()), SpanID: NormalizeHex(l.GetSpanId())}
				for _, si := range itemsByRecvKey[lk] {
					emitForItem(si)
				}
			}
			gotT, okT := AttrString(attrs, "gcp_pubsub.receive.trace_id")
			gotS, okS := AttrString(attrs, "gcp_pubsub.receive.span_id")
			if okT && okS {
				ak := SpanKey{TraceID: NormalizeHex(gotT), SpanID: NormalizeHex(gotS)}
				for _, si := range itemsByRecvKey[ak] {
					emitForItem(si)
				}
			}
		}

		sAssign := solveBipartiteMatching(len(opItems), len(candidates), nil, nil, sEdges)
		for _, rj := range sAssign {
			if rj >= 0 {
				matched[candidates[rj]] = true
			}
		}
		for si, item := range opItems {
			rj := sAssign[si]
			if rj < 0 {
				unlinkedCount := 0
				for _, c := range candidates {
					if !matched[c] && !hasCppABIV1OrV2Link(c, item.recvSpan.GetTraceId(), item.recvSpan.GetSpanId(), "receive") {
						unlinkedCount++
					}
				}
				msg := fmt.Sprintf("missing %q span linked to %q (%s)", effName, item.recvSpan.GetName(), item.recvSpan.GetSpanId())
				if unlinkedCount > 0 {
					msg = fmt.Sprintf("missing %q span linked to %q (%s) (found %d unmatched %q span(s) lacking ABI v1/v2 link to %q)", effName, item.recvSpan.GetName(), item.recvSpan.GetSpanId(), unlinkedCount, effName, item.recvSpan.GetName())
				}
				failures = append(failures, Violation{
					Code:     CodeMissingSpan,
					Rule:     "unary_pull." + op + ".present",
					Message:  msg,
					Expected: effName,
					Actual:   "<missing>",
					TraceID:  item.recvSpan.GetTraceId(),
					SpanID:   item.recvSpan.GetSpanId(),
					SpanName: effName,
				})
				continue
			}
			chosen := candidates[rj]
			failures = append(failures, verifyUnaryPullSettlementSpan(
				profile, projectID, fullSub, item.effSubID, item.recvSpan,
				chosen, item.ackID, item.deliveryAttempt, item.se, devs,
			)...)
		}
	}

	_ = topicID
	return failures
}

func hasCppABIV1OrV2Link(s *proxypb.TraceSpan, targetTraceID, targetSpanID, linkLabel string) bool {
	tt := NormalizeHex(targetTraceID)
	ts := NormalizeHex(targetSpanID)
	for _, l := range s.GetLinks() {
		if l != nil && NormalizeHex(l.GetTraceId()) == tt && NormalizeHex(l.GetSpanId()) == ts {
			return true
		}
	}
	tAttrKey := fmt.Sprintf("gcp_pubsub.%s.trace_id", linkLabel)
	sAttrKey := fmt.Sprintf("gcp_pubsub.%s.span_id", linkLabel)
	gotT, okT := AttrString(s.GetAttributes(), tAttrKey)
	gotS, okS := AttrString(s.GetAttributes(), sAttrKey)
	return okT && okS && NormalizeHex(gotT) == tt && NormalizeHex(gotS) == ts
}

func verifyCppABIV1OrV2Link(s *proxypb.TraceSpan, targetTraceID, targetSpanID, linkLabel, rule string, devs *deviationCollector) []Violation {
	tt := NormalizeHex(targetTraceID)
	ts := NormalizeHex(targetSpanID)

	// ABI >= 2: direct SpanLink. Reject duplicate links and dangling/unrelated links.
	var out []Violation
	matchCount := 0
	for li, l := range s.GetLinks() {
		if l == nil {
			continue
		}
		lt := NormalizeHex(l.GetTraceId())
		ls := NormalizeHex(l.GetSpanId())
		if lt == tt && ls == ts {
			matchCount++
			continue
		}
		out = append(out, Violation{
			Code:     CodeMissingLink,
			Rule:     rule + ".dangling_link",
			Message:  fmt.Sprintf("span %q link[%d] targets unexpected or dangling span (%s/%s) instead of %s/%s", s.GetName(), li, l.GetTraceId(), l.GetSpanId(), targetTraceID, targetSpanID),
			Expected: fmt.Sprintf("link -> %s/%s", targetTraceID, targetSpanID),
			Actual:   fmt.Sprintf("%s/%s", l.GetTraceId(), l.GetSpanId()),
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		})
	}
	if matchCount > 1 {
		out = append(out, Violation{
			Code:     CodeBrokenBidirectionalLink,
			Rule:     rule + ".duplicate_link",
			Message:  fmt.Sprintf("span %q has %d duplicate links referencing %s/%s", s.GetName(), matchCount, targetTraceID, targetSpanID),
			Expected: fmt.Sprintf("1 link -> %s/%s", targetTraceID, targetSpanID),
			Actual:   strconv.Itoa(matchCount),
			TraceID:  s.GetTraceId(),
			SpanID:   s.GetSpanId(),
			SpanName: s.GetName(),
		})
	}
	if matchCount >= 1 {
		return out
	}

	// ABI < 2: gcp_pubsub.<linkLabel>.{trace_id,span_id} on span attributes.
	tAttrKey := fmt.Sprintf("gcp_pubsub.%s.trace_id", linkLabel)
	sAttrKey := fmt.Sprintf("gcp_pubsub.%s.span_id", linkLabel)
	gotT, okT := AttrString(s.GetAttributes(), tAttrKey)
	gotS, okS := AttrString(s.GetAttributes(), sAttrKey)
	if okT && okS && NormalizeHex(gotT) == tt && NormalizeHex(gotS) == ts {
		devs.add(Deviation{
			Category:    "link",
			Rule:        "links.abi_v1_event_attrs_fallback",
			ExpectedGo:  "SpanLink",
			Observed:    fmt.Sprintf("attributes %s, %s", tAttrKey, sAttrKey),
			SpanName:    s.GetName(),
			Description: "C++ ABI v1 records link target trace/span IDs as attributes when SpanLink is unsupported",
		})
		return out
	}
	out = append(out, Violation{
		Code:     CodeMissingLink,
		Rule:     rule,
		Message:  fmt.Sprintf("span %q missing SpanLink or ABI v1 %s/%s attributes referencing %s/%s", s.GetName(), tAttrKey, sAttrKey, targetTraceID, targetSpanID),
		Expected: fmt.Sprintf("link -> %s/%s", targetTraceID, targetSpanID),
		Actual:   "<missing>",
		TraceID:  s.GetTraceId(),
		SpanID:   s.GetSpanId(),
		SpanName: s.GetName(),
	})
	return out
}

func verifyUnaryPullSettlementSpan(
	profile Profile,
	projectID, fullSub, subID string,
	recvSpan, chosen *proxypb.TraceSpan,
	ackID string,
	deliveryAttempt int32,
	se SettlementExpectation,
	devs *deviationCollector,
) []Violation {
	op := strings.ToLower(strings.TrimSpace(se.Operation))
	var out []Violation
	out = append(out, checkSpanScope(chosen, profile, "unary_pull."+op+".scope", devs)...)
	out = append(out, checkSpanKindV(chosen, proxypb.SpanKind_SPAN_KIND_CLIENT, "unary_pull."+op+".span_kind")...)
	out = append(out, checkCommonAttrs(chosen, profile, projectID, fullSub, subID, devs, "unary_pull."+op)...)
	out = append(out, checkAttrV(chosen, "messaging.operation.type", MatchString(op), "unary_pull."+op+".attr.operation_type")...)
	wantAckID := MatchStringNonEmpty()
	if ackID != "" {
		wantAckID = MatchString(ackID)
	}
	out = append(out, checkAttrV(chosen, "messaging.gcp_pubsub.message.ack_id", wantAckID, "unary_pull."+op+".attr.ack_id")...)
	out = append(out, verifyCppABIV1OrV2Link(chosen, recvSpan.GetTraceId(), recvSpan.GetSpanId(), "receive", "unary_pull."+op+".link_to_receive", devs)...)

	switch op {
	case "ack":
		out = append(out, checkAttrV(chosen, profile.CodeFunctionAttrKey, MatchString("pubsub::PullAckHandler::ack"), "unary_pull.ack.attr.code_function_name")...)
		if deliveryAttempt > 0 {
			out = append(out, checkAttrV(chosen, "messaging.gcp_pubsub.message.delivery_attempt", MatchInt64(int64(deliveryAttempt)), "unary_pull.ack.attr.delivery_attempt")...)
		}
	case "nack":
		out = append(out, checkAttrV(chosen, profile.CodeFunctionAttrKey, MatchString("pubsub::PullAckHandler::nack"), "unary_pull.nack.attr.code_function_name")...)
		if deliveryAttempt > 0 {
			out = append(out, checkAttrV(chosen, "messaging.gcp_pubsub.message.delivery_attempt", MatchInt64(int64(deliveryAttempt)), "unary_pull.nack.attr.delivery_attempt")...)
		}
	case "modack":
		out = append(out, checkRootSpanV(chosen, "unary_pull.modack.root")...)
		out = append(out, checkAttrV(chosen, profile.CodeFunctionAttrKey, MatchString("pubsub::PullLeaseManager::ExtendLease"), "unary_pull.modack.attr.code_function_name")...)
		wantDeadline := MatchInt64Min(1)
		if se.AckDeadlineSeconds > 0 {
			wantDeadline = MatchInt64(int64(se.AckDeadlineSeconds))
		}
		out = append(out, checkAttrV(chosen, "messaging.gcp_pubsub.message.ack_deadline_seconds", wantDeadline, "unary_pull.modack.attr.ack_deadline_seconds")...)
	}

	if se.ExpectError {
		out = append(out, checkStatusV(chosen, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, true, se.ErrorSubstring, "unary_pull."+op+".status")...)
	} else {
		out = append(out, checkStatusV(chosen, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, false, "", "unary_pull."+op+".status")...)
	}
	return out
}
