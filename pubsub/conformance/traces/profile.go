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
	"sort"
	"strings"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
)

// ProfileName identifies a client-side OpenTelemetry verification profile.
type ProfileName string

const (
	// ProfileGo is the strict canonical Go pubsub/v2 verification profile.
	ProfileGo ProfileName = "go"
	// ProfileCPP is the C++ OpenTelemetry verification profile that accepts
	// verified C++ SDK behaviors while recording structured Deviation entries.
	ProfileCPP ProfileName = "cpp"
)

// Well-known instrumentation scope names.
const (
	GoPubSubScopeName = "cloud.google.com/go/pubsub/v2"
	CppScopeName      = "gl-cpp"
	OTelGrpcScopeName = "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

// Deviation records a known cross-SDK OpenTelemetry divergence from the
// canonical Go pubsub/v2 tracing specification (e.g., C++ SDK deltas).
type Deviation struct {
	Category    string `json:"category"`
	Rule        string `json:"rule"`
	ExpectedGo  string `json:"expected_go"`
	Observed    string `json:"observed"`
	SpanName    string `json:"span_name,omitempty"`
	Severity    string `json:"severity,omitempty"`
	Description string `json:"description"`
}

// Profile configures canonical vs. SDK-specific OpenTelemetry verification rules.
type Profile struct {
	Name                                ProfileName
	ExpectedScope                       string
	IgnoredScopes                       []string
	SchedulerSpanName                   string
	FlowControlSpanKind                 proxypb.SpanKind
	BatchingSpanKind                    proxypb.SpanKind
	PublishSpanKind                     proxypb.SpanKind
	SubscribeSpanKind                   proxypb.SpanKind
	ProcessSpanKind                     proxypb.SpanKind
	ModAckSpanKind                      proxypb.SpanKind
	UnaryPullAckSpanKind                proxypb.SpanKind
	RequireGCPResourceName              bool
	RequireUnconditionalOrderingKey     bool
	RequireUnconditionalCreateMessageID bool
	UseEnvelopeSize                     bool
	CodeFunctionAttrKey                 string
	EOSSubscribeAttrKey                 string
	ResultValues                        map[string]string
	RequireBidirectionalPublishLinks    bool
	RequireReversePublishLinkOpName     bool
	AllowABIV1LinkAttrs                 bool
	AllowLinkFanoutChildren             bool
	DefaultMaxLinksPerSpan              int
	RequireBidirectionalSubscribeLinks  bool
	PerMessageSettle                    bool
	AllowOmittedFlowControlIfIgnored    bool
	AllowBlockingPublisher              bool
	AllowUnaryPull                      bool
	AllowBenignCppSpans                 bool
	RequireSampledParentOnSubscribe     bool
	AllowSyncOKOnFlowControlError       bool
	AllowPublishOKOnRPCError            bool
	AllowModAckUnsetOnError             bool
	AllowSubscribeUnsetOnEODError       bool
	RequireExceptionEventOnError        bool
	ProcessMinimalAttributes            bool
	ProcessEventsEnabled                bool
	ExpectZeroChildAttrs                bool
}

// ProfileGoCanonical returns the strict canonical Go pubsub/v2 tracing profile.
func ProfileGoCanonical() Profile {
	return Profile{
		Name:                                ProfileGo,
		ExpectedScope:                       GoPubSubScopeName,
		IgnoredScopes:                       []string{OTelGrpcScopeName},
		SchedulerSpanName:                   "subscribe scheduler",
		FlowControlSpanKind:                 proxypb.SpanKind_SPAN_KIND_INTERNAL,
		BatchingSpanKind:                    proxypb.SpanKind_SPAN_KIND_INTERNAL,
		PublishSpanKind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
		SubscribeSpanKind:                   proxypb.SpanKind_SPAN_KIND_INTERNAL,
		ProcessSpanKind:                     proxypb.SpanKind_SPAN_KIND_CONSUMER,
		ModAckSpanKind:                      proxypb.SpanKind_SPAN_KIND_INTERNAL,
		RequireGCPResourceName:              true,
		RequireUnconditionalOrderingKey:     true,
		RequireUnconditionalCreateMessageID: true,
		UseEnvelopeSize:                     false,
		CodeFunctionAttrKey:                 "code.function",
		EOSSubscribeAttrKey:                 "messaging.gcp_pubsub.exactly_once_delivery",
		ResultValues: map[string]string{
			"acked":   "acked",
			"nacked":  "nacked",
			"expired": "expired",
		},
		RequireBidirectionalPublishLinks:   true,
		RequireReversePublishLinkOpName:    true,
		AllowABIV1LinkAttrs:                false,
		AllowLinkFanoutChildren:            false,
		DefaultMaxLinksPerSpan:             128,
		RequireBidirectionalSubscribeLinks: true,
		PerMessageSettle:                   false,
		AllowOmittedFlowControlIfIgnored:   false,
		AllowBlockingPublisher:             false,
		AllowUnaryPull:                     false,
		AllowBenignCppSpans:                false,
		RequireSampledParentOnSubscribe:    false,
		AllowSyncOKOnFlowControlError:      false,
		AllowPublishOKOnRPCError:           false,
		AllowModAckUnsetOnError:            false,
		AllowSubscribeUnsetOnEODError:      false,
		RequireExceptionEventOnError:       true,
		ProcessMinimalAttributes:           false,
		ProcessEventsEnabled:               true,
		ExpectZeroChildAttrs:               true,
	}
}

// ProfileCpp returns the C++ OpenTelemetry verification profile matching
// google-cloud-cpp/pubsub/internal tracing while recording spec deviations.
func ProfileCpp() Profile {
	return Profile{
		Name:                                ProfileCPP,
		ExpectedScope:                       CppScopeName,
		IgnoredScopes:                       nil,
		SchedulerSpanName:                   "subscriber scheduler",
		FlowControlSpanKind:                 proxypb.SpanKind_SPAN_KIND_CLIENT,
		BatchingSpanKind:                    proxypb.SpanKind_SPAN_KIND_CLIENT,
		PublishSpanKind:                     proxypb.SpanKind_SPAN_KIND_CLIENT,
		SubscribeSpanKind:                   proxypb.SpanKind_SPAN_KIND_CONSUMER,
		ProcessSpanKind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
		ModAckSpanKind:                      proxypb.SpanKind_SPAN_KIND_CLIENT,
		UnaryPullAckSpanKind:                proxypb.SpanKind_SPAN_KIND_CLIENT,
		RequireGCPResourceName:              false,
		RequireUnconditionalOrderingKey:     false,
		RequireUnconditionalCreateMessageID: false,
		UseEnvelopeSize:                     true,
		CodeFunctionAttrKey:                 "code.function.name",
		EOSSubscribeAttrKey:                 "messaging.gcp_pubsub.subscription.exactly_once_delivery",
		ResultValues: map[string]string{
			"acked":   "ack",
			"nacked":  "nack",
			"expired": "expired",
		},
		RequireBidirectionalPublishLinks:   false,
		RequireReversePublishLinkOpName:    false,
		AllowABIV1LinkAttrs:                true,
		AllowLinkFanoutChildren:            true,
		DefaultMaxLinksPerSpan:             128,
		RequireBidirectionalSubscribeLinks: false,
		PerMessageSettle:                   true,
		AllowOmittedFlowControlIfIgnored:   true,
		AllowBlockingPublisher:             true,
		AllowUnaryPull:                     true,
		AllowBenignCppSpans:                true,
		RequireSampledParentOnSubscribe:    true,
		AllowSyncOKOnFlowControlError:      true,
		AllowPublishOKOnRPCError:           true,
		AllowModAckUnsetOnError:            true,
		AllowSubscribeUnsetOnEODError:      true,
		RequireExceptionEventOnError:       false,
		ProcessMinimalAttributes:           true,
		ProcessEventsEnabled:               false,
		ExpectZeroChildAttrs:               false,
	}
}

// ProfileForLanguage maps GetCapabilitiesResponse.ClientLanguage (case-insensitive)
// to a verification profile. "cpp" and "c++" select ProfileCpp; "go", "", or
// unknown values default to ProfileGoCanonical.
func ProfileForLanguage(clientLanguage string) Profile {
	switch strings.ToLower(strings.TrimSpace(clientLanguage)) {
	case "cpp", "c++":
		return ProfileCpp()
	default:
		return ProfileGoCanonical()
	}
}

// IsIgnoredScope reports whether the given instrumentation scope is a known
// non-Pub/Sub transport scope ignored by default under this profile.
func (p Profile) IsIgnoredScope(scope string) bool {
	for _, s := range p.IgnoredScopes {
		if scope == s {
			return true
		}
	}
	return false
}

// IsBenignCppLifecycleOrGRPCSpan reports whether span is an expected gRPC stub
// or publisher Flush/ResumePublish lifecycle span emitted by google-cloud-cpp.
func IsBenignCppLifecycleOrGRPCSpan(span *proxypb.TraceSpan) bool {
	if span == nil {
		return false
	}
	name := span.GetName()
	if strings.HasPrefix(name, "google.pubsub.v1.") {
		return true
	}
	if name == "Start" || name == "Finish" {
		return true
	}
	if strings.HasPrefix(name, "pubsub::") &&
		(strings.HasSuffix(name, "::Flush") || strings.HasSuffix(name, "::ResumePublish")) {
		return true
	}
	return false
}

type deviationCollector struct {
	seen map[deviationKey]struct{}
	list []Deviation
}

type deviationKey struct {
	category   string
	rule       string
	spanName   string
	observed   string
	expectedGo string
}

func newDeviationCollector() *deviationCollector {
	return &deviationCollector{
		seen: make(map[deviationKey]struct{}),
	}
}

func (c *deviationCollector) add(d Deviation) {
	if d.Severity == "" {
		d.Severity = "delta"
	}
	k := deviationKey{
		category:   d.Category,
		rule:       d.Rule,
		spanName:   d.SpanName,
		observed:   d.Observed,
		expectedGo: d.ExpectedGo,
	}
	if _, ok := c.seen[k]; ok {
		return
	}
	c.seen[k] = struct{}{}
	c.list = append(c.list, d)
}

func (c *deviationCollector) sorted() []Deviation {
	if len(c.list) == 0 {
		return nil
	}
	out := make([]Deviation, len(c.list))
	copy(out, c.list)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Rule != out[j].Rule {
			return out[i].Rule < out[j].Rule
		}
		if out[i].SpanName != out[j].SpanName {
			return out[i].SpanName < out[j].SpanName
		}
		if out[i].Category != out[j].Category {
			return out[i].Category < out[j].Category
		}
		return out[i].Observed < out[j].Observed
	})
	return out
}
