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

import "github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"

// PublishErrorKind identifies which stage of publish failed so the verifier
// applies exact Go/C++ error and span-leak rules.
type PublishErrorKind string

const (
	PublishErrorAuto             PublishErrorKind = ""
	PublishErrorPreValidation    PublishErrorKind = "pre_validation"
	PublishErrorOrderingRejected PublishErrorKind = "ordering_rejected"
	PublishErrorFlowControl      PublishErrorKind = "flow_control"
	PublishErrorOversized        PublishErrorKind = "oversized"
	PublishErrorRPC              PublishErrorKind = "rpc"
)

// CustomCheck is an escape hatch allowing scenarios to run additional
// span-graph assertions alongside declarative expectations.
type CustomCheck func(spans []*proxypb.TraceSpan, profile Profile) []Violation

// TraceExpectation declares expected OpenTelemetry spans, propagation, links,
// attributes, events, and error states for a conformance scenario.
type TraceExpectation struct {
	// ExpectNoSpans asserts that zero Pub/Sub spans were emitted (e.g., when
	// EnableMonitoring=false on a supports_monitoring=true proxy).
	ExpectNoSpans bool

	// StrictRawSpans prevents filtering out non-Pub/Sub transport spans (such as
	// otelgrpc) when checking ExpectNoSpans or unexpected extra spans.
	StrictRawSpans bool

	// AllowExtraPubSubSpans disables strict unexpected-Pub/Sub-span checking.
	// By default (false), unexpected or duplicate Pub/Sub spans cause failure.
	AllowExtraPubSubSpans bool

	// Topic and Subscription accept full resource paths ("projects/{p}/topics/{t}")
	// or short IDs; ProjectID overrides or supplies the project ID when short IDs are used.
	Topic        string
	Subscription string
	ProjectID    string

	Publish   *PublishExpectation
	Subscribe *SubscribeExpectation
	UnaryPull *UnaryPullExpectation

	CustomChecks []CustomCheck
}

// PublishExpectation specifies expected publisher spans for a scenario.
// Callers may use either the aggregate convenience fields (MessageCount,
// WireTraceparents, MessageIDs, etc.) or per-message Messages + Batches.
type PublishExpectation struct {
	MessageCount             int
	BatchCount               int
	ParentTraceparent        string
	OrderingKey              string
	MessageIDs               []string
	WireTraceparents         []string
	BodySizes                []int64
	RequireAttributes        map[string]AttrMatcher
	RequireFlowControlSpan   *bool
	RequireBatchingSpan      *bool
	FlowControlIgnored       bool
	BlockingPublisher        bool
	Hedged                   bool
	MinHedgedAttempts        int
	ExpectError              bool
	ErrorKind                PublishErrorKind
	ErrorSubstring           string
	MaxLinksPerSpan          int
	RequireSequentialBatches bool
	Messages                 []PublishMessageExpectation
	Batches                  []PublishBatchExpectation
}

// PublishMessageExpectation specifies expected spans for one published message.
type PublishMessageExpectation struct {
	MessageID              string
	Data                   []byte
	BodySize               *int64
	EnvelopeSize           *int64
	Attributes             map[string]string
	OrderingKey            string
	ParentTraceparent      string
	WireTraceparent        string
	RequireAttributes      map[string]AttrMatcher
	BlockingPublisher      bool
	FlowControlIgnored     bool
	RequireFlowControlSpan *bool
	RequireBatchingSpan    *bool
	Hedged                 bool
	MinHedgedAttempts      int
	ExpectError            bool
	ErrorKind              PublishErrorKind
	ErrorSubstring         string
	ExpectDroppedUnsampled bool
}

// PublishBatchExpectation specifies expectations for a `<topicID> publish` batch RPC span.
type PublishBatchExpectation struct {
	MessageCount int
}

// SubscribeInterruptKind identifies an explicit pre-dispatch cancellation or
// forced-shutdown settlement path so the verifier applies canonical Go/proxy
// child-span and process-event rules. Zero value ("") enforces full strict
// normal-delivery expectations.
type SubscribeInterruptKind string

const (
	SubscribeInterruptNone               SubscribeInterruptKind = ""
	SubscribeInterruptPreDispatchCancel  SubscribeInterruptKind = "pre_dispatch_cancel"
	SubscribeInterruptForcedShutdownNack SubscribeInterruptKind = "forced_shutdown_nack"
)

// SubscribeExpectation specifies expected streaming subscriber spans for a scenario.
type SubscribeExpectation struct {
	MessageCount                 int
	WireTraceparents             []string
	CallbackTraceparents         []string
	AckIDs                       []string
	MessageIDs                   []string
	OrderingKey                  string
	DeliveryAttempt              int32
	ExactlyOnce                  *bool
	BatchCount                   int
	ExpectedOutcome              string
	ExpectedSettlements          []SettlementExpectation
	ExpectDroppedUnsampled       bool
	StrictSettlementOps          bool
	InterruptKind                SubscribeInterruptKind
	ExpectConcurrencyControlSpan *bool
	ExpectSchedulerSpan          *bool
	ExpectProcessSpan            *bool
	ExpectProcessSettlementEvent *bool
	Messages                     []SubscribeMessageExpectation
}

// SubscribeMessageExpectation specifies expected spans for one delivered message.
type SubscribeMessageExpectation struct {
	MessageID                    string
	AckID                        string
	Data                         []byte
	BodySize                     *int64
	EnvelopeSize                 *int64
	Attributes                   map[string]string
	OrderingKey                  string
	DeliveryAttempt              int32
	ExactlyOnce                  *bool
	BatchCount                   int
	WireTraceparent              string
	CallbackTraceparent          string
	ExpectDroppedUnsampled       bool
	ExpectProcessSpan            *bool
	InterruptKind                SubscribeInterruptKind
	ExpectConcurrencyControlSpan *bool
	ExpectSchedulerSpan          *bool
	ExpectProcessSettlementEvent *bool
	ExpectedOutcome              string
	Settlements                  []SettlementExpectation
	RequireAttributes            map[string]AttrMatcher
}

// SettlementExpectation describes an expected ack, nack, or modack span linked
// to a delivered message's `<subID> subscribe` span.
type SettlementExpectation struct {
	Operation              string // "ack", "nack", or "modack"
	AckDeadlineSeconds     int32
	IsReceiptModack        *bool
	ExpectedBatchCount     int
	ExpectError            bool
	ErrorSubstring         string
	EODPartialBatchFailure bool
}

// UnaryPullExpectation describes expected C++ unary Pull spans (`<subID> receive`
// plus ack/nack/modack handlers).
type UnaryPullExpectation struct {
	Messages        []UnaryPullMessageExpectation
	ExpectPullError bool
	ErrorSubstring  string
}

// UnaryPullMessageExpectation describes one message pulled via UnaryPull.
// When EnvelopeSize and Data are unset under ProfileCpp (UseEnvelopeSize=true),
// BodySize enforces the universal MessageProtoSize lower bound
// (messaging.message.envelope.size >= 20 + *BodySize).
//
// Callers may specify a single expected settlement via Settlement (legacy) or
// one or more expected settlements via Settlements (e.g., modack + RAII nack,
// or periodic modacks + terminal ack/nack). Setting both Settlement and
// Settlements on the same message is rejected with a validation violation.
type UnaryPullMessageExpectation struct {
	MessageID       string
	AckID           string
	Data            []byte
	BodySize        *int64
	EnvelopeSize    *int64
	Attributes      map[string]string
	OrderingKey     string
	DeliveryAttempt int32
	WireTraceparent string
	Settlement      *SettlementExpectation
	Settlements     []SettlementExpectation
}

// ExpectNoTraces returns a TraceExpectation asserting that zero Pub/Sub spans
// were emitted (ignoring non-Pub/Sub transport spans like otelgrpc by default).
func ExpectNoTraces() *TraceExpectation {
	return &TraceExpectation{ExpectNoSpans: true}
}

// ExpectNoPubSubSpans is an alias for ExpectNoTraces.
func ExpectNoPubSubSpans() *TraceExpectation {
	return ExpectNoTraces()
}

// Int64Ptr returns a pointer to v for optional size fields.
func Int64Ptr(v int64) *int64 {
	return &v
}

// BoolPtr returns a pointer to v for optional boolean fields.
func BoolPtr(v bool) *bool {
	return &v
}
