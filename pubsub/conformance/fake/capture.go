// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fake

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// Canonical Pub/Sub v1 data-plane full gRPC method names and wire headers.
const (
	MethodPublish           = pubsubpb.Publisher_Publish_FullMethodName
	MethodPull              = pubsubpb.Subscriber_Pull_FullMethodName
	MethodAcknowledge       = pubsubpb.Subscriber_Acknowledge_FullMethodName
	MethodModifyAckDeadline = pubsubpb.Subscriber_ModifyAckDeadline_FullMethodName
	MethodStreamingPull     = pubsubpb.Subscriber_StreamingPull_FullMethodName

	TelemetryHeaderName     = "x-goog-pubsub-client-telemetry"
	RequestParamsHeaderName = "x-goog-request-params"
	TraceparentAttributeKey = "googclient_traceparent"
	TracestateAttributeKey  = "googclient_tracestate"
)

var (
	traceparentRegex = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)
	zeroTraceID      = strings.Repeat("0", 32)
	zeroSpanID       = strings.Repeat("0", 16)
)

// CapturedCall records a single incoming unary RPC, a single StreamingPullRequest
// frame, an unexpected RPC attempt, or a pre-interceptor transport/codec error.
type CapturedCall struct {
	Seq       int64
	Timestamp time.Time
	EndTime   time.Time
	Duration  time.Duration
	Completed bool

	Service    string
	Method     string
	FullMethod string

	StreamID  string
	StreamSeq int64

	InFlightAtStart       int
	MaxConcurrent         int
	KeyInFlightAtStart    int
	KeyMaxConcurrent      int
	ConcurrentKeysAtStart int

	CanceledByClient bool

	Metadata           metadata.MD
	RequestParams      []string
	RawTelemetryHeader string
	Telemetry          *pubsubpb.PubsubClientTelemetry
	TelemetryDecodeErr string

	Compression       string
	HeaderWireBytes   int
	UncompressedBytes int
	CompressedBytes   int
	WireBytes         int

	StatusCode codes.Code
	StatusErr  string
	Unexpected bool
	CodecError string

	PublishReq       *pubsubpb.PublishRequest
	PullReq          *pubsubpb.PullRequest
	StreamingPullReq *pubsubpb.StreamingPullRequest
	AckReq           *pubsubpb.AcknowledgeRequest
	ModAckReq        *pubsubpb.ModifyAckDeadlineRequest
	RawReq           proto.Message

	ResponseMessageIDs      []string
	ResponseErrorInfoDomain string
	ResponseErrorInfoReason string
	// ResponseEODFailures records per-ack_id failure strings from an
	// errdetails.ErrorInfo detail when Domain is EODDomainPubSub and Reason is
	// EODReasonAckFailed or EODReasonModAckFailed. Non-nil (possibly empty) when
	// such an EOD ErrorInfo was returned; nil otherwise.
	ResponseEODFailures map[string]string
}

// Clone returns an independent deep copy of the CapturedCall.
func (c CapturedCall) Clone() CapturedCall {
	out := c
	if c.Metadata != nil {
		out.Metadata = c.Metadata.Copy()
	}
	out.RequestParams = slices.Clone(c.RequestParams)
	out.Telemetry = proto.CloneOf(c.Telemetry)
	out.PublishReq = proto.CloneOf(c.PublishReq)
	out.PullReq = proto.CloneOf(c.PullReq)
	out.StreamingPullReq = proto.CloneOf(c.StreamingPullReq)
	out.AckReq = proto.CloneOf(c.AckReq)
	out.ModAckReq = proto.CloneOf(c.ModAckReq)
	if c.RawReq != nil {
		out.RawReq = proto.Clone(c.RawReq)
	}
	out.ResponseMessageIDs = slices.Clone(c.ResponseMessageIDs)
	out.ResponseEODFailures = maps.Clone(c.ResponseEODFailures)
	return out
}

func isEODErrorInfoReason(reason string) bool {
	return reason == EODReasonAckFailed || reason == EODReasonModAckFailed
}

func (c CapturedCall) hasEODPartialFailure() bool {
	if c.ResponseEODFailures == nil {
		return false
	}
	if c.ResponseErrorInfoDomain != "" && c.ResponseErrorInfoDomain != EODDomainPubSub {
		return false
	}
	if c.ResponseErrorInfoReason != "" && !isEODErrorInfoReason(c.ResponseErrorInfoReason) {
		return false
	}
	return true
}

// EffectiveCompression returns "" when compression is unset or "identity",
// and otherwise returns c.Compression (e.g., "gzip" or "deflate").
func (c CapturedCall) EffectiveCompression() string {
	if c.Compression == "" || strings.EqualFold(c.Compression, "identity") {
		return ""
	}
	return c.Compression
}

// ProtoSize returns the serialized protobuf byte size of the captured request
// proto, or 0 if no typed request proto was decoded.
func (c CapturedCall) ProtoSize() int {
	switch {
	case c.PublishReq != nil:
		return proto.Size(c.PublishReq)
	case c.AckReq != nil:
		return proto.Size(c.AckReq)
	case c.ModAckReq != nil:
		return proto.Size(c.ModAckReq)
	case c.PullReq != nil:
		return proto.Size(c.PullReq)
	case c.StreamingPullReq != nil:
		return proto.Size(c.StreamingPullReq)
	case c.RawReq != nil:
		return proto.Size(c.RawReq)
	default:
		return 0
	}
}

// IsKeepalivePing reports whether this call is a subsequent empty
// StreamingPullRequest keepalive ping (StreamSeq > 0 with all request fields empty/zero).
func (c CapturedCall) IsKeepalivePing() bool {
	return c.FullMethod == MethodStreamingPull &&
		c.StreamSeq > 0 &&
		isKeepalivePingReq(c.StreamingPullReq)
}

// IsStreamAckDeadlineUpdate reports whether this call is a subsequent
// StreamingPullRequest that updates only stream_ack_deadline_seconds.
func (c CapturedCall) IsStreamAckDeadlineUpdate() bool {
	return c.FullMethod == MethodStreamingPull &&
		c.StreamSeq > 0 &&
		isStreamAckDeadlineUpdateReq(c.StreamingPullReq)
}

// OrderingKey inspects PublishReq messages and returns the shared ordering key
// and whether distinct non-empty ordering keys were mixed within the batch.
func (c CapturedCall) OrderingKey() (key string, mixed bool) {
	if c.PublishReq == nil {
		return "", false
	}
	keys := uniquePublishOrderingKeys(c.PublishReq.GetMessages())
	if len(keys) == 0 {
		return "", false
	}
	if len(keys) > 1 {
		return keys[0], true
	}
	// Check whether non-empty ordering key was mixed with empty ordering key.
	for _, m := range c.PublishReq.GetMessages() {
		if m.GetOrderingKey() == "" {
			return keys[0], true
		}
	}
	return keys[0], false
}

// AckIDs returns the ack IDs carried in an Acknowledge, ModifyAckDeadline, or
// StreamingPull frame (when ack_ids are present).
func (c CapturedCall) AckIDs() []string {
	switch {
	case c.AckReq != nil:
		return slices.Clone(c.AckReq.GetAckIds())
	case c.ModAckReq != nil:
		return slices.Clone(c.ModAckReq.GetAckIds())
	case c.StreamingPullReq != nil && len(c.StreamingPullReq.GetAckIds()) > 0:
		return slices.Clone(c.StreamingPullReq.GetAckIds())
	default:
		return nil
	}
}

func isKeepalivePingReq(req *pubsubpb.StreamingPullRequest) bool {
	if req == nil {
		return false
	}
	return req.GetSubscription() == "" &&
		len(req.GetAckIds()) == 0 &&
		len(req.GetModifyDeadlineSeconds()) == 0 &&
		len(req.GetModifyDeadlineAckIds()) == 0 &&
		req.GetStreamAckDeadlineSeconds() == 0 &&
		req.GetClientId() == "" &&
		req.GetMaxOutstandingMessages() == 0 &&
		req.GetMaxOutstandingBytes() == 0 &&
		req.GetProtocolVersion() == 0
}

func isStreamAckDeadlineUpdateReq(req *pubsubpb.StreamingPullRequest) bool {
	if req == nil {
		return false
	}
	return req.GetSubscription() == "" &&
		len(req.GetAckIds()) == 0 &&
		len(req.GetModifyDeadlineSeconds()) == 0 &&
		len(req.GetModifyDeadlineAckIds()) == 0 &&
		req.GetStreamAckDeadlineSeconds() != 0 &&
		req.GetClientId() == "" &&
		req.GetMaxOutstandingMessages() == 0 &&
		req.GetMaxOutstandingBytes() == 0 &&
		req.GetProtocolVersion() == 0
}

func uniquePublishOrderingKeys(msgs []*pubsubpb.PubsubMessage) []string {
	var seen map[string]struct{}
	var out []string
	for _, m := range msgs {
		k := m.GetOrderingKey()
		if k == "" {
			continue
		}
		if seen == nil {
			seen = make(map[string]struct{}, 1)
		}
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			out = append(out, k)
		}
	}
	return out
}

// StreamSummary captures lifecycle metadata and exchanged messages for a single
// StreamingPull session.
type StreamSummary struct {
	StreamID                 string
	Subscription             string
	ClientID                 string
	ProtocolVersion          int64
	StreamAckDeadlineSeconds int32
	MaxOutstandingMessages   int64
	MaxOutstandingBytes      int64

	OpenedAt           time.Time
	ClosedAt           time.Time
	Closed             bool
	HasInitial         bool
	FrameCount         int
	KeepalivePingCount int
	AckIDsSent         []string
	ModAckCount        int

	InitialRequest  *pubsubpb.StreamingPullRequest
	InitialMetadata metadata.MD
	RequestParams   []string
	CloseCode       codes.Code
	CloseErr        string
	SentMessages    []*pubsubpb.ReceivedMessage
}

func (s StreamSummary) clone() StreamSummary {
	out := s
	out.AckIDsSent = slices.Clone(s.AckIDsSent)
	out.InitialRequest = proto.CloneOf(s.InitialRequest)
	if s.InitialMetadata != nil {
		out.InitialMetadata = s.InitialMetadata.Copy()
	}
	out.RequestParams = slices.Clone(s.RequestParams)
	if len(s.SentMessages) > 0 {
		out.SentMessages = make([]*pubsubpb.ReceivedMessage, len(s.SentMessages))
		for i, m := range s.SentMessages {
			out.SentMessages[i] = proto.CloneOf(m)
		}
	}
	return out
}

// Snapshot is an immutable deep-cloned view of the fake server's captured calls,
// StreamingPull sessions, and observed Publish concurrency peaks.
type Snapshot struct {
	Calls                     []CapturedCall
	Streams                   []StreamSummary
	MaxConcurrentPublishTotal int
	MaxConcurrentPublishByKey map[string]int
}

// PublishCalls returns all captured Publish calls in sequence order.
func (s Snapshot) PublishCalls() []CapturedCall {
	return s.CallsForMethod(MethodPublish)
}

// PullCalls returns all captured unary Pull calls in sequence order.
func (s Snapshot) PullCalls() []CapturedCall {
	return s.CallsForMethod(MethodPull)
}

// AckCalls returns all captured Acknowledge calls in sequence order.
func (s Snapshot) AckCalls() []CapturedCall {
	return s.CallsForMethod(MethodAcknowledge)
}

// ModAckCalls returns all captured ModifyAckDeadline calls in sequence order.
func (s Snapshot) ModAckCalls() []CapturedCall {
	return s.CallsForMethod(MethodModifyAckDeadline)
}

// StreamingPullCalls returns all captured StreamingPull frames in sequence order.
func (s Snapshot) StreamingPullCalls() []CapturedCall {
	return s.CallsForMethod(MethodStreamingPull)
}

// UnexpectedCalls returns all calls flagged as unexpected (unregistered, admin,
// schema, or non-data-plane methods).
func (s Snapshot) UnexpectedCalls() []CapturedCall {
	var out []CapturedCall
	for _, c := range s.Calls {
		if c.Unexpected {
			out = append(out, c)
		}
	}
	return out
}

// CallsForMethod returns all captured calls matching fullMethod in sequence order.
func (s Snapshot) CallsForMethod(fullMethod string) []CapturedCall {
	var out []CapturedCall
	for _, c := range s.Calls {
		if c.FullMethod == fullMethod {
			out = append(out, c)
		}
	}
	return out
}

// StreamsForSubscription returns all StreamSummary records whose initial
// subscription equals sub.
func (s Snapshot) StreamsForSubscription(sub string) []StreamSummary {
	var out []StreamSummary
	for _, st := range s.Streams {
		if st.Subscription == sub {
			out = append(out, st)
		}
	}
	return out
}

// DecodeTelemetryHeader extracts and decodes the x-goog-pubsub-client-telemetry
// base64-encoded PubsubClientTelemetry header from incoming gRPC metadata.
func DecodeTelemetryHeader(md metadata.MD) (raw string, decoded *pubsubpb.PubsubClientTelemetry, err error) {
	if len(md) == 0 {
		return "", nil, nil
	}
	vals := md.Get(TelemetryHeaderName)
	if len(vals) == 0 {
		return "", nil, nil
	}
	raw = vals[0]
	if raw == "" {
		return "", nil, fmt.Errorf("%s header is empty", TelemetryHeaderName)
	}
	b, decErr := base64.StdEncoding.DecodeString(raw)
	if decErr != nil {
		b, decErr = base64.RawStdEncoding.DecodeString(raw)
		if decErr != nil {
			return raw, nil, fmt.Errorf("decode base64 %s: %w", TelemetryHeaderName, decErr)
		}
	}
	tel := &pubsubpb.PubsubClientTelemetry{}
	if err := proto.Unmarshal(b, tel); err != nil {
		return raw, nil, fmt.Errorf("unmarshal %s: %w", TelemetryHeaderName, err)
	}
	return raw, tel, nil
}

// ExtractRequestParams returns cloned values of the x-goog-request-params header.
func ExtractRequestParams(md metadata.MD) []string {
	if len(md) == 0 {
		return nil
	}
	return slices.Clone(md.Get(RequestParamsHeaderName))
}

// IsValidTraceparent validates W3C traceparent formatting and ensures non-zero
// trace-id and parent-id (span-id) segments.
func IsValidTraceparent(v string) bool {
	if !traceparentRegex.MatchString(v) {
		return false
	}
	// Format: 00-<32hex>-<16hex>-<2hex>
	traceID := v[3:35]
	spanID := v[36:52]
	return traceID != zeroTraceID && spanID != zeroSpanID
}

// Calls returns an immutable deep-cloned slice of all captured calls in the
// current server epoch.
func (s *Server) Calls() []CapturedCall {
	return s.Snapshot().Calls
}

// Snapshot returns an immutable deep-cloned Snapshot of all captured calls,
// StreamingPull sessions, and Publish concurrency peaks in the current epoch.
func (s *Server) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Server) snapshotLocked() Snapshot {
	calls := make([]CapturedCall, len(s.calls))
	for i, c := range s.calls {
		calls[i] = c.toCaptured()
	}
	streams := make([]StreamSummary, len(s.streamOrder))
	for i, st := range s.streamOrder {
		streams[i] = st.summary()
	}
	return Snapshot{
		Calls:                     calls,
		Streams:                   streams,
		MaxConcurrentPublishTotal: s.maxPublishTotal,
		MaxConcurrentPublishByKey: maps.Clone(s.maxPublishByKey),
	}
}

// WaitFor blocks without polling until pred(s.Snapshot()) returns true or ctx
// expires.
func (s *Server) WaitFor(ctx context.Context, pred func(Snapshot) bool) error {
	for {
		s.mu.Lock()
		snap := s.snapshotLocked()
		ch := s.notifyCh
		s.mu.Unlock()

		if pred(snap) {
			return nil
		}

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// WaitForCalls blocks without polling until pred(s.Calls()) returns true or ctx
// expires.
func (s *Server) WaitForCalls(ctx context.Context, pred func([]CapturedCall) bool) error {
	return s.WaitFor(ctx, func(snap Snapshot) bool {
		return pred(snap.Calls)
	})
}

// WaitForPublish waits until at least count Publish calls have been captured
// and returns them.
func (s *Server) WaitForPublish(ctx context.Context, count int) ([]CapturedCall, error) {
	return s.waitForMethodCalls(ctx, MethodPublish, count)
}

// WaitForAck waits until at least count Acknowledge calls have been captured
// and returns them.
func (s *Server) WaitForAck(ctx context.Context, count int) ([]CapturedCall, error) {
	return s.waitForMethodCalls(ctx, MethodAcknowledge, count)
}

// WaitForModAck waits until at least count ModifyAckDeadline calls have been
// captured and returns them.
func (s *Server) WaitForModAck(ctx context.Context, count int) ([]CapturedCall, error) {
	return s.waitForMethodCalls(ctx, MethodModifyAckDeadline, count)
}

// WaitForPull waits until at least count unary Pull calls have been captured
// and returns them.
func (s *Server) WaitForPull(ctx context.Context, count int) ([]CapturedCall, error) {
	return s.waitForMethodCalls(ctx, MethodPull, count)
}

func (s *Server) waitForMethodCalls(ctx context.Context, fullMethod string, count int) ([]CapturedCall, error) {
	var matched []CapturedCall
	err := s.WaitFor(ctx, func(snap Snapshot) bool {
		calls := snap.CallsForMethod(fullMethod)
		if len(calls) >= count {
			matched = calls
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	return matched, nil
}

// WaitForStreamingPull waits until at least one healthy StreamingPull stream
// has completed frame-0 validation (with no InitialError), sent initial HTTP/2
// response headers, queued any initial SubscriptionProperties/InitialResponses,
// and invoked OnOpen, returning its StreamHandle. Streams rejected at or before
// frame 0 are skipped.
func (s *Server) WaitForStreamingPull(ctx context.Context) (*StreamHandle, error) {
	return s.WaitForStreamingPullN(ctx, 1)
}

// WaitForStreamingPullN waits until at least n healthy StreamingPull streams
// (1-based, in readiness order, skipping streams rejected at or before frame 0)
// have become ready for pushing responses and returns the n-th StreamHandle.
func (s *Server) WaitForStreamingPullN(ctx context.Context, n int) (*StreamHandle, error) {
	if n <= 0 {
		return nil, fmt.Errorf("stream index must be >= 1, got %d", n)
	}
	for {
		s.mu.Lock()
		ch := s.notifyCh
		if len(s.readyStreams) >= n {
			h := s.readyStreams[n-1].handle()
			s.mu.Unlock()
			return h, nil
		}
		s.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
