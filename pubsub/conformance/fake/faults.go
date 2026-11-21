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
	"bytes"
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// RepeatAlways configures a scripted rule to remain active for all matching
// calls in the current server epoch until Server.Reset or Server.Close.
const RepeatAlways = -1

// EOD ErrorInfo domain, reasons, and failure status strings per Cloud Pub/Sub
// exactly-once delivery semantics.
const (
	EODDomainPubSub       = "pubsub.googleapis.com"
	EODReasonAckFailed    = "EXACTLY_ONCE_ACK_FAILED"
	EODReasonModAckFailed = "EXACTLY_ONCE_MODACK_FAILED"

	EODTransientUnorderedAckID = "TRANSIENT_FAILURE_UNORDERED_ACK_ID"
	EODTransientInvalidAckID   = "TRANSIENT_FAILURE_INVALID_ACK_ID"
	EODTransientOther          = "TRANSIENT_FAILURE_OTHER"

	EODPermanentInvalidAckID     = "PERMANENT_FAILURE_INVALID_ACK_ID"
	EODPermanentPermissionDenied = "PERMANENT_FAILURE_PERMISSION_DENIED"
	EODPermanentOther            = "PERMANENT_FAILURE_OTHER"
)

// Gate blocks matching RPC handlers outside Server.mu until released or
// canceled by the caller context or server epoch reset/close.
type Gate struct {
	mu         sync.Mutex
	entered    int
	notifyCh   chan struct{}
	releaseCh  chan struct{}
	releaseErr error
	once       sync.Once
}

// NewGate creates a new closed Gate that blocks entering RPC handlers until
// Release or ReleaseWithError is called.
func NewGate() *Gate {
	return &Gate{
		notifyCh:  make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
}

// EnteredCount returns how many RPC handlers have reached this Gate so far.
func (g *Gate) EnteredCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.entered
}

// WaitEntered blocks until at least one RPC handler has entered the Gate or ctx
// expires.
func (g *Gate) WaitEntered(ctx context.Context) error {
	return g.WaitEnteredN(ctx, 1)
}

// WaitEnteredN blocks until at least n RPC handlers have entered the Gate or
// ctx expires.
func (g *Gate) WaitEnteredN(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	for {
		g.mu.Lock()
		if g.entered >= n {
			g.mu.Unlock()
			return nil
		}
		ch := g.notifyCh
		g.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Release unblocks all handlers waiting on the Gate with a nil error. It is
// safe to call multiple times or before any handler enters.
func (g *Gate) Release() {
	g.ReleaseWithError(nil)
}

// ReleaseWithError unblocks all handlers waiting on the Gate with err. It is
// safe to call multiple times or before any handler enters; only the first
// release takes effect.
func (g *Gate) ReleaseWithError(err error) {
	g.once.Do(func() {
		g.releaseErr = err
		close(g.releaseCh)
	})
}

func (g *Gate) wait(ctx, epCtx context.Context) error {
	g.mu.Lock()
	g.entered++
	close(g.notifyCh)
	g.notifyCh = make(chan struct{})
	g.mu.Unlock()

	select {
	case <-g.releaseCh:
		return g.releaseErr
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-epCtx.Done():
		return status.Error(codes.Unavailable, "fake server reset or closed")
	}
}

// PublishMatchContext provides request and header context to PublishRule matchers.
type PublishMatchContext struct {
	// Attempt is the 1-based Publish RPC index in the current epoch.
	Attempt   int
	Request   *pubsubpb.PublishRequest
	Metadata  metadata.MD
	Telemetry *pubsubpb.PubsubClientTelemetry
}

// PublishMatchFunc reports whether a PublishRule applies to a Publish call.
//
// Callbacks run while Server holds its internal mutex and MUST be non-reentrant
// (do not call any *Server methods such as Calls, Snapshot, Reset, Add*Rule, or Push*).
type PublishMatchFunc func(*PublishMatchContext) bool

// PublishRule controls how matching Publish RPCs behave.
//
// Times defaults to 1 (run once) when zero. Set Times to RepeatAlways (-1) to
// apply indefinitely until Server.Reset.
// When MessageIDs is non-nil (including an explicit empty slice []string{}), it
// overrides auto-generated message IDs verbatim.
type PublishRule struct {
	Name                string
	Match               PublishMatchFunc
	Times               int
	Gate                *Gate
	Delay               time.Duration
	Err                 error
	MessageIDs          []string
	MessageIDCountDelta int
	Response            *pubsubpb.PublishResponse
	ResponseFunc        func(context.Context, *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error)
}

// MatchTopic matches Publish calls targeting topic.
func MatchTopic(topic string) PublishMatchFunc {
	return func(mc *PublishMatchContext) bool {
		return mc.Request != nil && mc.Request.GetTopic() == topic
	}
}

// MatchOrderingKey matches Publish calls whose messages use orderingKey.
// If orderingKey is non-empty, it matches when any message has that key. If
// orderingKey is empty, it matches when all messages have an empty key.
func MatchOrderingKey(orderingKey string) PublishMatchFunc {
	return func(mc *PublishMatchContext) bool {
		if mc.Request == nil {
			return false
		}
		msgs := mc.Request.GetMessages()
		if orderingKey == "" {
			for _, m := range msgs {
				if m.GetOrderingKey() != "" {
					return false
				}
			}
			return true
		}
		for _, m := range msgs {
			if m.GetOrderingKey() == orderingKey {
				return true
			}
		}
		return false
	}
}

// MatchHedgedAttempt matches Publish calls with the given hedged attempt count
// from x-goog-pubsub-client-telemetry. For n == 0, requests without telemetry
// are also treated as primary attempt 0.
func MatchHedgedAttempt(n int32) PublishMatchFunc {
	return func(mc *PublishMatchContext) bool {
		op := mc.Telemetry.GetPublishOperation()
		if op == nil {
			return n == 0
		}
		return op.GetHedgedAttemptCount() == n
	}
}

// MatchHedgedAttemptGTE matches Publish calls with hedged_attempt_count >= n.
func MatchHedgedAttemptGTE(n int32) PublishMatchFunc {
	return func(mc *PublishMatchContext) bool {
		op := mc.Telemetry.GetPublishOperation()
		if op == nil {
			return n <= 0
		}
		return op.GetHedgedAttemptCount() >= n
	}
}

// MatchHasTelemetry matches Publish calls that carry a decoded PublishOperation
// telemetry header.
func MatchHasTelemetry() PublishMatchFunc {
	return func(mc *PublishMatchContext) bool {
		return mc.Telemetry != nil && mc.Telemetry.GetPublishOperation() != nil
	}
}

// MatchCallAttempt matches the n-th Publish RPC in the current epoch (1-based).
func MatchCallAttempt(n int) PublishMatchFunc {
	return func(mc *PublishMatchContext) bool {
		return mc.Attempt == n
	}
}

// MatchPayloadContains matches Publish calls where any message Data contains sub.
func MatchPayloadContains(sub []byte) PublishMatchFunc {
	needle := slices.Clone(sub)
	return func(mc *PublishMatchContext) bool {
		if mc.Request == nil {
			return false
		}
		for _, m := range mc.Request.GetMessages() {
			if bytes.Contains(m.GetData(), needle) {
				return true
			}
		}
		return false
	}
}

// MatchPublishAll combines PublishMatchFuncs with logical AND.
func MatchPublishAll(fns ...PublishMatchFunc) PublishMatchFunc {
	copied := slices.Clone(fns)
	return func(mc *PublishMatchContext) bool {
		for _, fn := range copied {
			if fn != nil && !fn(mc) {
				return false
			}
		}
		return true
	}
}

// MatchPublishAny combines PublishMatchFuncs with logical OR.
func MatchPublishAny(fns ...PublishMatchFunc) PublishMatchFunc {
	copied := slices.Clone(fns)
	return func(mc *PublishMatchContext) bool {
		for _, fn := range copied {
			if fn != nil && fn(mc) {
				return true
			}
		}
		return false
	}
}

// EODErrorStatus constructs a gRPC status error carrying an errdetails.ErrorInfo
// payload with Domain "pubsub.googleapis.com" and Reason "EXACTLY_ONCE_ACK_FAILED".
// If code is codes.OK, codes.InvalidArgument is used so gRPC transmits details on the wire.
func EODErrorStatus(code codes.Code, msg string, ackIDErrors map[string]string) error {
	return EODErrorStatusWithReason(code, msg, EODReasonAckFailed, ackIDErrors)
}

// EODModAckErrorStatus constructs a gRPC status error carrying an errdetails.ErrorInfo
// payload with Domain "pubsub.googleapis.com" and Reason "EXACTLY_ONCE_MODACK_FAILED".
func EODModAckErrorStatus(code codes.Code, msg string, ackIDErrors map[string]string) error {
	return EODErrorStatusWithReason(code, msg, EODReasonModAckFailed, ackIDErrors)
}

// EODErrorStatusWithReason constructs a gRPC status error carrying an errdetails.ErrorInfo
// payload with Domain "pubsub.googleapis.com" and the supplied reason and metadata.
func EODErrorStatusWithReason(code codes.Code, msg, reason string, ackIDErrors map[string]string) error {
	if code == codes.OK {
		code = codes.InvalidArgument
	}
	if reason == "" {
		reason = EODReasonAckFailed
	}
	md := maps.Clone(ackIDErrors)
	if md == nil {
		md = map[string]string{}
	}
	st, err := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{
		Domain:   EODDomainPubSub,
		Reason:   reason,
		Metadata: md,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "failed to attach ErrorInfo: %v", err)
	}
	return st.Err()
}

// EODAckBehavior defines how a single ack_id behaves in an EODScript.
type EODAckBehavior struct {
	// PermanentReason, if non-empty, always fails this ack_id with that reason
	// (e.g., EODPermanentInvalidAckID).
	PermanentReason string
	// TransientReason is emitted for the first TransientTimes RPC calls that
	// include this ack_id (defaults to EODTransientOther when TransientTimes > 0).
	TransientReason string
	// TransientTimes specifies how many times this ack_id transiently fails
	// before succeeding on subsequent retries.
	TransientTimes int
}

// EODScript simulates stateful per-ack_id EOD partial-batch retry behavior
// across Acknowledge or ModifyAckDeadline attempts.
type EODScript struct {
	Code      codes.Code
	Message   string
	Reason    string
	Behaviors map[string]EODAckBehavior

	mu            sync.Mutex
	transientSeen map[string]int
}

// NewEODScript creates an EODScript with a cloned behavior map.
func NewEODScript(code codes.Code, msg, reason string, behaviors map[string]EODAckBehavior) *EODScript {
	return &EODScript{
		Code:          code,
		Message:       msg,
		Reason:        reason,
		Behaviors:     maps.Clone(behaviors),
		transientSeen: make(map[string]int),
	}
}

func (s *EODScript) evaluate(ackIDs []string, defaultReason string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transientSeen == nil {
		s.transientSeen = make(map[string]int)
	}
	seen := make(map[string]struct{}, len(ackIDs))
	errMap := make(map[string]string)
	for _, id := range ackIDs {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		b, ok := s.Behaviors[id]
		if !ok {
			continue
		}
		if b.PermanentReason != "" {
			errMap[id] = b.PermanentReason
			continue
		}
		if b.TransientTimes > 0 && s.transientSeen[id] < b.TransientTimes {
			s.transientSeen[id]++
			reason := b.TransientReason
			if reason == "" {
				reason = EODTransientOther
			}
			errMap[id] = reason
		}
	}
	if len(errMap) == 0 {
		return nil
	}
	reason := s.Reason
	if reason == "" {
		reason = defaultReason
	}
	msg := s.Message
	if msg == "" {
		msg = "exactly-once delivery failure"
	}
	return EODErrorStatusWithReason(s.Code, msg, reason, errMap)
}

// AckMatchContext provides context to AckRule matchers.
type AckMatchContext struct {
	Attempt  int
	Request  *pubsubpb.AcknowledgeRequest
	Metadata metadata.MD
}

// AckMatchFunc reports whether an AckRule matches an Acknowledge call.
// Callbacks run under Server's internal mutex and MUST NOT call *Server methods.
type AckMatchFunc func(*AckMatchContext) bool

// AckRule controls fault/delay injection for Acknowledge RPCs.
// Times defaults to 1 when zero; set RepeatAlways (-1) for unlimited uses.
type AckRule struct {
	Name  string
	Match AckMatchFunc
	Times int
	Gate  *Gate
	Delay time.Duration
	Err   error
	EOD   *EODScript
}

// MatchAckID matches Acknowledge calls containing ackID.
func MatchAckID(ackID string) AckMatchFunc {
	return func(mc *AckMatchContext) bool {
		return mc.Request != nil && slices.Contains(mc.Request.GetAckIds(), ackID)
	}
}

// MatchAckIDsContaining matches Acknowledge calls containing all specified ackIDs.
func MatchAckIDsContaining(ackIDs ...string) AckMatchFunc {
	want := slices.Clone(ackIDs)
	return func(mc *AckMatchContext) bool {
		if mc.Request == nil {
			return false
		}
		got := mc.Request.GetAckIds()
		for _, id := range want {
			if !slices.Contains(got, id) {
				return false
			}
		}
		return true
	}
}

// MatchAckAttempt matches the n-th Acknowledge RPC in the current epoch (1-based).
func MatchAckAttempt(n int) AckMatchFunc {
	return func(mc *AckMatchContext) bool {
		return mc.Attempt == n
	}
}

// ModAckMatchContext provides context to ModAckRule matchers.
type ModAckMatchContext struct {
	Attempt  int
	Request  *pubsubpb.ModifyAckDeadlineRequest
	Metadata metadata.MD
}

// ModAckMatchFunc reports whether a ModAckRule matches a ModifyAckDeadline call.
// Callbacks run under Server's internal mutex and MUST NOT call *Server methods.
type ModAckMatchFunc func(*ModAckMatchContext) bool

// ModAckRule controls fault/delay injection for ModifyAckDeadline RPCs.
// Times defaults to 1 when zero; set RepeatAlways (-1) for unlimited uses.
type ModAckRule struct {
	Name  string
	Match ModAckMatchFunc
	Times int
	Gate  *Gate
	Delay time.Duration
	Err   error
	EOD   *EODScript
}

// MatchModAckID matches ModifyAckDeadline calls containing ackID.
func MatchModAckID(ackID string) ModAckMatchFunc {
	return func(mc *ModAckMatchContext) bool {
		return mc.Request != nil && slices.Contains(mc.Request.GetAckIds(), ackID)
	}
}

// MatchModAckIDsContaining matches ModifyAckDeadline calls containing all ackIDs.
func MatchModAckIDsContaining(ackIDs ...string) ModAckMatchFunc {
	want := slices.Clone(ackIDs)
	return func(mc *ModAckMatchContext) bool {
		if mc.Request == nil {
			return false
		}
		got := mc.Request.GetAckIds()
		for _, id := range want {
			if !slices.Contains(got, id) {
				return false
			}
		}
		return true
	}
}

// MatchModAckDeadline matches ModifyAckDeadline calls with ack_deadline_seconds == sec.
func MatchModAckDeadline(sec int32) ModAckMatchFunc {
	return func(mc *ModAckMatchContext) bool {
		return mc.Request != nil && mc.Request.GetAckDeadlineSeconds() == sec
	}
}

// MatchModAckAttempt matches the n-th ModifyAckDeadline RPC in the current epoch (1-based).
func MatchModAckAttempt(n int) ModAckMatchFunc {
	return func(mc *ModAckMatchContext) bool {
		return mc.Attempt == n
	}
}

// PullMatchContext provides context to PullRule matchers.
type PullMatchContext struct {
	Attempt  int
	Request  *pubsubpb.PullRequest
	Metadata metadata.MD
}

// PullMatchFunc reports whether a PullRule matches a unary Pull call.
// Callbacks run under Server's internal mutex and MUST NOT call *Server methods.
type PullMatchFunc func(*PullMatchContext) bool

// PullRule controls scripted responses and faults for unary Pull RPCs.
// Times defaults to 1 when zero; set RepeatAlways (-1) for unlimited uses.
type PullRule struct {
	Name         string
	Match        PullMatchFunc
	Times        int
	Gate         *Gate
	Delay        time.Duration
	Err          error
	Response     *pubsubpb.PullResponse
	ResponseFunc func(context.Context, *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error)
}

// StreamingPullScript controls scripted initial behavior, fault injection, and
// callbacks for matching StreamingPull streams.
// Times defaults to 1 when zero; set RepeatAlways (-1) for unlimited uses.
// MatchConnect and Match run under Server's internal mutex and MUST be
// non-reentrant (do not invoke any *Server methods from them).
type StreamingPullScript struct {
	Name                   string
	MatchConnect           func(attempt int, md metadata.MD) bool
	Match                  func(attempt int, initial *pubsubpb.StreamingPullRequest, md metadata.MD) bool
	Times                  int
	FailBeforeRecvErr      error
	InitialError           error
	InitialResponses       []*pubsubpb.StreamingPullResponse
	AfterInitialError      error
	SuppressPong           *bool
	SubscriptionProperties *pubsubpb.StreamingPullResponse_SubscriptionProperties
	OnOpen                 func(*StreamHandle)
	OnRequest              func(*StreamHandle, *pubsubpb.StreamingPullRequest)
	OnClose                func(*StreamHandle, error)
}

type ruleEntry[T any] struct {
	rule T
	rem  int
}

func initialRuleRemaining(times int) int {
	if times == 0 {
		return 1
	}
	if times < 0 {
		return RepeatAlways
	}
	return times
}

func (e *ruleEntry[T]) active() bool {
	return e.rem == RepeatAlways || e.rem > 0
}

func (e *ruleEntry[T]) consume() {
	if e.rem > 0 {
		e.rem--
	}
}

func waitRuleDelay(ctx, epCtx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-epCtx.Done():
		return status.Error(codes.Unavailable, "fake server reset or closed")
	}
}
