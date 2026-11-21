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
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// Cloud Pub/Sub protocol limits for Ack and ModifyAckDeadline batches.
const (
	MaxAckIDsPerRequest        = 1000
	MaxAckRequestBytes         = 512 * 1024
	MinLeaseExtensionSeconds   = 10
	MinEODLeaseExtensionSecond = 60
	MaxLeaseExtensionSeconds   = 600
)

// VerificationError aggregates one or more expectation failures with structured
// multi-line diagnostics.
type VerificationError struct {
	Errors []error
}

// Error formats all contained verification failures.
func (e *VerificationError) Error() string {
	if e == nil || len(e.Errors) == 0 {
		return ""
	}
	if len(e.Errors) == 1 {
		return e.Errors[0].Error()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d verification failure(s):", len(e.Errors))
	for i, err := range e.Errors {
		fmt.Fprintf(&b, "\n  [%d] %s", i+1, strings.ReplaceAll(err.Error(), "\n", "\n      "))
	}
	return b.String()
}

// Unwrap returns the underlying error slice for errors.Is / errors.As support.
func (e *VerificationError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return e.Errors
}

// Verifier accumulates multiple assertion checks into a single VerificationError.
type Verifier struct {
	errs []error
}

// NewVerifier creates an empty Verifier accumulator.
func NewVerifier() *Verifier {
	return &Verifier{}
}

// Check records err if non-nil, flattening nested *VerificationError slices.
func (v *Verifier) Check(err error) {
	if err == nil {
		return
	}
	var ve *VerificationError
	if errors.As(err, &ve) && len(ve.Errors) > 0 {
		v.errs = append(v.errs, ve.Errors...)
		return
	}
	v.errs = append(v.errs, err)
}

// Checkf records a formatted error.
func (v *Verifier) Checkf(format string, args ...any) {
	v.errs = append(v.errs, fmt.Errorf(format, args...))
}

// Require records a formatted error if cond is false.
func (v *Verifier) Require(cond bool, format string, args ...any) {
	if !cond {
		v.Checkf(format, args...)
	}
}

// Err returns nil if no failures were recorded, or a *VerificationError.
func (v *Verifier) Err() error {
	if len(v.errs) == 0 {
		return nil
	}
	return &VerificationError{Errors: slices.Clone(v.errs)}
}

// DiffProto returns a human-readable protobuf diff using protocmp.Transform.
func DiffProto(want, got proto.Message) string {
	return cmp.Diff(want, got, protocmp.Transform())
}

// VerifyOptions configures sequence and unordered call matching.
type VerifyOptions struct {
	IgnoreKeepalives               bool
	IgnoreStreamAckDeadlineUpdates bool
}

// DefaultVerifyOptions returns recommended VerifyOptions (ignoring periodic
// keepalive pings by default).
func DefaultVerifyOptions() VerifyOptions {
	return VerifyOptions{
		IgnoreKeepalives: true,
	}
}

// CallExpectation matches and validates a single CapturedCall.
type CallExpectation struct {
	Name       string
	FullMethod string
	Check      func(CapturedCall) error
}

func (e CallExpectation) label() string {
	if e.Name != "" {
		return e.Name
	}
	if e.FullMethod != "" {
		return e.FullMethod
	}
	return "unnamed expectation"
}

func (e CallExpectation) match(c CapturedCall) error {
	if e.FullMethod != "" && c.FullMethod != e.FullMethod {
		return fmt.Errorf("method mismatch: want %s, got %s (seq=%d, status=%s)", e.FullMethod, c.FullMethod, c.Seq, c.StatusCode)
	}
	if e.Check != nil {
		return e.Check(c)
	}
	return nil
}

func filterCalls(calls []CapturedCall, opts VerifyOptions) []CapturedCall {
	out := make([]CapturedCall, 0, len(calls))
	for _, c := range calls {
		if opts.IgnoreKeepalives && c.IsKeepalivePing() {
			continue
		}
		if opts.IgnoreStreamAckDeadlineUpdates && c.IsStreamAckDeadlineUpdate() {
			continue
		}
		out = append(out, c)
	}
	return out
}

func formatCallsSummary(calls []CapturedCall) string {
	if len(calls) == 0 {
		return "(none)"
	}
	var b strings.Builder
	for i, c := range calls {
		fmt.Fprintf(&b, "\n    #%d seq=%d method=%s stream=%s(%d) status=%s unexpected=%t",
			i, c.Seq, c.FullMethod, c.StreamID, c.StreamSeq, c.StatusCode, c.Unexpected)
	}
	return b.String()
}

// AssertNoUnexpectedRPCs fails if any CapturedCall is marked Unexpected, or (when
// allowedMethods is non-empty) if any call has a FullMethod outside allowedMethods.
func AssertNoUnexpectedRPCs(snap Snapshot, allowedMethods ...string) error {
	v := NewVerifier()
	var allowed map[string]struct{}
	if len(allowedMethods) > 0 {
		allowed = make(map[string]struct{}, len(allowedMethods))
		for _, m := range allowedMethods {
			allowed[m] = struct{}{}
		}
	}
	for _, c := range snap.Calls {
		if c.Unexpected {
			v.Checkf("unexpected RPC seq=%d method=%s status=%s err=%q", c.Seq, c.FullMethod, c.StatusCode, c.StatusErr)
			continue
		}
		if allowed != nil {
			if _, ok := allowed[c.FullMethod]; !ok {
				v.Checkf("disallowed RPC seq=%d method=%s (allowed: %v)", c.Seq, c.FullMethod, allowedMethods)
			}
		}
	}
	return v.Err()
}

// AssertNoCallsAfter fails if any CapturedCall occurred with Seq > maxSeq.
func AssertNoCallsAfter(snap Snapshot, maxSeq int64) error {
	v := NewVerifier()
	for _, c := range snap.Calls {
		if c.Seq > maxSeq {
			v.Checkf("unexpected call after seq %d: seq=%d method=%s status=%s", maxSeq, c.Seq, c.FullMethod, c.StatusCode)
		}
	}
	return v.Err()
}

// VerifySequence verifies that snap contains no unexpected RPCs and that the
// filtered call sequence matches want in exact 1-to-1 positional order.
func VerifySequence(snap Snapshot, opts VerifyOptions, want ...CallExpectation) error {
	v := NewVerifier()
	v.Check(AssertNoUnexpectedRPCs(snap))

	got := filterCalls(snap.Calls, opts)
	if len(got) != len(want) {
		v.Checkf("call count mismatch: want %d, got %d%s", len(want), len(got), formatCallsSummary(got))
	}

	limit := min(len(got), len(want))
	for i := range limit {
		if err := want[i].match(got[i]); err != nil {
			v.Checkf("call[%d] (%s) seq=%d: %v", i, want[i].label(), got[i].Seq, err)
		}
	}
	return v.Err()
}

// VerifyUnordered verifies that snap contains no unexpected RPCs and that the
// filtered calls form an exact 1-to-1 bipartite match against want regardless
// of arrival order.
func VerifyUnordered(snap Snapshot, opts VerifyOptions, want ...CallExpectation) error {
	v := NewVerifier()
	v.Check(AssertNoUnexpectedRPCs(snap))

	got := filterCalls(snap.Calls, opts)
	if len(got) != len(want) {
		v.Checkf("unordered call count mismatch: want %d, got %d%s", len(want), len(got), formatCallsSummary(got))
		return v.Err()
	}

	n := len(want)
	if n == 0 {
		return v.Err()
	}

	adj := make([][]int, n)
	errMatrix := make([][]error, n)
	for i := range n {
		errMatrix[i] = make([]error, n)
		for j := range n {
			err := want[i].match(got[j])
			errMatrix[i][j] = err
			if err == nil {
				adj[i] = append(adj[i], j)
			}
		}
	}

	matchGotToWant := make([]int, n)
	for j := range n {
		matchGotToWant[j] = -1
	}

	var dfs func(wantIdx int, seen []bool) bool
	dfs = func(wantIdx int, seen []bool) bool {
		for _, gotIdx := range adj[wantIdx] {
			if seen[gotIdx] {
				continue
			}
			seen[gotIdx] = true
			if matchGotToWant[gotIdx] == -1 || dfs(matchGotToWant[gotIdx], seen) {
				matchGotToWant[gotIdx] = wantIdx
				return true
			}
		}
		return false
	}

	matchedWant := make([]bool, n)
	for i := range n {
		seen := make([]bool, n)
		if dfs(i, seen) {
			matchedWant[i] = true
		}
	}

	allMatched := true
	for i := range n {
		if !matchedWant[i] {
			allMatched = false
			break
		}
	}
	if allMatched {
		return v.Err()
	}

	// Report unmatched expectations and unmatched calls cleanly.
	for i := range n {
		if matchedWant[i] {
			continue
		}
		var details strings.Builder
		for j := range n {
			if matchGotToWant[j] != -1 {
				continue
			}
			fmt.Fprintf(&details, "\n    candidate got[%d] seq=%d (%s): %v", j, got[j].Seq, got[j].FullMethod, errMatrix[i][j])
		}
		v.Checkf("unmatched expectation [%d] %s:%s", i, want[i].label(), details.String())
	}
	for j := range n {
		if matchGotToWant[j] == -1 {
			v.Checkf("unmatched captured call [%d] seq=%d method=%s status=%s", j, got[j].Seq, got[j].FullMethod, got[j].StatusCode)
		}
	}
	return v.Err()
}

// PublishCheck validates one property of a captured Publish call.
type PublishCheck func(CapturedCall) error

// ExpectPublish builds a CallExpectation matching MethodPublish and running checks.
func ExpectPublish(name string, checks ...PublishCheck) CallExpectation {
	return CallExpectation{
		Name:       name,
		FullMethod: MethodPublish,
		Check: func(c CapturedCall) error {
			if c.PublishReq == nil {
				return fmt.Errorf("PublishReq is nil (codecErr=%q, status=%s)", c.CodecError, c.StatusCode)
			}
			v := NewVerifier()
			for _, chk := range checks {
				if chk != nil {
					v.Check(chk(c))
				}
			}
			return v.Err()
		},
	}
}

// PublishTopic checks PublishRequest.Topic.
func PublishTopic(topic string) PublishCheck {
	return func(c CapturedCall) error {
		if got := c.PublishReq.GetTopic(); got != topic {
			return fmt.Errorf("topic mismatch: want %q, got %q", topic, got)
		}
		return nil
	}
}

// PublishBatchSize checks exact len(PublishRequest.Messages).
func PublishBatchSize(n int) PublishCheck {
	return func(c CapturedCall) error {
		if got := len(c.PublishReq.GetMessages()); got != n {
			return fmt.Errorf("batch size mismatch: want %d, got %d", n, got)
		}
		return nil
	}
}

// PublishBatchSizeRange checks len(PublishRequest.Messages) in [minSize, maxSize].
func PublishBatchSizeRange(minSize, maxSize int) PublishCheck {
	return func(c CapturedCall) error {
		got := len(c.PublishReq.GetMessages())
		if got < minSize || got > maxSize {
			return fmt.Errorf("batch size %d outside [%d, %d]", got, minSize, maxSize)
		}
		return nil
	}
}

// ExpectedMessage specifies expected fields for a PubsubMessage in PublishMessages.
type ExpectedMessage struct {
	Data               []byte
	IgnoreData         bool
	CheckData          func([]byte) error
	Attributes         map[string]string
	RequiredAttributes map[string]string
	OrderingKey        string
	RequireTraceparent bool
	Traceparent        string
}

// PublishSingleMessage checks that the PublishRequest contains exactly one message
// matching want.
func PublishSingleMessage(want ExpectedMessage) PublishCheck {
	return PublishMessages(want)
}

// PublishMessages validates PublishRequest.Messages element-by-element against want.
func PublishMessages(want ...ExpectedMessage) PublishCheck {
	copied := slices.Clone(want)
	return func(c CapturedCall) error {
		msgs := c.PublishReq.GetMessages()
		if len(msgs) != len(copied) {
			return fmt.Errorf("message count mismatch: want %d, got %d", len(copied), len(msgs))
		}
		v := NewVerifier()
		for i, w := range copied {
			m := msgs[i]
			if !w.IgnoreData {
				if diff := cmp.Diff(w.Data, m.GetData(), cmpopts.EquateEmpty()); diff != "" {
					v.Checkf("message[%d].data mismatch (-want +got):\n%s", i, diff)
				}
			}
			if w.CheckData != nil {
				if err := w.CheckData(m.GetData()); err != nil {
					v.Checkf("message[%d].data check failed: %v", i, err)
				}
			}
			if gotKey := m.GetOrderingKey(); gotKey != w.OrderingKey {
				v.Checkf("message[%d].ordering_key mismatch: want %q, got %q", i, w.OrderingKey, gotKey)
			}

			gotAttrs := m.GetAttributes()
			if w.Traceparent != "" {
				tp, ok := gotAttrs[TraceparentAttributeKey]
				if !ok {
					v.Checkf("message[%d].attributes missing %q", i, TraceparentAttributeKey)
				} else if tp != w.Traceparent {
					v.Checkf("message[%d].attributes[%q] mismatch: want %q, got %q", i, TraceparentAttributeKey, w.Traceparent, tp)
				}
			} else if w.RequireTraceparent {
				tp, ok := gotAttrs[TraceparentAttributeKey]
				if !ok {
					v.Checkf("message[%d].attributes missing %q", i, TraceparentAttributeKey)
				} else if !IsValidTraceparent(tp) {
					v.Checkf("message[%d].attributes[%q]=%q is not a valid W3C traceparent", i, TraceparentAttributeKey, tp)
				}
			}

			for k, wantVal := range w.RequiredAttributes {
				gotVal, ok := gotAttrs[k]
				if !ok {
					v.Checkf("message[%d].attributes missing required key %q", i, k)
				} else if gotVal != wantVal {
					v.Checkf("message[%d].attributes[%q] mismatch: want %q, got %q", i, k, wantVal, gotVal)
				}
			}

			if w.Attributes != nil {
				gotCmp := maps.Clone(gotAttrs)
				wantCmp := maps.Clone(w.Attributes)
				// When traceparent checking is requested (via RequireTraceparent or
				// Traceparent) without pinning the dynamic value in w.Attributes, validate
				// it above and exclude it (and optional tracestate) from exact map diff.
				if w.RequireTraceparent || w.Traceparent != "" {
					if _, pinned := wantCmp[TraceparentAttributeKey]; !pinned {
						delete(gotCmp, TraceparentAttributeKey)
					}
					if _, pinned := wantCmp[TracestateAttributeKey]; !pinned {
						delete(gotCmp, TracestateAttributeKey)
					}
				}
				if diff := cmp.Diff(wantCmp, gotCmp, cmpopts.EquateEmpty()); diff != "" {
					v.Checkf("message[%d].attributes mismatch (-want +got):\n%s", i, diff)
				}
			}
		}
		return v.Err()
	}
}

// PublishNoMixedOrderingKeys verifies all messages in a Publish batch share the
// same ordering key (either all empty or all identical non-empty).
func PublishNoMixedOrderingKeys() PublishCheck {
	return func(c CapturedCall) error {
		_, mixed := c.OrderingKey()
		if mixed {
			keys := make([]string, len(c.PublishReq.GetMessages()))
			for i, m := range c.PublishReq.GetMessages() {
				keys[i] = m.GetOrderingKey()
			}
			return fmt.Errorf("mixed ordering keys in single Publish batch: %q", keys)
		}
		return nil
	}
}

// PublishCompression verifies incoming wire compression encoding ("gzip", "deflate",
// or "" / "identity" for uncompressed).
func PublishCompression(want string) PublishCheck {
	normWant := want
	if strings.EqualFold(normWant, "identity") {
		normWant = ""
	}
	return func(c CapturedCall) error {
		got := c.EffectiveCompression()
		if got != normWant {
			return fmt.Errorf("compression mismatch: want %q, got %q (raw=%q)", normWant, got, c.Compression)
		}
		return nil
	}
}

// PublishTelemetry verifies x-goog-pubsub-client-telemetry presence and
// PublishOperation fields.
func PublishTelemetry(hedgedAttempt int32, requireStartTime bool) PublishCheck {
	return func(c CapturedCall) error {
		if c.TelemetryDecodeErr != "" {
			return fmt.Errorf("telemetry header decode error: %s (raw=%q)", c.TelemetryDecodeErr, c.RawTelemetryHeader)
		}
		if c.Telemetry == nil || c.Telemetry.GetPublishOperation() == nil {
			return fmt.Errorf("missing %s PublishOperation telemetry (raw=%q)", TelemetryHeaderName, c.RawTelemetryHeader)
		}
		op := c.Telemetry.GetPublishOperation()
		if op.GetHedgedAttemptCount() != hedgedAttempt {
			return fmt.Errorf("telemetry hedged_attempt_count mismatch: want %d, got %d", hedgedAttempt, op.GetHedgedAttemptCount())
		}
		if requireStartTime && op.GetPublishStartTime() == nil {
			return fmt.Errorf("telemetry publish_start_time is nil")
		}
		return nil
	}
}

// PublishRequestParams checks x-goog-request-params matches topic=<topic>.
func PublishRequestParams(topic string, allowUnencoded bool) PublishCheck {
	return checkRequestParams("topic", topic, allowUnencoded)
}

// PublishHeader verifies metadata key mdKey includes wantVal.
func PublishHeader(mdKey, wantVal string) PublishCheck {
	return func(c CapturedCall) error {
		return checkMetadataContains(c.Metadata, mdKey, wantVal)
	}
}

// PublishStatusCode verifies the Publish RPC returned wantCode.
func PublishStatusCode(wantCode codes.Code) PublishCheck {
	return func(c CapturedCall) error {
		if c.StatusCode != wantCode {
			return fmt.Errorf("Publish status mismatch: want %s, got %s (%q)", wantCode, c.StatusCode, c.StatusErr)
		}
		return nil
	}
}

func checkRequestParams(paramKey, resource string, allowUnencoded bool) func(CapturedCall) error {
	wantEncoded := fmt.Sprintf("%s=%s", paramKey, url.QueryEscape(resource))
	wantRaw := fmt.Sprintf("%s=%s", paramKey, resource)
	return func(c CapturedCall) error {
		for _, val := range c.RequestParams {
			for _, part := range strings.Split(val, "&") {
				if part == wantEncoded || (allowUnencoded && part == wantRaw) {
					return nil
				}
			}
		}
		return fmt.Errorf("%s mismatch: want %q (allowUnencoded=%t), got %q", RequestParamsHeaderName, wantEncoded, allowUnencoded, c.RequestParams)
	}
}

func checkMetadataContains(md metadataMap, key, wantVal string) error {
	if md == nil {
		return fmt.Errorf("metadata missing header %q", key)
	}
	vals := md.Get(key)
	if !slices.Contains(vals, wantVal) {
		return fmt.Errorf("metadata header %q mismatch: want value %q in %q", key, wantVal, vals)
	}
	return nil
}

type metadataMap interface {
	Get(k string) []string
}

// AssertOrderedPublishFIFO verifies all successful (codes.OK) Publish messages
// targeting orderingKey arrived strictly in expectedPayloads order without mixed
// ordering keys. Failed Publish calls (StatusCode != codes.OK) are skipped so
// transient-error retries do not introduce false duplicate mismatches; use
// AssertOrderedPublishAttemptedFIFO when every wire attempt must match.
func AssertOrderedPublishFIFO(snap Snapshot, orderingKey string, expectedPayloads [][]byte) error {
	return assertOrderedPublishFIFO(snap, orderingKey, expectedPayloads, true)
}

// AssertOrderedPublishAttemptedFIFO verifies all Publish messages targeting
// orderingKey across every wire attempt (including failed calls) arrived
// strictly in expectedPayloads order without mixed ordering keys.
func AssertOrderedPublishAttemptedFIFO(snap Snapshot, orderingKey string, expectedPayloads [][]byte) error {
	return assertOrderedPublishFIFO(snap, orderingKey, expectedPayloads, false)
}

func assertOrderedPublishFIFO(snap Snapshot, orderingKey string, expectedPayloads [][]byte, okOnly bool) error {
	var got [][]byte
	for _, c := range snap.PublishCalls() {
		if c.PublishReq == nil {
			continue
		}
		key, mixed := c.OrderingKey()
		hasKey := false
		for _, m := range c.PublishReq.GetMessages() {
			if m.GetOrderingKey() == orderingKey {
				hasKey = true
				if !okOnly || (c.Completed && c.StatusCode == codes.OK) {
					got = append(got, m.GetData())
				}
			}
		}
		if hasKey && (mixed || key != orderingKey) {
			return fmt.Errorf("Publish seq=%d mixed ordering keys alongside %q", c.Seq, orderingKey)
		}
	}
	if diff := cmp.Diff(expectedPayloads, got, cmpopts.EquateEmpty()); diff != "" {
		return fmt.Errorf("ordered Publish FIFO mismatch for key %q (-want +got):\n%s", orderingKey, diff)
	}
	return nil
}

// AssertMaxConcurrentPublishForKey verifies both atomic concurrency counters and
// [Timestamp, EndTime] interval overlap for orderingKey never exceeded maxAllowed.
func AssertMaxConcurrentPublishForKey(snap Snapshot, orderingKey string, maxAllowed int) error {
	v := NewVerifier()
	if peak := snap.MaxConcurrentPublishByKey[orderingKey]; peak > maxAllowed {
		v.Checkf("snapshot peak concurrency for ordering_key %q = %d exceeds maxAllowed %d", orderingKey, peak, maxAllowed)
	}
	var matching []CapturedCall
	for _, c := range snap.PublishCalls() {
		if !activeRecHasKey(c.PublishReq, orderingKey) {
			continue
		}
		matching = append(matching, c)
		if c.KeyInFlightAtStart > maxAllowed {
			v.Checkf("Publish seq=%d ordering_key %q had KeyInFlightAtStart=%d > %d", c.Seq, orderingKey, c.KeyInFlightAtStart, maxAllowed)
		}
		if c.KeyMaxConcurrent > maxAllowed {
			v.Checkf("Publish seq=%d ordering_key %q had KeyMaxConcurrent=%d > %d", c.Seq, orderingKey, c.KeyMaxConcurrent, maxAllowed)
		}
	}
	if overlap := maxCallIntervalOverlap(matching); overlap > maxAllowed {
		v.Checkf("Publish interval overlap for ordering_key %q = %d exceeds maxAllowed %d", orderingKey, overlap, maxAllowed)
	}
	return v.Err()
}

// AssertConcurrentPublishesObserved verifies that Publish calls overlapped
// concurrently (and across the specified ordering keys when >= 2 keys are given).
func AssertConcurrentPublishesObserved(snap Snapshot, keys ...string) error {
	pubCalls := snap.PublishCalls()
	if snap.MaxConcurrentPublishTotal < 2 && maxCallIntervalOverlap(pubCalls) < 2 {
		return fmt.Errorf("expected concurrent Publish RPCs, but max concurrent observed was %d", snap.MaxConcurrentPublishTotal)
	}
	if len(keys) < 2 {
		return nil
	}
	keyIntervals := make([][]CapturedCall, len(keys))
	for i, k := range keys {
		for _, c := range pubCalls {
			if activeRecHasKey(c.PublishReq, k) {
				keyIntervals[i] = append(keyIntervals[i], c)
			}
		}
		if len(keyIntervals[i]) == 0 {
			return fmt.Errorf("no Publish calls observed for ordering_key %q", k)
		}
	}
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if !anyIntervalsOverlap(keyIntervals[i], keyIntervals[j]) {
				return fmt.Errorf("ordering keys %q and %q never published concurrently", keys[i], keys[j])
			}
		}
	}
	return nil
}

func maxCallIntervalOverlap(calls []CapturedCall) int {
	if len(calls) == 0 {
		return 0
	}
	const (
		evEnd   = 0 // non-zero interval end (-1)
		evPoint = 1 // zero-duration completed call (point peak check)
		evStart = 2 // non-zero or open interval start (+1)
	)
	type ev struct {
		t    int64
		kind int
	}
	events := make([]ev, 0, len(calls)*2)
	for _, c := range calls {
		start := c.Timestamp.UnixNano()
		if c.Completed && !c.EndTime.IsZero() {
			end := c.EndTime.UnixNano()
			if end <= start {
				events = append(events, ev{t: start, kind: evPoint})
				continue
			}
			// Process non-zero endings before point checks and starts at identical
			// nanosecond timestamps so sequential handoffs don't look like concurrency,
			// while zero-duration calls nested strictly inside an active interval count.
			events = append(events, ev{t: start, kind: evStart})
			events = append(events, ev{t: end, kind: evEnd})
		} else {
			events = append(events, ev{t: start, kind: evStart})
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].t != events[j].t {
			return events[i].t < events[j].t
		}
		return events[i].kind < events[j].kind
	})
	cur, peak := 0, 0
	for _, e := range events {
		switch e.kind {
		case evEnd:
			cur--
		case evPoint:
			if cur+1 > peak {
				peak = cur + 1
			}
		case evStart:
			cur++
			if cur > peak {
				peak = cur
			}
		}
	}
	return peak
}

func anyIntervalsOverlap(a, b []CapturedCall) bool {
	for _, ca := range a {
		for _, cb := range b {
			if intervalsOverlap(ca, cb) {
				return true
			}
		}
	}
	return false
}

func intervalsOverlap(a, b CapturedCall) bool {
	aStart := a.Timestamp
	bStart := b.Timestamp
	// Open intervals (still blocked in flight) overlap any call that started before/during.
	if (!a.Completed || a.EndTime.IsZero()) && (!b.Completed || b.EndTime.IsZero()) {
		return true
	}
	if !a.Completed || a.EndTime.IsZero() {
		return !b.EndTime.Before(aStart) && b.EndTime.After(aStart)
	}
	if !b.Completed || b.EndTime.IsZero() {
		return !a.EndTime.Before(bStart) && a.EndTime.After(bStart)
	}
	return aStart.Before(b.EndTime) && bStart.Before(a.EndTime)
}

// AssertTelemetryHedgeSequence verifies the HedgedAttemptCount values observed
// across Publish calls and optionally that all share identical PublishStartTime.
func AssertTelemetryHedgeSequence(snap Snapshot, wantAttempts []int32, requireSameStartTime bool) error {
	pubCalls := snap.PublishCalls()
	if len(pubCalls) != len(wantAttempts) {
		return fmt.Errorf("Publish count mismatch: want %d, got %d", len(wantAttempts), len(pubCalls))
	}
	v := NewVerifier()
	var firstStart proto.Message
	for i, c := range pubCalls {
		if c.TelemetryDecodeErr != "" {
			v.Checkf("Publish[%d] seq=%d telemetry decode error: %s", i, c.Seq, c.TelemetryDecodeErr)
			continue
		}
		op := c.Telemetry.GetPublishOperation()
		if op == nil {
			v.Checkf("Publish[%d] seq=%d missing PublishOperation telemetry", i, c.Seq)
			continue
		}
		if op.GetHedgedAttemptCount() != wantAttempts[i] {
			v.Checkf("Publish[%d] seq=%d hedged_attempt_count mismatch: want %d, got %d", i, c.Seq, wantAttempts[i], op.GetHedgedAttemptCount())
		}
		if requireSameStartTime {
			if op.GetPublishStartTime() == nil {
				v.Checkf("Publish[%d] seq=%d publish_start_time is nil", i, c.Seq)
			} else if firstStart == nil {
				firstStart = op.GetPublishStartTime()
			} else if diff := DiffProto(firstStart, op.GetPublishStartTime()); diff != "" {
				v.Checkf("Publish[%d] seq=%d publish_start_time differs from attempt 0 (-want +got):\n%s", i, c.Seq, diff)
			}
		}
	}
	return v.Err()
}

// AckCheck validates one property of a captured Acknowledge call.
type AckCheck func(CapturedCall) error

// ExpectAck builds a CallExpectation matching MethodAcknowledge.
func ExpectAck(name string, checks ...AckCheck) CallExpectation {
	return CallExpectation{
		Name:       name,
		FullMethod: MethodAcknowledge,
		Check: func(c CapturedCall) error {
			if c.AckReq == nil {
				return fmt.Errorf("AckReq is nil (status=%s)", c.StatusCode)
			}
			v := NewVerifier()
			for _, chk := range checks {
				if chk != nil {
					v.Check(chk(c))
				}
			}
			return v.Err()
		},
	}
}

// AckSubscription checks AcknowledgeRequest.Subscription.
func AckSubscription(sub string) AckCheck {
	return func(c CapturedCall) error {
		if got := c.AckReq.GetSubscription(); got != sub {
			return fmt.Errorf("subscription mismatch: want %q, got %q", sub, got)
		}
		return nil
	}
}

// AckIDsExact checks exact ordered AckIds equality.
func AckIDsExact(ids ...string) AckCheck {
	want := slices.Clone(ids)
	return func(c CapturedCall) error {
		if diff := cmp.Diff(want, c.AckReq.GetAckIds(), cmpopts.EquateEmpty()); diff != "" {
			return fmt.Errorf("ack_ids mismatch (-want +got):\n%s", diff)
		}
		return nil
	}
}

// AckIDsUnordered checks AckIds multiset equality ignoring order.
func AckIDsUnordered(ids ...string) AckCheck {
	want := slices.Clone(ids)
	sort.Strings(want)
	return func(c CapturedCall) error {
		got := slices.Clone(c.AckReq.GetAckIds())
		sort.Strings(got)
		if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
			return fmt.Errorf("ack_ids unordered mismatch (-want +got):\n%s", diff)
		}
		return nil
	}
}

// AckWithinBounds verifies 1 <= len(AckIds) <= 1000 and proto.Size <= 512 KiB.
func AckWithinBounds() AckCheck {
	return func(c CapturedCall) error {
		n := len(c.AckReq.GetAckIds())
		if n == 0 || n > MaxAckIDsPerRequest {
			return fmt.Errorf("Acknowledge ack_ids count %d outside [1, %d]", n, MaxAckIDsPerRequest)
		}
		if sz := proto.Size(c.AckReq); sz > MaxAckRequestBytes {
			return fmt.Errorf("Acknowledge request size %d exceeds %d bytes", sz, MaxAckRequestBytes)
		}
		return nil
	}
}

// AckRequestParams checks x-goog-request-params matches subscription=<sub>.
func AckRequestParams(sub string, allowUnencoded bool) AckCheck {
	return checkRequestParams("subscription", sub, allowUnencoded)
}

// AckStatusCode checks the Acknowledge RPC status code.
func AckStatusCode(want codes.Code) AckCheck {
	return func(c CapturedCall) error {
		if c.StatusCode != want {
			return fmt.Errorf("Acknowledge status mismatch: want %s, got %s (%q)", want, c.StatusCode, c.StatusErr)
		}
		return nil
	}
}

// ModAckCheck validates one property of a captured ModifyAckDeadline call.
type ModAckCheck func(CapturedCall) error

// ExpectModAck builds a CallExpectation matching MethodModifyAckDeadline.
func ExpectModAck(name string, checks ...ModAckCheck) CallExpectation {
	return CallExpectation{
		Name:       name,
		FullMethod: MethodModifyAckDeadline,
		Check: func(c CapturedCall) error {
			if c.ModAckReq == nil {
				return fmt.Errorf("ModAckReq is nil (status=%s)", c.StatusCode)
			}
			v := NewVerifier()
			for _, chk := range checks {
				if chk != nil {
					v.Check(chk(c))
				}
			}
			return v.Err()
		},
	}
}

// ModAckSubscription checks ModifyAckDeadlineRequest.Subscription.
func ModAckSubscription(sub string) ModAckCheck {
	return func(c CapturedCall) error {
		if got := c.ModAckReq.GetSubscription(); got != sub {
			return fmt.Errorf("subscription mismatch: want %q, got %q", sub, got)
		}
		return nil
	}
}

// ModAckIDsExact checks exact ordered AckIds equality on ModifyAckDeadline.
func ModAckIDsExact(ids ...string) ModAckCheck {
	want := slices.Clone(ids)
	return func(c CapturedCall) error {
		if diff := cmp.Diff(want, c.ModAckReq.GetAckIds(), cmpopts.EquateEmpty()); diff != "" {
			return fmt.Errorf("modack ack_ids mismatch (-want +got):\n%s", diff)
		}
		return nil
	}
}

// ModAckIDsUnordered checks multiset AckIds equality on ModifyAckDeadline.
func ModAckIDsUnordered(ids ...string) ModAckCheck {
	want := slices.Clone(ids)
	sort.Strings(want)
	return func(c CapturedCall) error {
		got := slices.Clone(c.ModAckReq.GetAckIds())
		sort.Strings(got)
		if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
			return fmt.Errorf("modack ack_ids unordered mismatch (-want +got):\n%s", diff)
		}
		return nil
	}
}

// ModAckDeadline checks exact ModifyAckDeadlineRequest.AckDeadlineSeconds (use 0 for nack).
func ModAckDeadline(sec int32) ModAckCheck {
	return func(c CapturedCall) error {
		if got := c.ModAckReq.GetAckDeadlineSeconds(); got != sec {
			return fmt.Errorf("ack_deadline_seconds mismatch: want %d, got %d", sec, got)
		}
		return nil
	}
}

// ModAckDeadlineRange checks ModifyAckDeadlineRequest.AckDeadlineSeconds in [minSec, maxSec].
func ModAckDeadlineRange(minSec, maxSec int32) ModAckCheck {
	return func(c CapturedCall) error {
		got := c.ModAckReq.GetAckDeadlineSeconds()
		if got < minSec || got > maxSec {
			return fmt.Errorf("ack_deadline_seconds %d outside [%d, %d]", got, minSec, maxSec)
		}
		return nil
	}
}

// ModAckWithinBounds verifies 1 <= len(AckIds) <= 1000 and proto.Size <= 512 KiB.
func ModAckWithinBounds() ModAckCheck {
	return func(c CapturedCall) error {
		n := len(c.ModAckReq.GetAckIds())
		if n == 0 || n > MaxAckIDsPerRequest {
			return fmt.Errorf("ModifyAckDeadline ack_ids count %d outside [1, %d]", n, MaxAckIDsPerRequest)
		}
		if sz := proto.Size(c.ModAckReq); sz > MaxAckRequestBytes {
			return fmt.Errorf("ModifyAckDeadline request size %d exceeds %d bytes", sz, MaxAckRequestBytes)
		}
		return nil
	}
}

// ModAckRequestParams checks x-goog-request-params matches subscription=<sub>.
func ModAckRequestParams(sub string, allowUnencoded bool) ModAckCheck {
	return checkRequestParams("subscription", sub, allowUnencoded)
}

// ModAckStatusCode checks the ModifyAckDeadline RPC status code.
func ModAckStatusCode(want codes.Code) ModAckCheck {
	return func(c CapturedCall) error {
		if c.StatusCode != want {
			return fmt.Errorf("ModifyAckDeadline status mismatch: want %s, got %s (%q)", want, c.StatusCode, c.StatusErr)
		}
		return nil
	}
}

// AssertAllAckedIDs verifies the multiset/set of ack_ids acknowledged across
// Acknowledge RPCs and StreamingPullRequest frames matches wantIDs.
func AssertAllAckedIDs(snap Snapshot, wantIDs ...string) error {
	var got []string
	for _, c := range snap.Calls {
		switch {
		case c.AckReq != nil:
			got = append(got, c.AckReq.GetAckIds()...)
		case c.StreamingPullReq != nil && len(c.StreamingPullReq.GetAckIds()) > 0:
			got = append(got, c.StreamingPullReq.GetAckIds()...)
		}
	}
	return compareIDSet("acked", wantIDs, got)
}

// AssertAllNackedIDs verifies the set of ack_ids nacked (deadline == 0) across
// ModifyAckDeadline RPCs and StreamingPullRequest frames matches wantIDs.
func AssertAllNackedIDs(snap Snapshot, wantIDs ...string) error {
	var got []string
	for _, c := range snap.Calls {
		switch {
		case c.ModAckReq != nil && c.ModAckReq.GetAckDeadlineSeconds() == 0:
			got = append(got, c.ModAckReq.GetAckIds()...)
		case c.StreamingPullReq != nil && len(c.StreamingPullReq.GetModifyDeadlineAckIds()) > 0:
			secs := c.StreamingPullReq.GetModifyDeadlineSeconds()
			for i, id := range c.StreamingPullReq.GetModifyDeadlineAckIds() {
				if i < len(secs) && secs[i] == 0 {
					got = append(got, id)
				}
			}
		}
	}
	return compareIDSet("nacked", wantIDs, got)
}

func compareIDSet(kind string, wantIDs, gotIDs []string) error {
	want := uniqueSortedStrings(wantIDs)
	got := uniqueSortedStrings(gotIDs)
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		return fmt.Errorf("%s ack_ids set mismatch (-want +got):\n%s", kind, diff)
	}
	return nil
}

func uniqueSortedStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// AssertAckAndModAckWithinBounds verifies every unary Acknowledge, unary
// ModifyAckDeadline, and StreamingPull request frame obeys Pub/Sub limits
// (equal ModifyDeadline slice lengths, <= 1000 IDs, <= 512 KiB, non-nack
// deadlines in [10, 600], and non-zero StreamAckDeadlineSeconds in [10, 600]).
func AssertAckAndModAckWithinBounds(snap Snapshot) error {
	v := NewVerifier()
	for _, c := range snap.Calls {
		switch {
		case c.AckReq != nil:
			if err := AckWithinBounds()(c); err != nil {
				v.Checkf("seq=%d: %v", c.Seq, err)
			}
		case c.ModAckReq != nil:
			if err := ModAckWithinBounds()(c); err != nil {
				v.Checkf("seq=%d: %v", c.Seq, err)
			}
			dl := c.ModAckReq.GetAckDeadlineSeconds()
			if dl != 0 && (dl < MinLeaseExtensionSeconds || dl > MaxLeaseExtensionSeconds) {
				v.Checkf("seq=%d: ModifyAckDeadline ack_deadline_seconds=%d outside 0 or [%d, %d]", c.Seq, dl, MinLeaseExtensionSeconds, MaxLeaseExtensionSeconds)
			}
		case c.StreamingPullReq != nil:
			req := c.StreamingPullReq
			if len(req.GetModifyDeadlineAckIds()) != len(req.GetModifyDeadlineSeconds()) {
				v.Checkf("seq=%d: StreamingPull modify_deadline_ack_ids length (%d) != modify_deadline_seconds length (%d)",
					c.Seq, len(req.GetModifyDeadlineAckIds()), len(req.GetModifyDeadlineSeconds()))
			}
			if len(req.GetAckIds()) > MaxAckIDsPerRequest {
				v.Checkf("seq=%d: StreamingPull ack_ids count %d > %d", c.Seq, len(req.GetAckIds()), MaxAckIDsPerRequest)
			}
			if len(req.GetModifyDeadlineAckIds()) > MaxAckIDsPerRequest {
				v.Checkf("seq=%d: StreamingPull modify_deadline_ack_ids count %d > %d", c.Seq, len(req.GetModifyDeadlineAckIds()), MaxAckIDsPerRequest)
			}
			totalIDs := len(req.GetAckIds()) + len(req.GetModifyDeadlineAckIds())
			if totalIDs > MaxAckIDsPerRequest && len(req.GetAckIds()) <= MaxAckIDsPerRequest && len(req.GetModifyDeadlineAckIds()) <= MaxAckIDsPerRequest {
				v.Checkf("seq=%d: StreamingPull combined ack/modack ID count %d > %d", c.Seq, totalIDs, MaxAckIDsPerRequest)
			}
			if sz := proto.Size(req); sz > MaxAckRequestBytes {
				v.Checkf("seq=%d: StreamingPull request size %d > %d", c.Seq, sz, MaxAckRequestBytes)
			}
			if sdl := req.GetStreamAckDeadlineSeconds(); sdl != 0 && (sdl < MinLeaseExtensionSeconds || sdl > MaxLeaseExtensionSeconds) {
				v.Checkf("seq=%d: StreamingPull stream_ack_deadline_seconds=%d outside 0 or [%d, %d]", c.Seq, sdl, MinLeaseExtensionSeconds, MaxLeaseExtensionSeconds)
			}
			for i, dl := range req.GetModifyDeadlineSeconds() {
				if dl != 0 && (dl < MinLeaseExtensionSeconds || dl > MaxLeaseExtensionSeconds) {
					v.Checkf("seq=%d: StreamingPull modify_deadline_seconds[%d]=%d outside 0 or [%d, %d]", c.Seq, i, dl, MinLeaseExtensionSeconds, MaxLeaseExtensionSeconds)
				}
			}
		}
	}
	return v.Err()
}

// AssertEODSplitRetry verifies that after a completed EOD-failing Ack or
// ModAck call carrying the mixed batch (containing all permFailedIDs,
// transientRetriedIDs, and initiallySucceededIDs, with ResponseEODFailures
// matching EOD ErrorInfo semantics), subsequent retry calls of the same RPC
// method (and matching nack vs lease-extension semantics for ModifyAckDeadline)
// never resend permFailedIDs, initiallySucceededIDs, or already-recovered
// transient IDs, never repeat duplicate ack_ids within a single call, and
// recover every ID in transientRetriedIDs via either a completed codes.OK retry
// call or omission from ResponseEODFailures on a completed EOD ErrorInfo retry
// call (supporting staggered multi-round partial recovery).
//
// Note: For ModifyAckDeadline, laterCalls filters to calls with matching
// (AckDeadlineSeconds == 0) semantics so a subsequent nack does not collide with
// a lease-extension retry check; long-lived tests that expect periodic lease
// extensions of initiallySucceededIDs after settlement should snapshot right
// after settlement or pass nil for initiallySucceededIDs.
func AssertEODSplitRetry(snap Snapshot, permFailedIDs, transientRetriedIDs, initiallySucceededIDs []string) error {
	var ackOrModAck []CapturedCall
	for _, c := range snap.Calls {
		if c.AckReq != nil || c.ModAckReq != nil {
			ackOrModAck = append(ackOrModAck, c)
		}
	}
	if len(ackOrModAck) == 0 {
		return fmt.Errorf("no Acknowledge or ModifyAckDeadline calls observed")
	}

	containsAll := func(got, want []string) bool {
		for _, id := range want {
			if !slices.Contains(got, id) {
				return false
			}
		}
		return true
	}

	firstFailIdx := -1
	for i, c := range ackOrModAck {
		if !c.Completed || c.StatusCode == codes.OK || !c.hasEODPartialFailure() {
			continue
		}
		ids := c.AckIDs()
		if containsAll(ids, permFailedIDs) && containsAll(ids, transientRetriedIDs) && containsAll(ids, initiallySucceededIDs) {
			firstFailIdx = i
			break
		}
	}
	if firstFailIdx == -1 {
		return fmt.Errorf("expected a completed EOD-failing Ack/ModAck call containing all permFailedIDs %v, transientRetriedIDs %v, and initiallySucceededIDs %v, none found",
			permFailedIDs, transientRetriedIDs, initiallySucceededIDs)
	}

	failCall := ackOrModAck[firstFailIdx]
	var laterCalls []CapturedCall
	for _, c := range ackOrModAck[firstFailIdx+1:] {
		if !sameEODRetryScope(c, failCall) {
			continue
		}
		laterCalls = append(laterCalls, c)
	}
	if len(laterCalls) == 0 && len(transientRetriedIDs) > 0 {
		return fmt.Errorf("expected retry calls for transient IDs %v after failing call seq=%d (%s), found none", transientRetriedIDs, failCall.Seq, failCall.FullMethod)
	}

	v := NewVerifier()
	checkIntraCallDuplicateAckIDs(v, failCall, "initial failing call ")
	for _, c := range ackOrModAck[:firstFailIdx] {
		if sameEODRetryScope(c, failCall) {
			checkIntraCallDuplicateAckIDs(v, c, "pre-split call ")
		}
	}

	for _, id := range permFailedIDs {
		if _, ok := failCall.ResponseEODFailures[id]; !ok {
			v.Checkf("initial failing call seq=%d EOD ErrorInfo missing permanent ack_id %q", failCall.Seq, id)
		}
	}
	for _, id := range transientRetriedIDs {
		if _, ok := failCall.ResponseEODFailures[id]; !ok {
			v.Checkf("initial failing call seq=%d EOD ErrorInfo missing transient ack_id %q", failCall.Seq, id)
		}
	}
	for _, id := range initiallySucceededIDs {
		if reason, ok := failCall.ResponseEODFailures[id]; ok {
			v.Checkf("initial failing call seq=%d EOD ErrorInfo unexpectedly failed initiallySucceededID %q (%q)", failCall.Seq, id, reason)
		}
	}

	permSet := toStringSet(permFailedIDs)
	initOKSet := toStringSet(initiallySucceededIDs)
	seenLater := make(map[string]bool)
	recovered := make(map[string]bool)
	for _, c := range laterCalls {
		ids := c.AckIDs()
		seenInCall := make(map[string]bool, len(ids))
		reportedDup := make(map[string]bool)
		for _, id := range ids {
			if seenInCall[id] {
				if !reportedDup[id] {
					reportedDup[id] = true
					v.Checkf("duplicate ack_id %q in seq=%d (%s)", id, c.Seq, c.FullMethod)
				}
				continue
			}
			seenInCall[id] = true
			seenLater[id] = true
			if permSet[id] {
				v.Checkf("permanently failed ack_id %q was improperly retried in seq=%d (%s)", id, c.Seq, c.FullMethod)
			}
			if initOKSet[id] {
				v.Checkf("already-succeeded ack_id %q was improperly retried in seq=%d (%s)", id, c.Seq, c.FullMethod)
			}
			if recovered[id] {
				v.Checkf("already-recovered transient ack_id %q was improperly retried in seq=%d (%s)", id, c.Seq, c.FullMethod)
			}
		}
		if c.Completed {
			if c.StatusCode == codes.OK {
				for _, id := range ids {
					recovered[id] = true
				}
			} else if c.hasEODPartialFailure() {
				for _, id := range ids {
					if _, failed := c.ResponseEODFailures[id]; !failed {
						recovered[id] = true
					}
				}
			}
		}
	}
	for _, id := range transientRetriedIDs {
		if !seenLater[id] {
			v.Checkf("transient ack_id %q was not retried after initial failure seq=%d", id, failCall.Seq)
		} else if !recovered[id] {
			v.Checkf("transient ack_id %q was retried after seq=%d but never recovered via OK or EOD omission", id, failCall.Seq)
		}
	}
	return v.Err()
}

func sameEODRetryScope(c, failCall CapturedCall) bool {
	if c.FullMethod != failCall.FullMethod {
		return false
	}
	cIsAck, failIsAck := c.AckReq != nil, failCall.AckReq != nil
	cIsModAck, failIsModAck := c.ModAckReq != nil, failCall.ModAckReq != nil
	if cIsAck != failIsAck || cIsModAck != failIsModAck {
		return false
	}
	if failIsModAck {
		failIsNack := failCall.ModAckReq.GetAckDeadlineSeconds() == 0
		return (c.ModAckReq.GetAckDeadlineSeconds() == 0) == failIsNack
	}
	return true
}

func checkIntraCallDuplicateAckIDs(v *Verifier, c CapturedCall, prefix string) {
	ids := c.AckIDs()
	seenInCall := make(map[string]bool, len(ids))
	reportedDup := make(map[string]bool)
	for _, id := range ids {
		if seenInCall[id] {
			if !reportedDup[id] {
				reportedDup[id] = true
				v.Checkf("%sduplicate ack_id %q in seq=%d (%s)", prefix, id, c.Seq, c.FullMethod)
			}
			continue
		}
		seenInCall[id] = true
	}
}

func toStringSet(ids []string) map[string]bool {
	if len(ids) == 0 {
		return nil
	}
	out := make(map[string]bool, len(ids))
	for _, id := range ids {
		out[id] = true
	}
	return out
}

// PullCheck validates one property of a captured unary Pull call.
type PullCheck func(CapturedCall) error

// ExpectPull builds a CallExpectation matching MethodPull.
func ExpectPull(name string, checks ...PullCheck) CallExpectation {
	return CallExpectation{
		Name:       name,
		FullMethod: MethodPull,
		Check: func(c CapturedCall) error {
			if c.PullReq == nil {
				return fmt.Errorf("PullReq is nil (status=%s)", c.StatusCode)
			}
			v := NewVerifier()
			for _, chk := range checks {
				if chk != nil {
					v.Check(chk(c))
				}
			}
			return v.Err()
		},
	}
}

// PullSubscription checks PullRequest.Subscription.
func PullSubscription(sub string) PullCheck {
	return func(c CapturedCall) error {
		if got := c.PullReq.GetSubscription(); got != sub {
			return fmt.Errorf("subscription mismatch: want %q, got %q", sub, got)
		}
		return nil
	}
}

// PullMaxMessages checks PullRequest.MaxMessages.
func PullMaxMessages(n int32) PullCheck {
	return func(c CapturedCall) error {
		if got := c.PullReq.GetMaxMessages(); got != n {
			return fmt.Errorf("max_messages mismatch: want %d, got %d", n, got)
		}
		return nil
	}
}

// PullRequestParams checks x-goog-request-params matches subscription=<sub>.
func PullRequestParams(sub string, allowUnencoded bool) PullCheck {
	return checkRequestParams("subscription", sub, allowUnencoded)
}

// PullStatusCode checks the unary Pull RPC status code.
func PullStatusCode(wantCode codes.Code) PullCheck {
	return func(c CapturedCall) error {
		if c.StatusCode != wantCode {
			return fmt.Errorf("Pull status mismatch: want %s, got %s (%q)", wantCode, c.StatusCode, c.StatusErr)
		}
		return nil
	}
}

// StreamingPullInitialExpectation configures strict checks on frame 0 of a
// StreamingPull stream.
type StreamingPullInitialExpectation struct {
	Subscription                string
	StreamAckDeadlineMin        int32
	StreamAckDeadlineMax        int32
	MaxOutstandingMessages      *int64
	MaxOutstandingBytes         *int64
	ClientID                    string
	RequireClientID             bool
	RequireUUIDClientID         bool
	ProtocolVersion             *int64
	CheckRequestParams          bool
	AllowUnencodedRequestParams bool
}

// ExpectStreamingPullInitial builds a CallExpectation validating initial frame 0
// of a StreamingPull stream.
func ExpectStreamingPullInitial(name string, want StreamingPullInitialExpectation) CallExpectation {
	return CallExpectation{
		Name:       name,
		FullMethod: MethodStreamingPull,
		Check: func(c CapturedCall) error {
			if c.StreamSeq != 0 {
				return fmt.Errorf("expected initial frame (StreamSeq=0), got StreamSeq=%d", c.StreamSeq)
			}
			req := c.StreamingPullReq
			if req == nil {
				return fmt.Errorf("initial StreamingPullReq is nil (status=%s, err=%q)", c.StatusCode, c.StatusErr)
			}
			v := NewVerifier()
			if want.Subscription != "" && req.GetSubscription() != want.Subscription {
				v.Checkf("subscription mismatch: want %q, got %q", want.Subscription, req.GetSubscription())
			}
			if want.StreamAckDeadlineMin > 0 || want.StreamAckDeadlineMax > 0 {
				dl := req.GetStreamAckDeadlineSeconds()
				if dl < want.StreamAckDeadlineMin || (want.StreamAckDeadlineMax > 0 && dl > want.StreamAckDeadlineMax) {
					v.Checkf("stream_ack_deadline_seconds=%d outside [%d, %d]", dl, want.StreamAckDeadlineMin, want.StreamAckDeadlineMax)
				}
			}
			if want.MaxOutstandingMessages != nil && req.GetMaxOutstandingMessages() != *want.MaxOutstandingMessages {
				v.Checkf("max_outstanding_messages mismatch: want %d, got %d", *want.MaxOutstandingMessages, req.GetMaxOutstandingMessages())
			}
			if want.MaxOutstandingBytes != nil && req.GetMaxOutstandingBytes() != *want.MaxOutstandingBytes {
				v.Checkf("max_outstanding_bytes mismatch: want %d, got %d", *want.MaxOutstandingBytes, req.GetMaxOutstandingBytes())
			}
			cid := req.GetClientId()
			if want.ClientID != "" && cid != want.ClientID {
				v.Checkf("client_id mismatch: want %q, got %q", want.ClientID, cid)
			}
			if (want.RequireClientID || want.RequireUUIDClientID) && cid == "" {
				v.Checkf("client_id is empty")
			}
			if want.RequireUUIDClientID && cid != "" {
				if _, err := uuid.Parse(cid); err != nil {
					v.Checkf("client_id %q is not a valid UUID: %v", cid, err)
				}
			}
			if want.ProtocolVersion != nil && req.GetProtocolVersion() != *want.ProtocolVersion {
				v.Checkf("protocol_version mismatch: want %d, got %d", *want.ProtocolVersion, req.GetProtocolVersion())
			}
			if want.CheckRequestParams && want.Subscription != "" {
				v.Check(checkRequestParams("subscription", want.Subscription, want.AllowUnencodedRequestParams)(c))
			}
			return v.Err()
		},
	}
}

// ExpectStreamingPullKeepalive builds a CallExpectation matching a keepalive ping frame.
func ExpectStreamingPullKeepalive(name string) CallExpectation {
	return CallExpectation{
		Name:       name,
		FullMethod: MethodStreamingPull,
		Check: func(c CapturedCall) error {
			if !c.IsKeepalivePing() {
				return fmt.Errorf("expected keepalive ping frame (StreamSeq>0 empty request), got seq=%d streamSeq=%d req=%v", c.Seq, c.StreamSeq, c.StreamingPullReq)
			}
			return nil
		},
	}
}

// AssertConsistentClientIDs checks that all StreamingPull streams with an
// initial frame share the exact same non-empty ClientID (and valid UUID if requireUUID).
func AssertConsistentClientIDs(snap Snapshot, requireUUID bool) error {
	var first string
	var count int
	for _, st := range snap.Streams {
		if !st.HasInitial {
			continue
		}
		count++
		if st.ClientID == "" {
			return fmt.Errorf("stream %s had empty client_id", st.StreamID)
		}
		if requireUUID {
			if _, err := uuid.Parse(st.ClientID); err != nil {
				return fmt.Errorf("stream %s client_id %q is not a valid UUID: %v", st.StreamID, st.ClientID, err)
			}
		}
		if first == "" {
			first = st.ClientID
		} else if st.ClientID != first {
			return fmt.Errorf("inconsistent client_id across streams: stream %s sent %q, expected %q", st.StreamID, st.ClientID, first)
		}
	}
	if count == 0 {
		return fmt.Errorf("no StreamingPull streams with initial request observed")
	}
	return nil
}

// AssertKeepalivePings verifies at least minCount keepalive pings were received.
func AssertKeepalivePings(snap Snapshot, minCount int) error {
	var total int
	for _, c := range snap.StreamingPullCalls() {
		if c.IsKeepalivePing() {
			total++
		}
	}
	if total < minCount {
		return fmt.Errorf("expected at least %d keepalive ping(s), got %d", minCount, total)
	}
	return nil
}
