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

package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	goInstrumentationScopeName  = "cloud.google.com/go/pubsub/v2"
	cppInstrumentationScopeName = "gl-cpp"
)

// TraceStore holds captured OpenTelemetry TraceSpans in memory for ResetTraces / DrainTraces.
type TraceStore struct {
	mu    sync.Mutex
	cfg   *ProfileConfig
	spans []*proxypb.TraceSpan
}

// NewTraceStore creates a TraceStore bound to the proxy ProfileConfig.
func NewTraceStore(cfg *ProfileConfig) *TraceStore {
	return &TraceStore{cfg: cfg}
}

// Reset clears all buffered spans.
func (ts *TraceStore) Reset() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.spans = nil
}

// Drain returns and clears all buffered spans.
func (ts *TraceStore) Drain() []*proxypb.TraceSpan {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if !ts.cfg.SupportsMonitoring || ts.cfg.HasFault("missing-traces") {
		ts.spans = nil
		return nil
	}
	out := make([]*proxypb.TraceSpan, len(ts.spans))
	for i, s := range ts.spans {
		out[i] = proto.Clone(s).(*proxypb.TraceSpan)
	}
	ts.spans = nil
	return out
}

// Record appends a completed span to the buffer when monitoring is active.
func (ts *TraceStore) Record(enabled bool, span *proxypb.TraceSpan) {
	if span == nil || !enabled || !ts.cfg.SupportsMonitoring || ts.cfg.HasFault("missing-traces") {
		return
	}
	clone := proto.Clone(span).(*proxypb.TraceSpan)
	if clone.EndTime == nil {
		clone.EndTime = timestamppb.Now()
	}
	ts.applyFaults(clone)
	ts.mu.Lock()
	ts.spans = append(ts.spans, clone)
	ts.mu.Unlock()
}

func (ts *TraceStore) applyFaults(span *proxypb.TraceSpan) {
	if ts.cfg.HasFault("bad-span-name") {
		span.Name = "bad.span"
	}
	if ts.cfg.HasFault("bad-span-kind") {
		span.Kind = proxypb.SpanKind_SPAN_KIND_CLIENT
	}
	if ts.cfg.HasFault("missing-links") {
		span.Links = nil
	}
	if ts.cfg.HasFault("bad-status-code") {
		if span.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			span.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
		} else {
			span.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		}
	}
}

func (ts *TraceStore) scopeName() string {
	if ts.cfg.CppSpans {
		return cppInstrumentationScopeName
	}
	return goInstrumentationScopeName
}

func (ts *TraceStore) scopeVersion() string {
	return ts.cfg.Version
}

// newTraceID generates a 32 lowercase hex character W3C trace ID.
func newTraceID() string {
	var b [16]byte
	for {
		if _, err := rand.Read(b[:]); err == nil {
			allZero := true
			for _, x := range b {
				if x != 0 {
					allZero = false
					break
				}
			}
			if !allZero {
				return hex.EncodeToString(b[:])
			}
		}
	}
}

// newSpanID generates a 16 lowercase hex character W3C span ID.
func newSpanID() string {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err == nil {
			allZero := true
			for _, x := range b {
				if x != 0 {
					allZero = false
					break
				}
			}
			if !allZero {
				return hex.EncodeToString(b[:])
			}
		}
	}
}

func formatTraceparent(traceID, spanID string, flags uint32) string {
	if traceID == "" || spanID == "" {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-%02x", traceID, spanID, flags&0xff)
}

func parseTraceparent(tp string) (traceID, spanID string, flags uint32, ok bool) {
	parts := strings.Split(strings.TrimSpace(tp), "-")
	if len(parts) != 4 {
		return "", "", 0, false
	}
	if len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", "", 0, false
	}
	if parts[0] == "ff" {
		return "", "", 0, false
	}
	tidBytes, err := hex.DecodeString(parts[1])
	if err != nil || isAllZero(tidBytes) {
		return "", "", 0, false
	}
	sidBytes, err := hex.DecodeString(parts[2])
	if err != nil || isAllZero(sidBytes) {
		return "", "", 0, false
	}
	flagBytes, err := hex.DecodeString(parts[3])
	if err != nil || len(flagBytes) != 1 {
		return "", "", 0, false
	}
	return strings.ToLower(parts[1]), strings.ToLower(parts[2]), uint32(flagBytes[0]), true
}

func isAllZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

func attrStr(v string) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringValue{StringValue: v}}
}

func attrInt(v int64) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
}

func attrBool(v bool) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: v}}
}

func newSpanBase(ts *TraceStore, traceID, parentSpanID, name string, kind proxypb.SpanKind, flags uint32, traceState string) *proxypb.TraceSpan {
	if traceID == "" {
		traceID = newTraceID()
		flags = 1
	}
	return &proxypb.TraceSpan{
		TraceId:                     traceID,
		SpanId:                      newSpanID(),
		ParentSpanId:                parentSpanID,
		TraceFlags:                  flags,
		TraceState:                  traceState,
		Name:                        name,
		Kind:                        kind,
		StartTime:                   timestamppb.Now(),
		Attributes:                  make(map[string]*proxypb.AttributeValue),
		StatusCode:                  proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET,
		InstrumentationScopeName:    ts.scopeName(),
		InstrumentationScopeVersion: ts.scopeVersion(),
	}
}

func addSpanEvent(span *proxypb.TraceSpan, name string, attrs map[string]*proxypb.AttributeValue) {
	if span == nil {
		return
	}
	ev := &proxypb.SpanEvent{
		Name:       name,
		Timestamp:  timestamppb.Now(),
		Attributes: attrs,
	}
	span.Events = append(span.Events, ev)
}

func recordSpanError(span *proxypb.TraceSpan, errMsg string) {
	if span == nil {
		return
	}
	span.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
	span.StatusMessage = errMsg
	addSpanEvent(span, "exception", map[string]*proxypb.AttributeValue{
		"exception.message": attrStr(errMsg),
	})
}

func grpcCodeToCppStatusString(code codes.Code) string {
	switch code {
	case codes.OK:
		return "OK"
	case codes.Canceled:
		return "CANCELLED"
	case codes.Unknown:
		return "UNKNOWN"
	case codes.InvalidArgument:
		return "INVALID_ARGUMENT"
	case codes.DeadlineExceeded:
		return "DEADLINE_EXCEEDED"
	case codes.NotFound:
		return "NOT_FOUND"
	case codes.AlreadyExists:
		return "ALREADY_EXISTS"
	case codes.PermissionDenied:
		return "PERMISSION_DENIED"
	case codes.ResourceExhausted:
		return "RESOURCE_EXHAUSTED"
	case codes.FailedPrecondition:
		return "FAILED_PRECONDITION"
	case codes.Aborted:
		return "ABORTED"
	case codes.OutOfRange:
		return "OUT_OF_RANGE"
	case codes.Unimplemented:
		return "UNIMPLEMENTED"
	case codes.Internal:
		return "INTERNAL"
	case codes.Unavailable:
		return "UNAVAILABLE"
	case codes.DataLoss:
		return "DATA_LOSS"
	case codes.Unauthenticated:
		return "UNAUTHENTICATED"
	default:
		return "UNKNOWN"
	}
}

func recordCppSpanStatus(span *proxypb.TraceSpan, code codes.Code, errMsg string) {
	if span == nil {
		return
	}
	if code == codes.OK && errMsg != "" {
		code = codes.Unknown
	}
	span.Attributes["gl-cpp.status_code"] = attrStr(grpcCodeToCppStatusString(code))
	if code == codes.OK {
		span.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
		return
	}
	span.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
	span.StatusMessage = errMsg
	span.Attributes["gl-cpp.error.message"] = attrStr(errMsg)
}

func hasInvalidResourceChars(s string) bool {
	if !utf8.ValidString(s) {
		return true
	}
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return true
		}
	}
	return false
}

func extractResourceNames(fullName, resourceType, projectOverride string) (projectID, shortID, normalizedFull string, err error) {
	if fullName == "" {
		return "", "", "", fmt.Errorf("empty %s resource name", resourceType)
	}
	if hasInvalidResourceChars(fullName) {
		return "", "", "", fmt.Errorf("invalid %s resource name %q: must not contain whitespace, control characters, or invalid UTF-8", resourceType, fullName)
	}
	if projectOverride != "" {
		if strings.Contains(projectOverride, "/") {
			return "", "", "", fmt.Errorf("invalid project_id %q: must not contain '/'", projectOverride)
		}
		if hasInvalidResourceChars(projectOverride) || projectOverride == "." || projectOverride == ".." {
			return "", "", "", fmt.Errorf("invalid project_id %q: must not contain whitespace, control characters, invalid UTF-8, or dot segments", projectOverride)
		}
	}
	if strings.Contains(fullName, "/") {
		parts := strings.Split(fullName, "/")
		if len(parts) != 4 || parts[0] != "projects" || parts[1] == "" || parts[1] == "." || parts[1] == ".." || parts[2] != resourceType || parts[3] == "" || parts[3] == "." || parts[3] == ".." {
			return "", "", "", fmt.Errorf("invalid resource name %q: expected projects/<project>/%s/<id>", fullName, resourceType)
		}
		projectID = parts[1]
		if projectOverride != "" {
			projectID = projectOverride
		}
		shortID = parts[3]
		normalizedFull = fullName
		return projectID, shortID, normalizedFull, nil
	}
	if projectOverride == "" {
		return "", "", "", fmt.Errorf("short %s name %q requires non-empty project_id", resourceType, fullName)
	}
	if fullName == "." || fullName == ".." {
		return "", "", "", fmt.Errorf("invalid resource name %q: invalid dot segment", fullName)
	}
	projectID = projectOverride
	shortID = fullName
	normalizedFull = fmt.Sprintf("projects/%s/%s/%s", projectID, resourceType, shortID)
	return projectID, shortID, normalizedFull, nil
}

func messageEnvelopeSize(data []byte, attrs map[string]string, orderingKey, msgID string) int64 {
	size := int64(20 + len(data) + len(msgID) + len(orderingKey))
	for k, v := range attrs {
		size += int64(len(k) + len(v))
	}
	return size
}

// publishSpanBundle builds the canonical Go (or C++ delta) create/fc/batching spans for one Publish call.
type publishMessageTrace struct {
	mu          sync.Mutex
	enabled     bool
	store       *TraceStore
	createSpan  *proxypb.TraceSpan
	fcSpan      *proxypb.TraceSpan
	batchSpan   *proxypb.TraceSpan
	createEnded bool
	batchEnded  bool
	fcEnded     bool
	cppMode     bool
}

func newPublishMessageTrace(store *TraceStore, enabled bool, projectID, fullTopic, topicID string, data []byte, attrs map[string]string, orderingKey, parentTP, parentTS string, useBlocking bool) *publishMessageTrace {
	t := &publishMessageTrace{
		enabled: enabled && store.cfg.SupportsMonitoring && !store.cfg.HasFault("missing-traces"),
		store:   store,
		cppMode: store.cfg.CppSpans,
	}
	if !t.enabled {
		return t
	}
	var traceID, parentSpanID, traceState string
	var flags uint32 = 1
	if tid, sid, f, ok := parseTraceparent(parentTP); ok && !store.cfg.HasFault("missing-traceparent") {
		traceID = tid
		parentSpanID = sid
		flags = f
		traceState = parentTS
	}
	cs := newSpanBase(store, traceID, parentSpanID, topicID+" create", proxypb.SpanKind_SPAN_KIND_PRODUCER, flags, traceState)
	cs.Attributes["gcp.project_id"] = attrStr(projectID)
	cs.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	cs.Attributes["messaging.destination.name"] = attrStr(topicID)
	if t.cppMode {
		fn := "pubsub::PublisherConnection::Publish"
		if useBlocking {
			fn = "pubsub::BlockingPublisher::Publish"
		}
		cs.Attributes["code.function.name"] = attrStr(fn)
		cs.Attributes["messaging.operation.type"] = attrStr("create")
		cs.Attributes["messaging.message.envelope.size"] = attrInt(messageEnvelopeSize(data, attrs, orderingKey, ""))
		if orderingKey != "" {
			cs.Attributes["messaging.gcp_pubsub.message.ordering_key"] = attrStr(orderingKey)
		}
	} else {
		cs.Attributes["gcp.resource.name"] = attrStr(fullTopic)
		cs.Attributes["code.function"] = attrStr("Publish")
		cs.Attributes["messaging.message.body.size"] = attrInt(int64(len(data)))
		cs.Attributes["messaging.gcp_pubsub.message.ordering_key"] = attrStr(orderingKey)
		cs.Attributes["messaging.message.id"] = attrStr("")
	}

	t.createSpan = cs
	return t
}

func (t *publishMessageTrace) traceparentHeader() (traceparent, tracestate string) {
	if !t.enabled || t.store.cfg.HasFault("missing-traceparent") {
		return "", ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.createSpan == nil {
		return "", ""
	}
	return formatTraceparent(t.createSpan.TraceId, t.createSpan.SpanId, t.createSpan.TraceFlags), t.createSpan.TraceState
}

func (t *publishMessageTrace) startFlowControl() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.createEnded || t.createSpan == nil {
		return
	}
	kind := proxypb.SpanKind_SPAN_KIND_INTERNAL
	if t.cppMode {
		kind = proxypb.SpanKind_SPAN_KIND_CLIENT
	}
	fc := newSpanBase(t.store, t.createSpan.TraceId, t.createSpan.SpanId, "publisher flow control", kind, t.createSpan.TraceFlags, t.createSpan.TraceState)
	if t.cppMode {
		fc.Attributes["code.function.name"] = attrStr("pubsub::FlowControlledPublisherConnection::Publish")
	}
	t.fcSpan = fc
	t.createSpan.ChildSpanCount++
}

func (t *publishMessageTrace) endFlowControl(errMsg string) {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	if t.fcEnded || t.fcSpan == nil {
		t.mu.Unlock()
		return
	}
	t.fcEnded = true
	fc := t.fcSpan
	t.fcSpan = nil
	if t.cppMode {
		recordCppSpanStatus(fc, codes.OK, "")
	} else if errMsg != "" {
		recordSpanError(fc, errMsg)
	}
	fc.EndTime = timestamppb.Now()
	rec := proto.Clone(fc).(*proxypb.TraceSpan)
	t.mu.Unlock()
	t.store.Record(t.enabled, rec)
}

func (t *publishMessageTrace) startBatching() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.createEnded || t.createSpan == nil {
		return
	}
	kind := proxypb.SpanKind_SPAN_KIND_INTERNAL
	if t.cppMode {
		kind = proxypb.SpanKind_SPAN_KIND_CLIENT
	}
	bs := newSpanBase(t.store, t.createSpan.TraceId, t.createSpan.SpanId, "publisher batching", kind, t.createSpan.TraceFlags, t.createSpan.TraceState)
	if t.cppMode {
		bs.Attributes["code.function.name"] = attrStr("pubsub::BatchingPublisherConnection::Publish")
		addSpanEvent(t.createSpan, "gl-cpp.added_to_batch", nil)
	}
	t.batchSpan = bs
	t.createSpan.ChildSpanCount++
}

func (t *publishMessageTrace) endBatching() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	if t.batchEnded || t.batchSpan == nil {
		t.mu.Unlock()
		return
	}
	t.batchEnded = true
	bs := t.batchSpan
	t.batchSpan = nil
	if t.cppMode {
		recordCppSpanStatus(bs, codes.OK, "")
	}
	bs.EndTime = timestamppb.Now()
	rec := proto.Clone(bs).(*proxypb.TraceSpan)
	t.mu.Unlock()
	t.store.Record(t.enabled, rec)
}

func (t *publishMessageTrace) addEvent(name string, attrs map[string]*proxypb.AttributeValue) {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.createEnded || t.createSpan == nil {
		return
	}
	if t.cppMode {
		switch name {
		case "publish start":
			name = "gl-cpp.publish_start"
		case "publish end":
			name = "gl-cpp.publish_end"
		}
	}
	addSpanEvent(t.createSpan, name, attrs)
}

func (t *publishMessageTrace) linkToPublishSpan(pubSpan *proxypb.TraceSpan) {
	if !t.enabled || pubSpan == nil || t.store.cfg.HasFault("missing-links") {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.createEnded || t.createSpan == nil {
		return
	}
	if t.cppMode {
		t.createSpan.Links = append(t.createSpan.Links, &proxypb.SpanLink{
			TraceId:    pubSpan.TraceId,
			SpanId:     pubSpan.SpanId,
			TraceFlags: pubSpan.TraceFlags,
			TraceState: pubSpan.TraceState,
		})
	} else {
		t.createSpan.Links = append(t.createSpan.Links, &proxypb.SpanLink{
			TraceId:    pubSpan.TraceId,
			SpanId:     pubSpan.SpanId,
			TraceFlags: pubSpan.TraceFlags,
			TraceState: pubSpan.TraceState,
			Attributes: map[string]*proxypb.AttributeValue{
				"messaging.operation.name": attrStr("publish"),
			},
		})
	}
	pubSpan.Links = append(pubSpan.Links, &proxypb.SpanLink{
		TraceId:    t.createSpan.TraceId,
		SpanId:     t.createSpan.SpanId,
		TraceFlags: t.createSpan.TraceFlags,
		TraceState: t.createSpan.TraceState,
	})
}

func (t *publishMessageTrace) endCreate(msgID string, errCode codes.Code, errMsg string) {
	if !t.enabled {
		return
	}
	t.endBatching()
	t.mu.Lock()
	if t.createEnded || t.createSpan == nil {
		t.mu.Unlock()
		return
	}
	t.createEnded = true
	cs := t.createSpan
	if msgID != "" {
		cs.Attributes["messaging.message.id"] = attrStr(msgID)
	}
	if t.cppMode {
		if errMsg != "" && errCode == codes.OK {
			errCode = codes.Unknown
		}
		recordCppSpanStatus(cs, errCode, errMsg)
	} else if errMsg != "" {
		recordSpanError(cs, errMsg)
	}
	cs.EndTime = timestamppb.Now()
	rec := proto.Clone(cs).(*proxypb.TraceSpan)
	t.mu.Unlock()
	t.store.Record(t.enabled, rec)
}

func newPublishRPCSpan(store *TraceStore, enabled bool, projectID, fullTopic, topicID string, batchCount int) *proxypb.TraceSpan {
	if !enabled || !store.cfg.SupportsMonitoring || store.cfg.HasFault("missing-traces") {
		return nil
	}
	kind := proxypb.SpanKind_SPAN_KIND_INTERNAL
	if store.cfg.CppSpans {
		kind = proxypb.SpanKind_SPAN_KIND_CLIENT
	}
	sp := newSpanBase(store, "", "", topicID+" publish", kind, 1, "")
	sp.Attributes["gcp.project_id"] = attrStr(projectID)
	sp.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	sp.Attributes["messaging.destination.name"] = attrStr(topicID)
	sp.Attributes["messaging.batch.message_count"] = attrInt(int64(batchCount))
	if store.cfg.CppSpans {
		sp.Attributes["code.function.name"] = attrStr("BatchSink::AsyncPublish")
		sp.Attributes["messaging.operation.type"] = attrStr("publish")
		recordCppSpanStatus(sp, codes.OK, "")
	} else {
		sp.Attributes["gcp.resource.name"] = attrStr(fullTopic)
		sp.Attributes["code.function"] = attrStr("publishMessageBundle")
	}
	return sp
}

func newCppPublishFanoutSpan(store *TraceStore, pubSpan *proxypb.TraceSpan, idx int, links []*proxypb.SpanLink) *proxypb.TraceSpan {
	if store == nil || pubSpan == nil {
		return nil
	}
	sp := newSpanBase(store, pubSpan.TraceId, pubSpan.SpanId, fmt.Sprintf("publish #%d", idx), proxypb.SpanKind_SPAN_KIND_CLIENT, pubSpan.TraceFlags, pubSpan.TraceState)
	sp.Links = links
	sp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
	sp.EndTime = timestamppb.Now()
	return sp
}

// subscribeMessageTrace tracks subscribe, concurrency control, scheduler, and process spans for one message.
type subscribeMessageTrace struct {
	mu              sync.Mutex
	enabled         bool
	store           *TraceStore
	cppMode         bool
	subSpan         *proxypb.TraceSpan
	ccSpan          *proxypb.TraceSpan
	schedSpan       *proxypb.TraceSpan
	processSpan     *proxypb.TraceSpan
	processEnded    bool
	subEnded        bool
	expired         bool
	projectID       string
	fullSub         string
	subID           string
	deliveryAttempt int32
}

func (t *subscribeMessageTrace) markExpired() {
	if t == nil || !t.enabled {
		return
	}
	t.mu.Lock()
	if !t.subEnded {
		t.expired = true
	}
	t.mu.Unlock()
}

func newSubscribeMessageTrace(store *TraceStore, enabled bool, projectID, fullSub, subID, ackID, msgID string, data []byte, attrs map[string]string, orderingKey string, deliveryAttempt int32, batchCount int, isEOD bool) *subscribeMessageTrace {
	t := &subscribeMessageTrace{
		enabled:         enabled && store.cfg.SupportsMonitoring && !store.cfg.HasFault("missing-traces"),
		store:           store,
		cppMode:         store.cfg.CppSpans,
		projectID:       projectID,
		fullSub:         fullSub,
		subID:           subID,
		deliveryAttempt: deliveryAttempt,
	}
	if !t.enabled {
		return t
	}
	var traceID, parentSpanID, traceState string
	var flags uint32 = 1
	if !store.cfg.HasFault("missing-traceparent") && attrs != nil {
		if tp := attrs["googclient_traceparent"]; tp != "" {
			if tid, sid, f, ok := parseTraceparent(tp); ok {
				if !t.cppMode || (f&0x01) != 0 {
					traceID = tid
					parentSpanID = sid
					flags = f
					traceState = attrs["googclient_tracestate"]
				}
			}
		}
	}
	subKind := proxypb.SpanKind_SPAN_KIND_INTERNAL
	if t.cppMode {
		subKind = proxypb.SpanKind_SPAN_KIND_CONSUMER
	}
	ss := newSpanBase(store, traceID, parentSpanID, subID+" subscribe", subKind, flags, traceState)
	ss.Attributes["gcp.project_id"] = attrStr(projectID)
	ss.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	ss.Attributes["messaging.destination.name"] = attrStr(subID)
	ss.Attributes["messaging.message.id"] = attrStr(msgID)
	ss.Attributes["messaging.gcp_pubsub.message.ack_id"] = attrStr(ackID)
	if t.cppMode {
		ss.Attributes["messaging.operation.type"] = attrStr("subscribe")
		ss.Attributes["messaging.message.envelope.size"] = attrInt(messageEnvelopeSize(data, attrs, orderingKey, msgID))
		ss.Attributes["messaging.gcp_pubsub.subscription.exactly_once_delivery"] = attrBool(isEOD)
		if orderingKey != "" {
			ss.Attributes["messaging.gcp_pubsub.message.ordering_key"] = attrStr(orderingKey)
		}
	} else {
		ss.Attributes["gcp.resource.name"] = attrStr(fullSub)
		ss.Attributes["messaging.message.body.size"] = attrInt(int64(len(data)))
		ss.Attributes["messaging.gcp_pubsub.exactly_once_delivery"] = attrBool(isEOD)
		ss.Attributes["messaging.gcp_pubsub.message.ordering_key"] = attrStr(orderingKey)
		ss.Attributes["messaging.batch.message_count"] = attrInt(int64(batchCount))
		ss.Attributes["code.function"] = attrStr("receive")
		if deliveryAttempt > 0 {
			ss.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = attrInt(int64(deliveryAttempt))
		}
	}
	t.subSpan = ss
	return t
}

func (t *subscribeMessageTrace) startConcurrencyControl() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subEnded || t.ccSpan != nil || t.subSpan == nil {
		return
	}
	cc := newSpanBase(t.store, t.subSpan.TraceId, t.subSpan.SpanId, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, t.subSpan.TraceFlags, t.subSpan.TraceState)
	if t.cppMode {
		cc.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	}
	t.ccSpan = cc
	t.subSpan.ChildSpanCount++
}

func (t *subscribeMessageTrace) endConcurrencyControl() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	cc := t.ccSpan
	if cc == nil {
		t.mu.Unlock()
		return
	}
	t.ccSpan = nil
	cc.EndTime = timestamppb.Now()
	rec := proto.Clone(cc).(*proxypb.TraceSpan)
	t.mu.Unlock()
	t.store.Record(t.enabled, rec)
}

func (t *subscribeMessageTrace) startScheduler() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subEnded || t.schedSpan != nil || t.subSpan == nil {
		return
	}
	schedName := "subscribe scheduler"
	if t.cppMode {
		schedName = "subscriber scheduler"
	}
	sc := newSpanBase(t.store, t.subSpan.TraceId, t.subSpan.SpanId, schedName, proxypb.SpanKind_SPAN_KIND_INTERNAL, t.subSpan.TraceFlags, t.subSpan.TraceState)
	if t.cppMode {
		sc.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	}
	t.schedSpan = sc
	t.subSpan.ChildSpanCount++
}

func (t *subscribeMessageTrace) endScheduler() {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	sc := t.schedSpan
	if sc == nil {
		t.mu.Unlock()
		return
	}
	t.schedSpan = nil
	sc.EndTime = timestamppb.Now()
	rec := proto.Clone(sc).(*proxypb.TraceSpan)
	t.mu.Unlock()
	t.store.Record(t.enabled, rec)
}

func (t *subscribeMessageTrace) startProcess(msgID string, dataLen int, orderingKey string, deliveryAttempt int32, attrs map[string]string) (traceparent, tracestate string) {
	if !t.enabled {
		return "", ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subEnded || t.expired || t.subSpan == nil {
		return "", ""
	}
	kind := proxypb.SpanKind_SPAN_KIND_CONSUMER
	if t.cppMode {
		kind = proxypb.SpanKind_SPAN_KIND_INTERNAL
	}
	ps := newSpanBase(t.store, t.subSpan.TraceId, t.subSpan.SpanId, t.subID+" process", kind, t.subSpan.TraceFlags, t.subSpan.TraceState)
	if t.cppMode {
		ps.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	} else {
		ps.Attributes["gcp.project_id"] = attrStr(t.projectID)
		ps.Attributes["gcp.resource.name"] = attrStr(t.fullSub)
		ps.Attributes["messaging.system"] = attrStr("gcp_pubsub")
		ps.Attributes["messaging.destination.name"] = attrStr(t.subID)
		ps.Attributes["messaging.message.id"] = attrStr(msgID)
		ps.Attributes["messaging.message.body.size"] = attrInt(int64(dataLen))
		ps.Attributes["messaging.gcp_pubsub.message.ordering_key"] = attrStr(orderingKey)
		if deliveryAttempt > 0 {
			ps.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = attrInt(int64(deliveryAttempt))
		}
		ps.Attributes["messaging.operation.type"] = attrStr("process")
	}
	t.processSpan = ps
	t.subSpan.ChildSpanCount++
	if t.store.cfg.HasFault("missing-traceparent") {
		return "", ""
	}
	return formatTraceparent(ps.TraceId, ps.SpanId, ps.TraceFlags), ps.TraceState
}

func (t *subscribeMessageTrace) endProcess(action string) {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	if t.processEnded || t.processSpan == nil {
		t.mu.Unlock()
		return
	}
	t.processEnded = true
	ps := t.processSpan
	if !t.expired && !t.subEnded {
		if t.cppMode {
			switch action {
			case "ack":
				addSpanEvent(t.subSpan, "gl-cpp.message_ack", nil)
			case "nack":
				addSpanEvent(t.subSpan, "gl-cpp.message_nack", nil)
			}
		} else {
			if action == "ack" {
				addSpanEvent(ps, "ack called", nil)
			} else if action == "nack" {
				addSpanEvent(ps, "nack called", nil)
			}
		}
	}
	ps.EndTime = timestamppb.Now()
	rec := proto.Clone(ps).(*proxypb.TraceSpan)
	t.mu.Unlock()
	t.store.Record(t.enabled, rec)
}

func (t *subscribeMessageTrace) addSubscribeEventLocked(name string, batchCount int) {
	var attrs map[string]*proxypb.AttributeValue
	if t.cppMode {
		name = "gl-cpp." + strings.ReplaceAll(name, " ", "_")
	} else if batchCount > 0 {
		attrs = map[string]*proxypb.AttributeValue{
			"messaging.batch.message_count": attrInt(int64(batchCount)),
		}
	}
	addSpanEvent(t.subSpan, name, attrs)
}

func (t *subscribeMessageTrace) addSubscribeEvent(name string, batchCount int) {
	if !t.enabled {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subEnded || t.expired || t.subSpan == nil {
		return
	}
	t.addSubscribeEventLocked(name, batchCount)
}

func (t *subscribeMessageTrace) linkSubAndRPCSpanLocked(rpcSpan *proxypb.TraceSpan, opName string) {
	if rpcSpan == nil || t.store.cfg.HasFault("missing-links") {
		return
	}
	if !t.cppMode {
		t.subSpan.Links = append(t.subSpan.Links, &proxypb.SpanLink{
			TraceId:    rpcSpan.TraceId,
			SpanId:     rpcSpan.SpanId,
			TraceFlags: rpcSpan.TraceFlags,
			TraceState: rpcSpan.TraceState,
			Attributes: map[string]*proxypb.AttributeValue{
				"messaging.operation.name": attrStr(opName),
			},
		})
	}
	rpcSpan.Links = append(rpcSpan.Links, &proxypb.SpanLink{
		TraceId:    t.subSpan.TraceId,
		SpanId:     t.subSpan.SpanId,
		TraceFlags: t.subSpan.TraceFlags,
		TraceState: t.subSpan.TraceState,
	})
}

func (t *subscribeMessageTrace) linkSubAndRPCSpan(rpcSpan *proxypb.TraceSpan, opName string) {
	if !t.enabled || rpcSpan == nil || t.store.cfg.HasFault("missing-links") {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.subEnded || t.expired || t.subSpan == nil {
		return
	}
	t.linkSubAndRPCSpanLocked(rpcSpan, opName)
}

func collectUniqueTraces(ackIDs []string, traces map[string][]*subscribeMessageTrace) []*subscribeMessageTrace {
	var out []*subscribeMessageTrace
	var seen map[*subscribeMessageTrace]struct{}
	for _, id := range ackIDs {
		for _, tr := range traces[id] {
			if tr == nil {
				continue
			}
			if seen == nil {
				seen = make(map[*subscribeMessageTrace]struct{}, len(ackIDs))
			}
			if _, dup := seen[tr]; dup {
				continue
			}
			seen[tr] = struct{}{}
			out = append(out, tr)
		}
	}
	return out
}

// attachSettlementBatch atomically locks all unique traces in ascending uintptr order,
// filters active (!subEnded && !expired && subSpan != nil) traces, optionally allocates
// rpcSpan with messaging.batch.message_count == len(active), emits "<op> start" (with the
// exact same batch_count when emitOpStart is true), and links active subscribe spans to
// rpcSpan (bidirectional in Go, forward-only in C++).
func attachSettlementBatch(
	store *TraceStore,
	enabled bool,
	projectID, fullSub, subID, op string,
	deadlineSec int32,
	isReceipt bool,
	createRPCSpan bool,
	emitOpStart bool,
	unique []*subscribeMessageTrace,
) (*proxypb.TraceSpan, []*subscribeMessageTrace, int) {
	if len(unique) == 0 || !enabled || store == nil || !store.cfg.SupportsMonitoring || store.cfg.HasFault("missing-traces") {
		return nil, nil, 0
	}
	type lockItem struct {
		tr   *subscribeMessageTrace
		addr uintptr
	}
	ordered := make([]*subscribeMessageTrace, 0, len(unique))
	seen := make(map[*subscribeMessageTrace]struct{}, len(unique))
	for _, tr := range unique {
		if tr == nil || !tr.enabled {
			continue
		}
		if _, dup := seen[tr]; dup {
			continue
		}
		seen[tr] = struct{}{}
		ordered = append(ordered, tr)
	}
	if len(ordered) == 0 {
		return nil, nil, 0
	}
	locked := make([]lockItem, len(ordered))
	for i, tr := range ordered {
		locked[i] = lockItem{tr: tr, addr: uintptr(unsafe.Pointer(tr))}
	}
	if len(locked) > 1 {
		slices.SortFunc(locked, func(a, b lockItem) int {
			if a.addr < b.addr {
				return -1
			}
			if a.addr > b.addr {
				return 1
			}
			return 0
		})
	}
	for _, it := range locked {
		it.tr.mu.Lock()
	}

	active := make([]*subscribeMessageTrace, 0, len(ordered))
	for _, tr := range ordered {
		if !tr.subEnded && !tr.expired && tr.subSpan != nil {
			active = append(active, tr)
		}
	}
	attached := len(active)
	var rpcSpan *proxypb.TraceSpan
	if attached > 0 {
		if createRPCSpan {
			rpcSpan = newSubscribeRPCSpan(store, enabled, projectID, fullSub, subID, op, attached, deadlineSec, isReceipt)
		}
		for _, tr := range active {
			if emitOpStart {
				tr.addSubscribeEventLocked(op+" start", attached)
			}
			if rpcSpan != nil {
				tr.linkSubAndRPCSpanLocked(rpcSpan, op)
			}
		}
	}

	for i := len(locked) - 1; i >= 0; i-- {
		locked[i].tr.mu.Unlock()
	}
	return rpcSpan, active, attached
}

func (t *subscribeMessageTrace) isActive() bool {
	if t == nil {
		return false
	}
	if !t.enabled {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.subEnded && !t.expired && t.subSpan != nil
}

func (t *subscribeMessageTrace) endSubscribe(result string, errMsg string) {
	if !t.enabled {
		return
	}
	t.endConcurrencyControl()
	t.endScheduler()
	t.endProcess("")
	t.mu.Lock()
	if t.subEnded || t.subSpan == nil || (t.expired && result != "expired") {
		t.mu.Unlock()
		return
	}
	t.subEnded = true
	ss := t.subSpan
	if result != "" {
		if t.cppMode {
			switch result {
			case "acked":
				result = "ack"
			case "nacked":
				result = "nack"
			}
		}
		ss.Attributes["messaging.gcp_pubsub.result"] = attrStr(result)
	}
	if errMsg != "" && !t.cppMode {
		recordSpanError(ss, errMsg)
	}
	ss.EndTime = timestamppb.Now()
	rec := proto.Clone(ss).(*proxypb.TraceSpan)
	t.mu.Unlock()
	t.store.Record(t.enabled, rec)
}

func newSubscribeRPCSpan(store *TraceStore, enabled bool, projectID, fullSub, subID, op string, batchCount int, deadlineSec int32, isReceipt bool) *proxypb.TraceSpan {
	if !enabled || !store.cfg.SupportsMonitoring || store.cfg.HasFault("missing-traces") {
		return nil
	}
	kind := proxypb.SpanKind_SPAN_KIND_INTERNAL
	if store.cfg.CppSpans && op == "modack" {
		kind = proxypb.SpanKind_SPAN_KIND_CLIENT
	}
	sp := newSpanBase(store, "", "", subID+" "+op, kind, 1, "")
	sp.Attributes["gcp.project_id"] = attrStr(projectID)
	sp.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	sp.Attributes["messaging.destination.name"] = attrStr(subID)
	sp.Attributes["messaging.batch.message_count"] = attrInt(int64(batchCount))
	if store.cfg.CppSpans {
		if op == "modack" {
			sp.Attributes["messaging.operation.type"] = attrStr("extend")
			sp.Attributes["messaging.gcp_pubsub.message.ack_deadline_seconds"] = attrInt(int64(deadlineSec))
		}
		return sp
	}
	sp.Attributes["gcp.resource.name"] = attrStr(fullSub)
	if op == "ack" {
		sp.Attributes["code.function"] = attrStr("sendAck")
	} else {
		sp.Attributes["code.function"] = attrStr("sendModAck")
	}
	if op == "modack" {
		sp.Attributes["messaging.gcp_pubsub.message.ack_deadline"] = attrInt(int64(deadlineSec))
		sp.Attributes["messaging.gcp_pubsub.is_receipt_modack"] = attrBool(isReceipt)
	}
	return sp
}

func startCppExactlyOnceSettleSpan(store *TraceStore, enabled bool, projectID, fullSub, subID, op, ackID string, subTrace *subscribeMessageTrace) *proxypb.TraceSpan {
	if !enabled || !store.cfg.SupportsMonitoring || store.cfg.HasFault("missing-traces") {
		return nil
	}
	sp := newSpanBase(store, "", "", subID+" "+op, proxypb.SpanKind_SPAN_KIND_INTERNAL, 1, "")
	sp.Attributes["gcp.project_id"] = attrStr(projectID)
	sp.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	sp.Attributes["messaging.destination.name"] = attrStr(subID)
	sp.Attributes["code.function.name"] = attrStr("pubsub::AckHandler::" + op)
	sp.Attributes["messaging.gcp_pubsub.message.ack_id"] = attrStr(ackID)
	sp.Attributes["messaging.gcp_pubsub.subscription.template"] = attrStr(fullSub)
	sp.Attributes["messaging.operation.type"] = attrStr("settle")
	var attempt int32
	if subTrace != nil {
		attempt = subTrace.deliveryAttempt
		subTrace.linkSubAndRPCSpan(sp, op)
	}
	sp.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = attrInt(int64(attempt))
	return sp
}

func finishCppExactlyOnceSettleSpan(store *TraceStore, enabled bool, sp *proxypb.TraceSpan, errCode codes.Code, errMsg string) {
	if sp == nil || !enabled || !store.cfg.SupportsMonitoring || store.cfg.HasFault("missing-traces") {
		return
	}
	if errMsg != "" && errCode == codes.OK {
		errCode = codes.Unknown
	}
	recordCppSpanStatus(sp, errCode, errMsg)
	sp.EndTime = timestamppb.Now()
	store.Record(enabled, sp)
}

func newCppExactlyOnceSettleSpan(store *TraceStore, enabled bool, projectID, fullSub, subID, op, ackID string, subTrace *subscribeMessageTrace, errCode codes.Code, errMsg string) *proxypb.TraceSpan {
	sp := startCppExactlyOnceSettleSpan(store, enabled, projectID, fullSub, subID, op, ackID, subTrace)
	if sp == nil {
		return nil
	}
	if errMsg != "" && errCode == codes.OK {
		errCode = codes.Unknown
	}
	recordCppSpanStatus(sp, errCode, errMsg)
	sp.EndTime = timestamppb.Now()
	return sp
}

func newCppUnaryPullReceiveSpan(store *TraceStore, enabled bool, projectID, subID, msgID string, data []byte, attrs map[string]string, orderingKey string, errCode codes.Code, errMsg string) *proxypb.TraceSpan {
	if !enabled || !store.cfg.SupportsMonitoring || store.cfg.HasFault("missing-traces") {
		return nil
	}
	sp := newSpanBase(store, "", "", subID+" receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, 1, "")
	sp.Attributes["gcp.project_id"] = attrStr(projectID)
	sp.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	sp.Attributes["messaging.destination.name"] = attrStr(subID)
	sp.Attributes["messaging.operation.type"] = attrStr("receive")
	sp.Attributes["code.function.name"] = attrStr("pubsub::SubscriberConnection::Pull")
	if errCode == codes.OK && errMsg == "" {
		if msgID != "" {
			sp.Attributes["messaging.message.id"] = attrStr(msgID)
		}
		sp.Attributes["messaging.message.envelope.size"] = attrInt(messageEnvelopeSize(data, attrs, orderingKey, msgID))
		if orderingKey != "" {
			sp.Attributes["messaging.gcp_pubsub.message.ordering_key"] = attrStr(orderingKey)
		}
		if !store.cfg.HasFault("missing-links") && !store.cfg.HasFault("missing-traceparent") && attrs != nil {
			if tp := attrs["googclient_traceparent"]; tp != "" {
				if tid, sid, f, ok := parseTraceparent(tp); ok && (f&0x01) != 0 {
					sp.Links = append(sp.Links, &proxypb.SpanLink{
						TraceId:    tid,
						SpanId:     sid,
						TraceFlags: f,
						TraceState: attrs["googclient_tracestate"],
					})
				}
			}
		}
	}
	if errMsg != "" && errCode == codes.OK {
		errCode = codes.Unknown
	}
	recordCppSpanStatus(sp, errCode, errMsg)
	sp.EndTime = timestamppb.Now()
	return sp
}

func newCppUnaryPullSettleSpan(store *TraceStore, enabled bool, projectID, subID, op, ackID string, deliveryAttempt, deadlineSec int32, recvSpan *proxypb.TraceSpan, errCode codes.Code, errMsg string) *proxypb.TraceSpan {
	if !enabled || !store.cfg.SupportsMonitoring || store.cfg.HasFault("missing-traces") {
		return nil
	}
	sp := newSpanBase(store, "", "", subID+" "+op, proxypb.SpanKind_SPAN_KIND_CLIENT, 1, "")
	sp.Attributes["gcp.project_id"] = attrStr(projectID)
	sp.Attributes["messaging.system"] = attrStr("gcp_pubsub")
	sp.Attributes["messaging.destination.name"] = attrStr(subID)
	sp.Attributes["messaging.operation.type"] = attrStr(op)
	sp.Attributes["messaging.gcp_pubsub.message.ack_id"] = attrStr(ackID)
	switch op {
	case "ack":
		sp.Attributes["code.function.name"] = attrStr("pubsub::PullAckHandler::ack")
		if deliveryAttempt > 0 {
			sp.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = attrInt(int64(deliveryAttempt))
		}
	case "nack":
		sp.Attributes["code.function.name"] = attrStr("pubsub::PullAckHandler::nack")
		if deliveryAttempt > 0 {
			sp.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = attrInt(int64(deliveryAttempt))
		}
	case "modack":
		sp.Attributes["code.function.name"] = attrStr("pubsub::PullLeaseManager::ExtendLease")
		sp.Attributes["messaging.gcp_pubsub.message.ack_deadline_seconds"] = attrInt(int64(deadlineSec))
	}
	if recvSpan != nil && !store.cfg.HasFault("missing-links") {
		sp.Links = append(sp.Links, &proxypb.SpanLink{
			TraceId:    recvSpan.TraceId,
			SpanId:     recvSpan.SpanId,
			TraceFlags: recvSpan.TraceFlags,
			TraceState: recvSpan.TraceState,
		})
	}
	if errMsg != "" && errCode == codes.OK {
		errCode = codes.Unknown
	}
	recordCppSpanStatus(sp, errCode, errMsg)
	sp.EndTime = timestamppb.Now()
	return sp
}
