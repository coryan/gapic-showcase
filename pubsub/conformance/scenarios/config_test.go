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

package scenarios_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type m3eProfileCfg struct {
	Lang                                          string `json:"lang"`
	Monitoring, Hedging, EOD, UnaryPull, CppSpans bool   `json:",omitempty"`
	Fault                                         string `json:"fault,omitempty"`
}

func (m *m3eMockServer) hasFault(f string) bool { return m.cfg.Fault == f }

type m3ePublisher struct {
	conn                      *grpc.ClientConn
	client                    pubsubpb.PublisherClient
	topic, projectID, ep      string
	enableMon, shortEmptyProj bool
	headers                   map[string]string
	hedging                   *proxypb.HedgingSettings
	tokens                    float64
	mu                        sync.Mutex
}

type m3eMockServer struct {
	proxypb.UnimplementedPubSubProxyServer
	cfg            m3eProfileCfg
	emuAddr        string
	nodeSeq, idSeq atomic.Int64
	stopCtx        context.Context
	stopCancel     context.CancelFunc
	mu             sync.Mutex
	pubs           map[string]*m3ePublisher
	spans          []*proxypb.TraceSpan
}

func (m *m3eMockServer) closeAll() {
	m.stopCancel()
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, p := range m.pubs {
		if p.conn != nil {
			_ = p.conn.Close()
		}
		delete(m.pubs, id)
	}
}

func m3eShortRes(res string) string { p := strings.Split(res, "/"); return p[len(p)-1] }
func m3eStrA(s string) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringValue{StringValue: s}}
}
func m3eIntA(v int64) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
}
func m3eBoolA(v bool) *proxypb.AttributeValue {
	return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: v}}
}
func m3eEv(name string, t time.Time, batchN int64) *proxypb.SpanEvent {
	var attrs map[string]*proxypb.AttributeValue
	if batchN > 0 {
		attrs = map[string]*proxypb.AttributeValue{"messaging.batch.message_count": m3eIntA(batchN)}
	}
	return &proxypb.SpanEvent{Name: name, Timestamp: timestamppb.New(t), Attributes: attrs}
}
func m3eSpan(tr, sp, par, name string, kind proxypb.SpanKind, t0, t1 time.Time, code proxypb.SpanStatusCode, attrs map[string]*proxypb.AttributeValue, links []*proxypb.SpanLink, evs []*proxypb.SpanEvent, scope string) *proxypb.TraceSpan {
	return &proxypb.TraceSpan{TraceId: tr, SpanId: sp, ParentSpanId: par, TraceFlags: 1, Name: name, Kind: kind, StartTime: timestamppb.New(t0), EndTime: timestamppb.New(t1), StatusCode: code, Attributes: attrs, Links: links, Events: evs, InstrumentationScopeName: scope}
}
func (m *m3eMockServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	m.stopCancel()
	return &proxypb.ShutdownProxyResponse{}, nil
}

func m3eLeakWire(ctx context.Context, ep, kind string) {
	conn, err := grpc.NewClient(ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()
	switch kind {
	case "pub":
		_, _ = pubsubpb.NewPublisherClient(conn).Publish(ctx, &pubsubpb.PublishRequest{Topic: "projects/bad/topics/t", Messages: []*pubsubpb.PubsubMessage{{Data: []byte("leak")}}})
	case "sub":
		if sp, err := pubsubpb.NewSubscriberClient(conn).StreamingPull(ctx); err == nil {
			_ = sp.Send(&pubsubpb.StreamingPullRequest{Subscription: "projects/bad/subscriptions/s", StreamAckDeadlineSeconds: 10, ClientId: "m3e-default-client", ModifyDeadlineSeconds: []int32{-1}})
			_ = sp.CloseSend()
			_, _ = sp.Recv()
		}
	case "pull":
		_, _ = pubsubpb.NewSubscriberClient(conn).Pull(ctx, &pubsubpb.PullRequest{Subscription: "projects/bad/subscriptions/s", MaxMessages: 1, ReturnImmediately: true})
	}
}

func (m *m3eMockServer) resolveAndDial(proj, res, coll, ep string, useEmu bool) (string, string, *grpc.ClientConn, error) {
	if p := strings.Split(res, "/"); len(p) == 4 && p[0] == "projects" && p[2] == coll {
		if ((m.hasFault("rewrite-cross-project") && coll == "topics") || (m.hasFault("rewrite-cross-project-sub") && coll == "subscriptions")) && proj != "" {
			res = fmt.Sprintf("projects/%s/%s/%s", proj, coll, p[3])
		} else if m.hasFault("cfg03-rewrite-full-path") && proj == "" {
			res, proj = fmt.Sprintf("projects/wrong-proj/%s/%s", coll, p[3]), "wrong-proj"
		} else {
			proj = p[1]
		}
	} else if proj == "" {
		if m.hasFault("reject-empty-proj-unimpl") {
			return "", "", nil, status.Error(codes.Unimplemented, "unimplemented")
		}
		if !m.hasFault("accept-short-empty-proj") {
			return "", "", nil, status.Errorf(codes.InvalidArgument, "missing project_id for %s %q", coll, res)
		}
		proj, res = "test-project", fmt.Sprintf("projects/test-project/%s/%s", coll, res)
	} else {
		res = fmt.Sprintf("projects/%s/%s/%s", proj, coll, res)
	}
	target := ep
	if target == "" {
		if !useEmu || m.emuAddr == "" || m.hasFault("ignore-empty-endpoint") {
			return "", "", nil, status.Error(codes.InvalidArgument, "missing endpoint")
		}
		target = m.emuAddr
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return proj, res, conn, err
}

func (m *m3eMockServer) outCtx(ctx context.Context, rpc, v string, stream, reconnect bool, extra map[string]string) context.Context {
	k := "subscription"
	if rpc == "pub" {
		k = "topic"
	}
	pairs, lang, val := make([]string, 0, 2+len(extra)*2), strings.ToLower(m.cfg.Lang), url.QueryEscape(v)
	unencStream := lang == "rust" || lang == "java" || lang == "node" || lang == "python"
	if !stream {
		if lang == "rust" || m.hasFault("wrong-unary-params") || m.hasFault("wrong-unary-params-"+rpc) {
			val = v
		}
		pairs = append(pairs, "x-goog-request-params", k+"="+val)
	} else if !m.hasFault("ruby-missing-stream-params") {
		if (unencStream && !m.hasFault("wrong-stream-params")) || (!unencStream && m.hasFault("wrong-stream-params")) {
			val = v
		}
		pairs = append(pairs, "x-goog-request-params", k+"="+val)
	}
	if !m.hasFault("drop-custom-headers") && !m.hasFault("drop-hdr-"+rpc) && !(reconnect && m.hasFault("drop-headers-reconnect")) {
		for hk, hv := range extra {
			if m.hasFault("wrong-header-val") {
				hv = "wrong-val"
			}
			pairs = append(pairs, hk, hv)
		}
	}
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(pairs...))
}

func (m *m3eMockServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	lang := cmp.Or(map[bool]string{true: "cpp"}[m.cfg.CppSpans], m.cfg.Lang)
	return &proxypb.GetCapabilitiesResponse{ClientLanguage: lang, SupportsMonitoring: m.cfg.Monitoring, SupportsPublishHedging: m.cfg.Hedging, SupportsExactlyOnceDelivery: m.cfg.EOD, SupportsAckWithResult: m.cfg.EOD, SupportsUnaryPull: m.cfg.UnaryPull}, nil
}

func (m *m3eMockServer) ResetTraces(ctx context.Context, _ *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	if _, err := m.DrainTraces(ctx, nil); err != nil {
		return nil, err
	}
	return &proxypb.ResetTracesResponse{}, nil
}
func (m *m3eMockServer) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	if !m.cfg.Monitoring {
		return nil, status.Error(codes.Unimplemented, "no monitoring")
	}
	m.mu.Lock()
	out := append([]*proxypb.TraceSpan(nil), m.spans...)
	m.spans = nil
	m.mu.Unlock()
	return &proxypb.DrainTracesResponse{Spans: out}, nil
}
func (m *m3eMockServer) addSpans(spans ...*proxypb.TraceSpan) {
	m.mu.Lock()
	m.spans = append(m.spans, spans...)
	m.mu.Unlock()
}
func (m *m3eMockServer) nextHex(n int) string {
	return fmt.Sprintf(map[int]string{32: "1111111111111111%016x", 16: "22222222%08x"}[n], m.idSeq.Add(1))
}

func (m *m3eMockServer) commonAttrs(proj, fullRes, shortID string) map[string]*proxypb.AttributeValue {
	a := map[string]*proxypb.AttributeValue{"gcp.project_id": m3eStrA(proj), "messaging.system": m3eStrA("gcp_pubsub"), "messaging.destination.name": m3eStrA(shortID)}
	if traces.ProfileForLanguage(m.cfg.Lang).RequireGCPResourceName {
		a["gcp.resource.name"] = m3eStrA(fullRes)
	}
	return a
}

func (m *m3eMockServer) addPub(p *m3ePublisher) string {
	id := fmt.Sprintf("pub-%d", m.idSeq.Add(1))
	m.mu.Lock()
	m.pubs[id] = p
	m.mu.Unlock()
	return id
}

func (m *m3eMockServer) CreatePublisher(ctx context.Context, req *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	shortEmpty := req.GetProjectId() == "" && !strings.HasPrefix(req.GetTopic(), "projects/")
	if shortEmpty && m.hasFault("wire-leak-create-pub") {
		m3eLeakWire(ctx, req.GetEndpoint(), "pub")
		return nil, status.Error(codes.InvalidArgument, "missing project_id")
	}
	if shortEmpty && strings.HasPrefix(m.cfg.Fault, "deferred-pub-") {
		return &proxypb.CreatePublisherResponse{PublisherId: m.addPub(&m3ePublisher{topic: req.GetTopic(), ep: req.GetEndpoint(), shortEmptyProj: true})}, nil
	}
	proj, fullTopic, conn, err := m.resolveAndDial(req.GetProjectId(), req.GetTopic(), "topics", req.GetEndpoint(), req.GetUseEmulatorEnv())
	if err != nil {
		return nil, err
	}
	return &proxypb.CreatePublisherResponse{PublisherId: m.addPub(&m3ePublisher{conn: conn, client: pubsubpb.NewPublisherClient(conn), topic: fullTopic, projectID: proj, enableMon: req.GetEnableMonitoring() && m.cfg.Monitoring, headers: req.GetCustomHeaders(), hedging: req.GetHedging()})}, nil
}

func (m *m3eMockServer) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	if m.hasFault("block-pub-ctx") {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	m.mu.Lock()
	p := m.pubs[req.GetPublisherId()]
	m.mu.Unlock()
	if p == nil {
		return nil, status.Error(codes.NotFound, "publisher not found")
	}
	if p.shortEmptyProj {
		switch m.cfg.Fault {
		case "deferred-pub-unimpl":
			return &proxypb.PublishResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "unimplemented"}, nil
		case "deferred-pub-rpc-unimpl":
			return nil, status.Error(codes.Unimplemented, "unimplemented")
		case "deferred-pub-ok":
			return &proxypb.PublishResponse{MessageId: "bad-ok"}, nil
		case "deferred-pub-wire-leak":
			m3eLeakWire(ctx, p.ep, "pub")
		}
		return &proxypb.PublishResponse{ErrorCode: int32(codes.InvalidArgument), ErrorMessage: "missing project_id"}, nil
	}
	shortTopic, tStart, mon, prof := m3eShortRes(p.topic), time.Now(), p.enableMon, traces.ProfileForLanguage(m.cfg.Lang)
	cTraceID, cParent := m.nextHex(32), ""
	if tp, err := traces.ParseTraceparent(req.GetParentTraceparent()); err == nil {
		cTraceID, cParent = tp.TraceID, tp.SpanID
	}
	cSpanID, pTraceID, pSpanID, wireTP := m.nextHex(16), cTraceID, m.nextHex(16), ""
	dropPrefillTP := m.hasFault("trac02-drop-prefill-tp") && bytes.HasPrefix(req.GetData(), []byte("trac02-prefill-"))
	dropTargetTP := m.hasFault("trac02-drop-target-wire-trace") && bytes.Equal(req.GetData(), []byte("trac02-hedge-target"))
	if mon && !m.hasFault("drop-wire-trace") && !dropPrefillTP && !dropTargetTP {
		fl := map[bool]string{false: "01", true: "00"}[m.hasFault("unsampled-wire-trace")]
		wireTP = fmt.Sprintf("00-%s-%s-%s", cTraceID, cSpanID, fl)
	}
	if m.hasFault("trac02-skip-prefill") && bytes.Equal(req.GetData(), []byte("trac02-prefill-01")) {
		return &proxypb.PublishResponse{MessageId: "skipped-prefill"}, nil
	}
	sendAttempt := func(callCtx context.Context, attempt int32) (*pubsubpb.PublishResponse, error) {
		attrs := map[string]string{}
		maps.Copy(attrs, req.GetAttributes())
		if m.hasFault("wrong-pub-attr") {
			attrs["mode"] = "wrong"
		}
		if wireTP != "" {
			attrs[fake.TraceparentAttributeKey] = wireTP
		} else if !mon && m.hasFault("inject-wire-trace-disabled") {
			attrs[fake.TraceparentAttributeKey] = fmt.Sprintf("00-%s-%s-01", m.nextHex(32), m.nextHex(16))
		}
		oCtx := m.outCtx(callCtx, "pub", p.topic, false, false, p.headers)
		if p.hedging != nil && p.hedging.GetEnabled() {
			a := attempt
			if m.hasFault("trac02-single-attempt") && bytes.Contains(req.GetData(), []byte("trac02-hedge-target")) {
				a = 1
			}
			tb, _ := proto.Marshal(&pubsubpb.PubsubClientTelemetry{Operation: &pubsubpb.PubsubClientTelemetry_PublishOperation_{PublishOperation: &pubsubpb.PubsubClientTelemetry_PublishOperation{HedgedAttemptCount: a, PublishStartTime: timestamppb.New(tStart)}}})
			oCtx = metadata.AppendToOutgoingContext(oCtx, "x-goog-pubsub-client-telemetry", base64.StdEncoding.EncodeToString(tb))
		}
		msgs := []*pubsubpb.PubsubMessage{{Data: req.GetData(), Attributes: attrs, OrderingKey: req.GetOrderingKey()}}
		if m.hasFault("trac02-bad-prefill-batch") && bytes.Equal(req.GetData(), []byte("trac02-prefill-01")) {
			msgs = append(msgs, &pubsubpb.PubsubMessage{Data: req.GetData(), Attributes: attrs})
		}
		if m.hasFault("dup-pub-rpc") {
			_, _ = p.client.Publish(oCtx, &pubsubpb.PublishRequest{Topic: p.topic, Messages: msgs})
		}
		return p.client.Publish(oCtx, &pubsubpb.PublishRequest{Topic: p.topic, Messages: msgs})
	}
	p.mu.Lock()
	canHedge := p.hedging != nil && p.hedging.GetEnabled() && p.tokens > 1.0 && !m.hasFault("attempt0-wins") && !m.hasFault("trac02-single-attempt")
	if canHedge {
		p.tokens -= 1.0
	} else if p.hedging != nil && p.hedging.GetEnabled() {
		p.tokens = min(float64(p.hedging.GetMaxTokens()), p.tokens+p.hedging.GetRefillRatio())
	}
	p.mu.Unlock()
	var resp *pubsubpb.PublishResponse
	var err error
	hedgedWon := false
	if !canHedge {
		resp, err = sendAttempt(ctx, 0)
	} else {
		type pubRes struct {
			r *pubsubpb.PublishResponse
			e error
		}
		ch := make(chan pubRes, 2)
		hCtx, hCancel := context.WithCancel(ctx)
		if m.hasFault("attempt0-not-canceled") {
			hCancel()
			hCtx = m.stopCtx
		} else {
			defer hCancel()
		}
		go func() { r, e := sendAttempt(hCtx, 0); ch <- pubRes{r, e} }()
		timer := time.NewTimer(max(time.Duration(p.hedging.GetDelayMs())*time.Millisecond, 100*time.Millisecond))
		defer timer.Stop()
		var got pubRes
		select {
		case got = <-ch:
		case <-timer.C:
			hedgedWon = true
			go func() { r, e := sendAttempt(ctx, 1); ch <- pubRes{r, e} }()
			got = <-ch
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		resp, err = got.r, got.e
	}
	if err != nil {
		return &proxypb.PublishResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}, nil
	}
	msgID := cmp.Or(map[bool]string{true: "wrong-pub-id"}[m.hasFault("wrong-pub-msg-id")], map[bool]string{true: "msg-trac02-attempt0-lose"}[m.hasFault("trac02-wrong-win-id") && bytes.Contains(req.GetData(), []byte("trac02-hedge-target"))], resp.GetMessageIds()[0])
	if (mon && !m.hasFault("omit-emulator-trace")) || m.hasFault("inject-unexpected-span") {
		m.emitPublishSpans(prof, p.projectID, p.topic, shortTopic, msgID, len(req.GetData()), cTraceID, cSpanID, cParent, pTraceID, pSpanID, tStart, time.Now(), hedgedWon)
	}
	return &proxypb.PublishResponse{MessageId: msgID, CallerAttributesMutated: req.GetVerifyAttributesImmutability() && m.hasFault("mutate-caller-attrs")}, nil
}

func (m *m3eMockServer) emitPublishSpans(prof traces.Profile, proj, fullTopic, shortTopic, msgID string, bodyLen int, cTrace, cSpan, cParent, pTrace, pSpan string, t0, t1 time.Time, hedged bool) {
	cAttrs, pAttrs := m.commonAttrs(proj, fullTopic, shortTopic), m.commonAttrs(proj, fullTopic, shortTopic)
	cAttrs["messaging.message.id"], cAttrs["messaging.message.body.size"], pAttrs["messaging.batch.message_count"] = m3eStrA(msgID), m3eIntA(int64(bodyLen)), m3eIntA(1)
	if prof.RequireUnconditionalOrderingKey {
		cAttrs["messaging.gcp_pubsub.message.ordering_key"] = m3eStrA("")
	}
	if prof.CodeFunctionAttrKey != "" {
		cAttrs[prof.CodeFunctionAttrKey], pAttrs[prof.CodeFunctionAttrKey] = m3eStrA("Publish"), m3eStrA("publishMessageBundle")
	}
	pEvents := []*proxypb.SpanEvent{m3eEv("publish start", t0, 1), m3eEv("publish end", t1, 0)}
	if hedged && !m.hasFault("omit-hedged-events") {
		pEvents = append(pEvents, m3eEv("publish start (hedged)", t0.Add(time.Millisecond), 1), m3eEv("publish end (hedged)", t1, 0))
	}
	cEvents, cCode, pCode, fcCode, fcKind := append([]*proxypb.SpanEvent(nil), pEvents...), proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, proxypb.SpanKind_SPAN_KIND_INTERNAL
	var fcAttrs, bAttrs map[string]*proxypb.AttributeValue
	if prof.UseEnvelopeSize {
		cAttrs["messaging.message.envelope.size"], cAttrs["gl-cpp.status_code"] = m3eIntA(int64(bodyLen+32)), m3eStrA("OK")
		cAttrs["messaging.operation.type"], pAttrs["messaging.operation.type"] = m3eStrA("create"), m3eStrA("publish")
		pAttrs["gl-cpp.status_code"], cCode, pCode, fcCode, fcKind = m3eStrA("OK"), proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, prof.FlowControlSpanKind
		if prof.CodeFunctionAttrKey != "" {
			cAttrs[prof.CodeFunctionAttrKey], pAttrs[prof.CodeFunctionAttrKey] = m3eStrA("pubsub::PublisherConnection::Publish"), m3eStrA("BatchSink::AsyncPublish")
			fcAttrs = map[string]*proxypb.AttributeValue{prof.CodeFunctionAttrKey: m3eStrA("pubsub::FlowControlledPublisherConnection::Publish")}
			bAttrs = map[string]*proxypb.AttributeValue{prof.CodeFunctionAttrKey: m3eStrA("pubsub::BatchingPublisherConnection::Publish")}
		}
		cEvents = append(cEvents, m3eEv("gl-cpp.added_to_batch", t0, 0), m3eEv("gl-cpp.publish_start", t0, 0), m3eEv("gl-cpp.publish_end", t1, 0))
	}
	spans := []*proxypb.TraceSpan{
		m3eSpan(cTrace, cSpan, cParent, shortTopic+" create", proxypb.SpanKind_SPAN_KIND_PRODUCER, t0, t1, cCode, cAttrs, []*proxypb.SpanLink{{TraceId: pTrace, SpanId: pSpan, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": m3eStrA("publish")}}}, cEvents, prof.ExpectedScope),
		m3eSpan(cTrace, m.nextHex(16), cSpan, "publisher flow control", fcKind, t0, t1, fcCode, fcAttrs, nil, nil, prof.ExpectedScope),
		m3eSpan(cTrace, m.nextHex(16), cSpan, "publisher batching", prof.BatchingSpanKind, t0, t1, fcCode, bAttrs, nil, nil, prof.ExpectedScope),
		m3eSpan(pTrace, pSpan, "", shortTopic+" publish", prof.PublishSpanKind, t0, t1, pCode, pAttrs, []*proxypb.SpanLink{{TraceId: cTrace, SpanId: cSpan}}, pEvents, prof.ExpectedScope),
	}
	if m.hasFault("inject-unexpected-span") {
		spans = append(spans, m3eSpan(cTrace, m.nextHex(16), "", shortTopic+" extra", proxypb.SpanKind_SPAN_KIND_INTERNAL, t0, t1, 0, nil, nil, nil, prof.ExpectedScope))
	}
	m.addSpans(spans...)
}

func (m *m3eMockServer) ClosePublisher(_ context.Context, req *proxypb.ClosePublisherRequest) (*proxypb.ClosePublisherResponse, error) {
	m.mu.Lock()
	p := m.pubs[req.GetPublisherId()]
	delete(m.pubs, req.GetPublisherId())
	m.mu.Unlock()
	if p != nil && p.conn != nil {
		_ = p.conn.Close()
	}
	return &proxypb.ClosePublisherResponse{}, nil
}

func (m *m3eMockServer) UnaryPull(ctx context.Context, req *proxypb.UnaryPullRequest) (*proxypb.UnaryPullResponse, error) {
	if req.GetProjectId() == "" && !strings.HasPrefix(req.GetSubscription(), "projects/") {
		switch m.cfg.Fault {
		case "pull-empty-proj-inband-unimpl":
			return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "unimplemented"}, nil
		case "pull-empty-proj-rpc-unimpl":
			return nil, status.Error(codes.Unimplemented, "unimplemented")
		case "pull-empty-proj-ok":
			return &proxypb.UnaryPullResponse{}, nil
		case "pull-empty-proj-wire-leak":
			m3eLeakWire(ctx, req.GetEndpoint(), "pull")
			return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.InvalidArgument), ErrorMessage: "missing project_id"}, nil
		}
	}
	proj, fullSub, conn, err := m.resolveAndDial(req.GetProjectId(), req.GetSubscription(), "subscriptions", req.GetEndpoint(), req.GetUseEmulatorEnv())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	switch m.cfg.Fault {
	case "pull-inband-unimpl":
		return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "unimplemented"}, nil
	case "pull-inband-err":
		return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.Internal), ErrorMessage: "pull boom"}, nil
	}
	sc, t0 := pubsubpb.NewSubscriberClient(conn), time.Now()
	resp, err := sc.Pull(m.outCtx(ctx, "pull", fullSub, false, false, req.GetCustomHeaders()), &pubsubpb.PullRequest{Subscription: fullSub, MaxMessages: map[bool]int32{false: req.GetMaxMessages(), true: 2}[m.hasFault("pull-wrong-max")], ReturnImmediately: req.GetReturnImmediately()})
	if err != nil {
		return &proxypb.UnaryPullResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}, nil
	}
	msgs, ackIDs := make([]*proxypb.UnaryPullResponse_PulledMessage, 0, len(resp.GetReceivedMessages())), make([]string, 0, len(resp.GetReceivedMessages()))
	for _, rm := range resp.GetReceivedMessages() {
		id := cmp.Or(map[bool]string{true: "wrong-pull-id"}[m.hasFault("wrong-pull-msg-id")], rm.GetMessage().GetMessageId())
		msgs = append(msgs, &proxypb.UnaryPullResponse_PulledMessage{MessageId: id, Data: rm.GetMessage().GetData(), Attributes: rm.GetMessage().GetAttributes()})
		ackIDs = append(ackIDs, rm.GetAckId())
	}
	if req.GetSettleAction() == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK && len(ackIDs) > 0 {
		if _, err := sc.Acknowledge(m.outCtx(ctx, "pullack", fullSub, false, false, req.GetCustomHeaders()), &pubsubpb.AcknowledgeRequest{Subscription: fullSub, AckIds: map[bool][]string{false: ackIDs, true: {"wrong-pull-ack-id"}}[m.hasFault("pullack-wrong-id")]}); err != nil {
			return &proxypb.UnaryPullResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}, nil
		}
		switch m.cfg.Fault {
		case "after-pull-spurious-nack":
			_, _ = sc.ModifyAckDeadline(m.outCtx(ctx, "modack", fullSub, false, false, req.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: ackIDs, AckDeadlineSeconds: 0})
		case "after-pull-neg-modack":
			_, _ = sc.ModifyAckDeadline(m.outCtx(ctx, "modack", fullSub, false, false, req.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: ackIDs, AckDeadlineSeconds: -1})
		case "after-pull-modack-drop-hdr":
			_, _ = sc.ModifyAckDeadline(m.outCtx(ctx, "modack", fullSub, false, false, nil), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: ackIDs, AckDeadlineSeconds: 15})
		case "unexpected-gettopic-after-pull":
			_ = conn.Invoke(ctx, "/google.pubsub.v1.Publisher/GetTopic", &pubsubpb.GetTopicRequest{Topic: "projects/p/topics/t"}, &pubsubpb.Topic{})
		}
	}
	if prof := traces.ProfileForLanguage(m.cfg.Lang); req.GetEnableMonitoring() && m.cfg.Monitoring && !m.hasFault("omit-emulator-trace") && !m.hasFault("omit-pull-trace") && prof.AllowUnaryPull && len(resp.GetReceivedMessages()) > 0 {
		shortSub, rm, trID, spID, ackID, t1 := m3eShortRes(fullSub), resp.GetReceivedMessages()[0], m.nextHex(32), m.nextHex(16), m.nextHex(16), time.Now()
		rAttrs, aAttrs := m.commonAttrs(proj, fullSub, shortSub), m.commonAttrs(proj, fullSub, shortSub)
		rAttrs["messaging.message.id"], rAttrs["messaging.message.body.size"], rAttrs["messaging.message.envelope.size"] = m3eStrA(rm.GetMessage().GetMessageId()), m3eIntA(int64(len(rm.GetMessage().GetData()))), m3eIntA(int64(proto.Size(rm.GetMessage())+24))
		rAttrs["messaging.gcp_pubsub.message.ack_id"], rAttrs["messaging.operation.type"] = m3eStrA(rm.GetAckId()), m3eStrA("receive")
		aAttrs["messaging.batch.message_count"], aAttrs["messaging.gcp_pubsub.message.ack_id"], aAttrs["messaging.operation.type"], aAttrs["gl-cpp.status_code"], aAttrs["messaging.gcp_pubsub.subscription.template"] = m3eIntA(1), m3eStrA(rm.GetAckId()), m3eStrA("ack"), m3eStrA("OK"), m3eStrA(fullSub)
		if prof.CodeFunctionAttrKey != "" {
			rAttrs[prof.CodeFunctionAttrKey], aAttrs[prof.CodeFunctionAttrKey] = m3eStrA("pubsub::SubscriberConnection::Pull"), m3eStrA("pubsub::PullAckHandler::ack")
		}
		var rLinks []*proxypb.SpanLink
		if tp, err := traces.ParseTraceparent(rm.GetMessage().GetAttributes()[fake.TraceparentAttributeKey]); err == nil {
			rLinks = []*proxypb.SpanLink{{TraceId: tp.TraceID, SpanId: tp.SpanID}}
		}
		m.addSpans(
			m3eSpan(trID, spID, "", shortSub+" receive", prof.SubscribeSpanKind, t0, t1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, rAttrs, rLinks, nil, prof.ExpectedScope),
			m3eSpan(trID, ackID, "", shortSub+" ack", prof.UnaryPullAckSpanKind, t0, t1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, aAttrs, []*proxypb.SpanLink{{TraceId: trID, SpanId: spID}}, nil, prof.ExpectedScope),
		)
	}
	if m.hasFault("pull-wrong-count") {
		msgs = append(msgs, msgs...)
	}
	return &proxypb.UnaryPullResponse{Messages: msgs}, nil
}

type m3eSubDelivery struct {
	ackID, msgID, traceID, spanID, procID string
	data                                  []byte
	attrs                                 map[string]string
	tStart                                time.Time
	eod, settled, subSpanEmitted          bool
	mu                                    sync.Mutex
	modackWg                              sync.WaitGroup
	links                                 []*proxypb.SpanLink
	extraEvents                           []*proxypb.SpanEvent
}

func (m *m3eMockServer) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	cmd := first.GetStart()
	if cmd == nil {
		return status.Error(codes.InvalidArgument, "first command must be start")
	}
	if m.hasFault("block-sub-ctx") {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	stopErr := func(err error) error {
		st, _ := status.FromError(err)
		_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{ErrorCode: int32(st.Code()), ErrorMessage: st.Message()}}})
		return nil
	}
	if cmd.GetProjectId() == "" && !strings.HasPrefix(cmd.GetSubscription(), "projects/") {
		switch {
		case m.hasFault("wire-leak-init-sub"):
			m3eLeakWire(stream.Context(), cmd.GetEndpoint(), "sub")
			return stopErr(status.Error(codes.InvalidArgument, "missing project_id"))
		case m.hasFault("reject-sub-init-unimpl"):
			return stopErr(status.Error(codes.Unimplemented, "unimplemented"))
		case strings.HasPrefix(m.cfg.Fault, "deferred-sub-"):
			_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{}}})
			switch m.cfg.Fault {
			case "deferred-sub-hang":
				for _, rerr := stream.Recv(); rerr == nil; _, rerr = stream.Recv() {
				}
				return nil
			case "deferred-sub-deliv":
				_ = stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: "bad-tok"}}})
				return nil
			case "deferred-sub-unimpl":
				return stopErr(status.Error(codes.Unimplemented, "unimplemented"))
			case "deferred-sub-wire-leak":
				m3eLeakWire(stream.Context(), cmd.GetEndpoint(), "sub")
			}
			return stopErr(status.Error(codes.InvalidArgument, "missing project_id"))
		}
	}
	proj, fullSub, conn, err := m.resolveAndDial(cmd.GetProjectId(), cmd.GetSubscription(), "subscriptions", cmd.GetEndpoint(), cmd.GetUseEmulatorEnv())
	if err != nil {
		return stopErr(err)
	}
	defer conn.Close()
	sc := pubsubpb.NewSubscriberClient(conn)
	var sendMu, delivMu sync.Mutex
	sendEv := func(ev *proxypb.SubscriberEvent) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(ev)
	}
	_ = sendEv(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{}}})
	mon := cmd.GetEnableMonitoring() && m.cfg.Monitoring
	emitSpans, prof, shortSub := mon && !m.hasFault("omit-emulator-trace"), traces.ProfileForLanguage(m.cfg.Lang), m3eShortRes(fullSub)
	pullCtx, pullCancel := context.WithCancel(stream.Context())
	defer pullCancel()
	delivs, stopOnce, deliveredCount, wg := make(map[string]*m3eSubDelivery), sync.Once{}, atomic.Int64{}, sync.WaitGroup{}
	if cmd.MaxDurationPerAckExtensionMs != nil && *cmd.MaxDurationPerAckExtensionMs > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk := time.NewTicker(15 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-pullCtx.Done():
					return
				case <-tk.C:
					delivMu.Lock()
					var ids []string
					var ds []*m3eSubDelivery
					for _, d := range delivs {
						if !d.settled {
							d.modackWg.Add(1)
							ids, ds = append(ids, d.ackID), append(ds, d)
						}
					}
					delivMu.Unlock()
					if len(ids) > 0 {
						t0 := time.Now()
						_, merr := sc.ModifyAckDeadline(m.outCtx(pullCtx, "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: ids, AckDeadlineSeconds: 10})
						if merr == nil && emitSpans && !m.hasFault("omit-periodic-modack-span") {
							m.recordModAckSpan(prof, proj, fullSub, shortSub, ds, t0, false)
						}
						for _, d := range ds {
							d.modackWg.Done()
						}
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for attempt := 0; pullCtx.Err() == nil; attempt++ {
			sp, err := sc.StreamingPull(m.outCtx(pullCtx, "stream", fullSub, true, attempt > 0, cmd.GetCustomHeaders()))
			if err != nil {
				if pullCtx.Err() != nil || m.hasFault("no-reconnect") {
					return
				}
				continue
			}
			cid := cmp.Or(cmd.GetClientId(), "m3e-default-client")
			if lang := strings.ToLower(m.cfg.Lang); lang == "dotnet" || m.hasFault("empty-client-id") {
				cid = ""
			} else if m.hasFault("inconsistent-client-id") {
				cid = fmt.Sprintf("node-%d", m.nodeSeq.Add(1))
			}
			if sp.Send(&pubsubpb.StreamingPullRequest{Subscription: map[bool]string{false: fullSub, true: ""}[attempt > 0 && m.hasFault("reconnect-omit-sub")], StreamAckDeadlineSeconds: 10, ClientId: cid}) != nil {
				continue
			}
			if m.hasFault("trac04-phasea-sp-bad-mod-dl") && !cmd.GetUseExactlyOnce() {
				_ = sp.Send(&pubsubpb.StreamingPullRequest{ModifyDeadlineAckIds: []string{"ack-sp-bad"}, ModifyDeadlineSeconds: []int32{601}})
			}
			for {
				resp, err := sp.Recv()
				if err != nil {
					if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable && !m.hasFault("no-reconnect") && pullCtx.Err() == nil {
						break
					}
					return
				}
				eod := cmd.GetUseExactlyOnce()
				if spProp := resp.GetSubscriptionProperties(); spProp != nil {
					eod = spProp.GetExactlyOnceDeliveryEnabled()
				}
				for _, rm := range resp.GetReceivedMessages() {
					msg, tok, trID := rm.GetMessage(), fmt.Sprintf("tok-%d", m.idSeq.Add(1)), m.nextHex(32)
					attrs := msg.GetAttributes()
					if tp, err := traces.ParseTraceparent(attrs[fake.TraceparentAttributeKey]); err == nil {
						trID = tp.TraceID
					}
					tSt := time.Now()
					if m.hasFault("process-before-ack") {
						tSt = tSt.Add(-40 * time.Millisecond)
					}
					d := &m3eSubDelivery{ackID: rm.GetAckId(), msgID: msg.GetMessageId(), data: msg.GetData(), attrs: attrs, traceID: trID, spanID: m.nextHex(16), procID: m.nextHex(16), tStart: tSt, eod: eod}
					if mon && m.cfg.Lang == "go" {
						rt0 := time.Now()
						if _, merr := sc.ModifyAckDeadline(m.outCtx(pullCtx, "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: 10}); merr == nil && emitSpans {
							m.recordModAckSpan(prof, proj, fullSub, shortSub, []*m3eSubDelivery{d}, rt0, true)
						}
					}
					delivMu.Lock()
					delivs[tok] = d
					delivMu.Unlock()
					cbTP := ""
					if (mon || m.hasFault("cb-trace-disabled")) && !(m.hasFault("drop-cb-trace") && !eod) && !(m.hasFault("drop-cb-trace-b") && eod) {
						cbTP = fmt.Sprintf("00-%s-%s-01", d.traceID, d.procID)
					}
					dID := cmp.Or(map[bool]string{true: "wrong-msg-id"}[m.hasFault("wrong-sub-msg-id")], d.msgID)
					dData := map[bool][]byte{false: d.data, true: []byte("wrong-data")}[m.hasFault("wrong-sub-data")]
					dKey := cmp.Or(map[bool]string{true: "unexpected-key"}[m.hasFault("wrong-sub-key")], msg.GetOrderingKey())
					dAttrs := maps.Clone(attrs)
					if m.hasFault("wrong-sub-attr") {
						dAttrs = map[string]string{"user_k": "bad", "mode": "bad"}
					}
					deliveredCount.Add(1)
					_ = sendEv(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: tok, MessageId: dID, Data: dData, Attributes: dAttrs, OrderingKey: dKey, CallbackTraceparent: cbTP, IsExactlyOnce: eod}}})
				}
			}
		}
	}()
	for {
		in, err := stream.Recv()
		if err != nil {
			stopOnce.Do(pullCancel)
			wg.Wait()
			if err == io.EOF {
				return nil
			}
			return err
		}
		if sCmd := in.GetSettle(); sCmd != nil {
			delivMu.Lock()
			d := delivs[sCmd.GetDeliveryToken()]
			if d != nil {
				d.settled = true
			}
			delivMu.Unlock()
			if d == nil {
				continue
			}
			d.modackWg.Wait()
			tCalled, isNack, st0 := time.Now(), sCmd.GetAction() == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK, time.Now()
			var sErr error
			if isNack {
				_, sErr = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: 0})
				if m.hasFault("trac04-phasea-nack-also-ack") && !d.eod {
					_, _ = sc.Acknowledge(m.outCtx(stream.Context(), "ack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.AcknowledgeRequest{Subscription: fullSub, AckIds: []string{d.ackID}})
				}
			} else if d.eod && m.hasFault("eod-skip-wire-ack") {
				sErr = status.Error(codes.InvalidArgument, "trac04-eod-perm")
			} else {
				_, sErr = sc.Acknowledge(m.outCtx(stream.Context(), "ack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.AcknowledgeRequest{Subscription: fullSub, AckIds: []string{d.ackID}})
				switch m.cfg.Fault {
				case "cfg03-spurious-nack", "sub-spurious-nack", "trac01b-spurious-nack":
					_, _ = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: 0})
				case "sub-neg-modack", "trac01b-neg-modack":
					_, _ = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: -1})
				case "sub-oob-modack", "trac01b-oob-modack":
					_, _ = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: 5})
				case "unexpected-gettopic-sub":
					_ = conn.Invoke(stream.Context(), "/google.pubsub.v1.Publisher/GetTopic", &pubsubpb.GetTopicRequest{Topic: "projects/p/topics/t"}, &pubsubpb.Topic{})
				case "unexpected-pull-no-unary-pull", "sub-spurious-pull":
					_, _ = sc.Pull(m.outCtx(stream.Context(), "pull", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.PullRequest{Subscription: fullSub, MaxMessages: 1, ReturnImmediately: true})
				case "trac04-neg-modack":
					if d.eod {
						_, _ = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: -1})
					}
				case "trac04-oob-modack":
					if d.eod {
						_, _ = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: 5})
					}
				case "trac04-phasea-spurious-nack":
					if !d.eod && strings.Contains(d.msgID, "m-a1") {
						_, _ = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: 0})
					}
				case "trac04-phasea-neg-modack":
					if !d.eod && strings.Contains(d.msgID, "m-a1") {
						_, _ = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: -1})
					}
				case "trac04-phasea-oob-modack-601":
					if !d.eod && strings.Contains(d.msgID, "m-a1") {
						_, _ = sc.ModifyAckDeadline(m.outCtx(stream.Context(), "modack", fullSub, false, false, cmd.GetCustomHeaders()), &pubsubpb.ModifyAckDeadlineRequest{Subscription: fullSub, AckIds: []string{d.ackID}, AckDeadlineSeconds: 601})
					}
				}
			}
			if emitSpans {
				m.emitSettlementAndSubSpans(prof, proj, fullSub, shortSub, d, isNack, sErr, tCalled, st0, time.Now())
			}
			if sCmd.GetAction() == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT {
				st, code, msg := proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, int32(0), ""
				if sErr != nil && !m.hasFault("swallow-eod-settlement") {
					st, code, msg = proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID, int32(status.Code(sErr)), sErr.Error()
				}
				_ = sendEv(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_SettlementResult{SettlementResult: &proxypb.SettlementResultEvent{DeliveryToken: sCmd.GetDeliveryToken(), Status: st, GrpcErrorCode: code, ErrorMessage: msg}}})
			}
		} else if in.GetStop() != nil {
			stopOnce.Do(pullCancel)
			wg.Wait()
			_ = sendEv(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: deliveredCount.Load()}}})
			return nil
		}
	}
}

func (m *m3eMockServer) recordModAckSpan(prof traces.Profile, proj, fullSub, shortSub string, ds []*m3eSubDelivery, t0 time.Time, receipt bool) {
	if m.hasFault("only-receipt-modack") {
		receipt = true
	}
	mID, mTrace, t1, attrs := m.nextHex(16), m.nextHex(32), time.Now(), m.commonAttrs(proj, fullSub, shortSub)
	var active []*m3eSubDelivery
	var links []*proxypb.SpanLink
	for _, d := range ds {
		d.mu.Lock()
		if !d.subSpanEmitted {
			active, links = append(active, d), append(links, &proxypb.SpanLink{TraceId: d.traceID, SpanId: d.spanID})
		}
		d.mu.Unlock()
	}
	if len(links) == 0 {
		return
	}
	attrs["messaging.batch.message_count"], attrs["messaging.gcp_pubsub.is_receipt_modack"], attrs["messaging.gcp_pubsub.message.ack_deadline"] = m3eIntA(int64(len(links))), m3eBoolA(receipt), m3eIntA(10)
	for _, d := range active {
		d.mu.Lock()
		d.links = append(d.links, &proxypb.SpanLink{TraceId: mTrace, SpanId: mID, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": m3eStrA("modack")}})
		d.extraEvents = append(d.extraEvents, m3eEv("modack start", t0, int64(len(links))), m3eEv("modack end", t1, 0))
		if prof.UseEnvelopeSize {
			d.extraEvents = append(d.extraEvents, m3eEv("gl-cpp.modack_start", t0, 0), m3eEv("gl-cpp.modack_end", t1, 0))
		}
		d.mu.Unlock()
	}
	if prof.UseEnvelopeSize {
		attrs["messaging.gcp_pubsub.message.ack_deadline_seconds"], attrs["messaging.operation.type"], attrs["gl-cpp.status_code"], attrs["messaging.gcp_pubsub.subscription.template"] = m3eIntA(10), m3eStrA("extend"), m3eStrA("OK"), m3eStrA(fullSub)
		if prof.CodeFunctionAttrKey != "" {
			attrs[prof.CodeFunctionAttrKey] = m3eStrA("Acknowledge")
		}
	} else if prof.CodeFunctionAttrKey != "" {
		attrs[prof.CodeFunctionAttrKey] = m3eStrA("sendModAck")
	}
	m.addSpans(m3eSpan(mTrace, mID, "", shortSub+" modack", prof.ModAckSpanKind, t0, t1, 0, attrs, links, []*proxypb.SpanEvent{m3eEv("modack start", t0, 0), m3eEv("modack end", t1, 0)}, prof.ExpectedScope))
}

func (m *m3eMockServer) emitSettlementAndSubSpans(prof traces.Profile, proj, fullSub, shortSub string, d *m3eSubDelivery, isNack bool, sErr error, tCalled, s0, s1 time.Time) {
	op, fnGo, resKey := "ack", "sendAck", "acked"
	if isNack {
		op, fnGo, resKey = "nack", "sendModAck", "nacked"
	}
	settleID, settleTrace, sAttrs, subAttrs := m.nextHex(16), m.nextHex(32), m.commonAttrs(proj, fullSub, shortSub), m.commonAttrs(proj, fullSub, shortSub)
	sAttrs["messaging.batch.message_count"], subAttrs["messaging.batch.message_count"] = m3eIntA(1), m3eIntA(1)
	subAttrs["messaging.message.id"], subAttrs["messaging.message.body.size"], subAttrs["messaging.gcp_pubsub.message.ack_id"] = m3eStrA(d.msgID), m3eIntA(int64(len(d.data))), m3eStrA(d.ackID)
	if !prof.UseEnvelopeSize {
		subAttrs["messaging.gcp_pubsub.message.ordering_key"] = m3eStrA("")
	}
	if prof.EOSSubscribeAttrKey != "" {
		subAttrs[prof.EOSSubscribeAttrKey] = m3eBoolA(d.eod)
	}
	errSettle := sErr != nil && !m.hasFault("omit-eod-span-error")
	sCode, parentID, procEnd := proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", s1.Add(time.Millisecond)
	if sErr == nil || prof.UseEnvelopeSize {
		subAttrs["messaging.gcp_pubsub.result"] = m3eStrA(cmp.Or(prof.ResultValues[resKey], resKey))
	}
	if errSettle && !prof.UseEnvelopeSize {
		sCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
	}
	if tp, err := traces.ParseTraceparent(d.attrs[fake.TraceparentAttributeKey]); err == nil {
		parentID = tp.SpanID
	}
	switch m.cfg.Fault {
	case "process-before-ack":
		procEnd = tCalled.Add(-2 * time.Millisecond)
	case "process-too-short":
		tCalled, procEnd = d.tStart.Add(time.Millisecond), d.tStart.Add(2*time.Millisecond)
	}
	d.mu.Lock()
	d.subSpanEmitted = true
	links := append(append([]*proxypb.SpanLink(nil), d.links...), &proxypb.SpanLink{TraceId: settleTrace, SpanId: settleID, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": m3eStrA(op)}})
	subEvents := append(append([]*proxypb.SpanEvent(nil), d.extraEvents...), m3eEv(op+" start", s0, 1), m3eEv(op+" end", s1, 0))
	d.mu.Unlock()
	procAttrs := m.commonAttrs(proj, fullSub, shortSub)
	settleCode := map[bool]proxypb.SpanStatusCode{false: proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, true: proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR}[errSettle]
	var procEvents []*proxypb.SpanEvent
	if !prof.ProcessMinimalAttributes {
		procAttrs["messaging.message.id"], procAttrs["messaging.message.body.size"], procAttrs["messaging.gcp_pubsub.message.ordering_key"], procAttrs["messaging.operation.type"] = m3eStrA(d.msgID), m3eIntA(int64(len(d.data))), m3eStrA(""), m3eStrA("process")
	}
	if prof.ProcessEventsEnabled && !m.hasFault("omit-process-ack-called") {
		procEvents = []*proxypb.SpanEvent{m3eEv(op+" called", tCalled, 0)}
	}
	if prof.UseEnvelopeSize {
		subAttrs["messaging.message.envelope.size"], subAttrs["messaging.operation.type"] = m3eIntA(int64(proto.Size(&pubsubpb.PubsubMessage{MessageId: d.msgID, Data: d.data, Attributes: d.attrs})+24)), m3eStrA("subscribe")
		sAttrs["messaging.gcp_pubsub.message.ack_id"], sAttrs["messaging.operation.type"], sAttrs["messaging.gcp_pubsub.subscription.template"], sAttrs["gl-cpp.status_code"] = m3eStrA(d.ackID), m3eStrA("settle"), m3eStrA(fullSub), m3eStrA("OK")
		if sErr == nil {
			settleCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
		} else if !m.hasFault("omit-eod-span-error") {
			sAttrs["gl-cpp.status_code"] = m3eStrA("INVALID_ARGUMENT")
		}
		if prof.CodeFunctionAttrKey != "" {
			sAttrs[prof.CodeFunctionAttrKey] = m3eStrA("pubsub::AckHandler::" + op)
		}
		subEvents = append(subEvents, m3eEv("gl-cpp."+op+"_start", s0, 0), m3eEv("gl-cpp."+op+"_end", s1, 0))
	} else if prof.CodeFunctionAttrKey != "" {
		subAttrs[prof.CodeFunctionAttrKey], sAttrs[prof.CodeFunctionAttrKey] = m3eStrA("receive"), m3eStrA(fnGo)
	}
	childAttrs := map[bool]map[string]*proxypb.AttributeValue{false: m.commonAttrs(proj, fullSub, shortSub)}[prof.ExpectZeroChildAttrs]
	settleEvs, statusMsg := []*proxypb.SpanEvent{m3eEv(op+" start", s0, 0), m3eEv(op+" end", s1, 0)}, ""
	if errSettle {
		statusMsg = sErr.Error()
		if !prof.UseEnvelopeSize {
			exEv := &proxypb.SpanEvent{Name: "exception", Timestamp: timestamppb.New(s1), Attributes: map[string]*proxypb.AttributeValue{"exception.type": m3eStrA("*status.Error"), "exception.message": m3eStrA(statusMsg)}}
			settleEvs, subEvents = append(settleEvs, exEv), append(subEvents, exEv)
		}
	}
	settleSpan := m3eSpan(settleTrace, settleID, "", shortSub+" "+op, proxypb.SpanKind_SPAN_KIND_INTERNAL, s0, s1, settleCode, sAttrs, []*proxypb.SpanLink{{TraceId: d.traceID, SpanId: d.spanID}}, settleEvs, prof.ExpectedScope)
	subSpan := m3eSpan(d.traceID, d.spanID, parentID, shortSub+" subscribe", prof.SubscribeSpanKind, d.tStart, procEnd, sCode, subAttrs, links, subEvents, prof.ExpectedScope)
	settleSpan.StatusMessage, subSpan.StatusMessage = statusMsg, statusMsg
	spans := []*proxypb.TraceSpan{settleSpan, subSpan,
		m3eSpan(d.traceID, m.nextHex(16), d.spanID, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, d.tStart, s0, 0, childAttrs, nil, nil, prof.ExpectedScope),
		m3eSpan(d.traceID, m.nextHex(16), d.spanID, cmp.Or(prof.SchedulerSpanName, "subscribe scheduler"), proxypb.SpanKind_SPAN_KIND_INTERNAL, d.tStart, s0, 0, childAttrs, nil, nil, prof.ExpectedScope),
	}
	if !m.hasFault("omit-process-span") {
		spans = append(spans, m3eSpan(d.traceID, d.procID, d.spanID, shortSub+" process", prof.ProcessSpanKind, d.tStart, procEnd, 0, procAttrs, nil, procEvents, prof.ExpectedScope))
	}
	m.addSpans(spans...)
}

type m3eProxyHandle struct {
	srv    *grpc.Server
	mock   *m3eMockServer
	conn   *grpc.ClientConn
	client proxypb.PubSubProxyClient
	caps   *proxypb.GetCapabilitiesResponse
	exited chan error
	wg     sync.WaitGroup
}

func (h *m3eProxyHandle) Client() proxypb.PubSubProxyClient              { return h.client }
func (h *m3eProxyHandle) Capabilities() *proxypb.GetCapabilitiesResponse { return h.caps }
func (h *m3eProxyHandle) Exited() <-chan error                           { return h.exited }
func (h *m3eProxyHandle) Stderr() string                                 { return "" }
func (h *m3eProxyHandle) PID() int                                       { return 0 }
func (h *m3eProxyHandle) Close(context.Context) error {
	_ = h.conn.Close()
	h.mock.closeAll()
	h.srv.Stop()
	h.wg.Wait()
	return nil
}

func m3eNewServer(cfg m3eProfileCfg, emu string) (*m3eMockServer, *grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, nil, err
	}
	sctx, scancel := context.WithCancel(context.Background())
	mock, srv := &m3eMockServer{cfg: cfg, emuAddr: emu, stopCtx: sctx, stopCancel: scancel, pubs: make(map[string]*m3ePublisher)}, grpc.NewServer()
	proxypb.RegisterPubSubProxyServer(srv, mock)
	return mock, srv, lis, nil
}

func TestConfigMockSubprocess(t *testing.T) {
	if os.Getenv("CONFIG_MOCK_SUBPROC") != "1" {
		return
	}
	var cfg m3eProfileCfg
	if err := json.Unmarshal([]byte(os.Getenv("CONFIG_MOCK_JSON")), &cfg); err != nil {
		t.Fatalf("bad CONFIG_MOCK_JSON: %v", err)
	}
	mock, srv, lis, err := m3eNewServer(cfg, os.Getenv("PUBSUB_EMULATOR_HOST"))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer mock.closeAll()
	go func() { <-mock.stopCtx.Done(); srv.GracefulStop() }()
	fmt.Printf("READY %s\n", lis.Addr().String())
	_ = os.Stdout.Sync()
	_ = srv.Serve(lis)
}

func m3eTargetSpec(name string, cfg m3eProfileCfg, useSubproc bool) harness.TargetSpec {
	if useSubproc {
		b, _ := json.Marshal(cfg)
		exe, _ := os.Executable()
		return harness.TargetSpec{Name: name, Command: "/bin/sh", Args: []string{"-c", `exec "$1" -test.run=^TestConfigMockSubprocess$`, "sh", exe, "--port=0"}, Env: []string{"CONFIG_MOCK_SUBPROC=1", "CONFIG_MOCK_JSON=" + string(b)}}
	}
	return harness.TargetSpec{
		Name: name,
		Connect: func(ctx context.Context, _ harness.TargetSpec) (harness.ProxyHandle, error) {
			mock, srv, lis, err := m3eNewServer(cfg, "")
			if err != nil {
				return nil, err
			}
			h := &m3eProxyHandle{srv: srv, mock: mock, exited: make(chan error, 1)}
			h.wg.Add(1)
			go func() { defer h.wg.Done(); h.exited <- srv.Serve(lis) }()
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				mock.closeAll()
				srv.Stop()
				h.wg.Wait()
				return nil, err
			}
			h.conn, h.client = conn, proxypb.NewPubSubProxyClient(conn)
			h.caps, _ = h.client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
			return h, nil
		},
	}
}

type m3eFaultCase struct {
	name, scID string
	cfg        m3eProfileCfg
	to         time.Duration
	want       harness.Status
}

func m3eRunSuite(t *testing.T, scs []harness.Scenario, cfg m3eProfileCfg, to time.Duration) *harness.TargetReport {
	t.Helper()
	cloned := slices.Clone(scs)
	if to > 0 {
		for i := range cloned {
			if cloned[i].Timeout == 0 || cloned[i].Timeout > to {
				cloned[i].Timeout = to
			}
		}
	}
	useSubproc := slices.ContainsFunc(cloned, func(sc harness.Scenario) bool { return sc.UseEmulatorEnv })
	runner, err := harness.NewRunner(harness.RunConfig{Scenarios: cloned, ProjectID: "m3e-proj", ScenarioTimeout: to})
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	rep, err := runner.RunTarget(context.Background(), m3eTargetSpec("mock-"+cfg.Lang, cfg, useSubproc))
	if err != nil {
		t.Fatalf("RunTarget: %v", err)
	}
	return rep
}

func m3eRunFaults(t *testing.T, list []harness.Scenario, scID string, base m3eProfileCfg, to time.Duration, want harness.Status, faults ...string) {
	t.Helper()
	sc := list[slices.IndexFunc(list, func(sc harness.Scenario) bool { return sc.ID == scID })]
	want = cmp.Or(want, harness.StatusFail)
	for _, f := range faults {
		cfg := base
		cfg.Fault = f
		t.Run(scID+"_"+f, func(t *testing.T) {
			if rep := m3eRunSuite(t, []harness.Scenario{sc}, cfg, to); rep.Scenarios[0].Status != want {
				t.Fatalf("fault %q: want %s, got %+v", f, want, rep.Scenarios)
			}
		})
	}
}

func m3eRunNoLeakFaults(t *testing.T, list []harness.Scenario, to time.Duration, cases []m3eFaultCase) {
	t.Helper()
	settled := func(maxG, maxFD int) (int, int) {
		dl, tk, buf := time.Now().Add(2*time.Second), time.NewTicker(15*time.Millisecond), make([]byte, 256<<10)
		defer tk.Stop()
		for ; ; <-tk.C {
			runtime.GC()
			fd := -1
			if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
				fd = len(entries)
			}
			s, g := string(buf[:runtime.Stack(buf, true)]), runtime.NumGoroutine()
			if !strings.Contains(s, "m3eMock") && !strings.Contains(s, "startSubPump") && !strings.Contains(s, "subPump") && (maxG < 0 || g <= maxG) && (maxFD < 0 || fd <= maxFD) {
				return g, fd
			}
			if time.Now().After(dl) {
				t.Fatalf("REMSPLIT650 leak: g=%d(max=%d) fd=%d(max=%d)\n%s", g, maxG, fd, maxFD, s)
			}
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc, coldG, coldFD := list[slices.IndexFunc(list, func(sc harness.Scenario) bool { return sc.ID == tc.scID })], 0, 0
			for i := range 4 {
				if rep := m3eRunSuite(t, []harness.Scenario{sc}, tc.cfg, to); rep.Scenarios[0].Status != harness.StatusTimeout {
					t.Fatalf("iter %d: want TIMEOUT, got %+v", i, rep.Scenarios)
				}
				if i == 0 {
					coldG, coldFD = settled(-1, -1)
				} else if i == 1 || i == 3 {
					settled(coldG, coldFD)
				}
			}
		})
	}
}

func TestConfigScenarios_Metadata(t *testing.T) {
	cfgs := scenarios.ExportConfigScenarios()
	if scenarios.Len() != 65 || len(cfgs) != 4 {
		t.Fatalf("len mismatch: default=%d cfgs=%d", scenarios.Len(), len(cfgs))
	}
	for i, id := range []string{"CFG-01", "CFG-02", "CFG-03", "CFG-04"} {
		sc := cfgs[i]
		if sc.ID != id || sc.Category != "config" || sc.Feature == "" || sc.Description == "" || sc.TraceOnly || sc.Timeout != 0 {
			t.Fatalf("scenario[%d] unexpected metadata: %+v", i, sc)
		}
		if strings.Contains(sc.ID+sc.Name+sc.Category, "PUB-01") || strings.Contains(sc.ID+sc.Name+sc.Category, "NOMATCH") {
			t.Fatalf("scenario %s contains forbidden filter token", sc.ID)
		}
	}
	if !cfgs[0].IsolateProxy || !cfgs[0].UseEmulatorEnv || !cfgs[1].IsolateProxy {
		t.Fatalf("CFG-01/CFG-02 flags wrong: %+v %+v", cfgs[0], cfgs[1])
	}
	if out, err := harness.FilterScenarios(cfgs, "^NOMATCH$"); err != nil || len(out) != 0 {
		t.Fatalf("^NOMATCH$ matched %d (err=%v)", len(out), err)
	}
}

func TestConfigScenarios_AllNineSDKProfiles(t *testing.T) {
	cfgs := scenarios.ExportConfigScenarios()
	profiles := []m3eProfileCfg{{Lang: "go", Monitoring: true, Hedging: true, EOD: true}, {Lang: "cpp", Monitoring: true, UnaryPull: true, CppSpans: true}, {Lang: "rust", Hedging: true, UnaryPull: true}, {Lang: "java"}, {Lang: "python"}, {Lang: "ruby"}, {Lang: "node"}, {Lang: "dotnet"}, {Lang: "php"}}
	for _, prof := range profiles {
		t.Run(prof.Lang, func(t *testing.T) {
			wantTS := map[bool]traces.Status{false: traces.StatusSkippedNoMonitoring, true: traces.StatusPass}[prof.Monitoring]
			for _, grp := range [][]harness.Scenario{cfgs[:1], cfgs[1:]} {
				rep := m3eRunSuite(t, grp, prof, 0)
				if rep.TargetError != "" || rep.Summary.Fail != 0 || rep.Summary.Pass != len(grp) {
					t.Fatalf("profile %s failed: target_err=%q summary=%+v results=%+v", prof.Lang, rep.TargetError, rep.Summary, rep.Scenarios)
				}
				for _, sc := range rep.Scenarios {
					if sc.TraceStatus != wantTS {
						t.Fatalf("profile %s scenario %s TraceStatus = %q, want %q", prof.Lang, sc.ID, sc.TraceStatus, wantTS)
					}
				}
			}
		})
	}
}

func TestConfigScenarios_FaultDetection(t *testing.T) {
	cfgs := scenarios.ExportConfigScenarios()
	m3eRunFaults(t, cfgs, "CFG-01", m3eProfileCfg{Lang: "go", Monitoring: true}, 0, "", "ignore-empty-endpoint", "omit-emulator-trace", "drop-wire-trace", "drop-cb-trace", "wrong-stream-params", "wrong-unary-params-ack", "wrong-unary-params-pub")
	m3eRunFaults(t, cfgs, "CFG-01", m3eProfileCfg{Lang: "cpp", Monitoring: true, UnaryPull: true, CppSpans: true}, 0, "", "omit-pull-trace", "wrong-pull-msg-id", "wrong-unary-params-pullack")
	m3eRunFaults(t, cfgs, "CFG-01", m3eProfileCfg{Lang: "rust", UnaryPull: true}, 0, "", "wrong-pub-attr", "wrong-sub-attr", "wrong-pub-msg-id")
	m3eRunFaults(t, cfgs, "CFG-02", m3eProfileCfg{Lang: "go"}, 650*time.Millisecond, harness.StatusTimeout, "no-reconnect")
	m3eRunFaults(t, cfgs, "CFG-02", m3eProfileCfg{Lang: "go", UnaryPull: true}, 0, "",
		"drop-custom-headers", "wrong-header-val", "drop-headers-reconnect", "empty-client-id", "wrong-stream-params", "wrong-unary-params", "wrong-unary-params-pull", "wrong-unary-params-ack", "wrong-unary-params-modack", "wrong-unary-params-pub", "wrong-unary-params-pullack", "drop-hdr-pub", "drop-hdr-ack", "drop-hdr-modack", "drop-hdr-pull", "drop-hdr-pullack", "reconnect-omit-sub", "dup-pub-rpc", "pull-wrong-max", "pullack-wrong-id")
	m3eRunFaults(t, cfgs, "CFG-02", m3eProfileCfg{Lang: "node"}, 0, "", "inconsistent-client-id")
	m3eRunFaults(t, cfgs, "CFG-02", m3eProfileCfg{Lang: "ruby"}, 0, "", "ruby-missing-stream-params")
	m3eRunFaults(t, cfgs, "CFG-02", m3eProfileCfg{Lang: "go", Monitoring: true}, 0, "", "inject-unexpected-span")
	m3eRunFaults(t, cfgs, "CFG-03", m3eProfileCfg{Lang: "go"}, 0, harness.StatusPass, "deferred-pub-reject", "deferred-sub-reject")
	m3eRunFaults(t, cfgs, "CFG-03", m3eProfileCfg{Lang: "go"}, 0, "", "cfg03-rewrite-full-path")
	m3eRunFaults(t, cfgs, "CFG-03", m3eProfileCfg{Lang: "go", UnaryPull: true}, 0, "",
		"accept-short-empty-proj", "reject-empty-proj-unimpl", "wire-leak-create-pub", "deferred-pub-unimpl", "deferred-pub-rpc-unimpl", "deferred-pub-ok", "deferred-pub-wire-leak", "wire-leak-init-sub", "reject-sub-init-unimpl", "deferred-sub-hang", "deferred-sub-deliv", "deferred-sub-unimpl", "deferred-sub-wire-leak", "pull-empty-proj-inband-unimpl", "pull-empty-proj-rpc-unimpl", "pull-empty-proj-ok", "pull-empty-proj-wire-leak")
	m3eRunFaults(t, cfgs, "CFG-03", m3eProfileCfg{Lang: "go", Monitoring: true}, 0, "", "inject-unexpected-span")
	m3eRunFaults(t, cfgs, "CFG-04", m3eProfileCfg{Lang: "go"}, 0, "", "rewrite-cross-project", "rewrite-cross-project-sub")
	m3eRunFaults(t, cfgs, "CFG-04", m3eProfileCfg{Lang: "go", UnaryPull: true}, 0, "", "pull-inband-unimpl", "pull-inband-err", "pull-wrong-count")
	m3eRunFaults(t, cfgs, "CFG-04", m3eProfileCfg{Lang: "go", Monitoring: true}, 0, "", "inject-unexpected-span")
}

func TestConfigScenarios_NoGoroutineOrFDLeak_REMSPLIT650(t *testing.T) {
	m3eRunNoLeakFaults(t, scenarios.ExportConfigScenarios(), 650*time.Millisecond, []m3eFaultCase{
		{"CFG-02 no reconnect", "CFG-02", m3eProfileCfg{Lang: "go", Fault: "no-reconnect"}, 0, ""},
		{"CFG-04 block publish", "CFG-04", m3eProfileCfg{Lang: "go", Fault: "block-pub-ctx"}, 0, ""},
		{"CFG-04 block subscribe", "CFG-04", m3eProfileCfg{Lang: "go", Fault: "block-sub-ctx"}, 0, ""},
	})
}

func TestCFG03_SpuriousNack(t *testing.T) {
	cfgs := scenarios.ExportConfigScenarios()
	idx := slices.IndexFunc(cfgs, func(sc harness.Scenario) bool { return sc.ID == "CFG-03" })
	if idx < 0 {
		t.Fatal("CFG-03 missing from ExportConfigScenarios")
	}
	rep := m3eRunSuite(t, []harness.Scenario{cfgs[idx]}, m3eProfileCfg{Lang: "go", Fault: "cfg03-spurious-nack"}, 0)
	if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, "unexpected ModifyAckDeadline(0) nack call") {
		t.Fatalf("expected CFG-03 StatusFail containing 'unexpected ModifyAckDeadline(0) nack call', got scenarios=%+v", rep.Scenarios)
	}
}

func TestConfigScenarios_Iter12OracleTightening(t *testing.T) {
	cfgs := scenarios.ExportConfigScenarios()
	byID := func(id string) harness.Scenario {
		idx := slices.IndexFunc(cfgs, func(sc harness.Scenario) bool { return sc.ID == id })
		if idx < 0 {
			t.Fatalf("%s missing from ExportConfigScenarios", id)
		}
		return cfgs[idx]
	}

	cases := []struct {
		name, scID, fault, wantSub string
		cfg                        m3eProfileCfg
	}{
		// 9 CFG-01 cases
		{"CFG01_SubSpuriousNack", "CFG-01", "sub-spurious-nack", "nacked ack_ids set mismatch", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG01_SubSpuriousNack_NoUnaryPull", "CFG-01", "sub-spurious-nack", "nacked ack_ids set mismatch", m3eProfileCfg{Lang: "go"}},
		{"CFG01_AfterPullSpuriousNack", "CFG-01", "after-pull-spurious-nack", "unexpected post-pull ModifyAckDeadline(0) nack call", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG01_SubNegModAck", "CFG-01", "sub-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go"}},
		{"CFG01_SubOOBModAck", "CFG-01", "sub-oob-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go"}},
		{"CFG01_AfterPullNegModAck", "CFG-01", "after-pull-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG01_UnexpectedGetTopic_Sub", "CFG-01", "unexpected-gettopic-sub", "unexpected RPC", m3eProfileCfg{Lang: "go"}},
		{"CFG01_UnexpectedGetTopic_AfterPull", "CFG-01", "unexpected-gettopic-after-pull", "unexpected RPC", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG01_UnexpectedPull_NoUnaryPull", "CFG-01", "unexpected-pull-no-unary-pull", "disallowed RPC", m3eProfileCfg{Lang: "go"}},

		// 6 CFG-02..04 RPC cases
		{"CFG02_UnexpectedGetTopic_AfterPull", "CFG-02", "unexpected-gettopic-after-pull", "unexpected RPC", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG03_UnexpectedGetTopic_AfterPull", "CFG-03", "unexpected-gettopic-after-pull", "unexpected RPC", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG04_UnexpectedGetTopic_AfterPull", "CFG-04", "unexpected-gettopic-after-pull", "unexpected RPC", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG02_UnexpectedPull_NoUnaryPull", "CFG-02", "unexpected-pull-no-unary-pull", "disallowed RPC", m3eProfileCfg{Lang: "go"}},
		{"CFG03_UnexpectedPull_NoUnaryPull", "CFG-03", "unexpected-pull-no-unary-pull", "disallowed RPC", m3eProfileCfg{Lang: "go"}},
		{"CFG04_UnexpectedPull_NoUnaryPull", "CFG-04", "unexpected-pull-no-unary-pull", "disallowed RPC", m3eProfileCfg{Lang: "go"}},

		// 12 CFG-02..04 ModAck cases
		{"CFG02_AfterPullSpuriousNack", "CFG-02", "after-pull-spurious-nack", "unexpected post-pull ModifyAckDeadline(0) nack call", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG03_AfterPullSpuriousNack", "CFG-03", "after-pull-spurious-nack", "unexpected post-pull ModifyAckDeadline(0) nack call", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG04_AfterPullSpuriousNack", "CFG-04", "after-pull-spurious-nack", "unexpected post-pull ModifyAckDeadline(0) nack call", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG02_SubNegModAck", "CFG-02", "sub-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go"}},
		{"CFG03_SubNegModAck", "CFG-03", "sub-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go"}},
		{"CFG04_SubNegModAck", "CFG-04", "sub-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go"}},
		{"CFG02_SubOOBModAck", "CFG-02", "sub-oob-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go"}},
		{"CFG03_SubOOBModAck", "CFG-03", "sub-oob-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go"}},
		{"CFG04_SubOOBModAck", "CFG-04", "sub-oob-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "go"}},
		{"CFG02_AfterPullNegModAck", "CFG-02", "after-pull-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG03_AfterPullNegModAck", "CFG-03", "after-pull-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG04_AfterPullNegModAck", "CFG-04", "after-pull-neg-modack", "outside 0 or [10, 600]", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},

		// Additional branch coverage for m3eVerifySubAndPullAfter (Pull <= wmPull & pullModAcks customHeaders)
		{"CFG02_SubSpuriousPullBeforeUnaryPull", "CFG-02", "sub-spurious-pull", "unexpected Pull RPC", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
		{"CFG02_AfterPullModAckDropCustomHeaders", "CFG-02", "after-pull-modack-drop-hdr", "custom header", m3eProfileCfg{Lang: "cpp", UnaryPull: true}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Fault = tc.fault
			rep := m3eRunSuite(t, []harness.Scenario{byID(tc.scID)}, cfg, 0)
			if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, tc.wantSub) {
				t.Fatalf("%s: expected StatusFail containing %q, got scenarios=%+v", tc.name, tc.wantSub, rep.Scenarios)
			}
		})
	}
}
