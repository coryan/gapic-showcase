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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_CLI_MAIN") == "1" {
		_ = os.Unsetenv("GO_WANT_CLI_MAIN")
		_ = os.Setenv("GO_WANT_HELPER_PROCESS", "1")
		var args []string
		if raw := os.Getenv("CLI_MAIN_ARGS"); raw != "" && json.Unmarshal([]byte(raw), &args) != nil {
			os.Exit(2)
		}
		os.Args = append([]string{"pubsub-conformance"}, args...)
		main()
		return
	}
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	args := os.Args[1:]
	if idx := slices.Index(args, "--"); idx >= 0 {
		args = args[idx+1:]
	}
	mode, argsFile, pidFile, startedFile := "conformant-go", "", "", ""
	flags := map[string]*string{"--mode=": &mode, "--args-file=": &argsFile, "--pid-file=": &pidFile, "--started-file=": &startedFile}
	for _, a := range args {
		for k, dst := range flags {
			if v, ok := strings.CutPrefix(a, k); ok {
				*dst = v
			}
		}
	}
	if argsFile != "" {
		b, _ := json.Marshal(args)
		_ = os.WriteFile(argsFile, b, 0o600)
	}
	if pidFile != "" {
		_ = os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600)
	}
	switch mode {
	case "crash-startup":
		os.Exit(2)
	case "bad-ready":
		fmt.Println("NOT_READY 123")
		select {}
	case "ignore-shutdown":
		signal.Ignore(syscall.SIGTERM)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.Exit(1)
	}
	srv, hp := grpc.NewServer(), newHelperProxy(mode, startedFile)
	proxypb.RegisterPubSubProxyServer(srv, hp)
	go func() { _ = srv.Serve(lis) }()
	fmt.Printf("READY %s\n", lis.Addr().String())
	_ = os.Stdout.Sync()
	<-hp.done
	srv.GracefulStop()
	os.Exit(0)
}

type helperItem struct {
	data, key, parentTP string
	attrs               map[string]string
	resCh               chan *proxypb.PublishResponse
}
type helperPub struct {
	topic, projectID string
	enableMon        bool
	conn             *grpc.ClientConn
	client           pubsubpb.PublisherClient
	mu               sync.Mutex
	closed           bool
	keyBuf           map[string][]*helperItem
	keyCh            map[string]chan []*helperItem
	tokens           map[string]chan *proxypb.PublishResponse
	wg               sync.WaitGroup
}
type helperProxy struct {
	proxypb.UnimplementedPubSubProxyServer
	mode, startedFile string
	caps              *proxypb.GetCapabilitiesResponse
	done              chan struct{}
	doneOnce          sync.Once
	mu                sync.Mutex
	pubs              map[string]*helperPub
	spans             []*proxypb.TraceSpan
	seq               atomic.Int64
}

func newHelperProxy(mode, startedFile string) *helperProxy {
	caps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage: "go", ClientLibrary: "cloud.google.com/go/pubsub/v2", ClientVersion: "2.0.0",
		SupportsMonitoring: true, SupportsUnaryPull: true, SupportsOrderingOptIn: true, SupportsKeepalive: true,
		SupportsShutdownBehavior: true, SupportsShutdownTimeout: true,
		Defaults: &proxypb.ClientDefaults{PublishFlowControlBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE},
	}
	switch mode {
	case "no-mon":
		caps = &proxypb.GetCapabilitiesResponse{ClientLanguage: "rust", ClientLibrary: "google-cloud-pubsub", ClientVersion: "1.0.0"}
	case "partial-unimpl":
		caps = &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsUnaryPull: true}
	}
	return &helperProxy{mode: mode, startedFile: startedFile, caps: caps, done: make(chan struct{}), pubs: map[string]*helperPub{}}
}

func (h *helperProxy) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return h.caps, nil
}
func (h *helperProxy) ResetTraces(context.Context, *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	h.mu.Lock()
	h.spans = nil
	h.mu.Unlock()
	return &proxypb.ResetTracesResponse{}, nil
}
func (h *helperProxy) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	h.mu.Lock()
	out := slices.Clone(h.spans)
	h.spans = nil
	h.mu.Unlock()
	if h.mode == "bad-traces" {
		out = []*proxypb.TraceSpan{{TraceId: rHex(16), SpanId: rHex(8), Name: "broken.span", Kind: proxypb.SpanKind_SPAN_KIND_CLIENT}}
	}
	return &proxypb.DrainTracesResponse{Spans: out}, nil
}
func (h *helperProxy) ShutdownProxy(ctx context.Context, _ *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	if h.mode == "ignore-shutdown" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	h.doneOnce.Do(func() { close(h.done) })
	return &proxypb.ShutdownProxyResponse{}, nil
}
func (h *helperProxy) addSpan(s ...*proxypb.TraceSpan) {
	h.mu.Lock()
	h.spans = append(h.spans, s...)
	h.mu.Unlock()
}
func (h *helperProxy) getPub(id string, del bool) *helperPub {
	h.mu.Lock()
	defer h.mu.Unlock()
	p := h.pubs[id]
	if del {
		delete(h.pubs, id)
	}
	return p
}
func rHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	b[0] |= 1
	return hex.EncodeToString(b)
}
func mkAttrs(kv ...any) map[string]*proxypb.AttributeValue {
	m := make(map[string]*proxypb.AttributeValue, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		switch v := kv[i+1].(type) {
		case string:
			m[kv[i].(string)] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringValue{StringValue: v}}
		case int64:
			m[kv[i].(string)] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
		case bool:
			m[kv[i].(string)] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: v}}
		}
	}
	return m
}
func resAttrs(proj, full, short string, kv ...any) map[string]*proxypb.AttributeValue {
	m := mkAttrs("gcp.project_id", proj, "gcp.resource.name", full, "messaging.system", "gcp_pubsub", "messaging.destination.name", short)
	maps.Copy(m, mkAttrs(kv...))
	return m
}
func parseTPOrNew(tp string) (string, string) {
	if pt, err := traces.ParseTraceparent(tp); err == nil {
		return pt.TraceID, pt.SpanID
	}
	return rHex(16), ""
}
func mkSpan(tid, sid, psid, name string, kind proxypb.SpanKind, attrs map[string]*proxypb.AttributeValue, evs []*proxypb.SpanEvent, links []*proxypb.SpanLink) *proxypb.TraceSpan {
	now := time.Now()
	return &proxypb.TraceSpan{TraceId: tid, SpanId: sid, ParentSpanId: psid, TraceFlags: 1, Name: name, Kind: kind, StartTime: timestamppb.New(now), EndTime: timestamppb.New(now.Add(time.Millisecond)), InstrumentationScopeName: traces.GoPubSubScopeName, Attributes: attrs, Events: evs, Links: links}
}
func splitRes(full string) (string, string) {
	if p := strings.Split(full, "/"); len(p) >= 4 {
		return p[1], p[3]
	}
	return "conformance-project", full
}
func (h *helperProxy) outCtx(ctx context.Context, k, r string) context.Context {
	if h.mode != "no-mon" {
		r = url.QueryEscape(r)
	}
	return metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", k+"="+r)
}
func dialHelper(ep string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (h *helperProxy) CreatePublisher(ctx context.Context, req *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	if h.startedFile != "" {
		_ = os.WriteFile(h.startedFile, []byte("1"), 0o600)
	}
	if h.mode == "hang" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if h.mode == "crash-mid" {
		os.Exit(3)
	}
	conn, err := dialHelper(req.GetEndpoint())
	if err != nil {
		return nil, err
	}
	proj, _ := splitRes(req.GetTopic())
	id := fmt.Sprintf("hp-%d", h.seq.Add(1))
	h.mu.Lock()
	h.pubs[id] = &helperPub{topic: req.GetTopic(), projectID: proj, enableMon: req.GetEnableMonitoring(), conn: conn, client: pubsubpb.NewPublisherClient(conn), keyBuf: map[string][]*helperItem{}, keyCh: map[string]chan []*helperItem{}, tokens: map[string]chan *proxypb.PublishResponse{}}
	h.mu.Unlock()
	return &proxypb.CreatePublisherResponse{PublisherId: id}, nil
}

func (h *helperProxy) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	p := h.getPub(req.GetPublisherId(), false)
	if h.mode == "bad-rpc" {
		return &proxypb.PublishResponse{MessageId: "skipped-rpc-id"}, nil
	}
	it := &helperItem{data: string(req.GetData()), key: req.GetOrderingKey(), parentTP: req.GetParentTraceparent(), attrs: maps.Clone(req.GetAttributes()), resCh: make(chan *proxypb.PublishResponse, 1)}
	if req.GetAsyncFire() {
		tok := fmt.Sprintf("tok-%d", h.seq.Add(1))
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "publisher closed")
		}
		p.tokens[tok] = it.resCh
		p.keyBuf[it.key] = append(p.keyBuf[it.key], it)
		if q := p.keyBuf[it.key]; len(q) >= 2 || it.key == "" {
			p.keyBuf[it.key] = nil
			h.enqueueKeyBatchLocked(p, it.key, q)
		}
		p.mu.Unlock()
		return &proxypb.PublishResponse{PublishToken: tok}, nil
	}
	h.sendBatch(p, []*helperItem{it})
	return <-it.resCh, nil
}

func (h *helperProxy) enqueueKeyBatchLocked(p *helperPub, key string, batch []*helperItem) {
	if p.closed {
		return
	}
	ch := p.keyCh[key]
	if ch == nil {
		ch = make(chan []*helperItem, 64)
		p.keyCh[key] = ch
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for b := range ch {
				h.sendBatch(p, b)
			}
		}()
	}
	ch <- batch
}

func (h *helperProxy) sendBatch(p *helperPub, batch []*helperItem) {
	_, topicID := splitRes(p.topic)
	msgs := make([]*pubsubpb.PubsubMessage, len(batch))
	ptid, psid2, pubLinks, pending := rHex(16), rHex(8), []*proxypb.SpanLink(nil), make([]*proxypb.TraceSpan, len(batch))
	for i, it := range batch {
		attrs := maps.Clone(it.attrs)
		if p.enableMon && h.mode != "bad-traces" {
			tid, psid := parseTPOrNew(it.parentTP)
			csid := rHex(8)
			if attrs == nil {
				attrs = map[string]string{}
			}
			attrs[fake.TraceparentAttributeKey] = fmt.Sprintf("00-%s-%s-01", tid, csid)
			pending[i] = mkSpan(tid, csid, psid, topicID+" create", proxypb.SpanKind_SPAN_KIND_PRODUCER, resAttrs(p.projectID, p.topic, topicID, "messaging.message.body.size", int64(len(it.data)), "messaging.gcp_pubsub.message.ordering_key", it.key, "code.function", "Publish"), []*proxypb.SpanEvent{{Name: "publish start", Attributes: mkAttrs("messaging.batch.message_count", int64(len(batch)))}, {Name: "publish end"}}, []*proxypb.SpanLink{{TraceId: ptid, SpanId: psid2, Attributes: mkAttrs("messaging.operation.name", "publish")}})
			h.addSpan(mkSpan(tid, rHex(8), csid, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, nil, nil, nil), mkSpan(tid, rHex(8), csid, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, nil, nil, nil))
			pubLinks = append(pubLinks, &proxypb.SpanLink{TraceId: tid, SpanId: csid})
		}
		msgs[i] = &pubsubpb.PubsubMessage{Data: []byte(it.data), OrderingKey: it.key, Attributes: attrs}
	}
	resp, err := p.client.Publish(h.outCtx(context.Background(), "topic", p.topic), &pubsubpb.PublishRequest{Topic: p.topic, Messages: msgs})
	if err != nil {
		for _, it := range batch {
			it.resCh <- &proxypb.PublishResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}
		}
		return
	}
	if p.enableMon && h.mode != "bad-traces" {
		h.addSpan(mkSpan(ptid, psid2, "", topicID+" publish", proxypb.SpanKind_SPAN_KIND_INTERNAL, resAttrs(p.projectID, p.topic, topicID, "messaging.batch.message_count", int64(len(batch)), "code.function", "publishMessageBundle"), nil, pubLinks))
	}
	for i, it := range batch {
		msgID := resp.GetMessageIds()[i]
		if pending[i] != nil {
			pending[i].Attributes["messaging.message.id"] = mkAttrs("k", msgID)["k"]
			h.addSpan(pending[i])
		}
		it.resCh <- &proxypb.PublishResponse{MessageId: msgID}
	}
}

func (h *helperProxy) AwaitPublish(ctx context.Context, req *proxypb.AwaitPublishRequest) (*proxypb.PublishResponse, error) {
	p := h.getPub(req.GetPublisherId(), false)
	p.mu.Lock()
	ch := p.tokens[req.GetPublishToken()]
	delete(p.tokens, req.GetPublishToken())
	p.mu.Unlock()
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (h *helperProxy) ClosePublisher(_ context.Context, req *proxypb.ClosePublisherRequest) (*proxypb.ClosePublisherResponse, error) {
	if p := h.getPub(req.GetPublisherId(), true); p != nil {
		p.mu.Lock()
		for k, q := range p.keyBuf {
			if len(q) > 0 {
				h.enqueueKeyBatchLocked(p, k, q)
			}
		}
		p.closed = true
		for _, ch := range p.keyCh {
			close(ch)
		}
		p.mu.Unlock()
		p.wg.Wait()
		_ = p.conn.Close()
	}
	return &proxypb.ClosePublisherResponse{}, nil
}

func (h *helperProxy) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	conn, err := dialHelper(start.GetEndpoint())
	if err != nil {
		return err
	}
	defer conn.Close()
	subClient := pubsubpb.NewSubscriberClient(conn)
	sp, err := subClient.StreamingPull(h.outCtx(stream.Context(), "subscription", start.GetSubscription()))
	if err != nil {
		return err
	}
	initReq := &pubsubpb.StreamingPullRequest{Subscription: start.GetSubscription(), StreamAckDeadlineSeconds: 60, ProtocolVersion: map[bool]int64{true: 1}[h.caps.GetSupportsKeepalive()]}
	if err := sp.Send(initReq); err != nil || stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{}}}) != nil {
		return err
	}
	proj, subID := splitRes(start.GetSubscription())
	ackByTok, spanByTok := map[string]string{}, map[string]*proxypb.TraceSpan{}
	for {
		r, err := sp.Recv()
		if err != nil {
			return nil
		}
		for _, rm := range r.GetReceivedMessages() {
			tok, msg, cbTP := fmt.Sprintf("d-%d", h.seq.Add(1)), rm.GetMessage(), ""
			var subSpan *proxypb.TraceSpan
			if start.GetEnableMonitoring() && h.mode != "bad-traces" {
				wtp, _ := traces.ExtractTraceparent(msg.GetAttributes())
				tid, psid := parseTPOrNew(wtp)
				ssid, procSID := rHex(8), rHex(8)
				subSpan = mkSpan(tid, ssid, psid, subID+" subscribe", proxypb.SpanKind_SPAN_KIND_INTERNAL, resAttrs(proj, start.GetSubscription(), subID, "messaging.message.id", msg.GetMessageId(), "messaging.gcp_pubsub.message.ack_id", rm.GetAckId(), "messaging.message.body.size", int64(len(msg.GetData())), "messaging.gcp_pubsub.exactly_once_delivery", false, "messaging.gcp_pubsub.message.ordering_key", "", "messaging.batch.message_count", int64(1), "code.function", "receive", "messaging.gcp_pubsub.result", "acked"), nil, nil)
				h.addSpan(mkSpan(tid, rHex(8), ssid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, nil, nil, nil), mkSpan(tid, rHex(8), ssid, "subscribe scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, nil, nil, nil), mkSpan(tid, procSID, ssid, subID+" process", proxypb.SpanKind_SPAN_KIND_CONSUMER, resAttrs(proj, start.GetSubscription(), subID, "messaging.message.id", msg.GetMessageId(), "messaging.message.body.size", int64(len(msg.GetData())), "messaging.gcp_pubsub.message.ordering_key", "", "messaging.operation.type", "process"), []*proxypb.SpanEvent{{Name: "ack called"}}, nil))
				cbTP = fmt.Sprintf("00-%s-%s-01", tid, procSID)
			}
			ackByTok[tok], spanByTok[tok] = rm.GetAckId(), subSpan
			if err := stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{DeliveryToken: tok, MessageId: msg.GetMessageId(), Data: msg.GetData(), Attributes: msg.GetAttributes(), OrderingKey: msg.GetOrderingKey(), CallbackTraceparent: cbTP}}}); err != nil {
				return err
			}
			for c, rerr := stream.Recv(); rerr == nil; c, rerr = stream.Recv() {
				switch {
				case c.GetSettle() != nil:
					dt := c.GetSettle().GetDeliveryToken()
					ackID, subSp := ackByTok[dt], spanByTok[dt]
					_, _ = subClient.Acknowledge(h.outCtx(stream.Context(), "subscription", start.GetSubscription()), &pubsubpb.AcknowledgeRequest{Subscription: start.GetSubscription(), AckIds: []string{ackID}})
					if subSp != nil {
						ackSpan := mkSpan(rHex(16), rHex(8), "", subID+" ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, resAttrs(proj, start.GetSubscription(), subID, "messaging.batch.message_count", int64(1), "code.function", "sendAck"), nil, []*proxypb.SpanLink{{TraceId: subSp.TraceId, SpanId: subSp.SpanId}})
						subSp.Links = []*proxypb.SpanLink{{TraceId: ackSpan.TraceId, SpanId: ackSpan.SpanId, Attributes: mkAttrs("messaging.operation.name", "ack")}}
						subSp.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: mkAttrs("messaging.batch.message_count", int64(1))}, {Name: "ack end"}}
						h.addSpan(ackSpan, subSp)
					}
				case c.GetStop() != nil:
					_ = sp.CloseSend()
					return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
				}
			}
			return nil
		}
	}
}

func (h *helperProxy) UnaryPull(ctx context.Context, req *proxypb.UnaryPullRequest) (*proxypb.UnaryPullResponse, error) {
	if h.mode == "partial-unimpl" || !h.caps.GetSupportsUnaryPull() {
		return nil, status.Error(codes.Unimplemented, "unary pull unsupported")
	}
	conn, err := dialHelper(req.GetEndpoint())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	sc := pubsubpb.NewSubscriberClient(conn)
	pr, err := sc.Pull(h.outCtx(ctx, "subscription", req.GetSubscription()), &pubsubpb.PullRequest{Subscription: req.GetSubscription(), MaxMessages: req.GetMaxMessages(), ReturnImmediately: req.GetReturnImmediately()})
	if err != nil {
		return nil, err
	}
	var pulled []*proxypb.UnaryPullResponse_PulledMessage
	for _, rm := range pr.GetReceivedMessages() {
		pulled = append(pulled, &proxypb.UnaryPullResponse_PulledMessage{MessageId: rm.GetMessage().GetMessageId(), Data: rm.GetMessage().GetData(), Attributes: rm.GetMessage().GetAttributes(), OrderingKey: rm.GetMessage().GetOrderingKey(), SettleStatus: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS})
		if req.GetSettleAction() == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK {
			_, _ = sc.Acknowledge(h.outCtx(ctx, "subscription", req.GetSubscription()), &pubsubpb.AcknowledgeRequest{Subscription: req.GetSubscription(), AckIds: []string{rm.GetAckId()}})
		}
	}
	return &proxypb.UnaryPullResponse{Messages: pulled}, nil
}

func runCLICapture(t *testing.T, scens []harness.Scenario, args ...string) (int, string, string) {
	t.Helper()
	var out, errBuf bytes.Buffer
	return runCLIWithScenarios(context.Background(), args, strings.NewReader(""), &out, &errBuf, scens), out.String(), errBuf.String()
}
func mustReadJSON[T any](t *testing.T, path string) T {
	t.Helper()
	var out T
	raw, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(raw, &out) != nil {
		t.Fatalf("read/unmarshal %s: %v", path, err)
	}
	return out
}
func writeDummyReport(t *testing.T, dir, name, target, errMsg string) string {
	t.Helper()
	st, p := map[bool]harness.Status{false: harness.StatusPass, true: harness.StatusFail}[errMsg != ""], filepath.Join(dir, name)
	raw, _ := json.Marshal(map[string]any{"target": target, "scenarios": []map[string]any{{"id": "PUB-01", "status": st, "error": errMsg}}})
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}
func liveHelperArgs(mode string, extra ...string) []string {
	out := []string{"--proxy-cmd=" + os.Args[0], "--proxy-arg=-test.run=^TestHelperProcess$", "--proxy-arg=--", "--proxy-arg=--mode=" + mode}
	for _, a := range extra {
		out = append(out, "--proxy-arg="+a)
	}
	return out
}
func helperTarget(name, mode string, extra ...string) string {
	parts := append([]string{fmt.Sprintf("%q -test.run=^TestHelperProcess$ -- --mode=%s", os.Args[0], mode)}, extra...)
	return "--proxy-target=" + name + "=" + strings.Join(parts, " ")
}

func TestCLI_ValidationAndList(t *testing.T) {
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	c, out, _ := runCLICapture(t, scenarios.All(), "--help")
	if c != 0 {
		t.Fatalf("--help failed: code=%d", c)
	}
	for _, fl := range []string{"--proxy-cmd", "--proxy-arg", "--proxy-target", "--compare", "--filter", "--scenario-timeout", "--suite-timeout", "--report-json", "--report-md", "--strict-unsupported", "--list"} {
		if !strings.Contains(out, fl) {
			t.Errorf("--help missing flag %q", fl)
		}
	}
	dir, bin := t.TempDir(), os.Args[0]
	goJSON, rsJSON := writeDummyReport(t, dir, "go.json", "go", ""), writeDummyReport(t, dir, "rs.json", "rust", "")
	cmpArg, target := goJSON+","+rsJSON, helperTarget("go", "conformant-go")
	same, uncreated, sameDot := filepath.Join(dir, "same.out"), filepath.Join(dir, "never.out"), dir+string(os.PathSeparator)+"."+string(os.PathSeparator)+"same.out"
	livePUB := func(extra ...string) []string {
		return append(liveHelperArgs("conformant-go"), append([]string{"--filter=^PUB-01$"}, extra...)...)
	}
	type vc struct {
		args         []string
		want, absent string
	}
	cases := []vc{
		{nil, "specify --proxy-cmd", ""}, {[]string{"--list", "extra-arg"}, "unknown command", ""}, {[]string{"completion"}, "unknown command", ""}, {[]string{"--unknown-flag"}, "unknown flag", ""},
		{[]string{"--proxy-cmd="}, "--proxy-cmd cannot be empty", ""}, {[]string{"--proxy-cmd=   "}, "--proxy-cmd cannot be empty", ""},
		{[]string{"--proxy-arg=--x"}, "--proxy-arg requires --proxy-cmd", ""}, {[]string{"--list", "--proxy-arg=--x"}, "--proxy-arg requires --proxy-cmd", ""},
		{[]string{"--proxy-arg=--x", target, "--filter=^PUB-01$", "--report-json=" + uncreated}, "--proxy-arg requires --proxy-cmd", uncreated}, {[]string{"--compare=" + cmpArg, "--proxy-arg=--x", "--report-json=" + uncreated}, "--proxy-arg requires --proxy-cmd", uncreated},
		{livePUB("--scenario-timeout=0s", "--report-json="+uncreated), "--scenario-timeout must be > 0", uncreated}, {livePUB("--scenario-timeout=-1s"), "--scenario-timeout must be > 0", ""},
		{livePUB("--suite-timeout=0s", "--report-md="+uncreated), "--suite-timeout must be > 0", uncreated}, {livePUB("--suite-timeout=-1s"), "--suite-timeout must be > 0", ""},
		{append(liveHelperArgs("conformant-go"), "--filter=[bad", "--report-json="+uncreated), "invalid --filter regex", uncreated}, {[]string{"--list", "--filter=[bad"}, "invalid --filter regex", ""},
		{livePUB(target, "--report-json="+uncreated), "cannot combine --proxy-cmd and --proxy-target", uncreated}, {livePUB("--report-json="+same, "--report-md="+same), "cannot resolve to the same file", same}, {[]string{"--compare=" + cmpArg, "--report-json=" + same, "--report-md=" + sameDot}, "cannot resolve to the same file", same},
		{livePUB("--report-json="+dir, "--report-md="+uncreated), "--report-json path", uncreated}, {livePUB("--report-md="+filepath.Join(dir, "missing", "r.md"), "--report-json="+uncreated), "--report-md parent directory", uncreated},
		{[]string{"--proxy-target=missing-equals"}, "name=command", ""}, {[]string{"--proxy-target=dup=" + bin, "--proxy-target=dup=" + bin}, "duplicate target name", ""},
		{[]string{"--proxy-target=bad=\"unterminated"}, "unterminated quote", ""}, {[]string{"--proxy-cmd=\"unterminated"}, "unterminated quote", ""},
	}
	for _, fl := range []string{"--compare=" + cmpArg, "--proxy-cmd=" + bin, target, "--report-json=" + uncreated, "--report-md=" + uncreated, "--strict-unsupported"} {
		cases = append(cases, vc{[]string{"--list", fl}, "--list cannot be combined", uncreated})
	}
	for _, fl := range []string{"--proxy-cmd=" + bin, target, "--filter=PUB", "--filter=", "--strict-unsupported"} {
		cases = append(cases, vc{[]string{"--compare=" + cmpArg, fl, "--report-json=" + uncreated}, "--compare cannot be combined", uncreated})
	}
	for _, p := range [][2]string{{"--report-json", "--report-md"}, {"--report-md", "--report-json"}} {
		for _, s := range [][2]string{{"", "cannot be empty"}, {"   ", "cannot be empty"}, {dir, "path"}, {filepath.Join(dir, "m", "r"), "parent directory"}, {filepath.Join(goJSON, "r"), "parent"}} {
			cases = append(cases, vc{[]string{"--compare=" + cmpArg, p[0] + "=" + s[0], p[1] + "=" + uncreated}, p[0] + " " + s[1], uncreated})
		}
	}
	for _, tc := range cases {
		code, stdout, eb := runCLICapture(t, scenarios.All(), tc.args...)
		if code != 1 || stdout != "" || !strings.Contains(eb, tc.want) || strings.Contains(eb, "Usage:") || strings.Count(eb, "Error:") != 1 {
			t.Errorf("args=%v got code=%d stdout=%q stderr=%q, want code=1 empty stdout & %q", tc.args, code, stdout, eb, tc.want)
		}
		if _, err := os.Stat(tc.absent); tc.absent != "" && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("args=%v created side-effect file %s (err=%v)", tc.args, tc.absent, err)
			_ = os.Remove(tc.absent)
		}
	}
	if info, err := os.Stat(os.DevNull); err == nil && !info.IsDir() {
		if c, out, eb := runCLICapture(t, nil, "--compare="+cmpArg, "--report-json="+os.DevNull, "--report-md="+os.DevNull); c != 0 || out == "" || eb != "" {
			t.Fatalf("devnull exemption failed: code=%d out=%d err=%q", c, len(out), eb)
		}
	}
	c, out, eb := runCLICapture(t, scenarios.All(), "--list")
	if c != 0 || eb != "" {
		t.Fatalf("--list failed: code=%d stderr=%q", c, eb)
	}
	for _, w := range []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03", "requires:unary_pull", "trace-only"} {
		if !strings.Contains(out, w) {
			t.Errorf("--list missing %q", w)
		}
	}
	if c, out, _ = runCLICapture(t, scenarios.All(), "--list", "--filter=^traces$"); c != 0 || !strings.Contains(out, "TRAC-01") || strings.Contains(out, "PUB-01") {
		t.Fatalf("--list --filter unexpected: code=%d out=%q", c, out)
	}
	if c, out, eb = runCLICapture(t, scenarios.All(), "--list", "--filter=^NOMATCH$"); c != 0 || out != "" || !strings.Contains(eb, "No scenarios matched") {
		t.Fatalf("--list empty unexpected: code=%d out=%q err=%q", c, out, eb)
	}
	if c = runCLIWithScenarios(nil, []string{"--list"}, nil, nil, nil, scenarios.All()); c != 0 || runCLIWithScenarios(nil, []string{"--list", "--filter=^NOMATCH$"}, nil, nil, nil, scenarios.All()) != 0 || runCLIWithScenarios(nil, []string{"--list"}, nil, errWriter, nil, scenarios.All()) != 1 {
		t.Fatalf("nil/err streams --list unexpected")
	}
	if c, out, _ = runCLICapture(t, []harness.Scenario{{ID: "X", Category: "cat-x", Name: "name-x", TraceOnly: true, UseEmulatorEnv: true, RequiredCapabilities: []harness.Capability{harness.CapUnaryPull, harness.CapKeepalive}}}, "--list"); c != 0 || !strings.Contains(out, "cat-x") || !strings.Contains(out, "name-x") || !strings.Contains(out, "[trace-only; emulator-env; requires:unary_pull,keepalive]") {
		t.Fatalf("custom --list failed: code=%d out=%q", c, out)
	}
}

func TestCLI_CompareMode(t *testing.T) {
	dir := t.TempDir()
	goJSON, go2JSON, rsJSON := writeDummyReport(t, dir, "go.json", "go", ""), writeDummyReport(t, dir, "go2.json", "go", ""), writeDummyReport(t, dir, "rust.json", "rust", "broke")
	dupJSON, badJSON := filepath.Join(dir, "dup.json"), filepath.Join(dir, "bad.json")
	_ = os.WriteFile(dupJSON, []byte(`{"targets":[{"target":"go"},{"target":"go"}]}`), 0o600)
	_ = os.WriteFile(badJSON, []byte(`{not-json`), 0o600)

	outJSON, outMD := filepath.Join(dir, "cmp.json"), filepath.Join(dir, "cmp.md")
	c, out, eb := runCLICapture(t, nil, "--compare="+goJSON+","+rsJSON, "--report-json="+outJSON, "--report-md="+outMD)
	if c != 0 || !strings.Contains(out, "go") || !strings.Contains(out, "rust") {
		t.Fatalf("comma compare failed: code=%d err=%q", c, eb)
	}
	if dec := mustReadJSON[harness.Report](t, outJSON); dec.Mode != "compare" || len(dec.Targets) != 2 || len(dec.Differences) == 0 {
		t.Fatalf("bad compare JSON: %+v", dec)
	}
	if md, err := os.ReadFile(outMD); err != nil || len(md) == 0 {
		t.Fatalf("missing markdown: %v", err)
	}
	if c, out, eb = runCLICapture(t, nil, "--compare="+goJSON, "--compare="+go2JSON); c != 0 || out == "" || runCLIWithScenarios(nil, []string{"--compare=" + goJSON + "," + go2JSON}, nil, errWriter, nil, nil) != 1 {
		t.Fatalf("repeatable colliding compare failed: code=%d err=%q", c, eb)
	}
	for _, bad := range [][]string{{"--compare=" + dupJSON}, {"--compare=" + goJSON}, {"--compare=" + goJSON + ","}, {"--compare=" + goJSON, "--compare=" + rsJSON, "--compare="}, {"--compare=" + goJSON + "," + badJSON}, {"--compare=" + goJSON + "," + filepath.Join(dir, "missing.json")}} {
		if c, out, eb = runCLICapture(t, nil, bad...); c != 1 || out != "" || strings.Count(eb, "Error:") != 1 {
			t.Errorf("compare %v expected 1 with empty stdout, got %d out=%q err=%q", bad, c, out, eb)
		}
	}

	// BUG-02 Cases A, B, C, and colliding "default" stems via CLI --compare.
	writeSub := func(rel, content string) string {
		p := filepath.Join(dir, rel)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// Case A: colliding multi-target report.json keeps go, cpp, go-2, cpp-2.
	rA1 := writeSub("ca1/report.json", `{"mode":"compare","targets":[{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]},{"target":"cpp","scenarios":[{"id":"PUB-01","status":"FAIL","error":"cpp err"}]}]}`)
	rA2 := writeSub("ca2/report.json", `{"mode":"compare","targets":[{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]},{"target":"cpp","scenarios":[{"id":"PUB-01","status":"PASS"}]}]}`)
	outA := filepath.Join(dir, "outA.json")
	if c, _, eb = runCLICapture(t, nil, "--compare="+rA1+","+rA2, "--report-json="+outA); c != 0 {
		t.Fatalf("Case A --compare failed: %d stderr=%s", c, eb)
	}
	repA := mustReadJSON[harness.Report](t, outA)
	if got := []string{repA.Targets[0].Target, repA.Targets[1].Target, repA.Targets[2].Target, repA.Targets[3].Target}; !slices.Equal(got, []string{"go", "cpp", "go-2", "cpp-2"}) {
		t.Fatalf("Case A CLI targets = %v, want [go cpp go-2 cpp-2]", got)
	}
	// Case B: colliding single-target report.json with target "go" keeps go, go-2.
	rB1 := writeSub("cb1/report.json", `{"target":"go","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	rB2 := writeSub("cb2/report.json", `{"target":"go","scenarios":[{"id":"PUB-01","status":"FAIL","error":"b2 err"}]}`)
	outB := filepath.Join(dir, "outB.json")
	if c, _, eb = runCLICapture(t, nil, "--compare="+rB1+","+rB2, "--report-json="+outB); c != 0 {
		t.Fatalf("Case B --compare failed: %d stderr=%s", c, eb)
	}
	repB := mustReadJSON[harness.Report](t, outB)
	if got := []string{repB.Targets[0].Target, repB.Targets[1].Target}; !slices.Equal(got, []string{"go", "go-2"}) {
		t.Fatalf("Case B CLI targets = %v, want [go go-2]", got)
	}
	// Case C: generic "default" stems do not displace explicit "alpha".
	rC1 := writeSub("cc1/alpha.json", `{"target":"default","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	rC2 := writeSub("cc2/beta.json", `{"target":"default","scenarios":[{"id":"PUB-01","status":"PASS"}]}`)
	rC3 := writeSub("cc3/gamma.json", `{"target":"alpha","scenarios":[{"id":"PUB-01","status":"FAIL","error":"c3 err"}]}`)
	outC := filepath.Join(dir, "outC.json")
	if c, _, eb = runCLICapture(t, nil, "--compare="+rC1+","+rC2+","+rC3, "--report-json="+outC); c != 0 {
		t.Fatalf("Case C --compare failed: %d stderr=%s", c, eb)
	}
	repC := mustReadJSON[harness.Report](t, outC)
	if got := []string{repC.Targets[0].Target, repC.Targets[1].Target, repC.Targets[2].Target}; !slices.Equal(got, []string{"alpha-2", "beta", "alpha"}) {
		t.Fatalf("Case C CLI targets = %v, want [alpha-2 beta alpha]", got)
	}
}

func TestCLI_LiveSingleAndMultiTarget(t *testing.T) {
	t.Setenv("GO_WANT_HELPER_PROCESS", "1")
	dir := t.TempDir()
	repJSON, repMD := filepath.Join(dir, "go.json"), filepath.Join(dir, "go.md")
	const smokeFilter = "--filter=^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-01|TRAC-03)$"
	var outBuf, errBuf bytes.Buffer
	if c := runCLI(context.Background(), append(liveHelperArgs("conformant-go"), smokeFilter, "--report-json="+repJSON, "--report-md="+repMD, "--scenario-timeout=10s"), strings.NewReader(""), &outBuf, &errBuf); c != 0 || !strings.Contains(errBuf.String(), "[default] PUB-01: PASS") {
		t.Fatalf("runCLI conformant-go failed: code=%d stderr=%s", c, errBuf.String())
	}
	if got := mustReadJSON[harness.Report](t, repJSON); got.Mode != "single" || got.Summary == nil || got.Summary.Pass != 6 {
		t.Fatalf("bad single report: %+v", got)
	}
	if md, err := os.ReadFile(repMD); err != nil || !strings.Contains(string(md), "PUB-01") || string(md) != outBuf.String() {
		t.Fatalf("markdown file/stdout mismatch: err=%v", err)
	}

	soloArgs := filepath.Join(dir, "solo-args.json")
	if c, _, eb := runCLICapture(t, scenarios.All(), helperTarget("solo", "conformant-go", "--args-file="+soloArgs, "--extra=x,y"), "--filter=^PUB-01$"); c != 0 {
		t.Fatalf("single --proxy-target failed: code=%d stderr=%s", c, eb)
	}
	if got := mustReadJSON[[]string](t, soloArgs); !slices.Contains(got, "--extra=x,y") {
		t.Fatalf("--proxy-target comma split: %v", got)
	}
	multiJSON := filepath.Join(dir, "multi.json")
	if c, out, eb := runCLICapture(t, scenarios.All(), helperTarget("go", "conformant-go"), helperTarget("rust", "no-mon"), smokeFilter, "--report-json="+multiJSON, "--scenario-timeout=10s"); c != 0 || !strings.Contains(out, "TRAC-01") {
		t.Fatalf("multi-target failed: code=%d stderr=%s", c, eb)
	}
	if multi := mustReadJSON[harness.Report](t, multiJSON); multi.Mode != "compare" || len(multi.Targets) != 2 || len(multi.Differences) == 0 {
		t.Fatalf("bad multi-target report: %+v", multi)
	}

	argsFile := filepath.Join(dir, "args.json")
	for _, tc := range []struct {
		want    int
		wantErr string
		args    []string
	}{
		{0, "", append(liveHelperArgs("partial-unimpl"), "--filter=^PULL-04$")},
		{1, "", append(liveHelperArgs("partial-unimpl"), "--filter=^PULL-04$", "--strict-unsupported")},
		{0, "", append(liveHelperArgs("conformant-go", "--args-file="+argsFile, "--extra=a,b"), "--filter=^PUB-01$")},
		{1, "", append(liveHelperArgs("conformant-go"), "--filter=^NOMATCH$")},
		{1, "", append(liveHelperArgs("bad-rpc"), "--filter=^PUB-01$", "--scenario-timeout=2s")},
		{1, "", append(liveHelperArgs("bad-traces"), "--filter=^TRAC-01$", "--scenario-timeout=2s")},
		{1, "Target default error:", append(liveHelperArgs("crash-startup"), "--filter=^PUB-01$", "--scenario-timeout=2s")},
		{1, "", append(liveHelperArgs("bad-ready"), "--filter=^PUB-01$", "--scenario-timeout=2s")},
		{1, "", append(liveHelperArgs("crash-mid"), "--filter=^PUB-01$", "--scenario-timeout=2s")},
	} {
		if c, _, eb := runCLICapture(t, scenarios.All(), tc.args...); c != tc.want || (tc.wantErr != "" && !strings.Contains(eb, tc.wantErr)) {
			t.Errorf("args=%v got %d (want %d, err %q) stderr=%s", tc.args, c, tc.want, tc.wantErr, eb)
		}
	}
	if gotArgs, wantArgs := mustReadJSON[[]string](t, argsFile), []string{"--mode=conformant-go", "--args-file=" + argsFile, "--extra=a,b", "--port=0"}; !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("proxy args / --port=0 mismatch: %v", gotArgs)
	}
	if os.Geteuid() != 0 {
		roDir := filepath.Join(dir, "ro")
		_ = os.MkdirAll(roDir, 0o555)
		t.Cleanup(func() { _ = os.Chmod(roDir, 0o755) })
		if c, out, _ := runCLICapture(t, scenarios.All(), append(liveHelperArgs("conformant-go"), "--filter=^PUB-01$", "--report-json="+filepath.Join(roDir, "x.json"))...); c != 1 || !strings.Contains(out, "PUB-01") {
			t.Fatalf("read-only --report-json expected 1 w/ stdout MD, got %d", c)
		}
		if c, out, _ := runCLICapture(t, nil, "--compare="+repJSON+","+multiJSON, "--report-md="+filepath.Join(roDir, "x.md")); c != 1 || !strings.Contains(out, "PUB-01") {
			t.Fatalf("read-only --report-md compare expected 1 w/ stdout MD, got %d", c)
		}
	}

	hangJSON, pidFile, scStart := filepath.Join(dir, "hang.json"), filepath.Join(dir, "hang.pid"), time.Now()
	c, hangMD, _ := runCLICapture(t, scenarios.All(), append(liveHelperArgs("hang", "--pid-file="+pidFile), "--filter=^PUB-01$", "--scenario-timeout=350ms", "--report-json="+hangJSON)...)
	if rawHang, _ := os.ReadFile(hangJSON); c != 1 || time.Since(scStart) > 10*time.Second || !strings.Contains(hangMD, "FAIL (TIMEOUT)") || !bytes.Contains(rawHang, []byte(`"status": "FAIL"`)) || !bytes.Contains(rawHang, []byte(`"status_detail": "TIMEOUT"`)) {
		t.Fatalf("hang mismatch (elapsed=%v): code=%d md=%q json=%s", time.Since(scStart), c, hangMD, rawHang)
	}
	if hangRep := mustReadJSON[harness.Report](t, hangJSON); len(hangRep.Scenarios) != 1 || hangRep.Scenarios[0].Status != harness.StatusTimeout || !strings.Contains(hangRep.Scenarios[0].Error, "350ms") {
		t.Fatalf("hang expected StatusTimeout(350ms): %+v", hangRep)
	}
	assertPgidDead(t, pidFile)

	pidSuite, start := filepath.Join(dir, "suite.pid"), time.Now()
	if c, _, _ := runCLICapture(t, scenarios.All(), append(liveHelperArgs("hang", "--pid-file="+pidSuite), "--filter=^PUB-01$", "--scenario-timeout=10s", "--suite-timeout=350ms")...); c != 1 || time.Since(start) > 5*time.Second {
		t.Fatalf("--suite-timeout code=%d elapsed=%v", c, time.Since(start))
	}
	assertPgidDead(t, pidSuite)

	pidIgn := filepath.Join(dir, "ign.pid")
	if c, _, eb := runCLICapture(t, scenarios.All(), append(liveHelperArgs("ignore-shutdown", "--pid-file="+pidIgn), "--filter=^PUB-01$")...); c != 0 {
		t.Fatalf("ignore-shutdown failed: %d stderr=%s", c, eb)
	}
	assertPgidDead(t, pidIgn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if c := runCLIWithScenarios(ctx, liveHelperArgs("conformant-go"), nil, writerFunc(func(p []byte) (int, error) { cancel(); return len(p), nil }), nil, []harness.Scenario{{ID: "OK", Name: "ok", Category: "test"}}); c != 1 {
		t.Fatalf("post-run canceled ctx expected exit 1, got %d", c)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

var errWriter = writerFunc(func([]byte) (int, error) { return 0, errors.New("write error") })

func assertPgidDead(t *testing.T, pidFile string) {
	t.Helper()
	if b, err := os.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
			for range 40 {
				if errors.Is(syscall.Kill(-pid, 0), syscall.ESRCH) {
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			t.Fatalf("proxy pgid %d survived teardown", pid)
		}
	}
}

func TestCLI_MainSubprocessAndSIGTERM(t *testing.T) {
	mainCmd := func(args ...string) *exec.Cmd {
		b, _ := json.Marshal(args)
		c := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
		c.Env = append(os.Environ(), "GO_WANT_CLI_MAIN=1", "CLI_MAIN_ARGS="+string(b))
		return c
	}
	if out, err := mainCmd("--list").CombinedOutput(); err != nil || !strings.Contains(string(out), "PUB-01") {
		t.Fatalf("main --list failed: err=%v out=%s", err, string(out))
	}
	if out, err := mainCmd().CombinedOutput(); err == nil || !strings.Contains(string(out), "Error:") {
		t.Fatalf("main no-args expected err+stderr, got err=%v out=%s", err, string(out))
	}
	dir := t.TempDir()
	for _, sig := range []os.Signal{syscall.SIGTERM, os.Interrupt} {
		pidFile, startedFile := filepath.Join(dir, sig.String()+".pid"), filepath.Join(dir, sig.String()+".started")
		sigCmd := mainCmd(append(liveHelperArgs("hang", "--pid-file="+pidFile, "--started-file="+startedFile), "--filter=^PUB-01$", "--scenario-timeout=15s")...)
		if err := sigCmd.Start(); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			if _, err := os.Stat(startedFile); err == nil {
				break
			}
		}
		if _, err := os.Stat(startedFile); err != nil {
			_ = sigCmd.Process.Kill()
			t.Fatalf("proxy never started: %v", err)
		}
		_ = sigCmd.Process.Signal(sig)
		if waitErr := sigCmd.Wait(); waitErr == nil {
			t.Fatalf("expected non-zero exit after %v", sig)
		}
		assertPgidDead(t, pidFile)
	}
}
