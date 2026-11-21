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

package selftest_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type proxyConfig struct {
	port, profile, monitoring, pidLog, envLog, argsLog, rpcCountLog, startedFile, onceMarker, tag string
	faults                                                                                        map[string]bool
	spawnDescendant, ignoreSIGTERM, enforceEmulatorEnv                                            bool
}

type asyncPubItem struct {
	data, key, parentTP string
	attrs               map[string]string
	enableMon           bool
	resCh               chan *proxypb.PublishResponse
}

type mockPub struct {
	topic, projectID          string
	enableMon, enableOrdering bool
	conn                      *grpc.ClientConn
	client                    pubsubpb.PublisherClient
	mu                        sync.Mutex
	closed                    bool
	keyBuf                    map[string][]*asyncPubItem
	keyCh                     map[string]chan []*asyncPubItem
	tokens                    map[string]chan *proxypb.PublishResponse
	wg                        sync.WaitGroup
}

type mockProxyServer struct {
	proxypb.UnimplementedPubSubProxyServer
	cfg      proxyConfig
	caps     *proxypb.GetCapabilitiesResponse
	cppMode  bool
	store    *traceStore
	grpcSrv  *grpc.Server
	descCmd  *exec.Cmd
	descDone <-chan struct{}
	stopCh   chan struct{}
	once     sync.Once
	mu       sync.Mutex
	pubs     map[string]*mockPub
	seq      atomic.Int64
	traceRC  atomic.Int64
}

func parseProxyConfig(args []string) proxyConfig {
	cfg := proxyConfig{port: "0", profile: "go", monitoring: "auto", tag: "proxy", faults: map[string]bool{}}
	var faultCSV string
	parseKVFlags(args, map[string]*string{
		"--port": &cfg.port, "-port": &cfg.port, "--profile": &cfg.profile, "--monitoring": &cfg.monitoring,
		"--fault": &faultCSV, "--pid-log": &cfg.pidLog, "--env-log": &cfg.envLog, "--args-log": &cfg.argsLog,
		"--rpc-count-log": &cfg.rpcCountLog, "--started-file": &cfg.startedFile, "--once-marker": &cfg.onceMarker, "--tag": &cfg.tag,
	}, map[string]*bool{
		"--spawn-descendant": &cfg.spawnDescendant, "--ignore-sigterm": &cfg.ignoreSIGTERM, "--enforce-emulator-env": &cfg.enforceEmulatorEnv,
	})
	for _, f := range strings.Split(faultCSV, ",") {
		if f = strings.TrimSpace(f); f != "" {
			cfg.faults[f] = true
		}
	}
	if cfg.faults["ignore-sigterm"] {
		cfg.ignoreSIGTERM = true
	}
	return cfg
}

func runProxyHelper(args []string) int {
	cfg := parseProxyConfig(args)
	if cfg.argsLog != "" {
		b, _ := json.Marshal(args)
		_ = os.WriteFile(cfg.argsLog, b, 0o600)
	}
	if cfg.ignoreSIGTERM {
		signal.Ignore(syscall.SIGTERM)
	}

	var descCmd *exec.Cmd
	var descDone <-chan struct{}
	if cfg.spawnDescendant {
		var err error
		if descCmd, descDone, err = spawnSyncedDescendant(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "spawn descendant: %v\n", err)
			return 2
		}
	}

	pid := os.Getpid()
	pgid, _ := syscall.Getpgid(0)
	if err := appendPIDLog(cfg.pidLog, "proxy", pid, pgid, cfg.tag); err != nil {
		if descCmd != nil && descCmd.Process != nil {
			_ = descCmd.Process.Kill()
			if descDone != nil {
				<-descDone
			}
		}
		fmt.Fprintf(os.Stderr, "append proxy pid log: %v\n", err)
		return 2
	}
	_ = appendLine(cfg.envLog, "pid=%d pgid=%d tag=%s emu=%q", pid, pgid, cfg.tag, os.Getenv("PUBSUB_EMULATOR_HOST"))

	switch {
	case cfg.faults["crash-startup"]:
		fmt.Fprintln(os.Stderr, "selftest fatal startup crash")
		return 2
	case cfg.faults["silent-exit"]:
		return 0
	case cfg.faults["bad-ready"]:
		fmt.Println("READY not-a-host-port")
		_ = os.Stdout.Sync()
		return 0
	case cfg.faults["bad-ready-hang"]:
		fmt.Println("READY not-a-host-port")
		_ = os.Stdout.Sync()
		select {}
	case cfg.faults["hang-before-ready"]:
		select {}
	}

	lis, err := net.Listen("tcp", "127.0.0.1:"+cfg.port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen port %s: %v\n", cfg.port, err)
		return 3
	}
	caps := buildCapabilities(cfg)
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(32<<20), grpc.MaxSendMsgSize(32<<20))
	ms := &mockProxyServer{
		cfg: cfg, caps: caps, cppMode: strings.EqualFold(caps.GetClientLanguage(), "cpp"),
		store: &traceStore{}, grpcSrv: srv, descCmd: descCmd, descDone: descDone, stopCh: make(chan struct{}), pubs: map[string]*mockPub{},
	}
	proxypb.RegisterPubSubProxyServer(srv, ms)
	go func() { _ = srv.Serve(lis) }()

	fmt.Printf("READY %s\n", lis.Addr().String())
	_ = os.Stdout.Sync()

	if cfg.faults["crash-after-ready"] {
		_ = lis.Close()
		fmt.Fprintln(os.Stderr, "selftest crashed after ready")
		return 3
	}
	<-ms.stopCh
	_ = appendLine(cfg.rpcCountLog, "trace_rpcs=%d", ms.traceRC.Load())
	return 0
}

func spawnSyncedDescendant(cfg proxyConfig) (*exec.Cmd, <-chan struct{}, error) {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	dArgs := []string{"--helper-role=descendant", "--tag=" + cfg.tag + "-desc"}
	if cfg.pidLog != "" {
		dArgs = append(dArgs, "--pid-log="+cfg.pidLog)
	}
	if cfg.ignoreSIGTERM {
		dArgs = append(dArgs, "--ignore-sigterm")
	}
	cmd := exec.Command(exe, dArgs...)
	cmd.Env = envWith(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	br := bufio.NewReader(pipe)
	line, err := br.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "DESCENDANT_READY ") {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, nil, fmt.Errorf("expected DESCENDANT_READY, got %q (err=%v)", line, err)
	}
	descDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, br)
		_ = cmd.Wait()
		close(descDone)
	}()
	return cmd, descDone, nil
}

func buildCapabilities(cfg proxyConfig) *proxypb.GetCapabilitiesResponse {
	var caps *proxypb.GetCapabilitiesResponse
	switch cfg.profile {
	case "rust":
		caps = &proxypb.GetCapabilitiesResponse{ClientLanguage: "rust", ClientLibrary: "google-cloud-pubsub", ClientVersion: "1.0.0", Defaults: &proxypb.ClientDefaults{PublishBatchCountThreshold: 1000, PublishFlowControlBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE}}
	case "cpp":
		caps = &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", ClientLibrary: "google-cloud-cpp/pubsub", ClientVersion: "2.0.0", SupportsMonitoring: true, SupportsUnaryPull: true, SupportsOrderingOptIn: true, Defaults: &proxypb.ClientDefaults{PublishBatchByteThreshold: 1048576, PublishFlowControlBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK}}
	case "partial":
		caps = &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", ClientLibrary: "cloud.google.com/go/pubsub/v2", ClientVersion: "2.0.0", SupportsMonitoring: true, SupportsOrderingOptIn: true, SupportsKeepalive: true, SupportsShutdownBehavior: true, SupportsShutdownTimeout: true, Defaults: &proxypb.ClientDefaults{PublishFlowControlBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE}}
	default:
		caps = &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", ClientLibrary: "cloud.google.com/go/pubsub/v2", ClientVersion: "2.0.0", SupportsMonitoring: true, SupportsUnaryPull: true, SupportsOrderingOptIn: true, SupportsKeepalive: true, SupportsShutdownBehavior: true, SupportsShutdownTimeout: true, Defaults: &proxypb.ClientDefaults{PublishBatchCountThreshold: 100, PublishBatchByteThreshold: 1000000, PublishFlowControlBehavior: proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_IGNORE}}
	}
	if cfg.monitoring == "false" {
		caps.SupportsMonitoring = false
	}
	return caps
}

func (m *mockProxyServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	if m.cfg.faults["hang-caps"] {
		<-m.stopCh
		return nil, status.Error(codes.Canceled, "stopped")
	}
	return m.caps, nil
}

func (m *mockProxyServer) ResetTraces(context.Context, *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	m.traceRC.Add(1)
	_ = appendLine(m.cfg.rpcCountLog, "trace_rpc:ResetTraces")
	m.store.reset()
	return &proxypb.ResetTracesResponse{}, nil
}

func (m *mockProxyServer) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	m.traceRC.Add(1)
	_ = appendLine(m.cfg.rpcCountLog, "trace_rpc:DrainTraces")
	spans := m.store.drain()
	if m.cfg.faults["missing-traces"] || m.cfg.faults["missing-spans"] {
		spans = nil
	}
	return &proxypb.DrainTracesResponse{Spans: spans}, nil
}

func (m *mockProxyServer) ShutdownProxy(ctx context.Context, _ *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	if m.cfg.faults["ignore-shutdown"] {
		return &proxypb.ShutdownProxyResponse{}, nil
	}
	m.once.Do(func() {
		if m.descCmd != nil && m.descCmd.Process != nil {
			_ = m.descCmd.Process.Kill()
			if m.descDone != nil {
				<-m.descDone
			}
		}
		go func() {
			time.Sleep(10 * time.Millisecond)
			m.grpcSrv.Stop()
			close(m.stopCh)
		}()
	})
	return &proxypb.ShutdownProxyResponse{}, nil
}

func (m *mockProxyServer) outCtx(ctx context.Context, key, resource string) context.Context {
	if m.cfg.faults["missing-routing-header"] {
		return ctx
	}
	if !strings.EqualFold(m.caps.GetClientLanguage(), "rust") {
		resource = url.QueryEscape(resource)
	}
	return metadata.AppendToOutgoingContext(ctx, "x-goog-request-params", key+"="+resource)
}

func dialFakeAddr(ep string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///"+ep, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (m *mockProxyServer) markStarted() {
	if m.cfg.startedFile != "" {
		_ = os.WriteFile(m.cfg.startedFile, []byte("started"), 0o600)
	}
}

func (m *mockProxyServer) maybeCrashMidScenario(where string) {
	if m.cfg.faults["crash-mid-scenario"] {
		fmt.Fprintf(os.Stderr, "selftest crash-mid-scenario at %s\n", where)
		os.Exit(3)
	}
	if m.cfg.faults["crash-once"] && m.cfg.onceMarker != "" {
		f, err := os.OpenFile(m.cfg.onceMarker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_ = f.Close()
			fmt.Fprintf(os.Stderr, "selftest crash-once at %s\n", where)
			os.Exit(3)
		}
	}
}

func (m *mockProxyServer) issueUnexpectedOutOfBandRPCs(ctx context.Context, conn *grpc.ClientConn, proj, sub string) {
	if m.cfg.faults["unexpected-schema-rpc"] {
		_ = conn.Invoke(ctx, "/google.pubsub.v1.SchemaService/GetSchema", &emptypb.Empty{}, &emptypb.Empty{})
	}
	if m.cfg.faults["unexpected-admin-rpc"] {
		_ = conn.Invoke(ctx, "/google.pubsub.v1.Publisher/CreateTopic", &pubsubpb.Topic{Name: "projects/" + proj + "/topics/unregistered-admin"}, &pubsubpb.Topic{})
	}
	if m.cfg.faults["extra-pull-rpc"] {
		_, _ = pubsubpb.NewSubscriberClient(conn).Pull(m.outCtx(ctx, "subscription", "projects/"+proj+"/subscriptions/extra"), &pubsubpb.PullRequest{Subscription: "projects/" + proj + "/subscriptions/extra", MaxMessages: 1, ReturnImmediately: true})
	}
	if m.cfg.faults["extra-modack-rpc"] {
		_, _ = pubsubpb.NewSubscriberClient(conn).ModifyAckDeadline(m.outCtx(ctx, "subscription", sub), &pubsubpb.ModifyAckDeadlineRequest{Subscription: sub, AckIds: []string{"extra-ack-id"}, AckDeadlineSeconds: 30})
	}
}

func (m *mockProxyServer) getPub(id string, del bool) *mockPub {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pubs[id]
	if del {
		delete(m.pubs, id)
	}
	return p
}

func (m *mockProxyServer) CreatePublisher(ctx context.Context, req *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	m.markStarted()
	if m.cfg.enforceEmulatorEnv {
		gotEmu := os.Getenv("PUBSUB_EMULATOR_HOST")
		if req.GetUseEmulatorEnv() && gotEmu != req.GetEndpoint() {
			return nil, status.Errorf(codes.FailedPrecondition, "PUBSUB_EMULATOR_HOST=%q want %q", gotEmu, req.GetEndpoint())
		}
		if !req.GetUseEmulatorEnv() && gotEmu != "" {
			return nil, status.Errorf(codes.FailedPrecondition, "unexpected leaked PUBSUB_EMULATOR_HOST=%q", gotEmu)
		}
	}
	conn, err := dialFakeAddr(req.GetEndpoint())
	if err != nil {
		return nil, err
	}
	proj, _ := splitRes(req.GetTopic())
	id := fmt.Sprintf("pub-%d", m.seq.Add(1))
	m.mu.Lock()
	m.pubs[id] = &mockPub{
		topic: req.GetTopic(), projectID: proj, enableMon: req.GetEnableMonitoring(), enableOrdering: req.GetEnableMessageOrdering(),
		conn: conn, client: pubsubpb.NewPublisherClient(conn),
		keyBuf: map[string][]*asyncPubItem{}, keyCh: map[string]chan []*asyncPubItem{}, tokens: map[string]chan *proxypb.PublishResponse{},
	}
	m.mu.Unlock()
	return &proxypb.CreatePublisherResponse{PublisherId: id}, nil
}

func (m *mockProxyServer) Publish(ctx context.Context, req *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	p := m.getPub(req.GetPublisherId(), false)
	if p == nil {
		return nil, status.Error(codes.NotFound, "unknown publisher")
	}
	m.markStarted()
	if m.cfg.faults["hang-publish"] {
		<-m.stopCh
		return nil, status.Error(codes.Canceled, "stopped")
	}
	m.maybeCrashMidScenario("Publish")

	attrs, data, key := maps.Clone(req.GetAttributes()), slices.Clone(req.GetData()), req.GetOrderingKey()
	if attrs == nil {
		attrs = map[string]string{}
	}
	switch {
	case m.cfg.faults["wrong-payload"]:
		data = append(data, []byte("-corrupt")...)
	case m.cfg.faults["wrong-attrs"]:
		delete(attrs, "k1")
		attrs["unexpected"] = "1"
	case m.cfg.faults["unexpected-ordering-key"]:
		key = "unexpected-key"
	case m.cfg.faults["wrong-ordering-key"]:
		key += "-wrong"
	}
	if m.cfg.faults["wrong-ordering-key-b2"] && strings.HasPrefix(string(data), "k1-b2-") {
		key = "corrupt-key"
	}

	it := &asyncPubItem{data: string(data), key: key, attrs: attrs, parentTP: req.GetParentTraceparent(), enableMon: p.enableMon, resCh: make(chan *proxypb.PublishResponse, 1)}
	if req.GetAsyncFire() {
		tok := fmt.Sprintf("tok-%d", m.seq.Add(1))
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, status.Error(codes.FailedPrecondition, "publisher closed")
		}
		p.tokens[tok] = it.resCh
		p.keyBuf[it.key] = append(p.keyBuf[it.key], it)
		if q := p.keyBuf[it.key]; len(q) >= 2 || (it.key == "" && !m.cfg.faults["out-of-order-publish"]) {
			p.keyBuf[it.key] = nil
			m.enqueueKeyBatchLocked(p, it.key, q)
		}
		p.mu.Unlock()
		return &proxypb.PublishResponse{PublishToken: tok}, nil
	}

	m.sendBatch(p, []*asyncPubItem{it})
	resp := <-it.resCh
	if req.GetVerifyAttributesImmutability() && m.cfg.faults["caller-attrs-mutated"] {
		resp.CallerAttributesMutated = true
	}
	return resp, nil
}

func (m *mockProxyServer) enqueueKeyBatchLocked(p *mockPub, key string, batch []*asyncPubItem) {
	if p.closed {
		return
	}
	if m.cfg.faults["concurrent-same-key"] && key == "k1" {
		p.wg.Add(1)
		go func() { defer p.wg.Done(); m.sendBatch(p, batch) }()
		return
	}
	ch := p.keyCh[key]
	if ch == nil {
		ch = make(chan []*asyncPubItem, 64)
		p.keyCh[key] = ch
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for b := range ch {
				m.sendBatch(p, b)
			}
		}()
	}
	ch <- batch
}

func (m *mockProxyServer) sendBatch(p *mockPub, batch []*asyncPubItem) {
	if len(batch) > 1 && m.cfg.faults["out-of-order-publish"] && batch[0].key == "k1" {
		slices.Reverse(batch)
	}
	if m.cfg.faults["missing-rpc"] {
		for _, it := range batch {
			it.resCh <- &proxypb.PublishResponse{MessageId: fmt.Sprintf("local-skip-%d", m.seq.Add(1))}
		}
		return
	}
	proj, _ := splitRes(p.topic)
	m.issueUnexpectedOutOfBandRPCs(context.Background(), p.conn, proj, "projects/"+proj+"/subscriptions/extra-sub")

	if m.cfg.faults["emit-span-when-disabled"] && !p.enableMon {
		emitUnexpectedDisabledSpan(m.store, p.topic)
	}

	spanSet := buildPublishSpans(m.cppMode, m.cfg.faults, p.topic, batch, p.enableMon)
	pbMsgs := make([]*pubsubpb.PubsubMessage, len(batch))
	for i, it := range batch {
		attrs := maps.Clone(it.attrs)
		if spanSet.items[i].wireTP != "" {
			if attrs == nil {
				attrs = map[string]string{}
			}
			attrs[fake.TraceparentAttributeKey] = spanSet.items[i].wireTP
		}
		pbMsgs[i] = &pubsubpb.PubsubMessage{Data: []byte(it.data), OrderingKey: it.key, Attributes: attrs}
	}
	topic := p.topic
	if m.cfg.faults["wrong-topic"] {
		topic += "-wrong"
	}
	resp, err := p.client.Publish(m.outCtx(context.Background(), "topic", p.topic), &pubsubpb.PublishRequest{Topic: topic, Messages: pbMsgs})
	if err != nil {
		for _, it := range batch {
			it.resCh <- &proxypb.PublishResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}
		}
		return
	}
	msgIDs := resp.GetMessageIds()
	emitPublishSpans(m.store, spanSet, m.cfg.faults, msgIDs)
	for i, it := range batch {
		id := msgIDs[i]
		if m.cfg.faults["mismatched-message-id"] {
			id = "wrong-" + id
		}
		it.resCh <- &proxypb.PublishResponse{MessageId: id}
	}
}

func (m *mockProxyServer) AwaitPublish(ctx context.Context, req *proxypb.AwaitPublishRequest) (*proxypb.PublishResponse, error) {
	p := m.getPub(req.GetPublisherId(), false)
	if p == nil {
		return nil, status.Error(codes.NotFound, "unknown publisher")
	}
	p.mu.Lock()
	ch := p.tokens[req.GetPublishToken()]
	delete(p.tokens, req.GetPublishToken())
	p.mu.Unlock()
	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *mockProxyServer) ClosePublisher(_ context.Context, req *proxypb.ClosePublisherRequest) (*proxypb.ClosePublisherResponse, error) {
	if p := m.getPub(req.GetPublisherId(), true); p != nil {
		p.mu.Lock()
		for k, rem := range p.keyBuf {
			if len(rem) > 0 {
				p.keyBuf[k] = nil
				m.enqueueKeyBatchLocked(p, k, rem)
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

func (m *mockProxyServer) SubscriberSession(stream proxypb.PubSubProxy_SubscriberSessionServer) error {
	m.markStarted()
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if m.cfg.faults["hang-subscribe"] {
		<-m.stopCh
		return status.Error(codes.Canceled, "stopped")
	}
	m.maybeCrashMidScenario("SubscriberSession")

	start := first.GetStart()
	cid := fmt.Sprintf("cid-%d", m.seq.Add(1))
	if err := stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Started{Started: &proxypb.StartedEvent{ClientId: cid}}}); err != nil {
		return err
	}
	if m.cfg.faults["missing-rpc"] || m.cfg.faults["missing-streaming-pull"] {
		for cmd, rerr := stream.Recv(); rerr == nil; cmd, rerr = stream.Recv() {
			if cmd.GetStop() != nil {
				return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
			}
		}
		return nil
	}

	conn, err := dialFakeAddr(start.GetEndpoint())
	if err != nil {
		return err
	}
	defer conn.Close()
	subClient, sub := pubsubpb.NewSubscriberClient(conn), start.GetSubscription()
	proj, _ := splitRes(sub)
	out := func() context.Context { return m.outCtx(stream.Context(), "subscription", sub) }
	m.issueUnexpectedOutOfBandRPCs(stream.Context(), conn, proj, sub)

	if m.cfg.faults["ack-before-pull"] {
		_, _ = subClient.Acknowledge(out(), &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{"early-ack"}})
	}
	sp, err := subClient.StreamingPull(out())
	if err != nil {
		return err
	}
	var pv int64
	if m.caps.GetSupportsKeepalive() {
		pv = 1
	}
	wireSub := sub
	if m.cfg.faults["wrong-sub"] {
		wireSub += "-wrong"
	}
	if err := sp.Send(&pubsubpb.StreamingPullRequest{Subscription: wireSub, StreamAckDeadlineSeconds: 60, ClientId: cid, ProtocolVersion: pv}); err != nil {
		return err
	}

	cmdCh := make(chan *proxypb.SubscriberCommand, 8)
	go func() {
		defer close(cmdCh)
		for c, e := stream.Recv(); e == nil; c, e = stream.Recv() {
			cmdCh <- c
		}
	}()

	ackByTok, spanByTok := map[string]string{}, map[string]*proxypb.TraceSpan{}
	for {
		resp, err := sp.Recv()
		if err != nil {
			return nil
		}
		for _, rm := range resp.GetReceivedMessages() {
			msg := rm.GetMessage()
			tok := fmt.Sprintf("deliv-%d", m.seq.Add(1))
			if m.cfg.faults["premature-ack"] {
				_, _ = subClient.Acknowledge(out(), &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{rm.GetAckId()}})
			}
			subSpan, cbTP := buildSubscribeSpansOnDelivery(m.cppMode, m.cfg.faults, m.store, sub, msg.GetMessageId(), rm.GetAckId(), msg.GetData(), msg.GetAttributes(), start.GetEnableMonitoring())
			if m.cfg.faults["emit-span-when-disabled"] && !start.GetEnableMonitoring() {
				emitUnexpectedDisabledSpan(m.store, "projects/"+proj+"/topics/unexpected")
			}
			ackByTok[tok], spanByTok[tok] = rm.GetAckId(), subSpan

			msgID, data, attrs, key := m.corruptDeliveredMsg(msg)
			if err := stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_MessageDelivered{MessageDelivered: &proxypb.MessageDeliveredEvent{
				DeliveryToken: tok, MessageId: msgID, Data: data, Attributes: attrs, OrderingKey: key, CallbackTraceparent: cbTP,
			}}}); err != nil {
				return err
			}
			for cmd := range cmdCh {
				switch {
				case cmd.GetSettle() != nil:
					dt := cmd.GetSettle().GetDeliveryToken()
					ackID, subSp := ackByTok[dt], spanByTok[dt]
					delete(ackByTok, dt)
					if m.cfg.faults["modack-lease-ext"] || m.cfg.faults["out-of-bounds-modack"] {
						dl := int32(30)
						if m.cfg.faults["out-of-bounds-modack"] {
							dl = 5
						}
						_, _ = subClient.ModifyAckDeadline(out(), &pubsubpb.ModifyAckDeadlineRequest{Subscription: sub, AckIds: []string{ackID}, AckDeadlineSeconds: dl})
					}
					if !m.cfg.faults["missing-ack"] {
						sendID, ackSub := ackID, sub
						if m.cfg.faults["wrong-ack-id"] {
							sendID = ackID + "-wrong"
						}
						if m.cfg.faults["wrong-sub"] {
							ackSub += "-wrong"
						}
						_, _ = subClient.Acknowledge(out(), &pubsubpb.AcknowledgeRequest{Subscription: ackSub, AckIds: []string{sendID}})
					}
					if m.cfg.faults["duplicate-ack"] {
						_, _ = subClient.Acknowledge(out(), &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{ackID}})
					}
					emitSubscribeAckSpans(m.cppMode, m.cfg.faults, m.store, sub, ackID, subSp)
				case cmd.GetStop() != nil:
					_ = sp.CloseSend()
					return stream.Send(&proxypb.SubscriberEvent{Event: &proxypb.SubscriberEvent_Stopped{Stopped: &proxypb.StoppedEvent{TotalDelivered: 1}}})
				}
			}
			_ = sp.CloseSend()
			return nil
		}
	}
}

func (m *mockProxyServer) UnaryPull(ctx context.Context, req *proxypb.UnaryPullRequest) (*proxypb.UnaryPullResponse, error) {
	m.markStarted()
	if !m.caps.GetSupportsUnaryPull() || m.cfg.faults["runtime-unimpl-unary-pull"] {
		return nil, status.Error(codes.Unimplemented, "unary pull unsupported")
	}
	if m.cfg.faults["inband-unimpl-unary-pull"] {
		return &proxypb.UnaryPullResponse{ErrorCode: int32(codes.Unimplemented), ErrorMessage: "in-band unimplemented"}, nil
	}
	conn, err := dialFakeAddr(req.GetEndpoint())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	sc, sub := pubsubpb.NewSubscriberClient(conn), req.GetSubscription()
	proj, _ := splitRes(sub)
	m.issueUnexpectedOutOfBandRPCs(ctx, conn, proj, sub)
	pullSub := sub
	if m.cfg.faults["wrong-sub"] {
		pullSub += "-wrong"
	}
	pr, err := sc.Pull(m.outCtx(ctx, "subscription", sub), &pubsubpb.PullRequest{Subscription: pullSub, MaxMessages: req.GetMaxMessages(), ReturnImmediately: req.GetReturnImmediately()})
	if err != nil {
		return &proxypb.UnaryPullResponse{ErrorCode: int32(status.Code(err)), ErrorMessage: err.Error()}, nil
	}
	var out []*proxypb.UnaryPullResponse_PulledMessage
	for _, rm := range pr.GetReceivedMessages() {
		if m.cfg.faults["emit-span-when-disabled"] {
			emitUnexpectedDisabledSpan(m.store, "projects/"+proj+"/topics/unexpected")
		}
		if req.GetSettleAction() == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK && !m.cfg.faults["missing-ack"] {
			aid := rm.GetAckId()
			if m.cfg.faults["wrong-ack-id"] {
				aid += "-wrong"
			}
			if _, err := sc.Acknowledge(m.outCtx(ctx, "subscription", sub), &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: []string{aid}}); err != nil {
				return nil, err
			}
		}
		msgID, data, attrs, key := m.corruptDeliveredMsg(rm.GetMessage())
		out = append(out, &proxypb.UnaryPullResponse_PulledMessage{
			MessageId: msgID, Data: data, Attributes: attrs, OrderingKey: key,
			SettleStatus: proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS,
		})
	}
	return &proxypb.UnaryPullResponse{Messages: out}, nil
}

func (m *mockProxyServer) corruptDeliveredMsg(msg *pubsubpb.PubsubMessage) (string, []byte, map[string]string, string) {
	msgID, data, attrs, key := msg.GetMessageId(), slices.Clone(msg.GetData()), maps.Clone(msg.GetAttributes()), msg.GetOrderingKey()
	switch {
	case m.cfg.faults["wrong-payload"]:
		data = append(data, []byte("-bad")...)
	case m.cfg.faults["wrong-attrs"]:
		if attrs == nil {
			attrs = map[string]string{}
		}
		attrs["__corrupt__"] = "1"
	case m.cfg.faults["corrupt-ordering-key"]:
		key = "unexpected-key"
	case m.cfg.faults["mismatched-message-id"]:
		msgID = "wrong-" + msgID
	}
	return msgID, data, attrs, key
}
