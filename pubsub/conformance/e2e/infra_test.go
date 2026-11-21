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

package e2e_test

import (
	"bufio"
	"compress/zlib"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	wireGzipDecompressCount    atomic.Int64
	wireDeflateDecompressCount atomic.Int64
)

type spyCompressor struct {
	inner encoding.Compressor
	count *atomic.Int64
}

func (s *spyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return s.inner.Compress(w)
}

func (s *spyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	s.count.Add(1)
	return s.inner.Decompress(r)
}

func (s *spyCompressor) Name() string {
	return s.inner.Name()
}

type testDeflateCompressor struct {
	count *atomic.Int64
}

func (d *testDeflateCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return zlib.NewWriter(w), nil
}

func (d *testDeflateCompressor) Decompress(r io.Reader) (io.Reader, error) {
	d.count.Add(1)
	return zlib.NewReader(r)
}

func (d *testDeflateCompressor) Name() string {
	return "deflate"
}

func init() {
	if base := encoding.GetCompressor(gzip.Name); base != nil {
		encoding.RegisterCompressor(&spyCompressor{inner: base, count: &wireGzipDecompressCount})
	}
	encoding.RegisterCompressor(&testDeflateCompressor{count: &wireDeflateDecompressCount})
}

// --- Minimal In-Test Pub/Sub Data-Plane Server (Publisher + Subscriber ONLY) ---

type capturedPublishCall struct {
	Req      *pubsubpb.PublishRequest
	MD       metadata.MD
	CallTime time.Time
}

type capturedAckCall struct {
	Req *pubsubpb.AcknowledgeRequest
	MD  metadata.MD
}

type capturedModAckCall struct {
	Req *pubsubpb.ModifyAckDeadlineRequest
	MD  metadata.MD
}

type capturedStreamPullInit struct {
	Req *pubsubpb.StreamingPullRequest
	MD  metadata.MD
}

type capturedPullCall struct {
	Req *pubsubpb.PullRequest
	MD  metadata.MD
}

type miniPubSubServer struct {
	pubsubpb.UnimplementedPublisherServer
	pubsubpb.UnimplementedSubscriberServer

	grpcSrv *grpc.Server
	addr    string

	mu           sync.Mutex
	publishCalls []capturedPublishCall
	ackCalls     []capturedAckCall
	modAckCalls  []capturedModAckCall
	streamInits  []capturedStreamPullInit
	pullCalls    []capturedPullCall
	msgIDSeq     atomic.Int64

	onPublish              func(ctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attemptIdx int) (*pubsubpb.PublishResponse, error)
	onAck                  func(ctx context.Context, req *pubsubpb.AcknowledgeRequest, attemptIdx int) error
	onModAck               func(ctx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attemptIdx int) error
	onPull                 func(ctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error)
	onStreamPull           func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error
	onStreamPullBeforeRecv func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error

	pubAttempts    atomic.Int64
	ackAttempts    atomic.Int64
	modAckAttempts atomic.Int64
	streamSeq      atomic.Int64
}

func startMiniPubSubServer(t *testing.T) *miniPubSubServer {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("startMiniPubSubServer listen: %v", err)
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(32<<20),
		grpc.MaxSendMsgSize(32<<20),
	)
	m := &miniPubSubServer{
		grpcSrv: srv,
		addr:    lis.Addr().String(),
	}
	pubsubpb.RegisterPublisherServer(srv, m)
	pubsubpb.RegisterSubscriberServer(srv, m)
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
	})
	return m
}

func (m *miniPubSubServer) Publish(ctx context.Context, req *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	m.mu.Lock()
	m.publishCalls = append(m.publishCalls, capturedPublishCall{
		Req:      proto.Clone(req).(*pubsubpb.PublishRequest),
		MD:       md.Copy(),
		CallTime: time.Now(),
	})
	fn := m.onPublish
	m.mu.Unlock()

	attempt := int(m.pubAttempts.Add(1))
	if fn != nil {
		return fn(ctx, req, md, attempt)
	}
	ids := make([]string, len(req.Messages))
	for i := range req.Messages {
		ids[i] = fmt.Sprintf("srv-msg-%d", m.msgIDSeq.Add(1))
	}
	return &pubsubpb.PublishResponse{MessageIds: ids}, nil
}

func (m *miniPubSubServer) Acknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest) (*emptypb.Empty, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	m.mu.Lock()
	m.ackCalls = append(m.ackCalls, capturedAckCall{
		Req: proto.Clone(req).(*pubsubpb.AcknowledgeRequest),
		MD:  md.Copy(),
	})
	fn := m.onAck
	m.mu.Unlock()

	attempt := int(m.ackAttempts.Add(1))
	if fn != nil {
		if err := fn(ctx, req, attempt); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (m *miniPubSubServer) ModifyAckDeadline(ctx context.Context, req *pubsubpb.ModifyAckDeadlineRequest) (*emptypb.Empty, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	m.mu.Lock()
	m.modAckCalls = append(m.modAckCalls, capturedModAckCall{
		Req: proto.Clone(req).(*pubsubpb.ModifyAckDeadlineRequest),
		MD:  md.Copy(),
	})
	fn := m.onModAck
	m.mu.Unlock()

	attempt := int(m.modAckAttempts.Add(1))
	if fn != nil {
		if err := fn(ctx, req, attempt); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (m *miniPubSubServer) Pull(ctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	m.mu.Lock()
	m.pullCalls = append(m.pullCalls, capturedPullCall{
		Req: proto.Clone(req).(*pubsubpb.PullRequest),
		MD:  md.Copy(),
	})
	fn := m.onPull
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}
	return &pubsubpb.PullResponse{}, nil
}

func (m *miniPubSubServer) StreamingPull(stream pubsubpb.Subscriber_StreamingPullServer) error {
	m.mu.Lock()
	preFn := m.onStreamPullBeforeRecv
	m.mu.Unlock()
	var idx int
	if preFn != nil {
		idx = int(m.streamSeq.Add(1))
		if err := preFn(stream, idx); err != nil {
			return err
		}
	}

	initReq, err := stream.Recv()
	if err != nil {
		return err
	}
	md, _ := metadata.FromIncomingContext(stream.Context())
	m.mu.Lock()
	m.streamInits = append(m.streamInits, capturedStreamPullInit{
		Req: proto.Clone(initReq).(*pubsubpb.StreamingPullRequest),
		MD:  md.Copy(),
	})
	fn := m.onStreamPull
	m.mu.Unlock()

	if idx == 0 {
		idx = int(m.streamSeq.Add(1))
	}
	if fn != nil {
		return fn(stream, idx)
	}
	<-stream.Context().Done()
	return nil
}

func (m *miniPubSubServer) resetCounters() {
	m.mu.Lock()
	m.publishCalls = nil
	m.ackCalls = nil
	m.modAckCalls = nil
	m.streamInits = nil
	m.pullCalls = nil
	m.mu.Unlock()
	m.pubAttempts.Store(0)
	m.ackAttempts.Store(0)
	m.modAckAttempts.Store(0)
	m.streamSeq.Store(0)
}

func (m *miniPubSubServer) getPublishCalls() []capturedPublishCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]capturedPublishCall, len(m.publishCalls))
	copy(out, m.publishCalls)
	return out
}

func (m *miniPubSubServer) getAckCalls() []capturedAckCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]capturedAckCall, len(m.ackCalls))
	copy(out, m.ackCalls)
	return out
}

func (m *miniPubSubServer) getModAckCalls() []capturedModAckCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]capturedModAckCall, len(m.modAckCalls))
	copy(out, m.modAckCalls)
	return out
}

func (m *miniPubSubServer) getStreamInits() []capturedStreamPullInit {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]capturedStreamPullInit, len(m.streamInits))
	copy(out, m.streamInits)
	return out
}

func (m *miniPubSubServer) getPullCalls() []capturedPullCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]capturedPullCall, len(m.pullCalls))
	copy(out, m.pullCalls)
	return out
}

// --- 1. Strict Opaque-Box Import Boundary Guard ---

func TestInfra_OpaqueBoxImportsOnly(t *testing.T) {
	e2eRoot := filepath.Join(repoRootDir, "pubsub", "conformance", "e2e")
	allowedExact := map[string]bool{
		"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb":  true,
		"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb": true,
		"google.golang.org/grpc":                               true,
		"google.golang.org/grpc/codes":                         true,
		"google.golang.org/grpc/status":                        true,
		"google.golang.org/grpc/metadata":                      true,
		"google.golang.org/grpc/credentials/insecure":          true,
		"google.golang.org/grpc/encoding":                      true,
		"google.golang.org/grpc/encoding/gzip":                 true,
		"google.golang.org/protobuf/proto":                     true,
		"google.golang.org/protobuf/types/known/timestamppb":   true,
		"google.golang.org/protobuf/types/known/durationpb":    true,
		"google.golang.org/protobuf/types/known/emptypb":       true,
		"google.golang.org/protobuf/encoding/protojson":        true,
		"google.golang.org/genproto/googleapis/rpc/errdetails": true,
		"github.com/google/uuid":                               true,
	}

	fset := token.NewFileSet()
	var checkedFiles int
	err := filepath.WalkDir(e2eRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		checkedFiles++
		f, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if parseErr != nil {
			t.Errorf("ParseFile(%q): %v", path, parseErr)
			return nil
		}
		rel, _ := filepath.Rel(e2eRoot, path)
		if strings.HasSuffix(d.Name(), "_test.go") && f.Name.Name != "e2e_test" {
			t.Errorf("%s: expected external test package e2e_test, got %s", rel, f.Name.Name)
		}
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Errorf("%s: invalid import %s", rel, imp.Path.Value)
				continue
			}
			// Stdlib packages have no '.' in the first path element.
			firstSeg := p
			if slash := strings.IndexByte(p, '/'); slash >= 0 {
				firstSeg = p[:slash]
			}
			if !strings.Contains(firstSeg, ".") {
				continue
			}
			if !allowedExact[p] {
				t.Errorf("%s: forbidden non-opaque-box import %q", rel, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%q): %v", e2eRoot, err)
	}
	if checkedFiles == 0 {
		t.Fatalf("no .go files found under %q", e2eRoot)
	}
}

// --- 2. Subprocess Lifecycle & Startup/Shutdown Fault Profiles ---

func TestInfra_E2EProxyLifecycleAndStartupFaults(t *testing.T) {
	t.Run("ConformantGoHandshakeAndShutdownProxy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		caps, err := h.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("GetCapabilities: %v", err)
		}
		if caps.ClientLanguage != "go" || !caps.SupportsMonitoring || !caps.SupportsUnaryPull || !caps.SupportsPublishHedging {
			t.Fatalf("unexpected conformant-go capabilities: %+v", caps)
		}
		if err := h.Shutdown(t, 2*time.Second); err != nil {
			t.Fatalf("ShutdownProxy exit error: %v", err)
		}
	})

	t.Run("SIGTERMGracefulExit", func(t *testing.T) {
		h := startTestProxyDirect(t, "--mode=conformant-go")
		if err := h.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("Signal(SIGTERM): %v", err)
		}
		err, ok := h.WaitExit(3 * time.Second)
		if !ok {
			t.Fatalf("proxy did not exit on SIGTERM")
		}
		if err != nil {
			t.Fatalf("unexpected non-zero exit after SIGTERM: %v", err)
		}
		assertNoOrphanProxy(t, h.Marker)
	})

	t.Run("CrashStartup", func(t *testing.T) {
		out := runTestProxyRaw(t, 8*time.Second, "--mode=crash-startup")
		if out.TimedOut {
			t.Fatalf("crash-startup timed out")
		}
		if out.ExitCode != 2 {
			t.Fatalf("expected exit code 2 for crash-startup, got %d (stderr=%q)", out.ExitCode, out.Stderr)
		}
		if strings.Contains(out.Stdout, "READY ") {
			t.Fatalf("crash-startup unexpectedly emitted READY: %q", out.Stdout)
		}
		if !strings.Contains(out.Stderr, "simulated startup crash") {
			t.Fatalf("crash-startup stderr missing simulated startup crash: %q", out.Stderr)
		}
	})

	t.Run("BadReadyAndSilentExit", func(t *testing.T) {
		out := runTestProxyRaw(t, 8*time.Second, "--mode=bad-ready")
		if _, ok := parseReadyLine(out.Stdout); ok {
			t.Fatalf("parseReadyLine unexpectedly accepted bad-ready output: %q", out.Stdout)
		}
		if !strings.Contains(out.Stdout, "READY not-a-host-port") {
			t.Fatalf("unexpected bad-ready stdout: %q", out.Stdout)
		}

		silent := runTestProxyRaw(t, 8*time.Second, "--mode=malformed-ready", "--fault=silent-exit")
		if silent.ExitCode != 0 || strings.TrimSpace(silent.Stdout) != "" {
			t.Fatalf("expected silent exit 0, got code=%d stdout=%q", silent.ExitCode, silent.Stdout)
		}
	})

	t.Run("CrashPhaseReadyAndScenarioAndCrashAfter", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		readyCrash := runTestProxyRaw(t, 8*time.Second, "--port=0", "--crash-phase=ready")
		if readyCrash.TimedOut || readyCrash.ExitCode != 2 {
			t.Fatalf("expected exit code 2 for --crash-phase=ready, got timedOut=%v code=%d (stderr=%q)", readyCrash.TimedOut, readyCrash.ExitCode, readyCrash.Stderr)
		}
		if _, ok := parseReadyLine(readyCrash.Stdout); !ok {
			t.Fatalf("expected valid READY before crash-phase=ready exit, got %q", readyCrash.Stdout)
		}
		if !strings.Contains(readyCrash.Stderr, "simulated post-ready crash") {
			t.Fatalf("crash-phase=ready stderr missing simulated post-ready crash: %q", readyCrash.Stderr)
		}

		h := startTestProxyDirect(t, "--crash-phase=scenario")
		if _, err := h.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{}); err != nil {
			t.Fatalf("GetCapabilities before scenario crash failed: %v", err)
		}
		_, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: "127.0.0.1:1", Topic: "projects/p1/topics/t"})
		if err == nil {
			t.Fatalf("expected CreatePublisher to fail when --crash-phase=scenario triggers")
		}
		_, ok := h.WaitExit(2 * time.Second)
		if !ok {
			t.Fatalf("proxy did not exit after --crash-phase=scenario")
		}
		assertNoOrphanProxy(t, h.Marker)

		h2 := startTestProxyDirect(t, "--fault=crash-after=2")
		pubResp, err := h2.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: "127.0.0.1:1", Topic: "projects/p1/topics/t"})
		if err != nil {
			t.Fatalf("1st RPC before crash-after=2 failed: %v", err)
		}
		_, err = h2.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubResp.PublisherId, Data: []byte("x")})
		if err == nil {
			t.Fatalf("expected 2nd RPC to crash under --fault=crash-after=2")
		}
		_, ok = h2.WaitExit(2 * time.Second)
		if !ok {
			t.Fatalf("proxy did not exit after --fault=crash-after=2")
		}
		assertNoOrphanProxy(t, h2.Marker)
	})

	t.Run("IgnoreShutdownAndIgnoreSigterm", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=ignore-shutdown")
		if _, err := h.Client.ShutdownProxy(ctx, &proxypb.ShutdownProxyRequest{GracePeriodMs: 100}); err != nil {
			t.Fatalf("ShutdownProxy: %v", err)
		}
		if _, exited := h.WaitExit(200 * time.Millisecond); exited {
			t.Fatalf("ignore-shutdown exited on ShutdownProxy")
		}
		if err := h.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("SIGTERM: %v", err)
		}
		if _, exited := h.WaitExit(2 * time.Second); !exited {
			t.Fatalf("ignore-shutdown did not exit on SIGTERM")
		}
		assertNoOrphanProxy(t, h.Marker)

		h2 := startTestProxyDirect(t, "--mode=ignore-shutdown", "--fault=ignore-sigterm")
		if err := h2.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("SIGTERM: %v", err)
		}
		if _, exited := h2.WaitExit(200 * time.Millisecond); exited {
			t.Fatalf("ignore-sigterm unexpectedly exited on SIGTERM")
		}
		h2.ForceKill(t)
	})

	t.Run("HangScenarioBlockedUntilKilled", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=hang-scenario")
		if _, err := h.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{}); err != nil {
			t.Fatalf("GetCapabilities: %v", err)
		}
		pubResp, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: "127.0.0.1:1", Topic: "projects/p1/topics/t"})
		if err != nil {
			t.Fatalf("CreatePublisher: %v", err)
		}
		pubCtx, pubCancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer pubCancel()
		_, err = h.Client.Publish(pubCtx, &proxypb.PublishRequest{PublisherId: pubResp.PublisherId, Data: []byte("hi")})
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("expected DeadlineExceeded on hung Publish, got %v", err)
		}
		h.ForceKill(t)
	})

	t.Run("ProxyWrapperScriptAndMarker", func(t *testing.T) {
		wrapper, marker := writeProxyWrapper(t, "--mode=conformant-go")
		markers := extractRunMarkers([]string{"--proxy-cmd", wrapper, "--proxy-target", "go=" + wrapper})
		if len(markers) != 1 || markers[0] != marker {
			t.Fatalf("extractRunMarkers = %v, want [%s]", markers, marker)
		}
		cmd := exec.Command(wrapper, "--port=0")
		outPipe, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatalf("StdoutPipe: %v", err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatalf("Start wrapper: %v", err)
		}
		buf := make([]byte, 128)
		n, err := outPipe.Read(buf)
		if err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("Read wrapper stdout: %v", err)
		}
		if _, ok := parseReadyLine(string(buf[:n])); !ok {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("wrapper did not emit valid READY line: %q", string(buf[:n]))
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
		assertNoOrphanProxy(t, marker)
	})
}

// --- 3. Real Wire Publisher + Batching + Ordering + Hedging + Compression + Canonical Go Traces ---

func TestInfra_E2EProxyWirePublisherAndTracing(t *testing.T) {
	srv := startMiniPubSubServer(t)
	h := startTestProxyDirect(t, "--mode=conformant-go")
	defer func() {
		_ = h.Shutdown(t, 2*time.Second)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("BasicPublishMetadataTelemetryImmutabilityAndTraces", func(t *testing.T) {
		srv.resetCounters()
		if _, err := h.Client.ResetTraces(ctx, &proxypb.ResetTracesRequest{}); err != nil {
			t.Fatalf("ResetTraces: %v", err)
		}

		pubResp, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         srv.addr,
			Topic:            "projects/p1/topics/t1",
			EnableMonitoring: true,
			CountThreshold:   protoInt64(1),
			UseEmulatorEnv:   true,
			CustomHeaders:    map[string]string{"x-custom-test": "alpha"},
		})
		if err != nil {
			t.Fatalf("CreatePublisher: %v", err)
		}
		defer func() {
			_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubResp.PublisherId})
		}()

		parentTP := "00-11111111111111111111111111111111-2222222222222222-01"
		res, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId:                  pubResp.PublisherId,
			Data:                         []byte("hello-wire"),
			Attributes:                   map[string]string{"k1": "v1"},
			ParentTraceparent:            parentTP,
			VerifyAttributesImmutability: true,
		})
		if err != nil || res.ErrorCode != 0 || res.MessageId == "" {
			t.Fatalf("Publish failed: err=%v res=%+v", err, res)
		}
		if res.CallerAttributesMutated {
			t.Fatalf("expected CallerAttributesMutated=false")
		}

		calls := srv.getPublishCalls()
		if len(calls) != 1 || len(calls[0].Req.Messages) != 1 {
			t.Fatalf("expected 1 Publish RPC with 1 msg, got %+v", calls)
		}
		msg := calls[0].Req.Messages[0]
		if string(msg.Data) != "hello-wire" || msg.Attributes["k1"] != "v1" {
			t.Fatalf("unexpected wire payload/attrs: %+v", msg)
		}
		if got := calls[0].MD.Get("x-goog-request-params"); len(got) == 0 || got[0] != "topic=projects%2Fp1%2Ftopics%2Ft1" {
			t.Fatalf("missing/invalid x-goog-request-params: %v", got)
		}
		if got := calls[0].MD.Get("x-custom-test"); len(got) == 0 || got[0] != "alpha" {
			t.Fatalf("missing custom header: %v", got)
		}
		telVals := calls[0].MD.Get("x-goog-pubsub-client-telemetry")
		if len(telVals) != 1 {
			t.Fatalf("expected x-goog-pubsub-client-telemetry header, got %v", telVals)
		}
		telBytes, err := base64.StdEncoding.DecodeString(telVals[0])
		if err != nil {
			t.Fatalf("base64 decode telemetry: %v", err)
		}
		var tel pbTelemetryAlias
		if err := proto.Unmarshal(telBytes, &tel); err != nil {
			t.Fatalf("unmarshal PubsubClientTelemetry: %v", err)
		}
		if tel.GetPublishOperation() == nil || tel.GetPublishOperation().GetPublishStartTime() == nil {
			t.Fatalf("unexpected PubsubClientTelemetry payload: %+v", &tel)
		}
		tpInjected := msg.Attributes["googclient_traceparent"]
		if !strings.HasPrefix(tpInjected, "00-11111111111111111111111111111111-") {
			t.Fatalf("expected googclient_traceparent inheriting caller traceID, got %q", tpInjected)
		}

		drain, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		assertCanonicalGoPublishSpans(t, drain.Spans, "t1", res.MessageId)

		// Second DrainTraces must be empty (drain isolation).
		drain2, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil || len(drain2.Spans) != 0 {
			t.Fatalf("expected empty second DrainTraces, got len=%d err=%v", len(drain2.Spans), err)
		}
	})

	t.Run("GzipAndDeflateCompression", func(t *testing.T) {
		wireGzipDecompressCount.Store(0)
		wireDeflateDecompressCount.Store(0)

		pubGzip, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:                  srv.addr,
			Topic:                     "projects/p1/topics/t-gzip",
			CountThreshold:            protoInt64(1),
			EnableCompression:         protoBool(true),
			CompressionBytesThreshold: protoInt64(50),
			CompressionAlgorithm:      proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_GZIP,
		})
		if err != nil {
			t.Fatalf("CreatePublisher gzip: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubGzip.PublisherId})

		// Below threshold -> uncompressed.
		if _, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubGzip.PublisherId, Data: []byte("tiny")}); err != nil {
			t.Fatalf("tiny publish: %v", err)
		}
		if wireGzipDecompressCount.Load() != 0 {
			t.Fatalf("unexpected gzip compression for tiny message")
		}
		// Above threshold -> compressed.
		big := make([]byte, 200)
		for i := range big {
			big[i] = 'a'
		}
		if _, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubGzip.PublisherId, Data: big}); err != nil {
			t.Fatalf("big gzip publish: %v", err)
		}
		if wireGzipDecompressCount.Load() == 0 {
			t.Fatalf("expected gzip wire compression above threshold")
		}

		// Deflate compression.
		pubDeflate, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:                  srv.addr,
			Topic:                     "projects/p1/topics/t-deflate",
			CountThreshold:            protoInt64(1),
			EnableCompression:         protoBool(true),
			CompressionBytesThreshold: protoInt64(50),
			CompressionAlgorithm:      proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE,
		})
		if err != nil {
			t.Fatalf("CreatePublisher deflate: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubDeflate.PublisherId})

		if _, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubDeflate.PublisherId, Data: big}); err != nil {
			t.Fatalf("big deflate publish: %v", err)
		}
		if wireDeflateDecompressCount.Load() == 0 {
			t.Fatalf("expected deflate wire compression above threshold")
		}
	})

	t.Run("BatchingSplittingAsyncFlushStopAndValidation", func(t *testing.T) {
		srv.resetCounters()
		pubResp, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         srv.addr,
			Topic:            "projects/p1/topics/t-batch",
			CountThreshold:   protoInt64(10),
			ByteThreshold:    protoInt64(100),
			DelayThresholdMs: protoInt64(5000),
		})
		if err != nil {
			t.Fatalf("CreatePublisher: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubResp.PublisherId})

		// Pre-add ByteThreshold split: two 70B messages split into 2 batches on Flush.
		payload70 := make([]byte, 70)
		r1, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubResp.PublisherId, Data: payload70, AsyncFire: true})
		if err != nil || r1.PublishToken == "" {
			t.Fatalf("async publish 1: err=%v r=%+v", err, r1)
		}
		r2, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubResp.PublisherId, Data: payload70, AsyncFire: true})
		if err != nil || r2.PublishToken == "" {
			t.Fatalf("async publish 2: err=%v r=%+v", err, r2)
		}
		if _, err := h.Client.FlushPublisher(ctx, &proxypb.FlushPublisherRequest{PublisherId: pubResp.PublisherId}); err != nil {
			t.Fatalf("FlushPublisher: %v", err)
		}
		aw1, err := h.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubResp.PublisherId, PublishToken: r1.PublishToken})
		if err != nil || aw1.ErrorCode != 0 || aw1.MessageId == "" {
			t.Fatalf("AwaitPublish 1: err=%v aw=%+v", err, aw1)
		}
		aw2, err := h.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubResp.PublisherId, PublishToken: r2.PublishToken})
		if err != nil || aw2.ErrorCode != 0 || aw2.MessageId == "" {
			t.Fatalf("AwaitPublish 2: err=%v aw=%+v", err, aw2)
		}
		if len(srv.getPublishCalls()) != 2 {
			t.Fatalf("expected 2 batches due to pre-add ByteThreshold split, got %d", len(srv.getPublishCalls()))
		}

		// Singleton > ByteThreshold (<=10MB) succeeds; >10MB rejected without RPC.
		singleton250 := make([]byte, 250)
		resSing, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubResp.PublisherId, Data: singleton250, UseBlockingPublisher: true})
		if err != nil || resSing.ErrorCode != 0 {
			t.Fatalf("singleton > ByteThreshold failed: err=%v res=%+v", err, resSing)
		}
		oversized := make([]byte, 10_000_100)
		callsBefore := len(srv.getPublishCalls())
		resBig, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubResp.PublisherId, Data: oversized})
		if err != nil || resBig.ErrorCode != int32(codes.InvalidArgument) {
			t.Fatalf("expected InvalidArgument on >10MB msg, got err=%v res=%+v", err, resBig)
		}
		// Non-UTF-8 RawAttributes rejected without RPC.
		resBadUTF8, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId:   pubResp.PublisherId,
			Data:          []byte("ok"),
			RawAttributes: []*proxypb.RawAttribute{{Key: []byte("bad"), Value: []byte{0xff, 0xfe}}},
		})
		if err != nil || resBadUTF8.ErrorCode == 0 {
			t.Fatalf("expected error on non-UTF-8 RawAttributes, got err=%v res=%+v", err, resBadUTF8)
		}
		if len(srv.getPublishCalls()) != callsBefore {
			t.Fatalf("rejected messages unexpectedly sent Publish RPCs")
		}

		// StopPublisher drains and rejects subsequent Publish.
		if _, err := h.Client.StopPublisher(ctx, &proxypb.StopPublisherRequest{PublisherId: pubResp.PublisherId}); err != nil {
			t.Fatalf("StopPublisher: %v", err)
		}
		afterStop, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubResp.PublisherId, Data: []byte("late")})
		if err != nil || afterStop.ErrorCode == 0 {
			t.Fatalf("expected non-zero error after StopPublisher, got err=%v res=%+v", err, afterStop)
		}

		// DelayThresholdMs coalescing: 2 messages coalesce into 1 batch after 40ms.
		srv.resetCounters()
		pubDelay, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         srv.addr,
			Topic:            "projects/p1/topics/t-delay",
			CountThreshold:   protoInt64(10),
			DelayThresholdMs: protoInt64(150),
		})
		if err != nil {
			t.Fatalf("CreatePublisher delay: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubDelay.PublisherId})
		d1, _ := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubDelay.PublisherId, Data: []byte("d1"), AsyncFire: true})
		d2, _ := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubDelay.PublisherId, Data: []byte("d2"), AsyncFire: true})
		if _, err := h.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubDelay.PublisherId, PublishToken: d1.PublishToken}); err != nil {
			t.Fatalf("AwaitPublish d1: %v", err)
		}
		if _, err := h.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubDelay.PublisherId, PublishToken: d2.PublishToken}); err != nil {
			t.Fatalf("AwaitPublish d2: %v", err)
		}
		if calls := srv.getPublishCalls(); len(calls) != 1 || len(calls[0].Req.Messages) != 2 {
			t.Fatalf("expected DelayThresholdMs to coalesce 2 msgs into 1 batch, got %+v", calls)
		}
	})

	t.Run("OrderingOptInFIFOAndPauseResume", func(t *testing.T) {
		srv.resetCounters()
		// Opt-in disabled -> ordering_key rejected.
		pubUnordered, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:              srv.addr,
			Topic:                 "projects/p1/topics/t-no-ord",
			EnableMessageOrdering: protoBool(false),
		})
		if err != nil {
			t.Fatalf("CreatePublisher: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubUnordered.PublisherId})
		rej, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubUnordered.PublisherId, Data: []byte("x"), OrderingKey: "k1"})
		if err != nil || rej.ErrorCode == 0 {
			t.Fatalf("expected error for ordering key when opt-in false, got err=%v res=%+v", err, rej)
		}

		// Ordered publisher: FIFO <=1 in-flight per key + pause on error + ResumePublish.
		pubOrd, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:              srv.addr,
			Topic:                 "projects/p1/topics/t-ord",
			EnableMessageOrdering: protoBool(true),
			CountThreshold:        protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher ordered: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubOrd.PublisherId})

		var failKey1 atomic.Bool
		failKey1.Store(true)
		srv.mu.Lock()
		srv.onPublish = func(ctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attemptIdx int) (*pubsubpb.PublishResponse, error) {
			if failKey1.Load() && len(req.Messages) > 0 && req.Messages[0].OrderingKey == "k1" {
				return nil, status.Error(codes.PermissionDenied, "boom")
			}
			ids := make([]string, len(req.Messages))
			for i := range req.Messages {
				ids[i] = fmt.Sprintf("ord-%d-%d", attemptIdx, i)
			}
			return &pubsubpb.PublishResponse{MessageIds: ids}, nil
		}
		srv.mu.Unlock()
		defer func() {
			srv.mu.Lock()
			srv.onPublish = nil
			srv.mu.Unlock()
		}()

		errRes, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubOrd.PublisherId, Data: []byte("a"), OrderingKey: "k1"})
		if err != nil || errRes.ErrorCode == 0 {
			t.Fatalf("expected k1 permanent error, got err=%v res=%+v", err, errRes)
		}
		// k1 is now paused, while k2 succeeds!
		pausedRes, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubOrd.PublisherId, Data: []byte("b"), OrderingKey: "k1"})
		if err != nil || pausedRes.ErrorCode == 0 {
			t.Fatalf("expected paused k1 error, got err=%v res=%+v", err, pausedRes)
		}
		k2Res, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubOrd.PublisherId, Data: []byte("c"), OrderingKey: "k2"})
		if err != nil || k2Res.ErrorCode != 0 {
			t.Fatalf("expected k2 to remain unpaused, got err=%v res=%+v", err, k2Res)
		}

		failKey1.Store(false)
		if _, err := h.Client.ResumePublish(ctx, &proxypb.ResumePublishRequest{PublisherId: pubOrd.PublisherId, OrderingKey: "k1"}); err != nil {
			t.Fatalf("ResumePublish: %v", err)
		}
		resumedRes, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubOrd.PublisherId, Data: []byte("d"), OrderingKey: "k1"})
		if err != nil || resumedRes.ErrorCode != 0 {
			t.Fatalf("expected k1 publish to succeed after ResumePublish, got err=%v res=%+v", err, resumedRes)
		}
	})

	t.Run("FlowControlAndHedging", func(t *testing.T) {
		srv.resetCounters()
		// Conflict: Ordering + Hedging rejected.
		pubConflict, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:              srv.addr,
			Topic:                 "projects/p1/topics/t-conflict",
			EnableMessageOrdering: protoBool(true),
			Hedging:               &proxypb.HedgingSettings{Enabled: true},
		})
		if err != nil {
			t.Fatalf("CreatePublisher conflict: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubConflict.PublisherId})
		cRes, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubConflict.PublisherId, Data: []byte("x")})
		if err != nil || cRes.ErrorCode != int32(codes.InvalidArgument) {
			t.Fatalf("expected InvalidArgument on ordering+hedging conflict, got err=%v res=%+v", err, cRes)
		}

		// FlowControl SignalError.
		gate := make(chan struct{})
		srv.mu.Lock()
		srv.onPublish = func(ctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attemptIdx int) (*pubsubpb.PublishResponse, error) {
			if string(req.Messages[0].Data) == "hold" {
				<-gate
			}
			return &pubsubpb.PublishResponse{MessageIds: []string{fmt.Sprintf("fc-%d", attemptIdx)}}, nil
		}
		srv.mu.Unlock()
		pubFC, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:               srv.addr,
			Topic:                  "projects/p1/topics/t-fc",
			CountThreshold:         protoInt64(1),
			MaxOutstandingMessages: protoInt64(1),
			LimitExceededBehavior:  proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR,
		})
		if err != nil {
			t.Fatalf("CreatePublisher FC: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubFC.PublisherId})
		tok1, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubFC.PublisherId, Data: []byte("hold"), AsyncFire: true})
		if err != nil {
			t.Fatalf("FC hold publish: %v", err)
		}
		overflow, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubFC.PublisherId, Data: []byte("over")})
		if err != nil || overflow.ErrorCode == 0 {
			close(gate)
			t.Fatalf("expected flow control SignalError, got err=%v res=%+v", err, overflow)
		}
		close(gate)
		if _, err := h.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubFC.PublisherId, PublishToken: tok1.PublishToken}); err != nil {
			t.Fatalf("AwaitPublish FC: %v", err)
		}

		// Hedging: refill 5 times (0.2 * 5 = 1 token), then slow primary triggers hedged attempt!
		srv.resetCounters()
		if _, err := h.Client.ResetTraces(ctx, &proxypb.ResetTracesRequest{}); err != nil {
			t.Fatalf("ResetTraces: %v", err)
		}
		srv.mu.Lock()
		srv.onPublish = func(ctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attemptIdx int) (*pubsubpb.PublishResponse, error) {
			if string(req.Messages[0].Data) == "slow-hedge" {
				telVals := md.Get("x-goog-pubsub-client-telemetry")
				if len(telVals) > 0 {
					b, _ := base64.StdEncoding.DecodeString(telVals[0])
					var tel pbTelemetryAlias
					_ = proto.Unmarshal(b, &tel)
					if tel.GetPublishOperation().GetHedgedAttemptCount() == 0 {
						select {
						case <-time.After(3 * time.Second):
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					}
				}
				return &pubsubpb.PublishResponse{MessageIds: []string{"hedged-winner"}}, nil
			}
			return &pubsubpb.PublishResponse{MessageIds: []string{fmt.Sprintf("ok-%d", attemptIdx)}}, nil
		}
		srv.mu.Unlock()
		defer func() {
			srv.mu.Lock()
			srv.onPublish = nil
			srv.mu.Unlock()
		}()

		pubHedge, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         srv.addr,
			Topic:            "projects/p1/topics/t-hedge",
			EnableMonitoring: true,
			CountThreshold:   protoInt64(1),
			Hedging: &proxypb.HedgingSettings{
				Enabled:     true,
				DelayMs:     protoInt64(120),
				MaxTokens:   protoInt64(5),
				RefillRatio: protoFloat64(0.2),
			},
		})
		if err != nil {
			t.Fatalf("CreatePublisher hedge: %v", err)
		}
		defer h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubHedge.PublisherId})

		for i := 0; i < 5; i++ {
			r, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubHedge.PublisherId, Data: []byte("warmup")})
			if err != nil || r.ErrorCode != 0 {
				t.Fatalf("warmup publish %d failed: err=%v r=%+v", i, err, r)
			}
		}
		if _, err := h.Client.ResetTraces(ctx, &proxypb.ResetTracesRequest{}); err != nil {
			t.Fatalf("ResetTraces: %v", err)
		}
		hRes, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubHedge.PublisherId, Data: []byte("slow-hedge")})
		if err != nil || hRes.ErrorCode != 0 || hRes.MessageId != "hedged-winner" {
			t.Fatalf("hedged publish failed: err=%v r=%+v", err, hRes)
		}
		drain, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces hedge: %v", err)
		}
		foundHedgeStart, foundHedgeEnd := false, false
		for _, sp := range drain.Spans {
			for _, ev := range sp.Events {
				if ev.Name == "publish start (hedged)" {
					foundHedgeStart = true
				}
				if ev.Name == "publish end (hedged)" {
					foundHedgeEnd = true
				}
			}
		}
		if !foundHedgeStart || !foundHedgeEnd {
			t.Fatalf("expected hedged start/end events on span, got start=%v end=%v", foundHedgeStart, foundHedgeEnd)
		}
	})
}

// --- 4. Real Wire Subscriber + EOD ErrorInfo + Shutdown + UnaryPull + Canonical Subscribe Spans ---

func TestInfra_E2EProxyWireSubscriberAndEOD(t *testing.T) {
	srv := startMiniPubSubServer(t)
	h := startTestProxyDirect(t, "--mode=conformant-go")
	defer func() {
		_ = h.Shutdown(t, 2*time.Second)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("StreamingPullEODErrorInfoAndCanonicalTraces", func(t *testing.T) {
		srv.resetCounters()
		if _, err := h.Client.ResetTraces(ctx, &proxypb.ResetTracesRequest{}); err != nil {
			t.Fatalf("ResetTraces: %v", err)
		}

		// First publish a message to capture valid googclient_traceparent for end-to-end trace parenting.
		pubResp, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         srv.addr,
			Topic:            "projects/p1/topics/t-e2e",
			EnableMonitoring: true,
			CountThreshold:   protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher: %v", err)
		}
		pRes, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId: pubResp.PublisherId,
			Data:        []byte("trace-me"),
		})
		if err != nil || pRes.ErrorCode != 0 {
			t.Fatalf("Publish: err=%v r=%+v", err, pRes)
		}
		_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubResp.PublisherId})

		pubCalls := srv.getPublishCalls()
		if len(pubCalls) != 1 {
			t.Fatalf("expected 1 publish call, got %d", len(pubCalls))
		}
		publishedAttrs := pubCalls[0].Req.Messages[0].Attributes
		pubTraceparent := publishedAttrs["googclient_traceparent"]
		if pubTraceparent == "" {
			t.Fatalf("missing googclient_traceparent")
		}
		pubParts := strings.Split(pubTraceparent, "-")
		if _, err := h.Client.ResetTraces(ctx, &proxypb.ResetTracesRequest{}); err != nil {
			t.Fatalf("ResetTraces: %v", err)
		}

		// Configure server stream to deliver 3 EOD messages.
		srv.mu.Lock()
		srv.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				if err := stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{
							AckId:           "ack-ok",
							DeliveryAttempt: 2,
							Message: &pubsubpb.PubsubMessage{
								MessageId:   "m-1",
								Data:        []byte("one"),
								Attributes:  publishedAttrs,
								PublishTime: timestamppb.Now(),
							},
						},
						{
							AckId: "ack-transient",
							Message: &pubsubpb.PubsubMessage{
								MessageId: "m-2",
								Data:      []byte("two"),
							},
						},
						{
							AckId: "ack-perm",
							Message: &pubsubpb.PubsubMessage{
								MessageId: "m-3",
								Data:      []byte("three"),
							},
						},
					},
				}); err != nil {
					return err
				}
			}
			<-stream.Context().Done()
			return nil
		}
		var transientOnce atomic.Bool
		srv.onAck = func(ctx context.Context, req *pubsubpb.AcknowledgeRequest, attemptIdx int) error {
			meta := map[string]string{}
			for _, id := range req.AckIds {
				if id == "ack-transient" && transientOnce.CompareAndSwap(false, true) {
					meta[id] = "TRANSIENT_FAILURE_UNAVAILABLE"
				}
				if id == "ack-perm" {
					meta[id] = "PERMANENT_FAILURE_INVALID_ACK_ID"
				}
			}
			if len(meta) == 0 {
				return nil
			}
			st, err := status.New(codes.InvalidArgument, "partial ack error").WithDetails(&errdetails.ErrorInfo{
				Reason:   "EXACTLY_ONCE_DELIVERY_FAILURE",
				Domain:   "pubsub.googleapis.com",
				Metadata: meta,
			})
			if err != nil {
				return err
			}
			return st.Err()
		}
		srv.mu.Unlock()
		defer func() {
			srv.mu.Lock()
			srv.onStreamPull = nil
			srv.onAck = nil
			srv.mu.Unlock()
		}()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         srv.addr,
					Subscription:     "projects/p1/subscriptions/sub-eod",
					EnableMonitoring: true,
					ClientId:         "e2e-cid-1",
					ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING,
				},
			},
		}); err != nil {
			t.Fatalf("Send Start: %v", err)
		}

		ev1, err := sess.Recv()
		if err != nil || ev1.GetStarted() == nil || ev1.GetStarted().ClientId != "e2e-cid-1" {
			t.Fatalf("expected StartedEvent with client_id, got %+v err=%v", ev1, err)
		}

		delivered := make(map[string]*proxypb.MessageDeliveredEvent)
		for len(delivered) < 3 {
			ev, err := sess.Recv()
			if err != nil {
				t.Fatalf("Recv delivery: %v", err)
			}
			md := ev.GetMessageDelivered()
			if md == nil {
				t.Fatalf("expected MessageDeliveredEvent, got %+v", ev)
			}
			delivered[md.MessageId] = md
		}

		// Verify EOD receipt modack happened BEFORE deliveries (>=60s)!
		modAcks := srv.getModAckCalls()
		if len(modAcks) == 0 || modAcks[0].Req.AckDeadlineSeconds < 60 || len(modAcks[0].Req.AckIds) != 3 {
			t.Fatalf("expected EOD receipt ModifyAckDeadline(>=60) for 3 ackIDs before delivery, got %+v", modAcks)
		}
		if !delivered["m-1"].HasDeliveryAttempt || delivered["m-1"].DeliveryAttempt != 2 || delivered["m-1"].PublishTime == nil {
			t.Fatalf("expected DeliveryAttempt=2 and PublishTime on m-1, got %+v", delivered["m-1"])
		}
		if !strings.HasPrefix(delivered["m-1"].CallbackTraceparent, "00-"+pubParts[1]+"-") {
			t.Fatalf("expected CallbackTraceparent inheriting publish traceID %s, got %q", pubParts[1], delivered["m-1"].CallbackTraceparent)
		}

		// Send StopSubscriberCommand FIRST, then settle interactively AFTER Stop (testing post-stop Settle contract!).
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{
					ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING,
				},
			},
		}); err != nil {
			t.Fatalf("Send Stop: %v", err)
		}

		for _, msgID := range []string{"m-1", "m-2", "m-3"} {
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{
						DeliveryToken: delivered[msgID].DeliveryToken,
						Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
					},
				},
			}); err != nil {
				t.Fatalf("Send Settle %s: %v", msgID, err)
			}
		}

		settlements := make(map[string]*proxypb.SettlementResultEvent)
		var stopped *proxypb.StoppedEvent
		for stopped == nil {
			ev, err := sess.Recv()
			if err != nil {
				t.Fatalf("Recv settlement/stopped: %v", err)
			}
			if sr := ev.GetSettlementResult(); sr != nil {
				settlements[sr.DeliveryToken] = sr
			} else if st := ev.GetStopped(); st != nil {
				stopped = st
			}
		}
		if stopped.TotalDelivered != 3 || stopped.ErrorCode != 0 {
			t.Fatalf("unexpected StoppedEvent: %+v", stopped)
		}
		if settlements[delivered["m-1"].DeliveryToken].GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
			t.Fatalf("expected m-1 SUCCESS, got %+v", settlements[delivered["m-1"].DeliveryToken])
		}
		if settlements[delivered["m-2"].DeliveryToken].GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
			t.Fatalf("expected m-2 transient retry -> SUCCESS, got %+v", settlements[delivered["m-2"].DeliveryToken])
		}
		if settlements[delivered["m-3"].DeliveryToken].GetStatus() != proxypb.SettlementResultEvent_ACK_STATUS_INVALID_ACK_ID {
			t.Fatalf("expected m-3 INVALID_ACK_ID, got %+v", settlements[delivered["m-3"].DeliveryToken])
		}

		// Verify canonical Go subscribe/process/ack/modack spans.
		drain, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		assertCanonicalGoSubscribeSpans(t, drain.Spans, "sub-eod", pubParts[1])
	})

	t.Run("FastShutdownNackImmediatelyAndUnaryPull", func(t *testing.T) {
		srv.resetCounters()
		srv.mu.Lock()
		srv.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "ack-nack-imm", Message: &pubsubpb.PubsubMessage{MessageId: "m-imm", Data: []byte("d")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		srv.onPull = func(ctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
			return &pubsubpb.PullResponse{
				ReceivedMessages: []*pubsubpb.ReceivedMessage{
					{AckId: "pull-ack-1", DeliveryAttempt: 3, Message: &pubsubpb.PubsubMessage{MessageId: "pm-1", Data: []byte("pulled")}},
				},
			}, nil
		}
		srv.mu.Unlock()
		defer func() {
			srv.mu.Lock()
			srv.onStreamPull = nil
			srv.onPull = nil
			srv.mu.Unlock()
		}()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:     srv.addr,
					Subscription: "projects/p1/subscriptions/sub-fast",
				},
			},
		})
		_, _ = sess.Recv() // Started
		_, _ = sess.Recv() // MessageDelivered
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{
					ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY,
				},
			},
		})
		for {
			ev, err := sess.Recv()
			if err != nil {
				t.Fatalf("Recv stopped: %v", err)
			}
			if ev.GetStopped() != nil {
				break
			}
		}
		modAcks := srv.getModAckCalls()
		foundZero := false
		for _, ma := range modAcks {
			if ma.Req.AckDeadlineSeconds == 0 && len(ma.Req.AckIds) == 1 && ma.Req.AckIds[0] == "ack-nack-imm" {
				foundZero = true
			}
		}
		if !foundZero {
			t.Fatalf("expected NackImmediately bulk ModifyAckDeadline(0), got %+v", modAcks)
		}

		// UnaryPull test.
		upRes, err := h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
			Endpoint:          srv.addr,
			Subscription:      "projects/p1/subscriptions/sub-pull",
			MaxMessages:       1,
			ReturnImmediately: true,
			SettleAction:      proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
		})
		if err != nil || len(upRes.Messages) != 1 || upRes.Messages[0].MessageId != "pm-1" || !upRes.Messages[0].HasDeliveryAttempt || upRes.Messages[0].DeliveryAttempt != 3 {
			t.Fatalf("unexpected UnaryPull response: err=%v res=%+v", err, upRes)
		}
		acks := srv.getAckCalls()
		if len(acks) != 1 || acks[0].Req.AckIds[0] != "pull-ack-1" {
			t.Fatalf("expected UnaryPull Acknowledge, got %+v", acks)
		}
	})

	t.Run("MaxExtensionExpiryAndAckBatchSplitOver1000AndStreamRecovery", func(t *testing.T) {
		// 1. MaxExtensionMs expiration frees flow-control slot allowing next message delivery.
		srv.resetCounters()
		sendExp2 := make(chan struct{})
		exp1SentAt := make(chan time.Time, 1)
		srv.mu.Lock()
		srv.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				exp1SentAt <- time.Now()
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "ack-exp-1", Message: &pubsubpb.PubsubMessage{MessageId: "exp-1", Data: []byte("1")}},
					},
				})
				select {
				case <-sendExp2:
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: "ack-exp-2", Message: &pubsubpb.PubsubMessage{MessageId: "exp-2", Data: []byte("2")}},
						},
					})
				case <-stream.Context().Done():
					return nil
				}
			}
			<-stream.Context().Done()
			return nil
		}
		srv.mu.Unlock()

		sessExp, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession exp: %v", err)
		}
		_ = sessExp.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:               srv.addr,
					Subscription:           "projects/p1/subscriptions/sub-exp",
					MaxOutstandingMessages: protoInt64(1),
					MaxExtensionMs:         protoInt64(240),
					EnableMonitoring:       true,
				},
			},
		})
		_, _ = sessExp.Recv() // Started
		ev1, err := sessExp.Recv()
		if err != nil || ev1.GetMessageDelivered() == nil || ev1.GetMessageDelivered().MessageId != "exp-1" {
			t.Fatalf("expected 1st delivery exp-1, got %+v err=%v", ev1, err)
		}
		sent1 := <-exp1SentAt
		// Send exp-2 mid-lease (well before exp-1 expires at 240ms) so exp-2 blocks in fc.acquire
		// until exp-1 expires & frees FC, then delivers with >110ms lease left.
		time.Sleep(135 * time.Millisecond)
		close(sendExp2)
		// Do NOT settle exp-1; once MaxExtensionMs=240ms expires, FC frees up and exp-2 must arrive!
		ev2, err := sessExp.Recv()
		if err != nil || ev2.GetMessageDelivered() == nil || ev2.GetMessageDelivered().MessageId != "exp-2" {
			t.Fatalf("expected 2nd delivery after MaxExtension expiry freed FC, got %+v err=%v", ev2, err)
		}
		if elapsed := time.Since(sent1); elapsed < 230*time.Millisecond {
			t.Fatalf("expected exp-2 delivery only after exp-1 240ms expiry freed FC, arrived in %v", elapsed)
		}
		_ = sessExp.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{
					DeliveryToken: ev2.GetMessageDelivered().DeliveryToken,
					Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
				},
			},
		})
		_ = sessExp.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		var stoppedExp *proxypb.StoppedEvent
		for {
			ev, err := sessExp.Recv()
			if err != nil {
				t.Fatalf("Recv stopped exp: %v", err)
			}
			if st := ev.GetStopped(); st != nil {
				stoppedExp = st
				break
			}
		}
		if stoppedExp == nil || stoppedExp.ErrorCode != 0 || stoppedExp.TotalDelivered != 2 {
			t.Fatalf("unexpected StoppedEvent in exp subtest: %+v", stoppedExp)
		}
		var ackedExp1, ackedExp2 bool
		for _, ac := range srv.getAckCalls() {
			for _, id := range ac.Req.AckIds {
				if id == "ack-exp-1" {
					ackedExp1 = true
				}
				if id == "ack-exp-2" {
					ackedExp2 = true
				}
			}
		}
		if ackedExp1 || !ackedExp2 {
			t.Fatalf("expected only ack-exp-2 acked (ackedExp1=%v ackedExp2=%v)", ackedExp1, ackedExp2)
		}

		// 2. >1000 ack IDs split across multiple Acknowledge RPCs.
		srv.resetCounters()
		const totalBatch = 1005
		rms := make([]*pubsubpb.ReceivedMessage, totalBatch)
		for i := 0; i < totalBatch; i++ {
			rms[i] = &pubsubpb.ReceivedMessage{
				AckId:   fmt.Sprintf("split-ack-%d", i),
				Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("sm-%d", i), Data: []byte("x")},
			}
		}
		srv.mu.Lock()
		srv.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{ReceivedMessages: rms})
			}
			<-stream.Context().Done()
			return nil
		}
		srv.mu.Unlock()

		sessSplit, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession split: %v", err)
		}
		_ = sessSplit.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       srv.addr,
					Subscription:   "projects/p1/subscriptions/sub-split",
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK,
				},
			},
		})
		_, _ = sessSplit.Recv() // Started
		for i := 0; i < totalBatch; i++ {
			ev, err := sessSplit.Recv()
			if err != nil || ev.GetMessageDelivered() == nil {
				t.Fatalf("Recv split msg %d: ev=%+v err=%v", i, ev, err)
			}
		}
		_ = sessSplit.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		for {
			ev, err := sessSplit.Recv()
			if err != nil {
				t.Fatalf("Recv stopped split: %v", err)
			}
			if ev.GetStopped() != nil {
				break
			}
		}
		ackCalls := srv.getAckCalls()
		var totalAcked int
		for _, ac := range ackCalls {
			if len(ac.Req.AckIds) > 1000 {
				t.Fatalf("Acknowledge batch exceeded 1000 IDs: %d", len(ac.Req.AckIds))
			}
			totalAcked += len(ac.Req.AckIds)
		}
		if totalAcked != totalBatch {
			t.Fatalf("expected %d total acked across split RPCs, got %d (%d calls)", totalBatch, totalAcked, len(ackCalls))
		}

		// 3. StreamingPull transient Unavailable recovery followed by Go fatal ResourceExhausted.
		srv.resetCounters()
		srv.mu.Lock()
		srv.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				return status.Error(codes.Unavailable, "transient restart")
			}
			return status.Error(codes.ResourceExhausted, "go fatal resource exhausted")
		}
		srv.mu.Unlock()
		defer func() {
			srv.mu.Lock()
			srv.onStreamPull = nil
			srv.mu.Unlock()
		}()

		sessRec, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession recovery: %v", err)
		}
		_ = sessRec.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:            srv.addr,
					Subscription:        "projects/p1/subscriptions/sub-rec",
					InitialRetryDelayMs: protoInt64(10),
				},
			},
		})
		_, _ = sessRec.Recv() // Started
		evStop, err := sessRec.Recv()
		if err != nil || evStop.GetStopped() == nil {
			t.Fatalf("expected StoppedEvent after fatal ResourceExhausted, got %+v err=%v", evStop, err)
		}
		if evStop.GetStopped().ErrorCode != int32(codes.ResourceExhausted) {
			t.Fatalf("expected ResourceExhausted fatal code on Go profile, got %+v", evStop.GetStopped())
		}
		inits := srv.getStreamInits()
		if len(inits) < 2 || inits[0].Req.ProtocolVersion != 1 || inits[0].Req.StreamAckDeadlineSeconds != 60 {
			t.Fatalf("expected >=2 StreamingPull streams with ProtocolVersion=1, got %+v", inits)
		}
	})
}

// --- 5. All Remaining Profiles & Fault Modes ---

func TestInfra_E2EProxyAllProfilesAndFaultModes(t *testing.T) {
	srv := startMiniPubSubServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("ConformantNoMonAndRustAlias", func(t *testing.T) {
		srv.resetCounters()
		h := startTestProxyDirect(t, "--mode=conformant-rust-nomon")
		defer h.Shutdown(t, 2*time.Second)

		caps, err := h.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil || caps.SupportsMonitoring || caps.SupportsUnaryPull || caps.SupportsPublishCompression || caps.SupportsPublishFlowControl {
			t.Fatalf("unexpected rust no-mon capabilities: err=%v caps=%+v", err, caps)
		}
		if _, err := h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{Endpoint: srv.addr, Subscription: "s"}); status.Code(err) != codes.Unimplemented {
			t.Fatalf("expected Unimplemented on UnaryPull, got %v", err)
		}
		if _, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: srv.addr, Topic: "t", EnableCompression: protoBool(true),
		}); status.Code(err) != codes.Unimplemented {
			t.Fatalf("expected Unimplemented on compression, got %v", err)
		}
		pubResp, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: srv.addr, Topic: "projects/p1/topics/t-rust", EnableMonitoring: true, CountThreshold: protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher: %v", err)
		}
		res, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubResp.PublisherId, Data: []byte("rust-ok"), OrderingKey: "unified-ok"})
		if err != nil || res.ErrorCode != 0 {
			t.Fatalf("rust unified ordering publish failed: err=%v res=%+v", err, res)
		}
		calls := srv.getPublishCalls()
		if len(calls) != 1 || calls[0].MD.Get("x-goog-request-params")[0] != "topic=projects/p1/topics/t-rust" {
			t.Fatalf("expected unescaped routing header in rust mode, got %+v", calls)
		}
		drain, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil || len(drain.Spans) != 0 {
			t.Fatalf("expected 0 traces in no-mon mode, got len=%d err=%v", len(drain.Spans), err)
		}
	})

	t.Run("ConformantPartialUnimplAndCppDeltas", func(t *testing.T) {
		srv.resetCounters()
		h := startTestProxyDirect(t, "--mode=conformant-partial-unimpl")
		defer h.Shutdown(t, 2*time.Second)

		caps, err := h.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil || caps.SupportsUnaryPull || caps.SupportsPublishHedging {
			t.Fatalf("expected partial unimpl flags false, got %+v err=%v", caps, err)
		}
		if _, err := h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{Endpoint: srv.addr, Subscription: "s"}); status.Code(err) != codes.Unimplemented {
			t.Fatalf("expected Unimplemented UnaryPull, got %v", err)
		}

		// C++ partial & divergent-cpp span/behavioral delta checks.
		hCpp := startTestProxyDirect(t, "--mode=divergent-cpp")
		defer hCpp.Shutdown(t, 2*time.Second)

		cppCaps, err := hCpp.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil || cppCaps.ClientLanguage != "cpp" || !cppCaps.SupportsUnaryPull || cppCaps.SupportsPublishHedging {
			t.Fatalf("unexpected divergent-cpp capabilities: %+v err=%v", cppCaps, err)
		}
		pubCpp, err := hCpp.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: srv.addr, Topic: "projects/p1/topics/t-cpp", EnableMonitoring: true, CountThreshold: protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher cpp: %v", err)
		}
		if _, err := hCpp.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubCpp.PublisherId, Data: []byte("cpp")}); err != nil {
			t.Fatalf("Publish cpp: %v", err)
		}
		drain, err := hCpp.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces cpp: %v", err)
		}
		foundClientPublish, foundEnvelope := false, false
		for _, sp := range drain.Spans {
			if sp.Name == "t-cpp publish" && sp.Kind == proxypb.SpanKind_SPAN_KIND_CLIENT {
				foundClientPublish = true
			}
			if _, ok := sp.Attributes["messaging.message.envelope.size"]; ok {
				foundEnvelope = true
			}
		}
		if !foundClientPublish || !foundEnvelope {
			t.Fatalf("expected C++ OTel span deltas (CLIENT publish=%v envelope.size=%v)", foundClientPublish, foundEnvelope)
		}
	})

	t.Run("MissingWrongAndOutOfOrderRPCModes", func(t *testing.T) {
		// missing-rpc: claims OK with zero server RPCs.
		srv.resetCounters()
		hMissing := startTestProxyDirect(t, "--mode=missing-rpc")
		pubM, err := hMissing.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: srv.addr, Topic: "projects/p1/topics/t", CountThreshold: protoInt64(1)})
		if err != nil {
			t.Fatalf("CreatePublisher missing-rpc: %v", err)
		}
		resM, err := hMissing.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubM.PublisherId, Data: []byte("a")})
		if err != nil || resM.ErrorCode != 0 || resM.MessageId == "" {
			t.Fatalf("missing-rpc Publish: err=%v res=%+v", err, resM)
		}
		if len(srv.getPublishCalls()) != 0 {
			t.Fatalf("missing-rpc unexpectedly sent Publish RPC")
		}
		_ = hMissing.Shutdown(t, 2*time.Second)

		// wrong-rpc: mutates payload and adds attribute.
		srv.resetCounters()
		hWrong := startTestProxyDirect(t, "--mode=wrong-rpc")
		pubW, _ := hWrong.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: srv.addr, Topic: "projects/p1/topics/t", CountThreshold: protoInt64(1)})
		_, _ = hWrong.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubW.PublisherId, Data: []byte("clean")})
		callsW := srv.getPublishCalls()
		if len(callsW) != 1 || string(callsW[0].Req.Messages[0].Data) == "clean" {
			t.Fatalf("expected mutated payload in wrong-rpc, got %+v", callsW)
		}
		_ = hWrong.Shutdown(t, 2*time.Second)

		// out-of-order-rpc: reverses batch message order.
		srv.resetCounters()
		hOOO := startTestProxyDirect(t, "--mode=out-of-order-rpc")
		pubO, _ := hOOO.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: srv.addr, Topic: "projects/p1/topics/t", EnableMessageOrdering: protoBool(true), CountThreshold: protoInt64(2), DelayThresholdMs: protoInt64(5000),
		})
		a1, _ := hOOO.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubO.PublisherId, Data: []byte("first"), OrderingKey: "k", AsyncFire: true})
		a2, _ := hOOO.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubO.PublisherId, Data: []byte("second"), OrderingKey: "k", AsyncFire: true})
		_, _ = hOOO.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubO.PublisherId, PublishToken: a1.PublishToken})
		_, _ = hOOO.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: pubO.PublisherId, PublishToken: a2.PublishToken})
		callsO := srv.getPublishCalls()
		if len(callsO) != 1 || len(callsO[0].Req.Messages) != 2 || string(callsO[0].Req.Messages[0].Data) != "second" {
			t.Fatalf("expected reversed order in out-of-order-rpc, got %+v", callsO)
		}
		_ = hOOO.Shutdown(t, 2*time.Second)
	})

	t.Run("MissingAndInvalidTracesModes", func(t *testing.T) {
		srv.resetCounters()
		hMissT := startTestProxyDirect(t, "--mode=missing-traces")
		pubMT, _ := hMissT.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: srv.addr, Topic: "projects/p1/topics/t", EnableMonitoring: true, CountThreshold: protoInt64(1)})
		_, _ = hMissT.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubMT.PublisherId, Data: []byte("ok")})
		if len(srv.getPublishCalls()) != 1 {
			t.Fatalf("missing-traces did not send valid Publish RPC")
		}
		drainMT, err := hMissT.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil || len(drainMT.Spans) != 0 {
			t.Fatalf("expected 0 spans in missing-traces, got %d err=%v", len(drainMT.Spans), err)
		}
		_ = hMissT.Shutdown(t, 2*time.Second)

		srv.resetCounters()
		hInvT := startTestProxyDirect(t, "--mode=invalid-traces")
		pubIT, _ := hInvT.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: srv.addr, Topic: "projects/p1/topics/t", EnableMonitoring: true, CountThreshold: protoInt64(1)})
		_, _ = hInvT.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubIT.PublisherId, Data: []byte("ok")})
		callsIT := srv.getPublishCalls()
		if len(callsIT) != 1 || callsIT[0].Req.Messages[0].Attributes["googclient_traceparent"] != "" {
			t.Fatalf("expected dropped googclient_traceparent in invalid-traces, got %+v", callsIT)
		}
		drainIT, err := hInvT.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil || len(drainIT.Spans) == 0 {
			t.Fatalf("expected malformed spans in invalid-traces, got %d err=%v", len(drainIT.Spans), err)
		}
		for _, sp := range drainIT.Spans {
			if sp.Name != "bad.span" || sp.Kind != proxypb.SpanKind_SPAN_KIND_CLIENT || len(sp.Links) != 0 {
				t.Fatalf("expected invalid trace mutations on span, got %+v", sp)
			}
		}
		_ = hInvT.Shutdown(t, 2*time.Second)
	})
}

// --- 6. Orphan Process Leak Verifier Tests ---

type recordingTB struct {
	testing.TB
	mu     sync.Mutex
	errors []string
	fatals []string
}

func (r *recordingTB) Helper() {}
func (r *recordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}
func (r *recordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
	r.mu.Unlock()
}

func TestInfra_OrphanProxyDetection(t *testing.T) {
	marker := newRunMarker(t)

	// Negative check: spawn a real e2e-test-proxy carrying marker and verify assertNoOrphanProxyTB detects it.
	cmd := exec.Command(proxyBinaryPath, "--port=0", "--mode=ignore-shutdown", "--marker="+marker)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child proxy: %v", err)
	}
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	rec := &recordingTB{TB: t}
	assertNoOrphanProxyTB(rec, marker)
	if len(rec.errors) == 0 {
		t.Fatalf("expected assertNoOrphanProxyTB to report orphan process for marker %q", marker)
	}

	// Positive check: kill child and verify assertNoOrphanProxy passes cleanly.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = cmd.Wait()
	assertNoOrphanProxy(t, marker)

	// Suffix-identifier boundary check: a live child with <marker>-suffix must NOT match <marker>.
	if hasExactMarkerToken("--marker="+marker+"-suffix", marker) {
		t.Fatalf("hasExactMarkerToken falsely matched hyphen-suffixed marker")
	}
	if hasExactMarkerToken("--marker="+marker+"x", marker) {
		t.Fatalf("hasExactMarkerToken falsely matched alphanumeric-suffixed marker")
	}
	suffixMarker := marker + "-suffix"
	cmdSuffix := exec.Command(proxyBinaryPath, "--port=0", "--mode=ignore-shutdown", "--marker="+suffixMarker)
	if err := cmdSuffix.Start(); err != nil {
		t.Fatalf("start suffix child proxy: %v", err)
	}
	defer func() {
		if cmdSuffix.Process != nil {
			_ = cmdSuffix.Process.Kill()
			_ = cmdSuffix.Wait()
		}
	}()
	if suffixMatches, err := findProcessesWithMarker(suffixMarker); err != nil || len(suffixMatches) != 1 || suffixMatches[0].PID != cmdSuffix.Process.Pid {
		t.Fatalf("expected single match for suffixMarker %q (pid=%d), got %+v err=%v", suffixMarker, cmdSuffix.Process.Pid, suffixMatches, err)
	}
	if baseMatches, err := findProcessesWithMarker(marker); err != nil || len(baseMatches) != 0 {
		t.Fatalf("base marker %q falsely matched suffixed child %q: %+v err=%v", marker, suffixMarker, baseMatches, err)
	}
	assertNoOrphanProxy(t, marker)
	if err := cmdSuffix.Process.Kill(); err != nil {
		t.Fatalf("kill suffix child: %v", err)
	}
	_ = cmdSuffix.Wait()
	assertNoOrphanProxy(t, suffixMarker)

	// Self-exclusion check: own PID and ancestors must never be reported.
	excluded := collectSelfAndAncestorPIDs()
	if !excluded[os.Getpid()] {
		t.Fatalf("expected own PID %d in excluded set", os.Getpid())
	}
	if matches, err := findProcessesWithMarker("TestInfra_OrphanProxyDetection"); err != nil {
		t.Fatalf("findProcessesWithMarker: %v", err)
	} else {
		for _, m := range matches {
			if m.PID == os.Getpid() {
				t.Fatalf("self PID %d leaked into findProcessesWithMarker results", os.Getpid())
			}
		}
	}
}

// --- 7. JSON & Markdown Report Parser Tests ---

func TestInfra_ReportParsers(t *testing.T) {
	singleJSON := []byte(`{
	  "capabilities": {
	    "client_language": "go",
	    "client_library": "cloud.google.com/go/pubsub/v2",
	    "supports_monitoring": true
	  },
	  "summary": {
	    "total": 4,
	    "pass": 1,
	    "fail": 1,
	    "skipped_no_monitoring": 1,
	    "unsupported_by_client": 1
	  },
	  "results": [
	    {"id": "PUB-01", "category": "publish", "status": "PASS"},
	    {"scenario_id": "PUB-02", "status": "FAIL", "error_message": "bad payload"},
	    {"id": "TRAC-01", "status": "SKIPPED_NO_MONITORING"},
	    {"id": "PULL-04", "status": "UNSUPPORTED_BY_CLIENT"}
	  ]
	}`)
	rep, err := ParseJSONReport(singleJSON)
	if err != nil {
		t.Fatalf("ParseJSONReport single: %v", err)
	}
	AssertSummaryMatchesScenarios(t, rep)
	if rep.Capabilities == nil || rep.Capabilities.SupportsMonitoring == nil || !*rep.Capabilities.SupportsMonitoring {
		t.Fatalf("unexpected capabilities: %+v", rep.Capabilities)
	}
	if sc, ok := rep.FindScenario("PUB-02"); !ok || sc.Error != "bad payload" || sc.Status != StatusFail {
		t.Fatalf("FindScenario(PUB-02) = %+v, %v", sc, ok)
	}

	multiJSON := []byte(`{
	  "targets": [
	    {
	      "target": "go",
	      "capabilities": {"client_language": "go", "supports_monitoring": true},
	      "scenarios": [{"id": "TRAC-01", "status": "PASS"}]
	    },
	    {
	      "target": "rust",
	      "capabilities": {"client_language": "rust", "supports_monitoring": false},
	      "scenarios": [{"id": "TRAC-01", "status": "SKIPPED_NO_MONITORING"}]
	    }
	  ]
	}`)
	multi, err := ParseJSONReport(multiJSON)
	if err != nil {
		t.Fatalf("ParseJSONReport multi: %v", err)
	}
	if len(multi.Targets) != 2 || len(multi.Differences) != 1 || multi.Differences[0].ScenarioID != "TRAC-01" {
		t.Fatalf("expected synthesized diff for TRAC-01 across 2 targets, got %+v", multi)
	}

	if _, err := ParseJSONReport([]byte(`{not-json`)); err == nil {
		t.Fatalf("expected error on malformed JSON")
	}

	mdStr := "# Conformance Report\n\n| Scenario | go | rust |\n| --- | --- | --- |\n| **TRAC-01** | `PASS` | `SKIPPED_NO_MONITORING` |\n"
	md, err := ParseMarkdownReport(mdStr)
	if err != nil {
		t.Fatalf("ParseMarkdownReport: %v", err)
	}
	if !md.HasHeadingContaining("conformance report") {
		t.Fatalf("expected heading match")
	}
	if !md.HasTableRowContaining("TRAC-01", "PASS", "SKIPPED_NO_MONITORING") {
		t.Fatalf("expected table row match, got %+v", md.Tables)
	}
	if !md.ContainsTokens("TRAC-01", "SKIPPED_NO_MONITORING") {
		t.Fatalf("expected ContainsTokens match")
	}
}

// --- Helpers & Trace Assertion Utilities ---

type pbTelemetryAlias = pubsubpb.PubsubClientTelemetry

func protoInt64(v int64) *int64       { return &v }
func protoInt32(v int32) *int32       { return &v }
func protoBool(v bool) *bool          { return &v }
func protoFloat64(v float64) *float64 { return &v }

func assertCanonicalGoPublishSpans(t *testing.T, spans []*proxypb.TraceSpan, topicID, msgID string) {
	t.Helper()
	var createSpan, fcSpan, batchSpan, pubSpan *proxypb.TraceSpan
	for _, s := range spans {
		switch s.Name {
		case topicID + " create":
			createSpan = s
		case "publisher flow control":
			fcSpan = s
		case "publisher batching":
			batchSpan = s
		case topicID + " publish":
			pubSpan = s
		}
	}
	if createSpan == nil || fcSpan == nil || batchSpan == nil || pubSpan == nil {
		t.Fatalf("missing canonical Go publish spans (create=%v fc=%v batch=%v pub=%v)", createSpan != nil, fcSpan != nil, batchSpan != nil, pubSpan != nil)
	}
	if createSpan.Kind != proxypb.SpanKind_SPAN_KIND_PRODUCER ||
		fcSpan.Kind != proxypb.SpanKind_SPAN_KIND_INTERNAL ||
		batchSpan.Kind != proxypb.SpanKind_SPAN_KIND_INTERNAL ||
		pubSpan.Kind != proxypb.SpanKind_SPAN_KIND_INTERNAL {
		t.Fatalf("wrong SpanKind in publish spans")
	}
	if createSpan.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
		t.Fatalf("expected UNSET status on success, got %v", createSpan.StatusCode)
	}
	if createSpan.InstrumentationScopeName != "cloud.google.com/go/pubsub/v2" {
		t.Fatalf("unexpected scope name: %q", createSpan.InstrumentationScopeName)
	}
	if fcSpan.ParentSpanId != createSpan.SpanId || batchSpan.ParentSpanId != createSpan.SpanId || pubSpan.ParentSpanId != "" {
		t.Fatalf("invalid parent-child span hierarchy")
	}
	if createSpan.Attributes["messaging.message.id"].GetStringValue() != msgID {
		t.Fatalf("missing/wrong messaging.message.id on createSpan")
	}
	if len(createSpan.Links) != 1 || createSpan.Links[0].SpanId != pubSpan.SpanId ||
		createSpan.Links[0].Attributes["messaging.operation.name"].GetStringValue() != "publish" {
		t.Fatalf("missing create->publish SpanLink: %+v", createSpan.Links)
	}
	if len(pubSpan.Links) != 1 || pubSpan.Links[0].SpanId != createSpan.SpanId {
		t.Fatalf("missing publish->create SpanLink: %+v", pubSpan.Links)
	}
}

func assertCanonicalGoSubscribeSpans(t *testing.T, spans []*proxypb.TraceSpan, subID, expectedTraceID string) {
	t.Helper()
	var subSpans []*proxypb.TraceSpan
	var procSpans []*proxypb.TraceSpan
	var ccCount, schedCount, modAckCount, ackCount int
	for _, s := range spans {
		switch s.Name {
		case subID + " subscribe":
			subSpans = append(subSpans, s)
		case subID + " process":
			procSpans = append(procSpans, s)
		case "subscriber concurrency control":
			ccCount++
		case "subscribe scheduler":
			schedCount++
		case subID + " modack":
			modAckCount++
			if !s.Attributes["messaging.gcp_pubsub.is_receipt_modack"].GetBoolValue() {
				t.Fatalf("expected is_receipt_modack=true on receipt modack")
			}
			if s.Kind != proxypb.SpanKind_SPAN_KIND_INTERNAL || s.ParentSpanId != "" {
				t.Fatalf("modack span must be detached INTERNAL root")
			}
		case subID + " ack":
			ackCount++
			if s.Kind != proxypb.SpanKind_SPAN_KIND_INTERNAL || s.ParentSpanId != "" {
				t.Fatalf("ack span must be detached INTERNAL root")
			}
		}
	}
	if len(subSpans) != 3 || len(procSpans) != 3 || ccCount != 3 || schedCount != 3 || modAckCount == 0 || ackCount == 0 {
		t.Fatalf("unexpected subscribe span counts: sub=%d proc=%d cc=%d sched=%d modack=%d ack=%d", len(subSpans), len(procSpans), ccCount, schedCount, modAckCount, ackCount)
	}
	foundParented := false
	for _, s := range subSpans {
		if s.Kind != proxypb.SpanKind_SPAN_KIND_INTERNAL {
			t.Fatalf("subscribe span must be INTERNAL, got %v", s.Kind)
		}
		if s.TraceId == expectedTraceID && s.ParentSpanId != "" {
			foundParented = true
		}
	}
	if !foundParented {
		t.Fatalf("expected at least one subscribe span parented to published traceID %s", expectedTraceID)
	}
	for _, p := range procSpans {
		if p.Kind != proxypb.SpanKind_SPAN_KIND_CONSUMER {
			t.Fatalf("process span must be CONSUMER, got %v", p.Kind)
		}
		if p.Attributes["messaging.operation.type"].GetStringValue() != "process" {
			t.Fatalf("process span missing messaging.operation.type=process")
		}
	}
}

// startCustomProxyDirect starts any compiled e2e-test-proxy binary with optional
// extra environment variables (e.g. GORACE=halt_on_error=1) and cleanly waits
// for stdout draining before cmd.Wait().
func startCustomProxyDirect(t *testing.T, binPath string, extraEnv []string, extraArgs ...string) *DirectProxyHandle {
	t.Helper()
	marker := newRunMarker(t)
	args := append([]string{"--port=0", "--marker=" + marker}, extraArgs...)
	cmd := exec.Command(binPath, args...)
	cmd.Dir = repoRootDir
	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), extraEnv...)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("startCustomProxyDirect StdoutPipe: %v", err)
	}
	stderrBuf := &syncBuffer{}
	stdoutBuf := &syncBuffer{}
	cmd.Stderr = stderrBuf
	if err := cmd.Start(); err != nil {
		t.Fatalf("startCustomProxyDirect Start: %v", err)
	}

	waitCh := make(chan error, 1)
	readyCh := make(chan string, 1)
	readyErrCh := make(chan error, 1)
	readDone := make(chan struct{})

	go func() {
		defer close(readDone)
		reader := bufio.NewReader(io.TeeReader(stdoutPipe, stdoutBuf))
		line, err := reader.ReadString('\n')
		if err != nil {
			readyErrCh <- fmt.Errorf("read READY failed: %w (line=%q stderr=%q)", err, line, stderrBuf.String())
			_, _ = io.Copy(io.Discard, reader)
			return
		}
		readyCh <- line
		_, _ = io.Copy(io.Discard, reader)
	}()

	go func() {
		<-readDone
		waitCh <- cmd.Wait()
	}()

	var readyLine string
	select {
	case readyLine = <-readyCh:
	case err := <-readyErrCh:
		_ = cmd.Process.Kill()
		<-waitCh
		t.Fatalf("startCustomProxyDirect: %v", err)
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-waitCh
		t.Fatalf("startCustomProxyDirect: timed out waiting for READY (stderr=%q)", stderrBuf.String())
	}

	addr, ok := parseReadyLine(readyLine)
	if !ok {
		_ = cmd.Process.Kill()
		<-waitCh
		t.Fatalf("startCustomProxyDirect: malformed READY line %q (stderr=%q)", readyLine, stderrBuf.String())
	}

	conn, err := grpc.NewClient(
		"passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(32<<20),
			grpc.MaxCallSendMsgSize(32<<20),
		),
	)
	if err != nil {
		_ = cmd.Process.Kill()
		<-waitCh
		t.Fatalf("startCustomProxyDirect grpc.NewClient(%q): %v", addr, err)
	}

	h := &DirectProxyHandle{
		Cmd:    cmd,
		Addr:   addr,
		Marker: marker,
		Conn:   conn,
		Client: proxypb.NewPubSubProxyClient(conn),
		stdout: stdoutBuf,
		stderr: stderrBuf,
		waitCh: waitCh,
	}
	t.Cleanup(func() {
		if h.closed.Load() {
			return
		}
		_ = h.Conn.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGKILL)
		}
		select {
		case <-h.waitCh:
		case <-time.After(2 * time.Second):
		}
		killProcessesWithMarker(h.Marker)
	})
	return h
}

// --- 8. Iteration 2 Regression Tests ---

func TestInfra_NackImmediatelyStressAndBlockedAckAndRaceProxy(t *testing.T) {
	raceBin := buildRaceProxyBinary(t)

	mock := startMiniPubSubServer(t)
	h := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-go")

	t.Run("BlockedAckNackImmediately25x", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		for iter := 0; iter < 25; iter++ {
			mock.resetCounters()
			ackEntered := make(chan struct{}, 4)
			ackRelease := make(chan struct{})
			var releaseOnce sync.Once
			releaseAck := func() { releaseOnce.Do(func() { close(ackRelease) }) }
			defer releaseAck()

			mock.mu.Lock()
			mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
				select {
				case ackEntered <- struct{}{}:
				default:
				}
				select {
				case <-ackRelease:
					return nil
				case <-actx.Done():
					return actx.Err()
				}
			}
			mock.onModAck = nil
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: fmt.Sprintf("a-%d-1", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("m-%d-1", iter), Data: []byte("1")}},
							{AckId: fmt.Sprintf("a-%d-2", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("m-%d-2", iter), Data: []byte("2")}},
							{AckId: fmt.Sprintf("a-%d-3", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("m-%d-3", iter), Data: []byte("3")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("iter %d SubscriberSession: %v", iter, err)
			}
			err = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:         mock.addr,
						Subscription:     fmt.Sprintf("projects/p/subscriptions/sub-nack-%d", iter),
						EnableMonitoring: true,
						AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			})
			if err != nil {
				t.Fatalf("iter %d Start: %v", iter, err)
			}

			evCh := make(chan *proxypb.SubscriberEvent, 16)
			recvErrCh := make(chan error, 1)
			go func() {
				for {
					ev, rerr := sess.Recv()
					if rerr != nil {
						recvErrCh <- rerr
						return
					}
					evCh <- ev
				}
			}()

			var firstToken string
			deadline := time.After(5 * time.Second)
			for firstToken == "" {
				select {
				case ev := <-evCh:
					if md := ev.GetMessageDelivered(); md != nil {
						firstToken = md.DeliveryToken
					}
				case <-deadline:
					t.Fatalf("iter %d timed out waiting for first MessageDeliveredEvent", iter)
				}
			}

			// Trigger blocked ACK_WITH_RESULT and immediately stop with NACK_IMMEDIATELY.
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{
						DeliveryToken: firstToken,
						Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
					},
				},
			}); err != nil {
				t.Fatalf("iter %d Settle: %v", iter, err)
			}

			select {
			case <-ackEntered:
			case <-time.After(300 * time.Millisecond):
			}

			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{
						ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY,
					},
				},
			}); err != nil {
				t.Fatalf("iter %d Stop: %v", iter, err)
			}
			_ = sess.CloseSend()

			sawStopped := false
			drainDone := false
			stopWait := time.After(5 * time.Second)
			for !drainDone {
				select {
				case ev := <-evCh:
					if ev.GetStopped() != nil {
						if sawStopped {
							t.Fatalf("iter %d duplicate StoppedEvent", iter)
						}
						sawStopped = true
					} else if sawStopped {
						t.Fatalf("iter %d leaked post-StoppedEvent event: %+v", iter, ev)
					}
				case rerr := <-recvErrCh:
					if rerr != io.EOF {
						t.Fatalf("iter %d unexpected recv error: %v", iter, rerr)
					}
					for len(evCh) > 0 {
						ev := <-evCh
						if ev.GetStopped() != nil {
							sawStopped = true
						} else if sawStopped {
							t.Fatalf("iter %d leaked post-StoppedEvent buffered event: %+v", iter, ev)
						}
					}
					drainDone = true
				case <-stopWait:
					t.Fatalf("iter %d NACK_IMMEDIATELY stalled while Acknowledge was blocked", iter)
				}
			}
			if !sawStopped {
				t.Fatalf("iter %d did not receive StoppedEvent", iter)
			}
			releaseAck()
		}
	})

	t.Run("RaceProxyPeriodicLeaseAndDrainTraces", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		mock.resetCounters()
		modackSeen := make(chan struct{}, 2)

		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
			if req.AckDeadlineSeconds > 0 {
				select {
				case modackSeen <- struct{}{}:
				default:
				}
			}
			return nil
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "lease-a1", Message: &pubsubpb.PubsubMessage{MessageId: "lease-m1", Data: []byte("abc")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:                     mock.addr,
					Subscription:                 "projects/p/subscriptions/sub-lease-race",
					EnableMonitoring:             true,
					AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					MaxDurationPerAckExtensionMs: protoInt64(200),
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for delivery and at least one periodic extension modack.
		gotDeliv := false
		for !gotDeliv {
			ev, err := sess.Recv()
			if err != nil {
				t.Fatalf("Recv: %v", err)
			}
			if ev.GetMessageDelivered() != nil {
				gotDeliv = true
			}
		}
		select {
		case <-modackSeen:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for periodic modack under -race")
		}

		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{
					ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY,
				},
			},
		}); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		_ = sess.CloseSend()
		for {
			ev, rerr := sess.Recv()
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				t.Fatalf("Recv during stop: %v", rerr)
			}
			_ = ev
		}
		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil || len(drained.Spans) == 0 {
			t.Fatalf("DrainTraces failed or empty: err=%v spans=%d", err, len(drained.Spans))
		}
	})

	t.Run("OrderedExpiryBehindPrevCh20x", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		for iter := 0; iter < 20; iter++ {
			mock.resetCounters()
			_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = nil
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
							MessageOrderingEnabled: true,
						},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: fmt.Sprintf("oe-%d-1", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("moe-%d-1", iter), OrderingKey: "k-oe", Data: []byte("1")}},
							{AckId: fmt.Sprintf("oe-%d-2", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("moe-%d-2", iter), OrderingKey: "k-oe", Data: []byte("2")}},
							{AckId: fmt.Sprintf("oe-%d-3", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("moe-%d-3", iter), OrderingKey: "k-oe", Data: []byte("3")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("iter %d SubscriberSession: %v", iter, err)
			}
			subName := fmt.Sprintf("sub-oe-race-%d", iter)
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:         mock.addr,
						Subscription:     "projects/p/subscriptions/" + subName,
						EnableMonitoring: true,
						MaxExtensionMs:   protoInt64(55),
						AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				t.Fatalf("iter %d Start: %v", iter, err)
			}

			evCh := make(chan *proxypb.SubscriberEvent, 8)
			recvErr := make(chan error, 1)
			go func() {
				for {
					ev, rerr := sess.Recv()
					if rerr != nil {
						recvErr <- rerr
						return
					}
					evCh <- ev
				}
			}()

			deliveredCount := 0
			gotFirst := false
			for !gotFirst {
				select {
				case ev := <-evCh:
					if ev.GetMessageDelivered() != nil {
						deliveredCount++
						gotFirst = true
					}
				case rerr := <-recvErr:
					t.Fatalf("iter %d unexpected recv before delivery: %v (stderr=%q)", iter, rerr, h.Stderr())
				case <-time.After(5 * time.Second):
					t.Fatalf("iter %d timed out waiting for first delivery", iter)
				}
			}

			// Wait past MaxExtensionMs=55ms without settling so all 3 expire (2 behind prevCh).
			time.Sleep(130 * time.Millisecond)

			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			}); err != nil {
				t.Fatalf("iter %d Stop: %v", iter, err)
			}
			_ = sess.CloseSend()

			var stopped *proxypb.StoppedEvent
			drainDone := false
			for !drainDone {
				select {
				case ev := <-evCh:
					if ev.GetMessageDelivered() != nil {
						deliveredCount++
					}
					if st := ev.GetStopped(); st != nil {
						stopped = st
					}
				case rerr := <-recvErr:
					if rerr != io.EOF {
						t.Fatalf("iter %d Recv: %v (stderr=%q)", iter, rerr, h.Stderr())
					}
					for len(evCh) > 0 {
						ev := <-evCh
						if ev.GetMessageDelivered() != nil {
							deliveredCount++
						}
						if st := ev.GetStopped(); st != nil {
							stopped = st
						}
					}
					drainDone = true
				case <-time.After(5 * time.Second):
					t.Fatalf("iter %d timed out waiting for stop", iter)
				}
			}
			if stopped == nil || stopped.TotalDelivered != 1 || deliveredCount != 1 {
				t.Fatalf("iter %d expected TotalDelivered=1, stopped=%+v delivered=%d", iter, stopped, deliveredCount)
			}

			drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("iter %d DrainTraces: %v", iter, err)
			}
			var subCnt, procCnt int
			for _, sp := range drained.Spans {
				if sp.Name == subName+" subscribe" {
					subCnt++
					if sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "expired" {
						t.Fatalf("iter %d expected expired result, got %+v", iter, sp.Attributes)
					}
				}
				if sp.Name == subName+" process" {
					procCnt++
				}
			}
			if subCnt != 3 || procCnt != 1 {
				t.Fatalf("iter %d expected 3 subscribe and 1 process span, got sub=%d proc=%d", iter, subCnt, procCnt)
			}
		}
	})

	if err := h.Shutdown(t, 4*time.Second); err != nil {
		t.Fatalf("-race proxy exited with error (stderr=%q): %v", h.Stderr(), err)
	}
	if strings.Contains(h.Stderr(), "DATA RACE") {
		t.Fatalf("DATA RACE detected in stderr:\n%s", h.Stderr())
	}
}

func TestInfra_SubscriberConcurrencyOrderingShutdownAndFC(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("MaxConcurrencyAutoAckWithResultAndAbort", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)

		mock.resetCounters()
		var inFlightAck atomic.Int32
		var maxInFlightAck atomic.Int32
		mock.mu.Lock()
		mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
			cur := inFlightAck.Add(1)
			for {
				prev := maxInFlightAck.Load()
				if cur <= prev || maxInFlightAck.CompareAndSwap(prev, cur) {
					break
				}
			}
			defer inFlightAck.Add(-1)
			select {
			case <-time.After(30 * time.Millisecond):
			case <-actx.Done():
			}
			return nil
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "c1", Message: &pubsubpb.PubsubMessage{MessageId: "mc1", Data: []byte("1")}},
						{AckId: "c2", Message: &pubsubpb.PubsubMessage{MessageId: "mc2", Data: []byte("2")}},
						{AckId: "c3", Message: &pubsubpb.PubsubMessage{MessageId: "mc3", Data: []byte("3")}},
						{AckId: "c4", Message: &pubsubpb.PubsubMessage{MessageId: "mc4", Data: []byte("4")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       mock.addr,
					Subscription:   "projects/p/subscriptions/sub-conc",
					MaxConcurrency: protoInt32(1),
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var delivered, settled int
		for delivered < 4 || settled < 4 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				delivered++
				if md.ActiveCallbacks > 1 {
					t.Fatalf("MaxConcurrency=1 violated during AUTO_ACK_WITH_RESULT: ActiveCallbacks=%d", md.ActiveCallbacks)
				}
			}
			if ev.GetSettlementResult() != nil {
				settled++
			}
		}
		if maxInFlightAck.Load() > 1 {
			t.Fatalf("expected sequential settlement under MaxConcurrency=1, maxInFlightAck=%d", maxInFlightAck.Load())
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}
	})

	t.Run("FcAcquireCancellationBulkNacksAndNoOrphanCCSpans", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = nil
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "fc-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-fc-1", Data: []byte("1")}},
						{AckId: "fc-2", Message: &pubsubpb.PubsubMessage{MessageId: "m-fc-2", Data: []byte("2")}},
						{AckId: "fc-3", Message: &pubsubpb.PubsubMessage{MessageId: "m-fc-3", Data: []byte("3")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:               mock.addr,
					Subscription:           "projects/p/subscriptions/sub-fc-cancel",
					EnableMonitoring:       true,
					MaxOutstandingMessages: protoInt64(1),
					AutoSettleMode:         proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for first message; 2nd is blocked inside fc.acquire.
		for {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv: %v", rerr)
			}
			if ev.GetMessageDelivered() != nil {
				break
			}
		}

		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{
					ShutdownBehavior:  proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING,
					ShutdownTimeoutMs: protoInt64(0),
				},
			},
		}); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}

		nacked := map[string]bool{}
		for _, c := range mock.getModAckCalls() {
			if c.Req.AckDeadlineSeconds == 0 {
				for _, id := range c.Req.AckIds {
					nacked[id] = true
				}
			}
		}
		for _, want := range []string{"fc-1", "fc-2", "fc-3"} {
			if !nacked[want] {
				t.Fatalf("expected shutdown to nack remaining ackID %q; got %+v", want, nacked)
			}
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		var subSpans, ccSpans int
		for _, sp := range drained.Spans {
			if sp.EndTime == nil {
				t.Fatalf("span %q missing EndTime (orphan)", sp.Name)
			}
			if sp.Name == "subscriber concurrency control" {
				ccSpans++
			}
			if sp.Name == "sub-fc-cancel subscribe" {
				subSpans++
				if sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "nacked" {
					t.Fatalf("expected subscribe span result=nacked on shutdown nack, got %+v", sp.Attributes)
				}
			}
		}
		if subSpans != 3 || ccSpans != 2 {
			t.Fatalf("expected 3 subscribe spans and 2 concurrency control spans (for fc-1 and blocked fc-2), got sub=%d cc=%d", subSpans, ccSpans)
		}
	})

	t.Run("MsgOrderingSubscriptionPropertiesAndExpiredGuard", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)

		// 1) MessageOrderingEnabled: false -> ordered messages deliver concurrently.
		mock.resetCounters()
		mock.mu.Lock()
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						MessageOrderingEnabled: false,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "uo-1", Message: &pubsubpb.PubsubMessage{MessageId: "muo-1", OrderingKey: "k1", Data: []byte("1")}},
						{AckId: "uo-2", Message: &pubsubpb.PubsubMessage{MessageId: "muo-2", OrderingKey: "k1", Data: []byte("2")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       mock.addr,
					Subscription:   "projects/p/subscriptions/sub-unord",
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		var delivs []*proxypb.MessageDeliveredEvent
		for len(delivs) < 2 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv unord: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				delivs = append(delivs, md)
			}
		}
		for _, md := range delivs {
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{DeliveryToken: md.DeliveryToken, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK},
				},
			})
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}

		// 2) MessageOrderingEnabled: true + MaxExtensionMs -> 2nd expires while waiting on prevCh,
		// so 2nd MUST NOT emit MessageDeliveredEvent or process span.
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		mock.resetCounters()
		mock.mu.Lock()
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						MessageOrderingEnabled: true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "ord-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-ord-1", OrderingKey: "k-exp", Data: []byte("1")}},
						{AckId: "ord-2", Message: &pubsubpb.PubsubMessage{MessageId: "m-ord-2", OrderingKey: "k-exp", Data: []byte("2")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess2, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession 2: %v", err)
		}
		if err := sess2.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         mock.addr,
					Subscription:     "projects/p/subscriptions/sub-ord-exp",
					EnableMonitoring: true,
					MaxExtensionMs:   protoInt64(140),
					AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start 2: %v", err)
		}

		evCh2 := make(chan *proxypb.SubscriberEvent, 8)
		go func() {
			for {
				ev, rerr := sess2.Recv()
				if rerr != nil {
					close(evCh2)
					return
				}
				evCh2 <- ev
			}
		}()

		deliveredCount := 0
		for ev := range evCh2 {
			if ev.GetMessageDelivered() != nil {
				deliveredCount++
				// Wait past MaxExtensionMs without settling ord-1, then close after ord-1 & ord-2 expire.
				go func() {
					time.Sleep(280 * time.Millisecond)
					_ = sess2.Send(&proxypb.SubscriberCommand{
						Command: &proxypb.SubscriberCommand_Stop{
							Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
						},
					})
					_ = sess2.CloseSend()
				}()
			}
			if st := ev.GetStopped(); st != nil && st.TotalDelivered != 1 {
				t.Fatalf("expected TotalDelivered=1 (2nd expired while waiting on prevCh), got %d", st.TotalDelivered)
			}
		}
		if deliveredCount != 1 {
			t.Fatalf("expected exactly 1 MessageDeliveredEvent (expired ord-2 skipped), got %d", deliveredCount)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		var subCnt, procCnt int
		for _, sp := range drained.Spans {
			if sp.Name == "sub-ord-exp subscribe" {
				subCnt++
				if sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "expired" {
					t.Fatalf("expected expired subscribe result, got %+v", sp.Attributes)
				}
			}
			if sp.Name == "sub-ord-exp process" {
				procCnt++
			}
		}
		if subCnt != 2 || procCnt != 1 {
			t.Fatalf("expected 2 subscribe spans and 1 process span, got sub=%d proc=%d", subCnt, procCnt)
		}
	})

	t.Run("UnsupportedAckWithResultAndCustomRetryCodes", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go", "--unsupported=ack-with-result,custom-retry-codes")
		defer h.Shutdown(t, 2*time.Second)

		mock.resetCounters()
		mock.mu.Lock()
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "u-1", Message: &pubsubpb.PubsubMessage{MessageId: "mu-1", Data: []byte("x")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		// 1) UnaryPull rejects ACK_WITH_RESULT and custom RetryCodes.
		_, err := h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
			Endpoint:     mock.addr,
			Subscription: "projects/p/subscriptions/sub-u",
			SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
		})
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("UnaryPull(ACK_WITH_RESULT) expected Unimplemented, got %v", err)
		}
		_, err = h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
			Endpoint:     mock.addr,
			Subscription: "projects/p/subscriptions/sub-u",
			RetryCodes:   []int32{int32(codes.Unavailable)},
		})
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("UnaryPull(RetryCodes) expected Unimplemented, got %v", err)
		}

		// 2) StreamingPull rejects AUTO_ACK_WITH_RESULT and custom RetryCodes.
		s1, _ := h.Client.SubscriberSession(ctx)
		_ = s1.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       mock.addr,
					Subscription:   "projects/p/subscriptions/sub-u",
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT,
				},
			},
		})
		if _, err := s1.Recv(); status.Code(err) != codes.Unimplemented {
			t.Fatalf("AUTO_ACK_WITH_RESULT expected Unimplemented, got %v", err)
		}

		s2, _ := h.Client.SubscriberSession(ctx)
		_ = s2.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:     mock.addr,
					Subscription: "projects/p/subscriptions/sub-u",
					RetryCodes:   []int32{int32(codes.Unavailable)},
				},
			},
		})
		if _, err := s2.Recv(); status.Code(err) != codes.Unimplemented {
			t.Fatalf("Start(RetryCodes) expected Unimplemented, got %v", err)
		}

		// 3) Interactive Settle(ACK_WITH_RESULT) fails with StoppedEvent.ErrorCode == Unimplemented.
		s3, _ := h.Client.SubscriberSession(ctx)
		_ = s3.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       mock.addr,
					Subscription:   "projects/p/subscriptions/sub-u",
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		})
		var tok string
		var stopped *proxypb.StoppedEvent
		for stopped == nil {
			ev, rerr := s3.Recv()
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				t.Fatalf("s3 Recv: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil && tok == "" {
				tok = md.DeliveryToken
				_ = s3.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Settle{
						Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT},
					},
				})
				_ = s3.CloseSend()
			}
			if st := ev.GetStopped(); st != nil {
				stopped = st
			}
		}
		if stopped == nil || stopped.ErrorCode != int32(codes.Unimplemented) {
			t.Fatalf("expected StoppedEvent.ErrorCode=Unimplemented, got %+v", stopped)
		}
	})

	t.Run("AutoDropAndEodMaxAckExtPrecedenceAndZeroTotalTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)

		// 1) EOD with MaxDurationPerAckExtensionMs=20000 (< 60s) must honor 20s, not force 60s.
		mock.resetCounters()
		mock.mu.Lock()
		mock.onPublish = func(pctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attempt int) (*pubsubpb.PublishResponse, error) {
			time.Sleep(25 * time.Millisecond)
			return &pubsubpb.PublishResponse{MessageIds: []string{"m-zero-to"}}, nil
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "eod-dl-1", Message: &pubsubpb.PubsubMessage{MessageId: "meod-dl-1", Data: []byte("d")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		// TotalTimeoutMs == 0 must mean unlimited (not instant timeout).
		zeroPub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:       mock.addr,
			Topic:          "projects/p/topics/t-zero",
			TotalTimeoutMs: protoInt64(0),
		})
		if err != nil {
			t.Fatalf("CreatePublisher(t-zero): %v", err)
		}
		defer func() {
			_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: zeroPub.PublisherId})
		}()
		pubResp, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId: zeroPub.PublisherId,
			Data:        []byte("hi"),
		})
		if err != nil || pubResp.ErrorCode != 0 {
			t.Fatalf("Publish with TotalTimeoutMs=0 failed: resp=%+v err=%v", pubResp, err)
		}

		sess, _ := h.Client.SubscriberSession(ctx)
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:                     mock.addr,
					Subscription:                 "projects/p/subscriptions/sub-eod-dl",
					AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_DROP_WITHOUT_SETTLE,
					MaxDurationPerAckExtensionMs: protoInt64(20000),
				},
			},
		})
		for {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv: %v", rerr)
			}
			if ev.GetMessageDelivered() != nil {
				break
			}
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}
		saw20sReceipt := false
		sawZeroDropNack := false
		for _, c := range mock.getModAckCalls() {
			if c.Req.AckDeadlineSeconds == 20 {
				saw20sReceipt = true
			}
			if c.Req.AckDeadlineSeconds == 0 {
				sawZeroDropNack = true
			}
		}
		capsGo, err := h.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("GetCapabilities go: %v", err)
		}
		if capsGo.SupportsAutoNackOnDrop {
			t.Fatalf("expected conformant-go SupportsAutoNackOnDrop=false")
		}
		if !saw20sReceipt {
			t.Fatalf("expected EOD receipt modack deadline=20s honoring MaxDurationPerAckExtensionMs=20000, calls=%+v", mock.getModAckCalls())
		}
		if sawZeroDropNack {
			t.Fatalf("expected AUTO_DROP_WITHOUT_SETTLE to send 0 nacks on conformant-go when SupportsAutoNackOnDrop=false")
		}

		// 2) Verify conformant-rust-nomon has SupportsAutoNackOnDrop=true and nacks on AUTO_DROP_WITHOUT_SETTLE.
		mock.resetCounters()
		hRust := startTestProxyDirect(t, "--mode=conformant-rust-nomon")
		defer hRust.Shutdown(t, 2*time.Second)
		capsRust, err := hRust.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("GetCapabilities rust: %v", err)
		}
		if !capsRust.SupportsAutoNackOnDrop {
			t.Fatalf("expected conformant-rust-nomon SupportsAutoNackOnDrop=true")
		}
		sessRust, err := hRust.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession rust: %v", err)
		}
		if err := sessRust.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       mock.addr,
					Subscription:   "projects/p/subscriptions/sub-eod-dl-rust",
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_DROP_WITHOUT_SETTLE,
				},
			},
		}); err != nil {
			t.Fatalf("Start rust: %v", err)
		}
		for {
			ev, rerr := sessRust.Recv()
			if rerr != nil {
				t.Fatalf("Recv rust: %v", rerr)
			}
			if ev.GetMessageDelivered() != nil {
				break
			}
		}
		_ = sessRust.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sessRust.CloseSend()
		for {
			if _, err := sessRust.Recv(); err != nil {
				break
			}
		}
		var sawRustZeroDropNack bool
		for _, c := range mock.getModAckCalls() {
			if c.Req.AckDeadlineSeconds == 0 {
				sawRustZeroDropNack = true
			}
		}
		if !sawRustZeroDropNack {
			t.Fatalf("expected AUTO_DROP_WITHOUT_SETTLE to nack on conformant-rust-nomon when SupportsAutoNackOnDrop=true")
		}
	})
}

func TestInfra_StreamingPullKeepaliveAndFCPause(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("KeepalivePingPlusPongTimeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go", "--keepalive-ping=120ms", "--keepalive-pong=70ms")
		defer h.Shutdown(t, 2*time.Second)

		mock.resetCounters()
		stream1ClosedAt := make(chan time.Duration, 1)
		stream2Opened := make(chan struct{}, 1)
		mock.mu.Lock()
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				start := time.Now()
				// Read until ping arrives then stay silent so pongTimeout fires at ping+pong (~190ms).
				_, _ = stream.Recv()
				<-stream.Context().Done()
				stream1ClosedAt <- time.Since(start)
				return stream.Context().Err()
			}
			if streamIdx == 2 {
				select {
				case stream2Opened <- struct{}{}:
				default:
				}
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:     mock.addr,
					Subscription: "projects/p/subscriptions/sub-ka-to",
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var elapsed time.Duration
		select {
		case elapsed = <-stream1ClosedAt:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for keepalive timeout to close stream 1")
		}
		if elapsed < 150*time.Millisecond || elapsed > 500*time.Millisecond {
			t.Fatalf("expected keepalive ping+pong (~190ms) timeout, got %v", elapsed)
		}
		select {
		case <-stream2Opened:
		case <-time.After(3 * time.Second):
			t.Fatalf("expected stream 2 reconnect after keepalive timeout")
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}
	})

	t.Run("KeepaliveSurvivesFCPause", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go", "--keepalive-ping=120ms", "--keepalive-pong=70ms")
		defer h.Shutdown(t, 2*time.Second)

		mock.resetCounters()
		mock.mu.Lock()
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "ka-fc-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-kafc-1", Data: []byte("1")}},
						{AckId: "ka-fc-2", Message: &pubsubpb.PubsubMessage{MessageId: "m-kafc-2", Data: []byte("2")}},
					},
				})
				// Keep answering heartbeat pings with empty responses while FC blocks handleReceivedBatch.
				go func() {
					for {
						if _, rerr := stream.Recv(); rerr != nil {
							return
						}
						_ = stream.Send(&pubsubpb.StreamingPullResponse{})
					}
				}()
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:               mock.addr,
					Subscription:           "projects/p/subscriptions/sub-ka-fc",
					MaxOutstandingMessages: protoInt64(1),
					AutoSettleMode:         proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Receive message 1, hold it for 450ms (> 2 * (ping+pong)) so fc.acquire blocks on message 2.
		var tok1 string
		for tok1 == "" {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv 1: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				tok1 = md.DeliveryToken
			}
		}
		time.Sleep(450 * time.Millisecond)

		if len(mock.getStreamInits()) != 1 {
			t.Fatalf("expected StreamingPull stream to stay alive during FC pause (PULL-03), got %d stream openings", len(mock.getStreamInits()))
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok1, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK},
			},
		})

		var tok2 string
		for tok2 == "" {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv 2: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				tok2 = md.DeliveryToken
			}
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok2, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK},
			},
		})
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}
	})
}

func TestInfra_ModeAndFaultCompositionAndDivergentClient(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("ModeAndFaultCompositionPreservesBaseFaults", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		h := startTestProxyDirect(t, "--mode=missing-rpc", "--fault=ignore-sigterm")
		defer h.Shutdown(t, 2*time.Second)
		mock.resetCounters()

		// Base fault missing-rpc must STILL skip Publish and UnaryPull RPCs.
		compPub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: mock.addr,
			Topic:    "projects/p/topics/t-comp",
		})
		if err != nil {
			t.Fatalf("CreatePublisher missing-rpc: %v", err)
		}
		pubResp, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId: compPub.PublisherId,
			Data:        []byte("hi"),
		})
		_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: compPub.PublisherId})
		if err != nil || pubResp.ErrorCode != 0 {
			t.Fatalf("Publish missing-rpc+ignore-sigterm: resp=%+v err=%v", pubResp, err)
		}
		upResp, err := h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
			Endpoint:     mock.addr,
			Subscription: "projects/p/subscriptions/sub-comp",
		})
		if err != nil || len(upResp.Messages) != 0 {
			t.Fatalf("UnaryPull missing-rpc+ignore-sigterm: resp=%+v err=%v", upResp, err)
		}
		if len(mock.getPublishCalls()) != 0 || len(mock.getPullCalls()) != 0 {
			t.Fatalf("expected missing-rpc base fault preserved alongside --fault=ignore-sigterm")
		}
		// Base fault + ignore-sigterm: SIGTERM must be ignored.
		_ = h.Cmd.Process.Signal(syscall.SIGTERM)
		select {
		case werr := <-h.waitCh:
			t.Fatalf("expected proxy to ignore SIGTERM when composed with missing-rpc, exited: %v", werr)
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("InvalidTracesPlusIgnoreShutdownAndPartialUnimpl", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		h := startTestProxyDirect(t, "--mode=invalid-traces", "--fault=ignore-shutdown")
		defer h.Shutdown(t, 2*time.Second)
		mock.resetCounters()

		invPub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         mock.addr,
			Topic:            "projects/p/topics/t-inv",
			EnableMonitoring: true,
		})
		if err != nil {
			t.Fatalf("CreatePublisher inv: %v", err)
		}
		_, err = h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId: invPub.PublisherId,
			Data:        []byte("hi"),
		})
		_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: invPub.PublisherId})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		for _, sp := range drained.Spans {
			if len(sp.Links) != 0 {
				t.Fatalf("expected invalid-traces base fault (missing-links) preserved alongside ignore-shutdown")
			}
		}

		// ConformantPartialUnimpl + --unsupported=eod,ordering-opt-in keeps partial defaults and disables EOD/ordering.
		hPart := startTestProxyDirect(t, "--mode=conformant-partial-unimpl", "--unsupported=eod,ordering-opt-in")
		defer hPart.Shutdown(t, 2*time.Second)
		caps, err := hPart.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("GetCapabilities: %v", err)
		}
		if caps.SupportsUnaryPull || caps.SupportsPublishHedging || caps.SupportsOrderingOptIn || caps.SupportsExactlyOnceDelivery {
			t.Fatalf("expected additive --unsupported to keep partial unimpl AND disable EOD/ordering, got %+v", caps)
		}
	})

	t.Run("DivergentClientAndTimerCoalescing", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		hDiv := startTestProxyDirect(t, "--mode=divergent-client")
		defer hDiv.Shutdown(t, 2*time.Second)
		caps, err := hDiv.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("GetCapabilities divergent: %v", err)
		}
		if !caps.SupportsPublishFlowControl || !caps.SupportsUnaryPull {
			t.Fatalf("unexpected divergent capabilities: %+v", caps)
		}
		mock.resetCounters()
		mock.mu.Lock()
		mock.onPull = func(pctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
			return &pubsubpb.PullResponse{
				ReceivedMessages: []*pubsubpb.ReceivedMessage{
					{AckId: "div-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-div-1", Data: []byte("d")}},
				},
			}, nil
		}
		mock.mu.Unlock()
		up, err := hDiv.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
			Endpoint:     mock.addr,
			Subscription: "projects/p/subscriptions/sub-div",
			SettleAction: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
		})
		if err != nil || len(up.Messages) != 1 {
			t.Fatalf("UnaryPull divergent: resp=%+v err=%v", up, err)
		}

		// Timer coalescing stress across rapid sequential batch windows on --mode=conformant-go.
		hGo := startTestProxyDirect(t, "--mode=conformant-go")
		defer hGo.Shutdown(t, 2*time.Second)
		mock.resetCounters()
		mock.mu.Lock()
		mock.onPublish = nil
		mock.mu.Unlock()

		timerPub, err := hGo.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         mock.addr,
			Topic:            "projects/p/topics/t-timer",
			CountThreshold:   protoInt64(10),
			DelayThresholdMs: protoInt64(15),
		})
		if err != nil {
			t.Fatalf("CreatePublisher timer: %v", err)
		}
		defer func() {
			_, _ = hGo.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: timerPub.PublisherId})
		}()

		var wg sync.WaitGroup
		errs := make(chan error, 12)
		for round := 0; round < 3; round++ {
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func(r, j int) {
					defer wg.Done()
					resp, perr := hGo.Client.Publish(ctx, &proxypb.PublishRequest{
						PublisherId: timerPub.PublisherId,
						Data:        []byte(fmt.Sprintf("%d-%d", r, j)),
					})
					if perr != nil || resp.ErrorCode != 0 {
						errs <- fmt.Errorf("round %d msg %d err=%v resp=%+v", r, j, perr, resp)
					}
				}(round, i)
			}
			time.Sleep(20 * time.Millisecond)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if len(mock.getPublishCalls()) == 0 {
			t.Fatalf("expected batched publish calls")
		}
	})
}

func TestInfra_TraceFidelityGoAndCppDetailed(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("GoFlowControlErrorAndPublishErrorNoPublishEnd", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		holdPublish := make(chan struct{})
		inPublish := make(chan struct{}, 1)
		mock.mu.Lock()
		mock.onPublish = func(pctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attempt int) (*pubsubpb.PublishResponse, error) {
			if attempt == 1 {
				select {
				case inPublish <- struct{}{}:
				default:
				}
				<-holdPublish
				return &pubsubpb.PublishResponse{MessageIds: []string{"m-fc-ok"}}, nil
			}
			return nil, status.Error(codes.PermissionDenied, "pub denied")
		}
		mock.mu.Unlock()
		defer func() {
			mock.mu.Lock()
			mock.onPublish = nil
			mock.mu.Unlock()
		}()

		goFcPub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:               mock.addr,
			Topic:                  "projects/p/topics/t-gofc",
			EnableMonitoring:       true,
			MaxOutstandingMessages: protoInt64(1),
			LimitExceededBehavior:  proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR,
		})
		if err != nil {
			t.Fatalf("CreatePublisher(t-gofc): %v", err)
		}
		defer func() {
			_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: goFcPub.PublisherId})
		}()

		var resp1Err error
		var resp1 *proxypb.PublishResponse
		done1 := make(chan struct{})
		go func() {
			defer close(done1)
			resp1, resp1Err = h.Client.Publish(ctx, &proxypb.PublishRequest{
				PublisherId: goFcPub.PublisherId,
				Data:        []byte("12345"),
			})
		}()
		<-inPublish

		// 2nd Publish fails Go flow control SignalError -> must record publisher flow control ERROR and NO create span.
		resp2, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId: goFcPub.PublisherId,
			Data:        []byte("9"),
		})
		if err != nil || resp2.ErrorCode == 0 {
			t.Fatalf("expected FC SignalError failure, got resp=%+v err=%v", resp2, err)
		}
		close(holdPublish)
		<-done1
		if resp1Err != nil || resp1.ErrorCode != 0 {
			t.Fatalf("resp1 failed: resp=%+v err=%v", resp1, resp1Err)
		}

		// 3rd Publish hits permanent RPC error -> <topic> create MUST NOT emit "publish end".
		goErrPub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         mock.addr,
			Topic:            "projects/p/topics/t-goerr",
			EnableMonitoring: true,
		})
		if err != nil {
			t.Fatalf("CreatePublisher(t-goerr): %v", err)
		}
		defer func() {
			_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: goErrPub.PublisherId})
		}()
		resp3, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId: goErrPub.PublisherId,
			Data:        []byte("err"),
		})
		if err != nil || resp3.ErrorCode != int32(codes.PermissionDenied) {
			t.Fatalf("expected PermissionDenied on 3rd publish, got resp=%+v err=%v", resp3, err)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		var fcCreateCount, errFCs int
		var errCreateSpan *proxypb.TraceSpan
		for _, sp := range drained.Spans {
			if sp.Name == "t-gofc create" {
				fcCreateCount++
			}
			if sp.Name == "publisher flow control" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
				errFCs++
			}
			if sp.Name == "t-goerr create" {
				errCreateSpan = sp
			}
		}
		if fcCreateCount != 1 || errFCs != 1 {
			t.Fatalf("expected 1 create span (omitting FC SignalError create) and 1 error FC span, got creates=%d errFC=%d", fcCreateCount, errFCs)
		}
		if errCreateSpan == nil || errCreateSpan.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			t.Fatalf("expected t-goerr create error span, got %+v", errCreateSpan)
		}
		var sawPubStart, sawPubEnd bool
		for _, ev := range errCreateSpan.Events {
			if ev.Name == "publish start" {
				sawPubStart = true
			}
			if ev.Name == "publish end" {
				sawPubEnd = true
			}
		}
		if !sawPubStart || sawPubEnd {
			t.Fatalf("Go PublishErrorRPC create span expected publish start without publish end; start=%v end=%v", sawPubStart, sawPubEnd)
		}

		// 4th Publish hits oversized (>10MB) error -> Go MUST emit "publisher flow control" (without "publisher batching").
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		oversized := make([]byte, 10_000_100)
		resp4, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId: goErrPub.PublisherId,
			Data:        oversized,
		})
		if err != nil || resp4.ErrorCode != int32(codes.InvalidArgument) {
			t.Fatalf("expected InvalidArgument on oversized Go publish, got resp=%+v err=%v", resp4, err)
		}
		ovDrained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces oversized: %v", err)
		}
		var ovCreateErr, ovFC, ovBatch int
		for _, sp := range ovDrained.Spans {
			if sp.Name == "t-goerr create" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
				ovCreateErr++
			}
			if sp.Name == "publisher flow control" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
				ovFC++
			}
			if sp.Name == "publisher batching" {
				ovBatch++
			}
		}
		if ovCreateErr != 1 || ovFC != 1 || ovBatch != 0 {
			t.Fatalf("Go oversized publish expected 1 error create, 1 UNSET FC, 0 batching; got create=%d fc=%d batch=%d", ovCreateErr, ovFC, ovBatch)
		}
	})

	t.Run("GoEODAndALOErrorResultAndUnaryPullSpans", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		runOneSub := func(subName string, eod bool, settleMode proxypb.StartSubscriberCommand_AutoSettleMode) *proxypb.TraceSpan {
			mock.resetCounters()
			mock.mu.Lock()
			mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
				return status.Error(codes.PermissionDenied, "ack perm denied")
			}
			mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
				if req.AckDeadlineSeconds == 0 {
					return status.Error(codes.PermissionDenied, "nack perm denied")
				}
				return nil
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
							ExactlyOnceDeliveryEnabled: eod,
						},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: subName + "-a1", Message: &pubsubpb.PubsubMessage{MessageId: subName + "-m1", Data: []byte("d")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, _ := h.Client.SubscriberSession(ctx)
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:         mock.addr,
						Subscription:     "projects/p/subscriptions/" + subName,
						EnableMonitoring: true,
						AutoSettleMode:   settleMode,
					},
				},
			})
			for {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("%s Recv: %v", subName, rerr)
				}
				if ev.GetSettlementResult() != nil {
					break
				}
			}
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			})
			_ = sess.CloseSend()
			for {
				if _, err := sess.Recv(); err != nil {
					break
				}
			}
			drained, _ := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			for _, sp := range drained.Spans {
				if sp.Name == subName+" subscribe" {
					return sp
				}
			}
			t.Fatalf("missing subscribe span for %s", subName)
			return nil
		}

		hasSpanEvent := func(sp *proxypb.TraceSpan, name string) bool {
			for _, ev := range sp.Events {
				if ev.Name == name {
					return true
				}
			}
			return false
		}

		eodSpan := runOneSub("sub-goeod-err", true, proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT)
		if eodSpan.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			t.Fatalf("Go EOD failure must set subscribe ERROR status")
		}
		if _, hasRes := eodSpan.Attributes["messaging.gcp_pubsub.result"]; hasRes {
			t.Fatalf("Go EOD failure must omit messaging.gcp_pubsub.result, got %+v", eodSpan.Attributes)
		}
		if !hasSpanEvent(eodSpan, "ack end") {
			t.Fatalf("Go EOD ack failure must still emit \"ack end\" event, got %+v", eodSpan.Events)
		}

		eodNackSpan := runOneSub("sub-goeod-nack-err", true, proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT)
		if eodNackSpan.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			t.Fatalf("Go EOD nack failure must set subscribe ERROR status")
		}
		if _, hasRes := eodNackSpan.Attributes["messaging.gcp_pubsub.result"]; hasRes {
			t.Fatalf("Go EOD nack failure must omit messaging.gcp_pubsub.result, got %+v", eodNackSpan.Attributes)
		}
		if !hasSpanEvent(eodNackSpan, "nack end") {
			t.Fatalf("Go EOD nack failure must still emit \"nack end\" event, got %+v", eodNackSpan.Events)
		}

		aloSpan := runOneSub("sub-goalo-err", false, proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT)
		if aloSpan.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
			t.Fatalf("Go ALO failure must keep subscribe UNSET status, got %v", aloSpan.StatusCode)
		}
		if aloSpan.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "acked" {
			t.Fatalf("Go ALO failure must keep result=acked, got %+v", aloSpan.Attributes)
		}
		if !hasSpanEvent(aloSpan, "ack end") {
			t.Fatalf("Go ALO ack failure must emit \"ack end\" event, got %+v", aloSpan.Events)
		}

		// Go UnaryPull spans (modack + ack).
		mock.resetCounters()
		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = nil
		mock.onPull = func(pctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
			return &pubsubpb.PullResponse{
				ReceivedMessages: []*pubsubpb.ReceivedMessage{
					{AckId: "goup-1", Message: &pubsubpb.PubsubMessage{MessageId: "mgoup-1", Data: []byte("d")}},
				},
			}, nil
		}
		mock.mu.Unlock()
		up, err := h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
			Endpoint:         mock.addr,
			Subscription:     "projects/p/subscriptions/sub-go-up",
			EnableMonitoring: true,
			LeaseExtensionMs: protoInt64(15),
			SettleAction:     proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
		})
		if err != nil || len(up.Messages) != 1 {
			t.Fatalf("Go UnaryPull failed: resp=%+v err=%v", up, err)
		}
		drained, _ := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		var sawMod, sawAck bool
		for _, sp := range drained.Spans {
			if sp.Name == "sub-go-up modack" {
				sawMod = true
			}
			if sp.Name == "sub-go-up ack" {
				sawAck = true
			}
		}
		if !sawMod || !sawAck {
			t.Fatalf("Go UnaryPull expected modack and ack spans, got mod=%v ack=%v", sawMod, sawAck)
		}
	})

	t.Run("CppPublishBlockingFanoutStreamingAndUnaryPull", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-cpp-partial")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		mock.mu.Lock()
		mock.onPublish = nil
		mock.onAck = nil
		mock.onModAck = nil
		mock.onPull = nil
		mock.onStreamPull = nil
		mock.mu.Unlock()
		// 1) BlockingPublisher omits FC/batching/publish spans & wire traceparent.
		cppBlkPub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         mock.addr,
			Topic:            "projects/p/topics/t-cpp-blk",
			EnableMonitoring: true,
		})
		if err != nil {
			t.Fatalf("CreatePublisher cpp-blk: %v", err)
		}
		bResp, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId:          cppBlkPub.PublisherId,
			Data:                 []byte("blk"),
			UseBlockingPublisher: true,
		})
		_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: cppBlkPub.PublisherId})
		if err != nil || bResp.ErrorCode != 0 {
			t.Fatalf("Cpp BlockingPublisher failed: resp=%+v err=%v", bResp, err)
		}
		calls := mock.getPublishCalls()
		if len(calls) != 1 || calls[0].Req.Messages[0].Attributes["googclient_traceparent"] != "" {
			t.Fatalf("Cpp BlockingPublisher should omit wire traceparent")
		}
		drained, _ := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if len(drained.Spans) != 1 || drained.Spans[0].Name != "t-cpp-blk create" ||
			drained.Spans[0].InstrumentationScopeName != "gl-cpp" ||
			drained.Spans[0].StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK {
			t.Fatalf("unexpected C++ BlockingPublisher spans: %+v", drained.Spans)
		}

		// 1b) C++ local rejections (ordering not enabled & invalid UTF-8) emit FC + batching children.
		cppRejPub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:              mock.addr,
			Topic:                 "projects/p/topics/t-cpp-rej",
			EnableMonitoring:      true,
			EnableMessageOrdering: protoBool(false),
		})
		if err != nil {
			t.Fatalf("CreatePublisher cpp-rej: %v", err)
		}
		ordRej, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId: cppRejPub.PublisherId,
			Data:        []byte("ord"),
			OrderingKey: "k1",
		})
		if err != nil || ordRej.ErrorCode != int32(codes.InvalidArgument) {
			t.Fatalf("expected InvalidArgument on C++ ordering rejection, got resp=%+v err=%v", ordRej, err)
		}
		utfRej, err := h.Client.Publish(ctx, &proxypb.PublishRequest{
			PublisherId:   cppRejPub.PublisherId,
			Data:          []byte("utf"),
			RawAttributes: []*proxypb.RawAttribute{{Key: []byte("bad"), Value: []byte{0xff}}},
		})
		if err != nil || utfRej.ErrorCode != int32(codes.Internal) {
			t.Fatalf("expected Internal on C++ invalid UTF-8, got resp=%+v err=%v", utfRej, err)
		}
		_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: cppRejPub.PublisherId})
		drained, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		var rejCreateErr, rejFC, rejBatch int
		for _, sp := range drained.Spans {
			if sp.Name == "t-cpp-rej create" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
				rejCreateErr++
			}
			if sp.Name == "publisher flow control" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK {
				rejFC++
			}
			if sp.Name == "publisher batching" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK {
				rejBatch++
			}
		}
		if rejCreateErr != 2 || rejFC != 2 || rejBatch != 2 {
			t.Fatalf("expected 2 C++ local rejections with 2 FC and 2 batching OK spans; got create=%d fc=%d batch=%d", rejCreateErr, rejFC, rejBatch)
		}

		// 2) >128 message batch produces "publish #N" fan-out spans deterministically without timer flakiness.
		mock.resetCounters()
		cppFanPub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         mock.addr,
			Topic:            "projects/p/topics/t-cpp-fan",
			EnableMonitoring: true,
			CountThreshold:   protoInt64(200),
			DelayThresholdMs: protoInt64(10000),
		})
		if err != nil {
			t.Fatalf("CreatePublisher cpp-fan: %v", err)
		}
		tokens := make([]string, 0, 135)
		for i := 0; i < 135; i++ {
			resp, perr := h.Client.Publish(ctx, &proxypb.PublishRequest{
				PublisherId: cppFanPub.PublisherId,
				Data:        []byte(fmt.Sprintf("%03d", i)),
				AsyncFire:   true,
			})
			if perr != nil || resp.ErrorCode != 0 || resp.PublishToken == "" {
				t.Fatalf("AsyncFire publish %d failed: resp=%+v err=%v", i, resp, perr)
			}
			tokens = append(tokens, resp.PublishToken)
		}
		if _, err := h.Client.FlushPublisher(ctx, &proxypb.FlushPublisherRequest{PublisherId: cppFanPub.PublisherId}); err != nil {
			t.Fatalf("FlushPublisher cpp-fan: %v", err)
		}
		for i, tok := range tokens {
			aw, aerr := h.Client.AwaitPublish(ctx, &proxypb.AwaitPublishRequest{PublisherId: cppFanPub.PublisherId, PublishToken: tok})
			if aerr != nil || aw.ErrorCode != 0 {
				t.Fatalf("AwaitPublish %d failed: aw=%+v err=%v", i, aw, aerr)
			}
		}
		_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: cppFanPub.PublisherId})
		drained, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		var pubSpanID string
		var pubLinks int
		var fanChildCount int
		var fanChildLinks int
		for _, sp := range drained.Spans {
			if sp.Name == "t-cpp-fan publish" {
				pubSpanID = sp.SpanId
				pubLinks = len(sp.Links)
			}
		}
		for _, sp := range drained.Spans {
			if strings.HasPrefix(sp.Name, "publish #") {
				if pubSpanID == "" || sp.ParentSpanId != pubSpanID {
					t.Fatalf("expected fan-out child %q parent %q, got %q", sp.Name, pubSpanID, sp.ParentSpanId)
				}
				if len(sp.Links) > 128 {
					t.Fatalf("fan-out child %q exceeded 128 links: %d", sp.Name, len(sp.Links))
				}
				fanChildCount++
				fanChildLinks += len(sp.Links)
			}
		}
		if pubLinks != 0 || fanChildCount != 2 || fanChildLinks != 135 {
			t.Fatalf("expected C++ 0 root publish links + 2 fan-out children with 135 links, got pub=%d children=%d fan=%d", pubLinks, fanChildCount, fanChildLinks)
		}

		// 3) C++ StreamingPull: modack error stays UNSET, ack error emits gl-cpp.ack_end + result="ack",
		//    subscribe envelope.size includes len(msgID), cc/sched have messaging.system.
		mock.resetCounters()
		modAckErrSeen := make(chan struct{}, 2)
		mock.mu.Lock()
		mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
			if req.AckDeadlineSeconds > 0 {
				select {
				case modAckErrSeen <- struct{}{}:
				default:
				}
				return status.Error(codes.Unavailable, "extend err")
			}
			return nil
		}
		mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
			for _, id := range req.AckIds {
				if id == "cpp-a2" {
					return status.Error(codes.PermissionDenied, "perm fail")
				}
			}
			return nil
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "cpp-a1", DeliveryAttempt: 1, Message: &pubsubpb.PubsubMessage{MessageId: "cm1", Data: []byte("1")}},
						{AckId: "cpp-a2", DeliveryAttempt: 2, Message: &pubsubpb.PubsubMessage{MessageId: "cm2", Data: []byte("2")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, _ := h.Client.SubscriberSession(ctx)
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:                     mock.addr,
					Subscription:                 "projects/p/subscriptions/sub-cpp-s",
					EnableMonitoring:             true,
					MaxDurationPerAckExtensionMs: protoInt64(120),
					AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		})
		var cppTokens []string
		for len(cppTokens) < 2 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("cpp stream Recv: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				cppTokens = append(cppTokens, md.DeliveryToken)
			}
		}
		select {
		case <-modAckErrSeen:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for C++ periodic modack extension")
		}
		for _, tok := range cppTokens {
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{
						DeliveryToken: tok,
						Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
					},
				},
			}); err != nil {
				t.Fatalf("cpp Settle %q: %v", tok, err)
			}
			for {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("cpp settle Recv for %q: %v", tok, rerr)
				}
				if sr := ev.GetSettlementResult(); sr != nil {
					if sr.DeliveryToken != tok {
						t.Fatalf("unexpected settlement token %q, want %q", sr.DeliveryToken, tok)
					}
					break
				}
			}
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}
		drained, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		var sawModAckUnset, sawAckOK, sawAckErr, sawCCMsgSys, sawSchedMsgSys bool
		var cppSubSpans []*proxypb.TraceSpan
		for _, sp := range drained.Spans {
			if sp.InstrumentationScopeName != "gl-cpp" {
				t.Fatalf("expected scope gl-cpp, got %q on %q", sp.InstrumentationScopeName, sp.Name)
			}
			if sp.Name == "sub-cpp-s subscribe" {
				cppSubSpans = append(cppSubSpans, sp)
			}
			if sp.Name == "subscriber concurrency control" && sp.Attributes["messaging.system"].GetStringValue() == "gcp_pubsub" {
				sawCCMsgSys = true
			}
			if sp.Name == "subscriber scheduler" && sp.Attributes["messaging.system"].GetStringValue() == "gcp_pubsub" {
				sawSchedMsgSys = true
			}
			if sp.Name == "sub-cpp-s modack" && sp.Kind == proxypb.SpanKind_SPAN_KIND_CLIENT && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
				sawModAckUnset = true
			}
			if sp.Name == "sub-cpp-s ack" && sp.Attributes["code.function.name"].GetStringValue() == "pubsub::AckHandler::ack" {
				if sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK {
					sawAckOK = true
				}
				if sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
					sawAckErr = true
				}
			}
		}
		if !sawModAckUnset || !sawAckOK || !sawAckErr || !sawCCMsgSys || !sawSchedMsgSys {
			t.Fatalf("C++ streaming spans incomplete: modackUnset=%v ackOK=%v ackErr=%v cc=%v sched=%v", sawModAckUnset, sawAckOK, sawAckErr, sawCCMsgSys, sawSchedMsgSys)
		}
		if len(cppSubSpans) != 2 {
			t.Fatalf("expected 2 C++ subscribe spans, got %d", len(cppSubSpans))
		}
		for _, sp := range cppSubSpans {
			if gotEnv := sp.Attributes["messaging.message.envelope.size"].GetInt64Value(); gotEnv != 24 {
				t.Fatalf("C++ subscribe envelope.size expected 24 (20 + len(data)=1 + len(msgID)=3), got %d", gotEnv)
			}
			if gotRes := sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue(); gotRes != "ack" {
				t.Fatalf("C++ subscribe result expected \"ack\" (even on EOD failure), got %q", gotRes)
			}
			sawCppAckEnd := false
			for _, ev := range sp.Events {
				if ev.Name == "gl-cpp.ack_end" {
					sawCppAckEnd = true
				}
			}
			if !sawCppAckEnd {
				t.Fatalf("C++ subscribe span missing gl-cpp.ack_end event: %+v", sp.Events)
			}
		}

		// 4) C++ UnaryPull receive + modack + ack spans.
		mock.resetCounters()
		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = nil
		mock.onPull = func(pctx context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
			return &pubsubpb.PullResponse{
				ReceivedMessages: []*pubsubpb.ReceivedMessage{
					{AckId: "cppup-1", DeliveryAttempt: 1, Message: &pubsubpb.PubsubMessage{MessageId: "mcppup-1", Data: []byte("abc")}},
				},
			}, nil
		}
		mock.mu.Unlock()
		upResp, err := h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
			Endpoint:         mock.addr,
			Subscription:     "projects/p/subscriptions/sub-cpp-up",
			EnableMonitoring: true,
			LeaseExtensionMs: protoInt64(15),
			SettleAction:     proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
		})
		if err != nil || len(upResp.Messages) != 1 {
			t.Fatalf("C++ UnaryPull failed: resp=%+v err=%v", upResp, err)
		}
		drained, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		var sawUpRecv, sawUpMod, sawUpAck bool
		for _, sp := range drained.Spans {
			if sp.Name == "sub-cpp-up receive" && sp.Kind == proxypb.SpanKind_SPAN_KIND_CONSUMER &&
				sp.Attributes["code.function.name"].GetStringValue() == "pubsub::SubscriberConnection::Pull" &&
				sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK {
				sawUpRecv = true
			}
			if sp.Name == "sub-cpp-up modack" && sp.Kind == proxypb.SpanKind_SPAN_KIND_CLIENT &&
				sp.Attributes["code.function.name"].GetStringValue() == "pubsub::PullLeaseManager::ExtendLease" {
				sawUpMod = true
			}
			if sp.Name == "sub-cpp-up ack" && sp.Kind == proxypb.SpanKind_SPAN_KIND_CLIENT &&
				sp.Attributes["code.function.name"].GetStringValue() == "pubsub::PullAckHandler::ack" &&
				len(sp.Links) == 1 {
				sawUpAck = true
			}
		}
		if !sawUpRecv || !sawUpMod || !sawUpAck {
			t.Fatalf("C++ UnaryPull spans missing: recv=%v mod=%v ack=%v", sawUpRecv, sawUpMod, sawUpAck)
		}
	})
}

func TestInfra_LeakDetectorWorktreeAndReportParserEdgeCases(t *testing.T) {
	t.Run("MarkerPrefixCollisionAndPadding", func(t *testing.T) {
		m := newRunMarker(t)
		if !runMarkerRegexp.MatchString(m) {
			t.Fatalf("newRunMarker %q invalid", m)
		}
		if !strings.HasSuffix(m, fmt.Sprintf("-%06d", runMarkerSeq.Load())) {
			t.Fatalf("expected zero-padded 6-digit sequence in marker %q", m)
		}
		if hasExactMarkerToken("--run-id=e2e-run-123-10", "e2e-run-123-1") {
			t.Fatalf("prefix collision should not match -10 for -1")
		}
		if !hasExactMarkerToken("--run-id=e2e-run-123-1", "e2e-run-123-1") {
			t.Fatalf("exact token should match")
		}
		if hasExactMarkerToken("arg=custom-10", "custom-1") {
			t.Fatalf("arbitrary marker prefix collision should not match")
		}
		if hasExactMarkerToken("/tmp/e2e-run-123-1-extra/p.sh", "e2e-run-123-1") {
			t.Fatalf("right-side hyphenated suffix should not match e2e-run-123-1")
		}
		if !hasExactMarkerToken("/tmp/proxy-e2e-run-123-000001.sh", "e2e-run-123-000001") {
			t.Fatalf("wrapper filename token should match")
		}

		// Live child process test: marker "-10" must not be matched by marker "-1".
		m10 := fmt.Sprintf("e2e-run-%d-10", os.Getpid()+100000)
		m1 := fmt.Sprintf("e2e-run-%d-1", os.Getpid()+100000)
		sleepCmd := exec.Command("sh", "-c", "sleep 5; true", "sh", "--run-id="+m10)
		if err := sleepCmd.Start(); err != nil {
			t.Fatalf("start sleep: %v", err)
		}
		defer func() {
			_ = sleepCmd.Process.Kill()
			_ = sleepCmd.Wait()
		}()
		matches1, err := findProcessesWithMarker(m1)
		if err != nil {
			t.Fatalf("findProcessesWithMarker(%q): %v", m1, err)
		}
		if len(matches1) != 0 {
			t.Fatalf("marker %q falsely matched -10 child process: %+v", m1, matches1)
		}
		matches10, err := findProcessesWithMarker(m10)
		if err != nil || len(matches10) != 1 || matches10[0].PID != sleepCmd.Process.Pid {
			t.Fatalf("marker %q failed to match child pid %d: matches=%+v err=%v", m10, sleepCmd.Process.Pid, matches10, err)
		}
	})

	t.Run("RepoRootWorktreeGuard", func(t *testing.T) {
		outer := t.TempDir()
		if err := os.WriteFile(filepath.Join(outer, "go.mod"), []byte("module github.com/googleapis/gapic-showcase\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if isShowcaseRepoRoot(outer) {
			t.Fatalf("outer dir without pubsub/conformance/e2e/runner_test.go must not pass isShowcaseRepoRoot")
		}
		inner := filepath.Join(outer, "worktree")
		if err := os.MkdirAll(filepath.Join(inner, "pubsub", "conformance", "e2e"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(inner, "go.mod"), []byte("module github.com/googleapis/gapic-showcase\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(inner, "pubsub", "conformance", "e2e", "runner_test.go"), []byte("package e2e_test\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if !isShowcaseRepoRoot(inner) {
			t.Fatalf("inner worktree with go.mod and runner_test.go must pass isShowcaseRepoRoot")
		}
	})

	t.Run("ParseJSONReportEdgeCases", func(t *testing.T) {
		if _, err := ParseJSONReport([]byte("null")); err == nil {
			t.Fatalf("expected ParseJSONReport(null) to fail")
		}
		// Explicit empty differences must not be overwritten by synthesis.
		repEmptyDiffs, err := ParseJSONReport([]byte(`{
			"targets": {
				"t1": {"scenarios": [{"id": "PUB-01", "status": "PASS"}]},
				"t2": {"scenarios": [{"id": "PUB-01", "status": "FAIL"}]}
			},
			"differences": []
		}`))
		if err != nil {
			t.Fatalf("ParseJSONReport explicit empty diffs failed: %v", err)
		}
		if len(repEmptyDiffs.Differences) != 0 {
			t.Fatalf("expected explicit empty differences [] preserved, got %+v", repEmptyDiffs.Differences)
		}

		// Explicit null differences (or alias) must also suppress synthesis (hasExplicitDiffs = true).
		for _, key := range []string{"differences", "diffs"} {
			repNullDiffs, err := ParseJSONReport([]byte(fmt.Sprintf(`{
				"targets": {
					"t1": {"scenarios": [{"id": "PUB-01", "status": "PASS"}]},
					"t2": {"scenarios": [{"id": "PUB-01", "status": "FAIL"}]}
				},
				%q: null
			}`, key)))
			if err != nil {
				t.Fatalf("ParseJSONReport explicit %q: null failed: %v", key, err)
			}
			if len(repNullDiffs.Differences) != 0 {
				t.Fatalf("expected explicit %q: null preserved without synthesis, got %+v", key, repNullDiffs.Differences)
			}
		}

		// Omitted scenario in multi-target synthesis + nested error/verdict objects.
		repSynth, err := ParseJSONReport([]byte(`{
			"targets": {
				"t1": {"scenarios": [{"id": "PUB-01", "status": "PASS"}, {"id": "PUB-02", "status": "PASS", "error": {"message": "nested ok"}}]},
				"t2": {"scenarios": [{"id": "PUB-01", "status": "PASS"}]}
			}
		}`))
		if err != nil {
			t.Fatalf("ParseJSONReport omitted scenario synthesis failed: %v", err)
		}
		if len(repSynth.Differences) != 1 || repSynth.Differences[0].ScenarioID != "PUB-02" {
			t.Fatalf("expected synthesized difference for omitted PUB-02, got %+v", repSynth.Differences)
		}
		if repSynth.Targets["t1"].Scenarios[1].Error != "nested ok" {
			t.Fatalf("expected nested error.message extracted, got %q", repSynth.Targets["t1"].Scenarios[1].Error)
		}

		repNestedVerdict, err := ParseJSONReport([]byte(`{
			"differences": [{"scenario_id": "SUB-01", "verdicts": {"a": {"status": "FAIL"}, "b": "PASS"}}]
		}`))
		if err != nil || repNestedVerdict.Differences[0].Verdicts["a"] != StatusFail {
			t.Fatalf("expected nested verdict status=FAIL parsed, got %+v err=%v", repNestedVerdict, err)
		}

		if _, err := ParseJSONReport([]byte(`{"targets": {"t1": "not-an-object"}}`)); err == nil {
			t.Fatalf("expected error for non-object target")
		}
		if _, err := ParseJSONReport([]byte(`{"targets": [{"name": "dup", "scenarios": []}, {"name": "dup", "scenarios": []}]}`)); err == nil {
			t.Fatalf("expected error for duplicate target name in array")
		}
	})

	t.Run("ParseMarkdownReportEdgeCases", func(t *testing.T) {
		md := strings.Join([]string{
			"# Conformance _Summary_",
			"",
			"Scenario | Verdict | Extra",
			"-------- | -------",
			"mismatch-header | PASS | ignored",
			"",
			"| Scenario | Verdict | Notes |",
			"| --- | --- | --- |",
			"| `PUB-01` | _SKIPPED_NO_MONITORING_ | `a|b` and foo \\| bar | extraCell |",
			"",
			"NoLead | Status | Outcome",
			"------ | ------ | ------",
			"PUB-02 | *PASS* | _FAIL_",
		}, "\n")
		parsed, err := ParseMarkdownReport(md)
		if err != nil {
			t.Fatalf("ParseMarkdownReport: %v", err)
		}
		if !parsed.HasHeadingContaining("Conformance Summary") {
			t.Fatalf("expected stripped emphasis heading, got %+v", parsed.Headings)
		}
		if len(parsed.Tables) != 2 {
			t.Fatalf("expected 2 valid tables (skipping mismatched separator), got %d", len(parsed.Tables))
		}
		row0 := parsed.Tables[0].Rows[0]
		if row0["Verdict"] != StatusSkippedNoMonitoring {
			t.Fatalf("expected SKIPPED_NO_MONITORING preserved without mangling underscores, got %q", row0["Verdict"])
		}
		if row0["Notes"] != "a|b and foo | bar" {
			t.Fatalf("expected code pipe and escaped pipe unescaped, got %q", row0["Notes"])
		}
		if row0["_col_3"] != "extraCell" {
			t.Fatalf("expected extra column preserved under _col_3, got %+v", row0)
		}
		if !parsed.HasTableRowContaining("PUB-02", StatusPass, StatusFail) {
			t.Fatalf("expected no-leading-pipe GFM table row matched cleanly")
		}
		if parsed.HasTableRowContaining("PUB-01", "PASSWORD_NOPE") || parsed.ContainsTokens("PUB-01", "FAIL_NOPE") {
			t.Fatalf("unexpected match")
		}
		// Token boundary check: PASS inside PASSWORD and PUB-01 inside PUB-010 MUST NOT match.
		mdBoundary, err := ParseMarkdownReport("| Col1 | Col2 |\n| --- | --- |\n| PUB-010 | PASSWORD |\n")
		if err != nil {
			t.Fatal(err)
		}
		if mdBoundary.HasTableRowContaining("PASS") || mdBoundary.HasTableRowContaining("PUB-01") ||
			mdBoundary.ContainsTokens("PASS") || mdBoundary.ContainsTokens("PUB-01") {
			t.Fatalf("token boundary violated: matched PASS inside PASSWORD or PUB-01 inside PUB-010")
		}
	})
}

func idxStr(i int) int { return i }

func TestInfra_DuplicateAckIDAndPreCanceledReceiptModackAndEdgeCases(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("DuplicateAckIDConcurrentLeaseExtensionAndNack", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		mock.mu.Lock()
		mock.onAck = nil
		extModAckCh := make(chan struct{}, 4)
		mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
			if req.AckDeadlineSeconds > 0 {
				select {
				case extModAckCh <- struct{}{}:
				default:
				}
			}
			return nil
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "dup-ack-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-dup-1", Data: []byte("one")}},
					},
				})
				time.Sleep(15 * time.Millisecond)
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "dup-ack-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-dup-2", Data: []byte("two")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:                     mock.addr,
					Subscription:                 "projects/p/subscriptions/sub-dup-ack",
					EnableMonitoring:             true,
					MaxDurationPerAckExtensionMs: protoInt64(150),
					AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var deliveries []*proxypb.MessageDeliveredEvent
		for len(deliveries) < 2 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv delivery: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				deliveries = append(deliveries, md)
			}
		}
		if deliveries[0].DeliveryToken == deliveries[1].DeliveryToken {
			t.Fatalf("expected distinct delivery_tokens for duplicate ack_id, got %q", deliveries[0].DeliveryToken)
		}

		// Settle ONLY delivery #1; delivery #2 must remain in s.activeLeases.
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{
					DeliveryToken: deliveries[0].DeliveryToken,
					Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
				},
			},
		}); err != nil {
			t.Fatalf("Settle #1: %v", err)
		}
		for {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv settlement #1: %v", rerr)
			}
			if sr := ev.GetSettlementResult(); sr != nil {
				if sr.DeliveryToken != deliveries[0].DeliveryToken || sr.Status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
					t.Fatalf("unexpected settlement result: %+v", sr)
				}
				break
			}
		}

		// Drain any prior extension signal and verify delivery #2 continues receiving periodic extensions.
		for len(extModAckCh) > 0 {
			<-extModAckCh
		}
		select {
		case <-extModAckCh:
		case <-time.After(3 * time.Second):
			t.Fatalf("expected periodic lease extension to continue for active delivery #2 after delivery #1 settled")
		}

		// Now fast-stop with NACK_IMMEDIATELY and verify delivery #2 is explicitly nacked with ModifyAckDeadline(0).
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{
					ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY,
				},
			},
		}); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}

		ackCalls := mock.getAckCalls()
		var nackCount int
		for _, ma := range mock.getModAckCalls() {
			if ma.Req.AckDeadlineSeconds == 0 {
				for _, id := range ma.Req.AckIds {
					if id == "dup-ack-1" {
						nackCount++
					}
				}
			}
		}
		if len(ackCalls) != 1 || nackCount != 1 {
			t.Fatalf("expected 1 Acknowledge and 1 shutdown ModifyAckDeadline(0) for surviving duplicate ack_id, got acks=%d nacks=%d", len(ackCalls), nackCount)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		var subAcked, subNacked int
		for _, sp := range drained.Spans {
			if sp.Name == "sub-dup-ack subscribe" {
				if sp.EndTime == nil {
					t.Fatalf("unclosed subscribe span: %+v", sp)
				}
				switch sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() {
				case "acked":
					subAcked++
				case "nacked":
					subNacked++
				}
			}
		}
		if subAcked != 1 || subNacked != 1 {
			t.Fatalf("expected 1 acked and 1 nacked subscribe span, got acked=%d nacked=%d", subAcked, subNacked)
		}
		assertNoDuplicateWireAckIDs(t, ackCalls, mock.getModAckCalls())
		modackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-ack", "modack", false)
		if len(modackSpans) == 0 {
			t.Fatalf("expected at least 1 sub-dup-ack modack span")
		}
		assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-ack", "ack", false)
		assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-ack", "nack", false)
	})

	t.Run("PreCanceledPullCtxEODReceiptModackNacksImmediately", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		inBatch1Receipt := make(chan struct{}, 1)
		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
			if req.AckDeadlineSeconds > 0 && len(req.AckIds) > 0 && req.AckIds[0] == "eod-prec-1" {
				select {
				case inBatch1Receipt <- struct{}{}:
				default:
				}
				<-actx.Done()
				return actx.Err()
			}
			return nil
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "eod-prec-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-prec-1", Data: []byte("1")}},
					},
				})
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "eod-prec-2", Message: &pubsubpb.PubsubMessage{MessageId: "m-prec-2", Data: []byte("2")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         mock.addr,
					Subscription:     "projects/p/subscriptions/sub-eod-prec",
					EnableMonitoring: true,
					AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		select {
		case <-inBatch1Receipt:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for batch 1 EOD receipt modack")
		}
		// Give batch 2 a moment to land in batchCh while batch 1 is blocked in receipt modack.
		time.Sleep(30 * time.Millisecond)

		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{
					ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY,
				},
			},
		}); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}

		nacked := make(map[string]bool)
		var batch2ReceiptModAcks int
		for _, ma := range mock.getModAckCalls() {
			for _, id := range ma.Req.AckIds {
				if ma.Req.AckDeadlineSeconds == 0 {
					nacked[id] = true
				} else if id == "eod-prec-2" {
					batch2ReceiptModAcks++
				}
			}
		}
		if batch2ReceiptModAcks != 0 {
			t.Fatalf("expected 0 dead-context receipt modacks for pre-canceled batch 2, got %d", batch2ReceiptModAcks)
		}
		if !nacked["eod-prec-1"] || !nacked["eod-prec-2"] {
			t.Fatalf("expected both eod-prec-1 and eod-prec-2 nacked via ModifyAckDeadline(0), got %+v", nacked)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		var nackedSubs int
		for _, sp := range drained.Spans {
			if sp.Name == "sub-eod-prec subscribe" {
				if sp.EndTime == nil {
					t.Fatalf("unclosed subscribe span: %+v", sp)
				}
				if sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() == "nacked" {
					nackedSubs++
				}
			}
		}
		if nackedSubs != 2 {
			t.Fatalf("expected 2 nacked subscribe spans, got %d", nackedSubs)
		}
	})

	t.Run("CppEODAndALONackFailureTraces", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-cpp-partial")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		runCppNackFail := func(subName string, eod bool) []*proxypb.TraceSpan {
			mock.resetCounters()
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
				if req.AckDeadlineSeconds == 0 {
					return status.Error(codes.PermissionDenied, "nack denied")
				}
				return nil
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
							ExactlyOnceDeliveryEnabled: eod,
						},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: subName + "-a1", DeliveryAttempt: 1, Message: &pubsubpb.PubsubMessage{MessageId: subName + "-m1", Data: []byte("x")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("%s SubscriberSession: %v", subName, err)
			}
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:         mock.addr,
						Subscription:     "projects/p/subscriptions/" + subName,
						EnableMonitoring: true,
						AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_NACK_WITH_RESULT,
					},
				},
			})
			for {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("%s Recv: %v", subName, rerr)
				}
				if ev.GetSettlementResult() != nil {
					break
				}
			}
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			})
			_ = sess.CloseSend()
			for {
				if _, err := sess.Recv(); err != nil {
					break
				}
			}
			drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("%s DrainTraces: %v", subName, err)
			}
			return drained.Spans
		}

		for _, tc := range []struct {
			name string
			eod  bool
		}{
			{name: "sub-cpp-eod-nack", eod: true},
			{name: "sub-cpp-alo-nack", eod: false},
		} {
			spans := runCppNackFail(tc.name, tc.eod)
			var sawNackErrSpan, sawSubSpan bool
			for _, sp := range spans {
				if sp.Name == tc.name+" nack" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
					sawNackErrSpan = true
				}
				if sp.Name == tc.name+" subscribe" {
					sawSubSpan = true
					if sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "nack" {
						t.Fatalf("%s expected result=\"nack\", got %+v", tc.name, sp.Attributes)
					}
					sawNackEnd := false
					for _, ev := range sp.Events {
						if ev.Name == "gl-cpp.nack_end" {
							sawNackEnd = true
						}
					}
					if !sawNackEnd {
						t.Fatalf("%s expected gl-cpp.nack_end event on subscribe span, got %+v", tc.name, sp.Events)
					}
				}
			}
			if !sawNackErrSpan || !sawSubSpan {
				t.Fatalf("%s missing spans: nackErr=%v sub=%v", tc.name, sawNackErrSpan, sawSubSpan)
			}
		}
	})
}

type spanKey struct {
	traceID string
	spanID  string
}

func assertNoDuplicateWireAckIDs(t *testing.T, acks []capturedAckCall, modAcks []capturedModAckCall) {
	t.Helper()
	for i, c := range acks {
		seen := make(map[string]struct{}, len(c.Req.AckIds))
		for _, id := range c.Req.AckIds {
			if _, dup := seen[id]; dup {
				t.Fatalf("ack call[%d] contains duplicate wire ack_id %q", i, id)
			}
			seen[id] = struct{}{}
		}
	}
	for i, c := range modAcks {
		seen := make(map[string]struct{}, len(c.Req.AckIds))
		for _, id := range c.Req.AckIds {
			if _, dup := seen[id]; dup {
				t.Fatalf("modack call[%d] (deadline=%d) contains duplicate wire ack_id %q", i, c.Req.AckDeadlineSeconds, id)
			}
			seen[id] = struct{}{}
		}
	}
}

func assertSettlementBatchCountAndLinks(t *testing.T, spans []*proxypb.TraceSpan, subID, op string, cppMode bool) []*proxypb.TraceSpan {
	t.Helper()
	subByKey := make(map[spanKey]*proxypb.TraceSpan)
	var rpcSpans []*proxypb.TraceSpan
	for _, sp := range spans {
		if sp.Name == subID+" subscribe" {
			subByKey[spanKey{traceID: sp.TraceId, spanID: sp.SpanId}] = sp
		}
		if sp.Name == subID+" "+op {
			rpcSpans = append(rpcSpans, sp)
		}
	}
	for _, rpc := range rpcSpans {
		if cppMode && op != "modack" {
			continue
		}
		batchAttr, hasBatch := rpc.Attributes["messaging.batch.message_count"]
		if !hasBatch {
			t.Fatalf("%s %s span missing messaging.batch.message_count: %+v", subID, op, rpc.Attributes)
		}
		batchCnt := batchAttr.GetInt64Value()
		if batchCnt < 1 {
			t.Fatalf("%s %s span messaging.batch.message_count = %d, want >= 1", subID, op, batchCnt)
		}
		uniqueLinks := make(map[spanKey]struct{}, len(rpc.Links))
		for _, l := range rpc.Links {
			k := spanKey{traceID: l.TraceId, spanID: l.SpanId}
			if _, dup := uniqueLinks[k]; dup {
				t.Fatalf("%s %s span has duplicate forward link to (%s/%s)", subID, op, k.traceID, k.spanID)
			}
			uniqueLinks[k] = struct{}{}
			subSp, ok := subByKey[k]
			if !ok {
				t.Fatalf("%s %s span links to unknown subscribe span (%s/%s)", subID, op, k.traceID, k.spanID)
			}
			if cppMode && op == "modack" {
				if rpc.Kind != proxypb.SpanKind_SPAN_KIND_CLIENT {
					t.Fatalf("C++ %s modack expected SPAN_KIND_CLIENT, got %v", subID, rpc.Kind)
				}
				if len(subSp.Links) != 0 {
					t.Fatalf("C++ %s subscribe span expected 0 reverse links, got %d", subID, len(subSp.Links))
				}
				sawCppModackStart := false
				for _, ev := range subSp.Events {
					if ev.Name == "gl-cpp.modack_start" {
						sawCppModackStart = true
					}
				}
				if !sawCppModackStart {
					t.Fatalf("C++ %s subscribe span missing gl-cpp.modack_start event: %+v", subID, subSp.Events)
				}
			} else {
				var revMatches int
				for _, rl := range subSp.Links {
					if rl.TraceId == rpc.TraceId && rl.SpanId == rpc.SpanId {
						if gotOp := rl.Attributes["messaging.operation.name"].GetStringValue(); gotOp != op {
							t.Fatalf("%s subscribe reverse link has operation=%q, want %q", subID, gotOp, op)
						}
						revMatches++
					}
				}
				if revMatches != 1 {
					t.Fatalf("%s subscribe span (%s) expected 1 reverse link to %s span (%s), got %d", subID, subSp.SpanId, op, rpc.SpanId, revMatches)
				}
				var sawMatchingStart bool
				for _, ev := range subSp.Events {
					if ev.Name == op+" start" {
						evBC := ev.Attributes["messaging.batch.message_count"].GetInt64Value()
						if evBC == batchCnt && evBC >= int64(len(uniqueLinks)) {
							sawMatchingStart = true
						}
					}
				}
				if !sawMatchingStart {
					t.Fatalf("%s subscribe span (%s) missing %q event with messaging.batch.message_count=%d: %+v", subID, subSp.SpanId, op+" start", batchCnt, subSp.Events)
				}
			}
		}
		if batchCnt < int64(len(uniqueLinks)) {
			t.Fatalf("%s %s span messaging.batch.message_count=%d < uniqueLinks=%d", subID, op, batchCnt, len(uniqueLinks))
		}
	}
	return rpcSpans
}

func findSubSpanByAckID(spans []*proxypb.TraceSpan, subID, ackID string) *proxypb.TraceSpan {
	for _, sp := range spans {
		if sp.Name == subID+" subscribe" && sp.Attributes["messaging.gcp_pubsub.message.ack_id"].GetStringValue() == ackID {
			return sp
		}
	}
	return nil
}

func TestInfra_DuplicateAckIDSettlementBatchCount(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("GoALOSameBatchAck", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		inGateAck := make(chan struct{})
		releaseGateAck := make(chan struct{})
		var gateOnce sync.Once
		mock.mu.Lock()
		mock.onModAck = nil
		mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
			for _, id := range req.AckIds {
				if id == "gate-alo" {
					gateOnce.Do(func() { close(inGateAck) })
					select {
					case <-releaseGateAck:
					case <-actx.Done():
						return actx.Err()
					}
				}
			}
			return nil
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: false,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "gate-alo", Message: &pubsubpb.PubsubMessage{MessageId: "m-gate", Data: []byte("g")}},
						{AckId: "dup-ack-alo", Message: &pubsubpb.PubsubMessage{MessageId: "m-dup-1", Data: []byte("1")}},
						{AckId: "dup-ack-alo", Message: &pubsubpb.PubsubMessage{MessageId: "m-dup-2", Data: []byte("2")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         mock.addr,
					Subscription:     "projects/p/subscriptions/sub-dup-alo-ack",
					EnableMonitoring: true,
					AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var gateTok string
		var dupToks []string
		for gateTok == "" || len(dupToks) < 2 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv delivery: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				if md.MessageId == "m-gate" {
					gateTok = md.DeliveryToken
				} else {
					dupToks = append(dupToks, md.DeliveryToken)
				}
			}
		}

		// Freeze runSettlementLoop in mock.onAck on gate-alo so both dup settlements coalesce into one batch.
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{
					DeliveryToken: gateTok,
					Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
				},
			},
		}); err != nil {
			t.Fatalf("Settle gate: %v", err)
		}
		select {
		case <-inGateAck:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for gate Acknowledge")
		}
		for _, tok := range dupToks {
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{
						DeliveryToken: tok,
						Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
					},
				},
			}); err != nil {
				t.Fatalf("Settle dup %q: %v", tok, err)
			}
		}
		time.Sleep(40 * time.Millisecond)
		close(releaseGateAck)

		var results int
		for results < 3 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv settle: %v", rerr)
			}
			if sr := ev.GetSettlementResult(); sr != nil {
				if sr.Status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
					t.Fatalf("unexpected settlement failure: %+v", sr)
				}
				results++
			}
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}

		ackCalls := mock.getAckCalls()
		assertNoDuplicateWireAckIDs(t, ackCalls, mock.getModAckCalls())
		if len(ackCalls) != 2 || len(ackCalls[1].Req.AckIds) != 1 || ackCalls[1].Req.AckIds[0] != "dup-ack-alo" {
			t.Fatalf("expected 2 Acknowledge calls with 2nd=[dup-ack-alo], got %+v", ackCalls)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		ackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-alo-ack", "ack", false)
		var sawDupBatch2 bool
		for _, sp := range ackSpans {
			if len(sp.Links) == 2 && sp.Attributes["messaging.batch.message_count"].GetInt64Value() == 2 {
				sawDupBatch2 = true
			}
		}
		if !sawDupBatch2 {
			t.Fatalf("expected ack span with 2 links and messaging.batch.message_count=2, got %+v", ackSpans)
		}
	})

	t.Run("GoEODDuplicateAckIDReceiptModackSameBatchNackAndRetry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		sendDupBatch := make(chan struct{})
		inGateNack := make(chan struct{})
		releaseGateNack := make(chan struct{})
		var gateOnce sync.Once
		var dupTransientOnce atomic.Bool
		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
			if req.AckDeadlineSeconds == 0 {
				for _, id := range req.AckIds {
					if id == "gate-eod" {
						gateOnce.Do(func() { close(inGateNack) })
						select {
						case <-releaseGateNack:
						case <-actx.Done():
							return actx.Err()
						}
					}
					if id == "dup-nack-eod" && dupTransientOnce.CompareAndSwap(false, true) {
						st, err := status.New(codes.Unavailable, "transient dup nack").WithDetails(&errdetails.ErrorInfo{
							Reason:   "EXACTLY_ONCE_DELIVERY_FAILURE",
							Domain:   "pubsub.googleapis.com",
							Metadata: map[string]string{"dup-nack-eod": "TRANSIENT_FAILURE_UNAVAILABLE"},
						})
						if err != nil {
							return err
						}
						return st.Err()
					}
				}
			}
			return nil
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "gate-eod", Message: &pubsubpb.PubsubMessage{MessageId: "m-ge", Data: []byte("g")}},
					},
				})
				select {
				case <-sendDupBatch:
				case <-stream.Context().Done():
					return nil
				}
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "dup-nack-eod", Message: &pubsubpb.PubsubMessage{MessageId: "m-de-1", Data: []byte("1")}},
						{AckId: "dup-nack-eod", Message: &pubsubpb.PubsubMessage{MessageId: "m-de-2", Data: []byte("2")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         mock.addr,
					Subscription:     "projects/p/subscriptions/sub-dup-eod-nack",
					EnableMonitoring: true,
					AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var gateTok string
		for gateTok == "" {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv gate: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				gateTok = md.DeliveryToken
			}
		}
		close(sendDupBatch)

		var dupToks []string
		for len(dupToks) < 2 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv dup: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				dupToks = append(dupToks, md.DeliveryToken)
			}
		}

		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{
					DeliveryToken: gateTok,
					Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT,
				},
			},
		}); err != nil {
			t.Fatalf("Settle gate nack: %v", err)
		}
		select {
		case <-inGateNack:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for gate nack")
		}
		for _, tok := range dupToks {
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{
						DeliveryToken: tok,
						Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT,
					},
				},
			}); err != nil {
				t.Fatalf("Settle dup nack %q: %v", tok, err)
			}
		}
		time.Sleep(40 * time.Millisecond)
		close(releaseGateNack)

		var results int
		for results < 3 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv nack result: %v", rerr)
			}
			if sr := ev.GetSettlementResult(); sr != nil {
				if sr.Status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
					t.Fatalf("expected SUCCESS after EOD retry, got %+v", sr)
				}
				results++
			}
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}

		modAcks := mock.getModAckCalls()
		assertNoDuplicateWireAckIDs(t, mock.getAckCalls(), modAcks)
		var dupReceiptCalls, dupNackCalls int
		for _, ma := range modAcks {
			if len(ma.Req.AckIds) == 1 && ma.Req.AckIds[0] == "dup-nack-eod" {
				if ma.Req.AckDeadlineSeconds >= 60 {
					dupReceiptCalls++
				} else if ma.Req.AckDeadlineSeconds == 0 {
					dupNackCalls++
				}
			}
		}
		if dupReceiptCalls != 1 || dupNackCalls != 2 {
			t.Fatalf("expected 1 receipt modack and 2 retried nacks with 1 wire ack_id, got receipt=%d nack=%d", dupReceiptCalls, dupNackCalls)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		modSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-eod-nack", "modack", false)
		var sawReceiptBatch2 bool
		for _, sp := range modSpans {
			if sp.Attributes["messaging.gcp_pubsub.is_receipt_modack"].GetBoolValue() &&
				len(sp.Links) == 2 && sp.Attributes["messaging.batch.message_count"].GetInt64Value() == 2 {
				sawReceiptBatch2 = true
			}
		}
		if !sawReceiptBatch2 {
			t.Fatalf("expected EOD receipt modack span with 2 links and batch_count=2, got %+v", modSpans)
		}
		nackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-eod-nack", "nack", false)
		var dupNackErrBatch2, dupNackOKBatch2 int
		for _, sp := range nackSpans {
			if len(sp.Links) == 2 && sp.Attributes["messaging.batch.message_count"].GetInt64Value() == 2 {
				if sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
					var hasEx bool
					for _, ev := range sp.Events {
						if ev.Name == "exception" {
							hasEx = true
						}
					}
					if !hasEx {
						t.Fatalf("expected exception event on retried EOD nack span %s: %+v", sp.SpanId, sp.Events)
					}
					dupNackErrBatch2++
				} else {
					dupNackOKBatch2++
				}
			}
		}
		if dupNackErrBatch2 != 1 || dupNackOKBatch2 != 0 {
			t.Fatalf("expected 1 single-attempt ERROR nack span (0 retry nack spans) with 2 links & batch_count=2, got err=%d ok=%d", dupNackErrBatch2, dupNackOKBatch2)
		}
		var dupSubCnt int
		for _, sp := range drained.Spans {
			if sp.Name == "sub-dup-eod-nack subscribe" && sp.Attributes["messaging.gcp_pubsub.message.ack_id"].GetStringValue() == "dup-nack-eod" {
				dupSubCnt++
				if sp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
					t.Fatalf("expected UNSET on recovered dup-nack-eod subscribe span %s, got %v", sp.SpanId, sp.StatusCode)
				}
				if got := sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue(); got != "nacked" {
					t.Fatalf("expected result=nacked on dup-nack-eod subscribe span %s, got %q", sp.SpanId, got)
				}
				var startsBatch2, ends int
				for _, ev := range sp.Events {
					if ev.Name == "nack start" && ev.Attributes["messaging.batch.message_count"].GetInt64Value() == 2 {
						startsBatch2++
					}
					if ev.Name == "nack end" {
						ends++
					}
				}
				if startsBatch2 != 1 || ends != 2 {
					t.Fatalf("expected 1 \"nack start\" (batch_count=2) and 2 \"nack end\" on dup-nack-eod subscribe span %s, got starts=%d ends=%d events=%+v", sp.SpanId, startsBatch2, ends, sp.Events)
				}
			}
		}
		if dupSubCnt != 2 {
			t.Fatalf("expected 2 dup-nack-eod subscribe spans, got %d", dupSubCnt)
		}
	})

	t.Run("GoALOBulkNackOnShutdown", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = nil
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: false,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "dup-bn", Message: &pubsubpb.PubsubMessage{MessageId: "m-bn-1", Data: []byte("1")}},
						{AckId: "dup-bn", Message: &pubsubpb.PubsubMessage{MessageId: "m-bn-2", Data: []byte("2")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         mock.addr,
					Subscription:     "projects/p/subscriptions/sub-dup-alo-bn",
					EnableMonitoring: true,
					AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var got int
		for got < 2 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv delivery: %v", rerr)
			}
			if ev.GetMessageDelivered() != nil {
				got++
			}
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY},
			},
		}); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}

		modAcks := mock.getModAckCalls()
		assertNoDuplicateWireAckIDs(t, mock.getAckCalls(), modAcks)
		if len(modAcks) != 1 || modAcks[0].Req.AckDeadlineSeconds != 0 || len(modAcks[0].Req.AckIds) != 1 || modAcks[0].Req.AckIds[0] != "dup-bn" {
			t.Fatalf("expected 1 ModifyAckDeadline(0) with [dup-bn], got %+v", modAcks)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		nackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-alo-bn", "nack", false)
		if len(nackSpans) != 1 || len(nackSpans[0].Links) != 2 || nackSpans[0].Attributes["messaging.batch.message_count"].GetInt64Value() != 2 {
			t.Fatalf("expected 1 nack span with 2 links and batch_count=2, got %+v", nackSpans)
		}
	})

	t.Run("GoAndCppPeriodicLeaseModAck", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			mode    string
			subID   string
			cppMode bool
		}{
			{name: "Go", mode: "--mode=conformant-go", subID: "sub-dup-ext-go", cppMode: false},
			{name: "Cpp", mode: "--mode=conformant-cpp-partial", subID: "sub-dup-ext-cpp", cppMode: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				h := startTestProxyDirect(t, tc.mode)
				defer h.Shutdown(t, 2*time.Second)
				_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

				mock.resetCounters()
				extSeen := make(chan struct{}, 1)
				mock.mu.Lock()
				mock.onAck = nil
				mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
					if req.AckDeadlineSeconds > 0 {
						select {
						case extSeen <- struct{}{}:
						default:
						}
					}
					return nil
				}
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
					if idx == 1 {
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
								ExactlyOnceDeliveryEnabled: false,
							},
							ReceivedMessages: []*pubsubpb.ReceivedMessage{
								{AckId: "dup-ext", Message: &pubsubpb.PubsubMessage{MessageId: "m-ext-1", Data: []byte("1")}},
								{AckId: "dup-ext", Message: &pubsubpb.PubsubMessage{MessageId: "m-ext-2", Data: []byte("2")}},
							},
						})
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()

				sess, err := h.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("SubscriberSession: %v", err)
				}
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:                     mock.addr,
							Subscription:                 "projects/p/subscriptions/" + tc.subID,
							EnableMonitoring:             true,
							MaxDurationPerAckExtensionMs: protoInt64(150),
							AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
						},
					},
				}); err != nil {
					t.Fatalf("Start: %v", err)
				}

				var got int
				for got < 2 {
					ev, rerr := sess.Recv()
					if rerr != nil {
						t.Fatalf("Recv delivery: %v", rerr)
					}
					if ev.GetMessageDelivered() != nil {
						got++
					}
				}
				select {
				case <-extSeen:
				case <-time.After(3 * time.Second):
					t.Fatalf("timed out waiting for periodic modack extension")
				}
				_ = sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Stop{
						Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY},
					},
				})
				_ = sess.CloseSend()
				for {
					if _, err := sess.Recv(); err != nil {
						break
					}
				}

				modAcks := mock.getModAckCalls()
				assertNoDuplicateWireAckIDs(t, mock.getAckCalls(), modAcks)
				var sawWireExt bool
				for _, ma := range modAcks {
					if ma.Req.AckDeadlineSeconds > 0 && len(ma.Req.AckIds) == 1 && ma.Req.AckIds[0] == "dup-ext" {
						sawWireExt = true
					}
				}
				if !sawWireExt {
					t.Fatalf("expected periodic ModifyAckDeadline with 1 wire ack_id [dup-ext], got %+v", modAcks)
				}

				drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				if err != nil {
					t.Fatalf("DrainTraces: %v", err)
				}
				modSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, tc.subID, "modack", tc.cppMode)
				var sawBatch2 bool
				for _, sp := range modSpans {
					if len(sp.Links) == 2 && sp.Attributes["messaging.batch.message_count"].GetInt64Value() == 2 {
						sawBatch2 = true
					}
				}
				if !sawBatch2 {
					t.Fatalf("expected %s periodic modack span with 2 links and batch_count=2, got %+v", tc.name, modSpans)
				}
			})
		}
	})

	t.Run("Over1000BatchSplitWithDuplicateAckID", func(t *testing.T) {
		build1001Messages := func() []*pubsubpb.ReceivedMessage {
			rms := make([]*pubsubpb.ReceivedMessage, 0, 1001)
			for i := 0; i < 1000; i++ {
				rms = append(rms, &pubsubpb.ReceivedMessage{
					AckId:   fmt.Sprintf("u-%04d", i),
					Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("m-%04d", i), Data: []byte("x")},
				})
			}
			rms = append(rms, &pubsubpb.ReceivedMessage{
				AckId:   "u-0000",
				Message: &pubsubpb.PubsubMessage{MessageId: "m-dup-0000", Data: []byte("x")},
			})
			return rms
		}

		t.Run("BulkNack1001With1000Unique", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			h := startTestProxyDirect(t, "--mode=conformant-go")
			defer h.Shutdown(t, 2*time.Second)
			_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

			mock.resetCounters()
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = nil
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
				if idx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
							ExactlyOnceDeliveryEnabled: false,
						},
						ReceivedMessages: build1001Messages(),
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("SubscriberSession: %v", err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:               mock.addr,
						Subscription:           "projects/p/subscriptions/sub-dup-1001-bn",
						EnableMonitoring:       true,
						MaxOutstandingMessages: protoInt64(2000),
						AutoSettleMode:         proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				t.Fatalf("Start: %v", err)
			}
			var got int
			for got < 1001 {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("Recv: %v", rerr)
				}
				if ev.GetMessageDelivered() != nil {
					got++
				}
			}
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY},
				},
			})
			_ = sess.CloseSend()
			for {
				if _, err := sess.Recv(); err != nil {
					break
				}
			}

			modAcks := mock.getModAckCalls()
			assertNoDuplicateWireAckIDs(t, mock.getAckCalls(), modAcks)
			if len(modAcks) != 1 || len(modAcks[0].Req.AckIds) != 1000 || modAcks[0].Req.AckDeadlineSeconds != 0 {
				t.Fatalf("expected 1 ModifyAckDeadline(0) call with 1000 unique AckIds, got calls=%d", len(modAcks))
			}

			drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("DrainTraces: %v", err)
			}
			nackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-1001-bn", "nack", false)
			if len(nackSpans) != 1 || len(nackSpans[0].Links) != 1001 || nackSpans[0].Attributes["messaging.batch.message_count"].GetInt64Value() != 1001 {
				t.Fatalf("expected 1 nack span with 1001 links and batch_count=1001, got spans=%d links=%d batch=%d",
					len(nackSpans), len(nackSpans[0].Links), nackSpans[0].Attributes["messaging.batch.message_count"].GetInt64Value())
			}
		})

		t.Run("Acknowledge1001With1000Unique", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			h := startTestProxyDirect(t, "--mode=conformant-go")
			defer h.Shutdown(t, 2*time.Second)
			_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

			mock.resetCounters()
			sendBatch2 := make(chan struct{})
			inGateAck := make(chan struct{})
			releaseGateAck := make(chan struct{})
			var gateOnce sync.Once
			mock.mu.Lock()
			mock.onModAck = nil
			mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
				for _, id := range req.AckIds {
					if id == "gate-1001" {
						gateOnce.Do(func() { close(inGateAck) })
						select {
						case <-releaseGateAck:
						case <-actx.Done():
							return actx.Err()
						}
					}
				}
				return nil
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
				if idx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
							ExactlyOnceDeliveryEnabled: false,
						},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: "gate-1001", Message: &pubsubpb.PubsubMessage{MessageId: "m-gate-1001", Data: []byte("g")}},
						},
					})
					select {
					case <-sendBatch2:
					case <-stream.Context().Done():
						return nil
					}
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
							ExactlyOnceDeliveryEnabled: false,
						},
						ReceivedMessages: build1001Messages(),
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("SubscriberSession: %v", err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:               mock.addr,
						Subscription:           "projects/p/subscriptions/sub-dup-1001-ack",
						EnableMonitoring:       true,
						MaxOutstandingMessages: protoInt64(2000),
						AutoSettleMode:         proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				t.Fatalf("Start: %v", err)
			}

			var gateTok string
			for gateTok == "" {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("Recv gate: %v", rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					gateTok = md.DeliveryToken
				}
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{
						DeliveryToken: gateTok,
						Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
					},
				},
			}); err != nil {
				t.Fatalf("Settle gate: %v", err)
			}
			select {
			case <-inGateAck:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for gate Acknowledge")
			}

			close(sendBatch2)
			toks := make([]string, 0, 1001)
			for len(toks) < 1001 {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("Recv batch2 (%d): %v", len(toks), rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					toks = append(toks, md.DeliveryToken)
				}
			}
			for _, tok := range toks {
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Settle{
						Settle: &proxypb.SettleMessageCommand{
							DeliveryToken: tok,
							Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
						},
					},
				}); err != nil {
					t.Fatalf("Settle %q: %v", tok, err)
				}
			}
			time.Sleep(250 * time.Millisecond)
			close(releaseGateAck)

			var results int
			for results < 1002 {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("Recv results (%d): %v", results, rerr)
				}
				if sr := ev.GetSettlementResult(); sr != nil {
					if sr.Status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
						t.Fatalf("unexpected settlement failure: %+v", sr)
					}
					results++
				}
			}
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			})
			_ = sess.CloseSend()
			for {
				if _, err := sess.Recv(); err != nil {
					break
				}
			}

			ackCalls := mock.getAckCalls()
			assertNoDuplicateWireAckIDs(t, ackCalls, mock.getModAckCalls())
			if len(ackCalls) != 2 || len(ackCalls[1].Req.AckIds) != 1000 {
				t.Fatalf("expected 2 Acknowledge calls with 2nd having 1000 unique AckIds, got calls=%d", len(ackCalls))
			}

			drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("DrainTraces: %v", err)
			}
			ackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-dup-1001-ack", "ack", false)
			var sawBatch1001 bool
			for _, sp := range ackSpans {
				if len(sp.Links) == 1001 && sp.Attributes["messaging.batch.message_count"].GetInt64Value() == 1001 {
					sawBatch1001 = true
				}
			}
			if !sawBatch1001 {
				t.Fatalf("expected ack span with 1001 links and batch_count=1001")
			}
		})
	})
}

func TestInfra_GoALOAndEODNackFailureAndMidStreamEODToggle(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("GoALOVsEODBulkNackOnShutdownFailure", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			subName string
			eod     bool
		}{
			{name: "ALO", subName: "sub-alo-bn-fail", eod: false},
			{name: "EOD", subName: "sub-eod-bn-fail", eod: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				h := startTestProxyDirect(t, "--mode=conformant-go")
				defer h.Shutdown(t, 2*time.Second)
				_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

				mock.resetCounters()
				mock.mu.Lock()
				mock.onAck = nil
				mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
					if req.AckDeadlineSeconds == 0 {
						return status.Error(codes.PermissionDenied, "shutdown nack fail")
					}
					return nil
				}
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
					if idx == 1 {
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
								ExactlyOnceDeliveryEnabled: tc.eod,
							},
							ReceivedMessages: []*pubsubpb.ReceivedMessage{
								{AckId: tc.name + "-bn-1", Message: &pubsubpb.PubsubMessage{MessageId: tc.name + "-m1", Data: []byte("x")}},
							},
						})
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()

				sess, err := h.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("SubscriberSession: %v", err)
				}
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:         mock.addr,
							Subscription:     "projects/p/subscriptions/" + tc.subName,
							EnableMonitoring: true,
							AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
						},
					},
				}); err != nil {
					t.Fatalf("Start: %v", err)
				}

				for {
					ev, rerr := sess.Recv()
					if rerr != nil {
						t.Fatalf("Recv: %v", rerr)
					}
					if md := ev.GetMessageDelivered(); md != nil {
						if md.IsExactlyOnce != tc.eod {
							t.Fatalf("%s IsExactlyOnce=%v, want %v", tc.name, md.IsExactlyOnce, tc.eod)
						}
						break
					}
				}
				_ = sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Stop{
						Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY},
					},
				})
				_ = sess.CloseSend()
				for {
					if _, err := sess.Recv(); err != nil {
						break
					}
				}

				drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				if err != nil {
					t.Fatalf("DrainTraces: %v", err)
				}
				subSp := findSubSpanByAckID(drained.Spans, tc.subName, tc.name+"-bn-1")
				if subSp == nil || subSp.EndTime == nil {
					t.Fatalf("%s missing closed subscribe span: %+v", tc.name, subSp)
				}
				if tc.eod {
					if _, hasRes := subSp.Attributes["messaging.gcp_pubsub.result"]; hasRes {
						t.Fatalf("EOD failed shutdown nack should omit result, got %q", subSp.Attributes["messaging.gcp_pubsub.result"].GetStringValue())
					}
					if subSp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
						t.Fatalf("EOD failed shutdown nack expected ERROR status, got %v", subSp.StatusCode)
					}
				} else {
					if got := subSp.Attributes["messaging.gcp_pubsub.result"].GetStringValue(); got != "nacked" {
						t.Fatalf("Go ALO failed shutdown nack expected result=\"nacked\", got %q", got)
					}
					if subSp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
						t.Fatalf("Go ALO failed shutdown nack expected SPAN_STATUS_CODE_UNSET, got %v (msg=%q)", subSp.StatusCode, subSp.StatusMessage)
					}
				}
				nackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, tc.subName, "nack", false)
				if len(nackSpans) != 1 || nackSpans[0].StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
					t.Fatalf("expected 1 failed nack span with ERROR status, got %+v", nackSpans)
				}
			})
		}
	})

	t.Run("GoALONackAndCloseRemainingPreDispatchFailure", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			subName string
			eod     bool
		}{
			{name: "ALO", subName: "sub-alo-rem-fail", eod: false},
			{name: "EOD", subName: "sub-eod-rem-fail", eod: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				h := startTestProxyDirect(t, "--mode=conformant-go")
				defer h.Shutdown(t, 2*time.Second)
				_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

				remAck1 := tc.name + "-rem-1"
				remAck2 := tc.name + "-rem-2"
				mock.resetCounters()
				mock.mu.Lock()
				mock.onAck = nil
				mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
					if req.AckDeadlineSeconds == 0 {
						for _, id := range req.AckIds {
							if id == remAck2 {
								return status.Error(codes.PermissionDenied, "nack-and-close-remaining fail")
							}
						}
					}
					return nil
				}
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
					if idx == 1 {
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
								ExactlyOnceDeliveryEnabled: tc.eod,
							},
							ReceivedMessages: []*pubsubpb.ReceivedMessage{
								{AckId: remAck1, Message: &pubsubpb.PubsubMessage{MessageId: tc.name + "-rm1", Data: []byte("1")}},
								{AckId: remAck2, Message: &pubsubpb.PubsubMessage{MessageId: tc.name + "-rm2", Data: []byte("2")}},
							},
						})
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()

				sess, err := h.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("SubscriberSession: %v", err)
				}
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:               mock.addr,
							Subscription:           "projects/p/subscriptions/" + tc.subName,
							EnableMonitoring:       true,
							MaxOutstandingMessages: protoInt64(1),
							AutoSettleMode:         proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
						},
					},
				}); err != nil {
					t.Fatalf("Start: %v", err)
				}

				for {
					ev, rerr := sess.Recv()
					if rerr != nil {
						t.Fatalf("Recv: %v", rerr)
					}
					if md := ev.GetMessageDelivered(); md != nil {
						if md.MessageId != tc.name+"-rm1" {
							t.Fatalf("expected first delivered msg %s-rm1, got %q", tc.name, md.MessageId)
						}
						break
					}
				}
				// Message #2 is blocked on MaxOutstandingMessages=1 before dispatchDelivery; Stop cancels pullCtx.
				_ = sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Stop{
						Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY},
					},
				})
				_ = sess.CloseSend()
				var stopped *proxypb.StoppedEvent
				for {
					ev, rerr := sess.Recv()
					if rerr != nil {
						break
					}
					if st := ev.GetStopped(); st != nil {
						stopped = st
					}
				}
				if stopped == nil || stopped.TotalDelivered != 1 {
					t.Fatalf("expected StoppedEvent with TotalDelivered=1 (rem2 never dispatched), got %+v", stopped)
				}

				drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				if err != nil {
					t.Fatalf("DrainTraces: %v", err)
				}
				rem2Sub := findSubSpanByAckID(drained.Spans, tc.subName, remAck2)
				if rem2Sub == nil || rem2Sub.EndTime == nil {
					t.Fatalf("%s missing closed subscribe span for %s: %+v", tc.name, remAck2, rem2Sub)
				}
				if tc.eod {
					if _, hasRes := rem2Sub.Attributes["messaging.gcp_pubsub.result"]; hasRes {
						t.Fatalf("EOD nackAndCloseRemaining failure should omit result, got %q", rem2Sub.Attributes["messaging.gcp_pubsub.result"].GetStringValue())
					}
					if rem2Sub.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
						t.Fatalf("EOD nackAndCloseRemaining failure expected ERROR status, got %v", rem2Sub.StatusCode)
					}
				} else {
					if got := rem2Sub.Attributes["messaging.gcp_pubsub.result"].GetStringValue(); got != "nacked" {
						t.Fatalf("Go ALO nackAndCloseRemaining failure expected result=\"nacked\", got %q", got)
					}
					if rem2Sub.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
						t.Fatalf("Go ALO nackAndCloseRemaining failure expected SPAN_STATUS_CODE_UNSET, got %v (msg=%q)", rem2Sub.StatusCode, rem2Sub.StatusMessage)
					}
				}
				nackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, tc.subName, "nack", false)
				var sawRem2Err bool
				for _, sp := range nackSpans {
					if sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
						for _, l := range sp.Links {
							if l.SpanId == rem2Sub.SpanId {
								sawRem2Err = true
							}
						}
					}
				}
				if !sawRem2Err {
					t.Fatalf("expected failed nack span linked to %s", remAck2)
				}
			})
		}
	})

	t.Run("MidStreamEODToggleBufferedBatchesAndInterleavedSettlement", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		inB1Receipt := make(chan struct{})
		releaseB1Receipt := make(chan struct{})
		sendRemainingBatches := make(chan struct{})
		inAckGate := make(chan struct{})
		releaseAckGate := make(chan struct{})
		var b1Once, ackGateOnce sync.Once

		mock.mu.Lock()
		mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
			if req.AckDeadlineSeconds > 0 {
				for _, id := range req.AckIds {
					if id == "b1-eod" {
						b1Once.Do(func() { close(inB1Receipt) })
						select {
						case <-releaseB1Receipt:
						case <-actx.Done():
							return actx.Err()
						}
					}
				}
				return nil
			}
			return status.Error(codes.PermissionDenied, "mixed shutdown nack fail")
		}
		mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
			for _, id := range req.AckIds {
				if id == "b3-eod-ack" {
					ackGateOnce.Do(func() { close(inAckGate) })
					select {
					case <-releaseAckGate:
					case <-actx.Done():
						return actx.Err()
					}
				}
			}
			return status.Error(codes.PermissionDenied, "mixed ack fail")
		}
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
			if idx == 1 {
				// Batch 1: EOD=true, Ordering=true
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
						MessageOrderingEnabled:     true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "b1-eod", Message: &pubsubpb.PubsubMessage{MessageId: "m-b1", OrderingKey: "k1", Data: []byte("1")}},
					},
				})
				select {
				case <-sendRemainingBatches:
				case <-stream.Context().Done():
					return nil
				}
				// Batch 2: toggles EOD=false, Ordering=false (buffered in batchCh while Batch 1 blocks in receipt modack)
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: false,
						MessageOrderingEnabled:     false,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "b2-alo-ack", Message: &pubsubpb.PubsubMessage{MessageId: "m-b2-1", OrderingKey: "k1", Data: []byte("2a")}},
						{AckId: "b2-alo-nack", Message: &pubsubpb.PubsubMessage{MessageId: "m-b2-2", OrderingKey: "k1", Data: []byte("2b")}},
					},
				})
				// Batch 3: toggles EOD=true, Ordering=true BEFORE Batch 1 or Batch 2 runs!
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
						MessageOrderingEnabled:     true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "b3-eod-ack", Message: &pubsubpb.PubsubMessage{MessageId: "m-b3-1", Data: []byte("3a")}},
						{AckId: "b3-eod-nack", Message: &pubsubpb.PubsubMessage{MessageId: "m-b3-2", Data: []byte("3b")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         mock.addr,
					Subscription:     "projects/p/subscriptions/sub-eod-toggle",
					EnableMonitoring: true,
					AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait until Batch 1 blocks in EOD receipt modack, then send Batch 2 (EOD=false) & Batch 3 (EOD=true)
		// into batchCh BEFORE releasing Batch 1.
		select {
		case <-inB1Receipt:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for Batch 1 EOD receipt modack")
		}
		close(sendRemainingBatches)
		time.Sleep(30 * time.Millisecond)
		close(releaseB1Receipt)

		delivered := make(map[string]*proxypb.MessageDeliveredEvent, 5)
		for len(delivered) < 5 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv delivery (%d): %v", len(delivered), rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				delivered[md.MessageId] = md
			}
		}

		wantEOD := map[string]bool{
			"m-b1":   true,
			"m-b2-1": false,
			"m-b2-2": false,
			"m-b3-1": true,
			"m-b3-2": true,
		}
		for msgID, want := range wantEOD {
			if delivered[msgID] == nil || delivered[msgID].IsExactlyOnce != want {
				t.Fatalf("delivery %s IsExactlyOnce=%v, want %v (ev=%+v)", msgID, delivered[msgID].GetIsExactlyOnce(), want, delivered[msgID])
			}
		}

		// Freeze runSettlementLoop on m-b3-1, then interleave m-b1 (EOD=true) + m-b2-1 (EOD=false) into pendingAcks.
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{
					DeliveryToken: delivered["m-b3-1"].DeliveryToken,
					Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
				},
			},
		}); err != nil {
			t.Fatalf("Settle m-b3-1: %v", err)
		}
		select {
		case <-inAckGate:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for m-b3-1 Acknowledge")
		}
		for _, id := range []string{"m-b1", "m-b2-1"} {
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{
						DeliveryToken: delivered[id].DeliveryToken,
						Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
					},
				},
			}); err != nil {
				t.Fatalf("Settle %s: %v", id, err)
			}
		}
		time.Sleep(40 * time.Millisecond)
		close(releaseAckGate)

		var settleResults int
		for settleResults < 3 {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv settle result: %v", rerr)
			}
			if sr := ev.GetSettlementResult(); sr != nil {
				switch sr.DeliveryToken {
				case delivered["m-b2-1"].DeliveryToken:
					if sr.Status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS || sr.GrpcErrorCode != 0 || sr.ErrorMessage != "" {
						t.Fatalf("expected ALO m-b2-1 wantResult=true to emit ACK_STATUS_SUCCESS, got %+v", sr)
					}
				case delivered["m-b1"].DeliveryToken, delivered["m-b3-1"].DeliveryToken:
					if sr.Status != proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED {
						t.Fatalf("expected EOD PERMISSION_DENIED, got %+v", sr)
					}
				default:
					t.Fatalf("unexpected settlement result delivery token: %+v", sr)
				}
				settleResults++
			}
		}

		// Fast shutdown nacks remaining mixed messages: m-b2-2 (EOD=false) and m-b3-2 (EOD=true).
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, err := sess.Recv(); err != nil {
				break
			}
		}

		// Verify ALO Batch 2 never received EOD receipt modacks, while EOD Batches 1 and 3 did.
		modAcks := mock.getModAckCalls()
		for _, ma := range modAcks {
			if ma.Req.AckDeadlineSeconds > 0 {
				for _, id := range ma.Req.AckIds {
					if id == "b2-alo-ack" || id == "b2-alo-nack" {
						t.Fatalf("ALO batch 2 message %q unexpectedly received EOD receipt modack", id)
					}
				}
			}
		}

		// Verify mixed EOD/ALO settlement batch split into separate Acknowledge calls for b1-eod and b2-alo-ack.
		ackCalls := mock.getAckCalls()
		assertNoDuplicateWireAckIDs(t, ackCalls, modAcks)
		if len(ackCalls) != 3 {
			t.Fatalf("expected 3 Acknowledge calls (b3-eod-ack gate + split b1-eod + split b2-alo-ack), got %d (%+v)", len(ackCalls), ackCalls)
		}
		for i, ac := range ackCalls {
			if len(ac.Req.AckIds) != 1 {
				t.Fatalf("expected split Acknowledge call[%d] to have 1 ack_id, got %+v", i, ac.Req.AckIds)
			}
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-eod-toggle", "ack", false)
		assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-eod-toggle", "modack", false)
		nackSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, "sub-eod-toggle", "nack", false)
		if len(nackSpans) != 1 || len(nackSpans[0].Links) != 2 || nackSpans[0].Attributes["messaging.batch.message_count"].GetInt64Value() != 2 {
			t.Fatalf("expected 1 mixed shutdown nack span with 2 links and batch_count=2, got %+v", nackSpans)
		}

		// Check per-batch subscribe span attributes & error/result classification.
		specs := []struct {
			ackID      string
			eod        bool
			wantResult string // empty => must be omitted
			wantErr    bool
		}{
			{ackID: "b1-eod", eod: true, wantResult: "", wantErr: true},
			{ackID: "b2-alo-ack", eod: false, wantResult: "acked", wantErr: false},
			{ackID: "b3-eod-ack", eod: true, wantResult: "", wantErr: true},
			{ackID: "b2-alo-nack", eod: false, wantResult: "nacked", wantErr: false},
			{ackID: "b3-eod-nack", eod: true, wantResult: "", wantErr: true},
		}
		for _, spSpec := range specs {
			subSp := findSubSpanByAckID(drained.Spans, "sub-eod-toggle", spSpec.ackID)
			if subSp == nil || subSp.EndTime == nil {
				t.Fatalf("missing closed subscribe span for %q", spSpec.ackID)
			}
			if gotEOD := subSp.Attributes["messaging.gcp_pubsub.exactly_once_delivery"].GetBoolValue(); gotEOD != spSpec.eod {
				t.Fatalf("%q exactly_once_delivery=%v, want %v", spSpec.ackID, gotEOD, spSpec.eod)
			}
			resAttr, hasRes := subSp.Attributes["messaging.gcp_pubsub.result"]
			if spSpec.wantResult == "" {
				if hasRes {
					t.Fatalf("%q expected omitted result on EOD failure, got %q", spSpec.ackID, resAttr.GetStringValue())
				}
			} else {
				if !hasRes || resAttr.GetStringValue() != spSpec.wantResult {
					t.Fatalf("%q expected result=%q, got %q (present=%v)", spSpec.ackID, spSpec.wantResult, resAttr.GetStringValue(), hasRes)
				}
			}
			wantStatus := proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			if spSpec.wantErr {
				wantStatus = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			}
			if subSp.StatusCode != wantStatus {
				t.Fatalf("%q expected status=%v, got %v (msg=%q)", spSpec.ackID, wantStatus, subSp.StatusCode, subSp.StatusMessage)
			}
		}
	})
}

func TestInfra_E2EM1BSettlementLeaseAndValidationFixes(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("BugA_ALOWantResultSuccessAndFaultRawStatus", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		runCase := func(extraArgs []string, wantStatus proxypb.SettlementResultEvent_AckStatus, wantCode int32) {
			t.Helper()
			args := append([]string{"--mode=conformant-go"}, extraArgs...)
			h := startTestProxyDirect(t, args...)
			defer h.Shutdown(t, 2*time.Second)

			mock.resetCounters()
			mock.mu.Lock()
			mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
				return status.Error(codes.PermissionDenied, "denied-ack")
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
							ExactlyOnceDeliveryEnabled: false,
						},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: "buga-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-buga-1", Data: []byte("a")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()
			defer func() {
				mock.mu.Lock()
				mock.onAck = nil
				mock.onStreamPull = nil
				mock.mu.Unlock()
			}()

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("SubscriberSession: %v", err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:         mock.addr,
						Subscription:     "projects/p/subscriptions/sub-buga",
						EnableMonitoring: true,
						AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT,
					},
				},
			}); err != nil {
				t.Fatalf("Start: %v", err)
			}

			var sr *proxypb.SettlementResultEvent
			for sr == nil {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("Recv: %v", rerr)
				}
				sr = ev.GetSettlementResult()
			}
			if sr.Status != wantStatus || sr.GrpcErrorCode != wantCode {
				t.Fatalf("got settlement %+v, want status=%v code=%d", sr, wantStatus, wantCode)
			}

			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			})
			_ = sess.CloseSend()
			for {
				if _, rerr := sess.Recv(); rerr != nil {
					break
				}
			}

			drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("DrainTraces: %v", err)
			}
			var sawRPCErr, sawSubOK bool
			for _, sp := range drained.Spans {
				if sp.Name == "sub-buga ack" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
					sawRPCErr = true
				}
				if sp.Name == "sub-buga subscribe" && sp.StatusCode == proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET &&
					sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() == "acked" {
					sawSubOK = true
				}
			}
			if !sawRPCErr || !sawSubOK {
				t.Fatalf("expected ack RPC ERROR + ALO subscribe UNSET/acked spans; rpcErr=%v subOK=%v", sawRPCErr, sawSubOK)
			}
		}

		runCase(nil, proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS, 0)
		runCase([]string{"--fault=eod02-noneod-raw-status"}, proxypb.SettlementResultEvent_ACK_STATUS_PERMISSION_DENIED, int32(codes.PermissionDenied))
	})

	t.Run("BugB_AutoAckWithResultBatchCoalescing", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		for _, mode := range []string{"conformant-go", "conformant-cpp-partial"} {
			mode := mode
			t.Run(mode, func(t *testing.T) {
				h := startTestProxyDirect(t, "--mode="+mode)
				defer h.Shutdown(t, 2*time.Second)

				mock.resetCounters()
				mock.mu.Lock()
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
					if streamIdx == 1 {
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							ReceivedMessages: []*pubsubpb.ReceivedMessage{
								{AckId: "b-1", Message: &pubsubpb.PubsubMessage{MessageId: "mb-1", Data: []byte("1")}},
								{AckId: "b-2", Message: &pubsubpb.PubsubMessage{MessageId: "mb-2", Data: []byte("2")}},
								{AckId: "b-3", Message: &pubsubpb.PubsubMessage{MessageId: "mb-3", Data: []byte("3")}},
							},
						})
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()
				defer func() {
					mock.mu.Lock()
					mock.onStreamPull = nil
					mock.mu.Unlock()
				}()

				sess, err := h.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("SubscriberSession: %v", err)
				}
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:       mock.addr,
							Subscription:   "projects/p/subscriptions/sub-bugb",
							AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK_WITH_RESULT,
						},
					},
				}); err != nil {
					t.Fatalf("Start: %v", err)
				}

				results := 0
				for results < 3 {
					ev, rerr := sess.Recv()
					if rerr != nil {
						t.Fatalf("Recv: %v", rerr)
					}
					if sr := ev.GetSettlementResult(); sr != nil {
						if sr.Status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
							t.Fatalf("unexpected settlement status: %+v", sr)
						}
						results++
					}
				}
				_ = sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Stop{
						Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
					},
				})
				_ = sess.CloseSend()
				for {
					if _, rerr := sess.Recv(); rerr != nil {
						break
					}
				}

				acks := mock.getAckCalls()
				if len(acks) != 1 || len(acks[0].Req.AckIds) != 3 {
					t.Fatalf("%s expected 3 AUTO_ACK_WITH_RESULT items to coalesce into 1 Acknowledge call, got %d (%+v)", mode, len(acks), acks)
				}
			})
		}
	})

	t.Run("BugC_ShortNamesAndPathValidationAndOverride", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)
		mock.resetCounters()
		mock.mu.Lock()
		mock.onPublish = nil
		mock.onPull = nil
		mock.onStreamPull = nil
		mock.mu.Unlock()

		invalidTopics := []string{"", "t", "projects//topics/t", "projects/p/topics/t/extra", "projects/p/subscriptions/s"}
		for _, top := range invalidTopics {
			_, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
				Endpoint: mock.addr,
				Topic:    top,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CreatePublisher(%q) with empty ProjectId expected InvalidArgument, got %v", top, err)
			}
		}

		invalidSubs := []string{"", "s", "projects//subscriptions/s", "projects/p/subscriptions/s/extra", "projects/p/topics/t"}
		for _, sub := range invalidSubs {
			_, err := h.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
				Endpoint:     mock.addr,
				Subscription: sub,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("UnaryPull(%q) with empty ProjectId expected InvalidArgument, got %v", sub, err)
			}

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("SubscriberSession: %v", err)
			}
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:     mock.addr,
						Subscription: sub,
					},
				},
			})
			_, rerr := sess.Recv()
			if status.Code(rerr) != codes.InvalidArgument {
				t.Fatalf("SubscriberSession(%q) with empty ProjectId expected InvalidArgument, got %v", sub, rerr)
			}
		}
		if len(mock.getPublishCalls()) != 0 || len(mock.getPullCalls()) != 0 || len(mock.getStreamInits()) != 0 {
			t.Fatalf("expected 0 wire RPCs on invalid resource names")
		}

		// Cross-project path + ProjectId override (CFG-04): wire keeps path, trace gets ProjectId.
		pub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         mock.addr,
			Topic:            "projects/public-data/topics/cross-t",
			ProjectId:        "billing-proj",
			EnableMonitoring: true,
			CountThreshold:   protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher cross-project: %v", err)
		}
		defer func() {
			_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pub.PublisherId})
		}()
		if _, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pub.PublisherId, Data: []byte("hi")}); err != nil {
			t.Fatalf("Publish cross-project: %v", err)
		}
		pcalls := mock.getPublishCalls()
		if len(pcalls) != 1 || pcalls[0].Req.Topic != "projects/public-data/topics/cross-t" {
			t.Fatalf("expected wire Topic=projects/public-data/topics/cross-t, got %+v", pcalls)
		}
		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		var sawBillingProj bool
		for _, sp := range drained.Spans {
			if sp.Attributes["gcp.project_id"].GetStringValue() == "billing-proj" {
				sawBillingProj = true
			}
		}
		if !sawBillingProj {
			t.Fatalf("expected trace gcp.project_id=billing-proj override, spans=%+v", drained.Spans)
		}
	})

	t.Run("LEASE01_DeterministicBatchDeliveryOrder", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)

		const nMsgs = 10
		mock.resetCounters()
		mock.mu.Lock()
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				rms := make([]*pubsubpb.ReceivedMessage, 0, nMsgs)
				for i := 0; i < nMsgs; i++ {
					rms = append(rms, &pubsubpb.ReceivedMessage{
						AckId:   fmt.Sprintf("l1-%d", i),
						Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("ml1-%d", i), Data: []byte("d")},
					})
				}
				_ = stream.Send(&pubsubpb.StreamingPullResponse{ReceivedMessages: rms})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()
		defer func() {
			mock.mu.Lock()
			mock.onStreamPull = nil
			mock.mu.Unlock()
		}()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		maxConc := int32(10)
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       mock.addr,
					Subscription:   "projects/p/subscriptions/sub-lease01",
					MaxConcurrency: &maxConc,
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		tokens := make([]string, 0, nMsgs)
		var maxActive int32
		for len(tokens) < nMsgs {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv (%d): %v", len(tokens), rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				wantID := fmt.Sprintf("ml1-%d", len(tokens))
				if md.MessageId != wantID {
					t.Fatalf("delivery[%d] out of order: got %q, want %q", len(tokens), md.MessageId, wantID)
				}
				if md.ActiveCallbacks > maxActive {
					maxActive = md.ActiveCallbacks
				}
				tokens = append(tokens, md.DeliveryToken)
			}
		}
		if maxActive < 2 {
			t.Fatalf("expected concurrent callbacks (>= 2) with MaxConcurrency=10, got maxActive=%d", maxActive)
		}
		for _, tok := range tokens {
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK},
				},
			})
		}
		_ = sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sess.CloseSend()
		for {
			if _, rerr := sess.Recv(); rerr != nil {
				break
			}
		}
	})

	t.Run("LEASE03_GoDropCppPostExpiryAckAndPreExpiryModack", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// 1) Go: RETURN_WITHOUT_SETTLE continues lease extension (dl >= 10) and emits 0 nacks before/after expiry.
		hGo := startTestProxyDirect(t, "--mode=conformant-go")
		defer hGo.Shutdown(t, 2*time.Second)

		mock.resetCounters()
		mock.mu.Lock()
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "l3-go-1", Message: &pubsubpb.PubsubMessage{MessageId: "ml3-go-1", Data: []byte("g")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()
		defer func() {
			mock.mu.Lock()
			mock.onStreamPull = nil
			mock.mu.Unlock()
		}()

		sessGo, err := hGo.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession Go: %v", err)
		}
		_ = sessGo.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:                     mock.addr,
					Subscription:                 "projects/p/subscriptions/sub-l3-go",
					EnableMonitoring:             true,
					MaxExtensionMs:               protoInt64(250),
					MaxDurationPerAckExtensionMs: protoInt64(10_000),
					AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		})
		var tokGo string
		for tokGo == "" {
			ev, rerr := sessGo.Recv()
			if rerr != nil {
				t.Fatalf("Recv Go: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				tokGo = md.DeliveryToken
			}
		}
		_ = sessGo.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{DeliveryToken: tokGo, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE},
			},
		})
		time.Sleep(330 * time.Millisecond)
		_ = sessGo.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sessGo.CloseSend()
		for {
			if _, rerr := sessGo.Recv(); rerr != nil {
				break
			}
		}
		var sawPreExpModack, sawZeroNack bool
		for _, ma := range mock.getModAckCalls() {
			if ma.Req.AckDeadlineSeconds >= 10 {
				sawPreExpModack = true
			}
			if ma.Req.AckDeadlineSeconds == 0 {
				sawZeroNack = true
			}
		}
		if !sawPreExpModack || sawZeroNack {
			t.Fatalf("Go LEASE-03 expected >=1 pre-expiry modack (dl>=10) and 0 nacks, got preExp=%v zeroNack=%v calls=%+v", sawPreExpModack, sawZeroNack, mock.getModAckCalls())
		}

		// 2) C++: post-expiry SETTLE_ACTION_ACK still sends Acknowledge RPC.
		hCpp := startTestProxyDirect(t, "--mode=conformant-cpp-partial")
		defer hCpp.Shutdown(t, 2*time.Second)
		mock.resetCounters()
		sessCpp, err := hCpp.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession C++: %v", err)
		}
		_ = sessCpp.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       mock.addr,
					Subscription:   "projects/p/subscriptions/sub-l3-cpp",
					MaxExtensionMs: protoInt64(150),
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		})
		var tokCpp string
		for tokCpp == "" {
			ev, rerr := sessCpp.Recv()
			if rerr != nil {
				t.Fatalf("Recv C++: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				tokCpp = md.DeliveryToken
			}
		}
		time.Sleep(240 * time.Millisecond)
		_ = sessCpp.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{DeliveryToken: tokCpp, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK},
			},
		})
		time.Sleep(90 * time.Millisecond)
		_ = sessCpp.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sessCpp.CloseSend()
		for {
			if _, rerr := sessCpp.Recv(); rerr != nil {
				break
			}
		}
		if len(mock.getAckCalls()) != 1 {
			t.Fatalf("C++ post-expiry ACK expected 1 Acknowledge RPC, got %+v", mock.getAckCalls())
		}

		// 3) Rust: post-expiry RETURN_WITHOUT_SETTLE sends 0 nacks.
		hRust := startTestProxyDirect(t, "--mode=conformant-rust-nomon")
		defer hRust.Shutdown(t, 2*time.Second)
		mock.resetCounters()
		sessRust, err := hRust.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession Rust: %v", err)
		}
		_ = sessRust.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:       mock.addr,
					Subscription:   "projects/p/subscriptions/sub-l3-rust",
					MaxExtensionMs: protoInt64(150),
					AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		})
		var tokRust string
		for tokRust == "" {
			ev, rerr := sessRust.Recv()
			if rerr != nil {
				t.Fatalf("Recv Rust: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				tokRust = md.DeliveryToken
			}
		}
		time.Sleep(240 * time.Millisecond)
		_ = sessRust.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{DeliveryToken: tokRust, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE},
			},
		})
		_ = sessRust.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		})
		_ = sessRust.CloseSend()
		for {
			if _, rerr := sessRust.Recv(); rerr != nil {
				break
			}
		}
		for _, ma := range mock.getModAckCalls() {
			if ma.Req.AckDeadlineSeconds == 0 {
				t.Fatalf("Rust post-expiry RETURN_WITHOUT_SETTLE expected 0 nacks, got %+v", ma)
			}
		}
	})
}

func TestInfra_E2EM1BProfileBehaviorAndFlowControlFixes(t *testing.T) {
	mock := startMiniPubSubServer(t)

	t.Run("PFC02_FIFOWaiterQueueAndHeadCancelPromotion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		h := startTestProxyDirect(t, "--mode=conformant-go")
		defer h.Shutdown(t, 2*time.Second)

		mock.resetCounters()
		inB1 := make(chan struct{}, 1)
		releaseB1 := make(chan struct{})
		s1Arrived := make(chan struct{}, 1)
		var callN atomic.Int32
		mock.mu.Lock()
		mock.onPublish = func(pctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attempt int) (*pubsubpb.PublishResponse, error) {
			n := callN.Add(1)
			if n == 1 {
				select {
				case inB1 <- struct{}{}:
				default:
				}
				<-releaseB1
				return &pubsubpb.PublishResponse{MessageIds: []string{"fc-b1"}}, nil
			}
			select {
			case s1Arrived <- struct{}{}:
			default:
			}
			return &pubsubpb.PublishResponse{MessageIds: []string{fmt.Sprintf("fc-%d", n)}}, nil
		}
		mock.mu.Unlock()
		defer func() {
			mock.mu.Lock()
			mock.onPublish = nil
			mock.mu.Unlock()
		}()

		pub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:               mock.addr,
			Topic:                  "projects/p/topics/t-pfc02",
			MaxOutstandingMessages: protoInt64(100),
			MaxOutstandingBytes:    protoInt64(120),
			LimitExceededBehavior:  proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_BLOCK,
			CountThreshold:         protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher: %v", err)
		}
		defer func() {
			_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pub.PublisherId})
		}()

		b1Done := make(chan error, 1)
		go func() {
			r, perr := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pub.PublisherId, Data: make([]byte, 60)})
			if perr != nil {
				b1Done <- perr
			} else if r.ErrorCode != 0 {
				b1Done <- fmt.Errorf("b1 err=%d %s", r.ErrorCode, r.ErrorMessage)
			} else {
				b1Done <- nil
			}
		}()
		<-inB1

		// Queue b2 (60B, exceeds 120B limit behind b1).
		b2Ctx, b2Cancel := context.WithCancel(ctx)
		b2Done := make(chan error, 1)
		go func() {
			_, perr := h.Client.Publish(b2Ctx, &proxypb.PublishRequest{PublisherId: pub.PublisherId, Data: make([]byte, 60)})
			b2Done <- perr
		}()
		time.Sleep(40 * time.Millisecond)

		// Enqueue s1 (10B): 60+10 <= 120, yet s1 MUST stay queued behind b2 (FIFO non-bypass).
		s1Done := make(chan error, 1)
		go func() {
			r, perr := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pub.PublisherId, Data: make([]byte, 10)})
			if perr != nil {
				s1Done <- perr
			} else if r.ErrorCode != 0 {
				s1Done <- fmt.Errorf("s1 err=%d %s", r.ErrorCode, r.ErrorMessage)
			} else {
				s1Done <- nil
			}
		}()

		select {
		case <-s1Arrived:
			t.Fatalf("small s1 bypassed queued b2 in BLOCK FIFO queue")
		case <-time.After(100 * time.Millisecond):
		}

		// Cancelling blocked head b2 must immediately promote and admit s1 while b1 is still in-flight!
		b2Cancel()
		if perr := <-b2Done; status.Code(perr) != codes.DeadlineExceeded && status.Code(perr) != codes.Canceled {
			// Proxy converts ctx error or returns PublishResponse DeadlineExceeded on canceled Publish RPC.
			_ = perr
		}
		select {
		case <-s1Arrived:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected s1 to be promoted and published after head b2 canceled")
		}
		if err := <-s1Done; err != nil {
			t.Fatalf("s1 failed: %v", err)
		}
		close(releaseB1)
		if err := <-b1Done; err != nil {
			t.Fatalf("b1 failed: %v", err)
		}
	})

	t.Run("PFC03_CppIdleOversizedAndPFC03b_OrderedKeyPause", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 1) C++ idle oversized SIGNAL_ERROR allowed when idle + no key pause on rejection.
		hCpp := startTestProxyDirect(t, "--mode=conformant-cpp-partial")
		defer hCpp.Shutdown(t, 2*time.Second)
		mock.resetCounters()
		inOver := make(chan struct{}, 1)
		releaseOver := make(chan struct{})
		mock.mu.Lock()
		mock.onPublish = func(pctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attempt int) (*pubsubpb.PublishResponse, error) {
			if len(req.Messages) > 0 && len(req.Messages[0].Data) == 200 {
				select {
				case inOver <- struct{}{}:
				default:
				}
				<-releaseOver
			}
			return &pubsubpb.PublishResponse{MessageIds: []string{fmt.Sprintf("cpp-%d", attempt)}}, nil
		}
		mock.mu.Unlock()
		defer func() {
			mock.mu.Lock()
			mock.onPublish = nil
			mock.mu.Unlock()
		}()

		pubCpp, err := hCpp.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:               mock.addr,
			Topic:                  "projects/p/topics/t-pfc03-cpp",
			EnableMessageOrdering:  protoBool(true),
			MaxOutstandingMessages: protoInt64(10),
			MaxOutstandingBytes:    protoInt64(100),
			LimitExceededBehavior:  proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR,
			CountThreshold:         protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher C++: %v", err)
		}
		defer func() {
			_, _ = hCpp.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubCpp.PublisherId})
		}()

		overDone := make(chan *proxypb.PublishResponse, 1)
		go func() {
			r, _ := hCpp.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubCpp.PublisherId, OrderingKey: "k1", Data: make([]byte, 200)})
			overDone <- r
		}()
		<-inOver

		// Second publish while idle-oversized is in-flight must reject with ResourceExhausted without pausing "k1".
		rBusy, err := hCpp.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubCpp.PublisherId, OrderingKey: "k1", Data: []byte("busy")})
		if err != nil || rBusy.ErrorCode != int32(codes.ResourceExhausted) {
			t.Fatalf("expected ResourceExhausted while oversized in-flight, got resp=%+v err=%v", rBusy, err)
		}
		close(releaseOver)
		if rOver := <-overDone; rOver == nil || rOver.ErrorCode != 0 {
			t.Fatalf("expected C++ idle oversized publish to succeed, got %+v", rOver)
		}
		rFollow, err := hCpp.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubCpp.PublisherId, OrderingKey: "k1", Data: []byte("follow")})
		if err != nil || rFollow.ErrorCode != 0 {
			t.Fatalf("expected C++ ordering key k1 NOT paused after SIGNAL_ERROR, got resp=%+v err=%v", rFollow, err)
		}

		// 2) Go ordered SIGNAL_ERROR pauses ordering key until ResumePublish.
		hGo := startTestProxyDirect(t, "--mode=conformant-go")
		defer hGo.Shutdown(t, 2*time.Second)
		inHold := make(chan struct{}, 1)
		releaseHold := make(chan struct{})
		mock.resetCounters()
		mock.mu.Lock()
		mock.onPublish = func(pctx context.Context, req *pubsubpb.PublishRequest, md metadata.MD, attempt int) (*pubsubpb.PublishResponse, error) {
			if attempt == 1 {
				select {
				case inHold <- struct{}{}:
				default:
				}
				<-releaseHold
			}
			return &pubsubpb.PublishResponse{MessageIds: []string{fmt.Sprintf("go-%d", attempt)}}, nil
		}
		mock.mu.Unlock()
		pubGo, err := hGo.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:               mock.addr,
			Topic:                  "projects/p/topics/t-pfc03b-go",
			EnableMessageOrdering:  protoBool(true),
			MaxOutstandingMessages: protoInt64(1),
			LimitExceededBehavior:  proxypb.FlowControlLimitBehavior_FLOW_CONTROL_LIMIT_BEHAVIOR_SIGNAL_ERROR,
			CountThreshold:         protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher Go: %v", err)
		}
		defer func() {
			_, _ = hGo.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubGo.PublisherId})
		}()

		k11Done := make(chan *proxypb.PublishResponse, 1)
		go func() {
			r, _ := hGo.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubGo.PublisherId, OrderingKey: "k1", Data: []byte("k1-1")})
			k11Done <- r
		}()
		<-inHold
		k12, err := hGo.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubGo.PublisherId, OrderingKey: "k1", Data: []byte("k1-2")})
		if err != nil || k12.ErrorCode != int32(codes.ResourceExhausted) {
			t.Fatalf("expected ResourceExhausted on k1-2, got %+v err=%v", k12, err)
		}
		close(releaseHold)
		if r1 := <-k11Done; r1 == nil || r1.ErrorCode != 0 {
			t.Fatalf("k1-1 failed: %+v", r1)
		}
		k13, err := hGo.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubGo.PublisherId, OrderingKey: "k1", Data: []byte("k1-3")})
		if err != nil || k13.ErrorCode != int32(codes.FailedPrecondition) {
			t.Fatalf("expected FailedPrecondition on paused k1-3, got %+v err=%v", k13, err)
		}
		if _, err := hGo.Client.ResumePublish(ctx, &proxypb.ResumePublishRequest{PublisherId: pubGo.PublisherId, OrderingKey: "k1"}); err != nil {
			t.Fatalf("ResumePublish: %v", err)
		}
		k14, err := hGo.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubGo.PublisherId, OrderingKey: "k1", Data: []byte("k1-4")})
		if err != nil || k14.ErrorCode != 0 {
			t.Fatalf("expected k1-4 to succeed after ResumePublish, got %+v err=%v", k14, err)
		}
	})

	t.Run("CMP01_CppCompressionDisabledByDefault", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		h := startTestProxyDirect(t, "--mode=conformant-cpp-partial")
		defer h.Shutdown(t, 2*time.Second)

		caps, err := h.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("GetCapabilities: %v", err)
		}
		if caps.GetDefaults().GetPublishCompressionEnabledByDefault() {
			t.Fatalf("expected C++ PublishCompressionEnabledByDefault=false")
		}
		mock.resetCounters()
		wireGzipDecompressCount.Store(0)
		wireDeflateDecompressCount.Store(0)
		pub, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:       mock.addr,
			Topic:          "projects/p/topics/t-cmp01",
			CountThreshold: protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher: %v", err)
		}
		defer func() {
			_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pub.PublisherId})
		}()
		_, err = h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pub.PublisherId, Data: make([]byte, 1024)})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		pcalls := mock.getPublishCalls()
		if len(pcalls) != 1 || wireGzipDecompressCount.Load() != 0 || wireDeflateDecompressCount.Load() != 0 {
			t.Fatalf("expected uncompressed C++ default publish, calls=%d gzip=%d deflate=%d", len(pcalls), wireGzipDecompressCount.Load(), wireDeflateDecompressCount.Load())
		}

		// Explicit C++ compression at threshold=520: 499B (size 519) uncompressed, 500B (size 520) deflate.
		mock.resetCounters()
		wireDeflateDecompressCount.Store(0)
		pubCmpCpp, err := h.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:                  mock.addr,
			Topic:                     "projects/p/topics/t-cmp01-thresh",
			CountThreshold:            protoInt64(1),
			EnableCompression:         protoBool(true),
			CompressionBytesThreshold: protoInt64(520),
		})
		if err != nil {
			t.Fatalf("CreatePublisher cmp-thresh: %v", err)
		}
		defer func() {
			_, _ = h.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubCmpCpp.PublisherId})
		}()
		if _, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubCmpCpp.PublisherId, Data: make([]byte, 499)}); err != nil {
			t.Fatalf("Publish 499: %v", err)
		}
		if def499 := wireDeflateDecompressCount.Load(); def499 != 0 {
			t.Fatalf("expected 499B (size 519 < 520) uncompressed on C++, got deflate=%d", def499)
		}
		if _, err := h.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubCmpCpp.PublisherId, Data: make([]byte, 500)}); err != nil {
			t.Fatalf("Publish 500: %v", err)
		}
		if def500 := wireDeflateDecompressCount.Load(); def500 == 0 {
			t.Fatalf("expected 500B (size 520 >= 520) deflate compressed on C++, got deflate=%d", def500)
		}
	})

	t.Run("HDG01_RustHedgingClampingAndNonRustNaNError", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		protoF64 := func(v float64) *float64 { return &v }
		mock.resetCounters()
		mock.mu.Lock()
		mock.onPublish = nil
		mock.mu.Unlock()

		hRust := startTestProxyDirect(t, "--mode=conformant-rust-nomon")
		defer hRust.Shutdown(t, 2*time.Second)

		pubRust, err := hRust.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: mock.addr,
			Topic:    "projects/p/topics/t-hdg-rust",
			Hedging: &proxypb.HedgingSettings{
				Enabled:     true,
				DelayMs:     protoInt64(10),
				MaxTokens:   protoInt64(251),
				RefillRatio: protoF64(0.5),
			},
			CountThreshold: protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher Rust out-of-range hedging: %v", err)
		}
		defer func() {
			_, _ = hRust.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubRust.PublisherId})
		}()
		pr, err := hRust.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubRust.PublisherId, Data: []byte("ok")})
		if err != nil || pr.ErrorCode != 0 {
			t.Fatalf("expected Rust clamped hedging publish to succeed, got resp=%+v err=%v", pr, err)
		}

		pubRustNaN, err := hRust.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: mock.addr,
			Topic:    "projects/p/topics/t-hdg-rust-nan",
			Hedging: &proxypb.HedgingSettings{
				Enabled:     true,
				RefillRatio: protoF64(math.NaN()),
			},
			CountThreshold: protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher Rust NaN hedging: %v", err)
		}
		defer func() {
			_, _ = hRust.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubRustNaN.PublisherId})
		}()
		prNaN, err := hRust.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubRustNaN.PublisherId, Data: []byte("ok")})
		if err != nil || prNaN.ErrorCode != 0 {
			t.Fatalf("expected Rust NaN hedging publish to succeed, got resp=%+v err=%v", prNaN, err)
		}

		hGo := startTestProxyDirect(t, "--mode=conformant-go")
		defer hGo.Shutdown(t, 2*time.Second)
		pubGoBad, _ := hGo.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint: mock.addr,
			Topic:    "projects/p/topics/t-hdg-go-bad",
			Hedging: &proxypb.HedgingSettings{
				Enabled:     true,
				RefillRatio: protoF64(math.NaN()),
			},
			CountThreshold: protoInt64(1),
		})
		if pubGoBad != nil {
			defer func() {
				_, _ = hGo.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubGoBad.PublisherId})
			}()
			prGo, err := hGo.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubGoBad.PublisherId, Data: []byte("x")})
			if err == nil && (prGo == nil || prGo.ErrorCode == 0) {
				t.Fatalf("expected Go NaN hedging to fail with InvalidArgument")
			}
		}
	})

	t.Run("PULL02d_ShutdownNowRetryabilityGoVsCppAndRust", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cases := []struct {
			mode      string
			wantRetry bool
		}{
			{mode: "conformant-go", wantRetry: false},
			{mode: "conformant-cpp-partial", wantRetry: true},
			{mode: "conformant-rust-nomon", wantRetry: true},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.mode, func(t *testing.T) {
				h := startTestProxyDirect(t, "--mode="+tc.mode)
				defer h.Shutdown(t, 2*time.Second)

				mock.resetCounters()
				stream2Done := make(chan struct{}, 1)
				mock.mu.Lock()
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
					if streamIdx == 1 {
						return status.Error(codes.Unavailable, "Server shutdownNow invoked")
					}
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: "s2-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-s2-1", Data: []byte("ok")}},
						},
					})
					select {
					case stream2Done <- struct{}{}:
					default:
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()
				defer func() {
					mock.mu.Lock()
					mock.onStreamPull = nil
					mock.mu.Unlock()
				}()

				sess, err := h.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("SubscriberSession: %v", err)
				}
				_ = sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:       mock.addr,
							Subscription:   "projects/p/subscriptions/sub-p2d",
							AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_AUTO_ACK,
						},
					},
				})
				if tc.wantRetry {
					select {
					case <-stream2Done:
					case <-time.After(3 * time.Second):
						t.Fatalf("%s expected stream 2 reconnect on Server shutdownNow invoked", tc.mode)
					}
					_ = sess.Send(&proxypb.SubscriberCommand{
						Command: &proxypb.SubscriberCommand_Stop{
							Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
						},
					})
					_ = sess.CloseSend()
					for {
						if _, rerr := sess.Recv(); rerr != nil {
							break
						}
					}
				} else {
					for {
						ev, rerr := sess.Recv()
						if rerr != nil {
							break
						}
						if ev.GetStopped() != nil {
							break
						}
					}
					if len(mock.getStreamInits()) != 1 {
						t.Fatalf("conformant-go expected 1 stream open on shutdownNow, got %d", len(mock.getStreamInits()))
					}
				}
			})
		}
	})
}

func TestInfra_E2EProxyTimingsFlagsAndCanonicalDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	t.Run("CanonicalDefaultsUntouchedWithoutFastTimings", func(t *testing.T) {
		hGo := startTestProxyDirect(t, "--mode=conformant-go")
		defer hGo.Shutdown(t, 2*time.Second)
		capsGo, err := hGo.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("Go GetCapabilities: %v", err)
		}
		dg := capsGo.GetDefaults()
		if dg.GetSubscriberMaxExtensionMs() != 3_600_000 || dg.GetSubscriberMinAckExtensionMs() != 10_000 ||
			dg.GetKeepalivePingIntervalMs() != 30_000 || dg.GetKeepalivePongTimeoutMs() != 15_000 ||
			dg.GetRetryInitialDelayMs() != 100 {
			t.Fatalf("unexpected canonical Go defaults: %+v", dg)
		}

		hCpp := startTestProxyDirect(t, "--mode=conformant-cpp-partial")
		defer hCpp.Shutdown(t, 2*time.Second)
		capsCpp, err := hCpp.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("C++ GetCapabilities: %v", err)
		}
		dc := capsCpp.GetDefaults()
		if dc.GetPublishCompressionEnabledByDefault() || dc.GetSubscriberMaxExtensionMs() != 0 ||
			dc.GetKeepalivePingIntervalMs() != 0 || dc.GetKeepalivePongTimeoutMs() != 0 ||
			dc.GetSubscriberMinAckExtensionMs() != 60_000 {
			t.Fatalf("unexpected canonical C++ defaults: %+v", dc)
		}
	})

	t.Run("FastTimingsAndKeepaliveOverrides", func(t *testing.T) {
		hBase := startTestProxyDirect(t, "--mode=conformant-go")
		defer hBase.Shutdown(t, 2*time.Second)
		capsBase, err := hBase.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("base caps: %v", err)
		}

		hFast := startTestProxyDirect(t, "--mode=conformant-go", "--fast-timings")
		defer hFast.Shutdown(t, 2*time.Second)
		capsFast, err := hFast.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("fast caps: %v", err)
		}
		df := capsFast.GetDefaults()
		if df.GetSubscriberMinAckExtensionMs() != 250 || df.GetSubscriberMaxExtensionMs() != 350 ||
			df.GetRetryInitialDelayMs() != 20 || df.GetKeepalivePingIntervalMs() != 120 ||
			df.GetKeepalivePongTimeoutMs() != 70 {
			t.Fatalf("unexpected Go fast-timings defaults: %+v", df)
		}
		if capsFast.SupportsKeepalive != capsBase.SupportsKeepalive || capsFast.SupportsAutoNackOnDrop != capsBase.SupportsAutoNackOnDrop ||
			capsFast.SupportsExactlyOnceDelivery != capsBase.SupportsExactlyOnceDelivery {
			t.Fatalf("--fast-timings altered capabilities: base=%+v fast=%+v", capsBase, capsFast)
		}

		hKeep := startTestProxyDirect(t, "--mode=conformant-go", "--fast-timings", "--keepalive-ping=150ms", "--keepalive-pong=90ms")
		defer hKeep.Shutdown(t, 2*time.Second)
		capsKeep, err := hKeep.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("keepalive override caps: %v", err)
		}
		dk := capsKeep.GetDefaults()
		if dk.GetKeepalivePingIntervalMs() != 150 || dk.GetKeepalivePongTimeoutMs() != 90 || dk.GetSubscriberMaxExtensionMs() < 410 {
			t.Fatalf("expected keepalive overrides synced & maxExt raised >=410ms, got %+v", dk)
		}

		hCppFast := startTestProxyDirect(t, "--mode=conformant-cpp-partial", "--fast-timings")
		defer hCppFast.Shutdown(t, 2*time.Second)
		capsCppFast, err := hCppFast.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("C++ fast caps: %v", err)
		}
		dcf := capsCppFast.GetDefaults()
		if dcf.GetKeepalivePingIntervalMs() != 0 || dcf.GetKeepalivePongTimeoutMs() != 0 ||
			dcf.GetRetryInitialDelayMs() != 20 {
			t.Fatalf("unexpected C++ fast-timings defaults: %+v", dcf)
		}
	})
}

func TestInfra_V1ReportSchemaCompareAndCanonicalStatusFix(t *testing.T) {
	t.Run("SingleAndCompareJSONSchemaAndCanonicalStatusFix", func(t *testing.T) {
		singleJSON := []byte(`{
		  "schema_version": "1.0",
		  "mode": "single",
		  "target": "go",
		  "capabilities": {
		    "client_language": "go",
		    "client_library": "cloud.google.com/go/pubsub/v2",
		    "client_version": "v2.0.0",
		    "supports_monitoring": true,
		    "supports_unary_pull": true,
		    "supports_auto_nack_on_drop": false,
		    "defaults": {
		      "publish_batch_count_threshold": 100,
		      "publish_batch_byte_threshold": 1000000,
		      "publish_max_request_bytes": 10000000,
		      "publish_batch_delay_ms": 10,
		      "publish_total_timeout_ms": 60000,
		      "publish_flow_control_max_messages": 1000,
		      "publish_flow_control_max_bytes": -1,
		      "publish_flow_control_behavior": "IGNORE",
		      "publish_compression_enabled_by_default": false,
		      "publish_compression_bytes_threshold": 240,
		      "publish_compression_algorithm": "GZIP",
		      "publish_hedging_delay_ms": 1000,
		      "publish_hedging_max_tokens": 50,
		      "publish_hedging_refill_ratio": 0.1,
		      "subscriber_max_outstanding_messages": 1000,
		      "subscriber_max_outstanding_bytes": 1000000000,
		      "subscriber_max_extension_ms": 3600000,
		      "subscriber_min_ack_extension_ms": 10000,
		      "subscriber_max_ack_extension_ms": 600000,
		      "subscriber_adaptive_ack_extension": true,
		      "subscriber_per_stream_flow_control": false,
		      "subscriber_default_stream_ack_deadline_seconds": 60,
		      "keepalive_ping_interval_ms": 30000,
		      "keepalive_pong_timeout_ms": 15000,
		      "retry_initial_delay_ms": 100,
		      "retry_max_delay_ms": 60000,
		      "retry_delay_multiplier": 4.0
		    }
		  },
		  "summary": {
		    "total": 3,
		    "pass": 1,
		    "fail": 2,
		    "skipped_no_monitoring": 0,
		    "unsupported_by_client": 0,
		    "timeout_count": 1,
		    "error_count": 1
		  },
		  "scenarios": [
		    {"id": "TRAC-01", "feature": "tracing", "status": "PASS", "trace_status": "PASS", "duration_ms": 12, "deviations": [{"category": "tracing", "rule": "R1", "expected_go": "UNSET", "observed": "OK", "description": "demo"}]},
		    {"id": "PULL-03", "status": "FAIL", "status_detail": "TIMEOUT", "reason": "deadline exceeded", "error": "TIMEOUT: timed out", "duration_ms": 500, "diagnostics": ["d1"], "proxy_restarted": true},
		    {"id": "PUB-01", "status": "FAIL", "status_detail": "ERROR", "reason": "proxy crash", "error": "ERROR: crash", "duration_ms": 20}
		  ]
		}`)
		rep, err := ParseJSONReport(singleJSON)
		if err != nil {
			t.Fatalf("ParseJSONReport single v1.0: %v", err)
		}
		AssertSummaryMatchesScenarios(t, rep)
		if rep.Mode != "single" || len(rep.Capabilities.DefaultsRaw) != 27 || rep.Capabilities.Defaults == nil {
			t.Fatalf("unexpected single mode caps/defaults: %+v", rep.Capabilities)
		}
		if rep.Capabilities.SupportsAutoNackOnDrop || !rep.Capabilities.SupportsUnaryPull {
			t.Fatalf("unexpected parsed bools: %+v", rep.Capabilities)
		}
		sc0, _ := rep.FindScenario("TRAC-01")
		if sc0.Reason != "" || sc0.Error != "" || len(sc0.Deviations) != 1 || sc0.Deviations[0].Rule != "R1" {
			t.Fatalf("unexpected TRAC-01 parse: %+v", sc0)
		}
		sc1, _ := rep.FindScenario("PULL-03")
		if sc1.Reason != "deadline exceeded" || sc1.Error != "TIMEOUT: timed out" || !sc1.ProxyRestarted || len(sc1.Diagnostics) != 1 {
			t.Fatalf("unexpected PULL-03 parse (reason/error decoupling): %+v", sc1)
		}

		// Compare report: root summary/scenarios omitted, target_error ERROR verdict must NOT mutate to FAIL!
		compareJSON := []byte(`{
		  "schema_version": "1.0",
		  "mode": "compare",
		  "targets": [
		    {"target": "go", "summary": {"total": 1, "pass": 1, "fail": 0, "skipped_no_monitoring": 0, "unsupported_by_client": 0}, "scenarios": [{"id": "CAPS-01", "status": "PASS"}]},
		    {"target": "cpp", "target_error": "boom", "timed_out": false, "summary": {"total": 1, "pass": 0, "fail": 1, "skipped_no_monitoring": 0, "unsupported_by_client": 0, "error_count": 1}, "scenarios": [{"id": "CAPS-01", "status": "FAIL", "status_detail": "ERROR", "error": "ERROR: boom"}]}
		  ],
		  "differences": [
		    {"kind": "target_error", "description": "target error status differs", "verdicts": {"go": "OK", "cpp": "ERROR", "rust": "TIMEOUT"}},
		    {"kind": "capability", "field": "supports_keepalive", "verdicts": {"go": "true", "cpp": "false"}},
		    {"kind": "scenario_trace_delta", "scenario_id": "TRAC-01", "verdicts": {"go": "PASS", "cpp": "MISSING"}, "details": {"delta": "span"}}
		  ]
		}`)
		cmpRep, err := ParseJSONReport(compareJSON)
		if err != nil {
			t.Fatalf("ParseJSONReport compare v1.0: %v", err)
		}
		AssertSummaryMatchesScenarios(t, cmpRep)
		if cmpRep.Differences[0].Verdicts["cpp"] != StatusError || cmpRep.Differences[0].Verdicts["rust"] != StatusTimeout || cmpRep.Differences[0].Verdicts["go"] != "OK" {
			t.Fatalf("target_error diff verdict mutated unexpectedly: %+v", cmpRep.Differences[0].Verdicts)
		}
		if cmpRep.Differences[2].Details["delta"] != "span" || cmpRep.Targets["cpp"].TargetError != "boom" {
			t.Fatalf("missing diff details or target_error: diff=%+v tgt=%+v", cmpRep.Differences[2], cmpRep.Targets["cpp"])
		}
	})

	t.Run("MarkdownV1SectionsAndCompareCLI", func(t *testing.T) {
		mdSample := strings.Join([]string{
			"# Pub/Sub Cross-Client Conformance Comparison Report",
			"",
			"## Summary",
			"",
			"_Failure breakdown: 1 timeout(s), 2 error(s)._",
			"",
			"| Kind | Scenario / Field | go | cpp | Description |",
			"| --- | --- | --- | --- | --- |",
			"| `target_error` | - | `OK` | `ERROR` | target error |",
			"",
			"## Diagnostics",
			"- **Target cpp Error**: boom",
			"- **cpp/CAPS-01** (`FAIL`): err",
		}, "\n")
		mdParsed, err := ParseMarkdownReport(mdSample)
		if err != nil {
			t.Fatalf("ParseMarkdownReport: %v", err)
		}
		if !mdParsed.HasFailureBreakdown || mdParsed.TimeoutCount != 1 || mdParsed.ErrorCount != 2 || len(mdParsed.DiagnosticsBullets) != 2 {
			t.Fatalf("unexpected v1 markdown parse: %+v", mdParsed)
		}

		dir := t.TempDir()
		jsonPath := filepath.Join(dir, "cmp.json")
		mdPath := filepath.Join(dir, "cmp.md")
		goWrap, goMark := writeProxyWrapper(t, "--mode=conformant-go", "--fast-timings")
		cppWrap, cppMark := writeProxyWrapper(t, "--mode=conformant-cpp-partial", "--fast-timings")
		_ = goMark
		_ = cppMark

		res := runCLIWithEnv(t, 25*time.Second, nil,
			"--proxy-target=go="+goWrap,
			"--proxy-target=cpp="+cppWrap,
			"--filter=^PUB-01$",
			"--report-json="+jsonPath,
			"--report-md="+mdPath,
		)
		if res.ExitCode != 0 {
			t.Fatalf("CLI compare PUB-01 failed (%d): stdout=%s stderr=%s", res.ExitCode, res.Stdout, res.Stderr)
		}
		rep := ReadJSONReport(t, jsonPath)
		AssertSummaryMatchesScenarios(t, rep)
		if rep.Mode != "compare" || len(rep.Targets) != 2 {
			t.Fatalf("expected compare report with 2 targets, got mode=%q len=%d", rep.Mode, len(rep.Targets))
		}
		if len(rep.Targets["go"].Capabilities.DefaultsRaw) != 27 || rep.Targets["cpp"].Capabilities.SupportsKeepalive {
			t.Fatalf("unexpected target capabilities in compare JSON: %+v", rep.Targets)
		}
		mdOut := ReadMarkdownReport(t, mdPath)
		if !mdOut.HasHeadingContaining("Comparison Report") || !mdOut.HasTableRowContaining("PUB-01", "PASS") {
			t.Fatalf("unexpected compare MD report: %+v", mdOut)
		}
	})
}

func TestInfra_RunCLIWithEnvAndOrphanMarkerInEnv(t *testing.T) {
	t.Setenv("E2E_ENV_OVERRIDE_TEST", "parent-val")
	marker := newRunMarker(t)
	dir := t.TempDir()
	wrapperPath := filepath.Join(dir, "env_proxy_wrapper.sh")
	script := fmt.Sprintf("#!/usr/bin/env bash\nif [ \"${E2E_ENV_OVERRIDE_TEST:-}\" != \"extra-val\" ]; then\n  echo \"bad env: ${E2E_ENV_OVERRIDE_TEST:-}\" >&2\n  exit 42\nfi\nexec %s --marker=\"${E2E_ENV_MARKER:-missing}\" --mode=conformant-go --fast-timings \"$@\"\n", shellQuote(proxyBinaryPath))
	writeExecutableFile(t, wrapperPath, []byte(script))
	res := runCLIWithEnv(t, 20*time.Second,
		[]string{
			"E2E_ENV_OVERRIDE_TEST=extra-val",
			"E2E_ENV_MARKER=" + marker,
		},
		"--proxy-cmd="+wrapperPath,
		"--filter=^PUB-01$",
	)
	if res.ExitCode != 0 {
		t.Fatalf("runCLIWithEnv failed (%d): stdout=%s stderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	if got := os.Getenv("E2E_ENV_OVERRIDE_TEST"); got != "parent-val" {
		t.Fatalf("parent env unexpectedly mutated to %q", got)
	}
	assertNoOrphanProxy(t, marker)
}

type slowShutdownProxyServer struct {
	proxypb.UnimplementedPubSubProxyServer
}

func (s *slowShutdownProxyServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	return &proxypb.GetCapabilitiesResponse{
		ClientLanguage: "go",
		ClientLibrary:  "slow-shutdown-test-double",
		ClientVersion:  "1.0.0",
	}, nil
}

func (s *slowShutdownProxyServer) CreatePublisher(context.Context, *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	// Deliberately ignore ctx.Done() so harness exercises timeoutUnwindGrace (200ms).
	time.Sleep(15 * time.Second)
	return nil, status.Error(codes.DeadlineExceeded, "timed out")
}

func (s *slowShutdownProxyServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	// Exceeds harness rpcGrace (1s) so full shutdown budget reaches ~7.2s before SIGKILL.
	time.Sleep(1200 * time.Millisecond)
	return &proxypb.ShutdownProxyResponse{}, nil
}

func runSlowShutdownTestProxy() int {
	fs := flag.NewFlagSet("slow-shutdown-proxy", flag.ContinueOnError)
	port := fs.Int("port", 0, "")
	_ = fs.String("marker", "", "")
	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "slow-shutdown-proxy flags: %v\n", err)
		return 2
	}

	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGTERM)
	go func() {
		for range sigCh {
		}
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "slow-shutdown-proxy listen: %v\n", err)
		return 1
	}
	srv := grpc.NewServer()
	proxypb.RegisterPubSubProxyServer(srv, &slowShutdownProxyServer{})
	fmt.Printf("READY 127.0.0.1:%d\n", lis.Addr().(*net.TCPAddr).Port)
	_ = os.Stdout.Sync()
	if err := srv.Serve(lis); err != nil {
		return 1
	}
	return 0
}

func TestInfra_E2EM1BIter2Regressions(t *testing.T) {
	mock := startMiniPubSubServer(t)
	raceBin := buildRaceProxyBinary(t)

	hGo := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-go")
	t.Cleanup(func() {
		if err := hGo.Shutdown(t, 4*time.Second); err != nil {
			t.Errorf("-race Go proxy shutdown error (stderr=%q): %v", hGo.Stderr(), err)
		}
		if strings.Contains(hGo.Stderr(), "DATA RACE") {
			t.Errorf("DATA RACE in Go proxy stderr:\n%s", hGo.Stderr())
		}
	})

	// 1. Unordered expiry behind fc.acquire (MaxOutstandingMessages=1, MaxExtensionMs=60ms) -> TotalDelivered == 1.
	t.Run("UnorderedExpiryBehindFCAcquire", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		defer func() {
			mock.mu.Lock()
			mock.onStreamPull = nil
			mock.mu.Unlock()
		}()

		for iter := 0; iter < 5; iter++ {
			mock.resetCounters()
			_, _ = hGo.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = nil
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
							MessageOrderingEnabled: false,
						},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: fmt.Sprintf("ufc-%d-1", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("mufc-%d-1", iter), Data: []byte("1")}},
							{AckId: fmt.Sprintf("ufc-%d-2", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("mufc-%d-2", iter), Data: []byte("2")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := hGo.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("iter %d SubscriberSession: %v", iter, err)
			}
			subName := fmt.Sprintf("sub-ufc-%d", iter)
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:               mock.addr,
						Subscription:           "projects/p/subscriptions/" + subName,
						EnableMonitoring:       true,
						MaxOutstandingMessages: protoInt64(1),
						MaxExtensionMs:         protoInt64(60),
						AutoSettleMode:         proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				t.Fatalf("iter %d Start: %v", iter, err)
			}

			evCh := make(chan *proxypb.SubscriberEvent, 8)
			recvErr := make(chan error, 1)
			go func() {
				for {
					ev, rerr := sess.Recv()
					if rerr != nil {
						recvErr <- rerr
						return
					}
					evCh <- ev
				}
			}()

			deliveredCount := 0
			for deliveredCount == 0 {
				select {
				case ev := <-evCh:
					if ev.GetMessageDelivered() != nil {
						deliveredCount++
					}
				case rerr := <-recvErr:
					t.Fatalf("iter %d unexpected recv before delivery: %v", iter, rerr)
				case <-time.After(3 * time.Second):
					t.Fatalf("iter %d timed out waiting for first delivery", iter)
				}
			}

			// Wait past MaxExtensionMs=60ms so u1 expires & frees FC after u2's maxExtAt already elapsed.
			time.Sleep(140 * time.Millisecond)

			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			}); err != nil {
				t.Fatalf("iter %d Stop: %v", iter, err)
			}
			_ = sess.CloseSend()

			var stopped *proxypb.StoppedEvent
			for drainDone := false; !drainDone; {
				select {
				case ev := <-evCh:
					if ev.GetMessageDelivered() != nil {
						deliveredCount++
					}
					if st := ev.GetStopped(); st != nil {
						stopped = st
					}
				case rerr := <-recvErr:
					if rerr != io.EOF {
						t.Fatalf("iter %d Recv: %v", iter, rerr)
					}
					for len(evCh) > 0 {
						ev := <-evCh
						if ev.GetMessageDelivered() != nil {
							deliveredCount++
						}
						if st := ev.GetStopped(); st != nil {
							stopped = st
						}
					}
					drainDone = true
				case <-time.After(3 * time.Second):
					t.Fatalf("iter %d timed out waiting for stop", iter)
				}
			}
			if stopped == nil || stopped.TotalDelivered != 1 || deliveredCount != 1 {
				t.Fatalf("iter %d expected TotalDelivered=1 (u2 expired behind fc.acquire), stopped=%+v delivered=%d", iter, stopped, deliveredCount)
			}
			drained, err := hGo.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("iter %d DrainTraces: %v", iter, err)
			}
			var subCnt, procCnt int
			subByID := make(map[string]*proxypb.TraceSpan, 2)
			for _, sp := range drained.Spans {
				if sp.Name == subName+" subscribe" {
					subCnt++
					subByID[sp.SpanId] = sp
					if sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "expired" {
						t.Fatalf("iter %d expected expired result, got %+v", iter, sp.Attributes)
					}
				}
				if sp.Name == subName+" process" {
					procCnt++
				}
			}
			ccPerSub := make(map[string]int, 2)
			schedPerSub := make(map[string]int, 2)
			for _, sp := range drained.Spans {
				if sp.Name == "subscriber concurrency control" {
					if sp.EndTime == nil || subByID[sp.ParentSpanId] == nil {
						t.Fatalf("iter %d invalid cc span: %+v", iter, sp)
					}
					ccPerSub[sp.ParentSpanId]++
				}
				if sp.Name == "subscribe scheduler" {
					if sp.EndTime == nil || subByID[sp.ParentSpanId] == nil {
						t.Fatalf("iter %d invalid scheduler span: %+v", iter, sp)
					}
					schedPerSub[sp.ParentSpanId]++
				}
			}
			if subCnt != 2 || procCnt != 1 || len(ccPerSub) != 2 || len(schedPerSub) != 2 {
				t.Fatalf("iter %d expected 2 subscribe, 1 process, 2 cc, 2 scheduler spans, got sub=%d proc=%d ccSubs=%d schedSubs=%d", iter, subCnt, procCnt, len(ccPerSub), len(schedPerSub))
			}
			for id := range subByID {
				if ccPerSub[id] != 1 || schedPerSub[id] != 1 {
					t.Fatalf("iter %d subscribe %s expected 1 cc and 1 scheduler child, got cc=%d sched=%d", iter, id, ccPerSub[id], schedPerSub[id])
				}
			}
		}
	})

	// 2. conformant-go RETURN_WITHOUT_SETTLE followed immediately (0ms) by SETTLE_ACTION_ACK -> 0 dropped ACKs.
	t.Run("GoReturnWithoutSettleFollowedImmediatelyByAck25x", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		defer func() {
			mock.mu.Lock()
			mock.onStreamPull = nil
			mock.mu.Unlock()
		}()

		for iter := 0; iter < 25; iter++ {
			mock.resetCounters()
			wantAck := fmt.Sprintf("goret-%d", iter)
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = nil
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: wantAck, Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("mgoret-%d", iter), Data: []byte("g")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := hGo.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("iter %d SubscriberSession: %v", iter, err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:       mock.addr,
						Subscription:   fmt.Sprintf("projects/p/subscriptions/sub-goret-%d", iter),
						AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				t.Fatalf("iter %d Start: %v", iter, err)
			}

			var tok string
			for tok == "" {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("iter %d Recv delivery: %v", iter, rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					tok = md.DeliveryToken
				}
			}

			// Send RETURN_WITHOUT_SETTLE followed immediately (0ms) by ACK, then Stop(WAIT_FOR_PROCESSING).
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE},
				},
			}); err != nil {
				t.Fatalf("iter %d Settle RETURN_WITHOUT_SETTLE: %v", iter, err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK},
				},
			}); err != nil {
				t.Fatalf("iter %d Settle ACK: %v", iter, err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			}); err != nil {
				t.Fatalf("iter %d Stop: %v", iter, err)
			}
			_ = sess.CloseSend()

			var stopped *proxypb.StoppedEvent
			for {
				ev, rerr := sess.Recv()
				if rerr == io.EOF {
					break
				}
				if rerr != nil {
					t.Fatalf("iter %d Recv stop: %v", iter, rerr)
				}
				if st := ev.GetStopped(); st != nil {
					stopped = st
				}
			}
			if stopped == nil || stopped.ErrorCode != 0 {
				t.Fatalf("iter %d unexpected StoppedEvent: %+v", iter, stopped)
			}
			acks := mock.getAckCalls()
			if len(acks) != 1 || len(acks[0].Req.AckIds) != 1 || acks[0].Req.AckIds[0] != wantAck {
				t.Fatalf("iter %d follow-up ACK dropped or mismatched: acks=%+v", iter, acks)
			}
			for _, ma := range mock.getModAckCalls() {
				if ma.Req.AckDeadlineSeconds == 0 {
					t.Fatalf("iter %d unexpected nack on Go RETURN_WITHOUT_SETTLE+ACK: %+v", iter, ma)
				}
			}
		}
	})

	// 3. conformant-rust-nomon RETURN_WITHOUT_SETTLE right past MaxExtensionMs (before next expiryTick) -> 0 nacks.
	t.Run("RustPostMaxExtBeforeExpiryTickReturnWithoutSettle", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		hRust := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-rust-nomon")
		defer func() {
			if err := hRust.Shutdown(t, 4*time.Second); err != nil {
				t.Errorf("-race Rust proxy shutdown error (stderr=%q): %v", hRust.Stderr(), err)
			}
			if strings.Contains(hRust.Stderr(), "DATA RACE") {
				t.Errorf("DATA RACE in Rust proxy stderr:\n%s", hRust.Stderr())
			}
		}()
		defer func() {
			mock.mu.Lock()
			mock.onStreamPull = nil
			mock.mu.Unlock()
		}()

		for iter := 0; iter < 5; iter++ {
			mock.resetCounters()
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = nil
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: fmt.Sprintf("rustexp-%d", iter), Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("mrustexp-%d", iter), Data: []byte("r")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := hRust.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("iter %d SubscriberSession: %v", iter, err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:       mock.addr,
						Subscription:   fmt.Sprintf("projects/p/subscriptions/sub-rustexp-%d", iter),
						MaxExtensionMs: protoInt64(150),
						AutoSettleMode: proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				t.Fatalf("iter %d Start: %v", iter, err)
			}

			var tok string
			for tok == "" {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("iter %d Recv delivery: %v", iter, rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					tok = md.DeliveryToken
				}
			}

			// Sleep just past MaxExtensionMs=150ms (inside [maxExtAt, next 18.75ms expiryTick) window).
			time.Sleep(152 * time.Millisecond)

			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE},
				},
			}); err != nil {
				t.Fatalf("iter %d Settle: %v", iter, err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			}); err != nil {
				t.Fatalf("iter %d Stop: %v", iter, err)
			}
			_ = sess.CloseSend()

			for {
				if _, rerr := sess.Recv(); rerr == io.EOF {
					break
				} else if rerr != nil {
					t.Fatalf("iter %d Recv stop: %v", iter, rerr)
				}
			}
			for _, ma := range mock.getModAckCalls() {
				if ma.Req.AckDeadlineSeconds == 0 {
					t.Fatalf("iter %d spurious Rust post-MaxExtensionMs nack before expiryTick: %+v", iter, ma)
				}
			}
		}
	})

	// 4. runCLIWithEnv timeout against slow ShutdownProxy (1.2s) + ignore SIGTERM -> 0 orphan proxies (WaitDelay >= 12s).
	t.Run("RunCLIWithEnvTimeoutSlowShutdownProxyNoOrphans", func(t *testing.T) {
		exe, err := os.Executable()
		if err != nil {
			t.Fatalf("os.Executable: %v", err)
		}
		marker := newRunMarker(t)
		wrapper := filepath.Join(t.TempDir(), fmt.Sprintf("slow-proxy-%s.sh", marker))
		script := fmt.Sprintf("#!/bin/sh\nE2E_SLOW_SHUTDOWN_PROXY=1 exec %s %s \"$@\"\n", shellQuote(exe), shellQuote("--marker="+marker))
		writeExecutableFile(t, wrapper, []byte(script))

		res := runCLIWithEnv(t, 600*time.Millisecond, nil, "--proxy-cmd="+wrapper, "--filter=^PUB-01$")
		if !res.TimedOut {
			t.Fatalf("expected runCLIWithEnv timeout, got exit=%d err=%v stdout=%s stderr=%s", res.ExitCode, res.Err, res.Stdout, res.Stderr)
		}
		if res.ExitCode != 1 {
			t.Fatalf("expected clean CLI timeout exit code 1 (not WaitDelay SIGKILL -1), got exit=%d err=%v stderr=%s", res.ExitCode, res.Err, res.Stderr)
		}
		assertNoOrphanProxy(t, marker)
	})

	// 5. Pre-recv StreamingPull PermissionDenied & Unavailable("Server shutdownNow invoked") -> fatal stream 1 StoppedEvent.
	t.Run("PreRecvStreamingPullFatalErrors20x", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		defer func() {
			mock.mu.Lock()
			mock.onStreamPullBeforeRecv = nil
			mock.onStreamPull = nil
			mock.mu.Unlock()
		}()

		cases := []struct {
			name string
			code codes.Code
			msg  string
		}{
			{name: "PermissionDenied", code: codes.PermissionDenied, msg: "perm-before-recv"},
			{name: "ShutdownNow", code: codes.Unavailable, msg: "Server shutdownNow invoked"},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				for iter := 0; iter < 10; iter++ {
					mock.resetCounters()
					var streamsOpened atomic.Int32
					mock.mu.Lock()
					mock.onStreamPull = nil
					mock.onStreamPullBeforeRecv = func(_ pubsubpb.Subscriber_StreamingPullServer, _ int) error {
						streamsOpened.Add(1)
						return status.Error(tc.code, tc.msg)
					}
					mock.mu.Unlock()

					sess, err := hGo.Client.SubscriberSession(ctx)
					if err != nil {
						t.Fatalf("iter %d SubscriberSession: %v", iter, err)
					}
					if err := sess.Send(&proxypb.SubscriberCommand{
						Command: &proxypb.SubscriberCommand_Start{
							Start: &proxypb.StartSubscriberCommand{
								Endpoint:            mock.addr,
								Subscription:        fmt.Sprintf("projects/p/subscriptions/sub-prerecv-%s-%d", tc.name, iter),
								InitialRetryDelayMs: protoInt64(20),
							},
						},
					}); err != nil {
						t.Fatalf("iter %d Start: %v", iter, err)
					}

					evCh := make(chan *proxypb.SubscriberEvent, 4)
					errCh := make(chan error, 1)
					go func() {
						for {
							ev, rerr := sess.Recv()
							if rerr != nil {
								errCh <- rerr
								return
							}
							evCh <- ev
						}
					}()

					var stopped *proxypb.StoppedEvent
					deadline := time.After(2 * time.Second)
					for done := false; !done; {
						select {
						case ev := <-evCh:
							if st := ev.GetStopped(); st != nil {
								stopped = st
							}
						case rerr := <-errCh:
							if rerr != io.EOF {
								t.Fatalf("iter %d Recv: %v", iter, rerr)
							}
							for len(evCh) > 0 {
								if st := (<-evCh).GetStopped(); st != nil {
									stopped = st
								}
							}
							done = true
						case <-deadline:
							t.Fatalf("iter %d timed out waiting for fatal StoppedEvent on stream 1 (Send io.EOF masked status?)", iter)
						}
					}
					if stopped == nil || stopped.ErrorCode != int32(tc.code) || !strings.Contains(stopped.ErrorMessage, tc.msg) {
						t.Fatalf("iter %d expected fatal code=%v msg=%q, got %+v", iter, tc.code, tc.msg, stopped)
					}
					if got := streamsOpened.Load(); got != 1 {
						t.Fatalf("iter %d expected fatal stop on stream 1 (no spurious retry), opened %d streams", iter, got)
					}
				}
			})
		}
	})

	// 6. extractResourceNames whitespace, control-char, UTF-8 & dot-segment rejection -> InvalidArgument with 0 wire RPCs,
	// while valid domain-scoped & special-char paths (projects/google.com:my-proj/topics/t_1~a+b.c%20) still succeed.
	t.Run("ExtractResourceNamesWhitespaceRejection", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		mock.resetCounters()
		mock.mu.Lock()
		mock.onPublish = nil
		mock.onPull = nil
		mock.onStreamPull = nil
		mock.mu.Unlock()

		badTopics := []struct{ topic, proj string }{
			{"projects/   /topics/t", ""},
			{"projects/\t/topics/t", ""},
			{"projects/p/topics/ \t ", ""},
			{"projects/a b/topics/t", ""},
			{"projects/p/topics/a b", ""},
			{"projects/p/topics/a\tb", ""},
			{"projects/p/topics/a\nb", ""},
			{"projects/p/topics/a\u00a0b", ""},
			{"projects/p/topics/a\x00b", ""},
			{"projects/p/topics/a\x7fb", ""},
			{"  projects/p/topics/t  ", ""},
			{"projects/../topics/t", ""},
			{"projects/./topics/t", ""},
			{"projects/p/topics/..", ""},
			{"projects/p/topics/.", ""},
			{"..", "p"},
			{".", "p"},
			{"projects/p/topics/t", "   "},
			{"projects/p/topics/t", "a b"},
			{"projects/p/topics/t", "p\tb"},
			{"projects/p/topics/t", ".."},
			{"projects/p/topics/t", "."},
			{"t", "   "},
		}
		for _, tc := range badTopics {
			_, err := hGo.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
				Endpoint:  mock.addr,
				Topic:     tc.topic,
				ProjectId: tc.proj,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("CreatePublisher(topic=%q, proj=%q) expected InvalidArgument, got %v", tc.topic, tc.proj, err)
			}
		}

		badSubs := []struct{ sub, proj string }{
			{"projects/   /subscriptions/s", ""},
			{"projects/\t/subscriptions/s", ""},
			{"projects/p/subscriptions/ \t ", ""},
			{"projects/a b/subscriptions/s", ""},
			{"projects/p/subscriptions/a b", ""},
			{"projects/p/subscriptions/a\tb", ""},
			{"projects/p/subscriptions/a\nb", ""},
			{"projects/p/subscriptions/a\u00a0b", ""},
			{"projects/p/subscriptions/a\x00b", ""},
			{"projects/p/subscriptions/a\x7fb", ""},
			{"  projects/p/subscriptions/s  ", ""},
			{"projects/../subscriptions/s", ""},
			{"projects/./subscriptions/s", ""},
			{"projects/p/subscriptions/..", ""},
			{"projects/p/subscriptions/.", ""},
			{"..", "p"},
			{".", "p"},
			{"projects/p/subscriptions/s", "   "},
			{"projects/p/subscriptions/s", "a b"},
			{"projects/p/subscriptions/s", "p\tb"},
			{"projects/p/subscriptions/s", ".."},
			{"projects/p/subscriptions/s", "."},
			{"s", "   "},
		}
		for _, tc := range badSubs {
			_, err := hGo.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{
				Endpoint:     mock.addr,
				Subscription: tc.sub,
				ProjectId:    tc.proj,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("UnaryPull(sub=%q, proj=%q) expected InvalidArgument, got %v", tc.sub, tc.proj, err)
			}

			sess, err := hGo.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("SubscriberSession: %v", err)
			}
			_ = sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:     mock.addr,
						Subscription: tc.sub,
						ProjectId:    tc.proj,
					},
				},
			})
			if _, rerr := sess.Recv(); status.Code(rerr) != codes.InvalidArgument {
				t.Fatalf("SubscriberSession(sub=%q, proj=%q) expected InvalidArgument, got %v", tc.sub, tc.proj, rerr)
			}
		}
		if len(mock.getPublishCalls()) != 0 || len(mock.getPullCalls()) != 0 || len(mock.getStreamInits()) != 0 {
			t.Fatalf("expected 0 wire RPCs on malformed resource names")
		}

		// Valid domain-scoped & special-char topic path must still succeed and preserve project/topic IDs.
		_, _ = hGo.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		domainTopic := "projects/google.com:my-proj/topics/t_1~a+b.c%20"
		pub, err := hGo.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
			Endpoint:         mock.addr,
			Topic:            domainTopic,
			EnableMonitoring: true,
			CountThreshold:   protoInt64(1),
		})
		if err != nil {
			t.Fatalf("CreatePublisher(%q) failed: %v", domainTopic, err)
		}
		defer func() {
			_, _ = hGo.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pub.PublisherId})
		}()
		if _, err := hGo.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pub.PublisherId, Data: []byte("ok")}); err != nil {
			t.Fatalf("Publish(%q) failed: %v", domainTopic, err)
		}
		pcalls := mock.getPublishCalls()
		if len(pcalls) != 1 || pcalls[0].Req.Topic != domainTopic {
			t.Fatalf("expected wire Topic=%q, got %+v", domainTopic, pcalls)
		}
		drained, err := hGo.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		var sawDomainProj bool
		for _, sp := range drained.Spans {
			if sp.Attributes["gcp.project_id"].GetStringValue() == "google.com:my-proj" &&
				sp.Attributes["messaging.destination.name"].GetStringValue() == "t_1~a+b.c%20" {
				sawDomainProj = true
			}
		}
		if !sawDomainProj {
			t.Fatalf("expected domain-scoped gcp.project_id/destination in traces, got %+v", drained.Spans)
		}
	})

	// 7. --fast-timings + --keepalive-ping=100ms --keepalive-pong=10s -> >=21300ms on Go; 0/0 unchanged on C++.
	t.Run("KeepaliveFlagsLongPongAndSupportsKeepaliveGating", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		hGoKA := startTestProxyDirect(t, "--mode=conformant-go", "--fast-timings", "--keepalive-ping=100ms", "--keepalive-pong=10s")
		defer hGoKA.Shutdown(t, 2*time.Second)
		capsGo, err := hGoKA.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("Go GetCapabilities: %v", err)
		}
		dg := capsGo.GetDefaults()
		if dg.GetKeepalivePingIntervalMs() != 100 || dg.GetKeepalivePongTimeoutMs() != 10_000 || dg.GetSubscriberMaxExtensionMs() < 21_300 {
			t.Fatalf("Go --fast-timings 100ms/10s expected MaxExt >= 21300, got %+v", dg)
		}

		hGoLongKA := startTestProxyDirect(t, "--mode=conformant-go", "--keepalive-ping=60m", "--keepalive-pong=15s")
		defer hGoLongKA.Shutdown(t, 2*time.Second)
		capsGoLong, err := hGoLongKA.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("Go long GetCapabilities: %v", err)
		}
		dgl := capsGoLong.GetDefaults()
		if dgl.GetKeepalivePingIntervalMs() != 3_600_000 || dgl.GetKeepalivePongTimeoutMs() != 15_000 || dgl.GetSubscriberMaxExtensionMs() < 3_626_000 {
			t.Fatalf("Go 60m/15s without --fast-timings expected MaxExt >= 3626000, got %+v", dgl)
		}

		hCppKA := startTestProxyDirect(t, "--mode=conformant-cpp-partial", "--keepalive-ping=100ms", "--keepalive-pong=10s")
		defer hCppKA.Shutdown(t, 2*time.Second)
		capsCpp, err := hCppKA.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("C++ GetCapabilities: %v", err)
		}
		dc := capsCpp.GetDefaults()
		if capsCpp.SupportsKeepalive || dc.GetKeepalivePingIntervalMs() != 0 || dc.GetKeepalivePongTimeoutMs() != 0 || dc.GetSubscriberMaxExtensionMs() != 0 {
			t.Fatalf("C++ SupportsKeepalive=false expected 0/0/0 unchanged, got caps=%v defaults=%+v", capsCpp.SupportsKeepalive, dc)
		}

		hCppFastKA := startTestProxyDirect(t, "--mode=conformant-cpp-partial", "--fast-timings", "--keepalive-ping=100ms", "--keepalive-pong=10s")
		defer hCppFastKA.Shutdown(t, 2*time.Second)
		capsCppFast, err := hCppFastKA.Client.GetCapabilities(ctx, &proxypb.GetCapabilitiesRequest{})
		if err != nil {
			t.Fatalf("C++ fast GetCapabilities: %v", err)
		}
		dcf := capsCppFast.GetDefaults()
		if dcf.GetKeepalivePingIntervalMs() != 0 || dcf.GetKeepalivePongTimeoutMs() != 0 || dcf.GetSubscriberMaxExtensionMs() != 350 {
			t.Fatalf("C++ --fast-timings with keepalive flags expected 0/0/350 unchanged, got %+v", dcf)
		}
	})

	// 8. helpers_test.go un-normalized "status": "TIMEOUT" / "ERROR" backfill -> StatusDetail + TimeoutCount/ErrorCount.
	t.Run("ParseJSONReportUnnormalizedTimeoutErrorStatusDetail", func(t *testing.T) {
		singleRaw := []byte(`{
		  "summary": {
		    "total": 5, "pass": 1, "fail": 4,
		    "skipped_no_monitoring": 0, "unsupported_by_client": 0,
		    "timeout_count": 1, "error_count": 2
		  },
		  "scenarios": [
		    {"id": "S-TO", "status": "TIMEOUT", "error": "deadline"},
		    {"id": "S-ERR", "verdict": "error", "error": "crash"},
		    {"id": "S-EXP", "status": "FAIL", "status_detail": "ERROR", "error": "explicit"},
		    {"id": "S-PLAIN", "status": "FAIL", "error": "assertion"},
		    {"id": "S-PASS", "status": "PASS"}
		  ]
		}`)
		rep, err := ParseJSONReport(singleRaw)
		if err != nil {
			t.Fatalf("ParseJSONReport single un-normalized: %v", err)
		}
		AssertSummaryMatchesScenarios(t, rep)
		if sc, ok := rep.FindScenario("S-TO"); !ok || sc.Status != StatusFail || sc.StatusDetail != StatusTimeout {
			t.Fatalf("S-TO = %+v, ok=%v; want FAIL/TIMEOUT", sc, ok)
		}
		if sc, ok := rep.FindScenario("S-ERR"); !ok || sc.Status != StatusFail || sc.StatusDetail != StatusError {
			t.Fatalf("S-ERR = %+v, ok=%v; want FAIL/ERROR", sc, ok)
		}
		if sc, ok := rep.FindScenario("S-PLAIN"); !ok || sc.Status != StatusFail || sc.StatusDetail != "" {
			t.Fatalf("S-PLAIN = %+v, ok=%v; want FAIL/\"\"", sc, ok)
		}

		multiRaw := []byte(`{
		  "targets": {
		    "go": {
		      "summary": {"total": 2, "pass": 0, "fail": 2, "timeout_count": 1, "error_count": 1},
		      "scenarios": [{"id": "A", "status": "TIMEOUT"}, {"id": "B", "status": "ERROR"}]
		    },
		    "rust": {
		      "summary": {"total": 2, "pass": 1, "fail": 1, "timeout_count": 0, "error_count": 0},
		      "scenarios": [{"id": "A", "status": "PASS"}, {"id": "B", "status": "FAIL"}]
		    }
		  }
		}`)
		cmpRep, err := ParseJSONReport(multiRaw)
		if err != nil {
			t.Fatalf("ParseJSONReport multi un-normalized: %v", err)
		}
		AssertSummaryMatchesScenarios(t, cmpRep)
		if cmpRep.Targets["go"].Scenarios[0].StatusDetail != StatusTimeout || cmpRep.Targets["go"].Scenarios[1].StatusDetail != StatusError {
			t.Fatalf("multi target backfill mismatch: %+v", cmpRep.Targets["go"].Scenarios)
		}
	})
}

func assertIter3TraceInvariants(t *testing.T, spans []*proxypb.TraceSpan, subID string, wantSubCount int, allowCpp, allowAck, allowNack bool) (ackedCount, nackedCount, expiredCount int) {
	t.Helper()
	byID := make(map[string]*proxypb.TraceSpan, len(spans))
	for _, sp := range spans {
		if sp.EndTime == nil {
			t.Fatalf("span %q (id=%s) missing EndTime", sp.Name, sp.SpanId)
		}
		if _, dup := byID[sp.SpanId]; dup {
			t.Fatalf("duplicate SpanId %q (%s)", sp.SpanId, sp.Name)
		}
		byID[sp.SpanId] = sp
	}

	subSpanName := subID + " subscribe"
	procSpanName := subID + " process"
	ackSpanName := subID + " ack"
	nackSpanName := subID + " nack"
	schedSpanName := "subscribe scheduler"
	if allowCpp {
		schedSpanName = "subscriber scheduler"
	}

	var subSpans []*proxypb.TraceSpan
	subByID := make(map[string]*proxypb.TraceSpan, wantSubCount)
	for _, sp := range spans {
		for _, lnk := range sp.Links {
			if _, ok := byID[lnk.SpanId]; !ok {
				t.Fatalf("span %q (%s) has dangling link to missing SpanId %q", sp.Name, sp.SpanId, lnk.SpanId)
			}
		}
		if sp.Name == subSpanName {
			subSpans = append(subSpans, sp)
			subByID[sp.SpanId] = sp
		}
	}
	if len(subSpans) != wantSubCount {
		t.Fatalf("expected %d %q spans, got %d", wantSubCount, subSpanName, len(subSpans))
	}

	processSpanBySubID := make(map[string]*proxypb.TraceSpan, wantSubCount)
	ccCountBySubID := make(map[string]int, wantSubCount)
	schedCountBySubID := make(map[string]int, wantSubCount)
	for _, sp := range spans {
		switch sp.Name {
		case procSpanName:
			if subByID[sp.ParentSpanId] == nil {
				t.Fatalf("process span %s has non-subscribe parent %q", sp.SpanId, sp.ParentSpanId)
			}
			if processSpanBySubID[sp.ParentSpanId] != nil {
				t.Fatalf("duplicate process span under subscribe %s", sp.ParentSpanId)
			}
			processSpanBySubID[sp.ParentSpanId] = sp
		case "subscriber concurrency control":
			if subByID[sp.ParentSpanId] == nil {
				t.Fatalf("cc span %s has non-subscribe parent %q", sp.SpanId, sp.ParentSpanId)
			}
			ccCountBySubID[sp.ParentSpanId]++
		case schedSpanName:
			if subByID[sp.ParentSpanId] == nil {
				t.Fatalf("scheduler span %s (%q) has non-subscribe parent %q", sp.SpanId, sp.Name, sp.ParentSpanId)
			}
			schedCountBySubID[sp.ParentSpanId]++
		}
	}

	hasReverseLink := func(from, to *proxypb.TraceSpan) bool {
		for _, lnk := range from.Links {
			if lnk.SpanId == to.SpanId {
				return true
			}
		}
		return false
	}

	for _, sp := range subSpans {
		if ccCountBySubID[sp.SpanId] != 1 {
			t.Fatalf("subscribe span %s expected 1 %q child, got %d", sp.SpanId, "subscriber concurrency control", ccCountBySubID[sp.SpanId])
		}
		if schedCountBySubID[sp.SpanId] != 1 {
			t.Fatalf("subscribe span %s expected 1 %q child, got %d", sp.SpanId, schedSpanName, schedCountBySubID[sp.SpanId])
		}

		res := sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue()
		var hasAckStart, hasAckEnd bool
		var hasNackStart, hasNackEnd bool
		var hasCppAck, hasCppAckStart, hasCppAckEnd bool
		var hasCppNack, hasCppNackStart, hasCppNackEnd bool
		var hasCppExpired bool
		for _, ev := range sp.Events {
			switch ev.Name {
			case "ack called", "nack called":
				t.Fatalf("subscribe span %s unexpectedly carries process event %q on subscribe span", sp.SpanId, ev.Name)
			case "ack start":
				hasAckStart = true
			case "ack end":
				hasAckEnd = true
			case "nack start":
				hasNackStart = true
			case "nack end":
				hasNackEnd = true
			case "gl-cpp.message_ack":
				hasCppAck = true
			case "gl-cpp.ack_start":
				hasCppAck = true
				hasCppAckStart = true
			case "gl-cpp.ack_end":
				hasCppAck = true
				hasCppAckEnd = true
			case "gl-cpp.message_nack":
				hasCppNack = true
			case "gl-cpp.nack_start":
				hasCppNack = true
				hasCppNackStart = true
			case "gl-cpp.nack_end":
				hasCppNack = true
				hasCppNackEnd = true
			case "gl-cpp.expired":
				hasCppExpired = true
			}
		}

		var hasProcAckCalled, hasProcNackCalled bool
		if ps := processSpanBySubID[sp.SpanId]; ps != nil {
			for _, ev := range ps.Events {
				switch ev.Name {
				case "ack called":
					hasProcAckCalled = true
				case "nack called":
					hasProcNackCalled = true
				}
			}
			if hasProcAckCalled && hasProcNackCalled {
				t.Fatalf("process span %s has both ack called and nack called", ps.SpanId)
			}
		}

		var linksAck, linksNack bool
		for _, lnk := range sp.Links {
			target := byID[lnk.SpanId]
			switch target.Name {
			case ackSpanName:
				linksAck = true
			case nackSpanName:
				linksNack = true
			}
		}

		switch res {
		case "expired":
			expiredCount++
			if hasProcAckCalled || hasAckStart || hasAckEnd || hasCppAck || linksAck {
				t.Fatalf("expired subscribe span %s has conflicting ack evidence: subEvents=%+v procAck=%v linksAck=%v", sp.SpanId, sp.Events, hasProcAckCalled, linksAck)
			}
			if hasProcNackCalled || hasNackStart || hasNackEnd || hasCppNack || linksNack {
				t.Fatalf("expired subscribe span %s has conflicting nack evidence: subEvents=%+v procNack=%v linksNack=%v", sp.SpanId, sp.Events, hasProcNackCalled, linksNack)
			}
		case "acked", "ack":
			ackedCount++
			if !allowAck {
				t.Fatalf("unexpected acked subscribe span %s (result=%q)", sp.SpanId, res)
			}
			if hasProcNackCalled || hasNackStart || hasNackEnd || hasCppNack || hasCppExpired || linksNack {
				t.Fatalf("acked subscribe span %s has conflicting nack/expired evidence: subEvents=%+v procNack=%v linksNack=%v", sp.SpanId, sp.Events, hasProcNackCalled, linksNack)
			}
			if allowCpp {
				if !hasCppAckStart || !hasCppAckEnd {
					t.Fatalf("C++ acked subscribe span %s missing gl-cpp.ack_start/ack_end: events=%+v", sp.SpanId, sp.Events)
				}
			} else if !hasAckStart || !hasAckEnd || !linksAck {
				t.Fatalf("acked subscribe span %s missing ack start/end or link: events=%+v links=%+v", sp.SpanId, sp.Events, sp.Links)
			}
		case "nacked", "nack":
			nackedCount++
			if !allowNack {
				t.Fatalf("unexpected nacked subscribe span %s (result=%q)", sp.SpanId, res)
			}
			if hasProcAckCalled || hasAckStart || hasAckEnd || hasCppAck || hasCppExpired || linksAck {
				t.Fatalf("nacked subscribe span %s has conflicting ack/expired evidence: subEvents=%+v procAck=%v linksAck=%v", sp.SpanId, sp.Events, hasProcAckCalled, linksAck)
			}
			if allowCpp {
				if !hasCppNackStart || !hasCppNackEnd {
					t.Fatalf("C++ nacked subscribe span %s missing gl-cpp.nack_start/nack_end: events=%+v", sp.SpanId, sp.Events)
				}
			} else if !hasNackStart || !hasNackEnd || !linksNack {
				t.Fatalf("nacked subscribe span %s missing nack start/end or link: events=%+v links=%+v", sp.SpanId, sp.Events, sp.Links)
			}
		default:
			t.Fatalf("subscribe span %s unexpected result %q (attrs=%+v)", sp.SpanId, res, sp.Attributes)
		}
	}

	for _, sp := range spans {
		if sp.Name != ackSpanName && sp.Name != nackSpanName {
			continue
		}
		isAck := sp.Name == ackSpanName
		if allowCpp {
			var subTarget *proxypb.TraceSpan
			if p, ok := byID[sp.ParentSpanId]; ok && p.Name == subSpanName {
				subTarget = p
			} else {
				for _, lnk := range sp.Links {
					if tgt, ok := byID[lnk.SpanId]; ok && tgt.Name == subSpanName {
						subTarget = tgt
						break
					}
				}
			}
			if subTarget == nil {
				t.Fatalf("unexpected unlinked C++ settlement span %q (%s): links=%+v parent=%s", sp.Name, sp.SpanId, sp.Links, sp.ParentSpanId)
			}
			subRes := subTarget.Attributes["messaging.gcp_pubsub.result"].GetStringValue()
			if isAck && subRes != "ack" && subRes != "acked" {
				t.Fatalf("C++ ack span %s attached to non-acked subscribe span %s (result=%q)", sp.SpanId, subTarget.SpanId, subRes)
			}
			if !isAck && subRes != "nack" && subRes != "nacked" {
				t.Fatalf("C++ nack span %s attached to non-nacked subscribe span %s (result=%q)", sp.SpanId, subTarget.SpanId, subRes)
			}
			continue
		}

		if len(sp.Links) == 0 {
			t.Fatalf("unexpected unlinked Pub/Sub span %q (%s) with 0 links (attrs=%+v)", sp.Name, sp.SpanId, sp.Attributes)
		}
		if bcAttr, ok := sp.Attributes["messaging.batch.message_count"]; ok {
			if bcAttr.GetInt64Value() != int64(len(sp.Links)) {
				t.Fatalf("span %q (%s) batch_count=%d != len(Links)=%d", sp.Name, sp.SpanId, bcAttr.GetInt64Value(), len(sp.Links))
			}
		}
		for _, lnk := range sp.Links {
			sub := byID[lnk.SpanId]
			if sub.Name != subSpanName {
				t.Fatalf("span %q (%s) links to unexpected span %q (%s)", sp.Name, sp.SpanId, sub.Name, sub.SpanId)
			}
			if !hasReverseLink(sub, sp) {
				t.Fatalf("dangling reverse link: %q (%s) links to subscribe %s without reverse link", sp.Name, sp.SpanId, sub.SpanId)
			}
			subRes := sub.Attributes["messaging.gcp_pubsub.result"].GetStringValue()
			if isAck && subRes != "acked" {
				t.Fatalf("incompatible link: %q (%s) links to subscribe %s with result=%q", sp.Name, sp.SpanId, sub.SpanId, subRes)
			}
			if !isAck && subRes != "nacked" {
				t.Fatalf("incompatible link: %q (%s) links to subscribe %s with result=%q", sp.Name, sp.SpanId, sub.SpanId, subRes)
			}
		}
	}
	return ackedCount, nackedCount, expiredCount
}

func TestInfra_E2EM1BIter3Regressions(t *testing.T) {
	raceBin := buildRaceProxyBinary(t)
	mock := startMiniPubSubServer(t)

	runMultiCommandBurst := func(t *testing.T, subID string, total int, second, third proxypb.SettleMessageCommand_SettleAction) ([]string, []*proxypb.TraceSpan) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		h := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-go", "--fast-timings")
		defer h.Shutdown(t, 4*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = nil
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				msgs := make([]*pubsubpb.ReceivedMessage, total)
				for i := 0; i < total; i++ {
					msgs[i] = &pubsubpb.ReceivedMessage{
						AckId:   fmt.Sprintf("%s-ack-%03d", subID, i),
						Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("%s-msg-%03d", subID, i), Data: []byte("burst")},
					}
				}
				_ = stream.Send(&pubsubpb.StreamingPullResponse{ReceivedMessages: msgs})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         mock.addr,
					Subscription:     "projects/p/subscriptions/" + subID,
					EnableMonitoring: true,
					AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		wantResult := second == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT ||
			second == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT
		deliveredTokens := make(map[string]struct{}, total)
		resultTokens := make(map[string]*proxypb.SettlementResultEvent, total)
		for len(deliveredTokens) < total || (wantResult && len(resultTokens) < total) {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv loop (%d/%d delivered, %d results): %v", len(deliveredTokens), total, len(resultTokens), rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				deliveredTokens[md.DeliveryToken] = struct{}{}
				for _, act := range []proxypb.SettleMessageCommand_SettleAction{
					proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE,
					second,
					third,
				} {
					if serr := sess.Send(&proxypb.SubscriberCommand{
						Command: &proxypb.SubscriberCommand_Settle{
							Settle: &proxypb.SettleMessageCommand{DeliveryToken: md.DeliveryToken, Action: act},
						},
					}); serr != nil {
						t.Fatalf("Send(%v): %v", act, serr)
					}
				}
			}
			if sr := ev.GetSettlementResult(); sr != nil {
				if sr.Status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS {
					t.Fatalf("SettlementResult unexpected status: %+v", sr)
				}
				resultTokens[sr.DeliveryToken] = sr
			}
		}
		if wantResult {
			for tok := range deliveredTokens {
				if _, ok := resultTokens[tok]; !ok {
					t.Fatalf("missing SettlementResultEvent for token %q", tok)
				}
			}
		}

		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		}); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		_ = sess.CloseSend()
		var stopped *proxypb.StoppedEvent
		for {
			ev, rerr := sess.Recv()
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				t.Fatalf("Recv after Stop: %v", rerr)
			}
			if sr := ev.GetSettlementResult(); sr != nil {
				resultTokens[sr.DeliveryToken] = sr
			}
			if st := ev.GetStopped(); st != nil {
				stopped = st
			}
		}
		if stopped == nil || stopped.ErrorCode != 0 || int(stopped.TotalDelivered) != total {
			t.Fatalf("unexpected StoppedEvent: %+v (want TotalDelivered=%d)", stopped, total)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		if err := h.Shutdown(t, 4*time.Second); err != nil {
			t.Fatalf("-race proxy exited with error (stderr=%q): %v", h.Stderr(), err)
		}
		if strings.Contains(h.Stderr(), "DATA RACE") {
			t.Fatalf("DATA RACE detected in stderr:\n%s", h.Stderr())
		}

		var deliveredIDs []string
		for i := 0; i < total; i++ {
			deliveredIDs = append(deliveredIDs, fmt.Sprintf("%s-ack-%03d", subID, i))
		}
		return deliveredIDs, drained.Spans
	}

	t.Run("RapidMultiCommand_ReturnWithoutSettle_Ack_Nack_StrictFIFO_150x", func(t *testing.T) {
		const n = 150
		wantIDs, spans := runMultiCommandBurst(t, "sub-r3-ran", n,
			proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
			proxypb.SettleMessageCommand_SETTLE_ACTION_NACK,
		)
		acks := mock.getAckCalls()
		modAcks := mock.getModAckCalls()
		assertNoDuplicateWireAckIDs(t, acks, modAcks)

		acked := make(map[string]int, n)
		for _, c := range acks {
			for _, id := range c.Req.AckIds {
				acked[id]++
			}
		}
		var nacks int
		for _, c := range modAcks {
			if c.Req.AckDeadlineSeconds == 0 {
				nacks += len(c.Req.AckIds)
			}
		}
		if len(acked) != n || nacks != 0 {
			t.Fatalf("FIFO violation on RWS->ACK->NACK: acked=%d (want %d), nacks=%d (want 0)", len(acked), n, nacks)
		}
		for _, id := range wantIDs {
			if acked[id] != 1 {
				t.Fatalf("ackID %q ack count=%d (want 1)", id, acked[id])
			}
		}
		ackedSpans, nackedSpans, expSpans := assertIter3TraceInvariants(t, spans, "sub-r3-ran", n, false, true, false)
		if ackedSpans != n || nackedSpans != 0 || expSpans != 0 {
			t.Fatalf("unexpected trace counts: acked=%d nacked=%d expired=%d", ackedSpans, nackedSpans, expSpans)
		}
	})

	t.Run("RapidMultiCommand_ReturnWithoutSettle_Nack_Ack_StrictFIFO_150x", func(t *testing.T) {
		const n = 150
		wantIDs, spans := runMultiCommandBurst(t, "sub-r3-rna", n,
			proxypb.SettleMessageCommand_SETTLE_ACTION_NACK,
			proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
		)
		acks := mock.getAckCalls()
		modAcks := mock.getModAckCalls()
		assertNoDuplicateWireAckIDs(t, acks, modAcks)

		if len(acks) != 0 {
			t.Fatalf("FIFO violation on RWS->NACK->ACK: expected 0 wire ACKs, got %+v", acks)
		}
		nacked := make(map[string]int, n)
		for _, c := range modAcks {
			if c.Req.AckDeadlineSeconds == 0 {
				for _, id := range c.Req.AckIds {
					nacked[id]++
				}
			}
		}
		if len(nacked) != n {
			t.Fatalf("FIFO violation on RWS->NACK->ACK: nacked=%d (want %d)", len(nacked), n)
		}
		for _, id := range wantIDs {
			if nacked[id] != 1 {
				t.Fatalf("ackID %q nack count=%d (want 1)", id, nacked[id])
			}
		}
		ackedSpans, nackedSpans, expSpans := assertIter3TraceInvariants(t, spans, "sub-r3-rna", n, false, false, true)
		if ackedSpans != 0 || nackedSpans != n || expSpans != 0 {
			t.Fatalf("unexpected trace counts: acked=%d nacked=%d expired=%d", ackedSpans, nackedSpans, expSpans)
		}
	})

	t.Run("RapidMultiCommand_ReturnWithoutSettle_AckWithResult_Nack_StrictFIFO_100x", func(t *testing.T) {
		const n = 100
		wantIDs, spans := runMultiCommandBurst(t, "sub-r3-rawn", n,
			proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT,
			proxypb.SettleMessageCommand_SETTLE_ACTION_NACK,
		)
		acks := mock.getAckCalls()
		modAcks := mock.getModAckCalls()
		assertNoDuplicateWireAckIDs(t, acks, modAcks)

		acked := make(map[string]int, n)
		for _, c := range acks {
			for _, id := range c.Req.AckIds {
				acked[id]++
			}
		}
		var nacks int
		for _, c := range modAcks {
			if c.Req.AckDeadlineSeconds == 0 {
				nacks += len(c.Req.AckIds)
			}
		}
		if len(acked) != n || nacks != 0 {
			t.Fatalf("FIFO violation on RWS->ACK_WITH_RESULT->NACK: acked=%d (want %d), nacks=%d (want 0)", len(acked), n, nacks)
		}
		for _, id := range wantIDs {
			if acked[id] != 1 {
				t.Fatalf("ackID %q ack count=%d (want 1)", id, acked[id])
			}
		}
		ackedSpans, nackedSpans, expSpans := assertIter3TraceInvariants(t, spans, "sub-r3-rawn", n, false, true, false)
		if ackedSpans != n || nackedSpans != 0 || expSpans != 0 {
			t.Fatalf("unexpected trace counts: acked=%d nacked=%d expired=%d", ackedSpans, nackedSpans, expSpans)
		}
	})

	runBatch12ExpiryRace := func(t *testing.T, subPrefix string, iters int, returnBeforeSleep bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		h := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-go", "--fast-timings")
		defer h.Shutdown(t, 4*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		const batchSize = 12
		var totalAcked, totalExpired int
		for iter := 0; iter < iters; iter++ {
			subID := fmt.Sprintf("%s-%d", subPrefix, iter)
			mock.resetCounters()
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = nil
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
				if streamIdx == 1 {
					msgs := make([]*pubsubpb.ReceivedMessage, batchSize)
					for i := 0; i < batchSize; i++ {
						msgs[i] = &pubsubpb.ReceivedMessage{
							AckId:   fmt.Sprintf("%s-a-%d", subID, i),
							Message: &pubsubpb.PubsubMessage{MessageId: fmt.Sprintf("%s-m-%d", subID, i), Data: []byte("exp")},
						}
					}
					_ = stream.Send(&pubsubpb.StreamingPullResponse{ReceivedMessages: msgs})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("iter %d SubscriberSession: %v", iter, err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:         mock.addr,
						Subscription:     "projects/p/subscriptions/" + subID,
						EnableMonitoring: true,
						MaxExtensionMs:   protoInt64(80),
						AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				t.Fatalf("iter %d Start: %v", iter, err)
			}

			tokens := make([]string, 0, batchSize)
			for len(tokens) < batchSize {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("iter %d Recv: %v", iter, rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					tokens = append(tokens, md.DeliveryToken)
					if returnBeforeSleep {
						if serr := sess.Send(&proxypb.SubscriberCommand{
							Command: &proxypb.SubscriberCommand_Settle{
								Settle: &proxypb.SettleMessageCommand{
									DeliveryToken: md.DeliveryToken,
									Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE,
								},
							},
						}); serr != nil {
							t.Fatalf("iter %d RETURN_WITHOUT_SETTLE: %v", iter, serr)
						}
					}
				}
			}

			// Sweep 20ms..95ms across MaxExtensionMs=80ms so batches fall before, across, and after expiryTick.
			waitMs := 20
			if iters > 1 {
				waitMs += (iter * 75) / (iters - 1)
			}
			time.Sleep(time.Duration(waitMs) * time.Millisecond)

			for _, tok := range tokens {
				if serr := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Settle{
						Settle: &proxypb.SettleMessageCommand{
							DeliveryToken: tok,
							Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
						},
					},
				}); serr != nil {
					t.Fatalf("iter %d ACK: %v", iter, serr)
				}
			}

			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			}); err != nil {
				t.Fatalf("iter %d Stop: %v", iter, err)
			}
			_ = sess.CloseSend()
			var stopped *proxypb.StoppedEvent
			for {
				ev, rerr := sess.Recv()
				if rerr == io.EOF {
					break
				}
				if rerr != nil {
					t.Fatalf("iter %d Recv post-stop: %v", iter, rerr)
				}
				if st := ev.GetStopped(); st != nil {
					stopped = st
				}
			}
			if stopped == nil || stopped.ErrorCode != 0 || stopped.TotalDelivered != batchSize {
				t.Fatalf("iter %d unexpected StoppedEvent: %+v", iter, stopped)
			}

			drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("iter %d DrainTraces: %v", iter, err)
			}
			acked, nacked, expired := assertIter3TraceInvariants(t, drained.Spans, subID, batchSize, false, true, false)
			if nacked != 0 || acked+expired != batchSize {
				t.Fatalf("iter %d unexpected counts: acked=%d nacked=%d expired=%d", iter, acked, nacked, expired)
			}
			ackedSubIDs := make(map[string]bool, acked)
			for _, sp := range drained.Spans {
				if sp.Name == subID+" subscribe" && sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() == "acked" {
					ackedSubIDs[sp.SpanId] = true
				}
			}
			for _, sp := range drained.Spans {
				if sp.Name != subID+" process" {
					continue
				}
				var sawAckCalled, sawNackCalled bool
				for _, ev := range sp.Events {
					if ev.Name == "ack called" {
						sawAckCalled = true
					}
					if ev.Name == "nack called" {
						sawNackCalled = true
					}
				}
				if returnBeforeSleep {
					if sawAckCalled || sawNackCalled {
						t.Fatalf("iter %d RETURN_WITHOUT_SETTLE process span %s unexpectedly recorded ack/nack called: %+v", iter, sp.SpanId, sp.Events)
					}
				} else if ackedSubIDs[sp.ParentSpanId] && !sawAckCalled {
					t.Fatalf("iter %d direct-ack process span %s missing \"ack called\": %+v", iter, sp.SpanId, sp.Events)
				}
			}
			totalAcked += acked
			totalExpired += expired
		}

		if totalAcked == 0 || totalExpired == 0 {
			t.Fatalf("race window sweep did not exercise both outcomes across %d batches: totalAcked=%d totalExpired=%d", iters, totalAcked, totalExpired)
		}
		if err := h.Shutdown(t, 4*time.Second); err != nil {
			t.Fatalf("-race proxy exited with error (stderr=%q): %v", h.Stderr(), err)
		}
		if strings.Contains(h.Stderr(), "DATA RACE") {
			t.Fatalf("DATA RACE detected in stderr:\n%s", h.Stderr())
		}
	}

	t.Run("Batch12DirectAckVsExpiryTick_NoConflictingTraces_150x", func(t *testing.T) {
		runBatch12ExpiryRace(t, "sub-r3-dexp", 150, false)
	})

	t.Run("Batch12ExpiryVsReturnWithoutSettleAck_NoConflictingTraces_100x", func(t *testing.T) {
		runBatch12ExpiryRace(t, "sub-r3-rexp", 100, true)
	})

	t.Run("CppPostExpiryAck_WireAckPreservedAndNoConflictingOrDanglingSpans", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		h := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-cpp-partial", "--fast-timings")
		defer h.Shutdown(t, 4*time.Second)
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		mock.resetCounters()
		mock.mu.Lock()
		mock.onAck = nil
		mock.onModAck = nil
		mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, streamIdx int) error {
			if streamIdx == 1 {
				_ = stream.Send(&pubsubpb.StreamingPullResponse{
					SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
						ExactlyOnceDeliveryEnabled: true,
					},
					ReceivedMessages: []*pubsubpb.ReceivedMessage{
						{AckId: "cpp-postexp-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-cpp-postexp-1", Data: []byte("c")}},
					},
				})
			}
			<-stream.Context().Done()
			return nil
		}
		mock.mu.Unlock()

		sess, err := h.Client.SubscriberSession(ctx)
		if err != nil {
			t.Fatalf("SubscriberSession: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Start{
				Start: &proxypb.StartSubscriberCommand{
					Endpoint:         mock.addr,
					Subscription:     "projects/p/subscriptions/sub-r3-cppexp",
					EnableMonitoring: true,
					MaxExtensionMs:   protoInt64(40),
					AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
				},
			},
		}); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var tok string
		for tok == "" {
			ev, rerr := sess.Recv()
			if rerr != nil {
				t.Fatalf("Recv: %v", rerr)
			}
			if md := ev.GetMessageDelivered(); md != nil {
				tok = md.DeliveryToken
			}
		}

		// Wait past MaxExtensionMs=40ms so expiryTick definitively expires the lease first.
		time.Sleep(110 * time.Millisecond)

		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Settle{
				Settle: &proxypb.SettleMessageCommand{
					DeliveryToken: tok,
					Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
				},
			},
		}); err != nil {
			t.Fatalf("ACK post-expiry: %v", err)
		}
		if err := sess.Send(&proxypb.SubscriberCommand{
			Command: &proxypb.SubscriberCommand_Stop{
				Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
			},
		}); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		_ = sess.CloseSend()
		for {
			if _, rerr := sess.Recv(); rerr != nil {
				break
			}
		}

		acks := mock.getAckCalls()
		var sawWireAck bool
		for _, c := range acks {
			for _, id := range c.Req.AckIds {
				if id == "cpp-postexp-1" {
					sawWireAck = true
				}
			}
		}
		if !sawWireAck {
			t.Fatalf("expected C++ post-expiry wire Acknowledge to be preserved, got %+v", acks)
		}

		drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
		if err != nil {
			t.Fatalf("DrainTraces: %v", err)
		}
		ackedSpans, nackedSpans, expSpans := assertIter3TraceInvariants(t, drained.Spans, "sub-r3-cppexp", 1, true, false, false)
		if ackedSpans != 0 || nackedSpans != 0 || expSpans != 1 {
			t.Fatalf("expected 1 expired C++ subscribe span with 0 ack/nack spans, got acked=%d nacked=%d expired=%d", ackedSpans, nackedSpans, expSpans)
		}
		if err := h.Shutdown(t, 4*time.Second); err != nil {
			t.Fatalf("-race proxy exited with error (stderr=%q): %v", h.Stderr(), err)
		}
		if strings.Contains(h.Stderr(), "DATA RACE") {
			t.Fatalf("DATA RACE detected in stderr:\n%s", h.Stderr())
		}
	})
}

func TestInfra_E2EM1BIter4Regressions(t *testing.T) {
	raceBin := buildRaceProxyBinary(t)
	mock := startMiniPubSubServer(t)

	countEvent := func(sp *proxypb.TraceSpan, name string) int {
		n := 0
		for _, ev := range sp.Events {
			if ev.Name == name {
				n++
			}
		}
		return n
	}

	// 1. Pre-delivery fc.acquire / MaxConcurrency & nackAndCloseRemaining expiry MUST emit cc + scheduler child spans (Go & C++).
	t.Run("PreDeliveryFCMaxConcurrencyAndNackRemainingExpiryChildSpans", func(t *testing.T) {
		profiles := []struct {
			name      string
			modeFlag  string
			cppMode   bool
			schedName string
		}{
			{name: "Go", modeFlag: "--mode=conformant-go", cppMode: false, schedName: "subscribe scheduler"},
			{name: "Cpp", modeFlag: "--mode=conformant-cpp-partial", cppMode: true, schedName: "subscriber scheduler"},
		}
		for _, prof := range profiles {
			t.Run(prof.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				h := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, prof.modeFlag, "--fast-timings")
				defer func() {
					if err := h.Shutdown(t, 4*time.Second); err != nil {
						t.Errorf("-race proxy shutdown error (stderr=%q): %v", h.Stderr(), err)
					}
					if strings.Contains(h.Stderr(), "DATA RACE") {
						t.Errorf("DATA RACE detected in stderr:\n%s", h.Stderr())
					}
				}()

				assertPreDeliveryExpiryChildren := func(spans []*proxypb.TraceSpan, subID string, wantSubCount, wantProcCount int) {
					t.Helper()
					subByID := make(map[string]*proxypb.TraceSpan, wantSubCount)
					var procCnt int
					for _, sp := range spans {
						if sp.EndTime == nil {
							t.Fatalf("%s span %q (%s) missing EndTime", subID, sp.Name, sp.SpanId)
						}
						if sp.Name == subID+" subscribe" {
							subByID[sp.SpanId] = sp
							if got := sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue(); got != "expired" {
								t.Fatalf("%s subscribe span %s expected result=expired, got %q", subID, sp.SpanId, got)
							}
						}
						if sp.Name == subID+" process" {
							procCnt++
						}
					}
					if len(subByID) != wantSubCount || procCnt != wantProcCount {
						t.Fatalf("%s expected sub=%d proc=%d, got sub=%d proc=%d", subID, wantSubCount, wantProcCount, len(subByID), procCnt)
					}
					ccBySub := make(map[string]int, wantSubCount)
					schedBySub := make(map[string]int, wantSubCount)
					for _, sp := range spans {
						switch sp.Name {
						case "subscriber concurrency control":
							if subByID[sp.ParentSpanId] == nil {
								t.Fatalf("%s cc span %s has non-subscribe parent %q", subID, sp.SpanId, sp.ParentSpanId)
							}
							if prof.cppMode {
								if sp.Attributes["messaging.system"].GetStringValue() != "gcp_pubsub" {
									t.Fatalf("%s C++ cc span missing messaging.system: %+v", subID, sp.Attributes)
								}
							} else if len(sp.Attributes) != 0 {
								t.Fatalf("%s Go cc span expected zero attributes, got %+v", subID, sp.Attributes)
							}
							ccBySub[sp.ParentSpanId]++
						case prof.schedName:
							if subByID[sp.ParentSpanId] == nil {
								t.Fatalf("%s scheduler span %s has non-subscribe parent %q", subID, sp.SpanId, sp.ParentSpanId)
							}
							if prof.cppMode {
								if sp.Attributes["messaging.system"].GetStringValue() != "gcp_pubsub" {
									t.Fatalf("%s C++ scheduler span missing messaging.system: %+v", subID, sp.Attributes)
								}
							} else if len(sp.Attributes) != 0 {
								t.Fatalf("%s Go scheduler span expected zero attributes, got %+v", subID, sp.Attributes)
							}
							schedBySub[sp.ParentSpanId]++
						}
					}
					for id := range subByID {
						if ccBySub[id] != 1 || schedBySub[id] != 1 {
							t.Fatalf("%s subscribe %s expected 1 cc and 1 %q child, got cc=%d sched=%d", subID, id, prof.schedName, ccBySub[id], schedBySub[id])
						}
					}
				}

				// Part A: MaxConcurrency=1 blocks msg2 in fc.acquire past MaxExtensionMs=60ms.
				_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				mock.resetCounters()
				mock.mu.Lock()
				mock.onAck = nil
				mock.onModAck = nil
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
					if idx == 1 {
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							ReceivedMessages: []*pubsubpb.ReceivedMessage{
								{AckId: "r4-mc-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-mc-1", Data: []byte("1")}},
								{AckId: "r4-mc-2", Message: &pubsubpb.PubsubMessage{MessageId: "m-mc-2", Data: []byte("2")}},
							},
						})
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()

				subMC := "sub-r4-mc-" + strings.ToLower(prof.name)
				sessA, err := h.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("Part A SubscriberSession: %v", err)
				}
				if err := sessA.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:         mock.addr,
							Subscription:     "projects/p/subscriptions/" + subMC,
							EnableMonitoring: true,
							MaxConcurrency:   protoInt32(1),
							MaxExtensionMs:   protoInt64(60),
							AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
						},
					},
				}); err != nil {
					t.Fatalf("Part A Start: %v", err)
				}
				for got := 0; got < 1; {
					ev, rerr := sessA.Recv()
					if rerr != nil {
						t.Fatalf("Part A Recv: %v", rerr)
					}
					if ev.GetMessageDelivered() != nil {
						got++
					}
				}
				time.Sleep(140 * time.Millisecond)
				if err := sessA.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Stop{
						Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
					},
				}); err != nil {
					t.Fatalf("Part A Stop: %v", err)
				}
				_ = sessA.CloseSend()
				var stoppedA *proxypb.StoppedEvent
				for {
					ev, rerr := sessA.Recv()
					if rerr == io.EOF {
						break
					}
					if rerr != nil {
						t.Fatalf("Part A Recv post-stop: %v", rerr)
					}
					if st := ev.GetStopped(); st != nil {
						stoppedA = st
					}
				}
				if stoppedA == nil || stoppedA.TotalDelivered != 1 {
					t.Fatalf("Part A expected TotalDelivered=1, got %+v", stoppedA)
				}
				drainedA, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				if err != nil {
					t.Fatalf("Part A DrainTraces: %v", err)
				}
				assertPreDeliveryExpiryChildren(drainedA.Spans, subMC, 2, 1)

				// Part B: nackAndCloseRemaining at Stop(NACK_IMMEDIATELY) after MaxExtensionMs=50ms with queued msgs [n2, n3].
				mock.resetCounters()
				mock.mu.Lock()
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
					if idx == 1 {
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							ReceivedMessages: []*pubsubpb.ReceivedMessage{
								{AckId: "r4-ncr-1", Message: &pubsubpb.PubsubMessage{MessageId: "m-ncr-1", Data: []byte("1")}},
								{AckId: "r4-ncr-2", Message: &pubsubpb.PubsubMessage{MessageId: "m-ncr-2", Data: []byte("2")}},
								{AckId: "r4-ncr-3", Message: &pubsubpb.PubsubMessage{MessageId: "m-ncr-3", Data: []byte("3")}},
							},
						})
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()

				subNCR := "sub-r4-ncr-" + strings.ToLower(prof.name)
				sessB, err := h.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("Part B SubscriberSession: %v", err)
				}
				if err := sessB.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:               mock.addr,
							Subscription:           "projects/p/subscriptions/" + subNCR,
							EnableMonitoring:       true,
							MaxOutstandingMessages: protoInt64(1),
							MaxExtensionMs:         protoInt64(50),
							AutoSettleMode:         proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
						},
					},
				}); err != nil {
					t.Fatalf("Part B Start: %v", err)
				}
				for got := 0; got < 1; {
					ev, rerr := sessB.Recv()
					if rerr != nil {
						t.Fatalf("Part B Recv: %v", rerr)
					}
					if ev.GetMessageDelivered() != nil {
						got++
					}
				}
				time.Sleep(120 * time.Millisecond)
				if err := sessB.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Stop{
						Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_NACK_IMMEDIATELY},
					},
				}); err != nil {
					t.Fatalf("Part B Stop: %v", err)
				}
				_ = sessB.CloseSend()
				var stoppedB *proxypb.StoppedEvent
				for {
					ev, rerr := sessB.Recv()
					if rerr == io.EOF {
						break
					}
					if rerr != nil {
						t.Fatalf("Part B Recv post-stop: %v", rerr)
					}
					if st := ev.GetStopped(); st != nil {
						stoppedB = st
					}
				}
				if stoppedB == nil || stoppedB.TotalDelivered != 1 {
					t.Fatalf("Part B expected TotalDelivered=1, got %+v", stoppedB)
				}
				for _, ma := range mock.getModAckCalls() {
					if ma.Req.AckDeadlineSeconds == 0 {
						t.Fatalf("Part B expected 0 wire NACKs when all queued entries expired, got %+v", ma.Req)
					}
				}
				drainedB, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				if err != nil {
					t.Fatalf("Part B DrainTraces: %v", err)
				}
				assertPreDeliveryExpiryChildren(drainedB.Spans, subNCR, 3, 1)
			})
		}
	})

	// 2. Graceful Stop(WAIT_FOR_PROCESSING) MUST NOT cancel in-flight periodic ModifyAckDeadline RPCs (Go ALO/EOD & C++).
	t.Run("GracefulStopWaitForProcessingInFlightPeriodicModack", func(t *testing.T) {
		cases := []struct {
			name    string
			mode    string
			eod     bool
			cppMode bool
		}{
			{name: "GoALO", mode: "--mode=conformant-go", eod: false, cppMode: false},
			{name: "GoEOD", mode: "--mode=conformant-go", eod: true, cppMode: false},
			{name: "CppEOD", mode: "--mode=conformant-cpp-partial", eod: true, cppMode: true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				h := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, tc.mode, "--fast-timings")
				defer func() {
					if err := h.Shutdown(t, 4*time.Second); err != nil {
						t.Errorf("-race proxy shutdown error (stderr=%q): %v", h.Stderr(), err)
					}
					if strings.Contains(h.Stderr(), "DATA RACE") {
						t.Errorf("DATA RACE detected in stderr:\n%s", h.Stderr())
					}
				}()
				_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

				mock.resetCounters()
				var delivered atomic.Bool
				inPeriodic := make(chan struct{}, 1)
				releasePeriodic := make(chan struct{})
				var releaseOnce sync.Once
				closeRelease := func() { releaseOnce.Do(func() { close(releasePeriodic) }) }
				defer closeRelease()
				var periodicOnce sync.Once
				var periodicCtxErr atomic.Value

				mock.mu.Lock()
				mock.onAck = nil
				mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
					if req.AckDeadlineSeconds > 0 && delivered.Load() {
						shouldBlock := false
						periodicOnce.Do(func() { shouldBlock = true })
						if shouldBlock {
							select {
							case inPeriodic <- struct{}{}:
							default:
							}
							select {
							case <-releasePeriodic:
							case <-actx.Done():
								periodicCtxErr.Store(actx.Err())
								return status.FromContextError(actx.Err()).Err()
							}
							if err := actx.Err(); err != nil {
								periodicCtxErr.Store(err)
								return status.FromContextError(err).Err()
							}
						}
					}
					return nil
				}
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
					if idx == 1 {
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{
								ExactlyOnceDeliveryEnabled: tc.eod,
							},
							ReceivedMessages: []*pubsubpb.ReceivedMessage{
								{AckId: "r4-gmod-" + tc.name, Message: &pubsubpb.PubsubMessage{MessageId: "m-gmod-" + tc.name, Data: []byte("g")}},
							},
						})
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()

				subID := "sub-r4-gmod-" + strings.ToLower(tc.name)
				sess, err := h.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("SubscriberSession: %v", err)
				}
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:                     mock.addr,
							Subscription:                 "projects/p/subscriptions/" + subID,
							EnableMonitoring:             true,
							MaxExtensionMs:               protoInt64(5000),
							MaxDurationPerAckExtensionMs: protoInt64(120),
							AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
						},
					},
				}); err != nil {
					t.Fatalf("Start: %v", err)
				}

				evCh := make(chan *proxypb.SubscriberEvent, 16)
				recvErr := make(chan error, 1)
				go func() {
					for {
						ev, rerr := sess.Recv()
						if rerr != nil {
							recvErr <- rerr
							return
						}
						evCh <- ev
					}
				}()

				var tok string
				for tok == "" {
					select {
					case ev := <-evCh:
						if md := ev.GetMessageDelivered(); md != nil {
							tok = md.DeliveryToken
						}
					case rerr := <-recvErr:
						t.Fatalf("unexpected recv before delivery: %v", rerr)
					case <-time.After(5 * time.Second):
						t.Fatalf("timed out waiting for delivery")
					}
				}
				delivered.Store(true)

				select {
				case <-inPeriodic:
				case <-time.After(5 * time.Second):
					t.Fatalf("timed out waiting for periodic ModifyAckDeadline")
				}

				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Stop{
						Stop: &proxypb.StopSubscriberCommand{
							ShutdownBehavior:  proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING,
							ShutdownTimeoutMs: protoInt64(5000),
						},
					},
				}); err != nil {
					t.Fatalf("Stop: %v", err)
				}

				// Verify Stop(WAIT_FOR_PROCESSING) waits and does not cancel in-flight periodic modack.
				select {
				case ev := <-evCh:
					if st := ev.GetStopped(); st != nil {
						t.Fatalf("StoppedEvent arrived prematurely while periodic modack & callback active: %+v", st)
					}
				case rerr := <-recvErr:
					t.Fatalf("unexpected stream exit while waiting: %v", rerr)
				case <-time.After(60 * time.Millisecond):
				}

				closeRelease()
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Settle{
						Settle: &proxypb.SettleMessageCommand{
							DeliveryToken: tok,
							Action:        proxypb.SettleMessageCommand_SETTLE_ACTION_ACK,
						},
					},
				}); err != nil {
					t.Fatalf("Ack: %v", err)
				}
				_ = sess.CloseSend()

				var stopped *proxypb.StoppedEvent
				for done := false; !done; {
					select {
					case ev := <-evCh:
						if st := ev.GetStopped(); st != nil {
							stopped = st
						}
					case rerr := <-recvErr:
						if rerr != io.EOF {
							t.Fatalf("Recv post-stop: %v", rerr)
						}
						for len(evCh) > 0 {
							if st := (<-evCh).GetStopped(); st != nil {
								stopped = st
							}
						}
						done = true
					case <-time.After(5 * time.Second):
						t.Fatalf("timed out waiting for StoppedEvent")
					}
				}
				if stopped == nil || stopped.ErrorCode != 0 || stopped.TotalDelivered != 1 {
					t.Fatalf("unexpected StoppedEvent: %+v", stopped)
				}
				if perr := periodicCtxErr.Load(); perr != nil {
					t.Fatalf("periodic modack context unexpectedly canceled during Stop(WAIT_FOR_PROCESSING): %v", perr)
				}

				drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				if err != nil {
					t.Fatalf("DrainTraces: %v", err)
				}
				modSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, subID, "modack", tc.cppMode)
				var periodicCount int
				for _, ms := range modSpans {
					if ms.EndTime == nil {
						t.Fatalf("modack span %s missing EndTime", ms.SpanId)
					}
					if ms.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
						t.Fatalf("modack span %s expected UNSET status during graceful stop, got %v (msg=%q)", ms.SpanId, ms.StatusCode, ms.StatusMessage)
					}
					if tc.cppMode || !ms.Attributes["messaging.gcp_pubsub.is_receipt_modack"].GetBoolValue() {
						periodicCount++
					}
				}
				if periodicCount == 0 {
					t.Fatalf("expected >= 1 periodic modack span, got modSpans=%+v", modSpans)
				}
				ackedCnt, _, _ := assertIter3TraceInvariants(t, drained.Spans, subID, 1, tc.cppMode, true, false)
				if ackedCnt != 1 {
					t.Fatalf("expected 1 acked subscribe span, got %d", ackedCnt)
				}
			})
		}
	})

	// 3. RETURN_WITHOUT_SETTLE + ACK_WITH_RESULT vs Stop(WAIT_FOR_PROCESSING) shutdown race (200x under -race).
	t.Run("ReturnWithoutSettleStopWaitForProcessingAckWithResultShutdownRace200x", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		h := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-go", "--fast-timings")
		defer func() {
			if err := h.Shutdown(t, 4*time.Second); err != nil {
				t.Errorf("-race proxy shutdown error (stderr=%q): %v", h.Stderr(), err)
			}
			if strings.Contains(h.Stderr(), "DATA RACE") {
				t.Errorf("DATA RACE detected in stderr:\n%s", h.Stderr())
			}
		}()
		_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})

		const totalIters = 200
		for iter := 0; iter < totalIters; iter++ {
			mock.resetCounters()
			twoMsgs := iter%2 == 0
			subID := fmt.Sprintf("sub-r4-rws-%03d", iter)
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = nil
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
				if idx == 1 {
					rms := []*pubsubpb.ReceivedMessage{
						{AckId: fmt.Sprintf("rws-a2-%d", iter), Message: &pubsubpb.PubsubMessage{MessageId: "rws-m2", Data: []byte("2")}},
					}
					if twoMsgs {
						rms = []*pubsubpb.ReceivedMessage{
							{AckId: fmt.Sprintf("rws-a1-%d", iter), Message: &pubsubpb.PubsubMessage{MessageId: "rws-m1", Data: []byte("1")}},
							{AckId: fmt.Sprintf("rws-a2-%d", iter), Message: &pubsubpb.PubsubMessage{MessageId: "rws-m2", Data: []byte("2")}},
						}
					}
					_ = stream.Send(&pubsubpb.StreamingPullResponse{ReceivedMessages: rms})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("iter %d SubscriberSession: %v", iter, err)
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:         mock.addr,
						Subscription:     "projects/p/subscriptions/" + subID,
						EnableMonitoring: true,
						AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				t.Fatalf("iter %d Start: %v", iter, err)
			}

			wantDelivered := 1
			if twoMsgs {
				wantDelivered = 2
			}
			tokens := make(map[string]string, wantDelivered)
			for len(tokens) < wantDelivered {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("iter %d Recv delivery: %v", iter, rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					tokens[md.MessageId] = md.DeliveryToken
				}
			}
			tok2 := tokens["rws-m2"]
			if tok2 == "" {
				t.Fatalf("iter %d missing tok2: %+v", iter, tokens)
			}

			if twoMsgs {
				// Window A / interleaved detached settle while Stop(WAIT) waits on m1's callback:
				// RWS(m2) -> Stop(WAIT) -> Settle(m2, ACK_WITH_RESULT) -> Settle(m1, ACK)
				for _, cmd := range []*proxypb.SubscriberCommand{
					{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok2, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE}}},
					{Command: &proxypb.SubscriberCommand_Stop{Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING}}},
					{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok2, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT}}},
					{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: tokens["rws-m1"], Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK}}},
				} {
					if serr := sess.Send(cmd); serr != nil {
						t.Fatalf("iter %d Send: %v", iter, serr)
					}
				}
			} else {
				// Window B (dispatchDelivery exit defer handoff race):
				// RWS(m2) -> Settle(m2, ACK_WITH_RESULT) -> Stop(WAIT) back-to-back with 0ms delay.
				for _, cmd := range []*proxypb.SubscriberCommand{
					{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok2, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_RETURN_WITHOUT_SETTLE}}},
					{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: tok2, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT}}},
					{Command: &proxypb.SubscriberCommand_Stop{Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING}}},
				} {
					if serr := sess.Send(cmd); serr != nil {
						t.Fatalf("iter %d Send: %v", iter, serr)
					}
				}
			}

			var sr2 *proxypb.SettlementResultEvent
			var stopped *proxypb.StoppedEvent
			for sr2 == nil || stopped == nil {
				ev, rerr := sess.Recv()
				if rerr != nil {
					t.Fatalf("iter %d Recv post-stop (sr2=%v stopped=%v): %v", iter, sr2 != nil, stopped != nil, rerr)
				}
				if sr := ev.GetSettlementResult(); sr != nil && sr.DeliveryToken == tok2 {
					sr2 = sr
				}
				if st := ev.GetStopped(); st != nil {
					stopped = st
				}
			}
			_ = sess.CloseSend()
			for {
				if _, rerr := sess.Recv(); rerr != nil {
					break
				}
			}
			if sr2.Status != proxypb.SettlementResultEvent_ACK_STATUS_SUCCESS || sr2.GrpcErrorCode != 0 {
				t.Fatalf("iter %d expected ACK_STATUS_SUCCESS for tok2, got %+v", iter, sr2)
			}
			if stopped.ErrorCode != 0 || int(stopped.TotalDelivered) != wantDelivered {
				t.Fatalf("iter %d unexpected StoppedEvent: %+v (want TotalDelivered=%d)", iter, stopped, wantDelivered)
			}

			var wireAckIDs int
			for _, ac := range mock.getAckCalls() {
				wireAckIDs += len(ac.Req.AckIds)
			}
			var wireNackIDs int
			for _, ma := range mock.getModAckCalls() {
				if ma.Req.AckDeadlineSeconds == 0 {
					wireNackIDs += len(ma.Req.AckIds)
				}
			}
			if wireAckIDs != wantDelivered || wireNackIDs != 0 {
				t.Fatalf("iter %d expected %d wire ACKs and 0 NACKs, got ack=%d nack=%d", iter, wantDelivered, wireAckIDs, wireNackIDs)
			}

			drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("iter %d DrainTraces: %v", iter, err)
			}
			ackedSpans, nackedSpans, expSpans := assertIter3TraceInvariants(t, drained.Spans, subID, wantDelivered, false, true, false)
			if ackedSpans != wantDelivered || nackedSpans != 0 || expSpans != 0 {
				t.Fatalf("iter %d unexpected trace counts: acked=%d nacked=%d expired=%d", iter, ackedSpans, nackedSpans, expSpans)
			}
		}
	})

	// 4. 999 sampled -01 messages periodic modack batch_count MUST equal len(Links) & "<op> start" batch_count near MaxExtensionMs (Go & C++).
	t.Run("Batch999ModackBatchCountEqualsLinksNearMaxExtensionMs", func(t *testing.T) {
		profiles := []struct {
			name     string
			modeFlag string
			cppMode  bool
		}{
			{name: "Go", modeFlag: "--mode=conformant-go", cppMode: false},
			{name: "Cpp", modeFlag: "--mode=conformant-cpp-partial", cppMode: true},
		}
		for _, prof := range profiles {
			t.Run(prof.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				h := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, prof.modeFlag, "--fast-timings")
				defer func() {
					if err := h.Shutdown(t, 4*time.Second); err != nil {
						t.Errorf("-race proxy shutdown error (stderr=%q): %v", h.Stderr(), err)
					}
					if strings.Contains(h.Stderr(), "DATA RACE") {
						t.Errorf("DATA RACE detected in stderr:\n%s", h.Stderr())
					}
				}()

				const batchSize = 999
				var sawPeriodicModack bool
				for attempt := 0; attempt < 3 && !sawPeriodicModack; attempt++ {
					_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
					mock.resetCounters()
					mock.mu.Lock()
					mock.onAck = nil
					mock.onModAck = nil
					mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
						if idx == 1 {
							rms := make([]*pubsubpb.ReceivedMessage, batchSize)
							for i := 0; i < batchSize; i++ {
								tp := fmt.Sprintf("00-%032x-%016x-01", uint64(attempt*10000+i+1), uint64(attempt*10000+i+1))
								rms[i] = &pubsubpb.ReceivedMessage{
									AckId: fmt.Sprintf("b999-%d-a-%03d", attempt, i),
									Message: &pubsubpb.PubsubMessage{
										MessageId:  fmt.Sprintf("b999-%d-m-%03d", attempt, i),
										Data:       []byte("x"),
										Attributes: map[string]string{"googclient_traceparent": tp},
									},
								}
							}
							_ = stream.Send(&pubsubpb.StreamingPullResponse{ReceivedMessages: rms})
						}
						<-stream.Context().Done()
						return nil
					}
					mock.mu.Unlock()

					subID := fmt.Sprintf("sub-r4-999-%s-%d", strings.ToLower(prof.name), attempt)
					sess, err := h.Client.SubscriberSession(ctx)
					if err != nil {
						t.Fatalf("attempt %d SubscriberSession: %v", attempt, err)
					}
					if err := sess.Send(&proxypb.SubscriberCommand{
						Command: &proxypb.SubscriberCommand_Start{
							Start: &proxypb.StartSubscriberCommand{
								Endpoint:                     mock.addr,
								Subscription:                 "projects/p/subscriptions/" + subID,
								EnableMonitoring:             true,
								MaxExtensionMs:               protoInt64(110),
								MaxDurationPerAckExtensionMs: protoInt64(60),
								AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
							},
						},
					}); err != nil {
						t.Fatalf("attempt %d Start: %v", attempt, err)
					}

					var stopped *proxypb.StoppedEvent
					recvDone := make(chan error, 1)
					var delivered atomic.Int64
					go func() {
						for {
							ev, rerr := sess.Recv()
							if rerr != nil {
								recvDone <- rerr
								return
							}
							if ev.GetMessageDelivered() != nil {
								delivered.Add(1)
							}
							if st := ev.GetStopped(); st != nil {
								stopped = st
							}
						}
					}()

					// Wait long enough for periodic modack sweeps to collide with expiryTick past MaxExtensionMs=110ms.
					time.Sleep(165 * time.Millisecond)
					if err := sess.Send(&proxypb.SubscriberCommand{
						Command: &proxypb.SubscriberCommand_Stop{
							Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
						},
					}); err != nil {
						t.Fatalf("attempt %d Stop: %v", attempt, err)
					}
					_ = sess.CloseSend()
					if rerr := <-recvDone; rerr != io.EOF {
						t.Fatalf("attempt %d Recv: %v", attempt, rerr)
					}
					if stopped == nil {
						t.Fatalf("attempt %d missing StoppedEvent (delivered=%d)", attempt, delivered.Load())
					}

					drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
					if err != nil {
						t.Fatalf("attempt %d DrainTraces: %v", attempt, err)
					}
					modSpans := assertSettlementBatchCountAndLinks(t, drained.Spans, subID, "modack", prof.cppMode)
					subByKey := make(map[spanKey]*proxypb.TraceSpan, batchSize)
					for _, sp := range drained.Spans {
						if sp.EndTime == nil {
							t.Fatalf("attempt %d unclosed span %q (%s)", attempt, sp.Name, sp.SpanId)
						}
						if sp.Name == subID+" subscribe" {
							subByKey[spanKey{traceID: sp.TraceId, spanID: sp.SpanId}] = sp
						}
					}
					for _, ms := range modSpans {
						sawPeriodicModack = true
						bc := ms.Attributes["messaging.batch.message_count"].GetInt64Value()
						if bc != int64(len(ms.Links)) {
							t.Fatalf("attempt %d %s modack span %s batch_count=%d != len(Links)=%d", attempt, prof.name, ms.SpanId, bc, len(ms.Links))
						}
						if !prof.cppMode {
							for _, l := range ms.Links {
								subSp := subByKey[spanKey{traceID: l.TraceId, spanID: l.SpanId}]
								var matchedStart bool
								for _, ev := range subSp.Events {
									if ev.Name == "modack start" && ev.Attributes["messaging.batch.message_count"].GetInt64Value() == bc {
										matchedStart = true
									}
								}
								if !matchedStart {
									t.Fatalf("attempt %d Go subscribe span %s missing \"modack start\" with batch_count=%d (links=%d)", attempt, subSp.SpanId, bc, len(ms.Links))
								}
							}
						}
					}
				}
				if !sawPeriodicModack {
					t.Fatalf("%s 999-msg batch did not emit any periodic modack span across 3 attempts", prof.name)
				}
			})
		}
	})

	// 5. EOD transient retry single-span + <op> end invariants (Go & C++).
	t.Run("EODTransientRetrySingleSpanAndOpEndInvariants", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Helper for driving an interactive session, optionally gating settlement to force batch coalescing.
		driveBatch := func(
			h *DirectProxyHandle,
			subID string,
			msgs []*pubsubpb.ReceivedMessage,
			maxDurExtMs *int64,
			actionForAckID func(ackID string) proxypb.SettleMessageCommand_SettleAction,
			gateAckID string,
		) ([]*proxypb.TraceSpan, error) {
			_, _ = h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			sess, err := h.Client.SubscriberSession(ctx)
			if err != nil {
				return nil, err
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:                     mock.addr,
						Subscription:                 "projects/p/subscriptions/" + subID,
						EnableMonitoring:             true,
						MaxExtensionMs:               protoInt64(10000),
						MaxDurationPerAckExtensionMs: maxDurExtMs,
						AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			}); err != nil {
				return nil, err
			}
			tokByAckID := make(map[string]string, len(msgs))
			wantResults := 0
			for len(tokByAckID) < len(msgs) {
				ev, rerr := sess.Recv()
				if rerr != nil {
					return nil, rerr
				}
				if md := ev.GetMessageDelivered(); md != nil {
					var matchedAckID string
					for _, rm := range msgs {
						if rm.Message.MessageId == md.MessageId {
							matchedAckID = rm.AckId
							break
						}
					}
					tokByAckID[matchedAckID] = md.DeliveryToken
				}
			}
			for _, rm := range msgs {
				act := actionForAckID(rm.AckId)
				if act == proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT || act == proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT {
					wantResults++
				}
				if rm.AckId == gateAckID {
					continue
				}
				if serr := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Settle{
						Settle: &proxypb.SettleMessageCommand{DeliveryToken: tokByAckID[rm.AckId], Action: act},
					},
				}); serr != nil {
					return nil, serr
				}
			}
			var resultsGot int
			for resultsGot < wantResults {
				ev, rerr := sess.Recv()
				if rerr != nil {
					return nil, rerr
				}
				if ev.GetSettlementResult() != nil {
					resultsGot++
				}
			}
			if err := sess.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			}); err != nil {
				return nil, err
			}
			_ = sess.CloseSend()
			for {
				if _, rerr := sess.Recv(); rerr != nil {
					break
				}
			}
			drained, err := h.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				return nil, err
			}
			return drained.Spans, nil
		}

		// --- Part A: Go EOD ---
		t.Run("Go", func(t *testing.T) {
			hGo := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-go", "--fast-timings")
			defer func() {
				if err := hGo.Shutdown(t, 4*time.Second); err != nil {
					t.Errorf("-race Go proxy shutdown error (stderr=%q): %v", hGo.Stderr(), err)
				}
				if strings.Contains(hGo.Stderr(), "DATA RACE") {
					t.Errorf("DATA RACE in Go proxy:\n%s", hGo.Stderr())
				}
			}()

			// A1: Receipt modack transient -> ok
			mock.resetCounters()
			var recTransOnce atomic.Bool
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
				if req.AckDeadlineSeconds >= 60 && len(req.AckIds) == 1 && req.AckIds[0] == "go-rec-trans" && recTransOnce.CompareAndSwap(false, true) {
					st, _ := status.New(codes.Unavailable, "transient receipt").WithDetails(&errdetails.ErrorInfo{
						Metadata: map[string]string{"go-rec-trans": "TRANSIENT_FAILURE_UNAVAILABLE"},
					})
					return st.Err()
				}
				return nil
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
				if idx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: true},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: "go-rec-trans", Message: &pubsubpb.PubsubMessage{MessageId: "m-go-rec-t", Data: []byte("r")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()
			spansRec, err := driveBatch(hGo, "sub-r4-go-rec", []*pubsubpb.ReceivedMessage{
				{AckId: "go-rec-trans", Message: &pubsubpb.PubsubMessage{MessageId: "m-go-rec-t"}},
			}, nil, func(string) proxypb.SettleMessageCommand_SettleAction {
				return proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT
			}, "")
			if err != nil {
				t.Fatalf("Go receipt driveBatch: %v", err)
			}
			var recRPCs int
			for _, ma := range mock.getModAckCalls() {
				if ma.Req.AckDeadlineSeconds >= 60 {
					recRPCs++
				}
			}
			if recRPCs < 2 {
				t.Fatalf("Go receipt modack expected >= 2 wire RPCs, got %d", recRPCs)
			}
			modSpansRec := assertSettlementBatchCountAndLinks(t, spansRec, "sub-r4-go-rec", "modack", false)
			var receiptSpanCnt int
			for _, sp := range modSpansRec {
				if sp.Attributes["messaging.gcp_pubsub.is_receipt_modack"].GetBoolValue() {
					receiptSpanCnt++
					if sp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR || countEvent(sp, "exception") != 1 || len(sp.Links) != 1 || sp.Attributes["messaging.batch.message_count"].GetInt64Value() != 1 {
						t.Fatalf("unexpected Go receipt modack span: %+v", sp)
					}
				}
			}
			if receiptSpanCnt != 1 {
				t.Fatalf("expected 1 receipt modack span, got %d", receiptSpanCnt)
			}
			subRec := findSubSpanByAckID(spansRec, "sub-r4-go-rec", "go-rec-trans")
			if countEvent(subRec, "modack start") != 1 || countEvent(subRec, "modack end") != 2 {
				t.Fatalf("Go retried receipt modack expected 1 modack start and 2 modack end, got %+v", subRec.Events)
			}

			// A2: Coalesced mixed ack [ok, trans->ok, perm] AND all-transient ack [t1->ok, t2->ok]
			runGoCoalescedAck := func(subID string, targetAckIDs []string, onAckFn func(context.Context, *pubsubpb.AcknowledgeRequest, int) error) []*proxypb.TraceSpan {
				t.Helper()
				mock.resetCounters()
				inGate := make(chan struct{})
				releaseGate := make(chan struct{})
				var gateOnce sync.Once
				sendBatch := make(chan struct{})
				mock.mu.Lock()
				mock.onModAck = nil
				mock.onAck = func(actx context.Context, req *pubsubpb.AcknowledgeRequest, attempt int) error {
					if len(req.AckIds) == 1 && req.AckIds[0] == "gate-a" {
						gateOnce.Do(func() { close(inGate) })
						<-releaseGate
						return nil
					}
					return onAckFn(actx, req, attempt)
				}
				mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
					if idx == 1 {
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: true},
							ReceivedMessages: []*pubsubpb.ReceivedMessage{
								{AckId: "gate-a", Message: &pubsubpb.PubsubMessage{MessageId: "m-gate-a", Data: []byte("g")}},
							},
						})
						<-sendBatch
						rms := make([]*pubsubpb.ReceivedMessage, len(targetAckIDs))
						for i, aid := range targetAckIDs {
							rms[i] = &pubsubpb.ReceivedMessage{AckId: aid, Message: &pubsubpb.PubsubMessage{MessageId: "m-" + aid, Data: []byte("d")}}
						}
						_ = stream.Send(&pubsubpb.StreamingPullResponse{
							SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: true},
							ReceivedMessages:       rms,
						})
					}
					<-stream.Context().Done()
					return nil
				}
				mock.mu.Unlock()

				_, _ = hGo.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				sess, err := hGo.Client.SubscriberSession(ctx)
				if err != nil {
					t.Fatalf("%s SubscriberSession: %v", subID, err)
				}
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Start{
						Start: &proxypb.StartSubscriberCommand{
							Endpoint:         mock.addr,
							Subscription:     "projects/p/subscriptions/" + subID,
							EnableMonitoring: true,
							AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
						},
					},
				}); err != nil {
					t.Fatalf("%s Start: %v", subID, err)
				}
				var gateTok string
				for gateTok == "" {
					ev, rerr := sess.Recv()
					if rerr != nil {
						t.Fatalf("%s gate recv: %v", subID, rerr)
					}
					if md := ev.GetMessageDelivered(); md != nil {
						gateTok = md.DeliveryToken
					}
				}
				close(sendBatch)
				toks := make(map[string]string, len(targetAckIDs))
				for len(toks) < len(targetAckIDs) {
					ev, rerr := sess.Recv()
					if rerr != nil {
						t.Fatalf("%s target recv: %v", subID, rerr)
					}
					if md := ev.GetMessageDelivered(); md != nil {
						toks[strings.TrimPrefix(md.MessageId, "m-")] = md.DeliveryToken
					}
				}
				if err := sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Settle{
						Settle: &proxypb.SettleMessageCommand{DeliveryToken: gateTok, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT},
					},
				}); err != nil {
					t.Fatalf("%s settle gate: %v", subID, err)
				}
				<-inGate
				for _, aid := range targetAckIDs {
					if err := sess.Send(&proxypb.SubscriberCommand{
						Command: &proxypb.SubscriberCommand_Settle{
							Settle: &proxypb.SettleMessageCommand{DeliveryToken: toks[aid], Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT},
						},
					}); err != nil {
						t.Fatalf("%s settle %s: %v", subID, aid, err)
					}
				}
				time.Sleep(40 * time.Millisecond)
				close(releaseGate)
				results := 0
				for results < 1+len(targetAckIDs) {
					ev, rerr := sess.Recv()
					if rerr != nil {
						t.Fatalf("%s settle recv: %v", subID, rerr)
					}
					if ev.GetSettlementResult() != nil {
						results++
					}
				}
				_ = sess.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Stop{
						Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
					},
				})
				_ = sess.CloseSend()
				for {
					if _, rerr := sess.Recv(); rerr != nil {
						break
					}
				}
				drained, err := hGo.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
				if err != nil {
					t.Fatalf("%s DrainTraces: %v", subID, err)
				}
				return drained.Spans
			}

			var mixedOnce atomic.Bool
			spansMix := runGoCoalescedAck("sub-r4-go-mix", []string{"mix-ok", "mix-trans", "mix-perm"}, func(_ context.Context, req *pubsubpb.AcknowledgeRequest, _ int) error {
				if len(req.AckIds) == 3 && mixedOnce.CompareAndSwap(false, true) {
					st, _ := status.New(codes.InvalidArgument, "mixed ack").WithDetails(&errdetails.ErrorInfo{
						Metadata: map[string]string{
							"mix-perm":  "PERMANENT_FAILURE_INVALID_ACK_ID",
							"mix-trans": "TRANSIENT_FAILURE_UNORDERED_ACK_ID",
						},
					})
					return st.Err()
				}
				return nil
			})
			var nonGateMixAckCalls int
			for _, ac := range mock.getAckCalls() {
				if !(len(ac.Req.AckIds) == 1 && ac.Req.AckIds[0] == "gate-a") {
					nonGateMixAckCalls++
				}
			}
			if nonGateMixAckCalls < 2 {
				t.Fatalf("Go mixed ack expected >= 2 non-gate RPC calls, got %d", nonGateMixAckCalls)
			}
			mixAckSpans := assertSettlementBatchCountAndLinks(t, spansMix, "sub-r4-go-mix", "ack", false)
			var mixBatch3Spans int
			for _, sp := range mixAckSpans {
				if len(sp.Links) == 3 {
					mixBatch3Spans++
					if sp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR || countEvent(sp, "exception") != 1 || sp.Attributes["messaging.batch.message_count"].GetInt64Value() != 3 {
						t.Fatalf("unexpected mixed batch-3 ack span: %+v", sp)
					}
				}
			}
			if mixBatch3Spans != 1 {
				t.Fatalf("expected 1 batch-3 ack span in Go mixed EOD retry, got %d", mixBatch3Spans)
			}
			subMixOk := findSubSpanByAckID(spansMix, "sub-r4-go-mix", "mix-ok")
			subMixTrans := findSubSpanByAckID(spansMix, "sub-r4-go-mix", "mix-trans")
			subMixPerm := findSubSpanByAckID(spansMix, "sub-r4-go-mix", "mix-perm")
			if countEvent(subMixOk, "ack start") != 1 || countEvent(subMixOk, "ack end") != 1 || subMixOk.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "acked" {
				t.Fatalf("unexpected mix-ok subscribe span: %+v", subMixOk)
			}
			if countEvent(subMixTrans, "ack start") != 1 || countEvent(subMixTrans, "ack end") != 2 || subMixTrans.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "acked" {
				t.Fatalf("unexpected mix-trans subscribe span: %+v", subMixTrans)
			}
			if countEvent(subMixPerm, "ack start") != 1 || countEvent(subMixPerm, "ack end") != 1 || subMixPerm.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
				t.Fatalf("unexpected mix-perm subscribe span: %+v", subMixPerm)
			}
			if _, hasRes := subMixPerm.Attributes["messaging.gcp_pubsub.result"]; hasRes {
				t.Fatalf("mix-perm must omit messaging.gcp_pubsub.result on Go EOD failure: %+v", subMixPerm.Attributes)
			}

			// A3: All-transient [t1->ok, t2->ok]
			var allTransOnce atomic.Bool
			spansAllT := runGoCoalescedAck("sub-r4-go-allt", []string{"at-1", "at-2"}, func(_ context.Context, req *pubsubpb.AcknowledgeRequest, _ int) error {
				if len(req.AckIds) == 2 && allTransOnce.CompareAndSwap(false, true) {
					return status.Error(codes.Unavailable, "transient batch")
				}
				return nil
			})
			allTAckSpans := assertSettlementBatchCountAndLinks(t, spansAllT, "sub-r4-go-allt", "ack", false)
			var allTBatch2 int
			for _, sp := range allTAckSpans {
				if len(sp.Links) == 2 {
					allTBatch2++
					if sp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR || countEvent(sp, "exception") != 1 {
						t.Fatalf("unexpected all-transient ack span: %+v", sp)
					}
				}
			}
			if allTBatch2 != 1 {
				t.Fatalf("expected 1 batch-2 ack span for all-transient Go EOD, got %d", allTBatch2)
			}
			for _, aid := range []string{"at-1", "at-2"} {
				sp := findSubSpanByAckID(spansAllT, "sub-r4-go-allt", aid)
				if countEvent(sp, "ack start") != 1 || countEvent(sp, "ack end") != 2 || sp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET || sp.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "acked" {
					t.Fatalf("all-transient %s subscribe span mismatch: %+v", aid, sp)
				}
			}

			// A4: Coalesced mixed nack [nok, ntrans->ok]
			mock.resetCounters()
			inGateN := make(chan struct{})
			releaseGateN := make(chan struct{})
			var gateNOnce sync.Once
			var nackTransOnce atomic.Bool
			sendNackBatch := make(chan struct{})
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = func(actx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, attempt int) error {
				if req.AckDeadlineSeconds == 0 {
					if len(req.AckIds) == 1 && req.AckIds[0] == "gate-n" {
						gateNOnce.Do(func() { close(inGateN) })
						<-releaseGateN
						return nil
					}
					if len(req.AckIds) == 2 && nackTransOnce.CompareAndSwap(false, true) {
						st, _ := status.New(codes.Unavailable, "mixed nack").WithDetails(&errdetails.ErrorInfo{
							Metadata: map[string]string{"nack-trans": "TRANSIENT_FAILURE_UNAVAILABLE"},
						})
						return st.Err()
					}
				}
				return nil
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
				if idx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: true},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: "gate-n", Message: &pubsubpb.PubsubMessage{MessageId: "m-gate-n", Data: []byte("g")}},
						},
					})
					<-sendNackBatch
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: true},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: "nack-ok", Message: &pubsubpb.PubsubMessage{MessageId: "m-nack-ok", Data: []byte("1")}},
							{AckId: "nack-trans", Message: &pubsubpb.PubsubMessage{MessageId: "m-nack-trans", Data: []byte("2")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()
			_, _ = hGo.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			sessN, err := hGo.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("Go nack SubscriberSession: %v", err)
			}
			_ = sessN.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:         mock.addr,
						Subscription:     "projects/p/subscriptions/sub-r4-go-nack",
						EnableMonitoring: true,
						AutoSettleMode:   proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			})
			var gateNTok string
			for gateNTok == "" {
				ev, rerr := sessN.Recv()
				if rerr != nil {
					t.Fatalf("gateN recv: %v", rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					gateNTok = md.DeliveryToken
				}
			}
			close(sendNackBatch)
			ntoks := make(map[string]string, 2)
			for len(ntoks) < 2 {
				ev, rerr := sessN.Recv()
				if rerr != nil {
					t.Fatalf("nack target recv: %v", rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					ntoks[md.MessageId] = md.DeliveryToken
				}
			}
			_ = sessN.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{DeliveryToken: gateNTok, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT},
				},
			})
			<-inGateN
			for _, mid := range []string{"m-nack-ok", "m-nack-trans"} {
				_ = sessN.Send(&proxypb.SubscriberCommand{
					Command: &proxypb.SubscriberCommand_Settle{
						Settle: &proxypb.SettleMessageCommand{DeliveryToken: ntoks[mid], Action: proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT},
					},
				})
			}
			time.Sleep(40 * time.Millisecond)
			close(releaseGateN)
			for nRes := 0; nRes < 3; {
				ev, rerr := sessN.Recv()
				if rerr != nil {
					t.Fatalf("nack settle recv: %v", rerr)
				}
				if ev.GetSettlementResult() != nil {
					nRes++
				}
			}
			_ = sessN.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			})
			_ = sessN.CloseSend()
			for {
				if _, rerr := sessN.Recv(); rerr != nil {
					break
				}
			}
			spansNack, err := hGo.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("Go nack DrainTraces: %v", err)
			}
			nackSpans := assertSettlementBatchCountAndLinks(t, spansNack.Spans, "sub-r4-go-nack", "nack", false)
			var nackBatch2 int
			for _, sp := range nackSpans {
				if len(sp.Links) == 2 {
					nackBatch2++
					if sp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR || countEvent(sp, "exception") != 1 {
						t.Fatalf("unexpected Go mixed nack span: %+v", sp)
					}
				}
			}
			if nackBatch2 != 1 {
				t.Fatalf("expected 1 batch-2 nack span for Go EOD retry, got %d", nackBatch2)
			}
			subNOk := findSubSpanByAckID(spansNack.Spans, "sub-r4-go-nack", "nack-ok")
			subNTrans := findSubSpanByAckID(spansNack.Spans, "sub-r4-go-nack", "nack-trans")
			if countEvent(subNOk, "nack start") != 1 || countEvent(subNOk, "nack end") != 1 || subNOk.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "nacked" {
				t.Fatalf("unexpected nack-ok subscribe span: %+v", subNOk)
			}
			if countEvent(subNTrans, "nack start") != 1 || countEvent(subNTrans, "nack end") != 2 || subNTrans.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != "nacked" {
				t.Fatalf("unexpected nack-trans subscribe span: %+v", subNTrans)
			}
		})

		// --- Part B: C++ EOD ---
		t.Run("Cpp", func(t *testing.T) {
			hCpp := startCustomProxyDirect(t, raceBin, []string{"GORACE=halt_on_error=1"}, "--mode=conformant-cpp-partial", "--fast-timings")
			defer func() {
				if err := hCpp.Shutdown(t, 4*time.Second); err != nil {
					t.Errorf("-race C++ proxy shutdown error (stderr=%q): %v", hCpp.Stderr(), err)
				}
				if strings.Contains(hCpp.Stderr(), "DATA RACE") {
					t.Errorf("DATA RACE in C++ proxy:\n%s", hCpp.Stderr())
				}
			}()

			checkCppSettle := func(spans []*proxypb.TraceSpan, subID, ackID, op string, wantOK bool) {
				t.Helper()
				sub := findSubSpanByAckID(spans, subID, ackID)
				if sub == nil || sub.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET || sub.Attributes["messaging.gcp_pubsub.result"].GetStringValue() != op {
					t.Fatalf("C++ %s subscribe span mismatch: %+v", ackID, sub)
				}
				if countEvent(sub, "gl-cpp.message_"+op) != 1 || countEvent(sub, "gl-cpp."+op+"_start") != 1 || countEvent(sub, "gl-cpp."+op+"_end") != 1 {
					t.Fatalf("C++ %s subscribe events expected 1 message_%s, 1 %s_start, 1 %s_end, got %+v", ackID, op, op, op, sub.Events)
				}
				var rpcCnt int
				for _, sp := range spans {
					if sp.Name == subID+" "+op && sp.Attributes["messaging.gcp_pubsub.message.ack_id"].GetStringValue() == ackID {
						rpcCnt++
						if sp.Kind != proxypb.SpanKind_SPAN_KIND_INTERNAL {
							t.Fatalf("C++ %s span expected INTERNAL, got %v", ackID, sp.Kind)
						}
						if wantOK && sp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK {
							t.Fatalf("C++ %s span expected OK after retry recovery, got %v", ackID, sp.StatusCode)
						}
						if !wantOK && sp.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
							t.Fatalf("C++ %s span expected ERROR on permanent fail, got %v", ackID, sp.StatusCode)
						}
					}
				}
				if rpcCnt != 1 {
					t.Fatalf("C++ %s expected exactly 1 %s span across retries, got %d", ackID, op, rpcCnt)
				}
			}

			// B1a: C++ EOD transient ack & nack retry -> OK recovery (kept separate from perm-fail batch to avoid coalescing).
			mock.resetCounters()
			var cppAckTransOnce, cppNackTransOnce atomic.Bool
			mock.mu.Lock()
			mock.onAck = func(_ context.Context, req *pubsubpb.AcknowledgeRequest, _ int) error {
				for _, id := range req.AckIds {
					if id == "cpp-ack-trans" && cppAckTransOnce.CompareAndSwap(false, true) {
						return status.Error(codes.Unavailable, "cpp trans ack")
					}
				}
				return nil
			}
			mock.onModAck = func(_ context.Context, req *pubsubpb.ModifyAckDeadlineRequest, _ int) error {
				if req.AckDeadlineSeconds == 0 {
					for _, id := range req.AckIds {
						if id == "cpp-nack-trans" && cppNackTransOnce.CompareAndSwap(false, true) {
							return status.Error(codes.Unavailable, "cpp trans nack")
						}
					}
				}
				return nil
			}
			msgsB1a := []*pubsubpb.ReceivedMessage{
				{AckId: "cpp-ack-trans", Message: &pubsubpb.PubsubMessage{MessageId: "m-cat", Data: []byte("1")}},
				{AckId: "cpp-nack-trans", Message: &pubsubpb.PubsubMessage{MessageId: "m-cnt", Data: []byte("3")}},
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
				if idx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: true},
						ReceivedMessages:       msgsB1a,
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			spansB1a, err := driveBatch(hCpp, "sub-r4-cpp-trans", msgsB1a, nil, func(aid string) proxypb.SettleMessageCommand_SettleAction {
				if strings.Contains(aid, "nack") {
					return proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT
				}
				return proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT
			}, "")
			if err != nil {
				t.Fatalf("C++ B1a driveBatch: %v", err)
			}
			assertIter3TraceInvariants(t, spansB1a, "sub-r4-cpp-trans", 2, true, true, true)
			checkCppSettle(spansB1a, "sub-r4-cpp-trans", "cpp-ack-trans", "ack", true)
			checkCppSettle(spansB1a, "sub-r4-cpp-trans", "cpp-nack-trans", "nack", true)

			// B1b: C++ EOD permanent ack (PermissionDenied) & permanent nack (InvalidArgument + ErrorInfo) -> single ERROR span.
			mock.resetCounters()
			mock.mu.Lock()
			mock.onAck = func(_ context.Context, req *pubsubpb.AcknowledgeRequest, _ int) error {
				for _, id := range req.AckIds {
					if id == "cpp-ack-perm" {
						return status.Error(codes.PermissionDenied, "cpp perm ack")
					}
				}
				return nil
			}
			mock.onModAck = func(_ context.Context, req *pubsubpb.ModifyAckDeadlineRequest, _ int) error {
				if req.AckDeadlineSeconds == 0 {
					for _, id := range req.AckIds {
						if id == "cpp-nack-perm" {
							st, _ := status.New(codes.InvalidArgument, "cpp perm nack").WithDetails(&errdetails.ErrorInfo{
								Metadata: map[string]string{"cpp-nack-perm": "PERMANENT_FAILURE_INVALID_ACK_ID"},
							})
							return st.Err()
						}
					}
				}
				return nil
			}
			msgsB1b := []*pubsubpb.ReceivedMessage{
				{AckId: "cpp-ack-perm", Message: &pubsubpb.PubsubMessage{MessageId: "m-cap", Data: []byte("2")}},
				{AckId: "cpp-nack-perm", Message: &pubsubpb.PubsubMessage{MessageId: "m-cnp", Data: []byte("4")}},
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
				if idx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: true},
						ReceivedMessages:       msgsB1b,
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			spansB1b, err := driveBatch(hCpp, "sub-r4-cpp-perm", msgsB1b, nil, func(aid string) proxypb.SettleMessageCommand_SettleAction {
				if strings.Contains(aid, "nack") {
					return proxypb.SettleMessageCommand_SETTLE_ACTION_NACK_WITH_RESULT
				}
				return proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT
			}, "")
			if err != nil {
				t.Fatalf("C++ B1b driveBatch: %v", err)
			}
			assertIter3TraceInvariants(t, spansB1b, "sub-r4-cpp-perm", 2, true, true, true)
			checkCppSettle(spansB1b, "sub-r4-cpp-perm", "cpp-ack-perm", "ack", false)
			checkCppSettle(spansB1b, "sub-r4-cpp-perm", "cpp-nack-perm", "nack", false)

			// B2: C++ EOD periodic modack transient retry (N attempts => N CLIENT UNSET modack spans, 1 gl-cpp.modack_start, N gl-cpp.modack_end).
			mock.resetCounters()
			var cppModTransOnce atomic.Bool
			var cppModCalls atomic.Int32
			cppModDone := make(chan struct{})
			var cppModDoneOnce sync.Once
			mock.mu.Lock()
			mock.onAck = nil
			mock.onModAck = func(_ context.Context, req *pubsubpb.ModifyAckDeadlineRequest, _ int) error {
				if req.AckDeadlineSeconds > 0 {
					c := cppModCalls.Add(1)
					if c == 1 && cppModTransOnce.CompareAndSwap(false, true) {
						st, _ := status.New(codes.Unavailable, "cpp modack trans").WithDetails(&errdetails.ErrorInfo{
							Metadata: map[string]string{"cpp-mod-trans": "TRANSIENT_FAILURE_UNAVAILABLE"},
						})
						return st.Err()
					}
					cppModDoneOnce.Do(func() { close(cppModDone) })
				}
				return nil
			}
			mock.onStreamPull = func(stream pubsubpb.Subscriber_StreamingPullServer, idx int) error {
				if idx == 1 {
					_ = stream.Send(&pubsubpb.StreamingPullResponse{
						SubscriptionProperties: &pubsubpb.StreamingPullResponse_SubscriptionProperties{ExactlyOnceDeliveryEnabled: true},
						ReceivedMessages: []*pubsubpb.ReceivedMessage{
							{AckId: "cpp-mod-trans", Message: &pubsubpb.PubsubMessage{MessageId: "m-cmt", Data: []byte("m")}},
						},
					})
				}
				<-stream.Context().Done()
				return nil
			}
			mock.mu.Unlock()

			_, _ = hCpp.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			sessMod, err := hCpp.Client.SubscriberSession(ctx)
			if err != nil {
				t.Fatalf("C++ modack SubscriberSession: %v", err)
			}
			_ = sessMod.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Start{
					Start: &proxypb.StartSubscriberCommand{
						Endpoint:                     mock.addr,
						Subscription:                 "projects/p/subscriptions/sub-r4-cpp-mod",
						EnableMonitoring:             true,
						MaxExtensionMs:               protoInt64(5000),
						MaxDurationPerAckExtensionMs: protoInt64(120),
						AutoSettleMode:               proxypb.StartSubscriberCommand_AUTO_SETTLE_MODE_INTERACTIVE,
					},
				},
			})
			var mTok string
			for mTok == "" {
				ev, rerr := sessMod.Recv()
				if rerr != nil {
					t.Fatalf("C++ modack Recv: %v", rerr)
				}
				if md := ev.GetMessageDelivered(); md != nil {
					mTok = md.DeliveryToken
				}
			}
			select {
			case <-cppModDone:
			case <-time.After(5 * time.Second):
				t.Fatalf("timed out waiting for C++ periodic modack retry completion")
			}
			_ = sessMod.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Settle{
					Settle: &proxypb.SettleMessageCommand{DeliveryToken: mTok, Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK_WITH_RESULT},
				},
			})
			for {
				ev, rerr := sessMod.Recv()
				if rerr != nil {
					t.Fatalf("C++ modack ack recv: %v", rerr)
				}
				if ev.GetSettlementResult() != nil {
					break
				}
			}
			_ = sessMod.Send(&proxypb.SubscriberCommand{
				Command: &proxypb.SubscriberCommand_Stop{
					Stop: &proxypb.StopSubscriberCommand{ShutdownBehavior: proxypb.ShutdownBehavior_SHUTDOWN_BEHAVIOR_WAIT_FOR_PROCESSING},
				},
			})
			_ = sessMod.CloseSend()
			for {
				if _, rerr := sessMod.Recv(); rerr != nil {
					break
				}
			}
			drainedMod, err := hCpp.Client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				t.Fatalf("C++ modack DrainTraces: %v", err)
			}
			cppModSpans := assertSettlementBatchCountAndLinks(t, drainedMod.Spans, "sub-r4-cpp-mod", "modack", true)
			nModSpans := len(cppModSpans)
			if nModSpans < 2 {
				t.Fatalf("expected >= 2 C++ modack spans across transient retry, got %d", nModSpans)
			}
			for _, ms := range cppModSpans {
				if ms.Kind != proxypb.SpanKind_SPAN_KIND_CLIENT || ms.StatusCode != proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
					t.Fatalf("unexpected C++ modack span: %+v", ms)
				}
			}
			subCppMod := findSubSpanByAckID(drainedMod.Spans, "sub-r4-cpp-mod", "cpp-mod-trans")
			if gotS, gotE := countEvent(subCppMod, "gl-cpp.modack_start"), countEvent(subCppMod, "gl-cpp.modack_end"); gotS != 1 || gotE != nModSpans {
				t.Fatalf("C++ periodic modack expected 1 gl-cpp.modack_start and %d gl-cpp.modack_end, got start=%d end=%d (events=%+v)", nModSpans, gotS, gotE, subCppMod.Events)
			}
		})
	})
}

func TestInfra_WriteExecutableFileForkLockNoETXTBSY(t *testing.T) {
	t.Parallel()
	trueBin, err := exec.LookPath("true")
	if err != nil {
		t.Fatalf("LookPath(true): %v", err)
	}
	dir := t.TempDir()
	smokePath := filepath.Join(dir, "smoke.sh")
	writeExecutableFile(t, smokePath, []byte("#!/bin/sh\nexit 0\n"))
	info, err := os.Stat(smokePath)
	if err != nil {
		t.Fatalf("stat %q: %v", smokePath, err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("expected executable permissions on %q, got %v", smokePath, info.Mode().Perm())
	}
	if err := exec.Command(smokePath).Run(); err != nil {
		t.Fatalf("exec %q: %v", smokePath, err)
	}
	script := []byte("#!/bin/sh\n" + strings.Repeat("# pad\n", 4096) + "exit 0\n")

	const (
		numWriters = 16
		iters      = 25
		numForkers = 16
	)

	stopForkers := make(chan struct{})
	var writersWg, forkersWg sync.WaitGroup
	writerErrs := make([]error, numWriters)
	forkerErrs := make([]error, numForkers)

	for f := 0; f < numForkers; f++ {
		forkersWg.Add(1)
		go func(id int) {
			defer forkersWg.Done()
			for {
				select {
				case <-stopForkers:
					return
				default:
				}
				if err := exec.Command(trueBin).Run(); err != nil {
					forkerErrs[id] = fmt.Errorf("forker %d %s failed: %w", id, trueBin, err)
					return
				}
			}
		}(f)
	}

	for w := 0; w < numWriters; w++ {
		writersWg.Add(1)
		go func(id int) {
			defer writersWg.Done()
			for iter := 0; iter < iters; iter++ {
				p := filepath.Join(dir, fmt.Sprintf("w-%d-%d.sh", id, iter))
				if err := writeExecutableFileErr(p, script); err != nil {
					writerErrs[id] = fmt.Errorf("writer %d iter %d write failed: %w", id, iter, err)
					return
				}
				if err := exec.Command(p).Run(); err != nil {
					writerErrs[id] = fmt.Errorf("writer %d iter %d exec(%q) failed: %w", id, iter, p, err)
					return
				}
			}
		}(w)
	}

	writersWg.Wait()
	close(stopForkers)
	forkersWg.Wait()

	for _, err := range writerErrs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, err := range forkerErrs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
