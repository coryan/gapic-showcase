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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	StatusPass                = "PASS"
	StatusFail                = "FAIL"
	StatusSkippedNoMonitoring = "SKIPPED_NO_MONITORING"
	StatusUnsupportedByClient = "UNSUPPORTED_BY_CLIENT"
	StatusTimeout             = "TIMEOUT"
	StatusError               = "ERROR"
)

var runMarkerSeq atomic.Int64

func newRunMarker(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("e2e-run-%d-%06d", os.Getpid(), runMarkerSeq.Add(1))
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

// writeExecutableFileErr writes an executable (0o755) file while holding
// syscall.ForkLock.RLock() so concurrent forkExec calls in parallel tests
// cannot duplicate the writable file descriptor across fork/exec and trigger
// ETXTBSY (golang/go#22315).
func writeExecutableFileErr(path string, data []byte) error {
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	return os.WriteFile(path, data, 0o755)
}

// writeExecutableFile writes an executable (0o755) file guarded by
// syscall.ForkLock.RLock(), failing the test immediately on error.
func writeExecutableFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := writeExecutableFileErr(path, data); err != nil {
		t.Fatalf("writeExecutableFile(%q): %v", path, err)
	}
}

// writeProxyWrapper creates an executable shell wrapper script invoking e2e-test-proxy
// with a unique --marker and fixed proxyArgs, forwarding any additional CLI flags (e.g. --port=0).
func writeProxyWrapper(t *testing.T, proxyArgs ...string) (wrapperPath string, marker string) {
	t.Helper()
	if proxyBinaryPath == "" {
		t.Fatalf("writeProxyWrapper: proxyBinaryPath not initialized")
	}
	marker = newRunMarker(t)
	dir := t.TempDir()
	wrapperPath = filepath.Join(dir, fmt.Sprintf("proxy-%s.sh", marker))

	var b strings.Builder
	b.WriteString("#!/bin/sh\nexec ")
	b.WriteString(shellQuote(proxyBinaryPath))
	b.WriteString(" ")
	b.WriteString(shellQuote("--marker=" + marker))
	for _, arg := range proxyArgs {
		b.WriteString(" ")
		b.WriteString(shellQuote(arg))
	}
	b.WriteString(" \"$@\"\n")

	writeExecutableFile(t, wrapperPath, []byte(b.String()))
	return wrapperPath, marker
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// DirectProxyHandle manages an e2e-test-proxy child process connected directly via gRPC.
type DirectProxyHandle struct {
	Cmd    *exec.Cmd
	Addr   string
	Marker string
	Conn   *grpc.ClientConn
	Client proxypb.PubSubProxyClient

	stdout *syncBuffer
	stderr *syncBuffer
	waitCh chan error
	closed atomic.Bool
}

// startTestProxyDirect launches e2e-test-proxy, waits for READY 127.0.0.1:<port>\n,
// and dials PubSubProxy with 32 MiB message limits.
func startTestProxyDirect(t *testing.T, extraArgs ...string) *DirectProxyHandle {
	t.Helper()
	if proxyBinaryPath == "" {
		t.Fatalf("startTestProxyDirect: proxyBinaryPath not initialized")
	}

	marker := newRunMarker(t)
	args := []string{"--marker=" + marker}
	hasPort := false
	for _, a := range extraArgs {
		if strings.HasPrefix(a, "--port") || strings.HasPrefix(a, "-port") {
			hasPort = true
		}
		args = append(args, a)
	}
	if !hasPort {
		args = append(args, "--port=0")
	}

	cmd := exec.Command(proxyBinaryPath, args...)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("startTestProxyDirect: StdoutPipe failed: %v", err)
	}
	stderrBuf := &syncBuffer{}
	stdoutBuf := &syncBuffer{}
	cmd.Stderr = stderrBuf

	if err := cmd.Start(); err != nil {
		t.Fatalf("startTestProxyDirect: Start failed: %v", err)
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
		t.Fatalf("startTestProxyDirect: %v", err)
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-waitCh
		t.Fatalf("startTestProxyDirect: timed out waiting for READY (stderr=%q)", stderrBuf.String())
	}

	addr, ok := parseReadyLine(readyLine)
	if !ok {
		_ = cmd.Process.Kill()
		<-waitCh
		t.Fatalf("startTestProxyDirect: malformed READY line %q (stderr=%q)", readyLine, stderrBuf.String())
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
		t.Fatalf("startTestProxyDirect: grpc.NewClient(%q) failed: %v", addr, err)
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

func parseReadyLine(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "READY ") {
		return "", false
	}
	addr := strings.TrimSpace(strings.TrimPrefix(trimmed, "READY "))
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", false
	}
	return addr, true
}

func (h *DirectProxyHandle) Stdout() string { return h.stdout.String() }
func (h *DirectProxyHandle) Stderr() string { return h.stderr.String() }

// Shutdown calls ShutdownProxy, waits for clean exit, closes the gRPC connection,
// and asserts zero orphan proxy processes.
func (h *DirectProxyHandle) Shutdown(t *testing.T, grace time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	_, err := h.Client.ShutdownProxy(ctx, &proxypb.ShutdownProxyRequest{
		GracePeriodMs: grace.Milliseconds(),
	})
	_ = h.Conn.Close()
	if err != nil {
		return err
	}
	waitErr, ok := h.WaitExit(grace + 2*time.Second)
	if !ok {
		return fmt.Errorf("proxy did not exit within %v after ShutdownProxy", grace)
	}
	h.closed.Store(true)
	assertNoOrphanProxy(t, h.Marker)
	return waitErr
}

// Signal sends an OS signal to the proxy subprocess.
func (h *DirectProxyHandle) Signal(sig os.Signal) error {
	if h.Cmd.Process == nil {
		return fmt.Errorf("proxy process nil")
	}
	return h.Cmd.Process.Signal(sig)
}

// WaitExit waits up to timeout for the proxy subprocess to exit.
func (h *DirectProxyHandle) WaitExit(timeout time.Duration) (error, bool) {
	select {
	case err := <-h.waitCh:
		h.closed.Store(true)
		_ = h.Conn.Close()
		return err, true
	case <-time.After(timeout):
		return nil, false
	}
}

// ForceKill terminates the proxy subprocess with SIGKILL and waits for exit.
func (h *DirectProxyHandle) ForceKill(t *testing.T) {
	t.Helper()
	_ = h.Conn.Close()
	if h.Cmd.Process != nil {
		_ = h.Cmd.Process.Signal(syscall.SIGKILL)
	}
	_, _ = h.WaitExit(2 * time.Second)
	assertNoOrphanProxy(t, h.Marker)
}

// RawProxyOutcome captures the startup behavior of e2e-test-proxy when testing
// startup crashes or malformed READY lines.
type RawProxyOutcome struct {
	Marker   string
	ExitCode int
	Stdout   string
	Stderr   string
	TimedOut bool
	Err      error
}

func runTestProxyRaw(t *testing.T, timeout time.Duration, extraArgs ...string) RawProxyOutcome {
	t.Helper()
	if proxyBinaryPath == "" {
		t.Fatalf("runTestProxyRaw: proxyBinaryPath not initialized")
	}
	marker := newRunMarker(t)
	args := append([]string{"--marker=" + marker}, extraArgs...)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, proxyBinaryPath, args...)
	cmd.WaitDelay = time.Second
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	err := cmd.Run()
	exitCode := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exitCode = ee.ExitCode()
		} else {
			exitCode = -1
		}
	}
	assertNoOrphanProxy(t, marker)
	return RawProxyOutcome{
		Marker:   marker,
		ExitCode: exitCode,
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
		TimedOut: errors.Is(ctx.Err(), context.DeadlineExceeded),
		Err:      err,
	}
}

// --- Flexible JSON & Markdown Report Parsers (aligned with PROJECT.md § Interface Contracts 4) ---

type ParsedSummary struct {
	Total               int
	Pass                int
	Fail                int
	SkippedNoMonitoring int
	UnsupportedByClient int
	TimeoutCount        int
	ErrorCount          int
}

type ParsedDefaults struct {
	PublishBatchCountThreshold                int64
	PublishBatchByteThreshold                 int64
	PublishMaxRequestBytes                    int64
	PublishBatchDelayMs                       int64
	PublishTotalTimeoutMs                     int64
	PublishFlowControlMaxMessages             int64
	PublishFlowControlMaxBytes                int64
	PublishFlowControlBehavior                string
	PublishCompressionEnabledByDefault        bool
	PublishCompressionBytesThreshold          int64
	PublishCompressionAlgorithm               string
	PublishHedgingDelayMs                     int64
	PublishHedgingMaxTokens                   int64
	PublishHedgingRefillRatio                 float64
	SubscriberMaxOutstandingMessages          int64
	SubscriberMaxOutstandingBytes             int64
	SubscriberMaxExtensionMs                  int64
	SubscriberMinAckExtensionMs               int64
	SubscriberMaxAckExtensionMs               int64
	SubscriberAdaptiveAckExtension            bool
	SubscriberPerStreamFlowControl            bool
	SubscriberDefaultStreamAckDeadlineSeconds int32
	KeepalivePingIntervalMs                   int64
	KeepalivePongTimeoutMs                    int64
	RetryInitialDelayMs                       int64
	RetryMaxDelayMs                           int64
	RetryDelayMultiplier                      float64
	Raw                                       map[string]any
}

type ParsedCapabilities struct {
	ClientLanguage               string
	ClientLibrary                string
	ClientVersion                string
	SupportsMonitoring           *bool
	SupportsUnaryPull            bool
	SupportsBlockingPublish      bool
	SupportsPublishFlowControl   bool
	SupportsPublishCompression   bool
	SupportsPublishHedging       bool
	SupportsOrderingOptIn        bool
	SupportsSubscriberOrdering   bool
	SupportsKeepalive            bool
	SupportsEodReceiptModack     bool
	SupportsAutoNackOnDrop       bool
	SupportsAckWithResult        bool
	SupportsCustomRetryCodes     bool
	SupportsExactlyOnceDelivery  bool
	SupportsPerStreamFlowControl bool
	SupportsResumePublish        bool
	SupportsShutdownBehavior     bool
	SupportsShutdownTimeout      bool
	Defaults                     *ParsedDefaults
	DefaultsRaw                  map[string]any
	Raw                          map[string]any
}

type ParsedDeviation struct {
	Category    string
	Rule        string
	ExpectedGo  string
	Observed    string
	SpanName    string
	Severity    string
	Description string
	Raw         map[string]any
}

type ParsedScenarioResult struct {
	ID             string
	Name           string
	Category       string
	Feature        string
	Status         string
	StatusDetail   string
	TraceStatus    string
	Reason         string
	Error          string
	Duration       string
	DurationMs     int64
	Diagnostics    []string
	Deviations     []ParsedDeviation
	ProxyRestarted bool
	Raw            map[string]any
}

type ParsedTargetReport struct {
	TargetName   string
	TargetError  string
	TimedOut     bool
	Capabilities ParsedCapabilities
	Scenarios    []ParsedScenarioResult
	Summary      *ParsedSummary
	Raw          map[string]any
}

type ParsedDiffEntry struct {
	Kind        string
	ScenarioID  string
	Field       string
	Description string
	Verdicts    map[string]string
	Details     map[string]string
	Raw         map[string]any
}

type ParsedReport struct {
	Mode         string
	Target       string
	InfraError   string
	TimedOut     bool
	Capabilities *ParsedCapabilities
	Summary      *ParsedSummary
	Scenarios    []ParsedScenarioResult
	Targets      map[string]*ParsedTargetReport
	Differences  []ParsedDiffEntry
	Raw          map[string]any
}

type MarkdownTable struct {
	Headers []string
	Rows    []map[string]string
}

type ParsedMarkdownReport struct {
	Raw                 string
	Headings            []string
	Tables              []MarkdownTable
	Lines               []string
	FailureBreakdown    string
	TimeoutCount        int
	ErrorCount          int
	HasFailureBreakdown bool
	DiagnosticsBullets  []string
}

func normKey(k string) string {
	k = strings.ToLower(strings.TrimSpace(k))
	k = strings.ReplaceAll(k, "_", "")
	k = strings.ReplaceAll(k, "-", "")
	return k
}

func lookupField(m map[string]any, aliases ...string) (any, bool) {
	if m == nil {
		return nil, false
	}
	for _, a := range aliases {
		if v, ok := m[a]; ok {
			return v, true
		}
	}
	normAliases := make(map[string]struct{}, len(aliases))
	for _, a := range aliases {
		normAliases[normKey(a)] = struct{}{}
	}
	for k, v := range m {
		if _, ok := normAliases[normKey(k)]; ok {
			return v, true
		}
	}
	return nil, false
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return fmt.Sprintf("%v", x)
	case bool:
		if x {
			return "true"
		}
		return "false"
	case []any:
		var parts []string
		for _, elem := range x {
			if s := asString(elem); s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "; ")
	case map[string]any:
		if fv, ok := lookupField(x, "message", "msg", "error", "err", "reason", "detail", "details", "summary", "description", "text", "value", "status", "verdict", "outcome", "result"); ok {
			if s := asString(fv); s != "" {
				return s
			}
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			if s := asString(x[k]); s != "" {
				parts = append(parts, fmt.Sprintf("%s=%s", k, s))
			}
		}
		return strings.Join(parts, ", ")
	default:
		return ""
	}
}

func asInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case int:
		return x, true
	case int64:
		return int(x), true
	case json.Number:
		n, err := x.Int64()
		return int(n), err == nil
	default:
		return 0, false
	}
}

func asBool(v any) (bool, bool) {
	b, ok := v.(bool)
	return b, ok
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		return int64(x), true
	case int:
		return int64(x), true
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case json.Number:
		n, err := x.Int64()
		return n, err == nil
	default:
		return 0, false
	}
}

func asFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func asStringSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, elem := range arr {
		if s := asString(elem); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func asStringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		out[k] = asString(val)
	}
	return out
}

func canonicalStatus(raw string) (string, error) {
	s := strings.ToUpper(strings.TrimSpace(raw))
	s = strings.ReplaceAll(s, "-", "_")
	switch s {
	case "":
		return "", fmt.Errorf("empty scenario status")
	case StatusPass, "PASSED":
		return StatusPass, nil
	case StatusFail, "FAILED", StatusError, StatusTimeout:
		return StatusFail, nil
	case StatusSkippedNoMonitoring, "SKIPPED":
		return StatusSkippedNoMonitoring, nil
	case StatusUnsupportedByClient, "UNSUPPORTED":
		return StatusUnsupportedByClient, nil
	default:
		return "", fmt.Errorf("unknown canonical scenario status %q", raw)
	}
}

func parseDefaultsMap(m map[string]any) *ParsedDefaults {
	if m == nil {
		return nil
	}
	d := &ParsedDefaults{Raw: m}
	if v, ok := lookupField(m, "publish_batch_count_threshold"); ok {
		d.PublishBatchCountThreshold, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_batch_byte_threshold"); ok {
		d.PublishBatchByteThreshold, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_max_request_bytes"); ok {
		d.PublishMaxRequestBytes, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_batch_delay_ms"); ok {
		d.PublishBatchDelayMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_total_timeout_ms"); ok {
		d.PublishTotalTimeoutMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_flow_control_max_messages"); ok {
		d.PublishFlowControlMaxMessages, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_flow_control_max_bytes"); ok {
		d.PublishFlowControlMaxBytes, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_flow_control_behavior"); ok {
		d.PublishFlowControlBehavior = asString(v)
	}
	if v, ok := lookupField(m, "publish_compression_enabled_by_default"); ok {
		d.PublishCompressionEnabledByDefault, _ = asBool(v)
	}
	if v, ok := lookupField(m, "publish_compression_bytes_threshold"); ok {
		d.PublishCompressionBytesThreshold, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_compression_algorithm"); ok {
		d.PublishCompressionAlgorithm = asString(v)
	}
	if v, ok := lookupField(m, "publish_hedging_delay_ms"); ok {
		d.PublishHedgingDelayMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_hedging_max_tokens"); ok {
		d.PublishHedgingMaxTokens, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "publish_hedging_refill_ratio"); ok {
		d.PublishHedgingRefillRatio, _ = asFloat(v)
	}
	if v, ok := lookupField(m, "subscriber_max_outstanding_messages"); ok {
		d.SubscriberMaxOutstandingMessages, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "subscriber_max_outstanding_bytes"); ok {
		d.SubscriberMaxOutstandingBytes, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "subscriber_max_extension_ms"); ok {
		d.SubscriberMaxExtensionMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "subscriber_min_ack_extension_ms"); ok {
		d.SubscriberMinAckExtensionMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "subscriber_max_ack_extension_ms"); ok {
		d.SubscriberMaxAckExtensionMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "subscriber_adaptive_ack_extension"); ok {
		d.SubscriberAdaptiveAckExtension, _ = asBool(v)
	}
	if v, ok := lookupField(m, "subscriber_per_stream_flow_control"); ok {
		d.SubscriberPerStreamFlowControl, _ = asBool(v)
	}
	if v, ok := lookupField(m, "subscriber_default_stream_ack_deadline_seconds"); ok {
		if n, ok := asInt64(v); ok {
			d.SubscriberDefaultStreamAckDeadlineSeconds = int32(n)
		}
	}
	if v, ok := lookupField(m, "keepalive_ping_interval_ms"); ok {
		d.KeepalivePingIntervalMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "keepalive_pong_timeout_ms"); ok {
		d.KeepalivePongTimeoutMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "retry_initial_delay_ms"); ok {
		d.RetryInitialDelayMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "retry_max_delay_ms"); ok {
		d.RetryMaxDelayMs, _ = asInt64(v)
	}
	if v, ok := lookupField(m, "retry_delay_multiplier"); ok {
		d.RetryDelayMultiplier, _ = asFloat(v)
	}
	return d
}

func parseCapabilitiesMap(m map[string]any) ParsedCapabilities {
	caps := ParsedCapabilities{Raw: m}
	if v, ok := lookupField(m, "client_language", "language", "clientLanguage"); ok {
		caps.ClientLanguage = asString(v)
	}
	if v, ok := lookupField(m, "client_library", "library", "clientLibrary"); ok {
		caps.ClientLibrary = asString(v)
	}
	if v, ok := lookupField(m, "client_version", "version", "clientVersion"); ok {
		caps.ClientVersion = asString(v)
	}
	if v, ok := lookupField(m, "supports_monitoring", "supportsMonitoring", "monitoring"); ok {
		if b, ok := asBool(v); ok {
			caps.SupportsMonitoring = &b
		}
	}
	if v, ok := lookupField(m, "supports_unary_pull"); ok {
		caps.SupportsUnaryPull, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_blocking_publish"); ok {
		caps.SupportsBlockingPublish, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_publish_flow_control"); ok {
		caps.SupportsPublishFlowControl, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_publish_compression"); ok {
		caps.SupportsPublishCompression, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_publish_hedging"); ok {
		caps.SupportsPublishHedging, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_ordering_opt_in"); ok {
		caps.SupportsOrderingOptIn, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_subscriber_ordering"); ok {
		caps.SupportsSubscriberOrdering, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_keepalive"); ok {
		caps.SupportsKeepalive, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_eod_receipt_modack"); ok {
		caps.SupportsEodReceiptModack, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_auto_nack_on_drop"); ok {
		caps.SupportsAutoNackOnDrop, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_ack_with_result"); ok {
		caps.SupportsAckWithResult, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_custom_retry_codes"); ok {
		caps.SupportsCustomRetryCodes, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_exactly_once_delivery"); ok {
		caps.SupportsExactlyOnceDelivery, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_per_stream_flow_control"); ok {
		caps.SupportsPerStreamFlowControl, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_resume_publish"); ok {
		caps.SupportsResumePublish, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_shutdown_behavior"); ok {
		caps.SupportsShutdownBehavior, _ = asBool(v)
	}
	if v, ok := lookupField(m, "supports_shutdown_timeout"); ok {
		caps.SupportsShutdownTimeout, _ = asBool(v)
	}
	if v, ok := lookupField(m, "defaults"); ok {
		if dm, ok := v.(map[string]any); ok {
			caps.DefaultsRaw = dm
			caps.Defaults = parseDefaultsMap(dm)
		}
	}
	return caps
}

func parseSummaryMap(m map[string]any) *ParsedSummary {
	if m == nil {
		return nil
	}
	s := &ParsedSummary{}
	if v, ok := lookupField(m, "total", "total_scenarios", "count", "scenarios"); ok {
		s.Total, _ = asInt(v)
	}
	if v, ok := lookupField(m, "pass", "passed"); ok {
		s.Pass, _ = asInt(v)
	}
	if v, ok := lookupField(m, "fail", "failed"); ok {
		s.Fail, _ = asInt(v)
	}
	if v, ok := lookupField(m, "skipped_no_monitoring", "skipped", "skipped_count"); ok {
		s.SkippedNoMonitoring, _ = asInt(v)
	}
	if v, ok := lookupField(m, "unsupported_by_client", "unsupported", "unsupported_count"); ok {
		s.UnsupportedByClient, _ = asInt(v)
	}
	if v, ok := lookupField(m, "timeout_count"); ok {
		s.TimeoutCount, _ = asInt(v)
	}
	if v, ok := lookupField(m, "error_count"); ok {
		s.ErrorCount, _ = asInt(v)
	}
	return s
}

func parseDeviationsSlice(v any) []ParsedDeviation {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]ParsedDeviation, 0, len(arr))
	for _, item := range arr {
		dm, ok := item.(map[string]any)
		if !ok {
			continue
		}
		dv := ParsedDeviation{Raw: dm}
		if f, ok := lookupField(dm, "category"); ok {
			dv.Category = asString(f)
		}
		if f, ok := lookupField(dm, "rule"); ok {
			dv.Rule = asString(f)
		}
		if f, ok := lookupField(dm, "expected_go"); ok {
			dv.ExpectedGo = asString(f)
		}
		if f, ok := lookupField(dm, "observed"); ok {
			dv.Observed = asString(f)
		}
		if f, ok := lookupField(dm, "span_name"); ok {
			dv.SpanName = asString(f)
		}
		if f, ok := lookupField(dm, "severity"); ok {
			dv.Severity = asString(f)
		}
		if f, ok := lookupField(dm, "description"); ok {
			dv.Description = asString(f)
		}
		out = append(out, dv)
	}
	return out
}

func parseScenariosSlice(raw any) ([]ParsedScenarioResult, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("scenarios block is not an array")
	}
	out := make([]ParsedScenarioResult, 0, len(arr))
	for i, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("scenario[%d] is not an object", i)
		}
		sr := ParsedScenarioResult{Raw: m}
		if v, ok := lookupField(m, "id", "scenario_id", "name", "scenario"); ok {
			sr.ID = asString(v)
		}
		if v, ok := lookupField(m, "name"); ok {
			sr.Name = asString(v)
		}
		if v, ok := lookupField(m, "category", "group", "suite"); ok {
			sr.Category = asString(v)
		}
		if v, ok := lookupField(m, "feature"); ok {
			sr.Feature = asString(v)
		}
		var rawStatusNorm string
		if v, ok := lookupField(m, "status", "verdict", "outcome", "result", "state"); ok {
			rawStr := asString(v)
			canon, err := canonicalStatus(rawStr)
			if err != nil {
				return nil, fmt.Errorf("scenario[%d] (%s): %w", i, sr.ID, err)
			}
			sr.Status = canon
			rawStatusNorm = strings.ReplaceAll(strings.ToUpper(strings.TrimSpace(rawStr)), "-", "_")
		} else {
			return nil, fmt.Errorf("scenario[%d] (%s): missing status", i, sr.ID)
		}
		if v, ok := lookupField(m, "status_detail"); ok {
			sr.StatusDetail = strings.ToUpper(strings.TrimSpace(asString(v)))
		}
		if sr.StatusDetail == "" && (rawStatusNorm == StatusTimeout || rawStatusNorm == StatusError) {
			sr.StatusDetail = rawStatusNorm
		}
		if v, ok := lookupField(m, "trace_status"); ok {
			sr.TraceStatus = asString(v)
		}
		if v, ok := lookupField(m, "reason"); ok {
			sr.Reason = asString(v)
		}
		if v, ok := lookupField(m, "error", "err", "error_message", "message", "failure"); ok {
			sr.Error = asString(v)
		}
		if v, ok := lookupField(m, "duration", "elapsed"); ok {
			sr.Duration = asString(v)
		}
		if v, ok := lookupField(m, "duration_ms"); ok {
			sr.DurationMs, _ = asInt64(v)
			if sr.Duration == "" {
				sr.Duration = asString(v)
			}
		}
		if v, ok := lookupField(m, "diagnostics"); ok {
			sr.Diagnostics = asStringSlice(v)
		}
		if v, ok := lookupField(m, "deviations"); ok {
			sr.Deviations = parseDeviationsSlice(v)
		}
		if v, ok := lookupField(m, "proxy_restarted"); ok {
			sr.ProxyRestarted, _ = asBool(v)
		}
		out = append(out, sr)
	}
	return out, nil
}

// ParseJSONReport parses single-target, multi-target, or --compare JSON reports.
func ParseJSONReport(data []byte) (*ParsedReport, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("empty JSON report")
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("invalid JSON report: %w", err)
	}
	if root == nil {
		return nil, fmt.Errorf("invalid JSON report: expected JSON object, got null")
	}

	rep := &ParsedReport{
		Targets: make(map[string]*ParsedTargetReport),
		Raw:     root,
	}
	if v, ok := lookupField(root, "mode"); ok {
		rep.Mode = asString(v)
	}
	if v, ok := lookupField(root, "target"); ok {
		rep.Target = asString(v)
	}
	if v, ok := lookupField(root, "infra_error"); ok {
		rep.InfraError = asString(v)
	}
	if v, ok := lookupField(root, "timed_out"); ok {
		rep.TimedOut, _ = asBool(v)
	}

	if v, ok := lookupField(root, "capabilities", "client_capabilities"); ok {
		if cm, ok := v.(map[string]any); ok {
			c := parseCapabilitiesMap(cm)
			rep.Capabilities = &c
		}
	}
	if v, ok := lookupField(root, "summary", "counts", "totals", "stats"); ok {
		if sm, ok := v.(map[string]any); ok {
			rep.Summary = parseSummaryMap(sm)
		}
	}
	if v, ok := lookupField(root, "scenarios", "results", "scenario_results", "cases"); ok {
		scens, err := parseScenariosSlice(v)
		if err != nil {
			return nil, err
		}
		rep.Scenarios = scens
	}

	if v, ok := lookupField(root, "targets", "clients", "reports", "runs"); ok {
		switch tv := v.(type) {
		case map[string]any:
			for name, rawTarget := range tv {
				tm, ok := rawTarget.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("target %q: expected JSON object, got %T", name, rawTarget)
				}
				tr, err := parseTargetReport(name, tm)
				if err != nil {
					return nil, err
				}
				rep.Targets[name] = tr
			}
		case []any:
			for idx, rawTarget := range tv {
				tm, ok := rawTarget.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("targets[%d]: expected JSON object, got %T", idx, rawTarget)
				}
				name := fmt.Sprintf("target-%d", idx)
				if nv, ok := lookupField(tm, "target", "target_name", "name", "client", "id"); ok {
					if s := asString(nv); s != "" {
						name = s
					}
				}
				if _, dup := rep.Targets[name]; dup {
					return nil, fmt.Errorf("duplicate target report %q", name)
				}
				tr, err := parseTargetReport(name, tm)
				if err != nil {
					return nil, err
				}
				rep.Targets[name] = tr
			}
		default:
			if v != nil {
				return nil, fmt.Errorf("targets: expected object or array, got %T", v)
			}
		}
	}

	hasExplicitDiffs := false
	if v, ok := lookupField(root, "differences", "diffs", "divergences", "behavioral_differences", "comparisons"); ok {
		hasExplicitDiffs = true
		if v != nil {
			arr, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("differences: expected array, got %T", v)
			}
			for idx, item := range arr {
				dm, ok := item.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("differences[%d]: expected JSON object, got %T", idx, item)
				}
				de := ParsedDiffEntry{
					Verdicts: make(map[string]string),
					Raw:      dm,
				}
				if kv, ok := lookupField(dm, "kind"); ok {
					de.Kind = asString(kv)
				}
				if sv, ok := lookupField(dm, "scenario_id", "scenario", "id", "name"); ok {
					de.ScenarioID = asString(sv)
				}
				if fv, ok := lookupField(dm, "field"); ok {
					de.Field = asString(fv)
				}
				if dv, ok := lookupField(dm, "description", "summary", "reason", "delta"); ok {
					de.Description = asString(dv)
				}
				if dtv, ok := lookupField(dm, "details"); ok {
					de.Details = asStringMap(dtv)
				}
				if vv, ok := lookupField(dm, "verdicts", "statuses", "results", "by_target", "targets"); ok {
					if vm, ok := vv.(map[string]any); ok {
						for tName, rawSt := range vm {
							stStr := asString(rawSt)
							if rm, isObj := rawSt.(map[string]any); isObj {
								if sv, ok := lookupField(rm, "status", "verdict", "outcome", "result", "state"); ok {
									stStr = asString(sv)
								}
							}
							upper := strings.ToUpper(strings.TrimSpace(stStr))
							if (de.Kind == "" || de.Kind == "scenario_verdict") && upper != StatusError && upper != StatusTimeout && upper != "OK" && upper != "MISSING" {
								if st, err := canonicalStatus(stStr); err == nil {
									de.Verdicts[tName] = st
									continue
								}
							}
							de.Verdicts[tName] = stStr
						}
					}
				}
				rep.Differences = append(rep.Differences, de)
			}
		}
	}

	// If multi-target scenarios exist without an explicit differences field,
	// synthesize mismatches (including scenarios omitted from a subset of targets).
	if !hasExplicitDiffs && len(rep.Targets) > 1 {
		byScenario := make(map[string]map[string]string)
		for tName, tr := range rep.Targets {
			for _, sc := range tr.Scenarios {
				if byScenario[sc.ID] == nil {
					byScenario[sc.ID] = make(map[string]string)
				}
				byScenario[sc.ID][tName] = sc.Status
			}
		}
		scIDs := make([]string, 0, len(byScenario))
		for scID := range byScenario {
			scIDs = append(scIDs, scID)
		}
		sort.Strings(scIDs)
		for _, scID := range scIDs {
			verdicts := byScenario[scID]
			distinct := make(map[string]struct{})
			for _, st := range verdicts {
				distinct[st] = struct{}{}
			}
			omitted := len(verdicts) < len(rep.Targets)
			if omitted || len(distinct) > 1 {
				desc := ""
				if omitted {
					for tName := range rep.Targets {
						if _, present := verdicts[tName]; !present {
							verdicts[tName] = "MISSING"
						}
					}
					desc = "scenario omitted by one or more targets"
				}
				rep.Differences = append(rep.Differences, ParsedDiffEntry{
					Kind:        "scenario_verdict",
					ScenarioID:  scID,
					Description: desc,
					Verdicts:    verdicts,
				})
			}
		}
	}

	return rep, nil
}

func parseTargetReport(name string, m map[string]any) (*ParsedTargetReport, error) {
	tr := &ParsedTargetReport{
		TargetName: name,
		Raw:        m,
	}
	if v, ok := lookupField(m, "target_error"); ok {
		tr.TargetError = asString(v)
	}
	if v, ok := lookupField(m, "timed_out"); ok {
		tr.TimedOut, _ = asBool(v)
	}
	if v, ok := lookupField(m, "capabilities", "client_capabilities"); ok {
		if cm, ok := v.(map[string]any); ok {
			tr.Capabilities = parseCapabilitiesMap(cm)
		}
	}
	if v, ok := lookupField(m, "summary", "counts", "totals", "stats"); ok {
		if sm, ok := v.(map[string]any); ok {
			tr.Summary = parseSummaryMap(sm)
		}
	}
	if v, ok := lookupField(m, "scenarios", "results", "scenario_results", "cases"); ok {
		scens, err := parseScenariosSlice(v)
		if err != nil {
			return nil, fmt.Errorf("target %q: %w", name, err)
		}
		tr.Scenarios = scens
	}
	return tr, nil
}

func ReadJSONReport(t *testing.T, path string) *ParsedReport {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadJSONReport(%q): %v", path, err)
	}
	rep, err := ParseJSONReport(data)
	if err != nil {
		t.Fatalf("ReadJSONReport(%q): %v", path, err)
	}
	return rep
}

func computeScenarioCounts(scenarios []ParsedScenarioResult) ParsedSummary {
	var s ParsedSummary
	for _, sc := range scenarios {
		s.Total++
		switch sc.Status {
		case StatusPass:
			s.Pass++
		case StatusFail:
			s.Fail++
			if strings.EqualFold(sc.StatusDetail, StatusTimeout) {
				s.TimeoutCount++
			} else if strings.EqualFold(sc.StatusDetail, StatusError) {
				s.ErrorCount++
			}
		case StatusSkippedNoMonitoring:
			s.SkippedNoMonitoring++
		case StatusUnsupportedByClient:
			s.UnsupportedByClient++
		}
	}
	return s
}

func (r *ParsedReport) ComputedCounts() ParsedSummary {
	return computeScenarioCounts(r.Scenarios)
}

func (r *ParsedReport) FindScenario(id string) (*ParsedScenarioResult, bool) {
	for i := range r.Scenarios {
		if r.Scenarios[i].ID == id {
			return &r.Scenarios[i], true
		}
	}
	return nil, false
}

func assertSummaryMatchesCounts(t *testing.T, scope string, sum *ParsedSummary, scenarios []ParsedScenarioResult) {
	t.Helper()
	if sum == nil {
		t.Fatalf("%s: summary block is missing", scope)
	}
	computed := computeScenarioCounts(scenarios)
	if sum.TimeoutCount+sum.ErrorCount > sum.Fail {
		t.Errorf("%s: timeout_count (%d) + error_count (%d) exceeds fail (%d)", scope, sum.TimeoutCount, sum.ErrorCount, sum.Fail)
	}
	if sum.Pass+sum.Fail+sum.SkippedNoMonitoring+sum.UnsupportedByClient != sum.Total {
		t.Errorf("%s: summary components %+v do not sum to total %d", scope, *sum, sum.Total)
	}
	if sum.Total != computed.Total ||
		sum.Pass != computed.Pass ||
		sum.Fail != computed.Fail ||
		sum.SkippedNoMonitoring != computed.SkippedNoMonitoring ||
		sum.UnsupportedByClient != computed.UnsupportedByClient ||
		sum.TimeoutCount != computed.TimeoutCount ||
		sum.ErrorCount != computed.ErrorCount {
		t.Errorf("%s: summary %+v does not match computed scenario counts %+v", scope, *sum, computed)
	}
}

func AssertTargetSummaryMatchesScenarios(t *testing.T, tr ParsedTargetReport) {
	t.Helper()
	assertSummaryMatchesCounts(t, fmt.Sprintf("AssertTargetSummaryMatchesScenarios(%s)", tr.TargetName), tr.Summary, tr.Scenarios)
}

func AssertSummaryMatchesScenarios(t *testing.T, r *ParsedReport) {
	t.Helper()
	if r == nil {
		t.Fatalf("AssertSummaryMatchesScenarios: nil report")
	}
	if r.Mode == "compare" || (r.Summary == nil && len(r.Targets) > 1) {
		if len(r.Targets) == 0 {
			t.Fatalf("AssertSummaryMatchesScenarios: compare report has no targets")
		}
		if r.Mode == "compare" {
			if r.Summary != nil || len(r.Scenarios) != 0 || r.Capabilities != nil || r.Target != "" {
				t.Errorf("AssertSummaryMatchesScenarios: compare report should omit root target/capabilities/summary/scenarios, got target=%q caps=%v summary=%v scenarios=%d", r.Target, r.Capabilities != nil, r.Summary != nil, len(r.Scenarios))
			}
		}
		names := make([]string, 0, len(r.Targets))
		for name := range r.Targets {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			tr := r.Targets[name]
			if tr == nil {
				t.Fatalf("AssertSummaryMatchesScenarios: nil target %q", name)
			}
			AssertTargetSummaryMatchesScenarios(t, *tr)
		}
		return
	}
	if r.Summary == nil {
		t.Fatalf("AssertSummaryMatchesScenarios: report summary block is missing")
	}
	assertSummaryMatchesCounts(t, "AssertSummaryMatchesScenarios", r.Summary, r.Scenarios)
	if len(r.Targets) == 1 {
		for _, tr := range r.Targets {
			if tr != nil {
				AssertTargetSummaryMatchesScenarios(t, *tr)
			}
		}
	}
}

func isWordIdentByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func isTokenIdentByte(b byte) bool {
	return isWordIdentByte(b) || b == '-'
}

// stripPairedEmphasis removes matched emphasis delimiters (e.g. **...**, __...__,
// *...*, _..._) while leaving intraword underscores such as SKIPPED_NO_MONITORING intact.
func stripPairedEmphasis(s, delim string, wordBoundaryOnly bool) string {
	dlen := len(delim)
	if dlen == 0 || !strings.Contains(s, delim) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if strings.HasPrefix(s[i:], delim) {
			leftOK := !wordBoundaryOnly || i == 0 || !isWordIdentByte(s[i-1])
			afterOpen := i + dlen
			nextIsSpace := afterOpen < len(s) && s[afterOpen] == ' '
			if leftOK && afterOpen < len(s) && !nextIsSpace {
				closeIdx := -1
				for j := afterOpen + 1; j <= len(s)-dlen; j++ {
					if !strings.HasPrefix(s[j:], delim) {
						continue
					}
					if s[j-1] == ' ' {
						continue
					}
					afterClose := j + dlen
					rightOK := !wordBoundaryOnly || afterClose == len(s) || !isWordIdentByte(s[afterClose])
					if rightOK {
						closeIdx = j
						break
					}
				}
				if closeIdx >= 0 {
					inner := stripPairedEmphasis(s[afterOpen:closeIdx], delim, wordBoundaryOnly)
					b.WriteString(inner)
					i = closeIdx + dlen
					continue
				}
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

func stripMarkdownFormatting(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "`", "")
	s = stripPairedEmphasis(s, "**", false)
	s = stripPairedEmphasis(s, "__", true)
	s = stripPairedEmphasis(s, "*", false)
	s = stripPairedEmphasis(s, "_", true)
	return strings.TrimSpace(s)
}

func hasTrailingUnescapedPipe(s string) bool {
	if !strings.HasSuffix(s, "|") {
		return false
	}
	bs := 0
	for i := len(s) - 2; i >= 0 && s[i] == '\\'; i-- {
		bs++
	}
	return bs%2 == 0
}

func splitMarkdownRow(line string) ([]string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return nil, false
	}
	hasLeadingPipe := strings.HasPrefix(trimmed, "|")
	body := trimmed
	if hasLeadingPipe {
		body = body[1:]
	}
	if hasTrailingUnescapedPipe(body) {
		body = body[:len(body)-1]
	}

	var cells []string
	var cur strings.Builder
	inCode := false
	splitCount := 0
	for i := 0; i < len(body); i++ {
		ch := body[i]
		if ch == '\\' && i+1 < len(body) && body[i+1] == '|' {
			cur.WriteByte('|')
			i++
			continue
		}
		if ch == '`' {
			inCode = !inCode
			cur.WriteByte(ch)
			continue
		}
		if ch == '|' && !inCode {
			cells = append(cells, stripMarkdownFormatting(cur.String()))
			cur.Reset()
			splitCount++
			continue
		}
		cur.WriteByte(ch)
	}
	cells = append(cells, stripMarkdownFormatting(cur.String()))
	if !hasLeadingPipe && splitCount == 0 {
		return nil, false
	}
	return cells, true
}

func isMarkdownSeparatorRow(cells []string) bool {
	if len(cells) == 0 {
		return false
	}
	for _, c := range cells {
		c = strings.TrimSpace(c)
		if c == "" {
			return false
		}
		trimmed := strings.Trim(c, "-:")
		if trimmed != "" {
			return false
		}
		if !strings.Contains(c, "-") {
			return false
		}
	}
	return true
}

func ParseMarkdownReport(md string) (*ParsedMarkdownReport, error) {
	if strings.TrimSpace(md) == "" {
		return nil, fmt.Errorf("empty Markdown report")
	}
	lines := strings.Split(strings.ReplaceAll(md, "\r\n", "\n"), "\n")
	parsed := &ParsedMarkdownReport{
		Raw:   md,
		Lines: lines,
	}

	inDiagnostics := false
	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, "#") {
			heading := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
			if heading != "" {
				cleanHeading := stripMarkdownFormatting(heading)
				parsed.Headings = append(parsed.Headings, cleanHeading)
				inDiagnostics = strings.EqualFold(cleanHeading, "diagnostics")
			} else {
				inDiagnostics = false
			}
			continue
		}
		cleanEmphasis := strings.TrimSpace(strings.Trim(trimmed, "*_"))
		if strings.HasPrefix(cleanEmphasis, "Failure breakdown:") {
			parsed.FailureBreakdown = cleanEmphasis
			parsed.HasFailureBreakdown = true
			var toCnt, errCnt int
			if n, err := fmt.Sscanf(cleanEmphasis, "Failure breakdown: %d timeout(s), %d error(s)", &toCnt, &errCnt); err == nil && n == 2 {
				parsed.TimeoutCount = toCnt
				parsed.ErrorCount = errCnt
			}
			continue
		}
		if inDiagnostics && strings.HasPrefix(trimmed, "- ") {
			bullet := strings.TrimSpace(strings.TrimPrefix(trimmed, "- "))
			if bullet != "" {
				parsed.DiagnosticsBullets = append(parsed.DiagnosticsBullets, bullet)
			}
			continue
		}
		headers, ok := splitMarkdownRow(trimmed)
		if !ok || i+1 >= len(lines) {
			continue
		}
		sep, okSep := splitMarkdownRow(lines[i+1])
		if !okSep || len(sep) != len(headers) || !isMarkdownSeparatorRow(sep) {
			continue
		}
		table := MarkdownTable{Headers: headers}
		i += 2
		for i < len(lines) {
			rowCells, okRow := splitMarkdownRow(lines[i])
			if !okRow {
				i--
				break
			}
			rowMap := make(map[string]string, maxInt(len(headers), len(rowCells)))
			for idx, h := range headers {
				if idx < len(rowCells) {
					rowMap[h] = rowCells[idx]
				} else {
					rowMap[h] = ""
				}
			}
			for idx := len(headers); idx < len(rowCells); idx++ {
				rowMap[fmt.Sprintf("_col_%d", idx)] = rowCells[idx]
			}
			table.Rows = append(table.Rows, rowMap)
			i++
		}
		parsed.Tables = append(parsed.Tables, table)
	}
	return parsed, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func ReadMarkdownReport(t *testing.T, path string) *ParsedMarkdownReport {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadMarkdownReport(%q): %v", path, err)
	}
	rep, err := ParseMarkdownReport(string(data))
	if err != nil {
		t.Fatalf("ReadMarkdownReport(%q): %v", path, err)
	}
	return rep
}

func (m *ParsedMarkdownReport) HasHeadingContaining(substr string) bool {
	low := strings.ToLower(substr)
	for _, h := range m.Headings {
		if strings.Contains(strings.ToLower(h), low) {
			return true
		}
	}
	return false
}

func containsToken(haystackLower, tok string) bool {
	needle := strings.ToLower(strings.TrimSpace(tok))
	if needle == "" {
		return false
	}
	checkLeft := isTokenIdentByte(needle[0])
	checkRight := isTokenIdentByte(needle[len(needle)-1])
	for start := 0; start <= len(haystackLower)-len(needle); {
		idx := strings.Index(haystackLower[start:], needle)
		if idx < 0 {
			return false
		}
		pos := start + idx
		end := pos + len(needle)
		leftOK := !checkLeft || pos == 0 || !isTokenIdentByte(haystackLower[pos-1])
		rightOK := !checkRight || end == len(haystackLower) || !isTokenIdentByte(haystackLower[end])
		if leftOK && rightOK {
			return true
		}
		start = pos + 1
	}
	return false
}

func (m *ParsedMarkdownReport) HasTableRowContaining(tokens ...string) bool {
	if len(tokens) == 0 {
		return false
	}
	for _, tbl := range m.Tables {
		for _, row := range tbl.Rows {
			var combined strings.Builder
			for _, v := range row {
				combined.WriteString(v)
				combined.WriteByte(' ')
			}
			rowStr := strings.ToLower(combined.String())
			allMatch := true
			for _, tok := range tokens {
				if !containsToken(rowStr, tok) {
					allMatch = false
					break
				}
			}
			if allMatch {
				return true
			}
		}
	}
	return false
}

func (m *ParsedMarkdownReport) ContainsTokens(tokens ...string) bool {
	if len(tokens) == 0 {
		return false
	}
	// Strip Markdown delimiters and unescape \| for raw text scanning without
	// breaking token boundaries across multi-line reports.
	rawClean := strings.ReplaceAll(m.Raw, `\|`, "|")
	low := strings.ToLower(stripMarkdownFormatting(rawClean))
	for _, tok := range tokens {
		if !containsToken(low, tok) {
			return false
		}
	}
	return true
}
