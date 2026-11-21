// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultMaxOutputBytes    = 64 * 1024
	defaultMaxReadyLineBytes = 8 * 1024
	defaultMaxPreReadyLines  = 64
	defaultMaxGRPCMsgSize    = 32 << 20
	processWaitDelay         = 2 * time.Second
	startupProbeExitGrace    = 750 * time.Millisecond
)

type ProxyHandle interface {
	Client() proxypb.PubSubProxyClient
	Capabilities() *proxypb.GetCapabilitiesResponse
	Close(ctx context.Context) error
	Exited() <-chan error
	Stderr() string
	PID() int
}
type ProxyOptions struct {
	EmulatorHost                                        string
	MaxOutputBytes, MaxReadyLineBytes, MaxPreReadyLines int
	StartupTimeout, ShutdownTimeout                     time.Duration
}
type ProxyProcess struct {
	cmd                  *exec.Cmd
	pgid, exitCode       int
	addr                 string
	conn                 *grpc.ClientConn
	client               proxypb.PubSubProxyClient
	caps                 *proxypb.GetCapabilitiesResponse
	stdoutPipe           io.ReadCloser
	stdoutBuf, stderrBuf *cappedBuffer
	readDone, waitDone   chan struct{}
	exitCh               chan error
	shutdownTimeout      time.Duration
	mu                   sync.Mutex
	exited               bool
	waitErr, stopErr     error
	stopOnce             sync.Once
}
type readyResult struct {
	addr string
	err  error
}

func firstPos[T ~int | ~int64](vals ...T) T {
	for _, v := range vals {
		if v > 0 {
			return v
		}
	}
	return 0
}

func StartProxy(ctx context.Context, spec TargetSpec, opts ProxyOptions) (*ProxyProcess, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(spec.Command) == "" {
		return nil, InfraErrorf("proxy command is empty for target %q", spec.Name)
	}
	maxOut, maxLine, maxNoise := firstPos(opts.MaxOutputBytes, defaultMaxOutputBytes), firstPos(opts.MaxReadyLineBytes, defaultMaxReadyLineBytes), firstPos(opts.MaxPreReadyLines, defaultMaxPreReadyLines)
	startupTimeout, shutdownTimeout := firstPos(opts.StartupTimeout, spec.StartupTimeout, DefaultStartupTimeout), firstPos(opts.ShutdownTimeout, spec.ShutdownTimeout, DefaultShutdownTimeout)
	cmd := exec.Command(spec.Command, EnsurePortFlag(spec.Args)...)
	cmd.Dir, cmd.Env = spec.WorkDir, buildProxyEnv(append(slices.Clone(os.Environ()), spec.Env...), opts.EmulatorHost)
	cmd.SysProcAttr, cmd.WaitDelay = &syscall.SysProcAttr{Setpgid: true}, processWaitDelay
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, InfraErrorf("creating stdout pipe for %q: %w", spec.Command, err)
	}
	p := &ProxyProcess{cmd: cmd, stdoutPipe: stdoutPipe, stdoutBuf: newCappedBuffer(maxOut), stderrBuf: newCappedBuffer(maxOut), readDone: make(chan struct{}), waitDone: make(chan struct{}), exitCh: make(chan error, 1), shutdownTimeout: shutdownTimeout, exitCode: -1}
	cmd.Stderr = p.stderrBuf
	if err := cmd.Start(); err != nil {
		_ = stdoutPipe.Close()
		return nil, InfraErrorf("starting proxy %q: %w", spec.Command, err)
	}
	p.pgid = cmd.Process.Pid
	readyCh := make(chan readyResult, 1)
	go p.scanStdoutAndDrain(stdoutPipe, maxLine, maxNoise, readyCh)
	go func() { <-p.readDone; p.finishWait(p.cmd.Wait()) }()
	startupCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	failStart := func(cause error) (*ProxyProcess, error) {
		if p.conn != nil {
			_ = p.conn.Close()
		}
		if p.client != nil && startupCtx.Err() == nil && !p.leaderExited() {
			t := time.NewTimer(startupProbeExitGrace)
			select {
			case <-p.waitDone:
				t.Stop()
			case <-startupCtx.Done():
				t.Stop()
			case <-t.C:
			}
		}
		_ = p.killAndReap()
		extra := ""
		if _, code, exited := p.ExitInfo(); exited {
			extra = fmt.Sprintf(" (exit_code=%d)", code)
		}
		if stderr := strings.TrimSpace(p.stderrBuf.String()); stderr != "" {
			extra += fmt.Sprintf(" (stderr=%q)", stderr)
		}
		return nil, InfraErrorf("proxy %q startup failed%s: %w", spec.Name, extra, cause)
	}
	select {
	case res := <-readyCh:
		if res.err != nil {
			return failStart(res.err)
		}
		p.addr = res.addr
	case <-p.waitDone:
		detail := InfraErrorf("proxy exited before emitting READY")
		select {
		case res := <-readyCh:
			if res.err != nil {
				detail = res.err
			} else if res.addr != "" {
				detail = InfraErrorf("proxy exited immediately after READY %s", res.addr)
			}
		default:
		}
		return failStart(detail)
	case <-startupCtx.Done():
		return failStart(InfraErrorf("timed out waiting for READY after %v: %w", startupTimeout, startupCtx.Err()))
	}
	conn, err := grpc.NewClient("passthrough:///"+p.addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaultMaxGRPCMsgSize), grpc.MaxCallSendMsgSize(defaultMaxGRPCMsgSize)))
	if err != nil {
		return failStart(InfraErrorf("dialing proxy %q: %w", p.addr, err))
	}
	p.conn, p.client = conn, proxypb.NewPubSubProxyClient(conn)
	caps, err := p.client.GetCapabilities(startupCtx, &proxypb.GetCapabilitiesRequest{})
	if err != nil || caps == nil {
		if err == nil {
			err = errors.New("nil GetCapabilitiesResponse")
		}
		return failStart(InfraErrorf("GetCapabilities probe failed: %w", err))
	}
	p.caps = caps
	return p, nil
}
func (p *ProxyProcess) scanStdoutAndDrain(r io.Reader, maxLine, maxPreReady int, readyCh chan<- readyResult) {
	defer close(p.readDone)
	br := bufio.NewReaderSize(r, maxLine)
	for preReady := 0; ; preReady++ {
		frag, err := br.ReadSlice('\n')
		if len(frag) > 0 {
			_, _ = p.stdoutBuf.Write(frag)
		}
		trimmed := strings.TrimSpace(string(frag))
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			readyCh <- readyResult{err: InfraErrorf("proxy stdout line exceeded %d bytes before READY", maxLine)}
		case trimmed == "READY" || strings.HasPrefix(trimmed, "READY "):
			addr, parseErr := parseReadyLine(string(frag))
			readyCh <- readyResult{addr: addr, err: parseErr}
		case err != nil:
			readyCh <- readyResult{err: InfraErrorf("proxy exited or stdout failed before READY: %v", err)}
			return
		case preReady >= maxPreReady:
			readyCh <- readyResult{err: InfraErrorf("proxy exceeded %d pre-READY stdout lines", maxPreReady)}
		default:
			continue
		}
		_, _ = io.Copy(p.stdoutBuf, br)
		return
	}
}

func parseReadyLine(line string) (string, error) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "READY ") {
		return "", InfraErrorf("malformed READY line %q", trimmed)
	}
	host, portStr, err := net.SplitHostPort(strings.TrimSpace(strings.TrimPrefix(trimmed, "READY ")))
	port, pErr := strconv.Atoi(portStr)
	if err != nil || host == "" || portStr == "" || pErr != nil || port < 1 || port > 65535 {
		return "", InfraErrorf("malformed READY endpoint %q", trimmed)
	}
	return net.JoinHostPort(host, portStr), nil
}

func (p *ProxyProcess) finishWait(err error) {
	p.mu.Lock()
	p.waitErr, p.exited, p.exitCode = err, true, -1
	if err == nil {
		p.exitCode = 0
	} else if ee, ok := err.(*exec.ExitError); ok {
		p.exitCode = ee.ExitCode()
	}
	p.mu.Unlock()
	p.exitCh <- err
	close(p.exitCh)
	close(p.waitDone)
}
func (p *ProxyProcess) Client() proxypb.PubSubProxyClient              { return p.client }
func (p *ProxyProcess) Capabilities() *proxypb.GetCapabilitiesResponse { return p.caps }
func (p *ProxyProcess) Exited() <-chan error                           { return p.exitCh }
func (p *ProxyProcess) WaitDone() <-chan struct{}                      { return p.waitDone }
func (p *ProxyProcess) Stderr() string                                 { return p.stderrBuf.String() }
func (p *ProxyProcess) Stdout() string                                 { return p.stdoutBuf.String() }
func (p *ProxyProcess) PID() int                                       { return p.pgid }
func (p *ProxyProcess) Addr() string                                   { return p.addr }
func (p *ProxyProcess) Stop() error                                    { return p.Close(context.Background()) }
func (p *ProxyProcess) ExitInfo() (error, int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr, p.exitCode, p.exited
}
func (p *ProxyProcess) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.stopOnce.Do(func() { p.stopErr = p.shutdown(ctx) })
	return p.stopErr
}
func (p *ProxyProcess) shutdown(ctx context.Context) error {
	if !p.leaderExited() && p.client != nil && (ctx == nil || ctx.Err() == nil) {
		rpcGrace := max(100*time.Millisecond, min(time.Second, p.shutdownTimeout))
		rpcCtx, cancel := context.WithTimeout(context.Background(), rpcGrace)
		_, _ = p.client.ShutdownProxy(rpcCtx, &proxypb.ShutdownProxyRequest{GracePeriodMs: rpcGrace.Milliseconds()})
		cancel()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	if !p.leaderExited() {
		if p.waitGone(min(time.Second, max(150*time.Millisecond, p.shutdownTimeout/2))) {
			return nil
		}
		p.signalPGID(syscall.SIGTERM)
		if p.waitGone(max(150*time.Millisecond, p.shutdownTimeout)) {
			return nil
		}
	}
	if p.killAndReap() {
		return nil
	}
	return InfraErrorf("timed out waiting for proxy pgid %d exit after SIGKILL", p.pgid)
}

func (p *ProxyProcess) killAndReap() bool {
	p.signalPGID(syscall.SIGKILL)
	if p.waitGone(processWaitDelay) {
		return true
	}
	if p.stdoutPipe != nil {
		_ = p.stdoutPipe.Close()
	}
	return p.waitGone(processWaitDelay + 250*time.Millisecond)
}
func (p *ProxyProcess) leaderExited() bool {
	select {
	case <-p.waitDone:
		return true
	default:
		return p.waitDone == nil
	}
}
func (p *ProxyProcess) pgidGone() bool {
	return p.pgid <= 0 || errors.Is(syscall.Kill(-p.pgid, 0), syscall.ESRCH)
}
func (p *ProxyProcess) signalPGID(sig syscall.Signal) {
	if sig != 0 && p.pgid > 0 && !p.pgidGone() {
		_ = syscall.Kill(-p.pgid, sig)
	}
}
func (p *ProxyProcess) waitGone(d time.Duration) bool {
	done := func() bool { return p.leaderExited() && p.pgidGone() }
	if done() || d <= 0 {
		return done()
	}
	deadline := time.Now().Add(d)
	if !p.leaderExited() {
		t := time.NewTimer(d)
		select {
		case <-p.waitDone:
			t.Stop()
		case <-t.C:
			return done()
		}
	}
	for rem := time.Until(deadline); !done() && rem > 0; rem = time.Until(deadline) {
		time.Sleep(min(rem, 10*time.Millisecond))
	}
	return done()
}
func buildProxyEnv(base []string, emulatorHost string) []string {
	if base == nil {
		base = os.Environ()
	}
	out := slices.DeleteFunc(slices.Clone(base), func(s string) bool { return strings.HasPrefix(s, "PUBSUB_EMULATOR_HOST=") })
	if emulatorHost != "" {
		out = append(out, "PUBSUB_EMULATOR_HOST="+emulatorHost)
	}
	return out
}

type cappedBuffer struct {
	mu                    sync.Mutex
	max, headCap, tailCap int
	head, tail            []byte
	dropped               int64
}

func newCappedBuffer(maxBytes int) *cappedBuffer {
	m := max(128, maxBytes)
	return &cappedBuffer{max: m, headCap: m / 4, tailCap: max(16, m-m/4-64)}
}
func (b *cappedBuffer) Write(p []byte) (int, error) {
	origLen := len(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dropped == 0 && len(b.head)+origLen <= b.max {
		b.head = append(b.head, p...)
		return origLen, nil
	}
	if b.dropped == 0 {
		if len(b.head) < b.headCap {
			take := min(b.headCap-len(b.head), origLen)
			b.head, p = append(b.head, p[:take]...), p[take:]
		} else if len(b.head) > b.headCap {
			overflow := b.head[b.headCap:]
			b.head = append([]byte(nil), b.head[:b.headCap]...)
			b.appendTail(overflow)
		}
	}
	b.appendTail(p)
	return origLen, nil
}

func (b *cappedBuffer) appendTail(p []byte) {
	if len(p) >= b.tailCap {
		b.dropped += int64(len(b.tail) + len(p) - b.tailCap)
		b.tail = append([]byte(nil), p[len(p)-b.tailCap:]...)
	} else if drop := len(b.tail) + len(p) - b.tailCap; drop > 0 {
		b.dropped += int64(drop)
		copy(b.tail, b.tail[drop:])
		b.tail = append(b.tail[:len(b.tail)-drop], p...)
	} else {
		b.tail = append(b.tail, p...)
	}
}

func (b *cappedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dropped == 0 {
		return string(b.head)
	}
	return fmt.Sprintf("%s\n... [truncated %d bytes] ...\n%s", b.head, b.dropped, b.tail)
}
