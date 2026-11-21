// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type helperProxyServer struct {
	proxypb.UnimplementedPubSubProxyServer
	mode   string
	stopCh chan struct{}
	once   sync.Once
	grpc   *grpc.Server
	pubSeq int
}

func (s *helperProxyServer) GetCapabilities(context.Context, *proxypb.GetCapabilitiesRequest) (*proxypb.GetCapabilitiesResponse, error) {
	switch s.mode {
	case "hang-caps":
		<-s.stopCh
		return nil, status.Error(codes.Canceled, "stopped")
	case "caps-error-hang":
		if path := os.Getenv("HELPER_CAPS_CALLED_FILE"); path != "" {
			tmp := path + ".tmp"
			if os.WriteFile(tmp, []byte("called\n"), 0o600) == nil {
				_ = os.Rename(tmp, path)
			}
		}
		return nil, status.Error(codes.Internal, "caps boom")
	}
	return &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", ClientLibrary: "helper-proxy", ClientVersion: "0.1.0", SupportsMonitoring: true, SupportsPublishFlowControl: true}, nil
}
func (s *helperProxyServer) ResetTraces(context.Context, *proxypb.ResetTracesRequest) (*proxypb.ResetTracesResponse, error) {
	return &proxypb.ResetTracesResponse{}, nil
}
func (s *helperProxyServer) DrainTraces(context.Context, *proxypb.DrainTracesRequest) (*proxypb.DrainTracesResponse, error) {
	return &proxypb.DrainTracesResponse{}, nil
}
func (s *helperProxyServer) Publish(context.Context, *proxypb.PublishRequest) (*proxypb.PublishResponse, error) {
	return &proxypb.PublishResponse{MessageId: "m1"}, nil
}
func (s *helperProxyServer) CreatePublisher(context.Context, *proxypb.CreatePublisherRequest) (*proxypb.CreatePublisherResponse, error) {
	switch s.mode {
	case "crash-mid-scenario":
		fmt.Fprintln(os.Stderr, "helper crash-mid-scenario")
		os.Exit(2)
	case "hang-scenario":
		<-s.stopCh
		return nil, status.Error(codes.Canceled, "stopped")
	}
	s.pubSeq++
	return &proxypb.CreatePublisherResponse{PublisherId: fmt.Sprintf("p%d", s.pubSeq)}, nil
}
func (s *helperProxyServer) ShutdownProxy(context.Context, *proxypb.ShutdownProxyRequest) (*proxypb.ShutdownProxyResponse, error) {
	if s.mode != "ignore-shutdown" && !strings.HasPrefix(s.mode, "ignore-sigterm") {
		go func() { time.Sleep(15 * time.Millisecond); s.once.Do(func() { close(s.stopCh); s.grpc.Stop() }) }()
	}
	return &proxypb.ShutdownProxyResponse{}, nil
}
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	idx := slices.Index(os.Args, "--")
	if idx < 0 {
		os.Exit(2)
	}
	fs := flag.NewFlagSet("helper", flag.ExitOnError)
	port, mode, spawnChild := fs.Int("port", 0, ""), fs.String("mode", "normal", ""), fs.Bool("spawn-child", false, "")
	_ = fs.Parse(os.Args[idx+1:])
	startSleep := func(ign bool) int {
		if ign {
			signal.Ignore(syscall.SIGTERM)
		}
		c := exec.Command("sleep", "60")
		if c.Start() != nil {
			os.Exit(4)
		}
		return c.Process.Pid
	}
	appendEnv := func(env, s string) {
		if f, err := os.OpenFile(os.Getenv(env), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			_, _ = fmt.Fprint(f, s)
			_ = f.Close()
		}
	}
	if *mode == "child-ignore-sigterm" {
		fmt.Printf("%d %d\n", os.Getpid(), startSleep(true))
		_ = os.Stdout.Sync()
		select {}
	}
	appendEnv("HELPER_LOG_FILE", fmt.Sprintf("pid=%d emu=%q\n", os.Getpid(), os.Getenv("PUBSUB_EMULATOR_HOST")))
	if *spawnChild {
		ch := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$", "--", "--mode=child-ignore-sigterm")
		op, err := ch.StdoutPipe()
		var p1, p2 int
		if err != nil || ch.Start() != nil {
			os.Exit(4)
		}
		_, err = fmt.Fscan(op, &p1, &p2)
		_ = op.Close()
		if err != nil || p1 <= 0 || p2 <= 0 {
			os.Exit(4)
		}
		appendEnv("HELPER_CHILD_PID_FILE", fmt.Sprintf("%d\n%d\n", p1, p2))
	}
	if out, ok := map[string]string{"bad-ready": "READY not-a-host-port\n", "bare-ready": "READY\n", "silent-exit": "", "long-ready-line": strings.Repeat("X", 9000) + "\n", "spam-stdout": strings.Repeat("noise\n", 80)}[*mode]; ok {
		fmt.Print(out)
		os.Exit(0)
	}
	host := "127.0.0.1"
	switch *mode {
	case "crash-startup":
		fmt.Fprintln(os.Stderr, "fatal startup crash")
		os.Exit(2)
	case "bad-ready-hang", "hang-before-ready":
		if *mode == "bad-ready-hang" {
			fmt.Println("READY not-a-host-port")
			_ = os.Stdout.Sync()
		}
		select {}
	case "noisy-pre-ready":
		fmt.Print(strings.Repeat("boot\n", 10))
	case "spam-stderr":
		fmt.Fprintln(os.Stderr, "HEAD_MARKER:"+strings.Repeat("A", 200*1024)+":TAIL_MARKER")
	case "ignore-sigterm-with-child", "hang-caps":
		appendEnv("HELPER_CHILD_PID_FILE", fmt.Sprintf("%d\n", startSleep(*mode == "ignore-sigterm-with-child")))
	case "ready-ipv6-prelude":
		fmt.Println("READY_PRELUDE=initializing")
		host = "[::1]"
	}
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, *port))
	if err != nil {
		os.Exit(3)
	}
	if *mode == "crash-after-ready-delayed" || *mode == "crash-after-ready-delayed-325ms" {
		delay := 25 * time.Millisecond
		if *mode == "crash-after-ready-delayed-325ms" {
			delay = 325 * time.Millisecond
		}
		fmt.Printf("READY %s\n", lis.Addr().String())
		_ = os.Stdout.Sync()
		_ = lis.Close()
		time.Sleep(delay)
		fmt.Fprintln(os.Stderr, "delayed crash after ready")
		os.Exit(2)
	}
	srv := &helperProxyServer{mode: *mode, stopCh: make(chan struct{}), grpc: grpc.NewServer()}
	proxypb.RegisterPubSubProxyServer(srv.grpc, srv)
	go func() { _ = srv.grpc.Serve(lis) }()
	fmt.Printf("READY %s\n", lis.Addr().String())
	_ = os.Stdout.Sync()
	if *mode == "crash-after-ready" {
		_ = lis.Close()
		fmt.Fprintln(os.Stderr, "crashed after ready")
		os.Exit(2)
	}
	<-srv.stopCh
	os.Exit(0)
}

func helperSpec(t *testing.T, name, mode string, extraArgs ...string) TargetSpec {
	t.Helper()
	exe, err := os.Executable()
	must(t, err)
	return TargetSpec{Name: name, Command: exe, Args: append([]string{"-test.run=^TestHelperProcess$", "--", "--mode=" + mode}, extraArgs...), Env: []string{"GO_WANT_HELPER_PROCESS=1"}, StartupTimeout: 5 * time.Second, ShutdownTimeout: 450 * time.Millisecond}
}

func envFileSpec(t *testing.T, name, mode, envKey string, extraArgs ...string) (TargetSpec, string) {
	t.Helper()
	sp, path := helperSpec(t, name, mode, extraArgs...), filepath.Join(t.TempDir(), name+".dat")
	sp.Env = append(sp.Env, envKey+"="+path)
	return sp, path
}
func readChildPIDs(path string) []int {
	b, _ := os.ReadFile(path)
	var pids []int
	for _, f := range strings.Fields(string(b)) {
		if p, err := strconv.Atoi(f); err == nil && p > 0 {
			pids = append(pids, p)
		}
	}
	return pids
}
func assertPGIDsGone(t *testing.T, pids ...int) {
	t.Helper()
	for _, pid := range pids {
		gone := pid <= 0
		for dl := time.Now().Add(2 * time.Second); !gone && time.Now().Before(dl); time.Sleep(15 * time.Millisecond) {
			gone = errors.Is(syscall.Kill(-pid, 0), syscall.ESRCH) && errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
		}
		assert(t, gone, "process/pgid %d still alive", pid)
	}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func assert(t *testing.T, ok bool, format string, args ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, args...)
	}
}

func TestCommandParsingAndPortInjection(t *testing.T) {
	binPath := filepath.Join(t.TempDir(), "my proxy bin")
	must(t, os.WriteFile(binPath, []byte("#!/bin/sh\n"), 0o755))
	cmd, args, err := ParseCommandSpec(binPath, []string{"--v"})
	ts, tsErr := ParseTargetSpec(`go=proxy --mode="a b" --extra='c=d' --flag=x\ y`)
	got, utfErr := SplitCommandLine("proxy --label=voilà --other=\"café\" --sym=Å\\→")
	dqEsc, dqErr := SplitCommandLine(`"a \"b\" \\ \$ \` + "`" + ` \n"`)
	assert(t, err == nil && cmd == binPath && slices.Equal(args, []string{"--v"}) && tsErr == nil && ts.Name == "go" && ts.Command == "proxy" && slices.Equal(ts.Args, []string{"--mode=a b", "--extra=c=d", "--flag=x y"}) && utfErr == nil && slices.Equal(got, []string{"proxy", "--label=voilà", "--other=café", "--sym=Å→"}) && dqErr == nil && slices.Equal(dqEsc, []string{`a "b" \ $ ` + "`" + ` \n`}), "command/UTF-8/dq-escape parse mismatch: dqEsc=%v dqErr=%v", dqEsc, dqErr)
	for _, badSplit := range []string{"   ", `"unterminated \"` + "\\"} {
		_, sErr := SplitCommandLine(badSplit)
		assert(t, sErr != nil, "SplitCommandLine(%q) succeeded", badSplit)
	}
	assert(t, (TargetSpec{Name: "inproc", Connect: func(context.Context, TargetSpec) (ProxyHandle, error) { return nil, nil }}).CommandString() == "<in-process:inproc>" &&
		(TargetSpec{Command: "p", Args: []string{"a b", ""}}).CommandString() == `p "a b" ""` && (TargetSpec{}).CommandString() == "" &&
		slices.Equal(EnsurePortFlag([]string{"--port-offset=1"}), []string{"--port-offset=1", "--port=0"}), "CommandString/port mismatch")
	for _, bad := range []string{"noequals", "=cmd", "go=", `go="unterminated`, `go=trailing\`} {
		_, bErr := ParseTargetSpec(bad)
		assert(t, bErr != nil, "ParseTargetSpec(%q) succeeded", bad)
	}
	assert(t, !HasPortFlag([]string{"--port-offset=1"}) && !slices.ContainsFunc([][]string{{"--port=50051"}, {"--port", "50051"}, {"-port", "50051"}, {"-port=0"}, {"cargo", "run", "--", "--port=9000"}}, func(ex []string) bool { return !HasPortFlag(ex) || !slices.Equal(EnsurePortFlag(ex), ex) }), "HasPortFlag/EnsurePortFlag check failed")
	paths, pErr := ParseCompareArgs([]string{"a.json,b.json", "c.json"})
	_, badCmp := ParseCompareArgs([]string{"a.json,"})
	assert(t, ValidateTargets([]TargetSpec{{Name: "a", Command: "x"}, {Name: "a", Command: "y"}}) != nil && ValidateTargets(nil) != nil &&
		ValidateTargets([]TargetSpec{{Name: "a", Command: "x", StartupTimeout: -1}}) != nil && (RunConfig{Filter: "["}).Validate() != nil && (RunConfig{ScenarioTimeout: -1}).Validate() != nil &&
		pErr == nil && slices.Equal(paths, []string{"a.json", "b.json", "c.json"}) && badCmp != nil, "validation/ParseCompareArgs check failed")
}
func TestProxyLifecycle_StartupTeardownAndBuffers(t *testing.T) {
	p, err := StartProxy(context.Background(), helperSpec(t, "ok", "normal"), ProxyOptions{})
	must(t, err)
	assert(t, p.Capabilities().GetClientLibrary() == "helper-proxy", "caps=%v", p.Capabilities())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { must(t, p.Stop()) })
	}
	wg.Wait()
	<-p.WaitDone()
	assertPGIDsGone(t, p.PID())
	for _, tc := range []struct {
		mode, wantSub string
		timeout       time.Duration
		trials        int
		args          []string
	}{
		{"crash-startup", "fatal startup crash", 2 * time.Second, 1, []string{"--spawn-child"}}, {"bad-ready", "malformed READY", 2 * time.Second, 1, []string{"--spawn-child"}}, {"bare-ready", "malformed READY", 2 * time.Second, 1, []string{"--spawn-child"}},
		{"bad-ready-hang", "malformed READY", 2 * time.Second, 1, []string{"--spawn-child"}}, {"silent-exit", "before READY", 2 * time.Second, 1, []string{"--spawn-child"}},
		{"long-ready-line", "exceeded", 2 * time.Second, 1, nil}, {"spam-stdout", "pre-READY stdout lines", 2 * time.Second, 1, nil},
		{"hang-before-ready", "timed out waiting for READY", 400 * time.Millisecond, 1, []string{"--spawn-child"}}, {"crash-after-ready", "GetCapabilities", 2 * time.Second, 5, []string{"--spawn-child"}},
		{"hang-caps", "GetCapabilities", 400 * time.Millisecond, 1, []string{"--spawn-child"}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			for range tc.trials {
				spec, childFile := envFileSpec(t, tc.mode, tc.mode, "HELPER_CHILD_PID_FILE", tc.args...)
				spec.StartupTimeout = tc.timeout
				start := time.Now()
				_, err := StartProxy(context.Background(), spec, ProxyOptions{})
				cpids := readChildPIDs(childFile)
				subOK := err != nil && (strings.Contains(err.Error(), tc.wantSub) || (tc.mode == "crash-after-ready" && strings.Contains(err.Error(), "exited immediately after READY")))
				assert(t, subOK && (tc.mode != "bad-ready-hang" || time.Since(start) <= time.Second) && (len(tc.args) == 0 || len(cpids) >= 2), "StartProxy(%s) err=%v dt=%v cpids=%v", tc.mode, err, time.Since(start), cpids)
				assertPGIDsGone(t, cpids...)
			}
		})
	}
	cCtx, cCancel := context.WithCancel(context.Background())
	cspec, ccf := envFileSpec(t, "cancel-start", "hang-before-ready", "HELPER_CHILD_PID_FILE", "--spawn-child")
	go func() {
		for dl := time.Now().Add(3 * time.Second); len(readChildPIDs(ccf)) < 2 && time.Now().Before(dl); time.Sleep(5 * time.Millisecond) {
		}
		cCancel()
	}()
	_, cErr := StartProxy(cCtx, cspec, ProxyOptions{})
	cPids := readChildPIDs(ccf)
	assert(t, cErr != nil && len(cPids) == 2, "cancel-startup err=%v cpids=%v", cErr, cPids)
	assertPGIDsGone(t, cPids...)
	_, missErr := StartProxy(context.Background(), TargetSpec{Name: "missing", Command: "/nonexistent/binary/xyz"}, ProxyOptions{})
	assert(t, missErr != nil, "expected missing binary error")
	for _, mode := range []string{"noisy-pre-ready", "spam-stderr", "ready-ipv6-prelude"} {
		sp, err := StartProxy(context.Background(), helperSpec(t, mode, mode), ProxyOptions{MaxOutputBytes: 4096})
		must(t, err)
		got := sp.Stderr()
		assert(t, (mode != "spam-stderr" || (len(got) <= 4096 && strings.Contains(got, "HEAD_MARKER:") && strings.Contains(got, ":TAIL_MARKER") && strings.Contains(got, "truncated"))) &&
			(mode != "ready-ipv6-prelude" || (strings.HasPrefix(sp.Addr(), "[::1]:") && strings.Contains(sp.Stdout(), "READY_PRELUDE=initializing"))), "%s buffer/ready mismatch", mode)
		_ = sp.Stop()
		assertPGIDsGone(t, sp.PID())
	}
	cb := newCappedBuffer(128)
	for i := range 25 {
		_, _ = fmt.Fprintf(cb, "chunk-%02d--", i)
	}
	s := cb.String()
	assert(t, len(s) <= 128 && strings.Contains(s, "chunk-00--") && strings.HasSuffix(s, "chunk-24--") && strings.Contains(s, "truncated"), "small-chunk cappedBuffer invalid: %q", s)
	cbTail := newCappedBuffer(128)
	_, _ = cbTail.Write([]byte(strings.Repeat("H", 40)))
	_, _ = cbTail.Write([]byte(strings.Repeat("T", 90)))
	sTail := cbTail.String()
	assert(t, strings.Contains(sTail, "truncated") && strings.HasSuffix(sTail, strings.Repeat("T", 32)), "overflow tail edge invalid: %q", sTail)

	envEdge := newScenarioEnv(Scenario{ID: "E"}, nil, nil, nil, "p", "r")
	assert(t, envEdge.Endpoint() == "" && envEdge.TopicPath("projects/x/topics/t") == "projects/x/topics/t" && envEdge.SubscriptionPath("projects/x/subscriptions/s") == "projects/x/subscriptions/s" && envEdge.Defaults() != nil && envEdge.RequireCapability(CapUnaryPull) != nil && envEdge.CheckProxyErr(errors.New("plain")) != nil, "ScenarioEnv nil-fake/defaults/check edge mismatch")
	envCap := newScenarioEnv(Scenario{ID: "E2"}, nil, nil, &proxypb.GetCapabilitiesResponse{SupportsUnaryPull: true}, "p", "r")
	assert(t, envCap.RequireCapability(CapUnaryPull) == nil && envCap.CheckProxyErr(nil) == nil, "ScenarioEnv RequireCapability pass mismatch")
	verifyStop := func(mode string, shutTO time.Duration, extra []string, act func(*ProxyProcess)) (time.Duration, []int) {
		spec, cf := envFileSpec(t, mode, mode, "HELPER_CHILD_PID_FILE", extra...)
		spec.ShutdownTimeout = shutTO
		sp, err := StartProxy(context.Background(), spec, ProxyOptions{})
		must(t, err)
		pid, start := sp.PID(), time.Now()
		act(sp)
		elapsed, cpids := time.Since(start), readChildPIDs(cf)
		assertPGIDsGone(t, append([]int{pid}, cpids...)...)
		return elapsed, cpids
	}
	stopOnly := func(sp *ProxyProcess) { must(t, sp.Stop()) }
	verifyStop("ignore-shutdown", 450*time.Millisecond, nil, stopOnly)
	elapsed, cpids := verifyStop("ignore-sigterm-with-child", 450*time.Millisecond, nil, stopOnly)
	_, mpids := verifyStop("crash-mid-scenario", 450*time.Millisecond, []string{"--spawn-child"}, func(sp *ProxyProcess) {
		_, _ = sp.Client().CreatePublisher(context.Background(), &proxypb.CreatePublisherRequest{Topic: "t"})
		<-sp.WaitDone()
		must(t, sp.Stop())
	})
	cCtx2, cCancel2 := context.WithCancel(context.Background())
	cCancel2()
	_, tpids := verifyStop("normal", 150*time.Millisecond, []string{"--spawn-child"}, func(sp *ProxyProcess) { must(t, sp.Close(cCtx2)) })
	assert(t, elapsed >= 500*time.Millisecond && len(cpids) == 1 && cpids[0] > 0 && len(mpids) == 2 && len(tpids) == 2, "stop/desc mismatch elapsed=%v c=%v m=%v t=%v", elapsed, cpids, mpids, tpids)
}
func testNoop(context.Context, *ScenarioEnv) error { return nil }
func testCreatePub(ctx context.Context, env *ScenarioEnv) error {
	_, err := env.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Topic: "t", Endpoint: env.Endpoint(), UseEmulatorEnv: env.Scenario.UseEmulatorEnv})
	return err
}
func TestRunner_TimeoutsCrashRecoveryAndEmulatorIsolation(t *testing.T) {
	t.Setenv("PUBSUB_EMULATOR_HOST", "127.0.0.1:1")
	okSpec, logFile := envFileSpec(t, "go", "normal", "HELPER_LOG_FILE")
	rep := mustRunTarget(t, RunConfig{Scenarios: []Scenario{{ID: "S1", Run: testCreatePub}, {ID: "S2-EMU", UseEmulatorEnv: true, Run: testCreatePub}, {ID: "S3-POST", Run: testCreatePub}}}, okSpec)
	logBytes, _ := os.ReadFile(logFile)
	lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
	assert(t, rep.Summary.Pass == 3 && len(lines) == 3 && strings.Contains(lines[0], `emu=""`) && strings.Contains(lines[2], `emu=""`) && !strings.Contains(lines[1], `emu=""`) && lines[0] != lines[1] && lines[1] != lines[2], "emulator isolation failed:\n%s", string(logBytes))
	hRep := mustRunTarget(t, RunConfig{ScenarioTimeout: 120 * time.Millisecond, Scenarios: []Scenario{{ID: "H1", Run: testCreatePub}, {ID: "H2-UNWIND", Run: func(_ context.Context, env *ScenarioEnv) error { return testCreatePub(context.Background(), env) }}, {ID: "H3", Run: testNoop}}}, helperSpec(t, "hang", "hang-scenario"))
	sRep := mustRunTarget(t, RunConfig{SuiteTimeout: 250 * time.Millisecond, ScenarioTimeout: 2 * time.Second, Scenarios: []Scenario{{ID: "ST1", Run: func(context.Context, *ScenarioEnv) error { time.Sleep(350 * time.Millisecond); return nil }}, {ID: "ST2", Run: testNoop}}}, helperSpec(t, "s", "normal"))
	cSpec, cChild := envFileSpec(t, "crash", "crash-mid-scenario", "HELPER_CHILD_PID_FILE", "--spawn-child")
	cRep := mustRunTarget(t, RunConfig{Scenarios: []Scenario{{ID: "C1", Run: testCreatePub}, {ID: "C2", Run: testNoop}}}, cSpec)
	sSpec, sChild := envFileSpec(t, "stop", "crash-mid-scenario", "HELPER_CHILD_PID_FILE", "--spawn-child")
	stopRep := mustRunTarget(t, RunConfig{StopOnProxyCrash: true, Scenarios: []Scenario{{ID: "K1", Run: testCreatePub}, {ID: "K2", Run: testNoop}}}, sSpec)
	cPids, sPids := readChildPIDs(cChild), readChildPIDs(sChild)
	assert(t, len(hRep.Scenarios) == 3 && hRep.Scenarios[0].Status == StatusTimeout && hRep.Scenarios[1].Status == StatusTimeout && hRep.Scenarios[2].ProxyRestarted &&
		sRep.TimedOut && sRep.Summary.Total == len(sRep.Scenarios) && ExitCode(&Report{Targets: []TargetReport{*sRep}}, false) != 0 &&
		len(cRep.Scenarios) == 2 && cRep.Scenarios[0].Status == StatusError && strings.Contains(cRep.Scenarios[0].Error, "crash-mid-scenario") && cRep.Scenarios[1].ProxyRestarted &&
		len(stopRep.Scenarios) == 1 && stopRep.TargetError != "" && len(cPids) == 4 && len(sPids) == 2, "timeout/crash/stop-on-crash check failed (cPids=%v sPids=%v)", cPids, sPids)
	assertPGIDsGone(t, append(cPids, sPids...)...)
	isoSpec, isoLog := envFileSpec(t, "iso", "normal", "HELPER_LOG_FILE")
	rep = mustRunTarget(t, RunConfig{IsolateProxyPerScenario: true, Scenarios: []Scenario{{ID: "I1", Run: testCreatePub}, {ID: "I2", Run: testCreatePub}}}, isoSpec)
	b, _ := os.ReadFile(isoLog)
	assert(t, rep.Summary.Pass == 2 && len(strings.Split(strings.TrimSpace(string(b)), "\n")) == 3, "IsolateProxyPerScenario failed: %s", string(b))
}
func TestRunner_ObserveExitZeroDeterministic(t *testing.T) {
	baseSpec := helperSpec(t, "det", "normal")
	for _, pw := range []bool{false, true} {
		wantS1, trials := map[bool]Status{false: StatusError, true: StatusPass}[pw], map[bool]int{false: 30, true: 5}[pw]
		for range trials {
			var p1 *ProxyProcess
			killP1 := func() { _ = syscall.Kill(-p1.PID(), syscall.SIGKILL); <-p1.WaitDone() }
			spec := TargetSpec{Name: "det", Connect: func(ctx context.Context, _ TargetSpec) (ProxyHandle, error) {
				p, err := StartProxy(ctx, baseSpec, ProxyOptions{})
				if err == nil && p1 == nil {
					p1 = p
				}
				return p, err
			}}
			cfg := RunConfig{Scenarios: []Scenario{{ID: "S1", Run: func(ctx context.Context, env *ScenarioEnv) error {
				err := testCreatePub(ctx, env)
				if !pw && err == nil {
					killP1()
				}
				return err
			}}, {ID: "S2", Run: testCreatePub}}}
			if pw {
				once := sync.OnceFunc(killP1)
				cfg.ProgressWriter = writerFunc(func(p []byte) (int, error) { once(); return len(p), nil })
			}
			rep := mustRunTarget(t, cfg, spec)
			assert(t, len(rep.Scenarios) == 2 && rep.Scenarios[0].Status == wantS1 && rep.Scenarios[1].Status == StatusPass && rep.Scenarios[1].ProxyRestarted, "trial pw=%v failed: %+v", pw, rep.Scenarios)
		}
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestProxyLifecycle_FailStartProbeGrace(t *testing.T) {
	t.Run("delayed_exit_preserves_code_and_stderr_with_spawn_child", func(t *testing.T) {
		for range 3 {
			spec, childFile := envFileSpec(t, "delayed-crash", "crash-after-ready-delayed", "HELPER_CHILD_PID_FILE", "--spawn-child")
			spec.StartupTimeout = 2 * time.Second
			_, err := StartProxy(context.Background(), spec, ProxyOptions{})
			cpids := readChildPIDs(childFile)
			assert(t, err != nil &&
				strings.Contains(err.Error(), "GetCapabilities probe failed") &&
				strings.Contains(err.Error(), "exit_code=2") &&
				strings.Contains(err.Error(), "delayed crash after ready") &&
				len(cpids) >= 2,
				"delayed_exit err=%v cpids=%v", err, cpids)
			assertPGIDsGone(t, cpids...)
		}
	})

	t.Run("delayed_325ms_exit_preserves_code_and_stderr_with_spawn_child", func(t *testing.T) {
		for range 2 {
			spec, childFile := envFileSpec(t, "delayed-crash-325ms", "crash-after-ready-delayed-325ms", "HELPER_CHILD_PID_FILE", "--spawn-child")
			spec.StartupTimeout = 3 * time.Second
			_, err := StartProxy(context.Background(), spec, ProxyOptions{})
			cpids := readChildPIDs(childFile)
			assert(t, err != nil &&
				strings.Contains(err.Error(), "GetCapabilities probe failed") &&
				!strings.Contains(err.Error(), "exit_code=-1") &&
				strings.Contains(err.Error(), "exit_code=2") &&
				strings.Contains(err.Error(), "delayed crash after ready") &&
				len(cpids) >= 2,
				"delayed_325ms_exit err=%v cpids=%v", err, cpids)
			assertPGIDsGone(t, cpids...)
		}
	})

	t.Run("caps_error_live_proxy_grace_timeout_reaps_pgid", func(t *testing.T) {
		spec, childFile := envFileSpec(t, "caps-err-grace", "caps-error-hang", "HELPER_CHILD_PID_FILE", "--spawn-child")
		spec.StartupTimeout = 3 * time.Second
		start := time.Now()
		_, err := StartProxy(context.Background(), spec, ProxyOptions{})
		dt := time.Since(start)
		cpids := readChildPIDs(childFile)
		assert(t, err != nil &&
			strings.Contains(err.Error(), "GetCapabilities probe failed") &&
			strings.Contains(err.Error(), "exit_code=-1") &&
			dt >= 500*time.Millisecond && dt < 1500*time.Millisecond &&
			len(cpids) >= 2,
			"caps_error_grace err=%v dt=%v cpids=%v", err, dt, cpids)
		assertPGIDsGone(t, cpids...)
	})

	t.Run("caps_error_ctx_cancel_aborts_grace_wait", func(t *testing.T) {
		spec, childFile := envFileSpec(t, "caps-err-cancel", "caps-error-hang", "HELPER_CHILD_PID_FILE", "--spawn-child")
		capsFile := filepath.Join(t.TempDir(), "caps.called")
		spec.Env = append(spec.Env, "HELPER_CAPS_CALLED_FILE="+capsFile)
		spec.StartupTimeout = 5 * time.Second
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dl := time.Now().Add(4 * time.Second); time.Now().Before(dl); {
				if _, statErr := os.Stat(capsFile); statErr == nil {
					select {
					case <-time.After(25 * time.Millisecond):
						cancel()
					case <-done:
					}
					return
				}
				select {
				case <-time.After(2 * time.Millisecond):
				case <-done:
					return
				}
			}
			cancel()
		}()
		start := time.Now()
		_, err := StartProxy(ctx, spec, ProxyOptions{})
		close(done)
		wg.Wait()
		cpids := readChildPIDs(childFile)
		assert(t, err != nil &&
			strings.Contains(err.Error(), "GetCapabilities probe failed") &&
			strings.Contains(err.Error(), "exit_code=-1") &&
			time.Since(start) < 1500*time.Millisecond &&
			len(cpids) >= 2,
			"caps_error_cancel err=%v dt=%v cpids=%v", err, time.Since(start), cpids)
		assertPGIDsGone(t, cpids...)
	})
}
