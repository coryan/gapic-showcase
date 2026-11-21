// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
)

const (
	timeoutUnwindGrace = 200 * time.Millisecond
	livenessPingGrace  = 1 * time.Second
	postCrashWaitGrace = 2 * time.Second
)

type Runner struct {
	cfg       RunConfig
	scenarios []Scenario
	runSeq    atomic.Int64
}

func NewRunner(cfg RunConfig) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	filtered, err := FilterScenarios(cfg.Scenarios, cfg.Filter)
	if err != nil {
		return nil, err
	}
	if cfg.FailOnEmptyFilter && len(filtered) == 0 {
		return nil, fmt.Errorf("filter %q matched 0 scenarios", cfg.Filter)
	}
	cfg.ProjectID = firstNonEmpty(cfg.ProjectID, DefaultProjectID)
	cfg.ScenarioTimeout = firstPos(cfg.ScenarioTimeout, DefaultScenarioTimeout)
	return &Runner{cfg: cfg, scenarios: filtered}, nil
}
func (r *Runner) FilteredScenarios() []Scenario { return slices.Clone(r.scenarios) }
func (r *Runner) RunAll(ctx context.Context) (*Report, error) {
	if err := ValidateTargets(r.cfg.Targets); err != nil {
		return nil, err
	}
	suiteCtx, cancel := withOptionalTimeout(ctx, r.cfg.SuiteTimeout)
	defer cancel()
	reports := make([]TargetReport, 0, len(r.cfg.Targets))
	for _, target := range r.cfg.Targets {
		if err := suiteCtxErr(suiteCtx); err != nil {
			reports = append(reports, TargetReport{Target: target.Name, Command: target.CommandString(), TimedOut: true, TargetError: fmt.Sprintf("suite timed out before starting target: %v", err), Summary: &Summary{}, Scenarios: []ScenarioResult{}})
			continue
		}
		if rep, _ := r.runTargetWithCtx(suiteCtx, target); rep != nil {
			reports = append(reports, *rep)
		}
	}
	return BuildReport(reports)
}
func (r *Runner) RunTarget(ctx context.Context, target TargetSpec) (*TargetReport, error) {
	if err := ValidateTargets([]TargetSpec{target}); err != nil {
		return nil, err
	}
	suiteCtx, cancel := withOptionalTimeout(ctx, r.cfg.SuiteTimeout)
	defer cancel()
	return r.runTargetWithCtx(suiteCtx, target)
}
func deadlineExceeded(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	d, ok := ctx.Deadline()
	return errors.Is(ctx.Err(), context.DeadlineExceeded) || (ok && !d.After(time.Now()))
}
func suiteCtxErr(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if deadlineExceeded(ctx) {
		return context.DeadlineExceeded
	}
	return nil
}
func formatSuiteTimeoutMsg(suiteCtx context.Context, suiteErr error, dur time.Duration) string {
	if errors.Is(suiteErr, context.Canceled) && !deadlineExceeded(suiteCtx) {
		return fmt.Sprintf("TIMEOUT: suite canceled after %s", dur.Round(time.Millisecond))
	}
	return fmt.Sprintf("TIMEOUT: suite deadline exceeded after %s", dur.Round(time.Millisecond))
}
func withOptionalTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if d > 0 {
		return context.WithTimeout(ctx, d)
	}
	return ctx, func() {}
}

type activeProxy struct {
	handle  ProxyHandle
	mu      sync.Mutex
	exited  bool
	exitErr error
}

func (a *activeProxy) observeExit(wait time.Duration) (bool, error) {
	if a == nil || a.handle == nil {
		return false, nil
	}
	a.mu.Lock()
	if a.exited {
		err := a.exitErr
		a.mu.Unlock()
		return true, err
	}
	a.mu.Unlock()
	var err error
	ok := false
	select {
	case err, ok = <-a.handle.Exited():
	default:
		if wait <= 0 {
			return false, nil
		}
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case err, ok = <-a.handle.Exited():
		case <-t.C:
			return false, nil
		}
	}
	a.mu.Lock()
	if !a.exited {
		a.exited, a.exitErr = true, err
		if pp, isProc := a.handle.(*ProxyProcess); !ok && isProc {
			if waitErr, _, done := pp.ExitInfo(); done {
				a.exitErr = waitErr
			}
		}
	}
	exitErr := a.exitErr
	a.mu.Unlock()
	return true, exitErr
}
func (a *activeProxy) stderr() string {
	if a == nil || a.handle == nil {
		return ""
	}
	return strings.TrimSpace(a.handle.Stderr())
}
func closeActiveProxy(a *activeProxy) {
	if a != nil && a.handle != nil {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout+2*time.Second)
		defer cancel()
		_ = a.handle.Close(ctx)
	}
}
func (r *Runner) startProxy(ctx context.Context, target TargetSpec, emulatorHost string) (*activeProxy, error) {
	if target.Connect != nil {
		h, err := target.Connect(ctx, target)
		if err == nil && h == nil {
			return nil, InfraErrorf("target %q Connect returned nil ProxyHandle", target.Name)
		}
		if err != nil {
			return nil, err
		}
		return &activeProxy{handle: h}, nil
	}
	h, err := StartProxy(ctx, target, ProxyOptions{EmulatorHost: emulatorHost, StartupTimeout: target.StartupTimeout, ShutdownTimeout: target.ShutdownTimeout})
	if err != nil {
		return nil, err
	}
	return &activeProxy{handle: h}, nil
}

func (r *Runner) runTargetWithCtx(suiteCtx context.Context, target TargetSpec) (*TargetReport, error) {
	rep := &TargetReport{Target: target.Name, Command: target.CommandString(), Summary: &Summary{}, Scenarios: make([]ScenarioResult, 0, len(r.scenarios))}
	if err := suiteCtxErr(suiteCtx); err != nil {
		rep.TimedOut, rep.TargetError = true, fmt.Sprintf("suite timed out before starting target: %v", err)
		return rep, err
	}
	baseFakeOpts := append([]fake.Option{fake.WithMaxRecvMsgSize(defaultMaxGRPCMsgSize), fake.WithMaxSendMsgSize(defaultMaxGRPCMsgSize)}, r.cfg.FakeServerOptions...)
	sharedFake, err := fake.NewServer(baseFakeOpts...)
	if err != nil {
		rep.TargetError = fmt.Sprintf("failed to start fake Pub/Sub server: %v", err)
		return rep, err
	}
	defer func() { _ = sharedFake.Close() }()
	var sharedProxy *activeProxy
	defer func() { closeActiveProxy(sharedProxy) }()
	initialProxy, err := r.startProxy(suiteCtx, target, "")
	if err != nil {
		rep.TargetError, rep.TimedOut = err.Error(), suiteCtxErr(suiteCtx) != nil
		return rep, err
	}
	caps := initialProxy.handle.Capabilities()
	rep.Capabilities, sharedProxy = CapabilitiesFromProto(caps), initialProxy
	if len(r.scenarios) == 0 || r.cfg.IsolateProxyPerScenario {
		closeActiveProxy(sharedProxy)
		sharedProxy = nil
	}
	proxyRestartPending := false
	for _, sc := range r.scenarios {
		if err := suiteCtxErr(suiteCtx); err != nil {
			rep.TimedOut, rep.TargetError = true, firstNonEmpty(rep.TargetError, fmt.Sprintf("suite timed out: %v", err))
			break
		}
		if skipped, ok := checkPreRunGate(sc, caps); ok {
			r.appendOutcome(rep, skipped)
			continue
		}
		outcome, suiteTimedOut, crashed := r.runScenario(suiteCtx, target, sc, caps, baseFakeOpts, sharedFake, &sharedProxy, &proxyRestartPending)
		r.appendOutcome(rep, outcome)
		if suiteTimedOut {
			rep.TimedOut, rep.TargetError = true, firstNonEmpty(rep.TargetError, fmt.Sprintf("suite timed out during scenario %s", sc.ID))
			break
		}
		if crashed && r.cfg.StopOnProxyCrash {
			rep.TargetError = firstNonEmpty(rep.TargetError, fmt.Sprintf("stopped after proxy error in %s: %s", sc.ID, outcome.ErrorMessage))
			break
		}
	}
	rep.Summary = ComputeSummary(rep.Scenarios)
	return rep, nil
}
func checkPreRunGate(sc Scenario, caps *proxypb.GetCapabilitiesResponse) (ScenarioOutcome, bool) {
	out := ScenarioOutcome{ID: sc.ID, Name: sc.Name, Category: sc.Category, Feature: sc.Feature}
	if missing := MissingCapabilities(caps, sc.RequiredCapabilities); len(missing) > 0 {
		out.Status, out.Reason = StatusUnsupportedByClient, "missing required capabilities: "+strings.ReplaceAll(strings.Trim(fmt.Sprint(missing), "[]"), " ", ", ")
		return out, true
	}
	if sc.CheckCapabilities != nil {
		if ok, reason := sc.CheckCapabilities(caps); !ok {
			out.Status, out.Reason = StatusUnsupportedByClient, firstNonEmpty(reason, "capability check returned unsupported")
			return out, true
		}
	}
	if sc.TraceOnly && !caps.GetSupportsMonitoring() {
		out.Status, out.TraceStatus, out.Reason = StatusSkippedNoMonitoring, traces.StatusSkippedNoMonitoring, "client reports supports_monitoring=false"
		return out, true
	}
	return out, false
}
func (r *Runner) appendOutcome(rep *TargetReport, outcome ScenarioOutcome) {
	res := outcome.toResult()
	rep.Scenarios = append(rep.Scenarios, res)
	rep.Summary = ComputeSummary(rep.Scenarios)
	if r.cfg.ProgressWriter != nil {
		fmt.Fprintf(r.cfg.ProgressWriter, "[%s] %s: %s (%s)\n", rep.Target, res.ID, outcome.Status, res.Duration)
	}
}
func (r *Runner) runScenario(suiteCtx context.Context, target TargetSpec, sc Scenario, cachedCaps *proxypb.GetCapabilitiesResponse, baseFakeOpts []fake.Option, sharedFake *fake.Server, sharedProxy **activeProxy, proxyRestartPending *bool) (outcome ScenarioOutcome, suiteTimedOut, infraCrash bool) {
	start := time.Now()
	outcome = ScenarioOutcome{ID: sc.ID, Name: sc.Name, Category: sc.Category, Feature: sc.Feature, ProxyRestarted: *proxyRestartPending}
	*proxyRestartPending = false
	var ephemeralFake *fake.Server
	scenFake := sharedFake
	if len(sc.FakeServerOptions) > 0 {
		var err error
		if ephemeralFake, err = fake.NewServer(append(slices.Clone(baseFakeOpts), sc.FakeServerOptions...)...); err != nil {
			outcome.Status, outcome.ErrorMessage, outcome.Duration = StatusError, fmt.Sprintf("ERROR: starting fake server: %v", err), time.Since(start)
			return outcome, false, true
		}
		scenFake = ephemeralFake
	}
	defer resetOrCloseFake(ephemeralFake, sharedFake)
	needIsolated := sc.UseEmulatorEnv || sc.IsolateProxy || r.cfg.IsolateProxyPerScenario
	scenProxy, startErr, timedOutStart := r.acquireScenarioProxy(suiteCtx, target, sc, scenFake.Addr(), needIsolated, sharedProxy, &outcome)
	if startErr != nil {
		outcome.Duration, outcome.Status, outcome.ErrorMessage = time.Since(start), StatusError, fmt.Sprintf("ERROR: starting proxy: %v", startErr)
		if timedOutStart {
			outcome.Status, outcome.ErrorMessage = StatusTimeout, fmt.Sprintf("TIMEOUT: starting proxy: %v", startErr)
		}
		return outcome, timedOutStart, !timedOutStart
	}
	if needIsolated {
		defer closeActiveProxy(scenProxy)
	}
	activeCaps := scenProxy.handle.Capabilities()
	if activeCaps == nil {
		activeCaps = cachedCaps
	}
	scTimeout := firstPos(sc.Timeout, r.cfg.ScenarioTimeout, DefaultScenarioTimeout)
	scCtx, scCancel := context.WithTimeout(suiteCtx, scTimeout)
	defer scCancel()
	failDirty := func(st Status, msg string, err error, suiteTO, crash bool) (ScenarioOutcome, bool, bool) {
		outcome.Status, outcome.ErrorMessage, outcome.Err = st, msg, err
		if !needIsolated && *sharedProxy != nil {
			closeActiveProxy(*sharedProxy)
			*sharedProxy, *proxyRestartPending = nil, true
		}
		return outcome, suiteTO, crash
	}
	suiteMsgOr := func(suiteErr error, fallback string) (string, bool) {
		if suiteErr != nil {
			return formatSuiteTimeoutMsg(suiteCtx, suiteErr, outcome.Duration), true
		}
		return fallback, false
	}
	if err := traces.ResetTracesIfSupported(scCtx, scenProxy.handle.Client(), activeCaps); err != nil {
		suiteErr, scTimedOut := suiteCtxErr(suiteCtx), deadlineExceeded(scCtx)
		outcome.Duration = time.Since(start)
		if suiteErr != nil || scTimedOut {
			msg, st := suiteMsgOr(suiteErr, fmt.Sprintf("TIMEOUT: ResetTraces exceeded deadline: %v", err))
			return failDirty(StatusTimeout, msg, nil, st, false)
		}
		return failDirty(StatusError, formatCrashMessage(err, scenProxy.stderr(), nil), nil, false, true)
	}
	env := newScenarioEnv(sc, scenProxy.handle.Client(), scenFake, activeCaps, r.cfg.ProjectID, fmt.Sprintf("r%d", r.runSeq.Add(1)))
	ro, timedOut := executeScenarioRun(scCtx, sc, env, scenProxy, ephemeralFake, sharedFake)
	outcome.Duration = time.Since(start)
	envFailed, failMsgs, diags, devs, traceStatus, lastTrace, traceFailed, traceErrMsg, allSkipped := env.snapshotState()
	outcome.Diagnostics, outcome.Deviations, outcome.TraceStatus, outcome.TraceResult = diags, devs, traceStatus, lastTrace
	if suiteErr := suiteCtxErr(suiteCtx); timedOut || suiteErr != nil || errors.Is(scCtx.Err(), context.DeadlineExceeded) || (ro.err != nil && deadlineExceeded(scCtx)) {
		msg, st := suiteMsgOr(suiteErr, fmt.Sprintf("TIMEOUT: scenario exceeded timeout (%s)", scTimeout))
		return failDirty(StatusTimeout, msg, nil, st, false)
	}
	if ro.panicVal != nil {
		return failDirty(StatusError, fmt.Sprintf("ERROR: scenario panic: %v", ro.panicVal), InfraErrorf("panic: %v", ro.panicVal), false, true)
	}
	if errors.Is(ro.err, ErrProxyInfra) {
		return failDirty(StatusError, formatCrashMessage(nil, scenProxy.stderr(), ro.err), ro.err, false, true)
	}
	if crashed, crashMsg := detectProxyCrash(scenProxy, ro.err, envFailed); crashed {
		return failDirty(StatusError, crashMsg, InfraErrorf("%s", crashMsg), false, true)
	}
	switch {
	case IsUnimplemented(ro.err) && !envFailed && !traceFailed:
		outcome.Status, outcome.Reason, outcome.Err = StatusUnsupportedByClient, ro.err.Error(), ro.err
	case ro.err != nil || envFailed || traceFailed:
		outcome.Status, outcome.Err, outcome.ErrorMessage = StatusFail, ro.err, firstNonEmpty(strings.Join(failMsgs, "; "), traceErrMsg)
		if ro.err != nil && (!IsUnimplemented(ro.err) || outcome.ErrorMessage == "") {
			outcome.ErrorMessage = ro.err.Error()
		}
		outcome.ErrorMessage = firstNonEmpty(outcome.ErrorMessage, "scenario assertion failed")
	case sc.TraceOnly && (!activeCaps.GetSupportsMonitoring() || allSkipped):
		outcome.Status, outcome.Reason, outcome.TraceStatus = StatusSkippedNoMonitoring, "client reports supports_monitoring=false", traces.StatusSkippedNoMonitoring
	default:
		outcome.Status = StatusPass
	}
	return outcome, false, false
}

func resetOrCloseFake(ephemeral, shared *fake.Server) {
	if ephemeral != nil {
		_ = ephemeral.Close()
	} else if shared != nil {
		shared.Reset()
	}
}

type runOutcome struct {
	err      error
	panicVal any
}

func executeScenarioRun(scCtx context.Context, sc Scenario, env *ScenarioEnv, scenProxy *activeProxy, ephemeralFake, sharedFake *fake.Server) (runOutcome, bool) {
	if sc.Run == nil {
		return runOutcome{}, false
	}
	runDone := make(chan runOutcome, 1)
	go func() {
		var ro runOutcome
		defer func() {
			if pv := recover(); pv != nil {
				ro.panicVal = pv
			}
			runDone <- ro
		}()
		ro.err = sc.Run(scCtx, env)
	}()
	select {
	case ro := <-runDone:
		return ro, false
	case <-scCtx.Done():
		select {
		case ro := <-runDone:
			return ro, true
		case <-time.After(timeoutUnwindGrace):
			closeActiveProxy(scenProxy)
			resetOrCloseFake(ephemeralFake, sharedFake)
			return <-runDone, true
		}
	}
}
func detectProxyCrash(scenProxy *activeProxy, runErr error, envFailed bool) (bool, string) {
	if exited, exitErr := scenProxy.observeExit(0); exited {
		return true, formatCrashMessage(exitErr, scenProxy.stderr(), runErr)
	}
	if (runErr != nil || envFailed) && !IsUnimplemented(runErr) {
		pingCtx, cancel := context.WithTimeout(context.Background(), livenessPingGrace)
		_, pingErr := scenProxy.handle.Client().GetCapabilities(pingCtx, &proxypb.GetCapabilitiesRequest{})
		cancel()
		if pingErr != nil {
			if exited, exitErr := scenProxy.observeExit(postCrashWaitGrace); exited {
				pingErr = exitErr
			}
			return true, formatCrashMessage(pingErr, scenProxy.stderr(), runErr)
		}
	}
	return false, ""
}
func (r *Runner) acquireScenarioProxy(suiteCtx context.Context, target TargetSpec, sc Scenario, fakeAddr string, needIsolated bool, sharedProxy **activeProxy, outcome *ScenarioOutcome) (*activeProxy, error, bool) {
	startChecked := func(emu string) (*activeProxy, error, bool) {
		if err := suiteCtxErr(suiteCtx); err != nil {
			return nil, err, true
		}
		p, err := r.startProxy(suiteCtx, target, emu)
		return p, err, err != nil && suiteCtxErr(suiteCtx) != nil
	}
	if needIsolated {
		closeActiveProxy(*sharedProxy)
		*sharedProxy = nil
		return startChecked(map[bool]string{true: fakeAddr}[sc.UseEmulatorEnv])
	}
	if exited, _ := (*sharedProxy).observeExit(0); exited {
		closeActiveProxy(*sharedProxy)
		*sharedProxy, outcome.ProxyRestarted = nil, true
	}
	if *sharedProxy == nil {
		p, err, to := startChecked("")
		if err != nil {
			return nil, err, to
		}
		*sharedProxy = p
	}
	return *sharedProxy, nil, false
}
func formatCrashMessage(exitErr error, stderr string, runErr error) string {
	parts := []string{"ERROR: proxy exited or became unresponsive mid-scenario"}
	if exitErr != nil {
		parts = append(parts, fmt.Sprintf("exit=%v", exitErr))
	}
	if runErr != nil {
		parts = append(parts, fmt.Sprintf("rpc_err=%v", runErr))
	}
	if len(stderr) > 1024 {
		stderr = "..." + stderr[len(stderr)-1024:]
	}
	if stderr != "" {
		parts = append(parts, fmt.Sprintf("stderr=%q", stderr))
	}
	return strings.Join(parts, "; ")
}
