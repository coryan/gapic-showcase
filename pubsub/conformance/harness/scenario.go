// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Status string

const (
	StatusPass                Status = "PASS"
	StatusFail                Status = "FAIL"
	StatusSkippedNoMonitoring Status = "SKIPPED_NO_MONITORING"
	StatusUnsupportedByClient Status = "UNSUPPORTED_BY_CLIENT"
	StatusTimeout             Status = "TIMEOUT"
	StatusError               Status = "ERROR"
)

func (s Status) Valid() bool {
	return s == StatusPass || s == StatusFail || s == StatusSkippedNoMonitoring || s == StatusUnsupportedByClient || s == StatusTimeout || s == StatusError
}
func (s Status) Canonical() Status {
	if s == StatusTimeout || s == StatusError {
		return StatusFail
	}
	return s
}
func (s Status) IsPassing() bool { return s == StatusPass || s == StatusSkippedNoMonitoring }
func (s Status) IsFailure(strict bool) bool {
	return s == StatusFail || s == StatusTimeout || s == StatusError || (strict && s == StatusUnsupportedByClient)
}

var (
	ErrUnsupported = errors.New("unsupported by client")
	ErrProxyInfra  = errors.New("proxy infrastructure error")
)

func IsUnimplemented(err error) bool {
	return err != nil && (errors.Is(err, ErrUnsupported) || status.Code(err) == codes.Unimplemented)
}
func Unsupportedf(f string, a ...any) error {
	return fmt.Errorf("%w: %w", ErrUnsupported, fmt.Errorf(f, a...))
}
func InfraErrorf(f string, a ...any) error {
	return fmt.Errorf("%w: %w", ErrProxyInfra, fmt.Errorf(f, a...))
}
func IsInBandUnimplemented(code int32) bool { return code == int32(codes.Unimplemented) }

type Capability string

const (
	CapUnaryPull            Capability = "unary_pull"
	CapBlockingPublish      Capability = "blocking_publish"
	CapPublishFlowControl   Capability = "publish_flow_control"
	CapPublishCompression   Capability = "publish_compression"
	CapPublishHedging       Capability = "publish_hedging"
	CapOrderingOptIn        Capability = "ordering_opt_in"
	CapSubscriberOrdering   Capability = "subscriber_ordering"
	CapKeepalive            Capability = "keepalive"
	CapEodReceiptModack     Capability = "eod_receipt_modack"
	CapAutoNackOnDrop       Capability = "auto_nack_on_drop"
	CapAckWithResult        Capability = "ack_with_result"
	CapCustomRetryCodes     Capability = "custom_retry_codes"
	CapExactlyOnceDelivery  Capability = "exactly_once_delivery"
	CapPerStreamFlowControl Capability = "per_stream_flow_control"
	CapResumePublish        Capability = "resume_publish"
	CapShutdownBehavior     Capability = "shutdown_behavior"
	CapShutdownTimeout      Capability = "shutdown_timeout"
)

var capabilityGetters = map[Capability]func(*proxypb.GetCapabilitiesResponse) bool{
	CapUnaryPull: (*proxypb.GetCapabilitiesResponse).GetSupportsUnaryPull, CapBlockingPublish: (*proxypb.GetCapabilitiesResponse).GetSupportsBlockingPublish, CapPublishFlowControl: (*proxypb.GetCapabilitiesResponse).GetSupportsPublishFlowControl,
	CapPublishCompression: (*proxypb.GetCapabilitiesResponse).GetSupportsPublishCompression, CapPublishHedging: (*proxypb.GetCapabilitiesResponse).GetSupportsPublishHedging, CapOrderingOptIn: (*proxypb.GetCapabilitiesResponse).GetSupportsOrderingOptIn,
	CapSubscriberOrdering: (*proxypb.GetCapabilitiesResponse).GetSupportsSubscriberOrdering, CapKeepalive: (*proxypb.GetCapabilitiesResponse).GetSupportsKeepalive, CapEodReceiptModack: (*proxypb.GetCapabilitiesResponse).GetSupportsEodReceiptModack,
	CapAutoNackOnDrop: (*proxypb.GetCapabilitiesResponse).GetSupportsAutoNackOnDrop, CapAckWithResult: (*proxypb.GetCapabilitiesResponse).GetSupportsAckWithResult, CapCustomRetryCodes: (*proxypb.GetCapabilitiesResponse).GetSupportsCustomRetryCodes,
	CapExactlyOnceDelivery: (*proxypb.GetCapabilitiesResponse).GetSupportsExactlyOnceDelivery, CapPerStreamFlowControl: (*proxypb.GetCapabilitiesResponse).GetSupportsPerStreamFlowControl, CapResumePublish: (*proxypb.GetCapabilitiesResponse).GetSupportsResumePublish,
	CapShutdownBehavior: (*proxypb.GetCapabilitiesResponse).GetSupportsShutdownBehavior, CapShutdownTimeout: (*proxypb.GetCapabilitiesResponse).GetSupportsShutdownTimeout,
}

func HasCapability(caps *proxypb.GetCapabilitiesResponse, cap Capability) bool {
	fn, ok := capabilityGetters[cap]
	return caps != nil && ok && fn(caps)
}
func MissingCapabilities(caps *proxypb.GetCapabilitiesResponse, reqs []Capability) []Capability {
	var missing []Capability
	for _, req := range reqs {
		if !HasCapability(caps, req) {
			missing = append(missing, req)
		}
	}
	return missing
}

type Scenario struct {
	ID, Name, Category, Feature, Description string
	TraceOnly, UseEmulatorEnv, IsolateProxy  bool
	Timeout                                  time.Duration
	RequiredCapabilities                     []Capability
	CheckCapabilities                        func(caps *proxypb.GetCapabilitiesResponse) (supported bool, reason string)
	FakeServerOptions                        []fake.Option
	Run                                      func(ctx context.Context, env *ScenarioEnv) error
}

func FilterScenarios(scenarios []Scenario, pattern string) ([]Scenario, error) {
	if pattern == "" {
		return slices.Clone(scenarios), nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid scenario filter regex %q: %w", pattern, err)
	}
	return slices.DeleteFunc(slices.Clone(scenarios), func(sc Scenario) bool {
		return !re.MatchString(sc.ID) && !re.MatchString(sc.Name) && !re.MatchString(sc.Category)
	}), nil
}

type ScenarioOutcome struct {
	ID, Name, Category, Feature, Reason, ErrorMessage string
	Status                                            Status
	Diagnostics                                       []string
	Duration                                          time.Duration
	TraceStatus                                       traces.Status
	TraceResult                                       *traces.TraceResult
	Deviations                                        []traces.Deviation
	ProxyRestarted                                    bool
	Err                                               error
}

func formatStatusError(st Status, msg string) string {
	if msg == "" || strings.HasPrefix(msg, string(st)+":") {
		return firstNonEmpty(msg, string(st))
	}
	return string(st) + ": " + msg
}
func (o ScenarioOutcome) toResult() ScenarioResult {
	errMsg, detail := o.ErrorMessage, Status("")
	if o.Status.Canonical() != o.Status {
		detail, errMsg = o.Status, formatStatusError(o.Status, errMsg)
	}
	return ScenarioResult{ID: o.ID, Name: o.Name, Category: o.Category, Feature: o.Feature, Status: o.Status, StatusDetail: detail, TraceStatus: o.TraceStatus, Reason: o.Reason, Error: errMsg, Duration: o.Duration.Round(time.Microsecond).String(), DurationMs: o.Duration.Milliseconds(), Diagnostics: slices.Clone(o.Diagnostics), Deviations: slices.Clone(o.Deviations), ProxyRestarted: o.ProxyRestarted}
}

type ScenarioEnv struct {
	Scenario                               Scenario
	Client                                 proxypb.PubSubProxyClient
	Fake                                   *fake.Server
	Caps                                   *proxypb.GetCapabilitiesResponse
	Profile                                traces.Profile
	ProjectID                              string
	runID                                  string
	seq                                    atomic.Int64
	traceVerifier                          *traces.Verifier
	traceHooks                             traces.TraceHooks
	mu                                     sync.Mutex
	traceResults                           []traces.TraceResult
	deviations                             []traces.Deviation
	failMsgs, diagnostics, traceFailMsgs   []string
	traceSkipped, traceVerified, traceFail bool
}

func newScenarioEnv(sc Scenario, client proxypb.PubSubProxyClient, fakeSrv *fake.Server, caps *proxypb.GetCapabilitiesResponse, projectID, runID string) *ScenarioEnv {
	tv := traces.NewVerifier(caps)
	return &ScenarioEnv{Scenario: sc, Client: client, Fake: fakeSrv, Caps: caps, Profile: tv.Profile(), ProjectID: firstNonEmpty(projectID, DefaultProjectID), runID: runID, traceVerifier: tv, traceHooks: traces.HooksFromClient(client)}
}

func (e *ScenarioEnv) Endpoint() string {
	if e.Fake == nil {
		return ""
	}
	return e.Fake.Addr()
}
func (e *ScenarioEnv) TopicPath(short string) string { return e.resourcePath("topics", short) }
func (e *ScenarioEnv) SubscriptionPath(short string) string {
	return e.resourcePath("subscriptions", short)
}
func (e *ScenarioEnv) resourcePath(kind, short string) string {
	if strings.HasPrefix(short, "projects/") {
		return short
	}
	return fmt.Sprintf("projects/%s/%s/%s", e.ProjectID, kind, short)
}
func (e *ScenarioEnv) UniqueName(prefix string) string {
	clean := strings.Map(func(r rune) rune {
		return map[bool]rune{true: r, false: '-'}[(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_']
	}, prefix)
	return fmt.Sprintf("%s-%s-%d", firstNonEmpty(clean, "res"), e.runID, e.seq.Add(1))
}
func (e *ScenarioEnv) Language() string { return e.Caps.GetClientLanguage() }
func (e *ScenarioEnv) SupportsMonitoring() bool {
	return e.Caps != nil && e.Caps.GetSupportsMonitoring()
}
func (e *ScenarioEnv) HasCapability(c Capability) bool { return HasCapability(e.Caps, c) }
func (e *ScenarioEnv) NewVerifier() *fake.Verifier     { return fake.NewVerifier() }
func (e *ScenarioEnv) Defaults() *proxypb.ClientDefaults {
	if e.Caps != nil && e.Caps.GetDefaults() != nil {
		return e.Caps.GetDefaults()
	}
	return &proxypb.ClientDefaults{}
}
func (e *ScenarioEnv) RequireCapability(c Capability) error {
	if !e.HasCapability(c) {
		return Unsupportedf("client does not support capability %q", c)
	}
	return nil
}
func (e *ScenarioEnv) CheckProxyErr(err error) error {
	if err != nil && status.Code(err) == codes.Unimplemented {
		return Unsupportedf("%v", err)
	}
	return err
}
func (e *ScenarioEnv) VerifyTraces(ctx context.Context, exp *traces.TraceExpectation) traces.TraceResult {
	res, _ := e.traceVerifier.DrainAndVerify(ctx, e.traceHooks, exp)
	e.recordTraceResult(res)
	return res
}
func (e *ScenarioEnv) MustVerifyTraces(ctx context.Context, exp *traces.TraceExpectation) error {
	if res := e.VerifyTraces(ctx, exp); res.Failed() {
		return res.Err()
	}
	return nil
}
func (e *ScenarioEnv) DrainTraces(ctx context.Context) ([]*proxypb.TraceSpan, bool, error) {
	spans, drained, err := traces.DrainTracesIfSupported(ctx, e.Client, e.Caps)
	if !drained && err == nil {
		e.mu.Lock()
		e.traceSkipped = true
		e.mu.Unlock()
	}
	return spans, drained, err
}
func (e *ScenarioEnv) VerifySpans(spans []*proxypb.TraceSpan, exp *traces.TraceExpectation) traces.TraceResult {
	res := e.traceVerifier.Verify(spans, exp)
	e.recordTraceResult(res)
	return res
}
func (e *ScenarioEnv) recordTraceResult(res traces.TraceResult) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.traceResults, e.deviations = append(e.traceResults, res), append(e.deviations, res.Deviations...)
	switch res.Status {
	case traces.StatusSkippedNoMonitoring:
		e.traceSkipped = true
	case traces.StatusPass:
		e.traceVerified = true
	case traces.StatusFail:
		msg := res.Format()
		e.traceFail, e.diagnostics, e.traceFailMsgs = true, append(e.diagnostics, msg), append(e.traceFailMsgs, msg)
	}
}
func (e *ScenarioEnv) Failf(format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	e.mu.Lock()
	e.failMsgs, e.diagnostics = append(e.failMsgs, msg), append(e.diagnostics, msg)
	e.mu.Unlock()
	return errors.New(msg)
}
func (e *ScenarioEnv) Diagnosticf(format string, args ...any) {
	e.mu.Lock()
	e.diagnostics = append(e.diagnostics, fmt.Sprintf(format, args...))
	e.mu.Unlock()
}
func (e *ScenarioEnv) snapshotState() (failed bool, failMsgs, diags []string, devs []traces.Deviation, traceStatus traces.Status, lastTrace *traces.TraceResult, traceFailed bool, traceErrMsg string, allSkipped bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	failed, traceFailed = len(e.failMsgs) > 0 || e.traceFail, e.traceFail
	failMsgs, diags, devs, traceErrMsg = slices.Clone(e.failMsgs), slices.Clone(e.diagnostics), slices.Clone(e.deviations), strings.Join(e.traceFailMsgs, "\n---\n")
	if len(e.traceResults) > 0 {
		lastTrace = &slices.Clone(e.traceResults)[len(e.traceResults)-1]
	}
	switch {
	case e.traceFail:
		traceStatus = traces.StatusFail
	case e.traceVerified:
		traceStatus = traces.StatusPass
	case e.traceSkipped:
		traceStatus = traces.StatusSkippedNoMonitoring
	}
	allSkipped = !e.traceVerified && !e.traceFail && (e.traceSkipped || !e.SupportsMonitoring())
	return
}
