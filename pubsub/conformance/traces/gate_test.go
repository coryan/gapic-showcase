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

package traces

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeTraceClient struct {
	resetCalls int
	drainCalls int
	resetErr   error
	drainErr   error
	spans      []*proxypb.TraceSpan
}

func (f *fakeTraceClient) ResetTraces(context.Context, *proxypb.ResetTracesRequest, ...grpc.CallOption) (*proxypb.ResetTracesResponse, error) {
	f.resetCalls++
	if f.resetErr != nil {
		return nil, f.resetErr
	}
	return &proxypb.ResetTracesResponse{}, nil
}

func (f *fakeTraceClient) DrainTraces(context.Context, *proxypb.DrainTracesRequest, ...grpc.CallOption) (*proxypb.DrainTracesResponse, error) {
	f.drainCalls++
	if f.drainErr != nil {
		return nil, f.drainErr
	}
	return &proxypb.DrainTracesResponse{Spans: f.spans}, nil
}

func TestEvaluateCapabilities(t *testing.T) {
	tests := []struct {
		name        string
		caps        *proxypb.GetCapabilitiesResponse
		wantMonitor bool
		wantProfile ProfileName
	}{
		{
			name:        "nil capabilities disables monitoring and defaults to Go",
			caps:        nil,
			wantMonitor: false,
			wantProfile: ProfileGo,
		},
		{
			name: "rust without monitoring",
			caps: &proxypb.GetCapabilitiesResponse{
				ClientLanguage:     "rust",
				SupportsMonitoring: false,
			},
			wantMonitor: false,
			wantProfile: ProfileGo,
		},
		{
			name: "go with monitoring",
			caps: &proxypb.GetCapabilitiesResponse{
				ClientLanguage:     "go",
				SupportsMonitoring: true,
			},
			wantMonitor: true,
			wantProfile: ProfileGo,
		},
		{
			name: "cpp case insensitive",
			caps: &proxypb.GetCapabilitiesResponse{
				ClientLanguage:     "C++",
				SupportsMonitoring: true,
			},
			wantMonitor: true,
			wantProfile: ProfileCPP,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gate := EvaluateCapabilities(tc.caps)
			if gate.SupportsMonitoring != tc.wantMonitor {
				t.Fatalf("SupportsMonitoring = %v, want %v", gate.SupportsMonitoring, tc.wantMonitor)
			}
			if gate.Profile.Name != tc.wantProfile {
				t.Fatalf("Profile.Name = %q, want %q", gate.Profile.Name, tc.wantProfile)
			}
		})
	}
}

func TestMonitoringGate_DisabledSkipsRPCs(t *testing.T) {
	client := &fakeTraceClient{
		resetErr: status.Error(codes.Unimplemented, "monitoring unsupported"),
		drainErr: status.Error(codes.Unimplemented, "monitoring unsupported"),
	}
	caps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage:     "rust",
		SupportsMonitoring: false,
	}

	ctx := context.Background()
	if err := ResetTracesIfSupported(ctx, client, caps); err != nil {
		t.Fatalf("ResetTracesIfSupported error = %v, want nil", err)
	}
	spans, drained, err := DrainTracesIfSupported(ctx, client, caps)
	if err != nil || drained || len(spans) != 0 {
		t.Fatalf("DrainTracesIfSupported = (%v, %v, %v), want (nil, false, nil)", spans, drained, err)
	}
	if client.resetCalls != 0 || client.drainCalls != 0 {
		t.Fatalf("expected 0 RPC calls when monitoring disabled, got reset=%d drain=%d", client.resetCalls, client.drainCalls)
	}

	exp := &TraceExpectation{
		Topic:   "projects/p/topics/t",
		Publish: &PublishExpectation{MessageCount: 1},
	}
	res, err := DrainAndVerify(ctx, client, caps, exp)
	if err != nil {
		t.Fatalf("DrainAndVerify error = %v, want nil", err)
	}
	if res.Status != StatusSkippedNoMonitoring || !res.Passed() || res.Failed() || res.Err() != nil {
		t.Fatalf("unexpected skipped result: %+v", res)
	}
	if client.resetCalls != 0 || client.drainCalls != 0 {
		t.Fatalf("expected 0 RPC calls after DrainAndVerify, got reset=%d drain=%d", client.resetCalls, client.drainCalls)
	}

	// Nil caps and Verify helper also return SKIPPED_NO_MONITORING.
	resNil := Verify(nil, nil, exp)
	if resNil.Status != StatusSkippedNoMonitoring || !resNil.Passed() {
		t.Fatalf("Verify(nil, ...) = %+v, want SKIPPED_NO_MONITORING", resNil)
	}
}

func TestMonitoringGate_EnabledExecutesAndPropagatesErrors(t *testing.T) {
	ctx := context.Background()
	caps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage:     "go",
		SupportsMonitoring: true,
	}

	t.Run("reset error propagates", func(t *testing.T) {
		wantErr := errors.New("reset rpc failed")
		client := &fakeTraceClient{resetErr: wantErr}
		if err := ResetTracesIfSupported(ctx, client, caps); !errors.Is(err, wantErr) {
			t.Fatalf("ResetTracesIfSupported err = %v, want %v", err, wantErr)
		}
		if client.resetCalls != 1 {
			t.Fatalf("resetCalls = %d, want 1", client.resetCalls)
		}
	})

	t.Run("drain error propagates alongside StatusFail", func(t *testing.T) {
		wantErr := errors.New("drain rpc failed")
		client := &fakeTraceClient{drainErr: wantErr}
		res, err := DrainAndVerify(ctx, client, caps, ExpectNoTraces())
		if !errors.Is(err, wantErr) {
			t.Fatalf("DrainAndVerify err = %v, want %v", err, wantErr)
		}
		if res.Status != StatusFail || res.Passed() || !res.Failed() {
			t.Fatalf("expected StatusFail, got %+v", res)
		}
		if client.drainCalls != 1 {
			t.Fatalf("drainCalls = %d, want 1", client.drainCalls)
		}
	})

	t.Run("nil hooks or client when monitoring enabled errors", func(t *testing.T) {
		v := NewVerifierFromConfig(true, "go")
		if err := v.ResetIfSupported(ctx, TraceHooks{}); err == nil {
			t.Fatal("expected error for nil Reset hook")
		}
		res, err := v.DrainAndVerify(ctx, TraceHooks{}, ExpectNoTraces())
		if err == nil || res.Status != StatusFail {
			t.Fatalf("expected error + StatusFail for nil Drain hook, got (%+v, %v)", res, err)
		}
	})

	t.Run("ExpectNoTraces passes on empty or otelgrpc-only and fails on pubsub spans", func(t *testing.T) {
		client := &fakeTraceClient{
			spans: []*proxypb.TraceSpan{
				{
					TraceId:                  "0123456789abcdef0123456789abcdef",
					SpanId:                   "0123456789abcdef",
					Name:                     "google.pubsub.v1.Publisher/Publish",
					Kind:                     proxypb.SpanKind_SPAN_KIND_CLIENT,
					InstrumentationScopeName: OTelGrpcScopeName,
				},
			},
		}
		if err := ResetTracesIfSupported(ctx, client, caps); err != nil {
			t.Fatalf("ResetTracesIfSupported err = %v", err)
		}
		res, err := DrainAndVerify(ctx, client, caps, ExpectNoTraces())
		if err != nil {
			t.Fatalf("DrainAndVerify err = %v", err)
		}
		if res.Status != StatusPass || !res.Passed() {
			t.Fatalf("expected PASS ignoring otelgrpc span, got:\n%s", res.Format())
		}

		// With StrictRawSpans=true, otelgrpc span causes failure.
		strictExp := ExpectNoTraces()
		strictExp.StrictRawSpans = true
		resStrict := Verify(caps, client.spans, strictExp)
		if resStrict.Status != StatusFail || len(resStrict.Failures) == 0 {
			t.Fatalf("expected FAIL with StrictRawSpans=true, got %+v", resStrict)
		}

		// A Pub/Sub span fails ExpectNoTraces.
		client.spans = append(client.spans, &proxypb.TraceSpan{
			TraceId:                  "0123456789abcdef0123456789abcdef",
			SpanId:                   "fedcba9876543210",
			Name:                     "t create",
			Kind:                     proxypb.SpanKind_SPAN_KIND_PRODUCER,
			InstrumentationScopeName: GoPubSubScopeName,
		})
		resFail, err := DrainAndVerify(ctx, client, caps, ExpectNoPubSubSpans())
		if err != nil {
			t.Fatalf("unexpected RPC error: %v", err)
		}
		if resFail.Status != StatusFail || resFail.Err() == nil {
			t.Fatalf("expected FAIL when Pub/Sub span emitted with ExpectNoTraces, got %+v", resFail)
		}
		if !strings.Contains(resFail.Format(), string(CodeUnexpectedSpan)) {
			t.Fatalf("expected %s in diagnostic output:\n%s", CodeUnexpectedSpan, resFail.Format())
		}
	})

	t.Run("DrainTracesIfSupported and (*Verifier).Profile coverage", func(t *testing.T) {
		var nilV *Verifier
		if nilV.Profile().Name != ProfileGo {
			t.Fatalf("nil Verifier Profile = %q, want %q", nilV.Profile().Name, ProfileGo)
		}
		if p := NewVerifier(caps).Profile(); p.Name != ProfileGo {
			t.Fatalf("Verifier(go).Profile = %q", p.Name)
		}

		client := &fakeTraceClient{
			spans: []*proxypb.TraceSpan{
				{
					TraceId:                  "0123456789abcdef0123456789abcdef",
					SpanId:                   "0123456789abcdef",
					Name:                     "t create",
					Kind:                     proxypb.SpanKind_SPAN_KIND_PRODUCER,
					InstrumentationScopeName: GoPubSubScopeName,
				},
			},
		}
		got, drained, err := DrainTracesIfSupported(ctx, client, caps)
		if err != nil || !drained || len(got) != 1 {
			t.Fatalf("DrainTracesIfSupported(enabled) = (%d spans, %v, %v)", len(got), drained, err)
		}
		if _, drainedNil, errNil := DrainTracesIfSupported(ctx, nil, caps); errNil == nil || drainedNil {
			t.Fatalf("DrainTracesIfSupported(nil client) = (%v, %v), want error", drainedNil, errNil)
		}
		errClient := &fakeTraceClient{drainErr: errors.New("boom")}
		if _, drainedErr, errGot := DrainTracesIfSupported(ctx, errClient, caps); errGot == nil || drainedErr {
			t.Fatalf("DrainTracesIfSupported(error) = (%v, %v), want error", drainedErr, errGot)
		}
	})

	t.Run("monitoring enabled with zero spans fails expected publish", func(t *testing.T) {
		client := &fakeTraceClient{}
		res, err := DrainAndVerify(ctx, client, caps, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1},
		})
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if res.Status != StatusFail {
			t.Fatalf("expected FAIL on 0 spans when publish expected, got %+v", res)
		}
	})

	t.Run("GAP-A7-T14_HooksFromClientNilAndResetIfSupportedDisabled", func(t *testing.T) {
		hooks := HooksFromClient(nil)
		if hooks.Reset != nil || hooks.Drain != nil {
			t.Fatalf("HooksFromClient(nil) = %+v, want zero TraceHooks", hooks)
		}

		called := false
		if err := NewVerifier(nil).ResetIfSupported(ctx, TraceHooks{
			Reset: func(context.Context) error {
				called = true
				return errors.New("should not run")
			},
		}); err != nil {
			t.Fatalf("NewVerifier(nil).ResetIfSupported error = %v, want nil", err)
		}
		if called {
			t.Fatal("ResetIfSupported unexpectedly invoked Reset hook when SupportsMonitoring=false")
		}
		if err := NewVerifierFromConfig(false, "go").ResetIfSupported(ctx, TraceHooks{}); err != nil {
			t.Fatalf("NewVerifierFromConfig(false, ...).ResetIfSupported error = %v, want nil", err)
		}
	})
}
