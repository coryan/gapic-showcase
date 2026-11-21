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
	"fmt"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc"
)

// Status represents the R2 trace verification outcome for a scenario.
type Status string

const (
	// StatusPass indicates monitoring is enabled and all trace assertions passed.
	StatusPass Status = "PASS"
	// StatusFail indicates monitoring is enabled and one or more trace assertions
	// or trace RPC operations failed.
	StatusFail Status = "FAIL"
	// StatusSkippedNoMonitoring indicates the target proxy reported
	// supports_monitoring=false, so trace assertions were skipped cleanly per R2.
	StatusSkippedNoMonitoring Status = "SKIPPED_NO_MONITORING"
)

// TraceClient is the minimal proxy trace RPC subset required by the verifier.
// proxypb.PubSubProxyClient satisfies this interface directly.
type TraceClient interface {
	ResetTraces(ctx context.Context, in *proxypb.ResetTracesRequest, opts ...grpc.CallOption) (*proxypb.ResetTracesResponse, error)
	DrainTraces(ctx context.Context, in *proxypb.DrainTracesRequest, opts ...grpc.CallOption) (*proxypb.DrainTracesResponse, error)
}

var _ TraceClient = (proxypb.PubSubProxyClient)(nil)

// TraceHooks decouples the verifier from gRPC CallOption signatures.
type TraceHooks struct {
	Reset func(ctx context.Context) error
	Drain func(ctx context.Context) ([]*proxypb.TraceSpan, error)
}

// HooksFromClient adapts a TraceClient into TraceHooks.
func HooksFromClient(client TraceClient) TraceHooks {
	if client == nil {
		return TraceHooks{}
	}
	return TraceHooks{
		Reset: func(ctx context.Context) error {
			_, err := client.ResetTraces(ctx, &proxypb.ResetTracesRequest{})
			return err
		},
		Drain: func(ctx context.Context) ([]*proxypb.TraceSpan, error) {
			resp, err := client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
			if err != nil {
				return nil, err
			}
			if resp == nil {
				return nil, nil
			}
			return resp.GetSpans(), nil
		},
	}
}

// Gate holds the evaluated R2 monitoring decision and client profile.
type Gate struct {
	SupportsMonitoring bool
	ClientLanguage     string
	Profile            Profile
}

// EvaluateCapabilities derives the R2 monitoring gate and SDK verification
// profile from caps. A nil caps response disables monitoring (skip mode).
func EvaluateCapabilities(caps *proxypb.GetCapabilitiesResponse) Gate {
	if caps == nil {
		return Gate{
			SupportsMonitoring: false,
			Profile:            ProfileGoCanonical(),
		}
	}
	lang := caps.GetClientLanguage()
	return Gate{
		SupportsMonitoring: caps.GetSupportsMonitoring(),
		ClientLanguage:     lang,
		Profile:            ProfileForLanguage(lang),
	}
}

// ResetTracesIfSupported invokes client.ResetTraces only if caps reports
// supports_monitoring=true.
func ResetTracesIfSupported(ctx context.Context, client TraceClient, caps *proxypb.GetCapabilitiesResponse) error {
	gate := EvaluateCapabilities(caps)
	if !gate.SupportsMonitoring {
		return nil
	}
	return NewVerifier(caps).ResetIfSupported(ctx, HooksFromClient(client))
}

// DrainTracesIfSupported invokes client.DrainTraces only if caps reports
// supports_monitoring=true. When monitoring is unsupported, it returns
// (nil, false, nil) without issuing any RPC.
func DrainTracesIfSupported(ctx context.Context, client TraceClient, caps *proxypb.GetCapabilitiesResponse) (spans []*proxypb.TraceSpan, drained bool, err error) {
	gate := EvaluateCapabilities(caps)
	if !gate.SupportsMonitoring {
		return nil, false, nil
	}
	if client == nil {
		return nil, false, fmt.Errorf("traces: cannot drain traces with nil TraceClient when supports_monitoring=true")
	}
	resp, err := client.DrainTraces(ctx, &proxypb.DrainTracesRequest{})
	if err != nil {
		return nil, false, err
	}
	if resp == nil {
		return nil, true, nil
	}
	return resp.GetSpans(), true, nil
}

// Verifier gates and executes OpenTelemetry trace verification against a
// resolved Gate and Profile.
type Verifier struct {
	gate Gate
}

// NewVerifier creates a Verifier configured from caps.
func NewVerifier(caps *proxypb.GetCapabilitiesResponse) *Verifier {
	return &Verifier{gate: EvaluateCapabilities(caps)}
}

// NewVerifierFromConfig creates a Verifier directly from monitoring support
// and client language settings.
func NewVerifierFromConfig(supportsMonitoring bool, clientLanguage string) *Verifier {
	return &Verifier{
		gate: Gate{
			SupportsMonitoring: supportsMonitoring,
			ClientLanguage:     clientLanguage,
			Profile:            ProfileForLanguage(clientLanguage),
		},
	}
}

// Gate returns the verifier's evaluated R2 capability gate.
func (v *Verifier) Gate() Gate {
	if v == nil {
		return Gate{Profile: ProfileGoCanonical()}
	}
	return v.gate
}

// Profile returns the active SDK verification profile.
func (v *Verifier) Profile() Profile {
	return v.Gate().Profile
}

// ResetIfSupported executes hooks.Reset iff SupportsMonitoring is true.
func (v *Verifier) ResetIfSupported(ctx context.Context, hooks TraceHooks) error {
	if !v.Gate().SupportsMonitoring {
		return nil
	}
	if hooks.Reset == nil {
		return fmt.Errorf("traces: Reset hook is nil while supports_monitoring=true")
	}
	return hooks.Reset(ctx)
}

// DrainAndVerify drains spans via hooks.Drain and verifies them against exp
// when SupportsMonitoring is true. When SupportsMonitoring is false, hooks.Drain
// is not called and (TraceResult{Status: StatusSkippedNoMonitoring, ...}, nil)
// is returned cleanly.
func (v *Verifier) DrainAndVerify(ctx context.Context, hooks TraceHooks, exp *TraceExpectation) (TraceResult, error) {
	gate := v.Gate()
	if !gate.SupportsMonitoring {
		return TraceResult{
			Status:             StatusSkippedNoMonitoring,
			SupportsMonitoring: false,
			ClientLanguage:     gate.ClientLanguage,
			Profile:            gate.Profile.Name,
		}, nil
	}
	if hooks.Drain == nil {
		err := fmt.Errorf("traces: Drain hook is nil while supports_monitoring=true")
		return TraceResult{
			Status:             StatusFail,
			SupportsMonitoring: true,
			ClientLanguage:     gate.ClientLanguage,
			Profile:            gate.Profile.Name,
			Failures: []Violation{{
				Code:    CodeDrainError,
				Rule:    "traces.drain",
				Message: err.Error(),
			}},
		}, err
	}
	spans, err := hooks.Drain(ctx)
	if err != nil {
		return TraceResult{
			Status:             StatusFail,
			SupportsMonitoring: true,
			ClientLanguage:     gate.ClientLanguage,
			Profile:            gate.Profile.Name,
			Failures: []Violation{{
				Code:    CodeDrainError,
				Rule:    "traces.drain",
				Message: fmt.Sprintf("DrainTraces failed: %v", err),
			}},
		}, err
	}
	return v.Verify(spans, exp), nil
}

// Verify verifies pre-drained spans against exp according to v's Gate and Profile.
func (v *Verifier) Verify(spans []*proxypb.TraceSpan, exp *TraceExpectation) TraceResult {
	gate := v.Gate()
	if !gate.SupportsMonitoring {
		return TraceResult{
			Status:             StatusSkippedNoMonitoring,
			SupportsMonitoring: false,
			ClientLanguage:     gate.ClientLanguage,
			Profile:            gate.Profile.Name,
		}
	}
	res := verifySpans(spans, gate.Profile, exp)
	res.SupportsMonitoring = true
	res.ClientLanguage = gate.ClientLanguage
	return res
}

// DrainAndVerify is a convenience wrapper around NewVerifier(caps).DrainAndVerify.
func DrainAndVerify(ctx context.Context, client TraceClient, caps *proxypb.GetCapabilitiesResponse, exp *TraceExpectation) (TraceResult, error) {
	return NewVerifier(caps).DrainAndVerify(ctx, HooksFromClient(client), exp)
}

// Verify is a convenience wrapper around NewVerifier(caps).Verify.
func Verify(caps *proxypb.GetCapabilitiesResponse, spans []*proxypb.TraceSpan, exp *TraceExpectation) TraceResult {
	return NewVerifier(caps).Verify(spans, exp)
}
