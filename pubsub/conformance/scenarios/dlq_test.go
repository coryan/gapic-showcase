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

package scenarios_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestDLQScenarios_Unit(t *testing.T) {
	m3cAssertUnitMeta(t, scenarios.ExportDLQScenarios(), []harness.Scenario{{ID: "DLQ-01", Category: "dlq"}})
}
func TestDLQScenarios_Profiles(t *testing.T) {
	list := scenarios.ExportDLQScenarios()
	m3cRunProfileTests(t, list, 1, 3*time.Second, nil)
	m3cRunFaultCases(t, list, []m3cFaultCase{
		{prof: "rust", fault: "runtime-unimpl-sub", sc: "DLQ-01", timeout: 2 * time.Second, want: harness.StatusUnsupportedByClient},
		{prof: "go", fault: "runtime-unimpl-sub", sc: "DLQ-01", timeout: 2 * time.Second, want: harness.StatusUnsupportedByClient, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsUnaryPull = false }},
	})
}
func TestDLQScenarios_FaultDetection(t *testing.T) {
	m3cRunFaultCases(t, scenarios.ExportDLQScenarios(), []m3cFaultCase{
		{prof: "go", fault: "dlq01-unset-becomes-positive", sc: "DLQ-01", sub: "delivery_attempt mismatch"},
		{prof: "php", fault: "dlq01-unset-becomes-positive", sc: "DLQ-01", sub: "delivery_attempt mismatch"},
		{prof: "go", fault: "dlq01-drops-positive", sc: "DLQ-01", sub: "delivery_attempt mismatch"},
		{prof: "cpp", fault: "dlq01-wrong-unary-attempt", sc: "DLQ-01", sub: "DLQ-01 UnaryPull"},
		{prof: "php", fault: "dlq01-wrong-unary-attempt", sc: "DLQ-01", sub: "DLQ-01 UnaryPull"},
		{prof: "cpp", fault: "dlq01-unary-settle-err", sc: "DLQ-01", sub: "settle failed"},
		{prof: "cpp", fault: "dlq01-unary-inband-unimpl", sc: "DLQ-01", sub: "UNIMPLEMENTED"},
		{prof: "cpp", fault: "dlq01-unary-grpc-unimpl", sc: "DLQ-01", sub: "UNIMPLEMENTED"},
		{prof: "go", fault: "wrong-ack-sub", sc: "DLQ-01", sub: "subscription mismatch"},
		{prof: "cpp", fault: "wrong-ack-sub", sc: "DLQ-01", sub: "subscription mismatch"},
		{prof: "go", fault: "missing-ack-req-params", sc: "DLQ-01", sub: "x-goog-request-params"},
		{prof: "go", fault: "emit-unexpected-span", sc: "DLQ-01", sub: "expected zero spans"},
		{prof: "cpp", fault: "spurious-nack-after-ack", sc: "DLQ-01", sub: "nacked ack_ids set mismatch"},
	})
}
func TestDLQScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	m3cRunNoLeakFaults(t, scenarios.ExportDLQScenarios(), 400*time.Millisecond, [3]string{"go", "pull02-hangs-stream", "^DLQ-01$"})
}

type dlqWrapHandle struct {
	harness.ProxyHandle
	client proxypb.PubSubProxyClient
}

func (h dlqWrapHandle) Client() proxypb.PubSubProxyClient { return h.client }

type dlqWrapClient struct {
	proxypb.PubSubProxyClient
	duplicate bool
}

func (c dlqWrapClient) UnaryPull(ctx context.Context, in *proxypb.UnaryPullRequest, opts ...grpc.CallOption) (*proxypb.UnaryPullResponse, error) {
	resp, err := c.PubSubProxyClient.UnaryPull(ctx, in, opts...)
	if err != nil || resp == nil || resp.GetErrorCode() != 0 {
		return resp, err
	}
	cloned := proto.Clone(resp).(*proxypb.UnaryPullResponse)
	if c.duplicate {
		if len(cloned.Messages) > 0 {
			cloned.Messages = append(cloned.Messages, proto.Clone(cloned.Messages[0]).(*proxypb.UnaryPullResponse_PulledMessage))
		}
	} else {
		cloned.Messages = nil
	}
	return cloned, nil
}

func TestDLQ01_UnaryPullMessageCountMismatch(t *testing.T) {
	list := scenarios.ExportDLQScenarios()
	baseConnect := m3cMockConnect(m3cMockConfig{profile: "cpp"})

	for _, tc := range []struct {
		name      string
		duplicate bool
		wantSub   string
	}{
		{name: "duplicate_2_messages", duplicate: true, wantSub: "returned 2 messages, want 1"},
		{name: "empty_0_messages", duplicate: false, wantSub: "returned 0 messages, want 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := harness.TargetSpec{
				Name: "dlq01-" + tc.name,
				Connect: func(ctx context.Context, ts harness.TargetSpec) (harness.ProxyHandle, error) {
					h, err := baseConnect(ctx, ts)
					if err != nil {
						return nil, err
					}
					return dlqWrapHandle{
						ProxyHandle: h,
						client:      dlqWrapClient{PubSubProxyClient: h.Client(), duplicate: tc.duplicate},
					}, nil
				},
			}
			runner, err := harness.NewRunner(harness.RunConfig{
				Targets:         []harness.TargetSpec{target},
				Scenarios:       list,
				Filter:          "^DLQ-01$",
				ScenarioTimeout: 3 * time.Second,
			})
			if err != nil {
				t.Fatalf("NewRunner: %v", err)
			}
			rep, err := runner.RunTarget(context.Background(), target)
			if err != nil {
				t.Fatalf("RunTarget: %v", err)
			}
			if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, tc.wantSub) {
				t.Fatalf("%s: expected StatusFail containing %q, got scenarios=%+v", tc.name, tc.wantSub, rep.Scenarios)
			}
		})
	}
}
