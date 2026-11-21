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
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
)

func TestTraceFaultDetection_MonitoringEnabled_CLIAndRunner(t *testing.T) {
	traceCases := []struct {
		name, fault, filter string
		wantIDs             []string
		wantTraceFail       bool
		wantErrSubs         []string
	}{
		{"MissingSpans_ValidWireTP_ZeroDrainedSpans", "missing-spans", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"MISSING_SPAN"}},
		{"MissingTraces_NoWireTP_NoSpans", "missing-traces", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, false, []string{"googclient_traceparent"}},
		{"WrongSpanKind", "wrong-span-kind", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"WRONG_SPAN_KIND"}},
		{"InvalidSpanName", "invalid-span-name", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"MISSING_SPAN"}},
		{"BrokenWireTraceparent_WrongSpanID", "wrong-wire-span-id", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"BROKEN_TRACEPARENT"}},
		{"BrokenWireTraceparent_WrongTraceID", "wrong-trace-id", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, false, []string{"trace_id/sampled mismatch"}},
		{"BrokenParentSpanID_RootConstraint", "wrong-parent-span-id", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"WRONG_PARENT"}},
		{"BrokenCreateParent_Traceparent", "wrong-create-parent-id", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"BROKEN_TRACEPARENT"}},
		{"UnsampledTraceFlags", "unsampled-traces", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, false, []string{"trace_id/sampled mismatch"}},
		{"MissingBidirectionalLinks", "missing-links", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"MISSING_LINK"}},
		{"MissingReverseLink", "missing-reverse-link", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"BROKEN_BIDIRECTIONAL_LINK", "publish.bidirectional_link"}},
		{"MissingLinkOperationNameAttribute", "missing-link-attr", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"MISSING_LINK_ATTR"}},
		{"MissingEvents_PublishStartAndAckCalled", "missing-events", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, true, []string{"MISSING_EVENT"}},
		{"MissingSubscribeAckSpan", "missing-sub-ack-span", "^TRAC-03$", []string{"TRAC-03"}, true, []string{"MISSING_SPAN"}},
		{"BrokenSubscribeParent_TRAC03", "broken-sub-parent", "^TRAC-03$", []string{"TRAC-03"}, true, []string{"BROKEN_TRACEPARENT", "subscribe.parent_traceparent"}},
		{"WrongCallbackTraceparent_TRAC03", "wrong-cb-tp", "^TRAC-03$", []string{"TRAC-03"}, true, []string{"BROKEN_TRACEPARENT", "subscribe.process.callback_traceparent"}},
		{"DuplicateSpanID_TRAC03", "dup-span-id", "^TRAC-03$", []string{"TRAC-03"}, true, []string{"DUPLICATE_SPAN_ID", "span.unique_id"}},
		{"EmitSpansWhenMonitoringDisabled", "emit-span-when-disabled", "^(PUB-01|PORD-02|PULL-01|PULL-04)$", []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04"}, true, []string{"UNEXPECTED_SPAN"}},
	}

	for _, tc := range traceCases {
		t.Run("CLI_"+tc.name, func(t *testing.T) {
			rep := runFaultCLI(t, tc.fault, tc.filter, 5*time.Second, harness.StatusFail, tc.wantIDs, tc.wantErrSubs...)
			byID := statusByID(rep.Scenarios)
			for _, id := range tc.wantIDs {
				sr := byID[id]
				if tc.wantTraceFail && sr.TraceStatus != traces.StatusFail {
					t.Fatalf("%s scenario %s expected TraceStatus=FAIL, got %q (err=%s)", tc.name, id, sr.TraceStatus, sr.Error)
				}
			}
			if tc.fault == "missing-reverse-link" && !strings.Contains(byID["TRAC-03"].Error, "subscribe.settlement.reverse_link") {
				t.Fatalf("missing-reverse-link TRAC-03 missing subscribe reverse link failure: %s", byID["TRAC-03"].Error)
			}
		})
	}

	t.Run("Runner_TraceStatusFailVerification", func(t *testing.T) {
		for _, fault := range []string{"missing-spans", "wrong-wire-span-id", "missing-link-attr", "missing-reverse-link", "broken-sub-parent", "wrong-cb-tp", "dup-span-id", "emit-span-when-disabled"} {
			t.Run(fault, func(t *testing.T) {
				pidLog := filepath.Join(t.TempDir(), "pids.log")
				trackPIDLog(t, pidLog)
				filter := "^TRAC-03$"
				if fault == "emit-span-when-disabled" {
					filter = "^PUB-01$"
				}
				target := helperTargetSpec(t, "trace-"+fault, pidLog, "--profile=go", "--fault="+fault)
				rep := mustRunTarget(t, context.Background(), harness.RunConfig{
					Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: filter, ScenarioTimeout: 5 * time.Second,
				}, target)
				if len(rep.Scenarios) != 1 {
					t.Fatalf("RunTarget(%s): unexpected scenarios=%+v", fault, rep.Scenarios)
				}
				sr := rep.Scenarios[0]
				if sr.Status != harness.StatusFail || sr.TraceStatus != traces.StatusFail || !strings.Contains(sr.Error, "Status=FAIL") {
					t.Fatalf("Runner(%s) expected Status=FAIL & TraceStatus=FAIL, got %+v", fault, sr)
				}
				assertAllPIDsDead(t, pidLog, 1, false)
			})
		}
	})

	t.Run("ProfileCpp_NegativeTraceAndUnaryPullVerification", func(t *testing.T) {
		pidLog := filepath.Join(t.TempDir(), "pids.log")
		trackPIDLog(t, pidLog)
		target := helperTargetSpec(t, "cpp-bad-trace", pidLog, "--profile=cpp", "--fault=wrong-span-kind")
		if rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^TRAC-01$", ScenarioTimeout: 5 * time.Second}, target); statusByID(rep.Scenarios)["TRAC-01"].TraceStatus != traces.StatusFail {
			t.Fatalf("ProfileCpp TRAC-01 expected TraceStatus=FAIL, got %+v", rep.Scenarios)
		}
		sc := harness.Scenario{ID: "CPP-UNARY-TRACE", Category: "traces", Run: func(ctx context.Context, env *harness.ScenarioEnv) error {
			if err := env.RequireCapability(harness.CapUnaryPull); err != nil {
				return err
			}
			sub := env.SubscriptionPath(env.UniqueName("u"))
			_, _ = env.Client.UnaryPull(ctx, &proxypb.UnaryPullRequest{Endpoint: env.Endpoint(), Subscription: sub, EnableMonitoring: true, MaxMessages: 1, ReturnImmediately: true})
			return env.MustVerifyTraces(ctx, &traces.TraceExpectation{Subscription: sub, UnaryPull: &traces.UnaryPullExpectation{Messages: []traces.UnaryPullMessageExpectation{{MessageID: "m1"}}}})
		}}
		if repU := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: []harness.Scenario{sc}, ScenarioTimeout: 5 * time.Second}, target); len(repU.Scenarios) != 1 || repU.Scenarios[0].Status != harness.StatusFail || !strings.Contains(repU.Scenarios[0].Error, "MISSING_SPAN") {
			t.Fatalf("ProfileCpp UnaryPull expected MISSING_SPAN FAIL, got %+v", repU.Scenarios)
		}
		assertAllPIDsDead(t, pidLog, 2, false)
	})
}
