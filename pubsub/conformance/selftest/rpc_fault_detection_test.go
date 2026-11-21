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

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/pubsubpb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func runFaultCLI(t *testing.T, fault, filter string, scenarioTimeout time.Duration, wantStatus harness.Status, wantIDs []string, wantErrSubs ...string) *harness.Report {
	t.Helper()
	dir := t.TempDir()
	pidLog := filepath.Join(dir, "pids.log")
	repJSON := filepath.Join(dir, "fault.json")
	trackPIDLog(t, pidLog)

	if scenarioTimeout <= 0 {
		scenarioTimeout = 5 * time.Second
	}
	args := append(proxyCmdArgs("--profile=go", "--fault="+fault, "--pid-log="+pidLog),
		"--filter="+filter, "--scenario-timeout="+scenarioTimeout.String(), "--report-json="+repJSON)
	res := runCLI(t, 20*time.Second, nil, args...)
	if res.ExitCode != 1 {
		t.Fatalf("fault=%s filter=%s expected exit 1, got %d\nstdout:\n%s\nstderr:\n%s", fault, filter, res.ExitCode, res.Stdout, res.Stderr)
	}
	rep, rawMap := mustLoadJSONReport(t, repJSON)
	byID := statusByID(rep.Scenarios)
	for _, id := range wantIDs {
		sr, ok := byID[id]
		if !ok {
			t.Fatalf("fault=%s missing scenario %s in %+v", fault, id, rep.Scenarios)
		}
		if sr.Status != wantStatus {
			t.Fatalf("fault=%s scenario %s status=%s want=%s (err=%s)", fault, id, sr.Status, wantStatus, sr.Error)
		}
		rawSc := rawScenarioByID(t, rawMap, id)
		if rawSc["status"] != "FAIL" {
			t.Fatalf("fault=%s scenario %s expected canonical JSON status FAIL, got %+v", fault, id, rawSc)
		}
		for _, sub := range wantErrSubs {
			if !strings.Contains(sr.Error, sub) {
				t.Fatalf("fault=%s scenario %s error %q missing substring %q", fault, id, sr.Error, sub)
			}
		}
	}
	assertAllPIDsDead(t, pidLog, 1, false)
	return rep
}

func TestRPCFaultDetection_WrongFieldsAndOrderingKey_CLIAndRunner(t *testing.T) {
	cliCases := []struct {
		name, fault, filter string
		wantIDs             []string
		wantSub             string
	}{
		{"WrongPayload", "wrong-payload", "^(PUB-01|PULL-01|PULL-04|TRAC-01|TRAC-03)$", []string{"PUB-01", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"}, "mismatch"},
		{"WrongTopic", "wrong-topic", "^(PUB-01|PORD-02)$", []string{"PUB-01", "PORD-02"}, "topic mismatch"},
		{"WrongAttributes", "wrong-attrs", "^(PUB-01|PULL-01|PULL-04|TRAC-01|TRAC-03)$", []string{"PUB-01", "PULL-01", "PULL-04", "TRAC-01", "TRAC-03"}, "attributes mismatch"},
		{"CallerAttributesMutated", "caller-attrs-mutated", "^(TRAC-01|TRAC-03)$", []string{"TRAC-01", "TRAC-03"}, "mutated"},
		{"UnexpectedOrderingKey_PUB01", "unexpected-ordering-key", "^PUB-01$", []string{"PUB-01"}, "ordering_key mismatch"},
		{"WrongOrderingKey_PORD02_Batch2", "wrong-ordering-key-b2", "^PORD-02$", []string{"PORD-02"}, "k1"},
		{"CorruptDeliveredOrderingKey", "corrupt-ordering-key", "^(PULL-01|PULL-04|TRAC-03)$", []string{"PULL-01", "PULL-04", "TRAC-03"}, "key="},
		{"MissingRoutingHeader", "missing-routing-header", "^(PUB-01|PORD-02|PULL-01|PULL-04)$", []string{"PUB-01", "PORD-02", "PULL-01", "PULL-04"}, "x-goog-request-params"},
		{"WrongAckID", "wrong-ack-id", "^(PULL-01|PULL-04)$", []string{"PULL-01", "PULL-04"}, "ack_ids"},
		{"WrongSubscription", "wrong-sub", "^(PULL-01|PULL-04)$", []string{"PULL-01", "PULL-04"}, "subscription mismatch"},
		{"MismatchedMessageID", "mismatched-message-id", "^(PUB-01|PORD-02|PULL-04|TRAC-01|TRAC-03)$", []string{"PUB-01", "PORD-02", "PULL-04", "TRAC-01", "TRAC-03"}, "mismatch"},
	}
	for _, tc := range cliCases {
		t.Run("CLI_"+tc.name, func(t *testing.T) {
			runFaultCLI(t, tc.fault, tc.filter, 5*time.Second, harness.StatusFail, tc.wantIDs, tc.wantSub)
		})
	}

	t.Run("Runner_NonGatedWrongOrderingKey_ImmediateFail", func(t *testing.T) {
		pidLog := filepath.Join(t.TempDir(), "pids.log")
		trackPIDLog(t, pidLog)
		sc := harness.Scenario{
			ID: "CUSTOM-ORDKEY", Name: "Non-Gated OrderingKey Check", Category: "ordering",
			Run: func(ctx context.Context, env *harness.ScenarioEnv) error {
				topic := env.TopicPath(env.UniqueName("ordkey"))
				resp, err := env.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{
					Endpoint: env.Endpoint(), Topic: topic, ProjectId: env.ProjectID,
					EnableMessageOrdering: proto.Bool(true), CountThreshold: proto.Int64(1),
				})
				if err != nil {
					return err
				}
				pubID := resp.GetPublisherId()
				defer func() { _, _ = env.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: pubID}) }()
				if _, err := env.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: pubID, Data: []byte("ord"), OrderingKey: "key-a"}); err != nil {
					return err
				}
				return fake.VerifySequence(env.Fake.Snapshot(), fake.DefaultVerifyOptions(),
					fake.ExpectPublish("ord-pub", fake.PublishTopic(topic), fake.PublishSingleMessage(fake.ExpectedMessage{Data: []byte("ord"), OrderingKey: "key-a"})),
				)
			},
		}
		target := helperTargetSpec(t, "wrong-ord", pidLog, "--fault=wrong-ordering-key")
		rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: []harness.Scenario{sc}, ScenarioTimeout: 5 * time.Second}, target)
		if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, "ordering_key mismatch") {
			t.Fatalf("expected immediate StatusFail with ordering_key mismatch, got rep=%+v", rep)
		}
		assertAllPIDsDead(t, pidLog, 1, false)
	})

	for _, tc := range []struct{ fault, wantSub string }{
		{"wrong-payload", "mismatch"},
		{"wrong-topic", "topic mismatch"},
		{"wrong-attrs", "attributes mismatch"},
	} {
		t.Run("Runner_"+tc.fault, func(t *testing.T) {
			pidLog := filepath.Join(t.TempDir(), "pids.log")
			trackPIDLog(t, pidLog)
			target := helperTargetSpec(t, "runner-"+tc.fault, pidLog, "--profile=go", "--fault="+tc.fault)
			rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: scenarios.All(), Filter: "^PUB-01$", ScenarioTimeout: 5 * time.Second}, target)
			if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, tc.wantSub) {
				t.Fatalf("Runner %s expected StatusFail(%q), got %+v", tc.fault, tc.wantSub, rep.Scenarios)
			}
			assertAllPIDsDead(t, pidLog, 1, false)
		})
	}
}

func TestRPCFaultDetection_MissingExpectedRPCs_CLIAndRunner(t *testing.T) {
	for _, tc := range []struct {
		name, fault, filter string
		to                  time.Duration
		st                  harness.Status
		ids                 []string
		sub                 string
	}{
		{"CLI_MissingPublish_PUB01_StatusFail", "missing-rpc", "^PUB-01$", 5 * time.Second, harness.StatusFail, []string{"PUB-01"}, "call count mismatch"},
		{"CLI_MissingAcknowledge_PULL04_ImmediateFail", "missing-ack", "^PULL-04$", 5 * time.Second, harness.StatusFail, []string{"PULL-04"}, "call count mismatch"},
		{"CLI_MissingStreamingPull_BoundedTimeout", "missing-streaming-pull", "^(PULL-01|TRAC-03)$", 500 * time.Millisecond, harness.StatusTimeout, []string{"PULL-01", "TRAC-03"}, "TIMEOUT:"},
		{"CLI_MissingAcknowledge_PULL01_BoundedTimeout", "missing-ack", "^PULL-01$", 500 * time.Millisecond, harness.StatusTimeout, []string{"PULL-01"}, "TIMEOUT:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runFaultCLI(t, tc.fault, tc.filter, tc.to, tc.st, tc.ids, tc.sub)
		})
	}

	t.Run("Runner_NonBlockingMissingStreamingPull_ImmediateStatusFail", func(t *testing.T) {
		pidLog := filepath.Join(t.TempDir(), "pids.log")
		trackPIDLog(t, pidLog)
		sc := harness.Scenario{
			ID: "CUSTOM-MISS-SP", Name: "Immediate Missing StreamingPull", Category: "pull",
			Run: func(ctx context.Context, env *harness.ScenarioEnv) error {
				sub := env.SubscriptionPath(env.UniqueName("miss-sp"))
				stream, err := env.Client.SubscriberSession(ctx)
				if err != nil {
					return err
				}
				if err := stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Start{Start: &proxypb.StartSubscriberCommand{Endpoint: env.Endpoint(), Subscription: sub, ProjectId: env.ProjectID}}}); err != nil {
					return err
				}
				if _, err := stream.Recv(); err != nil {
					return err
				}
				if err := stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: &proxypb.StopSubscriberCommand{}}}); err != nil {
					return err
				}
				_, _ = stream.Recv()
				_ = stream.CloseSend()
				return fake.VerifySequence(env.Fake.Snapshot(), fake.DefaultVerifyOptions(),
					fake.ExpectStreamingPullInitial("init", fake.StreamingPullInitialExpectation{Subscription: sub}),
				)
			},
		}
		target := helperTargetSpec(t, "miss-sp", pidLog, "--fault=missing-rpc")
		rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: []harness.Scenario{sc}, ScenarioTimeout: 5 * time.Second}, target)
		if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, "call count mismatch") {
			t.Fatalf("expected immediate StatusFail on missing StreamingPull, got rep=%+v", rep)
		}
		assertAllPIDsDead(t, pidLog, 1, false)
	})
}

func runSingleAckSubSession(ctx context.Context, env *harness.ScenarioEnv, sub string, wantAcks int) error {
	stream, err := env.Client.SubscriberSession(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Start{Start: &proxypb.StartSubscriberCommand{Endpoint: env.Endpoint(), Subscription: sub, ProjectId: env.ProjectID}}}); err != nil {
		return err
	}
	if _, err := stream.Recv(); err != nil {
		return err
	}
	sh, err := env.Fake.WaitForStreamingPull(ctx)
	if err != nil {
		return err
	}
	if err := sh.PushMessages([]*pubsubpb.ReceivedMessage{{AckId: "a1", Message: &pubsubpb.PubsubMessage{MessageId: "m1", Data: []byte("d")}}}); err != nil {
		return err
	}
	ev, err := stream.Recv()
	if err != nil {
		return err
	}
	_ = stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Settle{Settle: &proxypb.SettleMessageCommand{DeliveryToken: ev.GetMessageDelivered().GetDeliveryToken(), Action: proxypb.SettleMessageCommand_SETTLE_ACTION_ACK}}})
	if err := env.Fake.WaitFor(ctx, func(s fake.Snapshot) bool { return len(s.AckCalls()) >= wantAcks }); err != nil {
		return err
	}
	_ = stream.Send(&proxypb.SubscriberCommand{Command: &proxypb.SubscriberCommand_Stop{Stop: &proxypb.StopSubscriberCommand{}}})
	_, _ = stream.Recv()
	_ = stream.CloseSend()
	return nil
}

func TestRPCFaultDetection_OutOfOrderAndUnexpectedRPCs_CLIAndRunner(t *testing.T) {
	outOfOrderAndExtra := []struct {
		name, fault, filter string
		wantIDs             []string
		wantSub             string
	}{
		{"OutOfOrderPublishFIFO", "out-of-order-publish", "^PORD-02$", []string{"PORD-02"}, "ordered Publish FIFO mismatch"},
		{"ConcurrentSameOrderingKey", "concurrent-same-key", "^PORD-02$", []string{"PORD-02"}, "exceeds maxAllowed 1"},
		{"PrematureAckBeforeSettle", "premature-ack", "^(PULL-01|TRAC-03)$", []string{"PULL-01", "TRAC-03"}, "premature ack"},
		{"DuplicateAcknowledge", "duplicate-ack", "^(PULL-01|TRAC-03)$", []string{"PULL-01", "TRAC-03"}, "ack_id"},
		{"UnscriptedModifyAckDeadline_PUB01_PULL04", "extra-modack-rpc", "^(PUB-01|PULL-04)$", []string{"PUB-01", "PULL-04"}, "ModifyAckDeadline"},
		{"InvalidModifyAckDeadline_PULL01", "out-of-bounds-modack", "^PULL-01$", []string{"PULL-01"}, "outside"},
		{"UnexpectedPullDuringPublish", "extra-pull-rpc", "^(PUB-01|PORD-02)$", []string{"PUB-01", "PORD-02"}, "Subscriber/Pull"},
		{"UnregisteredSchemaServiceRPC", "unexpected-schema-rpc", "^(PUB-01|PULL-01)$", []string{"PUB-01", "PULL-01"}, "/google.pubsub.v1.SchemaService/GetSchema"},
		{"UnregisteredAdminCreateTopicRPC", "unexpected-admin-rpc", "^(PUB-01|PULL-01)$", []string{"PUB-01", "PULL-01"}, "/google.pubsub.v1.Publisher/CreateTopic"},
	}
	for _, tc := range outOfOrderAndExtra {
		t.Run("CLI_"+tc.name, func(t *testing.T) {
			runFaultCLI(t, tc.fault, tc.filter, 5*time.Second, harness.StatusFail, tc.wantIDs, tc.wantSub)
		})
	}

	for _, tc := range []struct {
		name, id, fault, wantSub string
		wantAcks                 int
		verify                   func(fake.Snapshot, string) error
	}{
		{
			"Runner_AckBeforePullSequenceViolation", "CUSTOM-ACK-BEFORE-PULL", "ack-before-pull", "method mismatch", 2,
			func(s fake.Snapshot, sub string) error {
				return fake.VerifySequence(s, fake.DefaultVerifyOptions(),
					fake.ExpectStreamingPullInitial("init", fake.StreamingPullInitialExpectation{Subscription: sub}),
					fake.ExpectAck("ack", fake.AckSubscription(sub), fake.AckStatusCode(codes.OK)),
				)
			},
		},
		{
			"Runner_DisallowUnscriptedValidModifyAckDeadlineInStreamingPull", "CUSTOM-NO-MODACK-STREAM", "modack-lease-ext", "disallowed RPC", 1,
			func(s fake.Snapshot, _ string) error {
				return fake.AssertNoUnexpectedRPCs(s, fake.MethodStreamingPull, fake.MethodAcknowledge)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pidLog := filepath.Join(t.TempDir(), "pids.log")
			trackPIDLog(t, pidLog)
			sc := harness.Scenario{
				ID: tc.id, Name: tc.name, Category: "pull",
				Run: func(ctx context.Context, env *harness.ScenarioEnv) error {
					sub := env.SubscriptionPath(env.UniqueName(tc.fault))
					if err := runSingleAckSubSession(ctx, env, sub, tc.wantAcks); err != nil {
						return err
					}
					return tc.verify(env.Fake.Snapshot(), sub)
				},
			}
			target := helperTargetSpec(t, tc.fault, pidLog, "--fault="+tc.fault)
			rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: []harness.Scenario{sc}, ScenarioTimeout: 5 * time.Second}, target)
			if len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, tc.wantSub) {
				t.Fatalf("%s expected StatusFail(%q), got rep=%+v", tc.name, tc.wantSub, rep)
			}
			assertAllPIDsDead(t, pidLog, 1, false)
		})
	}
}

func TestMutationBlindspots_CompressionTelemetryKeepaliveAndEOD(t *testing.T) {
	pubOne := func(ctx context.Context, env *harness.ScenarioEnv) string {
		topic := env.TopicPath(env.UniqueName("m"))
		cr, _ := env.Client.CreatePublisher(ctx, &proxypb.CreatePublisherRequest{Endpoint: env.Endpoint(), Topic: topic, ProjectId: env.ProjectID, CountThreshold: proto.Int64(1)})
		_, _ = env.Client.Publish(ctx, &proxypb.PublishRequest{PublisherId: cr.GetPublisherId(), Data: []byte(strings.Repeat("z", 300))})
		_, _ = env.Client.ClosePublisher(ctx, &proxypb.ClosePublisherRequest{PublisherId: cr.GetPublisherId()})
		return topic
	}
	chk := func(name, fault, wantSub string, fn func(context.Context, *harness.ScenarioEnv) error) {
		t.Run(name, func(t *testing.T) {
			pidLog := filepath.Join(t.TempDir(), "pids.log")
			trackPIDLog(t, pidLog)
			target := helperTargetSpec(t, name, pidLog, "--profile=go", "--fault="+fault)
			if rep := mustRunTarget(t, context.Background(), harness.RunConfig{Targets: []harness.TargetSpec{target}, Scenarios: []harness.Scenario{{ID: "MUT-" + name, Name: name, Category: "mut", Run: fn}}, ScenarioTimeout: 5 * time.Second}, target); len(rep.Scenarios) != 1 || rep.Scenarios[0].Status != harness.StatusFail || !strings.Contains(rep.Scenarios[0].Error, wantSub) {
				t.Fatalf("%s expected FAIL(%q), got %+v", name, wantSub, rep.Scenarios)
			}
			assertAllPIDsDead(t, pidLog, 1, false)
		})
	}
	chk("MissingCompression", "", "compression mismatch", func(ctx context.Context, env *harness.ScenarioEnv) error {
		topic := pubOne(ctx, env)
		return fake.VerifySequence(env.Fake.Snapshot(), fake.DefaultVerifyOptions(), fake.ExpectPublish("gzip", fake.PublishTopic(topic), fake.PublishCompression("gzip")))
	})
	chk("MissingTelemetry", "", "PublishOperation telemetry", func(ctx context.Context, env *harness.ScenarioEnv) error {
		_ = pubOne(ctx, env)
		if fake.VerifySequence(env.Fake.Snapshot(), fake.DefaultVerifyOptions(), fake.ExpectPublish("tel", fake.PublishTelemetry(1, true))) == nil {
			return env.Failf("expected telemetry fail")
		}
		return fake.AssertTelemetryHedgeSequence(env.Fake.Snapshot(), []int32{0}, true)
	})
	chk("MissingKeepaliveAndModAckBounds", "out-of-bounds-modack", "keepalive", func(ctx context.Context, env *harness.ScenarioEnv) error {
		_ = runSingleAckSubSession(ctx, env, env.SubscriptionPath(env.UniqueName("m")), 1)
		if fake.AssertAckAndModAckWithinBounds(env.Fake.Snapshot()) == nil {
			return env.Failf("expected bounds fail")
		}
		return fake.AssertKeepalivePings(env.Fake.Snapshot(), 1)
	})
	chk("FaultyEODSplitRetry", "duplicate-ack", "improperly retried", func(ctx context.Context, env *harness.ScenarioEnv) error {
		env.Fake.AddAckRule(fake.AckRule{Times: 1, EOD: fake.NewEODScript(codes.InvalidArgument, "eod", fake.EODReasonAckFailed, map[string]fake.EODAckBehavior{"a1": {PermanentReason: fake.EODPermanentInvalidAckID}})})
		_ = runSingleAckSubSession(ctx, env, env.SubscriptionPath(env.UniqueName("m")), 2)
		return fake.AssertEODSplitRetry(env.Fake.Snapshot(), []string{"a1"}, nil, nil)
	})
}
