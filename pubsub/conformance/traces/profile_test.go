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
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func cppSpan(tid, sid, pid, name string, kind proxypb.SpanKind, offset time.Duration) *proxypb.TraceSpan {
	base := time.Date(2026, 11, 15, 18, 0, 0, 0, time.UTC).Add(offset)
	return &proxypb.TraceSpan{
		TraceId:                  tid,
		SpanId:                   sid,
		ParentSpanId:             pid,
		TraceFlags:               1,
		Name:                     name,
		Kind:                     kind,
		StartTime:                timestamppb.New(base),
		EndTime:                  timestamppb.New(base.Add(2 * time.Millisecond)),
		StatusCode:               proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
		Attributes:               make(map[string]*proxypb.AttributeValue),
		InstrumentationScopeName: CppScopeName,
	}
}

func buildCppStreamingE2EFixture(abiV2 bool, eod bool, eodFail bool) ([]*proxypb.TraceSpan, *TraceExpectation) {
	pubTID := testTraceID(200)
	pubSID := testSpanID(2000)
	pub := cppSpan(pubTID, pubSID, "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 10*time.Millisecond)
	pub.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                strAV("p"),
		"messaging.system":              strAV("gcp_pubsub"),
		"messaging.destination.name":    strAV("t"),
		"messaging.batch.message_count": intAV(1),
		"messaging.operation.type":      strAV("publish"),
		"code.function.name":            strAV("BatchSink::AsyncPublish"),
		"gl-cpp.status_code":            strAV("OK"),
	}

	cTID := testTraceID(101)
	cSID := testSpanID(1010)
	create := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
	create.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                  strAV("p"),
		"messaging.system":                strAV("gcp_pubsub"),
		"messaging.destination.name":      strAV("t"),
		"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
		"messaging.operation.type":        strAV("create"),
		"messaging.message.envelope.size": intAV(25),
		"messaging.message.id":            strAV("msg-1"),
		"gl-cpp.status_code":              strAV("OK"),
	}
	pubStartEv := &proxypb.SpanEvent{Name: "gl-cpp.publish_start"}
	if abiV2 {
		create.Links = []*proxypb.SpanLink{{TraceId: pubTID, SpanId: pubSID}} // Empty attrs in C++ ABI v2!
	} else {
		pubStartEv.Attributes = map[string]*proxypb.AttributeValue{
			"gcp_pubsub.publish.trace_id": strAV(pubTID),
			"gcp_pubsub.publish.span_id":  strAV(pubSID),
		}
	}
	create.Events = []*proxypb.SpanEvent{
		{Name: "gl-cpp.added_to_batch"},
		pubStartEv,
		{Name: "gl-cpp.publish_end"},
	}
	pub.Links = []*proxypb.SpanLink{{TraceId: cTID, SpanId: cSID}}

	fc := cppSpan(cTID, testSpanID(1011), cSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, 1*time.Millisecond)
	fc.Attributes = map[string]*proxypb.AttributeValue{
		"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish"),
		"gl-cpp.status_code": strAV("OK"),
	}
	batch := cppSpan(cTID, testSpanID(1012), cSID, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, 2*time.Millisecond)
	batch.Attributes = map[string]*proxypb.AttributeValue{
		"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish"),
		"gl-cpp.status_code": strAV("OK"),
	}
	flush := cppSpan(testTraceID(299), testSpanID(2999), "", "pubsub::Publisher::Flush", proxypb.SpanKind_SPAN_KIND_CLIENT, 3*time.Millisecond)
	grpcPub := cppSpan(pubTID, testSpanID(2005), pubSID, "google.pubsub.v1.Publisher/Publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 11*time.Millisecond)

	wireTP := fmt.Sprintf("00-%s-%s-01", cTID, cSID)

	// Subscribe tree (Consumer -> Internal concurrency / "subscriber scheduler" / process).
	subSID := testSpanID(3000)
	sub := cppSpan(cTID, subSID, cSID, "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, 20*time.Millisecond)
	sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
	sub.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                                          strAV("p"),
		"messaging.system":                                        strAV("gcp_pubsub"),
		"messaging.destination.name":                              strAV("s"),
		"messaging.operation.type":                                strAV("subscribe"),
		"messaging.message.id":                                    strAV("msg-1"),
		"messaging.message.envelope.size":                         intAV(25),
		"messaging.gcp_pubsub.message.ack_id":                     strAV("ack-1"),
		"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(eod),
		"messaging.gcp_pubsub.result":                             strAV("ack"),
	}
	sub.Events = []*proxypb.SpanEvent{
		{Name: "gl-cpp.ack_start"},
		{Name: "gl-cpp.message_ack"},
		{Name: "gl-cpp.ack_end"},
	}

	cc := cppSpan(cTID, testSpanID(3001), subSID, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 21*time.Millisecond)
	cc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
	cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}

	sched := cppSpan(cTID, testSpanID(3002), subSID, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, 22*time.Millisecond)
	sched.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
	sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}

	proc := cppSpan(cTID, testSpanID(3003), subSID, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, 23*time.Millisecond)
	proc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
	proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}

	ackTID := testTraceID(400)
	ackSID := testSpanID(4000)
	ack := cppSpan(ackTID, ackSID, "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 30*time.Millisecond)
	ack.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                                strAV("p"),
		"messaging.system":                              strAV("gcp_pubsub"),
		"messaging.destination.name":                    strAV("s"),
		"code.function.name":                            strAV("pubsub::AckHandler::ack"),
		"messaging.gcp_pubsub.message.ack_id":           strAV("ack-1"),
		"messaging.gcp_pubsub.subscription.template":    strAV("projects/p/subscriptions/s"),
		"messaging.operation.type":                      strAV("settle"),
		"messaging.gcp_pubsub.message.delivery_attempt": intAV(1),
		"gl-cpp.status_code":                            strAV("OK"),
	}
	if eodFail {
		ack.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack.StatusMessage = "permission denied"
		ack.Attributes["gl-cpp.status_code"] = strAV("PERMISSION_DENIED")
	}
	ack.Links = []*proxypb.SpanLink{{TraceId: cTID, SpanId: subSID}}

	spans := []*proxypb.TraceSpan{create, fc, batch, flush, pub, grpcPub, sub, cc, sched, proc, ack}
	exp := &TraceExpectation{
		Topic:        "projects/p/topics/t",
		Subscription: "projects/p/subscriptions/s",
		Publish: &PublishExpectation{
			MessageCount:     1,
			BatchCount:       1,
			MessageIDs:       []string{"msg-1"},
			WireTraceparents: []string{wireTP},
		},
		Subscribe: &SubscribeExpectation{
			MessageCount:     1,
			WireTraceparents: []string{wireTP},
			AckIDs:           []string{"ack-1"},
			MessageIDs:       []string{"msg-1"},
			ExactlyOnce:      BoolPtr(eod),
			ExpectedSettlements: []SettlementExpectation{{
				Operation:   "ack",
				ExpectError: eodFail,
			}},
		},
	}
	return spans, exp
}

func TestProfileForLanguage(t *testing.T) {
	for _, lang := range []string{"cpp", "c++", "C++", " cpp "} {
		if p := ProfileForLanguage(lang); p.Name != ProfileCPP {
			t.Fatalf("ProfileForLanguage(%q) = %q, want %q", lang, p.Name, ProfileCPP)
		}
	}
	for _, lang := range []string{"go", "Go", "", "rust", "unknown"} {
		if p := ProfileForLanguage(lang); p.Name != ProfileGo {
			t.Fatalf("ProfileForLanguage(%q) = %q, want %q", lang, p.Name, ProfileGo)
		}
	}
}

func TestProfiles_GoStrictRejectsCppAndCppRecordsDeviations(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	t.Run("Go canonical rejects C++ graph", func(t *testing.T) {
		spans, exp := buildCppStreamingE2EFixture(true, false, false)
		res := Verify(goCaps, spans, exp)
		if res.Status != StatusFail {
			t.Fatalf("expected Go canonical to FAIL on C++ trace graph, got PASS")
		}
		if len(res.Failures) < 5 {
			t.Fatalf("expected multiple violations for Go vs C++, got:\n%s", res.Format())
		}
	})

	t.Run("ProfileCpp accepts ABI v2 and records deduplicated deviations", func(t *testing.T) {
		spans, exp := buildCppStreamingE2EFixture(true, true, false)
		res := Verify(cppCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp PASS on ABI v2 fixture, got:\n%s", res.Format())
		}
		wantRules := map[string]bool{
			"scope.instrumentation_name":      true,
			"span_kind.flow_control":          true,
			"span_kind.publish":               true,
			"span_kind.subscribe":             true,
			"span_kind.process":               true,
			"span_name.subscriber_scheduler":  true,
			"attr.code_function_key":          true,
			"attr.envelope_size_vs_body_size": true,
			"attr.eos_key_name":               true,
			"attr.result_value_form":          true,
			"links.missing_operation_name":    true,
			"links.subscribe_unidirectional":  true,
			"cardinality.per_message_settle":  true,
			"spans.cpp_grpc_and_lifecycle":    true,
		}
		gotRules := make(map[string]bool)
		for _, d := range res.Deviations {
			gotRules[d.Rule] = true
		}
		for r := range wantRules {
			if !gotRules[r] {
				t.Errorf("missing expected Deviation rule %q; got %+v", r, res.Deviations)
			}
		}
	})

	t.Run("ProfileCpp accepts ABI v1 event attrs fallback and EOD error unset subscribe", func(t *testing.T) {
		spans, exp := buildCppStreamingE2EFixture(false, true, true)
		res := Verify(cppCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp PASS on ABI v1 + EOD error, got:\n%s", res.Format())
		}
		gotRules := make(map[string]bool)
		for _, d := range res.Deviations {
			gotRules[d.Rule] = true
		}
		for _, want := range []string{"links.abi_v1_event_attrs_fallback", "status.subscribe_unset_on_eod_error", "attr.result_set_on_eod_fail"} {
			if !gotRules[want] {
				t.Errorf("missing expected Deviation %q; got %+v", want, res.Deviations)
			}
		}
	})

	t.Run("ProfileCpp accepts 129-message >128 fan-out publish #0 and #1", func(t *testing.T) {
		const n = 129
		pubTID := testTraceID(7000)
		pubSID := testSpanID(70000)
		pub := cppSpan(pubTID, pubSID, "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		pub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                strAV("p"),
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV("t"),
			"messaging.batch.message_count": intAV(n),
			"messaging.operation.type":      strAV("publish"),
			"code.function.name":            strAV("BatchSink::AsyncPublish"),
			"gl-cpp.status_code":            strAV("OK"),
		}
		child0 := cppSpan(pubTID, testSpanID(70001), pubSID, "publish #0", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		child1 := cppSpan(pubTID, testSpanID(70002), pubSID, "publish #1", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)

		var spans []*proxypb.TraceSpan
		spans = append(spans, pub, child0, child1)
		var msgs []PublishMessageExpectation
		for i := 0; i < n; i++ {
			cTID := testTraceID(8000 + i)
			cSID := testSpanID(80000 + i)
			cs := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(i)*time.Microsecond)
			cs.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV("p"),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV("t"),
				"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
				"messaging.operation.type":        strAV("create"),
				"messaging.message.envelope.size": intAV(24),
				"messaging.message.id":            strAV(fmt.Sprintf("m-%d", i)),
				"gl-cpp.status_code":              strAV("OK"),
			}
			cs.Events = []*proxypb.SpanEvent{
				{Name: "gl-cpp.added_to_batch"},
				{
					Name: "gl-cpp.publish_start",
					Attributes: map[string]*proxypb.AttributeValue{
						"gcp_pubsub.publish.trace_id": strAV(pubTID),
						"gcp_pubsub.publish.span_id":  strAV(pubSID),
					},
				},
				{Name: "gl-cpp.publish_end"},
			}
			fc := cppSpan(cTID, testSpanID(90000+i), cSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
			fc.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish")}
			bc := cppSpan(cTID, testSpanID(95000+i), cSID, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
			bc.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish")}

			link := &proxypb.SpanLink{TraceId: cTID, SpanId: cSID}
			if i < 128 {
				child0.Links = append(child0.Links, link)
			} else {
				child1.Links = append(child1.Links, link)
			}
			spans = append(spans, cs, fc, bc)
			msgs = append(msgs, PublishMessageExpectation{
				MessageID:       fmt.Sprintf("m-%d", i),
				WireTraceparent: fmt.Sprintf("00-%s-%s-01", cTID, cSID),
			})
		}

		res := Verify(cppCaps, spans, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{BatchCount: 1, Messages: msgs},
		})
		if !res.Passed() {
			t.Fatalf("expected 129-message fan-out PASS, got:\n%s", res.Format())
		}
		foundFanoutDev := false
		for _, d := range res.Deviations {
			if d.Rule == "links.publish_fanout_children" {
				foundFanoutDev = true
			}
		}
		if !foundFanoutDev {
			t.Fatalf("expected links.publish_fanout_children deviation, got %+v", res.Deviations)
		}
	})

	t.Run("ProfileCpp accepts kIgnored omitted flow control and BlockingPublisher", func(t *testing.T) {
		cTID := testTraceID(333)
		cSID := testSpanID(3333)
		cs := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
		cs.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                  strAV("p"),
			"messaging.system":                strAV("gcp_pubsub"),
			"messaging.destination.name":      strAV("t"),
			"code.function.name":              strAV("pubsub::BlockingPublisher::Publish"),
			"messaging.operation.type":        strAV("create"),
			"messaging.message.envelope.size": intAV(24),
			"messaging.message.id":            strAV("blk-1"),
			"gl-cpp.status_code":              strAV("OK"),
		}
		gRPC := cppSpan(cTID, testSpanID(3334), cSID, "google.pubsub.v1.Publisher/Publish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		resBlk := Verify(cppCaps, []*proxypb.TraceSpan{cs, gRPC}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:      1,
				MessageIDs:        []string{"blk-1"},
				BlockingPublisher: true,
			},
		})
		if !resBlk.Passed() {
			t.Fatalf("expected BlockingPublisher PASS, got:\n%s", resBlk.Format())
		}

		// kIgnored streaming omits "publisher flow control".
		spans, exp := buildCppStreamingE2EFixture(true, false, false)
		var noFC []*proxypb.TraceSpan
		for _, sp := range spans {
			if sp.GetName() != "publisher flow control" {
				noFC = append(noFC, sp)
			}
		}
		exp.Publish.FlowControlIgnored = true
		resIgn := Verify(cppCaps, noFC, exp)
		if !resIgn.Passed() {
			t.Fatalf("expected kIgnored omitted FC PASS, got:\n%s", resIgn.Format())
		}
	})

	t.Run("ProfileCpp validates UnaryPull in ABI v2 and ABI v1 modes", func(t *testing.T) {
		for _, abiV2 := range []bool{true, false} {
			createTID := testTraceID(410)
			createSID := testSpanID(4100)
			wireTP := fmt.Sprintf("00-%s-%s-01", createTID, createSID)

			recvTID := testTraceID(411)
			recvSID := testSpanID(4110)
			recv := cppSpan(recvTID, recvSID, "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
			recv.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV("p"),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV("s"),
				"messaging.operation.type":        strAV("receive"),
				"code.function.name":              strAV("pubsub::SubscriberConnection::Pull"),
				"messaging.message.id":            strAV("up-1"),
				"messaging.message.envelope.size": intAV(30),
				"gl-cpp.status_code":              strAV("OK"),
			}

			mod := cppSpan(testTraceID(412), testSpanID(4120), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
			mod.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                    strAV("p"),
				"messaging.system":                                  strAV("gcp_pubsub"),
				"messaging.destination.name":                        strAV("s"),
				"messaging.operation.type":                          strAV("modack"),
				"code.function.name":                                strAV("pubsub::PullLeaseManager::ExtendLease"),
				"messaging.gcp_pubsub.message.ack_id":               strAV("uack-1"),
				"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(30),
			}

			if abiV2 {
				recv.Links = []*proxypb.SpanLink{{TraceId: createTID, SpanId: createSID}}
				mod.Links = []*proxypb.SpanLink{{TraceId: recvTID, SpanId: recvSID}}
			} else {
				recv.Attributes["gcp_pubsub.create.trace_id"] = strAV(createTID)
				recv.Attributes["gcp_pubsub.create.span_id"] = strAV(createSID)
				mod.Attributes["gcp_pubsub.receive.trace_id"] = strAV(recvTID)
				mod.Attributes["gcp_pubsub.receive.span_id"] = strAV(recvSID)
			}

			res := Verify(cppCaps, []*proxypb.TraceSpan{recv, mod}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull: &UnaryPullExpectation{
					Messages: []UnaryPullMessageExpectation{{
						MessageID:       "up-1",
						WireTraceparent: wireTP,
						Settlement:      &SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 30},
					}},
				},
			})
			if !res.Passed() {
				t.Fatalf("UnaryPull abiV2=%v expected PASS, got:\n%s", abiV2, res.Format())
			}

			// UnaryPull ack with delivery_attempt + error pull.
			ack := cppSpan(testTraceID(413), testSpanID(4130), "", "s ack", proxypb.SpanKind_SPAN_KIND_CLIENT, 2*time.Millisecond)
			ack.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                strAV("p"),
				"messaging.system":                              strAV("gcp_pubsub"),
				"messaging.destination.name":                    strAV("s"),
				"messaging.operation.type":                      strAV("ack"),
				"code.function.name":                            strAV("pubsub::PullAckHandler::ack"),
				"messaging.gcp_pubsub.message.ack_id":           strAV("uack-1"),
				"messaging.gcp_pubsub.message.delivery_attempt": intAV(2),
			}
			if abiV2 {
				ack.Links = []*proxypb.SpanLink{{TraceId: recvTID, SpanId: recvSID}}
			} else {
				ack.Attributes["gcp_pubsub.receive.trace_id"] = strAV(recvTID)
				ack.Attributes["gcp_pubsub.receive.span_id"] = strAV(recvSID)
			}
			resAck := Verify(cppCaps, []*proxypb.TraceSpan{recv, ack}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull: &UnaryPullExpectation{
					Messages: []UnaryPullMessageExpectation{{
						MessageID:       "up-1",
						DeliveryAttempt: 2,
						WireTraceparent: wireTP,
						Settlement:      &SettlementExpectation{Operation: "ack"},
					}},
				},
			})
			if !resAck.Passed() {
				t.Fatalf("UnaryPull ack abiV2=%v expected PASS, got:\n%s", abiV2, resAck.Format())
			}
		}
	})

	t.Run("ProfileCpp accepts FC sync-OK error, unsampled traceparent root fallback, and non-empty ordering key", func(t *testing.T) {
		cTID := testTraceID(450)
		cSID := testSpanID(4500)
		cs := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
		cs.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cs.StatusMessage = "fc full"
		cs.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                            strAV("p"),
			"messaging.system":                          strAV("gcp_pubsub"),
			"messaging.destination.name":                strAV("t"),
			"code.function.name":                        strAV("pubsub::PublisherConnection::Publish"),
			"messaging.operation.type":                  strAV("create"),
			"messaging.message.envelope.size":           intAV(30),
			"messaging.gcp_pubsub.message.ordering_key": strAV("k-1"),
			"gl-cpp.status_code":                        strAV("FAILED_PRECONDITION"),
		}
		fc := cppSpan(cTID, testSpanID(4501), cSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		fc.Attributes = map[string]*proxypb.AttributeValue{
			"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish"),
			"gl-cpp.status_code": strAV("OK"),
		}
		resFC := Verify(cppCaps, []*proxypb.TraceSpan{cs, fc}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:   1,
				OrderingKey:    "k-1",
				ExpectError:    true,
				ErrorKind:      PublishErrorFlowControl,
				ErrorSubstring: "fc full",
			},
		})
		if !resFC.Passed() {
			t.Fatalf("C++ FC sync-OK expected PASS, got:\n%s", resFC.Format())
		}

		// C++ unsampled wire traceparent (-00) falls back to root on <subID> subscribe.
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(460), testSpanID(4600))
		spansSub, expSub := buildCppStreamingE2EFixture(true, false, false)
		// Keep only subscribe tree and make subscribe root as C++ does for unsampled parent.
		var subOnly []*proxypb.TraceSpan
		for _, s := range spansSub {
			if s.GetName() == "s subscribe" {
				s.TraceId = testTraceID(461)
				s.ParentSpanId = ""
				subOnly = append(subOnly, s)
			} else if s.GetName() == "subscriber concurrency control" || s.GetName() == "subscriber scheduler" || s.GetName() == "s process" {
				s.TraceId = testTraceID(461)
				subOnly = append(subOnly, s)
			} else if s.GetName() == "s ack" {
				s.Links[0].TraceId = testTraceID(461)
				subOnly = append(subOnly, s)
			}
		}
		expSub.Publish = nil
		expSub.Topic = ""
		expSub.Subscribe.WireTraceparents = []string{unsampTP}
		resUnsamp := Verify(cppCaps, subOnly, expSub)
		if !resUnsamp.Passed() {
			t.Fatalf("C++ unsampled root fallback expected PASS, got:\n%s", resUnsamp.Format())
		}
	})

	t.Run("ProfileCpp still fails real C++ bugs", func(t *testing.T) {
		assertViolation := func(t *testing.T, res TraceResult, wantCode ViolationCode, wantRule string) {
			t.Helper()
			if res.Status != StatusFail {
				t.Fatalf("expected FAIL (%s/%s), got PASS", wantCode, wantRule)
			}
			for _, f := range res.Failures {
				if f.Code == wantCode && (wantRule == "" || f.Rule == wantRule) {
					return
				}
			}
			t.Fatalf("expected violation (%s, rule=%s), got:\n%s", wantCode, wantRule, res.Format())
		}

		spans, exp := buildCppStreamingE2EFixture(true, true, false)

		// 1. Missing publish -> create link.
		s1 := cloneSpans(spans)
		s1[4].Links = nil // pub
		assertViolation(t, Verify(cppCaps, s1, exp), CodeMissingLink, "publish.batch.forward_link")

		// 2. Missing both ABI v2 link and ABI v1 event attrs on create.
		s2 := cloneSpans(spans)
		s2[0].Links = nil
		assertViolation(t, Verify(cppCaps, s2, exp), CodeBrokenBidirectionalLink, "publish.bidirectional_link")

		// 3. Invalid "0000000000000000" ParentSpanId on root span.
		s3 := cloneSpans(spans)
		s3[4].ParentSpanId = "0000000000000000"
		assertViolation(t, Verify(cppCaps, s3, exp), CodeInvalidParentSpanID, "span.parent_span_id")

		// 4. Missing ERROR status on failed ack.
		spansErr, expErr := buildCppStreamingE2EFixture(true, true, true)
		spansErr[len(spansErr)-1].StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
		assertViolation(t, Verify(cppCaps, spansErr, expErr), CodeWrongStatusCode, "settlement.ack.status")

		// 5. >128 batch without "publish #N" splitting (with all 129 linked create spans so batch.fanout_required is isolated).
		const fanN = 129
		pubBig := cppSpan(testTraceID(777), testSpanID(7777), "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		pubBig.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                strAV("p"),
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV("t"),
			"messaging.batch.message_count": intAV(fanN),
			"messaging.operation.type":      strAV("publish"),
			"code.function.name":            strAV("BatchSink::AsyncPublish"),
			"gl-cpp.status_code":            strAV("OK"),
		}
		fanSpans := []*proxypb.TraceSpan{pubBig}
		var fanMsgs []PublishMessageExpectation
		for i := 0; i < fanN; i++ {
			cTID := testTraceID(12000 + i)
			cSID := testSpanID(120000 + i)
			cs := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(i)*time.Microsecond)
			cs.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV("p"),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV("t"),
				"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
				"messaging.operation.type":        strAV("create"),
				"messaging.message.envelope.size": intAV(24),
				"messaging.message.id":            strAV(fmt.Sprintf("mf-%d", i)),
				"gl-cpp.status_code":              strAV("OK"),
			}
			cs.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}, {Name: "gl-cpp.publish_start"}, {Name: "gl-cpp.publish_end"}}
			cs.Links = []*proxypb.SpanLink{{TraceId: pubBig.TraceId, SpanId: pubBig.SpanId}}
			pubBig.Links = append(pubBig.Links, &proxypb.SpanLink{TraceId: cTID, SpanId: cSID})
			fcOne := cppSpan(cTID, testSpanID(130000+i), cSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
			fcOne.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish")}
			bcOne := cppSpan(cTID, testSpanID(140000+i), cSID, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
			bcOne.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish")}
			fanSpans = append(fanSpans, cs, fcOne, bcOne)
			fanMsgs = append(fanMsgs, PublishMessageExpectation{
				MessageID:       fmt.Sprintf("mf-%d", i),
				WireTraceparent: fmt.Sprintf("00-%s-%s-01", cTID, cSID),
			})
		}
		assertViolation(t, Verify(cppCaps, fanSpans, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{BatchCount: 1, Messages: fanMsgs},
		}), CodeMissingSpan, "publish.batch.fanout_required")

		// 6. ExpectNoSpans fails even for benign C++ spans when monitoring was disabled on publisher/subscriber.
		assertViolation(t, Verify(cppCaps, spans, ExpectNoTraces()), CodeUnexpectedSpan, "traces.expect_no_spans")
	})

	t.Run("ProfileCpp RejectsWithOrderingKey (PORD-01) and CorkedError (PORD-03)", func(t *testing.T) {
		buildLocalReject := func(withAddedEvent bool, withFC bool) []*proxypb.TraceSpan {
			cTID := testTraceID(470)
			cSID := testSpanID(4700)
			cs := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
			cs.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			cs.StatusMessage = "ordering rejected"
			cs.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                            strAV("p"),
				"messaging.system":                          strAV("gcp_pubsub"),
				"messaging.destination.name":                strAV("t"),
				"code.function.name":                        strAV("pubsub::PublisherConnection::Publish"),
				"messaging.operation.type":                  strAV("create"),
				"messaging.message.envelope.size":           intAV(30),
				"messaging.gcp_pubsub.message.ordering_key": strAV("k1"),
				"gl-cpp.status_code":                        strAV("INVALID_ARGUMENT"),
			}
			if withAddedEvent {
				cs.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}}
			}
			bc := cppSpan(cTID, testSpanID(4702), cSID, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, 2*time.Millisecond)
			bc.Attributes = map[string]*proxypb.AttributeValue{
				"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish"),
				"gl-cpp.status_code": strAV("OK"),
			}
			out := []*proxypb.TraceSpan{cs, bc}
			if withFC {
				fc := cppSpan(cTID, testSpanID(4701), cSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
				fc.Attributes = map[string]*proxypb.AttributeValue{
					"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish"),
					"gl-cpp.status_code": strAV("OK"),
				}
				out = append(out, fc)
			}
			return out
		}

		// PORD-01: passes under PublishErrorAuto, PublishErrorPreValidation, and PublishErrorOrderingRejected.
		pord1 := buildLocalReject(false, true)
		for _, kind := range []PublishErrorKind{PublishErrorAuto, PublishErrorPreValidation, PublishErrorOrderingRejected} {
			res := Verify(cppCaps, pord1, &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					MessageCount:   1,
					OrderingKey:    "k1",
					ExpectError:    true,
					ErrorKind:      kind,
					ErrorSubstring: "ordering rejected",
				},
			})
			if !res.Passed() {
				t.Fatalf("PORD-01 with ErrorKind=%q expected PASS, got:\n%s", kind, res.Format())
			}
		}
		// PORD-01 with kIgnored (omits flow control).
		resIgn := Verify(cppCaps, buildLocalReject(false, false), &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:       1,
				OrderingKey:        "k1",
				FlowControlIgnored: true,
				ExpectError:        true,
				ErrorKind:          PublishErrorAuto,
				ErrorSubstring:     "ordering rejected",
			},
		})
		if !resIgn.Passed() {
			t.Fatalf("PORD-01 kIgnored expected PASS, got:\n%s", resIgn.Format())
		}
		// PORD-03 (CorkedError emits gl-cpp.added_to_batch before returning CorkedError).
		resCork := Verify(cppCaps, buildLocalReject(true, true), &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:   1,
				OrderingKey:    "k1",
				ExpectError:    true,
				ErrorKind:      PublishErrorAuto,
				ErrorSubstring: "ordering rejected",
			},
		})
		if !resCork.Passed() {
			t.Fatalf("PORD-03 expected PASS, got:\n%s", resCork.Format())
		}
	})

	t.Run("ProfileCpp fanout child wrong scope, UnaryPull error common attrs & AckID", func(t *testing.T) {
		// Fan-out child wrong scope fails.
		pubTID := testTraceID(15000)
		pubSID := testSpanID(150000)
		pub := cppSpan(pubTID, pubSID, "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		pub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                strAV("p"),
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV("t"),
			"messaging.batch.message_count": intAV(129),
			"messaging.operation.type":      strAV("publish"),
			"code.function.name":            strAV("BatchSink::AsyncPublish"),
			"gl-cpp.status_code":            strAV("OK"),
		}
		child0 := cppSpan(pubTID, testSpanID(150001), pubSID, "publish #0", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		child0.InstrumentationScopeName = "wrong-scope"
		child1 := cppSpan(pubTID, testSpanID(150002), pubSID, "publish #1", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		fanSpans := []*proxypb.TraceSpan{pub, child0, child1}
		var fanMsgs []PublishMessageExpectation
		for i := 0; i < 129; i++ {
			cTID := testTraceID(16000 + i)
			cSID := testSpanID(160000 + i)
			cs := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(i)*time.Microsecond)
			cs.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV("p"),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV("t"),
				"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
				"messaging.operation.type":        strAV("create"),
				"messaging.message.envelope.size": intAV(24),
				"messaging.message.id":            strAV(fmt.Sprintf("fs-%d", i)),
				"gl-cpp.status_code":              strAV("OK"),
			}
			cs.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}, {Name: "gl-cpp.publish_start"}, {Name: "gl-cpp.publish_end"}}
			cs.Links = []*proxypb.SpanLink{{TraceId: pubTID, SpanId: pubSID}}
			link := &proxypb.SpanLink{TraceId: cTID, SpanId: cSID}
			if i < 128 {
				child0.Links = append(child0.Links, link)
			} else {
				child1.Links = append(child1.Links, link)
			}
			fc := cppSpan(cTID, testSpanID(170000+i), cSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
			fc.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish")}
			bc := cppSpan(cTID, testSpanID(180000+i), cSID, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
			bc.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish")}
			fanSpans = append(fanSpans, cs, fc, bc)
			fanMsgs = append(fanMsgs, PublishMessageExpectation{MessageID: fmt.Sprintf("fs-%d", i), WireTraceparent: fmt.Sprintf("00-%s-%s-01", cTID, cSID)})
		}
		resFanScope := Verify(cppCaps, fanSpans, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{BatchCount: 1, Messages: fanMsgs},
		})
		if resFanScope.Status != StatusFail || resFanScope.Failures[0].Code != CodeWrongScope {
			foundScope := false
			for _, f := range resFanScope.Failures {
				if f.Code == CodeWrongScope && f.Rule == "publish.batch.fanout_scope" {
					foundScope = true
				}
			}
			if !foundScope {
				t.Fatalf("expected CodeWrongScope on fan-out child, got:\n%s", resFanScope.Format())
			}
		}

		// UnaryPull error checks common attrs (project_id & destination_name).
		errRecv := cppSpan(testTraceID(19000), testSpanID(190000), "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		errRecv.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		errRecv.StatusMessage = "pull failed"
		errRecv.Attributes = map[string]*proxypb.AttributeValue{
			"messaging.system":         strAV("gcp_pubsub"),
			"messaging.operation.type": strAV("receive"),
			"code.function.name":       strAV("pubsub::SubscriberConnection::Pull"),
		}
		resPullErr := Verify(cppCaps, []*proxypb.TraceSpan{errRecv}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{ExpectPullError: true, ErrorSubstring: "pull failed"},
		})
		if resPullErr.Status != StatusFail {
			t.Fatalf("expected FAIL on UnaryPull error missing common attrs, got PASS")
		}
		errRecv.Attributes["gcp.project_id"] = strAV("p")
		errRecv.Attributes["messaging.destination.name"] = strAV("s")
		if resOK := Verify(cppCaps, []*proxypb.TraceSpan{errRecv}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{ExpectPullError: true, ErrorSubstring: "pull failed"},
		}); !resOK.Passed() {
			t.Fatalf("UnaryPull error with common attrs expected PASS, got:\n%s", resOK.Format())
		}

		// UnaryPull AckID verification.
		recv := cppSpan(testTraceID(19100), testSpanID(191000), "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		recv.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                  strAV("p"),
			"messaging.system":                strAV("gcp_pubsub"),
			"messaging.destination.name":      strAV("s"),
			"messaging.operation.type":        strAV("receive"),
			"code.function.name":              strAV("pubsub::SubscriberConnection::Pull"),
			"messaging.message.id":            strAV("up-ack-1"),
			"messaging.message.envelope.size": intAV(24),
		}
		ack := cppSpan(testTraceID(19101), testSpanID(191001), "", "s ack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		ack.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                      strAV("p"),
			"messaging.system":                    strAV("gcp_pubsub"),
			"messaging.destination.name":          strAV("s"),
			"messaging.operation.type":            strAV("ack"),
			"code.function.name":                  strAV("pubsub::PullAckHandler::ack"),
			"messaging.gcp_pubsub.message.ack_id": strAV("actual-ack-id"),
		}
		ack.Links = []*proxypb.SpanLink{{TraceId: recv.TraceId, SpanId: recv.SpanId}}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{recv, ack}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:  "up-ack-1",
					AckID:      "actual-ack-id",
					Settlement: &SettlementExpectation{Operation: "ack"},
				}},
			},
		}); !res.Passed() {
			t.Fatalf("UnaryPull matching AckID expected PASS, got:\n%s", res.Format())
		}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{recv, ack}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:  "up-ack-1",
					AckID:      "wrong-ack-id",
					Settlement: &SettlementExpectation{Operation: "ack"},
				}},
			},
		}); res.Status != StatusFail {
			t.Fatal("UnaryPull mismatched AckID expected FAIL, got PASS")
		}

		// C++ Data + Attributes triggers cppEnvelopeSize minimum assertion on publish/subscribe/unary pull.
		spansE2E, expE2E := buildCppStreamingE2EFixture(true, false, false)
		spansE2E[0].Attributes["messaging.message.envelope.size"] = intAV(29) // >= 20 + len("hello") + len("k") + len("v") = 27
		spansE2E[6].Attributes["messaging.message.envelope.size"] = intAV(32) // >= 20 + len("hello") + len("k") + len("v") + len("msg-1") = 32
		expE2E.Publish.Messages = []PublishMessageExpectation{{
			MessageID:       "msg-1",
			Data:            []byte("hello"),
			Attributes:      map[string]string{"k": "v"},
			WireTraceparent: expE2E.Publish.WireTraceparents[0],
		}}
		expE2E.Subscribe.Messages = []SubscribeMessageExpectation{{
			MessageID:       "msg-1",
			AckID:           "ack-1",
			Data:            []byte("hello"),
			Attributes:      map[string]string{"k": "v"},
			WireTraceparent: expE2E.Subscribe.WireTraceparents[0],
			Settlements:     []SettlementExpectation{{Operation: "ack"}},
		}}
		if res := Verify(cppCaps, spansE2E, expE2E); !res.Passed() {
			t.Fatalf("C++ Data+Attributes cppEnvelopeSize expected PASS, got:\n%s", res.Format())
		}
	})
}

func TestProfileCpp_Iter3SymmetryAndUnaryPull(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	assertHasViolationLocal := func(t *testing.T, res TraceResult, wantCode ViolationCode, wantRule string) {
		t.Helper()
		if res.Status != StatusFail {
			t.Fatalf("expected FAIL (%s/%s), got PASS:\n%s", wantCode, wantRule, res.Format())
		}
		for _, f := range res.Failures {
			if f.Code == wantCode && (wantRule == "" || f.Rule == wantRule) {
				return
			}
		}
		t.Fatalf("expected violation (%s, rule=%s), got:\n%s", wantCode, wantRule, res.Format())
	}

	t.Run("C++ modack ExpectedBatchCount and Attack3A/3B/4G settlement symmetry", func(t *testing.T) {
		makeCppSub := func(seq int) (*proxypb.TraceSpan, *proxypb.TraceSpan, *proxypb.TraceSpan, *proxypb.TraceSpan) {
			tid := testTraceID(2100 + seq)
			subSID := testSpanID(21000 + seq*10)
			sub := cppSpan(tid, subSID, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
			sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sub.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                          strAV("p"),
				"messaging.system":                                        strAV("gcp_pubsub"),
				"messaging.destination.name":                              strAV("s"),
				"messaging.operation.type":                                strAV("subscribe"),
				"messaging.message.id":                                    strAV(fmt.Sprintf("cm-%d", seq)),
				"messaging.message.envelope.size":                         intAV(25),
				"messaging.gcp_pubsub.message.ack_id":                     strAV(fmt.Sprintf("ca-%d", seq)),
				"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(true),
				"messaging.gcp_pubsub.result":                             strAV("ack"),
			}
			sub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.ack_end"}, {Name: "gl-cpp.modack_start"}, {Name: "gl-cpp.modack_end"}}
			cc := cppSpan(tid, testSpanID(21001+seq*10), subSID, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			cc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			sched := cppSpan(tid, testSpanID(21002+seq*10), subSID, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			sched.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			proc := cppSpan(tid, testSpanID(21003+seq*10), subSID, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			proc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			return sub, cc, sched, proc
		}
		makeCppAck := func(seq int, sub *proxypb.TraceSpan) *proxypb.TraceSpan {
			ack := cppSpan(testTraceID(2200+seq), testSpanID(22000+seq), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 5*time.Millisecond)
			ack.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                             strAV("p"),
				"messaging.system":                           strAV("gcp_pubsub"),
				"messaging.destination.name":                 strAV("s"),
				"code.function.name":                         strAV("pubsub::AckHandler::ack"),
				"messaging.gcp_pubsub.message.ack_id":        strAV(fmt.Sprintf("ca-%d", seq)),
				"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
				"messaging.operation.type":                   strAV("settle"),
				"gl-cpp.status_code":                         strAV("OK"),
			}
			ack.Links = []*proxypb.SpanLink{{TraceId: sub.TraceId, SpanId: sub.SpanId}}
			return ack
		}
		makeCppModack := func(seq int, batchCount, deadline int64, subs ...*proxypb.TraceSpan) *proxypb.TraceSpan {
			mod := cppSpan(testTraceID(2300+seq), testSpanID(23000+seq), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, 2*time.Millisecond)
			mod.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			mod.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                    strAV("p"),
				"messaging.system":                                  strAV("gcp_pubsub"),
				"messaging.destination.name":                        strAV("s"),
				"messaging.operation.type":                          strAV("extend"),
				"messaging.batch.message_count":                     intAV(batchCount),
				"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(deadline),
			}
			for _, ss := range subs {
				mod.Links = append(mod.Links, &proxypb.SpanLink{TraceId: ss.TraceId, SpanId: ss.SpanId})
			}
			return mod
		}

		// 1. ExpectedBatchCount > 0 on C++ modack: 1 -> PASS, 2 -> FAIL.
		sub0, cc0, sc0, pr0 := makeCppSub(1)
		ack0 := makeCppAck(1, sub0)
		mod1 := makeCppModack(1, 1, 60, sub0)
		if res := Verify(cppCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack0, mod1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount: 1,
				ExactlyOnce:  BoolPtr(true),
				ExpectedSettlements: []SettlementExpectation{
					{Operation: "modack", AckDeadlineSeconds: 60, ExpectedBatchCount: 1},
					{Operation: "ack"},
				},
			},
		}); !res.Passed() {
			t.Fatalf("C++ modack ExpectedBatchCount=1 expected PASS, got:\n%s", res.Format())
		}
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack0, mod1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount: 1,
				ExactlyOnce:  BoolPtr(true),
				ExpectedSettlements: []SettlementExpectation{
					{Operation: "modack", AckDeadlineSeconds: 60, ExpectedBatchCount: 2},
					{Operation: "ack"},
				},
			},
		}), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		// 2. Attack3A C++: shared modack conflicting {60, 2} vs {30, 1}.
		subA, ccA, scA, prA := makeCppSub(2)
		subB, ccB, scB, prB := makeCppSub(3)
		ackA := makeCppAck(2, subA)
		ackB := makeCppAck(3, subB)
		modShared := makeCppModack(2, 2, 60, subA, subB)
		res3A := Verify(cppCaps, []*proxypb.TraceSpan{subA, ccA, scA, prA, subB, ccB, scB, prB, ackA, ackB, modShared}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(true),
				Messages: []SubscribeMessageExpectation{
					{AckID: "ca-2", Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, ExpectedBatchCount: 2}, {Operation: "ack"}}},
					{AckID: "ca-3", Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 30, ExpectedBatchCount: 1}, {Operation: "ack"}}},
				},
			},
		})
		assertHasViolationLocal(t, res3A, CodeWrongAttributeValue, "settlement.modack.attr.ack_deadline_seconds")
		assertHasViolationLocal(t, res3A, CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		// 3. Attack3B & Attack4G C++: dangling & duplicate links on s ack.
		ackDangle := cloneSpans([]*proxypb.TraceSpan{ack0})[0]
		ackDangle.Links = append(ackDangle.Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ackDangle}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExactlyOnce: BoolPtr(true), StrictSettlementOps: true, ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}), CodeMissingLink, "subscribe.settlement.dangling_link")

		ackDup := cloneSpans([]*proxypb.TraceSpan{ack0})[0]
		ackDup.Links = append(ackDup.Links, &proxypb.SpanLink{TraceId: sub0.TraceId, SpanId: sub0.SpanId})
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ackDup}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExactlyOnce: BoolPtr(true), StrictSettlementOps: true, ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}), CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_link")
	})

	t.Run("C++ -00 Publish, mixed slot accounting, fanout 0-link chunk, Attack3C & Attack4E", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(2400), testSpanID(24000))
		pub0 := cppSpan(testTraceID(2401), testSpanID(24010), "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		pub0.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                strAV("p"),
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV("t"),
			"messaging.batch.message_count": intAV(1),
			"messaging.operation.type":      strAV("publish"),
			"code.function.name":            strAV("BatchSink::AsyncPublish"),
			"gl-cpp.status_code":            strAV("OK"),
		}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{pub0}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{Messages: []PublishMessageExpectation{{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true}}},
		}); !res.Passed() {
			t.Fatalf("C++ -00 Publish (BatchCount=0) expected PASS, got:\n%s", res.Format())
		}

		// Mixed -01 + -00 + bogus 0-link publish -> FAIL; dangling link -> FAIL; leaked create -> FAIL.
		spans, exp := buildCppStreamingE2EFixture(true, false, false)
		pubOnly := cloneSpans(spans[:5]) // create, fc, batch, flush, pub
		pubOnly[4].Attributes["messaging.batch.message_count"] = intAV(2)
		expMix := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{
					{MessageID: "msg-1", WireTraceparent: exp.Publish.WireTraceparents[0]},
					{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true},
				},
			},
		}
		if res := Verify(cppCaps, pubOnly, expMix); !res.Passed() {
			t.Fatalf("C++ mixed -01+-00 expected PASS, got:\n%s", res.Format())
		}

		bogusPub := cloneSpans([]*proxypb.TraceSpan{pub0})[0]
		bogusPub.TraceId = testTraceID(2409)
		bogusPub.SpanId = testSpanID(24090)
		assertHasViolationLocal(t, Verify(cppCaps, append(cloneSpans(pubOnly), bogusPub), expMix), CodeWrongAttributeValue, "publish.batch.attr.message_count")

		danglePub := cloneSpans(pubOnly)
		danglePub[4].Links = append(danglePub[4].Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolationLocal(t, Verify(cppCaps, danglePub, expMix), CodeMissingLink, "publish.batch.dangling_link")

		leakCreate := cloneSpans([]*proxypb.TraceSpan{pubOnly[0]})[0]
		leakCreate.TraceId = testTraceID(2499)
		leakCreate.SpanId = testSpanID(24990)
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{leakCreate, pub0}, &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish:               &PublishExpectation{Messages: []PublishMessageExpectation{{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true}}},
		}), CodeUnexpectedSpan, "publish.unsampled_dropped")

		// >128 fanout with chunk #0 all -00 (0 links) and chunk #1 with 1 sampled link -> PASS.
		pubTID := testTraceID(2500)
		pubSID := testSpanID(25000)
		pubBig := cppSpan(pubTID, pubSID, "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		pubBig.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                strAV("p"),
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV("t"),
			"messaging.batch.message_count": intAV(129),
			"messaging.operation.type":      strAV("publish"),
			"code.function.name":            strAV("BatchSink::AsyncPublish"),
			"gl-cpp.status_code":            strAV("OK"),
		}
		ch0 := cppSpan(pubTID, testSpanID(25001), pubSID, "publish #0", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		ch1 := cppSpan(pubTID, testSpanID(25002), pubSID, "publish #1", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Second)
		cLastTID := testTraceID(2599)
		cLastSID := testSpanID(25990)
		cLast := cppSpan(cLastTID, cLastSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
		cLast.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                  strAV("p"),
			"messaging.system":                strAV("gcp_pubsub"),
			"messaging.destination.name":      strAV("t"),
			"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
			"messaging.operation.type":        strAV("create"),
			"messaging.message.envelope.size": intAV(24),
			"messaging.message.id":            strAV("m-last"),
			"gl-cpp.status_code":              strAV("OK"),
		}
		cLast.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}, {Name: "gl-cpp.publish_start"}, {Name: "gl-cpp.publish_end"}}
		cLast.Links = []*proxypb.SpanLink{{TraceId: pubTID, SpanId: pubSID}}
		ch1.Links = []*proxypb.SpanLink{{TraceId: cLastTID, SpanId: cLastSID}}
		fcLast := cppSpan(cLastTID, testSpanID(25991), cLastSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
		fcLast.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish")}
		bcLast := cppSpan(cLastTID, testSpanID(25992), cLastSID, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
		bcLast.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish")}
		var fanMixMsgs []PublishMessageExpectation
		for i := 0; i < 128; i++ {
			fanMixMsgs = append(fanMixMsgs, PublishMessageExpectation{
				ParentTraceparent:      fmt.Sprintf("00-%s-%s-00", testTraceID(2600+i), testSpanID(26000+i)),
				ExpectDroppedUnsampled: true,
			})
		}
		fanMixMsgs = append(fanMixMsgs, PublishMessageExpectation{
			MessageID:       "m-last",
			WireTraceparent: fmt.Sprintf("00-%s-%s-01", cLastTID, cLastSID),
		})
		if res := Verify(cppCaps, []*proxypb.TraceSpan{pubBig, ch0, ch1, cLast, fcLast, bcLast}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{BatchCount: 1, Messages: fanMixMsgs},
		}); !res.Passed() {
			t.Fatalf("C++ >128 fanout with all-00 0-link chunk expected PASS, got:\n%s", res.Format())
		}

		// Fanout children emitted when batch_count <= 128 MUST FAIL.
		pubSmallFan := cloneSpans([]*proxypb.TraceSpan{pub0})[0]
		pubSmallFan.Attributes["messaging.batch.message_count"] = intAV(1)
		badCh0 := cppSpan(pubSmallFan.TraceId, testSpanID(25111), pubSmallFan.SpanId, "publish #0", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{pubSmallFan, badCh0}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{Messages: []PublishMessageExpectation{{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true}}},
		}), CodeUnexpectedSpan, "publish.batch.unexpected_fanout")
	})

	t.Run("C++ -00 Subscribe benign gRPC span, Attack1B symmetry & EOD unlinked modack race", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(2800), testSpanID(28000))
		benignStream := cppSpan(testTraceID(2800), testSpanID(28001), testSpanID(28000), "google.pubsub.v1.Subscriber/StreamingPull", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
		ackDropped := cppSpan(testTraceID(2801), testSpanID(28010), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		ackDropped.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                             strAV("p"),
			"messaging.system":                           strAV("gcp_pubsub"),
			"messaging.destination.name":                 strAV("s"),
			"code.function.name":                         strAV("pubsub::AckHandler::ack"),
			"messaging.gcp_pubsub.message.ack_id":        strAV("drop-a"),
			"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
			"messaging.operation.type":                   strAV("settle"),
			"gl-cpp.status_code":                         strAV("OK"),
		}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{benignStream, ackDropped}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampTP},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		}); !res.Passed() {
			t.Fatalf("C++ -00 subscribe with benign gRPC span in dropped trace expected PASS, got:\n%s", res.Format())
		}

		// C++ Attack1B: acked m0 + dropped-ack m1 (unidir allowed -> PASS + deviation), vs nacked m0 + dropped-ack m1 (-> FAIL).
		spansBase, expBase := buildCppStreamingE2EFixture(true, true, false)
		subOnly := cloneSpans(spansBase[6:]) // sub, cc, sched, proc, ack
		subOnly[4].Links = append(subOnly[4].Links, &proxypb.SpanLink{TraceId: subOnly[0].TraceId, SpanId: subOnly[0].SpanId})
		// C++ per-message ack has no batch_count, so give shared ack batch_count=2 or use separate ack. Test shared ack with batch_count=2 on modack/ack:
		subOnly[4].Attributes["messaging.batch.message_count"] = intAV(2)
		subOnly[4].Links = []*proxypb.SpanLink{{TraceId: subOnly[0].TraceId, SpanId: subOnly[0].SpanId}}
		if res := Verify(cppCaps, subOnly, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(true),
				Messages: []SubscribeMessageExpectation{
					{WireTraceparent: expBase.Subscribe.WireTraceparents[0], AckID: "ack-1", MessageID: "msg-1", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("C++ mixed acked + dropped-ack unidirectional expected PASS, got:\n%s", res.Format())
		}
		// Change m0 to nacked + s nack, while s ack still links to sub0 -> FAIL!
		subNack := cloneSpans(subOnly)
		subNack[0].Attributes["messaging.gcp_pubsub.result"] = strAV("nack")
		subNack[0].Events = []*proxypb.SpanEvent{{Name: "gl-cpp.nack_start"}, {Name: "gl-cpp.nack_end"}}
		nackSpan := cloneSpans([]*proxypb.TraceSpan{subOnly[4]})[0]
		nackSpan.Name = "s nack"
		nackSpan.SpanId = testSpanID(28999)
		nackSpan.Attributes["code.function.name"] = strAV("pubsub::AckHandler::nack")
		delete(nackSpan.Attributes, "messaging.batch.message_count")
		assertHasViolationLocal(t, Verify(cppCaps, append(subNack, nackSpan), &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(true),
				Messages: []SubscribeMessageExpectation{
					{WireTraceparent: expBase.Subscribe.WireTraceparents[0], AckID: "ack-1", MessageID: "msg-1", Settlements: []SettlementExpectation{{Operation: "nack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}), CodeMissingSpan, "subscribe.unsampled_settlement.present")

		// C++ Phase 4 unlinked modack race under EOD (RefreshMessageLeases vs AckEnd) -> MUST PASS!
		unlinkedMod := cppSpan(testTraceID(2901), testSpanID(29010), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, 3*time.Millisecond)
		unlinkedMod.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		unlinkedMod.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                    strAV("p"),
			"messaging.system":                                  strAV("gcp_pubsub"),
			"messaging.destination.name":                        strAV("s"),
			"messaging.operation.type":                          strAV("extend"),
			"messaging.batch.message_count":                     intAV(1),
			"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(30),
		}
		subEOD := cloneSpans(spansBase[6:])
		if res := Verify(cppCaps, append(subEOD, unlinkedMod), &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				WireTraceparents:    expBase.Subscribe.WireTraceparents,
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: false,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}); !res.Passed() {
			t.Fatalf("C++ EOD unlinked modack race expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("C++ UnaryPull cross-project, http receive fallback, BodySize, Redelivery & link integrity", func(t *testing.T) {
		c1TID, c1SID := testTraceID(3001), testSpanID(30010)
		tp1 := fmt.Sprintf("00-%s-%s-01", c1TID, c1SID)
		c2TID, c2SID := testTraceID(3002), testSpanID(30020)
		tp2 := fmt.Sprintf("00-%s-%s-01", c2TID, c2SID)

		makeRecv := func(proj, sub, msgID string, envSize int64, seq int, linkTID, linkSID string) *proxypb.TraceSpan {
			r := cppSpan(testTraceID(3100+seq), testSpanID(31000+seq), "", sub+" receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
			r.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV(proj),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV(sub),
				"messaging.operation.type":        strAV("receive"),
				"code.function.name":              strAV("pubsub::SubscriberConnection::Pull"),
				"messaging.message.id":            strAV(msgID),
				"messaging.message.envelope.size": intAV(envSize),
			}
			if linkTID != "" {
				r.Links = []*proxypb.SpanLink{{TraceId: linkTID, SpanId: linkSID}}
			}
			return r
		}
		makeRecvAck := func(proj, sub, ackID string, seq int, recv *proxypb.TraceSpan) *proxypb.TraceSpan {
			a := cppSpan(testTraceID(3200+seq), testSpanID(32000+seq), "", sub+" ack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(10+seq)*time.Millisecond)
			a.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                      strAV(proj),
				"messaging.system":                    strAV("gcp_pubsub"),
				"messaging.destination.name":          strAV(sub),
				"messaging.operation.type":            strAV("ack"),
				"code.function.name":                  strAV("pubsub::PullAckHandler::ack"),
				"messaging.gcp_pubsub.message.ack_id": strAV(ackID),
			}
			a.Links = []*proxypb.SpanLink{{TraceId: recv.TraceId, SpanId: recv.SpanId}}
			return a
		}

		// (a) Cross-project Topic & Subscription + (b) benign "google.pubsub.v1.Subscriber/Pull" & "http receive" ("my-app") before "s receive".
		appHTTP := cppSpan(testTraceID(3050), testSpanID(30500), "", "http receive", proxypb.SpanKind_SPAN_KIND_SERVER, 0)
		appHTTP.InstrumentationScopeName = "my-app"
		grpcPull := cppSpan(testTraceID(3051), testSpanID(30510), "", "google.pubsub.v1.Subscriber/Pull", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		rCross := makeRecv("p-sub", "s", "up-cross", 30, 5, c1TID, c1SID)
		aCross := makeRecvAck("p-sub", "s", "ua-cross", 5, rCross)
		if res := Verify(cppCaps, []*proxypb.TraceSpan{appHTTP, grpcPull, rCross, aCross}, &TraceExpectation{
			Topic:        "",
			Subscription: "", // test fallback with "http receive" & benign grpc span
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:       "up-cross",
					AckID:           "ua-cross",
					BodySize:        Int64Ptr(10), // (c) >= 20 + 10 = 30 PASS
					WireTraceparent: tp1,
					Settlement:      &SettlementExpectation{Operation: "ack"},
				}},
			},
		}); !res.Passed() {
			t.Fatalf("C++ UnaryPull fallback + BodySize=10(30) expected PASS, got:\n%s", res.Format())
		}

		// Cross-project explicit Topic + Subscription (ProjectID: "").
		if res := Verify(cppCaps, []*proxypb.TraceSpan{rCross, aCross}, &TraceExpectation{
			Topic:        "projects/p-pub/topics/t",
			Subscription: "projects/p-sub/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:       "up-cross",
					AckID:           "ua-cross",
					BodySize:        Int64Ptr(10),
					WireTraceparent: tp1,
					Settlement:      &SettlementExpectation{Operation: "ack"},
				}},
			},
		}); !res.Passed() {
			t.Fatalf("C++ UnaryPull cross-project expected PASS, got:\n%s", res.Format())
		}

		// (c) BodySize: 10 with envelope.size=25 -> FAIL.
		rBadBody := makeRecv("p-sub", "s", "up-cross", 25, 6, c1TID, c1SID)
		aBadBody := makeRecvAck("p-sub", "s", "ua-cross", 6, rBadBody)
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{rBadBody, aBadBody}, &TraceExpectation{
			Subscription: "projects/p-sub/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:       "up-cross",
					AckID:           "ua-cross",
					BodySize:        Int64Ptr(10),
					WireTraceparent: tp1,
					Settlement:      &SettlementExpectation{Operation: "ack"},
				}},
			},
		}), CodeWrongAttributeValue, "unary_pull.receive.attr.envelope_size")

		// (d) Redelivery sharing MessageID="m": [{MessageID:"m"}, {MessageID:"m", WireTraceparent: tp1}] & [{MessageID:"m"}, {MessageID:"m", AckID:"a1", Settlement:ack}].
		rRed1 := makeRecv("p", "s", "m", 30, 1, c1TID, c1SID)
		rRed2 := makeRecv("p", "s", "m", 30, 2, c2TID, c2SID)
		aRed1 := makeRecvAck("p", "s", "a1", 1, rRed1)
		aRed2 := makeRecvAck("p", "s", "a2", 2, rRed2)
		if res := Verify(cppCaps, []*proxypb.TraceSpan{rRed1, rRed2, aRed1, aRed2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{
					{MessageID: "m", WireTraceparent: tp2, Settlement: &SettlementExpectation{Operation: "ack"}},
					{MessageID: "m", WireTraceparent: tp1, Settlement: &SettlementExpectation{Operation: "ack"}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("C++ UnaryPull redelivery WireTraceparent disambiguation expected PASS, got:\n%s", res.Format())
		}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{rRed1, rRed2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{
					{MessageID: "m"},
					{MessageID: "m", WireTraceparent: tp1},
				},
			},
		}); !res.Passed() {
			t.Fatalf("C++ UnaryPull redelivery MessageID vs WireTraceparent expected PASS, got:\n%s", res.Format())
		}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{rRed1, rRed2, aRed1, aRed2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{
					{MessageID: "m", Settlement: &SettlementExpectation{Operation: "ack"}},
					{MessageID: "m", AckID: "a1", Settlement: &SettlementExpectation{Operation: "ack"}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("C++ UnaryPull redelivery AckID disambiguation expected PASS, got:\n%s", res.Format())
		}

		// (e) Dangling and duplicate links on s receive and s ack -> FAIL.
		rDangle := cloneSpans([]*proxypb.TraceSpan{rCross})[0]
		rDangle.Links = append(rDangle.Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{rDangle, aCross}, &TraceExpectation{
			Subscription: "projects/p-sub/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-cross", WireTraceparent: tp1, Settlement: &SettlementExpectation{Operation: "ack"}}}},
		}), CodeMissingLink, "unary_pull.receive.link_to_create.dangling_link")

		rDup := cloneSpans([]*proxypb.TraceSpan{rCross})[0]
		rDup.Links = append(rDup.Links, &proxypb.SpanLink{TraceId: c1TID, SpanId: c1SID})
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{rDup, aCross}, &TraceExpectation{
			Subscription: "projects/p-sub/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-cross", WireTraceparent: tp1, Settlement: &SettlementExpectation{Operation: "ack"}}}},
		}), CodeBrokenBidirectionalLink, "unary_pull.receive.link_to_create.duplicate_link")

		aDangle := cloneSpans([]*proxypb.TraceSpan{aCross})[0]
		aDangle.Links = append(aDangle.Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{rCross, aDangle}, &TraceExpectation{
			Subscription: "projects/p-sub/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-cross", WireTraceparent: tp1, Settlement: &SettlementExpectation{Operation: "ack"}}}},
		}), CodeMissingLink, "unary_pull.ack.link_to_receive.dangling_link")

		aDup := cloneSpans([]*proxypb.TraceSpan{aCross})[0]
		aDup.Links = append(aDup.Links, &proxypb.SpanLink{TraceId: rCross.TraceId, SpanId: rCross.SpanId})
		assertHasViolationLocal(t, Verify(cppCaps, []*proxypb.TraceSpan{rCross, aDup}, &TraceExpectation{
			Subscription: "projects/p-sub/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-cross", WireTraceparent: tp1, Settlement: &SettlementExpectation{Operation: "ack"}}}},
		}), CodeBrokenBidirectionalLink, "unary_pull.ack.link_to_receive.duplicate_link")
	})
}

func TestProfileCpp_Iter4ProbeBAndUnaryPullClosure(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	assertViolation := func(t *testing.T, res TraceResult, wantCode ViolationCode, wantRule string) {
		t.Helper()
		if res.Status != StatusFail {
			t.Fatalf("expected FAIL (%s/%s), got PASS:\n%s", wantCode, wantRule, res.Format())
		}
		for _, f := range res.Failures {
			if f.Code == wantCode && (wantRule == "" || f.Rule == wantRule) {
				return
			}
		}
		t.Fatalf("expected violation (%s, rule=%s), got:\n%s", wantCode, wantRule, res.Format())
	}

	t.Run("Probe B (ProfileCpp): s ack linking [acked sub0, nacked sub1] fails under StrictSettlementOps true and false", func(t *testing.T) {
		makeCppSub := func(seq int, ackID, msgID, result string) (*proxypb.TraceSpan, *proxypb.TraceSpan, *proxypb.TraceSpan, *proxypb.TraceSpan) {
			tid := testTraceID(3500 + seq)
			sid := testSpanID(35000 + seq*10)
			sub := cppSpan(tid, sid, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
			sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sub.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                          strAV("p"),
				"messaging.system":                                        strAV("gcp_pubsub"),
				"messaging.destination.name":                              strAV("s"),
				"messaging.operation.type":                                strAV("subscribe"),
				"messaging.message.id":                                    strAV(msgID),
				"messaging.message.envelope.size":                         intAV(25),
				"messaging.gcp_pubsub.message.ack_id":                     strAV(ackID),
				"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(true),
				"messaging.gcp_pubsub.result":                             strAV(result),
			}
			sub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp." + result + "_start"}, {Name: "gl-cpp." + result + "_end"}}
			cc := cppSpan(tid, testSpanID(35001+seq*10), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			cc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			sched := cppSpan(tid, testSpanID(35002+seq*10), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			sched.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			proc := cppSpan(tid, testSpanID(35003+seq*10), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			proc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			return sub, cc, sched, proc
		}
		makeSettle := func(seq int, op, ackID string, subs ...*proxypb.TraceSpan) *proxypb.TraceSpan {
			sp := cppSpan(testTraceID(3550+seq), testSpanID(35500+seq), "", "s "+op, proxypb.SpanKind_SPAN_KIND_INTERNAL, 5*time.Millisecond)
			sp.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                             strAV("p"),
				"messaging.system":                           strAV("gcp_pubsub"),
				"messaging.destination.name":                 strAV("s"),
				"code.function.name":                         strAV("pubsub::AckHandler::" + op),
				"messaging.gcp_pubsub.message.ack_id":        strAV(ackID),
				"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
				"messaging.operation.type":                   strAV("settle"),
				"gl-cpp.status_code":                         strAV("OK"),
			}
			for _, sub := range subs {
				sp.Links = append(sp.Links, &proxypb.SpanLink{TraceId: sub.TraceId, SpanId: sub.SpanId})
			}
			return sp
		}

		sub0, cc0, sc0, pr0 := makeCppSub(1, "ca-0", "cm-0", "ack")
		sub1, cc1, sc1, pr1 := makeCppSub(2, "ca-1", "cm-1", "nack")
		ackGood := makeSettle(1, "ack", "ca-0", sub0)
		nackGood := makeSettle(2, "nack", "ca-1", sub1)

		// Baseline valid separate links -> PASS.
		if res := Verify(cppCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, ackGood, nackGood}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{
					{AckID: "ca-0", MessageID: "cm-0", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "ca-1", MessageID: "cm-1", Settlements: []SettlementExpectation{{Operation: "nack"}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Probe B valid C++ baseline expected PASS, got:\n%s", res.Format())
		}

		// Mutated s ack linking [sub0(ack), sub1(nack)] -> FAIL for BOTH StrictSettlementOps true and false.
		ackCross := makeSettle(3, "ack", "ca-0", sub0, sub1)
		for _, strict := range []bool{true, false} {
			assertViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, ackCross, nackGood}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce:         BoolPtr(true),
					StrictSettlementOps: strict,
					Messages: []SubscribeMessageExpectation{
						{AckID: "ca-0", MessageID: "cm-0", Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{AckID: "ca-1", MessageID: "cm-1", Settlements: []SettlementExpectation{{Operation: "nack"}}},
					},
				},
			}), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
		}
	})

	t.Run("Primary Reviewer Minor 2 & Challenger 1 UnaryPull gaps: malformed/unsampled TP, links, nack error, fallbacks", func(t *testing.T) {
		makeRecv := func(msgID, orderingKey string, envSize int64, seq int, links ...*proxypb.SpanLink) *proxypb.TraceSpan {
			r := cppSpan(testTraceID(3600+seq), testSpanID(36000+seq), "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
			r.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV("p"),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV("s"),
				"messaging.operation.type":        strAV("receive"),
				"code.function.name":              strAV("pubsub::SubscriberConnection::Pull"),
				"messaging.message.id":            strAV(msgID),
				"messaging.message.envelope.size": intAV(envSize),
			}
			if orderingKey != "" {
				r.Attributes["messaging.gcp_pubsub.message.ordering_key"] = strAV(orderingKey)
			}
			r.Links = links
			return r
		}

		// 1. Malformed WireTraceparent with link -> FAIL (forbidden_link); without link -> PASS.
		badTP := "not-a-traceparent"
		rMalLinked := makeRecv("up-mal", "", 24, 1, &proxypb.SpanLink{TraceId: testTraceID(9911), SpanId: testSpanID(99110)})
		assertViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{rMalLinked}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-mal", WireTraceparent: badTP}}},
		}), CodeBrokenTraceparent, "unary_pull.receive.forbidden_link")

		rMalClean := makeRecv("up-mal", "", 24, 2)
		if res := Verify(cppCaps, []*proxypb.TraceSpan{rMalClean}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-mal", WireTraceparent: badTP}}},
		}); !res.Passed() {
			t.Fatalf("UnaryPull malformed WireTraceparent with 0 links expected PASS, got:\n%s", res.Format())
		}

		// 2. Unsampled (-00) WireTraceparent: 0 links -> PASS; forbidden link -> FAIL; unrelated dangling link -> FAIL.
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(3650), testSpanID(36500))
		if res := Verify(cppCaps, []*proxypb.TraceSpan{makeRecv("up-00", "ok-1", 40, 3)}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{
				MessageID:       "up-00",
				OrderingKey:     "ok-1",
				EnvelopeSize:    Int64Ptr(40),
				WireTraceparent: unsampTP,
			}}},
		}); !res.Passed() {
			t.Fatalf("UnaryPull unsampled -00 with 0 links + OrderingKey + EnvelopeSize expected PASS, got:\n%s", res.Format())
		}

		rUnsampForbidden := makeRecv("up-00", "", 24, 4, &proxypb.SpanLink{TraceId: testTraceID(3650), SpanId: testSpanID(36500)})
		assertViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{rUnsampForbidden}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-00", WireTraceparent: unsampTP}}},
		}), CodeBrokenTraceparent, "unary_pull.receive.forbidden_link")

		rUnsampDangling := makeRecv("up-00", "", 24, 5, &proxypb.SpanLink{TraceId: testTraceID(3651), SpanId: testSpanID(36510)})
		assertViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{rUnsampDangling}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-00", WireTraceparent: unsampTP}}},
		}), CodeMissingLink, "unary_pull.receive.dangling_link")

		// 3. WireTraceparent=="" with duplicate or extra distinct links on s receive -> FAIL.
		rEmptyDup := makeRecv("up-ed", "", 24, 6,
			&proxypb.SpanLink{TraceId: testTraceID(3660), SpanId: testSpanID(36600)},
			&proxypb.SpanLink{TraceId: testTraceID(3660), SpanId: testSpanID(36600)},
		)
		assertViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{rEmptyDup}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-ed"}}},
		}), CodeBrokenBidirectionalLink, "unary_pull.receive.link_to_create.duplicate_link")

		rEmptyExtra := makeRecv("up-ee", "", 24, 7,
			&proxypb.SpanLink{TraceId: testTraceID(3661), SpanId: testSpanID(36610)},
			&proxypb.SpanLink{TraceId: testTraceID(3662), SpanId: testSpanID(36620)},
		)
		assertViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{rEmptyExtra}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-ee"}}},
		}), CodeMissingLink, "unary_pull.receive.link_to_create.dangling_link")

		// 4. nack with DeliveryAttempt + ExpectError + ErrorSubstring -> PASS; missing nack -> FAIL; missing receive -> FAIL.
		rNack := makeRecv("up-nk", "", 24, 8)
		nackErr := cppSpan(testTraceID(3680), testSpanID(36800), "", "s nack", proxypb.SpanKind_SPAN_KIND_CLIENT, 2*time.Millisecond)
		nackErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		nackErr.StatusMessage = "rpc nack failed"
		nackErr.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                strAV("p"),
			"messaging.system":                              strAV("gcp_pubsub"),
			"messaging.destination.name":                    strAV("s"),
			"messaging.operation.type":                      strAV("nack"),
			"code.function.name":                            strAV("pubsub::PullAckHandler::nack"),
			"messaging.gcp_pubsub.message.ack_id":           strAV("ua-nk"),
			"messaging.gcp_pubsub.message.delivery_attempt": intAV(3),
		}
		nackErr.Links = []*proxypb.SpanLink{{TraceId: rNack.TraceId, SpanId: rNack.SpanId}}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{rNack, nackErr}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{
				MessageID:       "up-nk",
				AckID:           "ua-nk",
				DeliveryAttempt: 3,
				Settlement:      &SettlementExpectation{Operation: "nack", ExpectError: true, ErrorSubstring: "nack failed"},
			}}},
		}); !res.Passed() {
			t.Fatalf("UnaryPull nack error + DeliveryAttempt expected PASS, got:\n%s", res.Format())
		}
		assertViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{rNack}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{
				MessageID:  "up-nk",
				Settlement: &SettlementExpectation{Operation: "nack"},
			}}},
		}), CodeMissingSpan, "unary_pull.nack.present")

		// Missing receive span (0 spans and partial 1/2 spans) + Go unsupported.
		assertViolation(t, Verify(cppCaps, nil, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{{MessageID: "up-miss"}}},
		}), CodeMissingSpan, "unary_pull.receive.present")
		assertViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{rNack}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{
				{MessageID: "up-nk"},
				{MessageID: "up-miss2"},
			}},
		}), CodeMissingSpan, "unary_pull.receive.present")
		assertViolation(t, Verify(goCaps, nil, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{},
		}), CodeUnexpectedSpan, "unary_pull.unsupported_in_profile")

		// 5. ExpectPullError: missing span -> FAIL; Subscription=="" fallback on error receive -> PASS.
		assertViolation(t, Verify(cppCaps, nil, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{ExpectPullError: true, ErrorSubstring: "boom"},
		}), CodeMissingSpan, "unary_pull.receive.error_present")

		errRecv := cppSpan(testTraceID(3690), testSpanID(36900), "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		errRecv.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		errRecv.StatusMessage = "deadline exceeded"
		errRecv.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":             strAV("p"),
			"messaging.system":           strAV("gcp_pubsub"),
			"messaging.destination.name": strAV("s"),
			"messaging.operation.type":   strAV("receive"),
			"code.function.name":         strAV("pubsub::SubscriberConnection::Pull"),
		}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{errRecv}, &TraceExpectation{
			Subscription: "",
			UnaryPull:    &UnaryPullExpectation{ExpectPullError: true, ErrorSubstring: "deadline"},
		}); !res.Passed() {
			t.Fatalf("UnaryPull ExpectPullError with Subscription==\"\" fallback expected PASS, got:\n%s", res.Format())
		}
	})
}

func TestProfileCpp_UnaryPullMultiSettlementAndRAIIAutoNack(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	makeRecv := func(seq int, msgID, orderingKey string, envSize int64, links ...*proxypb.SpanLink) *proxypb.TraceSpan {
		r := cppSpan(testTraceID(41000+seq), testSpanID(410000+seq), "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
		r.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                  strAV("p"),
			"messaging.system":                strAV("gcp_pubsub"),
			"messaging.destination.name":      strAV("s"),
			"messaging.operation.type":        strAV("receive"),
			"code.function.name":              strAV("pubsub::SubscriberConnection::Pull"),
			"messaging.message.id":            strAV(msgID),
			"messaging.message.envelope.size": intAV(envSize),
		}
		if orderingKey != "" {
			r.Attributes["messaging.gcp_pubsub.message.ordering_key"] = strAV(orderingKey)
		}
		r.Links = links
		return r
	}
	makeModack := func(seq int, ackID string, deadline int64, recv *proxypb.TraceSpan, abiV2 bool) *proxypb.TraceSpan {
		m := cppSpan(testTraceID(42000+seq), testSpanID(420000+seq), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(10+seq)*time.Millisecond)
		m.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                    strAV("p"),
			"messaging.system":                                  strAV("gcp_pubsub"),
			"messaging.destination.name":                        strAV("s"),
			"messaging.operation.type":                          strAV("modack"),
			"code.function.name":                                strAV("pubsub::PullLeaseManager::ExtendLease"),
			"messaging.gcp_pubsub.message.ack_id":               strAV(ackID),
			"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(deadline),
		}
		if recv != nil {
			if abiV2 {
				m.Links = []*proxypb.SpanLink{{TraceId: recv.TraceId, SpanId: recv.SpanId}}
			} else {
				m.Attributes["gcp_pubsub.receive.trace_id"] = strAV(recv.TraceId)
				m.Attributes["gcp_pubsub.receive.span_id"] = strAV(recv.SpanId)
			}
		}
		return m
	}
	makeSettle := func(seq int, op, ackID string, deliveryAttempt int32, hasDAZero bool, recv *proxypb.TraceSpan, abiV2 bool) *proxypb.TraceSpan {
		sp := cppSpan(testTraceID(43000+seq), testSpanID(430000+seq), "", "s "+op, proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(30+seq)*time.Millisecond)
		sp.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                      strAV("p"),
			"messaging.system":                    strAV("gcp_pubsub"),
			"messaging.destination.name":          strAV("s"),
			"messaging.operation.type":            strAV(op),
			"code.function.name":                  strAV("pubsub::PullAckHandler::" + op),
			"messaging.gcp_pubsub.message.ack_id": strAV(ackID),
		}
		if deliveryAttempt > 0 || hasDAZero {
			sp.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = intAV(int64(deliveryAttempt))
		}
		if recv != nil {
			if abiV2 {
				sp.Links = []*proxypb.SpanLink{{TraceId: recv.TraceId, SpanId: recv.SpanId}}
			} else {
				sp.Attributes["gcp_pubsub.receive.trace_id"] = strAV(recv.TraceId)
				sp.Attributes["gcp_pubsub.receive.span_id"] = strAV(recv.SpanId)
			}
		}
		return sp
	}
	assertFail := func(t *testing.T, res TraceResult, wantCode ViolationCode, wantRule string) Violation {
		t.Helper()
		if res.Status != StatusFail {
			t.Fatalf("expected FAIL (%s/%s), got PASS:\n%s", wantCode, wantRule, res.Format())
		}
		for _, f := range res.Failures {
			if f.Code == wantCode && (wantRule == "" || f.Rule == wantRule) {
				return f
			}
		}
		t.Fatalf("expected violation (%s, rule=%s), got:\n%s", wantCode, wantRule, res.Format())
		return Violation{}
	}

	t.Run("Backwards compatibility: Settlement vs Settlements equivalence", func(t *testing.T) {
		r := makeRecv(1, "up-bc", "", 25)
		nk := makeSettle(1, "nack", "ua-bc", 0, false, r, true)
		resLegacy := Verify(cppCaps, []*proxypb.TraceSpan{r, nk}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:  "up-bc",
					AckID:      "ua-bc",
					Settlement: &SettlementExpectation{Operation: "nack"},
				}},
			},
		})
		resSlice := Verify(cppCaps, []*proxypb.TraceSpan{r, nk}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:   "up-bc",
					AckID:       "ua-bc",
					Settlements: []SettlementExpectation{{Operation: "nack"}},
				}},
			},
		})
		if !resLegacy.Passed() || !resSlice.Passed() {
			t.Fatalf("expected both PASS: legacy=%v slice=%v", resLegacy.Format(), resSlice.Format())
		}
		if resLegacy.Format() != resSlice.Format() {
			t.Fatalf("expected identical output:\nlegacy:\n%s\nslice:\n%s", resLegacy.Format(), resSlice.Format())
		}
	})

	t.Run("Positive: [modack, nack] RAII auto-nack, [modack, modack, ack], ABI v1, delivery_attempt=0, traceparent matrix", func(t *testing.T) {
		// 1. [modack, nack] with AllowExtraPubSubSpans: false + delivery_attempt=0 (real C++).
		cTID1, cSID1 := testTraceID(40001), testSpanID(400010)
		tp01 := fmt.Sprintf("00-%s-%s-01", cTID1, cSID1)
		r1 := makeRecv(2, "up-raii", "", 30, &proxypb.SpanLink{TraceId: cTID1, SpanId: cSID1})
		mod1 := makeModack(2, "ua-raii", 10, r1, true)
		nk1 := makeSettle(2, "nack", "ua-raii", 0, true, r1, true)
		res1 := Verify(cppCaps, []*proxypb.TraceSpan{r1, mod1, nk1}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:       "up-raii",
					AckID:           "ua-raii",
					DeliveryAttempt: 0,
					WireTraceparent: tp01,
					Settlements: []SettlementExpectation{
						{Operation: "modack", AckDeadlineSeconds: 10},
						{Operation: "nack"},
					},
				}},
			},
		})
		if !res1.Passed() {
			t.Fatalf("[modack, nack] expected PASS with AllowExtraPubSubSpans=false, got:\n%s", res1.Format())
		}

		// 2. [modack, modack, ack] with distinct deadlines & error status on second modack.
		r2 := makeRecv(3, "up-multi", "", 30)
		mod2a := makeModack(3, "ua-multi", 60, r2, true)
		mod2b := makeModack(4, "ua-multi", 10, r2, true)
		mod2b.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		mod2b.StatusMessage = "transient modack err"
		ack2 := makeSettle(3, "ack", "ua-multi", 2, false, r2, true)
		res2 := Verify(cppCaps, []*proxypb.TraceSpan{r2, mod2a, mod2b, ack2}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:       "up-multi",
					AckID:           "ua-multi",
					DeliveryAttempt: 2,
					Settlements: []SettlementExpectation{
						{Operation: "modack", AckDeadlineSeconds: 60},
						{Operation: "modack", AckDeadlineSeconds: 10, ExpectError: true, ErrorSubstring: "transient"},
						{Operation: "ack"},
					},
				}},
			},
		})
		if !res2.Passed() {
			t.Fatalf("[modack, modack, ack] expected PASS, got:\n%s", res2.Format())
		}

		// 3. ABI v1 link fallback on [modack, nack].
		rV1 := makeRecv(4, "up-v1", "", 30)
		modV1 := makeModack(5, "ua-v1", 20, rV1, false)
		nkV1 := makeSettle(4, "nack", "ua-v1", 0, true, rV1, false)
		resV1 := Verify(cppCaps, []*proxypb.TraceSpan{rV1, modV1, nkV1}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID: "up-v1",
					AckID:     "ua-v1",
					Settlements: []SettlementExpectation{
						{Operation: "modack", AckDeadlineSeconds: 20},
						{Operation: "nack"},
					},
				}},
			},
		})
		if !resV1.Passed() {
			t.Fatalf("ABI v1 [modack, nack] expected PASS, got:\n%s", resV1.Format())
		}
		foundABIV1 := false
		for _, d := range resV1.Deviations {
			if d.Rule == "links.abi_v1_event_attrs_fallback" {
				foundABIV1 = true
			}
		}
		if !foundABIV1 {
			t.Fatalf("expected links.abi_v1_event_attrs_fallback deviation, got %+v", resV1.Deviations)
		}

		// 4. Traceparent matrix (-00 unsampled and malformed) with [modack, nack] and redelivery -01 vs -00 disambiguation.
		tp00 := fmt.Sprintf("00-%s-%s-00", testTraceID(40099), testSpanID(400990))
		for _, badTP := range []string{tp00, "malformed-tp"} {
			rUn := makeRecv(5, "up-tp", "", 30)
			modUn := makeModack(6, "ua-tp", 15, rUn, true)
			nkUn := makeSettle(5, "nack", "ua-tp", 0, false, rUn, true)
			resUn := Verify(cppCaps, []*proxypb.TraceSpan{rUn, modUn, nkUn}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				UnaryPull: &UnaryPullExpectation{
					Messages: []UnaryPullMessageExpectation{{
						MessageID:       "up-tp",
						WireTraceparent: badTP,
						Settlements: []SettlementExpectation{
							{Operation: "modack", AckDeadlineSeconds: 15},
							{Operation: "nack"},
						},
					}},
				},
			})
			if !resUn.Passed() {
				t.Fatalf("traceparent %q [modack, nack] expected PASS, got:\n%s", badTP, resUn.Format())
			}
		}

		// Redelivery with shared MessageID="m-tp": rA linked to -01, rB unlinked for -00 (wrongly ordered in expectation).
		rA := makeRecv(6, "m-tp", "", 30, &proxypb.SpanLink{TraceId: cTID1, SpanId: cSID1})
		rB := makeRecv(7, "m-tp", "", 30)
		modA, nkA := makeModack(7, "a-A", 10, rA, true), makeSettle(6, "nack", "a-A", 1, false, rA, true)
		modB, nkB := makeModack(8, "a-B", 10, rB, true), makeSettle(7, "nack", "a-B", 2, false, rB, true)
		resRedTP := Verify(cppCaps, []*proxypb.TraceSpan{rA, rB, modA, nkA, modB, nkB}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{
					{MessageID: "m-tp", WireTraceparent: tp00, Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "nack"}}},
					{MessageID: "m-tp", WireTraceparent: tp01, Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "nack"}}},
				},
			},
		})
		if !resRedTP.Passed() {
			t.Fatalf("redelivery -00 vs -01 disambiguation expected PASS, got:\n%s", resRedTP.Format())
		}
	})

	t.Run("Positive: Multi-message MCMF disambiguation via Settlements with shared MessageID and omitted AckID", func(t *testing.T) {
		rNack := makeRecv(10, "m-shared", "", 30)
		rAck := makeRecv(11, "m-shared", "", 30)
		modN := makeModack(10, "ack-1", 10, rNack, true)
		nk := makeSettle(10, "nack", "ack-1", 0, false, rNack, true)
		modA := makeModack(11, "ack-2", 10, rAck, true)
		ak := makeSettle(11, "ack", "ack-2", 0, false, rAck, true)

		spans := []*proxypb.TraceSpan{rNack, rAck, modN, nk, modA, ak}
		// Order [ack, nack] even though rNack appears first in spans!
		res1 := Verify(cppCaps, spans, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{
					{MessageID: "m-shared", Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}}},
					{MessageID: "m-shared", Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "nack"}}},
				},
			},
		})
		if !res1.Passed() {
			t.Fatalf("MCMF Settlements disambiguation [ack, nack] expected PASS, got:\n%s", res1.Format())
		}
	})

	t.Run("Positive: Multi-modack permutation & chronological mod60-before-mod10 disambiguation (Wave 7 Reviewer 1 & 2)", func(t *testing.T) {
		// Case A (Reviewer 1 & 2 shape): r0 chronologically has modack(60) BEFORE modack(10) + ack;
		// r1 has modack(10) + modack(30) + ack. Both share MessageID="m-shared" and omit AckID.
		r0 := makeRecv(20, "m-shared", "", 30)
		r1 := makeRecv(21, "m-shared", "", 30)
		mod0a := makeModack(20, "ua-0", 60, r0, true) // earlier timestamp (10+20ms)
		mod0b := makeModack(21, "ua-0", 10, r0, true) // later timestamp (10+21ms)
		ack0 := makeSettle(20, "ack", "ua-0", 0, false, r0, true)
		mod1a := makeModack(22, "ua-1", 10, r1, true)
		mod1b := makeModack(23, "ua-1", 30, r1, true)
		ack1 := makeSettle(21, "ack", "ua-1", 0, false, r1, true)

		orderA := []*proxypb.TraceSpan{r0, r1, mod0a, mod0b, ack0, mod1a, mod1b, ack1}
		orderB := []*proxypb.TraceSpan{r0, r1, mod0b, mod0a, ack0, mod1b, mod1a, ack1}

		msg60 := UnaryPullMessageExpectation{
			MessageID: "m-shared",
			Settlements: []SettlementExpectation{
				{Operation: "modack"},
				{Operation: "modack", AckDeadlineSeconds: 60},
				{Operation: "ack"},
			},
		}
		msg10 := UnaryPullMessageExpectation{
			MessageID: "m-shared",
			Settlements: []SettlementExpectation{
				{Operation: "modack"},
				{Operation: "modack", AckDeadlineSeconds: 10},
				{Operation: "ack"},
			},
		}
		msgsOrder1 := []UnaryPullMessageExpectation{msg60, msg10}
		msgsOrder2 := []UnaryPullMessageExpectation{msg10, msg60}

		var baseline string
		for idxOrder, msgs := range [][]UnaryPullMessageExpectation{msgsOrder1, msgsOrder2} {
			for spanIdx, spansIn := range [][]*proxypb.TraceSpan{orderA, orderB} {
				res := Verify(cppCaps, spansIn, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					UnaryPull:    &UnaryPullExpectation{Messages: msgs},
				})
				if !res.Passed() {
					t.Fatalf("Case A msgs=%d spans=%d expected PASS, got:\n%s", idxOrder, spanIdx, res.Format())
				}
				if baseline == "" {
					baseline = res.Format()
				} else if res.Format() != baseline {
					t.Fatalf("Case A msgs=%d spans=%d Format() differed:\nwant:\n%s\ngot:\n%s", idxOrder, spanIdx, baseline, res.Format())
				}
			}
		}
		for seed := int64(0); seed < 25; seed++ {
			perm := cloneSpans(orderA)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := msgsOrder1
			if seed%2 == 1 {
				msgs = msgsOrder2
			}
			res := Verify(cppCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull:    &UnaryPullExpectation{Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("Case A seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if res.Format() != baseline {
				t.Fatalf("Case A seed=%d Format() differed from baseline", seed)
			}
		}

		// Case B (Reviewer 2 shape): rFirst has [modack(10), modack(10), ack], rSecond (later StartTime) has
		// chronological [modack(60), modack(10), ack]. Tested across both message orders & 25 shuffles.
		rB0 := makeRecv(30, "m-r2", "", 30)
		rB1 := makeRecv(31, "m-r2", "", 30)
		modB0a := makeModack(30, "ub-0", 10, rB0, true)
		modB0b := makeModack(31, "ub-0", 10, rB0, true)
		ackB0 := makeSettle(30, "ack", "ub-0", 0, false, rB0, true)
		modB1a := makeModack(32, "ub-1", 60, rB1, true) // earlier than modB1b
		modB1b := makeModack(33, "ub-1", 10, rB1, true)
		ackB1 := makeSettle(31, "ack", "ub-1", 0, false, rB1, true)
		baseB := []*proxypb.TraceSpan{rB0, rB1, modB0a, modB0b, ackB0, modB1a, modB1b, ackB1}
		msgB0 := UnaryPullMessageExpectation{
			MessageID:   "m-r2",
			Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "modack", AckDeadlineSeconds: 60}, {Operation: "ack"}},
		}
		msgB1 := UnaryPullMessageExpectation{
			MessageID:   "m-r2",
			Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "modack", AckDeadlineSeconds: 10}, {Operation: "ack"}},
		}
		var baseBFmt string
		for seed := int64(0); seed < 25; seed++ {
			perm := cloneSpans(baseB)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := []UnaryPullMessageExpectation{msgB0, msgB1}
			if seed%2 == 1 {
				msgs = []UnaryPullMessageExpectation{msgB1, msgB0}
			}
			res := Verify(cppCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull:    &UnaryPullExpectation{Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("Case B seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseBFmt = res.Format()
			} else if res.Format() != baseBFmt {
				t.Fatalf("Case B seed=%d Format() differed from seed=0", seed)
			}
		}
	})

	t.Run("Negative: missing, non-root, extra, unlinked, conflict, invalid_operation, unlinkedCount order independence & ABI v1 -00 leak", func(t *testing.T) {
		r := makeRecv(20, "up-neg", "", 30)
		mod := makeModack(20, "ua-neg", 10, r, true)
		nk := makeSettle(20, "nack", "ua-neg", 0, false, r, true)
		ak := makeSettle(21, "ack", "ua-neg", 0, false, r, true)

		// 1. Expecting [modack, nack] when only receive+modack present -> FAIL unary_pull.nack.present.
		assertFail(t, Verify(cppCaps, []*proxypb.TraceSpan{r, mod}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:   "up-neg",
					Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "nack"}},
				}},
			},
		}), CodeMissingSpan, "unary_pull.nack.present")

		// 2. Expecting [modack, modack, ack] when only receive+modack+ack present -> FAIL unary_pull.modack.present.
		assertFail(t, Verify(cppCaps, []*proxypb.TraceSpan{r, mod, ak}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:   "up-neg",
					Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "modack"}, {Operation: "ack"}},
				}},
			},
		}), CodeMissingSpan, "unary_pull.modack.present")

		// 3. Non-root modack (ParentSpanId = recv.SpanId) -> FAIL unary_pull.modack.root.
		modNonRoot := makeModack(21, "ua-neg", 10, r, true)
		modNonRoot.TraceId = r.TraceId
		modNonRoot.ParentSpanId = r.SpanId
		assertFail(t, Verify(cppCaps, []*proxypb.TraceSpan{r, modNonRoot, nk}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:   "up-neg",
					Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "nack"}},
				}},
			},
		}), CodeWrongParent, "unary_pull.modack.root")

		// 4. Extra unexpected modack or ack when expecting [modack, nack] -> FAIL traces.no_extra_pubsub_spans.
		modExtra := makeModack(22, "ua-neg", 10, r, true)
		assertFail(t, Verify(cppCaps, []*proxypb.TraceSpan{r, mod, modExtra, nk}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:   "up-neg",
					Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "nack"}},
				}},
			},
		}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// 5. Expecting [modack] when unexpected RAII auto-nack emitted -> FAIL traces.no_extra_pubsub_spans.
		assertFail(t, Verify(cppCaps, []*proxypb.TraceSpan{r, mod, nk}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:   "up-neg",
					Settlements: []SettlementExpectation{{Operation: "modack"}},
				}},
			},
		}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// 6. Unlinked nack (lacks SpanLink and ABI v1 attrs) -> FAIL unary_pull.nack.present + traces.no_extra_pubsub_spans + descriptive message.
		nkUnlinked := makeSettle(22, "nack", "ua-neg", 0, false, nil, true)
		resUnlinked := Verify(cppCaps, []*proxypb.TraceSpan{r, mod, nkUnlinked}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:   "up-neg",
					Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "nack"}},
				}},
			},
		})
		fMiss := assertFail(t, resUnlinked, CodeMissingSpan, "unary_pull.nack.present")
		if !strings.Contains(fMiss.Message, "lacking ABI v1/v2 link") {
			t.Fatalf("expected missing-link context in message, got %q", fMiss.Message)
		}
		assertFail(t, resUnlinked, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// 7. Conflict: both Settlement and Settlements set -> CodeCustomCheckFailed unary_pull.settlement.conflict.
		assertFail(t, Verify(cppCaps, []*proxypb.TraceSpan{r, mod, nk}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull: &UnaryPullExpectation{
				Messages: []UnaryPullMessageExpectation{{
					MessageID:   "up-neg",
					Settlement:  &SettlementExpectation{Operation: "ack"},
					Settlements: []SettlementExpectation{{Operation: "nack"}},
				}},
			},
		}), CodeCustomCheckFailed, "unary_pull.settlement.conflict")

		// 8. Invalid or empty Operation -> CodeCustomCheckFailed unary_pull.settlement.invalid_operation.
		for _, badOp := range []string{"invalid", ""} {
			assertFail(t, Verify(cppCaps, nil, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull: &UnaryPullExpectation{
					Messages: []UnaryPullMessageExpectation{{
						MessageID:   "up-neg",
						Settlements: []SettlementExpectation{{Operation: badOp}},
					}},
				},
			}), CodeCustomCheckFailed, "unary_pull.settlement.invalid_operation")
		}

		// 9. Reviewer 1 Minor 2: unlinkedCount diagnostic in unary_pull.<op>.present must not depend on Messages order.
		rMiss := makeRecv(50, "m-ul-miss", "", 30)
		rOK := makeRecv(51, "m-ul-ok", "", 30)
		ackOK := makeSettle(51, "ack", "ua-ul-ok", 0, false, rOK, true)
		ackUnlinked := makeSettle(52, "ack", "ua-ul-free", 0, false, nil, true)
		mMiss := UnaryPullMessageExpectation{MessageID: "m-ul-miss", Settlements: []SettlementExpectation{{Operation: "ack"}}}
		mOK := UnaryPullMessageExpectation{MessageID: "m-ul-ok", AckID: "ua-ul-ok", Settlements: []SettlementExpectation{{Operation: "ack"}}}
		resUL1 := Verify(cppCaps, []*proxypb.TraceSpan{rMiss, rOK, ackOK, ackUnlinked}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{mMiss, mOK}},
		})
		resUL2 := Verify(cppCaps, []*proxypb.TraceSpan{rMiss, rOK, ackOK, ackUnlinked}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: []UnaryPullMessageExpectation{mOK, mMiss}},
		})
		fUL1 := assertFail(t, resUL1, CodeMissingSpan, "unary_pull.ack.present")
		fUL2 := assertFail(t, resUL2, CodeMissingSpan, "unary_pull.ack.present")
		if !strings.Contains(fUL1.Message, `found 1 unmatched "s ack" span(s)`) || fUL1.Message != fUL2.Message {
			t.Fatalf("expected identical unlinkedCount=1 message regardless of Messages order:\norder1: %q\norder2: %q", fUL1.Message, fUL2.Message)
		}

		// 10. Challenger 1 Low Challenge 4: ABI v1 gcp_pubsub.create.{trace_id,span_id} leak on -00 or malformed WireTraceparent -> FAIL.
		tp00Leak := fmt.Sprintf("00-%s-%s-00", testTraceID(40999), testSpanID(409990))
		for _, badTP := range []string{tp00Leak, "malformed-tp"} {
			rLeak := makeRecv(55, "m-abiv1-leak", "", 30)
			rLeak.Attributes["gcp_pubsub.create.trace_id"] = strAV(testTraceID(40999))
			rLeak.Attributes["gcp_pubsub.create.span_id"] = strAV(testSpanID(409990))
			resLeak := Verify(cppCaps, []*proxypb.TraceSpan{rLeak}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull: &UnaryPullExpectation{
					Messages: []UnaryPullMessageExpectation{{
						MessageID:       "m-abiv1-leak",
						WireTraceparent: badTP,
					}},
				},
			})
			assertFail(t, resLeak, CodeUnexpectedAttribute, "unary_pull.receive.attr.create_trace_id_absent")
			assertFail(t, resLeak, CodeUnexpectedAttribute, "unary_pull.receive.attr.create_span_id_absent")
		}
	})
}

func TestWave8Regressions_UnaryPullNormalizationErrorAndDeliveryAttempt(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	makeRecv := func(seq int, msgID, orderingKey string, envSize int64, links ...*proxypb.SpanLink) *proxypb.TraceSpan {
		r := cppSpan(testTraceID(51000+seq), testSpanID(510000+seq), "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
		r.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                  strAV("p"),
			"messaging.system":                strAV("gcp_pubsub"),
			"messaging.destination.name":      strAV("s"),
			"messaging.operation.type":        strAV("receive"),
			"code.function.name":              strAV("pubsub::SubscriberConnection::Pull"),
			"messaging.message.id":            strAV(msgID),
			"messaging.message.envelope.size": intAV(envSize),
		}
		if orderingKey != "" {
			r.Attributes["messaging.gcp_pubsub.message.ordering_key"] = strAV(orderingKey)
		}
		r.Links = links
		return r
	}
	makeModack := func(seq int, ackID string, deadline int64, recv *proxypb.TraceSpan) *proxypb.TraceSpan {
		m := cppSpan(testTraceID(52000+seq), testSpanID(520000+seq), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(10+seq)*time.Millisecond)
		m.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                    strAV("p"),
			"messaging.system":                                  strAV("gcp_pubsub"),
			"messaging.destination.name":                        strAV("s"),
			"messaging.operation.type":                          strAV("modack"),
			"code.function.name":                                strAV("pubsub::PullLeaseManager::ExtendLease"),
			"messaging.gcp_pubsub.message.ack_id":               strAV(ackID),
			"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(deadline),
		}
		if recv != nil {
			m.Links = []*proxypb.SpanLink{{TraceId: recv.TraceId, SpanId: recv.SpanId}}
		}
		return m
	}
	makeSettle := func(seq int, op, ackID string, deliveryAttempt int32, recv *proxypb.TraceSpan) *proxypb.TraceSpan {
		sp := cppSpan(testTraceID(53000+seq), testSpanID(530000+seq), "", "s "+op, proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(30+seq)*time.Millisecond)
		sp.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                      strAV("p"),
			"messaging.system":                    strAV("gcp_pubsub"),
			"messaging.destination.name":          strAV("s"),
			"messaging.operation.type":            strAV(op),
			"code.function.name":                  strAV("pubsub::PullAckHandler::" + op),
			"messaging.gcp_pubsub.message.ack_id": strAV(ackID),
		}
		if deliveryAttempt > 0 {
			sp.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = intAV(int64(deliveryAttempt))
		}
		if recv != nil {
			sp.Links = []*proxypb.SpanLink{{TraceId: recv.TraceId, SpanId: recv.SpanId}}
		}
		return sp
	}
	assertFail := func(t *testing.T, res TraceResult, wantCode ViolationCode, wantRule string) Violation {
		t.Helper()
		if res.Status != StatusFail {
			t.Fatalf("expected FAIL (%s/%s), got PASS:\n%s", wantCode, wantRule, res.Format())
		}
		for _, f := range res.Failures {
			if f.Code == wantCode && (wantRule == "" || f.Rule == wantRule) {
				return f
			}
		}
		t.Fatalf("expected violation (%s, rule=%s), got:\n%s", wantCode, wantRule, res.Format())
		return Violation{}
	}

	t.Run("Attack 8A_2: UnaryPull 14 vs 15 and >= 410 / >= 575 normalization fallback with shared MessageID and omitted AckID", func(t *testing.T) {
		runSettleCountTest := func(t *testing.T, baseSeq, lowModacks int, extraDL int64, shuffles int, withZeroSettleMsg, withErrMeta bool) {
			t.Helper()
			r0 := makeRecv(baseSeq, "m-up-8a", "", 30)
			r1 := makeRecv(baseSeq+1, "m-up-8a", "", 30)
			spans := []*proxypb.TraceSpan{r0, r1}
			var settlesLow, settlesHigh []SettlementExpectation
			seq := baseSeq * 100
			for k := 0; k < lowModacks; k++ {
				m0 := makeModack(seq, "ua-0", 10, r0)
				se0 := SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10}
				if withErrMeta {
					m0.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
					m0.StatusMessage = "transient err"
					se0.ExpectError = true
					se0.ErrorSubstring = "transient"
				}
				spans = append(spans, m0)
				settlesLow = append(settlesLow, se0)
				seq++

				m1 := makeModack(seq, "ua-1", 10, r1)
				se1 := SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10}
				if withErrMeta {
					m1.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
					m1.StatusMessage = "transient err"
					se1.ExpectError = true
					se1.ErrorSubstring = "transient"
				}
				spans = append(spans, m1)
				settlesHigh = append(settlesHigh, se1)
				seq++
			}
			mExtra := makeModack(seq, "ua-1", extraDL, r1)
			seExtra := SettlementExpectation{Operation: "modack", AckDeadlineSeconds: int32(extraDL)}
			if withErrMeta {
				mExtra.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				mExtra.StatusMessage = "transient err"
				seExtra.ExpectError = true
				seExtra.ErrorSubstring = "transient"
			}
			spans = append(spans, mExtra)
			settlesHigh = append(settlesHigh, seExtra)
			seq++

			spans = append(spans, makeSettle(seq, "ack", "ua-0", 0, r0))
			settlesLow = append(settlesLow, SettlementExpectation{Operation: "ack"})
			seq++
			spans = append(spans, makeSettle(seq, "ack", "ua-1", 0, r1))
			settlesHigh = append(settlesHigh, SettlementExpectation{Operation: "ack"})

			mLow := UnaryPullMessageExpectation{MessageID: "m-up-8a", Settlements: settlesLow}
			mHigh := UnaryPullMessageExpectation{MessageID: "m-up-8a", Settlements: settlesHigh}
			msgsA := []UnaryPullMessageExpectation{mLow, mHigh}
			msgsB := []UnaryPullMessageExpectation{mHigh, mLow}
			if withZeroSettleMsg {
				rZ := makeRecv(baseSeq+2, "m-up-zero", "", 30)
				spans = append(spans, rZ)
				mZ := UnaryPullMessageExpectation{MessageID: "m-up-zero"}
				msgsA = append(msgsA, mZ)
				msgsB = []UnaryPullMessageExpectation{mZ, mHigh, mLow}
			}

			var baseline string
			for seed := int64(0); seed < int64(shuffles); seed++ {
				perm := cloneSpans(spans)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				msgs := msgsA
				if seed%2 == 1 {
					msgs = msgsB
				}
				res := Verify(cppCaps, perm, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					UnaryPull:    &UnaryPullExpectation{Messages: msgs},
				})
				if !res.Passed() {
					t.Fatalf("low=%d seed=%d expected PASS, got:\n%s", lowModacks, seed, res.Format())
				}
				if seed == 0 {
					baseline = res.Format()
				} else if res.Format() != baseline {
					t.Fatalf("low=%d seed=%d Format() differed from baseline", lowModacks, seed)
				}
			}
		}

		t.Run("14 vs 15", func(t *testing.T) {
			runSettleCountTest(t, 100, 14, 60, 24, false, false)
		})
		t.Run("410 vs 411", func(t *testing.T) {
			runSettleCountTest(t, 200, 410, 60, 2, false, false)
		})
		t.Run("575 vs 576 exercises proportional normalization cap", func(t *testing.T) {
			// 575 * 700 + 500 = 403_000 > unaryPullSettleBonusCap (400_000) with omitted AckID.
			// Including a 0-settlement message covers raw==0 inside normalization too.
			runSettleCountTest(t, 300, 575, 60, 2, true, true)

			// Hierarchy check: MessageID (600_000) beats normalized totalSettleBonus (<= 400_000),
			// and WireTraceparent (1_200_000) beats MessageID + normalized totalSettleBonus.
			rH0 := makeRecv(400, "m-up-hi-0", "", 30)
			rH1 := makeRecv(401, "m-up-hi-1", "", 30)
			spansH := []*proxypb.TraceSpan{rH0, rH1}
			var settles580 []SettlementExpectation
			for k := 0; k < 580; k++ {
				m := makeModack(40000+k, "ua-hi-1", 10, rH1)
				m.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				m.StatusMessage = "transient err"
				spansH = append(spansH, m)
				settles580 = append(settles580, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10, ExpectError: true, ErrorSubstring: "transient"})
			}
			spansH = append(spansH, makeSettle(40900, "ack", "ua-hi-1", 0, rH1))
			settles580 = append(settles580, SettlementExpectation{Operation: "ack"})
			spansH = append(spansH, makeSettle(40901, "ack", "ua-hi-0", 0, rH0))

			resHi := Verify(cppCaps, spansH, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				UnaryPull: &UnaryPullExpectation{
					Messages: []UnaryPullMessageExpectation{
						{MessageID: "m-up-hi-0", Settlements: settles580},
						{MessageID: "m-up-hi-1", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					},
				},
			})
			assertFail(t, resHi, CodeMissingSpan, "unary_pull.modack.present")
			for _, f := range resHi.Failures {
				if f.Rule == "unary_pull.receive.attr.message_id" {
					t.Fatalf("normalized totalSettleBonus improperly overrode MessageID:\n%s", resHi.Format())
				}
			}

			// WireTraceparent (1_200_000) > MessageID (600_000) + normalized totalSettleBonus (<= 400_000).
			cTID, cSID := testTraceID(49999), testSpanID(499990)
			tp01 := fmt.Sprintf("00-%s-%s-01", cTID, cSID)
			rTP := makeRecv(402, "m-up-tp", "", 30, &proxypb.SpanLink{TraceId: cTID, SpanId: cSID})
			rMsg := makeRecv(403, "m-up-tp", "", 30)
			spansTP := []*proxypb.TraceSpan{rTP, rMsg}
			for k := 0; k < 580; k++ {
				m := makeModack(42000+k, "ua-msg", 10, rMsg)
				m.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				m.StatusMessage = "transient err"
				spansTP = append(spansTP, m)
			}
			spansTP = append(spansTP, makeSettle(42900, "ack", "ua-msg", 0, rMsg))
			spansTP = append(spansTP, makeSettle(42901, "ack", "ua-tp", 0, rTP))

			resTP := Verify(cppCaps, spansTP, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				UnaryPull: &UnaryPullExpectation{
					Messages: []UnaryPullMessageExpectation{
						{MessageID: "m-up-tp", WireTraceparent: tp01, Settlements: settles580},
						{MessageID: "m-up-tp", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					},
				},
			})
			assertFail(t, resTP, CodeMissingSpan, "unary_pull.modack.present")
			for _, f := range resTP.Failures {
				if f.Rule == "unary_pull.receive.link_to_create" {
					t.Fatalf("MessageID + normalized totalSettleBonus improperly overrode WireTraceparent:\n%s", resTP.Format())
				}
			}
		})
	})

	t.Run("Attack 8B_2: UnaryPull ErrorSubstring disambiguation with shared MessageID and omitted AckID", func(t *testing.T) {
		r0 := makeRecv(500, "m-up-8b2", "", 30)
		r1 := makeRecv(501, "m-up-8b2", "", 30)
		ack0 := makeSettle(500, "ack", "ua-8b2-0", 0, r0)
		ack0.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack0.StatusMessage = "permission denied"
		ack1 := makeSettle(501, "ack", "ua-8b2-1", 0, r1)
		ack1.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack1.StatusMessage = "deadline exceeded"

		base := []*proxypb.TraceSpan{r0, r1, ack0, ack1}
		mPerm := UnaryPullMessageExpectation{
			MessageID:   "m-up-8b2",
			Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, ErrorSubstring: "permission denied"}},
		}
		mDead := UnaryPullMessageExpectation{
			MessageID:   "m-up-8b2",
			Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, ErrorSubstring: "deadline exceeded"}},
		}
		msgsA := []UnaryPullMessageExpectation{mPerm, mDead}
		msgsB := []UnaryPullMessageExpectation{mDead, mPerm}

		var baseline string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := msgsA
			if seed%2 == 1 {
				msgs = msgsB
			}
			res := Verify(cppCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull:    &UnaryPullExpectation{Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseline = res.Format()
			} else if res.Format() != baseline {
				t.Fatalf("seed=%d Format() differed from baseline", seed)
			}
		}

		badSpans := cloneSpans(base)
		badSpans[3].StatusMessage = "wrong error text"
		resBad := Verify(cppCaps, badSpans, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			UnaryPull:    &UnaryPullExpectation{Messages: msgsB},
		})
		assertFail(t, resBad, CodeWrongStatusMessage, "unary_pull.ack.status.message")
	})

	t.Run("Attack 8B_4: UnaryPull DeliveryAttempt disambiguation with shared MessageID and omitted AckID", func(t *testing.T) {
		for _, op := range []string{"ack", "nack"} {
			t.Run(op, func(t *testing.T) {
				r0 := makeRecv(600, "m-up-8b4-"+op, "", 30)
				r1 := makeRecv(601, "m-up-8b4-"+op, "", 30)
				s0 := makeSettle(600, op, "ua-8b4-0", 1, r0)
				s1 := makeSettle(601, op, "ua-8b4-1", 2, r1)

				base := []*proxypb.TraceSpan{r0, r1, s0, s1}
				mDA1 := UnaryPullMessageExpectation{
					MessageID:       "m-up-8b4-" + op,
					DeliveryAttempt: 1,
					Settlements:     []SettlementExpectation{{Operation: op}},
				}
				mDA2 := UnaryPullMessageExpectation{
					MessageID:       "m-up-8b4-" + op,
					DeliveryAttempt: 2,
					Settlements:     []SettlementExpectation{{Operation: op}},
				}
				msgsA := []UnaryPullMessageExpectation{mDA1, mDA2}
				msgsB := []UnaryPullMessageExpectation{mDA2, mDA1}

				var baseline string
				for seed := int64(0); seed < 24; seed++ {
					perm := cloneSpans(base)
					rng := rand.New(rand.NewSource(seed))
					rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
					msgs := msgsA
					if seed%2 == 1 {
						msgs = msgsB
					}
					res := Verify(cppCaps, perm, &TraceExpectation{
						Subscription: "projects/p/subscriptions/s",
						UnaryPull:    &UnaryPullExpectation{Messages: msgs},
					})
					if !res.Passed() {
						t.Fatalf("op=%s seed=%d expected PASS, got:\n%s", op, seed, res.Format())
					}
					if seed == 0 {
						baseline = res.Format()
					} else if res.Format() != baseline {
						t.Fatalf("op=%s seed=%d Format() differed from baseline", op, seed)
					}
				}

				badSpans := cloneSpans(base)
				badSpans[3].Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = intAV(9)
				resBad := Verify(cppCaps, badSpans, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					UnaryPull:    &UnaryPullExpectation{Messages: msgsB},
				})
				assertFail(t, resBad, CodeWrongAttributeValue, "unary_pull."+op+".attr.delivery_attempt")
			})
		}
	})
}

func TestWave10Regressions_UnaryPullAckIDAndCppEnvelopeBodySize(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	t.Run("Hunt 5: UnaryPull AckID top-tier priority beats OrderingKey, EnvelopeSize, and unsampledTP", func(t *testing.T) {
		makeRecv := func(seq int, msgID, orderingKey string, envSize int64, links ...*proxypb.SpanLink) *proxypb.TraceSpan {
			r := cppSpan(testTraceID(61000+seq), testSpanID(610000+seq), "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
			r.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV("p"),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV("s"),
				"messaging.operation.type":        strAV("receive"),
				"code.function.name":              strAV("pubsub::SubscriberConnection::Pull"),
				"messaging.message.id":            strAV(msgID),
				"messaging.message.envelope.size": intAV(envSize),
			}
			if orderingKey != "" {
				r.Attributes["messaging.gcp_pubsub.message.ordering_key"] = strAV(orderingKey)
			}
			r.Links = links
			return r
		}
		makeAck := func(seq int, ackID string, recv *proxypb.TraceSpan) *proxypb.TraceSpan {
			a := cppSpan(testTraceID(62000+seq), testSpanID(620000+seq), "", "s ack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(10+seq)*time.Millisecond)
			a.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                      strAV("p"),
				"messaging.system":                    strAV("gcp_pubsub"),
				"messaging.destination.name":          strAV("s"),
				"messaging.operation.type":            strAV("ack"),
				"code.function.name":                  strAV("pubsub::PullAckHandler::ack"),
				"messaging.gcp_pubsub.message.ack_id": strAV(ackID),
			}
			a.Links = []*proxypb.SpanLink{{TraceId: recv.TraceId, SpanId: recv.SpanId}}
			return a
		}

		// Positive disambiguation via AckID even when OrderingKey, EnvelopeSize, and WireTraceparent are also present.
		tp00 := fmt.Sprintf("00-%s-%s-00", testTraceID(61900), testSpanID(619000))
		r0Pos := makeRecv(1, "m-up-pos", "k0", 30)
		r1Pos := makeRecv(2, "m-up-pos", "k1", 40)
		a0Pos := makeAck(1, "ack-pos-0", r0Pos)
		a1Pos := makeAck(2, "ack-pos-1", r1Pos)
		posBase := []*proxypb.TraceSpan{r0Pos, r1Pos, a0Pos, a1Pos}
		mPos0 := UnaryPullMessageExpectation{MessageID: "m-up-pos", AckID: "ack-pos-0", OrderingKey: "k0", EnvelopeSize: Int64Ptr(30), WireTraceparent: tp00, Settlements: []SettlementExpectation{{Operation: "ack"}}}
		mPos1 := UnaryPullMessageExpectation{MessageID: "m-up-pos", AckID: "ack-pos-1", OrderingKey: "k1", EnvelopeSize: Int64Ptr(40), WireTraceparent: tp00, Settlements: []SettlementExpectation{{Operation: "ack"}}}
		var posFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(posBase)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := []UnaryPullMessageExpectation{mPos0, mPos1}
			if seed%2 == 1 {
				msgs = []UnaryPullMessageExpectation{mPos1, mPos0}
			}
			res := Verify(cppCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull:    &UnaryPullExpectation{Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("Hunt 5 positive seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				posFmt = res.Format()
			} else if res.Format() != posFmt {
				t.Fatalf("Hunt 5 positive seed=%d Format() differed", seed)
			}
		}

		// Negative diagnostic attribution: r0 has ack-id-0 + wrong ordering/size/dangling-link, r1 has ack-id-1 + matching k/size/unsampled.
		// AckID (2_400_000) MUST NOT be overridden by OrderingKey (40_000) + EnvSize (20_000) + unsampledTP (50_000).
		r0 := makeRecv(10, "m-up", "wrong-k", 99, &proxypb.SpanLink{TraceId: testTraceID(61900), SpanId: testSpanID(619000)})
		r1 := makeRecv(11, "m-up", "k", 30)
		a0 := makeAck(10, "ack-id-0", r0)
		a1 := makeAck(11, "ack-id-1", r1)
		m0 := UnaryPullMessageExpectation{MessageID: "m-up", AckID: "ack-id-0", OrderingKey: "k", EnvelopeSize: Int64Ptr(30), WireTraceparent: tp00, Settlements: []SettlementExpectation{{Operation: "ack"}}}
		m1 := UnaryPullMessageExpectation{MessageID: "m-up", AckID: "ack-id-1", Settlements: []SettlementExpectation{{Operation: "ack"}}}

		for _, msgs := range [][]UnaryPullMessageExpectation{{m0, m1}, {m1, m0}} {
			res := Verify(cppCaps, []*proxypb.TraceSpan{r0, r1, a0, a1}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull:    &UnaryPullExpectation{Messages: msgs},
			})
			if res.Status != StatusFail {
				t.Fatalf("Hunt 5 negative expected FAIL, got PASS")
			}
			foundOrd := false
			for _, f := range res.Failures {
				if f.Rule == "unary_pull.ack.attr.ack_id" {
					t.Fatalf("Hunt 5: OrderingKey/EnvSize improperly overrode AckID, emitting ack_id swap:\n%s", res.Format())
				}
				if f.Rule == "unary_pull.receive.attr.ordering_key" {
					foundOrd = true
				}
			}
			if !foundOrd {
				t.Fatalf("Hunt 5: expected unary_pull.receive.attr.ordering_key failure, got:\n%s", res.Format())
			}
		}

		// Also verify AckID beats WireTraceparent (1_200_000) + MessageID (600_000) + OrderingKey + EnvSize.
		tp01 := fmt.Sprintf("00-%s-%s-01", testTraceID(61950), testSpanID(619500))
		rAckOnly := makeRecv(20, "m-wrong", "wrong-k", 99)
		aAckOnly := makeAck(20, "ack-id-0", rAckOnly)
		rTPMsg := makeRecv(21, "m-tp", "k", 30, &proxypb.SpanLink{TraceId: testTraceID(61950), SpanId: testSpanID(619500)})
		aTPMsg := makeAck(21, "ack-id-1", rTPMsg)
		mAckTP0 := UnaryPullMessageExpectation{MessageID: "m-tp", AckID: "ack-id-0", OrderingKey: "k", EnvelopeSize: Int64Ptr(30), WireTraceparent: tp01, Settlements: []SettlementExpectation{{Operation: "ack"}}}
		mAckTP1 := UnaryPullMessageExpectation{MessageID: "m-wrong", AckID: "ack-id-1", Settlements: []SettlementExpectation{{Operation: "ack"}}}
		for _, msgs := range [][]UnaryPullMessageExpectation{{mAckTP0, mAckTP1}, {mAckTP1, mAckTP0}} {
			resAckVsTP := Verify(cppCaps, []*proxypb.TraceSpan{rAckOnly, rTPMsg, aAckOnly, aTPMsg}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull:    &UnaryPullExpectation{Messages: msgs},
			})
			if resAckVsTP.Status != StatusFail {
				t.Fatalf("AckID vs WireTP+MessageID expected StatusFail, got %s", resAckVsTP.Status)
			}
			seenRules := make(map[string]bool)
			for _, f := range resAckVsTP.Failures {
				seenRules[f.Rule] = true
				if f.Rule == "unary_pull.ack.attr.ack_id" {
					t.Fatalf("AckID should outrank WireTP+MessageID without ack_id swap:\n%s", resAckVsTP.Format())
				}
			}
			for _, wantRule := range []string{
				"unary_pull.receive.attr.message_id",
				"unary_pull.receive.attr.ordering_key",
				"unary_pull.receive.attr.envelope_size",
				"unary_pull.receive.link_to_create",
			} {
				if !seenRules[wantRule] {
					t.Fatalf("AckID vs WireTP+MessageID missing expected rule %q:\n%s", wantRule, resAckVsTP.Format())
				}
			}
		}
	})

	t.Run("Hunt 2 (ProfileCPP): Subscribe AllowExtraPubSubSpans auto-EOD vs extra ALO and EOD acked spans", func(t *testing.T) {
		spansEODFail, _ := buildCppStreamingE2EFixture(true, true, true)
		subFail := cloneSpans(spansEODFail[6:]) // sub, cc, sched, proc, ackErr
		subFail[0].ParentSpanId = ""

		rekeySub := func(in []*proxypb.TraceSpan, base int, ackID string) []*proxypb.TraceSpan {
			out := cloneSpans(in)
			subSID := testSpanID(base * 10)
			subTID := testTraceID(base)
			for idx, s := range out {
				if s.GetName() == "s ack" {
					s.TraceId = testTraceID(base + 100)
					s.SpanId = testSpanID((base + 100) * 10)
					s.ParentSpanId = ""
					s.Attributes["messaging.gcp_pubsub.message.ack_id"] = strAV(ackID)
					s.Links[0].TraceId = subTID
					s.Links[0].SpanId = subSID
					continue
				}
				s.TraceId = subTID
				if s.GetName() == "s subscribe" {
					s.SpanId = subSID
					s.ParentSpanId = ""
					s.Attributes["messaging.gcp_pubsub.message.ack_id"] = strAV(ackID)
				} else {
					s.SpanId = testSpanID(base*10 + idx)
					s.ParentSpanId = subSID
				}
			}
			return out
		}

		spansALO, _ := buildCppStreamingE2EFixture(true, false, false)
		subALO := rekeySub(spansALO[6:], 6301, "ack-alo-extra")

		spansEODOK, _ := buildCppStreamingE2EFixture(true, true, false)
		subEODOK := rekeySub(spansEODOK[6:], 6302, "ack-eod-extra")

		base := append(append(subALO, subEODOK...), subFail...)
		exp := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "msg-1",
					ExactlyOnce: nil,
					Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true}},
				}},
			},
		}
		var baseFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(cppCaps, perm, exp)
			if !res.Passed() {
				t.Fatalf("Hunt 2 C++ seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseFmt = res.Format()
			} else if res.Format() != baseFmt {
				t.Fatalf("Hunt 2 C++ seed=%d Format() differed", seed)
			}
		}
	})

	t.Run("Whitebox coverage: C++ Subscribe EnvelopeSize/BodySize and UnaryPull Data envelope outer scoring", func(t *testing.T) {
		// 1. C++ Subscribe EnvelopeSize (exact) vs BodySize (lower bound >= 20+BodySize) disambiguating shared MessageID.
		makeCppSubSized := func(seq int, envSize int64) []*proxypb.TraceSpan {
			tid := testTraceID(6400 + seq)
			sid := testSpanID(64000 + seq*10)
			sub := cppSpan(tid, sid, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
			sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sub.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                          strAV("p"),
				"messaging.system":                                        strAV("gcp_pubsub"),
				"messaging.destination.name":                              strAV("s"),
				"messaging.operation.type":                                strAV("subscribe"),
				"messaging.message.id":                                    strAV("m-cpp-sz"),
				"messaging.message.envelope.size":                         intAV(envSize),
				"messaging.gcp_pubsub.message.ack_id":                     strAV(fmt.Sprintf("ca-sz-%d", seq)),
				"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(false),
				"messaging.gcp_pubsub.result":                             strAV("ack"),
			}
			sub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.ack_end"}}
			cc := cppSpan(tid, testSpanID(64001+seq*10), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			cc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			sched := cppSpan(tid, testSpanID(64002+seq*10), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			sched.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			proc := cppSpan(tid, testSpanID(64003+seq*10), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			proc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			ack := cppSpan(testTraceID(6450+seq), testSpanID(64500+seq), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 2*time.Millisecond)
			ack.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                             strAV("p"),
				"messaging.system":                           strAV("gcp_pubsub"),
				"messaging.destination.name":                 strAV("s"),
				"code.function.name":                         strAV("pubsub::AckHandler::ack"),
				"messaging.gcp_pubsub.message.ack_id":        strAV(fmt.Sprintf("ca-sz-%d", seq)),
				"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
				"messaging.operation.type":                   strAV("settle"),
				"gl-cpp.status_code":                         strAV("OK"),
			}
			ack.Links = []*proxypb.SpanLink{{TraceId: tid, SpanId: sid}}
			return []*proxypb.TraceSpan{sub, cc, sched, proc, ack}
		}
		spansSubSz := append(makeCppSubSized(1, 25), makeCppSubSized(2, 100)...)
		mSmall := SubscribeMessageExpectation{MessageID: "m-cpp-sz", EnvelopeSize: Int64Ptr(25), Settlements: []SettlementExpectation{{Operation: "ack"}}}
		mBigBody := SubscribeMessageExpectation{MessageID: "m-cpp-sz", BodySize: Int64Ptr(80), Settlements: []SettlementExpectation{{Operation: "ack"}}}
		for _, msgs := range [][]SubscribeMessageExpectation{{mBigBody, mSmall}, {mSmall, mBigBody}} {
			if res := Verify(cppCaps, spansSubSz, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
			}); !res.Passed() {
				t.Fatalf("C++ Subscribe EnvelopeSize vs BodySize disambiguation expected PASS, got:\n%s", res.Format())
			}
		}

		// 2. C++ UnaryPull Data + Attributes cppEnvelopeSize outer-scoring disambiguation (EnvelopeSize == nil).
		makeRecvSz := func(seq int, envSize int64) *proxypb.TraceSpan {
			r := cppSpan(testTraceID(6500+seq), testSpanID(65000+seq), "", "s receive", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
			r.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV("p"),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV("s"),
				"messaging.operation.type":        strAV("receive"),
				"code.function.name":              strAV("pubsub::SubscriberConnection::Pull"),
				"messaging.message.id":            strAV("m-up-sz"),
				"messaging.message.envelope.size": intAV(envSize),
			}
			return r
		}
		rSmall := makeRecvSz(1, 25)
		rLarge := makeRecvSz(2, 42)
		mUpSmall := UnaryPullMessageExpectation{MessageID: "m-up-sz", EnvelopeSize: Int64Ptr(25)}
		mUpData := UnaryPullMessageExpectation{MessageID: "m-up-sz", Data: []byte("hello-world"), Attributes: map[string]string{"k": "v"}}
		for _, msgs := range [][]UnaryPullMessageExpectation{{mUpData, mUpSmall}, {mUpSmall, mUpData}} {
			if res := Verify(cppCaps, []*proxypb.TraceSpan{rSmall, rLarge}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				UnaryPull:    &UnaryPullExpectation{Messages: msgs},
			}); !res.Passed() {
				t.Fatalf("C++ UnaryPull Data cppEnvelopeSize disambiguation expected PASS, got:\n%s", res.Format())
			}
		}
	})
}

func TestWave11Regressions_ProfileCppExplicitEODFailed(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	rekeyCppSub := func(in []*proxypb.TraceSpan, base int, ackID string, includeAck bool, envSize int64) []*proxypb.TraceSpan {
		out := cloneSpans(in)
		subSID := testSpanID(base * 10)
		subTID := testTraceID(base)
		var kept []*proxypb.TraceSpan
		for idx, s := range out {
			if s.GetName() == "s ack" {
				if !includeAck {
					continue
				}
				s.TraceId = testTraceID(base + 100)
				s.SpanId = testSpanID((base + 100) * 10)
				s.ParentSpanId = ""
				s.Attributes["messaging.gcp_pubsub.message.ack_id"] = strAV(ackID)
				s.Links[0].TraceId = subTID
				s.Links[0].SpanId = subSID
				kept = append(kept, s)
				continue
			}
			s.TraceId = subTID
			if s.GetName() == "s subscribe" {
				s.SpanId = subSID
				s.ParentSpanId = ""
				s.Attributes["messaging.gcp_pubsub.message.ack_id"] = strAV(ackID)
				if envSize > 0 {
					s.Attributes["messaging.message.envelope.size"] = intAV(envSize)
				}
			} else {
				s.SpanId = testSpanID(base*10 + idx)
				s.ParentSpanId = subSID
			}
			kept = append(kept, s)
		}
		return kept
	}

	t.Run("Probe 2B Case A: ALO span rejected under explicit eod_failed with ExactlyOnce=nil", func(t *testing.T) {
		spansALO, _ := buildCppStreamingE2EFixture(true, false, false)
		subALO := rekeyCppSub(spansALO[6:], 7101, "ack-alo-a", true, 25)

		for _, outcome := range []string{"eod_failed", "EOD_FAILED"} {
			for _, settles := range [][]SettlementExpectation{nil, {{Operation: "ack"}}} {
				res := Verify(cppCaps, subALO, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "msg-1",
							ExactlyOnce:     nil,
							ExpectedOutcome: outcome,
							Settlements:     settles,
						}},
					},
				})
				assertHasViolation(t, res, CodeWrongAttributeValue, "subscribe.attr.exactly_once_delivery")
			}
		}

		// Also reject missing exactly_once_delivery attribute under ExpectedOutcome: "eod_failed".
		subMissingEOS := cloneSpans(subALO)
		delete(subMissingEOS[0].Attributes, "messaging.gcp_pubsub.subscription.exactly_once_delivery")
		resMissing := Verify(cppCaps, subMissingEOS, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "msg-1",
					ExactlyOnce:     nil,
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		})
		assertHasViolation(t, resMissing, CodeMissingAttribute, "subscribe.attr.exactly_once_delivery")
	})

	t.Run("Probe 2B Case B: AllowExtraPubSubSpans extra ALO cannot steal from broken EOD span missing result", func(t *testing.T) {
		spansALO, _ := buildCppStreamingE2EFixture(true, false, false)
		subALO := rekeyCppSub(spansALO[6:], 7201, "ack-alo-b", false, 25)

		spansEOD, _ := buildCppStreamingE2EFixture(true, true, true)
		subBrokenEOD := rekeyCppSub(spansEOD[6:], 7202, "ack-eod-b", false, 25)
		delete(subBrokenEOD[0].Attributes, "messaging.gcp_pubsub.result")

		modack := cppSpan(testTraceID(7250), testSpanID(72500), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, 5*time.Millisecond)
		modack.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		modack.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                    strAV("p"),
			"messaging.system":                                  strAV("gcp_pubsub"),
			"messaging.destination.name":                        strAV("s"),
			"messaging.operation.type":                          strAV("extend"),
			"messaging.batch.message_count":                     intAV(1),
			"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(60),
		}
		modack.Links = []*proxypb.SpanLink{{TraceId: subBrokenEOD[0].TraceId, SpanId: subBrokenEOD[0].SpanId}}

		baseNoModack := append(cloneSpans(subALO), cloneSpans(subBrokenEOD)...)
		baseWithModack := append(cloneSpans(baseNoModack), modack)

		for _, tc := range []struct {
			name    string
			spans   []*proxypb.TraceSpan
			settles []SettlementExpectation
		}{
			{name: "Settlements nil", spans: baseNoModack, settles: nil},
			{name: "modack only", spans: baseWithModack, settles: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exp := &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "msg-1",
							ExactlyOnce:     nil,
							ExpectedOutcome: "eod_failed",
							Settlements:     tc.settles,
						}},
					},
				}
				var baseFmt string
				for seed := int64(0); seed < 24; seed++ {
					perm := cloneSpans(tc.spans)
					rng := rand.New(rand.NewSource(seed))
					rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
					res := Verify(cppCaps, perm, exp)
					assertHasViolation(t, res, CodeMissingAttribute, "subscribe.attr.result")
					for _, f := range res.Failures {
						if f.Rule == "subscribe.attr.exactly_once_delivery" {
							t.Fatalf("extra ALO stole from broken EOD span:\n%s", res.Format())
						}
					}
					if seed == 0 {
						baseFmt = res.Format()
					} else if res.Format() != baseFmt {
						t.Fatalf("Probe 2B Case B seed=%d Format() differed", seed)
					}
				}
			})
		}
	})

	t.Run("Probe 2B Case C: AllowExtraPubSubSpans extra ALO matching EnvelopeSize cannot steal from broken EOD span", func(t *testing.T) {
		spansALO, _ := buildCppStreamingE2EFixture(true, false, false)
		subALO := rekeyCppSub(spansALO[6:], 7301, "ack-alo-c", true, 25)

		spansEOD, _ := buildCppStreamingE2EFixture(true, true, true)
		subBrokenEOD := rekeyCppSub(spansEOD[6:], 7302, "ack-eod-c", true, 99) // wrong EnvelopeSize!

		base := append(cloneSpans(subALO), cloneSpans(subBrokenEOD)...)
		for _, settles := range [][]SettlementExpectation{nil, {{Operation: "ack", ExpectError: true}}} {
			exp := &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "msg-1",
						EnvelopeSize:    Int64Ptr(25),
						ExactlyOnce:     nil,
						ExpectedOutcome: "eod_failed",
						Settlements:     settles,
					}},
				},
			}
			var baseFmt string
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(base)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				res := Verify(cppCaps, perm, exp)
				assertHasViolation(t, res, CodeWrongAttributeValue, "subscribe.attr.envelope_size")
				for _, f := range res.Failures {
					if f.Rule == "subscribe.attr.exactly_once_delivery" {
						t.Fatalf("extra ALO matching EnvelopeSize stole from broken EOD span:\n%s", res.Format())
					}
				}
				if seed == 0 {
					baseFmt = res.Format()
				} else if res.Format() != baseFmt {
					t.Fatalf("Probe 2B Case C seed=%d Format() differed", seed)
				}
			}
		}
	})

	t.Run("Positive explicit eod_failed with ExactlyOnce=nil passes standalone and with extra ALO/EOD spans", func(t *testing.T) {
		spansEODFail, _ := buildCppStreamingE2EFixture(true, true, true)
		subFail := rekeyCppSub(spansEODFail[6:], 7400, "ack-eod-fail", true, 25)
		subFailNoAck := rekeyCppSub(spansEODFail[6:], 7400, "ack-eod-fail", false, 25)

		// Standalone PASS (both with ack Settlement and with Settlements omitted).
		for _, tc := range []struct {
			spans   []*proxypb.TraceSpan
			settles []SettlementExpectation
		}{
			{spans: subFailNoAck, settles: nil},
			{spans: subFail, settles: []SettlementExpectation{{Operation: "ack", ExpectError: true}}},
		} {
			res := Verify(cppCaps, cloneSpans(tc.spans), &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "msg-1",
						ExactlyOnce:     nil,
						ExpectedOutcome: "eod_failed",
						Settlements:     tc.settles,
					}},
				},
			})
			if !res.Passed() {
				t.Fatalf("ProfileCPP explicit eod_failed standalone expected PASS, got:\n%s", res.Format())
			}
		}

		// Multi-span with extra ALO, extra ALO error, extra EOD OK across 24 shuffles.
		spansALO, _ := buildCppStreamingE2EFixture(true, false, false)
		subALO := rekeyCppSub(spansALO[6:], 7401, "ack-alo-extra", true, 25)
		spansALOErr, _ := buildCppStreamingE2EFixture(true, false, true)
		subALOErr := rekeyCppSub(spansALOErr[6:], 7402, "ack-aloe-extra", true, 25)
		spansEODOK, _ := buildCppStreamingE2EFixture(true, true, false)
		subEODOK := rekeyCppSub(spansEODOK[6:], 7403, "ack-eod-ok-extra", true, 25)

		base := append(append(append(cloneSpans(subALO), subALOErr...), subEODOK...), subFail...)
		exp := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "msg-1",
					ExactlyOnce:     nil,
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "ack", ExpectError: true}},
				}},
			},
		}
		var baseFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(cppCaps, perm, exp)
			if !res.Passed() {
				t.Fatalf("ProfileCPP explicit eod_failed w/ extras seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseFmt = res.Format()
			} else if res.Format() != baseFmt {
				t.Fatalf("ProfileCPP explicit eod_failed w/ extras seed=%d Format() differed", seed)
			}
		}
	})
}

func TestWave12Regressions_ProfileCppEODFailedResultTie(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	makeCppSubOnly := func(seq int, msgID, ackID string, isEOD bool, result string, envSize int64, offset time.Duration) []*proxypb.TraceSpan {
		tid := testTraceID(13000 + seq)
		sid := testSpanID(130000 + seq*10)
		sub := cppSpan(tid, sid, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, offset)
		sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		sub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                          strAV("p"),
			"messaging.system":                                        strAV("gcp_pubsub"),
			"messaging.destination.name":                              strAV("s"),
			"messaging.operation.type":                                strAV("subscribe"),
			"messaging.message.id":                                    strAV(msgID),
			"messaging.message.envelope.size":                         intAV(envSize),
			"messaging.gcp_pubsub.message.ack_id":                     strAV(ackID),
			"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(isEOD),
		}
		if result != "" {
			sub.Attributes["messaging.gcp_pubsub.result"] = strAV(result)
			if result == "ack" || result == "nack" {
				sub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp." + result + "_start"}, {Name: "gl-cpp." + result + "_end"}}
			}
		}
		cc := cppSpan(tid, testSpanID(130001+seq*10), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, offset+time.Millisecond)
		cc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		sched := cppSpan(tid, testSpanID(130002+seq*10), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, offset+2*time.Millisecond)
		sched.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		proc := cppSpan(tid, testSpanID(130003+seq*10), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, offset+3*time.Millisecond)
		proc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		return []*proxypb.TraceSpan{sub, cc, sched, proc}
	}

	makeModack := func(seq int, sub *proxypb.TraceSpan) *proxypb.TraceSpan {
		mod := cppSpan(testTraceID(13500+seq), testSpanID(135000+seq), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, 50*time.Millisecond)
		mod.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		mod.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                    strAV("p"),
			"messaging.system":                                  strAV("gcp_pubsub"),
			"messaging.destination.name":                        strAV("s"),
			"messaging.operation.type":                          strAV("extend"),
			"messaging.batch.message_count":                     intAV(1),
			"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(60),
		}
		if sub != nil {
			mod.Links = []*proxypb.SpanLink{{TraceId: sub.TraceId, SpanId: sub.SpanId}}
		}
		return mod
	}

	t.Run("Bug B: earlier broken EOD missing result does not steal from later valid EOD (ack and nack, Settlements=nil and modack-only)", func(t *testing.T) {
		for _, validResult := range []string{"ack", "nack"} {
			brokenEOD := makeCppSubOnly(1, "msg-1", "ack-broken", true, "", 25, 1*time.Millisecond)
			extraALO := makeCppSubOnly(2, "msg-1", "ack-alo", false, "ack", 25, 2*time.Millisecond)
			validEOD := makeCppSubOnly(3, "msg-1", "ack-valid", true, validResult, 25, 10*time.Millisecond)
			validEOD[0].Events = append(validEOD[0].Events, &proxypb.SpanEvent{Name: "gl-cpp.modack_start"}, &proxypb.SpanEvent{Name: "gl-cpp.modack_end"})
			modValid := makeModack(1, validEOD[0])

			baseNoModack := append(append(cloneSpans(brokenEOD), cloneSpans(extraALO)...), cloneSpans(validEOD)...)
			baseWithModack := append(cloneSpans(baseNoModack), modValid)

			for _, tc := range []struct {
				name    string
				spans   []*proxypb.TraceSpan
				settles []SettlementExpectation
			}{
				{name: validResult + "_Settlements_nil", spans: baseNoModack, settles: nil},
				{name: validResult + "_modack_only", spans: baseWithModack, settles: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					exp := &TraceExpectation{
						Subscription:          "projects/p/subscriptions/s",
						AllowExtraPubSubSpans: true,
						Subscribe: &SubscribeExpectation{
							Messages: []SubscribeMessageExpectation{{
								MessageID:       "msg-1",
								ExactlyOnce:     nil,
								ExpectedOutcome: "eod_failed",
								Settlements:     tc.settles,
							}},
						},
					}
					var baseFmt string
					for seed := int64(0); seed < 24; seed++ {
						perm := cloneSpans(tc.spans)
						rng := rand.New(rand.NewSource(seed))
						rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
						res := Verify(cppCaps, perm, exp)
						if !res.Passed() {
							t.Fatalf("%s seed=%d expected PASS, got:\n%s", tc.name, seed, res.Format())
						}
						if seed == 0 {
							baseFmt = res.Format()
						} else if res.Format() != baseFmt {
							t.Fatalf("%s seed=%d Format() differed", tc.name, seed)
						}
					}
				})
			}
		}
	})

	t.Run("Bug B dual-message multi-order: broken EOD + valid EOD (ack/nack) + companion message", func(t *testing.T) {
		for _, validResult := range []string{"ack", "nack"} {
			brokenEOD := makeCppSubOnly(11, "msg-eod", "ack-broken-2", true, "", 25, 1*time.Millisecond)
			extraALO := makeCppSubOnly(12, "msg-eod", "ack-alo-2", false, "ack", 25, 2*time.Millisecond)
			validEOD := makeCppSubOnly(13, "msg-eod", "ack-valid-2", true, validResult, 25, 10*time.Millisecond)
			otherAcked := makeCppSubOnly(14, "msg-other", "ack-other", true, "ack", 30, 15*time.Millisecond)

			base := append(append(append(cloneSpans(brokenEOD), cloneSpans(extraALO)...), cloneSpans(validEOD)...), cloneSpans(otherAcked)...)
			mEOD := SubscribeMessageExpectation{
				MessageID:       "msg-eod",
				ExactlyOnce:     nil,
				ExpectedOutcome: "eod_failed",
				Settlements:     nil,
			}
			mOther := SubscribeMessageExpectation{
				MessageID:       "msg-other",
				ExactlyOnce:     BoolPtr(true),
				ExpectedOutcome: "acked",
			}

			var baseFmt string
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(base)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				msgs := []SubscribeMessageExpectation{mEOD, mOther}
				if seed%2 == 1 {
					msgs = []SubscribeMessageExpectation{mOther, mEOD}
				}
				res := Verify(cppCaps, perm, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe:             &SubscribeExpectation{Messages: msgs},
				})
				if !res.Passed() {
					t.Fatalf("dual-message validResult=%s seed=%d expected PASS, got:\n%s", validResult, seed, res.Format())
				}
				if seed == 0 {
					baseFmt = res.Format()
				} else if res.Format() != baseFmt {
					t.Fatalf("dual-message validResult=%s seed=%d Format() differed", validResult, seed)
				}
			}
		}
	})

	t.Run("Bug B negative confirmation: only broken EOD missing result + extra ALO still fails subscribe.attr.result", func(t *testing.T) {
		brokenEOD := makeCppSubOnly(21, "msg-1", "ack-broken-only", true, "", 25, 1*time.Millisecond)
		brokenEOD[0].Events = append(brokenEOD[0].Events, &proxypb.SpanEvent{Name: "gl-cpp.modack_start"}, &proxypb.SpanEvent{Name: "gl-cpp.modack_end"})
		modBroken := makeModack(21, brokenEOD[0])
		extraALO := makeCppSubOnly(22, "msg-1", "ack-alo-only", false, "ack", 25, 2*time.Millisecond)

		baseNoModack := append(cloneSpans(brokenEOD), cloneSpans(extraALO)...)
		baseWithModack := append(cloneSpans(baseNoModack), modBroken)

		for _, tc := range []struct {
			name    string
			spans   []*proxypb.TraceSpan
			settles []SettlementExpectation
		}{
			{name: "Settlements_nil", spans: baseNoModack, settles: nil},
			{name: "modack_only", spans: baseWithModack, settles: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exp := &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "msg-1",
							ExactlyOnce:     nil,
							ExpectedOutcome: "eod_failed",
							Settlements:     tc.settles,
						}},
					},
				}
				var baseFmt string
				for seed := int64(0); seed < 24; seed++ {
					perm := cloneSpans(tc.spans)
					rng := rand.New(rand.NewSource(seed))
					rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
					res := Verify(cppCaps, perm, exp)
					if res.Status != StatusFail {
						t.Fatalf("%s seed=%d expected FAIL, got PASS", tc.name, seed)
					}
					foundResultMiss := false
					for _, f := range res.Failures {
						if f.Rule == "subscribe.attr.exactly_once_delivery" {
							t.Fatalf("%s seed=%d extra ALO stole from broken EOD:\n%s", tc.name, seed, res.Format())
						}
						if f.Code == CodeMissingAttribute && f.Rule == "subscribe.attr.result" {
							foundResultMiss = true
						}
					}
					if !foundResultMiss {
						t.Fatalf("%s seed=%d expected CodeMissingAttribute subscribe.attr.result, got:\n%s", tc.name, seed, res.Format())
					}
					if seed == 0 {
						baseFmt = res.Format()
					} else if res.Format() != baseFmt {
						t.Fatalf("%s seed=%d Format() differed", tc.name, seed)
					}
				}
			})
		}
	})

	t.Run("Bug B scoring hierarchy: +4_000 result tiebreaker keeps maxNonID < MessageID (200_000)", func(t *testing.T) {
		subTrue := makeCppSubOnly(31, "m-true", "a-true", true, "", 99, 1*time.Millisecond)
		subFalse := makeCppSubOnly(32, "m-false", "a-false", true, "ack", 40, 2*time.Millisecond)
		subFalse[0].Attributes["messaging.gcp_pubsub.message.ordering_key"] = strAV("ok-k")
		reqAttrs := make(map[string]AttrMatcher, 10)
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("cpp.attr.%d", i)
			v := fmt.Sprintf("v-%d", i)
			subFalse[0].Attributes[k] = strAV(v)
			reqAttrs[k] = MatchString(v)
		}
		spans := append(cloneSpans(subFalse), cloneSpans(subTrue)...)
		mTrue := SubscribeMessageExpectation{
			MessageID:         "m-true",
			OrderingKey:       "ok-k",
			EnvelopeSize:      Int64Ptr(40),
			ExpectedOutcome:   "eod_failed",
			RequireAttributes: reqAttrs,
		}
		mFalse := SubscribeMessageExpectation{MessageID: "m-false"}
		for _, msgs := range [][]SubscribeMessageExpectation{{mTrue, mFalse}, {mFalse, mTrue}} {
			res := Verify(cppCaps, spans, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe:             &SubscribeExpectation{Messages: msgs},
			})
			if res.Status != StatusFail {
				t.Fatalf("expected FAIL, got PASS")
			}
			foundResult := false
			for _, f := range res.Failures {
				if f.Rule == "subscribe.attr.message_id" {
					t.Fatalf("Bug B +4_000 caused non-ID score to override MessageID:\n%s", res.Format())
				}
				if f.Code == CodeMissingAttribute && f.Rule == "subscribe.attr.result" && f.SpanID == subTrue[0].SpanId {
					foundResult = true
				}
			}
			if !foundResult {
				t.Fatalf("expected subscribe.attr.result on subTrue, got:\n%s", res.Format())
			}
		}
	})
}

func TestWave13Regressions_ProfileCppAllowExtraBrokenModack(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	makeCppSubOnly := func(seq int, msgID, ackID string, isEOD bool, result string, envSize int64, offset time.Duration) []*proxypb.TraceSpan {
		tid := testTraceID(14000 + seq)
		sid := testSpanID(140000 + seq*10)
		sub := cppSpan(tid, sid, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, offset)
		sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		sub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                          strAV("p"),
			"messaging.system":                                        strAV("gcp_pubsub"),
			"messaging.destination.name":                              strAV("s"),
			"messaging.operation.type":                                strAV("subscribe"),
			"messaging.message.id":                                    strAV(msgID),
			"messaging.message.envelope.size":                         intAV(envSize),
			"messaging.gcp_pubsub.message.ack_id":                     strAV(ackID),
			"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(isEOD),
		}
		if result != "" {
			sub.Attributes["messaging.gcp_pubsub.result"] = strAV(result)
			if result == "ack" || result == "nack" {
				sub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp." + result + "_start"}, {Name: "gl-cpp." + result + "_end"}}
			}
		}
		cc := cppSpan(tid, testSpanID(140001+seq*10), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, offset+time.Millisecond)
		cc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		sched := cppSpan(tid, testSpanID(140002+seq*10), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, offset+2*time.Millisecond)
		sched.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		proc := cppSpan(tid, testSpanID(140003+seq*10), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, offset+3*time.Millisecond)
		proc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		return []*proxypb.TraceSpan{sub, cc, sched, proc}
	}

	makeCppModack := func(seq int, sub *proxypb.TraceSpan) *proxypb.TraceSpan {
		mod := cppSpan(testTraceID(14500+seq), testSpanID(145000+seq), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(40+seq)*time.Millisecond)
		mod.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		mod.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                    strAV("p"),
			"messaging.system":                                  strAV("gcp_pubsub"),
			"messaging.destination.name":                        strAV("s"),
			"messaging.operation.type":                          strAV("extend"),
			"messaging.batch.message_count":                     intAV(1),
			"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(60),
		}
		if sub != nil {
			mod.Links = []*proxypb.SpanLink{{TraceId: sub.TraceId, SpanId: sub.SpanId}}
			sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: "gl-cpp.modack_start"}, &proxypb.SpanEvent{Name: "gl-cpp.modack_end"})
		}
		return mod
	}

	t.Run("Case 3: Bug B with C++ modack spans on BOTH broken earlier EOD and valid later EOD (Settlements=nil and dual orders)", func(t *testing.T) {
		for _, validResult := range []string{"ack", "nack"} {
			brokenEOD := makeCppSubOnly(1, "msg-cpp-b", "ca-broken", true, "", 25, 1*time.Millisecond)
			modBroken := makeCppModack(1, brokenEOD[0])

			extraALO := makeCppSubOnly(2, "msg-cpp-b", "ca-alo", false, "ack", 25, 2*time.Millisecond)
			modALO := makeCppModack(2, extraALO[0])

			validEOD := makeCppSubOnly(3, "msg-cpp-b", "ca-valid", true, validResult, 25, 10*time.Millisecond)
			modValid := makeCppModack(3, validEOD[0])

			companion := makeCppSubOnly(4, "msg-cpp-other", "ca-other", true, "ack", 30, 15*time.Millisecond)
			modComp := makeCppModack(4, companion[0])

			singleBase := append(append(append(cloneSpans(brokenEOD), modBroken), append(cloneSpans(extraALO), modALO)...), append(cloneSpans(validEOD), modValid)...)
			dualBase := append(cloneSpans(singleBase), append(cloneSpans(companion), modComp)...)

			// 1. Single-message Settlements=nil across 24 shuffles -> PASS.
			expSingle := &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "msg-cpp-b",
						ExactlyOnce:     nil,
						ExpectedOutcome: "eod_failed",
						Settlements:     nil,
					}},
				},
			}
			var singleFmt string
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(singleBase)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				res := Verify(cppCaps, perm, expSingle)
				if !res.Passed() {
					t.Fatalf("Case 3 single (%s) seed=%d expected PASS, got:\n%s", validResult, seed, res.Format())
				}
				if seed == 0 {
					singleFmt = res.Format()
				} else if res.Format() != singleFmt {
					t.Fatalf("Case 3 single (%s) seed=%d Format() differed", validResult, seed)
				}
			}

			// 2. Dual-message across 24 shuffles and BOTH Messages orders -> PASS with byte-identical Format().
			mEOD := SubscribeMessageExpectation{
				MessageID:       "msg-cpp-b",
				ExactlyOnce:     nil,
				ExpectedOutcome: "eod_failed",
				Settlements:     nil,
			}
			mComp := SubscribeMessageExpectation{
				MessageID:       "msg-cpp-other",
				ExactlyOnce:     BoolPtr(true),
				ExpectedOutcome: "acked",
			}
			var dualFmt string
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(dualBase)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				msgs := []SubscribeMessageExpectation{mEOD, mComp}
				if seed%2 == 1 {
					msgs = []SubscribeMessageExpectation{mComp, mEOD}
				}
				res := Verify(cppCaps, perm, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe:             &SubscribeExpectation{Messages: msgs},
				})
				if !res.Passed() {
					t.Fatalf("Case 3 dual (%s) seed=%d expected PASS, got:\n%s", validResult, seed, res.Format())
				}
				if seed == 0 {
					dualFmt = res.Format()
				} else if res.Format() != dualFmt {
					t.Fatalf("Case 3 dual (%s) seed=%d Format() differed", validResult, seed)
				}
			}

			// 3. Negative contrasts: AllowExtraPubSubSpans=false and StrictSettlementOps=true MUST fail.
			assertHasViolation(t, Verify(cppCaps, cloneSpans(singleBase), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				Subscribe:             expSingle.Subscribe,
			}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

			assertHasViolation(t, Verify(cppCaps, cloneSpans(singleBase), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					StrictSettlementOps: true,
					Messages:            expSingle.Subscribe.Messages,
				},
			}), CodeMissingLink, "subscribe.settlement.dangling_link")
		}
	})
}

func TestWave14Regressions_ProfileCppPublishAllowExtraAndSubscribeAudit(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	t.Run("Cpp Obj1: strict extra subscribe target filtering", func(t *testing.T) {
		base, exp := buildCppStreamingE2EFixture(true, true, false)
		subOnly := cloneSpans(base[6:]) // sub, cc, sched, proc, ack

		// Valid extra C++ subscribe target + extra s ack -> PASS across 24 shuffles.
		extraSub := cppSpan(testTraceID(15010), testSpanID(150100), "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, 5*time.Millisecond)
		extraSub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		extraSub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                          strAV("p"),
			"messaging.system":                                        strAV("gcp_pubsub"),
			"messaging.destination.name":                              strAV("s"),
			"messaging.operation.type":                                strAV("subscribe"),
			"messaging.message.id":                                    strAV("msg-extra"),
			"messaging.message.envelope.size":                         intAV(25),
			"messaging.gcp_pubsub.message.ack_id":                     strAV("ack-extra"),
			"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(true),
			"messaging.gcp_pubsub.result":                             strAV("ack"),
		}
		extraAck := cppSpan(testTraceID(15011), testSpanID(150110), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 6*time.Millisecond)
		extraAck.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                             strAV("p"),
			"messaging.system":                           strAV("gcp_pubsub"),
			"messaging.destination.name":                 strAV("s"),
			"code.function.name":                         strAV("pubsub::AckHandler::ack"),
			"messaging.gcp_pubsub.message.ack_id":        strAV("ack-extra"),
			"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
			"messaging.operation.type":                   strAV("settle"),
			"gl-cpp.status_code":                         strAV("OK"),
		}
		extraAck.Links = []*proxypb.SpanLink{{TraceId: extraSub.TraceId, SpanId: extraSub.SpanId}}

		expPass := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe:             exp.Subscribe,
		}
		expPass.Subscribe.StrictSettlementOps = false
		validAll := append(cloneSpans(subOnly), extraSub, extraAck)
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(validAll)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			if res := Verify(cppCaps, perm, expPass); !res.Passed() {
				t.Fatalf("Cpp Obj1 valid extra seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
		}

		// Negative probes: wrong SpanKind (INTERNAL instead of CONSUMER) and cross-SDK scope (GoPubSubScopeName) -> FAIL dangling_link.
		wrongKind := cloneSpans([]*proxypb.TraceSpan{extraSub, extraAck})
		wrongKind[0].Kind = proxypb.SpanKind_SPAN_KIND_INTERNAL
		assertHasViolation(t, Verify(cppCaps, append(cloneSpans(subOnly), wrongKind...), expPass), CodeMissingLink, "subscribe.settlement.dangling_link")

		wrongScope := cloneSpans([]*proxypb.TraceSpan{extraSub, extraAck})
		wrongScope[0].InstrumentationScopeName = GoPubSubScopeName
		assertHasViolation(t, Verify(cppCaps, append(cloneSpans(subOnly), wrongScope...), expPass), CodeMissingLink, "subscribe.settlement.dangling_link")
	})

	t.Run("Cpp Obj2: Edge Case A eod_failed Settlements=nil and link_incompatible_op", func(t *testing.T) {
		// 1. C++ eod_failed with Settlements: nil passes under both AllowExtraPubSubSpans false & true.
		baseFail, expFail := buildCppStreamingE2EFixture(true, true, true)
		subFailOnly := cloneSpans(baseFail[6:]) // sub(UNSET, result="ack"), cc, sched, proc, ack(ERROR)
		for _, allowExtra := range []bool{false, true} {
			expNilSettle := &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: allowExtra,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "msg-1",
						AckID:           "ack-1",
						WireTraceparent: expFail.Subscribe.WireTraceparents[0],
						ExpectedOutcome: "eod_failed",
						Settlements:     nil,
					}},
				},
			}
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(subFailOnly)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				if res := Verify(cppCaps, perm, expNilSettle); !res.Passed() {
					t.Fatalf("Cpp Edge Case A3 eod_failed (allowExtra=%v) seed=%d expected PASS, got:\n%s", allowExtra, seed, res.Format())
				}
			}
		}

		// 2. Incompatible extra C++ s ack linking to matched "nacked" subscribe span -> FAIL under BOTH strict & non-strict.
		baseNack, expNack := buildCppStreamingE2EFixture(true, false, false)
		subNackOnly := cloneSpans(baseNack[6:])
		subNackOnly[0].Attributes["messaging.gcp_pubsub.result"] = strAV("nack")
		subNackOnly[0].Events = []*proxypb.SpanEvent{{Name: "gl-cpp.nack_start"}, {Name: "gl-cpp.message_nack"}, {Name: "gl-cpp.nack_end"}}
		// Keep subNackOnly[4] as stray "s ack" linking to matched "nacked" sub!
		for _, strictOps := range []bool{false, true} {
			assertHasViolation(t, Verify(cppCaps, cloneSpans(subNackOnly), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					StrictSettlementOps: strictOps,
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "msg-1",
						AckID:           "ack-1",
						WireTraceparent: expNack.Subscribe.WireTraceparents[0],
						ExactlyOnce:     BoolPtr(false),
						ExpectedOutcome: "nacked",
						Settlements:     nil,
					}},
				},
			}), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
		}
	})

	t.Run("Cpp Obj4: Publish AllowExtraPubSubSpans=true separate, shared, >128 fanout, and negative checks", func(t *testing.T) {
		makeCppCreateAndChildren := func(seq int, msgID string, pub *proxypb.TraceSpan) ([]*proxypb.TraceSpan, *proxypb.TraceSpan, string) {
			cTID := testTraceID(15100 + seq)
			cSID := testSpanID(151000 + seq*10)
			c := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(seq)*time.Microsecond)
			c.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                  strAV("p"),
				"messaging.system":                strAV("gcp_pubsub"),
				"messaging.destination.name":      strAV("t"),
				"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
				"messaging.operation.type":        strAV("create"),
				"messaging.message.envelope.size": intAV(25),
				"messaging.message.id":            strAV(msgID),
				"gl-cpp.status_code":              strAV("OK"),
			}
			c.Events = []*proxypb.SpanEvent{
				{Name: "gl-cpp.added_to_batch"},
				{Name: "gl-cpp.publish_start"},
				{Name: "gl-cpp.publish_end"},
			}
			if pub != nil {
				c.Links = []*proxypb.SpanLink{{TraceId: pub.TraceId, SpanId: pub.SpanId}}
				pub.Links = append(pub.Links, &proxypb.SpanLink{TraceId: cTID, SpanId: cSID})
			}
			fc := cppSpan(cTID, testSpanID(151001+seq*10), cSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
			fc.Attributes = map[string]*proxypb.AttributeValue{
				"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish"),
				"gl-cpp.status_code": strAV("OK"),
			}
			bc := cppSpan(cTID, testSpanID(151002+seq*10), cSID, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
			bc.Attributes = map[string]*proxypb.AttributeValue{
				"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish"),
				"gl-cpp.status_code": strAV("OK"),
			}
			return []*proxypb.TraceSpan{c, fc, bc}, c, fmt.Sprintf("00-%s-%s-01", cTID, cSID)
		}
		makeCppPub := func(seq int, batchCount int64) *proxypb.TraceSpan {
			pub := cppSpan(testTraceID(15500+seq), testSpanID(155000+seq), "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 10*time.Millisecond)
			pub.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                strAV("p"),
				"messaging.system":              strAV("gcp_pubsub"),
				"messaging.destination.name":    strAV("t"),
				"messaging.batch.message_count": intAV(batchCount),
				"messaging.operation.type":      strAV("publish"),
				"code.function.name":            strAV("BatchSink::AsyncPublish"),
				"gl-cpp.status_code":            strAV("OK"),
			}
			return pub
		}

		// 1. Separate extra C++ batch + [M1].
		base, exp := buildCppStreamingE2EFixture(true, false, false)
		pubOnlyM1 := cloneSpans(base[:5]) // create, fc, batch, flush, pub
		pubExtra := makeCppPub(1, 1)
		cExtraSpans, _, _ := makeCppCreateAndChildren(1, "msg-cpp-extra", pubExtra)
		sepCpp := append(append([]*proxypb.TraceSpan{}, pubOnlyM1...), append(cExtraSpans, pubExtra)...)
		expCppM1 := &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{
					MessageID:       "msg-1",
					WireTraceparent: exp.Publish.WireTraceparents[0],
				}},
			},
		}
		var sepCppFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(sepCpp)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(cppCaps, perm, expCppM1)
			if !res.Passed() {
				t.Fatalf("Cpp separate extra seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				sepCppFmt = res.Format()
			} else if res.Format() != sepCppFmt {
				t.Fatalf("Cpp separate extra seed=%d Format() differed", seed)
			}
		}

		// 2. Shared C++ batch [M1, M2_extra] (batch_count=2).
		sharedCpp := cloneSpans(base[:5])
		sharedCpp[4].Attributes["messaging.batch.message_count"] = intAV(2)
		cShSpans, cSh, _ := makeCppCreateAndChildren(2, "msg-cpp-sh", nil)
		cSh.Links = []*proxypb.SpanLink{{TraceId: sharedCpp[4].TraceId, SpanId: sharedCpp[4].SpanId}}
		sharedCpp[4].Links = append(sharedCpp[4].Links, &proxypb.SpanLink{TraceId: cSh.TraceId, SpanId: cSh.SpanId})
		sharedAll := append(sharedCpp, cShSpans...)
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(sharedAll)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			if res := Verify(cppCaps, perm, expCppM1); !res.Passed() {
				t.Fatalf("Cpp shared batch seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
		}

		// 3. C++ >128 "publish #0"/"publish #1" extra fanout batch alongside [M1] -> PASS across 24 shuffles.
		pubBig := makeCppPub(2, 129)
		ch0 := cppSpan(pubBig.TraceId, testSpanID(155101), pubBig.SpanId, "publish #0", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		ch1 := cppSpan(pubBig.TraceId, testSpanID(155102), pubBig.SpanId, "publish #1", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		fanSpans := append(cloneSpans(pubOnlyM1), pubBig, ch0, ch1)
		for i := 0; i < 129; i++ {
			cFanSpans, cFan, _ := makeCppCreateAndChildren(10+i, fmt.Sprintf("msg-fan-%d", i), nil)
			cFan.Links = []*proxypb.SpanLink{{TraceId: pubBig.TraceId, SpanId: pubBig.SpanId}}
			if i < 128 {
				ch0.Links = append(ch0.Links, &proxypb.SpanLink{TraceId: cFan.TraceId, SpanId: cFan.SpanId})
			} else {
				ch1.Links = append(ch1.Links, &proxypb.SpanLink{TraceId: cFan.TraceId, SpanId: cFan.SpanId})
			}
			fanSpans = append(fanSpans, cFanSpans...)
		}
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(fanSpans)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			if res := Verify(cppCaps, perm, expCppM1); !res.Passed() {
				t.Fatalf("Cpp >128 extra fanout seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
		}

		// 4. Negative C++ contrasts: AllowExtraPubSubSpans=false -> unexpected_span; TraceFlags=0 extra create -> dangling_link; wrong scope -> dangling_link; batch_count < links -> wrong_attr.
		assertHasViolation(t, Verify(cppCaps, cloneSpans(sepCpp), &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: false,
			Publish:               expCppM1.Publish,
		}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		leakUnsamp := cloneSpans(sharedAll)
		leakUnsamp[5].TraceFlags = 0
		assertHasViolation(t, Verify(cppCaps, leakUnsamp, expCppM1), CodeMissingLink, "publish.batch.dangling_link")

		badScopeCreate := cloneSpans(sharedAll)
		badScopeCreate[5].InstrumentationScopeName = GoPubSubScopeName
		assertHasViolation(t, Verify(cppCaps, badScopeCreate, expCppM1), CodeMissingLink, "publish.batch.dangling_link")

		underBatchCpp := cloneSpans(sharedAll)
		underBatchCpp[4].Attributes["messaging.batch.message_count"] = intAV(1)
		assertHasViolation(t, Verify(cppCaps, underBatchCpp, expCppM1), CodeWrongAttributeValue, "publish.batch.attr.message_count")
	})
}

func TestTier5_CppPublishErrorRPC_NonBlocking(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	hasDeviation := func(res TraceResult, rule string) bool {
		for _, d := range res.Deviations {
			if d.Rule == rule {
				return true
			}
		}
		return false
	}

	makeErrCreate := func(seq int, msgID string, pub *proxypb.TraceSpan, rpcErr bool) (*proxypb.TraceSpan, []*proxypb.TraceSpan, string) {
		tid := testTraceID(17100 + seq)
		sid := testSpanID(171000 + seq*10)
		cs := cppSpan(tid, sid, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(seq)*time.Millisecond)
		cs.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                  strAV("p"),
			"messaging.system":                strAV("gcp_pubsub"),
			"messaging.destination.name":      strAV("t"),
			"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
			"messaging.operation.type":        strAV("create"),
			"messaging.message.envelope.size": intAV(25),
			"messaging.message.id":            strAV(msgID),
			"gl-cpp.status_code":              strAV("OK"),
		}
		if rpcErr {
			cs.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			cs.StatusMessage = "unavailable"
			cs.Attributes["gl-cpp.status_code"] = strAV("UNAVAILABLE")
		}
		cs.Events = []*proxypb.SpanEvent{
			{Name: "gl-cpp.added_to_batch"},
			{Name: "gl-cpp.publish_start"},
			{Name: "gl-cpp.publish_end"},
		}
		if pub != nil {
			cs.Links = []*proxypb.SpanLink{{TraceId: pub.TraceId, SpanId: pub.SpanId}}
			pub.Links = append(pub.Links, &proxypb.SpanLink{TraceId: tid, SpanId: sid})
		}
		fc := cppSpan(tid, testSpanID(171001+seq*10), sid, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(seq)*time.Millisecond)
		fc.Attributes = map[string]*proxypb.AttributeValue{
			"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish"),
			"gl-cpp.status_code": strAV("OK"),
		}
		bc := cppSpan(tid, testSpanID(171002+seq*10), sid, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(seq)*time.Millisecond)
		bc.Attributes = map[string]*proxypb.AttributeValue{
			"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish"),
			"gl-cpp.status_code": strAV("OK"),
		}
		return cs, []*proxypb.TraceSpan{cs, fc, bc}, fmt.Sprintf("00-%s-%s-01", tid, sid)
	}

	makePub := func(seq int, batchCount int64, statusCode string) *proxypb.TraceSpan {
		pub := cppSpan(testTraceID(17500+seq), testSpanID(175000+seq), "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 10*time.Millisecond)
		pub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
		pub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                strAV("p"),
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV("t"),
			"messaging.batch.message_count": intAV(batchCount),
			"messaging.operation.type":      strAV("publish"),
			"code.function.name":            strAV("BatchSink::AsyncPublish"),
			"gl-cpp.status_code":            strAV(statusCode),
		}
		return pub
	}

	t.Run("pubExp.ExpectError=true with non-blocking PublishErrorRPC emits cpp events and status deviation", func(t *testing.T) {
		pub := makePub(1, 1, "UNAVAILABLE")
		_, cSpans, tp := makeErrCreate(1, "", pub, true)
		grpcPub := cppSpan(pub.TraceId, testSpanID(175901), pub.SpanId, "google.pubsub.v1.Publisher/Publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 11*time.Millisecond)
		spans := append(cSpans, pub, grpcPub)
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: true,
				Messages: []PublishMessageExpectation{{
					ParentTraceparent: "",
					WireTraceparent:   tp,
					ExpectError:       true,
					ErrorKind:         PublishErrorRPC,
					ErrorSubstring:    "unavailable",
				}},
			},
		}
		res := Verify(cppCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp PASS, got:\n%s", res.Format())
		}
		if !hasDeviation(res, "events.cpp_publish_names") {
			t.Fatalf("expected deviation events.cpp_publish_names, got %+v", res.Deviations)
		}
		if !hasDeviation(res, "status.publish_ignores_rpc_error") {
			t.Fatalf("expected deviation status.publish_ignores_rpc_error, got %+v", res.Deviations)
		}

		// Go canonical profile must reject C++ error-create/OK-publish trace.
		goRes := Verify(goCaps, cloneSpans(spans), exp)
		if goRes.Passed() {
			t.Fatalf("expected Go profile to reject C++ RPC error spans")
		}
		if hasDeviation(goRes, "status.publish_ignores_rpc_error") {
			t.Fatalf("Go profile must not emit status.publish_ignores_rpc_error, got %+v", goRes.Deviations)
		}
	})

	t.Run("per-message ExpectError=true PublishErrorRPC with pubExp.ExpectError=false emits status.publish_ignores_rpc_error", func(t *testing.T) {
		for _, kind := range []PublishErrorKind{PublishErrorRPC, PublishErrorAuto} {
			pub := makePub(2, 1, "UNAVAILABLE")
			cs, cSpans, tp := makeErrCreate(2, "", pub, true)
			spans := append(cSpans, pub)
			exp := &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					ExpectError: false,
					Messages: []PublishMessageExpectation{{
						WireTraceparent: tp,
						ExpectError:     true,
						ErrorKind:       kind,
					}},
				},
			}
			res := Verify(cppCaps, cloneSpans(spans), exp)
			if !res.Passed() {
				t.Fatalf("kind=%v expected ProfileCpp PASS, got:\n%s", kind, res.Format())
			}
			if !hasDeviation(res, "events.cpp_publish_names") {
				t.Fatalf("kind=%v expected events.cpp_publish_names deviation, got %+v", kind, res.Deviations)
			}
			if !hasDeviation(res, "status.publish_ignores_rpc_error") {
				t.Fatalf("kind=%v expected status.publish_ignores_rpc_error deviation, got %+v", kind, res.Deviations)
			}

			// Negative check: removing gl-cpp.publish_end on error create fails.
			badEvents := cloneSpans(spans)
			badEvents[0].Events = []*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}, {Name: "gl-cpp.publish_start"}}
			_ = cs
			assertHasViolation(t, Verify(cppCaps, badEvents, exp), CodeMissingEvent, "publish.create.event.cpp_publish_end")
		}
	})

	t.Run("mixed success and error batch emits cpp events and status.publish_ignores_rpc_error", func(t *testing.T) {
		pub := makePub(3, 2, "PARTIAL_ERROR")
		_, okSpans, okTP := makeErrCreate(3, "msg-ok", pub, false)
		_, errSpans, errTP := makeErrCreate(4, "", pub, true)
		spans := append(append(okSpans, errSpans...), pub)
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{
					{
						MessageID:       "msg-ok",
						WireTraceparent: okTP,
						ExpectError:     false,
					},
					{
						WireTraceparent: errTP,
						ExpectError:     true,
						ErrorKind:       PublishErrorRPC,
					},
				},
			},
		}
		res := Verify(cppCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp mixed batch PASS, got:\n%s", res.Format())
		}
		if !hasDeviation(res, "events.cpp_publish_names") {
			t.Fatalf("expected events.cpp_publish_names deviation, got %+v", res.Deviations)
		}
		if !hasDeviation(res, "status.publish_ignores_rpc_error") {
			t.Fatalf("expected status.publish_ignores_rpc_error deviation, got %+v", res.Deviations)
		}
	})

	t.Run("local flow control error without RPC error does not emit status.publish_ignores_rpc_error", func(t *testing.T) {
		_, cSpans, tp := makeErrCreate(5, "", nil, true)
		// Drop publisher batching child and keep FC OK (C++ ends flow control span sync OK) for PublishErrorFlowControl.
		fc := cSpans[1]
		fc.Attributes["gl-cpp.status_code"] = strAV("OK")
		cSpans[0].Attributes["gl-cpp.status_code"] = strAV("RESOURCE_EXHAUSTED")
		spans := []*proxypb.TraceSpan{cSpans[0], fc}
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{{
					WireTraceparent: tp,
					ExpectError:     true,
					ErrorKind:       PublishErrorFlowControl,
				}},
			},
		}
		res := Verify(cppCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp flow control error PASS, got:\n%s", res.Format())
		}
		if hasDeviation(res, "status.publish_ignores_rpc_error") {
			t.Fatalf("local flow control error must not emit status.publish_ignores_rpc_error, got %+v", res.Deviations)
		}
	})

	t.Run("unsampled dropped PublishErrorRPC with pubExp.ExpectError=false emits status.publish_ignores_rpc_error", func(t *testing.T) {
		pub := makePub(6, 1, "UNAVAILABLE")
		spans := []*proxypb.TraceSpan{pub}
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{{
					ParentTraceparent:      "00-" + testTraceID(17600) + "-" + testSpanID(176001) + "-00",
					ExpectDroppedUnsampled: true,
					ExpectError:            true,
					ErrorKind:              PublishErrorRPC,
				}},
			},
		}
		res := Verify(cppCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp PASS for unsampled dropped PublishErrorRPC, got:\n%s", res.Format())
		}
		if !hasDeviation(res, "status.publish_ignores_rpc_error") {
			t.Fatalf("expected status.publish_ignores_rpc_error deviation, got %+v", res.Deviations)
		}
	})

	t.Run("unsampled dropped PublishErrorAuto pre-validation failure has 0 spans and 0 deviations", func(t *testing.T) {
		unsampTP := "00-" + testTraceID(17601) + "-" + testSpanID(176002) + "-00"
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{{
					ParentTraceparent:      unsampTP,
					ExpectDroppedUnsampled: true,
					ExpectError:            true,
					ErrorKind:              PublishErrorAuto,
				}},
			},
		}
		res := Verify(cppCaps, nil, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp PASS for unsampled pre-validation PublishErrorAuto, got:\n%s", res.Format())
		}
		if len(res.Deviations) != 0 {
			t.Fatalf("expected 0 deviations when 0 publish spans exist, got %+v", res.Deviations)
		}
		goRes := Verify(goCaps, nil, exp)
		if !goRes.Passed() {
			t.Fatalf("expected ProfileGo PASS for unsampled pre-validation PublishErrorAuto, got:\n%s", goRes.Format())
		}
		if len(goRes.Deviations) != 0 {
			t.Fatalf("expected 0 deviations under ProfileGo, got %+v", goRes.Deviations)
		}

		// Negative check: leaking a child span on -00 traceparent still triggers publish.unsampled_dropped.
		leakFC := cppSpan(testTraceID(17601), testSpanID(176099), testSpanID(176002), "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		assertHasViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{leakFC}, exp), CodeUnexpectedSpan, "publish.unsampled_dropped")
	})

	t.Run("unsampled dropped PublishErrorAuto RPC failure emits status.publish_ignores_rpc_error", func(t *testing.T) {
		pub := makePub(7, 1, "PERMISSION_DENIED")
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{{
					ParentTraceparent:      "00-" + testTraceID(17602) + "-" + testSpanID(176003) + "-00",
					ExpectDroppedUnsampled: true,
					ExpectError:            true,
					ErrorKind:              PublishErrorAuto,
				}},
			},
		}
		res := Verify(cppCaps, []*proxypb.TraceSpan{pub}, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp PASS for unsampled PublishErrorAuto RPC error, got:\n%s", res.Format())
		}
		if !hasDeviation(res, "status.publish_ignores_rpc_error") {
			t.Fatalf("expected status.publish_ignores_rpc_error deviation, got %+v", res.Deviations)
		}

		// Also when no sampled or required wire creates exist, a 0-link publish span still implies RPC failure.
		pubOK := makePub(8, 1, "OK")
		resOK := Verify(cppCaps, []*proxypb.TraceSpan{pubOK}, exp)
		if !resOK.Passed() {
			t.Fatalf("expected ProfileCpp PASS for 0-create unsampled PublishErrorAuto RPC span, got:\n%s", resOK.Format())
		}
		if !hasDeviation(resOK, "status.publish_ignores_rpc_error") {
			t.Fatalf("expected status.publish_ignores_rpc_error when wireCreateSpans==0 && droppedWireRequired==0, got %+v", resOK.Deviations)
		}

		// Go canonical 0-link publish span passes with 0 deviations; C++ OK span fails Go profile without C++ deviation.
		goPub := makeGoPublishBatchSpan("p", "t", 1, 17602)
		goRes := Verify(goCaps, []*proxypb.TraceSpan{goPub}, exp)
		if !goRes.Passed() {
			t.Fatalf("expected ProfileGo PASS for unsampled PublishErrorAuto RPC error, got:\n%s", goRes.Format())
		}
		if len(goRes.Deviations) != 0 {
			t.Fatalf("expected 0 deviations under ProfileGo, got %+v", goRes.Deviations)
		}
		goReject := Verify(goCaps, cloneSpans([]*proxypb.TraceSpan{pub}), exp)
		if goReject.Passed() {
			t.Fatalf("expected ProfileGo to reject C++ 0-link publish span")
		}
		if hasDeviation(goReject, "status.publish_ignores_rpc_error") {
			t.Fatalf("ProfileGo must not emit status.publish_ignores_rpc_error, got %+v", goReject.Deviations)
		}
	})

	t.Run("mixed batch with unsampled PublishErrorAuto pre-validation failure avoids false status deviation", func(t *testing.T) {
		unsampTP := "00-" + testTraceID(17603) + "-" + testSpanID(176004) + "-00"

		// Sampled healthy message + unsampled PublishErrorAuto pre-validation failure: ps has gl-cpp.status_code="OK".
		pubOK := makePub(9, 1, "OK")
		_, okSpans, okTP := makeErrCreate(9, "msg-ok", pubOK, false)
		expMixedOK := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{
					{
						MessageID:       "msg-ok",
						WireTraceparent: okTP,
						ExpectError:     false,
					},
					{
						ParentTraceparent:      unsampTP,
						ExpectDroppedUnsampled: true,
						ExpectError:            true,
						ErrorKind:              PublishErrorAuto,
					},
				},
			},
		}
		resMixedOK := Verify(cppCaps, append(okSpans, pubOK), expMixedOK)
		if !resMixedOK.Passed() {
			t.Fatalf("expected ProfileCpp PASS for mixed healthy + unsampled pre-validation batch, got:\n%s", resMixedOK.Format())
		}
		if hasDeviation(resMixedOK, "status.publish_ignores_rpc_error") {
			t.Fatalf("healthy OK batch with unsampled PublishErrorAuto must not emit status.publish_ignores_rpc_error, got %+v", resMixedOK.Deviations)
		}

		// Sampled healthy message + unsampled PublishErrorAuto RPC failure: ps has gl-cpp.status_code="PERMISSION_DENIED".
		pubErr := makePub(10, 2, "PERMISSION_DENIED")
		_, okSpansErr, okTPErr := makeErrCreate(10, "msg-ok-err", pubErr, false)
		expMixedErr := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{
					{
						MessageID:       "msg-ok-err",
						WireTraceparent: okTPErr,
						ExpectError:     false,
					},
					{
						ParentTraceparent:      unsampTP,
						ExpectDroppedUnsampled: true,
						ExpectError:            true,
						ErrorKind:              PublishErrorAuto,
					},
				},
			},
		}
		resMixedErr := Verify(cppCaps, append(okSpansErr, pubErr), expMixedErr)
		if !resMixedErr.Passed() {
			t.Fatalf("expected ProfileCpp PASS for mixed healthy + unsampled RPC failure batch, got:\n%s", resMixedErr.Format())
		}
		if !hasDeviation(resMixedErr, "status.publish_ignores_rpc_error") {
			t.Fatalf("expected status.publish_ignores_rpc_error when gl-cpp.status_code=PERMISSION_DENIED, got %+v", resMixedErr.Deviations)
		}

		// Unsampled healthy (droppedWireRequired=1) + unsampled PublishErrorAuto pre-validation (droppedWireOptional=1) with OK batch.
		pubUnsampMixed := makePub(11, 1, "OK")
		expUnsampMixed := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{
					{
						ParentTraceparent:      "00-" + testTraceID(17604) + "-" + testSpanID(176005) + "-00",
						ExpectDroppedUnsampled: true,
						ExpectError:            false,
					},
					{
						ParentTraceparent:      unsampTP,
						ExpectDroppedUnsampled: true,
						ExpectError:            true,
						ErrorKind:              PublishErrorAuto,
					},
				},
			},
		}
		resUnsampMixed := Verify(cppCaps, []*proxypb.TraceSpan{pubUnsampMixed}, expUnsampMixed)
		if !resUnsampMixed.Passed() {
			t.Fatalf("expected ProfileCpp PASS for unsampled healthy + unsampled pre-validation batch, got:\n%s", resUnsampMixed.Format())
		}
		if hasDeviation(resUnsampMixed, "status.publish_ignores_rpc_error") {
			t.Fatalf("unsampled healthy OK batch must not emit status.publish_ignores_rpc_error, got %+v", resUnsampMixed.Deviations)
		}

		// Malformed empty gl-cpp.status_code="" fails attribute check and must not emit status.publish_ignores_rpc_error.
		pubEmpty := makePub(12, 1, "")
		_, okSpansEmpty, okTPEmpty := makeErrCreate(12, "msg-ok-empty", pubEmpty, false)
		expEmpty := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{
					{
						MessageID:       "msg-ok-empty",
						WireTraceparent: okTPEmpty,
						ExpectError:     false,
					},
					{
						ParentTraceparent:      unsampTP,
						ExpectDroppedUnsampled: true,
						ExpectError:            true,
						ErrorKind:              PublishErrorAuto,
					},
				},
			},
		}
		resEmpty := Verify(cppCaps, append(okSpansEmpty, pubEmpty), expEmpty)
		assertHasViolation(t, resEmpty, CodeWrongAttributeValue, "publish.batch.attr.gl_cpp_status_code")
		if hasDeviation(resEmpty, "status.publish_ignores_rpc_error") {
			t.Fatalf("empty gl-cpp.status_code must not emit status.publish_ignores_rpc_error, got %+v", resEmpty.Deviations)
		}

		// Sampled FlowControl error (droppedWireOptional=0, len(wireCreateSpans)=0, droppedWireRequired=0) with AllowExtraPubSubSpans=true and extra OK publish span.
		_, fcErrSpans, fcTP := makeErrCreate(13, "", nil, true)
		fcErrSpans[1].Attributes["gl-cpp.status_code"] = strAV("OK")
		fcErrSpans[0].Attributes["gl-cpp.status_code"] = strAV("RESOURCE_EXHAUSTED")
		extraPub := makePub(14, 1, "OK")
		_, extraSpans, _ := makeErrCreate(14, "msg-extra", extraPub, false)
		expExtra := &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				ExpectError: false,
				Messages: []PublishMessageExpectation{{
					WireTraceparent: fcTP,
					ExpectError:     true,
					ErrorKind:       PublishErrorFlowControl,
				}},
			},
		}
		allExtra := append(append([]*proxypb.TraceSpan{fcErrSpans[0], fcErrSpans[1]}, extraSpans...), extraPub)
		resExtra := Verify(cppCaps, allExtra, expExtra)
		if !resExtra.Passed() {
			t.Fatalf("expected ProfileCpp PASS for FC error + extra OK batch, got:\n%s", resExtra.Format())
		}
		if hasDeviation(resExtra, "status.publish_ignores_rpc_error") {
			t.Fatalf("droppedWireOptional==0 must not emit status.publish_ignores_rpc_error, got %+v", resExtra.Deviations)
		}
	})
}

func TestTier5_CppModackExpectErrorUnset(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	hasDeviation := func(res TraceResult, rule string) bool {
		for _, d := range res.Deviations {
			if d.Rule == rule {
				return true
			}
		}
		return false
	}

	buildFixture := func(modackStatus proxypb.SpanStatusCode) ([]*proxypb.TraceSpan, *TraceExpectation) {
		spans, exp := buildCppStreamingE2EFixture(true, false, false)
		sub := spans[6] // "s subscribe"
		sub.Events = append([]*proxypb.SpanEvent{
			{Name: "gl-cpp.modack_start"},
			{Name: "gl-cpp.modack_end"},
		}, sub.Events...)
		modack := cppSpan(testTraceID(17800), testSpanID(178000), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, 25*time.Millisecond)
		modack.StatusCode = modackStatus
		if modackStatus == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			modack.StatusMessage = "modack rpc failed"
		}
		modack.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                    strAV("p"),
			"messaging.system":                                  strAV("gcp_pubsub"),
			"messaging.destination.name":                        strAV("s"),
			"messaging.operation.type":                          strAV("extend"),
			"messaging.batch.message_count":                     intAV(1),
			"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(30),
		}
		modack.Links = []*proxypb.SpanLink{{TraceId: sub.TraceId, SpanId: sub.SpanId}}
		spans = append(spans, modack)
		exp.Subscribe.ExpectedSettlements = []SettlementExpectation{
			{Operation: "modack", ExpectError: true, ExpectedBatchCount: 1, AckDeadlineSeconds: 30},
			{Operation: "ack"},
		}
		return spans, exp
	}

	t.Run("UNSET modack with ExpectError=true passes and records status.modack_unset", func(t *testing.T) {
		spans, exp := buildFixture(proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
		res := Verify(cppCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp PASS for UNSET modack with ExpectError=true, got:\n%s", res.Format())
		}
		if !hasDeviation(res, "status.modack_unset") {
			t.Fatalf("expected deviation status.modack_unset, got %+v", res.Deviations)
		}
	})

	t.Run("ERROR modack fails settlement.modack.status under ProfileCpp", func(t *testing.T) {
		spans, exp := buildFixture(proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
		res := Verify(cppCaps, spans, exp)
		assertHasViolation(t, res, CodeWrongStatusCode, "settlement.modack.status")
	})

	t.Run("UNSET modack with ExpectError=false does not emit status.modack_unset", func(t *testing.T) {
		spans, exp := buildFixture(proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
		exp.Subscribe.ExpectedSettlements[0].ExpectError = false
		res := Verify(cppCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected ProfileCpp PASS for UNSET modack with ExpectError=false, got:\n%s", res.Format())
		}
		if hasDeviation(res, "status.modack_unset") {
			t.Fatalf("must not emit status.modack_unset when ExpectError=false, got %+v", res.Deviations)
		}
	})
}

func TestTier6_ProfileCppSizesBlockingWireTPAndBenignGRPCSpans(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	hasRule := func(res TraceResult, rule string, code ViolationCode) bool {
		for _, f := range res.Failures {
			if f.Rule == rule && (code == "" || f.Code == code) {
				return true
			}
		}
		return false
	}

	makeCppCreateSpans := func(seq int, msgID string, envSize int64, startOffset time.Duration, pub *proxypb.TraceSpan) (*proxypb.TraceSpan, []*proxypb.TraceSpan) {
		tid := testTraceID(19500 + seq)
		sid := testSpanID(195000 + seq*10)
		cs := cppSpan(tid, sid, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, startOffset)
		cs.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                  strAV("p"),
			"messaging.system":                strAV("gcp_pubsub"),
			"messaging.destination.name":      strAV("t"),
			"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
			"messaging.operation.type":        strAV("create"),
			"messaging.message.envelope.size": intAV(envSize),
			"messaging.message.id":            strAV(msgID),
			"gl-cpp.status_code":              strAV("OK"),
		}
		cs.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}, {Name: "gl-cpp.publish_start"}, {Name: "gl-cpp.publish_end"}}
		if pub != nil {
			cs.Links = []*proxypb.SpanLink{{TraceId: pub.TraceId, SpanId: pub.SpanId}}
			pub.Links = append(pub.Links, &proxypb.SpanLink{TraceId: tid, SpanId: sid})
		}
		fc := cppSpan(tid, testSpanID(195001+seq*10), sid, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, startOffset)
		fc.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish")}
		bc := cppSpan(tid, testSpanID(195002+seq*10), sid, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, startOffset)
		bc.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish")}
		return cs, []*proxypb.TraceSpan{cs, fc, bc}
	}

	makeCppPub := func(seq int, batchCount int64) *proxypb.TraceSpan {
		pub := cppSpan(testTraceID(19600+seq), testSpanID(196000+seq), "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 20*time.Millisecond)
		pub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                strAV("p"),
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV("t"),
			"messaging.batch.message_count": intAV(batchCount),
			"messaging.operation.type":      strAV("publish"),
			"code.function.name":            strAV("BatchSink::AsyncPublish"),
			"gl-cpp.status_code":            strAV("OK"),
		}
		return pub
	}

	t.Run("GAP-A6-2_ProfileCpp_Publish_EnvelopeSize_and_BodySize", func(t *testing.T) {
		// 1. EnvelopeSize exact match & mismatch (both too low and too high).
		pub1 := makeCppPub(1, 1)
		_, s1 := makeCppCreateSpans(1, "env-1", 40, time.Millisecond, pub1)
		spans1 := append(s1, pub1)
		if res := Verify(cppCaps, spans1, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages:   []PublishMessageExpectation{{MessageID: "env-1", EnvelopeSize: Int64Ptr(40)}},
			},
		}); !res.Passed() {
			t.Fatalf("EnvelopeSize=40 on envelope.size=40 expected PASS, got:\n%s", res.Format())
		}
		for _, wantEnv := range []int64{45, 39} {
			resBad := Verify(cppCaps, cloneSpans(spans1), &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					BatchCount: 1,
					Messages:   []PublishMessageExpectation{{MessageID: "env-1", EnvelopeSize: Int64Ptr(wantEnv)}},
				},
			})
			if resBad.Passed() || !hasRule(resBad, "publish.create.attr.envelope_size", CodeWrongAttributeValue) {
				t.Fatalf("EnvelopeSize=%d on envelope.size=40 expected publish.create.attr.envelope_size failure, got:\n%s", wantEnv, resBad.Format())
			}
		}

		// 2. Duplicate-ID bipartite disambiguation driven strictly by EnvelopeSize (including exactSize=true).
		pubDupEnv := makeCppPub(2, 2)
		_, sEnv50 := makeCppCreateSpans(2, "dup-env", 50, 1*time.Millisecond, pubDupEnv)
		_, sEnv30 := makeCppCreateSpans(3, "dup-env", 30, 2*time.Millisecond, pubDupEnv)
		spansDupEnv := append(append(sEnv50, sEnv30...), pubDupEnv)
		if res := Verify(cppCaps, spansDupEnv, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{MessageID: "dup-env", EnvelopeSize: Int64Ptr(30)},
					{MessageID: "dup-env"},
				},
			},
		}); !res.Passed() {
			t.Fatalf("duplicate-ID EnvelopeSize exact bipartite disambiguation expected PASS, got:\n%s", res.Format())
		}
		if res := Verify(cppCaps, spansDupEnv, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{MessageID: "dup-env", EnvelopeSize: Int64Ptr(30)},
					{MessageID: "dup-env", EnvelopeSize: Int64Ptr(50)},
				},
			},
		}); !res.Passed() {
			t.Fatalf("duplicate-ID swapped EnvelopeSize bipartite disambiguation expected PASS, got:\n%s", res.Format())
		}

		// 3. BodySize (20 + *m.BodySize MatchInt64Min floor): exact floor 120 passes, 119 fails.
		pubBodyOK := makeCppPub(3, 1)
		_, sBody120 := makeCppCreateSpans(4, "body-120", 120, time.Millisecond, pubBodyOK)
		if res := Verify(cppCaps, append(sBody120, pubBodyOK), &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages:   []PublishMessageExpectation{{MessageID: "body-120", BodySize: Int64Ptr(100)}},
			},
		}); !res.Passed() {
			t.Fatalf("BodySize=100 on envelope.size=120 expected PASS, got:\n%s", res.Format())
		}

		pubBodyBad := makeCppPub(4, 1)
		_, sBody119 := makeCppCreateSpans(5, "body-119", 119, time.Millisecond, pubBodyBad)
		resBodyBad := Verify(cppCaps, append(sBody119, pubBodyBad), &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages:   []PublishMessageExpectation{{MessageID: "body-119", BodySize: Int64Ptr(100)}},
			},
		})
		if resBodyBad.Passed() || !hasRule(resBodyBad, "publish.create.attr.envelope_size", CodeWrongAttributeValue) {
			t.Fatalf("BodySize=100 on envelope.size=119 expected publish.create.attr.envelope_size failure, got:\n%s", resBodyBad.Format())
		}

		// 4. Duplicate-ID bipartite disambiguation driven strictly by BodySize (20 + 100 = 120):
		// Earlier span has envelope.size=105 (>=100, <120) so dropping "20 +" in buildPubMsgScoreMeta also fails!
		pubDupBody := makeCppPub(5, 2)
		_, sBody105 := makeCppCreateSpans(6, "dup-body", 105, 1*time.Millisecond, pubDupBody)
		_, sBody130 := makeCppCreateSpans(7, "dup-body", 130, 2*time.Millisecond, pubDupBody)
		if res := Verify(cppCaps, append(append(sBody105, sBody130...), pubDupBody), &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{MessageID: "dup-body", BodySize: Int64Ptr(100)},
					{MessageID: "dup-body", BodySize: Int64Ptr(10)},
				},
			},
		}); !res.Passed() {
			t.Fatalf("duplicate-ID BodySize bipartite disambiguation expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("GAP-A6-8_BlockingPublisher_with_WireTraceparent_emits_propagation_deviation", func(t *testing.T) {
		cTID, cSID := testTraceID(19701), testSpanID(197010)
		cs := cppSpan(cTID, cSID, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
		cs.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                  strAV("p"),
			"messaging.system":                strAV("gcp_pubsub"),
			"messaging.destination.name":      strAV("t"),
			"code.function.name":              strAV("pubsub::BlockingPublisher::Publish"),
			"messaging.operation.type":        strAV("create"),
			"messaging.message.envelope.size": intAV(24),
			"messaging.message.id":            strAV("blk-wire-1"),
			"gl-cpp.status_code":              strAV("OK"),
		}
		gRPC := cppSpan(cTID, testSpanID(197011), cSID, "google.pubsub.v1.Publisher/Publish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		nonMatchingWireTP := fmt.Sprintf("00-%s-%s-01", testTraceID(19799), testSpanID(197990))

		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:      1,
				MessageIDs:        []string{"blk-wire-1"},
				WireTraceparents:  []string{nonMatchingWireTP},
				BlockingPublisher: true,
			},
		}
		res := Verify(cppCaps, []*proxypb.TraceSpan{cs, gRPC}, exp)
		if !res.Passed() {
			t.Fatalf("BlockingPublisher with WireTraceparent expected PASS, got:\n%s", res.Format())
		}

		sawPropDev, sawHierDev := false, false
		for _, d := range res.Deviations {
			if d.Rule == "publish.blocking_publisher_no_batch_or_injection" {
				if d.Category == "propagation" {
					sawPropDev = true
				}
				if d.Category == "hierarchy" {
					sawHierDev = true
				}
			}
		}
		if !sawPropDev || !sawHierDev {
			t.Fatalf("expected both propagation and hierarchy BlockingPublisher deviations, got %+v", res.Deviations)
		}

		// Go canonical profile has AllowBlockingPublisher=false and rejects mismatched wire traceparent.
		resGo := Verify(goCaps, []*proxypb.TraceSpan{cs, gRPC}, exp)
		if resGo.Passed() || !hasRule(resGo, "publish.create.wire_traceparent", CodeBrokenTraceparent) {
			t.Fatalf("expected ProfileGo to fail publish.create.wire_traceparent, got:\n%s", resGo.Format())
		}
	})

	t.Run("GAP-A6-9_IsBenignCppLifecycleOrGRPCSpan_Start_and_Finish", func(t *testing.T) {
		baseSpans, baseExp := buildCppStreamingE2EFixture(true, false, false)

		for _, name := range []string{"Start", "Finish"} {
			sp := cppSpan(testTraceID(19800), testSpanID(198000), "", name, proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
			res := Verify(cppCaps, append(cloneSpans(baseSpans), sp), baseExp)
			if !res.Passed() {
				t.Fatalf("ProfileCpp with benign gRPC span %q expected PASS, got:\n%s", name, res.Format())
			}
			sawSpanDev := false
			for _, d := range res.Deviations {
				if d.Rule == "spans.cpp_grpc_and_lifecycle" && d.SpanName == name {
					sawSpanDev = true
				}
			}
			if !sawSpanDev {
				t.Fatalf("expected spans.cpp_grpc_and_lifecycle deviation for %q, got %+v", name, res.Deviations)
			}

			// Go profile rejects C++ Start/Finish spans as unexpected extra spans.
			goSpanOnly := goSpan(testTraceID(19801), testSpanID(198001), "", name, proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
			pub := makeGoPublishBatchSpan("p", "t", 1, 19801)
			c, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m-go", "", 5, 19801, pub, 1)
			resGo := Verify(goCaps, []*proxypb.TraceSpan{c, fc, batch, pub, goSpanOnly}, &TraceExpectation{
				Topic:   "projects/p/topics/t",
				Publish: &PublishExpectation{MessageCount: 1, BatchCount: 1, MessageIDs: []string{"m-go"}, WireTraceparents: []string{tp}},
			})
			if resGo.Passed() || !hasRule(resGo, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
				t.Fatalf("expected ProfileGo to reject %q span, got:\n%s", name, resGo.Format())
			}
		}

		// Both Start and Finish together pass under ProfileCpp, while non-benign span fails.
		bothSpans := append(cloneSpans(baseSpans),
			cppSpan(testTraceID(19802), testSpanID(198002), "", "Start", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond),
			cppSpan(testTraceID(19802), testSpanID(198003), "", "Finish", proxypb.SpanKind_SPAN_KIND_CLIENT, 2*time.Millisecond),
		)
		if resBoth := Verify(cppCaps, bothSpans, baseExp); !resBoth.Passed() {
			t.Fatalf("ProfileCpp with Start and Finish together expected PASS, got:\n%s", resBoth.Format())
		}
		badExtra := cppSpan(testTraceID(19803), testSpanID(198004), "", "UnrecognizedTransportSpan", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		if resBad := Verify(cppCaps, append(bothSpans, badExtra), baseExp); resBad.Passed() || !hasRule(resBad, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
			t.Fatalf("expected non-benign gl-cpp span to fail traces.no_extra_pubsub_spans, got:\n%s", resBad.Format())
		}

		// ExpectNoTraces still rejects Start and Finish spans.
		startOnly := cppSpan(testTraceID(19804), testSpanID(198005), "", "Start", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
		if resNo := Verify(cppCaps, []*proxypb.TraceSpan{startOnly}, ExpectNoTraces()); resNo.Passed() || !hasRule(resNo, "traces.expect_no_spans", CodeUnexpectedSpan) {
			t.Fatalf("expected ExpectNoTraces to reject Start span, got:\n%s", resNo.Format())
		}
	})

	t.Run("GAP-A7-T10_CppDuplicateReverseSettlementLinkRejected", func(t *testing.T) {
		spans, exp := buildCppStreamingE2EFixture(true, false, false)
		var subSpan, ackSpan *proxypb.TraceSpan
		for _, s := range spans {
			switch s.GetName() {
			case "s subscribe":
				subSpan = s
			case "s ack":
				ackSpan = s
			}
		}
		if subSpan == nil || ackSpan == nil {
			t.Fatalf("fixture missing subscribe/ack spans")
		}
		subSpan.Links = append(subSpan.Links,
			&proxypb.SpanLink{TraceId: ackSpan.GetTraceId(), SpanId: ackSpan.GetSpanId()},
			&proxypb.SpanLink{TraceId: ackSpan.GetTraceId(), SpanId: ackSpan.GetSpanId()},
		)
		res := Verify(cppCaps, spans, exp)
		if res.Passed() || !hasRule(res, "subscribe.settlement.duplicate_reverse_link", CodeBrokenBidirectionalLink) {
			t.Fatalf("expected subscribe.settlement.duplicate_reverse_link under ProfileCPP, got:\n%s", res.Format())
		}
	})

	t.Run("GAP-A7-T11_CppSettlementSubscriptionTemplateWhenSubscriptionEmpty", func(t *testing.T) {
		// 1. Present non-empty subscription.template passes when exp.Subscription == "".
		spansOK, expOK := buildCppStreamingE2EFixture(true, false, false)
		expOK.Subscription = ""
		if res := Verify(cppCaps, spansOK, expOK); !res.Passed() {
			t.Fatalf("ProfileCPP with Subscription=\"\" and non-empty subscription.template expected PASS, got:\n%s", res.Format())
		}

		// 2. Missing subscription.template fails even when exp.Subscription == "".
		spansMissing, expMissing := buildCppStreamingE2EFixture(true, false, false)
		expMissing.Subscription = ""
		for _, s := range spansMissing {
			if s.GetName() == "s ack" {
				delete(s.Attributes, "messaging.gcp_pubsub.subscription.template")
			}
		}
		resMissing := Verify(cppCaps, spansMissing, expMissing)
		if resMissing.Passed() || !hasRule(resMissing, "settlement.ack.attr.subscription_template", CodeMissingAttribute) {
			t.Fatalf("expected settlement.ack.attr.subscription_template failure when subscription.template missing and exp.Subscription==\"\", got:\n%s", resMissing.Format())
		}

		// 3. Empty string subscription.template also fails MatchStringNonEmpty when exp.Subscription == "".
		spansEmptyAttr, expEmptyAttr := buildCppStreamingE2EFixture(true, false, false)
		expEmptyAttr.Subscription = ""
		for _, s := range spansEmptyAttr {
			if s.GetName() == "s ack" {
				s.Attributes["messaging.gcp_pubsub.subscription.template"] = strAV("")
			}
		}
		resEmptyAttr := Verify(cppCaps, spansEmptyAttr, expEmptyAttr)
		if resEmptyAttr.Passed() || !hasRule(resEmptyAttr, "settlement.ack.attr.subscription_template", CodeWrongAttributeValue) {
			t.Fatalf("expected settlement.ack.attr.subscription_template failure for empty subscription.template when exp.Subscription==\"\", got:\n%s", resEmptyAttr.Format())
		}
	})
}

func TestProfiles_BenignCppResumePublishSpan(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	hasRule := func(res TraceResult, rule string, code ViolationCode) bool {
		for _, f := range res.Failures {
			if f.Rule == rule && (code == "" || f.Code == code) {
				return true
			}
		}
		return false
	}

	baseSpans, baseExp := buildCppStreamingE2EFixture(true, false, false)
	baseExp.AllowExtraPubSubSpans = false

	rp := cppSpan(testTraceID(19901), testSpanID(199010), "", "pubsub::Publisher::ResumePublish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
	res := Verify(cppCaps, append(cloneSpans(baseSpans), rp), baseExp)
	if !res.Passed() {
		t.Fatalf("ProfileCpp with benign pubsub::Publisher::ResumePublish expected PASS, got:\n%s", res.Format())
	}
	sawSpanDev := false
	for _, d := range res.Deviations {
		if d.Rule == "spans.cpp_grpc_and_lifecycle" && d.SpanName == "pubsub::Publisher::ResumePublish" {
			sawSpanDev = true
			break
		}
	}
	if !sawSpanDev {
		t.Fatalf("expected spans.cpp_grpc_and_lifecycle deviation for pubsub::Publisher::ResumePublish, got %+v", res.Deviations)
	}

	// Non-benign pubsub:: span must still fail traces.no_extra_pubsub_spans when AllowExtraPubSubSpans=false.
	badSpan := cppSpan(testTraceID(19902), testSpanID(199020), "", "pubsub::Publisher::BogusLifecycle", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
	resBad := Verify(cppCaps, append(cloneSpans(baseSpans), badSpan), baseExp)
	if resBad.Passed() || !hasRule(resBad, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
		t.Fatalf("expected ProfileCpp to reject non-benign pubsub::Publisher::BogusLifecycle, got:\n%s", resBad.Format())
	}

	// Go canonical profile must reject pubsub::Publisher::ResumePublish.
	pub := makeGoPublishBatchSpan("p", "t", 1, 19903)
	cs, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m-go-rp", "", 5, 19903, pub, 1)
	goExtra := goSpan(testTraceID(19904), testSpanID(199040), "", "pubsub::Publisher::ResumePublish", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
	resGo := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub, goExtra}, &TraceExpectation{
		Topic:                 "projects/p/topics/t",
		AllowExtraPubSubSpans: false,
		Publish:               &PublishExpectation{MessageCount: 1, BatchCount: 1, MessageIDs: []string{"m-go-rp"}, WireTraceparents: []string{tp}},
	})
	if resGo.Passed() || !hasRule(resGo, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
		t.Fatalf("expected ProfileGo to reject pubsub::Publisher::ResumePublish, got:\n%s", resGo.Format())
	}

	// ExpectNoTraces still rejects pubsub::Publisher::ResumePublish under ProfileCpp.
	if resNo := Verify(cppCaps, []*proxypb.TraceSpan{rp}, ExpectNoTraces()); resNo.Passed() || !hasRule(resNo, "traces.expect_no_spans", CodeUnexpectedSpan) {
		t.Fatalf("expected ExpectNoTraces to reject pubsub::Publisher::ResumePublish, got:\n%s", resNo.Format())
	}
}
