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
	"math"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testTraceID(n int) string {
	return fmt.Sprintf("%032x", n)
}

func testSpanID(n int) string {
	return fmt.Sprintf("%016x", n)
}

func goSpan(tid, sid, pid, name string, kind proxypb.SpanKind, startOffset time.Duration) *proxypb.TraceSpan {
	base := time.Date(2026, 11, 15, 18, 0, 0, 0, time.UTC).Add(startOffset)
	return &proxypb.TraceSpan{
		TraceId:                  tid,
		SpanId:                   sid,
		ParentSpanId:             pid,
		TraceFlags:               1,
		Name:                     name,
		Kind:                     kind,
		StartTime:                timestamppb.New(base),
		EndTime:                  timestamppb.New(base.Add(5 * time.Millisecond)),
		Attributes:               make(map[string]*proxypb.AttributeValue),
		InstrumentationScopeName: GoPubSubScopeName,
	}
}

func makeGoPublishMessageSpans(
	topicProj, topicID, msgID, orderingKey string,
	bodySize int64,
	seq int,
	pubSpan *proxypb.TraceSpan,
	batchSize int64,
) (create, fc, batch *proxypb.TraceSpan, wireTP string) {
	tid := testTraceID(100 + seq)
	cSID := testSpanID(1000 + seq*10)
	fcSID := testSpanID(1001 + seq*10)
	bSID := testSpanID(1002 + seq*10)
	fullTopic := fmt.Sprintf("projects/%s/topics/%s", topicProj, topicID)

	create = goSpan(tid, cSID, "", topicID+" create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(seq)*time.Millisecond)
	create.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                            strAV(topicProj),
		"gcp.resource.name":                         strAV(fullTopic),
		"messaging.system":                          strAV("gcp_pubsub"),
		"messaging.destination.name":                strAV(topicID),
		"code.function":                             strAV("Publish"),
		"messaging.message.body.size":               intAV(bodySize),
		"messaging.gcp_pubsub.message.ordering_key": strAV(orderingKey),
		"messaging.message.id":                      strAV(msgID),
	}
	create.Events = []*proxypb.SpanEvent{
		{
			Name: "publish start",
			Attributes: map[string]*proxypb.AttributeValue{
				"messaging.batch.message_count": intAV(batchSize),
			},
		},
		{Name: "publish end"},
	}

	fc = goSpan(tid, fcSID, cSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(seq)*time.Millisecond)
	batch = goSpan(tid, bSID, cSID, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(seq)*time.Millisecond)

	if pubSpan != nil {
		create.Links = append(create.Links, &proxypb.SpanLink{
			TraceId: pubSpan.TraceId,
			SpanId:  pubSpan.SpanId,
			Attributes: map[string]*proxypb.AttributeValue{
				"messaging.operation.name": strAV("publish"),
			},
		})
		pubSpan.Links = append(pubSpan.Links, &proxypb.SpanLink{
			TraceId: create.TraceId,
			SpanId:  create.SpanId,
		})
	}

	wireTP = fmt.Sprintf("00-%s-%s-01", tid, cSID)
	return create, fc, batch, wireTP
}

func makeGoPublishBatchSpan(topicProj, topicID string, batchCount int64, seq int) *proxypb.TraceSpan {
	tid := testTraceID(500 + seq)
	sid := testSpanID(5000 + seq)
	fullTopic := fmt.Sprintf("projects/%s/topics/%s", topicProj, topicID)
	ps := goSpan(tid, sid, "", topicID+" publish", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(10+seq)*time.Millisecond)
	ps.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                strAV(topicProj),
		"gcp.resource.name":             strAV(fullTopic),
		"messaging.system":              strAV("gcp_pubsub"),
		"messaging.destination.name":    strAV(topicID),
		"messaging.batch.message_count": intAV(batchCount),
		"code.function":                 strAV("publishMessageBundle"),
	}
	return ps
}

func makeGoSubscribeMessageSpans(
	subProj, subID, msgID, ackID, orderingKey string,
	bodySize int64,
	deliveryAttempt int32,
	isEOD bool,
	batchCount int64,
	parentTP string,
	result string,
	seq int,
) (sub, cc, sched, proc *proxypb.TraceSpan, callbackTP string) {
	tid := testTraceID(700 + seq)
	parentSID := ""
	if parentTP != "" {
		if tp, err := ParseTraceparent(parentTP); err == nil {
			tid = tp.TraceID
			parentSID = tp.SpanID
		}
	}
	subSID := testSpanID(7000 + seq*10)
	ccSID := testSpanID(7001 + seq*10)
	schedSID := testSpanID(7002 + seq*10)
	procSID := testSpanID(7003 + seq*10)
	fullSub := fmt.Sprintf("projects/%s/subscriptions/%s", subProj, subID)

	sub = goSpan(tid, subSID, parentSID, subID+" subscribe", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(20+seq)*time.Millisecond)
	sub.ChildSpanCount = 0 // Verify ChildSpanCount==0 works in Go SDK!
	sub.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                             strAV(subProj),
		"gcp.resource.name":                          strAV(fullSub),
		"messaging.system":                           strAV("gcp_pubsub"),
		"messaging.destination.name":                 strAV(subID),
		"messaging.message.id":                       strAV(msgID),
		"messaging.message.body.size":                intAV(bodySize),
		"messaging.gcp_pubsub.message.ordering_key":  strAV(orderingKey),
		"messaging.gcp_pubsub.exactly_once_delivery": boolAV(isEOD),
		"messaging.gcp_pubsub.message.ack_id":        strAV(ackID),
		"messaging.batch.message_count":              intAV(batchCount),
		"code.function":                              strAV("receive"),
	}
	if deliveryAttempt > 0 {
		sub.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = intAV(int64(deliveryAttempt))
	}
	if result != "" {
		sub.Attributes["messaging.gcp_pubsub.result"] = strAV(result)
	}

	cc = goSpan(tid, ccSID, subSID, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(21+seq)*time.Millisecond)
	sched = goSpan(tid, schedSID, subSID, "subscribe scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(22+seq)*time.Millisecond)
	proc = goSpan(tid, procSID, subSID, subID+" process", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(23+seq)*time.Millisecond)
	proc.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                            strAV(subProj),
		"gcp.resource.name":                         strAV(fullSub),
		"messaging.system":                          strAV("gcp_pubsub"),
		"messaging.destination.name":                strAV(subID),
		"messaging.message.id":                      strAV(msgID),
		"messaging.message.body.size":               intAV(bodySize),
		"messaging.gcp_pubsub.message.ordering_key": strAV(orderingKey),
		"messaging.operation.type":                  strAV("process"),
	}
	if deliveryAttempt > 0 {
		proc.Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = intAV(int64(deliveryAttempt))
	}

	callbackTP = fmt.Sprintf("00-%s-%s-01", tid, procSID)
	return sub, cc, sched, proc, callbackTP
}

func makeGoSettlementSpan(
	subProj, subID, op string,
	batchCount int64,
	deadlineSec int64,
	isReceipt bool,
	seq int,
	subSpans []*proxypb.TraceSpan,
) *proxypb.TraceSpan {
	tid := testTraceID(900 + seq)
	sid := testSpanID(9000 + seq)
	fullSub := fmt.Sprintf("projects/%s/subscriptions/%s", subProj, subID)
	sp := goSpan(tid, sid, "", subID+" "+op, proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(30+seq)*time.Millisecond)
	sp.Attributes = map[string]*proxypb.AttributeValue{
		"gcp.project_id":                strAV(subProj),
		"gcp.resource.name":             strAV(fullSub),
		"messaging.system":              strAV("gcp_pubsub"),
		"messaging.destination.name":    strAV(subID),
		"messaging.batch.message_count": intAV(batchCount),
	}
	switch op {
	case "ack":
		sp.Attributes["code.function"] = strAV("sendAck")
	case "modack":
		sp.Attributes["code.function"] = strAV("sendModAck")
		sp.Attributes["messaging.gcp_pubsub.message.ack_deadline"] = intAV(deadlineSec)
		sp.Attributes["messaging.gcp_pubsub.is_receipt_modack"] = boolAV(isReceipt)
	case "nack":
		sp.Attributes["code.function"] = strAV("sendModAck")
	}

	for _, ss := range subSpans {
		sp.Links = append(sp.Links, &proxypb.SpanLink{TraceId: ss.TraceId, SpanId: ss.SpanId})
		ss.Links = append(ss.Links, &proxypb.SpanLink{
			TraceId: sp.TraceId,
			SpanId:  sp.SpanId,
			Attributes: map[string]*proxypb.AttributeValue{
				"messaging.operation.name": strAV(op),
			},
		})
		ss.Events = append(ss.Events,
			&proxypb.SpanEvent{
				Name: op + " start",
				Attributes: map[string]*proxypb.AttributeValue{
					"messaging.batch.message_count": intAV(batchCount),
				},
			},
			&proxypb.SpanEvent{Name: op + " end"},
		)
	}
	return sp
}

func cloneSpans(in []*proxypb.TraceSpan) []*proxypb.TraceSpan {
	out := make([]*proxypb.TraceSpan, len(in))
	for i, s := range in {
		if s != nil {
			out[i] = proto.Clone(s).(*proxypb.TraceSpan)
		}
	}
	return out
}

func buildCanonicalGoE2EFixture() ([]*proxypb.TraceSpan, *TraceExpectation) {
	pub := makeGoPublishBatchSpan("p", "t", 1, 1)
	c, fc, batch, wireTP := makeGoPublishMessageSpans("p", "t", "msg-1", "", 5, 1, pub, 1)
	sub, cc, sched, proc, cbTP := makeGoSubscribeMessageSpans("p", "s", "msg-1", "ack-1", "", 5, 0, false, 1, wireTP, "acked", 1)
	proc.Events = append(proc.Events, &proxypb.SpanEvent{Name: "ack called"})
	ack := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 1, []*proxypb.TraceSpan{sub})

	spans := []*proxypb.TraceSpan{c, fc, batch, pub, sub, cc, sched, proc, ack}
	exp := &TraceExpectation{
		Topic:        "projects/p/topics/t",
		Subscription: "projects/p/subscriptions/s",
		Publish: &PublishExpectation{
			MessageCount:     1,
			BatchCount:       1,
			MessageIDs:       []string{"msg-1"},
			WireTraceparents: []string{wireTP},
			BodySizes:        []int64{5},
		},
		Subscribe: &SubscribeExpectation{
			MessageCount:         1,
			WireTraceparents:     []string{wireTP},
			CallbackTraceparents: []string{cbTP},
			AckIDs:               []string{"ack-1"},
			MessageIDs:           []string{"msg-1"},
			ExactlyOnce:          BoolPtr(false),
			BatchCount:           1,
			ExpectedOutcome:      "acked",
			ExpectedSettlements:  []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 1}},
		},
	}
	return spans, exp
}

func TestGoCanonical_PositiveFlows(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage:     "go",
		SupportsMonitoring: true,
	}

	t.Run("full publish + subscribe + W3C propagation + otelgrpc coexistence", func(t *testing.T) {
		spans, exp := buildCanonicalGoE2EFixture()
		// Add otelgrpc span which should be ignored by Go canonical profile.
		spans = append(spans, &proxypb.TraceSpan{
			TraceId:                  testTraceID(999),
			SpanId:                   testSpanID(9999),
			Name:                     "google.pubsub.v1.Publisher/Publish",
			Kind:                     proxypb.SpanKind_SPAN_KIND_CLIENT,
			InstrumentationScopeName: OTelGrpcScopeName,
		})
		res := Verify(goCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("shuffled multi-message multi-batch publish + concurrent deliveries + batched ack", func(t *testing.T) {
		pub1 := makeGoPublishBatchSpan("p", "t", 2, 1)
		pub2 := makeGoPublishBatchSpan("p", "t", 1, 2)
		c1, fc1, b1, tp1 := makeGoPublishMessageSpans("p", "t", "m1", "k1", 10, 1, pub1, 2)
		c2, fc2, b2, tp2 := makeGoPublishMessageSpans("p", "t", "m2", "k1", 12, 2, pub1, 2)
		c3, fc3, b3, tp3 := makeGoPublishMessageSpans("p", "t", "m3", "", 7, 3, pub2, 1)

		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m1", "a1", "k1", 10, 3, false, 3, tp1, "acked", 1)
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m2", "a2", "k1", 12, 0, false, 3, tp2, "acked", 2)
		sub3, cc3, sc3, pr3, _ := makeGoSubscribeMessageSpans("p", "s", "m3", "a3", "", 7, 0, false, 3, tp3, "acked", 3)
		for _, pr := range []*proxypb.TraceSpan{pr1, pr2, pr3} {
			pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
		}
		// Reverse order of subscribe links in batched ack span to test unordered link matching.
		ackBatch := makeGoSettlementSpan("p", "s", "ack", 3, 0, false, 1, []*proxypb.TraceSpan{sub3, sub1, sub2})

		// Deliberately shuffle span slice!
		spans := []*proxypb.TraceSpan{
			ackBatch, pr3, b2, sub2, pub2, fc1, c3, sc1, cc3, c1, pub1, sub3, pr1, fc3,
			b1, c2, sub1, cc2, sc3, b3, fc2, pr2, sc2, cc1,
		}

		exp := &TraceExpectation{
			Topic:        "projects/p/topics/t",
			Subscription: "projects/p/subscriptions/s",
			Publish: &PublishExpectation{
				BatchCount: 2,
				Batches:    []PublishBatchExpectation{{MessageCount: 1}, {MessageCount: 2}},
				Messages: []PublishMessageExpectation{
					{MessageID: "m1", OrderingKey: "k1", BodySize: Int64Ptr(10), WireTraceparent: tp1},
					{MessageID: "m2", OrderingKey: "k1", BodySize: Int64Ptr(12), WireTraceparent: tp2},
					{MessageID: "m3", OrderingKey: "", BodySize: Int64Ptr(7), WireTraceparent: tp3},
				},
			},
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m3", AckID: "a3", OrderingKey: "", BodySize: Int64Ptr(7), BatchCount: 3, WireTraceparent: tp3, Settlements: []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 3}}},
					{MessageID: "m1", AckID: "a1", OrderingKey: "k1", BodySize: Int64Ptr(10), DeliveryAttempt: 3, BatchCount: 3, WireTraceparent: tp1, Settlements: []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 3}}},
					{MessageID: "m2", AckID: "a2", OrderingKey: "k1", BodySize: Int64Ptr(12), BatchCount: 3, WireTraceparent: tp2, Settlements: []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 3}}},
				},
			},
		}
		res := Verify(goCaps, spans, exp)
		if !res.Passed() {
			t.Fatalf("expected PASS on shuffled multi-batch graph, got:\n%s", res.Format())
		}
	})

	t.Run("hedged publish events", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 1, 1)
		c, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m1", "", 4, 1, pub, 1)
		c.Events = append(c.Events,
			&proxypb.SpanEvent{
				Name:       "publish start (hedged)",
				Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)},
			},
			&proxypb.SpanEvent{Name: "publish end (hedged)"},
		)
		res := Verify(goCaps, []*proxypb.TraceSpan{c, fc, batch, pub}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:      1,
				WireTraceparents:  []string{tp},
				Hedged:            true,
				MinHedgedAttempts: 1,
			},
		})
		if !res.Passed() {
			t.Fatalf("expected PASS for hedged publish, got:\n%s", res.Format())
		}
	})

	t.Run("all 4 Go publish error semantics", func(t *testing.T) {
		// 1. Pre-validation error.
		cPre, _, _, _ := makeGoPublishMessageSpans("p", "t", "", "", 5, 1, nil, 1)
		cPre.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		cPre.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cPre.StatusMessage = "publisher stopped"
		resPre := Verify(goCaps, []*proxypb.TraceSpan{cPre}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:   1,
				ExpectError:    true,
				ErrorKind:      PublishErrorPreValidation,
				ErrorSubstring: "stopped",
			},
		})
		if !resPre.Passed() {
			t.Fatalf("pre-validation error expected PASS, got:\n%s", resPre.Format())
		}

		// 2. Flow control error (create leaked, ONLY "publisher flow control" exported).
		fcErr := goSpan(testTraceID(1), testSpanID(2), testSpanID(1), "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)
		fcErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		fcErr.StatusMessage = "flow control limit exceeded"
		fcErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		resFC := Verify(goCaps, []*proxypb.TraceSpan{fcErr}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:   1,
				ExpectError:    true,
				ErrorKind:      PublishErrorFlowControl,
				ErrorSubstring: "flow control",
			},
		})
		if !resFC.Passed() {
			t.Fatalf("flow control leak expected PASS, got:\n%s", resFC.Format())
		}

		// 3. Oversized error (create ERROR + FC UNSET, NO batching span).
		cOver, fcOver, _, _ := makeGoPublishMessageSpans("p", "t", "", "", 11000000, 2, nil, 1)
		cOver.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		cOver.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cOver.StatusMessage = "ErrOversizedMessage"
		resOver := Verify(goCaps, []*proxypb.TraceSpan{cOver, fcOver}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:   1,
				ExpectError:    true,
				ErrorKind:      PublishErrorOversized,
				ErrorSubstring: "Oversized",
			},
		})
		if !resOver.Passed() {
			t.Fatalf("oversized error expected PASS, got:\n%s", resOver.Format())
		}

		// 4. Publish RPC error (create ERROR + publish start + exception + NO publish end; publish stays UNSET).
		pubRPC := makeGoPublishBatchSpan("p", "t", 1, 3)
		cRPC, fcRPC, bRPC, tpRPC := makeGoPublishMessageSpans("p", "t", "", "", 8, 3, pubRPC, 1)
		cRPC.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cRPC.StatusMessage = "rpc failed: permission denied"
		cRPC.Events = []*proxypb.SpanEvent{
			{
				Name:       "publish start",
				Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)},
			},
			{Name: "exception"},
		}
		resRPC := Verify(goCaps, []*proxypb.TraceSpan{cRPC, fcRPC, bRPC, pubRPC}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:     1,
				WireTraceparents: []string{tpRPC},
				ExpectError:      true,
				ErrorKind:        PublishErrorRPC,
				ErrorSubstring:   "permission denied",
			},
		})
		if !resRPC.Passed() {
			t.Fatalf("publish RPC error expected PASS, got:\n%s", resRPC.Format())
		}
	})

	t.Run("subscribe nacked, modack, expired, ALO error, EOD error, unsampled & corrupt traceparent", func(t *testing.T) {
		// Nacked + receipt modack.
		subN, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "mn", "an", "", 5, 0, false, 1, "", "nacked", 10)
		prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
		modack := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 10, []*proxypb.TraceSpan{subN})
		nack := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 11, []*proxypb.TraceSpan{subN})
		resNack := Verify(goCaps, []*proxypb.TraceSpan{subN, ccN, scN, prN, modack, nack}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				ExpectedOutcome:     "nacked",
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{
					{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true)},
					{Operation: "nack"},
				},
			},
		})
		if !resNack.Passed() {
			t.Fatalf("nack+modack expected PASS, got:\n%s", resNack.Format())
		}

		// Expired (Go omits <subID> process).
		subExp, ccExp, scExp, _, _ := makeGoSubscribeMessageSpans("p", "s", "me", "ae", "", 5, 0, false, 1, "not-a-valid-tp", "expired", 12)
		resExp := Verify(goCaps, []*proxypb.TraceSpan{subExp, ccExp, scExp}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:     1,
				WireTraceparents: []string{"not-a-valid-tp"},
				ExpectedOutcome:  "expired",
			},
		})
		if !resExp.Passed() {
			t.Fatalf("expired expected PASS, got:\n%s", resExp.Format())
		}

		// ALO ack RPC error: ack has ERROR + exception, subscribe stays UNSET with result="acked".
		subALO, ccALO, scALO, prALO, _ := makeGoSubscribeMessageSpans("p", "s", "ma", "aa", "", 5, 0, false, 1, "", "acked", 13)
		prALO.Events = append(prALO.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13, []*proxypb.TraceSpan{subALO})
		ackALO.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackALO.StatusMessage = "unavailable"
		ackALO.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		resALO := Verify(goCaps, []*proxypb.TraceSpan{subALO, ccALO, scALO, prALO, ackALO}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, ErrorSubstring: "unavailable"}},
			},
		})
		if !resALO.Passed() {
			t.Fatalf("ALO error expected PASS, got:\n%s", resALO.Format())
		}

		// EOD partial batch failure: subscribe has ack start + ack end + exception, omits result, ERROR; ack has partial error msg without exception.
		subEOD, ccEOD, scEOD, prEOD, _ := makeGoSubscribeMessageSpans("p", "s", "meod", "aeod", "", 5, 0, true, 1, "", "", 14)
		prEOD.Events = append(prEOD.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackEOD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14, []*proxypb.TraceSpan{subEOD})
		subEOD.Events = append(subEOD.Events, &proxypb.SpanEvent{Name: "exception"})
		subEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subEOD.StatusMessage = "invalid ack id"
		ackEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackEOD.StatusMessage = eodPartialBatchErrorMsg
		resEOD := Verify(goCaps, []*proxypb.TraceSpan{subEOD, ccEOD, scEOD, prEOD, ackEOD}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount: 1,
				ExactlyOnce:  BoolPtr(true),
				ExpectedSettlements: []SettlementExpectation{{
					Operation:              "ack",
					ExpectError:            true,
					EODPartialBatchFailure: true,
				}},
			},
		})
		if !resEOD.Passed() {
			t.Fatalf("EOD error expected PASS, got:\n%s", resEOD.Format())
		}

		// Unsampled wire traceparent (-00): Go AlwaysSample parents subscribe, OR ParentBased drops subtree & emits unlinked ack.
		unsampledTP := fmt.Sprintf("00-%s-%s-00", testTraceID(42), testSpanID(42))
		subUnsamp, ccUnsamp, scUnsamp, prUnsamp, _ := makeGoSubscribeMessageSpans("p", "s", "mu", "au", "", 5, 0, false, 1, unsampledTP, "acked", 15)
		prUnsamp.Events = append(prUnsamp.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackUnsamp := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 15, []*proxypb.TraceSpan{subUnsamp})
		resUnsampParented := Verify(goCaps, []*proxypb.TraceSpan{subUnsamp, ccUnsamp, scUnsamp, prUnsamp, ackUnsamp}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				WireTraceparents:    []string{unsampledTP},
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		})
		if !resUnsampParented.Passed() {
			t.Fatalf("unsampled AlwaysSample expected PASS, got:\n%s", resUnsampParented.Format())
		}

		ackDropped := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 16, nil)
		resUnsampDropped := Verify(goCaps, []*proxypb.TraceSpan{ackDropped}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampledTP},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		})
		if !resUnsampDropped.Passed() {
			t.Fatalf("unsampled ParentBased drop expected PASS, got:\n%s", resUnsampDropped.Format())
		}
	})
}

func TestGoCanonical_NegativeDetection(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage:     "go",
		SupportsMonitoring: true,
	}

	removeSpanNamed := func(spans []*proxypb.TraceSpan, name string) []*proxypb.TraceSpan {
		var out []*proxypb.TraceSpan
		for _, s := range spans {
			if s.GetName() != name {
				out = append(out, s)
			}
		}
		return out
	}

	findNamed := func(spans []*proxypb.TraceSpan, name string) *proxypb.TraceSpan {
		for _, s := range spans {
			if s.GetName() == name {
				return s
			}
		}
		return nil
	}

	cases := []struct {
		name     string
		wantCode ViolationCode
		mutate   func(spans []*proxypb.TraceSpan, exp *TraceExpectation) []*proxypb.TraceSpan
	}{
		{"missing create", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "t create")
		}},
		{"missing flow control", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "publisher flow control")
		}},
		{"missing batching", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "publisher batching")
		}},
		{"missing publish", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "t publish")
		}},
		{"missing subscribe", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "s subscribe")
		}},
		{"missing concurrency control", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "subscriber concurrency control")
		}},
		{"missing subscribe scheduler", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "subscribe scheduler")
		}},
		{"missing process", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "s process")
		}},
		{"missing ack", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return removeSpanNamed(s, "s ack")
		}},
		{"extra duplicate create span", CodeUnexpectedSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			dup := goSpan(testTraceID(888), testSpanID(8888), "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
			return append(s, dup)
		}},
		{"unexpected bad.span under go scope", CodeUnexpectedSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			return append(s, goSpan(testTraceID(889), testSpanID(8889), "", "bad.span", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0))
		}},
		{"wrong kind on create", CodeWrongSpanKind, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t create").Kind = proxypb.SpanKind_SPAN_KIND_INTERNAL
			return s
		}},
		{"wrong kind on subscribe", CodeWrongSpanKind, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "s subscribe").Kind = proxypb.SpanKind_SPAN_KIND_CONSUMER
			return s
		}},
		{"wrong kind on process", CodeWrongSpanKind, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "s process").Kind = proxypb.SpanKind_SPAN_KIND_INTERNAL
			return s
		}},
		{"wrong operation type deliver on process", CodeWrongAttributeValue, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "s process").Attributes["messaging.operation.type"] = strAV("deliver")
			return s
		}},
		{"wrong kind on publish", CodeWrongSpanKind, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t publish").Kind = proxypb.SpanKind_SPAN_KIND_CLIENT
			return s
		}},
		{"batching parented to flow control instead of create", CodeMissingSpan, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			fc := findNamed(s, "publisher flow control")
			findNamed(s, "publisher batching").ParentSpanId = fc.SpanId
			return s
		}},
		{"publish parented to create instead of root", CodeWrongParent, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			c := findNamed(s, "t create")
			p := findNamed(s, "t publish")
			p.TraceId = c.TraceId
			p.ParentSpanId = c.SpanId
			return s
		}},
		{"all-zero ParentSpanId on root", CodeInvalidParentSpanID, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t publish").ParentSpanId = "0000000000000000"
			return s
		}},
		{"broken wire traceparent mismatch", CodeBrokenTraceparent, func(s []*proxypb.TraceSpan, exp *TraceExpectation) []*proxypb.TraceSpan {
			exp.Publish.WireTraceparents = []string{fmt.Sprintf("00-%s-%s-01", testTraceID(12345), testSpanID(12345))}
			return s
		}},
		{"subscribe ignores valid wire traceparent", CodeBrokenTraceparent, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			sub := findNamed(s, "s subscribe")
			sub.TraceId = testTraceID(7777)
			sub.ParentSpanId = ""
			return s
		}},
		{"unidirectional publish->create missing reverse link", CodeBrokenBidirectionalLink, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t create").Links = nil
			return s
		}},
		{"unidirectional ack->subscribe missing reverse link", CodeBrokenBidirectionalLink, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "s subscribe").Links = nil
			return s
		}},
		{"missing messaging.operation.name on create->publish reverse link", CodeMissingLinkAttr, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t create").Links[0].Attributes = nil
			return s
		}},
		{"wrong messaging.operation.name on subscribe->ack link", CodeMissingLinkAttr, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "s subscribe").Links[0].Attributes["messaging.operation.name"] = strAV("deliver")
			return s
		}},
		{"wrong attribute type body.size as string", CodeWrongAttributeType, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t create").Attributes["messaging.message.body.size"] = strAV("5")
			return s
		}},
		{"missing gcp.resource.name", CodeMissingAttribute, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			delete(findNamed(s, "t create").Attributes, "gcp.resource.name")
			return s
		}},
		{"missing unconditional empty ordering_key", CodeMissingAttribute, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			delete(findNamed(s, "t create").Attributes, "messaging.gcp_pubsub.message.ordering_key")
			return s
		}},
		{"wrong result ack vs acked", CodeWrongAttributeValue, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "s subscribe").Attributes["messaging.gcp_pubsub.result"] = strAV("ack")
			return s
		}},
		{"delivery_attempt present when 0", CodeUnexpectedAttribute, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "s subscribe").Attributes["messaging.gcp_pubsub.message.delivery_attempt"] = intAV(0)
			return s
		}},
		{"missing publish start event", CodeMissingEvent, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t create").Events = []*proxypb.SpanEvent{{Name: "publish end"}}
			return s
		}},
		{"missing ack called event on process", CodeMissingEvent, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "s process").Events = nil
			return s
		}},
		{"wrong instrumentation scope", CodeWrongScope, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t create").InstrumentationScopeName = "gl-cpp"
			return s
		}},
		{"wrong OK status on Go create", CodeWrongStatusCode, func(s []*proxypb.TraceSpan, _ *TraceExpectation) []*proxypb.TraceSpan {
			findNamed(s, "t create").StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
			return s
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseSpans, exp := buildCanonicalGoE2EFixture()
			mutated := tc.mutate(cloneSpans(baseSpans), exp)
			res := Verify(goCaps, mutated, exp)
			if res.Status != StatusFail {
				t.Fatalf("expected FAIL for %q, got PASS:\n%s", tc.name, res.Format())
			}
			foundCode := false
			for _, f := range res.Failures {
				if f.Code == tc.wantCode {
					foundCode = true
					break
				}
			}
			if !foundCode {
				t.Fatalf("expected violation code %s for %q, got failures:\n%s", tc.wantCode, tc.name, res.Format())
			}
		})
	}

	t.Run("nack wrongly including ack_deadline or is_receipt_modack fails", func(t *testing.T) {
		sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "m1", "a1", "", 5, 0, false, 1, "", "nacked", 50)
		pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "nack called"})
		nack := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 50, []*proxypb.TraceSpan{sub})
		nack.Attributes["messaging.gcp_pubsub.message.ack_deadline"] = intAV(0)
		res := Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, nack}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExpectedOutcome:     "nacked",
				ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}},
			},
		})
		if res.Status != StatusFail {
			t.Fatalf("expected FAIL when nack includes ack_deadline, got PASS")
		}
	})

	assertSingleRuleFailure := func(t *testing.T, res TraceResult, wantCode ViolationCode, wantRule string) {
		t.Helper()
		if res.Status != StatusFail {
			t.Fatalf("expected FAIL (%s/%s), got PASS", wantCode, wantRule)
		}
		for _, f := range res.Failures {
			if f.Code == wantCode && f.Rule == wantRule {
				return
			}
		}
		t.Fatalf("expected violation (%s, rule=%s), got:\n%s", wantCode, wantRule, res.Format())
	}

	t.Run("publish RPC error missing ERROR status fails", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 1, 60)
		c, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "", "", 5, 60, pub, 1)
		c.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		c.Events = []*proxypb.SpanEvent{
			{Name: "publish start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "exception"},
		}
		res := Verify(goCaps, []*proxypb.TraceSpan{c, fc, batch, pub}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, WireTraceparents: []string{tp}, ExpectError: true, ErrorKind: PublishErrorRPC},
		})
		assertSingleRuleFailure(t, res, CodeWrongStatusCode, "publish.create.status")
	})

	t.Run("publish RPC error empty StatusMessage fails", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 1, 61)
		c, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "", "", 5, 61, pub, 1)
		c.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		c.StatusMessage = ""
		c.Events = []*proxypb.SpanEvent{
			{Name: "publish start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "exception"},
		}
		res := Verify(goCaps, []*proxypb.TraceSpan{c, fc, batch, pub}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, WireTraceparents: []string{tp}, ExpectError: true, ErrorKind: PublishErrorRPC},
		})
		assertSingleRuleFailure(t, res, CodeWrongStatusMessage, "publish.create.status.message")
	})

	t.Run("publish RPC error spurious publish end fails", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 1, 62)
		c, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "", "", 5, 62, pub, 1)
		c.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		c.StatusMessage = "boom"
		c.Events = append(c.Events, &proxypb.SpanEvent{Name: "exception"})
		// makeGoPublishMessageSpans includes "publish end", which MUST be rejected on Go RPC failure.
		res := Verify(goCaps, []*proxypb.TraceSpan{c, fc, batch, pub}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, WireTraceparents: []string{tp}, ExpectError: true, ErrorKind: PublishErrorRPC},
		})
		assertSingleRuleFailure(t, res, CodeUnexpectedSpan, "publish.create.event.no_publish_end_on_error")
	})

	t.Run("custom check failure propagates", func(t *testing.T) {
		spans, exp := buildCanonicalGoE2EFixture()
		exp.CustomChecks = []CustomCheck{
			func(spans []*proxypb.TraceSpan, _ Profile) []Violation {
				return []Violation{{Rule: "custom.demo", Message: "custom invariant violated"}}
			},
		}
		res := Verify(goCaps, spans, exp)
		if res.Status != StatusFail || len(res.Failures) == 0 || res.Failures[0].Code != CodeCustomCheckFailed {
			t.Fatalf("expected custom check failure, got %+v", res)
		}
	})

	t.Run("additional Go edge cases: empty 0-byte payload, ParentTraceparent, sequential batches, error message.id=\"\" and EOD subscribe result", func(t *testing.T) {
		// Positive: 0-byte payload + Publish.ParentTraceparent.
		callerTP := fmt.Sprintf("00-%s-%s-01", testTraceID(444), testSpanID(4444))
		pub0 := makeGoPublishBatchSpan("p", "t", 1, 70)
		c0, fc0, b0, wire0 := makeGoPublishMessageSpans("p", "t", "m0", "", 0, 70, pub0, 1)
		c0.TraceId = testTraceID(444)
		c0.ParentSpanId = testSpanID(4444)
		fc0.TraceId = c0.TraceId
		b0.TraceId = c0.TraceId
		pub0.Links[0].TraceId = c0.TraceId
		wire0 = fmt.Sprintf("00-%s-%s-01", c0.TraceId, c0.SpanId)
		res0 := Verify(goCaps, []*proxypb.TraceSpan{c0, fc0, b0, pub0}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:      1,
				ParentTraceparent: callerTP,
				WireTraceparents:  []string{wire0},
				BodySizes:         []int64{0},
			},
		})
		if !res0.Passed() {
			t.Fatalf("0-byte + ParentTraceparent expected PASS, got:\n%s", res0.Format())
		}

		// Negative: broken ParentTraceparent on Publish.
		c0Bad := cloneSpans([]*proxypb.TraceSpan{c0, fc0, b0, pub0})
		c0Bad[0].ParentSpanId = testSpanID(9999)
		if res := Verify(goCaps, c0Bad, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:      1,
				ParentTraceparent: callerTP,
				WireTraceparents:  []string{wire0},
			},
		}); res.Status != StatusFail {
			t.Fatal("expected FAIL on broken Publish.ParentTraceparent")
		}

		// Negative: Go publish error omitting messaging.message.id="".
		cPreErr, _, _, _ := makeGoPublishMessageSpans("p", "t", "", "", 5, 71, nil, 1)
		delete(cPreErr.Attributes, "messaging.message.id")
		cPreErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cPreErr.StatusMessage = "invalid"
		cPreErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		if res := Verify(goCaps, []*proxypb.TraceSpan{cPreErr}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, ExpectError: true, ErrorKind: PublishErrorPreValidation},
		}); res.Status != StatusFail {
			t.Fatal("expected FAIL when failed Go create omits messaging.message.id")
		}

		// Negative: Go publish RPC failure wrongly sets ERROR on <topic> publish.
		pubRPCErr := makeGoPublishBatchSpan("p", "t", 1, 72)
		pubRPCErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		pubRPCErr.StatusMessage = "boom"
		cRPCErr, fcRPCErr, bRPCErr, tpRPCErr := makeGoPublishMessageSpans("p", "t", "", "", 5, 72, pubRPCErr, 1)
		cRPCErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cRPCErr.StatusMessage = "boom"
		cRPCErr.Events = []*proxypb.SpanEvent{
			{Name: "publish start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "exception"},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{cRPCErr, fcRPCErr, bRPCErr, pubRPCErr}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, WireTraceparents: []string{tpRPCErr}, ExpectError: true, ErrorKind: PublishErrorRPC},
		}); res.Status != StatusFail {
			t.Fatal("expected FAIL when Go <topic> publish has ERROR status")
		}

		// Negative: EOD failure wrongly retains result="acked" on <subID> subscribe.
		subEODBad, ccEODBad, scEODBad, prEODBad, _ := makeGoSubscribeMessageSpans("p", "s", "me1", "ae1", "", 5, 0, true, 1, "", "acked", 73)
		prEODBad.Events = append(prEODBad.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackEODBad := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 73, []*proxypb.TraceSpan{subEODBad})
		subEODBad.Events = append(subEODBad.Events, &proxypb.SpanEvent{Name: "exception"})
		subEODBad.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subEODBad.StatusMessage = "perm fail"
		ackEODBad.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackEODBad.StatusMessage = "perm fail"
		ackEODBad.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		if res := Verify(goCaps, []*proxypb.TraceSpan{subEODBad, ccEODBad, scEODBad, prEODBad, ackEODBad}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(true),
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack", ExpectError: true}},
			},
		}); res.Status != StatusFail {
			t.Fatal("expected FAIL when EOD failed subscribe wrongly sets result=\"acked\"")
		}

		// Negative: Sequential batches mismatch ([2, 1] observed vs [1, 2] required sequentially).
		pSeq1 := makeGoPublishBatchSpan("p", "t", 2, 74)
		pSeq2 := makeGoPublishBatchSpan("p", "t", 1, 75)
		cs1, fcs1, bs1, tps1 := makeGoPublishMessageSpans("p", "t", "ms1", "", 4, 74, pSeq1, 2)
		cs2, fcs2, bs2, tps2 := makeGoPublishMessageSpans("p", "t", "ms2", "", 4, 75, pSeq1, 2)
		cs3, fcs3, bs3, tps3 := makeGoPublishMessageSpans("p", "t", "ms3", "", 4, 76, pSeq2, 1)
		if res := Verify(goCaps, []*proxypb.TraceSpan{cs1, fcs1, bs1, cs2, fcs2, bs2, cs3, fcs3, bs3, pSeq1, pSeq2}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:             3,
				BatchCount:               2,
				WireTraceparents:         []string{tps1, tps2, tps3},
				RequireSequentialBatches: true,
				Batches:                  []PublishBatchExpectation{{MessageCount: 1}, {MessageCount: 2}},
			},
		}); res.Status != StatusFail {
			t.Fatal("expected FAIL when sequential batch order mismatches")
		}
	})
}

func assertHasViolation(t *testing.T, res TraceResult, wantCode ViolationCode, wantRule string) {
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

func TestRemediationRegressions_GoAndGeneric(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage:     "go",
		SupportsMonitoring: true,
	}

	t.Run("ALO fast ack unlinked receipt modack race and linked modack reverse link", func(t *testing.T) {
		sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "m-fast", "a-fast", "", 5, 0, false, 1, "", "acked", 101)
		pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 101, []*proxypb.TraceSpan{sub})
		// Unlinked receipt modack emitted when Ack ran <100ms before receiptTicker fired.
		unlinkedReceipt := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 102, nil)

		exp := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, unlinkedReceipt}, exp); !res.Passed() {
			t.Fatalf("expected PASS for unlinked ALO receipt modack race, got:\n%s", res.Format())
		}

		// StrictSettlementOps=true still rejects unexpected unlinked receipt modack.
		strictExp := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, unlinkedReceipt}, strictExp), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// Unlinked non-receipt modack (is_receipt_modack=false) on sampled ALO subscribe MUST fail.
		unlinkedNonReceipt := makeGoSettlementSpan("p", "s", "modack", 1, 10, false, 103, nil)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, unlinkedNonReceipt}, exp), CodeWrongAttributeValue, "settlement.modack.attr.is_receipt_modack")

		// Linked automatic modack missing reverse link on subscribe -> modack MUST fail even when StrictSettlementOps=false.
		linkedUnidirModack := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 104, nil)
		linkedUnidirModack.Links = []*proxypb.SpanLink{{TraceId: sub.TraceId, SpanId: sub.SpanId}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, linkedUnidirModack}, exp), CodeBrokenBidirectionalLink, "subscribe.settlement.reverse_link")

		// Linked automatic modack with reverse link missing messaging.operation.name MUST fail.
		subCopy := cloneSpans([]*proxypb.TraceSpan{sub})[0]
		linkedMissingAttr := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 105, []*proxypb.TraceSpan{subCopy})
		subCopy.Links[len(subCopy.Links)-1].Attributes = nil
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subCopy, cc, sc, pr, ack, linkedMissingAttr}, exp), CodeMissingLinkAttr, "subscribe.settlement.reverse_link_attr")
	})

	t.Run("verifyDroppedUnsampledSubscribe single+modack, batched, mixed sampled+unsampled, and fresh root failures", func(t *testing.T) {
		unsampTP1 := fmt.Sprintf("00-%s-%s-00", testTraceID(501), testSpanID(5010))
		unsampTP2 := fmt.Sprintf("00-%s-%s-00", testTraceID(502), testSpanID(5020))

		// (a) Single dropped -00 with automatic receipt modack + unlinked ack.
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 201, nil)
		mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 202, nil)
		resSingle := Verify(goCaps, []*proxypb.TraceSpan{mod1, ack1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampTP1},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		})
		if !resSingle.Passed() {
			t.Fatalf("dropped -00 + receipt modack expected PASS, got:\n%s", resSingle.Format())
		}

		// (b) Two dropped -00 messages sharing ONE batched ack (batch.message_count=2, links=[]).
		ackBatch2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 203, nil)
		modBatch2 := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 204, nil)
		resBatchDropped := Verify(goCaps, []*proxypb.TraceSpan{modBatch2, ackBatch2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:           2,
				WireTraceparents:       []string{unsampTP1, unsampTP2},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		})
		if !resBatchDropped.Passed() {
			t.Fatalf("two dropped -00 batched ack expected PASS, got:\n%s", resBatchDropped.Format())
		}
		// Single-slot ack (batch.message_count=1) cannot satisfy two dropped -00 messages.
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{ack1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:           2,
				WireTraceparents:       []string{unsampTP1, unsampTP2},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		}), CodeMissingSpan, "subscribe.unsampled_settlement.present")

		// (c) Mixed sampled (-01) + dropped (-00) sharing ONE batched ack (batch.message_count=2, links=[subSamp]).
		sampTP := fmt.Sprintf("00-%s-%s-01", testTraceID(503), testSpanID(5030))
		subSamp, ccSamp, scSamp, prSamp, _ := makeGoSubscribeMessageSpans("p", "s", "msamp", "asamp", "", 5, 0, false, 2, sampTP, "acked", 205)
		prSamp.Events = append(prSamp.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackMixed := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 205, []*proxypb.TraceSpan{subSamp})
		modMixed := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 206, []*proxypb.TraceSpan{subSamp})
		mixedSpans := []*proxypb.TraceSpan{subSamp, ccSamp, scSamp, prSamp, modMixed, ackMixed}
		for _, orderName := range []string{"sampled_first", "dropped_first"} {
			msgs := []SubscribeMessageExpectation{
				{MessageID: "msamp", AckID: "asamp", WireTraceparent: sampTP, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
			}
			if orderName == "dropped_first" {
				msgs[0], msgs[1] = msgs[1], msgs[0]
			}
			resMix := Verify(goCaps, mixedSpans, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce: BoolPtr(false),
					Messages:    msgs,
				},
			})
			if !resMix.Passed() {
				t.Fatalf("mixed sampled+dropped (%s) expected PASS, got:\n%s", orderName, resMix.Format())
			}
		}

		// Mixed negative: if batched ack has batch.message_count=1, dropped -00 has no spare slot.
		subSamp1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "msamp", "asamp", "", 5, 0, false, 1, sampTP, "acked", 207)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackOnly1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 207, []*proxypb.TraceSpan{subSamp1})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subSamp1, cc1, sc1, pr1, ackOnly1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{MessageID: "msamp", AckID: "asamp", WireTraceparent: sampTP, BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}), CodeMissingSpan, "subscribe.unsampled_settlement.present")

		// Forbidden link targeting dropped trace on ack fails.
		badAckLink := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 208, nil)
		badAckLink.Links = []*proxypb.SpanLink{{TraceId: testTraceID(501), SpanId: testSpanID(9999)}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{badAckLink}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampTP1},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		}), CodeBrokenTraceparent, "subscribe.unsampled_settlement.forbidden_link")

		// (d) Buggy client ignoring -00 and creating fresh root subscribe span (even with AllowExtraPubSubSpans=true) fails!
		freshRootSub, freshCC, freshSC, freshPR, _ := makeGoSubscribeMessageSpans("p", "s", "mbad", "abad", "", 5, 0, false, 1, "", "acked", 209)
		freshPR.Events = append(freshPR.Events, &proxypb.SpanEvent{Name: "ack called"})
		freshAck := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 209, []*proxypb.TraceSpan{freshRootSub})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{freshRootSub, freshCC, freshSC, freshPR, freshAck}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampTP1},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		}), CodeUnexpectedSpan, "subscribe.unsampled_dropped")

		// (d) Any process/cc/scheduler span emitted in dropped trace fails.
		leakedProc := goSpan(testTraceID(501), testSpanID(5015), testSpanID(5010), "s process", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{leakedProc, ack1}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampTP1},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		}), CodeUnexpectedSpan, "subscribe.unsampled_dropped")
	})

	t.Run("mixed healthy publish + Go PublishErrorFlowControl (PFC-03) and FC scope check", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 1, 301)
		cOK, fcOK, bOK, tpOK := makeGoPublishMessageSpans("p", "t", "m-ok", "", 5, 301, pub, 1)
		// FC error span has earlier StartTime to test non-greedy matching!
		fcErr := goSpan(testTraceID(302), testSpanID(3022), testSpanID(3021), "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)
		fcErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		fcErr.StatusMessage = "fc limit exceeded"
		fcErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		spans := []*proxypb.TraceSpan{fcErr, cOK, fcOK, bOK, pub}
		for _, errKind := range []PublishErrorKind{PublishErrorFlowControl, PublishErrorAuto} {
			for _, orderName := range []string{"fc_first", "ok_first"} {
				msgs := []PublishMessageExpectation{
					{ExpectError: true, ErrorKind: errKind, ErrorSubstring: "fc limit"},
					{MessageID: "m-ok", WireTraceparent: tpOK},
				}
				if orderName == "ok_first" {
					msgs[0], msgs[1] = msgs[1], msgs[0]
				}
				res := Verify(goCaps, spans, &TraceExpectation{
					Topic:   "projects/p/topics/t",
					Publish: &PublishExpectation{BatchCount: 1, Messages: msgs},
				})
				if !res.Passed() {
					t.Fatalf("PFC-03 mixed (%s/%s) expected PASS, got:\n%s", errKind, orderName, res.Format())
				}
			}
		}

		// Wrong scope on leaked Go flow control error span MUST fail with CodeWrongScope.
		fcBadScope := cloneSpans([]*proxypb.TraceSpan{fcErr})[0]
		fcBadScope.InstrumentationScopeName = "wrong-scope"
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{fcBadScope}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:   1,
				ExpectError:    true,
				ErrorKind:      PublishErrorFlowControl,
				ErrorSubstring: "fc limit",
			},
		}), CodeWrongScope, "publish.flow_control.scope")
	})

	t.Run("non-greedy specificity-ordered matching for Publish, Subscribe, Redelivery, and Batches", func(t *testing.T) {
		// 1. Publish: weak Messages[0] before strong Messages[1] with c2 earlier than c1.
		pub := makeGoPublishBatchSpan("p", "t", 2, 310)
		c2, fc2, b2, tp2 := makeGoPublishMessageSpans("p", "t", "m2", "", 5, 1, pub, 2)
		c1, fc1, b1, _ := makeGoPublishMessageSpans("p", "t", "m1", "", 5, 2, pub, 2)
		resPub := Verify(goCaps, []*proxypb.TraceSpan{c2, fc2, b2, c1, fc1, b1, pub}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{BodySize: Int64Ptr(5)},
					{BodySize: Int64Ptr(5), MessageID: "m2", WireTraceparent: tp2},
				},
			},
		})
		if !resPub.Passed() {
			t.Fatalf("Publish specificity order expected PASS, got:\n%s", resPub.Format())
		}

		// 2. Subscribe: weak Messages[0] before strong Messages[1] with sub2 earlier than sub1.
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m2", "a2", "k1", 5, 0, false, 2, "", "acked", 1)
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m1", "a1", "k1", 5, 0, false, 2, "", "acked", 2)
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 311, []*proxypb.TraceSpan{sub2, sub1})
		resSub := Verify(goCaps, []*proxypb.TraceSpan{sub2, cc2, sc2, pr2, sub1, cc1, sc1, pr1, ack2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{OrderingKey: "k1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "a2", MessageID: "m2", OrderingKey: "k1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		})
		if !resSub.Passed() {
			t.Fatalf("Subscribe specificity order expected PASS, got:\n%s", resSub.Format())
		}

		// 3. Redelivery of same MessageID without AckID (earlier nacked, later acked; Messages=[acked, nacked]).
		subNack, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "m-redeliv", "a-nack", "", 5, 0, false, 1, "", "nacked", 10)
		prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
		nackSp := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 312, []*proxypb.TraceSpan{subNack})
		subAck, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-redeliv", "a-ack", "", 5, 2, false, 1, "", "acked", 11)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackSp := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 313, []*proxypb.TraceSpan{subAck})
		resRedeliv := Verify(goCaps, []*proxypb.TraceSpan{subNack, ccN, scN, prN, nackSp, subAck, ccA, scA, prA, ackSp}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-redeliv", DeliveryAttempt: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{MessageID: "m-redeliv", Settlements: []SettlementExpectation{{Operation: "nack"}}},
				},
			},
		})
		if !resRedeliv.Passed() {
			t.Fatalf("Redelivery without AckID ordered [acked, nacked] expected PASS, got:\n%s", resRedeliv.Format())
		}

		// 4. Publish.Batches wildcard MessageCount=0 before specific MessageCount=2 when [count=2, count=1].
		pB1 := makeGoPublishBatchSpan("p", "t", 2, 314)
		pB2 := makeGoPublishBatchSpan("p", "t", 1, 315)
		cb1, fcb1, bb1, tpb1 := makeGoPublishMessageSpans("p", "t", "mb1", "", 4, 1, pB1, 2)
		cb2, fcb2, bb2, tpb2 := makeGoPublishMessageSpans("p", "t", "mb2", "", 4, 2, pB1, 2)
		cb3, fcb3, bb3, tpb3 := makeGoPublishMessageSpans("p", "t", "mb3", "", 4, 3, pB2, 1)
		resBatches := Verify(goCaps, []*proxypb.TraceSpan{cb1, fcb1, bb1, cb2, fcb2, bb2, cb3, fcb3, bb3, pB1, pB2}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:     3,
				BatchCount:       2,
				WireTraceparents: []string{tpb1, tpb2, tpb3},
				Batches:          []PublishBatchExpectation{{MessageCount: 0}, {MessageCount: 2}},
			},
		})
		if !resBatches.Passed() {
			t.Fatalf("Publish.Batches wildcard-before-specific expected PASS, got:\n%s", resBatches.Format())
		}
	})

	t.Run("TraceFlags==0 does NOT bypass forward/reverse link verification", func(t *testing.T) {
		spans, exp := buildCanonicalGoE2EFixture()
		// 1. TraceFlags=0 with missing publish->create AND create->publish links.
		s1 := cloneSpans(spans)
		s1[0].TraceFlags = 0
		s1[0].Links = nil
		s1[3].TraceFlags = 0
		s1[3].Links = nil
		assertHasViolation(t, Verify(goCaps, s1, exp), CodeMissingLink, "publish.batch.forward_link")

		// 2. TraceFlags=0 on publish with missing reverse create->publish link.
		s2 := cloneSpans(spans)
		s2[3].TraceFlags = 0
		s2[0].Links = nil
		assertHasViolation(t, Verify(goCaps, s2, exp), CodeBrokenBidirectionalLink, "publish.bidirectional_link")

		// 3. TraceFlags=0 on ack with missing reverse subscribe->ack link.
		s3 := cloneSpans(spans)
		s3[8].TraceFlags = 0
		s3[4].Links = nil
		assertHasViolation(t, Verify(goCaps, s3, exp), CodeBrokenBidirectionalLink, "subscribe.settlement.reverse_link")
	})

	t.Run("short Topic/Subscription IDs and cross-resource projectID inference", func(t *testing.T) {
		spans, exp := buildCanonicalGoE2EFixture()
		exp1 := *exp
		exp1.Topic = "t" // short ID, inferred projectID="p" from Subscription="projects/p/subscriptions/s"
		if res := Verify(goCaps, spans, &exp1); !res.Passed() {
			t.Fatalf("short Topic + full Subscription expected PASS, got:\n%s", res.Format())
		}
		// Short Topic alone without Subscription or ProjectID leaves fullTopic="" (non-empty check).
		exp2 := &TraceExpectation{
			Topic: "t",
			Publish: &PublishExpectation{
				MessageCount:     1,
				BatchCount:       1,
				WireTraceparents: exp.Publish.WireTraceparents,
			},
		}
		pubOnly := cloneSpans(spans[:4])
		if res := Verify(goCaps, pubOnly, exp2); !res.Passed() {
			t.Fatalf("short Topic without ProjectID expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("ExactlyOnce==nil consistency and shared EOD partial-batch SettlementExpectation merge", func(t *testing.T) {
		// 1. ALO ack error with ExactlyOnce==nil MUST catch missing "ack end".
		subALO, ccALO, scALO, prALO, _ := makeGoSubscribeMessageSpans("p", "s", "malo", "aalo", "", 5, 0, false, 1, "", "acked", 330)
		prALO.Events = append(prALO.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 330, []*proxypb.TraceSpan{subALO})
		ackALO.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackALO.StatusMessage = "unavailable"
		ackALO.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		// Remove "ack end" event to verify detection.
		subALO.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subALO, ccALO, scALO, prALO, ackALO}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         nil, // inferred from span (false)
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, ErrorSubstring: "unavailable"}},
			},
		}), CodeMissingEvent, "subscribe.event.ack_end")

		// 2. EOD ack error with ExactlyOnce==nil infers eod_failed from span attribute and PASSES.
		subEOD, ccEOD, scEOD, prEOD, _ := makeGoSubscribeMessageSpans("p", "s", "meod", "aeod", "", 5, 0, true, 1, "", "", 331)
		prEOD.Events = append(prEOD.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackEOD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 331, []*proxypb.TraceSpan{subEOD})
		subEOD.Events = append(subEOD.Events, &proxypb.SpanEvent{Name: "exception"})
		subEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subEOD.StatusMessage = "invalid ack"
		ackEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackEOD.StatusMessage = eodPartialBatchErrorMsg
		if res := Verify(goCaps, []*proxypb.TraceSpan{subEOD, ccEOD, scEOD, prEOD, ackEOD}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         nil,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, EODPartialBatchFailure: true}},
			},
		}); !res.Passed() {
			t.Fatalf("EOD error with ExactlyOnce=nil expected PASS, got:\n%s", res.Format())
		}

		// 3. Shared EOD partial-batch ack: m0 succeeded + m1 failed sharing ONE ack span.
		subOK, ccOK, scOK, prOK, _ := makeGoSubscribeMessageSpans("p", "s", "m0", "a0", "", 5, 0, true, 2, "", "acked", 332)
		subFail, ccFail, scFail, prFail, _ := makeGoSubscribeMessageSpans("p", "s", "m1", "a1", "", 5, 0, true, 2, "", "", 333)
		prOK.Events = append(prOK.Events, &proxypb.SpanEvent{Name: "ack called"})
		prFail.Events = append(prFail.Events, &proxypb.SpanEvent{Name: "ack called"})
		sharedAck := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 332, []*proxypb.TraceSpan{subOK, subFail})
		subFail.Events = append(subFail.Events, &proxypb.SpanEvent{Name: "exception"})
		subFail.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subFail.StatusMessage = "perm error"
		sharedAck.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		sharedAck.StatusMessage = eodPartialBatchErrorMsg
		resPartial := Verify(goCaps, []*proxypb.TraceSpan{subOK, ccOK, scOK, prOK, subFail, ccFail, scFail, prFail, sharedAck}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(true),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a0", MessageID: "m0", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: false}}},
					{AckID: "a1", MessageID: "m1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, EODPartialBatchFailure: true}}},
				},
			},
		})
		if !resPartial.Passed() {
			t.Fatalf("shared EOD partial-batch ack expected PASS, got:\n%s", resPartial.Format())
		}
	})

	t.Run("StrictRawSpans, scope-safe caller span, 0-link/dangling publish, sameAttrType, EOD hatch, sub duplicates, and inheritance", func(t *testing.T) {
		// 1. StrictRawSpans=true rejects otelgrpc span during Publish/Subscribe.
		spans, exp := buildCanonicalGoE2EFixture()
		spansWithGRPC := append(cloneSpans(spans), &proxypb.TraceSpan{
			TraceId:                  testTraceID(991),
			SpanId:                   testSpanID(9911),
			Name:                     "google.pubsub.v1.Publisher/Publish",
			Kind:                     proxypb.SpanKind_SPAN_KIND_CLIENT,
			InstrumentationScopeName: OTelGrpcScopeName,
		})
		strictExp := *exp
		strictExp.StrictRawSpans = true
		assertHasViolation(t, Verify(goCaps, spansWithGRPC, &strictExp), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// 2. Scope-safe isPubSubSpanOrScope: user caller span "order create" with scope "my-app" PASSES.
		callerTP := fmt.Sprintf("00-%s-%s-01", testTraceID(992), testSpanID(9920))
		pubApp := makeGoPublishBatchSpan("p", "t", 1, 340)
		cApp, fcApp, bApp, wireApp := makeGoPublishMessageSpans("p", "t", "m-app", "", 5, 340, pubApp, 1)
		cApp.TraceId = testTraceID(992)
		cApp.ParentSpanId = testSpanID(9920)
		fcApp.TraceId = cApp.TraceId
		bApp.TraceId = cApp.TraceId
		pubApp.Links[0].TraceId = cApp.TraceId
		wireApp = fmt.Sprintf("00-%s-%s-01", cApp.TraceId, cApp.SpanId)
		orderCreate := &proxypb.TraceSpan{
			TraceId:                  testTraceID(992),
			SpanId:                   testSpanID(9920),
			TraceFlags:               1,
			Name:                     "order create",
			Kind:                     proxypb.SpanKind_SPAN_KIND_SERVER,
			InstrumentationScopeName: "my-app",
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{orderCreate, cApp, fcApp, bApp, pubApp}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, ParentTraceparent: callerTP, WireTraceparents: []string{wireApp}},
		}); !res.Passed() {
			t.Fatalf("caller span \"order create\" under scope \"my-app\" expected PASS, got:\n%s", res.Format())
		}

		// 3. Extra 0-link <topic> publish span fails even when BatchCount == 0.
		pubExtra := makeGoPublishBatchSpan("p", "t", 1, 341)
		expNoBatchCount := *exp
		expNoBatchCount.Publish = &PublishExpectation{
			MessageCount:     1,
			WireTraceparents: exp.Publish.WireTraceparents,
		}
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(spans[:4]), pubExtra), &expNoBatchCount), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// 4. Dangling link on <topic> publish fails.
		spansDangle := cloneSpans(spans[:4])
		spansDangle[3].Links = append(spansDangle[3].Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolation(t, Verify(goCaps, spansDangle, &expNoBatchCount), CodeMissingLink, "publish.batch.dangling_link")

		// 5. sameAttrType: StringValue vs MatchStringList & Int64Value vs MatchInt64List & StringList vs MatchString.
		spansAttr := cloneSpans(spans[:4])
		spansAttr[0].Attributes["custom.list"] = strAV("not-a-list")
		expList := *exp
		expList.Subscribe = nil
		expList.Publish = &PublishExpectation{
			MessageCount:      1,
			BatchCount:        1,
			WireTraceparents:  exp.Publish.WireTraceparents,
			RequireAttributes: map[string]AttrMatcher{"custom.list": MatchStringList([]string{"a", "b"})},
		}
		assertHasViolation(t, Verify(goCaps, spansAttr, &expList), CodeWrongAttributeType, "publish.create.attr.custom.custom.list")

		spansAttr[0].Attributes["custom.list"] = strListAV("a", "b")
		expScalar := *exp
		expScalar.Subscribe = nil
		expScalar.Publish = &PublishExpectation{
			MessageCount:      1,
			BatchCount:        1,
			WireTraceparents:  exp.Publish.WireTraceparents,
			RequireAttributes: map[string]AttrMatcher{"custom.list": MatchString("a")},
		}
		assertHasViolation(t, Verify(goCaps, spansAttr, &expScalar), CodeWrongAttributeType, "publish.create.attr.custom.custom.list")

		// 6. verifySettlementSpan does not bypass "exception" event check when EODPartialBatchFailure == false.
		subH, ccH, scH, prH, _ := makeGoSubscribeMessageSpans("p", "s", "mh", "ah", "", 5, 0, true, 1, "", "", 342)
		prH.Events = append(prH.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackH := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 342, []*proxypb.TraceSpan{subH})
		subH.Events = append(subH.Events, &proxypb.SpanEvent{Name: "exception"})
		subH.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subH.StatusMessage = "perm"
		ackH.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackH.StatusMessage = eodPartialBatchErrorMsg // NO exception event, yet EODPartialBatchFailure == false!
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subH, ccH, scH, prH, ackH}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(true),
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, EODPartialBatchFailure: false}},
			},
		}), CodeMissingEvent, "settlement.ack.event.exception")

		// 7. Duplicate subscribe children fail even when AllowExtraPubSubSpans == true.
		sDupCC := cloneSpans(spans[4:])
		dupCC := goSpan(sDupCC[0].TraceId, testSpanID(99901), sDupCC[0].SpanId, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		assertHasViolation(t, Verify(goCaps, append(sDupCC, dupCC), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe:             exp.Subscribe,
		}), CodeUnexpectedSpan, "subscribe.concurrency_control.unique")

		// 8. expandPublishMessages OrderingKey + RequireFlowControlSpan/RequireBatchingSpan inheritance & Batches sum.
		pubSum := makeGoPublishBatchSpan("p", "t", 2, 343)
		cs1, fcs1, bs1, tps1 := makeGoPublishMessageSpans("p", "t", "mo1", "ok-shared", 4, 343, pubSum, 2)
		cs2, fcs2, bs2, tps2 := makeGoPublishMessageSpans("p", "t", "mo2", "ok-shared", 4, 344, pubSum, 2)
		if res := Verify(goCaps, []*proxypb.TraceSpan{cs1, fcs1, bs1, cs2, fcs2, bs2, pubSum}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				OrderingKey:            "ok-shared",
				RequireFlowControlSpan: BoolPtr(true),
				RequireBatchingSpan:    BoolPtr(true),
				Batches:                []PublishBatchExpectation{{MessageCount: 2}},
				Messages: []PublishMessageExpectation{
					{MessageID: "mo1", WireTraceparent: tps1},
					{MessageID: "mo2", WireTraceparent: tps2},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Publish OrderingKey/RequireFlowControlSpan/RequireBatchingSpan inheritance expected PASS, got:\n%s", res.Format())
		}
		// Batches-only expectation sums Batches[0].MessageCount = 2 without needing MessageCount=2.
		if res := Verify(goCaps, []*proxypb.TraceSpan{cs1, fcs1, bs1, cs2, fcs2, bs2, pubSum}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				OrderingKey: "ok-shared",
				Batches:     []PublishBatchExpectation{{MessageCount: 2}},
			},
		}); !res.Passed() {
			t.Fatalf("Batches-only publish sum expected PASS, got:\n%s", res.Format())
		}
		// Subscribe OrderingKey + DeliveryAttempt inheritance when Messages is populated.
		subInh, ccInh, scInh, prInh, _ := makeGoSubscribeMessageSpans("p", "s", "mi1", "ai1", "ok-sub", 5, 4, false, 1, "", "acked", 345)
		prInh.Events = append(prInh.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackInh := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 345, []*proxypb.TraceSpan{subInh})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subInh, ccInh, scInh, prInh, ackInh}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				OrderingKey:     "ok-sub",
				DeliveryAttempt: 4,
				ExactlyOnce:     BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "ai1", Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Subscribe OrderingKey/DeliveryAttempt inheritance expected PASS, got:\n%s", res.Format())
		}

		// 9. isPubSubSpanName catches extra Pub/Sub-named span with empty scope.
		extraNoScope := goSpan(testTraceID(994), testSpanID(9941), "", "other create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
		extraNoScope.InstrumentationScopeName = ""
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(spans), extraNoScope), exp), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// 10. PublishErrorAuto resolves PreValidation, Oversized, and RPC automatically on Go.
		cPreA, _, _, _ := makeGoPublishMessageSpans("p", "t", "", "", 5, 350, nil, 1)
		cPreA.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cPreA.StatusMessage = "stopped"
		cPreA.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		if res := Verify(goCaps, []*proxypb.TraceSpan{cPreA}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "stopped"},
		}); !res.Passed() {
			t.Fatalf("PublishErrorAuto PreValidation expected PASS, got:\n%s", res.Format())
		}
		cOverA, fcOverA, _, _ := makeGoPublishMessageSpans("p", "t", "", "", 11000000, 351, nil, 1)
		cOverA.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cOverA.StatusMessage = "oversized"
		cOverA.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		if res := Verify(goCaps, []*proxypb.TraceSpan{cOverA, fcOverA}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "oversized"},
		}); !res.Passed() {
			t.Fatalf("PublishErrorAuto Oversized expected PASS, got:\n%s", res.Format())
		}
		pubRPCA := makeGoPublishBatchSpan("p", "t", 1, 352)
		cRPCA, fcRPCA, bRPCA, tpRPCA := makeGoPublishMessageSpans("p", "t", "", "", 5, 352, pubRPCA, 1)
		cRPCA.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cRPCA.StatusMessage = "rpc err"
		cRPCA.Events = []*proxypb.SpanEvent{
			{Name: "publish start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "exception"},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{cRPCA, fcRPCA, bRPCA, pubRPCA}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, WireTraceparents: []string{tpRPCA}, ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "rpc err"},
		}); !res.Passed() {
			t.Fatalf("PublishErrorAuto RPC expected PASS, got:\n%s", res.Format())
		}

		// 11. Additional scalar-vs-list sameAttrType mismatches (Int64/Double/Bool vs List).
		spansTypes := cloneSpans(spans[:4])
		spansTypes[0].Attributes["il"] = intAV(1)
		spansTypes[0].Attributes["dl"] = dblAV(1.5)
		spansTypes[0].Attributes["bl"] = boolAV(true)
		expTypes := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:     1,
				BatchCount:       1,
				WireTraceparents: exp.Publish.WireTraceparents,
				RequireAttributes: map[string]AttrMatcher{
					"il": MatchInt64List([]int64{1}),
					"dl": MatchDoubleList([]float64{1.5}),
					"bl": MatchBoolList([]bool{true}),
				},
			},
		}
		resTypes := Verify(goCaps, spansTypes, expTypes)
		if resTypes.Status != StatusFail || len(resTypes.Failures) != 3 {
			t.Fatalf("expected 3 CodeWrongAttributeType failures, got:\n%s", resTypes.Format())
		}
		for _, f := range resTypes.Failures {
			if f.Code != CodeWrongAttributeType {
				t.Errorf("expected CodeWrongAttributeType, got %s (%s)", f.Code, f.Message)
			}
		}
	})
}

func TestSolveBipartiteMatching_EdgeCasesAndCapacities(t *testing.T) {
	if got := solveBipartiteMatching(0, 2, nil, nil, nil); len(got) != 0 {
		t.Fatalf("numLeft=0 got %v, want []", got)
	}
	if got := solveBipartiteMatching(2, 0, nil, nil, []bipartiteEdge{{0, 0, 10}}); got[0] != -1 || got[1] != -1 {
		t.Fatalf("numRight=0 got %v, want [-1 -1]", got)
	}
	if got := solveBipartiteMatching(2, 2, []int{0, 0}, nil, []bipartiteEdge{{0, 0, 10}, {1, 1, 10}}); got[0] != -1 || got[1] != -1 {
		t.Fatalf("zero rightCap got %v, want [-1 -1]", got)
	}
	// Duplicate (left, right) edges keep max weight; out-of-bounds edges ignored; negative weights clamped to 0.
	got := solveBipartiteMatching(2, 2, nil, nil, []bipartiteEdge{
		{left: -1, right: 0, weight: 999},
		{left: 0, right: 9, weight: 999},
		{left: 0, right: 0, weight: -5},
		{left: 0, right: 0, weight: 50},
		{left: 0, right: 1, weight: 10},
		{left: 1, right: 0, weight: 100},
	})
	if got[0] != 1 || got[1] != 0 {
		t.Fatalf("expected [1 0] (max weight 110), got %v", got)
	}

	// leftGroup prevents two items from the same group sharing a capacity-2 right node,
	// while allowing distinct groups to share that capacity-2 right node.
	grp := []int{0, 0, 1}
	caps := []int{2, 1}
	edges := []bipartiteEdge{
		{left: 0, right: 0, weight: 100},
		{left: 1, right: 0, weight: 100},
		{left: 1, right: 1, weight: 50},
		{left: 2, right: 0, weight: 100},
	}
	grpAssign := solveBipartiteMatching(3, 2, caps, grp, edges)
	if grpAssign[0] != 0 || grpAssign[1] != 1 || grpAssign[2] != 0 {
		t.Fatalf("expected [0 1 0] with leftGroup+rightCap, got %v", grpAssign)
	}
}

func TestIter3Regressions_Counterexamples(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("Attack1B: dropped -00 ack linking to NACKED sampled sub0 fails (unidir and bidir)", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(4101), testSpanID(41010))
		sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m0", "a0", "", 5, 0, false, 1, "", "nacked", 4101)
		pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "nack called"})
		nack0 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 4101, []*proxypb.TraceSpan{sub0})

		// Unidirectional s ack -> sub0 (missing reverse link on sub0).
		ackUnidir := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 4102, nil)
		ackUnidir.Links = []*proxypb.SpanLink{{TraceId: sub0.TraceId, SpanId: sub0.SpanId}}

		exp := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a0", MessageID: "m0", Settlements: []SettlementExpectation{{Operation: "nack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, nack0, ackUnidir}, exp), CodeBrokenBidirectionalLink, "subscribe.settlement.reverse_link")

		// Even if a buggy proxy forged a bidirectional reverse link sub0 -> s ack on NACKED sub0, MUST STILL FAIL!
		subBidir := cloneSpans([]*proxypb.TraceSpan{sub0})[0]
		ackBidir := cloneSpans([]*proxypb.TraceSpan{ackUnidir})[0]
		subBidir.Links = append(subBidir.Links, &proxypb.SpanLink{
			TraceId:    ackBidir.TraceId,
			SpanId:     ackBidir.SpanId,
			Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("ack")},
		})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subBidir, cc0, sc0, pr0, nack0, ackBidir}, exp), CodeMissingSpan, "subscribe.unsampled_settlement.present")
	})

	t.Run("Attack1C and Attack1D: multi-modack specificity for dropped -00 and sampled subscribe", func(t *testing.T) {
		unsampTP1 := fmt.Sprintf("00-%s-%s-00", testTraceID(4103), testSpanID(41030))
		unsampTP2 := fmt.Sprintf("00-%s-%s-00", testTraceID(4104), testSpanID(41040))

		// Attack1C: mod60 at 0ms, mod10 at 1ms.
		mod60 := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 4103, nil)
		mod60.StartTime = timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, 0, time.UTC))
		mod10 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 4104, nil)
		mod10.StartTime = timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, int(time.Millisecond), time.UTC))

		if res := Verify(goCaps, []*proxypb.TraceSpan{mod60, mod10}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{
					{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "modack"}}},
					{WireTraceparent: unsampTP2, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(false)}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Attack1C expected PASS, got:\n%s", res.Format())
		}

		// Attack1D: 1 sampled message expecting [{modack}, {modack, 60, false}, {ack}].
		sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "m1d", "a1d", "", 5, 0, false, 1, "", "acked", 4105)
		pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
		modS60 := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 4105, []*proxypb.TraceSpan{sub})
		modS60.StartTime = timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, 0, time.UTC))
		modS10 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 4106, []*proxypb.TraceSpan{sub})
		modS10.StartTime = timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, int(time.Millisecond), time.UTC))
		ackS := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 4107, []*proxypb.TraceSpan{sub})

		if res := Verify(goCaps, []*proxypb.TraceSpan{modS60, modS10, ackS, sub, cc, sc, pr}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{
					{Operation: "modack"},
					{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(false)},
					{Operation: "ack"},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Attack1D expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Attack2E, Attack2F, Attack2G and ALO unlinked slot exhaustion", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(4201), testSpanID(42010))
		sampTP := fmt.Sprintf("00-%s-%s-01", testTraceID(4202), testSpanID(42020))

		// Attack2E: mixed -01 + -00 with unlinked non-receipt modack batch_count=2 -> FAIL; batch_count=1 -> PASS.
		sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "m2e", "a2e", "", 5, 0, false, 2, sampTP, "acked", 4201)
		pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 4201, []*proxypb.TraceSpan{sub})
		modBad2 := makeGoSettlementSpan("p", "s", "modack", 2, 60, false, 4202, nil)
		mixExp := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a2e", MessageID: "m2e", WireTraceparent: sampTP, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modBad2}, mixExp), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		modOK1 := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 4203, nil)
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modOK1}, mixExp); !res.Passed() {
			t.Fatalf("Attack2E batch_count=1 expected PASS, got:\n%s", res.Format())
		}

		// Attack2F: Go EOD unlinked receipt modack fails for both ExactlyOnce=true and ExactlyOnce=nil.
		subEOD, ccEOD, scEOD, prEOD, _ := makeGoSubscribeMessageSpans("p", "s", "m2f", "a2f", "", 5, 0, true, 1, "", "acked", 4204)
		prEOD.Events = append(prEOD.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackEOD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 4204, []*proxypb.TraceSpan{subEOD})
		unlinkedReceipt := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 4205, nil)
		for _, eosPtr := range []*bool{BoolPtr(true), nil} {
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subEOD, ccEOD, scEOD, prEOD, ackEOD, unlinkedReceipt}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					MessageCount:        1,
					ExactlyOnce:         eosPtr,
					ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
				},
			}), CodeMissingLink, "subscribe.modack.missing_link")
		}

		// Extra unlinked Go ALO receipt modack slot exhaustion (2 unlinked receipt modacks for 1 ALO msg) -> FAIL even with AllowExtraPubSubSpans=true!
		subALO, ccALO, scALO, prALO, _ := makeGoSubscribeMessageSpans("p", "s", "malo", "aalo", "", 5, 0, false, 1, "", "acked", 4206)
		prALO.Events = append(prALO.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 4206, []*proxypb.TraceSpan{subALO})
		rec1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 4207, nil)
		rec2 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 4208, nil)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subALO, ccALO, scALO, prALO, ackALO, rec1, rec2}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}), CodeMissingLink, "subscribe.modack.missing_link")

		// Attack2G: Phase 4 auto-modack links to dropped -00 trace (both with [sub, tp00] and with [tp00] alone under AllowExtraPubSubSpans=true).
		modBadLinkMixed := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 4209, []*proxypb.TraceSpan{sub})
		modBadLinkMixed.Links = append(modBadLinkMixed.Links, &proxypb.SpanLink{TraceId: testTraceID(4201), SpanId: testSpanID(99999)})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modBadLinkMixed}, mixExp), CodeBrokenTraceparent, "subscribe.unsampled_settlement.forbidden_link")

		modOnlyBadLink := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 4210, nil)
		modOnlyBadLink.Links = []*proxypb.SpanLink{{TraceId: testTraceID(4201), SpanId: testSpanID(99999)}}
		mixAllowExtra := *mixExp
		mixAllowExtra.AllowExtraPubSubSpans = true
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modOnlyBadLink}, &mixAllowExtra), CodeBrokenTraceparent, "subscribe.unsampled_settlement.forbidden_link")
	})

	t.Run("Attack3A, Attack3B, Attack3C and Attack4G: lossless merge, dangling & duplicate links", func(t *testing.T) {
		// Attack3A: lossless mergeSettlementExpectations fails conflicting deadline, receipt flag, and batch_count.
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m3a1", "a3a1", "", 5, 0, false, 2, "", "acked", 4301)
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m3a2", "a3a2", "", 5, 0, false, 2, "", "acked", 4302)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		modShared := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 4301, []*proxypb.TraceSpan{sub1, sub2})
		ackShared := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 4302, []*proxypb.TraceSpan{sub1, sub2})
		res3A := Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, sub2, cc2, sc2, pr2, modShared, ackShared}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a3a1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true), ExpectedBatchCount: 2}, {Operation: "ack"}}},
					{AckID: "a3a2", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 30, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1}, {Operation: "ack"}}},
				},
			},
		})
		assertHasViolation(t, res3A, CodeWrongAttributeValue, "settlement.modack.attr.ack_deadline")
		assertHasViolation(t, res3A, CodeWrongAttributeValue, "settlement.modack.attr.is_receipt_modack")
		assertHasViolation(t, res3A, CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		// Attack3B: dangling link on s ack, s nack, and explicit s modack.
		ackDangle := cloneSpans([]*proxypb.TraceSpan{ackShared})[0]
		ackDangle.Links = []*proxypb.SpanLink{{TraceId: sub1.TraceId, SpanId: sub1.SpanId}, {TraceId: testTraceID(99999), SpanId: testSpanID(99999)}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, ackDangle}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExactlyOnce: BoolPtr(false), StrictSettlementOps: true, ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}), CodeMissingLink, "subscribe.settlement.dangling_link")

		subN, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "m3bn", "a3bn", "", 5, 0, false, 1, "", "nacked", 4303)
		prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
		nackDangle := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 4303, []*proxypb.TraceSpan{subN})
		nackDangle.Links = append(nackDangle.Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subN, ccN, scN, prN, nackDangle}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExactlyOnce: BoolPtr(false), ExpectedOutcome: "nacked", StrictSettlementOps: true, ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeMissingLink, "subscribe.settlement.dangling_link")

		modDangle := makeGoSettlementSpan("p", "s", "modack", 1, 30, false, 4304, []*proxypb.TraceSpan{subN})
		modDangle.Links = append(modDangle.Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subN, ccN, scN, prN, modDangle}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExactlyOnce: BoolPtr(false), ExpectedOutcome: "nacked", StrictSettlementOps: true, ExpectedSettlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 30}}},
		}), CodeMissingLink, "subscribe.settlement.dangling_link")

		// Attack3C: dangling link on mixed -01 + -00 publish AND dangling reverse link on t create.
		unsampPubTP := fmt.Sprintf("00-%s-%s-00", testTraceID(4310), testSpanID(43100))
		pubMix := makeGoPublishBatchSpan("p", "t", 2, 4311)
		c0, fc0, b0, tp0 := makeGoPublishMessageSpans("p", "t", "mp0", "", 5, 4311, pubMix, 2)
		pubMixDangle := cloneSpans([]*proxypb.TraceSpan{pubMix})[0]
		pubMixDangle.Links = append(pubMixDangle.Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{c0, fc0, b0, pubMixDangle}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{
					{MessageID: "mp0", WireTraceparent: tp0},
					{ParentTraceparent: unsampPubTP, ExpectDroppedUnsampled: true},
				},
			},
		}), CodeMissingLink, "publish.batch.dangling_link")

		c0DangleRev := cloneSpans([]*proxypb.TraceSpan{c0})[0]
		c0DangleRev.Links = append(c0DangleRev.Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999), Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("publish")}})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{c0DangleRev, fc0, b0, pubMix}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{
					{MessageID: "mp0", WireTraceparent: tp0},
					{ParentTraceparent: unsampPubTP, ExpectDroppedUnsampled: true},
				},
			},
		}), CodeMissingLink, "publish.create.dangling_link")

		// Attack4G: duplicate links on s ack, t publish, t create, and s subscribe.
		ackDup := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 4320, []*proxypb.TraceSpan{sub1})
		ackDup.Links = append(ackDup.Links, &proxypb.SpanLink{TraceId: sub1.TraceId, SpanId: sub1.SpanId})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, ackDup}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExactlyOnce: BoolPtr(false), StrictSettlementOps: true, ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}), CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_link")

		pubDup := makeGoPublishBatchSpan("p", "t", 1, 4321)
		cDup, fcDup, bDup, tpDup := makeGoPublishMessageSpans("p", "t", "mdup", "", 5, 4321, pubDup, 1)
		pubDup.Links = append(pubDup.Links, &proxypb.SpanLink{TraceId: cDup.TraceId, SpanId: cDup.SpanId})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{cDup, fcDup, bDup, pubDup}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, WireTraceparents: []string{tpDup}},
		}), CodeBrokenBidirectionalLink, "publish.batch.duplicate_link")

		cDupRev := cloneSpans([]*proxypb.TraceSpan{cDup})[0]
		pubClean := makeGoPublishBatchSpan("p", "t", 1, 4322)
		cDupRev.Links = []*proxypb.SpanLink{
			{TraceId: pubClean.TraceId, SpanId: pubClean.SpanId, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("publish")}},
			{TraceId: pubClean.TraceId, SpanId: pubClean.SpanId, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("publish")}},
		}
		pubClean.Links = []*proxypb.SpanLink{{TraceId: cDupRev.TraceId, SpanId: cDupRev.SpanId}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{cDupRev, fcDup, bDup, pubClean}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, WireTraceparents: []string{tpDup}},
		}), CodeBrokenBidirectionalLink, "publish.create.duplicate_reverse_link")

		subDupRev, ccDR, scDR, prDR, _ := makeGoSubscribeMessageSpans("p", "s", "mdr", "adr", "", 5, 0, false, 1, "", "acked", 4323)
		prDR.Events = append(prDR.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackDR := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 4323, []*proxypb.TraceSpan{subDupRev})
		subDupRev.Links = append(subDupRev.Links, proto.Clone(subDupRev.Links[0]).(*proxypb.SpanLink))
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subDupRev, ccDR, scDR, prDR, ackDR}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExactlyOnce: BoolPtr(false), StrictSettlementOps: true, ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}), CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_reverse_link")
	})

	t.Run("Attack4A, Attack4B, Attack4C, Attack4D, Attack4E and Attack4F: global bipartite matching & fresh-root leaks", func(t *testing.T) {
		// Attack4A: Stage 2a PublishErrorRPC + Stage 2b PublishErrorAuto(WireTraceparent) with c1 earlier than c0.
		pub4A := makeGoPublishBatchSpan("p", "t", 2, 4401)
		c1, fc1, b1, tp1 := makeGoPublishMessageSpans("p", "t", "", "", 5, 4401, pub4A, 2)
		c0, fc0, b0, _ := makeGoPublishMessageSpans("p", "t", "", "", 5, 4402, pub4A, 2)
		for _, c := range []*proxypb.TraceSpan{c1, c0} {
			c.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			c.StatusMessage = "rpc failed"
			c.Events = []*proxypb.SpanEvent{
				{Name: "publish start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
				{Name: "exception"},
			}
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{c1, fc1, b1, c0, fc0, b0, pub4A}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{ExpectError: true, ErrorKind: PublishErrorRPC},
					{ExpectError: true, ErrorKind: PublishErrorAuto, WireTraceparent: tp1},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Attack4A expected PASS, got:\n%s", res.Format())
		}

		// Attack4B: mixed Go PublishErrorAuto ("fc limit" + "rpc failed") in both orders, Stop PreValidation + FC, and missing FC error.
		fcErr := goSpan(testTraceID(4410), testSpanID(44102), testSpanID(44101), "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)
		fcErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		fcErr.StatusMessage = "fc limit exceeded"
		fcErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		pubRPC := makeGoPublishBatchSpan("p", "t", 1, 4411)
		cRPC, fcRPC, bRPC, _ := makeGoPublishMessageSpans("p", "t", "", "", 5, 4411, pubRPC, 1)
		cRPC.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cRPC.StatusMessage = "rpc failed"
		cRPC.Events = []*proxypb.SpanEvent{
			{Name: "publish start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "exception"},
		}
		for _, msgs4B := range [][]PublishMessageExpectation{
			{{ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "fc limit"}, {ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "rpc failed"}},
			{{ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "rpc failed"}, {ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "fc limit"}},
		} {
			if res := Verify(goCaps, []*proxypb.TraceSpan{fcErr, cRPC, fcRPC, bRPC, pubRPC}, &TraceExpectation{
				Topic:   "projects/p/topics/t",
				Publish: &PublishExpectation{BatchCount: 1, Messages: msgs4B},
			}); !res.Passed() {
				t.Fatalf("Attack4B expected PASS, got:\n%s", res.Format())
			}
		}
		cPre, _, _, _ := makeGoPublishMessageSpans("p", "t", "", "", 5, 4412, nil, 1)
		cPre.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cPre.StatusMessage = "publisher stopped"
		cPre.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		if res := Verify(goCaps, []*proxypb.TraceSpan{fcErr, cPre}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{Messages: []PublishMessageExpectation{
				{ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "fc limit"},
				{ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "stopped"},
			}},
		}); !res.Passed() {
			t.Fatalf("Attack4B FC + PreValidation expected PASS, got:\n%s", res.Format())
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{cRPC, fcRPC, bRPC, pubRPC}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{BatchCount: 1, Messages: []PublishMessageExpectation{
				{ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "fc limit"},
				{ExpectError: true, ErrorKind: PublishErrorAuto, ErrorSubstring: "rpc failed"},
			}},
		}); res.Status != StatusFail {
			t.Fatal("Attack4B missing fcErr expected FAIL, got PASS")
		}

		// Attack4C: Publish orthogonal specificity:
		// (1) Go {OrderingKey, BodySize} vs {OrderingKey, ParentTraceparent}
		// (2) Go {BodySize} vs {ParentTraceparent}
		// (3) ProfileCpp {OrderingKey} vs {ParentTraceparent} (omitting OrderingKey on m1)
		parentTP := fmt.Sprintf("00-%s-%s-01", testTraceID(4420), testSpanID(44200))
		pub4C := makeGoPublishBatchSpan("p", "t", 2, 4421)
		cWithParent, fcP, bP, _ := makeGoPublishMessageSpans("p", "t", "m4c1", "k1", 5, 4421, pub4C, 2)
		cWithParent.TraceId = testTraceID(4420)
		cWithParent.ParentSpanId = testSpanID(44200)
		fcP.TraceId = cWithParent.TraceId
		bP.TraceId = cWithParent.TraceId
		pub4C.Links[0].TraceId = cWithParent.TraceId
		cRoot, fcR, bR, _ := makeGoPublishMessageSpans("p", "t", "m4c2", "k1", 5, 4422, pub4C, 2)
		if res := Verify(goCaps, []*proxypb.TraceSpan{cWithParent, fcP, bP, cRoot, fcR, bR, pub4C}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{OrderingKey: "k1", BodySize: Int64Ptr(5)},
					{OrderingKey: "k1", ParentTraceparent: parentTP},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Attack4C Go OrderingKey+BodySize vs ParentTraceparent expected PASS, got:\n%s", res.Format())
		}

		pub4C2 := makeGoPublishBatchSpan("p", "t", 2, 4423)
		cWithParentEmpty, fcP2, bP2, _ := makeGoPublishMessageSpans("p", "t", "m4c3", "", 5, 4423, pub4C2, 2)
		cWithParentEmpty.TraceId = testTraceID(4420)
		cWithParentEmpty.ParentSpanId = testSpanID(44200)
		fcP2.TraceId = cWithParentEmpty.TraceId
		bP2.TraceId = cWithParentEmpty.TraceId
		pub4C2.Links[0].TraceId = cWithParentEmpty.TraceId
		cRootEmpty, fcR2, bR2, _ := makeGoPublishMessageSpans("p", "t", "m4c4", "", 5, 4424, pub4C2, 2)
		if res := Verify(goCaps, []*proxypb.TraceSpan{cWithParentEmpty, fcP2, bP2, cRootEmpty, fcR2, bR2, pub4C2}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{BodySize: Int64Ptr(5)},
					{ParentTraceparent: parentTP},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Attack4C Go BodySize vs ParentTraceparent expected PASS, got:\n%s", res.Format())
		}

		cppCapsLocal := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
		cppPub4C := &proxypb.TraceSpan{
			TraceId: testTraceID(4425), SpanId: testSpanID(44250), TraceFlags: 1, Name: "t publish",
			Kind: proxypb.SpanKind_SPAN_KIND_CLIENT, StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
			InstrumentationScopeName: CppScopeName,
			Attributes: map[string]*proxypb.AttributeValue{
				"gcp.project_id": strAV("p"), "messaging.system": strAV("gcp_pubsub"), "messaging.destination.name": strAV("t"),
				"messaging.batch.message_count": intAV(2), "messaging.operation.type": strAV("publish"),
				"code.function.name": strAV("BatchSink::AsyncPublish"), "gl-cpp.status_code": strAV("OK"),
			},
		}
		makeCpp4CCreate := func(tid, sid, pid string, seq int) (*proxypb.TraceSpan, *proxypb.TraceSpan, *proxypb.TraceSpan) {
			cs := &proxypb.TraceSpan{
				TraceId: tid, SpanId: sid, ParentSpanId: pid, TraceFlags: 1, Name: "t create",
				Kind: proxypb.SpanKind_SPAN_KIND_PRODUCER, StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
				StartTime:                timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, seq*int(time.Millisecond), time.UTC)),
				InstrumentationScopeName: CppScopeName,
				Attributes: map[string]*proxypb.AttributeValue{
					"gcp.project_id": strAV("p"), "messaging.system": strAV("gcp_pubsub"), "messaging.destination.name": strAV("t"),
					"code.function.name": strAV("pubsub::PublisherConnection::Publish"), "messaging.operation.type": strAV("create"),
					"messaging.message.envelope.size": intAV(30), "messaging.gcp_pubsub.message.ordering_key": strAV("k1"),
					"messaging.message.id": strAV(fmt.Sprintf("mc-%d", seq)), "gl-cpp.status_code": strAV("OK"),
				},
				Events: []*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}, {Name: "gl-cpp.publish_start"}, {Name: "gl-cpp.publish_end"}},
				Links:  []*proxypb.SpanLink{{TraceId: cppPub4C.TraceId, SpanId: cppPub4C.SpanId}},
			}
			cppPub4C.Links = append(cppPub4C.Links, &proxypb.SpanLink{TraceId: tid, SpanId: sid})
			fc := &proxypb.TraceSpan{
				TraceId: tid, SpanId: testSpanID(44260 + seq*10), ParentSpanId: sid, TraceFlags: 1, Name: "publisher flow control",
				Kind: proxypb.SpanKind_SPAN_KIND_CLIENT, StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
				InstrumentationScopeName: CppScopeName,
				Attributes:               map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish")},
			}
			bc := &proxypb.TraceSpan{
				TraceId: tid, SpanId: testSpanID(44261 + seq*10), ParentSpanId: sid, TraceFlags: 1, Name: "publisher batching",
				Kind: proxypb.SpanKind_SPAN_KIND_CLIENT, StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
				InstrumentationScopeName: CppScopeName,
				Attributes:               map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish")},
			}
			return cs, fc, bc
		}
		ccP, fcCP, bcCP := makeCpp4CCreate(testTraceID(4420), testSpanID(44251), testSpanID(44200), 1)
		ccR, fcCR, bcCR := makeCpp4CCreate(testTraceID(4426), testSpanID(44252), "", 2)
		if res := Verify(cppCapsLocal, []*proxypb.TraceSpan{ccP, fcCP, bcCP, ccR, fcCR, bcCR, cppPub4C}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{OrderingKey: "k1"},
					{ParentTraceparent: parentTP},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Attack4C Cpp OrderingKey vs ParentTraceparent expected PASS, got:\n%s", res.Format())
		}

		// Attack4D: Subscribe orthogonal specificity ({MessageID} vs {WireTraceparent} & {MessageID} vs {CallbackTraceparent}).
		wireTP1 := fmt.Sprintf("00-%s-%s-01", testTraceID(4430), testSpanID(44300))
		subWithParents, ccWP, scWP, prWP, cbTP1 := makeGoSubscribeMessageSpans("p", "s", "m-dup", "a-1", "", 5, 0, false, 2, wireTP1, "acked", 4431)
		subRoot, ccSR, scSR, prSR, _ := makeGoSubscribeMessageSpans("p", "s", "m-dup", "a-2", "", 5, 0, false, 2, "", "acked", 4432)
		prWP.Events = append(prWP.Events, &proxypb.SpanEvent{Name: "ack called"})
		prSR.Events = append(prSR.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack4D := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 4431, []*proxypb.TraceSpan{subWithParents, subRoot})
		for _, msgs4D := range [][]SubscribeMessageExpectation{
			{{MessageID: "m-dup", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}}, {WireTraceparent: wireTP1, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}}},
			{{MessageID: "m-dup", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}}, {CallbackTraceparent: cbTP1, WireTraceparent: wireTP1, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}}},
		} {
			if res := Verify(goCaps, []*proxypb.TraceSpan{subWithParents, ccWP, scWP, prWP, subRoot, ccSR, scSR, prSR, ack4D}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs4D},
			}); !res.Passed() {
				t.Fatalf("Attack4D expected PASS, got:\n%s", res.Format())
			}
		}

		// Attack4E: Publish -00 fresh-root create leak under AllowExtraPubSubSpans=true fails; mixed -01+-00 valid passes.
		unsamp4E := fmt.Sprintf("00-%s-%s-00", testTraceID(4440), testSpanID(44400))
		pub4E := makeGoPublishBatchSpan("p", "t", 1, 4441)
		cLeak, fcLeak, bLeak, _ := makeGoPublishMessageSpans("p", "t", "m-leak", "", 5, 4441, pub4E, 1)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{cLeak, fcLeak, bLeak, pub4E}, &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{ParentTraceparent: unsamp4E, ExpectDroppedUnsampled: true}},
			},
		}), CodeUnexpectedSpan, "publish.unsampled_dropped")

		pubMix4E := makeGoPublishBatchSpan("p", "t", 2, 4442)
		cSamp, fcSamp, bSamp, tpSamp := makeGoPublishMessageSpans("p", "t", "m-samp", "", 5, 4442, pubMix4E, 2)
		if res := Verify(goCaps, []*proxypb.TraceSpan{cSamp, fcSamp, bSamp, pubMix4E}, &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{
					{MessageID: "m-samp", WireTraceparent: tpSamp},
					{ParentTraceparent: unsamp4E, ExpectDroppedUnsampled: true},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Attack4E mixed -01+-00 with AllowExtraPubSubSpans=true expected PASS, got:\n%s", res.Format())
		}

		// Attack4F: 3 dropped -00 messages matching ack2 (batch_count=2) and ack1 (batch_count=1).
		ack2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 4451, nil)
		ack2.StartTime = timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, 0, time.UTC))
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 4452, nil)
		ack1.StartTime = timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, int(time.Millisecond), time.UTC))
		if res := Verify(goCaps, []*proxypb.TraceSpan{ack2, ack1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{WireTraceparent: fmt.Sprintf("00-%s-%s-00", testTraceID(4451), testSpanID(44510)), ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: fmt.Sprintf("00-%s-%s-00", testTraceID(4452), testSpanID(44520)), ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 2}}},
					{WireTraceparent: fmt.Sprintf("00-%s-%s-00", testTraceID(4453), testSpanID(44530)), ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 2}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Attack4F expected PASS, got:\n%s", res.Format())
		}
	})
}

func TestIter3Regressions_UnsampledCrossProjectAndFallbacks(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("Reviewer2 Major 3: live-modeled -00 Publish with BatchCount=0 and bogus 0-link detection", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(4501), testSpanID(45010))
		pub1 := makeGoPublishBatchSpan("p", "t", 1, 4501)

		// (a) Single -00 with BatchCount=0 -> PASS.
		expSingle := &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{Messages: []PublishMessageExpectation{{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true}}},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{pub1}, expSingle); !res.Passed() {
			t.Fatalf("single -00 Publish BatchCount=0 expected PASS, got:\n%s", res.Format())
		}
		// (b) Missing t publish -> FAIL.
		assertHasViolation(t, Verify(goCaps, nil, expSingle), CodeMissingSpan, "publish.batch.present")
		// (c) batch_count=2 for 1 -00 msg -> FAIL.
		pub2 := makeGoPublishBatchSpan("p", "t", 2, 4502)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{pub2}, expSingle), CodeWrongAttributeValue, "publish.batch.attr.message_count")
		// (d) 2 root t publish spans for 1 -00 msg -> FAIL.
		pub1b := makeGoPublishBatchSpan("p", "t", 1, 4503)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{pub1, pub1b}, expSingle), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// Mixed -01 + -00 with BatchCount=0: shared batch_count=2 -> PASS; bogus extra 0-link -> FAIL; batch_count=1 -> FAIL.
		pubMix := makeGoPublishBatchSpan("p", "t", 2, 4504)
		cSamp, fcSamp, bSamp, tpSamp := makeGoPublishMessageSpans("p", "t", "m-mix", "", 5, 4504, pubMix, 2)
		expMix := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{
					{MessageID: "m-mix", WireTraceparent: tpSamp},
					{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true},
				},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{cSamp, fcSamp, bSamp, pubMix}, expMix); !res.Passed() {
			t.Fatalf("mixed -01+-00 BatchCount=0 expected PASS, got:\n%s", res.Format())
		}
		bogus99 := makeGoPublishBatchSpan("p", "t", 99, 4505)
		expMixAllow := *expMix
		expMixAllow.AllowExtraPubSubSpans = true
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{cSamp, fcSamp, bSamp, pubMix, bogus99}, &expMixAllow), CodeWrongAttributeValue, "publish.batch.attr.message_count")

		pubShort := makeGoPublishBatchSpan("p", "t", 1, 4506)
		cShort, fcShort, bShort, tpShort := makeGoPublishMessageSpans("p", "t", "m-short", "", 5, 4506, pubShort, 1)
		expMixShort := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{
					{MessageID: "m-short", WireTraceparent: tpShort},
					{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true},
				},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{cShort, fcShort, bShort, pubShort}, expMixShort), CodeMissingSpan, "publish.unsampled_batch_slot")
	})

	t.Run("Cross-project Topic+Subscription, Fallback ID safety, Auto-modack Gap 4, and Mixed Subtree Leak", func(t *testing.T) {
		// Cross-project PASS and FAIL.
		pub := makeGoPublishBatchSpan("proj-pub", "t", 1, 4601)
		c, fc, b, wireTP := makeGoPublishMessageSpans("proj-pub", "t", "mx", "", 5, 4601, pub, 1)
		sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("proj-sub", "s", "mx", "ax", "", 5, 0, false, 1, wireTP, "acked", 4601)
		pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack := makeGoSettlementSpan("proj-sub", "s", "ack", 1, 0, false, 4601, []*proxypb.TraceSpan{sub})
		if res := Verify(goCaps, []*proxypb.TraceSpan{c, fc, b, pub, sub, cc, sc, pr, ack}, &TraceExpectation{
			Topic:        "projects/proj-pub/topics/t",
			Subscription: "projects/proj-sub/subscriptions/s",
			Publish:      &PublishExpectation{MessageCount: 1, BatchCount: 1, WireTraceparents: []string{wireTP}},
			Subscribe:    &SubscribeExpectation{MessageCount: 1, WireTraceparents: []string{wireTP}, ExactlyOnce: BoolPtr(false), ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}); !res.Passed() {
			t.Fatalf("cross-project expected PASS, got:\n%s", res.Format())
		}

		subWrongProj, ccW, scW, prW, _ := makeGoSubscribeMessageSpans("proj-pub", "s", "mx", "ax", "", 5, 0, false, 1, wireTP, "acked", 4602)
		prW.Events = append(prW.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackW := makeGoSettlementSpan("proj-pub", "s", "ack", 1, 0, false, 4602, []*proxypb.TraceSpan{subWrongProj})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{c, fc, b, pub, subWrongProj, ccW, scW, prW, ackW}, &TraceExpectation{
			Topic:        "projects/proj-pub/topics/t",
			Subscription: "projects/proj-sub/subscriptions/s",
			Publish:      &PublishExpectation{MessageCount: 1, BatchCount: 1, WireTraceparents: []string{wireTP}},
			Subscribe:    &SubscribeExpectation{MessageCount: 1, WireTraceparents: []string{wireTP}, ExactlyOnce: BoolPtr(false), ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}), CodeWrongAttributeValue, "subscribe.attr.gcp_project_id")

		// Fallback ID safety (Challenger 1 Gap 2): app spans with matching suffixes under "my-app" are ignored!
		appCreate := goSpan(testTraceID(4610), testSpanID(46100), "", "order create", proxypb.SpanKind_SPAN_KIND_SERVER, 0)
		appCreate.InstrumentationScopeName = "my-app"
		if res := Verify(goCaps, []*proxypb.TraceSpan{appCreate, c, fc, b, pub}, &TraceExpectation{
			Topic:   "",
			Publish: &PublishExpectation{MessageCount: 1, BatchCount: 1, WireTraceparents: []string{wireTP}},
		}); !res.Passed() {
			t.Fatalf("fallback create with app span expected PASS, got:\n%s", res.Format())
		}

		appPub := goSpan(testTraceID(4611), testSpanID(46110), "", "fake publish", proxypb.SpanKind_SPAN_KIND_SERVER, 0)
		appPub.InstrumentationScopeName = "my-app"
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(4612), testSpanID(46120))
		pubDropOnly := makeGoPublishBatchSpan("p", "t", 1, 4612)
		if res := Verify(goCaps, []*proxypb.TraceSpan{appPub, pubDropOnly}, &TraceExpectation{
			Topic:   "",
			Publish: &PublishExpectation{Messages: []PublishMessageExpectation{{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true}}},
		}); !res.Passed() {
			t.Fatalf("fallback -00 publish with app span expected PASS, got:\n%s", res.Format())
		}

		appSub := goSpan(testTraceID(4613), testSpanID(46130), "", "user subscribe", proxypb.SpanKind_SPAN_KIND_SERVER, 0)
		appSub.InstrumentationScopeName = "my-app"
		if res := Verify(goCaps, []*proxypb.TraceSpan{appSub, sub, cc, sc, pr, ack}, &TraceExpectation{
			Subscription: "",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, WireTraceparents: []string{wireTP}, ExactlyOnce: BoolPtr(false), ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}); !res.Passed() {
			t.Fatalf("fallback subscribe with app span expected PASS, got:\n%s", res.Format())
		}

		appAck := goSpan(testTraceID(4614), testSpanID(46140), "", "user ack", proxypb.SpanKind_SPAN_KIND_SERVER, 0)
		appAck.InstrumentationScopeName = "my-app"
		ackDrop := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 4614, nil)
		modDrop := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 4615, nil)
		if res := Verify(goCaps, []*proxypb.TraceSpan{appAck, ackDrop, modDrop}, &TraceExpectation{
			Subscription: "",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, WireTraceparents: []string{unsampTP}, ExpectDroppedUnsampled: true, ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}); !res.Passed() {
			t.Fatalf("fallback -00 ack with app span expected PASS, got:\n%s", res.Format())
		}

		// Auto-modack dangling link (Challenger 1 Gap 4).
		subG4, ccG4, scG4, prG4, _ := makeGoSubscribeMessageSpans("p", "s", "mg4", "ag4", "", 5, 0, false, 1, "", "acked", 4620)
		prG4.Events = append(prG4.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackG4 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 4620, []*proxypb.TraceSpan{subG4})
		modG4 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 4621, []*proxypb.TraceSpan{subG4})
		modG4.Links = append(modG4.Links, &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subG4, ccG4, scG4, prG4, ackG4, modG4}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExactlyOnce: BoolPtr(false), ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}}},
		}), CodeMissingLink, "subscribe.settlement.dangling_link")

		// Mixed -01 + -00 Subscribe subtree leak (exercises isDroppedSubscribeSubtreeSpan directly).
		subMix, ccMix, scMix, prMix, _ := makeGoSubscribeMessageSpans("p", "s", "mg4", "ag4", "", 5, 0, false, 1, "", "acked", 4630)
		prMix.Events = append(prMix.Events, &proxypb.SpanEvent{Name: "ack called"})
		appInDrop := goSpan(testTraceID(4612), testSpanID(46300), testSpanID(46120), "my helper", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)
		appInDrop.InstrumentationScopeName = "my-app"
		ackMix := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 4630, []*proxypb.TraceSpan{subMix})
		expMixSub := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "ag4", MessageID: "mg4", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{subMix, ccMix, scMix, prMix, ackMix, appInDrop}, expMixSub); !res.Passed() {
			t.Fatalf("mixed -01+-00 with benign app span in dropped trace expected PASS, got:\n%s", res.Format())
		}
		leakedProcMix := goSpan(testTraceID(4612), testSpanID(46301), testSpanID(46120), "s process", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subMix, ccMix, scMix, prMix, ackMix, leakedProcMix}, expMixSub), CodeUnexpectedSpan, "subscribe.unsampled_dropped")
	})
}

func TestIter3Regressions_60SeedPermutations(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	// Build complex Go E2E graph: mixed sampled + unsampled publish, mixed sampled + unsampled subscribe,
	// redelivery without AckID, multi-modack, and batched settlements.
	callerTP := fmt.Sprintf("00-%s-%s-01", testTraceID(4700), testSpanID(47000))
	unsampPubTP := fmt.Sprintf("00-%s-%s-00", testTraceID(4701), testSpanID(47010))
	pub1 := makeGoPublishBatchSpan("p", "t", 2, 4701)
	pub2 := makeGoPublishBatchSpan("p", "t", 1, 4702)
	c1, fc1, b1, tp1 := makeGoPublishMessageSpans("p", "t", "mp1", "k1", 5, 4701, pub1, 2)
	c1.TraceId = testTraceID(4700)
	c1.ParentSpanId = testSpanID(47000)
	fc1.TraceId = c1.TraceId
	b1.TraceId = c1.TraceId
	pub1.Links[0].TraceId = c1.TraceId
	tp1 = fmt.Sprintf("00-%s-%s-01", c1.TraceId, c1.SpanId)
	c2, fc2, b2, tp2 := makeGoPublishMessageSpans("p", "t", "mp2", "k1", 5, 4702, pub2, 1)

	unsampSubTP := fmt.Sprintf("00-%s-%s-00", testTraceID(4710), testSpanID(47100))
	sub1, cc1, sc1, pr1, cb1 := makeGoSubscribeMessageSpans("p", "s", "m-dup", "a-1", "", 5, 0, false, 2, tp1, "acked", 4711)
	sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-dup", "a-2", "", 5, 0, false, 2, "", "acked", 4712)
	pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
	pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
	mod60 := makeGoSettlementSpan("p", "s", "modack", 2, 60, false, 4711, []*proxypb.TraceSpan{sub1, sub2})
	mod10 := makeGoSettlementSpan("p", "s", "modack", 3, 10, true, 4712, []*proxypb.TraceSpan{sub1, sub2})
	ackAll := makeGoSettlementSpan("p", "s", "ack", 3, 0, false, 4713, []*proxypb.TraceSpan{sub1, sub2})

	goBaseSpans := []*proxypb.TraceSpan{
		c1, fc1, b1, c2, fc2, b2, pub1, pub2,
		sub1, cc1, sc1, pr1, sub2, cc2, sc2, pr2,
		mod60, mod10, ackAll,
	}
	goExp := &TraceExpectation{
		Topic:        "projects/p/topics/t",
		Subscription: "projects/p/subscriptions/s",
		Publish: &PublishExpectation{
			Batches: []PublishBatchExpectation{{MessageCount: 1}, {MessageCount: 2}},
			Messages: []PublishMessageExpectation{
				{OrderingKey: "k1", BodySize: Int64Ptr(5)},
				{OrderingKey: "k1", ParentTraceparent: callerTP, WireTraceparent: tp1},
				{ParentTraceparent: unsampPubTP, ExpectDroppedUnsampled: true},
			},
		},
		Subscribe: &SubscribeExpectation{
			ExactlyOnce:         BoolPtr(false),
			StrictSettlementOps: true,
			Messages: []SubscribeMessageExpectation{
				{MessageID: "m-dup", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(false)}, {Operation: "ack"}}},
				{MessageID: "m-dup", CallbackTraceparent: cb1, WireTraceparent: tp1, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 10}, {Operation: "modack", AckDeadlineSeconds: 60}, {Operation: "ack"}}},
				{WireTraceparent: unsampSubTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(true)}, {Operation: "ack", ExpectedBatchCount: 3}}},
			},
		},
	}
	_ = tp2

	var goBaseline string
	for seed := int64(0); seed < 60; seed++ {
		perm := make([]*proxypb.TraceSpan, len(goBaseSpans))
		copy(perm, goBaseSpans)
		rng := rand.New(rand.NewSource(seed))
		rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
		res := Verify(goCaps, perm, goExp)
		if !res.Passed() {
			t.Fatalf("seed=%d Go permutation expected PASS, got:\n%s", seed, res.Format())
		}
		formatted := res.Format()
		if seed == 0 {
			goBaseline = formatted
		} else if formatted != goBaseline {
			t.Fatalf("seed=%d Go permutation report differed from seed=0:\nwant:\n%s\ngot:\n%s", seed, goBaseline, formatted)
		}
	}

	// 60-seed C++ UnaryPull + redelivery permutation test.
	c1TID, c1SID := testTraceID(4801), testSpanID(48010)
	c2TID, c2SID := testTraceID(4802), testSpanID(48020)
	r1 := &proxypb.TraceSpan{
		TraceId: testTraceID(4811), SpanId: testSpanID(48110), TraceFlags: 1, Name: "s receive",
		Kind: proxypb.SpanKind_SPAN_KIND_CONSUMER, StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
		StartTime:                timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, 0, time.UTC)),
		InstrumentationScopeName: CppScopeName,
		Attributes: map[string]*proxypb.AttributeValue{
			"gcp.project_id": strAV("p"), "messaging.system": strAV("gcp_pubsub"), "messaging.destination.name": strAV("s"),
			"messaging.operation.type": strAV("receive"), "code.function.name": strAV("pubsub::SubscriberConnection::Pull"),
			"messaging.message.id": strAV("m-dup"), "messaging.message.envelope.size": intAV(30),
		},
		Links: []*proxypb.SpanLink{{TraceId: c1TID, SpanId: c1SID}},
	}
	r2 := &proxypb.TraceSpan{
		TraceId: testTraceID(4812), SpanId: testSpanID(48120), TraceFlags: 1, Name: "s receive",
		Kind: proxypb.SpanKind_SPAN_KIND_CONSUMER, StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
		StartTime:                timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, int(time.Millisecond), time.UTC)),
		InstrumentationScopeName: CppScopeName,
		Attributes: map[string]*proxypb.AttributeValue{
			"gcp.project_id": strAV("p"), "messaging.system": strAV("gcp_pubsub"), "messaging.destination.name": strAV("s"),
			"messaging.operation.type": strAV("receive"), "code.function.name": strAV("pubsub::SubscriberConnection::Pull"),
			"messaging.message.id": strAV("m-dup"), "messaging.message.envelope.size": intAV(30),
		},
		Links: []*proxypb.SpanLink{{TraceId: c2TID, SpanId: c2SID}},
	}
	a1 := &proxypb.TraceSpan{
		TraceId: testTraceID(4821), SpanId: testSpanID(48210), TraceFlags: 1, Name: "s ack",
		Kind: proxypb.SpanKind_SPAN_KIND_CLIENT, StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
		StartTime:                timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, int(2*time.Millisecond), time.UTC)),
		InstrumentationScopeName: CppScopeName,
		Attributes: map[string]*proxypb.AttributeValue{
			"gcp.project_id": strAV("p"), "messaging.system": strAV("gcp_pubsub"), "messaging.destination.name": strAV("s"),
			"messaging.operation.type": strAV("ack"), "code.function.name": strAV("pubsub::PullAckHandler::ack"),
			"messaging.gcp_pubsub.message.ack_id": strAV("ack-1"),
		},
		Links: []*proxypb.SpanLink{{TraceId: r1.TraceId, SpanId: r1.SpanId}},
	}
	a2 := &proxypb.TraceSpan{
		TraceId: testTraceID(4822), SpanId: testSpanID(48220), TraceFlags: 1, Name: "s ack",
		Kind: proxypb.SpanKind_SPAN_KIND_CLIENT, StatusCode: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
		StartTime:                timestamppb.New(time.Date(2026, 11, 15, 18, 0, 0, int(3*time.Millisecond), time.UTC)),
		InstrumentationScopeName: CppScopeName,
		Attributes: map[string]*proxypb.AttributeValue{
			"gcp.project_id": strAV("p"), "messaging.system": strAV("gcp_pubsub"), "messaging.destination.name": strAV("s"),
			"messaging.operation.type": strAV("ack"), "code.function.name": strAV("pubsub::PullAckHandler::ack"),
			"messaging.gcp_pubsub.message.ack_id": strAV("ack-2"),
		},
		Links: []*proxypb.SpanLink{{TraceId: r2.TraceId, SpanId: r2.SpanId}},
	}
	cppBase := []*proxypb.TraceSpan{r1, r2, a1, a2}
	cppExp := &TraceExpectation{
		Subscription: "projects/p/subscriptions/s",
		UnaryPull: &UnaryPullExpectation{
			Messages: []UnaryPullMessageExpectation{
				{MessageID: "m-dup", Settlement: &SettlementExpectation{Operation: "ack"}},
				{MessageID: "m-dup", WireTraceparent: fmt.Sprintf("00-%s-%s-01", c1TID, c1SID), AckID: "ack-1", Settlement: &SettlementExpectation{Operation: "ack"}},
			},
		},
	}
	var cppBaseline string
	for seed := int64(0); seed < 60; seed++ {
		perm := make([]*proxypb.TraceSpan, len(cppBase))
		copy(perm, cppBase)
		rng := rand.New(rand.NewSource(seed))
		rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
		res := Verify(cppCaps, perm, cppExp)
		if !res.Passed() {
			t.Fatalf("seed=%d C++ permutation expected PASS, got:\n%s", seed, res.Format())
		}
		formatted := res.Format()
		if seed == 0 {
			cppBaseline = formatted
		} else if formatted != cppBaseline {
			t.Fatalf("seed=%d C++ permutation report differed from seed=0:\nwant:\n%s\ngot:\n%s", seed, cppBaseline, formatted)
		}
	}
}

func TestIter4Regressions_RemediationsAndCoverage(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("Probe A / Attack6E: linked mixed receipt modack unlinked-slot accounting", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(5100), testSpanID(51000))
		sampTP := fmt.Sprintf("00-%s-%s-01", testTraceID(5101), testSpanID(51010))

		for _, eod := range []bool{false, true} {
			mode := "ALO"
			if eod {
				mode = "EOD"
			}
			seq := 5110
			if eod {
				seq = 5120
			}
			sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "m-"+mode, "a-"+mode, "", 5, 0, eod, 2, sampTP, "acked", seq)
			pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
			ack := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, seq, []*proxypb.TraceSpan{sub})
			modLinked := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, seq+1, []*proxypb.TraceSpan{sub})

			exp := &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce:         BoolPtr(eod),
					StrictSettlementOps: false,
					Messages: []SubscribeMessageExpectation{
						{AckID: "a-" + mode, MessageID: "m-" + mode, WireTraceparent: sampTP, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					},
				},
			}

			// Valid coalesced mixed -01 + -00 receipt modack (batch_count=2, links=[sub]) -> PASS.
			if res := Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modLinked}, exp); !res.Passed() {
				t.Fatalf("%s valid coalesced mixed receipt modack expected PASS, got:\n%s", mode, res.Format())
			}

			// Subcase 1 (linked_mixed_plus_bogus_unlinked): extra spurious unlinked receipt modack under AllowExtraPubSubSpans=false -> FAIL.
			modBogus := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, seq+2, nil)
			resExtra := Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modLinked, modBogus}, exp)
			assertHasViolation(t, resExtra, CodeMissingLink, "subscribe.modack.missing_link")
			assertHasViolation(t, resExtra, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

			// Subcase 2 (linked_mixed_oversized_batch_count_5): inflated batch_count=5 -> FAIL.
			modOversized := makeGoSettlementSpan("p", "s", "modack", 5, 10, true, seq+3, []*proxypb.TraceSpan{sub})
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modOversized}, exp), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
		}

		// Coalesced partial fast-ack ALO batch (2 ALO msgs, 1 linked receipt modack with batch_count=2, no dropped msgs).
		subA0, ccA0, scA0, prA0, _ := makeGoSubscribeMessageSpans("p", "s", "ma0", "aa0", "", 5, 0, false, 2, "", "acked", 5130)
		subA1, ccA1, scA1, prA1, _ := makeGoSubscribeMessageSpans("p", "s", "ma1", "aa1", "", 5, 0, false, 2, "", "acked", 5131)
		prA0.Events = append(prA0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prA1.Events = append(prA1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALO := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 5130, []*proxypb.TraceSpan{subA0, subA1})
		modALOLinked := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 5131, []*proxypb.TraceSpan{subA0})
		expALO := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{
					{AckID: "aa0", MessageID: "ma0", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "aa1", MessageID: "ma1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{subA0, ccA0, scA0, prA0, subA1, ccA1, scA1, prA1, ackALO, modALOLinked}, expALO); !res.Passed() {
			t.Fatalf("ALO partial fast-ack linked batch_count=2 expected PASS, got:\n%s", res.Format())
		}
		modALOBogus := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 5132, nil)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subA0, ccA0, scA0, prA0, subA1, ccA1, scA1, prA1, ackALO, modALOLinked, modALOBogus}, expALO), CodeMissingLink, "subscribe.modack.missing_link")

		// Linked periodic non-receipt modack exceeding len(uniqueMatched)+len(droppedMsgs) -> FAIL.
		modLinkedPerBad := makeGoSettlementSpan("p", "s", "modack", 3, 60, false, 5133, []*proxypb.TraceSpan{subA0})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subA0, ccA0, scA0, prA0, subA1, ccA1, scA1, prA1, ackALO, modLinkedPerBad}, expALO), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
	})

	t.Run("Attack6F: Phase 3 matched dropped modack capacity enforcement", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(5200), testSpanID(52000))
		sampTP := fmt.Sprintf("00-%s-%s-01", testTraceID(5201), testSpanID(52010))

		sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "m6f", "a6f", "", 5, 0, true, 1, sampTP, "acked", 5210)
		pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 5210, []*proxypb.TraceSpan{sub})
		modEOD := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 5211, []*proxypb.TraceSpan{sub})

		exp6F := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{
					{AckID: "a6f", MessageID: "m6f", WireTraceparent: sampTP, BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}}},
				},
			},
		}

		// Subcase 1 (oversized_periodic_non_receipt): is_receipt_modack=false, batch_count=2 > len(droppedMsgs)=1 -> FAIL.
		modPerBad := makeGoSettlementSpan("p", "s", "modack", 2, 60, false, 5212, nil)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modEOD, modPerBad}, exp6F), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		// Subcase 2 (oversized_unlinked_receipt): is_receipt_modack=true, batch_count=5 > remainingMixedReceiptSlots=1 -> FAIL.
		modRecBad := makeGoSettlementSpan("p", "s", "modack", 5, 10, true, 5213, nil)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modEOD, modRecBad}, exp6F), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		// Valid batch_count=1 unlinked receipt modack -> PASS.
		modRecOK := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 5214, nil)
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modEOD, modRecOK}, exp6F); !res.Passed() {
			t.Fatalf("Attack6F valid batch_count=1 unlinked receipt modack expected PASS, got:\n%s", res.Format())
		}

		// Valid batch_count=1 unlinked periodic modack -> PASS.
		modPerOK := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 5215, nil)
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modEOD, modPerOK}, exp6F); !res.Passed() {
			t.Fatalf("Attack6F valid batch_count=1 unlinked periodic modack expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Probe B (Go): Phase 5 forward-link sampledSubAllowsSettlementOp check", func(t *testing.T) {
		for _, strict := range []bool{true, false} {
			sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "mpb0", "apb0", "", 5, 0, false, 1, "", "acked", 5310)
			sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "mpb1", "apb1", "", 5, 0, false, 1, "", "nacked", 5311)
			pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "ack called"})
			pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "nack called"})
			// Buggy s ack links BOTH acked sub0 and nacked sub1, with forged reverse link on sub1.
			ack := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 5310, []*proxypb.TraceSpan{sub0, sub1})
			nack := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 5311, []*proxypb.TraceSpan{sub1})

			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, ack, nack}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce:         BoolPtr(false),
					StrictSettlementOps: strict,
					Messages: []SubscribeMessageExpectation{
						{AckID: "apb0", MessageID: "mpb0", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{AckID: "apb1", MessageID: "mpb1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "nack"}}},
					},
				},
			}), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
		}

		// Expired message with ack linked -> FAIL via sampledSubAllowsSettlementOp("expired").
		subA, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "mpba", "apba", "", 5, 0, false, 1, "", "acked", 5320)
		subExp, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "mpbe", "apbe", "", 5, 0, false, 1, "", "expired", 5321)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackBadExp := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 5320, []*proxypb.TraceSpan{subA, subExp})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subA, ccA, scA, prA, subExp, ccE, scE, prE, ackBadExp}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "apba", MessageID: "mpba", BatchCount: 1, ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "apbe", MessageID: "mpbe", BatchCount: 1, ExpectedOutcome: "expired"},
				},
			},
		}), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
	})

	t.Run("Probe C: len(sampledMsgs)==0 with Subscription==\"\" and AllowExtraPubSubSpans==true catches leaked process span", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(5400), testSpanID(54000))
		leakedProc := goSpan(testTraceID(5401), testSpanID(54010), testSpanID(54000), "s process", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{leakedProc}, &TraceExpectation{
			Subscription:          "",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true}},
			},
		}), CodeUnexpectedSpan, "subscribe.unsampled_dropped")

		// Benign my-app span with " process" suffix must still be ignored when dropped message expired.
		appProc := goSpan(testTraceID(5402), testSpanID(54020), "", "order process", proxypb.SpanKind_SPAN_KIND_SERVER, 0)
		appProc.InstrumentationScopeName = "my-app"
		if res := Verify(goCaps, []*proxypb.TraceSpan{appProc}, &TraceExpectation{
			Subscription:          "",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "expired"}},
			},
		}); !res.Passed() {
			t.Fatalf("Probe C benign app process span expected PASS, got:\n%s", res.Format())
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{appProc}, &TraceExpectation{
			Subscription:          "",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true}},
			},
		}), CodeMissingSpan, "subscribe.settlement.missing_span")
	})

	t.Run("Probe H / Attack6G: asymmetric reverse link from subscribe to 0-link settlement span", func(t *testing.T) {
		sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "mph", "aph", "", 5, 0, false, 1, "", "acked", 5500)
		pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 5500, []*proxypb.TraceSpan{sub})
		modUnlinked := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 5501, nil)
		// Buggy proxy adds reverse link sub -> modUnlinked, while modUnlinked has 0 forward links.
		sub.Links = append(sub.Links, &proxypb.SpanLink{
			TraceId:    modUnlinked.TraceId,
			SpanId:     modUnlinked.SpanId,
			Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("modack")},
		})

		exp := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, modUnlinked}, exp), CodeBrokenBidirectionalLink, "subscribe.settlement.forward_link")

		// Duplicate asymmetric reverse links emit both forward_link and duplicate_reverse_link.
		subDup := cloneSpans([]*proxypb.TraceSpan{sub})[0]
		subDup.Links = append(subDup.Links, proto.Clone(subDup.Links[len(subDup.Links)-1]).(*proxypb.SpanLink))
		resDup := Verify(goCaps, []*proxypb.TraceSpan{subDup, cc, sc, pr, ack, modUnlinked}, exp)
		assertHasViolation(t, resDup, CodeBrokenBidirectionalLink, "subscribe.settlement.forward_link")
		assertHasViolation(t, resDup, CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_reverse_link")
	})

	t.Run("sampledSubAllowsSettlementOp covers all outcomes and strictness modes", func(t *testing.T) {
		cases := []struct {
			name   string
			msg    SubscribeMessageExpectation
			op     string
			strict bool
			want   bool
		}{
			{"explicit ack allowed", SubscribeMessageExpectation{Settlements: []SettlementExpectation{{Operation: "ack"}}}, "ack", true, true},
			{"explicit ack rejects nack strict", SubscribeMessageExpectation{Settlements: []SettlementExpectation{{Operation: "ack"}}}, "nack", true, false},
			{"explicit ack rejects nack non-strict", SubscribeMessageExpectation{Settlements: []SettlementExpectation{{Operation: "ack"}}}, "nack", false, false},
			{"strict blocks implicit modack", SubscribeMessageExpectation{ExpectedOutcome: "acked"}, "modack", true, false},
			{"non-strict allows implicit modack", SubscribeMessageExpectation{Settlements: []SettlementExpectation{{Operation: "ack"}}}, "modack", false, true},
			{"outcome empty allows ack", SubscribeMessageExpectation{ExpectedOutcome: ""}, "ack", false, true},
			{"outcome acked allows ack", SubscribeMessageExpectation{ExpectedOutcome: "acked"}, "ack", false, true},
			{"outcome acked blocks nack", SubscribeMessageExpectation{ExpectedOutcome: "acked"}, "nack", false, false},
			{"outcome nacked allows nack", SubscribeMessageExpectation{ExpectedOutcome: "nacked"}, "nack", false, true},
			{"outcome nacked blocks ack", SubscribeMessageExpectation{ExpectedOutcome: "nacked"}, "ack", false, false},
			{"outcome eod_failed allows ack", SubscribeMessageExpectation{ExpectedOutcome: "eod_failed"}, "ack", false, true},
			{"outcome eod_failed allows nack", SubscribeMessageExpectation{ExpectedOutcome: "eod_failed"}, "nack", false, true},
			{"outcome expired blocks ack", SubscribeMessageExpectation{ExpectedOutcome: "expired"}, "ack", false, false},
			{"outcome expired blocks nack", SubscribeMessageExpectation{ExpectedOutcome: "expired"}, "nack", false, false},
			{"outcome unknown blocks ack", SubscribeMessageExpectation{ExpectedOutcome: "other"}, "ack", false, false},
		}
		for _, tc := range cases {
			if got := sampledSubAllowsSettlementOp(tc.msg, tc.op, tc.strict); got != tc.want {
				t.Errorf("%s: sampledSubAllowsSettlementOp(%+v, %q, %v) = %v, want %v", tc.name, tc.msg, tc.op, tc.strict, got, tc.want)
			}
		}
	})
}

func TestIter5Regressions_DuplicateReceiptAndBatchUpperBounds(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	t.Run("Attack8A_10: duplicate linked is_receipt_modack=true spans targeting same subscribe span", func(t *testing.T) {
		// Attack8A_10a: 1 ALO message, 2 linked receipt modacks targeting sub0.
		subALO, ccALO, scALO, prALO, _ := makeGoSubscribeMessageSpans("p", "s", "m-8a10a", "a-8a10a", "", 5, 0, false, 1, "", "acked", 6001)
		prALO.Events = append(prALO.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 6001, []*proxypb.TraceSpan{subALO})
		modALO1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6002, []*proxypb.TraceSpan{subALO})
		modALO2 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6003, []*proxypb.TraceSpan{subALO})

		expALO := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}
		res8A10a := Verify(goCaps, []*proxypb.TraceSpan{subALO, ccALO, scALO, prALO, ackALO, modALO1, modALO2}, expALO)
		assertHasViolation(t, res8A10a, CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")
		assertHasViolation(t, res8A10a, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// Even with AllowExtraPubSubSpans=true and no reverse link from subALO to modALO2, duplicate_receipt MUST fail.
		subNoRev, ccNR, scNR, prNR, _ := makeGoSubscribeMessageSpans("p", "s", "m-nr", "a-nr", "", 5, 0, false, 1, "", "acked", 6004)
		prNR.Events = append(prNR.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackNR := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 6004, []*proxypb.TraceSpan{subNoRev})
		modNR1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6005, []*proxypb.TraceSpan{subNoRev})
		modNR2FwdOnly := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6006, nil)
		modNR2FwdOnly.Links = []*proxypb.SpanLink{{TraceId: subNoRev.TraceId, SpanId: subNoRev.SpanId}}
		expALOAllow := *expALO
		expALOAllow.AllowExtraPubSubSpans = true
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subNoRev, ccNR, scNR, prNR, ackNR, modNR1, modNR2FwdOnly}, &expALOAllow), CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")

		// Attack8A_10d: 1 EOD message, 2 linked receipt modacks targeting sub0.
		subEOD, ccEOD, scEOD, prEOD, _ := makeGoSubscribeMessageSpans("p", "s", "m-8a10d", "a-8a10d", "", 5, 0, true, 1, "", "acked", 6010)
		prEOD.Events = append(prEOD.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackEOD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 6010, []*proxypb.TraceSpan{subEOD})
		modEOD1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6011, []*proxypb.TraceSpan{subEOD})
		modEOD2 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6012, []*proxypb.TraceSpan{subEOD})
		expEOD := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: false,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}
		res8A10d := Verify(goCaps, []*proxypb.TraceSpan{subEOD, ccEOD, scEOD, prEOD, ackEOD, modEOD1, modEOD2}, expEOD)
		assertHasViolation(t, res8A10d, CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")
		assertHasViolation(t, res8A10d, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// Attack8A_10e: Mixed EOD sub0 + dropped -00 d0: mod1(bc=2)->[sub0] + bogus mod2(bc=1)->[sub0] (both timestamp orders).
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(6020), testSpanID(60200))
		sampTP := fmt.Sprintf("00-%s-%s-01", testTraceID(6021), testSpanID(60210))
		for _, bogusFirst := range []bool{false, true} {
			seq1, seq2 := 6021, 6022
			if bogusFirst {
				seq1, seq2 = 6023, 6020
			}
			subMix, ccMix, scMix, prMix, _ := makeGoSubscribeMessageSpans("p", "s", "m-8a10e", "a-8a10e", "", 5, 0, true, 2, sampTP, "acked", 6025)
			prMix.Events = append(prMix.Events, &proxypb.SpanEvent{Name: "ack called"})
			ackMix := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 6025, []*proxypb.TraceSpan{subMix})
			modCoalesced := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, seq1, []*proxypb.TraceSpan{subMix})
			modBogusDup := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, seq2, nil)
			modBogusDup.Links = []*proxypb.SpanLink{{TraceId: subMix.TraceId, SpanId: subMix.SpanId}}

			expMix := &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce:         BoolPtr(true),
					StrictSettlementOps: false,
					Messages: []SubscribeMessageExpectation{
						{AckID: "a-8a10e", MessageID: "m-8a10e", WireTraceparent: sampTP, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					},
				},
			}
			resMix := Verify(goCaps, []*proxypb.TraceSpan{subMix, ccMix, scMix, prMix, ackMix, modCoalesced, modBogusDup}, expMix)
			assertHasViolation(t, resMix, CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")
			assertHasViolation(t, resMix, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

			expMixAllow := *expMix
			expMixAllow.AllowExtraPubSubSpans = true
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subMix, ccMix, scMix, prMix, ackMix, modCoalesced, modBogusDup}, &expMixAllow), CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")
		}

		// Attack8A_10c: 2 ALO messages (sub0, sub1): mod1->[sub0](1) + mod2->[sub0](1) + unlinked fast-ack mod3->[](1) = 3 receipt slots.
		subC0, ccC0, scC0, prC0, _ := makeGoSubscribeMessageSpans("p", "s", "mc0", "ac0", "", 5, 0, false, 2, "", "acked", 6030)
		subC1, ccC1, scC1, prC1, _ := makeGoSubscribeMessageSpans("p", "s", "mc1", "ac1", "", 5, 0, false, 2, "", "acked", 6031)
		prC0.Events = append(prC0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prC1.Events = append(prC1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackC := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 6030, []*proxypb.TraceSpan{subC0, subC1})
		modC1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6031, []*proxypb.TraceSpan{subC0})
		modC2 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6032, []*proxypb.TraceSpan{subC0})
		modC3Unlinked := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6033, nil)
		exp8A10c := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{
					{AckID: "ac0", MessageID: "mc0", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "ac1", MessageID: "mc1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}
		res8A10c := Verify(goCaps, []*proxypb.TraceSpan{subC0, ccC0, scC0, prC0, subC1, ccC1, scC1, prC1, ackC, modC1, modC2, modC3Unlinked}, exp8A10c)
		assertHasViolation(t, res8A10c, CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")
		assertHasViolation(t, res8A10c, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// Contrasts & edge cases:
		// 1. Valid 1 linked receipt + 1 linked periodic non-receipt on same sub0 -> PASS.
		subMixRecPer, ccMRP, scMRP, prMRP, _ := makeGoSubscribeMessageSpans("p", "s", "mmrp", "amrp", "", 5, 0, false, 1, "", "acked", 6040)
		prMRP.Events = append(prMRP.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackMRP := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 6040, []*proxypb.TraceSpan{subMixRecPer})
		modRecMRP := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6041, []*proxypb.TraceSpan{subMixRecPer})
		modPerMRP := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 6042, []*proxypb.TraceSpan{subMixRecPer})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subMixRecPer, ccMRP, scMRP, prMRP, ackMRP, modRecMRP, modPerMRP}, expALO); !res.Passed() {
			t.Fatalf("valid linked receipt + linked periodic modack on same sub expected PASS, got:\n%s", res.Format())
		}

		// 2. Phase 2/3-matched priority over bogus earlier unrequested linked receipt -> flags bogus span with both duplicate_receipt and no_extra_pubsub_spans.
		subP2, ccP2, scP2, prP2, _ := makeGoSubscribeMessageSpans("p", "s", "mp2", "ap2", "", 5, 0, false, 1, "", "acked", 6043)
		prP2.Events = append(prP2.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackP2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 6043, []*proxypb.TraceSpan{subP2})
		modBogusEarly := makeGoSettlementSpan("p", "s", "modack", 1, 20, true, 6044, nil)
		modBogusEarly.Links = []*proxypb.SpanLink{{TraceId: subP2.TraceId, SpanId: subP2.SpanId}}
		modValidLate := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6045, []*proxypb.TraceSpan{subP2})
		resP2 := Verify(goCaps, []*proxypb.TraceSpan{subP2, ccP2, scP2, prP2, ackP2, modBogusEarly, modValidLate}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				ExpectedSettlements: []SettlementExpectation{
					{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(true)},
					{Operation: "ack"},
				},
			},
		})
		assertHasViolation(t, resP2, CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")
		assertHasViolation(t, resP2, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// 3. Self-duplicate link [sub0, sub0] inside a single linked receipt modack preserves duplicate_link without false duplicate_receipt.
		subSelfDup, ccSD, scSD, prSD, _ := makeGoSubscribeMessageSpans("p", "s", "msd", "asd", "", 5, 0, false, 1, "", "acked", 6046)
		prSD.Events = append(prSD.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackSD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 6046, []*proxypb.TraceSpan{subSelfDup})
		modSelfDup := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6047, []*proxypb.TraceSpan{subSelfDup})
		modSelfDup.Links = append(modSelfDup.Links, &proxypb.SpanLink{TraceId: subSelfDup.TraceId, SpanId: subSelfDup.SpanId})
		resSD := Verify(goCaps, []*proxypb.TraceSpan{subSelfDup, ccSD, scSD, prSD, ackSD, modSelfDup}, expALO)
		assertHasViolation(t, resSD, CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_link")
		for _, f := range resSD.Failures {
			if f.Rule == "subscribe.modack.duplicate_receipt" {
				t.Fatalf("single modack with self-duplicate link must not emit subscribe.modack.duplicate_receipt, got:\n%s", resSD.Format())
			}
		}

		// 4. Linked modack omitting is_receipt_modack (!hasRec) preserves CodeMissingAttribute settlement.modack.attr.is_receipt_modack.
		subNoAttr, ccNA, scNA, prNA, _ := makeGoSubscribeMessageSpans("p", "s", "mna", "ana", "", 5, 0, false, 1, "", "acked", 6048)
		prNA.Events = append(prNA.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackNA := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 6048, []*proxypb.TraceSpan{subNoAttr})
		modNoAttr := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6049, []*proxypb.TraceSpan{subNoAttr})
		delete(modNoAttr.Attributes, "messaging.gcp_pubsub.is_receipt_modack")
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subNoAttr, ccNA, scNA, prNA, ackNA, modNoAttr}, expALO), CodeMissingAttribute, "settlement.modack.attr.is_receipt_modack")

		// 5. Overlapping duplicate mod1->[sub0], mod2->[sub0,sub1], mod3->[sub1]: rejects mod2 only without false-blaming mod3.
		subO0, ccO0, scO0, prO0, _ := makeGoSubscribeMessageSpans("p", "s", "mo0", "ao0", "", 5, 0, false, 2, "", "acked", 6050)
		subO1, ccO1, scO1, prO1, _ := makeGoSubscribeMessageSpans("p", "s", "mo1", "ao1", "", 5, 0, false, 2, "", "acked", 6051)
		prO0.Events = append(prO0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prO1.Events = append(prO1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackO := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 6050, []*proxypb.TraceSpan{subO0, subO1})
		modO1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6051, []*proxypb.TraceSpan{subO0})
		modO2 := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 6052, nil)
		modO2.Links = []*proxypb.SpanLink{
			{TraceId: subO0.TraceId, SpanId: subO0.SpanId},
			{TraceId: subO1.TraceId, SpanId: subO1.SpanId},
		}
		modO3 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6053, []*proxypb.TraceSpan{subO1})
		resOverlap := Verify(goCaps, []*proxypb.TraceSpan{subO0, ccO0, scO0, prO0, subO1, ccO1, scO1, prO1, ackO, modO1, modO2, modO3}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{
					{AckID: "ao0", MessageID: "mo0", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "ao1", MessageID: "mo1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		})
		dupViolCount := 0
		for _, f := range resOverlap.Failures {
			if f.Rule == "subscribe.modack.duplicate_receipt" {
				dupViolCount++
				if f.SpanID != modO2.SpanId {
					t.Fatalf("expected duplicate_receipt only on modO2 (%s), got on %s", modO2.SpanId, f.SpanID)
				}
			}
		}
		if dupViolCount != 1 {
			t.Fatalf("expected exactly 1 duplicate_receipt violation on overlapping modO2, got %d:\n%s", dupViolCount, resOverlap.Format())
		}

		// 6. ProfileCpp allows multiple linked periodic modack spans targeting the same sub0.
		subCpp := cppSpan(testTraceID(6060), testSpanID(60600), "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		subCpp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		subCpp.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                          strAV("p"),
			"messaging.system":                                        strAV("gcp_pubsub"),
			"messaging.destination.name":                              strAV("s"),
			"messaging.operation.type":                                strAV("subscribe"),
			"messaging.message.id":                                    strAV("cm-dup-ok"),
			"messaging.message.envelope.size":                         intAV(25),
			"messaging.gcp_pubsub.message.ack_id":                     strAV("ca-dup-ok"),
			"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(true),
			"messaging.gcp_pubsub.result":                             strAV("ack"),
		}
		subCpp.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.ack_end"}, {Name: "gl-cpp.modack_start"}, {Name: "gl-cpp.modack_end"}}
		ccCpp := cppSpan(testTraceID(6060), testSpanID(60601), subCpp.SpanId, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		ccCpp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		ccCpp.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		scCpp := cppSpan(testTraceID(6060), testSpanID(60602), subCpp.SpanId, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		scCpp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		scCpp.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		prCpp := cppSpan(testTraceID(6060), testSpanID(60603), subCpp.SpanId, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		prCpp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		prCpp.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		ackCpp := cppSpan(testTraceID(6061), testSpanID(60610), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 5*time.Millisecond)
		ackCpp.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                             strAV("p"),
			"messaging.system":                           strAV("gcp_pubsub"),
			"messaging.destination.name":                 strAV("s"),
			"code.function.name":                         strAV("pubsub::AckHandler::ack"),
			"messaging.gcp_pubsub.message.ack_id":        strAV("ca-dup-ok"),
			"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
			"messaging.operation.type":                   strAV("settle"),
			"gl-cpp.status_code":                         strAV("OK"),
		}
		ackCpp.Links = []*proxypb.SpanLink{{TraceId: subCpp.TraceId, SpanId: subCpp.SpanId}}
		makeCppMod := func(seq int) *proxypb.TraceSpan {
			m := cppSpan(testTraceID(6070+seq), testSpanID(60700+seq), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(seq)*time.Millisecond)
			m.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			m.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                    strAV("p"),
				"messaging.system":                                  strAV("gcp_pubsub"),
				"messaging.destination.name":                        strAV("s"),
				"messaging.operation.type":                          strAV("extend"),
				"messaging.batch.message_count":                     intAV(1),
				"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(30),
			}
			m.Links = []*proxypb.SpanLink{{TraceId: subCpp.TraceId, SpanId: subCpp.SpanId}}
			return m
		}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{subCpp, ccCpp, scCpp, prCpp, ackCpp, makeCppMod(1), makeCppMod(2)}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: false,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}); !res.Passed() {
			t.Fatalf("ProfileCpp multiple linked modacks on same sub expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Attack8A_11: s ack and s nack batch_count upper bound under !AllowExtraPubSubSpans", func(t *testing.T) {
		// Attack8A_11: 1 sampled ALO msg, s ack batch_count=99 (ExpectedBatchCount=0, AllowExtraPubSubSpans=false) -> FAIL.
		subAck, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-8a11", "a-8a11", "", 5, 0, false, 1, "", "acked", 6101)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack99 := makeGoSettlementSpan("p", "s", "ack", 99, 0, false, 6101, []*proxypb.TraceSpan{subAck})
		expAck := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subAck, ccA, scA, prA, ack99}, expAck), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// Contrast: AllowExtraPubSubSpans=true with ExpectedBatchCount=0 permits batch_count=99.
		expAckAllow := *expAck
		expAckAllow.AllowExtraPubSubSpans = true
		if res := Verify(goCaps, []*proxypb.TraceSpan{subAck, ccA, scA, prA, ack99}, &expAckAllow); !res.Passed() {
			t.Fatalf("s ack batch_count=99 with AllowExtraPubSubSpans=true expected PASS, got:\n%s", res.Format())
		}

		// Attack8A_11b: 1 sampled ALO msg, s nack batch_count=99 (ExpectedBatchCount=0, AllowExtraPubSubSpans=false) -> FAIL.
		subNack, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "m-8a11b", "a-8a11b", "", 5, 0, false, 1, "", "nacked", 6102)
		prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
		nack99 := makeGoSettlementSpan("p", "s", "nack", 99, 0, false, 6102, []*proxypb.TraceSpan{subNack})
		expNack := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				ExpectedOutcome:     "nacked",
				ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subNack, ccN, scN, prN, nack99}, expNack), CodeWrongAttributeValue, "settlement.nack.attr.batch_count")

		expNackAllow := *expNack
		expNackAllow.AllowExtraPubSubSpans = true
		if res := Verify(goCaps, []*proxypb.TraceSpan{subNack, ccN, scN, prN, nack99}, &expNackAllow); !res.Passed() {
			t.Fatalf("s nack batch_count=99 with AllowExtraPubSubSpans=true expected PASS, got:\n%s", res.Format())
		}

		// Pure -00 dropped 1-msg trace with s ack/s nack batch_count=99 -> FAIL; batch_count=1 -> PASS.
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(6103), testSpanID(61030))
		ackDrop99 := makeGoSettlementSpan("p", "s", "ack", 99, 0, false, 6103, nil)
		expDropAck := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampTP},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{ackDrop99}, expDropAck), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		nackDrop99 := makeGoSettlementSpan("p", "s", "nack", 99, 0, false, 6104, nil)
		expDropNack := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampTP},
				ExpectDroppedUnsampled: true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "nack"}},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{nackDrop99}, expDropNack), CodeWrongAttributeValue, "settlement.nack.attr.batch_count")

		// Positive boundary & mixed sampling: 1 sampled acked + 1 sampled nacked + 1 dropped -00 acked.
		// s ack links only [sub0] (uniqueValidLinks=1) + len(droppedMsgs)=1 -> batch_count=2 PASSES, batch_count=3 FAILS.
		sampTP := fmt.Sprintf("00-%s-%s-01", testTraceID(6110), testSpanID(61100))
		subS0, ccS0, scS0, prS0, _ := makeGoSubscribeMessageSpans("p", "s", "ms0", "as0", "", 5, 0, false, 3, sampTP, "acked", 6110)
		subS1, ccS1, scS1, prS1, _ := makeGoSubscribeMessageSpans("p", "s", "ms1", "as1", "", 5, 0, false, 3, "", "nacked", 6111)
		prS0.Events = append(prS0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prS1.Events = append(prS1.Events, &proxypb.SpanEvent{Name: "nack called"})
		nackS1 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 6111, []*proxypb.TraceSpan{subS1})
		expMixedBound := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "as0", MessageID: "ms0", WireTraceparent: sampTP, BatchCount: 3, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "as1", MessageID: "ms1", BatchCount: 3, Settlements: []SettlementExpectation{{Operation: "nack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}

		subS0Copy2 := cloneSpans([]*proxypb.TraceSpan{subS0})[0]
		ackBoundary2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 6112, []*proxypb.TraceSpan{subS0Copy2})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subS0Copy2, ccS0, scS0, prS0, subS1, ccS1, scS1, prS1, nackS1, ackBoundary2}, expMixedBound); !res.Passed() {
			t.Fatalf("mixed boundary batch_count == uniqueValidLinks+len(droppedMsgs) (2) expected PASS, got:\n%s", res.Format())
		}

		subS0Copy3 := cloneSpans([]*proxypb.TraceSpan{subS0})[0]
		ackOver3 := makeGoSettlementSpan("p", "s", "ack", 3, 0, false, 6113, []*proxypb.TraceSpan{subS0Copy3})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subS0Copy3, ccS0, scS0, prS0, subS1, ccS1, scS1, prS1, nackS1, ackOver3}, expMixedBound), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// Contrast: StrictSettlementOps=true on 2 ALO msgs with coalesced fast-ack receipt modack (links=[sub0], batch_count=2, droppedMsgs=0) must NOT false-fail Phase 5 upper bound.
		subM0, ccM0, scM0, prM0, _ := makeGoSubscribeMessageSpans("p", "s", "mm0", "am0", "", 5, 0, false, 2, "", "acked", 6120)
		subM1, ccM1, scM1, prM1, _ := makeGoSubscribeMessageSpans("p", "s", "mm1", "am1", "", 5, 0, false, 2, "", "acked", 6121)
		prM0.Events = append(prM0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prM1.Events = append(prM1.Events, &proxypb.SpanEvent{Name: "ack called"})
		modFastCoalesced := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 6120, []*proxypb.TraceSpan{subM0})
		ackFastCoalesced := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 6121, []*proxypb.TraceSpan{subM0, subM1})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subM0, ccM0, scM0, prM0, subM1, ccM1, scM1, prM1, modFastCoalesced, ackFastCoalesced}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{
					{AckID: "am0", MessageID: "mm0", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true), ExpectedBatchCount: 2}, {Operation: "ack"}}},
					{AckID: "am1", MessageID: "mm1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("StrictSettlementOps=true coalesced fast-ack receipt modack expected PASS, got:\n%s", res.Format())
		}

		// Coalesced dropped message with omitted Settlements still allows batch_count=2 (1 sampled + 1 dropped) -> PASS, batch_count=3 -> FAIL.
		subOmit, ccOm, scOm, prOm, _ := makeGoSubscribeMessageSpans("p", "s", "mom0", "aom0", "", 5, 0, false, 2, sampTP, "acked", 6125)
		prOm.Events = append(prOm.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackOm2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 6125, []*proxypb.TraceSpan{subOmit})
		expOm := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "aom0", MessageID: "mom0", WireTraceparent: sampTP, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true},
				},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{subOmit, ccOm, scOm, prOm, ackOm2}, expOm); !res.Passed() {
			t.Fatalf("coalesced dropped message with omitted Settlements batch_count=2 expected PASS, got:\n%s", res.Format())
		}

		// Under-reported batch_count=1 on 2-link s ack (< minRequired=2) fails lower bound.
		subU0, ccU0, scU0, prU0, _ := makeGoSubscribeMessageSpans("p", "s", "mu0", "au0", "", 5, 0, false, 2, "", "acked", 6130)
		subU1, ccU1, scU1, prU1, _ := makeGoSubscribeMessageSpans("p", "s", "mu1", "au1", "", 5, 0, false, 2, "", "acked", 6131)
		prU0.Events = append(prU0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prU1.Events = append(prU1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackUnder1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 6130, []*proxypb.TraceSpan{subU0, subU1})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subU0, ccU0, scU0, prU0, subU1, ccU1, scU1, prU1, ackUnder1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "au0", MessageID: "mu0", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "au1", MessageID: "mu1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// Phase 3 explicit modack on dropped message + duplicate linked receipt modack on sub0 -> still fails duplicate_receipt.
		subP3, ccP3, scP3, prP3, _ := makeGoSubscribeMessageSpans("p", "s", "mp3", "ap3", "", 5, 0, true, 2, sampTP, "acked", 6140)
		prP3.Events = append(prP3.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackP3 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 6140, []*proxypb.TraceSpan{subP3})
		modP3Good := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 6142, []*proxypb.TraceSpan{subP3})
		modP3BogusEarly := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 6141, nil)
		modP3BogusEarly.Links = []*proxypb.SpanLink{{TraceId: subP3.TraceId, SpanId: subP3.SpanId}}
		resP3 := Verify(goCaps, []*proxypb.TraceSpan{subP3, ccP3, scP3, prP3, ackP3, modP3Good, modP3BogusEarly}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{
					{AckID: "ap3", MessageID: "mp3", WireTraceparent: sampTP, BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}}},
				},
			},
		})
		assertHasViolation(t, resP3, CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")
		assertHasViolation(t, resP3, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
	})
}

func TestIter6Regressions_SharedDroppedBudgetAndStrictModackBounds(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
	setSpanStart := func(s *proxypb.TraceSpan, start time.Time) {
		s.StartTime = timestamppb.New(start)
		s.EndTime = timestamppb.New(start.Add(5 * time.Millisecond))
	}

	t.Run("Attack9A_2 and Attack9A_3: shared dropped -00 budget across multiple s ack and s nack spans", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(7000), testSpanID(70000))
		baseTime := time.Date(2026, 11, 15, 18, 1, 0, 0, time.UTC)

		for _, op := range []string{"ack", "nack"} {
			outcome := "acked"
			evName := "ack called"
			if op == "nack" {
				outcome = "nacked"
				evName = "nack called"
			}
			for _, flipTimestamps := range []bool{false, true} {
				sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m9a0-"+op, "a9a0-"+op, "", 5, 0, false, 1, "", outcome, 7001)
				sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m9a1-"+op, "a9a1-"+op, "", 5, 0, false, 1, "", outcome, 7002)
				pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: evName})
				pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: evName})
				sp1 := makeGoSettlementSpan("p", "s", op, 2, 0, false, 7001, []*proxypb.TraceSpan{sub0})
				sp2 := makeGoSettlementSpan("p", "s", op, 2, 0, false, 7002, []*proxypb.TraceSpan{sub1})
				if !flipTimestamps {
					setSpanStart(sp1, baseTime)
					setSpanStart(sp2, baseTime.Add(time.Millisecond))
				} else {
					setSpanStart(sp1, baseTime.Add(time.Millisecond))
					setSpanStart(sp2, baseTime)
				}

				exp := &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe: &SubscribeExpectation{
						ExactlyOnce: BoolPtr(false),
						Messages: []SubscribeMessageExpectation{
							{AckID: "a9a0-" + op, MessageID: "m9a0-" + op, BatchCount: 1, Settlements: []SettlementExpectation{{Operation: op}}},
							{AckID: "a9a1-" + op, MessageID: "m9a1-" + op, BatchCount: 1, Settlements: []SettlementExpectation{{Operation: op}}},
							{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: op}}},
						},
					},
				}
				assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, sp1, sp2}, exp), CodeWrongAttributeValue, "settlement."+op+".attr.batch_count")

				// Valid boundary: total 3 across 2 sampled + 1 dropped -> PASS in both timestamp orders.
				sp2Valid := cloneSpans([]*proxypb.TraceSpan{sp2})[0]
				sp2Valid.Attributes["messaging.batch.message_count"] = intAV(1)
				sub1Valid := cloneSpans([]*proxypb.TraceSpan{sub1})[0]
				if res := Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1Valid, cc1, sc1, pr1, sp1, sp2Valid}, exp); !res.Passed() {
					t.Fatalf("valid boundary %s(2)+%s(1) flip=%v expected PASS, got:\n%s", op, op, flipTimestamps, res.Format())
				}
			}
		}

		// Mixed cross-op valid vs invalid boundary when d0 allows ack/nack (omitted Settlements & empty ExpectedOutcome, and eod_failed).
		for _, outcome := range []string{"", "eod_failed"} {
			for _, flipTimestamps := range []bool{false, true} {
				subEOD := outcome == "eod_failed"
				subA, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "mxa", "axa", "", 5, 0, subEOD, 1, "", "acked", 7010)
				subN, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "mxn", "axn", "", 5, 0, subEOD, 1, "", "nacked", 7011)
				prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
				prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
				ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7010, []*proxypb.TraceSpan{subA})
				nack2 := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 7011, []*proxypb.TraceSpan{subN})
				nack2BadUnset := cloneSpans([]*proxypb.TraceSpan{nack2})[0]
				if subEOD {
					subA.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}}, {Name: "ack end"}}
					subN.Events = []*proxypb.SpanEvent{{Name: "nack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: "nack end"}}
					nack2.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
					nack2.StatusMessage = eodPartialBatchErrorMsg
				}
				if !flipTimestamps {
					setSpanStart(ack1, baseTime)
					setSpanStart(nack2, baseTime.Add(time.Millisecond))
				} else {
					setSpanStart(ack1, baseTime.Add(time.Millisecond))
					setSpanStart(nack2, baseTime)
				}
				var d0EOS *bool
				if subEOD {
					d0EOS = BoolPtr(true)
				}
				expMixed := &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe: &SubscribeExpectation{
						ExactlyOnce: BoolPtr(subEOD),
						Messages: []SubscribeMessageExpectation{
							{AckID: "axa", MessageID: "mxa", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
							{AckID: "axn", MessageID: "mxn", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "nack"}}},
							{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: d0EOS, ExpectedOutcome: outcome},
						},
					},
				}
				extraSpans := []*proxypb.TraceSpan{subA, ccA, scA, prA, subN, ccN, scN, prN, ack1, nack2}
				if subEOD {
					modAN := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 7012, []*proxypb.TraceSpan{subA, subN})
					extraSpans = append(extraSpans, modAN)
				}
				if res := Verify(goCaps, extraSpans, expMixed); !res.Passed() {
					t.Fatalf("cross-op ack(1)+nack(2) outcome=%q flip=%v expected PASS, got:\n%s", outcome, flipTimestamps, res.Format())
				}
				if subEOD {
					// Status inversion: UNSET coalesced nack2(bc=2) with eod_failed dropped -00 MUST FAIL settlement.nack.status.
					badUnset := []*proxypb.TraceSpan{subA, ccA, scA, prA, subN, ccN, scN, prN, ack1, nack2BadUnset}
					assertHasViolation(t, Verify(goCaps, badUnset, expMixed), CodeWrongStatusCode, "settlement.nack.status")
				}

				// Both ack(2) and nack(2) claiming the same single dropped -00 -> FAIL regardless of timestamp order.
				ack2 := cloneSpans([]*proxypb.TraceSpan{ack1})[0]
				ack2.Attributes["messaging.batch.message_count"] = intAV(2)
				bothSpans := []*proxypb.TraceSpan{subA, ccA, scA, prA, subN, ccN, scN, prN, ack2, nack2}
				resBoth2 := Verify(goCaps, bothSpans, expMixed)
				if resBoth2.Status != StatusFail {
					t.Fatalf("cross-op ack(2)+nack(2) for 1 dropped outcome=%q flip=%v expected FAIL, got PASS", outcome, flipTimestamps)
				}
				foundBatchErr := false
				for _, f := range resBoth2.Failures {
					if f.Code == CodeWrongAttributeValue && (f.Rule == "settlement.ack.attr.batch_count" || f.Rule == "settlement.nack.attr.batch_count") {
						foundBatchErr = true
					}
				}
				if !foundBatchErr {
					t.Fatalf("expected settlement.{ack,nack}.attr.batch_count violation, got:\n%s", resBoth2.Format())
				}
			}
		}
	})

	t.Run("Attack9B_1 and Attack9B_2: dropped -00 cross-op and outcome eligibility enforcement", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(7100), testSpanID(71000))

		// Attack9B_1: m0=Ack, d0=-00 Nack ONLY -> ack(links=[sub0], bc=2) + nack(links=[], bc=1) MUST FAIL.
		sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m9b1", "a9b1", "", 5, 0, false, 1, "", "acked", 7101)
		pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7101, []*proxypb.TraceSpan{sub0})
		nackDrop1 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 7102, nil)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack2, nackDrop1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9b1", MessageID: "m9b1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "nack"}}},
				},
			},
		}), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// Attack9B_2: m0=Ack, d0=-00 ExpectedOutcome="expired" -> ack(links=[sub0], bc=2) MUST FAIL.
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9b1", MessageID: "m9b1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "expired"},
				},
			},
		}), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// ExpectedOutcome="acked" on d0: ack(bc=2) PASSES, nack(bc=2) FAILS.
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9b1", MessageID: "m9b1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
				},
			},
		}); !res.Passed() {
			t.Fatalf("d0 ExpectedOutcome=\"acked\" coalesced with ack(bc=2) expected PASS, got:\n%s", res.Format())
		}

		subN, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "m9bn", "a9bn", "", 5, 0, false, 1, "", "nacked", 7103)
		prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
		nack2 := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 7103, []*proxypb.TraceSpan{subN})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subN, ccN, scN, prN, nack2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9bn", MessageID: "m9bn", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "nack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
				},
			},
		}), CodeWrongAttributeValue, "settlement.nack.attr.batch_count")

		// ExpectedOutcome="nacked" on d0: ack(bc=2) FAILS.
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9b1", MessageID: "m9b1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "nacked"},
				},
			},
		}), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// ExpectedOutcome="eod_failed" without Settlements allows ack(bc=2, ERROR partial-batch) OR nack(bc=2), rejects UNSET ack(bc=2), and if Settlements=[{nack}] then ack(bc=2) FAILS.
		subE, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m9be", "a9be", "", 5, 0, true, 1, "", "acked", 7105)
		subE.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: "ack end"}}
		prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackE2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7105, []*proxypb.TraceSpan{subE})
		ackE2Err := cloneSpans([]*proxypb.TraceSpan{ackE2})[0]
		ackE2Err.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackE2Err.StatusMessage = eodPartialBatchErrorMsg
		modE := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 7106, []*proxypb.TraceSpan{subE})
		exp9BEod := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(true),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9be", MessageID: "m9be", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "eod_failed"},
				},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{subE, ccE, scE, prE, modE, ackE2Err}, exp9BEod); !res.Passed() {
			t.Fatalf("d0 ExpectedOutcome=\"eod_failed\" coalesced with ack(bc=2, ERROR) expected PASS, got:\n%s", res.Format())
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subE, ccE, scE, prE, modE, ackE2}, exp9BEod), CodeWrongStatusCode, "settlement.ack.status")
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subE, ccE, scE, prE, modE, ackE2, nackDrop1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(true),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9be", MessageID: "m9be", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "eod_failed", Settlements: []SettlementExpectation{{Operation: "nack"}}},
				},
			},
		}), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// Under StrictSettlementOps=true, d0 with only modack in Settlements is NOT eligible for ack or nack;
		// under StrictSettlementOps=false + ExpectedOutcome="acked", modack-only d0 IS eligible and coalesces with ack(bc=2).
		modDrop1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 7104, nil)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack2, modDrop1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9b1", MessageID: "m9b1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "modack"}}},
				},
			},
		}), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack2, modDrop1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a9b1", MessageID: "m9b1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "modack"}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("StrictSettlementOps=false modack-only + ExpectedOutcome=\"acked\" coalesced expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Phase 3 pre-reservation, retried -00, ExpectedBatchCount consistency & 60-seed permutations", func(t *testing.T) {
		unsampTP0 := fmt.Sprintf("00-%s-%s-00", testTraceID(7200), testSpanID(72000))
		unsampTP1 := fmt.Sprintf("00-%s-%s-00", testTraceID(7201), testSpanID(72010))
		baseTime := time.Date(2026, 11, 15, 18, 1, 5, 0, time.UTC)

		// Inflated ack1(links=[sub0], bc=3) cannot steal Phase-3-matched d0 from nack1(links=[], bc=1) in EITHER timestamp order.
		for _, ackEarlier := range []bool{true, false} {
			sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m-res", "a-res", "", 5, 0, false, 1, "", "acked", 7201)
			pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "ack called"})
			ack1 := makeGoSettlementSpan("p", "s", "ack", 3, 0, false, 7201, []*proxypb.TraceSpan{sub0})
			nack1 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 7202, nil)
			if ackEarlier {
				setSpanStart(ack1, baseTime)
				setSpanStart(nack1, baseTime.Add(time.Millisecond))
			} else {
				setSpanStart(ack1, baseTime.Add(time.Millisecond))
				setSpanStart(nack1, baseTime)
			}
			res := Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, ack1, nack1}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce: BoolPtr(false),
					Messages: []SubscribeMessageExpectation{
						{AckID: "a-res", MessageID: "m-res", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "nack"}}},
					},
				},
			})
			assertHasViolation(t, res, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")
			for _, f := range res.Failures {
				if f.Rule == "settlement.nack.attr.batch_count" || f.Code == CodeInvalidTimestamp {
					t.Fatalf("unexpected violation (ackEarlier=%v), got:\n%s", ackEarlier, res.Format())
				}
			}
		}

		// Retried -00 settlements: d0 has 2 acks (ack1, ack2), d1 has omitted Settlements coalesced into ack3(links=[sub0], bc=2) -> PASS!
		subR, ccR, scR, prR, _ := makeGoSubscribeMessageSpans("p", "s", "m-ret", "a-ret", "", 5, 0, true, 1, "", "acked", 7205)
		prR.Events = append(prR.Events, &proxypb.SpanEvent{Name: "ack called"})
		modR := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 7205, []*proxypb.TraceSpan{subR})
		ackD1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7206, nil)
		ackD1.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackD1.StatusMessage = "transient"
		ackD1.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		ackD2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7207, nil)
		ackS3 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7208, []*proxypb.TraceSpan{subR})
		resRet := Verify(goCaps, []*proxypb.TraceSpan{subR, ccR, scR, prR, modR, ackD1, ackD2, ackS3}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(true),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-ret", MessageID: "m-ret", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true}, {Operation: "ack"}}},
					{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true},
				},
			},
		})
		if !resRet.Passed() {
			t.Fatalf("retried -00 + coalesced omitted -00 expected PASS, got:\n%s", resRet.Format())
		}

		// ExpectedBatchCount=2 on 1-msg trace with !AllowExtraPubSubSpans still FAILS physical upper bound.
		subE, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-ebc", "a-ebc", "", 5, 0, false, 1, "", "acked", 7209)
		prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackE2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7209, []*proxypb.TraceSpan{subE})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subE, ccE, scE, prE, ackE2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 2}},
			},
		}), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// 60-seed span+link permutation determinism on mixed multi-batch ack + nack + -00.
		subP0, ccP0, scP0, prP0, _ := makeGoSubscribeMessageSpans("p", "s", "mp0", "ap0", "", 5, 0, false, 2, "", "acked", 7210)
		subP1, ccP1, scP1, prP1, _ := makeGoSubscribeMessageSpans("p", "s", "mp1", "ap1", "", 5, 0, false, 2, "", "acked", 7211)
		subP2, ccP2, scP2, prP2, _ := makeGoSubscribeMessageSpans("p", "s", "mp2", "ap2", "", 5, 0, false, 1, "", "nacked", 7212)
		prP0.Events = append(prP0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prP1.Events = append(prP1.Events, &proxypb.SpanEvent{Name: "ack called"})
		prP2.Events = append(prP2.Events, &proxypb.SpanEvent{Name: "nack called"})
		ackP1 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7210, []*proxypb.TraceSpan{subP0})
		ackP2 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7211, []*proxypb.TraceSpan{subP1})
		nackP := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 7212, []*proxypb.TraceSpan{subP2})
		expPerm := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "ap0", MessageID: "mp0", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "ap1", MessageID: "mp1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "ap2", MessageID: "mp2", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "nack"}}},
					{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}
		baseFailSpans := []*proxypb.TraceSpan{subP0, ccP0, scP0, prP0, subP1, ccP1, scP1, prP1, subP2, ccP2, scP2, prP2, ackP1, ackP2, nackP}
		var failBaseline string
		for seed := int64(0); seed < 60; seed++ {
			perm := cloneSpans(baseFailSpans)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			for _, s := range perm {
				if len(s.Links) > 1 {
					rng.Shuffle(len(s.Links), func(i, j int) { s.Links[i], s.Links[j] = s.Links[j], s.Links[i] })
				}
			}
			res := Verify(goCaps, perm, expPerm)
			assertHasViolation(t, res, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")
			formatted := res.Format()
			if seed == 0 {
				failBaseline = formatted
			} else if formatted != failBaseline {
				t.Fatalf("seed=%d Attack9A permutation Format() differed from seed=0:\nwant:\n%s\ngot:\n%s", seed, failBaseline, formatted)
			}
		}
	})

	t.Run("Attack9C_1 to Attack9C_4: StrictSettlementOps=true modack batch_count bounds & duplicate receipt", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(7300), testSpanID(73000))

		// Attack9C_1: 1 ALO msg, StrictSettlementOps=true, explicit linked receipt modack batch_count=99 -> FAIL.
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m9c1", "a9c1", "", 5, 0, false, 1, "", "acked", 7301)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7301, []*proxypb.TraceSpan{sub1})
		modRec99 := makeGoSettlementSpan("p", "s", "modack", 99, 10, true, 7302, []*proxypb.TraceSpan{sub1})
		exp9C1 := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{
					{Operation: "modack", IsReceiptModack: BoolPtr(true)},
					{Operation: "ack"},
				},
			},
		}
		res9C1 := Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, ack1, modRec99}, exp9C1)
		assertHasViolation(t, res9C1, CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
		if len(res9C1.Failures) != 1 {
			t.Fatalf("Attack9C_1 expected single batch_count violation, got:\n%s", res9C1.Format())
		}

		// Missing is_receipt_modack + batch_count=99 under StrictSettlementOps=true -> catches both batch_count and missing attr.
		sub1NR, cc1NR, sc1NR, pr1NR, _ := makeGoSubscribeMessageSpans("p", "s", "m9c1nr", "a9c1nr", "", 5, 0, false, 1, "", "acked", 7305)
		pr1NR.Events = append(pr1NR.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack1NR := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7305, []*proxypb.TraceSpan{sub1NR})
		modNoRec99 := makeGoSettlementSpan("p", "s", "modack", 99, 10, true, 7306, []*proxypb.TraceSpan{sub1NR})
		delete(modNoRec99.Attributes, "messaging.gcp_pubsub.is_receipt_modack")
		resNoRec := Verify(goCaps, []*proxypb.TraceSpan{sub1NR, cc1NR, sc1NR, pr1NR, ack1NR, modNoRec99}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}},
			},
		})
		assertHasViolation(t, resNoRec, CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
		assertHasViolation(t, resNoRec, CodeMissingAttribute, "settlement.modack.attr.is_receipt_modack")

		// Attack9C_2: 1 ALO msg, StrictSettlementOps=true, explicit linked periodic modack batch_count=99 -> FAIL.
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m9c2", "a9c2", "", 5, 0, false, 1, "", "acked", 7311)
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7311, []*proxypb.TraceSpan{sub2})
		modPer99 := makeGoSettlementSpan("p", "s", "modack", 99, 60, false, 7312, []*proxypb.TraceSpan{sub2})
		exp9C2 := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{
					{Operation: "modack", IsReceiptModack: BoolPtr(false)},
					{Operation: "ack"},
				},
			},
		}
		res9C2 := Verify(goCaps, []*proxypb.TraceSpan{sub2, cc2, sc2, pr2, ack2, modPer99}, exp9C2)
		assertHasViolation(t, res9C2, CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
		if len(res9C2.Failures) != 1 {
			t.Fatalf("Attack9C_2 expected single batch_count violation, got:\n%s", res9C2.Format())
		}

		// Attack9C_3: 1 dropped -00 msg, StrictSettlementOps=true, explicit unlinked modack batch_count=99 (both receipt & periodic) -> FAIL.
		ack3 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7321, nil)
		for _, isRec := range []bool{true, false} {
			modDrop99 := makeGoSettlementSpan("p", "s", "modack", 99, 10, isRec, 7322, nil)
			exp9C3 := &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					MessageCount:           1,
					WireTraceparents:       []string{unsampTP},
					ExpectDroppedUnsampled: true,
					StrictSettlementOps:    true,
					ExpectedSettlements:    []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}},
				},
			}
			res9C3 := Verify(goCaps, []*proxypb.TraceSpan{ack3, modDrop99}, exp9C3)
			assertHasViolation(t, res9C3, CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
			if len(res9C3.Failures) != 1 {
				t.Fatalf("Attack9C_3 (isRec=%v) expected single batch_count violation, got:\n%s", isRec, res9C3.Format())
			}
		}

		// Attack9C_4: 1 ALO msg, StrictSettlementOps=true, AllowExtraPubSubSpans=true, 2 linked receipt modacks -> FAIL duplicate_receipt.
		for _, expectModack := range []bool{true, false} {
			sub4, cc4, sc4, pr4, _ := makeGoSubscribeMessageSpans("p", "s", "m9c4", "a9c4", "", 5, 0, false, 1, "", "acked", 7331)
			pr4.Events = append(pr4.Events, &proxypb.SpanEvent{Name: "ack called"})
			ack4 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7331, []*proxypb.TraceSpan{sub4})
			mod4a := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 7332, []*proxypb.TraceSpan{sub4})
			mod4b := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 7333, []*proxypb.TraceSpan{sub4})
			settlements := []SettlementExpectation{{Operation: "ack"}}
			if expectModack {
				settlements = append([]SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}}, settlements...)
			}
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub4, cc4, sc4, pr4, ack4, mod4a, mod4b}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					MessageCount:        1,
					ExactlyOnce:         BoolPtr(false),
					StrictSettlementOps: true,
					ExpectedSettlements: settlements,
				},
			}), CodeUnexpectedSpan, "subscribe.modack.duplicate_receipt")
		}

		// Phase 2 forward_link deduplication under StrictSettlementOps=true:
		// unlinked EOD modack (bc=99) emits forward_link + batch_count without redundant subscribe.modack.missing_link.
		subEOD, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-dedup", "a-dedup", "", 5, 0, true, 1, "", "acked", 7341)
		prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackE := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7341, []*proxypb.TraceSpan{subEOD})
		unlinkedModEOD := makeGoSettlementSpan("p", "s", "modack", 99, 10, true, 7342, nil)
		resDedup := Verify(goCaps, []*proxypb.TraceSpan{subEOD, ccE, scE, prE, ackE, unlinkedModEOD}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}},
			},
		})
		assertHasViolation(t, resDedup, CodeMissingLink, "subscribe.settlement.forward_link")
		assertHasViolation(t, resDedup, CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
		for _, f := range resDedup.Failures {
			if f.Rule == "subscribe.modack.missing_link" {
				t.Fatalf("expected no duplicate subscribe.modack.missing_link when Phase 2 already emitted forward_link, got:\n%s", resDedup.Format())
			}
		}

		// Unlinked slot-exhausted ALO modack (bc=99) when forward_link already failed also skips duplicate missing_link.
		subALOEx, ccAE, scAE, prAE, _ := makeGoSubscribeMessageSpans("p", "s", "m-alo-ex", "a-alo-ex", "", 5, 0, false, 1, "", "acked", 7345)
		prAE.Events = append(prAE.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackAE := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7345, []*proxypb.TraceSpan{subALOEx})
		modLinkedRec := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 7346, []*proxypb.TraceSpan{subALOEx})
		modUnlinkedPer99 := makeGoSettlementSpan("p", "s", "modack", 99, 60, false, 7347, nil)
		resALODedup := Verify(goCaps, []*proxypb.TraceSpan{subALOEx, ccAE, scAE, prAE, ackAE, modLinkedRec, modUnlinkedPer99}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{
					{Operation: "modack", IsReceiptModack: BoolPtr(true)},
					{Operation: "modack", IsReceiptModack: BoolPtr(false)},
					{Operation: "ack"},
				},
			},
		})
		assertHasViolation(t, resALODedup, CodeMissingLink, "subscribe.settlement.forward_link")
		assertHasViolation(t, resALODedup, CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
		for _, f := range resALODedup.Failures {
			if f.Rule == "subscribe.modack.missing_link" {
				t.Fatalf("expected no duplicate subscribe.modack.missing_link on ALO forward_link failure, got:\n%s", resALODedup.Format())
			}
		}

		// Audit unmatched linked modack links under StrictSettlementOps=true + AllowExtraPubSubSpans=true.
		subL, ccL, scL, prL, _ := makeGoSubscribeMessageSpans("p", "s", "m-lnk", "a-lnk", "", 5, 0, false, 2, "", "acked", 7351)
		prL.Events = append(prL.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackL := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7351, []*proxypb.TraceSpan{subL})
		modExtraBadLinks := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 7352, []*proxypb.TraceSpan{subL})
		modExtraBadLinks.Links = append(modExtraBadLinks.Links,
			&proxypb.SpanLink{TraceId: subL.TraceId, SpanId: subL.SpanId},
			&proxypb.SpanLink{TraceId: testTraceID(7300), SpanId: testSpanID(73999)},
			&proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)},
		)
		resExtraLinks := Verify(goCaps, []*proxypb.TraceSpan{subL, ccL, scL, prL, ackL, modExtraBadLinks}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-lnk", MessageID: "m-lnk", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		})
		assertHasViolation(t, resExtraLinks, CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_link")
		assertHasViolation(t, resExtraLinks, CodeBrokenTraceparent, "subscribe.unsampled_settlement.forbidden_link")
		assertHasViolation(t, resExtraLinks, CodeMissingLink, "subscribe.settlement.dangling_link")
	})

	t.Run("Contrast cases: valid StrictSettlementOps=true, AllowExtraPubSubSpans=true, and ProfileCpp", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(7400), testSpanID(74000))

		// 1. Valid StrictSettlementOps=true linked receipt + periodic modack + dropped -00 modack -> PASS.
		subV, ccV, scV, prV, _ := makeGoSubscribeMessageSpans("p", "s", "m-v", "a-v", "", 5, 0, false, 1, "", "acked", 7401)
		prV.Events = append(prV.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackV := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7401, []*proxypb.TraceSpan{subV})
		modRecV := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 7402, []*proxypb.TraceSpan{subV})
		modPerV := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 7403, []*proxypb.TraceSpan{subV})
		modDropV := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 7404, nil)
		if res := Verify(goCaps, []*proxypb.TraceSpan{subV, ccV, scV, prV, ackV, modRecV, modPerV, modDropV}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-v", MessageID: "m-v", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}, {Operation: "modack", IsReceiptModack: BoolPtr(false)}, {Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}, {Operation: "ack"}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("valid StrictSettlementOps=true linked receipt+periodic+dropped modack expected PASS, got:\n%s", res.Format())
		}

		// Valid StrictSettlementOps=true coalesced fast-ack receipt modack boundary: bc=2 PASSES, bc=3 FAILS.
		subF0, ccF0, scF0, prF0, _ := makeGoSubscribeMessageSpans("p", "s", "mf0", "af0", "", 5, 0, false, 2, "", "acked", 7405)
		subF1, ccF1, scF1, prF1, _ := makeGoSubscribeMessageSpans("p", "s", "mf1", "af1", "", 5, 0, false, 2, "", "acked", 7406)
		prF0.Events = append(prF0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prF1.Events = append(prF1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackF := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7405, []*proxypb.TraceSpan{subF0, subF1})
		modF3 := makeGoSettlementSpan("p", "s", "modack", 3, 10, true, 7406, []*proxypb.TraceSpan{subF0})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subF0, ccF0, scF0, prF0, subF1, ccF1, scF1, prF1, ackF, modF3}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{
					{AckID: "af0", MessageID: "mf0", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}, {Operation: "ack"}}},
					{AckID: "af1", MessageID: "mf1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		// 2. AllowExtraPubSubSpans=true contrasts on Attack9A, Attack9B, and Attack9C_1-3 (ExpectedBatchCount=0 -> PASS).
		subA0, ccA0, scA0, prA0, _ := makeGoSubscribeMessageSpans("p", "s", "m-ca0", "a-ca0", "", 5, 0, false, 1, "", "acked", 7410)
		subA1, ccA1, scA1, prA1, _ := makeGoSubscribeMessageSpans("p", "s", "m-ca1", "a-ca1", "", 5, 0, false, 1, "", "acked", 7411)
		prA0.Events = append(prA0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prA1.Events = append(prA1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackC0 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7410, []*proxypb.TraceSpan{subA0})
		ackC1 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7411, []*proxypb.TraceSpan{subA1})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subA0, ccA0, scA0, prA0, subA1, ccA1, scA1, prA1, ackC0, ackC1}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-ca0", MessageID: "m-ca0", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "a-ca1", MessageID: "m-ca1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("AllowExtraPubSubSpans=true Attack9A contrast expected PASS, got:\n%s", res.Format())
		}

		if res := Verify(goCaps, []*proxypb.TraceSpan{subA0, ccA0, scA0, prA0, ackC0}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-ca0", MessageID: "m-ca0", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "expired"},
				},
			},
		}); !res.Passed() {
			t.Fatalf("AllowExtraPubSubSpans=true Attack9B contrast expected PASS, got:\n%s", res.Format())
		}

		// Attack9C_1 and 9C_2 with AllowExtraPubSubSpans=true (ExpectedBatchCount=0 -> PASS; ExpectedBatchCount=1 -> FAIL).
		subA0c, ccA0c, scA0c, prA0c, _ := makeGoSubscribeMessageSpans("p", "s", "m-ca0c", "a-ca0c", "", 5, 0, false, 1, "", "acked", 7412)
		prA0c.Events = append(prA0c.Events, &proxypb.SpanEvent{Name: "ack called"})
		modRecC99 := makeGoSettlementSpan("p", "s", "modack", 99, 10, true, 7412, []*proxypb.TraceSpan{subA0c})
		ackC99a := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7413, []*proxypb.TraceSpan{subA0c})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subA0c, ccA0c, scA0c, prA0c, ackC99a, modRecC99}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}, {Operation: "ack"}},
			},
		}); !res.Passed() {
			t.Fatalf("AllowExtraPubSubSpans=true Attack9C_1 contrast expected PASS, got:\n%s", res.Format())
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subA0c, ccA0c, scA0c, prA0c, ackC99a, modRecC99}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true), ExpectedBatchCount: 1}, {Operation: "ack"}},
			},
		}), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		subA0b, ccA0b, scA0b, prA0b, _ := makeGoSubscribeMessageSpans("p", "s", "m-ca0b", "a-ca0b", "", 5, 0, false, 1, "", "acked", 7414)
		prA0b.Events = append(prA0b.Events, &proxypb.SpanEvent{Name: "ack called"})
		modPerC99 := makeGoSettlementSpan("p", "s", "modack", 99, 60, false, 7414, []*proxypb.TraceSpan{subA0b})
		ackC99b := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7415, []*proxypb.TraceSpan{subA0b})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subA0b, ccA0b, scA0b, prA0b, ackC99b, modPerC99}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(false)}, {Operation: "ack"}},
			},
		}); !res.Passed() {
			t.Fatalf("AllowExtraPubSubSpans=true Attack9C_2 contrast expected PASS, got:\n%s", res.Format())
		}
		// Attack9C_3 with AllowExtraPubSubSpans=true (ExpectedBatchCount=0) -> PASS.
		modDropC99 := makeGoSettlementSpan("p", "s", "modack", 99, 10, true, 7416, nil)
		ackC99c := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7417, nil)
		if res := Verify(goCaps, []*proxypb.TraceSpan{ackC99c, modDropC99}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				MessageCount:           1,
				WireTraceparents:       []string{unsampTP},
				ExpectDroppedUnsampled: true,
				StrictSettlementOps:    true,
				ExpectedSettlements:    []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}},
			},
		}); !res.Passed() {
			t.Fatalf("AllowExtraPubSubSpans=true Attack9C_3 contrast expected PASS, got:\n%s", res.Format())
		}

		// 3. ProfileCpp multi-modack & per-message ack contrast under StrictSettlementOps=true -> PASS.
		subCpp := cppSpan(testTraceID(7450), testSpanID(74500), "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		subCpp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		subCpp.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                          strAV("p"),
			"messaging.system":                                        strAV("gcp_pubsub"),
			"messaging.destination.name":                              strAV("s"),
			"messaging.operation.type":                                strAV("subscribe"),
			"messaging.message.id":                                    strAV("cm-6"),
			"messaging.message.envelope.size":                         intAV(25),
			"messaging.gcp_pubsub.message.ack_id":                     strAV("ca-6"),
			"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(true),
			"messaging.gcp_pubsub.result":                             strAV("ack"),
		}
		subCpp.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.ack_end"}, {Name: "gl-cpp.modack_start"}, {Name: "gl-cpp.modack_end"}}
		ccCpp := cppSpan(testTraceID(7450), testSpanID(74501), subCpp.SpanId, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		ccCpp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		ccCpp.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		scCpp := cppSpan(testTraceID(7450), testSpanID(74502), subCpp.SpanId, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		scCpp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		scCpp.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		prCpp := cppSpan(testTraceID(7450), testSpanID(74503), subCpp.SpanId, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		prCpp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		prCpp.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		ackCpp := cppSpan(testTraceID(7451), testSpanID(74510), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 5*time.Millisecond)
		ackCpp.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                             strAV("p"),
			"messaging.system":                           strAV("gcp_pubsub"),
			"messaging.destination.name":                 strAV("s"),
			"code.function.name":                         strAV("pubsub::AckHandler::ack"),
			"messaging.gcp_pubsub.message.ack_id":        strAV("ca-6"),
			"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
			"messaging.operation.type":                   strAV("settle"),
			"gl-cpp.status_code":                         strAV("OK"),
		}
		ackCpp.Links = []*proxypb.SpanLink{{TraceId: subCpp.TraceId, SpanId: subCpp.SpanId}}
		makeCppMod := func(seq int) *proxypb.TraceSpan {
			m := cppSpan(testTraceID(7460+seq), testSpanID(74600+seq), "", "s modack", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Duration(seq)*time.Millisecond)
			m.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			m.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                    strAV("p"),
				"messaging.system":                                  strAV("gcp_pubsub"),
				"messaging.destination.name":                        strAV("s"),
				"messaging.operation.type":                          strAV("extend"),
				"messaging.batch.message_count":                     intAV(1),
				"messaging.gcp_pubsub.message.ack_deadline_seconds": intAV(30),
			}
			m.Links = []*proxypb.SpanLink{{TraceId: subCpp.TraceId, SpanId: subCpp.SpanId}}
			return m
		}
		if res := Verify(cppCaps, []*proxypb.TraceSpan{subCpp, ccCpp, scCpp, prCpp, ackCpp, makeCppMod(1), makeCppMod(2)}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "modack"}, {Operation: "ack"}},
			},
		}); !res.Passed() {
			t.Fatalf("ProfileCpp multi-modack + per-message ack under StrictSettlementOps=true expected PASS, got:\n%s", res.Format())
		}

		// 4. ALO unlinked receipt modack with oversized bc=99 under StrictSettlementOps=true:
		// AllowExtraPubSubSpans=false -> FAIL batch_count; AllowExtraPubSubSpans=true -> PASS (bounded slot consumption).
		subU, ccU, scU, prU, _ := makeGoSubscribeMessageSpans("p", "s", "m-ul99", "a-ul99", "", 5, 0, false, 1, "", "acked", 7470)
		prU.Events = append(prU.Events, &proxypb.SpanEvent{Name: "ack called"})
		modUnlinked99 := makeGoSettlementSpan("p", "s", "modack", 99, 10, true, 7470, nil)
		ackU := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 7471, []*proxypb.TraceSpan{subU})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subU, ccU, scU, prU, ackU, modUnlinked99}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")
		if res := Verify(goCaps, []*proxypb.TraceSpan{subU, ccU, scU, prU, ackU, modUnlinked99}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack"}},
			},
		}); !res.Passed() {
			t.Fatalf("AllowExtraPubSubSpans=true unlinked ALO receipt modack bc=99 contrast expected PASS, got:\n%s", res.Format())
		}

		// 5. Coalesced 2 sampled + 2 dropped -00 messages split across ack1(bc=2)+ack2(bc=2) -> PASS.
		unsampTP1 := fmt.Sprintf("00-%s-%s-00", testTraceID(7480), testSpanID(74800))
		unsampTP2 := fmt.Sprintf("00-%s-%s-00", testTraceID(7481), testSpanID(74810))
		subC0, ccC0, scC0, prC0, _ := makeGoSubscribeMessageSpans("p", "s", "mc0", "ac0", "", 5, 0, false, 1, "", "acked", 7482)
		subC1, ccC1, scC1, prC1, _ := makeGoSubscribeMessageSpans("p", "s", "mc1", "ac1", "", 5, 0, false, 1, "", "acked", 7483)
		prC0.Events = append(prC0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prC1.Events = append(prC1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackC0b := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7482, []*proxypb.TraceSpan{subC0})
		ackC1b := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 7483, []*proxypb.TraceSpan{subC1})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subC0, ccC0, scC0, prC0, subC1, ccC1, scC1, prC1, ackC0b, ackC1b}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "ac0", MessageID: "mc0", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "ac1", MessageID: "mc1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true},
					{WireTraceparent: unsampTP2, ExpectDroppedUnsampled: true},
				},
			},
		}); !res.Passed() {
			t.Fatalf("2 sampled + 2 dropped -00 across ack(2)+ack(2) expected PASS, got:\n%s", res.Format())
		}
	})
}

func TestGoCanonical_SubscribeInterruptModes(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("Default zero-valued SubscribeExpectation strict regressions preserved", func(t *testing.T) {
		// 1. Missing concurrency control fails.
		sub1, _, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-def1", "a-def1", "", 5, 0, false, 1, "", "nacked", 8001)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "nack called"})
		nk1 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8001, []*proxypb.TraceSpan{sub1})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub1, sc1, pr1, nk1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExpectedOutcome: "nacked", ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeMissingSpan, "subscribe.concurrency_control.present")

		// 2. Missing scheduler fails.
		sub2, cc2, _, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-def2", "a-def2", "", 5, 0, false, 1, "", "nacked", 8002)
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "nack called"})
		nk2 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8002, []*proxypb.TraceSpan{sub2})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub2, cc2, pr2, nk2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExpectedOutcome: "nacked", ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeMissingSpan, "subscribe.scheduler.present")

		// 3. Missing process fails (even when bulkNackAllUnsettled-style spans are passed without InterruptKind).
		sub3, cc3, sc3, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-def3", "a-def3", "", 5, 0, false, 1, "", "nacked", 8003)
		nk3 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8003, []*proxypb.TraceSpan{sub3})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub3, cc3, sc3, nk3}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExpectedOutcome: "nacked", ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeMissingSpan, "subscribe.process.present")

		// 4. Process present without "nack called" fails under default zero InterruptKind.
		sub4, cc4, sc4, pr4, _ := makeGoSubscribeMessageSpans("p", "s", "m-def4", "a-def4", "", 5, 0, false, 1, "", "nacked", 8004)
		nk4 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8004, []*proxypb.TraceSpan{sub4})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub4, cc4, sc4, pr4, nk4}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, ExpectedOutcome: "nacked", ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeMissingEvent, "subscribe.process.event.nack_called")
	})

	t.Run("Positive: SubscribeInterruptPreDispatchCancel (0 and 1 cc, ALO, EOD error, aggregate fallback)", func(t *testing.T) {
		// 1. Real Go fc.acquire cancel: 0 cc, 0 sched, 0 process, ALO nack + aggregate top-level InterruptKind without Messages.
		sub0, _, _, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-pdc0", "a-pdc0", "", 5, 0, false, 1, "", "nacked", 8101)
		nk0 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8101, []*proxypb.TraceSpan{sub0})
		res0 := Verify(goCaps, []*proxypb.TraceSpan{sub0, nk0}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				InterruptKind:       SubscribeInterruptPreDispatchCancel,
				ExpectedOutcome:     "nacked",
				ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}},
			},
		})
		if !res0.Passed() {
			t.Fatalf("PreDispatchCancel aggregate fallback (0 cc) expected PASS, got:\n%s", res0.Format())
		}

		// 2. Proxy entries[i] (1 cc) + entries[i+1] (0 cc) + EOD nack error.
		subA, ccA, _, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-pdc1", "a-pdc1", "", 5, 0, true, 2, "", "", 8102)
		subB, _, _, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-pdc2", "a-pdc2", "", 5, 0, true, 2, "", "", 8103)
		for _, sub := range []*proxypb.TraceSpan{subA, subB} {
			sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			sub.StatusMessage = "eod nack failed"
			sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: "exception"})
		}
		nkEOD := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 8102, []*proxypb.TraceSpan{subA, subB})
		nkEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		nkEOD.StatusMessage = eodPartialBatchErrorMsg
		resBatch := Verify(goCaps, []*proxypb.TraceSpan{subA, ccA, subB, nkEOD}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:   BoolPtr(true),
				InterruptKind: SubscribeInterruptPreDispatchCancel,
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-pdc1", MessageID: "m-pdc1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "nack", ExpectError: true, EODPartialBatchFailure: true}}},
					{AckID: "a-pdc2", MessageID: "m-pdc2", BatchCount: 2, ExpectConcurrencyControlSpan: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "nack", ExpectError: true, EODPartialBatchFailure: true}}},
				},
			},
		})
		if !resBatch.Passed() {
			t.Fatalf("PreDispatchCancel (1 cc + 0 cc EOD error) expected PASS, got:\n%s", resBatch.Format())
		}
	})

	t.Run("Positive: SubscribeInterruptForcedShutdownNack (with/without process, CallbackTraceparent, overrides)", func(t *testing.T) {
		// 1. In-callback abort: cc + sched + process (NO "nack called") + CallbackTraceparent + ExpectProcessSpan: true.
		sub1, cc1, sc1, pr1, cb1 := makeGoSubscribeMessageSpans("p", "s", "m-fsn1", "a-fsn1", "", 5, 0, false, 1, "", "nacked", 8201)
		nk1 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8201, []*proxypb.TraceSpan{sub1})
		res1 := Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, nk1}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{{
					AckID:               "a-fsn1",
					MessageID:           "m-fsn1",
					CallbackTraceparent: cb1,
					InterruptKind:       SubscribeInterruptForcedShutdownNack,
					ExpectProcessSpan:   BoolPtr(true),
					Settlements:         []SettlementExpectation{{Operation: "nack"}},
				}},
			},
		})
		if !res1.Passed() {
			t.Fatalf("ForcedShutdownNack with process expected PASS, got:\n%s", res1.Format())
		}

		// 2. Queued in scheduler before startProcess: cc + sched + NO process -> PASS.
		sub2, cc2, sc2, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-fsn2", "a-fsn2", "", 5, 0, false, 1, "", "nacked", 8202)
		nk2 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8202, []*proxypb.TraceSpan{sub2})
		res2 := Verify(goCaps, []*proxypb.TraceSpan{sub2, cc2, sc2, nk2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         BoolPtr(false),
				InterruptKind:       SubscribeInterruptForcedShutdownNack,
				ExpectedOutcome:     "nacked",
				ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}},
			},
		})
		if !res2.Passed() {
			t.Fatalf("ForcedShutdownNack without process expected PASS, got:\n%s", res2.Format())
		}

		// 3. Explicit ExpectProcessSettlementEvent: true on normal path with forced mode override.
		sub3, cc3, sc3, pr3, _ := makeGoSubscribeMessageSpans("p", "s", "m-fsn3", "a-fsn3", "", 5, 0, false, 1, "", "nacked", 8203)
		pr3.Events = append(pr3.Events, &proxypb.SpanEvent{Name: "nack called"})
		nk3 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8203, []*proxypb.TraceSpan{sub3})
		res3 := Verify(goCaps, []*proxypb.TraceSpan{sub3, cc3, sc3, pr3, nk3}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:                 1,
				ExactlyOnce:                  BoolPtr(false),
				InterruptKind:                SubscribeInterruptForcedShutdownNack,
				ExpectProcessSettlementEvent: BoolPtr(true),
				ExpectedSettlements:          []SettlementExpectation{{Operation: "nack"}},
			},
		})
		if !res3.Passed() {
			t.Fatalf("ForcedShutdownNack ExpectProcessSettlementEvent=true expected PASS, got:\n%s", res3.Format())
		}
	})

	t.Run("Mixed batch deterministic matching across span and expectation permutations", func(t *testing.T) {
		// ForcedShutdownNack (cc+sched+no proc) + PreDispatchCancel (cc+no sched+no proc) without AckID/MessageID.
		subF, ccF, scF, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-mix", "a-f", "", 5, 0, false, 2, "", "nacked", 8301)
		subP, ccP, _, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-mix", "a-p", "", 5, 0, false, 2, "", "nacked", 8302)
		nk := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 8301, []*proxypb.TraceSpan{subF, subP})
		base := []*proxypb.TraceSpan{subF, ccF, scF, subP, ccP, nk}

		msgsOrderA := []SubscribeMessageExpectation{
			{MessageID: "m-mix", BatchCount: 2, InterruptKind: SubscribeInterruptPreDispatchCancel, Settlements: []SettlementExpectation{{Operation: "nack"}}},
			{MessageID: "m-mix", BatchCount: 2, InterruptKind: SubscribeInterruptForcedShutdownNack, Settlements: []SettlementExpectation{{Operation: "nack"}}},
		}
		msgsOrderB := []SubscribeMessageExpectation{msgsOrderA[1], msgsOrderA[0]}

		var baseline string
		for seed := int64(0); seed < 20; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := msgsOrderA
			if seed%2 == 1 {
				msgs = msgsOrderB
			}
			res := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("seed=%d mixed batch expected PASS, got:\n%s", seed, res.Format())
			}
			out := res.Format()
			if seed == 0 {
				baseline = out
			} else if out != baseline {
				t.Fatalf("seed=%d mixed batch Format() differed from seed=0", seed)
			}
		}
	})

	t.Run("Negative: forbidden scheduler/process under AllowExtra false & true, missing cc/sched, unexpected nack/ack called", func(t *testing.T) {
		for _, allowExtra := range []bool{false, true} {
			// 1. PreDispatchCancel with scheduler present -> FAIL subscribe.scheduler.unexpected.
			sub1, cc1, sc1, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-n1", "a-n1", "", 5, 0, false, 1, "", "nacked", 8401)
			nk1 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8401, []*proxypb.TraceSpan{sub1})
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, nk1}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: allowExtra,
				Subscribe:             &SubscribeExpectation{MessageCount: 1, InterruptKind: SubscribeInterruptPreDispatchCancel, ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
			}), CodeUnexpectedSpan, "subscribe.scheduler.unexpected")

			// 2. PreDispatchCancel with process present -> FAIL subscribe.process.unexpected.
			sub2, cc2, _, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-n2", "a-n2", "", 5, 0, false, 1, "", "nacked", 8402)
			nk2 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8402, []*proxypb.TraceSpan{sub2})
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub2, cc2, pr2, nk2}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: allowExtra,
				Subscribe:             &SubscribeExpectation{MessageCount: 1, InterruptKind: SubscribeInterruptPreDispatchCancel, ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
			}), CodeUnexpectedSpan, "subscribe.process.unexpected")

			// 3. PreDispatchCancel with ExpectConcurrencyControlSpan=false and cc present -> FAIL subscribe.concurrency_control.unexpected.
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, nk1}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: allowExtra,
				Subscribe:             &SubscribeExpectation{MessageCount: 1, InterruptKind: SubscribeInterruptPreDispatchCancel, ExpectConcurrencyControlSpan: BoolPtr(false), ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
			}), CodeUnexpectedSpan, "subscribe.concurrency_control.unexpected")
		}

		// 4. PreDispatchCancel with ExpectConcurrencyControlSpan=true and cc missing -> FAIL subscribe.concurrency_control.present.
		subNoCC, _, _, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-n3", "a-n3", "", 5, 0, false, 1, "", "nacked", 8403)
		nkNoCC := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8403, []*proxypb.TraceSpan{subNoCC})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subNoCC, nkNoCC}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, InterruptKind: SubscribeInterruptPreDispatchCancel, ExpectConcurrencyControlSpan: BoolPtr(true), ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeMissingSpan, "subscribe.concurrency_control.present")

		// 5. ForcedShutdownNack missing cc -> FAIL; missing sched -> FAIL; process with "nack called" or "ack called" -> FAIL.
		subF, ccF, scF, prF, _ := makeGoSubscribeMessageSpans("p", "s", "m-n4", "a-n4", "", 5, 0, false, 1, "", "nacked", 8404)
		nkF := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 8404, []*proxypb.TraceSpan{subF})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subF, scF, prF, nkF}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, InterruptKind: SubscribeInterruptForcedShutdownNack, ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeMissingSpan, "subscribe.concurrency_control.present")

		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subF, ccF, prF, nkF}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, InterruptKind: SubscribeInterruptForcedShutdownNack, ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeMissingSpan, "subscribe.scheduler.present")

		prNackEv := cloneSpans([]*proxypb.TraceSpan{prF})[0]
		prNackEv.Events = append(prNackEv.Events, &proxypb.SpanEvent{Name: "nack called"})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subF, ccF, scF, prNackEv, nkF}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, InterruptKind: SubscribeInterruptForcedShutdownNack, ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeUnexpectedSpan, "subscribe.process.event.unexpected_nack_called")

		prAckEv := cloneSpans([]*proxypb.TraceSpan{prF})[0]
		prAckEv.Events = append(prAckEv.Events, &proxypb.SpanEvent{Name: "ack called"})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subF, ccF, scF, prAckEv, nkF}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{MessageCount: 1, InterruptKind: SubscribeInterruptForcedShutdownNack, ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}}},
		}), CodeUnexpectedSpan, "subscribe.process.event.unexpected_ack_called")
	})
}

func refSolveBipartiteMatching(numLeft, numRight int, rightCap []int, leftGroup []int, edges []bipartiteEdge) []int {
	assign := make([]int, numLeft)
	for i := range assign {
		assign[i] = -1
	}
	if numLeft <= 0 || numRight <= 0 || len(edges) == 0 {
		return assign
	}

	bestByPair := make(map[[2]int]int64)
	for _, e := range edges {
		if e.left < 0 || e.left >= numLeft || e.right < 0 || e.right >= numRight {
			continue
		}
		if rightCap != nil && (e.right >= len(rightCap) || rightCap[e.right] <= 0) {
			continue
		}
		w := e.weight
		if w < 0 {
			w = 0
		}
		k := [2]int{e.left, e.right}
		if prev, ok := bestByPair[k]; !ok || w > prev {
			bestByPair[k] = w
		}
	}
	if len(bestByPair) == 0 {
		return assign
	}

	type rGroupKey struct {
		group int
		right int
	}
	var groupCount map[int]int
	midNode := make(map[rGroupKey]int)
	if len(leftGroup) == numLeft {
		groupCount = make(map[int]int)
		for _, g := range leftGroup {
			groupCount[g]++
		}
		var midKeys []rGroupKey
		midSeen := make(map[rGroupKey]bool)
		for k := range bestByPair {
			g := leftGroup[k[0]]
			if groupCount[g] > 1 {
				rk := rGroupKey{group: g, right: k[1]}
				if !midSeen[rk] {
					midSeen[rk] = true
					midKeys = append(midKeys, rk)
				}
			}
		}
		for idx, rk := range midKeys {
			midNode[rk] = 1 + numLeft + idx
		}
	}

	numMid := len(midNode)
	rightBase := 1 + numLeft + numMid
	sink := rightBase + numRight
	numNodes := sink + 1
	const source = 0

	type rEdge struct {
		to, rev, cap int
		cw, ct       int64
	}
	adj := make([][]rEdge, numNodes)
	addEdge := func(u, v, capVal int, cw, ct int64) {
		f := len(adj[u])
		r := len(adj[v])
		adj[u] = append(adj[u], rEdge{to: v, rev: r, cap: capVal, cw: cw, ct: ct})
		adj[v] = append(adj[v], rEdge{to: u, rev: f, cap: 0, cw: -cw, ct: -ct})
	}
	for i := 0; i < numLeft; i++ {
		addEdge(source, 1+i, 1, 0, 0)
	}
	midAdded := make([]bool, numMid)
	for k, w := range bestByPair {
		l, r := k[0], k[1]
		tie := int64(numLeft-l) * int64(numRight-r)
		if len(midNode) > 0 && groupCount[leftGroup[l]] > 1 {
			mIdx := midNode[rGroupKey{group: leftGroup[l], right: r}]
			addEdge(1+l, mIdx, 1, -w, -tie)
			off := mIdx - (1 + numLeft)
			if !midAdded[off] {
				midAdded[off] = true
				addEdge(mIdx, rightBase+r, 1, 0, 0)
			}
		} else {
			addEdge(1+l, rightBase+r, 1, -w, -tie)
		}
	}
	for r := 0; r < numRight; r++ {
		c := 1
		if rightCap != nil {
			if r < len(rightCap) {
				c = rightCap[r]
			} else {
				c = 0
			}
		}
		if c > 0 {
			addEdge(rightBase+r, sink, c, 0, 0)
		}
	}

	// Reference SPFA SSAP: handles any weight scale deterministically without potential approximation.
	distW := make([]int64, numNodes)
	distT := make([]int64, numNodes)
	inQ := make([]bool, numNodes)
	valid := make([]bool, numNodes)
	pNode := make([]int, numNodes)
	pEdge := make([]int, numNodes)
	queue := make([]int, 0, numNodes*2)

	for {
		for i := 0; i < numNodes; i++ {
			valid[i] = false
			inQ[i] = false
			pNode[i] = -1
			pEdge[i] = -1
		}
		queue = queue[:0]
		valid[source] = true
		distW[source] = 0
		distT[source] = 0
		queue = append(queue, source)
		inQ[source] = true

		for head := 0; head < len(queue); head++ {
			u := queue[head]
			inQ[u] = false
			for ei, e := range adj[u] {
				if e.cap <= 0 {
					continue
				}
				cw := distW[u] + e.cw
				ct := distT[u] + e.ct
				if !valid[e.to] || cw < distW[e.to] || (cw == distW[e.to] && ct < distT[e.to]) {
					valid[e.to] = true
					distW[e.to] = cw
					distT[e.to] = ct
					pNode[e.to] = u
					pEdge[e.to] = ei
					if !inQ[e.to] {
						queue = append(queue, e.to)
						inQ[e.to] = true
					}
				}
			}
		}
		if !valid[sink] {
			break
		}
		for v := sink; v != source; v = pNode[v] {
			u := pNode[v]
			ei := pEdge[v]
			rev := adj[u][ei].rev
			adj[u][ei].cap--
			adj[v][rev].cap++
		}
	}

	for i := 0; i < numLeft; i++ {
		u := 1 + i
		for _, e := range adj[u] {
			if e.cap != 0 {
				continue
			}
			if e.to >= rightBase && e.to < sink {
				assign[i] = e.to - rightBase
				break
			}
			if e.to >= 1+numLeft && e.to < rightBase {
				for _, me := range adj[e.to] {
					if me.cap == 0 && me.to >= rightBase && me.to < sink {
						assign[i] = me.to - rightBase
						break
					}
				}
				if assign[i] >= 0 {
					break
				}
			}
		}
	}
	return assign
}

func evalMatchingObjective(numLeft, numRight int, rightCap []int, leftGroup []int, edges []bipartiteEdge, assign []int) (card int, sumW int64, sumT int64, err error) {
	bestW := make(map[[2]int]int64)
	for _, e := range edges {
		if e.left < 0 || e.left >= numLeft || e.right < 0 || e.right >= numRight {
			continue
		}
		if rightCap != nil && (e.right >= len(rightCap) || rightCap[e.right] <= 0) {
			continue
		}
		w := e.weight
		if w < 0 {
			w = 0
		}
		k := [2]int{e.left, e.right}
		if prev, ok := bestW[k]; !ok || w > prev {
			bestW[k] = w
		}
	}
	usedRight := make([]int, numRight)
	seenGroupRight := make(map[[2]int]bool)
	for l, r := range assign {
		if r < 0 {
			continue
		}
		if r >= numRight {
			return 0, 0, 0, fmt.Errorf("invalid right index %d", r)
		}
		w, ok := bestW[[2]int{l, r}]
		if !ok {
			return 0, 0, 0, fmt.Errorf("assigned non-existent edge (%d,%d)", l, r)
		}
		capLim := 1
		if rightCap != nil {
			if r < len(rightCap) {
				capLim = rightCap[r]
			} else {
				capLim = 0
			}
		}
		usedRight[r]++
		if usedRight[r] > capLim {
			return 0, 0, 0, fmt.Errorf("right node %d exceeded cap %d", r, capLim)
		}
		if len(leftGroup) == numLeft {
			gk := [2]int{leftGroup[l], r}
			if seenGroupRight[gk] {
				return 0, 0, 0, fmt.Errorf("group %d reused right %d", leftGroup[l], r)
			}
			seenGroupRight[gk] = true
		}
		card++
		sumW += w
		sumT += int64(numLeft-l) * int64(numRight-r)
	}
	return card, sumW, sumT, nil
}

func TestSolveBipartiteMatching_EquivalenceAgainstReferenceSSAP(t *testing.T) {
	const totalCases = 1500
	rng := rand.New(rand.NewSource(20261115))

	for tc := 0; tc < totalCases; tc++ {
		var numLeft, numRight int
		var rightCap []int
		var leftGroup []int
		var edges []bipartiteEdge

		switch tc % 15 {
		case 0: // Sparse random
			numLeft = 1 + rng.Intn(25)
			numRight = 1 + rng.Intn(25)
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					if rng.Intn(4) == 0 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(500))})
					}
				}
			}
		case 1: // Dense complete / rectangular
			numLeft = 1 + rng.Intn(22)
			numRight = 1 + rng.Intn(22)
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(100))})
				}
			}
		case 2: // Dense incomplete (missing edges, some perfect, some imperfect)
			numLeft = 2 + rng.Intn(20)
			numRight = numLeft
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					if rng.Intn(10) < 7 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(1000))})
					}
				}
			}
		case 3: // Isolated rows/cols inside square dense
			numLeft = 4 + rng.Intn(16)
			numRight = 4 + rng.Intn(16)
			skipL := rng.Intn(numLeft)
			skipR := rng.Intn(numRight)
			for i := 0; i < numLeft; i++ {
				if i == skipL {
					continue
				}
				for j := 0; j < numRight; j++ {
					if j == skipR {
						continue
					}
					if rng.Intn(10) < 8 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(50))})
					}
				}
			}
		case 4: // Uniform weights
			numLeft = 1 + rng.Intn(25)
			numRight = 1 + rng.Intn(25)
			w := int64(rng.Intn(1_000_000))
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					if rng.Intn(5) < 4 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: w})
					}
				}
			}
		case 5: // Large weights (> packed int64 bounds -> cost2Pair/cost3 fallback)
			numLeft = 2 + rng.Intn(16)
			numRight = 2 + rng.Intn(16)
			base := int64(math.MaxInt64 / 100)
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					if rng.Intn(5) < 4 {
						w := base - int64(rng.Intn(1_000_000))
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: w})
					}
				}
			}
		case 6: // rightCap with zeros and multi-capacities (numRight <= 16)
			numLeft = 4 + rng.Intn(25)
			numRight = 2 + rng.Intn(12)
			rightCap = make([]int, numRight)
			for r := range rightCap {
				rightCap[r] = rng.Intn(5)
			}
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					if rng.Intn(3) != 0 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(50))})
					}
				}
			}
		case 7: // rightCap expansion path (numRight > 16, cap ~ 2)
			numLeft = 20 + rng.Intn(20)
			numRight = 17 + rng.Intn(8)
			rightCap = make([]int, numRight)
			for r := range rightCap {
				rightCap[r] = 1 + rng.Intn(2)
			}
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					if rng.Intn(4) != 0 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(100))})
					}
				}
			}
		case 8: // leftGroup + rightCap (multi-member & negative group IDs)
			numLeft = 4 + rng.Intn(20)
			numRight = 2 + rng.Intn(10)
			rightCap = make([]int, numRight)
			for r := range rightCap {
				rightCap[r] = 1 + rng.Intn(3)
			}
			leftGroup = make([]int, numLeft)
			numG := 1 + rng.Intn(numLeft/2+1)
			for i := range leftGroup {
				leftGroup[i] = rng.Intn(numG) - 2
			}
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					if rng.Intn(3) != 0 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(200))})
					}
				}
			}
		case 9: // Duplicate, OOB, negative-weight, and unsorted edges + >64 edges (counting sort)
			numLeft = 10 + rng.Intn(10)
			numRight = 10 + rng.Intn(10)
			for k := 0; k < 120; k++ {
				edges = append(edges, bipartiteEdge{
					left:   rng.Intn(numLeft+4) - 2,
					right:  rng.Intn(numRight+4) - 2,
					weight: int64(rng.Intn(200) - 40),
				})
			}
		case 10: // Shortest-length rightCap slice (len(rightCap) < numRight) & len(leftGroup) != numLeft ignored
			numLeft = 6 + rng.Intn(10)
			numRight = 6 + rng.Intn(10)
			rightCap = []int{2, 0, 1}
			if rng.Intn(2) == 0 {
				leftGroup = []int{0, 1} // mismatched len ignored
			}
			for i := 0; i < numLeft; i++ {
				for j := 0; j < numRight; j++ {
					if rng.Intn(2) == 0 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(50))})
					}
				}
			}
		case 11: // Sparse large rectangular triggering solveSparseHungarian (activeL*activeR > 4M? no, <25% density)
			numLeft = 20 + rng.Intn(15)
			numRight = 20 + rng.Intn(15)
			for i := 0; i < numLeft; i++ {
				// ~10% density (<25% sparse threshold)
				for c := 0; c < 3; c++ {
					edges = append(edges, bipartiteEdge{left: i, right: rng.Intn(numRight), weight: int64(rng.Intn(100))})
				}
			}
		case 12: // Sparse extreme weights (<25% density -> solveSparseHungarian !packOK cost3)
			numLeft = 20 + rng.Intn(12)
			numRight = 20 + rng.Intn(12)
			base := int64(math.MaxInt64 / 100)
			for i := 0; i < numLeft; i++ {
				for c := 0; c < 3; c++ {
					edges = append(edges, bipartiteEdge{
						left:   i,
						right:  rng.Intn(numRight),
						weight: base - int64(rng.Intn(1_000_000)),
					})
				}
			}
		case 13: // Dense incomplete no-perfect medium-large weights (packOK && !packSkipOK -> cost2Pair in solveHungarianDense)
			numLeft = 12 + rng.Intn(8)
			numRight = numLeft
			base := int64(5_000_000_000_000)
			for i := 0; i < numLeft; i++ {
				if i < 2 {
					edges = append(edges, bipartiteEdge{left: i, right: 0, weight: base + int64(rng.Intn(1_000_000))})
					continue
				}
				for j := 1; j < numRight; j++ {
					if rng.Intn(10) < 8 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: base + int64(rng.Intn(1_000_000))})
					}
				}
			}
		case 14: // Dense incomplete no-perfect extreme weights (!packOK -> cost3 in solveHungarianDense)
			numLeft = 10 + rng.Intn(10)
			numRight = numLeft
			base := int64(math.MaxInt64 / 100)
			for i := 0; i < numLeft; i++ {
				if i < 2 {
					edges = append(edges, bipartiteEdge{left: i, right: 0, weight: base - int64(rng.Intn(1_000_000))})
					continue
				}
				for j := 1; j < numRight; j++ {
					if rng.Intn(10) < 8 {
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: base - int64(rng.Intn(1_000_000))})
					}
				}
			}
		}

		// Also randomly shuffle edges on even test cases so fast-path & sort-path both get exercised!
		permEdges := make([]bipartiteEdge, len(edges))
		copy(permEdges, edges)
		rng.Shuffle(len(permEdges), func(i, j int) { permEdges[i], permEdges[j] = permEdges[j], permEdges[i] })

		got := solveBipartiteMatching(numLeft, numRight, rightCap, leftGroup, edges)
		gotPerm := solveBipartiteMatching(numLeft, numRight, rightCap, leftGroup, permEdges)
		if !slices.Equal(got, gotPerm) {
			t.Fatalf("tc=%d permutation invariance failed:\ngot=%v\nperm=%v", tc, got, gotPerm)
		}

		ref := refSolveBipartiteMatching(numLeft, numRight, rightCap, leftGroup, edges)
		gCard, gW, gT, gErr := evalMatchingObjective(numLeft, numRight, rightCap, leftGroup, edges, got)
		if gErr != nil {
			t.Fatalf("tc=%d invalid solver assignment: %v\ngot=%v", tc, gErr, got)
		}
		rCard, rW, rT, rErr := evalMatchingObjective(numLeft, numRight, rightCap, leftGroup, edges, ref)
		if rErr != nil {
			t.Fatalf("tc=%d invalid ref assignment: %v", tc, rErr)
		}
		if gCard != rCard || gW != rW || gT != rT {
			t.Fatalf("tc=%d mismatch vs reference SSAP:\n  got: card=%d w=%d tie=%d assign=%v\n  ref: card=%d w=%d tie=%d assign=%v",
				tc, gCard, gW, gT, got, rCard, rW, rT, ref)
		}
	}
}

func TestScale1002_SolverAndVerifySubSecondUnderRace(t *testing.T) {
	const N = 1002

	checkTimed := func(t *testing.T, name string, fn func()) {
		t.Helper()
		start := time.Now()
		fn()
		elapsed := time.Since(start)
		t.Logf("%s: %v", name, elapsed)
		if elapsed > 10*time.Second {
			t.Fatalf("%s took %v (> 10s budget)", name, elapsed)
		}
	}

	t.Run("Solver 1002-scale workloads", func(t *testing.T) {
		// 1. diag-id
		checkTimed(t, "diag-id", func() {
			edges := make([]bipartiteEdge, 0, N*N)
			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					w := int64(0)
					if i == j {
						w = 100_000
					}
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: w})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			for i := 0; i < N; i++ {
				if got[i] != i {
					t.Fatalf("diag-id got[%d]=%d, want %d", i, got[i], i)
				}
			}
		})

		// 2. diag-rev
		checkTimed(t, "diag-rev", func() {
			edges := make([]bipartiteEdge, 0, N*N)
			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					w := int64(0)
					if j == N-1-i {
						w = 100_000
					}
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: w})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			for i := 0; i < N; i++ {
				if got[i] != N-1-i {
					t.Fatalf("diag-rev got[%d]=%d, want %d", i, got[i], N-1-i)
				}
			}
		})

		// 3. uniform
		checkTimed(t, "uniform", func() {
			edges := make([]bipartiteEdge, 0, N*N)
			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: 50_000})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			for i := 0; i < N; i++ {
				if got[i] != i {
					t.Fatalf("uniform got[%d]=%d, want %d", i, got[i], i)
				}
			}
		})

		// 4. uniform large weight (> packed int64 bounds -> cost2Pair path)
		checkTimed(t, "uniform_large_weight", func() {
			edges := make([]bipartiteEdge, 0, N*N)
			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: 2_000_000})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			for i := 0; i < N; i++ {
				if got[i] != i {
					t.Fatalf("uniform_large_weight got[%d]=%d, want %d", i, got[i], i)
				}
			}
		})

		// 5. 1000+2 (1000 uniform + 2 distinct)
		checkTimed(t, "1000+2", func() {
			edges := make([]bipartiteEdge, 0, N*N)
			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					w := int64(50_000)
					if (i == 0 && j == 1001) || (i == 1 && j == 1000) {
						w = 500_000
					}
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: w})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			if got[0] != 1001 || got[1] != 1000 {
				t.Fatalf("1000+2 got[0..1]=[%d %d], want [1001 1000]", got[0], got[1])
			}
		})

		// 6. random
		checkTimed(t, "random", func() {
			rng := rand.New(rand.NewSource(99))
			edges := make([]bipartiteEdge, 0, N*N)
			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: int64(rng.Intn(1_000_000))})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			seen := make([]bool, N)
			for i, r := range got {
				if r < 0 || r >= N || seen[r] {
					t.Fatalf("random invalid assignment got[%d]=%d", i, r)
				}
				seen[r] = true
			}
		})

		// 7. sparse1 (1002 deg-1 edges)
		checkTimed(t, "sparse1", func() {
			edges := make([]bipartiteEdge, 0, N)
			for i := 0; i < N; i++ {
				edges = append(edges, bipartiteEdge{left: i, right: N - 1 - i, weight: 300_000})
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			for i := 0; i < N; i++ {
				if got[i] != N-1-i {
					t.Fatalf("sparse1 got[%d]=%d, want %d", i, got[i], N-1-i)
				}
			}
		})

		// 8. 2blocks (K_{501,501} + K_{501,501})
		checkTimed(t, "2blocks", func() {
			edges := make([]bipartiteEdge, 0, 2*501*501)
			for i := 0; i < N; i++ {
				start, end := 0, 501
				if i >= 501 {
					start, end = 501, N
				}
				for j := start; j < end; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: 100})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			for i := 0; i < N; i++ {
				if got[i] != i {
					t.Fatalf("2blocks got[%d]=%d, want %d", i, got[i], i)
				}
			}
		})

		// 9. 1002x1005 rectangular complete
		checkTimed(t, "1002x1005", func() {
			edges := make([]bipartiteEdge, 0, N*1005)
			for i := 0; i < N; i++ {
				for j := 0; j < 1005; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: 50_000})
				}
			}
			got := solveBipartiteMatching(N, 1005, nil, nil, edges)
			for i := 0; i < N; i++ {
				if got[i] != i {
					t.Fatalf("1002x1005 got[%d]=%d, want %d", i, got[i], i)
				}
			}
		})

		// 10. 1001x1002 inside 1002x1002 (1 zero-degree row)
		checkTimed(t, "1001x1002_in_1002x1002", func() {
			edges := make([]bipartiteEdge, 0, 1001*N)
			for i := 0; i < N; i++ {
				if i == 500 {
					continue
				}
				for j := 0; j < N; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: 10_000})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			if got[500] != -1 {
				t.Fatalf("expected got[500]=-1, got %d", got[500])
			}
			matchedCnt := 0
			for _, r := range got {
				if r >= 0 {
					matchedCnt++
				}
			}
			if matchedCnt != 1001 {
				t.Fatalf("expected 1001 matched, got %d", matchedCnt)
			}
		})

		// 11. incomplete dense with perfect matching (Hopcroft-Karp + solveHungarianPerfectOrComplete)
		checkTimed(t, "incomplete_perfect", func() {
			edges := make([]bipartiteEdge, 0, N*N/2)
			for i := 0; i < N; i++ {
				for j := 0; j < N; j++ {
					if i == j || (i+j)%2 == 0 {
						w := int64(10)
						if i == j {
							w = 100
						}
						edges = append(edges, bipartiteEdge{left: i, right: j, weight: w})
					}
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			for i := 0; i < N; i++ {
				if got[i] != i {
					t.Fatalf("incomplete_perfect got[%d]=%d, want %d", i, got[i], i)
				}
			}
		})

		// 12. incomplete dense WITHOUT perfect matching (solveHungarianDense)
		checkTimed(t, "incomplete_no_perfect", func() {
			edges := make([]bipartiteEdge, 0, N*N)
			for i := 0; i < N; i++ {
				if i < 2 {
					edges = append(edges, bipartiteEdge{left: i, right: 0, weight: int64(100 + i*100)})
					continue
				}
				for j := 1; j < N; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: 50})
				}
			}
			got := solveBipartiteMatching(N, N, nil, nil, edges)
			if got[0] != -1 || got[1] != 0 {
				t.Fatalf("incomplete_no_perfect got[0..1]=[%d %d], want [-1 0]", got[0], got[1])
			}
		})

		// 13. cap501x2 (1002x501, rightCap=2)
		checkTimed(t, "cap501x2", func() {
			rightCap := make([]int, 501)
			for r := range rightCap {
				rightCap[r] = 2
			}
			edges := make([]bipartiteEdge, 0, N*501)
			for i := 0; i < N; i++ {
				for j := 0; j < 501; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: 1000})
				}
			}
			got := solveBipartiteMatching(N, 501, rightCap, nil, edges)
			counts := make([]int, 501)
			for i, r := range got {
				if r < 0 || r >= 501 {
					t.Fatalf("cap501x2 invalid got[%d]=%d", i, r)
				}
				counts[r]++
			}
			for r, c := range counts {
				if c != 2 {
					t.Fatalf("cap501x2 right[%d] count=%d, want 2", r, c)
				}
			}
		})

		// 14. cap2_1001 (1002x2, rightCap=[1, 1001])
		checkTimed(t, "cap2_1001", func() {
			rightCap := []int{1, 1001}
			edges := make([]bipartiteEdge, 0, N*2)
			for i := 0; i < N; i++ {
				edges = append(edges,
					bipartiteEdge{left: i, right: 0, weight: int64(i)},
					bipartiteEdge{left: i, right: 1, weight: 100},
				)
			}
			got := solveBipartiteMatching(N, 2, rightCap, nil, edges)
			if got[N-1] != 0 {
				t.Fatalf("cap2_1001 expected got[%d]=0, got %d", N-1, got[N-1])
			}
		})

		// 15. leftGroup (200x100, cap=2, 100 groups of 2)
		checkTimed(t, "leftGroup_200x100", func() {
			const gL, gR = 200, 100
			rightCap := make([]int, gR)
			for r := range rightCap {
				rightCap[r] = 2
			}
			leftGroup := make([]int, gL)
			for i := 0; i < gL; i++ {
				leftGroup[i] = i / 2
			}
			edges := make([]bipartiteEdge, 0, gL*gR)
			for i := 0; i < gL; i++ {
				for j := 0; j < gR; j++ {
					edges = append(edges, bipartiteEdge{left: i, right: j, weight: 100})
				}
			}
			got := solveBipartiteMatching(gL, gR, rightCap, leftGroup, edges)
			for i := 0; i < gL; i += 2 {
				if got[i] < 0 || got[i+1] < 0 || got[i] == got[i+1] {
					t.Fatalf("leftGroup pair (%d,%d) violated exclusivity: %d, %d", i, i+1, got[i], got[i+1])
				}
			}
		})
	})

	t.Run("End-to-end Verify 1002-message Subscribe+Ack (and shuffled permutation) + Publish with/without WireTraceparent", func(t *testing.T) {
		goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

		// 1. Over1000BatchSplitWithDuplicateAckID shape: 1 + 1001 = 1002 deliveries + ack(1) + ack(1001) without WireTraceparent.
		var subSpans []*proxypb.TraceSpan
		var subBatch1, subBatch2 []*proxypb.TraceSpan
		subMsgs := make([]SubscribeMessageExpectation, N)
		for i := 0; i < N; i++ {
			sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", fmt.Sprintf("msg-%d", i), fmt.Sprintf("ack-%d", i), "", 10, 0, false, 1, "", "acked", 10000+i)
			pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
			subSpans = append(subSpans, sub, cc, sc, pr)
			if i == 0 {
				subBatch1 = append(subBatch1, sub)
			} else {
				subBatch2 = append(subBatch2, sub)
			}
			subMsgs[i] = SubscribeMessageExpectation{
				MessageID:   fmt.Sprintf("msg-%d", i),
				AckID:       fmt.Sprintf("ack-%d", i),
				BodySize:    Int64Ptr(10),
				BatchCount:  1,
				Settlements: []SettlementExpectation{{Operation: "ack"}},
			}
		}
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 10000, subBatch1)
		ack1001 := makeGoSettlementSpan("p", "s", "ack", 1001, 0, false, 10001, subBatch2)
		subSpans = append(subSpans, ack1, ack1001)
		subExp := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages:    subMsgs,
			},
		}

		var baselineFmt string
		checkTimed(t, "Verify_Subscribe_1002_Ordered", func() {
			res := Verify(goCaps, subSpans, subExp)
			if !res.Passed() {
				t.Fatalf("Verify 1002 Subscribe expected PASS, got:\n%s", res.Format())
			}
			baselineFmt = res.Format()
		})

		checkTimed(t, "Verify_Subscribe_1002_ShuffledPermutations", func() {
			for seed := int64(1); seed <= 2; seed++ {
				perm := cloneSpans(subSpans)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				for _, s := range perm {
					if len(s.Links) > 1 {
						rng.Shuffle(len(s.Links), func(i, j int) { s.Links[i], s.Links[j] = s.Links[j], s.Links[i] })
					}
				}
				res := Verify(goCaps, perm, subExp)
				if !res.Passed() {
					t.Fatalf("seed=%d Verify 1002 Subscribe permutation expected PASS, got:\n%s", seed, res.Format())
				}
				if res.Format() != baselineFmt {
					t.Fatalf("seed=%d Verify 1002 Subscribe Format() differed from baseline", seed)
				}
			}
		})

		// 2. Publish 1002 messages (2 batches: 1000 + 2) WITH and WITHOUT WireTraceparent.
		pubB1 := makeGoPublishBatchSpan("p", "t", 1000, 20001)
		pubB2 := makeGoPublishBatchSpan("p", "t", 2, 20002)
		var pubSpans []*proxypb.TraceSpan
		pubMsgsWithTP := make([]PublishMessageExpectation, N)
		pubMsgsNoTP := make([]PublishMessageExpectation, N)
		for i := 0; i < N; i++ {
			pb := pubB1
			bSize := int64(1000)
			if i >= 1000 {
				pb = pubB2
				bSize = 2
			}
			msgID := fmt.Sprintf("pm-%d", i)
			cs, fc, b, wtp := makeGoPublishMessageSpans("p", "t", msgID, "", 16, 20000+i, pb, bSize)
			pubSpans = append(pubSpans, cs, fc, b)
			pubMsgsWithTP[i] = PublishMessageExpectation{
				MessageID:       msgID,
				BodySize:        Int64Ptr(16),
				WireTraceparent: wtp,
			}
			pubMsgsNoTP[i] = PublishMessageExpectation{
				MessageID: msgID,
				BodySize:  Int64Ptr(16),
			}
		}
		pubSpans = append(pubSpans, pubB1, pubB2)

		checkTimed(t, "Verify_Publish_1002_WithWireTP", func() {
			res := Verify(goCaps, pubSpans, &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					BatchCount: 2,
					Batches:    []PublishBatchExpectation{{MessageCount: 2}, {MessageCount: 1000}},
					Messages:   pubMsgsWithTP,
				},
			})
			if !res.Passed() {
				t.Fatalf("Verify 1002 Publish w/ WireTP expected PASS, got:\n%s", res.Format())
			}
		})

		checkTimed(t, "Verify_Publish_1002_NoWireTP", func() {
			res := Verify(goCaps, pubSpans, &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					BatchCount: 2,
					Batches:    []PublishBatchExpectation{{MessageCount: 2}, {MessageCount: 1000}},
					Messages:   pubMsgsNoTP,
				},
			})
			if !res.Passed() {
				t.Fatalf("Verify 1002 Publish w/o WireTP expected PASS, got:\n%s", res.Format())
			}
		})
	})
}

func TestSolveBipartiteMatching_PackingAndFindMatchingLinkEdgeCases(t *testing.T) {
	if got := findMatchingLink(nil, nil); got != nil {
		t.Fatalf("expected nil for nil spans, got %+v", got)
	}
	from := &proxypb.TraceSpan{TraceId: testTraceID(1), SpanId: testSpanID(1), Name: "from"}
	target := &proxypb.TraceSpan{TraceId: testTraceID(2), SpanId: testSpanID(2), Name: "target"}
	if got := findMatchingLink(from, target); got != nil {
		t.Fatalf("expected nil when unlinked, got %+v", got)
	}
	from.Links = []*proxypb.SpanLink{{TraceId: testTraceID(2), SpanId: testSpanID(2)}}
	if got := findMatchingLink(from, target); got == nil || got.GetSpanId() != testSpanID(2) {
		t.Fatalf("expected matching link, got %+v", got)
	}

	if _, ok := canPackWeightAndTie(10, 0, 5, 5, 5); ok {
		t.Fatal("expected false for origNumL <= 0")
	}
	if _, ok := canPackWeightAndTie(10, math.MaxInt64/2, 10, 5, 5); ok {
		t.Fatal("expected false for origNumL*origNumR overflow")
	}
	if _, ok := canPackWeightAndTie(math.MaxInt64/10, 100, 100, 100, 10); ok {
		t.Fatal("expected false for maxW*tieScale overflow")
	}
	if _, ok := canPackWeightAndTie(1_000_000, 10, 10, math.MaxInt/2, 10); ok {
		t.Fatal("expected false for maxCard*maxTie overflow")
	}
	if _, _, ok := canPackDenseWithSkip(10, 0, 5, 5, 5, 5, 5); ok {
		t.Fatal("expected false for canPackDenseWithSkip when base pack fails")
	}
	if _, _, ok := canPackDenseWithSkip(10_000_000_000_000, 15, 15, 15, 15, 15, 15); ok {
		t.Fatal("expected false for skipPenalty*safetyFactor overflow")
	}
	if _, _, ok := canPackDenseWithSkip(math.MaxInt64/(48*961)-10, 4, 4, 4, 4, 60, 4); ok {
		t.Fatal("expected false for maxCard*maxEdgeAbs overflow")
	}
}

func TestWave7Regressions_ExactDistinctSettleProcessEventsAndMinInt64(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("Challenge 2A: Subscribe multi-settlement [{ack},{ack}] vs [{ack}] distinct-span scoring", func(t *testing.T) {
		sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m-dup", "a-dup-0", "", 5, 0, true, 1, "", "acked", 9001)
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-dup", "a-dup-1", "", 5, 0, true, 1, "", "acked", 9002)
		pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod0 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 9000, []*proxypb.TraceSpan{sub0})
		mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 9001, []*proxypb.TraceSpan{sub1})
		ack0 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9002, []*proxypb.TraceSpan{sub0})
		ack1a := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9003, []*proxypb.TraceSpan{sub1})
		ack1b := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9004, []*proxypb.TraceSpan{sub1})

		base := []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, mod0, mod1, ack0, ack1a, ack1b}
		mTwo := SubscribeMessageExpectation{
			MessageID:   "m-dup",
			Settlements: []SettlementExpectation{{Operation: "ack"}, {Operation: "ack"}},
		}
		mOne := SubscribeMessageExpectation{
			MessageID:   "m-dup",
			Settlements: []SettlementExpectation{{Operation: "ack"}},
		}
		msgsA := []SubscribeMessageExpectation{mTwo, mOne}
		msgsB := []SubscribeMessageExpectation{mOne, mTwo}

		var baseline string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := msgsA
			if seed%2 == 1 {
				msgs = msgsB
			}
			res := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(true), Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseline = res.Format()
			} else if res.Format() != baseline {
				t.Fatalf("seed=%d Format() differed from baseline", seed)
			}
			for _, s := range perm {
				if len(s.Links) > 1 {
					rng.Shuffle(len(s.Links), func(i, j int) { s.Links[i], s.Links[j] = s.Links[j], s.Links[i] })
				}
			}
			if resLink := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(true), Messages: msgs},
			}); !resLink.Passed() {
				t.Fatalf("seed=%d link-shuffled expected PASS, got:\n%s", seed, resLink.Format())
			}
		}
	})

	t.Run("Challenge 2B: Subscribe multi-modack [modack(any), modack(60)] vs [modack(any), modack(10)]", func(t *testing.T) {
		// sub0 chronologically has modack(60) BEFORE modack(10); sub1 has [modack(10), modack(10)].
		sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m-mod", "a-mod-0", "", 5, 0, false, 1, "", "acked", 9101)
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-mod", "a-mod-1", "", 5, 0, false, 1, "", "acked", 9102)
		pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod0a := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 9101, []*proxypb.TraceSpan{sub0}) // earlier StartTime
		mod0b := makeGoSettlementSpan("p", "s", "modack", 1, 10, false, 9102, []*proxypb.TraceSpan{sub0})
		ack0 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9103, []*proxypb.TraceSpan{sub0})
		mod1a := makeGoSettlementSpan("p", "s", "modack", 1, 10, false, 9104, []*proxypb.TraceSpan{sub1})
		mod1b := makeGoSettlementSpan("p", "s", "modack", 1, 10, false, 9105, []*proxypb.TraceSpan{sub1})
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9106, []*proxypb.TraceSpan{sub1})

		base := []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, mod0a, mod0b, ack0, mod1a, mod1b, ack1}
		m60 := SubscribeMessageExpectation{
			MessageID:   "m-mod",
			Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "modack", AckDeadlineSeconds: 60}, {Operation: "ack"}},
		}
		m10 := SubscribeMessageExpectation{
			MessageID:   "m-mod",
			Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "modack", AckDeadlineSeconds: 10}, {Operation: "ack"}},
		}
		var baseline string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := []SubscribeMessageExpectation{m60, m10}
			if seed%2 == 1 {
				msgs = []SubscribeMessageExpectation{m10, m60}
			}
			res := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("seed=%d multi-modack expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseline = res.Format()
			} else if res.Format() != baseline {
				t.Fatalf("seed=%d multi-modack Format() differed from baseline", seed)
			}
			for _, s := range perm {
				if len(s.Links) > 1 {
					rng.Shuffle(len(s.Links), func(i, j int) { s.Links[i], s.Links[j] = s.Links[j], s.Links[i] })
				}
			}
			if resLink := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
			}); !resLink.Passed() {
				t.Fatalf("seed=%d multi-modack link-shuffled expected PASS, got:\n%s", seed, resLink.Format())
			}
		}

		// Hierarchy check: MessageID (200_000) must beat capped totalSettleBonus (<= 180_000).
		subH0, ccH0, scH0, prH0, _ := makeGoSubscribeMessageSpans("p", "s", "m-hi-0", "a-hi-0", "", 5, 0, false, 1, "", "acked", 9151)
		subH1, ccH1, scH1, prH1, _ := makeGoSubscribeMessageSpans("p", "s", "m-hi-1", "a-hi-1", "", 5, 0, false, 1, "", "acked", 9152)
		prH0.Events = append(prH0.Events, &proxypb.SpanEvent{Name: "ack called"})
		prH1.Events = append(prH1.Events, &proxypb.SpanEvent{Name: "ack called"})
		spansH := []*proxypb.TraceSpan{
			subH0, ccH0, scH0, prH0,
			subH1, ccH1, scH1, prH1,
			makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9151, []*proxypb.TraceSpan{subH0}),
		}
		var nineAcks []SettlementExpectation
		for k := 0; k < 9; k++ {
			spansH = append(spansH, makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9160+k, []*proxypb.TraceSpan{subH1}))
			nineAcks = append(nineAcks, SettlementExpectation{Operation: "ack"})
		}
		resHi := Verify(goCaps, spansH, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-hi-0", Settlements: nineAcks},
					{MessageID: "m-hi-1", Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		})
		// m-hi-0 must stay matched to subH0 (failing missing ack links on subH0, NOT swapping to subH1 with wrong message_id).
		assertHasViolation(t, resHi, CodeMissingLink, "subscribe.settlement.forward_link")
		for _, f := range resHi.Failures {
			if f.Rule == "subscribe.attr.message_id" {
				t.Fatalf("totalSettleBonus improperly overrode MessageID binding:\n%s", resHi.Format())
			}
		}
	})

	t.Run("Challenge 3: Subscribe mixed ForcedShutdownNack + None process events", func(t *testing.T) {
		// Case 1: subWithEv precedes subNoEv, and Case 2: subNoEv precedes subWithEv.
		for _, noEvFirst := range []bool{false, true} {
			seqWith, seqNo := 9201, 9202
			if noEvFirst {
				seqWith, seqNo = 9212, 9211
			}
			subWith, ccW, scW, prW, _ := makeGoSubscribeMessageSpans("p", "s", "m-ev", "a-ev-w", "", 5, 0, false, 2, "", "nacked", seqWith)
			prW.Events = append(prW.Events, &proxypb.SpanEvent{Name: "nack called"})
			subNo, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "m-ev", "a-ev-n", "", 5, 0, false, 2, "", "nacked", seqNo)
			nk := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, seqWith, []*proxypb.TraceSpan{subWith, subNo})

			base := []*proxypb.TraceSpan{subWith, ccW, scW, prW, subNo, ccN, scN, prN, nk}
			mForced := SubscribeMessageExpectation{
				MessageID:     "m-ev",
				BatchCount:    2,
				InterruptKind: SubscribeInterruptForcedShutdownNack,
				Settlements:   []SettlementExpectation{{Operation: "nack"}},
			}
			mNormal := SubscribeMessageExpectation{
				MessageID:   "m-ev",
				BatchCount:  2,
				Settlements: []SettlementExpectation{{Operation: "nack"}},
			}

			var baseline string
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(base)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				msgs := []SubscribeMessageExpectation{mForced, mNormal}
				if seed%2 == 1 {
					msgs = []SubscribeMessageExpectation{mNormal, mForced}
				}
				res := Verify(goCaps, perm, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
				})
				if !res.Passed() {
					t.Fatalf("noEvFirst=%v seed=%d expected PASS, got:\n%s", noEvFirst, seed, res.Format())
				}
				if seed == 0 {
					baseline = res.Format()
				} else if res.Format() != baseline {
					t.Fatalf("noEvFirst=%v seed=%d Format() differed from baseline", noEvFirst, seed)
				}
				for _, s := range perm {
					if len(s.Links) > 1 {
						rng.Shuffle(len(s.Links), func(i, j int) { s.Links[i], s.Links[j] = s.Links[j], s.Links[i] })
					}
				}
				if resLink := Verify(goCaps, perm, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
				}); !resLink.Passed() {
					t.Fatalf("noEvFirst=%v seed=%d link-shuffled expected PASS, got:\n%s", noEvFirst, seed, resLink.Format())
				}
			}

			// Also verify ExpectProcessSettlementEvent overrides with strict settlements, and omitted-Settlements
			// ExpectedOutcome="nacked"/"acked"/"" vs ForcedShutdownNack on evented vs no-event process spans.
			for _, msgs := range [][]SubscribeMessageExpectation{
				{
					{MessageID: "m-ev", BatchCount: 2, ExpectProcessSettlementEvent: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "nack"}}},
					{MessageID: "m-ev", BatchCount: 2, ExpectProcessSettlementEvent: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "nack"}}},
				},
				{
					{MessageID: "m-ev", BatchCount: 2, ExpectProcessSettlementEvent: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "nack"}}},
					{MessageID: "m-ev", BatchCount: 2, ExpectProcessSettlementEvent: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "nack"}}},
				},
			} {
				if res := Verify(goCaps, base, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
				}); !res.Passed() {
					t.Fatalf("noEvFirst=%v ExpectProcessSettlementEvent override mixed batch expected PASS, got:\n%s", noEvFirst, res.Format())
				}
			}

			for _, outcome := range []string{"nacked", "acked", ""} {
				evName := "ack called"
				spanResult := "acked"
				settleOp := "ack"
				if outcome == "nacked" {
					evName = "nack called"
					spanResult = "nacked"
					settleOp = "nack"
				}
				subEv, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-ev-o", "a-ev-o1", "", 5, 0, false, 1, "", spanResult, seqWith)
				subEv.Events = append(subEv.Events,
					&proxypb.SpanEvent{Name: settleOp + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
					&proxypb.SpanEvent{Name: settleOp + " end"},
				)
				prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: evName})
				subNoO, ccNO, scNO, prNO, _ := makeGoSubscribeMessageSpans("p", "s", "m-ev-o", "a-ev-o2", "", 5, 0, false, 1, "", spanResult, seqNo)
				subNoO.Events = append(subNoO.Events,
					&proxypb.SpanEvent{Name: settleOp + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
					&proxypb.SpanEvent{Name: settleOp + " end"},
				)
				spansO := []*proxypb.TraceSpan{subEv, ccE, scE, prE, subNoO, ccNO, scNO, prNO}
				for _, msgs := range [][]SubscribeMessageExpectation{
					{
						{MessageID: "m-ev-o", BatchCount: 1, InterruptKind: SubscribeInterruptForcedShutdownNack, ExpectedOutcome: outcome},
						{MessageID: "m-ev-o", BatchCount: 1, ExpectedOutcome: outcome},
					},
					{
						{MessageID: "m-ev-o", BatchCount: 1, ExpectedOutcome: outcome},
						{MessageID: "m-ev-o", BatchCount: 1, InterruptKind: SubscribeInterruptForcedShutdownNack, ExpectedOutcome: outcome},
					},
				} {
					if res := Verify(goCaps, spansO, &TraceExpectation{
						Subscription: "projects/p/subscriptions/s",
						Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
					}); !res.Passed() {
						t.Fatalf("noEvFirst=%v outcome=%q omitted-settlement mixed expected PASS, got:\n%s", noEvFirst, outcome, res.Format())
					}
				}
			}
		}
	})

	t.Run("Minor 4: math.MinInt64 batch_count does not underflow in Phase 3 or Phase 5", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(9300), testSpanID(93000))
		ackMin := makeGoSettlementSpan("p", "s", "ack", math.MinInt64, 0, false, 9301, nil)
		for _, allowExtra := range []bool{false, true} {
			res := Verify(goCaps, []*proxypb.TraceSpan{ackMin}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: allowExtra,
				Subscribe: &SubscribeExpectation{
					MessageCount:           1,
					WireTraceparents:       []string{unsampTP},
					ExpectDroppedUnsampled: true,
					ExpectedSettlements:    []SettlementExpectation{{Operation: "ack"}},
				},
			})
			assertHasViolation(t, res, CodeMissingSpan, "subscribe.unsampled_settlement.present")
			for _, f := range res.Failures {
				if f.Rule == "settlement.ack.attr.batch_count" {
					t.Fatalf("unexpected settlement.ack.attr.batch_count when span wasn't matched:\n%s", res.Format())
				}
			}
		}

		// Sampled + dropped -00 sharing linked ack with bc=math.MinInt64: must fail BOTH
		// subscribe.unsampled_settlement.present AND settlement.ack.attr.batch_count (no int(math.MinInt64)-1 wrap).
		subM, ccM, scM, prM, _ := makeGoSubscribeMessageSpans("p", "s", "m-min-c", "a-min-c", "", 5, 0, false, 1, "", "acked", 9305)
		prM.Events = append(prM.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackLinkedMin := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9305, []*proxypb.TraceSpan{subM})
		ackLinkedMin.Attributes["messaging.batch.message_count"] = intAV(math.MinInt64)
		resLinkedMin := Verify(goCaps, []*proxypb.TraceSpan{subM, ccM, scM, prM, ackLinkedMin}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-min-c", MessageID: "m-min-c", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		})
		assertHasViolation(t, resLinkedMin, CodeMissingSpan, "subscribe.unsampled_settlement.present")
		assertHasViolation(t, resLinkedMin, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// Phase 5 extraNeeded64 guard: sub0's ack has bc=math.MinInt64 (minRequired=1), while sub1's ack(bc=2)
		// coalesces dropped -00. Phase 5 must NOT underflow extraNeeded64 on sub0's ack to drain unreservedAckRemaining.
		sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m-min0", "a-min0", "", 5, 0, false, 1, "", "acked", 9311)
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-min1", "a-min1", "", 5, 0, false, 1, "", "acked", 9312)
		pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackSub0Min := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9311, []*proxypb.TraceSpan{sub0})
		ackSub0Min.Attributes["messaging.batch.message_count"] = intAV(math.MinInt64)
		ackSub1Coal := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 9312, []*proxypb.TraceSpan{sub1})
		resP5 := Verify(goCaps, []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, ackSub0Min, ackSub1Coal}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-min0", MessageID: "m-min0", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "a-min1", MessageID: "m-min1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
				},
			},
		})
		assertHasViolation(t, resP5, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")
		for _, f := range resP5.Failures {
			if f.SpanID != ackSub0Min.SpanId || f.Rule != "settlement.ack.attr.batch_count" {
				t.Fatalf("expected failures strictly on ackSub0Min (%s) batch_count only (NOT ackSub1Coal %s), got:\n%s", ackSub0Min.SpanId, ackSub1Coal.SpanId, resP5.Format())
			}
		}
	})
}

func TestWave8Regressions_SubscribeNormalizationAndErrorMetadata(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("normalizeSettleBonus boundary semantics", func(t *testing.T) {
		if got := normalizeSettleBonus(0, 1000, 120_000); got != 0 {
			t.Fatalf("raw=0 -> got %d, want 0", got)
		}
		if got := normalizeSettleBonus(10, 0, 120_000); got != 0 {
			t.Fatalf("maxRaw=0 -> got %d, want 0", got)
		}
		if got := normalizeSettleBonus(10, 1000, 0); got != 0 {
			t.Fatalf("cap=0 -> got %d, want 0", got)
		}
		if got := normalizeSettleBonus(200_000, 200_000, 120_000); got != 120_000 {
			t.Fatalf("raw==maxRaw -> got %d, want 120_000", got)
		}
		if got := normalizeSettleBonus(1, 1_000_000, 120_000); got != 1 {
			t.Fatalf("sub-unit positive raw -> got %d, want 1", got)
		}
		a := normalizeSettleBonus(350*450, 351*450, 120_000)
		b := normalizeSettleBonus(351*450, 351*450, 120_000)
		if !(a > 0 && a < b && b == 120_000) {
			t.Fatalf("monotonicity failed: a=%d b=%d", a, b)
		}
	})

	t.Run("Attack 8A_1: Subscribe 9 vs 10 and 12 vs 13 settlements sharing MessageID with omitted AckID", func(t *testing.T) {
		buildCase := func(baseSeq, lowModacks, highExtraDeadline int) ([]*proxypb.TraceSpan, []SubscribeMessageExpectation) {
			subLow, ccL, scL, prL, _ := makeGoSubscribeMessageSpans("p", "s", "m-8a", "a-8a-low", "", 5, 0, false, 1, "", "acked", baseSeq)
			subHigh, ccH, scH, prH, _ := makeGoSubscribeMessageSpans("p", "s", "m-8a", "a-8a-high", "", 5, 0, false, 1, "", "acked", baseSeq+1)
			prL.Events = append(prL.Events, &proxypb.SpanEvent{Name: "ack called"})
			prH.Events = append(prH.Events, &proxypb.SpanEvent{Name: "ack called"})

			spans := []*proxypb.TraceSpan{subLow, ccL, scL, prL, subHigh, ccH, scH, prH}
			var settlesLow, settlesHigh []SettlementExpectation
			seq := baseSeq * 100
			for k := 0; k < lowModacks; k++ {
				spans = append(spans, makeGoSettlementSpan("p", "s", "modack", 1, 10, false, seq, []*proxypb.TraceSpan{subLow}))
				settlesLow = append(settlesLow, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
				seq++

				spans = append(spans, makeGoSettlementSpan("p", "s", "modack", 1, 10, false, seq, []*proxypb.TraceSpan{subHigh}))
				settlesHigh = append(settlesHigh, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
				seq++
			}
			// High gets one extra modack(highExtraDeadline).
			spans = append(spans, makeGoSettlementSpan("p", "s", "modack", 1, int64(highExtraDeadline), false, seq, []*proxypb.TraceSpan{subHigh}))
			settlesHigh = append(settlesHigh, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: int32(highExtraDeadline), IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
			seq++

			spans = append(spans, makeGoSettlementSpan("p", "s", "ack", 1, 0, false, seq, []*proxypb.TraceSpan{subLow}))
			settlesLow = append(settlesLow, SettlementExpectation{Operation: "ack"})
			seq++
			spans = append(spans, makeGoSettlementSpan("p", "s", "ack", 1, 0, false, seq, []*proxypb.TraceSpan{subHigh}))
			settlesHigh = append(settlesHigh, SettlementExpectation{Operation: "ack"})

			msgs := []SubscribeMessageExpectation{
				{MessageID: "m-8a", Settlements: settlesLow},
				{MessageID: "m-8a", Settlements: settlesHigh},
			}
			return spans, msgs
		}

		for _, tc := range []struct {
			name     string
			baseSeq  int
			lowCount int
			extraDL  int
		}{
			{name: "9 vs 10", baseSeq: 9500, lowCount: 9, extraDL: 10},
			{name: "12 vs 13", baseSeq: 9600, lowCount: 12, extraDL: 60},
		} {
			t.Run(tc.name, func(t *testing.T) {
				base, msgsA := buildCase(tc.baseSeq, tc.lowCount, tc.extraDL)
				msgsB := []SubscribeMessageExpectation{msgsA[1], msgsA[0]}
				var baseline string
				for seed := int64(0); seed < 24; seed++ {
					perm := cloneSpans(base)
					rng := rand.New(rand.NewSource(seed))
					rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
					msgs := msgsA
					if seed%2 == 1 {
						msgs = msgsB
					}
					res := Verify(goCaps, perm, &TraceExpectation{
						Subscription: "projects/p/subscriptions/s",
						Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
					})
					if !res.Passed() {
						t.Fatalf("seed=%d expected PASS, got:\n%s", seed, res.Format())
					}
					if seed == 0 {
						baseline = res.Format()
					} else if res.Format() != baseline {
						t.Fatalf("seed=%d Format() differed from baseline", seed)
					}
					for _, s := range perm {
						if len(s.Links) > 1 {
							rng.Shuffle(len(s.Links), func(i, j int) { s.Links[i], s.Links[j] = s.Links[j], s.Links[i] })
						}
					}
					if resLink := Verify(goCaps, perm, &TraceExpectation{
						Subscription: "projects/p/subscriptions/s",
						Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
					}); !resLink.Passed() {
						t.Fatalf("seed=%d link-shuffled expected PASS, got:\n%s", seed, resLink.Format())
					}
				}
			})
		}
	})

	t.Run("Attack 8A_1: >= 350 settlements triggers Subscribe proportional normalization and preserves MessageID priority", func(t *testing.T) {
		sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m-norm", "a-norm-0", "", 5, 0, false, 1, "", "acked", 9701)
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-norm", "a-norm-1", "", 5, 0, false, 1, "", "acked", 9702)
		subZero, ccZ, scZ, prZ, _ := makeGoSubscribeMessageSpans("p", "s", "m-norm-zero", "a-norm-z", "", 5, 0, false, 1, "", "acked", 9703)
		subZero.Events = append(subZero.Events,
			&proxypb.SpanEvent{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			&proxypb.SpanEvent{Name: "ack end"},
		)
		pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		prZ.Events = append(prZ.Events, &proxypb.SpanEvent{Name: "ack called"})

		spans := []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, subZero, ccZ, scZ, prZ}
		var settles350, settles351 []SettlementExpectation
		seq := 970000
		for k := 0; k < 350; k++ {
			spans = append(spans, makeGoSettlementSpan("p", "s", "modack", 1, 10, false, seq, []*proxypb.TraceSpan{sub0}))
			settles350 = append(settles350, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
			seq++

			spans = append(spans, makeGoSettlementSpan("p", "s", "modack", 1, 10, false, seq, []*proxypb.TraceSpan{sub1}))
			settles351 = append(settles351, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
			seq++
		}
		spans = append(spans, makeGoSettlementSpan("p", "s", "modack", 1, 60, false, seq, []*proxypb.TraceSpan{sub1}))
		settles351 = append(settles351, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
		seq++

		spans = append(spans, makeGoSettlementSpan("p", "s", "ack", 1, 0, false, seq, []*proxypb.TraceSpan{sub0}))
		settles350 = append(settles350, SettlementExpectation{Operation: "ack"})
		seq++
		spans = append(spans, makeGoSettlementSpan("p", "s", "ack", 1, 0, false, seq, []*proxypb.TraceSpan{sub1}))
		settles351 = append(settles351, SettlementExpectation{Operation: "ack"})

		m350 := SubscribeMessageExpectation{MessageID: "m-norm", Settlements: settles350}
		m351 := SubscribeMessageExpectation{MessageID: "m-norm", Settlements: settles351}
		mZero := SubscribeMessageExpectation{MessageID: "m-norm-zero", ExpectedOutcome: "acked"}
		msgsA := []SubscribeMessageExpectation{m350, m351, mZero}
		msgsB := []SubscribeMessageExpectation{mZero, m351, m350}

		var baseline string
		for seed := int64(0); seed < 2; seed++ {
			perm := cloneSpans(spans)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := msgsA
			if seed%2 == 1 {
				msgs = msgsB
			}
			res := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("seed=%d expected PASS at 350 vs 351 settlements, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseline = res.Format()
			} else if res.Format() != baseline {
				t.Fatalf("seed=%d Format() differed from baseline at 350 vs 351", seed)
			}
			if seed == 1 {
				for _, s := range perm {
					if len(s.Links) > 1 {
						rng.Shuffle(len(s.Links), func(i, j int) { s.Links[i], s.Links[j] = s.Links[j], s.Links[i] })
					}
				}
				if resLink := Verify(goCaps, perm, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
				}); !resLink.Passed() {
					t.Fatalf("seed=%d link-shuffled expected PASS at 350 vs 351, got:\n%s", seed, resLink.Format())
				}
			}
		}

		// Hierarchy check under normalization: MessageID (200_000) must beat normalized
		// totalSettleBonus (<= 120_000) + processEvents (<= 10_000) + attr/size/child tiebreakers.
		subH0, ccH0, scH0, prH0, _ := makeGoSubscribeMessageSpans("p", "s", "m-hi-norm-0", "a-hi-n0", "wrong-key", 99, 0, false, 9, "", "acked", 9751)
		// Omit "ack called" on prH0 and omit ccH0/scH0 to maximize non-MessageID score gap in subH1's favor.
		subH1, ccH1, scH1, prH1, _ := makeGoSubscribeMessageSpans("p", "s", "m-hi-norm-1", "a-hi-n1", "", 10, 0, false, 1, "", "acked", 9752)
		prH1.Events = append(prH1.Events, &proxypb.SpanEvent{Name: "ack called"})
		spansH := []*proxypb.TraceSpan{subH0, prH0, subH1, ccH1, scH1, prH1}
		_ = ccH0
		_ = scH0
		var settlesH []SettlementExpectation
		for k := 0; k < 360; k++ {
			spansH = append(spansH, makeGoSettlementSpan("p", "s", "modack", 1, 10, false, 980000+k, []*proxypb.TraceSpan{subH1}))
			settlesH = append(settlesH, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
		}
		spansH = append(spansH, makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 980500, []*proxypb.TraceSpan{subH1}))
		settlesH = append(settlesH, SettlementExpectation{Operation: "ack"})
		spansH = append(spansH, makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 980501, []*proxypb.TraceSpan{subH0}))

		resHi := Verify(goCaps, spansH, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-hi-norm-0", BodySize: Int64Ptr(10), BatchCount: 1, Settlements: settlesH},
					{MessageID: "m-hi-norm-1", Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		})
		assertHasViolation(t, resHi, CodeMissingLink, "subscribe.settlement.forward_link")
		for _, f := range resHi.Failures {
			if f.Rule == "subscribe.attr.message_id" {
				t.Fatalf("normalized totalSettleBonus + processEvents + tiebreakers improperly overrode MessageID:\n%s", resHi.Format())
			}
		}
	})

	t.Run("Attack 8B_1: Subscribe ErrorSubstring disambiguation with shared MessageID and omitted AckID", func(t *testing.T) {
		sub0, cc0, sc0, pr0, _ := makeGoSubscribeMessageSpans("p", "s", "m-8b1", "a-8b1-0", "", 5, 0, true, 1, "", "", 9801)
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-8b1", "a-8b1-1", "", 5, 0, true, 1, "", "", 9802)
		sub0.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		sub0.StatusMessage = "permission denied"
		sub0.Events = append(sub0.Events, &proxypb.SpanEvent{Name: "exception"})
		sub1.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		sub1.StatusMessage = "deadline exceeded"
		sub1.Events = append(sub1.Events, &proxypb.SpanEvent{Name: "exception"})
		pr0.Events = append(pr0.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})

		mod0 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 9801, []*proxypb.TraceSpan{sub0})
		mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 9802, []*proxypb.TraceSpan{sub1})
		ack0 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9803, []*proxypb.TraceSpan{sub0})
		ack0.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack0.StatusMessage = "permission denied"
		ack0.Events = append(ack0.Events, &proxypb.SpanEvent{Name: "exception"})
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9804, []*proxypb.TraceSpan{sub1})
		ack1.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack1.StatusMessage = "deadline exceeded"
		ack1.Events = append(ack1.Events, &proxypb.SpanEvent{Name: "exception"})

		base := []*proxypb.TraceSpan{sub0, cc0, sc0, pr0, sub1, cc1, sc1, pr1, mod0, mod1, ack0, ack1}
		mPerm := SubscribeMessageExpectation{
			MessageID:   "m-8b1",
			Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, ErrorSubstring: "permission denied"}},
		}
		mDead := SubscribeMessageExpectation{
			MessageID:   "m-8b1",
			Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, ErrorSubstring: "deadline exceeded"}},
		}
		msgsA := []SubscribeMessageExpectation{mPerm, mDead}
		msgsB := []SubscribeMessageExpectation{mDead, mPerm}

		var baseline string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := msgsA
			if seed%2 == 1 {
				msgs = msgsB
			}
			res := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(true), Messages: msgs},
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

		// Negative check: wrong ErrorSubstring fails cleanly.
		badSpans := cloneSpans(base)
		badSpans[len(badSpans)-1].StatusMessage = "completely different error"
		resBad := Verify(goCaps, badSpans, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(true), Messages: msgsB},
		})
		assertHasViolation(t, resBad, CodeWrongStatusMessage, "settlement.ack.status.message")
	})

	t.Run("Attack 8B_3: Subscribe EODPartialBatchFailure disambiguation with shared MessageID and omitted AckID", func(t *testing.T) {
		subPart, ccP, scP, prP, _ := makeGoSubscribeMessageSpans("p", "s", "m-8b3", "a-8b3-p", "", 5, 0, true, 1, "", "", 9851)
		subFull, ccF, scF, prF, _ := makeGoSubscribeMessageSpans("p", "s", "m-8b3", "a-8b3-f", "", 5, 0, true, 1, "", "", 9852)
		for _, sub := range []*proxypb.TraceSpan{subPart, subFull} {
			sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			sub.StatusMessage = "eod failed"
			sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: "exception"})
		}
		prP.Events = append(prP.Events, &proxypb.SpanEvent{Name: "ack called"})
		prF.Events = append(prF.Events, &proxypb.SpanEvent{Name: "ack called"})

		modP := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 9851, []*proxypb.TraceSpan{subPart})
		modF := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 9852, []*proxypb.TraceSpan{subFull})
		ackPart := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9853, []*proxypb.TraceSpan{subPart})
		ackPart.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackPart.StatusMessage = eodPartialBatchErrorMsg // no "exception" event
		ackFull := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9854, []*proxypb.TraceSpan{subFull})
		ackFull.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackFull.StatusMessage = "rpc failed"
		ackFull.Events = append(ackFull.Events, &proxypb.SpanEvent{Name: "exception"})

		base := []*proxypb.TraceSpan{subPart, ccP, scP, prP, subFull, ccF, scF, prF, modP, modF, ackPart, ackFull}
		mPart := SubscribeMessageExpectation{
			MessageID:   "m-8b3",
			Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, EODPartialBatchFailure: true}},
		}
		mFull := SubscribeMessageExpectation{
			MessageID:   "m-8b3",
			Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, EODPartialBatchFailure: false}},
		}
		msgsA := []SubscribeMessageExpectation{mPart, mFull}
		msgsB := []SubscribeMessageExpectation{mFull, mPart}

		var baseline string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := msgsA
			if seed%2 == 1 {
				msgs = msgsB
			}
			res := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(true), Messages: msgs},
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

		// Negative check: removing "exception" event from ackFull fails cleanly.
		badSpans := cloneSpans(base)
		badSpans[len(badSpans)-1].Events = nil
		resBad := Verify(goCaps, badSpans, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(true), Messages: msgsB},
		})
		assertHasViolation(t, resBad, CodeMissingEvent, "settlement.ack.event.exception")
	})
}

func TestWave10Regressions_PublishHedgedAutoEODRequireAttrsAndWhitebox(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("Hunt 1 & Hunt 1B: Publish MinHedgedAttempts & publish end (hedged) across both Messages orders and shuffles", func(t *testing.T) {
		buildHedgedCreate := func(seq int, bodySize int64, pubSpan *proxypb.TraceSpan, batchSize int64, hedgedStarts int, withStartCount bool, withHedgedEnd bool) *proxypb.TraceSpan {
			cs, _, _, _ := makeGoPublishMessageSpans("p", "t", fmt.Sprintf("m-hdg-%d", seq), "", bodySize, seq, pubSpan, batchSize)
			for i := 0; i < hedgedStarts; i++ {
				ev := &proxypb.SpanEvent{Name: "publish start (hedged)"}
				if withStartCount {
					ev.Attributes = map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(batchSize)}
				}
				cs.Events = append(cs.Events, ev)
			}
			if withHedgedEnd {
				cs.Events = append(cs.Events, &proxypb.SpanEvent{Name: "publish end (hedged)"})
			}
			return cs
		}

		// Hunt 1: MinHedgedAttempts 1 vs 3 with omitted MessageID and WireTraceparent.
		pubH1 := makeGoPublishBatchSpan("p", "t", 2, 10100)
		c1 := buildHedgedCreate(10101, 8, pubH1, 2, 1, true, true)
		fc1 := goSpan(c1.TraceId, testSpanID(101011), c1.SpanId, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		b1 := goSpan(c1.TraceId, testSpanID(101012), c1.SpanId, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		c3 := buildHedgedCreate(10102, 8, pubH1, 2, 3, true, true)
		fc3 := goSpan(c3.TraceId, testSpanID(101021), c3.SpanId, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		b3 := goSpan(c3.TraceId, testSpanID(101022), c3.SpanId, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		spansH1 := []*proxypb.TraceSpan{c1, fc1, b1, c3, fc3, b3, pubH1}

		m1 := PublishMessageExpectation{BodySize: Int64Ptr(8), Hedged: true, MinHedgedAttempts: 1}
		m3 := PublishMessageExpectation{BodySize: Int64Ptr(8), Hedged: true, MinHedgedAttempts: 3}
		var baselineH1 string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(spansH1)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := []PublishMessageExpectation{m1, m3}
			if seed%2 == 1 {
				msgs = []PublishMessageExpectation{m3, m1}
			}
			res := Verify(goCaps, perm, &TraceExpectation{
				Topic:   "projects/p/topics/t",
				Publish: &PublishExpectation{BatchCount: 1, Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("Hunt 1 seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baselineH1 = res.Format()
			} else if res.Format() != baselineH1 {
				t.Fatalf("Hunt 1 seed=%d Format() differed from baseline", seed)
			}
		}

		// Negative check: only 2 hedged starts when expecting >= 3 fails with min_hedged_attempts.
		c2Only := cloneSpans([]*proxypb.TraceSpan{c3})[0]
		c2Only.Events = []*proxypb.SpanEvent{
			{Name: "publish start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
			{Name: "publish end"},
			{Name: "publish start (hedged)", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
			{Name: "publish start (hedged)", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
			{Name: "publish end (hedged)"},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{c1, fc1, b1, c2Only, fc3, b3, pubH1}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{BatchCount: 1, Messages: []PublishMessageExpectation{m3, m1}},
		}), CodeMissingEvent, "publish.create.event.min_hedged_attempts")

		// Hunt 1B: Hedged: false vs MinHedgedAttempts: 2 (with Hedged: true and Hedged: false).
		pubH1B := makeGoPublishBatchSpan("p", "t", 2, 10110)
		c1B := buildHedgedCreate(10111, 8, pubH1B, 2, 1, true, true)
		fc1B := goSpan(c1B.TraceId, testSpanID(101111), c1B.SpanId, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		b1B := goSpan(c1B.TraceId, testSpanID(101112), c1B.SpanId, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		c2B := buildHedgedCreate(10112, 8, pubH1B, 2, 2, true, true)
		fc2B := goSpan(c2B.TraceId, testSpanID(101121), c2B.SpanId, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		b2B := goSpan(c2B.TraceId, testSpanID(101122), c2B.SpanId, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		spansH1B := []*proxypb.TraceSpan{c1B, fc1B, b1B, c2B, fc2B, b2B, pubH1B}

		for _, hedgedFlag := range []bool{true, false} {
			mNo := PublishMessageExpectation{BodySize: Int64Ptr(8), Hedged: false}
			mMin2 := PublishMessageExpectation{BodySize: Int64Ptr(8), Hedged: hedgedFlag, MinHedgedAttempts: 2}
			var baseH1B string
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(spansH1B)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				msgs := []PublishMessageExpectation{mNo, mMin2}
				if seed%2 == 1 {
					msgs = []PublishMessageExpectation{mMin2, mNo}
				}
				res := Verify(goCaps, perm, &TraceExpectation{
					Topic:   "projects/p/topics/t",
					Publish: &PublishExpectation{BatchCount: 1, Messages: msgs},
				})
				if !res.Passed() {
					t.Fatalf("Hunt 1B hedged=%v seed=%d expected PASS, got:\n%s", hedgedFlag, seed, res.Format())
				}
				if seed == 0 {
					baseH1B = res.Format()
				} else if res.Format() != baseH1B {
					t.Fatalf("Hunt 1B hedged=%v seed=%d Format() differed", hedgedFlag, seed)
				}
			}
		}

		// publish end (hedged) & valid start attr disambiguation + negative detection.
		pubEnd := makeGoPublishBatchSpan("p", "t", 2, 10120)
		cValidEnd := buildHedgedCreate(10121, 8, pubEnd, 2, 1, true, true)
		fcVE := goSpan(cValidEnd.TraceId, testSpanID(101211), cValidEnd.SpanId, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		bVE := goSpan(cValidEnd.TraceId, testSpanID(101212), cValidEnd.SpanId, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		cMissingEnd := buildHedgedCreate(10122, 8, pubEnd, 2, 1, true, false)
		fcME := goSpan(cMissingEnd.TraceId, testSpanID(101221), cMissingEnd.SpanId, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		bME := goSpan(cMissingEnd.TraceId, testSpanID(101222), cMissingEnd.SpanId, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		for _, msgs := range [][]PublishMessageExpectation{
			{{BodySize: Int64Ptr(8), Hedged: true}, {BodySize: Int64Ptr(8), Hedged: false}},
			{{BodySize: Int64Ptr(8), Hedged: false}, {BodySize: Int64Ptr(8), Hedged: true}},
		} {
			if res := Verify(goCaps, []*proxypb.TraceSpan{cMissingEnd, fcME, bME, cValidEnd, fcVE, bVE, pubEnd}, &TraceExpectation{
				Topic:   "projects/p/topics/t",
				Publish: &PublishExpectation{BatchCount: 1, Messages: msgs},
			}); !res.Passed() {
				t.Fatalf("hedged end disambiguation expected PASS, got:\n%s", res.Format())
			}
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{cMissingEnd, fcME, bME, pubEnd}, &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish:               &PublishExpectation{Messages: []PublishMessageExpectation{{BodySize: Int64Ptr(8), Hedged: true}}},
		}), CodeMissingEvent, "publish.create.event.publish_end_hedged")

		// Invalid hedged start (missing batch_count) vs valid hedged start.
		cBadStart := buildHedgedCreate(10123, 8, pubEnd, 2, 1, false, true)
		fcBS := goSpan(cBadStart.TraceId, testSpanID(101231), cBadStart.SpanId, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		bBS := goSpan(cBadStart.TraceId, testSpanID(101232), cBadStart.SpanId, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		cBadStart.Links = nil
		cMissingEnd.Links = nil
		pubEnd2 := makeGoPublishBatchSpan("p", "t", 2, 10124)
		for _, c := range []*proxypb.TraceSpan{cBadStart, cValidEnd} {
			c.Links = []*proxypb.SpanLink{{TraceId: pubEnd2.TraceId, SpanId: pubEnd2.SpanId, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("publish")}}}
			pubEnd2.Links = append(pubEnd2.Links, &proxypb.SpanLink{TraceId: c.TraceId, SpanId: c.SpanId})
		}
		for _, msgs := range [][]PublishMessageExpectation{
			{{BodySize: Int64Ptr(8), Hedged: true}, {BodySize: Int64Ptr(8), Hedged: false}},
			{{BodySize: Int64Ptr(8), Hedged: false}, {BodySize: Int64Ptr(8), Hedged: true}},
		} {
			if res := Verify(goCaps, []*proxypb.TraceSpan{cBadStart, fcBS, bBS, cValidEnd, fcVE, bVE, pubEnd2}, &TraceExpectation{
				Topic:   "projects/p/topics/t",
				Publish: &PublishExpectation{BatchCount: 1, Messages: msgs},
			}); !res.Passed() {
				t.Fatalf("hedged start attr disambiguation expected PASS, got:\n%s", res.Format())
			}
		}
	})

	t.Run("Hunt 2 (Go): Subscribe AllowExtraPubSubSpans auto-EOD vs extra ALO and EOD acked spans sharing MessageID", func(t *testing.T) {
		subValidEOD, ccV, scV, prV, _ := makeGoSubscribeMessageSpans("p", "s", "m-hunt2", "a-hunt2-eod", "", 5, 0, true, 1, "", "", 10201)
		prV.Events = append(prV.Events, &proxypb.SpanEvent{Name: "ack called"})
		subValidEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subValidEOD.StatusMessage = "invalid ack id"
		subValidEOD.Events = append(subValidEOD.Events, &proxypb.SpanEvent{Name: "exception"})
		ackErr := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 10201, []*proxypb.TraceSpan{subValidEOD})
		ackErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackErr.StatusMessage = "invalid ack id"
		ackErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		subExtraALO, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-hunt2", "a-hunt2-alo", "", 5, 0, false, 1, "", "acked", 10202)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 10202, []*proxypb.TraceSpan{subExtraALO})

		subExtraALOErr, ccAE, scAE, prAE, _ := makeGoSubscribeMessageSpans("p", "s", "m-hunt2", "a-hunt2-aloe", "", 5, 0, false, 1, "", "acked", 10203)
		prAE.Events = append(prAE.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALOErr := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 10203, []*proxypb.TraceSpan{subExtraALOErr})
		ackALOErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackALOErr.StatusMessage = "rpc error"
		ackALOErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		subExtraEOD, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-hunt2", "a-hunt2-eoda", "", 5, 0, true, 1, "", "acked", 10204)
		prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackEOD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 10204, []*proxypb.TraceSpan{subExtraEOD})

		base := []*proxypb.TraceSpan{
			subExtraALO, ccA, scA, prA, ackALO,
			subExtraALOErr, ccAE, scAE, prAE, ackALOErr,
			subExtraEOD, ccE, scE, prE, ackEOD,
			subValidEOD, ccV, scV, prV, ackErr,
		}
		exp := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-hunt2",
					ExactlyOnce: nil,
					Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true}},
				}},
			},
		}

		var baseline string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, exp)
			if !res.Passed() {
				t.Fatalf("Hunt 2 Go seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseline = res.Format()
			} else if res.Format() != baseline {
				t.Fatalf("Hunt 2 Go seed=%d Format() differed", seed)
			}
		}

		// Negative check: buggy EOD-failed span retaining result="acked" still beats extra ALO/EOD acked spans and reports result_omitted_on_eod_fail.
		badSpans := cloneSpans(base)
		for _, s := range badSpans {
			if s.GetSpanId() == subValidEOD.GetSpanId() {
				s.Attributes["messaging.gcp_pubsub.result"] = strAV("acked")
			}
		}
		resBad := Verify(goCaps, badSpans, exp)
		assertHasViolation(t, resBad, CodeUnexpectedAttribute, "subscribe.attr.result_omitted_on_eod_fail")
	})

	t.Run("Hunt 3A & Hunt 3B (Subscribe): capped RequireAttributes & Go Data scoring vs MessageID", func(t *testing.T) {
		makeAttrs := func(n int) (map[string]*proxypb.AttributeValue, map[string]AttrMatcher) {
			raw := make(map[string]*proxypb.AttributeValue, n)
			req := make(map[string]AttrMatcher, n)
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("custom.attr.%d", i)
				v := fmt.Sprintf("val-%d", i)
				raw[k] = strAV(v)
				if i == 0 {
					req[k] = nil // exercise MatchPresent() default
				} else {
					req[k] = MatchString(v)
				}
			}
			return raw, req
		}

		// Positive passing: 10 RequireAttributes + 350 settlements + Data (no BodySize) across both orders & shuffles.
		raw10, req10 := makeAttrs(10)
		subA, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-3b-0", "a-3b-0", "", 5, 0, false, 1, "", "acked", 10301)
		subB, ccB, scB, prB, _ := makeGoSubscribeMessageSpans("p", "s", "m-3b-1", "a-3b-1", "", 5, 0, false, 1, "", "acked", 10302)
		for k, v := range raw10 {
			subA.Attributes[k] = v
			subB.Attributes[k] = v
		}
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		prB.Events = append(prB.Events, &proxypb.SpanEvent{Name: "ack called"})
		spansPos := []*proxypb.TraceSpan{subA, ccA, scA, prA, subB, ccB, scB, prB}
		var settles350 []SettlementExpectation
		for k := 0; k < 350; k++ {
			spansPos = append(spansPos, makeGoSettlementSpan("p", "s", "modack", 1, 10, false, 103100+k, []*proxypb.TraceSpan{subA}))
			settles350 = append(settles350, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
		}
		spansPos = append(spansPos,
			makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 103900, []*proxypb.TraceSpan{subA}),
			makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 103901, []*proxypb.TraceSpan{subB}),
		)
		settles350 = append(settles350, SettlementExpectation{Operation: "ack"})
		mPosA := SubscribeMessageExpectation{MessageID: "m-3b-0", Data: []byte("hello"), RequireAttributes: req10, Settlements: settles350}
		mPosB := SubscribeMessageExpectation{MessageID: "m-3b-1", Data: []byte("hello"), RequireAttributes: req10, Settlements: []SettlementExpectation{{Operation: "ack"}}}
		var basePosFmt string
		for seed := int64(0); seed < 4; seed++ {
			perm := cloneSpans(spansPos)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			msgs := []SubscribeMessageExpectation{mPosA, mPosB}
			if seed%2 == 1 {
				msgs = []SubscribeMessageExpectation{mPosB, mPosA}
			}
			res := Verify(goCaps, perm, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
			})
			if !res.Passed() {
				t.Fatalf("Hunt 3B positive seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				basePosFmt = res.Format()
			} else if res.Format() != basePosFmt {
				t.Fatalf("Hunt 3B positive seed=%d Format() differed", seed)
			}
		}

		// Partial-attribute monotonicity under proportional cap (7/10 vs 3/10 attrs).
		rawPart, reqPart := makeAttrs(10)
		sub7, cc7, sc7, pr7, _ := makeGoSubscribeMessageSpans("p", "s", "m-part", "a-part-7", "", 5, 0, false, 1, "", "acked", 10350)
		sub3, cc3, sc3, pr3, _ := makeGoSubscribeMessageSpans("p", "s", "m-part", "a-part-3", "", 5, 0, false, 1, "", "acked", 10351)
		pr7.Events = append(pr7.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr3.Events = append(pr3.Events, &proxypb.SpanEvent{Name: "ack called"})
		for i := 0; i < 7; i++ {
			k := fmt.Sprintf("custom.attr.%d", i)
			sub7.Attributes[k] = rawPart[k]
		}
		for i := 0; i < 3; i++ {
			k := fmt.Sprintf("custom.attr.%d", i)
			sub3.Attributes[k] = rawPart[k]
		}
		ack7 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 10350, []*proxypb.TraceSpan{sub7})
		ack3 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 10351, []*proxypb.TraceSpan{sub3})
		mPart7 := SubscribeMessageExpectation{MessageID: "m-part", RequireAttributes: reqPart, Settlements: []SettlementExpectation{{Operation: "ack"}}}
		mPart3 := SubscribeMessageExpectation{MessageID: "m-part", RequireAttributes: map[string]AttrMatcher{"custom.attr.0": nil, "custom.attr.1": MatchString("val-1"), "custom.attr.2": MatchString("val-2")}, Settlements: []SettlementExpectation{{Operation: "ack"}}}
		for _, msgs := range [][]SubscribeMessageExpectation{{mPart7, mPart3}, {mPart3, mPart7}} {
			resPart := Verify(goCaps, []*proxypb.TraceSpan{sub3, cc3, sc3, pr3, ack3, sub7, cc7, sc7, pr7, ack7}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    &SubscribeExpectation{ExactlyOnce: BoolPtr(false), Messages: msgs},
			})
			assertHasViolation(t, resPart, CodeMissingAttribute, "subscribe.attr.custom.custom.attr.7")
			for _, f := range resPart.Failures {
				if f.Rule == "subscribe.attr.custom.custom.attr.3" {
					t.Fatalf("partial 7/10 attrs should match sub7 (only missing 7..9), not sub3 (missing 3..9):\n%s", resPart.Format())
				}
			}
		}

		// Negative attribution checks: 2 attrs + tiebreakers (Hunt 3A), and 10 / 105 attrs + 360 settlements (Hunt 3B).
		for _, tc := range []struct {
			name        string
			attrCount   int
			settleCount int
		}{
			{name: "Hunt 3A: 2 RequireAttributes + max non-ID tiebreakers vs MessageID", attrCount: 2, settleCount: 5},
			{name: "Hunt 3B: 10 RequireAttributes + 360 normalized settlements vs MessageID", attrCount: 10, settleCount: 360},
			{name: "Hunt 3B: 105 RequireAttributes + 360 normalized settlements vs MessageID", attrCount: 105, settleCount: 360},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rawAttrs, reqAttrs := makeAttrs(tc.attrCount)
				// subTrue carries m-sub0 (only MessageID matches), failing custom attrs, ordering_key, size, DA, batch_count, result, children, settlements.
				subTrue, _, _, prTrue, _ := makeGoSubscribeMessageSpans("p", "s", "m-sub0", "a-sub0", "wrong-k", 99, 0, true, 9, "", "nacked", 10401)
				// subFalse carries m-sub1 (wrong MessageID), matching ALL custom attrs + all non-ID tiebreakers.
				subFalse, ccF, scF, prF, _ := makeGoSubscribeMessageSpans("p", "s", "m-sub1", "a-sub1", "ok-k", 10, 2, false, 1, "", "acked", 10402)
				prF.Events = append(prF.Events, &proxypb.SpanEvent{Name: "ack called"})
				for k, v := range rawAttrs {
					subFalse.Attributes[k] = v
				}
				spans := []*proxypb.TraceSpan{subTrue, prTrue, subFalse, ccF, scF, prF}
				var settles []SettlementExpectation
				for k := 0; k < tc.settleCount; k++ {
					spans = append(spans, makeGoSettlementSpan("p", "s", "modack", 1, 10, false, 104100+k, []*proxypb.TraceSpan{subFalse}))
					settles = append(settles, SettlementExpectation{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(false), ExpectedBatchCount: 1})
				}
				spans = append(spans, makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 104900, []*proxypb.TraceSpan{subFalse}))
				settles = append(settles, SettlementExpectation{Operation: "ack"})

				m0 := SubscribeMessageExpectation{
					MessageID:         "m-sub0",
					OrderingKey:       "ok-k",
					Data:              []byte("0123456789"),
					DeliveryAttempt:   2,
					BatchCount:        1,
					ExactlyOnce:       BoolPtr(false),
					ExpectedOutcome:   "acked",
					RequireAttributes: reqAttrs,
					Settlements:       settles,
				}
				m1 := SubscribeMessageExpectation{MessageID: "m-sub1"}
				for _, msgs := range [][]SubscribeMessageExpectation{{m0, m1}, {m1, m0}} {
					res := Verify(goCaps, spans, &TraceExpectation{
						Subscription:          "projects/p/subscriptions/s",
						AllowExtraPubSubSpans: true,
						Subscribe:             &SubscribeExpectation{Messages: msgs},
					})
					assertHasViolation(t, res, CodeMissingAttribute, "subscribe.attr.custom.custom.attr.0")
					for _, f := range res.Failures {
						if f.Rule == "subscribe.attr.message_id" || f.Rule == "subscribe.process.attr.message_id" {
							t.Fatalf("%s: RequireAttributes improperly overrode MessageID:\n%s", tc.name, res.Format())
						}
					}
				}
			})
		}
	})

	t.Run("Hunt 3C (Publish): capped RequireAttributes (1, 14, 30) vs MessageID", func(t *testing.T) {
		makeAttrs := func(n int) (map[string]*proxypb.AttributeValue, map[string]AttrMatcher) {
			raw := make(map[string]*proxypb.AttributeValue, n)
			req := make(map[string]AttrMatcher, n)
			for i := 0; i < n; i++ {
				k := fmt.Sprintf("pub.attr.%d", i)
				v := fmt.Sprintf("pval-%d", i)
				raw[k] = strAV(v)
				if i == 0 {
					req[k] = nil
				} else {
					req[k] = MatchString(v)
				}
			}
			return raw, req
		}

		// Positive passing: 1 attr (<= cap branch) and 14 attrs (> cap branch) + Data + Hedged.
		for _, nAttrs := range []int{1, 14} {
			raw, req := makeAttrs(nAttrs)
			pub := makeGoPublishBatchSpan("p", "t", 2, 10500+nAttrs)
			c0, fc0, b0, _ := makeGoPublishMessageSpans("p", "t", fmt.Sprintf("mp-pos-%d-0", nAttrs), "k0", 5, 10510+nAttrs, pub, 2)
			c1, fc1, b1, _ := makeGoPublishMessageSpans("p", "t", fmt.Sprintf("mp-pos-%d-1", nAttrs), "k0", 5, 10520+nAttrs, pub, 2)
			for k, v := range raw {
				c0.Attributes[k] = v
				c1.Attributes[k] = v
			}
			c0.Events = append(c0.Events,
				&proxypb.SpanEvent{Name: "publish start (hedged)", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
				&proxypb.SpanEvent{Name: "publish end (hedged)"},
			)
			m0 := PublishMessageExpectation{MessageID: fmt.Sprintf("mp-pos-%d-0", nAttrs), OrderingKey: "k0", Data: []byte("12345"), Hedged: true, RequireAttributes: req}
			m1 := PublishMessageExpectation{MessageID: fmt.Sprintf("mp-pos-%d-1", nAttrs), OrderingKey: "k0", Data: []byte("12345")}
			for _, msgs := range [][]PublishMessageExpectation{{m0, m1}, {m1, m0}} {
				if res := Verify(goCaps, []*proxypb.TraceSpan{c1, fc1, b1, c0, fc0, b0, pub}, &TraceExpectation{
					Topic:   "projects/p/topics/t",
					Publish: &PublishExpectation{BatchCount: 1, Messages: msgs},
				}); !res.Passed() {
					t.Fatalf("Publish positive %d attrs expected PASS, got:\n%s", nAttrs, res.Format())
				}
			}
		}

		// Negative diagnostic attribution: 14 and 30 RequireAttributes + max tiebreakers vs MessageID.
		for _, nAttrs := range []int{14, 30} {
			raw, req := makeAttrs(nAttrs)
			parentTP := fmt.Sprintf("00-%s-%s-01", testTraceID(10600+nAttrs), testSpanID(106000+nAttrs))
			pub := makeGoPublishBatchSpan("p", "t", 2, 10600+nAttrs)
			// cTrue has MessageID="m-pub0" ONLY, failing ParentTraceparent, OrderingKey, Size, Hedged, children, and custom attrs.
			cTrue, _, _, _ := makeGoPublishMessageSpans("p", "t", "m-pub0", "wrong-k", 99, 10610+nAttrs, pub, 2)
			// cFalse has MessageID="m-pub1" (wrong!) and passes EVERY non-ID tiebreaker + all custom attrs.
			cFalse, fcFalse, bFalse, _ := makeGoPublishMessageSpans("p", "t", "m-pub1", "ok-k", 5, 10620+nAttrs, pub, 2)
			cFalse.TraceId = testTraceID(10600 + nAttrs)
			cFalse.ParentSpanId = testSpanID(106000 + nAttrs)
			fcFalse.TraceId = cFalse.TraceId
			bFalse.TraceId = cFalse.TraceId
			pub.Links[1].TraceId = cFalse.TraceId
			cFalse.Events = append(cFalse.Events,
				&proxypb.SpanEvent{Name: "publish start (hedged)", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
				&proxypb.SpanEvent{Name: "publish start (hedged)", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
				&proxypb.SpanEvent{Name: "publish end (hedged)"},
			)
			for k, v := range raw {
				cFalse.Attributes[k] = v
			}

			m0 := PublishMessageExpectation{
				MessageID:         "m-pub0",
				ParentTraceparent: parentTP,
				OrderingKey:       "ok-k",
				BodySize:          Int64Ptr(5),
				Hedged:            true,
				MinHedgedAttempts: 2,
				RequireAttributes: req,
			}
			m1 := PublishMessageExpectation{MessageID: "m-pub1"}
			for _, msgs := range [][]PublishMessageExpectation{{m0, m1}, {m1, m0}} {
				res := Verify(goCaps, []*proxypb.TraceSpan{cFalse, fcFalse, bFalse, cTrue, pub}, &TraceExpectation{
					Topic:                 "projects/p/topics/t",
					AllowExtraPubSubSpans: true,
					Publish:               &PublishExpectation{Messages: msgs},
				})
				assertHasViolation(t, res, CodeMissingAttribute, "publish.create.attr.custom.pub.attr.0")
				for _, f := range res.Failures {
					if f.Rule == "publish.create.attr.message_id" {
						t.Fatalf("Publish %d RequireAttributes improperly overrode MessageID:\n%s", nAttrs, res.Format())
					}
				}
			}
		}
	})
}

func TestWave11Regressions_EODFailedEOSExpandPublishAndProbe4(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("Probe 2B Case D (ProfileGo): explicit eod_failed with ExactlyOnce=nil rejects eos=false and avoids ALO stealing", func(t *testing.T) {
		// 1. Standalone buggy Go span: eos=false + ERROR status + exception + no result MUST fail exactly_once_delivery.
		subBuggy, ccB, scB, prB, _ := makeGoSubscribeMessageSpans("p", "s", "m-2bd", "a-2bd", "", 5, 0, false, 1, "", "", 11001)
		subBuggy.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subBuggy.StatusMessage = "eod error"
		subBuggy.Events = append(subBuggy.Events, &proxypb.SpanEvent{Name: "exception"})
		prB.Events = append(prB.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackBuggy := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 11001, []*proxypb.TraceSpan{subBuggy})
		ackBuggy.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackBuggy.StatusMessage = "eod error"
		ackBuggy.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		for _, outcome := range []string{"eod_failed", "EOD_FAILED"} {
			for _, settles := range [][]SettlementExpectation{nil, {{Operation: "ack", ExpectError: true}}} {
				res := Verify(goCaps, []*proxypb.TraceSpan{subBuggy, ccB, scB, prB, ackBuggy}, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: settles == nil,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-2bd",
							ExactlyOnce:     nil,
							ExpectedOutcome: outcome,
							Settlements:     settles,
						}},
					},
				})
				assertHasViolation(t, res, CodeWrongAttributeValue, "subscribe.attr.exactly_once_delivery")
			}
		}

		// 2. AllowExtraPubSubSpans: true with extra ALO span + broken EOD span (UNSET status, eos=true, no result) MUST blame broken EOD, not ALO.
		subExtraALO, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-2bd", "a-2bd-alo", "", 5, 0, false, 1, "", "acked", 11002)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		subBrokenEOD, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-2bd", "a-2bd-eod", "", 5, 0, true, 1, "", "", 11003)
		prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: "ack called"})

		baseBroken := []*proxypb.TraceSpan{subExtraALO, ccA, scA, prA, subBrokenEOD, ccE, scE, prE}
		expBroken := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-2bd",
					ExactlyOnce:     nil,
					ExpectedOutcome: "eod_failed",
				}},
			},
		}
		var brokenFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(baseBroken)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, expBroken)
			assertHasViolation(t, res, CodeWrongStatusCode, "subscribe.eod_error.status")
			for _, f := range res.Failures {
				if f.Rule == "subscribe.attr.exactly_once_delivery" {
					t.Fatalf("extra ALO stole from broken EOD span in Go:\n%s", res.Format())
				}
			}
			if seed == 0 {
				brokenFmt = res.Format()
			} else if res.Format() != brokenFmt {
				t.Fatalf("Probe 2B Go broken EOD seed=%d Format() differed", seed)
			}
		}

		// 3. Positive Go explicit eod_failed with ExactlyOnce=nil passes standalone and with extra ALO/EOD spans.
		subValidEOD, ccV, scV, prV, _ := makeGoSubscribeMessageSpans("p", "s", "m-2bd", "a-2bd-ok", "", 5, 0, true, 1, "", "", 11004)
		prV.Events = append(prV.Events, &proxypb.SpanEvent{Name: "ack called"})
		subValidEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subValidEOD.StatusMessage = "perm fail"
		subValidEOD.Events = append(subValidEOD.Events, &proxypb.SpanEvent{Name: "exception"})
		ackValidEOD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 11004, []*proxypb.TraceSpan{subValidEOD})
		ackValidEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackValidEOD.StatusMessage = "perm fail"
		ackValidEOD.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		if res := Verify(goCaps, []*proxypb.TraceSpan{subValidEOD, ccV, scV, prV, ackValidEOD}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-2bd",
					ExactlyOnce:     nil,
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "ack", ExpectError: true}},
				}},
			},
		}); !res.Passed() {
			t.Fatalf("Go explicit eod_failed standalone expected PASS, got:\n%s", res.Format())
		}

		ackExtraALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 11002, []*proxypb.TraceSpan{subExtraALO})
		subExtraEODOK, ccEO, scEO, prEO, _ := makeGoSubscribeMessageSpans("p", "s", "m-2bd", "a-2bd-eodok", "", 5, 0, true, 1, "", "acked", 11005)
		prEO.Events = append(prEO.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackExtraEODOK := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 11005, []*proxypb.TraceSpan{subExtraEODOK})
		basePos := []*proxypb.TraceSpan{
			subExtraALO, ccA, scA, prA, ackExtraALO,
			subExtraEODOK, ccEO, scEO, prEO, ackExtraEODOK,
			subValidEOD, ccV, scV, prV, ackValidEOD,
		}
		expPos := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-2bd",
					ExactlyOnce:     nil,
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "ack", ExpectError: true}},
				}},
			},
		}
		var posFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(basePos)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, expPos)
			if !res.Passed() {
				t.Fatalf("Go explicit eod_failed w/ extras seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				posFmt = res.Format()
			} else if res.Format() != posFmt {
				t.Fatalf("Go explicit eod_failed w/ extras seed=%d Format() differed", seed)
			}
		}

		// 4. resolveIsEOD infers EOD when subscribe span is missing and unlinked modack is present (contrast with ExpectedOutcome="").
		unlinkedMod := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 11006, nil)
		resMissingSub := Verify(goCaps, []*proxypb.TraceSpan{unlinkedMod}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExpectedOutcome: "eod_failed",
				ExactlyOnce:     nil,
			},
		})
		assertHasViolation(t, resMissingSub, CodeMissingSpan, "subscribe.present")
		assertHasViolation(t, resMissingSub, CodeMissingLink, "subscribe.modack.missing_link")

		resMissingSubALO := Verify(goCaps, []*proxypb.TraceSpan{unlinkedMod}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount: 1,
				ExactlyOnce:  nil,
			},
		})
		assertHasViolation(t, resMissingSubALO, CodeMissingSpan, "subscribe.present")
		for _, f := range resMissingSubALO.Failures {
			if f.Rule == "subscribe.modack.missing_link" {
				t.Fatalf("unexpected subscribe.modack.missing_link when ExpectedOutcome==\"\" allows 1 unlinked ALO receipt slot:\n%s", resMissingSubALO.Format())
			}
		}

		// 5. ALO ack(ExpectError: true) with ExactlyOnce=nil & ExpectedOutcome="" still PASSES cleanly.
		subALOErr, ccAE, scAE, prAE, _ := makeGoSubscribeMessageSpans("p", "s", "m-alo-err", "a-alo-err", "", 5, 0, false, 1, "", "acked", 11007)
		prAE.Events = append(prAE.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALOErr := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 11007, []*proxypb.TraceSpan{subALOErr})
		ackALOErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackALOErr.StatusMessage = "unavailable"
		ackALOErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		if res := Verify(goCaps, []*proxypb.TraceSpan{subALOErr, ccAE, scAE, prAE, ackALOErr}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				MessageCount:        1,
				ExactlyOnce:         nil,
				ExpectedSettlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, ErrorSubstring: "unavailable"}},
			},
		}); !res.Passed() {
			t.Fatalf("ALO ack(ExpectError:true) with ExactlyOnce=nil, ExpectedOutcome=\"\" expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("expandPublishMessages symmetry: MinHedgedAttempts>0 without Hedged or MessageCount", func(t *testing.T) {
		// Contrast: zero-valued PublishExpectation expands 0 messages and passes on 0 spans.
		if res := Verify(goCaps, nil, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{},
		}); !res.Passed() {
			t.Fatalf("empty PublishExpectation on 0 spans expected PASS, got:\n%s", res.Format())
		}

		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MinHedgedAttempts: 2,
			},
		}
		// 0 spans MUST fail, not vacuously pass.
		assertHasViolation(t, Verify(goCaps, nil, exp), CodeMissingSpan, "publish.create.present")

		// 1 create span with only 1 hedged start MUST fail min_hedged_attempts.
		pub1 := makeGoPublishBatchSpan("p", "t", 1, 11101)
		c1, fc1, b1, _ := makeGoPublishMessageSpans("p", "t", "m-sym-1", "", 5, 11101, pub1, 1)
		c1.Events = append(c1.Events,
			&proxypb.SpanEvent{Name: "publish start (hedged)", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			&proxypb.SpanEvent{Name: "publish end (hedged)"},
		)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{c1, fc1, b1, pub1}, exp), CodeMissingEvent, "publish.create.event.min_hedged_attempts")

		// 1 create span with 2 valid hedged starts + end MUST PASS.
		c2 := cloneSpans([]*proxypb.TraceSpan{c1})[0]
		c2.Events = append(c2.Events, &proxypb.SpanEvent{Name: "publish start (hedged)", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}})
		if res := Verify(goCaps, []*proxypb.TraceSpan{c2, fc1, b1, pub1}, exp); !res.Passed() {
			t.Fatalf("MinHedgedAttempts=2 with 2 valid hedged starts expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Probe 4: broken hedged span ranks ahead of clean non-hedged span across both Messages orders", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 2, 11200)
		// cBrokenHedged has "publish start (hedged)" missing both batch_count AND "publish end (hedged)".
		cBrokenHedged, fcB, bB, _ := makeGoPublishMessageSpans("p", "t", "m-p4-broken", "", 8, 11201, pub, 2)
		cBrokenHedged.Events = append(cBrokenHedged.Events, &proxypb.SpanEvent{Name: "publish start (hedged)"})

		// cCleanPlain has NO hedged events.
		cCleanPlain, fcP, bP, _ := makeGoPublishMessageSpans("p", "t", "m-p4-plain", "", 8, 11202, pub, 2)

		base := []*proxypb.TraceSpan{cCleanPlain, fcP, bP, cBrokenHedged, fcB, bB, pub}
		for _, mHedged := range []PublishMessageExpectation{
			{BodySize: Int64Ptr(8), Hedged: true},
			{BodySize: Int64Ptr(8), MinHedgedAttempts: 2},
		} {
			mPlain := PublishMessageExpectation{BodySize: Int64Ptr(8), Hedged: false}
			var baselineFmt string
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(base)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				msgs := []PublishMessageExpectation{mHedged, mPlain}
				if seed%2 == 1 {
					msgs = []PublishMessageExpectation{mPlain, mHedged}
				}
				res := Verify(goCaps, perm, &TraceExpectation{
					Topic:   "projects/p/topics/t",
					Publish: &PublishExpectation{BatchCount: 1, Messages: msgs},
				})
				assertHasViolation(t, res, CodeMissingEvent, "publish.create.event.publish_end_hedged")
				assertHasViolation(t, res, CodeMissingEvent, "publish.create.event.publish_start_hedged")
				for _, f := range res.Failures {
					if f.SpanID != cBrokenHedged.GetSpanId() {
						t.Fatalf("Probe 4 seed=%d blamed wrong span %s instead of cBrokenHedged %s:\n%s", seed, f.SpanID, cBrokenHedged.GetSpanId(), res.Format())
					}
				}
				if seed == 0 {
					baselineFmt = res.Format()
				} else if res.Format() != baselineFmt {
					t.Fatalf("Probe 4 seed=%d Format() differed across shuffle/order", seed)
				}
			}
		}
	})
}

func TestWave12Regressions_SubscribeExpectationConflict(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	// Build Go ALO, Go EOD-fail, C++ ALO, and C++ EOD-fail span sets.
	subGoALO, ccGA, scGA, prGA, _ := makeGoSubscribeMessageSpans("p", "s", "m-alo", "a-alo", "", 5, 0, false, 1, "", "acked", 12001)
	prGA.Events = append(prGA.Events, &proxypb.SpanEvent{Name: "ack called"})
	ackGoALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 12001, []*proxypb.TraceSpan{subGoALO})
	goALOSpans := []*proxypb.TraceSpan{subGoALO, ccGA, scGA, prGA, ackGoALO}

	subGoEOD, ccGE, scGE, prGE, _ := makeGoSubscribeMessageSpans("p", "s", "m-eod", "a-eod", "", 5, 0, true, 1, "", "", 12002)
	subGoEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
	subGoEOD.StatusMessage = "eod perm fail"
	subGoEOD.Events = append(subGoEOD.Events, &proxypb.SpanEvent{Name: "exception"})
	prGE.Events = append(prGE.Events, &proxypb.SpanEvent{Name: "ack called"})
	ackGoEOD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 12002, []*proxypb.TraceSpan{subGoEOD})
	ackGoEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
	ackGoEOD.StatusMessage = "eod perm fail"
	ackGoEOD.Events = []*proxypb.SpanEvent{{Name: "exception"}}
	unlinkedMod := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 12003, nil)
	goEODSpans := []*proxypb.TraceSpan{subGoEOD, ccGE, scGE, prGE, ackGoEOD, unlinkedMod}

	makeCppSub := func(seq int, msgID, ackID string, isEOD bool, result string) []*proxypb.TraceSpan {
		tid := testTraceID(12100 + seq)
		sid := testSpanID(121000 + seq*10)
		sub := goSpan(tid, sid, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
		sub.InstrumentationScopeName = CppScopeName
		sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
		sub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                          strAV("p"),
			"messaging.system":                                        strAV("gcp_pubsub"),
			"messaging.destination.name":                              strAV("s"),
			"messaging.operation.type":                                strAV("subscribe"),
			"messaging.message.id":                                    strAV(msgID),
			"messaging.message.envelope.size":                         intAV(25),
			"messaging.gcp_pubsub.message.ack_id":                     strAV(ackID),
			"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(isEOD),
		}
		if result != "" {
			sub.Attributes["messaging.gcp_pubsub.result"] = strAV(result)
			evOp := result
			if evOp == "ack" || evOp == "nack" {
				sub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp." + evOp + "_start"}, {Name: "gl-cpp." + evOp + "_end"}}
			}
		}
		cc := goSpan(tid, testSpanID(121001+seq*10), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		cc.InstrumentationScopeName = CppScopeName
		cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		sched := goSpan(tid, testSpanID(121002+seq*10), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		sched.InstrumentationScopeName = CppScopeName
		sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		proc := goSpan(tid, testSpanID(121003+seq*10), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		proc.InstrumentationScopeName = CppScopeName
		proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		return []*proxypb.TraceSpan{sub, cc, sched, proc}
	}

	cppALOSpans := makeCppSub(1, "m-alo", "ca-alo", false, "ack")
	cppEODSpans := makeCppSub(2, "m-eod", "ca-eod", true, "ack")

	assertConflictOnly := func(t *testing.T, res TraceResult, allowExtra bool) {
		t.Helper()
		assertHasViolation(t, res, CodeCustomCheckFailed, "subscribe.expectation.conflict")
		for _, f := range res.Failures {
			switch f.Rule {
			case "subscribe.present",
				"subscribe.attr.exactly_once_delivery",
				"subscribe.attr.result",
				"subscribe.eod_error.status",
				"subscribe.modack.missing_link":
				t.Fatalf("Phase 2/4 executed despite upfront expectation conflict; unexpected failure %+v\n%s", f, res.Format())
			}
		}
		if allowExtra && len(res.Failures) != 1 {
			t.Fatalf("expected exactly 1 conflict violation with AllowExtraPubSubSpans=true, got %d:\n%s", len(res.Failures), res.Format())
		}
	}

	t.Run("Bug A: contradictory ExactlyOnce=false + ExpectedOutcome=eod_failed rejected upfront across profiles and inheritance forms", func(t *testing.T) {
		for _, prof := range []struct {
			name  string
			caps  *proxypb.GetCapabilitiesResponse
			cases []struct {
				label string
				spans []*proxypb.TraceSpan
			}
		}{
			{
				name: "ProfileGo",
				caps: goCaps,
				cases: []struct {
					label string
					spans []*proxypb.TraceSpan
				}{
					{label: "valid_ALO", spans: goALOSpans},
					{label: "valid_EOD", spans: goEODSpans},
					{label: "zero_spans", spans: nil},
				},
			},
			{
				name: "ProfileCPP",
				caps: cppCaps,
				cases: []struct {
					label string
					spans []*proxypb.TraceSpan
				}{
					{label: "valid_ALO", spans: cppALOSpans},
					{label: "valid_EOD", spans: cppEODSpans},
					{label: "zero_spans", spans: nil},
				},
			},
		} {
			for _, outcome := range []string{"eod_failed", "EOD_FAILED"} {
				forms := []struct {
					name string
					sub  *SubscribeExpectation
				}{
					{
						name: "direct_message",
						sub: &SubscribeExpectation{
							Messages: []SubscribeMessageExpectation{{
								ExactlyOnce:     BoolPtr(false),
								ExpectedOutcome: outcome,
							}},
						},
					},
					{
						name: "top_eos_false_per_msg_eod_failed",
						sub: &SubscribeExpectation{
							ExactlyOnce: BoolPtr(false),
							Messages: []SubscribeMessageExpectation{{
								ExpectedOutcome: outcome,
							}},
						},
					},
					{
						name: "top_eod_failed_per_msg_eos_false",
						sub: &SubscribeExpectation{
							ExpectedOutcome: outcome,
							Messages: []SubscribeMessageExpectation{{
								ExactlyOnce: BoolPtr(false),
							}},
						},
					},
					{
						name: "top_level_both_nil_messages",
						sub: &SubscribeExpectation{
							ExactlyOnce:     BoolPtr(false),
							ExpectedOutcome: outcome,
						},
					},
					{
						name: "top_level_both_overridden_by_messages",
						sub: &SubscribeExpectation{
							ExactlyOnce:     BoolPtr(false),
							ExpectedOutcome: outcome,
							Messages: []SubscribeMessageExpectation{{
								ExactlyOnce:     BoolPtr(true),
								ExpectedOutcome: "acked",
							}},
						},
					},
					{
						name: "dropped_unsampled_conflict",
						sub: &SubscribeExpectation{
							ExactlyOnce: BoolPtr(false),
							Messages: []SubscribeMessageExpectation{{
								WireTraceparent:        fmt.Sprintf("00-%s-%s-00", testTraceID(12999), testSpanID(129990)),
								ExpectDroppedUnsampled: true,
								ExpectedOutcome:        outcome,
							}},
						},
					},
				}

				for _, form := range forms {
					for _, allowExtra := range []bool{false, true} {
						for _, sc := range prof.cases {
							res := Verify(prof.caps, cloneSpans(sc.spans), &TraceExpectation{
								Subscription:          "projects/p/subscriptions/s",
								AllowExtraPubSubSpans: allowExtra,
								Subscribe:             form.sub,
							})
							assertConflictOnly(t, res, allowExtra)
						}
					}
				}
			}
		}
	})

	t.Run("Preserves mixed batch override and valid EOD acked/nacked/expired", func(t *testing.T) {
		// Mixed batch: top-level ExactlyOnce=false default with per-message EOD override passes cleanly.
		mixedGo := append(cloneSpans(goALOSpans), goEODSpans[:5]...)
		resMixGo := Verify(goCaps, mixedGo, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-alo", AckID: "a-alo", ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{MessageID: "m-eod", AckID: "a-eod", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "eod_failed", Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true}}},
				},
			},
		})
		if !resMixGo.Passed() {
			t.Fatalf("mixed ALO+EOD Go expected PASS, got:\n%s", resMixGo.Format())
		}

		// Valid EOD + acked/nacked/expired on ProfileGo and ProfileCPP.
		for _, tc := range []struct {
			outcome  string
			goResult string
			cppRes   string
			hasProc  bool
		}{
			{outcome: "acked", goResult: "acked", cppRes: "ack", hasProc: true},
			{outcome: "nacked", goResult: "nacked", cppRes: "nack", hasProc: true},
			{outcome: "expired", goResult: "expired", cppRes: "expired", hasProc: false},
		} {
			subG, ccG, scG, prG, _ := makeGoSubscribeMessageSpans("p", "s", "m-ok", "a-ok", "", 5, 0, true, 1, "", tc.goResult, 12201)
			if tc.outcome == "acked" || tc.outcome == "nacked" {
				settleOp := "ack"
				if tc.outcome == "nacked" {
					settleOp = "nack"
				}
				subG.Events = append(subG.Events,
					&proxypb.SpanEvent{Name: settleOp + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
					&proxypb.SpanEvent{Name: settleOp + " end"},
				)
				prG.Events = append(prG.Events, &proxypb.SpanEvent{Name: settleOp + " called"})
			}
			goSpans := []*proxypb.TraceSpan{subG, ccG, scG}
			if tc.hasProc {
				goSpans = append(goSpans, prG)
			}
			if res := Verify(goCaps, goSpans, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: tc.outcome,
				},
			}); !res.Passed() {
				t.Fatalf("Go EOD + %q expected PASS, got:\n%s", tc.outcome, res.Format())
			}

			cppSpans := makeCppSub(10, "m-ok", "ca-ok", true, tc.cppRes)
			if !tc.hasProc {
				cppSpans = cppSpans[:3]
			}
			if res := Verify(cppCaps, cppSpans, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: tc.outcome,
				},
			}); !res.Passed() {
				t.Fatalf("C++ EOD + %q expected PASS, got:\n%s", tc.outcome, res.Format())
			}
		}
	})
}

func TestWave13Regressions_AllowExtraValidSubLinksAndOutcomeTrim(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	t.Run("Case 1: Live-style 1-msg Go EOD redelivery (modack+nack -> modack+ack_error) with AllowExtraPubSubSpans=true", func(t *testing.T) {
		// Attempt 1: nacked EOD delivery with receipt modack + nack.
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-redeliv", "a-redeliv-1", "", 8, 1, true, 1, "", "nacked", 13001)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "nack called"})
		mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 13001, []*proxypb.TraceSpan{sub1})
		nack1 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 13002, []*proxypb.TraceSpan{sub1})

		// Attempt 2: failed EOD AckWithResult delivery with receipt modack + ERROR ack.
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-redeliv", "a-redeliv-2", "", 8, 2, true, 1, "", "", 13003)
		sub2.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		sub2.StatusMessage = "permission denied"
		sub2.Events = append(sub2.Events, &proxypb.SpanEvent{Name: "exception"})
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod2 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 13004, []*proxypb.TraceSpan{sub2})
		ack2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13005, []*proxypb.TraceSpan{sub2})
		ack2.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack2.StatusMessage = "permission denied"
		ack2.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		base := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, mod1, nack1, sub2, cc2, sc2, pr2, mod2, ack2}
		expPass := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-redeliv",
					DeliveryAttempt: 2,
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
			res := Verify(goCaps, perm, expPass)
			if !res.Passed() {
				t.Fatalf("Case 1 seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseFmt = res.Format()
			} else if res.Format() != baseFmt {
				t.Fatalf("Case 1 seed=%d Format() differed", seed)
			}
		}

		// Contrasts: AllowExtraPubSubSpans=false and StrictSettlementOps=true MUST fail.
		expNoExtra := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe:             expPass.Subscribe,
		}
		resNoExtra := Verify(goCaps, cloneSpans(base), expNoExtra)
		assertHasViolation(t, resNoExtra, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
		assertHasViolation(t, resNoExtra, CodeMissingLink, "subscribe.settlement.dangling_link")

		expStrict := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: true,
				Messages:            expPass.Subscribe.Messages,
			},
		}
		assertHasViolation(t, Verify(goCaps, cloneSpans(base), expStrict), CodeMissingLink, "subscribe.settlement.dangling_link")
	})

	t.Run("Case 2: 2-msg EOD subset [M1] with extra M2 (sub+modack+ack/nack) and ALO unlinked slot protection", func(t *testing.T) {
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-sub-1", "a-sub-1", "", 6, 0, true, 1, "", "acked", 13101)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 13101, []*proxypb.TraceSpan{sub1})
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13102, []*proxypb.TraceSpan{sub1})

		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-sub-2", "a-sub-2", "", 6, 0, true, 1, "", "acked", 13103)
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod2 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 13103, []*proxypb.TraceSpan{sub2})
		ack2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13104, []*proxypb.TraceSpan{sub2})

		base := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, mod1, ack1, sub2, cc2, sc2, pr2, mod2, ack2}
		expPass := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-sub-1",
					AckID:       "a-sub-1",
					Settlements: []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}
		var baseFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, expPass)
			if !res.Passed() {
				t.Fatalf("Case 2 seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseFmt = res.Format()
			} else if res.Format() != baseFmt {
				t.Fatalf("Case 2 seed=%d Format() differed", seed)
			}
		}

		// Fails when AllowExtraPubSubSpans=false or StrictSettlementOps=true (both mod2 and ack2-only).
		resNoExtra := Verify(goCaps, cloneSpans(base), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe:             expPass.Subscribe,
		})
		assertHasViolation(t, resNoExtra, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
		assertHasViolation(t, resNoExtra, CodeMissingLink, "subscribe.settlement.dangling_link")

		expStrict := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(true),
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-sub-1",
					AckID:       "a-sub-1",
					Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}},
				}},
			},
		}
		assertHasViolation(t, Verify(goCaps, cloneSpans(base), expStrict), CodeMissingLink, "subscribe.settlement.dangling_link")

		// Even without mod2, unmatched M2_ack fails StrictSettlementOps=true with dangling_link, and passes StrictSettlementOps=false.
		baseNoMod2 := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, mod1, ack1, sub2, cc2, sc2, pr2, ack2}
		if res := Verify(goCaps, cloneSpans(baseNoMod2), expPass); !res.Passed() {
			t.Fatalf("Case 2 ack2-only non-strict expected PASS, got:\n%s", res.Format())
		}
		assertHasViolation(t, Verify(goCaps, cloneSpans(baseNoMod2), expStrict), CodeMissingLink, "subscribe.settlement.dangling_link")

		// Mixed ALO unlinked receipt modack slot protection: M2 linked receipt modack + coalesced [M1_sub, M2_sub] modack
		// must NOT steal unlinked ALO receipt slots from matched fast-acked ALO messages.
		subALO, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-alo-fast", "a-alo-fast", "", 5, 0, false, 1, "", "acked", 13110)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13110, []*proxypb.TraceSpan{subALO})
		unlinkedModALO := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 13111, nil)

		subEx, ccEx, scEx, prEx, _ := makeGoSubscribeMessageSpans("p", "s", "m-ex", "a-ex", "", 5, 0, false, 1, "", "nacked", 13112)
		prEx.Events = append(prEx.Events, &proxypb.SpanEvent{Name: "nack called"})
		modEx := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 13113, []*proxypb.TraceSpan{subEx})
		nackEx := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 13114, []*proxypb.TraceSpan{subEx})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subALO, ccA, scA, prA, ackALO, unlinkedModALO, subEx, ccEx, scEx, prEx, modEx, nackEx}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce: BoolPtr(false),
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-alo-fast",
					AckID:       "a-alo-fast",
					Settlements: []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}); !res.Passed() {
			t.Fatalf("Case 2 mixed ALO unlinked slot with extra linked modack expected PASS, got:\n%s", res.Format())
		}

		// Coalesced modack [M1_sub, M2_sub] (batch_count=2) + separate M3 ALO unlinked receipt modack -> PASS.
		subM1b, ccM1b, scM1b, prM1b, _ := makeGoSubscribeMessageSpans("p", "s", "m-c1", "a-c1", "", 5, 0, true, 2, "", "acked", 13120)
		prM1b.Events = append(prM1b.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackM1b := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13120, []*proxypb.TraceSpan{subM1b})
		subM2b, ccM2b, scM2b, prM2b, _ := makeGoSubscribeMessageSpans("p", "s", "m-c2", "a-c2", "", 5, 0, true, 2, "", "acked", 13121)
		prM2b.Events = append(prM2b.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackM2b := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13121, []*proxypb.TraceSpan{subM2b})
		modShared12 := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 13122, []*proxypb.TraceSpan{subM1b, subM2b})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subM1b, ccM1b, scM1b, prM1b, ackM1b, subM2b, ccM2b, scM2b, prM2b, ackM2b, modShared12, subALO, ccA, scA, prA, ackALO, unlinkedModALO}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-c1", AckID: "a-c1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{MessageID: "m-alo-fast", AckID: "a-alo-fast", ExactlyOnce: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}); !res.Passed() {
			t.Fatalf("Case 2 coalesced [M1, M2] modack + M3 ALO unlinked slot expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Case 4: Live-style 2-msg batched ALO ack and modack [M1_sub, M2_sub] verifying subset [M1]", func(t *testing.T) {
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-bat-1", "a-bat-1", "", 7, 0, false, 2, "", "acked", 13201)
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-bat-2", "a-bat-2", "", 7, 0, false, 2, "", "acked", 13202)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		modBatch := makeGoSettlementSpan("p", "s", "modack", 2, 10, true, 13201, []*proxypb.TraceSpan{sub1, sub2})
		ackBatch := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 13202, []*proxypb.TraceSpan{sub1, sub2})

		base := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, sub2, cc2, sc2, pr2, modBatch, ackBatch}
		expPass := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-bat-1",
					AckID:       "a-bat-1",
					BatchCount:  2,
					Settlements: []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}
		var baseFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(base)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, expPass)
			if !res.Passed() {
				t.Fatalf("Case 4 seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseFmt = res.Format()
			} else if res.Format() != baseFmt {
				t.Fatalf("Case 4 seed=%d Format() differed", seed)
			}
		}

		// batch_count=1 on batched ack -> FAIL lower bound (>= 2).
		underAck := cloneSpans(base)
		underAck[9].Attributes["messaging.batch.message_count"] = intAV(1)
		assertHasViolation(t, Verify(goCaps, underAck, expPass), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// batch_count=1 on batched modack -> FAIL lower bound (>= 2).
		underMod := cloneSpans(base)
		underMod[8].Attributes["messaging.batch.message_count"] = intAV(1)
		assertHasViolation(t, Verify(goCaps, underMod, expPass), CodeWrongAttributeValue, "settlement.modack.attr.batch_count")

		// StrictSettlementOps=true -> FAIL dangling_link.
		expStrict := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-bat-1",
					AckID:       "a-bat-1",
					BatchCount:  2,
					Settlements: []SettlementExpectation{{Operation: "modack"}, {Operation: "ack"}},
				}},
			},
		}
		assertHasViolation(t, Verify(goCaps, cloneSpans(base), expStrict), CodeMissingLink, "subscribe.settlement.dangling_link")

		// AllowExtraPubSubSpans=false -> FAIL dangling_link + no_extra_pubsub_spans.
		resNoExtra := Verify(goCaps, cloneSpans(base), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe:             expPass.Subscribe,
		})
		assertHasViolation(t, resNoExtra, CodeMissingLink, "subscribe.settlement.dangling_link")
		assertHasViolation(t, resNoExtra, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
	})

	t.Run("Case 5: Strict negative link auditing preserved under AllowExtraPubSubSpans=true && StrictSettlementOps=false", func(t *testing.T) {
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-neg-1", "a-neg-1", "", 5, 0, false, 1, "", "acked", 13301)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13301, []*proxypb.TraceSpan{sub1})
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-neg-2", "a-neg-2", "", 5, 0, false, 1, "", "acked", 13302)
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		pubCreate := goSpan(testTraceID(13350), testSpanID(133500), "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
		appFakeSub := goSpan(testTraceID(13351), testSpanID(133510), "", "s subscribe", proxypb.SpanKind_SPAN_KIND_SERVER, 0)
		appFakeSub.InstrumentationScopeName = "my-app"

		exp := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-neg-1",
					AckID:       "a-neg-1",
					Settlements: []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}

		base := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, ack1, sub2, cc2, sc2, pr2, pubCreate, appFakeSub}
		badTargets := []struct {
			name string
			link *proxypb.SpanLink
		}{
			{name: "non_existent", link: &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)}},
			{name: "topic_create", link: &proxypb.SpanLink{TraceId: pubCreate.TraceId, SpanId: pubCreate.SpanId}},
			{name: "sub_process", link: &proxypb.SpanLink{TraceId: pr2.TraceId, SpanId: pr2.SpanId}},
			{name: "non_pubsub_subscribe", link: &proxypb.SpanLink{TraceId: appFakeSub.TraceId, SpanId: appFakeSub.SpanId}},
		}

		for _, bt := range badTargets {
			// Extra modack linking to bad target -> FAIL dangling_link.
			extraMod := makeGoSettlementSpan("p", "s", "modack", 1, 60, false, 13310, nil)
			extraMod.Links = []*proxypb.SpanLink{bt.link}
			assertHasViolation(t, Verify(goCaps, append(cloneSpans(base), extraMod), exp), CodeMissingLink, "subscribe.settlement.dangling_link")

			// Extra ack / nack linking to bad target -> FAIL dangling_link.
			for _, op := range []string{"ack", "nack"} {
				extraSettle := makeGoSettlementSpan("p", "s", op, 1, 0, false, 13311, nil)
				extraSettle.Links = []*proxypb.SpanLink{bt.link}
				assertHasViolation(t, Verify(goCaps, append(cloneSpans(base), extraSettle), exp), CodeMissingLink, "subscribe.settlement.dangling_link")
			}

			// Batched matched ack linking [sub1, sub2, badTarget] -> FAIL dangling_link.
			spansBatchedBad := cloneSpans(base)
			spansBatchedBad[4].Links = append(spansBatchedBad[4].Links,
				&proxypb.SpanLink{TraceId: sub2.TraceId, SpanId: sub2.SpanId},
				bt.link,
			)
			spansBatchedBad[4].Attributes["messaging.batch.message_count"] = intAV(3)
			assertHasViolation(t, Verify(goCaps, spansBatchedBad, exp), CodeMissingLink, "subscribe.settlement.dangling_link")
		}

		// Duplicate link on extra modack, extra ack, and batched matched ack -> FAIL duplicate_link.
		dupMod := makeGoSettlementSpan("p", "s", "modack", 2, 60, false, 13320, nil)
		dupMod.Links = []*proxypb.SpanLink{
			{TraceId: sub2.TraceId, SpanId: sub2.SpanId},
			{TraceId: sub2.TraceId, SpanId: sub2.SpanId},
		}
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(base), dupMod), exp), CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_link")

		dupAck := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 13321, nil)
		dupAck.Links = cloneSpans([]*proxypb.TraceSpan{dupMod})[0].Links
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(base), dupAck), exp), CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_link")

		spansBatchedDup := cloneSpans(base)
		spansBatchedDup[4].Links = append(spansBatchedDup[4].Links,
			&proxypb.SpanLink{TraceId: sub2.TraceId, SpanId: sub2.SpanId},
			&proxypb.SpanLink{TraceId: sub2.TraceId, SpanId: sub2.SpanId},
		)
		assertHasViolation(t, Verify(goCaps, spansBatchedDup, exp), CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_link")

		// Forbidden link to droppedTraceIDs on extra modack, extra ack, and batched matched ack -> FAIL forbidden_link.
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(13390), testSpanID(133900))
		subD1, ccD1, scD1, prD1, _ := makeGoSubscribeMessageSpans("p", "s", "m-neg-d1", "a-neg-d1", "", 5, 0, false, 2, "", "acked", 13322)
		prD1.Events = append(prD1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackD1 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 13322, []*proxypb.TraceSpan{subD1})
		expDrop := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-neg-d1", AckID: "a-neg-d1", BatchCount: 2, Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}

		// Also verify a leaked <subID> subscribe span inside droppedTraceIDs is NEVER added to validSampledSubByKey!
		leakedDropSub := goSpan(testTraceID(13390), testSpanID(133901), "", "s subscribe", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)

		modBadDrop := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 13323, nil)
		modBadDrop.Links = []*proxypb.SpanLink{{TraceId: testTraceID(13390), SpanId: testSpanID(133901)}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subD1, ccD1, scD1, prD1, ackD1, modBadDrop}, expDrop), CodeBrokenTraceparent, "subscribe.unsampled_settlement.forbidden_link")
		resLeakedSubDrop := Verify(goCaps, []*proxypb.TraceSpan{subD1, ccD1, scD1, prD1, ackD1, leakedDropSub, modBadDrop}, expDrop)
		assertHasViolation(t, resLeakedSubDrop, CodeBrokenTraceparent, "subscribe.unsampled_settlement.forbidden_link")
		assertHasViolation(t, resLeakedSubDrop, CodeUnexpectedSpan, "subscribe.unsampled_dropped")

		ackExtraBadDrop := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13324, nil)
		ackExtraBadDrop.Links = []*proxypb.SpanLink{{TraceId: testTraceID(13390), SpanId: testSpanID(99999)}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subD1, ccD1, scD1, prD1, ackD1, ackExtraBadDrop}, expDrop), CodeBrokenTraceparent, "subscribe.unsampled_settlement.forbidden_link")

		ackMatchedBadDrop := cloneSpans([]*proxypb.TraceSpan{ackD1})[0]
		ackMatchedBadDrop.Links = append(ackMatchedBadDrop.Links, &proxypb.SpanLink{TraceId: testTraceID(13390), SpanId: testSpanID(99999)})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subD1, ccD1, scD1, prD1, ackMatchedBadDrop}, expDrop), CodeBrokenTraceparent, "subscribe.unsampled_settlement.forbidden_link")
	})

	t.Run("Case 6: Whitespace ExpectedOutcome trimming, inheritance, and invalid_outcome rejection", func(t *testing.T) {
		subALO, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-ws-alo", "a-ws-alo", "", 5, 0, false, 1, "", "acked", 13401)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13401, []*proxypb.TraceSpan{subALO})
		aloSpans := []*proxypb.TraceSpan{subALO, ccA, scA, prA, ackALO}

		subEOD, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-ws-eod", "a-ws-eod", "", 5, 0, true, 1, "", "", 13402)
		subEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subEOD.StatusMessage = "perm fail"
		subEOD.Events = append(subEOD.Events, &proxypb.SpanEvent{Name: "exception"})
		prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackEOD := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 13402, []*proxypb.TraceSpan{subEOD})
		ackEOD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackEOD.StatusMessage = "perm fail"
		ackEOD.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		eodSpans := []*proxypb.TraceSpan{subEOD, ccE, scE, prE, ackEOD}

		// 1. Whitespace-padded eod_failed with ExactlyOnce=false -> FAIL subscribe.expectation.conflict (Go & C++).
		for _, wsOutcome := range []string{" eod_failed ", "\tEOD_FAILED\n"} {
			for _, caps := range []*proxypb.GetCapabilitiesResponse{goCaps, cppCaps} {
				assertHasViolation(t, Verify(caps, cloneSpans(aloSpans), &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe: &SubscribeExpectation{
						ExactlyOnce:     BoolPtr(false),
						ExpectedOutcome: wsOutcome,
					},
				}), CodeCustomCheckFailed, "subscribe.expectation.conflict")

				// Per-message whitespace-only ExpectedOutcome inherits top-level " eod_failed ".
				assertHasViolation(t, Verify(caps, cloneSpans(aloSpans), &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe: &SubscribeExpectation{
						ExactlyOnce:     BoolPtr(false),
						ExpectedOutcome: wsOutcome,
						Messages:        []SubscribeMessageExpectation{{ExpectedOutcome: "   "}},
					},
				}), CodeCustomCheckFailed, "subscribe.expectation.conflict")
			}

			// 2. Whitespace-padded eod_failed with ExactlyOnce=nil against ALO -> FAIL subscribe.attr.exactly_once_delivery.
			assertHasViolation(t, Verify(goCaps, cloneSpans(aloSpans), &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce: nil,
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-ws-alo",
						ExpectedOutcome: wsOutcome,
						Settlements:     []SettlementExpectation{{Operation: "ack"}},
					}},
				},
			}), CodeWrongAttributeValue, "subscribe.attr.exactly_once_delivery")

			// 3. Whitespace-padded eod_failed with ExactlyOnce=nil against valid EOD -> PASS.
			if res := Verify(goCaps, cloneSpans(eodSpans), &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce: nil,
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-ws-eod",
						ExpectedOutcome: wsOutcome,
						Settlements:     []SettlementExpectation{{Operation: "ack", ExpectError: true}},
					}},
				},
			}); !res.Passed() {
				t.Fatalf("whitespace %q against valid EOD expected PASS, got:\n%s", wsOutcome, res.Format())
			}
		}

		// 4. Whitespace-padded " acked ", " nacked ", " expired " work identically to trimmed forms.
		if res := Verify(goCaps, cloneSpans(aloSpans), &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:     BoolPtr(false),
				ExpectedOutcome: " acked ",
				Messages:        []SubscribeMessageExpectation{{MessageID: "m-ws-alo", ExpectedOutcome: " ACKED ", Settlements: []SettlementExpectation{{Operation: "ack"}}}},
			},
		}); !res.Passed() {
			t.Fatalf("whitespace acked expected PASS, got:\n%s", res.Format())
		}

		subNk, ccNk, scNk, prNk, _ := makeGoSubscribeMessageSpans("p", "s", "m-ws-nk", "a-ws-nk", "", 5, 0, false, 1, "", "nacked", 13403)
		prNk.Events = append(prNk.Events, &proxypb.SpanEvent{Name: "nack called"})
		nackWs := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 13403, []*proxypb.TraceSpan{subNk})
		if res := Verify(goCaps, []*proxypb.TraceSpan{subNk, ccNk, scNk, prNk, nackWs}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				ExpectedOutcome:     " nacked ",
				ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}},
			},
		}); !res.Passed() {
			t.Fatalf("whitespace nacked expected PASS, got:\n%s", res.Format())
		}

		subExp, ccEx, scEx, _, _ := makeGoSubscribeMessageSpans("p", "s", "m-ws-ex", "a-ws-ex", "", 5, 0, false, 1, "", "expired", 13404)
		if res := Verify(goCaps, []*proxypb.TraceSpan{subExp, ccEx, scEx}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:     BoolPtr(false),
				ExpectedOutcome: " EXPIRED ",
			},
		}); !res.Passed() {
			t.Fatalf("whitespace expired expected PASS, got:\n%s", res.Format())
		}

		// 5. Whitespace-only top-level ExpectedOutcome on empty SubscribeExpectation does not demand spans.
		if res := Verify(goCaps, nil, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe:    &SubscribeExpectation{ExpectedOutcome: "   "},
		}); !res.Passed() {
			t.Fatalf("whitespace-only ExpectedOutcome on empty expectation expected PASS, got:\n%s", res.Format())
		}

		// 6. Unknown non-empty ExpectedOutcome fails upfront with subscribe.expectation.invalid_outcome.
		for _, badExp := range []*SubscribeExpectation{
			{ExpectedOutcome: "bogus"},
			{Messages: []SubscribeMessageExpectation{{ExpectedOutcome: " invalid_state "}}},
			{ExpectedOutcome: "bogus_top", Messages: []SubscribeMessageExpectation{{ExpectedOutcome: "acked"}}},
		} {
			assertHasViolation(t, Verify(goCaps, cloneSpans(aloSpans), &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe:    badExp,
			}), CodeCustomCheckFailed, "subscribe.expectation.invalid_outcome")
		}

		// 7. sampledSubAllowsSettlementOp trims whitespace consistently.
		if !sampledSubAllowsSettlementOp(SubscribeMessageExpectation{ExpectedOutcome: " acked "}, "ack", false) {
			t.Fatal("expected sampledSubAllowsSettlementOp to allow ack for \" acked \"")
		}
		if !sampledSubAllowsSettlementOp(SubscribeMessageExpectation{ExpectedOutcome: " eod_failed "}, "nack", false) {
			t.Fatal("expected sampledSubAllowsSettlementOp to allow nack for \" eod_failed \"")
		}
	})
}

func TestWave14Regressions_StrictValidTargetsUnmatchedAckNackPassBALOAndVerifyPublish(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	t.Run("Obj1: isStrictValidSampledSubSpan and hardened validSampledSubByKey", func(t *testing.T) {
		goProf := ProfileGoCanonical()
		if isStrictValidSampledSubSpan(nil, goProf, "s") {
			t.Fatal("expected nil span to fail isStrictValidSampledSubSpan")
		}

		noScopeProf := goProf
		noScopeProf.ExpectedScope = ""
		badCandidate := goSpan(testTraceID(14001), testSpanID(14001), "", "custom", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)
		badCandidate.InstrumentationScopeName = ""
		badCandidate.Attributes["messaging.system"] = strAV("gcp_pubsub")
		badCandidate.Attributes["messaging.destination.name"] = strAV("s")
		if isStrictValidSampledSubSpan(badCandidate, noScopeProf, "s") {
			t.Fatal("expected non-pubsub empty-scope span to fail fallback isCandidatePubSubSpan")
		}
		goodNoScope := goSpan(testTraceID(14002), testSpanID(14002), "", "s subscribe", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)
		goodNoScope.Attributes["messaging.system"] = strAV("gcp_pubsub")
		goodNoScope.Attributes["messaging.destination.name"] = strAV("s")
		if !isStrictValidSampledSubSpan(goodNoScope, noScopeProf, "s") {
			t.Fatal("expected valid Go subscribe span to pass when ExpectedScope is empty")
		}
		if !isStrictValidSampledSubSpan(goodNoScope, noScopeProf, "") {
			t.Fatal("expected valid Go subscribe span with subID=\"\" to pass when destination is non-empty")
		}
		anyKindProf := goProf
		anyKindProf.SubscribeSpanKind = proxypb.SpanKind_SPAN_KIND_UNSPECIFIED
		consumerSub := goSpan(testTraceID(14003), testSpanID(14003), "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
		consumerSub.Attributes["messaging.system"] = strAV("gcp_pubsub")
		consumerSub.Attributes["messaging.destination.name"] = strAV("s")
		if !isStrictValidSampledSubSpan(consumerSub, anyKindProf, "s") {
			t.Fatal("expected CONSUMER span to pass when SubscribeSpanKind is UNSPECIFIED")
		}
		if isStrictValidSampledSubSpan(consumerSub, goProf, "s") {
			t.Fatal("expected CONSUMER span to fail strict ProfileGo check")
		}

		// Symmetry checks: TraceFlags & 0x01 and messaging.destination.name.
		unsampSub := cloneSpans([]*proxypb.TraceSpan{goodNoScope})[0]
		unsampSub.TraceFlags = 0
		if isStrictValidSampledSubSpan(unsampSub, goProf, "s") {
			t.Fatal("expected TraceFlags=0 span to fail isStrictValidSampledSubSpan")
		}
		noDstSub := cloneSpans([]*proxypb.TraceSpan{goodNoScope})[0]
		delete(noDstSub.Attributes, "messaging.destination.name")
		if isStrictValidSampledSubSpan(noDstSub, goProf, "s") {
			t.Fatal("expected missing messaging.destination.name to fail isStrictValidSampledSubSpan")
		}
		emptyDstSub := cloneSpans([]*proxypb.TraceSpan{goodNoScope})[0]
		emptyDstSub.Attributes["messaging.destination.name"] = strAV("")
		if isStrictValidSampledSubSpan(emptyDstSub, goProf, "") {
			t.Fatal("expected empty messaging.destination.name to fail isStrictValidSampledSubSpan with subID=\"\"")
		}
		otherDstSub := cloneSpans([]*proxypb.TraceSpan{goodNoScope})[0]
		otherDstSub.Attributes["messaging.destination.name"] = strAV("other-sub")
		if isStrictValidSampledSubSpan(otherDstSub, goProf, "s") {
			t.Fatal("expected mismatched messaging.destination.name to fail isStrictValidSampledSubSpan")
		}

		// End-to-end negative probes for extra unmatched "s subscribe" spans under AllowExtraPubSubSpans=true.
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m14-1", "a14-1", "", 5, 0, true, 1, "", "acked", 14010)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14010, []*proxypb.TraceSpan{sub1})

		makeBadExtraSub := func(seq int, scope string, kind proxypb.SpanKind, msgSys string) *proxypb.TraceSpan {
			sp := goSpan(testTraceID(14020+seq), testSpanID(140200+seq), "", "s subscribe", kind, time.Duration(seq)*time.Millisecond)
			sp.InstrumentationScopeName = scope
			sp.Attributes["messaging.destination.name"] = strAV("s")
			if msgSys != "" {
				sp.Attributes["messaging.system"] = strAV(msgSys)
			}
			return sp
		}

		badProbes := []struct {
			name string
			sub  *proxypb.TraceSpan
		}{
			{"empty_scope_server_kafka", makeBadExtraSub(1, "", proxypb.SpanKind_SPAN_KIND_SERVER, "kafka")},
			{"empty_scope_internal_pubsub", makeBadExtraSub(2, "", proxypb.SpanKind_SPAN_KIND_INTERNAL, "gcp_pubsub")},
			{"go_scope_wrong_kind_consumer", makeBadExtraSub(3, GoPubSubScopeName, proxypb.SpanKind_SPAN_KIND_CONSUMER, "gcp_pubsub")},
			{"cross_sdk_cpp_scope", makeBadExtraSub(4, CppScopeName, proxypb.SpanKind_SPAN_KIND_INTERNAL, "gcp_pubsub")},
			{"missing_messaging_system", makeBadExtraSub(5, GoPubSubScopeName, proxypb.SpanKind_SPAN_KIND_INTERNAL, "")},
		}
		expObj1 := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m14-1",
					AckID:       "a14-1",
					ExactlyOnce: BoolPtr(true),
					Settlements: []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}
		for _, tc := range badProbes {
			// Unmatched ack targeting bad extra sub directly -> FAIL dangling_link.
			extraAck := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14030, []*proxypb.TraceSpan{tc.sub})
			spans := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, ack1, tc.sub, extraAck}
			assertHasViolation(t, Verify(goCaps, cloneSpans(spans), expObj1), CodeMissingLink, "subscribe.settlement.dangling_link")

			// Phase 5 batched ack targeting [sub1 (matched), bad extra sub] -> FAIL dangling_link.
			sub1B := cloneSpans([]*proxypb.TraceSpan{sub1})[0]
			sub1B.Links = nil
			sub1B.Events = nil
			batchedAck := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 14031, []*proxypb.TraceSpan{sub1B, tc.sub})
			batchSpans := []*proxypb.TraceSpan{sub1B, cc1, sc1, pr1, batchedAck, tc.sub}
			assertHasViolation(t, Verify(goCaps, batchSpans, expObj1), CodeMissingLink, "subscribe.settlement.dangling_link")
		}

		// Valid extra Go subscribe target PASSES both standalone and Phase 5 batched.
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m14-2", "a14-2", "", 5, 0, true, 1, "", "acked", 14040)
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14040, []*proxypb.TraceSpan{sub2})
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, ack1, sub2, cc2, sc2, pr2, ack2}, expObj1); !res.Passed() {
			t.Fatalf("valid extra Go subscribe target expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Obj2: Edge Case A Settlements=nil, eod_failed, retries, and link_incompatible_op", func(t *testing.T) {
		// 1. Incompatible unmatched extra s ack linking to matched "nacked" or "expired" sub -> FAIL under BOTH strict & non-strict.
		for _, outcome := range []string{"nacked", "expired"} {
			subN, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "m-inc-"+outcome, "a-inc-"+outcome, "", 5, 0, false, 1, "", outcome, 14100)
			if outcome == "nacked" {
				prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
			}
			nackN := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 14100, []*proxypb.TraceSpan{subN})
			bogusAck := goSpan(testTraceID(14101), testSpanID(141001), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 5*time.Millisecond)
			bogusAck.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                strAV("p"),
				"gcp.resource.name":             strAV("projects/p/subscriptions/s"),
				"messaging.system":              strAV("gcp_pubsub"),
				"messaging.destination.name":    strAV("s"),
				"messaging.batch.message_count": intAV(1),
				"code.function":                 strAV("sendAck"),
			}
			bogusAck.Links = []*proxypb.SpanLink{{TraceId: subN.TraceId, SpanId: subN.SpanId}}

			baseSpans := []*proxypb.TraceSpan{subN, ccN, scN, prN, bogusAck}
			var settleExp []SettlementExpectation
			if outcome == "nacked" {
				baseSpans = append(baseSpans, nackN)
				settleExp = []SettlementExpectation{{Operation: "nack"}}
			}
			for _, strictOps := range []bool{false, true} {
				exp := &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						StrictSettlementOps: strictOps,
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-inc-" + outcome,
							AckID:           "a-inc-" + outcome,
							ExactlyOnce:     BoolPtr(false),
							ExpectedOutcome: outcome,
							Settlements:     settleExp,
						}},
					},
				}
				if outcome == "nacked" && !strictOps {
					// Also exercise Settlements: nil on nacked + bogus extra ack.
					exp.Subscribe.Messages[0].Settlements = nil
				}
				assertHasViolation(t, Verify(goCaps, cloneSpans(baseSpans), exp), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
			}
		}

		// 2. Edge Case A: Go ALO "acked", EOD "acked", and ALO "nacked" with Settlements: nil pass across 24 permutations.
		for _, tc := range []struct {
			name    string
			isEOD   bool
			outcome string
			op      string
		}{
			{"ALO_acked", false, "acked", "ack"},
			{"EOD_acked", true, "acked", "ack"},
			{"ALO_nacked", false, "nacked", "nack"},
		} {
			subA, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-edge-a", "a-edge-a", "", 5, 0, tc.isEOD, 1, "", tc.outcome, 14110)
			prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			setA := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, 14110, []*proxypb.TraceSpan{subA})
			var modA *proxypb.TraceSpan
			if tc.isEOD {
				modA = makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 14111, []*proxypb.TraceSpan{subA})
			} else {
				modA = makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 14112, nil)
			}
			base := []*proxypb.TraceSpan{subA, ccA, scA, prA, modA, setA}

			for _, allowExtra := range []bool{false, true} {
				expA := &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: allowExtra,
					Subscribe: &SubscribeExpectation{
						StrictSettlementOps: false,
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-edge-a",
							AckID:           "a-edge-a",
							ExactlyOnce:     BoolPtr(tc.isEOD),
							ExpectedOutcome: tc.outcome,
							Settlements:     nil,
						}},
					},
				}
				var baseFmt string
				for seed := int64(0); seed < 24; seed++ {
					perm := cloneSpans(base)
					rng := rand.New(rand.NewSource(seed))
					rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
					res := Verify(goCaps, perm, expA)
					if !res.Passed() {
						t.Fatalf("Edge Case A (%s, allowExtra=%v) seed=%d expected PASS, got:\n%s", tc.name, allowExtra, seed, res.Format())
					}
					if seed == 0 {
						baseFmt = res.Format()
					} else if res.Format() != baseFmt {
						t.Fatalf("Edge Case A (%s, allowExtra=%v) seed=%d Format() differed", tc.name, allowExtra, seed)
					}
				}
			}

			// Contrast: missing reverse link on subA -> setA MUST FAIL subscribe.settlement.reverse_link.
			noRev := cloneSpans(base)
			var filteredLinks []*proxypb.SpanLink
			for _, l := range noRev[0].Links {
				if l.SpanId != setA.SpanId {
					filteredLinks = append(filteredLinks, l)
				}
			}
			noRev[0].Links = filteredLinks
			assertHasViolation(t, Verify(goCaps, noRev, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-edge-a",
						AckID:           "a-edge-a",
						ExactlyOnce:     BoolPtr(tc.isEOD),
						ExpectedOutcome: tc.outcome,
					}},
				},
			}), CodeBrokenBidirectionalLink, "subscribe.settlement.reverse_link")

			// Contrast: duplicate settlement under AllowExtraPubSubSpans=false MUST FAIL traces.no_extra_pubsub_spans.
			dupSet := cloneSpans([]*proxypb.TraceSpan{setA})[0]
			dupSet.TraceId = testTraceID(14119)
			dupSet.SpanId = testSpanID(141190)
			assertHasViolation(t, Verify(goCaps, append(cloneSpans(base), dupSet), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-edge-a",
						AckID:           "a-edge-a",
						ExactlyOnce:     BoolPtr(tc.isEOD),
						ExpectedOutcome: tc.outcome,
					}},
				},
			}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

			// Contrast: duplicate settlement under AllowExtraPubSubSpans=false with explicit Settlements: [op] MUST ALSO FAIL.
			assertHasViolation(t, Verify(goCaps, append(cloneSpans(base), dupSet), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-edge-a",
						AckID:           "a-edge-a",
						ExactlyOnce:     BoolPtr(tc.isEOD),
						ExpectedOutcome: tc.outcome,
						Settlements:     []SettlementExpectation{{Operation: tc.op}},
					}},
				},
			}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

			// Contrast: StrictSettlementOps=true + Settlements=nil MUST FAIL link_incompatible_op + dangling_reverse_link.
			resStrict := Verify(goCaps, cloneSpans(base), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					StrictSettlementOps: true,
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-edge-a",
						AckID:           "a-edge-a",
						ExactlyOnce:     BoolPtr(tc.isEOD),
						ExpectedOutcome: tc.outcome,
					}},
				},
			})
			assertHasViolation(t, resStrict, CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
			assertHasViolation(t, resStrict, CodeMissingLink, "subscribe.settlement.dangling_reverse_link")

			// Contrast: StrictSettlementOps=true + explicit Settlements + extra linked dupSet reverse-linked by subA -> FAIL dangling_reverse_link.
			if tc.op == "ack" && tc.isEOD {
				withExtraRev := cloneSpans(base)
				withExtraRev[0].Links = append(withExtraRev[0].Links, &proxypb.SpanLink{
					TraceId:    dupSet.TraceId,
					SpanId:     dupSet.SpanId,
					Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("ack")},
				})
				withExtraRev = append(withExtraRev, dupSet)
				assertHasViolation(t, Verify(goCaps, withExtraRev, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						StrictSettlementOps: true,
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-edge-a",
							AckID:           "a-edge-a",
							ExactlyOnce:     BoolPtr(true),
							ExpectedOutcome: "acked",
							Settlements:     []SettlementExpectation{{Operation: "ack"}},
						}},
					},
				}), CodeMissingLink, "subscribe.settlement.dangling_reverse_link")
			}
		}

		// 3. Edge Case A3: ExpectedOutcome="eod_failed" + Settlements:nil (full error + partial-batch failure), plus non-error fail.
		for _, partial := range []bool{false, true} {
			subF, ccF, scF, prF, _ := makeGoSubscribeMessageSpans("p", "s", "m-eodf", "a-eodf", "", 5, 0, true, 1, "", "", 14130)
			subF.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			subF.StatusMessage = "permission denied"
			subF.Events = append(subF.Events, &proxypb.SpanEvent{Name: "exception"})
			prF.Events = append(prF.Events, &proxypb.SpanEvent{Name: "ack called"})
			modF := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 14130, []*proxypb.TraceSpan{subF})
			ackF := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14131, []*proxypb.TraceSpan{subF})
			ackF.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			if partial {
				ackF.StatusMessage = eodPartialBatchErrorMsg
			} else {
				ackF.StatusMessage = "permission denied"
				ackF.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			}

			spansF := []*proxypb.TraceSpan{subF, ccF, scF, prF, modF, ackF}
			expF := &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-eodf",
						AckID:           "a-eodf",
						ExpectedOutcome: "eod_failed",
						Settlements:     nil,
					}},
				},
			}
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(spansF)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				if res := Verify(goCaps, perm, expF); !res.Passed() {
					t.Fatalf("Edge Case A3 (partial=%v) seed=%d expected PASS, got:\n%s", partial, seed, res.Format())
				}
			}

			if !partial {
				// Non-error ack on eod_failed MUST FAIL settlement.ack.status.
				badAckF := cloneSpans(spansF)
				badAckF[5].StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
				badAckF[5].StatusMessage = ""
				badAckF[5].Events = nil
				assertHasViolation(t, Verify(goCaps, badAckF, expF), CodeWrongStatusCode, "settlement.ack.status")
			}
		}

		// 4a. Real Go iterator.go EOD transient retry (single error settlement span + 2nd "<op> end" event on subscribe) passes for Settlements=nil and explicit ExpectError=true.
		subGoR, ccGoR, scGoR, prGoR, _ := makeGoSubscribeMessageSpans("p", "s", "m-retry-go", "a-retry-go", "", 5, 0, true, 1, "", "acked", 14138)
		prGoR.Events = append(prGoR.Events, &proxypb.SpanEvent{Name: "ack called"})
		modGoR := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 14138, []*proxypb.TraceSpan{subGoR})
		ackErrGoR := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14139, []*proxypb.TraceSpan{subGoR})
		ackErrGoR.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackErrGoR.StatusMessage = "unavailable"
		ackErrGoR.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		subGoR.Events = append(subGoR.Events, &proxypb.SpanEvent{Name: "ack end"})
		goRetrySpans := []*proxypb.TraceSpan{subGoR, ccGoR, scGoR, prGoR, modGoR, ackErrGoR}

		for _, explicit := range []bool{false, true} {
			var setList []SettlementExpectation
			if explicit {
				setList = []SettlementExpectation{{Operation: "ack", ExpectError: true}}
			}
			expGoRetry := &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-retry-go",
						AckID:           "a-retry-go",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "acked",
						Settlements:     setList,
					}},
				},
			}
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(goRetrySpans)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				if res := Verify(goCaps, perm, expGoRetry); !res.Passed() {
					t.Fatalf("Go iterator.go EOD retry (explicit=%v) seed=%d expected PASS, got:\n%s", explicit, seed, res.Format())
				}
			}
		}

		// Raw slice with nil *SpanLink on Go EOD retry span must safely skip nil link under Settlements=nil.
		rawGoErr := cloneSpans([]*proxypb.TraceSpan{ackErrGoR})[0]
		rawGoErr.Links = append([]*proxypb.SpanLink{nil}, rawGoErr.Links...)
		if res := Verify(goCaps, []*proxypb.TraceSpan{subGoR, ccGoR, scGoR, prGoR, modGoR, rawGoErr}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-retry-go",
					AckID:           "a-retry-go",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
				}},
			},
		}); !res.Passed() {
			t.Fatalf("Go raw slice with nil SpanLink expected PASS, got:\n%s", res.Format())
		}

		// 4b. Dual-span EOD ack retry (ack_err + ack_ok, both reverse-linked):
		//     - On ProfileGo: explicit Settlements=[{Operation:"ack"}] passes (Wave 14/HUNT_F compatibility),
		//       while Settlements=nil (2 ack start, 2 ack end) MUST FAIL settlement.ack.status because Go iterator.go never emits dual settlement spans.
		//     - On ProfileCPP: dual-span ackErr + ackOK passes under Settlements=nil via hasNonErrForSub.
		subR, ccR, scR, prR, _ := makeGoSubscribeMessageSpans("p", "s", "m-retry", "a-retry", "", 5, 0, true, 1, "", "acked", 14140)
		prR.Events = append(prR.Events, &proxypb.SpanEvent{Name: "ack called"})
		modR := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 14140, []*proxypb.TraceSpan{subR})
		ackErr := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14141, []*proxypb.TraceSpan{subR})
		ackErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackErr.StatusMessage = "unavailable"
		ackErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		ackOK := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14142, []*proxypb.TraceSpan{subR})
		retrySpans := []*proxypb.TraceSpan{subR, ccR, scR, prR, modR, ackErr, ackOK}

		// Go explicit Settlements=[{Operation:"ack"}] preserves Wave 14/HUNT_F compatibility across 24 shuffles.
		expExplicitRetry := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-retry",
					AckID:           "a-retry",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(retrySpans)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			if res := Verify(goCaps, perm, expExplicitRetry); !res.Passed() {
				t.Fatalf("Go explicit EOD retry seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
		}

		// Go Settlements=nil with dual-span ackErr + ackOK (2 start, 2 end) MUST FAIL settlement.ack.status.
		assertHasViolation(t, Verify(goCaps, cloneSpans(retrySpans), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-retry",
					AckID:           "a-retry",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
				}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// Negative contrast: AllowExtraPubSubSpans=false rejects extra ackErr; ackErr alone without 2nd ack end for "acked" FAILS status.
		assertHasViolation(t, Verify(goCaps, cloneSpans(retrySpans), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-retry",
					AckID:           "a-retry",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
				}},
			},
		}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		onlyErrAck := []*proxypb.TraceSpan{subR, ccR, scR, prR, modR, ackErr}
		assertHasViolation(t, Verify(goCaps, cloneSpans(onlyErrAck), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-retry",
					AckID:           "a-retry",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
				}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// ProfileCPP dual-span EOD retry (ackErr + ackOK) with Settlements=nil preserves hasNonErrForSub across 24 shuffles (incl. raw nil *SpanLink).
		cppCapsW14 := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
		makeCppRetrySpans := func(eod bool, op string) []*proxypb.TraceSpan {
			tid := testTraceID(14145)
			sid := testSpanID(141450)
			resAttr := "ack"
			if op == "nack" {
				resAttr = "nack"
			}
			subC := cppSpan(tid, sid, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0)
			subC.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			subC.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                          strAV("p"),
				"messaging.system":                                        strAV("gcp_pubsub"),
				"messaging.destination.name":                              strAV("s"),
				"messaging.operation.type":                                strAV("subscribe"),
				"messaging.message.id":                                    strAV("m-cpp-r"),
				"messaging.message.envelope.size":                         intAV(25),
				"messaging.gcp_pubsub.message.ack_id":                     strAV("a-cpp-r"),
				"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(eod),
				"messaging.gcp_pubsub.result":                             strAV(resAttr),
			}
			subC.Events = []*proxypb.SpanEvent{{Name: "gl-cpp." + op + "_start"}, {Name: "gl-cpp." + op + "_end"}}
			ccC := cppSpan(tid, testSpanID(141451), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			ccC.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			ccC.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			schedC := cppSpan(tid, testSpanID(141452), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			schedC.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			schedC.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			procC := cppSpan(tid, testSpanID(141453), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			procC.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			procC.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}

			mkSettle := func(seq int, isErr bool) *proxypb.TraceSpan {
				sp := cppSpan(testTraceID(14146+seq), testSpanID(141460+seq), "", "s "+op, proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(10+seq)*time.Millisecond)
				statusStr := "OK"
				if isErr {
					sp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
					sp.StatusMessage = "unavailable"
					statusStr = "UNAVAILABLE"
				}
				sp.Attributes = map[string]*proxypb.AttributeValue{
					"gcp.project_id":                             strAV("p"),
					"messaging.system":                           strAV("gcp_pubsub"),
					"messaging.destination.name":                 strAV("s"),
					"code.function.name":                         strAV("pubsub::AckHandler::" + op),
					"messaging.gcp_pubsub.message.ack_id":        strAV("a-cpp-r"),
					"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
					"messaging.operation.type":                   strAV("settle"),
					"gl-cpp.status_code":                         strAV(statusStr),
				}
				sp.Links = []*proxypb.SpanLink{{TraceId: tid, SpanId: sid}}
				return sp
			}
			return []*proxypb.TraceSpan{subC, ccC, schedC, procC, mkSettle(1, true), mkSettle(2, false)}
		}
		for _, tcCpp := range []struct {
			op      string
			outcome string
		}{
			{op: "ack", outcome: "acked"},
			{op: "nack", outcome: "nacked"},
		} {
			cppRetrySpans := makeCppRetrySpans(true, tcCpp.op)
			expCppRetry := &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-cpp-r",
						AckID:           "a-cpp-r",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: tcCpp.outcome,
					}},
				},
			}
			for seed := int64(0); seed < 24; seed++ {
				perm := cloneSpans(cppRetrySpans)
				rng := rand.New(rand.NewSource(seed))
				rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
				if res := Verify(cppCapsW14, perm, expCppRetry); !res.Passed() {
					t.Fatalf("ProfileCPP dual-span %s retry seed=%d expected PASS, got:\n%s", tcCpp.op, seed, res.Format())
				}
			}
			// Raw nil *SpanLink in non-error partner span must safely skip nil link in hasNonErrForSub scan.
			rawCppRetry := cloneSpans(cppRetrySpans)
			rawCppRetry[5].Links = append([]*proxypb.SpanLink{nil}, rawCppRetry[5].Links...)
			if res := Verify(cppCapsW14, rawCppRetry, expCppRetry); !res.Passed() {
				t.Fatalf("ProfileCPP raw slice nil SpanLink expected PASS, got:\n%s", res.Format())
			}
			// ProfileCPP AllowExtraPubSubSpans=false rejects dual-span retry; ALO EOD=false rejects spurious error even with ok partner.
			assertHasViolation(t, Verify(cppCapsW14, cloneSpans(cppRetrySpans), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				Subscribe:             expCppRetry.Subscribe,
			}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
			assertHasViolation(t, Verify(cppCapsW14, makeCppRetrySpans(false, tcCpp.op), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-cpp-r",
						AckID:           "a-cpp-r",
						ExactlyOnce:     BoolPtr(false),
						ExpectedOutcome: tcCpp.outcome,
					}},
				},
			}), CodeWrongStatusCode, "settlement."+tcCpp.op+".status")
		}

		// 5. Unmatched ack with duplicate link targeting [subR, subR] MUST FAIL subscribe.settlement.duplicate_link.
		dupLinkAck := cloneSpans([]*proxypb.TraceSpan{ackOK})[0]
		dupLinkAck.Links = []*proxypb.SpanLink{
			{TraceId: subR.TraceId, SpanId: subR.SpanId},
			{TraceId: subR.TraceId, SpanId: subR.SpanId},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subR, ccR, scR, prR, modR, dupLinkAck}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-retry",
					AckID:           "a-retry",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
				}},
			},
		}), CodeBrokenBidirectionalLink, "subscribe.settlement.duplicate_link")

		// Defensive nil SpanLink guard check without proto.Clone stripping nil pointers (explicit Settlements).
		rawAckErr := cloneSpans([]*proxypb.TraceSpan{ackErr})[0]
		rawAckOK := cloneSpans([]*proxypb.TraceSpan{ackOK})[0]
		rawAckOK.Links = append([]*proxypb.SpanLink{nil}, rawAckOK.Links...)
		if res := Verify(goCaps, []*proxypb.TraceSpan{subR, ccR, scR, prR, modR, rawAckErr, rawAckOK}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-retry",
					AckID:           "a-retry",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}); !res.Passed() {
			t.Fatalf("raw slice with nil SpanLink expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Obj3: Edge Case B Phase 4 Pass B unlinked ALO receipt modack slots", func(t *testing.T) {
		// 1. Two sequential ALO deliveries [M1, M2] with 2 unlinked receipt modacks verifying [M1] -> PASS across 24 shuffles.
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-alo-1", "a-alo-1", "", 5, 0, false, 1, "", "acked", 14200)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14200, []*proxypb.TraceSpan{sub1})
		mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 14201, nil)

		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-alo-2", "a-alo-2", "", 5, 0, false, 1, "", "acked", 14202)
		delete(sub2.Attributes, "messaging.gcp_pubsub.exactly_once_delivery") // Also test omitted ALO EOS attr.
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		ack2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14202, []*proxypb.TraceSpan{sub2})
		mod2 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 14203, nil)

		baseALO := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, mod1, ack1, sub2, cc2, sc2, pr2, mod2, ack2}
		expM1Only := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: false,
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-alo-1",
					AckID:       "a-alo-1",
					ExactlyOnce: BoolPtr(false),
					Settlements: []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}
		var baseFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(baseALO)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, expM1Only)
			if !res.Passed() {
				t.Fatalf("Edge Case B 2-ALO verifying [M1] seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baseFmt = res.Format()
			} else if res.Format() != baseFmt {
				t.Fatalf("Edge Case B seed=%d Format() differed", seed)
			}
		}

		// 2. Excess 3rd unlinked modack -> FAIL subscribe.modack.missing_link.
		mod3 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 14204, nil)
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(baseALO), mod3), expM1Only), CodeMissingLink, "subscribe.modack.missing_link")

		// 3. Strict AllowExtraPubSubSpans=false MUST reject 2nd unlinked modack + extra M2.
		resStrict := Verify(goCaps, cloneSpans(baseALO), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe:             expM1Only.Subscribe,
		})
		assertHasViolation(t, resStrict, CodeMissingLink, "subscribe.modack.missing_link")
		assertHasViolation(t, resStrict, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		// 4. Extra M2 ALO with its OWN linked receipt modack MUST NOT grant an extra unlinked slot.
		mod2Linked := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 14205, []*proxypb.TraceSpan{sub2})
		mixedLinkedExtra := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, mod1, ack1, sub2, cc2, sc2, pr2, mod2Linked, ack2}
		if res := Verify(goCaps, cloneSpans(mixedLinkedExtra), expM1Only); !res.Passed() {
			t.Fatalf("Edge Case B linked extra M2 + M1 unlinked expected PASS, got:\n%s", res.Format())
		}
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(mixedLinkedExtra), mod3), expM1Only), CodeMissingLink, "subscribe.modack.missing_link")

		// 5. Matched EOD [M1_eod] + extra ALO [sub2] + 1 unlinked receipt modack -> PASS across 24 shuffles.
		subE, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-eod-m", "a-eod-m", "", 5, 0, true, 1, "", "acked", 14210)
		prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: "ack called"})
		modE := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 14210, []*proxypb.TraceSpan{subE})
		ackE := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14211, []*proxypb.TraceSpan{subE})
		mixedEODBase := []*proxypb.TraceSpan{subE, ccE, scE, prE, modE, ackE, sub2, cc2, sc2, pr2, mod2, ack2}
		expEODSubset := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-eod-m",
					AckID:       "a-eod-m",
					ExactlyOnce: BoolPtr(true),
					Settlements: []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(mixedEODBase)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			if res := Verify(goCaps, perm, expEODSubset); !res.Passed() {
				t.Fatalf("matched EOD + extra ALO seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
		}

		// 6. Pure-EOD (extra sub is ALSO EOD) MUST STILL reject unlinked modack!
		sub2EOD, cc2E, sc2E, pr2E, _ := makeGoSubscribeMessageSpans("p", "s", "m-eod-x", "a-eod-x", "", 5, 0, true, 1, "", "acked", 14215)
		pr2E.Events = append(pr2E.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod2E := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 14215, []*proxypb.TraceSpan{sub2EOD})
		ack2E := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 14216, []*proxypb.TraceSpan{sub2EOD})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subE, ccE, scE, prE, modE, ackE, sub2EOD, cc2E, sc2E, pr2E, mod2E, ack2E, mod1}, expEODSubset), CodeMissingLink, "subscribe.modack.missing_link")

		// 7. Coverage edge: dropped -00 message trace ID excluding leaked ALO sub from extraSampledALOCount, plus duplicate-key sub span.
		dropTP := fmt.Sprintf("00-%s-%s-00", sub2.TraceId, testSpanID(14299))
		expWithDrop := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-eod-m", AckID: "a-eod-m", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{WireTraceparent: dropTP, ExpectDroppedUnsampled: true},
				},
			},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subE, ccE, scE, prE, modE, ackE, sub2, mod1}, expWithDrop), CodeUnexpectedSpan, "subscribe.unsampled_dropped")
	})

	t.Run("Obj4: verifyPublish AllowExtraPubSubSpans=true and isStrictValidSampledCreateSpan", func(t *testing.T) {
		goProf := ProfileGoCanonical()
		// 1. Unit tests of isStrictValidSampledCreateSpan.
		if isStrictValidSampledCreateSpan(nil, goProf, "t") {
			t.Fatal("expected nil span to fail isStrictValidSampledCreateSpan")
		}
		noScopeProf := goProf
		noScopeProf.ExpectedScope = ""
		badCand := goSpan(testTraceID(14301), testSpanID(143001), "", "custom", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
		badCand.InstrumentationScopeName = ""
		if isStrictValidSampledCreateSpan(badCand, noScopeProf, "t") {
			t.Fatal("expected non-pubsub empty-scope create candidate to fail")
		}
		goodCand := goSpan(testTraceID(14302), testSpanID(143002), "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0)
		goodCand.Attributes["messaging.system"] = strAV("gcp_pubsub")
		goodCand.Attributes["messaging.destination.name"] = strAV("t")
		if !isStrictValidSampledCreateSpan(goodCand, noScopeProf, "") {
			t.Fatal("expected valid create candidate with topicID=\"\" to pass")
		}
		if !isStrictValidSampledCreateSpan(goodCand, goProf, "t") {
			t.Fatal("expected valid create candidate with topicID=\"t\" to pass")
		}
		goodNoDst := cloneSpans([]*proxypb.TraceSpan{goodCand})[0]
		delete(goodNoDst.Attributes, "messaging.destination.name")
		if isStrictValidSampledCreateSpan(goodNoDst, goProf, "t") {
			t.Fatal("expected create without messaging.destination.name to fail")
		}

		// 2. Separate extra batch [M2_extra_create + M2_extra_publish] + [M1_create + M1_publish] verifying [M1] -> PASS across 24 permutations.
		pub1 := makeGoPublishBatchSpan("p", "t", 1, 14310)
		c1, fc1, b1, wtp1 := makeGoPublishMessageSpans("p", "t", "m-pub-1", "", 5, 14310, pub1, 1)
		pub2 := makeGoPublishBatchSpan("p", "t", 1, 14311)
		c2, fc2, b2, _ := makeGoPublishMessageSpans("p", "t", "m-pub-2", "", 7, 14311, pub2, 1)

		sepBase := []*proxypb.TraceSpan{c1, fc1, b1, pub1, c2, fc2, b2, pub2}
		expPubM1 := &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{
					MessageID:       "m-pub-1",
					WireTraceparent: wtp1,
				}},
			},
		}
		var sepFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(sepBase)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, expPubM1)
			if !res.Passed() {
				t.Fatalf("Separate extra batch seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				sepFmt = res.Format()
			} else if res.Format() != sepFmt {
				t.Fatalf("Separate extra batch seed=%d Format() differed", seed)
			}
		}

		// Negative contrasts for separate extra batch:
		assertHasViolation(t, Verify(goCaps, cloneSpans(sepBase), &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: false,
			Publish:               expPubM1.Publish,
		}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

		assertHasViolation(t, Verify(goCaps, cloneSpans(sepBase), &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages:   expPubM1.Publish.Messages,
			},
		}), CodeMissingSpan, "publish.batch.count")

		// 3. Shared batch [c1, c2] under pubShared (batch_count=2) verifying [M1] -> PASS across 24 shuffles.
		pubShared := makeGoPublishBatchSpan("p", "t", 2, 14320)
		cs1, fcs1, bs1, wtps1 := makeGoPublishMessageSpans("p", "t", "m-sh-1", "", 5, 14320, pubShared, 2)
		cs2, fcs2, bs2, _ := makeGoPublishMessageSpans("p", "t", "m-sh-2", "", 7, 14321, pubShared, 2)
		sharedBase := []*proxypb.TraceSpan{cs1, fcs1, bs1, cs2, fcs2, bs2, pubShared}
		expSharedM1 := &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{
					MessageID:       "m-sh-1",
					WireTraceparent: wtps1,
				}},
			},
		}
		var sharedFmt string
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(sharedBase)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, expSharedM1)
			if !res.Passed() {
				t.Fatalf("Shared batch seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				sharedFmt = res.Format()
			} else if res.Format() != sharedFmt {
				t.Fatalf("Shared batch seed=%d Format() differed", seed)
			}
		}

		// Batches strictness: len(Batches) > 0 forbids extra links -> FAIL dangling_link.
		assertHasViolation(t, Verify(goCaps, cloneSpans(sharedBase), &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Batches:  []PublishBatchExpectation{{MessageCount: 2}},
				Messages: expSharedM1.Publish.Messages,
			},
		}), CodeMissingLink, "publish.batch.dangling_link")

		// 4. Shared -01 (M1) + -00 dropped (M2) with batch_count=2 PASSES; adding extra sampled create MUST FAIL under anyDroppedUnsampled!
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(14390), testSpanID(143900))
		pubMix := makeGoPublishBatchSpan("p", "t", 2, 14330)
		cm1, fcm1, bm1, wtpm1 := makeGoPublishMessageSpans("p", "t", "m-mix-1", "", 5, 14330, pubMix, 2)
		expMix := &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{
					{MessageID: "m-mix-1", WireTraceparent: wtpm1},
					{ParentTraceparent: unsampTP, ExpectDroppedUnsampled: true},
				},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{cm1, fcm1, bm1, pubMix}, expMix); !res.Passed() {
			t.Fatalf("Shared -01 + -00 expected PASS, got:\n%s", res.Format())
		}
		pubMixExtra := cloneSpans([]*proxypb.TraceSpan{pubMix})[0]
		pubMixExtra.Links = append(pubMixExtra.Links, &proxypb.SpanLink{TraceId: cs2.TraceId, SpanId: cs2.SpanId})
		resMixLeak := Verify(goCaps, []*proxypb.TraceSpan{cm1, fcm1, bm1, cs2, pubMixExtra}, expMix)
		assertHasViolation(t, resMixLeak, CodeUnexpectedSpan, "publish.unsampled_dropped")
		assertHasViolation(t, resMixLeak, CodeMissingLink, "publish.batch.dangling_link")

		// 5. All negative publish probes under AllowExtraPubSubSpans=true.
		badCreateProbes := []struct {
			name string
			mut  func(c *proxypb.TraceSpan)
			link *proxypb.SpanLink
		}{
			{
				name: "unresolved_99999",
				link: &proxypb.SpanLink{TraceId: testTraceID(99999), SpanId: testSpanID(99999)},
			},
			{
				name: "wrong_topic_create",
				mut:  func(c *proxypb.TraceSpan) { c.Name = "other-topic create" },
			},
			{
				name: "wrong_span_name_subscribe",
				mut:  func(c *proxypb.TraceSpan) { c.Name = "s subscribe" },
			},
			{
				name: "wrong_scope",
				mut:  func(c *proxypb.TraceSpan) { c.InstrumentationScopeName = "my-app" },
			},
			{
				name: "wrong_kind",
				mut:  func(c *proxypb.TraceSpan) { c.Kind = proxypb.SpanKind_SPAN_KIND_INTERNAL },
			},
			{
				name: "wrong_messaging_system",
				mut:  func(c *proxypb.TraceSpan) { c.Attributes["messaging.system"] = strAV("kafka") },
			},
			{
				name: "wrong_destination_name",
				mut:  func(c *proxypb.TraceSpan) { c.Attributes["messaging.destination.name"] = strAV("other-topic") },
			},
			{
				name: "leaked_unsampled_trace_flags",
				mut:  func(c *proxypb.TraceSpan) { c.TraceFlags = 0 },
			},
		}
		for _, tc := range badCreateProbes {
			pubTest := makeGoPublishBatchSpan("p", "t", 2, 14340)
			cm, fcm, bm, wtpm := makeGoPublishMessageSpans("p", "t", "m-neg", "", 5, 14340, pubTest, 2)
			extraSp := makeGoPublishBatchSpan("p", "t", 1, 14341)
			ce, fce, be, _ := makeGoPublishMessageSpans("p", "t", "m-bad", "", 5, 14341, nil, 1)
			var extraLink *proxypb.SpanLink
			if tc.link != nil {
				extraLink = tc.link
			} else {
				tc.mut(ce)
				extraLink = &proxypb.SpanLink{TraceId: ce.TraceId, SpanId: ce.SpanId}
			}
			pubTest.Links = append(pubTest.Links, extraLink)
			_ = extraSp
			res := Verify(goCaps, []*proxypb.TraceSpan{cm, fcm, bm, ce, fce, be, pubTest}, &TraceExpectation{
				Topic:                 "projects/p/topics/t",
				AllowExtraPubSubSpans: true,
				Publish: &PublishExpectation{
					Messages: []PublishMessageExpectation{{MessageID: "m-neg", WireTraceparent: wtpm}},
				},
			})
			assertHasViolation(t, res, CodeMissingLink, "publish.batch.dangling_link")
		}

		// Duplicate link [c1, c1], non-empty link attrs, batch_count < linked, excess > remainingDropped, bogus 0-link publish.
		dupLinkPub := cloneSpans(sharedBase)
		dupLinkPub[6].Links = append(dupLinkPub[6].Links, &proxypb.SpanLink{TraceId: cs1.TraceId, SpanId: cs1.SpanId})
		assertHasViolation(t, Verify(goCaps, dupLinkPub, expSharedM1), CodeBrokenBidirectionalLink, "publish.batch.duplicate_link")

		attrLinkPub := cloneSpans(sharedBase)
		attrLinkPub[6].Links[1].Attributes = map[string]*proxypb.AttributeValue{"k": strAV("v")}
		assertHasViolation(t, Verify(goCaps, attrLinkPub, expSharedM1), CodeUnexpectedAttribute, "publish.batch.link_attrs_empty")

		underBatch := cloneSpans(sharedBase)
		underBatch[6].Attributes["messaging.batch.message_count"] = intAV(1)
		assertHasViolation(t, Verify(goCaps, underBatch, expSharedM1), CodeWrongAttributeValue, "publish.batch.attr.message_count")

		overBatch := cloneSpans(sharedBase)
		overBatch[6].Attributes["messaging.batch.message_count"] = intAV(3)
		assertHasViolation(t, Verify(goCaps, overBatch, expSharedM1), CodeWrongAttributeValue, "publish.batch.attr.message_count")

		bogusZeroLink := makeGoPublishBatchSpan("p", "t", 1, 14350)
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(sepBase), bogusZeroLink), expPubM1), CodeWrongAttributeValue, "publish.batch.attr.message_count")

		// Asymmetric reverse link: c1 reverse-links to pub2 (matched via c2) even though pub2 did NOT link to c1 -> MUST FAIL publish.create.dangling_link.
		asymSpans := cloneSpans(sepBase)
		asymSpans[0].Links = append(asymSpans[0].Links, &proxypb.SpanLink{
			TraceId:    pub2.TraceId,
			SpanId:     pub2.SpanId,
			Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("publish")},
		})
		assertHasViolation(t, Verify(goCaps, asymSpans, expPubM1), CodeMissingLink, "publish.create.dangling_link")

		// Defensive nil SpanLink guard check on publish without proto.Clone.
		rawShared := cloneSpans(sharedBase)
		rawShared[6].Links = append([]*proxypb.SpanLink{nil}, rawShared[6].Links...)
		if res := Verify(goCaps, rawShared, expSharedM1); !res.Passed() {
			t.Fatalf("raw shared publish slice with nil SpanLink expected PASS, got:\n%s", res.Format())
		}
	})
}

func TestWave15Regressions_GoEODRetriesSubSymmetryAutoClaimAndZeroCreateAudit(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	t.Run("Obj1: Go pubsub/v2 iterator.go EOD transient ack/nack retries and mixed partial-split subsets", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			op      string
			outcome string
			errMsg  string
		}{
			{name: "AckWithResult_Transient", op: "ack", outcome: "acked", errMsg: "rpc error: code = InvalidArgument desc = transient eod"},
			{name: "NackWithResult_Transient", op: "nack", outcome: "nacked", errMsg: "rpc error: code = InvalidArgument desc = transient eod nack"},
		} {
			// Faithful iterator.go retry model: single error settlement span + extra "<op> end" event on subscribe.
			sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "m-tr-"+tc.op, "a-tr-"+tc.op, "", 5, 0, true, 1, "", tc.outcome, 15000)
			pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			mod := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 15000, []*proxypb.TraceSpan{sub})
			settleErr := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, 15001, []*proxypb.TraceSpan{sub})
			settleErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			settleErr.StatusMessage = tc.errMsg
			settleErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			// retryAcks/retryModAcks appends a 2nd "<op> end" event onto subscribe without emitting a 2nd settlement span.
			sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: tc.op + " end"})

			retrySpans := []*proxypb.TraceSpan{sub, cc, sc, pr, mod, settleErr}
			for _, allowExtra := range []bool{false, true} {
				expNil := &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: allowExtra,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-tr-" + tc.op,
							AckID:           "a-tr-" + tc.op,
							ExactlyOnce:     BoolPtr(true),
							ExpectedOutcome: tc.outcome,
							Settlements:     nil,
						}},
					},
				}
				var baseFmt string
				for seed := int64(0); seed < 24; seed++ {
					perm := cloneSpans(retrySpans)
					rng := rand.New(rand.NewSource(seed))
					rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
					res := Verify(goCaps, perm, expNil)
					if !res.Passed() {
						t.Fatalf("%s (allowExtra=%v) seed=%d expected PASS, got:\n%s", tc.name, allowExtra, seed, res.Format())
					}
					if seed == 0 {
						baseFmt = res.Format()
					} else if res.Format() != baseFmt {
						t.Fatalf("%s (allowExtra=%v) seed=%d Format() differed", tc.name, allowExtra, seed)
					}
				}
			}

			// Explicit Settlements with ExpectError=true also passes all phases including verifySubscribeSettlementEvents.
			if res := Verify(goCaps, cloneSpans(retrySpans), &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-tr-" + tc.op,
						AckID:           "a-tr-" + tc.op,
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: tc.outcome,
						Settlements:     []SettlementExpectation{{Operation: tc.op, ExpectError: true}},
					}},
				},
			}); !res.Passed() {
				t.Fatalf("%s explicit ExpectError expected PASS, got:\n%s", tc.name, res.Format())
			}

			// StrictSettlementOps=true with Settlements=nil MUST still fail link_incompatible_op + dangling_reverse_link.
			resStrict := Verify(goCaps, cloneSpans(retrySpans), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					StrictSettlementOps: true,
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-tr-" + tc.op,
						AckID:           "a-tr-" + tc.op,
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: tc.outcome,
					}},
				},
			})
			assertHasViolation(t, resStrict, CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
			assertHasViolation(t, resStrict, CodeMissingLink, "subscribe.settlement.dangling_reverse_link")

			// Negative: unretried single-attempt error span (1 start, 1 end) without EOD-failed partner MUST FAIL settlement.<op>.status under both AllowExtraPubSubSpans false and true.
			subNoRetry, ccNR, scNR, prNR, _ := makeGoSubscribeMessageSpans("p", "s", "m-nr-"+tc.op, "a-nr-"+tc.op, "", 5, 0, true, 1, "", tc.outcome, 15005)
			prNR.Events = append(prNR.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			modNR := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 15005, []*proxypb.TraceSpan{subNoRetry})
			settleBad := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, 15006, []*proxypb.TraceSpan{subNoRetry})
			settleBad.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			settleBad.StatusMessage = tc.errMsg
			settleBad.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			for _, allowExtra := range []bool{false, true} {
				assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subNoRetry, ccNR, scNR, prNR, modNR, settleBad}, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: allowExtra,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-nr-" + tc.op,
							AckID:           "a-nr-" + tc.op,
							ExactlyOnce:     BoolPtr(true),
							ExpectedOutcome: tc.outcome,
						}},
					},
				}), CodeWrongStatusCode, "settlement."+tc.op+".status")
			}
		}

		// Mixed partial-split batch: m1 (transient -> acked) + m2 (permanent -> eod_failed) sharing 1 s ack with InvalidArgument ErrorInfo message.
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-split-1", "a-split-1", "", 5, 0, true, 2, "", "acked", 15010)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-split-2", "a-split-2", "", 5, 0, true, 2, "", "", 15011)
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		sub2.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		sub2.StatusMessage = "PERMANENT_FAILURE_INVALID_ACK_ID"
		sub2.Events = append(sub2.Events, &proxypb.SpanEvent{Name: "exception"})
		modSplit := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 15010, []*proxypb.TraceSpan{sub1, sub2})
		ackSplit := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 15011, []*proxypb.TraceSpan{sub1, sub2})
		ackSplit.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackSplit.StatusMessage = "rpc error: code = InvalidArgument desc = partial split"
		ackSplit.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		sub1.Events = append(sub1.Events, &proxypb.SpanEvent{Name: "ack end"})

		splitSpans := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, sub2, cc2, sc2, pr2, modSplit, ackSplit}
		// Full pair passes under both strict & non-strict.
		for _, allowExtra := range []bool{false, true} {
			if res := Verify(goCaps, cloneSpans(splitSpans), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: allowExtra,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "m-split-1", AckID: "a-split-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"},
						{MessageID: "m-split-2", AckID: "a-split-2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "eod_failed"},
					},
				},
			}); !res.Passed() {
				t.Fatalf("mixed partial-split full pair (allowExtra=%v) expected PASS, got:\n%s", allowExtra, res.Format())
			}
		}
		// Subset [m1] with AllowExtraPubSubSpans=true passes across 24 shuffles.
		expSubsetM1 := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-split-1",
					AckID:           "a-split-1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
				}},
			},
		}
		for seed := int64(0); seed < 24; seed++ {
			perm := cloneSpans(splitSpans)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			if res := Verify(goCaps, perm, expSubsetM1); !res.Passed() {
				t.Fatalf("mixed partial-split subset [m1] seed=%d expected PASS, got:\n%s", seed, res.Format())
			}
		}

		// Subset [m1] even when m1 completed on attempt 1 (no extra ack end on sub1) while extraSub sub2 failed permanently EOD -> PASS via validSampledSubByKey[k] error check.
		noRetryM1Spans := cloneSpans(splitSpans)
		noRetryM1Spans[0].Events = noRetryM1Spans[0].Events[:len(noRetryM1Spans[0].Events)-1]
		if res := Verify(goCaps, noRetryM1Spans, expSubsetM1); !res.Passed() {
			t.Fatalf("mixed partial-split subset [m1] without m1 retry event expected PASS via extraSub EOD-failed target, got:\n%s", res.Format())
		}

		// Subset [m1] when m1 completed on attempt 1 while unmatched extraSub sub2 completed an EOD retry -> PASS via validSampledSubByKey[k] retry event check.
		sub1Imm, cc1I, sc1I, pr1I, _ := makeGoSubscribeMessageSpans("p", "s", "m-imm-1", "a-imm-1", "", 5, 0, true, 2, "", "acked", 15015)
		pr1I.Events = append(pr1I.Events, &proxypb.SpanEvent{Name: "ack called"})
		sub2Ret, cc2R, sc2R, pr2R, _ := makeGoSubscribeMessageSpans("p", "s", "m-ret-2", "a-ret-2", "", 5, 0, true, 2, "", "acked", 15016)
		pr2R.Events = append(pr2R.Events, &proxypb.SpanEvent{Name: "ack called"})
		modImm := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 15015, []*proxypb.TraceSpan{sub1Imm, sub2Ret})
		ackImm := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 15016, []*proxypb.TraceSpan{sub1Imm, sub2Ret})
		ackImm.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackImm.StatusMessage = "rpc error: code = InvalidArgument desc = transient on m2"
		ackImm.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		sub2Ret.Events = append(sub2Ret.Events, &proxypb.SpanEvent{Name: "ack end"})
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub1Imm, cc1I, sc1I, pr1I, sub2Ret, cc2R, sc2R, pr2R, modImm, ackImm}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-imm-1",
					AckID:           "a-imm-1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
				}},
			},
		}); !res.Passed() {
			t.Fatalf("subset [m1] with unmatched extraSub EOD retry completion expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Obj2: Probes 1F, 1F-ALO, and 1G on extra subscribe spans", func(t *testing.T) {
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-p1f", "a-p1f", "", 5, 0, true, 1, "", "acked", 15020)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 15020, []*proxypb.TraceSpan{sub1})
		ack1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 15021, []*proxypb.TraceSpan{sub1})
		pureEODBase := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, mod1, ack1}
		expEOD := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-p1f",
					AckID:           "a-p1f",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     []SettlementExpectation{{Operation: "ack"}, {Operation: "modack"}},
				}},
			},
		}

		// Probe 1F: unsampled (TraceFlags=0) extra "s subscribe" + linked extra ack -> FAIL dangling_link.
		unsampExtraSub, ccU, scU, prU, _ := makeGoSubscribeMessageSpans("p", "s", "m-unsamp", "a-unsamp", "", 5, 0, false, 1, "", "acked", 15022)
		unsampExtraSub.TraceFlags = 0
		ackToUnsamp := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 15022, []*proxypb.TraceSpan{unsampExtraSub})
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(pureEODBase), unsampExtraSub, ccU, scU, prU, ackToUnsamp), expEOD), CodeMissingLink, "subscribe.settlement.dangling_link")

		// Probe 1F-ALO: pure-EOD + TraceFlags=0 ALO extra "s subscribe" + bogus unlinked receipt modack -> FAIL subscribe.modack.missing_link.
		bogusUnlinkedMod := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 15023, nil)
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(pureEODBase), unsampExtraSub, ccU, scU, prU, bogusUnlinkedMod), expEOD), CodeMissingLink, "subscribe.modack.missing_link")

		// Probe 1G: wrong destination ("other-sub") extra "s subscribe" + linked extra ack -> FAIL dangling_link, and + unlinked modack -> FAIL missing_link.
		wrongDstSub, ccW, scW, prW, _ := makeGoSubscribeMessageSpans("p", "s", "m-wdst", "a-wdst", "", 5, 0, false, 1, "", "acked", 15024)
		wrongDstSub.Attributes["messaging.destination.name"] = strAV("other-sub")
		ackToWrongDst := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 15024, []*proxypb.TraceSpan{wrongDstSub})
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(pureEODBase), wrongDstSub, ccW, scW, prW, ackToWrongDst), expEOD), CodeMissingLink, "subscribe.settlement.dangling_link")
		assertHasViolation(t, Verify(goCaps, append(cloneSpans(pureEODBase), wrongDstSub, ccW, scW, prW, bogusUnlinkedMod), expEOD), CodeMissingLink, "subscribe.modack.missing_link")
	})

	t.Run("Obj3 and Obj4: Probes 2G.2, 2G.7, and 2G.10", func(t *testing.T) {
		// Probe 2G.2 (Candidate 3): 1-msg s ack with ERROR + eodPartialBatchErrorMsg and no retry MUST FAIL WRONG_STATUS_CODE for ExpectedOutcome="acked".
		sub2g2, cc2g2, sc2g2, pr2g2, _ := makeGoSubscribeMessageSpans("p", "s", "m-2g2", "a-2g2", "", 5, 0, true, 1, "", "acked", 15030)
		pr2g2.Events = append(pr2g2.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod2g2 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 15030, []*proxypb.TraceSpan{sub2g2})
		ack2g2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 15031, []*proxypb.TraceSpan{sub2g2})
		ack2g2.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack2g2.StatusMessage = eodPartialBatchErrorMsg
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub2g2, cc2g2, sc2g2, pr2g2, mod2g2, ack2g2}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-2g2",
					AckID:           "a-2g2",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     nil,
				}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// Real 2-msg partial batch (batch_count=2, len(links)=2) verifying [m1] under AllowExtraPubSubSpans=true PASSES.
		sub2g2A, cc2g2A, sc2g2A, pr2g2A, _ := makeGoSubscribeMessageSpans("p", "s", "m-2g2-a", "a-2g2-a", "", 5, 0, true, 2, "", "acked", 15032)
		pr2g2A.Events = append(pr2g2A.Events, &proxypb.SpanEvent{Name: "ack called"})
		sub2g2B, cc2g2B, sc2g2B, pr2g2B, _ := makeGoSubscribeMessageSpans("p", "s", "m-2g2-b", "a-2g2-b", "", 5, 0, true, 2, "", "", 15033)
		pr2g2B.Events = append(pr2g2B.Events, &proxypb.SpanEvent{Name: "ack called"})
		sub2g2B.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		sub2g2B.StatusMessage = "invalid ack id"
		sub2g2B.Events = append(sub2g2B.Events, &proxypb.SpanEvent{Name: "exception"})
		mod2g2B := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 15032, []*proxypb.TraceSpan{sub2g2A, sub2g2B})
		ack2g2Batch := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 15033, []*proxypb.TraceSpan{sub2g2A, sub2g2B})
		ack2g2Batch.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack2g2Batch.StatusMessage = eodPartialBatchErrorMsg
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub2g2A, cc2g2A, sc2g2A, pr2g2A, sub2g2B, cc2g2B, sc2g2B, pr2g2B, mod2g2B, ack2g2Batch}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-2g2-a",
					AckID:           "a-2g2-a",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     nil,
				}},
			},
		}); !res.Passed() {
			t.Fatalf("real 2-msg partial batch subset [m1] expected PASS, got:\n%s", res.Format())
		}

		// Real 2-msg partial batch where sibling is -00 unsampled (len(links)=1, batch_count=2) + eodPartialBatchErrorMsg PASSES via isMultiMsgBatch.
		sub2g2S, cc2g2S, sc2g2S, pr2g2S, _ := makeGoSubscribeMessageSpans("p", "s", "m-2g2-s", "a-2g2-s", "", 5, 0, true, 2, "", "acked", 15034)
		pr2g2S.Events = append(pr2g2S.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod2g2S := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 15034, []*proxypb.TraceSpan{sub2g2S})
		ack2g2S := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 15035, []*proxypb.TraceSpan{sub2g2S})
		ack2g2S.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack2g2S.StatusMessage = eodPartialBatchErrorMsg
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub2g2S, cc2g2S, sc2g2S, pr2g2S, mod2g2S, ack2g2S}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-2g2-s",
					AckID:           "a-2g2-s",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     nil,
				}},
			},
		}); !res.Passed() {
			t.Fatalf("real 2-msg partial batch (batch_count=2, len(links)=1) expected PASS, got:\n%s", res.Format())
		}

		// Negative: 0 "ack start" + 1 "ack end" MUST NOT count as retry completion -> FAIL WRONG_STATUS_CODE.
		subZeroStart, ccZS, scZS, prZS, _ := makeGoSubscribeMessageSpans("p", "s", "m-zs", "a-zs", "", 5, 0, true, 1, "", "acked", 15036)
		prZS.Events = append(prZS.Events, &proxypb.SpanEvent{Name: "ack called"})
		modZS := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 15036, []*proxypb.TraceSpan{subZeroStart})
		ackZS := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 15037, []*proxypb.TraceSpan{subZeroStart})
		ackZS.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackZS.StatusMessage = "rpc error"
		ackZS.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		var filteredZSEvents []*proxypb.SpanEvent
		for _, ev := range subZeroStart.Events {
			if ev.Name != "ack start" {
				filteredZSEvents = append(filteredZSEvents, ev)
			}
		}
		subZeroStart.Events = filteredZSEvents
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subZeroStart, ccZS, scZS, prZS, modZS, ackZS}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-zs",
					AckID:           "a-zs",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     nil,
				}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// Probe 2G.7: AllowExtraPubSubSpans=false MUST reject double-acked c1 via cAck1([c1]) + cAckBatch([c1, c2]) in both orders.
		makeCppSubSimple := func(seq int, msgID, ackID string) []*proxypb.TraceSpan {
			tid := testTraceID(15040 + seq)
			sid := testSpanID(150400 + seq*10)
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
				"messaging.gcp_pubsub.result":                             strAV("ack"),
			}
			sub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.ack_end"}}
			cc := cppSpan(tid, testSpanID(150401+seq*10), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			sched := cppSpan(tid, testSpanID(150402+seq*10), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			proc := cppSpan(tid, testSpanID(150403+seq*10), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			return []*proxypb.TraceSpan{sub, cc, sched, proc}
		}
		makeCppAckLinks := func(seq int, offset time.Duration, ackID string, subs ...*proxypb.TraceSpan) *proxypb.TraceSpan {
			sp := cppSpan(testTraceID(15050+seq), testSpanID(150500+seq), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, offset)
			sp.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                             strAV("p"),
				"messaging.system":                           strAV("gcp_pubsub"),
				"messaging.destination.name":                 strAV("s"),
				"code.function.name":                         strAV("pubsub::AckHandler::ack"),
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
		for _, ack1First := range []bool{true, false} {
			c1 := makeCppSubSimple(1, "m-2g7-1", "a-2g7-1")
			c2 := makeCppSubSimple(2, "m-2g7-2", "a-2g7-2")
			off1, offBatch := 10*time.Millisecond, 20*time.Millisecond
			if !ack1First {
				off1, offBatch = 20*time.Millisecond, 10*time.Millisecond
			}
			cAck1 := makeCppAckLinks(1, off1, "a-2g7-1", c1[0])
			cAckBatch := makeCppAckLinks(2, offBatch, "a-2g7-2", c1[0], c2[0])
			spans2g7 := append(append(c1, c2...), cAck1, cAckBatch)
			assertHasViolation(t, Verify(cppCaps, spans2g7, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "m-2g7-1", AckID: "a-2g7-1", ExpectedOutcome: "acked", Settlements: nil},
						{MessageID: "m-2g7-2", AckID: "a-2g7-2", ExpectedOutcome: "acked", Settlements: nil},
					},
				},
			}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
		}

		// Probe 2G.10: ALO (ExactlyOnce=false) with ackOK + spurious ackErr MUST FAIL settlement.ack.status under AllowExtraPubSubSpans=true.
		subALO, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-2g10", "a-2g10", "", 5, 0, false, 1, "", "acked", 15060)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		modALO := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 15060, nil)
		ackOKALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 15061, []*proxypb.TraceSpan{subALO})
		ackErrALO := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 15062, []*proxypb.TraceSpan{subALO})
		ackErrALO.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackErrALO.StatusMessage = "unavailable"
		ackErrALO.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subALO, ccA, scA, prA, modALO, ackOKALO, ackErrALO}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-2g10",
					AckID:           "a-2g10",
					ExactlyOnce:     BoolPtr(false),
					ExpectedOutcome: "acked",
					Settlements:     nil,
				}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")
	})

	t.Run("Obj5: zero-wire-create pubSpans audit under AllowExtraPubSubSpans", func(t *testing.T) {
		fcErr := goSpan(testTraceID(15070), testSpanID(150701), testSpanID(150700), "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)
		fcErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		fcErr.StatusMessage = "flow control limit"
		fcErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		expFC := &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{
					ExpectError:    true,
					ErrorKind:      PublishErrorFlowControl,
					ErrorSubstring: "flow control",
				}},
			},
		}

		// 1. Baseline 0 pubSpans still passes.
		if res := Verify(goCaps, []*proxypb.TraceSpan{fcErr}, expFC); !res.Passed() {
			t.Fatalf("baseline PublishErrorFlowControl with 0 pubSpans expected PASS, got:\n%s", res.Format())
		}

		// 2. Stray t publish with dangling link (99999/99999) -> FAIL dangling_link + attr.message_count.
		danglingPub := makeGoPublishBatchSpan("p", "t", 1, 15071)
		danglingPub.Links = []*proxypb.SpanLink{{TraceId: testTraceID(99999), SpanId: testSpanID(99999)}}
		resDangling := Verify(goCaps, []*proxypb.TraceSpan{fcErr, danglingPub}, expFC)
		assertHasViolation(t, resDangling, CodeMissingLink, "publish.batch.dangling_link")
		assertHasViolation(t, resDangling, CodeWrongAttributeValue, "publish.batch.attr.message_count")

		// 3. Stray bogus 0-link t publish -> FAIL publish.batch.attr.message_count.
		zeroLinkPub := makeGoPublishBatchSpan("p", "t", 1, 15072)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{fcErr, zeroLinkPub}, expFC), CodeWrongAttributeValue, "publish.batch.attr.message_count")

		// 4. Stray t publish linking to unsampled (TraceFlags=0) extra create -> FAIL dangling_link.
		pubUnsamp := makeGoPublishBatchSpan("p", "t", 1, 15073)
		cUnsamp, fcU, bU, _ := makeGoPublishMessageSpans("p", "t", "m-uns", "", 5, 15073, pubUnsamp, 1)
		cUnsamp.TraceFlags = 0
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{fcErr, cUnsamp, fcU, bU, pubUnsamp}, expFC), CodeMissingLink, "publish.batch.dangling_link")

		// 5. Valid extra background publish (c_extra + pub_extra) passes under AllowExtraPubSubSpans=true and fails under AllowExtraPubSubSpans=false.
		pubExtra := makeGoPublishBatchSpan("p", "t", 1, 15074)
		cExtra, fcEx, bEx, _ := makeGoPublishMessageSpans("p", "t", "m-bg", "", 5, 15074, pubExtra, 1)
		validBgSpans := []*proxypb.TraceSpan{fcErr, cExtra, fcEx, bEx, pubExtra}
		if res := Verify(goCaps, validBgSpans, expFC); !res.Passed() {
			t.Fatalf("valid extra background publish under AllowExtraPubSubSpans=true expected PASS, got:\n%s", res.Format())
		}
		expFCStrict := cloneTraceExpPublish(expFC, false)
		resStrictBg := Verify(goCaps, validBgSpans, expFCStrict)
		assertHasViolation(t, resStrictBg, CodeMissingLink, "publish.batch.dangling_link")
		assertHasViolation(t, resStrictBg, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
	})
}

func cloneTraceExpPublish(src *TraceExpectation, allowExtra bool) *TraceExpectation {
	cp := *src
	cp.AllowExtraPubSubSpans = allowExtra
	return &cp
}

func TestWave16Regressions_ALOEODExplicitAutoClaimBatchSlotsAndCrossOp(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	t.Run("Obj1: L1/L4/HUNT_A explicit Settlements on ALO must reject spurious ERROR, mixed batch must not leak EOD, HUNT_F EOD explicit still passes", func(t *testing.T) {
		for _, tc := range []struct {
			op      string
			outcome string
		}{
			{op: "ack", outcome: "acked"},
			{op: "nack", outcome: "nacked"},
		} {
			for _, eosPtr := range []*bool{BoolPtr(false), nil} {
				subALO, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-l1-"+tc.op, "a-l1-"+tc.op, "", 5, 0, false, 1, "", tc.outcome, 16001)
				prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
				modALO := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 16001, nil)
				okSpan := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, 16002, []*proxypb.TraceSpan{subALO})
				errSpan := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, 16003, []*proxypb.TraceSpan{subALO})
				errSpan.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				errSpan.StatusMessage = "spurious rpc failure"
				errSpan.Events = []*proxypb.SpanEvent{{Name: "exception"}}

				assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subALO, ccA, scA, prA, modALO, okSpan, errSpan}, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-l1-" + tc.op,
							AckID:           "a-l1-" + tc.op,
							ExactlyOnce:     eosPtr,
							ExpectedOutcome: tc.outcome,
							Settlements:     []SettlementExpectation{{Operation: tc.op, ExpectError: false}},
						}},
					},
				}), CodeWrongStatusCode, "settlement."+tc.op+".status")
			}
		}

		// Mixed batch linking explicit ALO sub1 and Settlements=nil non-retried EOD sub2 (both with real non-error ackOK1/ackOK2) must NOT pass spurious ackErrMixed.
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-mix-alo", "a-mix-alo", "", 5, 0, false, 1, "", "acked", 16010)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 16010, nil)
		ackOK1 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 16011, []*proxypb.TraceSpan{sub1})

		sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-mix-eod", "a-mix-eod", "", 5, 0, true, 1, "", "acked", 16012)
		pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
		mod2 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 16012, []*proxypb.TraceSpan{sub2})
		ackOK2 := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 16014, []*proxypb.TraceSpan{sub2})
		ackErrMixed := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 16013, []*proxypb.TraceSpan{sub1, sub2})
		ackErrMixed.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackErrMixed.StatusMessage = "spurious batch failure"
		ackErrMixed.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		// Keep sub2 at 1 start / 1 end (non-retried).
		sub2.Events = []*proxypb.SpanEvent{
			{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "ack end"},
		}

		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{
			sub1, cc1, sc1, pr1, mod1, ackOK1,
			sub2, cc2, sc2, pr2, mod2, ackOK2, ackErrMixed,
		}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-mix-alo", AckID: "a-mix-alo", ExactlyOnce: BoolPtr(false), ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{MessageID: "m-mix-eod", AckID: "a-mix-eod", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked", Settlements: nil},
				},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// Go EOD Probe 2G.10 analog: real non-error ackOK/nackOK (1 start, 1 end) + spurious ERROR span MUST FAIL under Settlements=nil, AllowExtraPubSubSpans=true.
		for _, tc := range []struct {
			op      string
			outcome string
		}{
			{op: "ack", outcome: "acked"},
			{op: "nack", outcome: "nacked"},
		} {
			for _, eosPtr := range []*bool{BoolPtr(true), nil} {
				subE2G, ccE2G, scE2G, prE2G, _ := makeGoSubscribeMessageSpans("p", "s", "m-2g10eod-"+tc.op, "a-2g10eod-"+tc.op, "", 5, 0, true, 1, "", tc.outcome, 16018)
				prE2G.Events = append(prE2G.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
				modE2G := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 16018, []*proxypb.TraceSpan{subE2G})
				okE2G := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, 16018, []*proxypb.TraceSpan{subE2G})
				errE2G := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, 16019, []*proxypb.TraceSpan{subE2G})
				errE2G.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				errE2G.StatusMessage = "unavailable"
				errE2G.Events = []*proxypb.SpanEvent{{Name: "exception"}}
				// Keep real single-attempt 1 start / 1 end on subE2G.
				subE2G.Events = []*proxypb.SpanEvent{
					{Name: tc.op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
					{Name: tc.op + " end"},
				}
				assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subE2G, ccE2G, scE2G, prE2G, modE2G, okE2G, errE2G}, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-2g10eod-" + tc.op,
							AckID:           "a-2g10eod-" + tc.op,
							ExactlyOnce:     eosPtr,
							ExpectedOutcome: tc.outcome,
							Settlements:     nil,
						}},
					},
				}), CodeWrongStatusCode, "settlement."+tc.op+".status")
			}
		}

		// Contrast COUNTEREXAMPLE_HUNT_F: Wave 14 EOD explicit Settlements=[{Operation:"ack"}] + ackOK + ackErr (even with 1 start, 1 end) continues to PASS.
		subF, ccF, scF, prF, _ := makeGoSubscribeMessageSpans("p", "s", "m-huntf", "a-huntf", "", 5, 0, true, 1, "", "acked", 16015)
		prF.Events = append(prF.Events, &proxypb.SpanEvent{Name: "ack called"})
		modF := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 16015, []*proxypb.TraceSpan{subF})
		ackOKF := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 16016, []*proxypb.TraceSpan{subF})
		ackErrF := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 16017, []*proxypb.TraceSpan{subF})
		ackErrF.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackErrF.StatusMessage = "unavailable"
		ackErrF.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		subF.Events = []*proxypb.SpanEvent{
			{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "ack end"},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{subF, ccF, scF, prF, modF, ackOKF, ackErrF}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-huntf",
					AckID:           "a-huntf",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		}); !res.Passed() {
			t.Fatalf("COUNTEREXAMPLE_HUNT_F explicit EOD ackOK+ackErr expected PASS, got:\n%s", res.Format())
		}
	})

	t.Run("Obj2: L3 pure-ALO partial batch spoof and L5 full non-retried EOD batch with eodPartialBatchErrorMsg must FAIL", func(t *testing.T) {
		// L3a: Pure-ALO 1-link s ack/nack with batch_count=2 + eodPartialBatchErrorMsg -> FAIL WRONG_STATUS_CODE.
		for _, tc := range []struct {
			op      string
			outcome string
		}{
			{op: "ack", outcome: "acked"},
			{op: "nack", outcome: "nacked"},
		} {
			subL3, ccL3, scL3, prL3, _ := makeGoSubscribeMessageSpans("p", "s", "m-l3-"+tc.op, "a-l3-"+tc.op, "", 5, 0, false, 2, "", tc.outcome, 16020)
			prL3.Events = append(prL3.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			modL3 := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 16020, nil)
			badL3 := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, 16021, []*proxypb.TraceSpan{subL3})
			badL3.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			badL3.StatusMessage = eodPartialBatchErrorMsg

			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subL3, ccL3, scL3, prL3, modL3, badL3}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-l3-" + tc.op,
						AckID:           "a-l3-" + tc.op,
						ExactlyOnce:     BoolPtr(false),
						ExpectedOutcome: tc.outcome,
						Settlements:     nil,
					}},
				},
			}), CodeWrongStatusCode, "settlement."+tc.op+".status")
		}

		// L3b: Pure-ALO 2-link [sub1, extraALOSub2] with batch_count=2 + eodPartialBatchErrorMsg -> FAIL WRONG_STATUS_CODE.
		subA1, ccA1, scA1, prA1, _ := makeGoSubscribeMessageSpans("p", "s", "m-l3b-1", "a-l3b-1", "", 5, 0, false, 2, "", "acked", 16022)
		subA2, ccA2, scA2, prA2, _ := makeGoSubscribeMessageSpans("p", "s", "m-l3b-2", "a-l3b-2", "", 5, 0, false, 2, "", "acked", 16023)
		prA1.Events = append(prA1.Events, &proxypb.SpanEvent{Name: "ack called"})
		prA2.Events = append(prA2.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackL3b := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 16024, []*proxypb.TraceSpan{subA1, subA2})
		ackL3b.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackL3b.StatusMessage = eodPartialBatchErrorMsg
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subA1, ccA1, scA1, prA1, subA2, ccA2, scA2, prA2, ackL3b}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-l3b-1",
					AckID:           "a-l3b-1",
					ExactlyOnce:     BoolPtr(false),
					ExpectedOutcome: "acked",
					Settlements:     nil,
				}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// L5: Full 2-msg EOD batch [m1, m2] both matched acked/nacked with 1 start / 1 end each (both without and with real ackOK/nackOK) + ERROR eodPartialBatchErrorMsg -> FAIL WRONG_STATUS_CODE.
		for _, opTC := range []struct {
			op      string
			outcome string
		}{
			{op: "ack", outcome: "acked"},
			{op: "nack", outcome: "nacked"},
		} {
			for _, withException := range []bool{false, true} {
				for _, withOK := range []bool{false, true} {
					subE1, ccE1, scE1, prE1, _ := makeGoSubscribeMessageSpans("p", "s", "m-l5-1-"+opTC.op, "a-l5-1-"+opTC.op, "", 5, 0, true, 2, "", opTC.outcome, 16030)
					subE2, ccE2, scE2, prE2, _ := makeGoSubscribeMessageSpans("p", "s", "m-l5-2-"+opTC.op, "a-l5-2-"+opTC.op, "", 5, 0, true, 2, "", opTC.outcome, 16031)
					prE1.Events = append(prE1.Events, &proxypb.SpanEvent{Name: opTC.op + " called"})
					prE2.Events = append(prE2.Events, &proxypb.SpanEvent{Name: opTC.op + " called"})
					modL5 := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 16030, []*proxypb.TraceSpan{subE1, subE2})
					errL5 := makeGoSettlementSpan("p", "s", opTC.op, 2, 0, false, 16031, []*proxypb.TraceSpan{subE1, subE2})
					errL5.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
					errL5.StatusMessage = eodPartialBatchErrorMsg
					if withException {
						errL5.Events = []*proxypb.SpanEvent{{Name: "exception"}}
					}
					spansL5 := []*proxypb.TraceSpan{subE1, ccE1, scE1, prE1, subE2, ccE2, scE2, prE2, modL5, errL5}
					if withOK {
						okL5 := makeGoSettlementSpan("p", "s", opTC.op, 2, 0, false, 16032, []*proxypb.TraceSpan{subE1, subE2})
						// Keep subE1 and subE2 at exact 1 start / 1 end (non-retried).
						for _, ss := range []*proxypb.TraceSpan{subE1, subE2} {
							ss.Events = []*proxypb.SpanEvent{
								{Name: opTC.op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
								{Name: opTC.op + " end"},
							}
						}
						spansL5 = append(spansL5, okL5)
					}
					assertHasViolation(t, Verify(goCaps, spansL5, &TraceExpectation{
						Subscription:          "projects/p/subscriptions/s",
						AllowExtraPubSubSpans: true,
						Subscribe: &SubscribeExpectation{
							Messages: []SubscribeMessageExpectation{
								{MessageID: "m-l5-1-" + opTC.op, AckID: "a-l5-1-" + opTC.op, ExactlyOnce: BoolPtr(true), ExpectedOutcome: opTC.outcome, Settlements: nil},
								{MessageID: "m-l5-2-" + opTC.op, AckID: "a-l5-2-" + opTC.op, ExactlyOnce: BoolPtr(true), ExpectedOutcome: opTC.outcome, Settlements: nil},
							},
						},
					}), CodeWrongStatusCode, "settlement."+opTC.op+".status")
				}
			}
		}
	})

	t.Run("Obj3: COUNTEREXAMPLE_HUNT_B mixed nil+explicit double-settlement rejected under AllowExtraPubSubSpans=false and valid shared batch passes", func(t *testing.T) {
		for _, tc := range []struct {
			op      string
			outcome string
		}{
			{op: "ack", outcome: "acked"},
			{op: "nack", outcome: "nacked"},
		} {
			for _, dupFirst := range []bool{true, false} {
				for _, nilFirst := range []bool{true, false} {
					sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-hb1", "a-hb1", "", 5, 0, true, 1, "", tc.outcome, 16040)
					sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-hb2", "a-hb2", "", 5, 0, true, 1, "", tc.outcome, 16041)
					pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
					pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
					mod12 := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 16040, []*proxypb.TraceSpan{sub1, sub2})

					seqBatch, seqDup := 16042, 16043
					if dupFirst {
						seqBatch, seqDup = 16043, 16042
					}
					batchSpan := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, seqBatch, []*proxypb.TraceSpan{sub1, sub2})
					dupSpan := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, seqDup, []*proxypb.TraceSpan{sub1})

					m1 := SubscribeMessageExpectation{MessageID: "m-hb1", AckID: "a-hb1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome, Settlements: nil}
					m2 := SubscribeMessageExpectation{MessageID: "m-hb2", AckID: "a-hb2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome, Settlements: []SettlementExpectation{{Operation: tc.op}}}
					msgs := []SubscribeMessageExpectation{m1, m2}
					if !nilFirst {
						msgs = []SubscribeMessageExpectation{m2, m1}
					}

					assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{
						sub1, cc1, sc1, pr1, sub2, cc2, sc2, pr2, mod12, batchSpan, dupSpan,
					}, &TraceExpectation{
						Subscription:          "projects/p/subscriptions/s",
						AllowExtraPubSubSpans: false,
						Subscribe: &SubscribeExpectation{
							StrictSettlementOps: false,
							Messages:            msgs,
						},
					}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
				}
			}

			// Valid shared batch (no dupSpan) with mixed nil + explicit MUST PASS under AllowExtraPubSubSpans=false across both message orders and 24 shuffles.
			subV1, ccV1, scV1, prV1, _ := makeGoSubscribeMessageSpans("p", "s", "m-hbv1", "a-hbv1", "", 5, 0, true, 1, "", tc.outcome, 16045)
			subV2, ccV2, scV2, prV2, _ := makeGoSubscribeMessageSpans("p", "s", "m-hbv2", "a-hbv2", "", 5, 0, true, 1, "", tc.outcome, 16046)
			prV1.Events = append(prV1.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			prV2.Events = append(prV2.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			modV := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 16045, []*proxypb.TraceSpan{subV1, subV2})
			sharedV := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, 16046, []*proxypb.TraceSpan{subV1, subV2})
			validSpans := []*proxypb.TraceSpan{subV1, ccV1, scV1, prV1, subV2, ccV2, scV2, prV2, modV, sharedV}

			for _, nilFirst := range []bool{true, false} {
				mV1 := SubscribeMessageExpectation{MessageID: "m-hbv1", AckID: "a-hbv1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome, Settlements: nil}
				mV2 := SubscribeMessageExpectation{MessageID: "m-hbv2", AckID: "a-hbv2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome, Settlements: []SettlementExpectation{{Operation: tc.op}}}
				msgs := []SubscribeMessageExpectation{mV1, mV2}
				if !nilFirst {
					msgs = []SubscribeMessageExpectation{mV2, mV1}
				}
				expValid := &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: false,
					Subscribe:             &SubscribeExpectation{Messages: msgs},
				}
				for seed := int64(0); seed < 24; seed++ {
					perm := cloneSpans(validSpans)
					rng := rand.New(rand.NewSource(seed))
					rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
					if res := Verify(goCaps, perm, expValid); !res.Passed() {
						t.Fatalf("valid mixed nil+explicit shared %s batch (nilFirst=%v, seed=%d) expected PASS, got:\n%s", tc.op, nilFirst, seed, res.Format())
					}
				}
			}

			// Raw slice with nil *SpanLink on Phase-2 matched shared settlement span must safely skip nil link and PASS.
			rawShared := cloneSpans(validSpans)
			rawShared[9].Links = append([]*proxypb.SpanLink{nil}, rawShared[9].Links...)
			if res := Verify(goCaps, rawShared, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "m-hbv1", AckID: "a-hbv1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome, Settlements: nil},
						{MessageID: "m-hbv2", AckID: "a-hbv2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome, Settlements: []SettlementExpectation{{Operation: tc.op}}},
					},
				},
			}); !res.Passed() {
				t.Fatalf("raw mixed nil+explicit shared %s batch with nil SpanLink expected PASS, got:\n%s", tc.op, res.Format())
			}
		}

		// Phase-3 mixed sampled (nil) + dropped -00 (explicit) batch under AllowExtraPubSubSpans=false:
		// valid shared batch passes, while duplicate ack on sampled sub fails traces.no_extra_pubsub_spans.
		subP3, ccP3, scP3, prP3, _ := makeGoSubscribeMessageSpans("p", "s", "m-p3s", "a-p3s", "", 5, 0, true, 1, "", "acked", 16050)
		prP3.Events = append(prP3.Events, &proxypb.SpanEvent{Name: "ack called"})
		modP3 := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 16050, []*proxypb.TraceSpan{subP3})
		ackP3Batch := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 16051, []*proxypb.TraceSpan{subP3})
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(16055), testSpanID(160550))
		expP3Mixed := &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-p3s", AckID: "a-p3s", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked", Settlements: nil},
					{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "ack"}, {Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true)}}},
				},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{subP3, ccP3, scP3, prP3, modP3, ackP3Batch}, expP3Mixed); !res.Passed() {
			t.Fatalf("Phase-3 mixed sampled+dropped shared batch expected PASS, got:\n%s", res.Format())
		}
		ackP3Dup := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 16052, []*proxypb.TraceSpan{subP3})
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subP3, ccP3, scP3, prP3, modP3, ackP3Batch, ackP3Dup}, expP3Mixed), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
	})

	t.Run("Obj4: COUNTEREXAMPLE_HUNT_C cross-op extraSub EOD-failed on ack must not excuse bogus nackErr on ProfileGo while preserving ProfileCPP", func(t *testing.T) {
		// Go: sub1 nacked (with real nackOK1, 1 start / 1 end), extraSub failed on ack (ERROR + ack start/end, 0 nack events),
		// bogus nackErr links [sub1, extraSub] (both arbitrary msg & eodPartialBatchErrorMsg) -> FAIL settlement.nack.status.
		for _, tc := range []struct {
			op      string
			otherOp string
			outcome string
			errMsg  string
			withOK  bool
		}{
			{op: "nack", otherOp: "ack", outcome: "nacked", errMsg: "bogus nack failure", withOK: false},
			{op: "nack", otherOp: "ack", outcome: "nacked", errMsg: "bogus nack failure", withOK: true},
			{op: "nack", otherOp: "ack", outcome: "nacked", errMsg: eodPartialBatchErrorMsg, withOK: true},
			{op: "ack", otherOp: "nack", outcome: "acked", errMsg: "bogus ack failure", withOK: true},
			{op: "ack", otherOp: "nack", outcome: "acked", errMsg: eodPartialBatchErrorMsg, withOK: true},
		} {
			sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-hc-1-"+tc.op, "a-hc-1-"+tc.op, "", 5, 0, true, 2, "", tc.outcome, 16060)
			pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			mod1 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 16060, []*proxypb.TraceSpan{sub1})

			extraOtherFail, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-hc-extra-"+tc.op, "a-hc-extra-"+tc.op, "", 5, 0, true, 1, "", "", 16061)
			prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: tc.otherOp + " called"})
			extraOtherFail.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			extraOtherFail.StatusMessage = "invalid " + tc.otherOp
			modE := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 16061, []*proxypb.TraceSpan{extraOtherFail})
			otherExtra := makeGoSettlementSpan("p", "s", tc.otherOp, 1, 0, false, 16062, []*proxypb.TraceSpan{extraOtherFail})
			otherExtra.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			otherExtra.StatusMessage = "invalid " + tc.otherOp
			otherExtra.Events = []*proxypb.SpanEvent{{Name: "exception"}}

			bogusErr := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, 16063, []*proxypb.TraceSpan{sub1, extraOtherFail})
			bogusErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			bogusErr.StatusMessage = tc.errMsg
			if tc.errMsg != eodPartialBatchErrorMsg {
				bogusErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			}

			spansHC := []*proxypb.TraceSpan{
				sub1, cc1, sc1, pr1, mod1, extraOtherFail, ccE, scE, prE, modE, otherExtra, bogusErr,
			}
			if tc.withOK {
				ok1 := makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, 16064, []*proxypb.TraceSpan{sub1})
				spansHC = append(spansHC, ok1)
			}
			// Keep strictly otherOp events (0 tc.op events) on extraOtherFail and exact 1 start / 1 end on sub1.
			extraOtherFail.Events = []*proxypb.SpanEvent{
				{Name: tc.otherOp + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
				{Name: tc.otherOp + " end"},
				{Name: "exception"},
			}
			sub1.Events = []*proxypb.SpanEvent{
				{Name: tc.op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
				{Name: tc.op + " end"},
			}

			assertHasViolation(t, Verify(goCaps, spansHC, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-hc-1-" + tc.op,
						AckID:           "a-hc-1-" + tc.op,
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: tc.outcome,
						Settlements:     nil,
					}},
				},
			}), CodeWrongStatusCode, "settlement."+tc.op+".status")
		}

		// ProfileCPP preserves unmatched extraSub StatusCode=ERROR handling without Go "<op> start" events,
		// AND rejects cross-op gl-cpp.<otherOp>_start extraSub (Hunt R16-3B) while accepting matching gl-cpp.<op>_start.
		makeCppSubSimple := func(seq int, msgID, ackID, resStr string, isErr bool, events ...string) []*proxypb.TraceSpan {
			tid := testTraceID(16070 + seq)
			sid := testSpanID(160700 + seq*10)
			sub := cppSpan(tid, sid, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, time.Duration(seq)*time.Millisecond)
			if isErr {
				sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				sub.StatusMessage = "perm fail"
			} else {
				sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			}
			for _, ev := range events {
				sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: ev})
			}
			sub.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                                          strAV("p"),
				"messaging.system":                                        strAV("gcp_pubsub"),
				"messaging.destination.name":                              strAV("s"),
				"messaging.operation.type":                                strAV("subscribe"),
				"messaging.message.id":                                    strAV(msgID),
				"messaging.message.envelope.size":                         intAV(25),
				"messaging.gcp_pubsub.message.ack_id":                     strAV(ackID),
				"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(true),
				"messaging.gcp_pubsub.result":                             strAV(resStr),
			}
			cc := cppSpan(tid, testSpanID(160701+seq*10), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			cc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			sched := cppSpan(tid, testSpanID(160702+seq*10), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			sched.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			proc := cppSpan(tid, testSpanID(160703+seq*10), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			proc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			return []*proxypb.TraceSpan{sub, cc, sched, proc}
		}
		mkCppSettleErr := func(seq int, op, ackID string, links ...*proxypb.TraceSpan) *proxypb.TraceSpan {
			sp := cppSpan(testTraceID(16075+seq), testSpanID(160750+seq), "", "s "+op, proxypb.SpanKind_SPAN_KIND_INTERNAL, 10*time.Millisecond)
			sp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			sp.StatusMessage = "invalid " + op
			sp.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                             strAV("p"),
				"messaging.system":                           strAV("gcp_pubsub"),
				"messaging.destination.name":                 strAV("s"),
				"code.function.name":                         strAV("pubsub::AckHandler::" + op),
				"messaging.gcp_pubsub.message.ack_id":        strAV(ackID),
				"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
				"messaging.operation.type":                   strAV("settle"),
				"gl-cpp.status_code":                         strAV("INVALID_ARGUMENT"),
			}
			for _, ls := range links {
				sp.Links = append(sp.Links, &proxypb.SpanLink{TraceId: ls.TraceId, SpanId: ls.SpanId})
			}
			return sp
		}

		// C++ synthetic Events=nil extraSub and matching gl-cpp.ack_start extraSub -> PASS.
		for idx, extraEvents := range [][]string{nil, {"gl-cpp.ack_start", "gl-cpp.ack_end"}} {
			cppMain := makeCppSubSimple(1+idx*2, "m-hc-cpp1", "a-hc-cpp1", "ack", false, "gl-cpp.ack_start", "gl-cpp.ack_end")
			cppExtra := makeCppSubSimple(2+idx*2, "m-hc-cpp2", "a-hc-cpp2", "ack", true, extraEvents...)
			cppAckErr := mkCppSettleErr(idx, "ack", "a-hc-cpp1", cppMain[0], cppExtra[0])
			cppSpans := append(append(cppMain, cppExtra...), cppAckErr)
			if res := Verify(cppCaps, cppSpans, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-hc-cpp1",
						AckID:           "a-hc-cpp1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "acked",
						Settlements:     nil,
					}},
				},
			}); !res.Passed() {
				t.Fatalf("ProfileCPP unmatched extraSub ERROR (events=%v) expected PASS, got:\n%s", extraEvents, res.Format())
			}
		}

		// Hunt R16-3B: ProfileCPP extraSub failed on gl-cpp.ack_start (0 gl-cpp.nack_start) linked into bogus s nack ERROR MUST FAIL settlement.nack.status.
		cppNackMain := makeCppSubSimple(10, "m-r163b-1", "a-r163b-1", "nack", false, "gl-cpp.nack_start", "gl-cpp.nack_end")
		cppAckFailExtra := makeCppSubSimple(11, "m-r163b-2", "a-r163b-2", "ack", true, "gl-cpp.ack_start", "gl-cpp.ack_end")
		cppBogusNackErr := mkCppSettleErr(10, "nack", "a-r163b-1", cppNackMain[0], cppAckFailExtra[0])
		assertHasViolation(t, Verify(cppCaps, append(append(cppNackMain, cppAckFailExtra...), cppBogusNackErr), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-r163b-1",
					AckID:           "a-r163b-1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "nacked",
					Settlements:     nil,
				}},
			},
		}), CodeWrongStatusCode, "settlement.nack.status")
	})
}

func TestWave17Regressions_HasNonErrGuard_BatchSlots_And_DualOpAutoClaim(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}

	t.Run("BUG_HUNT_G and Hunt R16-1A/1B: subset [m1] of clean 2-msg Go batch must reject spurious eodPartialBatchErrorMsg", func(t *testing.T) {
		for _, tc := range []struct {
			op      string
			outcome string
		}{
			{op: "ack", outcome: "acked"},
			{op: "nack", outcome: "nacked"},
		} {
			// 1. BUG_HUNT_G / Hunt R16-1B: [matched EOD 1/1, extraEOD_clean 1/1] (both with & without real settleOK) + spurious ERROR eodPartialBatchErrorMsg -> FAIL.
			for _, withOK := range []bool{false, true} {
				sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-bhg1-"+tc.op, "a-bhg1-"+tc.op, "", 5, 0, true, 2, "", tc.outcome, 17001)
				sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-bhg2-"+tc.op, "a-bhg2-"+tc.op, "", 5, 0, true, 2, "", tc.outcome, 17002)
				pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
				pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
				mod := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 17001, []*proxypb.TraceSpan{sub1, sub2})
				errBatch := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, 17002, []*proxypb.TraceSpan{sub1, sub2})
				errBatch.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				errBatch.StatusMessage = eodPartialBatchErrorMsg
				spans := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, sub2, cc2, sc2, pr2, mod, errBatch}
				if withOK {
					okBatch := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, 17003, []*proxypb.TraceSpan{sub1, sub2})
					for _, ss := range []*proxypb.TraceSpan{sub1, sub2} {
						ss.Events = []*proxypb.SpanEvent{
							{Name: tc.op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
							{Name: tc.op + " end"},
						}
					}
					spans = append(spans, okBatch)
				}
				assertHasViolation(t, Verify(goCaps, spans, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-bhg1-" + tc.op,
							AckID:           "a-bhg1-" + tc.op,
							ExactlyOnce:     BoolPtr(true),
							ExpectedOutcome: tc.outcome,
							Settlements:     nil,
						}},
					},
				}), CodeWrongStatusCode, "settlement."+tc.op+".status")
			}

			// 2. Hunt R16-1A: [matched EOD 1/1, extraALO 1/1] + spurious ERROR eodPartialBatchErrorMsg -> FAIL.
			subE, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-r161a-"+tc.op, "a-r161a-"+tc.op, "", 5, 0, true, 2, "", tc.outcome, 17010)
			subA, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-r161a-alo-"+tc.op, "a-r161a-alo-"+tc.op, "", 5, 0, false, 2, "", tc.outcome, 17011)
			prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			modE := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 17010, []*proxypb.TraceSpan{subE})
			errMixed := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, 17011, []*proxypb.TraceSpan{subE, subA})
			errMixed.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			errMixed.StatusMessage = eodPartialBatchErrorMsg
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subE, ccE, scE, prE, subA, ccA, scA, prA, modE, errMixed}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-r161a-" + tc.op,
						AckID:           "a-r161a-" + tc.op,
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: tc.outcome,
						Settlements:     nil,
					}},
				},
			}), CodeWrongStatusCode, "settlement."+tc.op+".status")
		}

		// 3. Cross-op clean extraSub (settled on ack with 1 ack start/1 ack end, UNSET) linked into bogus nack ERROR eodPartialBatchErrorMsg -> FAIL.
		subN, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "m-xclean-1", "a-xclean-1", "", 5, 0, true, 1, "", "nacked", 17020)
		prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
		modN := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 17020, []*proxypb.TraceSpan{subN})
		nackOKN := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 17021, []*proxypb.TraceSpan{subN})

		extraAckClean, ccAC, scAC, prAC, _ := makeGoSubscribeMessageSpans("p", "s", "m-xclean-2", "a-xclean-2", "", 5, 0, true, 1, "", "acked", 17021)
		prAC.Events = append(prAC.Events, &proxypb.SpanEvent{Name: "ack called"})
		modAC := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 17021, []*proxypb.TraceSpan{extraAckClean})
		ackOKAC := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 17022, []*proxypb.TraceSpan{extraAckClean})

		bogusNack := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 17023, []*proxypb.TraceSpan{subN, extraAckClean})
		bogusNack.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		bogusNack.StatusMessage = eodPartialBatchErrorMsg
		subN.Events = []*proxypb.SpanEvent{
			{Name: "nack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "nack end"},
		}
		extraAckClean.Events = []*proxypb.SpanEvent{
			{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "ack end"},
		}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{
			subN, ccN, scN, prN, modN, nackOKN, extraAckClean, ccAC, scAC, prAC, modAC, ackOKAC, bogusNack,
		}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-xclean-1",
					AckID:           "a-xclean-1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "nacked",
					Settlements:     nil,
				}},
			},
		}), CodeWrongStatusCode, "settlement.nack.status")

		// 4. Omitted batch_count (!hasBC) with clean [sub1, extraEOD_clean] or [nil, sub1] + eodPartialBatchErrorMsg MUST FAIL settlement.ack.status.
		subNoBC1, ccNB1, scNB1, prNB1, _ := makeGoSubscribeMessageSpans("p", "s", "m-nobc1", "a-nobc1", "", 5, 0, true, 2, "", "acked", 17030)
		subNoBC2, ccNB2, scNB2, prNB2, _ := makeGoSubscribeMessageSpans("p", "s", "m-nobc2", "a-nobc2", "", 5, 0, true, 2, "", "acked", 17031)
		prNB1.Events = append(prNB1.Events, &proxypb.SpanEvent{Name: "ack called"})
		prNB2.Events = append(prNB2.Events, &proxypb.SpanEvent{Name: "ack called"})
		modNB := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 17030, []*proxypb.TraceSpan{subNoBC1, subNoBC2})
		ackNoBC := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 17031, []*proxypb.TraceSpan{subNoBC1, subNoBC2})
		delete(ackNoBC.Attributes, "messaging.batch.message_count")
		ackNoBC.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackNoBC.StatusMessage = eodPartialBatchErrorMsg
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subNoBC1, ccNB1, scNB1, prNB1, subNoBC2, ccNB2, scNB2, prNB2, modNB, ackNoBC}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-nobc1", AckID: "a-nobc1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		ackNilLinkNoBC := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 17032, []*proxypb.TraceSpan{subNoBC1})
		delete(ackNilLinkNoBC.Attributes, "messaging.batch.message_count")
		ackNilLinkNoBC.Links = []*proxypb.SpanLink{nil, ackNilLinkNoBC.Links[0]}
		ackNilLinkNoBC.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackNilLinkNoBC.StatusMessage = eodPartialBatchErrorMsg
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subNoBC1, ccNB1, scNB1, prNB1, modNB, ackNilLinkNoBC}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-nobc1", AckID: "a-nobc1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// 5. Preserved pass cases:
		//    a) Matched m1 retried (1 ack start, 2 ack end, eodPartialBatchErrorMsg without exception) + clean extraSub m2 -> PASS.
		subRet1, ccR1, scR1, prR1, _ := makeGoSubscribeMessageSpans("p", "s", "m-ret-m1", "a-ret-m1", "", 5, 0, true, 2, "", "acked", 17040)
		subClean2, ccC2, scC2, prC2, _ := makeGoSubscribeMessageSpans("p", "s", "m-clean-m2", "a-clean-m2", "", 5, 0, true, 2, "", "acked", 17041)
		prR1.Events = append(prR1.Events, &proxypb.SpanEvent{Name: "ack called"})
		prC2.Events = append(prC2.Events, &proxypb.SpanEvent{Name: "ack called"})
		modRet := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 17040, []*proxypb.TraceSpan{subRet1, subClean2})
		ackRetPartial := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 17041, []*proxypb.TraceSpan{subRet1, subClean2})
		ackRetPartial.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackRetPartial.StatusMessage = eodPartialBatchErrorMsg
		subRet1.Events = append(subRet1.Events, &proxypb.SpanEvent{Name: "ack end"})
		if res := Verify(goCaps, []*proxypb.TraceSpan{
			subRet1, ccR1, scR1, prR1, subClean2, ccC2, scC2, prC2, modRet, ackRetPartial,
		}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-ret-m1", AckID: "a-ret-m1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"}},
			},
		}); !res.Passed() {
			t.Fatalf("subset [m1] retried + clean extraSub [m2] expected PASS, got:\n%s", res.Format())
		}

		//    b) Synthetic Events=nil Go extraSub EOD (ambiguous/unverified) -> PASS.
		subSyn1, ccS1, scS1, prS1, _ := makeGoSubscribeMessageSpans("p", "s", "m-syn-1", "a-syn-1", "", 5, 0, true, 2, "", "acked", 17045)
		subSynExtra, ccS2, scS2, prS2, _ := makeGoSubscribeMessageSpans("p", "s", "m-syn-2", "a-syn-2", "", 5, 0, true, 2, "", "acked", 17046)
		prS1.Events = append(prS1.Events, &proxypb.SpanEvent{Name: "ack called"})
		prS2.Events = append(prS2.Events, &proxypb.SpanEvent{Name: "ack called"})
		modSyn := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 17045, []*proxypb.TraceSpan{subSyn1, subSynExtra})
		ackSyn := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 17046, []*proxypb.TraceSpan{subSyn1, subSynExtra})
		ackSyn.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackSyn.StatusMessage = eodPartialBatchErrorMsg
		subSynExtra.Events = nil
		if res := Verify(goCaps, []*proxypb.TraceSpan{
			subSyn1, ccS1, scS1, prS1, subSynExtra, ccS2, scS2, prS2, modSyn, ackSyn,
		}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-syn-1", AckID: "a-syn-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"}},
			},
		}); !res.Passed() {
			t.Fatalf("synthetic Events=nil Go extraSub EOD expected PASS, got:\n%s", res.Format())
		}

		//    c) Synthetic and explicit-event ProfileCPP non-error EOD extraSub in 2-link batch with eodPartialBatchErrorMsg (without s ack OK) -> PASS.
		makeCppOKSub := func(seq int, msgID, ackID string, events ...string) []*proxypb.TraceSpan {
			tid := testTraceID(17050 + seq)
			sid := testSpanID(170500 + seq*10)
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
				"messaging.gcp_pubsub.result":                             strAV("ack"),
			}
			for _, ev := range events {
				sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: ev})
			}
			cc := cppSpan(tid, testSpanID(170501+seq*10), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			cc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			cc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			sched := cppSpan(tid, testSpanID(170502+seq*10), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			sched.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sched.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			proc := cppSpan(tid, testSpanID(170503+seq*10), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
			proc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			proc.Attributes = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
			return []*proxypb.TraceSpan{sub, cc, sched, proc}
		}
		for idx, extraEvents := range [][]string{nil, {"gl-cpp.ack_start", "gl-cpp.ack_end"}} {
			c1 := makeCppOKSub(1+idx*3, "m-cpp-e1", "a-cpp-e1", "gl-cpp.ack_start", "gl-cpp.ack_end")
			c2 := makeCppOKSub(2+idx*3, "m-cpp-e2", "a-cpp-e2", extraEvents...)
			cppBatchAck := cppSpan(testTraceID(17055+idx), testSpanID(170550+idx), "", "s ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 10*time.Millisecond)
			cppBatchAck.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			cppBatchAck.StatusMessage = eodPartialBatchErrorMsg
			cppBatchAck.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                             strAV("p"),
				"messaging.system":                           strAV("gcp_pubsub"),
				"messaging.destination.name":                 strAV("s"),
				"code.function.name":                         strAV("pubsub::AckHandler::ack"),
				"messaging.gcp_pubsub.message.ack_id":        strAV("a-cpp-e1"),
				"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
				"messaging.operation.type":                   strAV("settle"),
				"gl-cpp.status_code":                         strAV("INVALID_ARGUMENT"),
			}
			cppBatchAck.Links = []*proxypb.SpanLink{
				{TraceId: c1[0].TraceId, SpanId: c1[0].SpanId},
				{TraceId: c2[0].TraceId, SpanId: c2[0].SpanId},
			}
			if res := Verify(cppCaps, append(append(c1, c2...), cppBatchAck), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-cpp-e1", AckID: "a-cpp-e1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"}},
				},
			}); !res.Passed() {
				t.Fatalf("ProfileCPP non-error extraSub (events=%v) in 2-link batch expected PASS, got:\n%s", extraEvents, res.Format())
			}
		}
	})

	t.Run("HUNT_Q: AllowExtraPubSubSpans=false + Settlements=nil must reject dual ack+nack terminal ops while preserving single terminal op + modack", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			isEOD   bool
			outcome string
			isErr   bool
		}{
			{name: "eod_failed", isEOD: true, outcome: "eod_failed", isErr: true},
			{name: "empty_outcome_alo", isEOD: false, outcome: "", isErr: false},
			{name: "empty_outcome_eod", isEOD: true, outcome: "", isErr: false},
		} {
			subQ, ccQ, scQ, prQ, _ := makeGoSubscribeMessageSpans("p", "s", "m-hq-"+tc.name, "a-hq-"+tc.name, "", 5, 0, tc.isEOD, 1, "", "", 17100)
			if tc.isErr {
				subQ.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				subQ.StatusMessage = "permission denied"
				subQ.Events = append(subQ.Events, &proxypb.SpanEvent{Name: "exception"})
			} else {
				subQ.Attributes["messaging.gcp_pubsub.result"] = strAV("acked")
			}
			prQ.Events = append(prQ.Events, &proxypb.SpanEvent{Name: "ack called"})
			var modQ *proxypb.TraceSpan
			if tc.isEOD {
				modQ = makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 17100, []*proxypb.TraceSpan{subQ})
			} else {
				modQ = makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 17100, nil)
			}
			subQSingle := cloneSpans([]*proxypb.TraceSpan{subQ})[0]
			ackQSingle := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 17101, []*proxypb.TraceSpan{subQSingle})
			ackQ := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 17101, []*proxypb.TraceSpan{subQ})
			nackQ := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 17102, []*proxypb.TraceSpan{subQ})
			if tc.isErr {
				ackQSingle.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				ackQSingle.StatusMessage = "permission denied"
				ackQSingle.Events = []*proxypb.SpanEvent{{Name: "exception"}}
				ackQ.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				ackQ.StatusMessage = "permission denied"
				ackQ.Events = []*proxypb.SpanEvent{{Name: "exception"}}
				nackQ.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				nackQ.StatusMessage = "permission denied"
				nackQ.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			}

			expQ := &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-hq-" + tc.name,
						AckID:           "a-hq-" + tc.name,
						ExactlyOnce:     BoolPtr(tc.isEOD),
						ExpectedOutcome: tc.outcome,
						Settlements:     nil,
					}},
				},
			}
			// Dual ack + nack MUST FAIL traces.no_extra_pubsub_spans.
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subQ, ccQ, scQ, prQ, modQ, ackQ, nackQ}, expQ), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

			// Single ack (+ modack) MUST PASS; single nack (+ modack, when result="nacked" or empty) MUST PASS.
			if res := Verify(goCaps, []*proxypb.TraceSpan{subQSingle, ccQ, scQ, prQ, modQ, ackQSingle}, expQ); !res.Passed() {
				t.Fatalf("HUNT_Q single ack (%s) expected PASS, got:\n%s", tc.name, res.Format())
			}
			subQN, ccQN, scQN, prQN, _ := makeGoSubscribeMessageSpans("p", "s", "m-hqn-"+tc.name, "a-hqn-"+tc.name, "", 5, 0, tc.isEOD, 1, "", "", 17105)
			if tc.isErr {
				subQN.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				subQN.StatusMessage = "permission denied"
				subQN.Events = append(subQN.Events, &proxypb.SpanEvent{Name: "exception"})
			} else {
				subQN.Attributes["messaging.gcp_pubsub.result"] = strAV("nacked")
			}
			prQN.Events = append(prQN.Events, &proxypb.SpanEvent{Name: "nack called"})
			var modQN *proxypb.TraceSpan
			if tc.isEOD {
				modQN = makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 17105, []*proxypb.TraceSpan{subQN})
			} else {
				modQN = makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 17105, nil)
			}
			nackSingle := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 17106, []*proxypb.TraceSpan{subQN})
			if tc.isErr {
				nackSingle.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				nackSingle.StatusMessage = "permission denied"
				nackSingle.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			}
			if res := Verify(goCaps, []*proxypb.TraceSpan{subQN, ccQN, scQN, prQN, modQN, nackSingle}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: false,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-hqn-" + tc.name,
						AckID:           "a-hqn-" + tc.name,
						ExactlyOnce:     BoolPtr(tc.isEOD),
						ExpectedOutcome: tc.outcome,
						Settlements:     nil,
					}},
				},
			}); !res.Passed() {
				t.Fatalf("HUNT_Q single nack (%s) expected PASS, got:\n%s", tc.name, res.Format())
			}
		}

		// Mixed nil + explicit batchNack([sub1, sub2]) (matched in Phase 2 via sub2) + extra ack([sub1]) under AllowExtraPubSubSpans=false:
		// pre-seeding autoMatchedTerminalSub before the op loop MUST block extra ack in the "ack" iteration.
		subM1, ccM1, scM1, prM1, _ := makeGoSubscribeMessageSpans("p", "s", "m-hqm-1", "a-hqm-1", "", 5, 0, true, 1, "", "", 17110)
		subM2, ccM2, scM2, prM2, _ := makeGoSubscribeMessageSpans("p", "s", "m-hqm-2", "a-hqm-2", "", 5, 0, true, 1, "", "", 17111)
		subM1.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subM1.StatusMessage = "permission denied"
		subM1.Events = append(subM1.Events, &proxypb.SpanEvent{Name: "exception"})
		subM2.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subM2.StatusMessage = "permission denied"
		subM2.Events = append(subM2.Events, &proxypb.SpanEvent{Name: "exception"})
		prM1.Events = append(prM1.Events, &proxypb.SpanEvent{Name: "nack called"})
		prM2.Events = append(prM2.Events, &proxypb.SpanEvent{Name: "nack called"})
		modM := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 17110, []*proxypb.TraceSpan{subM1, subM2})
		batchNack := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 17111, []*proxypb.TraceSpan{subM1, subM2})
		batchNack.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		batchNack.StatusMessage = "permission denied"
		batchNack.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		extraAck := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 17112, []*proxypb.TraceSpan{subM1})
		extraAck.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		extraAck.StatusMessage = "permission denied"
		extraAck.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{
			subM1, ccM1, scM1, prM1, subM2, ccM2, scM2, prM2, modM, batchNack, extraAck,
		}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-hqm-1", AckID: "a-hqm-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "eod_failed", Settlements: nil},
					{MessageID: "m-hqm-2", AckID: "a-hqm-2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "eod_failed", Settlements: []SettlementExpectation{{Operation: "nack", ExpectError: true}}},
				},
			},
		}), CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
	})
}

func TestWave18Iter18Closures(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
	baseTime := time.Unix(1700000000, 0).UTC()

	shuffleSpans := func(seed int64, in []*proxypb.TraceSpan) []*proxypb.TraceSpan {
		out := append([]*proxypb.TraceSpan(nil), in...)
		rng := rand.New(rand.NewSource(seed))
		rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
		return out
	}

	cppSubSpan := func(seq int, msgID, ackID string, eod, isErr bool, res string, events ...string) []*proxypb.TraceSpan {
		tid := testTraceID(18000 + seq)
		sid := testSpanID(180000 + seq*10)
		attrs := map[string]*proxypb.AttributeValue{
			"gcp.project_id":                                          strAV("p"),
			"messaging.system":                                        strAV("gcp_pubsub"),
			"messaging.destination.name":                              strAV("s"),
			"messaging.operation.type":                                strAV("subscribe"),
			"messaging.message.id":                                    strAV(msgID),
			"messaging.message.envelope.size":                         intAV(25),
			"messaging.gcp_pubsub.message.ack_id":                     strAV(ackID),
			"messaging.gcp_pubsub.subscription.exactly_once_delivery": boolAV(eod),
		}
		if res != "" {
			attrs["messaging.gcp_pubsub.result"] = strAV(res)
		}
		sub := &proxypb.TraceSpan{
			TraceId:                  tid,
			SpanId:                   sid,
			TraceFlags:               1,
			Name:                     "s subscribe",
			Kind:                     proxypb.SpanKind_SPAN_KIND_CONSUMER,
			InstrumentationScopeName: "gl-cpp",
			StartTime:                timestamppb.New(baseTime.Add(time.Duration(seq) * time.Millisecond)),
			EndTime:                  timestamppb.New(baseTime.Add(time.Duration(seq)*time.Millisecond + 10*time.Millisecond)),
			Attributes:               attrs,
		}
		if isErr {
			sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			sub.StatusMessage = "perm fail"
		}
		for _, ev := range events {
			sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: ev})
		}
		mkChild := func(offset int, name string) *proxypb.TraceSpan {
			return &proxypb.TraceSpan{
				TraceId:                  tid,
				SpanId:                   testSpanID(180000 + seq*10 + offset),
				ParentSpanId:             sid,
				TraceFlags:               1,
				Name:                     name,
				Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
				InstrumentationScopeName: "gl-cpp",
				StartTime:                timestamppb.New(baseTime.Add(time.Duration(seq) * time.Millisecond)),
				EndTime:                  timestamppb.New(baseTime.Add(time.Duration(seq)*time.Millisecond + time.Millisecond)),
				Attributes:               map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")},
			}
		}
		return []*proxypb.TraceSpan{sub, mkChild(1, "subscriber concurrency control"), mkChild(2, "subscriber scheduler"), mkChild(3, "s process")}
	}

	cppSettleSpan := func(seq int, op, ackID string, bc int64, status proxypb.SpanStatusCode, statusMsg string, links ...*proxypb.TraceSpan) *proxypb.TraceSpan {
		tid := testTraceID(18200 + seq)
		sid := testSpanID(182000 + seq*10)
		fn := "pubsub::AckHandler::ack"
		if op == "nack" {
			fn = "pubsub::AckHandler::nack"
		}
		attrs := map[string]*proxypb.AttributeValue{
			"gcp.project_id":                             strAV("p"),
			"messaging.system":                           strAV("gcp_pubsub"),
			"messaging.destination.name":                 strAV("s"),
			"code.function.name":                         strAV(fn),
			"messaging.gcp_pubsub.message.ack_id":        strAV(ackID),
			"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
			"messaging.operation.type":                   strAV("settle"),
		}
		if bc > 0 {
			attrs["messaging.batch.message_count"] = intAV(bc)
		}
		if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			attrs["gl-cpp.status_code"] = strAV("INVALID_ARGUMENT")
		}
		sp := &proxypb.TraceSpan{
			TraceId:                  tid,
			SpanId:                   sid,
			TraceFlags:               1,
			Name:                     "s " + op,
			Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
			InstrumentationScopeName: "gl-cpp",
			StartTime:                timestamppb.New(baseTime.Add(20*time.Millisecond + time.Duration(seq)*time.Millisecond)),
			EndTime:                  timestamppb.New(baseTime.Add(25*time.Millisecond + time.Duration(seq)*time.Millisecond)),
			StatusCode:               status,
			StatusMessage:            statusMsg,
			Attributes:               attrs,
		}
		for _, ls := range links {
			sp.Links = append(sp.Links, &proxypb.SpanLink{TraceId: ls.TraceId, SpanId: ls.SpanId})
		}
		return sp
	}

	t.Run("Probe I and HUNT_F: wasMatched Phase 4 batch failure and explicit ExpectError contracts", func(t *testing.T) {
		for _, op := range []string{"ack", "nack"} {
			outcome := "acked"
			called := "ack called"
			if op == "nack" {
				outcome = "nacked"
				called = "nack called"
			}
			// Case X: m1 has explicit ExpectError=false, matched in Phase 2 to batch settle sp, while extraSub failed EOD.
			// Batch UNSET or OK must FAIL settlement.<op>.status; batch ERROR with non-partial msg and no exception MUST FAIL exception.
			for _, tc := range []struct {
				name      string
				status    proxypb.SpanStatusCode
				statusMsg string
				hasExc    bool
				excMsg    string
				errSub    string
				expectErr bool
				wantPass  bool
				wantCode  ViolationCode
				wantRule  string
			}{
				{name: "UNSET_fails", status: proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, wantPass: false, wantCode: CodeWrongStatusCode, wantRule: "settlement." + op + ".status"},
				{name: "OK_fails", status: proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, wantPass: false, wantCode: CodeWrongStatusCode, wantRule: "settlement." + op + ".status"},
				{name: "ERROR_no_exception_nonpartial_fails", status: proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, statusMsg: "transient", wantPass: false, wantCode: CodeMissingEvent, wantRule: "settlement." + op + ".event.exception"},
				{name: "ERROR_with_exception_passes", status: proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, statusMsg: "perm fail", hasExc: true, excMsg: "perm fail", wantPass: true},
				{name: "ERROR_partial_batch_passes", status: proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, statusMsg: eodPartialBatchErrorMsg, wantPass: true},
				{name: "ERROR_substring_passes", status: proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, statusMsg: "quota exceeded", hasExc: true, excMsg: "quota exceeded", errSub: "quota", expectErr: true, wantPass: true},
				{name: "ERROR_substring_mismatch_fails", status: proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, statusMsg: "wrong err", hasExc: true, excMsg: "permission denied", errSub: "quota", expectErr: true, wantPass: false, wantCode: CodeWrongStatusMessage, wantRule: "settlement." + op + ".status.message"},
			} {
				sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-pi-1", "a-pi-1", "", 5, 0, true, 2, "", outcome, 18001)
				subE, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-pi-e", "a-pi-e", "", 5, 0, true, 2, "", "", 18002)
				pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: called})
				prE.Events = append(prE.Events, &proxypb.SpanEvent{Name: called})
				mod1 := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 18001, []*proxypb.TraceSpan{sub1, subE})
				batch := makeGoSettlementSpan("p", "s", op, 2, 0, false, 18002, []*proxypb.TraceSpan{sub1, subE})
				sub1.Events = []*proxypb.SpanEvent{
					{Name: op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
					{Name: op + " end"},
				}
				subE.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				subE.StatusMessage = "perm fail"
				subE.Events = []*proxypb.SpanEvent{
					{Name: op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
					{Name: op + " end"},
					{Name: "exception"},
				}
				batch.StatusCode = tc.status
				batch.StatusMessage = tc.statusMsg
				if tc.hasExc {
					batch.Events = []*proxypb.SpanEvent{{Name: "exception", Attributes: map[string]*proxypb.AttributeValue{"exception.message": strAV(tc.excMsg)}}}
				}

				base := []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, subE, ccE, scE, prE, mod1, batch}
				exp := &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{
							MessageID:       "m-pi-1",
							AckID:           "a-pi-1",
							ExactlyOnce:     BoolPtr(true),
							ExpectedOutcome: outcome,
							Settlements: []SettlementExpectation{{
								Operation:      op,
								ExpectError:    tc.expectErr,
								ErrorSubstring: tc.errSub,
							}},
						}},
					},
				}
				for _, spans := range [][]*proxypb.TraceSpan{base, shuffleSpans(1801, base)} {
					res := Verify(goCaps, spans, exp)
					if tc.wantPass && !res.Passed() {
						t.Fatalf("ProbeI (%s/%s) expected PASS, got:\n%s", op, tc.name, res.Format())
					}
					if !tc.wantPass {
						assertHasViolation(t, res, tc.wantCode, tc.wantRule)
					}
				}
			}

			// Case Y partial-batch via retry-success partner when m1 has explicit ExpectError=false -> PASS.
			subM, ccM, scM, prM, _ := makeGoSubscribeMessageSpans("p", "s", "m-pir-1", "a-pir-1", "", 5, 0, true, 2, "", outcome, 18011)
			subP, ccP, scP, prP, _ := makeGoSubscribeMessageSpans("p", "s", "m-pir-p", "a-pir-p", "", 5, 0, true, 2, "", outcome, 18012)
			subM.Events = []*proxypb.SpanEvent{
				{Name: op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
				{Name: op + " end"},
			}
			subP.Events = []*proxypb.SpanEvent{
				{Name: op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}},
				{Name: op + " end"},
				{Name: op + " end"},
			}
			prM.Events = append(prM.Events, &proxypb.SpanEvent{Name: called})
			prP.Events = append(prP.Events, &proxypb.SpanEvent{Name: called})
			modR := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 18011, []*proxypb.TraceSpan{subM, subP})
			batchErr := makeGoSettlementSpan("p", "s", op, 2, 0, false, 18012, []*proxypb.TraceSpan{subM, subP})
			batchErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			batchErr.StatusMessage = eodPartialBatchErrorMsg
			partnerOK := makeGoSettlementSpan("p", "s", op, 1, 0, false, 18013, []*proxypb.TraceSpan{subP})
			if res := Verify(goCaps, []*proxypb.TraceSpan{subM, ccM, scM, prM, subP, ccP, scP, prP, modR, batchErr, partnerOK}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m-pir-1",
						AckID:           "a-pir-1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: outcome,
						Settlements:     []SettlementExpectation{{Operation: op, ExpectError: false}},
					}},
				},
			}); !res.Passed() {
				t.Fatalf("ProbeI (%s retry partner partial-batch) expected PASS, got:\n%s", op, res.Format())
			}
		}

		// COUNTEREXAMPLE_HUNT_F contrast: wasMatched=true must NEVER allow self-override via anyEODWithExplicit.
		subF, ccF, scF, prF, _ := makeGoSubscribeMessageSpans("p", "s", "m-hf-1", "a-hf-1", "", 5, 0, true, 1, "", "acked", 18021)
		subF.Events = []*proxypb.SpanEvent{
			{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "ack end"},
		}
		prF.Events = append(prF.Events, &proxypb.SpanEvent{Name: "ack called"})
		modF := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 18021, []*proxypb.TraceSpan{subF})
		ackErrF := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 18022, []*proxypb.TraceSpan{subF})
		ackErrF.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackErrF.StatusMessage = "bogus error"
		ackErrF.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subF, ccF, scF, prF, modF, ackErrF}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-hf-1",
					AckID:           "a-hf-1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     []SettlementExpectation{{Operation: "ack", ExpectError: false}},
				}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// StrictSettlementOps=true skips wasMatched Phase 4 cleanly.
		subSt, ccSt, scSt, prSt, _ := makeGoSubscribeMessageSpans("p", "s", "m-hst-1", "a-hst-1", "", 5, 0, true, 2, "", "acked", 18025)
		subStE, ccStE, scStE, prStE, _ := makeGoSubscribeMessageSpans("p", "s", "m-hst-e", "a-hst-e", "", 5, 0, true, 2, "", "expired", 18026)
		subSt.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: "ack end"}}
		prSt.Events = append(prSt.Events, &proxypb.SpanEvent{Name: "ack called"})
		subStE.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		subStE.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: "ack end"}}
		modSt := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 18025, []*proxypb.TraceSpan{subSt, subStE})
		ackSt := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 18026, []*proxypb.TraceSpan{subSt, subStE})
		ackSt.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackSt.StatusMessage = eodPartialBatchErrorMsg
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subSt, ccSt, scSt, prSt, subStE, ccStE, scStE, prStE, modSt, ackSt}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m-hst-1",
					AckID:           "a-hst-1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "acked",
					Settlements:     []SettlementExpectation{{Operation: "ack", ExpectError: false}},
				}},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// wasMatched=true with incompatible link to m2 ("nacked") emits link_incompatible_op ONCE (Phase 5 only, no Phase 4 duplicate).
		subD1, ccD1, scD1, prD1, _ := makeGoSubscribeMessageSpans("p", "s", "m-dup-1", "a-dup-1", "", 5, 0, true, 2, "", "acked", 18028)
		subD2, ccD2, scD2, prD2, _ := makeGoSubscribeMessageSpans("p", "s", "m-dup-2", "a-dup-2", "", 5, 0, true, 2, "", "nacked", 18029)
		subD1.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: "ack end"}}
		subD2.Events = []*proxypb.SpanEvent{{Name: "nack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: "nack end"}}
		prD1.Events = append(prD1.Events, &proxypb.SpanEvent{Name: "ack called"})
		prD2.Events = append(prD2.Events, &proxypb.SpanEvent{Name: "nack called"})
		modD := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 18028, []*proxypb.TraceSpan{subD1, subD2})
		ackD := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 18029, []*proxypb.TraceSpan{subD1, subD2})
		nackD := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 18030, []*proxypb.TraceSpan{subD2})
		resDup := Verify(goCaps, []*proxypb.TraceSpan{subD1, ccD1, scD1, prD1, subD2, ccD2, scD2, prD2, modD, ackD, nackD}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-dup-1", AckID: "a-dup-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{MessageID: "m-dup-2", AckID: "a-dup-2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "nacked"},
				},
			},
		})
		incompCount := 0
		for _, v := range resDup.Failures {
			if v.Rule == "subscribe.settlement.link_incompatible_op" {
				incompCount++
			}
		}
		if incompCount != 1 {
			t.Fatalf("expected exactly 1 link_incompatible_op (no Phase 4 duplicate when wasMatched), got %d:\n%s", incompCount, resDup.Format())
		}
	})

	t.Run("r17_1 Obs 1: Dropped unsampled EOD partial-batch gating under strictExtra=true", func(t *testing.T) {
		unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(18040), testSpanID(180400))

		// Positive cases on Go: nil ExactlyOnce (default EOD) and Explicit *true, both omitted and explicit m1, ack and nack.
		for _, tc := range []struct {
			name        string
			op          string
			outcome     string
			m1Explicit  bool
			dropEODPtr  *bool
			dropOutcome string
		}{
			{name: "ack_drop_explicit_true_omitted_m1", op: "ack", outcome: "acked", dropEODPtr: BoolPtr(true), dropOutcome: "acked"},
			{name: "ack_drop_nil_eod_omitted_m1", op: "ack", outcome: "acked", dropEODPtr: nil, dropOutcome: "acked"},
			{name: "ack_drop_explicit_true_explicit_m1", op: "ack", outcome: "acked", m1Explicit: true, dropEODPtr: BoolPtr(true), dropOutcome: "acked"},
			{name: "nack_drop_explicit_true_omitted_m1", op: "nack", outcome: "nacked", dropEODPtr: BoolPtr(true), dropOutcome: "nacked"},
		} {
			called := tc.op + " called"
			sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-r171-1", "a-r171-1", "", 5, 0, true, 2, "", tc.outcome, 18041)
			sub1.Events = []*proxypb.SpanEvent{{Name: tc.op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: tc.op + " end"}}
			pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: called})
			mod1 := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 18041, []*proxypb.TraceSpan{sub1})
			batch := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, 18042, []*proxypb.TraceSpan{sub1})
			batch.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			batch.StatusMessage = eodPartialBatchErrorMsg

			m1 := SubscribeMessageExpectation{
				MessageID:       "m-r171-1",
				AckID:           "a-r171-1",
				ExactlyOnce:     BoolPtr(true),
				ExpectedOutcome: tc.outcome,
			}
			if tc.m1Explicit {
				m1.Settlements = []SettlementExpectation{{Operation: tc.op, ExpectError: false}}
			}
			mDrop := SubscribeMessageExpectation{
				MessageID:              "m-r171-drop",
				AckID:                  "a-r171-drop",
				WireTraceparent:        unsampTP,
				ExpectDroppedUnsampled: true,
				ExactlyOnce:            tc.dropEODPtr,
				ExpectedOutcome:        tc.dropOutcome,
			}
			for _, msgs := range [][]SubscribeMessageExpectation{{m1, mDrop}, {mDrop, m1}} {
				if res := Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, mod1, batch}, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: false,
					Subscribe:             &SubscribeExpectation{Messages: msgs},
				}); !res.Passed() {
					t.Fatalf("r17_1 (%s) expected PASS, got:\n%s", tc.name, res.Format())
				}
			}
		}

		// Negative 1: non-EOD dropped message (ExactlyOnce=false) -> unreservedEODOpDropped==0 -> FAIL.
		subN1, ccN1, scN1, prN1, _ := makeGoSubscribeMessageSpans("p", "s", "m-r171-n1", "a-r171-n1", "", 5, 0, true, 2, "", "acked", 18050)
		subN1.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: "ack end"}}
		prN1.Events = append(prN1.Events, &proxypb.SpanEvent{Name: "ack called"})
		modN1 := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, 18050, []*proxypb.TraceSpan{subN1})
		ackN1 := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 18051, []*proxypb.TraceSpan{subN1})
		ackN1.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackN1.StatusMessage = eodPartialBatchErrorMsg
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subN1, ccN1, scN1, prN1, modN1, ackN1}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-r171-n1", AckID: "a-r171-n1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"},
					{MessageID: "m-r171-nd", AckID: "a-r171-nd", WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(false), ExpectedOutcome: "acked"},
				},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// Negative 2: dropped EOD already reserved by separate settlement span in Phase 3 -> unreservedEODOpDropped==0 -> FAIL.
		ackDropOwn := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 18052, nil)
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subN1, ccN1, scN1, prN1, modN1, ackN1, ackDropOwn}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-r171-n1", AckID: "a-r171-n1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"},
					{MessageID: "m-r171-nd", AckID: "a-r171-nd", WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
				},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// Negative 3: dropped EOD disallows ack (ExpectedOutcome="nacked") -> FAIL.
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subN1, ccN1, scN1, prN1, modN1, ackN1}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-r171-n1", AckID: "a-r171-n1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"},
					{MessageID: "m-r171-nd", AckID: "a-r171-nd", WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), ExpectedOutcome: "nacked"},
				},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")

		// Negative 4: ProfileCPP dropped unsampled with ExactlyOnce=nil does NOT default to EOD -> FAIL.
		cppM := cppSubSpan(60, "m-cpp-r171", "a-cpp-r171", true, false, "ack", "gl-cpp.ack_start", "gl-cpp.ack_end")
		cppBatch := cppSettleSpan(60, "ack", "a-cpp-r171", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, cppM[0])
		assertHasViolation(t, Verify(cppCaps, append(cppM, cppBatch), &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: false,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-cpp-r171", AckID: "a-cpp-r171", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"},
					{MessageID: "m-cpp-drop", AckID: "a-cpp-drop", WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: nil, ExpectedOutcome: "acked"},
				},
			},
		}), CodeWrongStatusCode, "settlement.ack.status")
	})

	t.Run("Explorer 2: C++ and Go extraSub partial-batch & retry hardening (R17_2B/2C, R17_3A/3B, Probe E, Probe J)", func(t *testing.T) {
		// R17_3A / R17_3B: C++ clean extraSub + sibling non-error settle targeting c2 MUST FAIL in BOTH span orders and BOTH ack/nack.
		for _, op := range []string{"ack", "nack"} {
			c1 := cppSubSpan(70, "m-r173-1", "a-r173-1", true, false, op, "gl-cpp."+op+"_start", "gl-cpp."+op+"_end")
			c2 := cppSubSpan(71, "m-r173-2", "a-r173-2", true, false, op, "gl-cpp."+op+"_start", "gl-cpp."+op+"_end")
			batchErr := cppSettleSpan(70, op, "a-r173-1", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, c1[0], c2[0])
			sibOK := cppSettleSpan(71, op, "a-r173-2", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", c2[0])
			exp := &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-r173-1", AckID: "a-r173-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: op + "ed"}},
				},
			}
			for _, order := range [][]*proxypb.TraceSpan{
				append(append(append([]*proxypb.TraceSpan{}, c1...), c2...), batchErr, sibOK),
				append(append(append([]*proxypb.TraceSpan{}, c1...), c2...), sibOK, batchErr),
			} {
				assertHasViolation(t, Verify(cppCaps, order, exp), CodeWrongStatusCode, "settlement."+op+".status")
			}
		}

		// PROBE_CPP_CLEAN_CROSSOP_UNSET: C++ UNSET extraSub invalid shapes without sibling OK MUST FAIL.
		for idx, tc := range []struct {
			name   string
			op     string
			res    string
			events []string
		}{
			{name: "crossop_nack_events_on_ack", op: "ack", res: "nack", events: []string{"gl-cpp.nack_start", "gl-cpp.nack_end"}},
			{name: "message_nack_on_ack", op: "ack", res: "", events: []string{"gl-cpp.message_nack"}},
			{name: "expired_event", op: "ack", res: "expired", events: []string{"gl-cpp.expired"}},
			{name: "expired_attr_only", op: "ack", res: "expired", events: []string{"gl-cpp.ack_start", "gl-cpp.ack_end"}},
			{name: "crossop_nacked_attr_with_ack_events", op: "ack", res: "nacked", events: []string{"gl-cpp.ack_start", "gl-cpp.ack_end"}},
			{name: "modack_only_events", op: "ack", res: "", events: []string{"gl-cpp.modack_start", "gl-cpp.modack_end"}},
			{name: "ack_start_missing_end", op: "ack", res: "ack", events: []string{"gl-cpp.ack_start"}},
			{name: "crossop_ack_on_nack", op: "nack", res: "ack", events: []string{"gl-cpp.ack_start", "gl-cpp.ack_end"}},
		} {
			c1 := cppSubSpan(80+idx*2, "m-cppx-1", "a-cppx-1", true, false, tc.op, "gl-cpp."+tc.op+"_start", "gl-cpp."+tc.op+"_end")
			c2 := cppSubSpan(81+idx*2, "m-cppx-2", "a-cppx-2", true, false, tc.res, tc.events...)
			batchErr := cppSettleSpan(80+idx, tc.op, "a-cppx-1", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, c1[0], c2[0])
			assertHasViolation(t, Verify(cppCaps, append(append(c1, c2...), batchErr), &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-cppx-1", AckID: "a-cppx-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.op + "ed"}},
				},
			}), CodeWrongStatusCode, "settlement."+tc.op+".status")
		}

		// Probe J: C++ ERROR extraSub with modack-only / cross-op / expired / sibling OK MUST FAIL; with gl-cpp.ack_start or nil events MUST PASS.
		for idx, tc := range []struct {
			name     string
			res      string
			events   []string
			withOK   bool
			wantPass bool
		}{
			{name: "err_modack_only_fails", res: "", events: []string{"gl-cpp.modack_start"}, wantPass: false},
			{name: "err_expired_attr_fails", res: "expired", events: nil, wantPass: false},
			{name: "err_with_sibling_ok_fails", res: "ack", events: []string{"gl-cpp.ack_start"}, withOK: true, wantPass: false},
			{name: "err_matching_start_passes", res: "", events: []string{"gl-cpp.ack_start"}, wantPass: true},
			{name: "err_synthetic_nil_passes", res: "", events: nil, wantPass: true},
		} {
			c1 := cppSubSpan(100+idx*2, "m-pj-1", "a-pj-1", true, false, "ack", "gl-cpp.ack_start", "gl-cpp.ack_end")
			c2 := cppSubSpan(101+idx*2, "m-pj-2", "a-pj-2", true, true, tc.res, tc.events...)
			batchErr := cppSettleSpan(100+idx, "ack", "a-pj-1", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "perm fail", c1[0], c2[0])
			batchErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			spans := append(append([]*proxypb.TraceSpan{}, c1...), c2...)
			spans = append(spans, batchErr)
			if tc.withOK {
				spans = append(spans, cppSettleSpan(120+idx, "ack", "a-pj-2", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", c2[0]))
			}
			res := Verify(cppCaps, spans, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-pj-1", AckID: "a-pj-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked"}},
				},
			})
			if tc.wantPass && !res.Passed() {
				t.Fatalf("ProbeJ (%s) expected PASS, got:\n%s", tc.name, res.Format())
			}
			if !tc.wantPass {
				assertHasViolation(t, res, CodeWrongStatusCode, "settlement.ack.status")
			}
		}

		// R17_2B/2C & Probe E: Go extraSub negative & positive controls.
		for idx, tc := range []struct {
			name     string
			op       string
			isErr    bool
			res      string
			events   []string
			withOK   bool
			wantPass bool
		}{
			{name: "crossop_nack_retry_on_ack_fails", op: "ack", events: []string{"nack start", "nack end", "nack end"}, wantPass: false},
			{name: "crossop_ack_retry_on_nack_fails", op: "nack", events: []string{"ack start", "ack end", "ack end"}, wantPass: false},
			{name: "single_attempt_no_retry_fails", op: "ack", events: []string{"ack start", "ack end"}, wantPass: false},
			{name: "modack_only_events_fails", op: "ack", events: []string{"modack start", "modack end"}, wantPass: false},
			{name: "retry_plus_mixed_nack_end_fails", op: "ack", events: []string{"ack start", "ack end", "ack end", "nack end"}, wantPass: false},
			{name: "retry_plus_expired_attr_fails", op: "ack", res: "expired", events: []string{"ack start", "ack end", "ack end"}, wantPass: false},
			{name: "synthetic_nil_with_sibling_ok_fails", op: "ack", events: nil, withOK: true, wantPass: false},
			{name: "error_extrasub_with_nacked_attr_fails", op: "ack", isErr: true, res: "nacked", events: []string{"ack start"}, wantPass: false},
			{name: "real_retry_with_sibling_ok_passes", op: "ack", events: []string{"ack start", "ack end", "ack end"}, withOK: true, wantPass: true},
			{name: "synthetic_nil_without_sibling_ok_passes", op: "ack", events: nil, withOK: false, wantPass: true},
		} {
			seqBase := 18300 + idx*2
			outcome := tc.op + "ed"
			g1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-ge-1", "a-ge-1", "", 5, 0, true, 2, "", outcome, seqBase)
			g2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans("p", "s", "m-ge-2", "a-ge-2", "", 5, 0, true, 2, "", tc.res, seqBase+1)
			if tc.isErr {
				g2.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				g2.StatusMessage = "perm fail"
			}
			pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: tc.op + " called"})
			mod := makeGoSettlementSpan("p", "s", "modack", 2, 60, true, seqBase, []*proxypb.TraceSpan{g1, g2})
			batchErr := makeGoSettlementSpan("p", "s", tc.op, 2, 0, false, seqBase+1, []*proxypb.TraceSpan{g1, g2})
			batchErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			batchErr.StatusMessage = eodPartialBatchErrorMsg
			spans := []*proxypb.TraceSpan{g1, cc1, sc1, pr1, g2, cc2, sc2, pr2, mod, batchErr}
			if tc.withOK {
				spans = append(spans, makeGoSettlementSpan("p", "s", tc.op, 1, 0, false, seqBase+2, []*proxypb.TraceSpan{g2}))
			}
			g1.Events = []*proxypb.SpanEvent{{Name: tc.op + " start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: tc.op + " end"}}
			g2.Events = nil
			for _, ev := range tc.events {
				g2.Events = append(g2.Events, &proxypb.SpanEvent{Name: ev})
			}
			res := Verify(goCaps, spans, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-ge-1", AckID: "a-ge-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome}},
				},
			})
			if tc.wantPass && !res.Passed() {
				t.Fatalf("Go extraSub (%s) expected PASS, got:\n%s", tc.name, res.Format())
			}
			if !tc.wantPass {
				assertHasViolation(t, res, CodeWrongStatusCode, "settlement."+tc.op+".status")
			}
		}
	})

	t.Run("Explorer 3: HUNT_D omitted-settlement events, sampledSubSpanAllowsSettlementOp, and conflicting_evidence", func(t *testing.T) {
		// R17_5C/5D: Omitted Settlements + acked/nacked requires settlement events on Go and C++, ALO and EOD.
		for _, eod := range []bool{false, true} {
			for _, op := range []string{"ack", "nack"} {
				outcome := op + "ed"
				subG, ccG, scG, prG, _ := makeGoSubscribeMessageSpans("p", "s", "m-hd-g", "a-hd-g", "", 5, 0, eod, 1, "", outcome, 18400)
				prG.Events = append(prG.Events, &proxypb.SpanEvent{Name: op + " called"})
				settleG := makeGoSettlementSpan("p", "s", op, 1, 0, false, 18401, []*proxypb.TraceSpan{subG})
				subG.Events = nil
				assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{subG, ccG, scG, prG, settleG}, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{MessageID: "m-hd-g", AckID: "a-hd-g", ExactlyOnce: BoolPtr(eod), ExpectedOutcome: outcome}},
					},
				}), CodeMissingEvent, "subscribe.event."+op+"_start")

				subC := cppSubSpan(200, "m-hd-c", "a-hd-c", eod, false, op)
				subC[3].Events = nil
				settleC := cppSettleSpan(200, op, "a-hd-c", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", subC[0])
				assertHasViolation(t, Verify(cppCaps, append(subC, settleC), &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{MessageID: "m-hd-c", AckID: "a-hd-c", ExactlyOnce: BoolPtr(eod), ExpectedOutcome: outcome}},
					},
				}), CodeMissingEvent, "subscribe.event.cpp_"+op+"_start")
			}
		}

		// R17_5A/5B & R17_6B/2F: Settlements=nil, ExpectedOutcome="" / "eod_failed" blocks incompatible/expired ops while allowing modack & modack-only.
		for idx, tc := range []struct {
			name     string
			isCPP    bool
			outcome  string
			evNames  []string
			resAttr  string
			settleOp string
			wantCode ViolationCode
			wantRule string
		}{
			{name: "go_ack_ev_vs_nack_unset", outcome: "", evNames: []string{"ack start", "ack end"}, settleOp: "nack", wantCode: CodeBrokenBidirectionalLink, wantRule: "subscribe.settlement.link_incompatible_op"},
			{name: "go_nack_ev_vs_ack_eod_failed", outcome: "eod_failed", evNames: []string{"nack start", "nack end"}, settleOp: "ack", wantCode: CodeBrokenBidirectionalLink, wantRule: "subscribe.settlement.link_incompatible_op"},
			{name: "go_expired_attr_vs_ack_unset", outcome: "", resAttr: "expired", settleOp: "ack", wantCode: CodeBrokenBidirectionalLink, wantRule: "subscribe.settlement.link_incompatible_op"},
			{name: "cpp_expired_ev_vs_nack_unset", isCPP: true, outcome: "", evNames: []string{"gl-cpp.expired"}, resAttr: "expired", settleOp: "nack", wantCode: CodeBrokenBidirectionalLink, wantRule: "subscribe.settlement.link_incompatible_op"},
			{name: "cpp_msg_ack_vs_nack_eod_failed", isCPP: true, outcome: "eod_failed", evNames: []string{"gl-cpp.message_ack"}, settleOp: "nack", wantCode: CodeBrokenBidirectionalLink, wantRule: "subscribe.settlement.link_incompatible_op"},
			{name: "go_conflicting_ack_and_nack", outcome: "", evNames: []string{"ack start", "nack start"}, settleOp: "ack", wantCode: CodeCustomCheckFailed, wantRule: "subscribe.settlement.conflicting_evidence"},
			{name: "cpp_conflicting_expired_and_ack", isCPP: true, outcome: "", evNames: []string{"gl-cpp.expired"}, resAttr: "ack", settleOp: "ack", wantCode: CodeCustomCheckFailed, wantRule: "subscribe.settlement.conflicting_evidence"},
		} {
			if tc.isCPP {
				c := cppSubSpan(300+idx, "m-e3-"+tc.name, "a-e3-"+tc.name, true, false, tc.resAttr, tc.evNames...)
				settle := cppSettleSpan(300+idx, tc.settleOp, "a-e3-"+tc.name, 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", c[0])
				res := Verify(cppCaps, append(c, settle), &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{MessageID: "m-e3-" + tc.name, AckID: "a-e3-" + tc.name, ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome}},
					},
				})
				assertHasViolation(t, res, tc.wantCode, tc.wantRule)
				continue
			}
			seq := 18500 + idx
			g, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "m-e3-"+tc.name, "a-e3-"+tc.name, "", 5, 0, true, 1, "", tc.resAttr, seq)
			pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
			mod := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, seq, []*proxypb.TraceSpan{g})
			settle := makeGoSettlementSpan("p", "s", tc.settleOp, 1, 0, false, seq+100, []*proxypb.TraceSpan{g})
			g.Events = nil
			for _, ev := range tc.evNames {
				g.Events = append(g.Events, &proxypb.SpanEvent{Name: ev})
			}
			if tc.outcome == "eod_failed" {
				settle.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				settle.StatusMessage = "perm fail"
				settle.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			}
			res := Verify(goCaps, []*proxypb.TraceSpan{g, cc, sc, pr, mod, settle}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-e3-" + tc.name, AckID: "a-e3-" + tc.name, ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome}},
				},
			})
			assertHasViolation(t, res, tc.wantCode, tc.wantRule)
		}

		// Phase 3 dropped+link candidate rejection & Phase 5 wasMatched link_incompatible_op via batch linking to sub2(Settlements=nil, ack evidence) on s nack.
		unsampTP3 := fmt.Sprintf("00-%s-%s-00", testTraceID(18600), testSpanID(186000))
		gP3, ccP3, scP3, prP3, _ := makeGoSubscribeMessageSpans("p", "s", "m-p3-sampled", "a-p3-sampled", "", 5, 0, true, 2, "", "acked", 18601)
		prP3.Events = append(prP3.Events, &proxypb.SpanEvent{Name: "ack called"})
		modP3 := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 18601, []*proxypb.TraceSpan{gP3})
		nackP3 := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 18602, []*proxypb.TraceSpan{gP3})
		gP3.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(2)}}, {Name: "ack end"}}
		assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{gP3, ccP3, scP3, prP3, modP3, nackP3}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-p3-sampled", AckID: "a-p3-sampled", ExactlyOnce: BoolPtr(true), ExpectedOutcome: ""},
					{MessageID: "m-p3-dropped", AckID: "a-p3-dropped", WireTraceparent: unsampTP3, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "nack"}}},
				},
			},
		}), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")

		// Phase 5 wasMatched link_incompatible_op: m1 has explicit Settlements:[ack], batch s ack also links m2(Settlements:nil, gl-cpp.nack_start).
		cM5 := cppSubSpan(320, "m-p5-1", "a-p5-1", true, false, "ack", "gl-cpp.ack_start", "gl-cpp.ack_end")
		cN5 := cppSubSpan(321, "m-p5-2", "a-p5-2", true, false, "", "gl-cpp.nack_start", "gl-cpp.nack_end")
		cN5[3].Events = nil
		ackP5 := cppSettleSpan(320, "ack", "a-p5-1", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", cM5[0], cN5[0])
		nackP5 := cppSettleSpan(321, "nack", "a-p5-2", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", cN5[0])
		assertHasViolation(t, Verify(cppCaps, append(append(cM5, cN5...), ackP5, nackP5), &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{MessageID: "m-p5-1", AckID: "a-p5-1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{MessageID: "m-p5-2", AckID: "a-p5-2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: ""},
				},
			},
		}), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")

		// Modack STILL allowed on acked, nacked, and expired subscribe spans with Settlements=nil, ExpectedOutcome="".
		cExp := cppSubSpan(330, "m-mod-exp", "a-mod-exp", true, false, "expired", "gl-cpp.expired")
		modExp := cppSettleSpan(330, "modack", "a-mod-exp", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", cExp[0])
		modExp.Kind = proxypb.SpanKind_SPAN_KIND_CLIENT
		modExp.Attributes["code.function.name"] = strAV("pubsub::Subscription::ModifyAckDeadline")
		modExp.Attributes["messaging.operation.type"] = strAV("extend")
		modExp.Attributes["messaging.gcp_pubsub.message.ack_deadline_seconds"] = intAV(10)
		if res := Verify(cppCaps, append(cExp, modExp), &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-mod-exp", AckID: "a-mod-exp", ExactlyOnce: BoolPtr(true), ExpectedOutcome: ""}},
			},
		}); !res.Passed() {
			t.Fatalf("modack on expired cpp span expected PASS, got:\n%s", res.Format())
		}

		// Subscribe_2000_Cases fuzz invariance + nil SpanEvent: Settlements=nil, ExpectedOutcome="", modack-only subSpan + s ack with messaging.batch.message_count=1 and subSpan without terminal events/result -> PASS.
		gFuzz, ccFz, scFz, prFz, _ := makeGoSubscribeMessageSpans("p", "s", "m-fz-1", "a-fz-1", "", 5, 0, false, 1, "", "", 18610)
		delete(gFuzz.Attributes, "messaging.gcp_pubsub.result")
		prFz.Events = append(prFz.Events, &proxypb.SpanEvent{Name: "ack called"})
		modFz := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, 18610, []*proxypb.TraceSpan{gFuzz})
		ackFz := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 18611, []*proxypb.TraceSpan{gFuzz})
		gFuzz.Events = []*proxypb.SpanEvent{nil, {Name: "modack start"}, {Name: "modack end"}}
		if res := Verify(goCaps, []*proxypb.TraceSpan{gFuzz, ccFz, scFz, prFz, modFz, ackFz}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-fz-1", AckID: "a-fz-1", ExpectedOutcome: ""}},
			},
		}); !res.Passed() {
			t.Fatalf("Subscribe_2000_Cases modack-only subSpan expected PASS, got:\n%s", res.Format())
		}

		// Direct helper coverage for analyzeSubSpanTerminalEvidence & sampledSubSpanAllowsSettlementOp (nil subSpan, custom ResultValues, StrictSettlementOps).
		goProf := ProfileGoCanonical()
		if ha, hn, he := analyzeSubSpanTerminalEvidence(nil, goProf); ha || hn || he {
			t.Fatalf("nil subSpan should return false,false,false; got %v,%v,%v", ha, hn, he)
		}
		customProf := goProf
		customProf.ResultValues = map[string]string{"acked": "accepted", "nacked": "rejected", "expired": "timed_out"}
		customSub := &proxypb.TraceSpan{
			Events:     []*proxypb.SpanEvent{nil},
			Attributes: map[string]*proxypb.AttributeValue{"messaging.gcp_pubsub.result": strAV("timed_out")},
		}
		if ha, hn, he := analyzeSubSpanTerminalEvidence(customSub, customProf); ha || hn || !he {
			t.Fatalf("custom expired result mismatch: got %v,%v,%v", ha, hn, he)
		}
		if sampledSubSpanAllowsSettlementOp(nil, SubscribeMessageExpectation{}, "ack", true, goProf) {
			t.Fatal("StrictSettlementOps=true with empty Settlements should disallow ack")
		}
		if !sampledSubSpanAllowsSettlementOp(nil, SubscribeMessageExpectation{}, "modack", false, goProf) {
			t.Fatal("modack with StrictSettlementOps=false should be allowed")
		}
		if !sampledSubSpanAllowsSettlementOp(nil, SubscribeMessageExpectation{}, "ack", false, goProf) {
			t.Fatal("nil subSpan with unset outcome should allow ack")
		}
		if sampledSubSpanAllowsSettlementOp(customSub, SubscribeMessageExpectation{}, "ack", false, customProf) {
			t.Fatal("expired custom subSpan should disallow ack")
		}
	})
}

func TestWave19Regressions_Wave18Closures(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage:              "go",
		SupportsMonitoring:          true,
		SupportsExactlyOnceDelivery: true,
	}
	cppCaps := &proxypb.GetCapabilitiesResponse{
		ClientLanguage:              "cpp",
		SupportsMonitoring:          true,
		SupportsExactlyOnceDelivery: true,
	}

	makeCppSubMessageSpans := func(subProj, subID, msgID, ackID string, isEOD bool, result string, seq int) (sub, cc, sc, ps *proxypb.TraceSpan) {
		tid := testTraceID(seq)
		base := time.Duration(seq) * time.Second
		sub = cppSpan(tid, testSpanID(seq*10+1), "", subID+" subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, base)
		sub.Attributes = map[string]*proxypb.AttributeValue{
			"messaging.system":                           strAV("gcp_pubsub"),
			"messaging.destination.name":                 strAV(subID),
			"gcp.project_id":                             strAV(subProj),
			"messaging.message.id":                       strAV(msgID),
			"messaging.gcp_pubsub.message.ack_id":        strAV(ackID),
			"messaging.message.body.size":                intAV(5),
			"messaging.gcp_pubsub.message.ordering_key":  strAV(""),
			"messaging.gcp_pubsub.exactly_once_delivery": boolAV(isEOD),
		}
		if result != "" {
			sub.Attributes["messaging.gcp_pubsub.result"] = strAV(result)
		}

		cc = cppSpan(tid, testSpanID(seq*10+2), sub.SpanId, "subscribe concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, base+time.Millisecond)
		sc = cppSpan(tid, testSpanID(seq*10+3), sub.SpanId, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, base+2*time.Millisecond)
		ps = cppSpan(tid, testSpanID(seq*10+4), sub.SpanId, subID+" process", proxypb.SpanKind_SPAN_KIND_INTERNAL, base+3*time.Millisecond)
		ps.Attributes = map[string]*proxypb.AttributeValue{
			"messaging.system": strAV("gcp_pubsub"),
		}
		return sub, cc, sc, ps
	}

	makeCppSettlementSpan := func(subProj, subID, op string, batchCount int64, seq int, subs []*proxypb.TraceSpan) *proxypb.TraceSpan {
		tid := testTraceID(seq + 500)
		base := time.Duration(seq) * time.Second
		sp := cppSpan(tid, testSpanID(seq*10+9), "", subID+" "+op, proxypb.SpanKind_SPAN_KIND_CLIENT, base+15*time.Millisecond)
		sp.Attributes = map[string]*proxypb.AttributeValue{
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV(subID),
			"gcp.project_id":                strAV(subProj),
			"messaging.batch.message_count": intAV(batchCount),
		}
		for _, sub := range subs {
			sp.Links = append(sp.Links, &proxypb.SpanLink{TraceId: sub.TraceId, SpanId: sub.SpanId})
			sub.Links = append(sub.Links, &proxypb.SpanLink{TraceId: sp.TraceId, SpanId: sp.SpanId})
		}
		return sp
	}

	t.Run("H1_UnsampledUsedCount_And_PositiveContrast", func(t *testing.T) {
		for _, op := range []string{"ack", "nack"} {
			otherOp := "nack"
			outcome := "acked"
			otherOutcome := "nacked"
			if op == "nack" {
				otherOp = "ack"
				outcome = "nacked"
				otherOutcome = "acked"
			}
			for _, g1First := range []bool{true, false} {
				// Negative 1: bc=2 with g1 (explicit ExpectError:false) + unsamp0 (explicit ExpectError:false),
				// unreserved unsamp1 present, sp1 ERROR eodPartialBatchErrorMsg -> MUST FAIL ONLY settlement.<op>.status
				// because all 2 batch slots of sp1 are accounted for (1 sampled + 1 unsampledUsedCount).
				g1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-h1-g1", "a-h1-g1", "", 5, 0, true, 1, "", outcome, 19001)
				pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: op + " called"})
				sp1 := makeGoSettlementSpan("p", "s", op, 2, 0, false, 19001, []*proxypb.TraceSpan{g1})
				sp1.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				sp1.StatusMessage = eodPartialBatchErrorMsg
				sp1.Events = append(sp1.Events, &proxypb.SpanEvent{Name: "exception"})

				mG1 := SubscribeMessageExpectation{
					MessageID:       "m-h1-g1",
					AckID:           "a-h1-g1",
					ExpectedOutcome: outcome,
					Settlements:     []SettlementExpectation{{Operation: op, ExpectError: false}},
				}
				mUnsamp0 := SubscribeMessageExpectation{
					MessageID:              "m-h1-u0",
					ExpectDroppedUnsampled: true,
					ExactlyOnce:            BoolPtr(true),
					ExpectedOutcome:        outcome,
					Settlements:            []SettlementExpectation{{Operation: op, ExpectError: false}},
				}
				mUnsamp1 := SubscribeMessageExpectation{
					MessageID:              "m-h1-u1",
					ExpectDroppedUnsampled: true,
					ExactlyOnce:            BoolPtr(true),
					ExpectedOutcome:        outcome,
				}
				msgsNeg1 := []SubscribeMessageExpectation{mG1, mUnsamp0, mUnsamp1}
				if !g1First {
					msgsNeg1 = []SubscribeMessageExpectation{mUnsamp1, mUnsamp0, mG1}
				}
				resNeg1 := Verify(goCaps, []*proxypb.TraceSpan{g1, cc1, sc1, pr1, sp1}, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe:    &SubscribeExpectation{Messages: msgsNeg1},
				})
				assertHasViolation(t, resNeg1, CodeWrongStatusCode, "settlement."+op+".status")
				assertHasViolation(t, resNeg1, CodeMissingSpan, "subscribe.settlement.missing_span")
				if len(resNeg1.Failures) != 2 {
					t.Fatalf("H1 Negative 1 expected exactly 2 failures (settlement.%s.status + subscribe.settlement.missing_span), got %d:\n%s", op, len(resNeg1.Failures), resNeg1.Format())
				}

				// Negative 2: unreserved unsamp1 settled cleanly elsewhere (sp3b on otherOp, shared batch with g2b) while
				// sp1b (bc=2 ERROR eodPartialBatchErrorMsg) has g1b + unsamp0b (both ExpectError:false) -> MUST FAIL ONLY settlement.<op>.status.
				g1b, cc1b, sc1b, pr1b, _ := makeGoSubscribeMessageSpans("p", "s", "m-h1b-g1", "a-h1b-g1", "", 5, 0, true, 1, "", outcome, 19005)
				pr1b.Events = append(pr1b.Events, &proxypb.SpanEvent{Name: op + " called"})
				g2b, cc2b, sc2b, pr2b, _ := makeGoSubscribeMessageSpans("p", "s", "m-h1b-g2", "a-h1b-g2", "", 5, 0, true, 1, "", otherOutcome, 19006)
				pr2b.Events = append(pr2b.Events, &proxypb.SpanEvent{Name: otherOp + " called"})
				sp1b := makeGoSettlementSpan("p", "s", op, 2, 0, false, 19005, []*proxypb.TraceSpan{g1b})
				sp1b.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				sp1b.StatusMessage = eodPartialBatchErrorMsg
				sp1b.Events = append(sp1b.Events, &proxypb.SpanEvent{Name: "exception"})
				sp3b := makeGoSettlementSpan("p", "s", otherOp, 2, 0, false, 19006, []*proxypb.TraceSpan{g2b})

				mG1b := SubscribeMessageExpectation{
					MessageID:       "m-h1b-g1",
					AckID:           "a-h1b-g1",
					ExpectedOutcome: outcome,
					Settlements:     []SettlementExpectation{{Operation: op, ExpectError: false}},
				}
				mG2b := SubscribeMessageExpectation{
					MessageID:       "m-h1b-g2",
					AckID:           "a-h1b-g2",
					ExpectedOutcome: otherOutcome,
					Settlements:     []SettlementExpectation{{Operation: otherOp, ExpectError: false}},
				}
				mUnsamp0b := SubscribeMessageExpectation{
					MessageID:              "m-h1b-u0",
					ExpectDroppedUnsampled: true,
					ExactlyOnce:            BoolPtr(true),
					ExpectedOutcome:        outcome,
					Settlements:            []SettlementExpectation{{Operation: op, ExpectError: false}},
				}
				mUnsamp1b := SubscribeMessageExpectation{
					MessageID:              "m-h1b-u1",
					ExpectDroppedUnsampled: true,
					ExactlyOnce:            BoolPtr(true),
					ExpectedOutcome:        "",
				}
				msgsNeg2 := []SubscribeMessageExpectation{mG1b, mUnsamp0b, mUnsamp1b, mG2b}
				if !g1First {
					msgsNeg2 = []SubscribeMessageExpectation{mG2b, mUnsamp1b, mUnsamp0b, mG1b}
				}
				resNeg2 := Verify(goCaps, []*proxypb.TraceSpan{g1b, cc1b, sc1b, pr1b, g2b, cc2b, sc2b, pr2b, sp1b, sp3b}, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe:    &SubscribeExpectation{Messages: msgsNeg2},
				})
				assertHasViolation(t, resNeg2, CodeWrongStatusCode, "settlement."+op+".status")
				if len(resNeg2.Failures) != 1 {
					t.Fatalf("H1 Negative 2 expected exactly 1 failure (settlement.%s.status), got %d:\n%s", op, len(resNeg2.Failures), resNeg2.Format())
				}

				// Positive contrast: bc=3 with g1 (sampled), unsamp0 (explicit), AND unsamp1 actually in sp1 -> PASS.
				g1p, cc1p, sc1p, pr1p, _ := makeGoSubscribeMessageSpans("p", "s", "m-h1-g1", "a-h1-g1", "", 5, 0, true, 1, "", outcome, 19010)
				pr1p.Events = append(pr1p.Events, &proxypb.SpanEvent{Name: op + " called"})
				sp1p := makeGoSettlementSpan("p", "s", op, 3, 0, false, 19010, []*proxypb.TraceSpan{g1p})
				sp1p.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				sp1p.StatusMessage = eodPartialBatchErrorMsg
				sp1p.Events = append(sp1p.Events, &proxypb.SpanEvent{Name: "exception"})
				resPos := Verify(goCaps, []*proxypb.TraceSpan{g1p, cc1p, sc1p, pr1p, sp1p}, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe:    &SubscribeExpectation{Messages: msgsNeg1},
				})
				if !resPos.Passed() {
					t.Fatalf("H1 positive contrast (%s, g1First=%v) expected PASS, got:\n%s", op, g1First, resPos.Format())
				}
			}
		}
	})

	t.Run("H2a_CrossOpCleanSettlement_GoAndCPP", func(t *testing.T) {
		for _, op := range []string{"ack", "nack"} {
			otherOp := "nack"
			if op == "nack" {
				otherOp = "ack"
			}
			outcome := "acked"
			if op == "nack" {
				outcome = "nacked"
			}

			// Go extraSub (UNSET, Events: nil, result: "") settled cleanly by otherOp UNSET + bogus 2-link sp ERROR.
			g1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-h2a-g1", "a-h2a-g1", "", 5, 0, true, 1, "", outcome, 19020)
			pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: op + " called"})
			extraSub, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-h2a-ex", "a-h2a-ex", "", 5, 0, true, 1, "", "", 19021)
			delete(extraSub.Attributes, "messaging.gcp_pubsub.result")
			cleanOther := makeGoSettlementSpan("p", "s", otherOp, 1, 0, false, 19021, []*proxypb.TraceSpan{extraSub})
			extraSub.Events = nil
			bogusOp := makeGoSettlementSpan("p", "s", op, 2, 0, false, 19020, []*proxypb.TraceSpan{g1, extraSub})
			bogusOp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			bogusOp.StatusMessage = eodPartialBatchErrorMsg
			extraSub.Events = nil

			resGo := Verify(goCaps, []*proxypb.TraceSpan{g1, cc1, sc1, pr1, extraSub, ccE, scE, prE, cleanOther, bogusOp}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-h2a-g1", AckID: "a-h2a-g1", ExpectedOutcome: outcome}},
				},
			})
			assertHasViolation(t, resGo, CodeWrongStatusCode, "settlement."+op+".status")

			// Go extraSub with retry-looking partner events on op, PLUS clean otherOp UNSET -> must still reject!
			g1r, cc1r, sc1r, pr1r, _ := makeGoSubscribeMessageSpans("p", "s", "m-h2ar-g1", "a-h2ar-g1", "", 5, 0, true, 1, "", outcome, 19025)
			pr1r.Events = append(pr1r.Events, &proxypb.SpanEvent{Name: op + " called"})
			extraR, ccEr, scEr, prEr, _ := makeGoSubscribeMessageSpans("p", "s", "m-h2ar-ex", "a-h2ar-ex", "", 5, 0, true, 1, "", "", 19026)
			delete(extraR.Attributes, "messaging.gcp_pubsub.result")
			cleanOtherR := makeGoSettlementSpan("p", "s", otherOp, 1, 0, false, 19026, []*proxypb.TraceSpan{extraR})
			bogusOpR := makeGoSettlementSpan("p", "s", op, 2, 0, false, 19025, []*proxypb.TraceSpan{g1r, extraR})
			bogusOpR.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			bogusOpR.StatusMessage = "retry"
			extraR.Events = []*proxypb.SpanEvent{{Name: op + " start"}, {Name: op + " end"}, {Name: op + " end"}}

			resGoRetry := Verify(goCaps, []*proxypb.TraceSpan{g1r, cc1r, sc1r, pr1r, extraR, ccEr, scEr, prEr, cleanOtherR, bogusOpR}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-h2ar-g1", AckID: "a-h2ar-g1", ExpectedOutcome: outcome}},
				},
			})
			assertHasViolation(t, resGoRetry, CodeWrongStatusCode, "settlement."+op+".status")

			// C++ extraSub (UNSET, Events: nil) settled cleanly by otherOp UNSET + bogus 2-link sp ERROR.
			cSub, cCC, cSC, cPR := makeCppSubMessageSpans("p", "s", "m-h2ac-g1", "a-h2ac-g1", true, "", 19030)
			cExtra, cExCC, cExSC, cExPR := makeCppSubMessageSpans("p", "s", "m-h2ac-ex", "a-h2ac-ex", true, "", 19031)
			cCleanOther := makeCppSettlementSpan("p", "s", otherOp, 1, 19031, []*proxypb.TraceSpan{cExtra})
			cBogusOp := makeCppSettlementSpan("p", "s", op, 2, 19030, []*proxypb.TraceSpan{cSub, cExtra})
			cBogusOp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			cBogusOp.StatusMessage = eodPartialBatchErrorMsg
			cSub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp." + op + "_start"}, {Name: "gl-cpp." + op + "_end"}}

			resCpp := Verify(cppCaps, []*proxypb.TraceSpan{cSub, cCC, cSC, cPR, cExtra, cExCC, cExSC, cExPR, cCleanOther, cBogusOp}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-h2ac-g1", AckID: "a-h2ac-g1", ExpectedOutcome: outcome}},
				},
			})
			assertHasViolation(t, resCpp, CodeWrongStatusCode, "settlement."+op+".status")
		}
	})

	t.Run("H2b_And_H2c_ErrorExtraSub_BypassClosures", func(t *testing.T) {
		// H2b (Hunt R18-A1): Go ERROR extraSub + clean sibling UNSET span (both orderings) + bogus ERROR span -> FAIL.
		for _, cleanFirst := range []bool{true, false} {
			g1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-h2b-g1", "a-h2b-g1", "", 5, 0, true, 1, "", "acked", 19040)
			pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
			extraErr, ccE, scE, prE, _ := makeGoSubscribeMessageSpans("p", "s", "m-h2b-ex", "a-h2b-ex", "", 5, 0, true, 1, "", "", 19041)
			delete(extraErr.Attributes, "messaging.gcp_pubsub.result")
			extraErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			seqClean, seqBogus := 19042, 19043
			if !cleanFirst {
				seqClean, seqBogus = 19043, 19042
			}
			cleanSibling := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, seqClean, []*proxypb.TraceSpan{extraErr})
			bogusErr := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, seqBogus, []*proxypb.TraceSpan{g1, extraErr})
			bogusErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			bogusErr.StatusMessage = "fail"
			extraErr.Events = []*proxypb.SpanEvent{{Name: "ack start"}, {Name: "ack end"}}

			resH2b := Verify(goCaps, []*proxypb.TraceSpan{g1, cc1, sc1, pr1, extraErr, ccE, scE, prE, cleanSibling, bogusErr}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-h2b-g1", AckID: "a-h2b-g1", ExpectedOutcome: "acked"}},
				},
			})
			assertHasViolation(t, resH2b, CodeWrongStatusCode, "settlement.ack.status")
		}

		// H2c (Hunt R18-A2 / r18_2 Bypass 3): Go ERROR extraSub with conflicting ["ack start", "nack start"] -> FAIL.
		g1c, cc1c, sc1c, pr1c, _ := makeGoSubscribeMessageSpans("p", "s", "m-h2c-g1", "a-h2c-g1", "", 5, 0, true, 1, "", "acked", 19045)
		pr1c.Events = append(pr1c.Events, &proxypb.SpanEvent{Name: "ack called"})
		extraConf, ccEc, scEc, prEc, _ := makeGoSubscribeMessageSpans("p", "s", "m-h2c-ex", "a-h2c-ex", "", 5, 0, true, 1, "", "", 19046)
		delete(extraConf.Attributes, "messaging.gcp_pubsub.result")
		extraConf.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		bogusConf := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 19045, []*proxypb.TraceSpan{g1c, extraConf})
		bogusConf.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		bogusConf.StatusMessage = "conflicting"
		extraConf.Events = []*proxypb.SpanEvent{{Name: "ack start"}, {Name: "nack start"}}

		resGoConf := Verify(goCaps, []*proxypb.TraceSpan{g1c, cc1c, sc1c, pr1c, extraConf, ccEc, scEc, prEc, bogusConf}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-h2c-g1", AckID: "a-h2c-g1", ExpectedOutcome: "acked"}},
			},
		})
		assertHasViolation(t, resGoConf, CodeWrongStatusCode, "settlement.ack.status")

		// H2c (Hunt R18-A2-CPP): C++ ERROR extraSub with ["gl-cpp.ack_start", "gl-cpp.expired"] (heExtra, empty result)
		// and ["gl-cpp.ack_start", "gl-cpp.nack_start"] (hasConflictExtra), plus ERROR extraSub with clean sibling ack -> FAIL.
		for _, events := range [][]*proxypb.SpanEvent{
			{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.expired"}},
			{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.nack_start"}},
		} {
			cSub, cCC, cSC, cPR := makeCppSubMessageSpans("p", "s", "m-h2cc-g1", "a-h2cc-g1", true, "", 19050)
			cSub.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.ack_end"}}
			cEx, cExCC, cExSC, cExPR := makeCppSubMessageSpans("p", "s", "m-h2cc-ex", "a-h2cc-ex", true, "", 19051)
			cEx.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			cEx.Events = events
			cBogus := makeCppSettlementSpan("p", "s", "ack", 2, 19050, []*proxypb.TraceSpan{cSub, cEx})
			cBogus.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			cBogus.StatusMessage = "fail"

			resCppConf := Verify(cppCaps, []*proxypb.TraceSpan{cSub, cCC, cSC, cPR, cEx, cExCC, cExSC, cExPR, cBogus}, &TraceExpectation{
				Subscription:          "projects/p/subscriptions/s",
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-h2cc-g1", AckID: "a-h2cc-g1", ExpectedOutcome: "acked"}},
				},
			})
			assertHasViolation(t, resCppConf, CodeWrongStatusCode, "settlement.ack.status")
		}

		// C++ non-ERROR extraSub with gl-cpp.expired event -> FAIL settlement.ack.status.
		cSubE, cCCE, cSCE, cPRE := makeCppSubMessageSpans("p", "s", "m-h2ce-g1", "a-h2ce-g1", true, "", 19055)
		cSubE.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.ack_end"}}
		cExE, cExCCE, cExSCE, cExPRE := makeCppSubMessageSpans("p", "s", "m-h2ce-ex", "a-h2ce-ex", true, "", 19056)
		cExE.Events = []*proxypb.SpanEvent{{Name: "gl-cpp.expired"}}
		cBogusE := makeCppSettlementSpan("p", "s", "ack", 2, 19055, []*proxypb.TraceSpan{cSubE, cExE})
		cBogusE.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cBogusE.StatusMessage = eodPartialBatchErrorMsg
		resCppExp := Verify(cppCaps, []*proxypb.TraceSpan{cSubE, cCCE, cSCE, cPRE, cExE, cExCCE, cExSCE, cExPRE, cBogusE}, &TraceExpectation{
			Subscription:          "projects/p/subscriptions/s",
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-h2ce-g1", AckID: "a-h2ce-g1", ExpectedOutcome: "acked"}},
			},
		})
		assertHasViolation(t, resCppExp, CodeWrongStatusCode, "settlement.ack.status")
	})

	t.Run("H3a_And_H3b_EODFailed_And_ProcessSettlementCalled", func(t *testing.T) {
		// H3a: Go eod_failed with Settlements: nil linked to s ack ERROR missing ack start/end -> FAIL.
		gAck, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-h3a-1", "a-h3a-1", "", 5, 0, true, 1, "", "", 19060)
		delete(gAck.Attributes, "messaging.gcp_pubsub.result")
		gAck.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		sAck := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 19060, []*proxypb.TraceSpan{gAck})
		sAck.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		gAck.Events = nil // strip ack start/end after linking

		resH3aAck := Verify(goCaps, []*proxypb.TraceSpan{gAck, ccA, scA, prA, sAck}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-h3a-1", AckID: "a-h3a-1", ExpectedOutcome: "eod_failed"}},
			},
		})
		assertHasViolation(t, resH3aAck, CodeMissingEvent, "subscribe.event.ack_start")
		assertHasViolation(t, resH3aAck, CodeMissingEvent, "subscribe.event.ack_end")

		// H3a Go linked to s nack ERROR: positive contrast + negative missing nack start/end + H3b wrong "ack called".
		gNackPos, ccNp, scNp, prNp, _ := makeGoSubscribeMessageSpans("p", "s", "m-h3a-np", "a-h3a-np", "", 5, 0, true, 1, "", "", 19061)
		delete(gNackPos.Attributes, "messaging.gcp_pubsub.result")
		gNackPos.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		gNackPos.StatusMessage = "eod nack failed"
		prNp.Events = append(prNp.Events, &proxypb.SpanEvent{Name: "nack called"})
		sNackPos := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 19061, []*proxypb.TraceSpan{gNackPos})
		sNackPos.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		sNackPos.StatusMessage = "eod nack failed"
		sNackPos.Events = append(sNackPos.Events, &proxypb.SpanEvent{Name: "exception"})
		gNackPos.Events = append(gNackPos.Events, &proxypb.SpanEvent{Name: "exception"})
		resNackPos := Verify(goCaps, []*proxypb.TraceSpan{gNackPos, ccNp, scNp, prNp, sNackPos}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-h3a-np", AckID: "a-h3a-np", ExpectedOutcome: "eod_failed"}},
			},
		})
		if !resNackPos.Passed() {
			t.Fatalf("H3a Go eod_failed linked s nack positive expected PASS, got:\n%s", resNackPos.Format())
		}

		// Wrong "ack called" when linked to s nack, plus missing nack_start/nack_end on subSpan.
		gNackNeg, ccNn, scNn, prNn, _ := makeGoSubscribeMessageSpans("p", "s", "m-h3a-nn", "a-h3a-nn", "", 5, 0, true, 1, "", "", 19062)
		delete(gNackNeg.Attributes, "messaging.gcp_pubsub.result")
		gNackNeg.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		prNn.Events = append(prNn.Events, &proxypb.SpanEvent{Name: "ack called"})
		sNackNeg := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 19062, []*proxypb.TraceSpan{gNackNeg})
		sNackNeg.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		gNackNeg.Events = nil
		resNackNeg := Verify(goCaps, []*proxypb.TraceSpan{gNackNeg, ccNn, scNn, prNn, sNackNeg}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-h3a-nn", AckID: "a-h3a-nn", ExpectedOutcome: "eod_failed"}},
			},
		})
		assertHasViolation(t, resNackNeg, CodeMissingEvent, "subscribe.event.nack_start")
		assertHasViolation(t, resNackNeg, CodeMissingEvent, "subscribe.process.event.nack_called")
		assertHasViolation(t, resNackNeg, CodeUnexpectedSpan, "subscribe.process.event.unexpected_ack_called")

		// H3a Go unlinked eod_failed: (1) has ack start only -> missing ack_end, (2) has nack start only -> missing nack_end,
		// (3) empty events -> missing settlement_events + process settlement_called.
		for _, tc := range []struct {
			name     string
			events   []*proxypb.SpanEvent
			prEvents []*proxypb.SpanEvent
			wantRule string
			prRule   string
			prCode   ViolationCode
		}{
			{
				name:     "unlinked_ack_start_only",
				events:   []*proxypb.SpanEvent{{Name: "ack start"}},
				prEvents: []*proxypb.SpanEvent{{Name: "ack called"}},
				wantRule: "subscribe.event.ack_end",
			},
			{
				name:     "unlinked_nack_start_only",
				events:   []*proxypb.SpanEvent{{Name: "nack start"}},
				prEvents: []*proxypb.SpanEvent{{Name: "nack called"}},
				wantRule: "subscribe.event.nack_end",
			},
			{
				name:     "unlinked_empty_events_empty_process",
				events:   nil,
				prEvents: nil,
				wantRule: "subscribe.event.settlement_events",
				prRule:   "subscribe.process.event.settlement_called",
				prCode:   CodeMissingEvent,
			},
			{
				name:     "unlinked_empty_events_both_called",
				events:   nil,
				prEvents: []*proxypb.SpanEvent{{Name: "ack called"}, {Name: "nack called"}},
				wantRule: "subscribe.event.settlement_events",
				prRule:   "subscribe.process.event.unexpected_nack_called",
				prCode:   CodeUnexpectedSpan,
			},
		} {
			gU, ccU, scU, prU, _ := makeGoSubscribeMessageSpans("p", "s", "m-h3au", "a-h3au", "", 5, 0, true, 1, "", "", 19065)
			delete(gU.Attributes, "messaging.gcp_pubsub.result")
			gU.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			gU.Events = tc.events
			prU.Events = tc.prEvents
			resU := Verify(goCaps, []*proxypb.TraceSpan{gU, ccU, scU, prU}, &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m-h3au", AckID: "a-h3au", ExpectedOutcome: "eod_failed"}},
				},
			})
			assertHasViolation(t, resU, CodeMissingEvent, tc.wantRule)
			if tc.prRule != "" {
				assertHasViolation(t, resU, tc.prCode, tc.prRule)
			}
		}

		// H3a C++ eod_failed linked to s ack ERROR missing gl-cpp.ack_start/end -> FAIL.
		cSub, cCC, cSC, cPR := makeCppSubMessageSpans("p", "s", "m-h3ac-1", "a-h3ac-1", true, "", 19070)
		cSub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		csAck := makeCppSettlementSpan("p", "s", "ack", 1, 19070, []*proxypb.TraceSpan{cSub})
		csAck.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		resCppH3a := Verify(cppCaps, []*proxypb.TraceSpan{cSub, cCC, cSC, cPR, csAck}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-h3ac-1", AckID: "a-h3ac-1", ExpectedOutcome: "eod_failed"}},
			},
		})
		assertHasViolation(t, resCppH3a, CodeMissingEvent, "subscribe.event.cpp_ack_start")
		assertHasViolation(t, resCppH3a, CodeMissingEvent, "subscribe.event.cpp_ack_end")

		// H3b: ExpectedOutcome "acked"/"nacked" with Settlements: nil (or modack-only) verifies process "<op> called"
		// and rejects opposite called or both called.
		gAckProc, ccAp, scAp, prAp, _ := makeGoSubscribeMessageSpans("p", "s", "m-h3b-a", "a-h3b-a", "", 5, 0, false, 1, "", "acked", 19075)
		gAckProc.Events = []*proxypb.SpanEvent{{Name: "ack start"}, {Name: "ack end"}}
		prAp.Events = []*proxypb.SpanEvent{{Name: "nack called"}}
		resAckWrongProc := Verify(goCaps, []*proxypb.TraceSpan{gAckProc, ccAp, scAp, prAp}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-h3b-a", AckID: "a-h3b-a", ExpectedOutcome: "acked"}},
			},
		})
		assertHasViolation(t, resAckWrongProc, CodeMissingEvent, "subscribe.process.event.ack_called")
		assertHasViolation(t, resAckWrongProc, CodeUnexpectedSpan, "subscribe.process.event.unexpected_nack_called")

		gNackProc, ccNp2, scNp2, prNp2, _ := makeGoSubscribeMessageSpans("p", "s", "m-h3b-n", "a-h3b-n", "", 5, 0, false, 1, "", "nacked", 19076)
		gNackProc.Events = []*proxypb.SpanEvent{{Name: "nack start"}, {Name: "nack end"}}
		prNp2.Events = []*proxypb.SpanEvent{{Name: "ack called"}}
		resNackWrongProc := Verify(goCaps, []*proxypb.TraceSpan{gNackProc, ccNp2, scNp2, prNp2}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{MessageID: "m-h3b-n", AckID: "a-h3b-n", ExpectedOutcome: "nacked"}},
			},
		})
		assertHasViolation(t, resNackWrongProc, CodeMissingEvent, "subscribe.process.event.nack_called")
		assertHasViolation(t, resNackWrongProc, CodeUnexpectedSpan, "subscribe.process.event.unexpected_ack_called")

		// Explicit ack settlement with BOTH "ack called" and "nack called" on process span -> FAIL unexpected_nack_called.
		gBoth, ccB, scB, prB, _ := makeGoSubscribeMessageSpans("p", "s", "m-h3b-b", "a-h3b-b", "", 5, 0, false, 1, "", "acked", 19077)
		prB.Events = []*proxypb.SpanEvent{{Name: "ack called"}, {Name: "nack called"}}
		sAckB := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 19077, []*proxypb.TraceSpan{gBoth})
		resBothProc := Verify(goCaps, []*proxypb.TraceSpan{gBoth, ccB, scB, prB, sAckB}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:   "m-h3b-b",
					AckID:       "a-h3b-b",
					Settlements: []SettlementExpectation{{Operation: "ack"}},
				}},
			},
		})
		assertHasViolation(t, resBothProc, CodeUnexpectedSpan, "subscribe.process.event.unexpected_nack_called")
	})

	t.Run("Minor_WhitespaceResultAndZeroProfile", func(t *testing.T) {
		wsSub := &proxypb.TraceSpan{
			Attributes: map[string]*proxypb.AttributeValue{"messaging.gcp_pubsub.result": strAV("   \t ")},
		}
		if ha, hn, he := analyzeSubSpanTerminalEvidence(wsSub, Profile{}); ha || hn || he {
			t.Fatalf("whitespace-only result with zero Profile should return false,false,false; got %v,%v,%v", ha, hn, he)
		}
		customZeroSub := &proxypb.TraceSpan{
			Attributes: map[string]*proxypb.AttributeValue{"messaging.gcp_pubsub.result": strAV("custom")},
		}
		if ha, hn, he := analyzeSubSpanTerminalEvidence(customZeroSub, Profile{}); ha || hn || he {
			t.Fatalf("unmatched result with zero Profile should return false,false,false; got %v,%v,%v", ha, hn, he)
		}
	})
}

func TestWave20Regressions_Wave19ChallengerClosures(t *testing.T) {
	t.Parallel()

	goProf := ProfileGoCanonical()
	cppProf := ProfileCpp()
	goCaps := &proxypb.GetCapabilitiesResponse{
		SupportsMonitoring:          true,
		ClientLanguage:              "go",
		ClientLibrary:               "cloud.google.com/go/pubsub/v2",
		SupportsExactlyOnceDelivery: true,
	}
	cppCaps := &proxypb.GetCapabilitiesResponse{
		SupportsMonitoring:          true,
		ClientLanguage:              "cpp",
		ClientLibrary:               "google-cloud-cpp",
		SupportsExactlyOnceDelivery: true,
	}

	tid := func(n int) string { return fmt.Sprintf("%032x", n) }
	sid := func(n int) string { return fmt.Sprintf("%016x", n) }
	intAV := func(v int64) *proxypb.AttributeValue {
		return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
	}
	boolAV := func(v bool) *proxypb.AttributeValue {
		return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: v}}
	}
	baseT0 := time.Date(2026, 11, 16, 5, 0, 0, 0, time.UTC)
	ts := func(ms int64) *timestamppb.Timestamp {
		return timestamppb.New(baseT0.Add(time.Duration(ms) * time.Millisecond))
	}
	ev := func(name string, ms int64) *proxypb.SpanEvent {
		return &proxypb.SpanEvent{Name: name, Timestamp: ts(ms)}
	}
	evWithBC := func(name string, ms, bcVal int64) *proxypb.SpanEvent {
		return &proxypb.SpanEvent{
			Name:       name,
			Timestamp:  ts(ms),
			Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(bcVal)},
		}
	}
	assertPass := func(t *testing.T, r TraceResult) {
		t.Helper()
		if !r.Passed() {
			t.Fatalf("expected PASS, got %s:\n%s", r.Status, r.Format())
		}
	}

	const (
		proj    = "proj-w20"
		subID   = "sub-w20"
		fullSub = "projects/proj-w20/subscriptions/sub-w20"
	)

	makeSubCommonAttrs := func(prof Profile, msgID, ackID string, eod bool) map[string]*proxypb.AttributeValue {
		codeKey := prof.CodeFunctionAttrKey
		if codeKey == "" {
			codeKey = "code.function"
		}
		attrs := map[string]*proxypb.AttributeValue{
			"messaging.system":                    strAV("gcp_pubsub"),
			"messaging.destination.name":          strAV(subID),
			"gcp.project_id":                      strAV(proj),
			"messaging.message.id":                strAV(msgID),
			"messaging.gcp_pubsub.message.ack_id": strAV(ackID),
			"messaging.operation.name":            strAV("receive"),
			codeKey:                               strAV("receive"),
		}
		if prof.UseEnvelopeSize {
			attrs["messaging.message.envelope.size"] = intAV(100)
			attrs["messaging.message.body.size"] = intAV(10)
			attrs["messaging.operation.type"] = strAV("subscribe")
		} else {
			attrs["messaging.message.body.size"] = intAV(10)
			attrs["messaging.batch.message_count"] = intAV(1)
			attrs["messaging.gcp_pubsub.message.ordering_key"] = strAV("")
		}
		if prof.RequireGCPResourceName {
			attrs["gcp.resource.name"] = strAV(fullSub)
		}
		if prof.EOSSubscribeAttrKey != "" {
			attrs[prof.EOSSubscribeAttrKey] = boolAV(eod)
		}
		return attrs
	}

	makeSubTree := func(prof Profile, traceIdx, spanBase int, msgID, ackID string, eod bool, procEvents []*proxypb.SpanEvent, subEvents []*proxypb.SpanEvent, subResult string, subStatus proxypb.SpanStatusCode) []*proxypb.TraceSpan {
		tr := tid(traceIdx)
		subSID := sid(spanBase)
		ccSID := sid(spanBase + 1)
		schedSID := sid(spanBase + 2)
		procSID := sid(spanBase + 3)

		codeKey := prof.CodeFunctionAttrKey
		if codeKey == "" {
			codeKey = "code.function"
		}
		subAttrs := makeSubCommonAttrs(prof, msgID, ackID, eod)
		if subResult != "" {
			subAttrs["messaging.gcp_pubsub.result"] = strAV(subResult)
		}
		subStatusMsg := ""
		if subStatus == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			subStatusMsg = "eod failed"
		}
		sub := &proxypb.TraceSpan{
			TraceId:                  tr,
			SpanId:                   subSID,
			TraceFlags:               1,
			Name:                     subID + " subscribe",
			Kind:                     prof.SubscribeSpanKind,
			StartTime:                ts(10),
			EndTime:                  ts(50),
			InstrumentationScopeName: prof.ExpectedScope,
			Attributes:               subAttrs,
			Events:                   subEvents,
			StatusCode:               subStatus,
			StatusMessage:            subStatusMsg,
		}
		childAttrs := map[string]*proxypb.AttributeValue{
			"messaging.system":           strAV("gcp_pubsub"),
			"messaging.destination.name": strAV(subID),
			"gcp.project_id":             strAV(proj),
			codeKey:                      strAV("receive"),
		}
		if prof.ExpectZeroChildAttrs {
			childAttrs = nil
		}
		cc := &proxypb.TraceSpan{
			TraceId:                  tr,
			SpanId:                   ccSID,
			ParentSpanId:             subSID,
			TraceFlags:               1,
			Name:                     "subscriber concurrency control",
			Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
			StartTime:                ts(12),
			EndTime:                  ts(48),
			InstrumentationScopeName: prof.ExpectedScope,
			Attributes:               childAttrs,
		}
		sched := &proxypb.TraceSpan{
			TraceId:                  tr,
			SpanId:                   schedSID,
			ParentSpanId:             subSID,
			TraceFlags:               1,
			Name:                     prof.SchedulerSpanName,
			Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
			StartTime:                ts(14),
			EndTime:                  ts(46),
			InstrumentationScopeName: prof.ExpectedScope,
			Attributes:               childAttrs,
		}
		procAttrs := map[string]*proxypb.AttributeValue{
			"messaging.system":                          strAV("gcp_pubsub"),
			"messaging.destination.name":                strAV(subID),
			"gcp.project_id":                            strAV(proj),
			"gcp.resource.name":                         strAV(fullSub),
			"messaging.message.id":                      strAV(msgID),
			"messaging.message.body.size":               intAV(10),
			"messaging.gcp_pubsub.message.ordering_key": strAV(""),
			"messaging.operation.name":                  strAV("process"),
			"messaging.operation.type":                  strAV("process"),
			codeKey:                                     strAV("receive"),
		}
		if prof.ProcessMinimalAttributes {
			procAttrs = map[string]*proxypb.AttributeValue{
				"messaging.system": strAV("gcp_pubsub"),
			}
		}
		proc := &proxypb.TraceSpan{
			TraceId:                  tr,
			SpanId:                   procSID,
			ParentSpanId:             subSID,
			TraceFlags:               1,
			Name:                     subID + " process",
			Kind:                     prof.ProcessSpanKind,
			StartTime:                ts(16),
			EndTime:                  ts(44),
			InstrumentationScopeName: prof.ExpectedScope,
			Attributes:               procAttrs,
			Events:                   procEvents,
		}
		return []*proxypb.TraceSpan{sub, cc, sched, proc}
	}

	makeSettle := func(prof Profile, traceIdx, spanNum int, op string, bc *int64, status proxypb.SpanStatusCode, statusMsg string, targets ...*proxypb.TraceSpan) *proxypb.TraceSpan {
		tr := tid(traceIdx)
		stSID := sid(spanNum)
		codeKey := prof.CodeFunctionAttrKey
		if codeKey == "" {
			codeKey = "code.function"
		}
		codeFn := "sendAck"
		if op == "nack" || op == "modack" {
			codeFn = "sendModAck"
		}
		if prof.Name == cppProf.Name {
			codeFn = "pubsub::AckHandler::" + op
			if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
				status = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
			}
		}
		attrs := map[string]*proxypb.AttributeValue{
			"messaging.system":           strAV("gcp_pubsub"),
			"messaging.destination.name": strAV(subID),
			"gcp.project_id":             strAV(proj),
			"messaging.operation.name":   strAV(op),
			codeKey:                      strAV(codeFn),
		}
		if prof.RequireGCPResourceName {
			attrs["gcp.resource.name"] = strAV(fullSub)
		}
		if prof.Name == cppProf.Name {
			attrs["messaging.operation.type"] = strAV("settle")
			attrs["messaging.gcp_pubsub.subscription.template"] = strAV(fullSub)
			attrs["messaging.gcp_pubsub.message.ack_id"] = strAV("a-cpp")
			stCodeStr := "OK"
			if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
				stCodeStr = "INTERNAL"
			}
			attrs["gl-cpp.status_code"] = strAV(stCodeStr)
		}
		if bc != nil {
			attrs["messaging.batch.message_count"] = intAV(*bc)
		}
		var links []*proxypb.SpanLink
		for _, tgt := range targets {
			links = append(links, &proxypb.SpanLink{
				TraceId: tgt.GetTraceId(),
				SpanId:  tgt.GetSpanId(),
			})
			if prof.RequireBidirectionalSubscribeLinks {
				tgt.Links = append(tgt.Links, &proxypb.SpanLink{
					TraceId:    tr,
					SpanId:     stSID,
					Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV(op)},
				})
			}
		}
		var evs []*proxypb.SpanEvent
		if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			evs = []*proxypb.SpanEvent{ev("exception", 35)}
		}
		return &proxypb.TraceSpan{
			TraceId:                  tr,
			SpanId:                   stSID,
			TraceFlags:               1,
			Name:                     subID + " " + op,
			Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
			StartTime:                ts(25),
			EndTime:                  ts(40),
			InstrumentationScopeName: prof.ExpectedScope,
			Attributes:               attrs,
			Links:                    links,
			Events:                   evs,
			StatusCode:               status,
			StatusMessage:            statusMsg,
		}
	}

	makeModack := func(prof Profile, traceIdx, spanNum int, deadline int64, receipt bool, targets ...*proxypb.TraceSpan) *proxypb.TraceSpan {
		tr := tid(traceIdx)
		spSID := sid(spanNum)
		codeKey := prof.CodeFunctionAttrKey
		if codeKey == "" {
			codeKey = "code.function"
		}
		attrs := map[string]*proxypb.AttributeValue{
			"messaging.system":                          strAV("gcp_pubsub"),
			"messaging.destination.name":                strAV(subID),
			"gcp.project_id":                            strAV(proj),
			"gcp.resource.name":                         strAV(fullSub),
			"messaging.operation.name":                  strAV("modack"),
			"messaging.batch.message_count":             intAV(int64(max(1, len(targets)))),
			"messaging.gcp_pubsub.message.ack_deadline": intAV(deadline),
			"messaging.gcp_pubsub.is_receipt_modack":    boolAV(receipt),
			codeKey:                                     strAV("sendModAck"),
		}
		var links []*proxypb.SpanLink
		for _, tgt := range targets {
			links = append(links, &proxypb.SpanLink{TraceId: tgt.GetTraceId(), SpanId: tgt.GetSpanId()})
			if prof.RequireBidirectionalSubscribeLinks {
				tgt.Links = append(tgt.Links, &proxypb.SpanLink{
					TraceId:    tr,
					SpanId:     spSID,
					Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("modack")},
				})
			}
		}
		return &proxypb.TraceSpan{
			TraceId:                  tr,
			SpanId:                   spSID,
			TraceFlags:               1,
			Name:                     subID + " modack",
			Kind:                     prof.ModAckSpanKind,
			StartTime:                ts(20),
			EndTime:                  ts(24),
			InstrumentationScopeName: prof.ExpectedScope,
			Attributes:               attrs,
			Links:                    links,
		}
	}

	bc1 := int64(1)
	bc2 := int64(2)
	bc0 := int64(0)

	// 1. Target 1: Attack_A4a & Attack_A4d on AllowExtraPubSubSpans ERROR extraSub.
	t.Run("Target1_Attack_A4a_And_A4d_ExtraSubErrorValidation", func(t *testing.T) {
		t.Run("Attack_A4a_GoExtraSubErrorWithResultAckedRejected", func(t *testing.T) {
			m1 := makeSubTree(goProf, 1, 100, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			extraSub := makeSubTree(goProf, 2, 200, "m2", "a2", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			stM1 := makeSettle(goProf, 10, 1000, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
			stExtra := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "rpc failed", m1[0], extraSub[0])
			spans := append(append(m1, extraSub...), stM1, stExtra)
			res := Verify(goCaps, spans, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m1",
						AckID:       "a1",
						ExactlyOnce: BoolPtr(true),
						Settlements: []SettlementExpectation{{Operation: "ack"}},
					}},
				},
			})
			assertHasViolation(t, res, CodeWrongStatusCode, "settlement.ack.status")
		})

		t.Run("Attack_A4d_GoExtraSubErrorMissingAckEndRejected", func(t *testing.T) {
			m1 := makeSubTree(goProf, 1, 100, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			extraSub := makeSubTree(goProf, 2, 200, "m2", "a2", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			stM1 := makeSettle(goProf, 10, 1000, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
			stExtra := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "rpc failed", m1[0], extraSub[0])
			spans := append(append(m1, extraSub...), stM1, stExtra)
			res := Verify(goCaps, spans, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m1",
						AckID:       "a1",
						ExactlyOnce: BoolPtr(true),
						Settlements: []SettlementExpectation{{Operation: "ack"}},
					}},
				},
			})
			assertHasViolation(t, res, CodeWrongStatusCode, "settlement.ack.status")
		})

		t.Run("GoExtraSubErrorCompleteEventsWithoutResultPasses", func(t *testing.T) {
			m1 := makeSubTree(goProf, 1, 100, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			extraSub := makeSubTree(goProf, 2, 200, "m2", "a2", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			stM1 := makeSettle(goProf, 10, 1000, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
			stExtra := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "rpc failed", m1[0], extraSub[0])
			spans := append(append(m1, extraSub...), stM1, stExtra)
			res := Verify(goCaps, spans, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m1",
						AckID:       "a1",
						ExactlyOnce: BoolPtr(true),
						Settlements: []SettlementExpectation{{Operation: "ack"}},
					}},
				},
			})
			assertPass(t, res)
		})

		t.Run("CppExtraSubErrorWithResultAckStillAllowed", func(t *testing.T) {
			m1 := makeSubTree(cppProf, 1, 100, "m1", "a1", true, nil, []*proxypb.SpanEvent{ev("gl-cpp.ack_start", 22), ev("gl-cpp.ack_end", 38)}, "ack", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			extraSub := makeSubTree(cppProf, 2, 200, "m2", "a2", true, nil, []*proxypb.SpanEvent{ev("gl-cpp.ack_start", 22)}, "ack", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			stM1 := makeSettle(cppProf, 10, 1000, "ack", nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
			stExtra := makeSettle(cppProf, 11, 1100, "ack", nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "rpc failed", m1[0], extraSub[0])
			spans := append(append(m1, extraSub...), stM1, stExtra)
			res := Verify(cppCaps, spans, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m1",
						AckID:       "a1",
						ExactlyOnce: BoolPtr(true),
						Settlements: []SettlementExpectation{{Operation: "ack"}},
					}},
				},
			})
			assertPass(t, res)
		})
	})

	// 2. Target 2: Attack_A1, Attack_A3, and Attack_A3b process event inference.
	t.Run("Target2_Attack_A1_And_A3_ProcessInference", func(t *testing.T) {
		t.Run("Attack_A1_EODFailedAckEvWithNackCalledRejected", func(t *testing.T) {
			m1 := makeSubTree(goProf, 1, 100, "m1", "a1", true, []*proxypb.SpanEvent{ev("nack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38), ev("exception", 39)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			res := Verify(goCaps, m1, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m1",
						AckID:           "a1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "eod_failed",
					}},
				},
			})
			assertHasViolation(t, res, CodeMissingEvent, "subscribe.process.event.ack_called")
		})

		t.Run("Attack_A1_EODFailedNackEvMissingNackCalledRejected", func(t *testing.T) {
			m1 := makeSubTree(goProf, 1, 100, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("nack start", 22, 1), ev("nack end", 38), ev("exception", 39)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			res := Verify(goCaps, m1, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m1",
						AckID:           "a1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "eod_failed",
					}},
				},
			})
			assertHasViolation(t, res, CodeMissingEvent, "subscribe.process.event.nack_called")
		})

		t.Run("Attack_A1_EODFailedNackEvPassesWhenMatching", func(t *testing.T) {
			m1 := makeSubTree(goProf, 1, 100, "m1", "a1", true, []*proxypb.SpanEvent{ev("nack called", 20)}, []*proxypb.SpanEvent{evWithBC("nack start", 22, 1), ev("nack end", 38), ev("exception", 39)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			st := makeSettle(goProf, 10, 1000, "nack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "err", m1[0])
			res := Verify(goCaps, append(m1, st), &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m1",
						AckID:           "a1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "eod_failed",
					}},
				},
			})
			assertPass(t, res)
		})

		t.Run("Attack_A3_OmittedOutcomeCrossOpRejected", func(t *testing.T) {
			// Sub has ack evidence + Modack-only Settlement, process wrongly reports nack called.
			m1 := makeSubTree(goProf, 1, 100, "m1", "a1", true, []*proxypb.SpanEvent{ev("nack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			mod := makeModack(goProf, 11, 1101, 10, true, m1[0])
			res1 := Verify(goCaps, append(m1, mod), &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m1",
						AckID:       "a1",
						ExactlyOnce: BoolPtr(true),
						Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 10, IsReceiptModack: BoolPtr(true)}},
					}},
				},
			})
			assertHasViolation(t, res1, CodeUnexpectedSpan, "subscribe.process.event.unexpected_nack_called")

			// Sub has nack evidence, process wrongly reports ack called.
			m2 := makeSubTree(goProf, 2, 200, "m2", "a2", false, []*proxypb.SpanEvent{ev("ack called", 20)}, nil, "nacked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			st2 := makeSettle(goProf, 12, 1200, "nack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m2[0])
			res2 := Verify(goCaps, append(m2, st2), &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m2",
						AckID:       "a2",
						ExactlyOnce: BoolPtr(false),
					}},
				},
			})
			assertHasViolation(t, res2, CodeUnexpectedSpan, "subscribe.process.event.unexpected_ack_called")

			// Linked ack with empty sub events, process wrongly reports nack called.
			m3 := makeSubTree(goProf, 3, 300, "m3", "a3", true, []*proxypb.SpanEvent{ev("nack called", 20)}, nil, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			st3 := makeSettle(goProf, 13, 1300, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m3[0])
			res3 := Verify(goCaps, append(m3, st3), &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m3",
						AckID:       "a3",
						ExactlyOnce: BoolPtr(true),
					}},
				},
			})
			assertHasViolation(t, res3, CodeUnexpectedSpan, "subscribe.process.event.unexpected_nack_called")

			// Attack_A3b: abandoned handler with empty process events still passes when ExpectedOutcome is "".
			m4 := makeSubTree(goProf, 4, 400, "m4", "a4", false, nil, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			st4 := makeSettle(goProf, 14, 1400, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m4[0])
			res4 := Verify(goCaps, append(m4, st4), &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m4",
						AckID:       "a4",
						ExactlyOnce: BoolPtr(false),
					}},
				},
			})
			assertPass(t, res4)
		})
	})

	// 3. Target 4: Hunt19_H15a/b/c + positive contrasts.
	t.Run("Target4_Hunt19_H15_BatchPartnerGating", func(t *testing.T) {
		buildExplicitM1WithRetriedOK := func() ([]*proxypb.TraceSpan, *proxypb.TraceSpan) {
			m1 := makeSubTree(goProf, 1, 100, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stOK := makeSettle(goProf, 10, 1000, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
			return append(m1, stOK), m1[0]
		}

		t.Run("H15a_ExtraALO_Rejected", func(t *testing.T) {
			base, m1Sub := buildExplicitM1WithRetriedOK()
			extraALO := makeSubTree(goProf, 2, 200, "m2", "a2", false, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stErr := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad", m1Sub, extraALO[0])
			res := Verify(goCaps, append(append(base, extraALO...), stErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:   "m1",
						AckID:       "a1",
						ExactlyOnce: BoolPtr(true),
						Settlements: []SettlementExpectation{{Operation: "ack"}},
					}},
				},
			})
			assertHasViolation(t, res, CodeWrongStatusCode, "settlement.ack.status")
		})

		t.Run("H15a_MatchedALO_Rejected", func(t *testing.T) {
			base, m1Sub := buildExplicitM1WithRetriedOK()
			m2ALO := makeSubTree(goProf, 2, 200, "m2", "a2", false, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stM2 := makeSettle(goProf, 12, 1200, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m2ALO[0])
			stErr := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad", m1Sub, m2ALO[0])
			res := Verify(goCaps, append(append(base, m2ALO...), stM2, stErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{MessageID: "m2", AckID: "a2", ExactlyOnce: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "ack"}}},
					},
				},
			})
			assertHasViolation(t, res, CodeWrongStatusCode, "settlement.ack.status")
		})

		t.Run("H15_IncompatibleMatchedSubAndDanglingLink_Rejected", func(t *testing.T) {
			base, m1Sub := buildExplicitM1WithRetriedOK()
			m2Nack := makeSubTree(goProf, 2, 200, "m2", "a2", true, []*proxypb.SpanEvent{ev("nack called", 20)}, []*proxypb.SpanEvent{evWithBC("nack start", 22, 1), ev("nack end", 38)}, "nacked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stM2 := makeSettle(goProf, 12, 1200, "nack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m2Nack[0])
			stErr := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad", m1Sub, m2Nack[0])
			// Also add an unresolved link to cover extraSub==nil branch.
			stErr.Links = append(stErr.Links, &proxypb.SpanLink{TraceId: tid(99), SpanId: sid(9999)})
			res := Verify(goCaps, append(append(base, m2Nack...), stM2, stErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{MessageID: "m2", AckID: "a2", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "nack"}}},
					},
				},
			})
			assertHasViolation(t, res, CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
			assertHasViolation(t, res, CodeMissingLink, "subscribe.settlement.dangling_link")
		})

		t.Run("H15_ExtraSubConflictAndNonErrOtherOp_Rejected", func(t *testing.T) {
			base, m1Sub := buildExplicitM1WithRetriedOK()
			extraConf := makeSubTree(goProf, 2, 200, "m2", "a2", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38), evWithBC("nack start", 24, 1)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stErr := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad", m1Sub, extraConf[0])
			res1 := Verify(goCaps, append(append(cloneSpans(base), extraConf...), stErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}}},
				},
			})
			assertHasViolation(t, res1, CodeWrongStatusCode, "settlement.ack.status")

			base2, m1Sub2 := buildExplicitM1WithRetriedOK()
			extraOtherOp := makeSubTree(goProf, 3, 300, "m3", "a3", true, []*proxypb.SpanEvent{ev("nack called", 20)}, nil, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stOtherOK := makeSettle(goProf, 13, 1300, "nack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", extraOtherOp[0])
			stErr2 := makeSettle(goProf, 14, 1400, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad", m1Sub2, extraOtherOp[0])
			res2 := Verify(goCaps, append(append(base2, extraOtherOp...), stOtherOK, stErr2), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}}},
				},
			})
			assertHasViolation(t, res2, CodeWrongStatusCode, "settlement.ack.status")
		})

		t.Run("H15b_GoNonRetryExtraSubWithSuccessAck_Rejected", func(t *testing.T) {
			base, m1Sub := buildExplicitM1WithRetriedOK()
			extraOK := makeSubTree(goProf, 2, 200, "m2", "a2", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stExtraOK := makeSettle(goProf, 12, 1200, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", extraOK[0])
			stErr := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad", m1Sub, extraOK[0])
			res := Verify(goCaps, append(append(base, extraOK...), stExtraOK, stErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}}},
				},
			})
			assertHasViolation(t, res, CodeWrongStatusCode, "settlement.ack.status")
		})

		t.Run("H15c_ErrorExtraSubWithPriorSuccessAck_Rejected", func(t *testing.T) {
			base, m1Sub := buildExplicitM1WithRetriedOK()
			extraErr := makeSubTree(goProf, 2, 200, "m2", "a2", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			stExtraOK := makeSettle(goProf, 12, 1200, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", extraErr[0])
			stErr := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad", m1Sub, extraErr[0])
			res := Verify(goCaps, append(append(base, extraErr...), stExtraOK, stErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}}},
				},
			})
			assertHasViolation(t, res, CodeWrongStatusCode, "settlement.ack.status")
		})

		t.Run("H15_CppExtraSubWithPriorSuccessAck_Rejected", func(t *testing.T) {
			m1 := makeSubTree(cppProf, 1, 100, "m1", "a1", true, nil, []*proxypb.SpanEvent{ev("gl-cpp.ack_start", 22), ev("gl-cpp.ack_end", 38)}, "ack", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stM1OK := makeSettle(cppProf, 10, 1000, "ack", nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
			extraOK := makeSubTree(cppProf, 2, 200, "m2", "a2", true, nil, []*proxypb.SpanEvent{ev("gl-cpp.ack_start", 22), ev("gl-cpp.ack_end", 38)}, "ack", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stExtraOK := makeSettle(cppProf, 12, 1200, "ack", nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", extraOK[0])
			stErr := makeSettle(cppProf, 11, 1100, "ack", nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad", m1[0], extraOK[0])
			res := Verify(cppCaps, append(append(m1, extraOK...), stM1OK, stExtraOK, stErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}}},
				},
			})
			assertHasViolation(t, res, CodeWrongStatusCode, "settlement.ack.status")
		})

		t.Run("PositiveContrasts_ProbeICaseY_And_ExplicitEODRetry", func(t *testing.T) {
			// Probe I Case Y: retried extra EOD partner (ack start=1, ack end=2) stays valid.
			base, m1Sub := buildExplicitM1WithRetriedOK()
			extraRetry := makeSubTree(goProf, 2, 200, "m2", "a2", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 30), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stExtraRetryOK := makeSettle(goProf, 12, 1200, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", extraRetry[0])
			stErr := makeSettle(goProf, 11, 1100, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "partial", m1Sub, extraRetry[0])
			res1 := Verify(goCaps, append(append(base, extraRetry...), stExtraRetryOK, stErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}}},
				},
			})
			assertPass(t, res1)

			// Explicit EOD single-message retry still passes.
			base2, m1Sub2 := buildExplicitM1WithRetriedOK()
			stErr2 := makeSettle(goProf, 11, 1100, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "retry", m1Sub2)
			res2 := Verify(goCaps, append(base2, stErr2), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}}},
				},
			})
			assertPass(t, res2)
		})
	})

	// 4. Target 3: Live_Drop_NilSettlements (Phase 4.6).
	t.Run("Target3_LiveDrop_NilSettlements_Phase4_6", func(t *testing.T) {
		t.Run("Standalone_Go_Ack_Nack_EODFailed_BatchedTwoDrops", func(t *testing.T) {
			// Standalone Go dropped ack (0-link ack + 0-link modack).
			stAck := makeSettle(goProf, 20, 2000, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			stMod := makeModack(goProf, 21, 2100, 10, true)
			resAck := Verify(goCaps, []*proxypb.TraceSpan{stAck, stMod}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:              "d1",
						AckID:                  "da1",
						ExpectDroppedUnsampled: true,
						WireTraceparent:        "00-" + tid(901) + "-" + sid(901) + "-00",
						ExpectedOutcome:        "acked",
					}},
				},
			})
			assertPass(t, resAck)

			// Standalone Go dropped nack (exercises opAvail<=0 on ack loop, allowAck=false, allowNack=true).
			stNack := makeSettle(goProf, 22, 2200, "nack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resNack := Verify(goCaps, []*proxypb.TraceSpan{stNack}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:              "d1",
						AckID:                  "da1",
						ExpectDroppedUnsampled: true,
						WireTraceparent:        "00-" + tid(902) + "-" + sid(902) + "-00",
						ExpectedOutcome:        "nacked",
					}},
				},
			})
			assertPass(t, resNack)

			// Standalone Go dropped eod_failed (ERROR 0-link ack).
			stErr := makeSettle(goProf, 23, 2300, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "eod err")
			resErr := Verify(goCaps, []*proxypb.TraceSpan{stErr}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:              "d1",
						AckID:                  "da1",
						ExpectDroppedUnsampled: true,
						WireTraceparent:        "00-" + tid(903) + "-" + sid(903) + "-00",
						ExactlyOnce:            BoolPtr(true),
						ExpectedOutcome:        "eod_failed",
					}},
				},
			})
			assertPass(t, resErr)

			// Batched 2 dropped messages into one 0-link ack (bc=2).
			stAck2 := makeSettle(goProf, 24, 2400, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resBatch2 := Verify(goCaps, []*proxypb.TraceSpan{stAck2}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d1", AckID: "da1", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(904) + "-" + sid(904) + "-00", ExpectedOutcome: "acked"},
						{MessageID: "d2", AckID: "da2", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(905) + "-" + sid(905) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertPass(t, resBatch2)
		})

		t.Run("JointPermutation_WildcardBeforeExplicitAcked", func(t *testing.T) {
			stAck := makeSettle(goProf, 30, 3000, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			stNack := makeSettle(goProf, 31, 3100, "nack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			res := Verify(goCaps, []*proxypb.TraceSpan{stAck, stNack}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(910) + "-" + sid(910) + "-00", ExpectedOutcome: ""},
						{MessageID: "d1", AckID: "da1", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(911) + "-" + sid(911) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertPass(t, res)
		})

		t.Run("MixedExplicitExpiredStrictMatchedLinkedAndPartialAssign", func(t *testing.T) {
			// Positive: explicit + expired + "" + "nacked" all have matching settlement spans -> PASS.
			stExplicit := makeSettle(goProf, 40, 4000, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			stNil := makeSettle(goProf, 41, 4100, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			stNack := makeSettle(goProf, 42, 4200, "nack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resMixed := Verify(goCaps, []*proxypb.TraceSpan{stExplicit, stNil, stNack}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(920) + "-" + sid(920) + "-00", Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{MessageID: "d1", AckID: "da1", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(921) + "-" + sid(921) + "-00", ExpectedOutcome: "expired"},
						{MessageID: "d2", AckID: "da2", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(922) + "-" + sid(922) + "-00", ExpectedOutcome: ""},
						{MessageID: "d3", AckID: "da3", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(923) + "-" + sid(923) + "-00", ExpectedOutcome: "nacked"},
					},
				},
			})
			assertPass(t, resMixed)

			// Partial assignment (rj < 0, stNack omitted) -> MUST FAIL CodeMissingSpan.
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{stExplicit, stNil}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(920) + "-" + sid(920) + "-00", Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{MessageID: "d1", AckID: "da1", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(921) + "-" + sid(921) + "-00", ExpectedOutcome: "expired"},
						{MessageID: "d2", AckID: "da2", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(922) + "-" + sid(922) + "-00", ExpectedOutcome: ""},
						{MessageID: "d3", AckID: "da3", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(923) + "-" + sid(923) + "-00", ExpectedOutcome: "nacked"},
					},
				},
			}), CodeMissingSpan, "subscribe.settlement.missing_span")

			// Wrong op (ack span only for "nacked", AllowExtra=true, Attack A1) -> MUST FAIL CodeMissingSpan.
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{stNil}, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d3", AckID: "da3", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(923) + "-" + sid(923) + "-00", ExpectedOutcome: "nacked"},
					},
				},
			}), CodeMissingSpan, "subscribe.settlement.missing_span")

			// StrictSettlementOps=true with empty Settlements + ExpectedOutcome="acked" -> MUST FAIL CodeMissingSpan.
			resStrict := Verify(goCaps, nil, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					StrictSettlementOps: true,
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(924) + "-" + sid(924) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertHasViolation(t, resStrict, CodeMissingSpan, "subscribe.settlement.missing_span")
		})

		t.Run("OversizedBC_ZeroBC_WrongOp_And_DoubleSpend_Rejected", func(t *testing.T) {
			// Oversized bc=2 for 1 dropped msg fails settlement.ack.attr.batch_count.
			stBC2 := makeSettle(goProf, 50, 5000, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resBC2 := Verify(goCaps, []*proxypb.TraceSpan{stBC2}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(930) + "-" + sid(930) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertHasViolation(t, resBC2, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

			// bc=0 is skipped and fails traces.no_extra_pubsub_spans.
			stBC0 := makeSettle(goProf, 51, 5100, "ack", &bc0, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resBC0 := Verify(goCaps, []*proxypb.TraceSpan{stBC0}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(931) + "-" + sid(931) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertHasViolation(t, resBC0, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")

			// Double-spend: linked s1 ack (bc=2) + unlinked d0 ack (bc=1) with only 1 dropped msg -> rejects s1 bc=2.
			s1 := makeSubTree(goProf, 52, 5200, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("ack start", 22, 1), ev("ack end", 38)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stLinked := makeSettle(goProf, 53, 5300, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", s1[0])
			stUnlinked := makeSettle(goProf, 54, 5400, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resDouble := Verify(goCaps, append(s1, stLinked, stUnlinked), &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(932) + "-" + sid(932) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertHasViolation(t, resDouble, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

			// Wrong op (0-link nack when dropped expects acked) fails traces.no_extra_pubsub_spans.
			stWrongOp := makeSettle(goProf, 55, 5500, "nack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resWrong := Verify(goCaps, []*proxypb.TraceSpan{stWrongOp}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(933) + "-" + sid(933) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertHasViolation(t, resWrong, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
		})

		t.Run("CppPerMessageSettle_And_AllowExtraSkipsErrorMismatch", func(t *testing.T) {
			stCpp := makeSettle(cppProf, 60, 6000, "ack", nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resCpp := Verify(cppCaps, []*proxypb.TraceSpan{stCpp}, &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(940) + "-" + sid(940) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertPass(t, resCpp)
			hasDev := false
			for _, d := range resCpp.Deviations {
				if d.Rule == "cardinality.per_message_settle" {
					hasDev = true
					break
				}
			}
			if !hasDev {
				t.Fatalf("expected cardinality.per_message_settle deviation on C++, got %+v", resCpp.Deviations)
			}

			// AllowExtraPubSubSpans=true ignores mismatched-status background 0-link ERROR ack and picks UNSET ack.
			stBgErr := makeSettle(goProf, 61, 6100, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bg err")
			stOK := makeSettle(goProf, 62, 6200, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "")
			resAllowExtra := Verify(goCaps, []*proxypb.TraceSpan{stBgErr, stOK}, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "d0", AckID: "da0", ExpectDroppedUnsampled: true, WireTraceparent: "00-" + tid(941) + "-" + sid(941) + "-00", ExpectedOutcome: "acked"},
					},
				},
			})
			assertPass(t, resAllowExtra)
		})
	})
}

func TestWave21Regressions_Target1_Target2_Target4(t *testing.T) {
	t.Parallel()

	goProf := ProfileGoCanonical()
	cppProf := ProfileCpp()
	goCaps := &proxypb.GetCapabilitiesResponse{
		SupportsMonitoring:          true,
		ClientLanguage:              "go",
		ClientLibrary:               "cloud.google.com/go/pubsub/v2",
		SupportsExactlyOnceDelivery: true,
	}
	cppCaps := &proxypb.GetCapabilitiesResponse{
		SupportsMonitoring:          true,
		ClientLanguage:              "cpp",
		ClientLibrary:               "google-cloud-cpp",
		SupportsExactlyOnceDelivery: true,
	}

	tid := func(n int) string { return fmt.Sprintf("%032x", n) }
	sid := func(n int) string { return fmt.Sprintf("%016x", n) }
	intAV := func(v int64) *proxypb.AttributeValue {
		return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
	}
	boolAV := func(v bool) *proxypb.AttributeValue {
		return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: v}}
	}
	baseT0 := time.Date(2026, 11, 16, 6, 0, 0, 0, time.UTC)
	ts := func(ms int64) *timestamppb.Timestamp {
		return timestamppb.New(baseT0.Add(time.Duration(ms) * time.Millisecond))
	}
	ev := func(name string, ms int64) *proxypb.SpanEvent {
		return &proxypb.SpanEvent{Name: name, Timestamp: ts(ms)}
	}
	evWithBC := func(name string, ms, bcVal int64) *proxypb.SpanEvent {
		return &proxypb.SpanEvent{
			Name:       name,
			Timestamp:  ts(ms),
			Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(bcVal)},
		}
	}
	assertPass := func(t *testing.T, r TraceResult) {
		t.Helper()
		if !r.Passed() {
			t.Fatalf("expected PASS, got %s:\n%s", r.Status, r.Format())
		}
	}

	const (
		proj    = "proj-w21"
		subID   = "sub-w21"
		fullSub = "projects/proj-w21/subscriptions/sub-w21"
	)

	makeSubTree := func(prof Profile, traceIdx, spanBase int, msgID, ackID string, eod bool, procEvents, subEvents []*proxypb.SpanEvent, subResult string, subStatus proxypb.SpanStatusCode) []*proxypb.TraceSpan {
		tr := tid(traceIdx)
		subSID := sid(spanBase)
		ccSID := sid(spanBase + 1)
		schedSID := sid(spanBase + 2)
		procSID := sid(spanBase + 3)

		codeKey := prof.CodeFunctionAttrKey
		if codeKey == "" {
			codeKey = "code.function"
		}
		subAttrs := map[string]*proxypb.AttributeValue{
			"messaging.system":                    strAV("gcp_pubsub"),
			"messaging.destination.name":          strAV(subID),
			"gcp.project_id":                      strAV(proj),
			"messaging.message.id":                strAV(msgID),
			"messaging.gcp_pubsub.message.ack_id": strAV(ackID),
			"messaging.operation.name":            strAV("receive"),
			codeKey:                               strAV("receive"),
		}
		if prof.UseEnvelopeSize {
			subAttrs["messaging.message.envelope.size"] = intAV(100)
			subAttrs["messaging.message.body.size"] = intAV(10)
			subAttrs["messaging.operation.type"] = strAV("subscribe")
		} else {
			subAttrs["messaging.message.body.size"] = intAV(10)
			subAttrs["messaging.batch.message_count"] = intAV(1)
			subAttrs["messaging.gcp_pubsub.message.ordering_key"] = strAV("")
		}
		if prof.RequireGCPResourceName {
			subAttrs["gcp.resource.name"] = strAV(fullSub)
		}
		if prof.EOSSubscribeAttrKey != "" {
			subAttrs[prof.EOSSubscribeAttrKey] = boolAV(eod)
		}
		if subResult != "" {
			subAttrs["messaging.gcp_pubsub.result"] = strAV(subResult)
		}
		subStatusMsg := ""
		if subStatus == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			subStatusMsg = "eod failed"
		}
		sub := &proxypb.TraceSpan{
			TraceId:                  tr,
			SpanId:                   subSID,
			TraceFlags:               1,
			Name:                     subID + " subscribe",
			Kind:                     prof.SubscribeSpanKind,
			StartTime:                ts(10),
			EndTime:                  ts(50),
			InstrumentationScopeName: prof.ExpectedScope,
			Attributes:               subAttrs,
			Events:                   subEvents,
			StatusCode:               subStatus,
			StatusMessage:            subStatusMsg,
		}
		childAttrs := map[string]*proxypb.AttributeValue{
			"messaging.system":           strAV("gcp_pubsub"),
			"messaging.destination.name": strAV(subID),
			"gcp.project_id":             strAV(proj),
			codeKey:                      strAV("receive"),
		}
		if prof.ExpectZeroChildAttrs {
			childAttrs = nil
		}
		cc := &proxypb.TraceSpan{
			TraceId: tr, SpanId: ccSID, ParentSpanId: subSID, TraceFlags: 1,
			Name: "subscriber concurrency control", Kind: proxypb.SpanKind_SPAN_KIND_INTERNAL,
			StartTime: ts(12), EndTime: ts(48), InstrumentationScopeName: prof.ExpectedScope, Attributes: childAttrs,
		}
		sched := &proxypb.TraceSpan{
			TraceId: tr, SpanId: schedSID, ParentSpanId: subSID, TraceFlags: 1,
			Name: prof.SchedulerSpanName, Kind: proxypb.SpanKind_SPAN_KIND_INTERNAL,
			StartTime: ts(14), EndTime: ts(46), InstrumentationScopeName: prof.ExpectedScope, Attributes: childAttrs,
		}
		procAttrs := map[string]*proxypb.AttributeValue{
			"messaging.system":                          strAV("gcp_pubsub"),
			"messaging.destination.name":                strAV(subID),
			"gcp.project_id":                            strAV(proj),
			"gcp.resource.name":                         strAV(fullSub),
			"messaging.message.id":                      strAV(msgID),
			"messaging.message.body.size":               intAV(10),
			"messaging.gcp_pubsub.message.ordering_key": strAV(""),
			"messaging.operation.name":                  strAV("process"),
			"messaging.operation.type":                  strAV("process"),
			codeKey:                                     strAV("receive"),
		}
		if prof.ProcessMinimalAttributes {
			procAttrs = map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub")}
		}
		proc := &proxypb.TraceSpan{
			TraceId: tr, SpanId: procSID, ParentSpanId: subSID, TraceFlags: 1,
			Name: subID + " process", Kind: prof.ProcessSpanKind,
			StartTime: ts(16), EndTime: ts(44), InstrumentationScopeName: prof.ExpectedScope,
			Attributes: procAttrs, Events: procEvents,
		}
		return []*proxypb.TraceSpan{sub, cc, sched, proc}
	}

	makeSettle := func(prof Profile, traceIdx, spanNum int, op string, bc *int64, status proxypb.SpanStatusCode, statusMsg string, targets ...*proxypb.TraceSpan) *proxypb.TraceSpan {
		tr := tid(traceIdx)
		stSID := sid(spanNum)
		codeKey := prof.CodeFunctionAttrKey
		if codeKey == "" {
			codeKey = "code.function"
		}
		codeFn := "sendAck"
		if op == "nack" || op == "modack" {
			codeFn = "sendModAck"
		}
		if prof.Name == cppProf.Name {
			codeFn = "pubsub::AckHandler::" + op
			if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET {
				status = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
			}
		}
		attrs := map[string]*proxypb.AttributeValue{
			"messaging.system":           strAV("gcp_pubsub"),
			"messaging.destination.name": strAV(subID),
			"gcp.project_id":             strAV(proj),
			"messaging.operation.name":   strAV(op),
			codeKey:                      strAV(codeFn),
		}
		if prof.RequireGCPResourceName {
			attrs["gcp.resource.name"] = strAV(fullSub)
		}
		if prof.Name == cppProf.Name {
			attrs["messaging.operation.type"] = strAV("settle")
			attrs["messaging.gcp_pubsub.subscription.template"] = strAV(fullSub)
			attrs["messaging.gcp_pubsub.message.ack_id"] = strAV("a-cpp")
			stCodeStr := "OK"
			if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
				stCodeStr = "INTERNAL"
			}
			attrs["gl-cpp.status_code"] = strAV(stCodeStr)
		}
		if bc != nil {
			attrs["messaging.batch.message_count"] = intAV(*bc)
		}
		var links []*proxypb.SpanLink
		for _, tgt := range targets {
			links = append(links, &proxypb.SpanLink{TraceId: tgt.GetTraceId(), SpanId: tgt.GetSpanId()})
			if prof.RequireBidirectionalSubscribeLinks {
				tgt.Links = append(tgt.Links, &proxypb.SpanLink{
					TraceId:    tr,
					SpanId:     stSID,
					Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV(op)},
				})
			}
		}
		var evs []*proxypb.SpanEvent
		if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
			evs = []*proxypb.SpanEvent{ev("exception", 35)}
		}
		return &proxypb.TraceSpan{
			TraceId: tr, SpanId: stSID, TraceFlags: 1, Name: subID + " " + op,
			Kind: proxypb.SpanKind_SPAN_KIND_INTERNAL, StartTime: ts(25), EndTime: ts(40),
			InstrumentationScopeName: prof.ExpectedScope, Attributes: attrs,
			Links: links, Events: evs, StatusCode: status, StatusMessage: statusMsg,
		}
	}

	makeModack := func(prof Profile, traceIdx, spanNum int, deadline int64, receipt bool, targets ...*proxypb.TraceSpan) *proxypb.TraceSpan {
		tr := tid(traceIdx)
		spSID := sid(spanNum)
		attrs := map[string]*proxypb.AttributeValue{
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV(subID),
			"gcp.project_id":                strAV(proj),
			"messaging.operation.name":      strAV("modack"),
			"messaging.batch.message_count": intAV(int64(max(1, len(targets)))),
		}
		if prof.Name == ProfileGo {
			attrs["gcp.resource.name"] = strAV(fullSub)
			attrs["code.function"] = strAV("sendModAck")
			attrs["messaging.gcp_pubsub.message.ack_deadline"] = intAV(deadline)
			attrs["messaging.gcp_pubsub.is_receipt_modack"] = boolAV(receipt)
		} else {
			attrs["messaging.operation.type"] = strAV("extend")
			attrs["messaging.gcp_pubsub.message.ack_deadline_seconds"] = intAV(deadline)
		}
		var links []*proxypb.SpanLink
		for _, tgt := range targets {
			links = append(links, &proxypb.SpanLink{TraceId: tgt.GetTraceId(), SpanId: tgt.GetSpanId()})
			if prof.RequireBidirectionalSubscribeLinks {
				tgt.Links = append(tgt.Links, &proxypb.SpanLink{
					TraceId:    tr,
					SpanId:     spSID,
					Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("modack")},
				})
			}
		}
		return &proxypb.TraceSpan{
			TraceId: tr, SpanId: spSID, TraceFlags: 1, Name: subID + " modack",
			Kind: prof.ModAckSpanKind, StartTime: ts(20), EndTime: ts(24),
			InstrumentationScopeName: prof.ExpectedScope, Attributes: attrs, Links: links,
		}
	}

	bc1 := int64(1)
	bc2 := int64(2)

	// 1. Target 1 Fix: !isErrSpan (UNSET on Go / OK on C++) linking ANY ERROR extraSub MUST fail settlement.<op>.status.
	t.Run("Target1_NonErrSpan_LinksErrorExtraSub_GoAndCPP", func(t *testing.T) {
		for _, op := range []string{"ack", "nack"} {
			otherOp := "nack"
			if op == "nack" {
				otherOp = "ack"
			}
			outcome := op + "ed"
			goCases := []struct {
				name     string
				extraEOD bool
				res      string
				events   []*proxypb.SpanEvent
			}{
				{name: "valid_eod_error", extraEOD: true, res: "", events: []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 38)}},
				{name: "contradictory_result_acked", extraEOD: true, res: "acked", events: []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 38)}},
				{name: "contradictory_result_nacked", extraEOD: true, res: "nacked", events: []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 38)}},
				{name: "contradictory_result_expired", extraEOD: true, res: "expired", events: []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 38)}},
				{name: "missing_end_event", extraEOD: true, res: "", events: []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2)}},
				{name: "missing_start_event", extraEOD: true, res: "", events: []*proxypb.SpanEvent{ev(op+" end", 38)}},
				{name: "nil_events", extraEOD: true, res: "", events: nil},
				{name: "conflicting_ack_nack_events", extraEOD: true, res: "", events: []*proxypb.SpanEvent{evWithBC("ack start", 22, 2), evWithBC("nack start", 24, 2)}},
				{name: "cross_op_events", extraEOD: true, res: "", events: []*proxypb.SpanEvent{evWithBC(otherOp+" start", 22, 2), ev(otherOp+" end", 38)}},
				{name: "alo_error_extrasub", extraEOD: false, res: "", events: []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 38)}},
			}
			for idx, gc := range goCases {
				for _, withMatchedSibling := range []bool{true, false} {
					m1 := makeSubTree(goProf, 100+idx, 1000+idx*20, "m1", "a1", true, []*proxypb.SpanEvent{ev(op+" called", 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 1), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
					extra := makeSubTree(goProf, 200+idx, 2000+idx*20, "m2", "a2", gc.extraEOD, []*proxypb.SpanEvent{ev(op+" called", 20)}, gc.events, gc.res, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
					spans := append(append([]*proxypb.TraceSpan{}, m1...), extra...)
					if withMatchedSibling {
						stM1 := makeSettle(goProf, 300+idx, 3000+idx, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
						spans = append(spans, stM1)
					}
					stNonErr := makeSettle(goProf, 400+idx, 4000+idx, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0], extra[0])
					spans = append(spans, stNonErr)

					res := Verify(goCaps, spans, &TraceExpectation{
						ProjectID:             proj,
						Subscription:          fullSub,
						AllowExtraPubSubSpans: true,
						Subscribe: &SubscribeExpectation{
							Messages: []SubscribeMessageExpectation{{
								MessageID:       "m1",
								AckID:           "a1",
								ExactlyOnce:     BoolPtr(true),
								ExpectedOutcome: outcome,
								Settlements:     []SettlementExpectation{{Operation: op, ExpectError: false}},
							}},
						},
					})
					assertHasViolation(t, res, CodeWrongStatusCode, "settlement."+op+".status")
				}
			}

			// Positive guard: UNSET settlement linking [m1, non-error valid extraSub] still PASSES.
			m1Pos := makeSubTree(goProf, 150, 1500, "m1", "a1", true, []*proxypb.SpanEvent{ev(op+" called", 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			extraPos := makeSubTree(goProf, 250, 2500, "m2", "a2", true, []*proxypb.SpanEvent{ev(op+" called", 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stPos := makeSettle(goProf, 450, 4500, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1Pos[0], extraPos[0])
			assertPass(t, Verify(goCaps, append(append(m1Pos, extraPos...), stPos), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m1",
						AckID:           "a1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: outcome,
						Settlements:     []SettlementExpectation{{Operation: op, ExpectError: false}},
					}},
				},
			}))

			cppCases := []struct {
				name     string
				extraEOD bool
				res      string
				events   []*proxypb.SpanEvent
			}{
				{name: "valid_eod_error", extraEOD: true, res: op, events: []*proxypb.SpanEvent{ev("gl-cpp."+op+"_start", 22)}},
				{name: "end_only", extraEOD: true, res: "", events: []*proxypb.SpanEvent{ev("gl-cpp."+op+"_end", 38)}},
				{name: "cross_op_result_nil_events", extraEOD: true, res: otherOp, events: nil},
				{name: "conflicting_ack_expired", extraEOD: true, res: "", events: []*proxypb.SpanEvent{ev("gl-cpp.ack_start", 22), ev("gl-cpp.expired", 30)}},
				{name: "conflicting_ack_nack", extraEOD: true, res: "", events: []*proxypb.SpanEvent{ev("gl-cpp.ack_start", 22), ev("gl-cpp.nack_start", 24)}},
				{name: "alo_error_extrasub", extraEOD: false, res: op, events: []*proxypb.SpanEvent{ev("gl-cpp."+op+"_start", 22)}},
			}
			for idx, cc := range cppCases {
				for _, withMatchedSibling := range []bool{true, false} {
					m1 := makeSubTree(cppProf, 500+idx, 5000+idx*20, "m1", "a1", true, nil, []*proxypb.SpanEvent{ev("gl-cpp."+op+"_start", 22), ev("gl-cpp."+op+"_end", 38)}, op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
					extra := makeSubTree(cppProf, 600+idx, 6000+idx*20, "m2", "a2", cc.extraEOD, nil, cc.events, cc.res, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
					spans := append(append([]*proxypb.TraceSpan{}, m1...), extra...)
					if withMatchedSibling {
						stM1 := makeSettle(cppProf, 700+idx, 7000+idx, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m1[0])
						spans = append(spans, stM1)
					}
					stNonErr := makeSettle(cppProf, 800+idx, 8000+idx, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m1[0], extra[0])
					spans = append(spans, stNonErr)

					res := Verify(cppCaps, spans, &TraceExpectation{
						ProjectID:             proj,
						Subscription:          fullSub,
						AllowExtraPubSubSpans: true,
						Subscribe: &SubscribeExpectation{
							Messages: []SubscribeMessageExpectation{{
								MessageID:       "m1",
								AckID:           "a1",
								ExactlyOnce:     BoolPtr(true),
								ExpectedOutcome: outcome,
								Settlements:     []SettlementExpectation{{Operation: op, ExpectError: false}},
							}},
						},
					})
					assertHasViolation(t, res, CodeWrongStatusCode, "settlement."+op+".status")
				}
			}
		}
	})

	// 2. Target 4 Fix: allowUnmatchedExplicitEOD scoped strictly to single-message (!isMultiMsgBatch).
	t.Run("Target4_AllowUnmatchedExplicitEOD_MultiMsgGuards", func(t *testing.T) {
		for _, op := range []string{"ack", "nack"} {
			outcome := op + "ed"
			called := op + " called"

			// 2a. Attack_T4_MatchedPair_CleanNoRetry_ExplicitM1_BogusFail (forward/reversed msgs, "bogus fail"/partial msg, mixed nil/both explicit).
			for _, tc := range []struct {
				name       string
				statusMsg  string
				reverseMsg bool
				m2Explicit bool
			}{
				{name: "bogus_fail_m1_explicit_m2_nil", statusMsg: "bogus fail"},
				{name: "bogus_fail_reversed_msgs", statusMsg: "bogus fail", reverseMsg: true},
				{name: "partial_msg_clean_matched_pair", statusMsg: eodPartialBatchErrorMsg},
				{name: "both_explicit_clean_pair", statusMsg: "bogus fail", m2Explicit: true},
			} {
				m1 := makeSubTree(goProf, 901, 9010, "m1", "a1", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 1), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				m2 := makeSubTree(goProf, 902, 9020, "m2", "a2", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 1), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				st1 := makeSettle(goProf, 903, 9030, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
				st2 := makeSettle(goProf, 904, 9040, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m2[0])
				stBogus := makeSettle(goProf, 905, 9050, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, tc.statusMsg, m1[0], m2[0])
				e1 := SubscribeMessageExpectation{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome, Settlements: []SettlementExpectation{{Operation: op}}}
				e2 := SubscribeMessageExpectation{MessageID: "m2", AckID: "a2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome}
				if tc.m2Explicit {
					e2.Settlements = []SettlementExpectation{{Operation: op}}
				}
				msgs := []SubscribeMessageExpectation{e1, e2}
				if tc.reverseMsg {
					msgs = []SubscribeMessageExpectation{e2, e1}
				}
				res := Verify(goCaps, append(append(m1, m2...), st1, st2, stBogus), &TraceExpectation{
					ProjectID:             proj,
					Subscription:          fullSub,
					AllowExtraPubSubSpans: true,
					Subscribe:             &SubscribeExpectation{Messages: msgs},
				})
				assertHasViolation(t, res, CodeWrongStatusCode, "settlement."+op+".status")
			}

			// 2b. Attack_T4_AllowUnmatchedExplicitEOD_CleanExtraNoSiblingOK ("bogus fail" and eodPartialBatchErrorMsg).
			for _, statusMsg := range []string{"bogus fail", eodPartialBatchErrorMsg} {
				m1 := makeSubTree(goProf, 911, 9110, "m1", "a1", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 1), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				extra := makeSubTree(goProf, 912, 9120, "m2", "a2", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 1), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				stM1 := makeSettle(goProf, 913, 9130, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
				stErr := makeSettle(goProf, 914, 9140, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, statusMsg, m1[0], extra[0])
				res := Verify(goCaps, append(append(m1, extra...), stM1, stErr), &TraceExpectation{
					ProjectID:             proj,
					Subscription:          fullSub,
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome, Settlements: []SettlementExpectation{{Operation: op}}}},
					},
				})
				assertHasViolation(t, res, CodeWrongStatusCode, "settlement."+op+".status")
			}

			// 2c. Attack_T4_ExplicitM1_EventsNilExtraSub_NonPartialMsg -> FAIL; with eodPartialBatchErrorMsg -> PASS.
			for _, wantPass := range []bool{false, true} {
				m1 := makeSubTree(goProf, 921, 9210, "m1", "a1", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 1), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				extra := makeSubTree(goProf, 922, 9220, "m2", "a2", true, []*proxypb.SpanEvent{ev(called, 20)}, nil, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				stM1 := makeSettle(goProf, 923, 9230, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1[0])
				msg := "bogus fail"
				if wantPass {
					msg = eodPartialBatchErrorMsg
				}
				stErr := makeSettle(goProf, 924, 9240, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, msg, m1[0], extra[0])
				extra[0].Events = nil
				res := Verify(goCaps, append(append(m1, extra...), stM1, stErr), &TraceExpectation{
					ProjectID:             proj,
					Subscription:          fullSub,
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome, Settlements: []SettlementExpectation{{Operation: op}}}},
					},
				})
				if wantPass {
					assertPass(t, res)
				} else {
					assertHasViolation(t, res, CodeWrongStatusCode, "settlement."+op+".status")
				}
			}

			// 2d. Attack_T4_ExplicitM1_OversizedBC2_SingleLink_NonPartialMsg -> FAIL.
			m1BC := makeSubTree(goProf, 931, 9310, "m1", "a1", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 1), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stM1BC := makeSettle(goProf, 932, 9320, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1BC[0])
			stErrBC := makeSettle(goProf, 933, 9330, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bogus fail", m1BC[0])
			assertHasViolation(t, Verify(goCaps, append(m1BC, stM1BC, stErrBC), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome, Settlements: []SettlementExpectation{{Operation: op}}}},
				},
			}), CodeWrongStatusCode, "settlement."+op+".status")

			// 2e. Positive guards: Single-message explicit EOD retry + TestWave17:9528 explicit variant -> PASS.
			m1Single := makeSubTree(goProf, 941, 9410, "m1", "a1", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 1), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stSingleOK := makeSettle(goProf, 942, 9420, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1Single[0])
			stSingleErr := makeSettle(goProf, 943, 9430, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "unavailable", m1Single[0])
			assertPass(t, Verify(goCaps, append(m1Single, stSingleOK, stSingleErr), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome, Settlements: []SettlementExpectation{{Operation: op}}}},
				},
			}))

			m1W17 := makeSubTree(goProf, 951, 9510, "m1", "a1", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 30), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			extraW17 := makeSubTree(goProf, 952, 9520, "m2", "a2", true, []*proxypb.SpanEvent{ev(called, 20)}, []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 38)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stM1W17 := makeSettle(goProf, 953, 9530, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", m1W17[0])
			stErrW17 := makeSettle(goProf, 954, 9540, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, m1W17[0], extraW17[0])
			m1W17[0].Events = []*proxypb.SpanEvent{evWithBC(op+" start", 22, 2), ev(op+" end", 30), ev(op+" end", 38)}
			assertPass(t, Verify(goCaps, append(append(m1W17, extraW17...), stM1W17, stErrW17), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome, Settlements: []SettlementExpectation{{Operation: op}}}},
				},
			}))
		}
	})

	// 3. Target 2 Fix: modack-only Settlements MUST enforce terminal settlement events on subscribe span.
	t.Run("Target2_ModackOnly_TerminalEventsEnforcement", func(t *testing.T) {
		// 3a. Go eod_failed with modack-only + zero terminal events -> FAIL subscribe.event.settlement_events.
		for _, withModackEv := range []bool{false, true} {
			g0 := makeSubTree(goProf, 961, 9610, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, nil, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			if withModackEv {
				g0[0].Events = []*proxypb.SpanEvent{ev("modack start", 18), ev("modack end", 19)}
			}
			mod0 := makeModack(goProf, 962, 9620, 60, true, g0[0])
			res0 := Verify(goCaps, append(g0, mod0), &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m1",
						AckID:           "a1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "eod_failed",
						Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true)}},
					}},
				},
			})
			assertHasViolation(t, res0, CodeMissingEvent, "subscribe.event.settlement_events")
		}

		// 3b. C++ eod_failed with modack-only + zero terminal events -> FAIL subscribe.event.settlement_events.
		c0 := makeSubTree(cppProf, 963, 9630, "m1", "a1", true, nil, nil, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
		modC0 := makeModack(cppProf, 964, 9640, 60, false, c0[0])
		resC0 := Verify(cppCaps, append(c0, modC0), &TraceExpectation{
			ProjectID:    proj,
			Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m1",
					AckID:           "a1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}},
				}},
			},
		})
		assertHasViolation(t, resC0, CodeMissingEvent, "subscribe.event.settlement_events")

		// 3c. Go modack-only eod_failed with partial ack start (missing ack end) and result="acked" only (no events).
		gPart := makeSubTree(goProf, 965, 9650, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("modack start", 18, 1), ev("modack end", 19), evWithBC("ack start", 22, 1)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
		modPart := makeModack(goProf, 966, 9660, 60, true, gPart[0])
		assertHasViolation(t, Verify(goCaps, append(gPart, modPart), &TraceExpectation{
			ProjectID:    proj,
			Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m1",
					AckID:           "a1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true)}},
				}},
			},
		}), CodeMissingEvent, "subscribe.event.ack_end")

		gAttrOnly := makeSubTree(goProf, 967, 9670, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("modack start", 18, 1), ev("modack end", 19)}, "acked", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
		modAttr := makeModack(goProf, 968, 9680, 60, true, gAttrOnly[0])
		assertHasViolation(t, Verify(goCaps, append(gAttrOnly, modAttr), &TraceExpectation{
			ProjectID:    proj,
			Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m1",
					AckID:           "a1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true)}},
				}},
			},
		}), CodeMissingEvent, "subscribe.event.ack_start")

		// 3d. Go & C++ modack-only eod_failed with linked ack/nack ERROR span + missing subSpan terminal events.
		for _, op := range []string{"ack", "nack"} {
			gL := makeSubTree(goProf, 970, 9700, "m1", "a1", true, []*proxypb.SpanEvent{ev(op+" called", 20)}, nil, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			modL := makeModack(goProf, 971, 9710, 60, true, gL[0])
			stL := makeSettle(goProf, 972, 9720, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "err", gL[0])
			gL[0].Events = []*proxypb.SpanEvent{evWithBC("modack start", 18, 1), ev("modack end", 19)} // keep modack, strip terminal events
			resGo3d := Verify(goCaps, append(gL, modL, stL), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m1",
						AckID:           "a1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "eod_failed",
						Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true)}},
					}},
				},
			})
			assertHasViolation(t, resGo3d, CodeMissingEvent, "subscribe.event."+op+"_start")
			for _, f := range resGo3d.Failures {
				if f.Rule == "subscribe.settlement.link_incompatible_op" || f.Rule == "subscribe.settlement.dangling_reverse_link" {
					t.Fatalf("3d Go unexpected Bug 3 violation: %+v", f)
				}
			}

			cL := makeSubTree(cppProf, 973, 9730, "m1", "a1", true, nil, []*proxypb.SpanEvent{ev("gl-cpp.modack_start", 18), ev("gl-cpp.modack_end", 19)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			modCL := makeModack(cppProf, 974, 9740, 60, false, cL[0])
			stCL := makeSettle(cppProf, 975, 9750, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "err", cL[0])
			resCpp3d := Verify(cppCaps, append(cL, modCL, stCL), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m1",
						AckID:           "a1",
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "eod_failed",
						Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}},
					}},
				},
			})
			assertHasViolation(t, resCpp3d, CodeMissingEvent, "subscribe.event.cpp_"+op+"_start")
			for _, f := range resCpp3d.Failures {
				if f.Rule == "subscribe.settlement.link_incompatible_op" || f.Rule == "subscribe.settlement.dangling_reverse_link" {
					t.Fatalf("3d C++ unexpected Bug 3 violation: %+v", f)
				}
			}
		}

		// 3e. modack-only with ExpectedOutcome="acked" / "nacked" missing terminal events on subSpan -> FAIL.
		for _, op := range []string{"ack", "nack"} {
			outcome := op + "ed"
			gOut := makeSubTree(goProf, 980, 9800, "m1", "a1", false, []*proxypb.SpanEvent{ev(op+" called", 20)}, []*proxypb.SpanEvent{evWithBC("modack start", 18, 1), ev("modack end", 19)}, outcome, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			modOut := makeModack(goProf, 981, 9810, 60, true, gOut[0])
			assertHasViolation(t, Verify(goCaps, append(gOut, modOut), &TraceExpectation{
				ProjectID:    proj,
				Subscription: fullSub,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       "m1",
						AckID:           "a1",
						ExactlyOnce:     BoolPtr(false),
						ExpectedOutcome: outcome,
						Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true)}},
					}},
				},
			}), CodeMissingEvent, "subscribe.event."+op+"_start")
		}

		// 3f. Positive contrasts: modack-only eod_failed with valid subSpan terminal events PASSES (Go & C++).
		gPos := makeSubTree(goProf, 985, 9850, "m1", "a1", true, []*proxypb.SpanEvent{ev("ack called", 20)}, []*proxypb.SpanEvent{evWithBC("modack start", 18, 1), ev("modack end", 19), evWithBC("ack start", 22, 1), ev("ack end", 38), ev("exception", 39)}, "", proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
		modPos := makeModack(goProf, 986, 9860, 60, true, gPos[0])
		assertPass(t, Verify(goCaps, append(gPos, modPos), &TraceExpectation{
			ProjectID:    proj,
			Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m1",
					AckID:           "a1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60, IsReceiptModack: BoolPtr(true)}},
				}},
			},
		}))

		cPos := makeSubTree(cppProf, 987, 9870, "m1", "a1", true, nil, []*proxypb.SpanEvent{ev("gl-cpp.modack_start", 18), ev("gl-cpp.modack_end", 19), ev("gl-cpp.ack_start", 22), ev("gl-cpp.ack_end", 38)}, "ack", proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
		modCPos := makeModack(cppProf, 988, 9880, 60, false, cPos[0])
		assertPass(t, Verify(cppCaps, append(cPos, modCPos), &TraceExpectation{
			ProjectID:    proj,
			Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{
					MessageID:       "m1",
					AckID:           "a1",
					ExactlyOnce:     BoolPtr(true),
					ExpectedOutcome: "eod_failed",
					Settlements:     []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}},
				}},
			},
		}))
	})

	// 4. Finding 2 Fix: C++ hasNonErrForSub[subSpan] MUST only promote to anyRetrySuccessPartner when !isMultiMsgBatch.
	t.Run("Target4_Cpp_HasNonErrForSub_MultiMsgGuards", func(t *testing.T) {
		for _, op := range []string{"ack", "nack"} {
			outcome := op + "ed"
			cppEv := func() []*proxypb.SpanEvent {
				return []*proxypb.SpanEvent{ev("gl-cpp."+op+"_start", 22), ev("gl-cpp."+op+"_end", 38)}
			}

			// 4a. Clean matched pairs (explicit+nil, nil+nil, explicit+explicit, reversed Messages, eodPartialBatchErrorMsg, bc=nil/bc=2, SubSpan Events=nil).
			for _, tc := range []struct {
				name       string
				statusMsg  string
				bc         *int64
				m1Explicit bool
				m2Explicit bool
				reverseMsg bool
				nilEvents  bool
			}{
				{name: "Attack_W21_T4_Cpp_MatchedPair_CleanNoRetry_BogusFail", statusMsg: "bogus fail", m1Explicit: true},
				{name: "Attack_W21_T4_Cpp_MatchedPair_CleanNoRetry_BogusFail_bc2", statusMsg: "bogus fail", bc: &bc2, m1Explicit: true},
				{name: "Attack_W21_T4_Cpp_BothNil_CleanNoRetry_BogusFail", statusMsg: "bogus fail"},
				{name: "Attack_W21_T4_Cpp_BothNil_CleanNoRetry_BogusFail_bc2", statusMsg: "bogus fail", bc: &bc2},
				{name: "explicit_plus_explicit", statusMsg: "bogus fail", m1Explicit: true, m2Explicit: true},
				{name: "reversed_messages", statusMsg: "bogus fail", m1Explicit: true, reverseMsg: true},
				{name: "partial_msg_explicit_nil", statusMsg: eodPartialBatchErrorMsg, m1Explicit: true},
				{name: "partial_msg_nil_nil", statusMsg: eodPartialBatchErrorMsg},
				{name: "subspan_events_nil", statusMsg: "bogus fail", m1Explicit: true, m2Explicit: true, nilEvents: true},
			} {
				ev1, ev2 := cppEv(), cppEv()
				if tc.nilEvents {
					ev1, ev2 = nil, nil
				}
				m1 := makeSubTree(cppProf, 1901, 19010, "m1", "a1", true, nil, ev1, op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				m2 := makeSubTree(cppProf, 1902, 19020, "m2", "a2", true, nil, ev2, op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				st1 := makeSettle(cppProf, 1903, 19030, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m1[0])
				st2 := makeSettle(cppProf, 1904, 19040, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m2[0])
				cbog := makeSettle(cppProf, 1905, 19050, op, tc.bc, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, tc.statusMsg, m1[0], m2[0])
				e1 := SubscribeMessageExpectation{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome}
				if tc.m1Explicit {
					e1.Settlements = []SettlementExpectation{{Operation: op}}
				}
				e2 := SubscribeMessageExpectation{MessageID: "m2", AckID: "a2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome}
				if tc.m2Explicit {
					e2.Settlements = []SettlementExpectation{{Operation: op}}
				}
				msgs := []SubscribeMessageExpectation{e1, e2}
				if tc.reverseMsg {
					msgs = []SubscribeMessageExpectation{e2, e1}
				}
				res := Verify(cppCaps, append(append(m1, m2...), st1, st2, cbog), &TraceExpectation{
					ProjectID:             proj,
					Subscription:          fullSub,
					AllowExtraPubSubSpans: true,
					Subscribe:             &SubscribeExpectation{Messages: msgs},
				})
				assertHasViolation(t, res, CodeWrongStatusCode, "settlement."+op+".status")
			}

			// 4b. wasMatched == true variant: m1 explicit without st1(OK), m2 nil WITH st2(OK), cbog matched to m1.
			m1W := makeSubTree(cppProf, 1911, 19110, "m1", "a1", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			m2W := makeSubTree(cppProf, 1912, 19120, "m2", "a2", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			st2W := makeSettle(cppProf, 1913, 19130, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m2W[0])
			cbogW := makeSettle(cppProf, 1914, 19140, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bogus fail", m1W[0], m2W[0])
			resW := Verify(cppCaps, append(append(m1W, m2W...), st2W, cbogW), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{
						{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome, Settlements: []SettlementExpectation{{Operation: op}}},
						{MessageID: "m2", AckID: "a2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome},
					},
				},
			})
			assertHasViolation(t, resW, CodeWrongStatusCode, "settlement."+op+".status")

			// 4c. Oversized bc=2 single-link "bogus fail" (+ exception) under ProfileCpp (explicit and nil) -> FAIL.
			for _, explicitM1 := range []bool{true, false} {
				m1BC := makeSubTree(cppProf, 1921, 19210, "m1", "a1", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				st1BC := makeSettle(cppProf, 1922, 19220, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m1BC[0])
				cbogBC := makeSettle(cppProf, 1923, 19230, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bogus fail", m1BC[0])
				cbogBC.Events = []*proxypb.SpanEvent{ev("exception", 35)}
				e1 := SubscribeMessageExpectation{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome}
				if explicitM1 {
					e1.Settlements = []SettlementExpectation{{Operation: op}}
				}
				assertHasViolation(t, Verify(cppCaps, append(m1BC, st1BC, cbogBC), &TraceExpectation{
					ProjectID:             proj,
					Subscription:          fullSub,
					AllowExtraPubSubSpans: true,
					Subscribe:             &SubscribeExpectation{Messages: []SubscribeMessageExpectation{e1}},
				}), CodeWrongStatusCode, "settlement."+op+".status")
			}

			// 4d. extraSub combinations under ProfileCpp -> FAIL settlement.<op>.status.
			// (i) m1 (st1 OK) + extraSub (no st2, start+end) + cbog("bogus fail") -> FAIL.
			m1E1 := makeSubTree(cppProf, 1931, 19310, "m1", "a1", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			ex1 := makeSubTree(cppProf, 1932, 19320, "m2", "a2", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			st1E1 := makeSettle(cppProf, 1933, 19330, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m1E1[0])
			cbogE1 := makeSettle(cppProf, 1934, 19340, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bogus fail", m1E1[0], ex1[0])
			assertHasViolation(t, Verify(cppCaps, append(append(m1E1, ex1...), st1E1, cbogE1), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome}},
				},
			}), CodeWrongStatusCode, "settlement."+op+".status")

			// (ii) m1 (st1 OK) + extraSub WITH st2(OK) + cbog(eodPartialBatchErrorMsg) -> FAIL.
			m1E2 := makeSubTree(cppProf, 1941, 19410, "m1", "a1", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			ex2 := makeSubTree(cppProf, 1942, 19420, "m2", "a2", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			st1E2 := makeSettle(cppProf, 1943, 19430, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m1E2[0])
			st2E2 := makeSettle(cppProf, 1944, 19440, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", ex2[0])
			cbogE2 := makeSettle(cppProf, 1945, 19450, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, m1E2[0], ex2[0])
			assertHasViolation(t, Verify(cppCaps, append(append(m1E2, ex2...), st1E2, st2E2, cbogE2), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome}},
				},
			}), CodeWrongStatusCode, "settlement."+op+".status")

			// 4e. Positive single-message explicit C++ EOD retry guards (bc=nil and bc=&bc1) + TestWave17:9602 extraSub -> PASS.
			for _, bcOpt := range []*int64{nil, &bc1} {
				m1Pos := makeSubTree(cppProf, 1951, 19510, "m1", "a1", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
				stPosOK := makeSettle(cppProf, 1952, 19520, op, bcOpt, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", m1Pos[0])
				stPosErr := makeSettle(cppProf, 1953, 19530, op, bcOpt, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "unavailable", m1Pos[0])
				assertPass(t, Verify(cppCaps, append(m1Pos, stPosOK, stPosErr), &TraceExpectation{
					ProjectID:             proj,
					Subscription:          fullSub,
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome, Settlements: []SettlementExpectation{{Operation: op}}}},
					},
				}))
			}

			// Unverified extraSub without st2 + eodPartialBatchErrorMsg stays PASS (TestWave17:9602 guard).
			m1W17C := makeSubTree(cppProf, 1961, 19610, "m1", "a1", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			exW17C := makeSubTree(cppProf, 1962, 19620, "m2", "a2", true, nil, cppEv(), op, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET)
			stW17C := makeSettle(cppProf, 1963, 19630, op, nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, m1W17C[0], exW17C[0])
			assertPass(t, Verify(cppCaps, append(append(m1W17C, exW17C...), stW17C), &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: outcome}},
				},
			}))
		}
	})
}

func TestPhase46_AllowExtra_StatusInversionAttacks(t *testing.T) {
	t.Parallel()

	goProf := ProfileGoCanonical()
	goCaps := &proxypb.GetCapabilitiesResponse{
		SupportsMonitoring:          true,
		ClientLanguage:              "go",
		ClientLibrary:               "cloud.google.com/go/pubsub/v2",
		SupportsExactlyOnceDelivery: true,
	}

	const (
		proj    = "proj-p46"
		subID   = "sub-p46"
		fullSub = "projects/proj-p46/subscriptions/sub-p46"
	)

	intAV := func(v int64) *proxypb.AttributeValue {
		return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
	}
	baseT0 := time.Date(2026, 11, 16, 6, 10, 0, 0, time.UTC)
	mk0Link := func(seq int, op string, bc int64, status proxypb.SpanStatusCode, msg string, withExc bool) *proxypb.TraceSpan {
		attrs := map[string]*proxypb.AttributeValue{
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV(subID),
			"gcp.project_id":                strAV(proj),
			"gcp.resource.name":             strAV(fullSub),
			"messaging.batch.message_count": intAV(bc),
		}
		if op == "ack" {
			attrs["code.function"] = strAV("sendAck")
		} else {
			attrs["code.function"] = strAV("sendModAck")
		}
		var evs []*proxypb.SpanEvent
		if withExc {
			evs = []*proxypb.SpanEvent{{Name: "exception", Timestamp: timestamppb.New(baseT0.Add(time.Duration(seq)*time.Millisecond + time.Microsecond))}}
		}
		return &proxypb.TraceSpan{
			TraceId:                  fmt.Sprintf("%032x", 1000+seq),
			SpanId:                   fmt.Sprintf("%016x", 10000+seq),
			TraceFlags:               1,
			Name:                     subID + " " + op,
			Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
			StartTime:                timestamppb.New(baseT0.Add(time.Duration(seq) * time.Millisecond)),
			EndTime:                  timestamppb.New(baseT0.Add(time.Duration(seq)*time.Millisecond + 5*time.Millisecond)),
			InstrumentationScopeName: goProf.ExpectedScope,
			Attributes:               attrs,
			Events:                   evs,
			StatusCode:               status,
			StatusMessage:            msg,
		}
	}
	dropMsg := func(idx int, outcome string, eod bool) SubscribeMessageExpectation {
		return SubscribeMessageExpectation{
			MessageID:              fmt.Sprintf("d-%d", idx),
			AckID:                  fmt.Sprintf("da-%d", idx),
			ExpectDroppedUnsampled: true,
			WireTraceparent:        fmt.Sprintf("00-%032x-%016x-00", 5000+idx, 50000+idx),
			ExactlyOnce:            BoolPtr(eod),
			ExpectedOutcome:        outcome,
		}
	}

	cases := []struct {
		name     string
		msgs     []SubscribeMessageExpectation
		spans    []*proxypb.TraceSpan
		wantPass bool
		wantCode ViolationCode
		wantRule string
	}{
		{
			name:     "Attack_P46_AllowExtra_ErrorOnly",
			msgs:     []SubscribeMessageExpectation{dropMsg(0, "acked", true)},
			spans:    []*proxypb.TraceSpan{mk0Link(1, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "perm fail", true)},
			wantPass: false, wantCode: CodeWrongStatusCode, wantRule: "settlement.ack.status",
		},
		{
			name:     "Attack_P46_AllowExtra_EODFailed_UnsetOnly",
			msgs:     []SubscribeMessageExpectation{dropMsg(0, "eod_failed", true)},
			spans:    []*proxypb.TraceSpan{mk0Link(2, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)},
			wantPass: false, wantCode: CodeWrongStatusCode, wantRule: "settlement.ack.status",
		},
		{
			name:     "Attack_P46_AllowExtra_ErrorOnly_PartialBatchMsg_UnsetOutcome",
			msgs:     []SubscribeMessageExpectation{dropMsg(0, "acked", true)},
			spans:    []*proxypb.TraceSpan{mk0Link(3, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, false)},
			wantPass: false, wantCode: CodeWrongStatusCode, wantRule: "settlement.ack.status",
		},
		{
			name:     "Attack_P46_AllowExtra_Nack_ErrorOnly",
			msgs:     []SubscribeMessageExpectation{dropMsg(0, "nacked", true)},
			spans:    []*proxypb.TraceSpan{mk0Link(4, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "nack fail", true)},
			wantPass: false, wantCode: CodeWrongStatusCode, wantRule: "settlement.nack.status",
		},
		{
			name: "Live_Drop_TransientRetry_00_ViaReactor",
			msgs: []SubscribeMessageExpectation{dropMsg(0, "acked", true)},
			spans: []*proxypb.TraceSpan{
				mk0Link(5, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "transient", true),
				mk0Link(6, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
			},
			wantPass: true,
		},
		{
			name: "MultiDrop_MixedOutcomes_WithExtraError",
			msgs: []SubscribeMessageExpectation{dropMsg(0, "acked", true), dropMsg(1, "eod_failed", true)},
			spans: []*proxypb.TraceSpan{
				mk0Link(7, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mk0Link(8, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "eod err", true),
				mk0Link(9, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "extra err", true),
			},
			wantPass: true,
		},
		{
			name: "MultiDrop_MixedOutcomes_BothUnset",
			msgs: []SubscribeMessageExpectation{dropMsg(0, "acked", true), dropMsg(1, "eod_failed", true)},
			spans: []*proxypb.TraceSpan{
				mk0Link(10, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mk0Link(11, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
			},
			wantPass: false, wantCode: CodeWrongStatusCode, wantRule: "settlement.ack.status",
		},
		{
			name:     "MultiDrop_PartialBatch_SharedBC2_Pass",
			msgs:     []SubscribeMessageExpectation{dropMsg(0, "acked", true), dropMsg(1, "eod_failed", true)},
			spans:    []*proxypb.TraceSpan{mk0Link(12, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, false)},
			wantPass: true,
		},
		{
			name:     "MultiDrop_PartialBatch_SharedBC2_UnsetFail",
			msgs:     []SubscribeMessageExpectation{dropMsg(0, "acked", true), dropMsg(1, "eod_failed", true)},
			spans:    []*proxypb.TraceSpan{mk0Link(13, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)},
			wantPass: false, wantCode: CodeWrongStatusCode, wantRule: "settlement.ack.status",
		},
		{
			name:     "ZeroSpans_AllowExtra_FailMissingSpan",
			msgs:     []SubscribeMessageExpectation{dropMsg(0, "acked", true), dropMsg(1, "eod_failed", true)},
			spans:    nil,
			wantPass: false, wantCode: CodeMissingSpan, wantRule: "subscribe.settlement.missing_span",
		},
		{
			name: "MultiDrop_Pass2_AckBudgetExhausted_ExtraAckErrIgnored",
			msgs: []SubscribeMessageExpectation{dropMsg(0, "acked", true), dropMsg(1, "nacked", true)},
			spans: []*proxypb.TraceSpan{
				mk0Link(14, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mk0Link(15, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bg ack err", true),
				mk0Link(150, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
			},
			wantPass: true,
		},
		{
			name: "MultiDrop_Pass2_AckBudgetExhausted_NoNackFailsMissingSpan",
			msgs: []SubscribeMessageExpectation{dropMsg(0, "acked", true), dropMsg(1, "nacked", true)},
			spans: []*proxypb.TraceSpan{
				mk0Link(151, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mk0Link(152, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bg ack err", true),
			},
			wantPass: false, wantCode: CodeMissingSpan, wantRule: "subscribe.settlement.missing_span",
		},
		{
			name: "MultiDrop_Pass2_NackBudgetExhausted_ExtraNackErrIgnored",
			msgs: []SubscribeMessageExpectation{dropMsg(0, "nacked", true), dropMsg(1, "acked", true)},
			spans: []*proxypb.TraceSpan{
				mk0Link(16, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mk0Link(17, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bg nack err", true),
				mk0Link(170, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
			},
			wantPass: true,
		},
		{
			name: "MultiDrop_Pass2_NackBudgetExhausted_NoAckFailsMissingSpan",
			msgs: []SubscribeMessageExpectation{dropMsg(0, "nacked", true), dropMsg(1, "acked", true)},
			spans: []*proxypb.TraceSpan{
				mk0Link(171, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mk0Link(172, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bg nack err", true),
			},
			wantPass: false, wantCode: CodeMissingSpan, wantRule: "subscribe.settlement.missing_span",
		},
		{
			name:     "MultiDrop_Pass2_NoLeftoverCands_CapacityExhausted",
			msgs:     []SubscribeMessageExpectation{dropMsg(0, "acked", true), dropMsg(1, "acked", true)},
			spans:    []*proxypb.TraceSpan{mk0Link(18, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)},
			wantPass: false, wantCode: CodeMissingSpan, wantRule: "subscribe.settlement.missing_span",
		},
		{
			name: "MultiDrop_Heterogeneous_WildcardAndExplicit_EndToEnd",
			msgs: []SubscribeMessageExpectation{dropMsg(0, "", true), dropMsg(1, "acked", true), dropMsg(2, "nacked", true)},
			spans: []*proxypb.TraceSpan{
				mk0Link(19, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mk0Link(20, "nack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mk0Link(21, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
			},
			wantPass: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			revSpans := cloneSpans(tc.spans)
			slices.Reverse(revSpans)
			for _, ordSpans := range [][]*proxypb.TraceSpan{cloneSpans(tc.spans), revSpans} {
				res := Verify(goCaps, ordSpans, &TraceExpectation{
					ProjectID:             proj,
					Subscription:          fullSub,
					AllowExtraPubSubSpans: true,
					Subscribe:             &SubscribeExpectation{Messages: tc.msgs},
				})
				if tc.wantPass {
					if !res.Passed() {
						t.Fatalf("%s expected PASS, got:\n%s", tc.name, res.Format())
					}
				} else {
					assertHasViolation(t, res, tc.wantCode, tc.wantRule)
				}
			}
		})
	}
}

func TestPhase46_Wave22_CoalescedAndZeroBC_Regressions(t *testing.T) {
	t.Parallel()

	goProf := ProfileGoCanonical()
	cppProf := ProfileCpp()
	goCaps := &proxypb.GetCapabilitiesResponse{
		SupportsMonitoring:          true,
		ClientLanguage:              "go",
		ClientLibrary:               "cloud.google.com/go/pubsub/v2",
		SupportsExactlyOnceDelivery: true,
	}
	cppCaps := &proxypb.GetCapabilitiesResponse{
		SupportsMonitoring:          true,
		ClientLanguage:              "cpp",
		ClientLibrary:               "google-cloud-cpp",
		SupportsExactlyOnceDelivery: true,
	}

	const (
		proj    = "proj-w22-p46"
		subID   = "sub-w22-p46"
		fullSub = "projects/proj-w22-p46/subscriptions/sub-w22-p46"
	)

	intAV := func(v int64) *proxypb.AttributeValue {
		return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
	}
	baseT0 := time.Date(2026, 11, 16, 7, 30, 0, 0, time.UTC)
	mkSpan := func(prof Profile, seq int, op string, bc *int64, status proxypb.SpanStatusCode, msg string, withExc bool, targets ...*proxypb.TraceSpan) *proxypb.TraceSpan {
		tr := fmt.Sprintf("%032x", 20000+seq)
		spID := fmt.Sprintf("%016x", 200000+seq)
		codeKey := prof.CodeFunctionAttrKey
		if codeKey == "" {
			codeKey = "code.function"
		}
		fn := "sendAck"
		if op == "nack" {
			fn = "sendModAck"
		}
		attrs := map[string]*proxypb.AttributeValue{
			"messaging.system":           strAV("gcp_pubsub"),
			"messaging.destination.name": strAV(subID),
			"gcp.project_id":             strAV(proj),
			"messaging.operation.name":   strAV(op),
			codeKey:                      strAV(fn),
		}
		if prof.RequireGCPResourceName {
			attrs["gcp.resource.name"] = strAV(fullSub)
		}
		if prof.Name == cppProf.Name {
			attrs[codeKey] = strAV("pubsub::AckHandler::" + op)
			attrs["messaging.operation.type"] = strAV("settle")
			attrs["messaging.gcp_pubsub.subscription.template"] = strAV(fullSub)
			attrs["messaging.gcp_pubsub.message.ack_id"] = strAV("da-cpp")
			stCode := "OK"
			if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
				stCode = "INTERNAL"
			}
			attrs["gl-cpp.status_code"] = strAV(stCode)
		}
		if bc != nil {
			attrs["messaging.batch.message_count"] = intAV(*bc)
		}
		var links []*proxypb.SpanLink
		for _, tgt := range targets {
			links = append(links, &proxypb.SpanLink{TraceId: tgt.GetTraceId(), SpanId: tgt.GetSpanId()})
			if prof.RequireBidirectionalSubscribeLinks {
				tgt.Links = append(tgt.Links, &proxypb.SpanLink{
					TraceId:    tr,
					SpanId:     spID,
					Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV(op)},
				})
			}
		}
		var evs []*proxypb.SpanEvent
		if withExc {
			evs = []*proxypb.SpanEvent{{Name: "exception", Timestamp: timestamppb.New(baseT0.Add(time.Duration(seq) * time.Millisecond))}}
		}
		return &proxypb.TraceSpan{
			TraceId: tr, SpanId: spID, TraceFlags: 1, Name: subID + " " + op,
			Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
			StartTime:                timestamppb.New(baseT0.Add(time.Duration(seq) * time.Millisecond)),
			EndTime:                  timestamppb.New(baseT0.Add(time.Duration(seq)*time.Millisecond + time.Millisecond)),
			InstrumentationScopeName: prof.ExpectedScope, Attributes: attrs,
			Links: links, Events: evs, StatusCode: status, StatusMessage: msg,
		}
	}
	dropExp := func(idx int, outcome string, settleOp string, settleErr bool) SubscribeMessageExpectation {
		m := SubscribeMessageExpectation{
			MessageID:              fmt.Sprintf("d22-%d", idx),
			AckID:                  fmt.Sprintf("da22-%d", idx),
			ExpectDroppedUnsampled: true,
			WireTraceparent:        fmt.Sprintf("00-%032x-%016x-00", 30000+idx, 300000+idx),
			ExactlyOnce:            BoolPtr(true),
			ExpectedOutcome:        outcome,
		}
		if settleOp != "" {
			m.Settlements = []SettlementExpectation{{Operation: settleOp, ExpectError: settleErr}}
		}
		return m
	}

	bc1 := int64(1)
	bc2 := int64(2)
	bc3 := int64(3)
	bc0 := int64(0)
	bcNeg := int64(-5)
	bcMax := int64(math.MaxInt64)

	// 1. Finding 1: Phase 3 coalesced 0-link spans expose remaining batchCnt - unsampledUsedCount capacity across ack/nack and forward/reversed spans+msgs.
	t.Run("Attack_W21_P46_Phase3_Coalesced_BC2_SkipsRemainingSlot", func(t *testing.T) {
		for _, op := range []string{"ack", "nack"} {
			outcome := op + "ed"
			baseMsgs := []SubscribeMessageExpectation{
				dropExp(0, outcome, op, false),
				dropExp(1, outcome, "", false),
			}
			revMsgs := slices.Clone(baseMsgs)
			slices.Reverse(revMsgs)

			baseSpans := []*proxypb.TraceSpan{
				mkSpan(goProf, 1, op, &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false),
				mkSpan(goProf, 2, op, &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "unrelated bg err", true),
			}
			revSpans := cloneSpans(baseSpans)
			slices.Reverse(revSpans)

			for _, msgs := range [][]SubscribeMessageExpectation{baseMsgs, revMsgs} {
				for _, spans := range [][]*proxypb.TraceSpan{cloneSpans(baseSpans), cloneSpans(revSpans)} {
					res := Verify(goCaps, spans, &TraceExpectation{
						ProjectID:             proj,
						Subscription:          fullSub,
						AllowExtraPubSubSpans: true,
						Subscribe:             &SubscribeExpectation{Messages: msgs},
					})
					if !res.Passed() {
						t.Fatalf("op=%s expected PASS, got:\n%s", op, res.Format())
					}
				}
			}
		}
	})

	t.Run("Phase3_Coalesced_BC2_StrictExtra_And_Exhausted_Negatives", func(t *testing.T) {
		// (a) AllowExtraPubSubSpans=false: explicit ack + nil eod_failed on UNSET bc=2 -> FAIL settlement.ack.status.
		spCoalA := mkSpan(goProf, 10, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
		resA := Verify(goCaps, []*proxypb.TraceSpan{spCoalA}, &TraceExpectation{
			ProjectID:    proj,
			Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					dropExp(0, "acked", "ack", false),
					dropExp(1, "eod_failed", "", false),
				},
			},
		})
		assertHasViolation(t, resA, CodeWrongStatusCode, "settlement.ack.status")

		// (b) bc=1 Phase-3-matched span has rem=0 and stays skipped -> Pass 2 picks spBgErr -> FAIL.
		spFullB := mkSpan(goProf, 11, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
		spBgErrB := mkSpan(goProf, 12, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bg err", true)
		resB := Verify(goCaps, []*proxypb.TraceSpan{spFullB, spBgErrB}, &TraceExpectation{
			ProjectID:             proj,
			Subscription:          fullSub,
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					dropExp(0, "acked", "ack", false),
					dropExp(1, "acked", "", false),
				},
			},
		})
		assertHasViolation(t, resB, CodeWrongStatusCode, "settlement.ack.status")

		// (c) ProfileCpp (!hasBatch) matched[sp] stays skipped -> Pass 2 picks spBgErr -> FAIL.
		spCppC := mkSpan(cppProf, 13, "ack", nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, "", false)
		spBgErrC := mkSpan(cppProf, 14, "ack", nil, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bg err", false)
		resC := Verify(cppCaps, []*proxypb.TraceSpan{spCppC, spBgErrC}, &TraceExpectation{
			ProjectID:             proj,
			Subscription:          fullSub,
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					dropExp(0, "acked", "ack", false),
					dropExp(1, "acked", "", false),
				},
			},
		})
		assertHasViolation(t, resC, CodeWrongStatusCode, "settlement.ack.status")

		// (d) AllowExtra=false positive bc=2 + bc=math.MaxInt64 (AllowExtra=true PASS without int overflow; AllowExtra=false FAIL batch_count).
		spPosD := mkSpan(goProf, 15, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
		resPosD := Verify(goCaps, []*proxypb.TraceSpan{spPosD}, &TraceExpectation{
			ProjectID:    proj,
			Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					dropExp(0, "acked", "ack", false),
					dropExp(1, "acked", "", false),
				},
			},
		})
		if !resPosD.Passed() {
			t.Fatalf("expected PASS, got:\n%s", resPosD.Format())
		}

		spMaxD := mkSpan(goProf, 16, "ack", &bcMax, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
		resMaxAllow := Verify(goCaps, []*proxypb.TraceSpan{spMaxD}, &TraceExpectation{
			ProjectID:             proj,
			Subscription:          fullSub,
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					dropExp(0, "acked", "ack", false),
					dropExp(1, "acked", "", false),
				},
			},
		})
		if !resMaxAllow.Passed() {
			t.Fatalf("expected PASS with bc=MaxInt64 under AllowExtra=true, got:\n%s", resMaxAllow.Format())
		}
		resMaxStrict := Verify(goCaps, []*proxypb.TraceSpan{spMaxD}, &TraceExpectation{
			ProjectID:    proj,
			Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					dropExp(0, "acked", "ack", false),
					dropExp(1, "acked", "", false),
				},
			},
		})
		assertHasViolation(t, resMaxStrict, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// (e) d0 explicit EODPartialBatchFailure=true + d1 nil "acked" sharing ERROR eodPartialBatchErrorMsg (no exception) -> PASS.
		spPartE := mkSpan(goProf, 17, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, eodPartialBatchErrorMsg, false)
		resE := Verify(goCaps, []*proxypb.TraceSpan{spPartE}, &TraceExpectation{
			ProjectID:             proj,
			Subscription:          fullSub,
			AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{
					{
						MessageID: "d22-0", AckID: "da22-0", ExpectDroppedUnsampled: true,
						WireTraceparent: fmt.Sprintf("00-%032x-%016x-00", 30000, 300000), ExactlyOnce: BoolPtr(true),
						Settlements: []SettlementExpectation{{Operation: "ack", ExpectError: true, EODPartialBatchFailure: true}},
					},
					dropExp(1, "acked", "", false),
				},
			},
		})
		if !resE.Passed() {
			t.Fatalf("expected PASS on EODPartialBatchFailure shared 0-link bc=2, got:\n%s", resE.Format())
		}

		// (f) Linked contrast guard: linked span s1(bc=2) stays skipped by Phase 4.6 so Phase 5 catches bc=2 > 1.
		sub1 := &proxypb.TraceSpan{
			TraceId: fmt.Sprintf("%032x", 20050), SpanId: fmt.Sprintf("%016x", 200500), TraceFlags: 1,
			Name: subID + " subscribe", Kind: proxypb.SpanKind_SPAN_KIND_INTERNAL,
			StartTime: timestamppb.New(baseT0), EndTime: timestamppb.New(baseT0.Add(10 * time.Millisecond)),
			InstrumentationScopeName: goProf.ExpectedScope,
			Attributes: map[string]*proxypb.AttributeValue{
				"messaging.system":                           strAV("gcp_pubsub"),
				"messaging.destination.name":                 strAV(subID),
				"gcp.project_id":                             strAV(proj),
				"gcp.resource.name":                          strAV(fullSub),
				"messaging.message.id":                       strAV("m-linked"),
				"messaging.gcp_pubsub.message.ack_id":        strAV("a-linked"),
				"messaging.message.body.size":                intAV(10),
				"messaging.batch.message_count":              intAV(1),
				"messaging.gcp_pubsub.message.ordering_key":  strAV(""),
				"messaging.operation.name":                   strAV("receive"),
				"code.function":                              strAV("receive"),
				"messaging.gcp_pubsub.exactly_once_delivery": {Value: &proxypb.AttributeValue_BoolValue{BoolValue: true}},
				"messaging.gcp_pubsub.result":                strAV("acked"),
			},
			Events: []*proxypb.SpanEvent{
				{Name: "ack start", Timestamp: timestamppb.New(baseT0.Add(time.Millisecond)), Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
				{Name: "ack end", Timestamp: timestamppb.New(baseT0.Add(2 * time.Millisecond))},
			},
		}
		cc1 := &proxypb.TraceSpan{TraceId: sub1.TraceId, SpanId: fmt.Sprintf("%016x", 200501), ParentSpanId: sub1.SpanId, TraceFlags: 1, Name: "subscriber concurrency control", Kind: proxypb.SpanKind_SPAN_KIND_INTERNAL, StartTime: timestamppb.New(baseT0), EndTime: timestamppb.New(baseT0.Add(9 * time.Millisecond)), InstrumentationScopeName: goProf.ExpectedScope, Attributes: map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub"), "messaging.destination.name": strAV(subID), "gcp.project_id": strAV(proj), "code.function": strAV("receive")}}
		sc1 := &proxypb.TraceSpan{TraceId: sub1.TraceId, SpanId: fmt.Sprintf("%016x", 200502), ParentSpanId: sub1.SpanId, TraceFlags: 1, Name: "subscribe scheduler", Kind: proxypb.SpanKind_SPAN_KIND_INTERNAL, StartTime: timestamppb.New(baseT0), EndTime: timestamppb.New(baseT0.Add(9 * time.Millisecond)), InstrumentationScopeName: goProf.ExpectedScope, Attributes: cc1.Attributes}
		pr1 := &proxypb.TraceSpan{TraceId: sub1.TraceId, SpanId: fmt.Sprintf("%016x", 200503), ParentSpanId: sub1.SpanId, TraceFlags: 1, Name: subID + " process", Kind: proxypb.SpanKind_SPAN_KIND_CONSUMER, StartTime: timestamppb.New(baseT0), EndTime: timestamppb.New(baseT0.Add(9 * time.Millisecond)), InstrumentationScopeName: goProf.ExpectedScope, Attributes: map[string]*proxypb.AttributeValue{"messaging.system": strAV("gcp_pubsub"), "messaging.destination.name": strAV(subID), "gcp.project_id": strAV(proj), "gcp.resource.name": strAV(fullSub), "messaging.message.id": strAV("m-linked"), "messaging.message.body.size": intAV(10), "messaging.gcp_pubsub.message.ordering_key": strAV(""), "messaging.operation.name": strAV("process"), "messaging.operation.type": strAV("process"), "code.function": strAV("receive")}, Events: []*proxypb.SpanEvent{{Name: "ack called", Timestamp: timestamppb.New(baseT0.Add(time.Millisecond))}}}
		stLinked := mkSpan(goProf, 51, "ack", &bc2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false, sub1)
		stUnlinked := mkSpan(goProf, 52, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
		resLinked := Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, stLinked, stUnlinked}, &TraceExpectation{
			ProjectID: proj, Subscription: fullSub,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
				{MessageID: "m-linked", AckID: "a-linked", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
				dropExp(1, "acked", "", false),
			}},
		})
		assertHasViolation(t, resLinked, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

		// (g) Phase 3 bc=3 used 1 + Pass 1 + Pass 2 uses remaining 2 -> FAIL settlement.ack.status.
		spBC3 := mkSpan(goProf, 60, "ack", &bc3, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
		resBC3 := Verify(goCaps, []*proxypb.TraceSpan{spBC3}, &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
				dropExp(0, "acked", "ack", false),
				dropExp(1, "acked", "", false),
				dropExp(2, "eod_failed", "", false),
			}},
		})
		assertHasViolation(t, resBC3, CodeWrongStatusCode, "settlement.ack.status")
	})

	// 2. Finding 4: bc <= 0 under !strictExtra caught (attribute & status) without stealing from valid bc >= 1 spans.
	t.Run("Attack_W21_P46_DropZeroBC_AllowExtra_And_StealingGuards", func(t *testing.T) {
		// (a) bc=0 ERROR under AllowExtra=true fails BOTH settlement.ack.attr.batch_count AND settlement.ack.status.
		spBad0 := mkSpan(goProf, 100, "ack", &bc0, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "bad bc0", true)
		resBad0 := Verify(goCaps, []*proxypb.TraceSpan{spBad0}, &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{dropExp(0, "acked", "", false)}},
		})
		assertHasViolation(t, resBad0, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")
		assertHasViolation(t, resBad0, CodeWrongStatusCode, "settlement.ack.status")

		// (b) Negative bc=-5 (UNSET & ERROR) under AllowExtra=true fails settlement.ack.attr.batch_count.
		for _, st := range []proxypb.SpanStatusCode{proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR} {
			spNeg := mkSpan(goProf, 101, "ack", &bcNeg, st, "neg", st == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR)
			resNeg := Verify(goCaps, []*proxypb.TraceSpan{spNeg}, &TraceExpectation{
				ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{dropExp(0, "acked", "", false)}},
			})
			assertHasViolation(t, resNeg, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")
		}

		// (c) Stealing guard (spBgZero sorts BEFORE spOK): valid spOK strictly beats invalid bc=0 span -> PASS.
		spBgZero := mkSpan(goProf, 102, "ack", &bc0, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
		spOK := mkSpan(goProf, 103, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
		resSteal1 := Verify(goCaps, []*proxypb.TraceSpan{spBgZero, spOK}, &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{dropExp(0, "acked", "", false)}},
		})
		if !resSteal1.Passed() {
			t.Fatalf("stealing guard expected PASS, got:\n%s", resSteal1.Format())
		}

		// (d) Pass 1 & Pass 2 stealing guard for eod_failed:
		spErr := mkSpan(goProf, 104, "ack", &bc1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, "perm fail", true)
		resStealErr := Verify(goCaps, []*proxypb.TraceSpan{spBgZero, spErr}, &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{dropExp(0, "eod_failed", "", false)}},
		})
		if !resStealErr.Passed() {
			t.Fatalf("expected PASS picking spErr over spBgZero, got:\n%s", resStealErr.Format())
		}

		resStealP2 := Verify(goCaps, []*proxypb.TraceSpan{spBgZero, spOK}, &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{dropExp(0, "eod_failed", "", false)}},
		})
		assertHasViolation(t, resStealP2, CodeWrongStatusCode, "settlement.ack.status")
		for _, f := range resStealP2.Failures {
			if f.Rule == "settlement.ack.attr.batch_count" {
				t.Fatalf("Pass 2 stole bc=0 span over valid bc=1 UNSET span: %v", f)
			}
		}

		// (e) AllowExtra=false with bc=0 still fails traces.no_extra_pubsub_spans (ack & nack).
		for _, op := range []string{"ack", "nack"} {
			spStrict0 := mkSpan(goProf, 110, op, &bc0, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, "", false)
			resStrict0 := Verify(goCaps, []*proxypb.TraceSpan{spStrict0}, &TraceExpectation{
				ProjectID: proj, Subscription: fullSub,
				Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{dropExp(0, op+"ed", "", false)}},
			})
			assertHasViolation(t, resStrict0, CodeUnexpectedSpan, "traces.no_extra_pubsub_spans")
		}
	})
}

func TestPhase46_Theorem1_Theorem2_ExactEquivalence(t *testing.T) {
	t.Parallel()

	checkExactParity := func(t *testing.T, tag string, items []unreservedDroppedItem, cands []unlinkedSettleCandidate, caps []int, statusMatchOnly bool) {
		t.Helper()
		got := matchPhase46Pass(items, cands, caps, statusMatchOnly)

		edges := make([]bipartiteEdge, 0, len(items)*len(cands))
		for i, d := range items {
			for j, c := range cands {
				if (c.op == "ack" && !d.allowAck) || (c.op == "nack" && !d.allowNack) {
					continue
				}
				if statusMatchOnly && c.isErr != d.wantErr {
					continue
				}
				edges = append(edges, bipartiteEdge{
					left:   i,
					right:  j,
					weight: phase46EdgeWeight(d, c),
				})
			}
		}
		want := solveBipartiteMatching(len(items), len(cands), caps, nil, edges)
		if !slices.Equal(got, want) {
			t.Fatalf("%s mismatch:\n  got:  %v\n  want: %v", tag, got, want)
		}
		ref := refSolveBipartiteMatching(len(items), len(cands), caps, nil, edges)
		gCard, gW, gT, gErr := evalMatchingObjective(len(items), len(cands), caps, nil, edges, got)
		rCard, rW, rT, rErr := evalMatchingObjective(len(items), len(cands), caps, nil, edges, ref)
		if gErr != nil || rErr != nil || gCard != rCard || gW != rW || gT != rT {
			t.Fatalf("%s objective mismatch: got=(%d,%d,%d,%v) ref=(%d,%d,%d,%v)", tag, gCard, gW, gT, gErr, rCard, rW, rT, rErr)
		}

		// Also verify Stage 3 solveTightComponentSSAP directly (smallThreshold=0) achieves identical exact objective (card, W, T).
		gotSSAP := matchPhase46PassImpl(items, cands, caps, statusMatchOnly, 0)
		sCard, sW, sT, sErr := evalMatchingObjective(len(items), len(cands), caps, nil, edges, gotSSAP)
		if sErr != nil || sCard != rCard || sW != rW || sT != rT {
			t.Fatalf("%s Stage3-SSAP objective mismatch: ssap=(%d,%d,%d,%v) ref=(%d,%d,%d,%v)", tag, sCard, sW, sT, sErr, rCard, rW, rT, rErr)
		}
	}

	// Deterministic heterogeneous + multi-component + Theorem 1 truncation/clipping + gap-preserving tie-break cases.
	cAckOK := func(bOK, bErr int64) unlinkedSettleCandidate {
		return unlinkedSettleCandidate{op: "ack", isErr: false, baseOK: bOK, baseErr: bErr}
	}
	cAckErr := func(bOK, bErr int64) unlinkedSettleCandidate {
		return unlinkedSettleCandidate{op: "ack", isErr: true, baseOK: bOK, baseErr: bErr}
	}
	cNackOK := func(bOK, bErr int64) unlinkedSettleCandidate {
		return unlinkedSettleCandidate{op: "nack", isErr: false, baseOK: bOK, baseErr: bErr}
	}
	cNackErr := func(bOK, bErr int64) unlinkedSettleCandidate {
		return unlinkedSettleCandidate{op: "nack", isErr: true, baseOK: bOK, baseErr: bErr}
	}

	// 1. Mixed homogeneous + heterogeneous components + excess signature prefix-cap clipping + unmatched het item.
	itemsHet := []unreservedDroppedItem{
		{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: false}, // ack-only OK -> het component
		{droppedIdx: 1, wantErr: true, allowAck: false, allowNack: true},  // nack-only Err -> homo component (when statusMatchOnly)
		{droppedIdx: 2, wantErr: false, allowAck: true, allowNack: true},  // both OK -> bridges ack-only OK & nack-only OK
		{droppedIdx: 3, wantErr: false, allowAck: true, allowNack: false}, // ack-only OK -> het component
	}
	candsHet := []unlinkedSettleCandidate{
		cAckOK(80_000, 0),   // elig=3, cap=2 -> kept(2)
		cAckOK(80_000, 0),   // elig=3, cap=2 -> clipped(1)!
		cAckOK(80_000, 0),   // excess -> pruned(used >= elig)!
		cNackErr(0, 80_000), // homo component for item[1]
		cNackOK(80_000, 0),  // bridges to item[2] in het component (incompatible with items 0,3!)
	}
	checkExactParity(t, "het_clip_and_homo_split", itemsHet, candsHet, []int{2, 2, 2, 1, 1}, true)

	// 2. Heterogeneous component with insufficient capacity leaving hetAssign[i] < 0 on some items.
	checkExactParity(t, "het_unmatched_items",
		[]unreservedDroppedItem{
			{droppedIdx: 0, allowAck: true, allowNack: false},
			{droppedIdx: 1, allowAck: true, allowNack: true},
			{droppedIdx: 2, allowAck: false, allowNack: true},
		},
		[]unlinkedSettleCandidate{cAckOK(80_000, 0)},
		[]int{1},
		false,
	)

	// 3. Homogeneous with zero compatible candidates when numDistinctLT > 1.
	checkExactParity(t, "multi_lt_empty_compat_homo",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: false},
			{droppedIdx: 1, wantErr: true, allowAck: false, allowNack: true},
		},
		[]unlinkedSettleCandidate{cAckOK(80_000, 0)},
		[]int{1},
		true,
	)

	// 4. Two disconnected heterogeneous components with non-contiguous global indices (verifies exact tie-breaking parity).
	checkExactParity(t, "two_het_components_with_global_gaps",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: false},
			{droppedIdx: 1, wantErr: true, allowAck: true, allowNack: true},
			{droppedIdx: 2, wantErr: false, allowAck: true, allowNack: true},
			{droppedIdx: 3, wantErr: true, allowAck: false, allowNack: true},
		},
		[]unlinkedSettleCandidate{
			cAckOK(80_000, 0),
			cAckErr(0, 80_000),
			cNackErr(0, 80_000),
			cNackOK(80_000, 0),
		},
		[]int{1, 1, 1, 1},
		true,
	)

	// 5. 1000 randomized trials covering all 6 left types, 4 right types, caps 1..4, multiple weight signatures, and both statusMatchOnly modes.
	rng := rand.New(rand.NewSource(20261116))
	weights := []int64{0, 20_000, 80_000, 100_000}
	for tc := 0; tc < 1000; tc++ {
		nL := 1 + rng.Intn(14)
		nR := 1 + rng.Intn(16)
		items := make([]unreservedDroppedItem, nL)
		for i := 0; i < nL; i++ {
			opMode := rng.Intn(3)
			items[i] = unreservedDroppedItem{
				droppedIdx: i,
				wantErr:    rng.Intn(2) == 1,
				allowAck:   opMode == 0 || opMode == 2,
				allowNack:  opMode == 1 || opMode == 2,
			}
		}
		cands := make([]unlinkedSettleCandidate, nR)
		caps := make([]int, nR)
		for j := 0; j < nR; j++ {
			op := "ack"
			if rng.Intn(2) == 1 {
				op = "nack"
			}
			cands[j] = unlinkedSettleCandidate{
				op:      op,
				isErr:   rng.Intn(2) == 1,
				baseOK:  weights[rng.Intn(len(weights))],
				baseErr: weights[rng.Intn(len(weights))],
			}
			caps[j] = 1 + rng.Intn(4)
		}
		statusMatchOnly := tc%2 == 0
		checkExactParity(t, fmt.Sprintf("rand_%d", tc), items, cands, caps, statusMatchOnly)
	}

	// 6. Counterexample 1: 3-node tight tie-cycle (disproves greedy-by-i on E_tight).
	checkExactParity(t, "counterexample_1_3node_cycle",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: true},
			{droppedIdx: 1, wantErr: true, allowAck: true, allowNack: false},
			{droppedIdx: 2, wantErr: true, allowAck: true, allowNack: true},
		},
		[]unlinkedSettleCandidate{
			cAckOK(10, 14),
			cNackOK(10, 14),
			cAckOK(0, 9),
		},
		[]int{1, 1, 1},
		false,
	)

	// 7. Counterexample 3: exact (card, W, T) tie checking bit-for-bit slices.Equal parity.
	checkExactParity(t, "counterexample_3_exact_tie_caps",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: true},
			{droppedIdx: 1, wantErr: true, allowAck: false, allowNack: true},
			{droppedIdx: 2, wantErr: true, allowAck: true, allowNack: false},
			{droppedIdx: 3, wantErr: true, allowAck: true, allowNack: false},
		},
		[]unlinkedSettleCandidate{
			cNackOK(100, 0),
			cAckOK(0, 0),
			cAckOK(100, 0),
			cNackOK(0, 0),
		},
		[]int{1, 0, 1, 0},
		false,
	)
	checkExactParity(t, "counterexample_3_exact_tie_statusMatch",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: true},
			{droppedIdx: 1, wantErr: false, allowAck: false, allowNack: true},
			{droppedIdx: 2, wantErr: false, allowAck: true, allowNack: false},
			{droppedIdx: 3, wantErr: false, allowAck: true, allowNack: false},
		},
		[]unlinkedSettleCandidate{
			cNackOK(100, 0),
			cAckErr(0, 0),
			cAckOK(100, 0),
			cNackErr(0, 0),
		},
		[]int{1, 1, 1, 1},
		true,
	)

	// 8. Mixed S_req + S_opt (L_opt = empty, dominant earlier S_opt vs higher-weight S_req) + Shortcut B optBudget & large cap clip.
	checkExactParity(t, "mixed_sreq_and_sopt",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: false},
			{droppedIdx: 1, wantErr: false, allowAck: true, allowNack: true},
			{droppedIdx: 2, wantErr: false, allowAck: true, allowNack: true},
		},
		[]unlinkedSettleCandidate{
			cNackOK(50, 0), // S_opt (earlier)
			cAckOK(50, 0),  // S_opt
			cNackOK(60, 0), // S_req (higher weight!)
		},
		[]int{1, 10, 1},
		false,
	)
	checkExactParity(t, "allRReq_mixed_lreq_shortcutB",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: true}, // lOpt (w=50)
			{droppedIdx: 1, wantErr: false, allowAck: true, allowNack: true}, // lOpt (w=50)
			{droppedIdx: 2, wantErr: true, allowAck: true, allowNack: true},  // lReq (w=90)
		},
		[]unlinkedSettleCandidate{
			{op: "ack", isErr: false, baseOK: 50, baseErr: 90},
			{op: "ack", isErr: false, baseOK: 50, baseErr: 90},
		},
		[]int{1, 1},
		false,
	)

	// 9. Input sanitation: caps[j] == 0, !allowAck && !allowNack, all disallowed, negative weights.
	checkExactParity(t, "sanitation_zero_caps_disallowed_neg_weights",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: false, allowNack: false},
			{droppedIdx: 1, wantErr: false, allowAck: true, allowNack: false},
			{droppedIdx: 2, wantErr: false, allowAck: true, allowNack: true},
			{droppedIdx: 3, wantErr: false, allowAck: false, allowNack: true},
		},
		[]unlinkedSettleCandidate{
			cAckOK(-10, -5),
			cAckOK(5, 0),
			cNackOK(-3, -1),
		},
		[]int{0, 2, 1},
		false,
	)
	if got := matchPhase46Pass([]unreservedDroppedItem{{allowAck: false, allowNack: false}}, []unlinkedSettleCandidate{cAckOK(1, 1)}, []int{1}, false); !slices.Equal(got, []int{-1}) {
		t.Fatalf("expected [-1] for all-disallowed items, got %v", got)
	}
	if got := matchPhase46Pass(nil, []unlinkedSettleCandidate{cAckOK(1, 1)}, []int{1}, false); len(got) != 0 {
		t.Fatalf("expected [] for empty items, got %v", got)
	}
	if got := matchPhase46Pass([]unreservedDroppedItem{{allowAck: true}}, nil, nil, false); !slices.Equal(got, []int{-1}) {
		t.Fatalf("expected [-1] for empty cands, got %v", got)
	}
	// 2-group allLReq component with an unused rReq=false tight candidate (cNackOK).
	checkExactParity(t, "shortcutC_remOptItems_zero",
		[]unreservedDroppedItem{
			{droppedIdx: 0, wantErr: false, allowAck: true, allowNack: false},
			{droppedIdx: 1, wantErr: true, allowAck: true, allowNack: true},
		},
		[]unlinkedSettleCandidate{
			cAckOK(90, 90),
			cAckOK(90, 90),
			cAckOK(80, 0),
			cNackOK(0, 80),
		},
		[]int{1, 1, 1, 1},
		false,
	)

	// 10. N=60 (>2500 triggers Stage 3 solveTightComponentSSAP directly!) with 60 distinct weights + allRReq > totalCap dummy swaps.
	for _, caseType := range []string{"distinct60", "allRReq60x50"} {
		nL, nR := 60, 60
		if caseType == "allRReq60x50" {
			nR = 50
		}
		items60 := make([]unreservedDroppedItem, nL)
		for i := 0; i < nL; i++ {
			opMode := i % 3
			items60[i] = unreservedDroppedItem{
				droppedIdx: i,
				wantErr:    i%5 == 0,
				allowAck:   opMode == 0 || opMode == 2,
				allowNack:  opMode == 1 || opMode == 2,
			}
		}
		cands60 := make([]unlinkedSettleCandidate, nR)
		caps60 := make([]int, nR)
		for j := 0; j < nR; j++ {
			op := "ack"
			if j%2 == 1 {
				op = "nack"
			}
			bOK := int64(80_000 + (j*37)%1000)
			bErr := int64(20_000 + (j*53)%1000)
			if caseType == "allRReq60x50" {
				bOK = 50_000
				bErr = 50_000
				if j%3 == 0 {
					bOK = 60_000
				}
			}
			cands60[j] = unlinkedSettleCandidate{op: op, isErr: j%4 == 0, baseOK: bOK, baseErr: bErr}
			caps60[j] = 1
		}
		got60 := matchPhase46Pass(items60, cands60, caps60, false)
		edges60 := make([]bipartiteEdge, 0, nL*nR)
		for i, d := range items60 {
			for j, c := range cands60 {
				if (c.op == "ack" && !d.allowAck) || (c.op == "nack" && !d.allowNack) {
					continue
				}
				edges60 = append(edges60, bipartiteEdge{left: i, right: j, weight: phase46EdgeWeight(d, c)})
			}
		}
		ref60 := refSolveBipartiteMatching(nL, nR, caps60, nil, edges60)
		gCard, gW, gT, gErr := evalMatchingObjective(nL, nR, caps60, nil, edges60, got60)
		rCard, rW, rT, rErr := evalMatchingObjective(nL, nR, caps60, nil, edges60, ref60)
		if gErr != nil || rErr != nil || gCard != rCard || gW != rW || gT != rT {
			t.Fatalf("%s (>2500 Stage 3) mismatch: got=(%d,%d,%d,%v) ref=(%d,%d,%d,%v)", caseType, gCard, gW, gT, gErr, rCard, rW, rT, rErr)
		}
	}

	// 11. N=1002 heterogeneous objective spot check (501 empty + 501 acked vs 251 ack bc=2 + 251 nack bc=2).
	const N1002 = 1002
	items1002 := make([]unreservedDroppedItem, N1002)
	for i := 0; i < N1002; i++ {
		if i < 501 {
			items1002[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true, allowNack: true}
		} else {
			items1002[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true, allowNack: false}
		}
	}
	cands1002 := make([]unlinkedSettleCandidate, 502)
	caps1002 := make([]int, 502)
	for j := 0; j < 502; j++ {
		if j < 251 {
			cands1002[j] = cAckOK(80_010, 10)
		} else {
			cands1002[j] = cNackOK(80_010, 10)
		}
		caps1002[j] = 2
	}
	caps1002[0] = 1 // exact 501 ack slots for 501 acked items (+1 weight over empty) and 502 nack slots for 501 empty items
	got1002 := matchPhase46Pass(items1002, cands1002, caps1002, true)
	matched1002 := 0
	for i, r := range got1002 {
		if r < 0 {
			t.Fatalf("N=1002 spot check item %d unmatched", i)
		}
		matched1002++
		if i < 501 && r < 251 {
			t.Fatalf("N=1002 spot check empty item %d matched ack %d instead of optimal nack", i, r)
		}
		if i >= 501 && r >= 251 {
			t.Fatalf("N=1002 spot check acked item %d matched invalid nack %d", i, r)
		}
	}
	if matched1002 != N1002 {
		t.Fatalf("N=1002 spot check matched=%d want %d", matched1002, N1002)
	}

	// 12. 600-case differential SPFA parity check (smallThreshold=0 vs exact integer SPFA matching oracle, N=1..120, cap=1..3).
	t.Run("Differential_SPFA_600_SmallThresholdZero", func(t *testing.T) {
		rngDiff := rand.New(rand.NewSource(20261123))
		baseWPool := []int64{10_000, 20_000, 40_000, 60_000, 80_000, 100_000}
		solveExactSPFA := func(nL, nR int, caps []int, edges []bipartiteEdge) []int {
			nV := nL + nR + 2
			src, snk := nL+nR, nL+nR+1
			type sEdge struct {
				to, rev, cap int
				cost         int64
			}
			adj := make([][]sEdge, nV)
			addE := func(u, v, cap int, cost int64) {
				adj[u] = append(adj[u], sEdge{to: v, rev: len(adj[v]), cap: cap, cost: cost})
				adj[v] = append(adj[v], sEdge{to: u, rev: len(adj[u]) - 1, cap: 0, cost: -cost})
			}
			var maxAbsW int64
			for _, e := range edges {
				w := e.weight
				if w < 0 {
					w = -w
				}
				if w > maxAbsW {
					maxAbsW = w
				}
			}
			tieScale := int64(nL+1)*int64(nR+1) + 1
			Bbig := (int64(nL)+1)*(maxAbsW+1)*tieScale + 1
			for i := 0; i < nL; i++ {
				addE(src, i, 1, 0)
			}
			for j := 0; j < nR; j++ {
				if caps[j] > 0 {
					addE(nL+j, snk, caps[j], 0)
				}
			}
			for _, e := range edges {
				tie := int64(nL-e.left) * int64(nR-e.right)
				addE(e.left, nL+e.right, 1, -Bbig-e.weight*tieScale-tie)
			}
			const inf = int64(1) << 62
			dist := make([]int64, nV)
			inQ := make([]bool, nV)
			pV := make([]int, nV)
			pE := make([]int, nV)
			q := make([]int, 0, nV*8)
			for {
				for i := 0; i < nV; i++ {
					dist[i] = inf
					inQ[i] = false
					pV[i] = -1
				}
				dist[src] = 0
				q = append(q[:0], src)
				inQ[src] = true
				for head := 0; head < len(q); head++ {
					u := q[head]
					inQ[u] = false
					du := dist[u]
					for ei, e := range adj[u] {
						if e.cap > 0 && du+e.cost < dist[e.to] {
							dist[e.to] = du + e.cost
							pV[e.to] = u
							pE[e.to] = ei
							if !inQ[e.to] {
								q = append(q, e.to)
								inQ[e.to] = true
							}
						}
					}
				}
				if pV[snk] < 0 || dist[snk] >= 0 {
					break
				}
				delta := 1 << 30
				for cur := snk; cur != src; cur = pV[cur] {
					if c := adj[pV[cur]][pE[cur]].cap; c < delta {
						delta = c
					}
				}
				for cur := snk; cur != src; cur = pV[cur] {
					e := &adj[pV[cur]][pE[cur]]
					e.cap -= delta
					adj[cur][e.rev].cap += delta
				}
			}
			out := make([]int, nL)
			for i := range out {
				out[i] = -1
			}
			for i := 0; i < nL; i++ {
				for _, e := range adj[i] {
					if e.to >= nL && e.to < src && adj[e.to][e.rev].cap > 0 {
						out[i] = e.to - nL
						break
					}
				}
			}
			return out
		}

		for tc := 0; tc < 600; tc++ {
			nL := 2 + rngDiff.Intn(119)
			nR := 2 + rngDiff.Intn(119)
			if tc < 50 {
				nL = 2 + rngDiff.Intn(16)
				nR = 2 + rngDiff.Intn(16)
			}
			itemsD := make([]unreservedDroppedItem, nL)
			for i := 0; i < nL; i++ {
				opMode := rngDiff.Intn(3)
				itemsD[i] = unreservedDroppedItem{
					droppedIdx: i,
					wantErr:    rngDiff.Intn(2) == 1,
					allowAck:   opMode == 0 || opMode == 2,
					allowNack:  opMode == 1 || opMode == 2,
				}
			}
			candsD := make([]unlinkedSettleCandidate, nR)
			capsD := make([]int, nR)
			for j := 0; j < nR; j++ {
				op := "ack"
				if rngDiff.Intn(2) == 1 {
					op = "nack"
				}
				candsD[j] = unlinkedSettleCandidate{
					op:      op,
					isErr:   rngDiff.Intn(2) == 1,
					baseOK:  baseWPool[rngDiff.Intn(len(baseWPool))],
					baseErr: baseWPool[rngDiff.Intn(len(baseWPool))],
				}
				capsD[j] = 1 + rngDiff.Intn(3)
			}
			statusMatchOnly := tc%2 == 0
			edgesD := make([]bipartiteEdge, 0, nL*nR/2)
			for i, d := range itemsD {
				for j, c := range candsD {
					if (c.op == "ack" && !d.allowAck) || (c.op == "nack" && !d.allowNack) {
						continue
					}
					if statusMatchOnly && c.isErr != d.wantErr {
						continue
					}
					edgesD = append(edgesD, bipartiteEdge{left: i, right: j, weight: phase46EdgeWeight(d, c)})
				}
			}
			got := matchPhase46PassImpl(itemsD, candsD, capsD, statusMatchOnly, 0)
			ref := solveExactSPFA(nL, nR, capsD, edgesD)
			gCard, gW, gT, gErr := evalMatchingObjective(nL, nR, capsD, nil, edgesD, got)
			rCard, rW, rT, rErr := evalMatchingObjective(nL, nR, capsD, nil, edgesD, ref)
			if gErr != nil || rErr != nil || gCard != rCard || gW != rW || gT != rT {
				t.Fatalf("diff tc=%d (nL=%d nR=%d statusMatch=%v) mismatch: got=(%d,%d,%d,%v) ref=(%d,%d,%d,%v)",
					tc, nL, nR, statusMatchOnly, gCard, gW, gT, gErr, rCard, rW, rT, rErr)
			}
		}
	})
}

func TestScale1002_Phase46_SubSecondUnderRace(t *testing.T) {
	const N = 1002
	goProf := ProfileGoCanonical()
	goCaps := &proxypb.GetCapabilitiesResponse{
		SupportsMonitoring:          true,
		ClientLanguage:              "go",
		ClientLibrary:               "cloud.google.com/go/pubsub/v2",
		SupportsExactlyOnceDelivery: true,
	}
	const (
		proj    = "proj-scale-p46"
		subID   = "sub-scale-p46"
		fullSub = "projects/proj-scale-p46/subscriptions/sub-scale-p46"
	)
	intAV := func(v int64) *proxypb.AttributeValue {
		return &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
	}
	baseT0 := time.Date(2026, 11, 16, 6, 15, 0, 0, time.UTC)

	makeDrops := func(outcome string) []SubscribeMessageExpectation {
		msgs := make([]SubscribeMessageExpectation, N)
		for i := 0; i < N; i++ {
			msgs[i] = SubscribeMessageExpectation{
				MessageID:              fmt.Sprintf("sm-%d", i),
				AckID:                  fmt.Sprintf("sa-%d", i),
				ExpectDroppedUnsampled: true,
				WireTraceparent:        fmt.Sprintf("00-%032x-%016x-00", 70000+i, 700000+i),
				ExactlyOnce:            BoolPtr(true),
				ExpectedOutcome:        outcome,
			}
		}
		return msgs
	}
	make0LinkBatch := func(count int, op string, bc int64, status proxypb.SpanStatusCode, offset int) []*proxypb.TraceSpan {
		spans := make([]*proxypb.TraceSpan, count)
		fn := "sendAck"
		if op == "nack" {
			fn = "sendModAck"
		}
		for i := 0; i < count; i++ {
			var evs []*proxypb.SpanEvent
			msg := ""
			if status == proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR {
				msg = "err"
				evs = []*proxypb.SpanEvent{{Name: "exception"}}
			}
			spans[i] = &proxypb.TraceSpan{
				TraceId:                  fmt.Sprintf("%032x", 80000+offset+i),
				SpanId:                   fmt.Sprintf("%016x", 800000+offset+i),
				TraceFlags:               1,
				Name:                     subID + " " + op,
				Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
				StartTime:                timestamppb.New(baseT0.Add(time.Duration(i) * time.Microsecond)),
				EndTime:                  timestamppb.New(baseT0.Add(time.Duration(i)*time.Microsecond + time.Millisecond)),
				InstrumentationScopeName: goProf.ExpectedScope,
				Attributes: map[string]*proxypb.AttributeValue{
					"messaging.system":              strAV("gcp_pubsub"),
					"messaging.destination.name":    strAV(subID),
					"gcp.project_id":                strAV(proj),
					"gcp.resource.name":             strAV(fullSub),
					"code.function":                 strAV(fn),
					"messaging.batch.message_count": intAV(bc),
				},
				Events:        evs,
				StatusCode:    status,
				StatusMessage: msg,
			}
		}
		return spans
	}

	runTimed := func(t *testing.T, name string, allowExtra bool, msgs []SubscribeMessageExpectation, spans []*proxypb.TraceSpan, wantPass bool) {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			res := Verify(goCaps, spans, &TraceExpectation{
				ProjectID:             proj,
				Subscription:          fullSub,
				AllowExtraPubSubSpans: allowExtra,
				Subscribe:             &SubscribeExpectation{Messages: msgs},
			})
			elapsed := time.Since(start)
			t.Logf("%s: %v (passed=%v)", name, elapsed, res.Passed())
			if res.Passed() != wantPass {
				t.Fatalf("%s wantPass=%v got=%v:\n%s", name, wantPass, res.Passed(), res.Format())
			}
			if elapsed >= time.Second {
				t.Fatalf("%s took %v (>= 1s budget under -race)", name, elapsed)
			}
		})
	}

	runTimed(t, "1002_acked_vs_501_ack_bc2", false, makeDrops("acked"),
		make0LinkBatch(501, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 0), true)

	runTimed(t, "1002_acked_vs_1002_ack_bc1", false, makeDrops("acked"),
		make0LinkBatch(1002, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 2000), true)

	runTimed(t, "1002_acked_vs_1002_ack_bc2_allowExtra", true, makeDrops("acked"),
		make0LinkBatch(1002, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 4000), true)

	ackNackSpans := append(
		make0LinkBatch(501, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 6000),
		make0LinkBatch(501, "nack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 7000)...,
	)
	runTimed(t, "1002_empty_vs_501_ack_bc2_plus_501_nack_bc2_allowExtra", true, makeDrops(""),
		ackNackSpans, true)

	runTimed(t, "1002_acked_vs_501_ack_bc1_fail_missing_spans", false, makeDrops("acked"),
		make0LinkBatch(501, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 7500), false)

	failSpans := append(
		make0LinkBatch(501, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 8000),
		make0LinkBatch(1, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 9000)...,
	)
	runTimed(t, "1002_acked_vs_501_ack_bc1_fast_fail_extra_nack", false, makeDrops("acked"),
		failSpans, false)

	// Heterogeneous N=1002 scale workloads (all must execute < 1s under -race).
	makeDrop := func(i int, outcome string) SubscribeMessageExpectation {
		return SubscribeMessageExpectation{
			MessageID:              fmt.Sprintf("hm-%d", i),
			AckID:                  fmt.Sprintf("ha-%d", i),
			ExpectDroppedUnsampled: true,
			WireTraceparent:        fmt.Sprintf("00-%032x-%016x-00", 90000+i, 900000+i),
			ExactlyOnce:            BoolPtr(true),
			ExpectedOutcome:        outcome,
		}
	}

	// Het 1 (contiguous & interleaved): 501 acked + 501 "" vs 501 ack (bc=2) + 1 nack (bc=1), AllowExtra=true.
	het1Spans := append(
		make0LinkBatch(501, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 10000),
		make0LinkBatch(1, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 11000)...,
	)
	het1Contig := make([]SubscribeMessageExpectation, N)
	het1Inter := make([]SubscribeMessageExpectation, N)
	for i := 0; i < N; i++ {
		if i < 501 {
			het1Contig[i] = makeDrop(i, "acked")
		} else {
			het1Contig[i] = makeDrop(i, "")
		}
		if i%2 == 0 {
			het1Inter[i] = makeDrop(i, "acked")
		} else {
			het1Inter[i] = makeDrop(i, "")
		}
	}
	runTimed(t, "Het_501_acked_501_empty_vs_501_ack_bc2_plus_1_nack_allowExtra", true, het1Contig, het1Spans, true)
	runTimed(t, "Het_501_acked_501_empty_interleaved_vs_501_ack_bc2_plus_1_nack_allowExtra", true, het1Inter, het1Spans, true)

	// Het 2: 500 acked + 502 "" vs 600 ack (bc=2) + 10 nack (bc=2), AllowExtra=true.
	het2Msgs := make([]SubscribeMessageExpectation, N)
	for i := 0; i < N; i++ {
		if i < 500 {
			het2Msgs[i] = makeDrop(i, "acked")
		} else {
			het2Msgs[i] = makeDrop(i, "")
		}
	}
	het2Spans := append(
		make0LinkBatch(600, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 12000),
		make0LinkBatch(10, "nack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 13000)...,
	)
	runTimed(t, "Het_500_acked_502_empty_vs_600_ack_bc2_plus_10_nack_bc2_allowExtra", true, het2Msgs, het2Spans, true)

	// Het 3: 500 acked + 500 nacked + 2 "" vs 501 ack (bc=2) + 501 nack (bc=2), AllowExtra=true.
	het3Msgs := make([]SubscribeMessageExpectation, N)
	for i := 0; i < N; i++ {
		switch {
		case i < 500:
			het3Msgs[i] = makeDrop(i, "acked")
		case i < 1000:
			het3Msgs[i] = makeDrop(i, "nacked")
		default:
			het3Msgs[i] = makeDrop(i, "")
		}
	}
	het3Spans := append(
		make0LinkBatch(501, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 14000),
		make0LinkBatch(501, "nack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 15000)...,
	)
	runTimed(t, "Het_500_acked_500_nacked_2_empty_vs_501_ack_bc2_plus_501_nack_bc2_allowExtra", true, het3Msgs, het3Spans, true)

	// Het 4: 501 "" + 501 acked vs 250 ack (bc=2) + 1 ack (bc=1) + 250 nack (bc=2) + 1 nack (bc=1), AllowExtra=false (non-dominated tight cap!).
	het4Msgs := make([]SubscribeMessageExpectation, N)
	for i := 0; i < N; i++ {
		if i < 501 {
			het4Msgs[i] = makeDrop(i, "")
		} else {
			het4Msgs[i] = makeDrop(i, "acked")
		}
	}
	het4Spans := append(
		append(
			make0LinkBatch(250, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 16000),
			make0LinkBatch(1, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 16500)...,
		),
		append(
			make0LinkBatch(250, "nack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 17000),
			make0LinkBatch(1, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 17500)...,
		)...,
	)
	runTimed(t, "Het_501_empty_501_acked_vs_251_ack_plus_251_nack_strictExtra", false, het4Msgs, het4Spans, true)

	// Het 5: Interleaved 501 "" / 501 acked vs alternating 501 ack (bc=1) / 501 nack (bc=1), AllowExtra=false.
	het5Msgs := make([]SubscribeMessageExpectation, N)
	het5Spans := make([]*proxypb.TraceSpan, N)
	for i := 0; i < N; i++ {
		if i%2 == 0 {
			het5Msgs[i] = makeDrop(i, "")
			het5Spans[i] = make0LinkBatch(1, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 18000+i)[0]
		} else {
			het5Msgs[i] = makeDrop(i, "acked")
			het5Spans[i] = make0LinkBatch(1, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 19500+i)[0]
		}
	}
	runTimed(t, "Het_Interleaved_501_empty_501_acked_vs_501_ack_bc1_501_nack_bc1", false, het5Msgs, het5Spans, true)

	// Het 6: Interleaved 334 acked + 334 eod_failed + 334 "" vs 334 ack UNSET + 334 ack ERROR + 334 nack UNSET (bc=2), AllowExtra=true.
	het6Msgs := make([]SubscribeMessageExpectation, N)
	het6Spans := make([]*proxypb.TraceSpan, 0, N)
	for i := 0; i < N; i++ {
		switch i % 3 {
		case 0:
			het6Msgs[i] = makeDrop(i, "acked")
			het6Spans = append(het6Spans, make0LinkBatch(1, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 21000+i)[0])
		case 1:
			het6Msgs[i] = makeDrop(i, "eod_failed")
			het6Spans = append(het6Spans, make0LinkBatch(1, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, 22500+i)[0])
		default:
			het6Msgs[i] = makeDrop(i, "")
			het6Spans = append(het6Spans, make0LinkBatch(1, "nack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 24000+i)[0])
		}
	}
	runTimed(t, "Het_334_acked_334_eodFailed_334_empty_mixed_weights_allowExtra", true, het6Msgs, het6Spans, true)

	// Het 7: 501 acked + 501 "" vs 501 ack (bc=2) + 501 nack (bc=2), AllowExtra=false (fast fail).
	runTimed(t, "Het_501_acked_501_empty_vs_501_ack_bc2_501_nack_bc2_strictExtra", false, het1Contig, het3Spans, false)

	// Het 8: Adversarial N=1002 distinct candidate weights (cap=1, |S|=1002, heterogeneous ops) via matchPhase46Pass.
	t.Run("Adv_1002_DistinctWeights", func(t *testing.T) {
		itemsDW := make([]unreservedDroppedItem, N)
		candsDW := make([]unlinkedSettleCandidate, N)
		capsDW := make([]int, N)
		for i := 0; i < N; i++ {
			switch i % 3 {
			case 0:
				itemsDW[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true, allowNack: true}
			case 1:
				itemsDW[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true, allowNack: false}
			default:
				itemsDW[i] = unreservedDroppedItem{droppedIdx: i, wantErr: true, allowAck: true, allowNack: true}
			}
			op := "ack"
			if i%2 == 1 {
				op = "nack"
			}
			candsDW[i] = unlinkedSettleCandidate{
				op:      op,
				isErr:   i%4 == 0,
				baseOK:  int64(10_000 + ((i*97)%1002)*10),
				baseErr: int64(20_000 + ((i*131)%1002)*10),
			}
			capsDW[i] = 1
		}
		start := time.Now()
		got := matchPhase46Pass(itemsDW, candsDW, capsDW, false)
		elapsed := time.Since(start)
		t.Logf("Adv_1002_DistinctWeights: %v", elapsed)
		if elapsed >= time.Second {
			t.Fatalf("Adv_1002_DistinctWeights took %v (>= 1s budget under -race)", elapsed)
		}
		matchedCnt := 0
		for _, r := range got {
			if r >= 0 {
				matchedCnt++
			}
		}
		if matchedCnt == 0 {
			t.Fatalf("Adv_1002_DistinctWeights expected non-empty matching")
		}
	})

	runMatTimed := func(t *testing.T, name string, items []unreservedDroppedItem, cands []unlinkedSettleCandidate, caps []int, statusMatchOnly bool, wantCard int) {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			got := matchPhase46PassImpl(items, cands, caps, statusMatchOnly, 2500)
			elapsed := time.Since(start)
			t.Logf("%s: %v", name, elapsed)
			if elapsed >= time.Second {
				t.Fatalf("%s took %v (>= 1s budget under -race)", name, elapsed)
			}
			card := 0
			used := make([]int, len(cands))
			for i, r := range got {
				if r < 0 {
					continue
				}
				c := cands[r]
				d := items[i]
				if (c.op == "ack" && !d.allowAck) || (c.op == "nack" && !d.allowNack) {
					t.Fatalf("%s illegal match item %d -> cand %d", name, i, r)
				}
				if statusMatchOnly && c.isErr != d.wantErr {
					t.Fatalf("%s status mismatch item %d -> cand %d", name, i, r)
				}
				used[r]++
				if used[r] > caps[r] {
					t.Fatalf("%s cap overflow cand %d (%d > %d)", name, r, used[r], caps[r])
				}
				card++
			}
			if wantCard >= 0 && card != wantCard {
				t.Fatalf("%s card=%d want %d", name, card, wantCard)
			}
		})
	}

	// Wave 22 Attack 1/9: TwoGroup_250Acked_752Empty_SharesAck_1002 (AllowExtra=true, shares ack across groups).
	twoGrpMsgs := make([]SubscribeMessageExpectation, N)
	for i := 0; i < N; i++ {
		if i < 250 {
			twoGrpMsgs[i] = makeDrop(i, "acked")
		} else {
			twoGrpMsgs[i] = makeDrop(i, "")
		}
	}
	twoGrpSpans := append(
		make0LinkBatch(501, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 30000),
		make0LinkBatch(501, "nack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 31500)...,
	)
	runTimed(t, "TwoGroup_250Acked_752Empty_SharesAck_1002", true, twoGrpMsgs, twoGrpSpans, true)

	// Wave 22 Attack 2-3/9: Attack8_Interleaved_1002 (AllowExtra=true and AllowExtra=false).
	atk8Msgs := make([]SubscribeMessageExpectation, N)
	atk8Spans := make([]*proxypb.TraceSpan, 0, N)
	for i := 0; i < N; i++ {
		switch i % 3 {
		case 0:
			atk8Msgs[i] = makeDrop(i, "acked")
			atk8Spans = append(atk8Spans, make0LinkBatch(1, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 33000+i)[0])
		case 1:
			atk8Msgs[i] = makeDrop(i, "nacked")
			atk8Spans = append(atk8Spans, make0LinkBatch(1, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 34500+i)[0])
		default:
			atk8Msgs[i] = makeDrop(i, "")
			op := "ack"
			if i%2 == 1 {
				op = "nack"
			}
			atk8Spans = append(atk8Spans, make0LinkBatch(1, op, 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 36000+i)[0])
		}
	}
	runTimed(t, "Attack8_Interleaved_AllowExtra_1002", true, atk8Msgs, atk8Spans, true)
	runTimed(t, "Attack8_Interleaved_StrictExtra_1002", false, atk8Msgs, atk8Spans, true)

	// Wave 22 Attack 4/9: ThreeOutcome_334_BC2_StrictExtra_1002.
	threeMsgs := make([]SubscribeMessageExpectation, N)
	for i := 0; i < N; i++ {
		switch i % 3 {
		case 0:
			threeMsgs[i] = makeDrop(i, "acked")
		case 1:
			threeMsgs[i] = makeDrop(i, "nacked")
		default:
			threeMsgs[i] = makeDrop(i, "eod_failed")
		}
	}
	threeSpans := append(
		append(
			make0LinkBatch(167, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 37500),
			make0LinkBatch(167, "nack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 38000)...,
		),
		make0LinkBatch(167, "ack", 2, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, 38500)...,
	)
	runTimed(t, "ThreeOutcome_334_BC2_StrictExtra_1002", false, threeMsgs, threeSpans, true)

	// Wave 22 Attack 5-6/9: Adversarial_N1002_6x4_MixedWeights (Allow and Strict).
	build6x4 := func(extraCaps bool) ([]unreservedDroppedItem, []unlinkedSettleCandidate, []int) {
		items := make([]unreservedDroppedItem, N)
		for i := 0; i < N; i++ {
			opMode := i % 3
			items[i] = unreservedDroppedItem{
				droppedIdx: i,
				wantErr:    (i/3)%2 == 1,
				allowAck:   opMode == 0 || opMode == 2,
				allowNack:  opMode == 1 || opMode == 2,
			}
		}
		nC := N
		if extraCaps {
			nC = 1100
		}
		cands := make([]unlinkedSettleCandidate, nC)
		caps := make([]int, nC)
		for j := 0; j < nC; j++ {
			op := "ack"
			if j%2 == 1 {
				op = "nack"
			}
			isErr := (j/2)%2 == 1
			bOK := int64(80_000 + (j%7)*100)
			bErr := int64(20_000 + (j%5)*100)
			if isErr {
				bOK, bErr = 20_000+int64(j%5)*100, 80_000+int64(j%7)*100
			}
			cands[j] = unlinkedSettleCandidate{op: op, isErr: isErr, baseOK: bOK, baseErr: bErr}
			caps[j] = 1
		}
		return items, cands, caps
	}
	it64A, cd64A, cp64A := build6x4(true)
	runMatTimed(t, "Adversarial_N1002_6x4_MixedWeights_Allow", it64A, cd64A, cp64A, false, N)
	it64S, cd64S, cp64S := build6x4(false)
	runMatTimed(t, "Adversarial_N1002_6x4_MixedWeights_Strict", it64S, cd64S, cp64S, false, N)

	// Wave 22 Attack 7/9: CrossGroup_MixedRReq_Caps12_NonMono_1002.
	crossItems := make([]unreservedDroppedItem, N)
	crossCands := make([]unlinkedSettleCandidate, 668)
	crossCaps := make([]int, 668)
	for i := 0; i < N; i++ {
		switch i % 4 {
		case 0:
			crossItems[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true}
		case 1:
			crossItems[i] = unreservedDroppedItem{droppedIdx: i, wantErr: true, allowAck: true, allowNack: true}
		case 2:
			crossItems[i] = unreservedDroppedItem{droppedIdx: i, allowNack: true}
		default:
			crossItems[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true, allowNack: true}
		}
	}
	for j := 0; j < 668; j++ {
		op := "ack"
		if j%2 == 1 {
			op = "nack"
		}
		crossCands[j] = unlinkedSettleCandidate{op: op, isErr: j%3 == 0, baseOK: 80_010, baseErr: 20_010}
		if j%3 == 0 {
			crossCands[j].baseOK, crossCands[j].baseErr = 20_010, 80_010
		}
		crossCaps[j] = 1 + (j % 2)
	}
	runMatTimed(t, "CrossGroup_MixedRReq_Caps12_NonMono_1002", crossItems, crossCands, crossCaps, false, N)

	// Wave 22 Attack 8-9/9: Attack4_Contig_StrictExtra and Attack4_Interleaved_StrictExtra.
	makeAttack4 := func(interleaved bool) ([]SubscribeMessageExpectation, []*proxypb.TraceSpan) {
		msgs := make([]SubscribeMessageExpectation, N)
		for i := 0; i < N; i++ {
			if !interleaved {
				switch {
				case i < 251:
					msgs[i] = makeDrop(i, "acked")
				case i < 501:
					msgs[i] = makeDrop(i, "")
				case i < 751:
					msgs[i] = makeDrop(i, "nacked")
				default:
					msgs[i] = makeDrop(i, "eod_failed")
				}
			} else {
				switch i % 4 {
				case 0:
					msgs[i] = makeDrop(i, "acked")
				case 1:
					msgs[i] = makeDrop(i, "")
				case 2:
					msgs[i] = makeDrop(i, "nacked")
				default:
					msgs[i] = makeDrop(i, "eod_failed")
				}
			}
		}
		spans := make([]*proxypb.TraceSpan, N)
		unsetAckCnt := 501
		if interleaved {
			unsetAckCnt = 502
		}
		for j := 0; j < N; j++ {
			if j < unsetAckCnt {
				spans[j] = make0LinkBatch(1, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 40000+j)[0]
			} else if j < unsetAckCnt+250 {
				spans[j] = make0LinkBatch(1, "nack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET, 40000+j)[0]
			} else {
				spans[j] = make0LinkBatch(1, "ack", 1, proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR, 40000+j)[0]
			}
		}
		return msgs, spans
	}
	a4ContigM, a4ContigS := makeAttack4(false)
	runTimed(t, "Attack4_Contig_StrictExtra", false, a4ContigM, a4ContigS, true)
	a4InterM, a4InterS := makeAttack4(true)
	runTimed(t, "Attack4_Interleaved_StrictExtra", false, a4InterM, a4InterS, true)

	// Attack4 matrix form (251+250+250+251 vs 668 ack UNSET + 334 nack ERROR) verifying <1s SPFA + cut paths.
	buildAtk4Mat := func(interleaved bool) ([]unreservedDroppedItem, []unlinkedSettleCandidate, []int) {
		items := make([]unreservedDroppedItem, N)
		for i := 0; i < N; i++ {
			k := i % 4
			if !interleaved {
				switch {
				case i < 251:
					k = 0
				case i < 501:
					k = 1
				case i < 751:
					k = 2
				default:
					k = 3
				}
			}
			switch k {
			case 0:
				items[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true}
			case 1:
				items[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true, allowNack: true}
			case 2:
				items[i] = unreservedDroppedItem{droppedIdx: i, allowNack: true}
			default:
				items[i] = unreservedDroppedItem{droppedIdx: i, wantErr: true, allowAck: true, allowNack: true}
			}
		}
		cands := make([]unlinkedSettleCandidate, N)
		caps := make([]int, N)
		for j := 0; j < N; j++ {
			if j < 668 {
				cands[j] = unlinkedSettleCandidate{op: "ack", isErr: false, baseOK: 80_010, baseErr: 20_010}
			} else {
				cands[j] = unlinkedSettleCandidate{op: "nack", isErr: true, baseOK: 20_010, baseErr: 80_010}
			}
			caps[j] = 1
		}
		return items, cands, caps
	}
	a4mCI, a4mCC, a4mCP := buildAtk4Mat(false)
	runMatTimed(t, "Attack4_Contig_Matrix_668Ack_334NackErr", a4mCI, a4mCC, a4mCP, false, N)
	a4mII, a4mIC, a4mIP := buildAtk4Mat(true)
	runMatTimed(t, "Attack4_Interleaved_Matrix_668Ack_334NackErr", a4mII, a4mIC, a4mIP, false, N)

	// Attack6_RandN1002_Seed0..9 (10 seeds).
	for seed := int64(0); seed < 10; seed++ {
		rng := rand.New(rand.NewSource(6000 + seed))
		items := make([]unreservedDroppedItem, N)
		cands := make([]unlinkedSettleCandidate, N)
		caps := make([]int, N)
		for i := 0; i < N; i++ {
			m := rng.Intn(3)
			items[i] = unreservedDroppedItem{
				droppedIdx: i,
				wantErr:    rng.Intn(2) == 1,
				allowAck:   m == 0 || m == 2,
				allowNack:  m == 1 || m == 2,
			}
		}
		for j := 0; j < N; j++ {
			op := "ack"
			if rng.Intn(2) == 1 {
				op = "nack"
			}
			isErr := rng.Intn(2) == 1
			bOK, bErr := int64(80_000+rng.Intn(4)*100), int64(20_000+rng.Intn(4)*100)
			if isErr {
				bOK, bErr = bErr, bOK
			}
			cands[j] = unlinkedSettleCandidate{op: op, isErr: isErr, baseOK: bOK, baseErr: bErr}
			caps[j] = 1
		}
		runMatTimed(t, fmt.Sprintf("Attack6_RandN1002_Seed%d", seed), items, cands, caps, false, -1)
	}

	// Attack1_4Group_8Sig_Interleaved (allow and strict).
	a1Items := make([]unreservedDroppedItem, N)
	a1Cands := make([]unlinkedSettleCandidate, N)
	a1Caps := make([]int, N)
	for i := 0; i < N; i++ {
		switch i % 4 {
		case 0:
			a1Items[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true}
		case 1:
			a1Items[i] = unreservedDroppedItem{droppedIdx: i, allowNack: true}
		case 2:
			a1Items[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true, allowNack: true}
		default:
			a1Items[i] = unreservedDroppedItem{droppedIdx: i, wantErr: true, allowAck: true, allowNack: true}
		}
	}
	for j := 0; j < N; j++ {
		op := "ack"
		if j%2 == 1 {
			op = "nack"
		}
		isErr := (j/2)%2 == 1
		delta := int64(((j / 4) % 2) * 50)
		bOK, bErr := int64(80_000)+delta, int64(20_000)+delta
		if isErr {
			bOK, bErr = bErr, bOK
		}
		a1Cands[j] = unlinkedSettleCandidate{op: op, isErr: isErr, baseOK: bOK, baseErr: bErr}
		a1Caps[j] = 1
	}
	runMatTimed(t, "Attack1_4Group_8Sig_Interleaved_Allow", a1Items, a1Cands, a1Caps, false, N)
	runMatTimed(t, "Attack1_4Group_8Sig_Interleaved_Strict", a1Items, a1Cands[:N], a1Caps[:N], false, N)

	// Attack2_MixedRReq_NonMono_StrictExtra.
	a2Items := make([]unreservedDroppedItem, N)
	a2Cands := make([]unlinkedSettleCandidate, N)
	a2Caps := make([]int, N)
	for i := 0; i < N; i++ {
		a2Items[i] = unreservedDroppedItem{droppedIdx: i, wantErr: i%5 == 0, allowAck: i%3 != 1, allowNack: i%3 != 0}
	}
	for j := 0; j < N; j++ {
		op := "ack"
		if j%3 == 0 {
			op = "nack"
		}
		isErr := j%7 == 0
		a2Cands[j] = unlinkedSettleCandidate{op: op, isErr: isErr, baseOK: int64(70_000 + (j%11)*10), baseErr: int64(60_000 + (j%13)*10)}
		a2Caps[j] = 1
	}
	runMatTimed(t, "Attack2_MixedRReq_NonMono_StrictExtra", a2Items, a2Cands, a2Caps, false, -1)

	// Attack3_Modular6Pattern_ZeroBC.
	a3Items := make([]unreservedDroppedItem, N)
	a3Cands := make([]unlinkedSettleCandidate, N)
	a3Caps := make([]int, N)
	for i := 0; i < N; i++ {
		m := i % 6
		a3Items[i] = unreservedDroppedItem{droppedIdx: i, wantErr: m >= 3, allowAck: m%3 != 1, allowNack: m%3 != 0}
	}
	for j := 0; j < N; j++ {
		op := "ack"
		if j%2 == 1 {
			op = "nack"
		}
		a3Cands[j] = unlinkedSettleCandidate{op: op, isErr: j%4 == 0, baseOK: 1000, baseErr: 1000}
		if j%5 == 0 {
			a3Caps[j] = 2
		} else {
			a3Caps[j] = 1
		}
	}
	runMatTimed(t, "Attack3_Modular6Pattern_ZeroBC", a3Items, a3Cands, a3Caps, false, N)

	// Cycle334_Gapped_And_Hybrid_N1002.
	cycItems := make([]unreservedDroppedItem, N)
	cycCands := make([]unlinkedSettleCandidate, N)
	cycCaps := make([]int, N)
	for i := 0; i < N; i++ {
		switch i % 3 {
		case 0:
			cycItems[i] = unreservedDroppedItem{droppedIdx: i, wantErr: false, allowAck: true, allowNack: true}
		case 1:
			cycItems[i] = unreservedDroppedItem{droppedIdx: i, wantErr: true, allowAck: true, allowNack: false}
		default:
			cycItems[i] = unreservedDroppedItem{droppedIdx: i, wantErr: true, allowAck: true, allowNack: true}
		}
	}
	for j := 0; j < N; j++ {
		switch j % 3 {
		case 0:
			cycCands[j] = unlinkedSettleCandidate{op: "ack", baseOK: 10, baseErr: 14}
		case 1:
			cycCands[j] = unlinkedSettleCandidate{op: "nack", baseOK: 10, baseErr: 14}
		default:
			cycCands[j] = unlinkedSettleCandidate{op: "ack", baseOK: 0, baseErr: 9}
		}
		cycCaps[j] = 1
	}
	runMatTimed(t, "Cycle334_Gapped_And_Hybrid_N1002", cycItems, cycCands, cycCaps, false, N)

	// Counterexample1_x334_Gapped (N=668, verifies exact tie optimality).
	t.Run("Counterexample1_x334_Gapped", func(t *testing.T) {
		const nCE = 668
		ceItems := make([]unreservedDroppedItem, nCE)
		ceCands := make([]unlinkedSettleCandidate, nCE)
		ceCaps := make([]int, nCE)
		for i := 0; i < nCE; i++ {
			ceItems[i] = unreservedDroppedItem{droppedIdx: i, allowAck: true, allowNack: true}
		}
		for j := 0; j < nCE; j++ {
			op := "ack"
			if j >= 334 {
				op = "nack"
			}
			ceCands[j] = unlinkedSettleCandidate{op: op, baseOK: 80_010, baseErr: 0}
			ceCaps[j] = 1
		}
		start := time.Now()
		got := matchPhase46PassImpl(ceItems, ceCands, ceCaps, false, 2500)
		elapsed := time.Since(start)
		t.Logf("Counterexample1_x334_Gapped: %v", elapsed)
		if elapsed >= time.Second {
			t.Fatalf("Counterexample1_x334_Gapped took %v (>= 1s)", elapsed)
		}
		for i := 0; i < nCE; i++ {
			if got[i] != i {
				t.Fatalf("Counterexample1_x334_Gapped optimal tie expected identity got[%d]=%d", i, got[i])
			}
		}
	})

	// HardRandom20_N1002_6Groups_Caps12 (20 seeds, 6 groups, <=12 tsigs, cap=1..2).
	wPoolHard := []int64{0, 20_000, 60_000, 80_000, 100_000}
	for seed := int64(0); seed < 20; seed++ {
		rng := rand.New(rand.NewSource(9000 + seed))
		items := make([]unreservedDroppedItem, N)
		for i := 0; i < N; i++ {
			m := rng.Intn(3)
			items[i] = unreservedDroppedItem{
				droppedIdx: i,
				wantErr:    rng.Intn(2) == 1,
				allowAck:   m == 0 || m == 2,
				allowNack:  m == 1 || m == 2,
			}
		}
		cands := make([]unlinkedSettleCandidate, 760)
		caps := make([]int, 760)
		for j := 0; j < 760; j++ {
			op := "ack"
			if rng.Intn(2) == 1 {
				op = "nack"
			}
			cands[j] = unlinkedSettleCandidate{
				op:      op,
				isErr:   rng.Intn(2) == 1,
				baseOK:  wPoolHard[rng.Intn(len(wPoolHard))],
				baseErr: wPoolHard[rng.Intn(len(wPoolHard))],
			}
			caps[j] = 1 + rng.Intn(2)
		}
		runMatTimed(t, fmt.Sprintf("HardRandom20_N1002_6Groups_Caps12_Seed%d", seed), items, cands, caps, false, -1)
	}
}

func TestIter23_Bug3_ModackOnlySettlementsMatrix(t *testing.T) {
	t.Parallel()
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true, SupportsExactlyOnceDelivery: true}
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true, SupportsExactlyOnceDelivery: true}
	goProf := ProfileGoCanonical()
	cppProf := ProfileCpp()
	const (
		proj    = "p"
		subID   = "s"
		fullSub = "projects/p/subscriptions/s"
	)
	mkSub := func(prof Profile, seq int, msgID, ackID string, eod bool, result string, isErr bool, evNames ...string) []*proxypb.TraceSpan {
		if prof.Name == ProfileGo {
			sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans(proj, subID, msgID, ackID, "", 5, 0, eod, 1, "", result, seq)
			if isErr {
				sub.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				sub.StatusMessage = "eod error"
				sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: "exception"})
			}
			for _, en := range evNames {
				if en == "ack called" || en == "nack called" {
					pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: en})
				} else if en == "ack start" || en == "nack start" || en == "modack start" {
					sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: en, Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}})
				} else {
					sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: en})
				}
			}
			return []*proxypb.TraceSpan{sub, cc, sc, pr}
		}
		tr := testTraceID(40000 + seq)
		subSID := testSpanID(400000 + seq*10)
		ccSID := testSpanID(400001 + seq*10)
		scSID := testSpanID(400002 + seq*10)
		prSID := testSpanID(400003 + seq*10)
		mkC := func(sid, pid, name string, kind proxypb.SpanKind) *proxypb.TraceSpan {
			base := time.Date(2026, 11, 16, 8, 0, 0, 0, time.UTC).Add(time.Duration(seq) * time.Millisecond)
			return &proxypb.TraceSpan{
				TraceId: tr, SpanId: sid, ParentSpanId: pid, TraceFlags: 1, Name: name, Kind: kind,
				StartTime: timestamppb.New(base), EndTime: timestamppb.New(base.Add(5 * time.Millisecond)),
				InstrumentationScopeName: cppProf.ExpectedScope,
				Attributes: map[string]*proxypb.AttributeValue{
					"gcp.project_id":             strAV(proj),
					"messaging.system":           strAV("gcp_pubsub"),
					"messaging.destination.name": strAV(subID),
				},
			}
		}
		sub := mkC(subSID, "", subID+" subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER)
		sub.Attributes["messaging.message.id"] = strAV(msgID)
		sub.Attributes["messaging.gcp_pubsub.message.ack_id"] = strAV(ackID)
		sub.Attributes["messaging.message.envelope.size"] = intAV(30)
		sub.Attributes["messaging.gcp_pubsub.message.ordering_key"] = strAV("")
		sub.Attributes["messaging.gcp_pubsub.subscription.exactly_once_delivery"] = boolAV(eod)
		sub.Attributes["code.function.name"] = strAV("SubscriptionSession::OnMessage")
		sub.Attributes["messaging.operation.type"] = strAV("subscribe")
		if result != "" {
			sub.Attributes["messaging.gcp_pubsub.result"] = strAV(result)
		}
		for _, en := range evNames {
			sub.Events = append(sub.Events, &proxypb.SpanEvent{Name: en})
		}
		cc := mkC(ccSID, subSID, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL)
		sc := mkC(scSID, subSID, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL)
		pr := mkC(prSID, subSID, subID+" process", proxypb.SpanKind_SPAN_KIND_INTERNAL)
		pr.Attributes["messaging.message.id"] = strAV(msgID)
		pr.Attributes["messaging.message.envelope.size"] = intAV(30)
		pr.Attributes["messaging.gcp_pubsub.message.ordering_key"] = strAV("")
		pr.Attributes["messaging.operation.type"] = strAV("process")
		return []*proxypb.TraceSpan{sub, cc, sc, pr}
	}
	mkCppSettle := func(seq int, op, ackID string, deadline int64, isErr bool, subSpans ...*proxypb.TraceSpan) *proxypb.TraceSpan {
		tr := testTraceID(41000 + seq)
		sid := testSpanID(410000 + seq)
		base := time.Date(2026, 11, 16, 8, 1, 0, 0, time.UTC).Add(time.Duration(seq) * time.Millisecond)
		kind := proxypb.SpanKind_SPAN_KIND_INTERNAL
		attrs := map[string]*proxypb.AttributeValue{
			"gcp.project_id":             strAV(proj),
			"messaging.system":           strAV("gcp_pubsub"),
			"messaging.destination.name": strAV(subID),
		}
		if op == "modack" {
			kind = proxypb.SpanKind_SPAN_KIND_CLIENT
			attrs["messaging.operation.type"] = strAV("extend")
			attrs["messaging.batch.message_count"] = intAV(1)
			attrs["messaging.gcp_pubsub.message.ack_deadline_seconds"] = intAV(deadline)
		} else {
			attrs["code.function.name"] = strAV("pubsub::AckHandler::" + op)
			attrs["messaging.gcp_pubsub.message.ack_id"] = strAV(ackID)
			attrs["messaging.gcp_pubsub.subscription.template"] = strAV(fullSub)
			attrs["messaging.operation.type"] = strAV("settle")
			stStr := "OK"
			if isErr {
				stStr = "INTERNAL"
			}
			attrs["gl-cpp.status_code"] = strAV(stStr)
		}
		sp := &proxypb.TraceSpan{
			TraceId: tr, SpanId: sid, TraceFlags: 1, Name: subID + " " + op, Kind: kind,
			StartTime: timestamppb.New(base), EndTime: timestamppb.New(base.Add(2 * time.Millisecond)),
			InstrumentationScopeName: cppProf.ExpectedScope, Attributes: attrs,
		}
		if isErr && op != "modack" {
			sp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			sp.StatusMessage = "cpp eod err"
		} else if op != "modack" {
			sp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
		}
		for _, ss := range subSpans {
			sp.Links = append(sp.Links, &proxypb.SpanLink{TraceId: ss.TraceId, SpanId: ss.SpanId})
		}
		return sp
	}

	unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(42000), testSpanID(420000))

	// Case 1 & 2: -01 Go EOD modack-only + ExpectedOutcome="acked" (single OK and retry) under AllowExtra false & true -> PASS.
	for _, allowExtra := range []bool{false, true} {
		s1 := mkSub(goProf, 1, "m1", "a1", true, "acked", false, "ack called")
		mod1 := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40001, s1[:1])
		ack1 := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40002, s1[:1])
		if res := Verify(goCaps, append(s1, mod1, ack1), &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: allowExtra,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
				MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked",
				Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true), AckDeadlineSeconds: 60}},
			}}},
		}); !res.Passed() {
			t.Fatalf("Case 1 (allowExtra=%v) expected PASS, got:\n%s", allowExtra, res.Format())
		}

		// Case 2a: Go retry (ack start x1, ack end x2 + ack ERROR) -> PASS under AllowExtra false & true.
		s2 := mkSub(goProf, 2, "m2", "a2", true, "acked", false, "ack called")
		mod2 := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40003, s2[:1])
		ackErr := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40004, s2[:1])
		ackErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackErr.StatusMessage = "transient"
		ackErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		s2[0].Events = append(s2[0].Events, &proxypb.SpanEvent{Name: "ack end"})
		if res := Verify(goCaps, append(s2, mod2, ackErr), &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: allowExtra,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
				MessageID: "m2", AckID: "a2", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked",
				Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true), AckDeadlineSeconds: 60}},
			}}},
		}); !res.Passed() {
			t.Fatalf("Case 2a (allowExtra=%v) expected PASS, got:\n%s", allowExtra, res.Format())
		}
		if allowExtra {
			// Case 2b: Go retry with both ack(ERROR) + ack(UNSET) under AllowExtra=true -> PASS.
			s2b := mkSub(goProf, 202, "m2b", "a2b", true, "acked", false, "ack called")
			mod2b := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40053, s2b[:1])
			ack2bErr := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40054, s2b[:1])
			ack2bErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
			ack2bErr.StatusMessage = "transient"
			ack2bErr.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			ack2bOK := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40055, s2b[:1])
			// Go emits 1 "ack start" and 2 "ack end" events across the initial attempt and retry.
			filteredEvs := s2b[0].Events[:0]
			seenAckStart := false
			for _, ev := range s2b[0].Events {
				if ev.Name == "ack start" {
					if seenAckStart {
						continue
					}
					seenAckStart = true
				}
				filteredEvs = append(filteredEvs, ev)
			}
			s2b[0].Events = filteredEvs
			if res := Verify(goCaps, append(s2b, mod2b, ack2bErr, ack2bOK), &TraceExpectation{
				ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
				Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
					MessageID: "m2b", AckID: "a2b", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked",
					Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true), AckDeadlineSeconds: 60}},
				}}},
			}); !res.Passed() {
				t.Fatalf("Case 2b (allowExtra=true) expected PASS, got:\n%s", res.Format())
			}
		}

		// Case 3: Go unretried ack ERROR on ExpectedOutcome="acked" MUST FAIL settlement.ack.status (proves anyEODWithExplicit not triggered).
		s3 := mkSub(goProf, 3, "m3", "a3", true, "acked", false, "ack called")
		mod3 := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40006, s3[:1])
		ackBad := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40007, s3[:1])
		ackBad.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ackBad.StatusMessage = "perm fail"
		ackBad.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		assertHasViolation(t, Verify(goCaps, append(s3, mod3, ackBad), &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: allowExtra,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
				MessageID: "m3", AckID: "a3", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked",
				Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true), AckDeadlineSeconds: 60}},
			}}},
		}), CodeWrongStatusCode, "settlement.ack.status")
	}

	// Case 4: Wrong op (nack emitted when ExpectedOutcome="acked") -> FAIL link_incompatible_op + ack_start.
	s4 := mkSub(goProf, 4, "m4", "a4", true, "acked", false, "ack called")
	mod4 := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40008, s4[:1])
	nack4 := makeGoSettlementSpan(proj, subID, "nack", 1, 0, false, 40009, s4[:1])
	res4 := Verify(goCaps, append(s4, mod4, nack4), &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
			MessageID: "m4", AckID: "a4", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked",
			Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true), AckDeadlineSeconds: 60}},
		}}},
	})
	assertHasViolation(t, res4, CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
	assertHasViolation(t, res4, CodeMissingEvent, "subscribe.event.ack_start")

	// Case 5: -01 Go modack-only + ExpectedOutcome="nacked" -> PASS.
	s5 := mkSub(goProf, 5, "m5", "a5", false, "nacked", false, "nack called")
	mod5 := makeGoSettlementSpan(proj, subID, "modack", 1, 10, true, 40010, s5[:1])
	nack5 := makeGoSettlementSpan(proj, subID, "nack", 1, 0, false, 40011, s5[:1])
	if res := Verify(goCaps, append(s5, mod5, nack5), &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
			MessageID: "m5", AckID: "a5", ExactlyOnce: BoolPtr(false), ExpectedOutcome: "nacked",
			Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}},
		}}},
	}); !res.Passed() {
		t.Fatalf("Case 5 expected PASS, got:\n%s", res.Format())
	}

	// Case 6: Go & C++ modack-only + ExpectedOutcome="eod_failed" (PASS on ack ERROR, FAIL on UNSET, FAIL wrong nack when ack evidence).
	for _, allowExtra := range []bool{false, true} {
		s6 := mkSub(goProf, 6, "m6", "a6", true, "", true, "ack called")
		mod6 := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40012, s6[:1])
		ack6 := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40013, s6[:1])
		ack6Unset := cloneSpans([]*proxypb.TraceSpan{ack6})[0]
		ack6.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		ack6.StatusMessage = "eod fail"
		ack6.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		exp6 := &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: allowExtra,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
				MessageID: "m6", AckID: "a6", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "eod_failed",
				Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}},
			}}},
		}
		if res := Verify(goCaps, append(s6, mod6, ack6), exp6); !res.Passed() {
			t.Fatalf("Case 6 Go (allowExtra=%v) expected PASS, got:\n%s", allowExtra, res.Format())
		}
		assertHasViolation(t, Verify(goCaps, append(s6, mod6, ack6Unset), exp6), CodeWrongStatusCode, "settlement.ack.status")

		s6c := mkSub(cppProf, 7, "m6c", "a6c", true, "ack", false, "gl-cpp.modack_start", "gl-cpp.modack_end", "gl-cpp.ack_start", "gl-cpp.ack_end")
		mod6c := mkCppSettle(7, "modack", "a6c", 60, false, s6c[0])
		ack6c := mkCppSettle(8, "ack", "a6c", 0, true, s6c[0])
		exp6c := &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: allowExtra,
			Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
				MessageID: "m6c", AckID: "a6c", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "eod_failed",
				Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}},
			}}},
		}
		if res := Verify(cppCaps, append(s6c, mod6c, ack6c), exp6c); !res.Passed() {
			t.Fatalf("Case 6 C++ (allowExtra=%v) expected PASS, got:\n%s", allowExtra, res.Format())
		}
		nack6cWrong := mkCppSettle(9, "nack", "a6c", 0, true, s6c[0])
		assertHasViolation(t, Verify(cppCaps, append(s6c, mod6c, nack6cWrong), exp6c), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
	}

	// Case 7: C++ -01 EOD modack-only + ExpectedOutcome="acked" with ack(ERROR)+ack(OK) vs ack(ERROR) alone.
	s7 := mkSub(cppProf, 8, "m7", "a7", true, "ack", false, "gl-cpp.modack_start", "gl-cpp.modack_end", "gl-cpp.ack_start", "gl-cpp.ack_end")
	mod7 := mkCppSettle(10, "modack", "a7", 60, false, s7[0])
	ack7Err := mkCppSettle(11, "ack", "a7", 0, true, s7[0])
	ack7OK := mkCppSettle(12, "ack", "a7", 0, false, s7[0])
	exp7 := &TraceExpectation{
		ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
			MessageID: "m7", AckID: "a7", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked",
			Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}},
		}}},
	}
	if res := Verify(cppCaps, append(s7, mod7, ack7Err, ack7OK), exp7); !res.Passed() {
		t.Fatalf("Case 7 C++ retry expected PASS, got:\n%s", res.Format())
	}
	assertHasViolation(t, Verify(cppCaps, append(s7, mod7, ack7Err), exp7), CodeWrongStatusCode, "settlement.ack.status")

	// Case 8: -01 modack-only + ExpectedOutcome="expired" -> PASS without ack, FAIL if ack emitted.
	s8 := mkSub(goProf, 9, "m8", "a8", false, "expired", false)
	mod8 := makeGoSettlementSpan(proj, subID, "modack", 1, 10, true, 40020, s8[:1])
	exp8 := &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
			MessageID: "m8", AckID: "a8", ExactlyOnce: BoolPtr(false), ExpectedOutcome: "expired",
			Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}},
		}}},
	}
	if res := Verify(goCaps, append(s8, mod8), exp8); !res.Passed() {
		t.Fatalf("Case 8 expected PASS, got:\n%s", res.Format())
	}
	s8Bad := mkSub(goProf, 10, "m8", "a8", false, "expired", false)
	mod8b := makeGoSettlementSpan(proj, subID, "modack", 1, 10, true, 40021, s8Bad[:1])
	ack8Bad := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40022, s8Bad[:1])
	assertHasViolation(t, Verify(goCaps, append(s8Bad, mod8b, ack8Bad), exp8), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")

	// Case 9: StrictSettlementOps=true + modack-only + ExpectedOutcome="acked" -> FAIL link_incompatible_op + dangling_reverse_link.
	for _, allowExtra := range []bool{false, true} {
		s9 := mkSub(goProf, 11, "m9", "a9", true, "acked", false, "ack called")
		mod9 := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40023, s9[:1])
		ack9 := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40024, s9[:1])
		res9 := Verify(goCaps, append(s9, mod9, ack9), &TraceExpectation{
			ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: allowExtra,
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: true,
				Messages: []SubscribeMessageExpectation{{
					MessageID: "m9", AckID: "a9", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked",
					Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}},
				}},
			},
		})
		assertHasViolation(t, res9, CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")
		assertHasViolation(t, res9, CodeMissingLink, "subscribe.settlement.dangling_reverse_link")
	}

	// Case 10: -01 modack-only + ExpectedOutcome="" (Option 1) -> acked PASS, wrong nack FAIL link_incompatible_op.
	s10 := mkSub(goProf, 12, "m10", "a10", true, "acked", false, "ack called")
	mod10 := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40025, s10[:1])
	ack10 := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 40026, s10[:1])
	if res := Verify(goCaps, append(s10, mod10, ack10), &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
			MessageID: "m10", AckID: "a10", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "",
			Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}},
		}}},
	}); !res.Passed() {
		t.Fatalf("Case 10 Option 1 expected PASS, got:\n%s", res.Format())
	}
	s10Bad := mkSub(goProf, 13, "m10b", "a10b", true, "acked", false, "ack called")
	mod10Bad := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40027, s10Bad[:1])
	nack10Bad := makeGoSettlementSpan(proj, subID, "nack", 1, 0, false, 40028, s10Bad[:1])
	assertHasViolation(t, Verify(goCaps, append(s10Bad, mod10Bad, nack10Bad), &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
			MessageID: "m10b", AckID: "a10b", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "",
			Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}},
		}}},
	}), CodeBrokenBidirectionalLink, "subscribe.settlement.link_incompatible_op")

	// Case 11: -00 Go & C++ modack-only + ExpectedOutcome acked/nacked/eod_failed -> PASS with 0-link terminal span, FAIL CodeMissingSpan without it.
	for _, tc := range []struct {
		outcome string
		op      string
		wantErr bool
	}{
		{"acked", "ack", false},
		{"nacked", "nack", false},
		{"eod_failed", "ack", true},
	} {
		for _, allowExtra := range []bool{false, true} {
			modD := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40030, nil)
			termD := makeGoSettlementSpan(proj, subID, tc.op, 1, 0, false, 40031, nil)
			if tc.wantErr {
				termD.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
				termD.StatusMessage = "err"
				termD.Events = []*proxypb.SpanEvent{{Name: "exception"}}
			}
			exp11 := &TraceExpectation{
				ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: allowExtra,
				Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
					WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), ExpectedOutcome: tc.outcome,
					Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}},
				}}},
			}
			if res := Verify(goCaps, []*proxypb.TraceSpan{modD, termD}, exp11); !res.Passed() {
				t.Fatalf("Case 11 Go (%s, allowExtra=%v) expected PASS, got:\n%s", tc.outcome, allowExtra, res.Format())
			}
			assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{modD}, exp11), CodeMissingSpan, "subscribe.settlement.missing_span")

			modC := mkCppSettle(30, "modack", "", 60, false)
			termC := mkCppSettle(31, tc.op, "da", 0, tc.wantErr)
			if res := Verify(cppCaps, []*proxypb.TraceSpan{modC, termC}, exp11); !res.Passed() {
				t.Fatalf("Case 11 C++ (%s, allowExtra=%v) expected PASS, got:\n%s", tc.outcome, allowExtra, res.Format())
			}
			assertHasViolation(t, Verify(cppCaps, []*proxypb.TraceSpan{modC}, exp11), CodeMissingSpan, "subscribe.settlement.missing_span")
		}
	}

	// Case 12: Mixed -01 + -00 coalesced modack(bc=2) + ack(bc=2) with both modack-only + ExpectedOutcome="acked" -> PASS.
	s12 := mkSub(goProf, 14, "m12", "a12", true, "acked", false, "ack called")
	mod12 := makeGoSettlementSpan(proj, subID, "modack", 2, 60, true, 40040, s12[:1])
	ack12 := makeGoSettlementSpan(proj, subID, "ack", 2, 0, false, 40041, s12[:1])
	if res := Verify(goCaps, append(s12, mod12, ack12), &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
			{MessageID: "m12", AckID: "a12", ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}}},
			{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "modack", IsReceiptModack: BoolPtr(true)}}},
		}},
	}); !res.Passed() {
		t.Fatalf("Case 12 expected PASS, got:\n%s", res.Format())
	}

	// Case 13: -00 modack-only + ExpectedOutcome="expired" -> PASS w/ modack only, FAIL batch_count w/ ack2(bc=2).
	mod13 := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40042, nil)
	if res := Verify(goCaps, []*proxypb.TraceSpan{mod13}, &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{{
			WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), ExpectedOutcome: "expired",
			Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}},
		}}},
	}); !res.Passed() {
		t.Fatalf("Case 13 PASS contrast failed:\n%s", res.Format())
	}
	s13 := mkSub(goProf, 15, "m13", "a13", true, "acked", false, "ack called")
	ack13 := makeGoSettlementSpan(proj, subID, "ack", 2, 0, false, 40043, s13[:1])
	mod13s := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40044, s13[:1])
	assertHasViolation(t, Verify(goCaps, append(s13, mod13s, ack13, mod13), &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
			{MessageID: "m13", AckID: "a13", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
			{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), ExpectedOutcome: "expired", Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}}},
		}},
	}), CodeWrongAttributeValue, "settlement.ack.attr.batch_count")

	// Case 14: StrictSettlementOps=true + -00 modack-only + ExpectedOutcome="acked" + ack2(bc=2) -> FAIL batch_count (verifies 6337 fix) + CodeMissingSpan.
	s14 := mkSub(goProf, 16, "m14", "a14", true, "acked", false, "ack called")
	ack14 := makeGoSettlementSpan(proj, subID, "ack", 2, 0, false, 40045, s14[:1])
	mod14d := makeGoSettlementSpan(proj, subID, "modack", 1, 60, true, 40046, nil)
	res14 := Verify(goCaps, append(s14, ack14, mod14d), &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{
			StrictSettlementOps: true,
			Messages: []SubscribeMessageExpectation{
				{MessageID: "m14", AckID: "a14", ExactlyOnce: BoolPtr(true), Settlements: []SettlementExpectation{{Operation: "ack"}}},
				{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "modack", AckDeadlineSeconds: 60}}},
			},
		},
	})
	assertHasViolation(t, res14, CodeWrongAttributeValue, "settlement.ack.attr.batch_count")
	assertHasViolation(t, res14, CodeMissingSpan, "subscribe.settlement.missing_span")

	// Case 15: Whitespace / case normalization ("  MODACK ", " ACKED ").
	s15 := mkSub(goProf, 17, "m15", "a15", true, "acked", false, "ack called")
	mod15 := makeGoSettlementSpan(proj, subID, "modack", 2, 60, true, 40047, s15[:1])
	ack15 := makeGoSettlementSpan(proj, subID, "ack", 2, 0, false, 40048, s15[:1])
	if res := Verify(goCaps, append(s15, mod15, ack15), &TraceExpectation{
		ProjectID: proj, Subscription: fullSub,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
			{MessageID: "m15", AckID: "a15", ExactlyOnce: BoolPtr(true), ExpectedOutcome: " ACKED ", Settlements: []SettlementExpectation{{Operation: "  MODACK ", IsReceiptModack: BoolPtr(true)}}},
			{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExactlyOnce: BoolPtr(true), ExpectedOutcome: " ACKED ", Settlements: []SettlementExpectation{{Operation: "  MODACK ", IsReceiptModack: BoolPtr(true)}}},
		}},
	}); !res.Passed() {
		t.Fatalf("Case 15 expected PASS, got:\n%s", res.Format())
	}
}

func TestIter23_Bug2_MissingSpanAndCoalescingHardening(t *testing.T) {
	t.Parallel()
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true, SupportsExactlyOnceDelivery: true}
	const (
		proj    = "p"
		subID   = "s"
		fullSub = "projects/p/subscriptions/s"
	)
	unsampTP0 := fmt.Sprintf("00-%s-%s-00", testTraceID(43000), testSpanID(430000))
	unsampTP1 := fmt.Sprintf("00-%s-%s-00", testTraceID(43001), testSpanID(430010))

	// 1. 0-link exhausted under AllowExtra=true -> CodeMissingSpan.
	assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{
		makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 43001, nil),
	}, &TraceExpectation{
		ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
			{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
			{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
		}},
	}), CodeMissingSpan, "subscribe.settlement.missing_span")

	// 2. >=1-link bc=2 linking [m1(matched), extraSub(validSampledSubByKey)] has ZERO spare capacity for dropped -00 -> MUST FAIL CodeMissingSpan!
	sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans(proj, subID, "m1", "a1", "", 5, 0, false, 1, "", "acked", 43010)
	subEx, ccEx, scEx, prEx, _ := makeGoSubscribeMessageSpans(proj, subID, "m-extra", "a-extra", "", 5, 0, false, 1, "", "acked", 43011)
	pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
	prEx.Events = append(prEx.Events, &proxypb.SpanEvent{Name: "ack called"})
	ackCoalEx := makeGoSettlementSpan(proj, subID, "ack", 2, 0, false, 43012, []*proxypb.TraceSpan{sub1, subEx})
	assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, subEx, ccEx, scEx, prEx, ackCoalEx}, &TraceExpectation{
		ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
			{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "ack"}}},
			{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
		}},
	}), CodeMissingSpan, "subscribe.settlement.missing_span")

	// 3. >=1-link bc=2 full with m1 + explicit d0 leaves unreserved d1 unmatched -> CodeMissingSpan.
	sub3, cc3, sc3, pr3, _ := makeGoSubscribeMessageSpans(proj, subID, "m3", "a3", "", 5, 0, false, 1, "", "acked", 43020)
	pr3.Events = append(pr3.Events, &proxypb.SpanEvent{Name: "ack called"})
	ackFull3 := makeGoSettlementSpan(proj, subID, "ack", 2, 0, false, 43021, []*proxypb.TraceSpan{sub3})
	assertHasViolation(t, Verify(goCaps, []*proxypb.TraceSpan{sub3, cc3, sc3, pr3, ackFull3}, &TraceExpectation{
		ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
			{MessageID: "m3", AckID: "a3", ExactlyOnce: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "ack"}}},
			{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, Settlements: []SettlementExpectation{{Operation: "ack"}}},
			{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
		}},
	}), CodeMissingSpan, "subscribe.settlement.missing_span")

	// 4. StrictSettlementOps=true with Settlements=nil and "" or "expired" passes with 0 spans; "acked"/"nacked"/"eod_failed" fails CodeMissingSpan.
	for _, passOut := range []string{"", "expired"} {
		if res := Verify(goCaps, nil, &TraceExpectation{
			ProjectID: proj, Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: true,
				Messages:            []SubscribeMessageExpectation{{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, ExpectedOutcome: passOut}},
			},
		}); !res.Passed() {
			t.Fatalf("StrictSettlementOps=true outcome=%q expected PASS, got:\n%s", passOut, res.Format())
		}
	}
	for _, failOut := range []string{"acked", "nacked", "eod_failed"} {
		assertHasViolation(t, Verify(goCaps, nil, &TraceExpectation{
			ProjectID: proj, Subscription: fullSub,
			Subscribe: &SubscribeExpectation{
				StrictSettlementOps: true,
				Messages:            []SubscribeMessageExpectation{{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, ExpectedOutcome: failOut}},
			},
		}), CodeMissingSpan, "subscribe.settlement.missing_span")
	}

	// 5. Subscription=="" (subID=="") with 0 spans and ExpectedOutcome="acked"/"nacked" catches CodeMissingSpan.
	for _, out := range []string{"acked", "nacked"} {
		assertHasViolation(t, Verify(goCaps, nil, &TraceExpectation{
			Subscription: "",
			Subscribe: &SubscribeExpectation{
				Messages: []SubscribeMessageExpectation{{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, ExpectedOutcome: out}},
			},
		}), CodeMissingSpan, "subscribe.settlement.missing_span")
	}
}

func TestIter23_ColdPathsAndCoverageAudit(t *testing.T) {
	t.Parallel()
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true, SupportsExactlyOnceDelivery: true}
	const (
		proj    = "p"
		subID   = "s"
		fullSub = "projects/p/subscriptions/s"
	)

	// 1. Phase 4.6 Pass 1b rem > len(unmatchedDrops): 0-link ack (bc=1) takes 1 dropped msg in Pass 1a,
	// leaving 1 in unmatchedDrops while coalesced >=1-link ack (bc=4, 1 sampled) has rem=2 > 1.
	sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans(proj, subID, "m1", "a1", "", 5, 0, false, 1, "", "acked", 44001)
	pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "ack called"})
	ack0 := makeGoSettlementSpan(proj, subID, "ack", 1, 0, false, 44002, nil)
	ackCoal := makeGoSettlementSpan(proj, subID, "ack", 4, 0, false, 44003, []*proxypb.TraceSpan{sub1})
	unsampTP0 := fmt.Sprintf("00-%s-%s-00", testTraceID(44004), testSpanID(440040))
	unsampTP1 := fmt.Sprintf("00-%s-%s-00", testTraceID(44005), testSpanID(440050))
	if res := Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, ack0, ackCoal}, &TraceExpectation{
		ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: true,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
			{MessageID: "m1", AckID: "a1", ExactlyOnce: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "ack"}}},
			{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
			{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true, ExpectedOutcome: "acked"},
		}},
	}); !res.Passed() {
		t.Fatalf("Pass 1b rem > len(unmatchedDrops) expected PASS, got:\n%s", res.Format())
	}

	// 2. Phase 5b strictExtra usedExtra < maxExtraAvail & unreservedTotalRemaining < maxExtraAvail, plus nil Link on subSpan.
	sub2, cc2, sc2, pr2, _ := makeGoSubscribeMessageSpans(proj, subID, "m2", "a2", "", 5, 0, false, 1, "", "acked", 44010)
	pr2.Events = append(pr2.Events, &proxypb.SpanEvent{Name: "ack called"})
	ackEx := makeGoSettlementSpan(proj, subID, "ack", 2, 0, false, 44011, []*proxypb.TraceSpan{sub2})
	ackEx.Links = append(ackEx.Links, &proxypb.SpanLink{TraceId: testTraceID(44999), SpanId: testSpanID(449990)})
	nackEx := makeGoSettlementSpan(proj, subID, "nack", 2, 0, false, 44012, []*proxypb.TraceSpan{sub2})
	nackEx.Links = append(nackEx.Links, &proxypb.SpanLink{TraceId: testTraceID(44998), SpanId: testSpanID(449980)})
	sub2.Links = []*proxypb.SpanLink{
		nil,
		{TraceId: ackEx.GetTraceId(), SpanId: ackEx.GetSpanId()},
		{TraceId: nackEx.GetTraceId(), SpanId: nackEx.GetSpanId()},
	}
	resStrict := Verify(goCaps, []*proxypb.TraceSpan{sub2, cc2, sc2, pr2, ackEx, nackEx}, &TraceExpectation{
		ProjectID: proj, Subscription: fullSub, AllowExtraPubSubSpans: false,
		Subscribe: &SubscribeExpectation{Messages: []SubscribeMessageExpectation{
			{MessageID: "m2", AckID: "a2", ExactlyOnce: BoolPtr(false), Settlements: []SettlementExpectation{{Operation: "ack"}, {Operation: "nack"}}},
			{WireTraceparent: unsampTP0, ExpectDroppedUnsampled: true, ExpectedOutcome: ""},
			{WireTraceparent: unsampTP1, ExpectDroppedUnsampled: true, ExpectedOutcome: ""},
		}},
	})
	assertHasViolation(t, resStrict, CodeMissingLink, "subscribe.settlement.dangling_link")

	// 3. Direct solveTightComponentSSAP large-B (B > 1<<50) dropInit > 0 + freeColQ + len(satCols)==0 drop > 0 verification.
	// Using globalL=1<<25, globalR=1<<25 gives maxCard*globalL*globalR + 1 > 1<<50 so chainColsV bypasses SPFA.
	const bigShift = 1 << 25
	checkExactDirect := func(name string, gL, gR int, items []phase46HetItem, cands []phase46HetCand, numG, numTsigs int) {
		t.Helper()
		maxOrigL := 0
		for _, it := range items {
			if it.origL+1 > maxOrigL {
				maxOrigL = it.origL + 1
			}
		}
		assign := make([]int, maxOrigL)
		for i := range assign {
			assign[i] = -1
		}
		solveTightComponentSSAP(gL, gR, items, cands, numG, numTsigs, assign)

		rToC := make(map[int]int, len(cands))
		for c, rc := range cands {
			rToC[rc.origR] = c
		}
		var gotCard int
		var gotPri, gotTie int64
		capUsed := make([]int, len(cands))
		for _, it := range items {
			rj := assign[it.origL]
			if rj < 0 {
				continue
			}
			ci, ok := rToC[rj]
			if !ok {
				t.Fatalf("%s invalid origR assignment %d", name, rj)
			}
			if (cands[ci].lMask & (uint8(1) << uint(it.group))) == 0 {
				t.Fatalf("%s invalid group match row=%d col=%d", name, it.origL, ci)
			}
			capUsed[ci]++
			if capUsed[ci] > cands[ci].cap {
				t.Fatalf("%s capacity exceeded on col %d", name, ci)
			}
			gotCard++
			pri := int64(1)
			if it.lReq {
				pri++
			}
			if cands[ci].rReq {
				pri++
			}
			gotPri += pri
			gotTie += int64(gL-it.origL) * int64(gR-cands[ci].origR)
		}

		// Exact Bellman-Ford SPFA MCMF oracle with lexicographic (pri, tie) pair — polynomial O(V*E*F) and overflow-free even for bigShift.
		nL := len(items)
		nR := len(cands)
		src := nL + nR
		snk := src + 1
		nV := snk + 1
		type oEdge struct {
			to, rev, cap int
			pri, tie     int64
		}
		adj := make([][]oEdge, nV)
		addOEdge := func(u, v, cap int, pri, tie int64) {
			a := oEdge{to: v, rev: len(adj[v]), cap: cap, pri: pri, tie: tie}
			b := oEdge{to: u, rev: len(adj[u]), cap: 0, pri: -pri, tie: -tie}
			adj[u] = append(adj[u], a)
			adj[v] = append(adj[v], b)
		}
		for r, it := range items {
			addOEdge(src, r, 1, 0, 0)
			bit := uint8(1) << uint(it.group)
			for c, rc := range cands {
				if (rc.lMask & bit) == 0 {
					continue
				}
				ep := int64(1)
				if it.lReq {
					ep++
				}
				if rc.rReq {
					ep++
				}
				et := int64(gL-it.origL) * int64(gR-rc.origR)
				addOEdge(r, nL+c, 1, -ep, -et)
			}
		}
		for c, rc := range cands {
			addOEdge(nL+c, snk, rc.cap, 0, 0)
		}
		var bestCard int
		var bestPriNeg, bestTieNeg int64
		for {
			distP := make([]int64, nV)
			distT := make([]int64, nV)
			reached := make([]bool, nV)
			parV := make([]int, nV)
			parE := make([]int, nV)
			inQ := make([]bool, nV)
			for i := range parV {
				parV[i] = -1
			}
			reached[src] = true
			q := []int{src}
			inQ[src] = true
			for head := 0; head < len(q); head++ {
				u := q[head]
				inQ[u] = false
				for ei, e := range adj[u] {
					if e.cap <= 0 {
						continue
					}
					np := distP[u] + e.pri
					nt := distT[u] + e.tie
					if !reached[e.to] || np < distP[e.to] || (np == distP[e.to] && nt < distT[e.to]) {
						reached[e.to] = true
						distP[e.to] = np
						distT[e.to] = nt
						parV[e.to] = u
						parE[e.to] = ei
						if !inQ[e.to] {
							q = append(q, e.to)
							inQ[e.to] = true
						}
					}
				}
			}
			if !reached[snk] {
				break
			}
			bestCard++
			bestPriNeg += distP[snk]
			bestTieNeg += distT[snk]
			for cur := snk; cur != src; cur = parV[cur] {
				pv := parV[cur]
				ei := parE[cur]
				rev := adj[pv][ei].rev
				adj[pv][ei].cap--
				adj[cur][rev].cap++
			}
		}
		bestPri, bestTie := -bestPriNeg, -bestTieNeg
		if gotCard != bestCard || gotPri != bestPri || gotTie != bestTie {
			t.Fatalf("%s mismatch: ssap=(%d,%d,%d) oracle=(%d,%d,%d)", name, gotCard, gotPri, gotTie, bestCard, bestPri, bestTie)
		}
	}

	// Case A: B > 1<<50 triggers dropInit > 0 across 2nd seedDuals() -> freeColQ + rowMatch removal.
	checkExactDirect("bigB_freeColQ", bigShift, bigShift, []phase46HetItem{
		{origL: 100, group: 0, lReq: true},
		{origL: 200, group: 0, lReq: false},
		{origL: 300, group: 1, lReq: true},
		{origL: 400, group: 1, lReq: false},
		{origL: 500, group: 0, lReq: false},
	}, []phase46HetCand{
		{origR: 150, lMask: 0b01, tsig: 0, cap: 1, rReq: false},
		{origR: 250, lMask: 0b11, tsig: 1, cap: 1, rReq: true},
		{origR: 350, lMask: 0b10, tsig: 2, cap: 1, rReq: false},
		{origR: 450, lMask: 0b11, tsig: 1, cap: 1, rReq: true},
		{origR: 550, lMask: 0b11, tsig: 3, cap: 1, rReq: false},
	}, 2, 4)

	// Case B: Multi-seed B > 1<<50 exact-oracle fuzz to genuinely cover dropInit > 0, len(satCols)==0 && drop>0, and freeColQ paths.
	var rng uint64 = 0x23a7b91c5e3f1011
	nextU64 := func() uint64 {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		return rng
	}
	for seed := 0; seed < 400; seed++ {
		mL := 5 + int(nextU64()%2) // 5 or 6
		mR := 5 + int(nextU64()%2) // 5 or 6
		items := make([]phase46HetItem, mL)
		cands := make([]phase46HetCand, mR)
		curL := 10
		for i := 0; i < mL; i++ {
			curL += 10 + int(nextU64()%90)
			items[i] = phase46HetItem{
				origL: curL,
				group: int(nextU64() % 4),
				lReq:  (nextU64() & 1) == 0,
			}
		}
		curR := 10
		for i := 0; i < mR; i++ {
			curR += 10 + int(nextU64()%90)
			mask := uint8(1<<uint(nextU64()%4)) | uint8(1<<uint(nextU64()%4))
			if (nextU64() % 3) == 0 {
				mask |= uint8(1 << uint(nextU64()%4))
			}
			cands[i] = phase46HetCand{
				origR: curR,
				lMask: mask,
				cap:   1 + int(nextU64()%2),
				rReq:  (nextU64() & 1) == 1,
			}
		}
		tsMap := make(map[uint16]int)
		numTs := 0
		for i := range cands {
			key := uint16(cands[i].lMask)<<1 | b2u16(cands[i].rReq)
			id, ok := tsMap[key]
			if !ok {
				id = numTs
				tsMap[key] = id
				numTs++
			}
			cands[i].tsig = id
		}
		checkExactDirect(fmt.Sprintf("bigB_seed_%d", seed), bigShift, bigShift, items, cands, 4, numTs)
	}

	// Case C: Overlapping multi-tsig negative cycles (300 items, numG=5, staggered caps/rReq)
	// exercising deep SPFA passes + cutting planes + exact optimality parity vs diffCheckTightComponentSSAP.
	for seed := 0; seed < 3; seed++ {
		const nC = 270
		items := make([]phase46HetItem, nC)
		cands := make([]phase46HetCand, nC)
		for i := 0; i < nC; i++ {
			g := (i + seed) % 5
			items[i] = phase46HetItem{
				origL: 10*(i+1) + (i % 7),
				group: g,
				lReq:  (i % 4) == 0,
			}
			m1 := uint8(1 << uint((i*2)%5))
			m2 := uint8(1 << uint((i*2+1)%5))
			m3 := uint8(1 << uint((i*2+2)%5))
			mask := m1 | m2
			if i%3 == 0 {
				mask |= m3
			}
			cands[i] = phase46HetCand{
				origR: 12*(i+1) + ((i * 5) % 9),
				lMask: mask,
				cap:   1,
				rReq:  (i % 3) == 1,
			}
		}
		tsMap := make(map[uint16]int)
		numTs := 0
		for i := range cands {
			key := uint16(cands[i].lMask)<<1 | b2u16(cands[i].rReq)
			id, ok := tsMap[key]
			if !ok {
				id = numTs
				tsMap[key] = id
				numTs++
			}
			cands[i].tsig = id
		}
		assign := make([]int, 5000)
		for i := range assign {
			assign[i] = -1
		}
		solveTightComponentSSAP(5000, 5000, items, cands, 5, numTs, assign)
		// Verify feasibility & exact optimality via direct oracle SPFA helper if small, or capacity/validity checks.
		usedC := make([]int, nC)
		matchedCnt := 0
		for r, it := range items {
			rj := assign[it.origL]
			if rj < 0 {
				continue
			}
			matchedCnt++
			found := false
			bit := uint8(1) << uint(it.group)
			for c, rc := range cands {
				if rc.origR == rj {
					if (rc.lMask & bit) == 0 {
						t.Fatalf("CaseC seed=%d row %d invalid mask match", seed, r)
					}
					usedC[c]++
					if usedC[c] > rc.cap {
						t.Fatalf("CaseC seed=%d col %d cap overflow", seed, c)
					}
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("CaseC seed=%d missing origR %d", seed, rj)
			}
		}
		if matchedCnt == 0 {
			t.Fatalf("CaseC seed=%d unexpected 0 cardinality", seed)
		}
	}

	// Case D: Attempt triggering deep SPFA passes (!spfaConverged at 5919) with mL=62 (bypasses M_chunk) staggered 6-group chains.
	{
		const nC = 62
		items := make([]phase46HetItem, nC)
		cands := make([]phase46HetCand, nC)
		for i := 0; i < nC; i++ {
			g := i % 6
			items[i] = phase46HetItem{origL: 10*(i+1) + (i % 5), group: g, lReq: (i % 3) == 1}
			mask := uint8(1<<uint(g)) | uint8(1<<uint((g+1)%6)) | uint8(1<<uint((g+2)%6))
			cands[i] = phase46HetCand{origR: 12*(nC-i) + (i % 7), lMask: mask, cap: 1, rReq: (i % 2) == 0}
		}
		slices.SortFunc(cands, func(a, b phase46HetCand) int { return a.origR - b.origR })
		tsMap := make(map[uint16]int)
		numTs := 0
		for i := range cands {
			key := uint16(cands[i].lMask)<<1 | b2u16(cands[i].rReq)
			id, ok := tsMap[key]
			if !ok {
				id = numTs
				tsMap[key] = id
				numTs++
			}
			cands[i].tsig = id
		}
		checkExactDirect("caseD_m62", 1000, 1000, items, cands, 6, numTs)
	}

	// Case E: Hub column (cap=85) replaced 1-for-1 from 85 cap=1 source columns keeps used[c_hub]==85,
	// bottlenecking 85 sequential SPFA cancellations to 1 per pass and triggering pass == 80 (!spfaConverged -> break)
	// while still matching the exact SPFA oracle.
	{
		items := make([]phase46HetItem, 0, 255)
		for i := 0; i < 85; i++ {
			items = append(items, phase46HetItem{origL: 1 + i, group: 0, lReq: false})
		}
		for i := 0; i < 85; i++ {
			items = append(items, phase46HetItem{origL: 86 + i, group: 1, lReq: false})
		}
		for i := 0; i < 85; i++ {
			items = append(items, phase46HetItem{origL: 171 + i, group: 2, lReq: false})
		}
		var cands []phase46HetCand
		for j := 0; j < 85; j++ {
			cands = append(cands, phase46HetCand{origR: 1 + j, lMask: 0b000111, cap: 1, rReq: true, tsig: 0})
		}
		for j := 0; j < 85; j++ {
			cands = append(cands, phase46HetCand{origR: 101 + j, lMask: 0b000001 | (uint8(j%8) << 3), cap: 1, rReq: true, tsig: 1 + (j % 8)})
		}
		cands = append(cands, phase46HetCand{origR: 1000, lMask: 0b000111, cap: 85, rReq: true, tsig: 0})
		for j := 0; j < 85; j++ {
			cands = append(cands, phase46HetCand{origR: 2001 + j, lMask: 0b000100, cap: 1, rReq: true, tsig: 9})
		}
		for k := 0; k < 40; k++ {
			var mask uint8
			if k < 8 {
				mask = 0b000001 | (uint8(k) << 3)
			} else if k < 16 {
				mask = 0b000010 | (uint8(k-8) << 3)
			} else if k < 24 {
				mask = 0b000100 | (uint8(k-16) << 3)
			} else {
				mask = 0b000011 | (uint8(k-24) << 3)
			}
			cands = append(cands, phase46HetCand{origR: 3000 + k, lMask: mask, cap: 1, rReq: false, tsig: 10 + k})
		}
		checkExactDirect("caseE_hub_spfaNotConverged", 4000, 4000, items, cands, 6, 50)
	}

	// Case F: Exhaust cancelsLeft (2048) via >2048 bottlenecked negative cycles, covering BOTH:
	//   (1) cancelsLeft == 0 top-of-pass exit when pass finishes cleanly without spfaAborted, and
	//   (2) pushCnt[c2] > mR+4 mid-pass spfaAborted exit (without falsely marking spfaConverged = true).
	// Uses a fast exact Johnson-Dijkstra MCMF oracle (cross-checked against checkExactDirect on Case E size).
	t.Run("caseF_cancelsExhausted", func(t *testing.T) {
		checkExactFast := func(name string, gL, gR int, items []phase46HetItem, cands []phase46HetCand, numG, numTsigs int) {
			t.Helper()
			maxOrigL := 0
			for _, it := range items {
				if it.origL+1 > maxOrigL {
					maxOrigL = it.origL + 1
				}
			}
			assign := make([]int, maxOrigL)
			for i := range assign {
				assign[i] = -1
			}
			t0 := time.Now()
			solveTightComponentSSAP(gL, gR, items, cands, numG, numTsigs, assign)
			tSSAP := time.Since(t0)
			t1 := time.Now()

			rToC := make(map[int]int, len(cands))
			for c, rc := range cands {
				rToC[rc.origR] = c
			}
			var gotCard int
			var gotPri, gotTie int64
			capUsed := make([]int, len(cands))
			for _, it := range items {
				rj := assign[it.origL]
				if rj < 0 {
					continue
				}
				ci, ok := rToC[rj]
				if !ok {
					t.Fatalf("%s invalid origR assignment %d", name, rj)
				}
				if (cands[ci].lMask & (uint8(1) << uint(it.group))) == 0 {
					t.Fatalf("%s invalid group match row=%d col=%d", name, it.origL, ci)
				}
				capUsed[ci]++
				if capUsed[ci] > cands[ci].cap {
					t.Fatalf("%s capacity exceeded on col %d", name, ci)
				}
				gotCard++
				pri := int64(1)
				if it.lReq {
					pri++
				}
				if cands[ci].rReq {
					pri++
				}
				gotPri += pri
				gotTie += int64(gL-it.origL) * int64(gR-cands[ci].origR)
			}

			// Exact DAG-initialized Johnson-Dijkstra MCMF oracle over lexicographic (pri, tie) pairs.
			nL := len(items)
			nR := len(cands)
			src := nL + nR
			snk := src + 1
			nV := snk + 1
			type oEdge struct {
				to, rev, cap int
				pri, tie     int64
			}
			adj := make([][]oEdge, nV)
			addOEdge := func(u, v, cap int, pri, tie int64) {
				adj[u] = append(adj[u], oEdge{to: v, rev: len(adj[v]), cap: cap, pri: pri, tie: tie})
				adj[v] = append(adj[v], oEdge{to: u, rev: len(adj[u]) - 1, cap: 0, pri: -pri, tie: -tie})
			}
			for r, it := range items {
				addOEdge(src, r, 1, 0, 0)
				bit := uint8(1) << uint(it.group)
				for c, rc := range cands {
					if (rc.lMask & bit) == 0 {
						continue
					}
					ep := int64(1)
					if it.lReq {
						ep++
					}
					if rc.rReq {
						ep++
					}
					et := int64(gL-it.origL) * int64(gR-rc.origR)
					addOEdge(r, nL+c, 1, -ep, -et)
				}
			}
			for c, rc := range cands {
				addOEdge(nL+c, snk, rc.cap, 0, 0)
			}
			potP := make([]int64, nV)
			potT := make([]int64, nV)
			// Backward DAG shortest-path potentials d[u] from u to snk (pot[u] = -d[u]):
			// pot[snk] = 0, pot[nL+c] = 0, pot[r] = max_{c}(ep, et), pot[src] = max_{r}(pot[r]).
			for r := 0; r < nL; r++ {
				hasEdge := false
				for _, e := range adj[r] {
					if e.cap <= 0 {
						continue
					}
					cp, ct := -e.pri, -e.tie
					if !hasEdge || cp > potP[r] || (cp == potP[r] && ct > potT[r]) {
						hasEdge = true
						potP[r] = cp
						potT[r] = ct
					}
				}
				if hasEdge && (r == 0 || potP[r] > potP[src] || (potP[r] == potP[src] && potT[r] > potT[src])) {
					potP[src] = potP[r]
					potT[src] = potT[r]
				}
			}

			distP := make([]int64, nV)
			distT := make([]int64, nV)
			seenGen := make([]int, nV)
			closedGen := make([]int, nV)
			parV := make([]int, nV)
			parE := make([]int, nV)
			heapPos := make([]int, nV)
			var heapArr []int
			var closedNodes []int
			lessNode := func(a, b int) bool {
				if distP[a] != distP[b] {
					return distP[a] < distP[b]
				}
				return distT[a] < distT[b]
			}
			hPushOrUp := func(v int) {
				idx := heapPos[v]
				if idx < 0 {
					idx = len(heapArr)
					heapArr = append(heapArr, v)
					heapPos[v] = idx
				}
				for idx > 0 {
					p := (idx - 1) / 2
					pv := heapArr[p]
					if !lessNode(v, pv) {
						break
					}
					heapArr[idx] = pv
					heapPos[pv] = idx
					idx = p
				}
				heapArr[idx] = v
				heapPos[v] = idx
			}
			hPop := func() int {
				top := heapArr[0]
				heapPos[top] = -1
				n := len(heapArr) - 1
				if n > 0 {
					last := heapArr[n]
					heapArr[0] = last
					heapPos[last] = 0
				}
				heapArr = heapArr[:n]
				i := 0
				for {
					l := 2*i + 1
					if l >= n {
						break
					}
					best := l
					if r := l + 1; r < n && lessNode(heapArr[r], heapArr[l]) {
						best = r
					}
					if !lessNode(heapArr[best], heapArr[i]) {
						break
					}
					heapArr[i], heapArr[best] = heapArr[best], heapArr[i]
					heapPos[heapArr[i]] = i
					heapPos[heapArr[best]] = best
					i = best
				}
				return top
			}

			activeRows := make([]int, nL)
			rowActivePos := make([]int, nL)
			for r := 0; r < nL; r++ {
				activeRows[r] = r
				rowActivePos[r] = r
			}
			// Lazy min-heap over unmatched rows ordered by (-potP[r], -potT[r]), i.e., rc(src->r) up to constant pot[src].
			srcHeap := make([]int, nL)
			srcHeapPos := make([]int, nL)
			lessSrcRow := func(ra, rb int) bool {
				if potP[ra] != potP[rb] {
					return potP[ra] > potP[rb]
				}
				return potT[ra] > potT[rb]
			}
			srcHeapSwap := func(i, j int) {
				ri, rj := srcHeap[i], srcHeap[j]
				srcHeap[i], srcHeap[j] = rj, ri
				srcHeapPos[ri], srcHeapPos[rj] = j, i
			}
			srcHeapFix := func(idx int) {
				for idx > 0 {
					p := (idx - 1) / 2
					if !lessSrcRow(srcHeap[idx], srcHeap[p]) {
						break
					}
					srcHeapSwap(idx, p)
					idx = p
				}
				n := len(srcHeap)
				for {
					l := 2*idx + 1
					if l >= n {
						break
					}
					best := l
					if r := l + 1; r < n && lessSrcRow(srcHeap[r], srcHeap[l]) {
						best = r
					}
					if !lessSrcRow(srcHeap[best], srcHeap[idx]) {
						break
					}
					srcHeapSwap(idx, best)
					idx = best
				}
			}
			srcHeapPush := func(r int) {
				idx := len(srcHeap)
				srcHeap = append(srcHeap, r)
				srcHeapPos[r] = idx
				srcHeapFix(idx)
			}
			srcHeapPop := func() int {
				top := srcHeap[0]
				srcHeapPos[top] = -1
				n := len(srcHeap) - 1
				if n > 0 {
					last := srcHeap[n]
					srcHeap[0] = last
					srcHeapPos[last] = 0
				}
				srcHeap = srcHeap[:n]
				if n > 0 {
					srcHeapFix(0)
				}
				return top
			}
			for r := 0; r < nL; r++ {
				srcHeap[r] = r
				srcHeapPos[r] = r
			}
			for i := nL/2 - 1; i >= 0; i-- {
				srcHeapFix(i)
			}

			colActiveRev := make([][]int, nR)
			colRevPos := make([]map[int]int, nR)
			for c := 0; c < nR; c++ {
				colRevPos[c] = make(map[int]int)
			}

			var pulledRows []int
			var bestCard int
			var bestPriNeg, bestTieNeg int64
			gen := 0
			for {
				gen++
				heapArr = heapArr[:0]
				closedNodes = closedNodes[:0]
				pulledRows = pulledRows[:0]
				seenGen[src] = gen
				distP[src] = 0
				distT[src] = 0
				closedGen[src] = gen
				closedNodes = append(closedNodes, src)
				puP, puT := potP[src], potT[src]
				reachedSnk := false
				for len(heapArr) > 0 || len(srcHeap) > 0 {
					if len(srcHeap) > 0 {
						rTop := srcHeap[0]
						rcP := puP - potP[rTop]
						rcT := puT - potT[rTop]
						needPull := len(heapArr) == 0
						if !needPull {
							hTop := heapArr[0]
							if rcP < distP[hTop] || (rcP == distP[hTop] && rcT < distT[hTop]) {
								needPull = true
							}
						}
						if needPull {
							srcHeapPop()
							pulledRows = append(pulledRows, rTop)
							if seenGen[rTop] != gen || rcP < distP[rTop] || (rcP == distP[rTop] && rcT < distT[rTop]) {
								if seenGen[rTop] != gen {
									seenGen[rTop] = gen
									heapPos[rTop] = -1
								}
								distP[rTop] = rcP
								distT[rTop] = rcT
								parV[rTop] = src
								parE[rTop] = rTop
								hPushOrUp(rTop)
							}
							continue
						}
					}
					u := hPop()
					closedGen[u] = gen
					closedNodes = append(closedNodes, u)
					if u == snk {
						reachedSnk = true
						break
					}
					duP, duT := distP[u], distT[u]
					uPotP, uPotT := potP[u], potT[u]
					if u >= nL && u < src {
						c := u - nL
						snkEI := len(adj[u]) - 1
						if e := adj[u][snkEI]; e.cap > 0 {
							v := e.to
							ndP := duP + e.pri + uPotP - potP[v]
							ndT := duT + e.tie + uPotT - potT[v]
							if seenGen[v] != gen || ndP < distP[v] || (ndP == distP[v] && ndT < distT[v]) {
								if seenGen[v] != gen {
									seenGen[v] = gen
									heapPos[v] = -1
								}
								distP[v] = ndP
								distT[v] = ndT
								parV[v] = u
								parE[v] = snkEI
								hPushOrUp(v)
							}
						}
						for _, ei := range colActiveRev[c] {
							e := adj[u][ei]
							v := e.to
							ndP := duP + e.pri + uPotP - potP[v]
							ndT := duT + e.tie + uPotT - potT[v]
							if seenGen[v] != gen || ndP < distP[v] || (ndP == distP[v] && ndT < distT[v]) {
								if seenGen[v] != gen {
									seenGen[v] = gen
									heapPos[v] = -1
								}
								distP[v] = ndP
								distT[v] = ndT
								parV[v] = u
								parE[v] = ei
								hPushOrUp(v)
							}
						}
						continue
					}
					for ei, e := range adj[u] {
						if e.cap <= 0 {
							continue
						}
						v := e.to
						ndP := duP + e.pri + uPotP - potP[v]
						ndT := duT + e.tie + uPotT - potT[v]
						if seenGen[v] != gen || ndP < distP[v] || (ndP == distP[v] && ndT < distT[v]) {
							if seenGen[v] != gen {
								seenGen[v] = gen
								heapPos[v] = -1
							}
							distP[v] = ndP
							distT[v] = ndT
							parV[v] = u
							parE[v] = ei
							hPushOrUp(v)
						}
					}
				}
				if !reachedSnk {
					break
				}
				snkDP, snkDT := distP[snk], distT[snk]
				for _, v := range closedNodes {
					potP[v] += distP[v] - snkDP
					potT[v] += distT[v] - snkDT
					if v < nL && srcHeapPos[v] >= 0 {
						srcHeapFix(srcHeapPos[v])
					}
				}
				pathP := potP[snk] - potP[src]
				pathT := potT[snk] - potT[src]
				if pathP > 0 || (pathP == 0 && pathT >= 0) {
					break
				}
				bestCard++
				bestPriNeg += pathP
				bestTieNeg += pathT
				for cur := snk; cur != src; cur = parV[cur] {
					pv := parV[cur]
					ei := parE[cur]
					e := &adj[pv][ei]
					revEI := e.rev
					e.cap--
					adj[cur][revEI].cap++
					if pv < nL && cur >= nL && cur < src {
						c := cur - nL
						if adj[cur][revEI].cap == 1 {
							colRevPos[c][revEI] = len(colActiveRev[c])
							colActiveRev[c] = append(colActiveRev[c], revEI)
						}
					} else if pv >= nL && pv < src && cur < nL {
						c := pv - nL
						if e.cap == 0 {
							pos := colRevPos[c][ei]
							last := colActiveRev[c][len(colActiveRev[c])-1]
							colActiveRev[c][pos] = last
							colRevPos[c][last] = pos
							colActiveRev[c] = colActiveRev[c][:len(colActiveRev[c])-1]
							delete(colRevPos[c], ei)
						}
					}
				}
				for _, r := range pulledRows {
					if adj[src][r].cap > 0 {
						srcHeapPush(r)
					}
				}
			}
			bestPri, bestTie := -bestPriNeg, -bestTieNeg
			t.Logf("%s: ssap=%v oracle=%v card=%d pri=%d tie=%d", name, tSSAP, time.Since(t1), gotCard, gotPri, gotTie)
			if gotCard != bestCard || gotPri != bestPri || gotTie != bestTie {
				t.Fatalf("%s mismatch: ssap=(%d,%d,%d) oracle=(%d,%d,%d)", name, gotCard, gotPri, gotTie, bestCard, bestPri, bestTie)
			}
		}

		// Subcases 1 & 2: 6-group graphs (>48 distinct (lMask,rReq) tsigs bypassing M_chunk)
		// exhausting cancelsLeft (2048) cleanly and covering BOTH:
		//   (1) cancelsLeft == 0 top-of-pass exit (560x3 strided groups, verify.go:5666), and
		//   (2) pushCnt[c2] > mR+4 mid-pass spfaAborted exit across inner/outer/post-pass loops
		//       (380x4 contiguous groups, verify.go:5859-5860, 5875, 5958)
		// while matching the exact Johnson-Dijkstra MCMF oracle.
		var masks50 []uint8
		for m := uint8(1); m < 64; m++ {
			cnt := 0
			for b := 0; b < 6; b++ {
				if (m & (1 << uint(b))) != 0 {
					cnt++
				}
			}
			if cnt >= 2 && cnt <= 4 {
				masks50 = append(masks50, m)
			}
		}
		buildTest := func(nR, capPerCol, grpStride int) ([]phase46HetItem, []phase46HetCand, int) {
			nL := nR * capPerCol
			items := make([]phase46HetItem, nL)
			cands := make([]phase46HetCand, nR)
			for i := 0; i < nL; i++ {
				var g int
				if grpStride == 0 {
					g = (i * 6) / nL
				} else {
					g = (i / grpStride) % 6
				}
				items[i] = phase46HetItem{
					origL: 10*(i+1) + (i % 7),
					group: g,
					lReq:  false,
				}
			}
			for c := 0; c < nR; c++ {
				cands[c] = phase46HetCand{
					origR: 40*(c+1) + ((c * 5) % 9),
					lMask: masks50[(c*7+c/13)%len(masks50)],
					cap:   capPerCol,
					rReq:  true,
				}
			}
			tsMap := make(map[uint16]int)
			numTs := 0
			for i := range cands {
				key := uint16(cands[i].lMask)<<1 | b2u16(cands[i].rReq)
				id, ok := tsMap[key]
				if !ok {
					id = numTs
					tsMap[key] = id
					numTs++
				}
				cands[i].tsig = id
			}
			return items, cands, numTs
		}
		{
			items, cands, numTs := buildTest(560, 3, 2)
			checkExactFast("caseF_topOfPass_cancelsExhausted", 25000, 45000, items, cands, 6, numTs)
		}
		{
			items, cands, numTs := buildTest(380, 4, 0)
			checkExactFast("caseF_pushLimitAbort_cancelsExhausted", 20000, 40000, items, cands, 6, numTs)
		}
	})
}

func TestTier5_AdversarialNegativeVerifyBranches(t *testing.T) {
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

	t.Run("wire_and_callback_traceparent_mismatch", func(t *testing.T) {
		spans, exp := buildCanonicalGoE2EFixture()
		exp.Publish.WireTraceparents = []string{"00-" + spans[0].GetTraceId() + "-" + testSpanID(9998) + "-01"}
		resPub := Verify(goCaps, spans, exp)
		if resPub.Passed() || !hasRule(resPub, "publish.create.wire_traceparent", CodeBrokenTraceparent) {
			t.Fatalf("expected publish.create.wire_traceparent failure, got %s", resPub.Format())
		}

		spans2, exp2 := buildCanonicalGoE2EFixture()
		exp2.Subscribe.CallbackTraceparents = []string{"00-" + spans2[4].GetTraceId() + "-" + testSpanID(9997) + "-01"}
		resSub := Verify(goCaps, spans2, exp2)
		if resSub.Passed() || !hasRule(resSub, "subscribe.process.callback_traceparent", CodeBrokenTraceparent) {
			t.Fatalf("expected subscribe.process.callback_traceparent failure, got %s", resSub.Format())
		}
	})

	t.Run("unsampled_00_create_and_flow_control_leak", func(t *testing.T) {
		unsampledParent := "00-" + testTraceID(4100) + "-" + testSpanID(41001) + "-00"
		pub := makeGoPublishBatchSpan("p", "t", 1, 1)
		c, fc, batch, _ := makeGoPublishMessageSpans("p", "t", "msg-u", "", 4, 1, pub, 1)
		c.TraceId = testTraceID(4100)
		c.ParentSpanId = testSpanID(41001)
		fc.TraceId = testTraceID(4100)
		fc.ParentSpanId = c.SpanId
		batch.TraceId = testTraceID(4100)
		batch.ParentSpanId = c.SpanId
		pub.Links = []*proxypb.SpanLink{{TraceId: c.TraceId, SpanId: c.SpanId}}
		c.Links = []*proxypb.SpanLink{{TraceId: pub.TraceId, SpanId: pub.SpanId, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("publish")}}}

		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{
					ParentTraceparent:      unsampledParent,
					ExpectDroppedUnsampled: true,
				}},
			},
		}
		resCreate := Verify(goCaps, []*proxypb.TraceSpan{c, fc, batch, pub}, exp)
		if resCreate.Passed() || !hasRule(resCreate, "publish.unsampled_dropped", CodeUnexpectedSpan) {
			t.Fatalf("expected publish.unsampled_dropped on leaked create+fc+batch, got %s", resCreate.Format())
		}

		// Also leak only fc/batch under unsampled parent trace_id without create span.
		resFCOnly := Verify(goCaps, []*proxypb.TraceSpan{fc, batch, pub}, exp)
		if resFCOnly.Passed() || !hasRule(resFCOnly, "publish.unsampled_dropped", CodeUnexpectedSpan) {
			t.Fatalf("expected publish.unsampled_dropped on leaked fc/batch only, got %s", resFCOnly.Format())
		}
	})

	t.Run("fanout_negative_branches", func(t *testing.T) {
		// 1. Go canonical rejects fanout children.
		pubGo := makeGoPublishBatchSpan("p", "t", 1, 1)
		cGo, fcGo, bGo, _ := makeGoPublishMessageSpans("p", "t", "m1", "", 4, 1, pubGo, 1)
		fanGo := goSpan(pubGo.TraceId, testSpanID(5501), pubGo.SpanId, "publish #0", proxypb.SpanKind_SPAN_KIND_CLIENT, 12*time.Millisecond)
		resGoFan := Verify(goCaps, []*proxypb.TraceSpan{cGo, fcGo, bGo, pubGo, fanGo}, &TraceExpectation{
			Topic:   "projects/p/topics/t",
			Publish: &PublishExpectation{MessageCount: 1, BatchCount: 1},
		})
		if resGoFan.Passed() || !hasRule(resGoFan, "publish.batch.unexpected_fanout", CodeUnexpectedSpan) {
			t.Fatalf("expected Go profile to reject fanout children, got %s", resGoFan.Format())
		}

		// 2. C++ parent publish with both direct links and fanout children + wrong fanout count + wrong child name & link bounds.
		spansCpp, expCpp := buildCppStreamingE2EFixture(true, false, false)
		// Modify publish span to claim batch size 3 with MaxLinksPerSpan=2 (wants 2 fanouts: publish #0 [2], publish #1 [1]),
		// keep direct link on parent, provide 1 misnamed child "publish #9" with 0 links -> triggers all fanout rules!
		var pubCpp *proxypb.TraceSpan
		for _, s := range spansCpp {
			if s.GetName() == "t publish" {
				pubCpp = s
				break
			}
		}
		pubCpp.Attributes["messaging.batch.message_count"] = intAV(3)
		badFan := cppSpan(pubCpp.TraceId, testSpanID(2099), pubCpp.SpanId, "publish #9", proxypb.SpanKind_SPAN_KIND_CLIENT, 12*time.Millisecond)
		badFan.Links = nil
		expCpp.Subscribe = nil
		expCpp.Publish.MaxLinksPerSpan = 2
		expCpp.AllowExtraPubSubSpans = true
		resCppFan := Verify(cppCaps, append(spansCpp, badFan), expCpp)
		for _, wantRule := range []string{
			"publish.batch.fanout_parent_links",
			"publish.batch.fanout_count",
			"publish.batch.fanout_contiguous_names",
			"publish.batch.fanout_link_bounds",
		} {
			if !hasRule(resCppFan, wantRule, "") {
				t.Errorf("expected C++ fanout violation %q, got:\n%s", wantRule, resCppFan.Format())
			}
		}
	})

	t.Run("publish_batches_count_large_product_and_multiset", func(t *testing.T) {
		var manySpans []*proxypb.TraceSpan
		var wantBatches []PublishBatchExpectation
		for i := range 17 {
			ps := makeGoPublishBatchSpan("p", "t", int64(i+1), i)
			manySpans = append(manySpans, ps)
			wantBatches = append(wantBatches, PublishBatchExpectation{MessageCount: i + 1})
		}
		if v := verifyPublishBatches(manySpans[:16], wantBatches, false); len(v) == 0 || v[0].Rule != "publish.batches.count" {
			t.Fatalf("expected publish.batches.count mismatch, got %+v", v)
		}
		// len(wantBatches)*len(ordered) = 17*17 = 289 > 256 -> fast indexed path with 1 mismatch + 1 wildcard.
		wantMismatch := slices.Clone(wantBatches)
		wantMismatch[16] = PublishBatchExpectation{MessageCount: 999}
		wantMismatch[0] = PublishBatchExpectation{MessageCount: 0}
		vMult := verifyPublishBatches(manySpans, wantMismatch, false)
		if len(vMult) == 0 || vMult[0].Rule != "publish.batches.multiset" {
			t.Fatalf("expected publish.batches.multiset mismatch on >256 matrix, got %+v", vMult)
		}
	})

	t.Run("subscribe_duplicate_children_and_extra_settlement_links_and_cpp_err_substr", func(t *testing.T) {
		spans, exp := buildCanonicalGoE2EFixture()
		sub := spans[4]
		dupSched := goSpan(sub.TraceId, testSpanID(7791), sub.SpanId, "subscribe scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, 22*time.Millisecond)
		dupProc := goSpan(sub.TraceId, testSpanID(7792), sub.SpanId, "s process", proxypb.SpanKind_SPAN_KIND_CONSUMER, 23*time.Millisecond)
		resDup := Verify(goCaps, append(spans, dupSched, dupProc), exp)
		if !hasRule(resDup, "subscribe.scheduler.unique", CodeUnexpectedSpan) || !hasRule(resDup, "subscribe.process.unique", CodeUnexpectedSpan) {
			t.Fatalf("expected duplicate scheduler/process violations, got:\n%s", resDup.Format())
		}

		// auditExtraSettlementLinks: duplicate forward link, dropped forbidden link, dangling link
		spans2, _ := buildCanonicalGoE2EFixture()
		sub2, ack2 := spans2[4], spans2[8]
		droppedTID := testTraceID(882)
		ack2.Links = []*proxypb.SpanLink{
			{TraceId: sub2.TraceId, SpanId: sub2.SpanId},
			{TraceId: sub2.TraceId, SpanId: sub2.SpanId},
			{TraceId: droppedTID, SpanId: testSpanID(8820)},
			{TraceId: testTraceID(883), SpanId: testSpanID(8830)},
		}
		resAudit := auditExtraSettlementLinks(
			ack2,
			map[SpanKey]*proxypb.TraceSpan{{TraceID: sub2.TraceId, SpanID: sub2.SpanId}: sub2},
			map[string]bool{droppedTID: true},
			true,
		)
		if len(resAudit) != 3 {
			t.Fatalf("expected 3 auditExtraSettlementLinks violations, got %+v", resAudit)
		}

		// Unsampled settlement dropped phase 3: missing reverse, duplicate reverse, bad attr, duplicate link
		unsampParentTP := "00-" + testTraceID(6601) + "-" + testSpanID(66011) + "-00"
		spansU, expU := buildCanonicalGoE2EFixture()
		subU, ackU := spansU[4], spansU[8]
		// Add a second dropped message with ExpectDroppedUnsampled and an extra settlement span linking back to subU improperly.
		expU.Subscribe.Messages = []SubscribeMessageExpectation{
			{
				MessageID:       "msg-1",
				AckID:           "ack-1",
				Data:            []byte("hello"),
				WireTraceparent: expU.Subscribe.WireTraceparents[0],
				ExactlyOnce:     BoolPtr(false),
				ExpectedOutcome: "acked",
				Settlements:     []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 1}},
			},
			{
				WireTraceparent:        unsampParentTP,
				ExpectDroppedUnsampled: true,
				Settlements:            []SettlementExpectation{{Operation: "nack"}},
			},
		}
		expU.Subscribe.MessageCount = 0
		expU.Subscribe.AckIDs = nil
		expU.Subscribe.MessageIDs = nil
		expU.Subscribe.WireTraceparents = nil
		expU.Subscribe.CallbackTraceparents = nil
		expU.Subscribe.ExpectedSettlements = nil
		// Make 3 bad settlement candidates targeting sub: (1) Phase 3 duplicate_link (without duplicate_reverse_link shadowing),
		// (2) duplicate_reverse_link, (3) bad_reverse_link_attr.
		spansUDup, expUDup := buildCanonicalGoE2EFixture()
		subUDup := spansUDup[4]
		expUDup.Subscribe.Messages = []SubscribeMessageExpectation{
			{
				MessageID:       "msg-1",
				AckID:           "ack-1",
				Data:            []byte("hello"),
				WireTraceparent: expUDup.Subscribe.WireTraceparents[0],
				ExactlyOnce:     BoolPtr(false),
				ExpectedOutcome: "acked",
				Settlements:     []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 1}},
			},
			{
				WireTraceparent:        unsampParentTP,
				ExpectDroppedUnsampled: true,
				Settlements:            []SettlementExpectation{{Operation: "ack"}},
			},
		}
		expUDup.Subscribe.MessageCount = 0
		expUDup.Subscribe.AckIDs = nil
		expUDup.Subscribe.MessageIDs = nil
		expUDup.Subscribe.WireTraceparents = nil
		expUDup.Subscribe.CallbackTraceparents = nil
		expUDup.Subscribe.ExpectedSettlements = nil
		badAckDup := makeGoSettlementSpan("p", "s", "ack", 2, 0, false, 11, nil)
		badAckDup.Links = []*proxypb.SpanLink{
			{TraceId: subUDup.TraceId, SpanId: subUDup.SpanId},
			{TraceId: subUDup.TraceId, SpanId: subUDup.SpanId},
		}
		subUDup.Links = append(subUDup.Links,
			&proxypb.SpanLink{TraceId: badAckDup.TraceId, SpanId: badAckDup.SpanId, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("ack")}},
		)
		resPhase3DupLink := Verify(goCaps, append(spansUDup, badAckDup), expUDup)
		var sawPhase3DupForwardMsg bool
		for _, v := range resPhase3DupLink.Failures {
			if v.Code == CodeBrokenBidirectionalLink && v.Rule == "subscribe.settlement.duplicate_link" && strings.Contains(v.Message, "duplicate forward link on ") {
				sawPhase3DupForwardMsg = true
			}
		}
		if !sawPhase3DupForwardMsg {
			t.Fatalf("expected Phase 3 subscribe.settlement.duplicate_link ('duplicate forward link on '), got:\n%s", resPhase3DupLink.Format())
		}

		badNackDupRev := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 12, nil)
		badNackDupRev.Links = []*proxypb.SpanLink{{TraceId: subU.TraceId, SpanId: subU.SpanId}}
		spansU2, expU2 := buildCanonicalGoE2EFixture()
		*expU2.Subscribe = *expU.Subscribe
		subU2 := spansU2[4]
		subU2.Links = append(subU2.Links,
			&proxypb.SpanLink{TraceId: badNackDupRev.TraceId, SpanId: badNackDupRev.SpanId, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("nack")}},
			&proxypb.SpanLink{TraceId: badNackDupRev.TraceId, SpanId: badNackDupRev.SpanId, Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("nack")}},
		)
		resPhase3DupRev := Verify(goCaps, append(spansU2, badNackDupRev), expU2)
		if !hasRule(resPhase3DupRev, "subscribe.settlement.duplicate_reverse_link", CodeBrokenBidirectionalLink) {
			t.Fatalf("expected subscribe.settlement.duplicate_reverse_link, got:\n%s", resPhase3DupRev.Format())
		}

		badNackAttr := makeGoSettlementSpan("p", "s", "nack", 2, 0, false, 13, nil)
		badNackAttr.Links = []*proxypb.SpanLink{{TraceId: subU.TraceId, SpanId: subU.SpanId}}
		spansU3, expU3 := buildCanonicalGoE2EFixture()
		*expU3.Subscribe = *expU.Subscribe
		subU3 := spansU3[4]
		badNackAttr.Links[0] = &proxypb.SpanLink{TraceId: subU3.TraceId, SpanId: subU3.SpanId}
		subU3.Links = append(subU3.Links, &proxypb.SpanLink{
			TraceId:    badNackAttr.TraceId,
			SpanId:     badNackAttr.SpanId,
			Attributes: map[string]*proxypb.AttributeValue{"messaging.operation.name": strAV("wrong-op")},
		})
		resPhase3BadAttr := Verify(goCaps, append(spansU3, badNackAttr), expU3)
		if !hasRule(resPhase3BadAttr, "subscribe.settlement.reverse_link_attr", CodeMissingLinkAttr) {
			t.Fatalf("expected subscribe.settlement.reverse_link_attr, got:\n%s", resPhase3BadAttr.Format())
		}
		_ = ackU

		// C++ ack settlement error substring check (verify.go:9377).
		spansCppEOD, expCppEOD := buildCppStreamingE2EFixture(true, true, true)
		expCppEOD.Subscribe.ExpectedSettlements[0].ErrorSubstring = "not-found-substring"
		resCppErrSub := Verify(cppCaps, spansCppEOD, expCppEOD)
		if resCppErrSub.Passed() || !hasRule(resCppErrSub, "settlement.ack.status.message", CodeWrongStatusMessage) {
			t.Fatalf("expected C++ settlement.ack.status.message wrong status message, got:\n%s", resCppErrSub.Format())
		}
	})

	t.Run("go_flow_control_error_span_missing", func(t *testing.T) {
		resFC := verifySpans(nil, ProfileGoCanonical(), &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{
					ExpectError: true,
					ErrorKind:   PublishErrorFlowControl,
				}},
			},
		})
		if resFC.Passed() || !hasRule(resFC, "publish.flow_control.error_span", CodeMissingSpan) {
			t.Fatalf("expected publish.flow_control.error_span failure, got:\n%s", resFC.Format())
		}
	})

	t.Run("mixed_sampled_and_unsampled_flow_control_leak_dedup", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 2, 1)
		c, fc, batch, wireTP := makeGoPublishMessageSpans("p", "t", "m1", "", 5, 1, pub, 2)
		unsampledParent := "00-" + testTraceID(4199) + "-" + testSpanID(41991) + "-00"
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{
						MessageID:       "m1",
						Data:            []byte("hello"),
						WireTraceparent: wireTP,
					},
					{
						ParentTraceparent:      unsampledParent,
						ExpectDroppedUnsampled: true,
					},
				},
			},
		}
		baseSpans := []*proxypb.TraceSpan{c, fc, batch, pub}
		if resOK := Verify(goCaps, baseSpans, exp); !resOK.Passed() {
			t.Fatalf("mixed sampled + dropped baseline expected PASS, got:\n%s", resOK.Format())
		}

		leakFC := goSpan(testTraceID(4199), testSpanID(41992), testSpanID(41993), "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 2*time.Millisecond)
		resLeak := Verify(goCaps, append(slices.Clone(baseSpans), leakFC), exp)
		if resLeak.Passed() || len(resLeak.Failures) != 1 || resLeak.Failures[0].Rule != "publish.unsampled_dropped" || resLeak.Failures[0].Code != CodeUnexpectedSpan {
			t.Fatalf("expected single publish.unsampled_dropped violation without duplicate no_extra_pubsub_spans, got:\n%s", resLeak.Format())
		}
	})
}

func b2u16(v bool) uint16 {
	if v {
		return 1
	}
	return 0
}

func TestTier5_Phase3LargeMatrix_OrphanFC_And_CppFanoutBounds(t *testing.T) {
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

	t.Run("phase3_gt64_matrix_81_pairs_tiebreak_and_mismatch", func(t *testing.T) {
		spans, exp := buildCanonicalGoE2EFixture()
		msgs := []SubscribeMessageExpectation{{
			MessageID:       "msg-1",
			AckID:           "ack-1",
			Data:            []byte("hello"),
			WireTraceparent: exp.Subscribe.WireTraceparents[0],
			ExactlyOnce:     BoolPtr(false),
			ExpectedOutcome: "acked",
			Settlements:     []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 1}},
		}}
		allSpans := slices.Clone(spans)
		// 9 dropped unsampled ack items x 9 root "s ack" spans = 81 pairs (>64 matrix branch).
		for i := 0; i < 9; i++ {
			tp := fmt.Sprintf("00-%s-%s-00", testTraceID(18100+i), testSpanID(181000+i))
			msgs = append(msgs, SubscribeMessageExpectation{
				WireTraceparent:        tp,
				ExpectDroppedUnsampled: true,
				Settlements:            []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 1}},
			})
			ackSpan := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 200+i, nil)
			allSpans = append(allSpans, ackSpan)
		}
		exp.Subscribe = &SubscribeExpectation{Messages: msgs}

		var baselineFmt string
		for seed := int64(0); seed < 8; seed++ {
			perm := slices.Clone(allSpans)
			rng := rand.New(rand.NewSource(seed))
			rng.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
			res := Verify(goCaps, perm, exp)
			if !res.Passed() {
				t.Fatalf("seed=%d expected >64 Phase 3 matrix PASS, got:\n%s", seed, res.Format())
			}
			if seed == 0 {
				baselineFmt = res.Format()
			} else if res.Format() != baselineFmt {
				t.Fatalf("seed=%d >64 Phase 3 matrix tie-break not deterministic", seed)
			}
		}

		// Negative check on >64 branch: batch_count mismatch on one dropped settlement fails.
		badExp := &TraceExpectation{
			Topic:        exp.Topic,
			Subscription: exp.Subscription,
			Publish:      exp.Publish,
			Subscribe:    &SubscribeExpectation{Messages: slices.Clone(msgs)},
		}
		badExp.Subscribe.Messages[9].Settlements = []SettlementExpectation{{Operation: "ack", ExpectedBatchCount: 7}}
		resBad := Verify(goCaps, slices.Clone(allSpans), badExp)
		if resBad.Passed() || !hasRule(resBad, "settlement.ack.attr.batch_count", CodeWrongAttributeValue) {
			t.Fatalf("expected >64 Phase 3 settlement.ack.attr.batch_count mismatch, got:\n%s", resBad.Format())
		}
	})

	t.Run("go_orphan_fc_empty_parent_span_id", func(t *testing.T) {
		fc := goSpan(testTraceID(18200), testSpanID(182001), "", "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Millisecond)
		fc.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		fc.StatusMessage = "flow control blocked"
		fc.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		res := Verify(goCaps, []*proxypb.TraceSpan{fc}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{
					ExpectError: true,
					ErrorKind:   PublishErrorFlowControl,
				}},
			},
		})
		if res.Passed() || !hasRule(res, "publish.flow_control.parent", CodeWrongParent) {
			t.Fatalf("expected publish.flow_control.parent violation for orphan FC with empty ParentSpanId, got:\n%s", res.Format())
		}
	})

	t.Run("cpp_gt128_fanout_link_bounds_dropped_unsampled", func(t *testing.T) {
		buildCpp129 := func(includeDroppedUnsampled bool) ([]*proxypb.TraceSpan, *TraceExpectation) {
			pub := cppSpan(testTraceID(18500), testSpanID(185000), "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 10*time.Millisecond)
			pub.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":                strAV("p"),
				"messaging.system":              strAV("gcp_pubsub"),
				"messaging.destination.name":    strAV("t"),
				"messaging.batch.message_count": intAV(129),
				"messaging.operation.type":      strAV("publish"),
				"code.function.name":            strAV("BatchSink::AsyncPublish"),
				"gl-cpp.status_code":            strAV("OK"),
			}
			ch0 := cppSpan(pub.TraceId, testSpanID(185101), pub.SpanId, "publish #0", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
			ch1 := cppSpan(pub.TraceId, testSpanID(185102), pub.SpanId, "publish #1", proxypb.SpanKind_SPAN_KIND_CLIENT, time.Millisecond)
			spans := []*proxypb.TraceSpan{pub, ch0, ch1}
			var msgs []PublishMessageExpectation
			for i := 0; i < 129; i++ {
				tid := testTraceID(18600 + i)
				sid := testSpanID(186000 + i*10)
				c := cppSpan(tid, sid, "", "t create", proxypb.SpanKind_SPAN_KIND_PRODUCER, time.Duration(i)*time.Microsecond)
				c.Attributes = map[string]*proxypb.AttributeValue{
					"gcp.project_id":                  strAV("p"),
					"messaging.system":                strAV("gcp_pubsub"),
					"messaging.destination.name":      strAV("t"),
					"code.function.name":              strAV("pubsub::PublisherConnection::Publish"),
					"messaging.operation.type":        strAV("create"),
					"messaging.message.envelope.size": intAV(25),
					"messaging.message.id":            strAV(fmt.Sprintf("msg-%d", i)),
					"gl-cpp.status_code":              strAV("OK"),
				}
				c.Events = []*proxypb.SpanEvent{
					{Name: "gl-cpp.added_to_batch"},
					{Name: "gl-cpp.publish_start"},
					{Name: "gl-cpp.publish_end"},
				}
				c.Links = []*proxypb.SpanLink{{TraceId: pub.TraceId, SpanId: pub.SpanId}}
				fc := cppSpan(tid, testSpanID(186001+i*10), sid, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
				fc.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::FlowControlledPublisherConnection::Publish"), "gl-cpp.status_code": strAV("OK")}
				bc := cppSpan(tid, testSpanID(186002+i*10), sid, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, 0)
				bc.Attributes = map[string]*proxypb.AttributeValue{"code.function.name": strAV("pubsub::BatchingPublisherConnection::Publish"), "gl-cpp.status_code": strAV("OK")}
				spans = append(spans, c, fc, bc)
				if i < 127 {
					ch0.Links = append(ch0.Links, &proxypb.SpanLink{TraceId: c.TraceId, SpanId: c.SpanId})
				} else {
					// Put 2 links on publish #1 when rem=1 (chunkMax=1) -> linkCnt > chunkMax!
					ch1.Links = append(ch1.Links, &proxypb.SpanLink{TraceId: c.TraceId, SpanId: c.SpanId})
				}
				if !includeDroppedUnsampled || i < 128 {
					msgs = append(msgs, PublishMessageExpectation{
						MessageID:       fmt.Sprintf("msg-%d", i),
						WireTraceparent: fmt.Sprintf("00-%s-%s-01", tid, sid),
					})
				}
			}
			if includeDroppedUnsampled {
				msgs = append(msgs, PublishMessageExpectation{
					ParentTraceparent:      "00-" + testTraceID(18999) + "-" + testSpanID(189991) + "-00",
					ExpectDroppedUnsampled: true,
				})
			}
			return spans, &TraceExpectation{
				Topic:                 "projects/p/topics/t",
				AllowExtraPubSubSpans: includeDroppedUnsampled,
				Publish:               &PublishExpectation{Messages: msgs},
			}
		}

		// With anyDroppedUnsampled=true, hits verify.go:2371-2388 (want 0..1 on publish #1, got 2).
		spansDrop, expDrop := buildCpp129(true)
		resDrop := Verify(cppCaps, spansDrop, expDrop)
		if resDrop.Passed() || !hasRule(resDrop, "publish.batch.fanout_link_bounds", CodeMissingLink) {
			t.Fatalf("expected publish.batch.fanout_link_bounds (anyDroppedUnsampled=true), got:\n%s", resDrop.Format())
		}

		// With anyDroppedUnsampled=false, hits verify.go:2358-2370 exactChunk bounds.
		spansExact, expExact := buildCpp129(false)
		resExact := Verify(cppCaps, spansExact, expExact)
		if resExact.Passed() || !hasRule(resExact, "publish.batch.fanout_link_bounds", CodeMissingLink) {
			t.Fatalf("expected publish.batch.fanout_link_bounds (exactChunk), got:\n%s", resExact.Format())
		}
	})
}

func TestTier6_PublishAndSubscribeExpectationInheritanceAndOverrides(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	hasRule := func(res TraceResult, rule string, code ViolationCode) bool {
		for _, f := range res.Failures {
			if f.Rule == rule && (code == "" || f.Code == code) {
				return true
			}
		}
		return false
	}

	t.Run("GAP-A6-3_PublishExpectation_RequireAttributes_merge_and_override", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 2, 19101)
		cs1, fcs1, bs1, tp1 := makeGoPublishMessageSpans("p", "t", "attr-1", "", 5, 19101, pub, 2)
		cs2, fcs2, bs2, tp2 := makeGoPublishMessageSpans("p", "t", "attr-2", "", 5, 19102, pub, 2)
		cs1.Attributes["a"] = strAV("shared-a")
		cs1.Attributes["b"] = strAV("shared-b")
		cs1.Attributes["c"] = strAV("msg1-c")
		cs2.Attributes["a"] = strAV("shared-a")
		cs2.Attributes["b"] = strAV("override-b")

		baseSpans := []*proxypb.TraceSpan{cs1, fcs1, bs1, cs2, fcs2, bs2, pub}
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				RequireAttributes: map[string]AttrMatcher{
					"a": MatchString("shared-a"),
					"b": MatchString("shared-b"),
				},
				Messages: []PublishMessageExpectation{
					{
						MessageID:         "attr-1",
						WireTraceparent:   tp1,
						RequireAttributes: map[string]AttrMatcher{"c": MatchString("msg1-c")},
					},
					{
						MessageID:         "attr-2",
						WireTraceparent:   tp2,
						RequireAttributes: map[string]AttrMatcher{"b": MatchString("override-b")},
					},
				},
			},
		}
		if res := Verify(goCaps, cloneSpans(baseSpans), exp); !res.Passed() {
			t.Fatalf("merged RequireAttributes positive expected PASS, got:\n%s", res.Format())
		}

		// Inherited attribute mismatch on message 0 fails publish.create.attr.custom.a.
		badInherited := cloneSpans(baseSpans)
		badInherited[0].Attributes["a"] = strAV("wrong-a")
		resInh := Verify(goCaps, badInherited, exp)
		if resInh.Passed() || !hasRule(resInh, "publish.create.attr.custom.a", CodeWrongAttributeValue) {
			t.Fatalf("expected publish.create.attr.custom.a failure, got:\n%s", resInh.Format())
		}

		// Per-message extra attribute is preserved (not dropped during merge).
		badPerMsg := cloneSpans(baseSpans)
		badPerMsg[0].Attributes["c"] = strAV("wrong-c")
		resPer := Verify(goCaps, badPerMsg, exp)
		if resPer.Passed() || !hasRule(resPer, "publish.create.attr.custom.c", CodeWrongAttributeValue) {
			t.Fatalf("expected publish.create.attr.custom.c failure, got:\n%s", resPer.Format())
		}

		// Per-message override takes precedence over PublishExpectation.RequireAttributes.
		badOverride := cloneSpans(baseSpans)
		badOverride[3].Attributes["b"] = strAV("shared-b")
		resOvr := Verify(goCaps, badOverride, exp)
		if resOvr.Passed() || !hasRule(resOvr, "publish.create.attr.custom.b", CodeWrongAttributeValue) {
			t.Fatalf("expected override precedence failure publish.create.attr.custom.b, got:\n%s", resOvr.Format())
		}
	})

	t.Run("GAP-A6-4_SubscribeExpectation_ExpectedSettlements_inheritance_with_StrictSettlementOps", func(t *testing.T) {
		sub1, cc1, sc1, pr1, _ := makeGoSubscribeMessageSpans("p", "s", "m-inh-1", "a-inh-1", "", 5, 0, false, 1, "", "nacked", 19201)
		pr1.Events = append(pr1.Events, &proxypb.SpanEvent{Name: "nack called"})
		nack1 := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 19201, []*proxypb.TraceSpan{sub1})

		exp := &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedOutcome:     "nacked",
				ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}},
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-inh-1", MessageID: "m-inh-1"},
				},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{sub1, cc1, sc1, pr1, nack1}, exp); !res.Passed() {
			t.Fatalf("ExpectedSettlements inheritance under StrictSettlementOps=true expected PASS, got:\n%s", res.Format())
		}

		// Inherited ExpectedSettlements enforces nack when actual settlement is ack.
		subWrong, ccW, scW, prW, _ := makeGoSubscribeMessageSpans("p", "s", "m-wrong", "a-wrong", "", 5, 0, false, 1, "", "acked", 19202)
		prW.Events = append(prW.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackWrong := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 19202, []*proxypb.TraceSpan{subWrong})
		resWrong := Verify(goCaps, []*proxypb.TraceSpan{subWrong, ccW, scW, prW, ackWrong}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedOutcome:     "acked",
				ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}},
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-wrong", MessageID: "m-wrong"},
				},
			},
		})
		if resWrong.Passed() || !hasRule(resWrong, "subscribe.settlement.present", CodeMissingSpan) {
			t.Fatalf("expected subscribe.settlement.present failure when inherited nack mismatches ack, got:\n%s", resWrong.Format())
		}

		// Non-empty Messages[i].Settlements is NOT overwritten by SubscribeExpectation.ExpectedSettlements.
		subAck, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", "m-ovr", "a-ovr", "", 5, 0, false, 1, "", "acked", 19203)
		prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: "ack called"})
		ackOvr := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 19203, []*proxypb.TraceSpan{subAck})
		subNack, ccN, scN, prN, _ := makeGoSubscribeMessageSpans("p", "s", "m-inh-2", "a-inh-2", "", 5, 0, false, 1, "", "nacked", 19204)
		prN.Events = append(prN.Events, &proxypb.SpanEvent{Name: "nack called"})
		nackInh := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 19204, []*proxypb.TraceSpan{subNack})
		resMixed := Verify(goCaps, []*proxypb.TraceSpan{subAck, ccA, scA, prA, ackOvr, subNack, ccN, scN, prN, nackInh}, &TraceExpectation{
			Subscription: "projects/p/subscriptions/s",
			Subscribe: &SubscribeExpectation{
				ExactlyOnce:         BoolPtr(false),
				StrictSettlementOps: true,
				ExpectedSettlements: []SettlementExpectation{{Operation: "nack"}},
				Messages: []SubscribeMessageExpectation{
					{AckID: "a-ovr", MessageID: "m-ovr", ExpectedOutcome: "acked", Settlements: []SettlementExpectation{{Operation: "ack"}}},
					{AckID: "a-inh-2", MessageID: "m-inh-2", ExpectedOutcome: "nacked"},
				},
			},
		})
		if !resMixed.Passed() {
			t.Fatalf("non-empty Messages[i].Settlements should not be overwritten, got:\n%s", resMixed.Format())
		}
	})

	t.Run("GAP-A6-5_PublishExpectation_RequireFlowControlSpan_and_RequireBatchingSpan", func(t *testing.T) {
		// 1. Both inherited as false, msg1 overrides both to true.
		pub := makeGoPublishBatchSpan("p", "t", 2, 19301)
		cs0, _, _, tp0 := makeGoPublishMessageSpans("p", "t", "m-none", "", 5, 19301, pub, 2)
		cs1, fc1, batch1, tp1 := makeGoPublishMessageSpans("p", "t", "m-both", "", 5, 19302, pub, 2)
		resInhFalse := Verify(goCaps, []*proxypb.TraceSpan{cs0, cs1, fc1, batch1, pub}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount:             1,
				RequireFlowControlSpan: BoolPtr(false),
				RequireBatchingSpan:    BoolPtr(false),
				Messages: []PublishMessageExpectation{
					{MessageID: "m-none", WireTraceparent: tp0},
					{MessageID: "m-both", WireTraceparent: tp1, RequireFlowControlSpan: BoolPtr(true), RequireBatchingSpan: BoolPtr(true)},
				},
			},
		})
		if !resInhFalse.Passed() {
			t.Fatalf("RequireFlowControlSpan/RequireBatchingSpan false inheritance + true override expected PASS, got:\n%s", resInhFalse.Format())
		}

		// 2. Inherited true, overridden independently per-message: FC=false (Batch inherited true) vs Batch=false (FC inherited true).
		pubAsym := makeGoPublishBatchSpan("p", "t", 2, 19303)
		csA, _, batchA, tpA := makeGoPublishMessageSpans("p", "t", "m-no-fc", "", 5, 19303, pubAsym, 2)
		csB, fcB, _, tpB := makeGoPublishMessageSpans("p", "t", "m-no-batch", "", 5, 19304, pubAsym, 2)
		resAsym := Verify(goCaps, []*proxypb.TraceSpan{csA, batchA, csB, fcB, pubAsym}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount:             1,
				RequireFlowControlSpan: BoolPtr(true),
				RequireBatchingSpan:    BoolPtr(true),
				Messages: []PublishMessageExpectation{
					{MessageID: "m-no-fc", WireTraceparent: tpA, RequireFlowControlSpan: BoolPtr(false)},
					{MessageID: "m-no-batch", WireTraceparent: tpB, RequireBatchingSpan: BoolPtr(false)},
				},
			},
		})
		if !resAsym.Passed() {
			t.Fatalf("independent FC-false and Batch-false overrides expected PASS, got:\n%s", resAsym.Format())
		}

		// 3. Unexpected child span when RequireFlowControlSpan=false or RequireBatchingSpan=false fails traces.no_extra_pubsub_spans.
		pubStrict := makeGoPublishBatchSpan("p", "t", 1, 19305)
		csS, fcS, batchS, tpS := makeGoPublishMessageSpans("p", "t", "m-strict", "", 5, 19305, pubStrict, 1)
		resExtraFC := Verify(goCaps, []*proxypb.TraceSpan{csS, fcS, batchS, pubStrict}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount:             1,
				RequireFlowControlSpan: BoolPtr(false),
				Messages:               []PublishMessageExpectation{{MessageID: "m-strict", WireTraceparent: tpS}},
			},
		})
		if resExtraFC.Passed() || !hasRule(resExtraFC, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
			t.Fatalf("expected extra flow control span failure when RequireFlowControlSpan=false, got:\n%s", resExtraFC.Format())
		}
		resExtraBatch := Verify(goCaps, []*proxypb.TraceSpan{csS, fcS, batchS, pubStrict}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount:          1,
				RequireBatchingSpan: BoolPtr(false),
				Messages:            []PublishMessageExpectation{{MessageID: "m-strict", WireTraceparent: tpS}},
			},
		})
		if resExtraBatch.Passed() || !hasRule(resExtraBatch, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
			t.Fatalf("expected extra batching span failure when RequireBatchingSpan=false, got:\n%s", resExtraBatch.Format())
		}

		// 4. RequireFlowControlSpan=true / RequireBatchingSpan=true override PreValidation default=false.
		cPre, _, _, _ := makeGoPublishMessageSpans("p", "t", "", "", 5, 19306, nil, 1)
		cPre.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		cPre.StatusMessage = "stopped"
		cPre.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		resReqFC := Verify(goCaps, []*proxypb.TraceSpan{cPre}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:           1,
				ExpectError:            true,
				ErrorKind:              PublishErrorPreValidation,
				ErrorSubstring:         "stopped",
				RequireFlowControlSpan: BoolPtr(true),
			},
		})
		if resReqFC.Passed() || !hasRule(resReqFC, "publish.flow_control.present", CodeMissingSpan) {
			t.Fatalf("expected publish.flow_control.present when RequireFlowControlSpan=true on PreValidation error, got:\n%s", resReqFC.Format())
		}
		resReqBatch := Verify(goCaps, []*proxypb.TraceSpan{cPre}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:        1,
				ExpectError:         true,
				ErrorKind:           PublishErrorPreValidation,
				ErrorSubstring:      "stopped",
				RequireBatchingSpan: BoolPtr(true),
			},
		})
		if resReqBatch.Passed() || !hasRule(resReqBatch, "publish.batching.present", CodeMissingSpan) {
			t.Fatalf("expected publish.batching.present when RequireBatchingSpan=true on PreValidation error, got:\n%s", resReqBatch.Format())
		}
	})
}

func TestTier7_VerifyAndQueryAdversarialCoverage(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	hasRule := func(res TraceResult, rule string, code ViolationCode) bool {
		for _, f := range res.Failures {
			if f.Rule == rule && (code == "" || f.Code == code) {
				return true
			}
		}
		return false
	}
	countRule := func(res TraceResult, rule string, code ViolationCode) int {
		n := 0
		for _, f := range res.Failures {
			if f.Rule == rule && (code == "" || f.Code == code) {
				n++
			}
		}
		return n
	}

	t.Run("GAP-A7-T1_TransportSpansValidatedWhenIgnored", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			span *proxypb.TraceSpan
		}{
			{
				name: "invalid_trace_id",
				span: &proxypb.TraceSpan{
					TraceId:                  "00000000000000000000000000000000",
					SpanId:                   testSpanID(1),
					Name:                     "google.pubsub.v1.Publisher/Publish",
					Kind:                     proxypb.SpanKind_SPAN_KIND_CLIENT,
					InstrumentationScopeName: OTelGrpcScopeName,
				},
			},
			{
				name: "invalid_span_id",
				span: &proxypb.TraceSpan{
					TraceId:                  testTraceID(1),
					SpanId:                   "bad",
					Name:                     "google.pubsub.v1.Publisher/Publish",
					Kind:                     proxypb.SpanKind_SPAN_KIND_CLIENT,
					InstrumentationScopeName: OTelGrpcScopeName,
				},
			},
			{
				name: "invalid_parent_span_id",
				span: &proxypb.TraceSpan{
					TraceId:                  testTraceID(1),
					SpanId:                   testSpanID(1),
					ParentSpanId:             "0000000000000000",
					Name:                     "google.pubsub.v1.Publisher/Publish",
					Kind:                     proxypb.SpanKind_SPAN_KIND_CLIENT,
					InstrumentationScopeName: OTelGrpcScopeName,
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				res := Verify(goCaps, []*proxypb.TraceSpan{tc.span}, ExpectNoTraces())
				if res.Passed() || !hasRule(res, "span.transport_ids", CodeInvalidSpanID) {
					t.Fatalf("expected span.transport_ids / CodeInvalidSpanID under !StrictRawSpans, got:\n%s", res.Format())
				}
			})
		}

		// Valid otelgrpc transport span continues to be ignored cleanly.
		validTransport := &proxypb.TraceSpan{
			TraceId:                  testTraceID(2),
			SpanId:                   testSpanID(2),
			Name:                     "google.pubsub.v1.Publisher/Publish",
			Kind:                     proxypb.SpanKind_SPAN_KIND_CLIENT,
			InstrumentationScopeName: OTelGrpcScopeName,
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{validTransport}, ExpectNoTraces()); !res.Passed() {
			t.Fatalf("valid otelgrpc span expected PASS under ExpectNoTraces, got:\n%s", res.Format())
		}
	})

	t.Run("GAP-A7-T2_PublishExpectErrorInheritedIntoNonEmptyMessages", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 1, 2001)
		csErr, fcErr, batchErr, tpErr := makeGoPublishMessageSpans("p", "t", "", "", 5, 2001, pub, 1)
		csErr.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		csErr.StatusMessage = "unavailable"
		csErr.Events = []*proxypb.SpanEvent{
			{Name: "publish start", Attributes: map[string]*proxypb.AttributeValue{"messaging.batch.message_count": intAV(1)}},
			{Name: "exception"},
		}
		exp := &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount:  1,
				ExpectError: true,
				ErrorKind:   PublishErrorRPC,
				Messages:    []PublishMessageExpectation{{WireTraceparent: tpErr}},
			},
		}
		if resOK := Verify(goCaps, []*proxypb.TraceSpan{csErr, fcErr, batchErr, pub}, exp); !resOK.Passed() {
			t.Fatalf("inherited ExpectError=true on error create span expected PASS, got:\n%s", resOK.Format())
		}

		// UNSET status on create fails publish.create.status because ExpectError=true was inherited.
		pub2 := makeGoPublishBatchSpan("p", "t", 1, 2002)
		csUnset, fcUnset, batchUnset, tpUnset := makeGoPublishMessageSpans("p", "t", "m-unset", "", 5, 2002, pub2, 1)
		resFail := Verify(goCaps, []*proxypb.TraceSpan{csUnset, fcUnset, batchUnset, pub2}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount:  1,
				ExpectError: true,
				ErrorKind:   PublishErrorRPC,
				Messages:    []PublishMessageExpectation{{MessageID: "m-unset", WireTraceparent: tpUnset}},
			},
		})
		if resFail.Passed() || !hasRule(resFail, "publish.create.status", CodeWrongStatusCode) {
			t.Fatalf("expected publish.create.status failure via inherited ExpectError=true, got:\n%s", resFail.Format())
		}
	})

	t.Run("GAP-A7-T3_ExpandPublishMessages_SliceLengthInference", func(t *testing.T) {
		tp1 := fmt.Sprintf("00-%s-%s-01", testTraceID(2011), testSpanID(20110))
		tp2 := fmt.Sprintf("00-%s-%s-01", testTraceID(2012), testSpanID(20120))
		cases := []struct {
			name   string
			pubExp *PublishExpectation
		}{
			{"MessageIDs", &PublishExpectation{MessageIDs: []string{"m1", "m2"}}},
			{"WireTraceparents", &PublishExpectation{WireTraceparents: []string{tp1, tp2}}},
			{"BodySizes", &PublishExpectation{BodySizes: []int64{5, 7}}},
			{"ZeroCountBatches", &PublishExpectation{Batches: []PublishBatchExpectation{{MessageCount: 0}, {MessageCount: 0}}}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if expanded := expandPublishMessages(tc.pubExp); len(expanded) != 2 {
					t.Fatalf("expandPublishMessages(%s) len = %d, want 2", tc.name, len(expanded))
				}
				res := Verify(goCaps, nil, &TraceExpectation{
					Topic:   "projects/p/topics/t",
					Publish: tc.pubExp,
				})
				if res.Passed() || countRule(res, "publish.create.present", CodeMissingSpan) != 2 {
					t.Fatalf("%s expected 2 publish.create.present failures on nil spans, got:\n%s", tc.name, res.Format())
				}
			})
		}
	})

	t.Run("GAP-A7-T4_OrphanFlowControlParentTraceparentDisambiguation", func(t *testing.T) {
		badTID := testTraceID(2021)
		goodTID := testTraceID(2022)
		badParentSID := testSpanID(20210)
		goodParentSID := testSpanID(20220)
		parentTP := fmt.Sprintf("00-%s-%s-01", goodTID, goodParentSID)

		fcBad := goSpan(badTID, testSpanID(20211), badParentSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 1*time.Millisecond)
		fcBad.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		fcBad.StatusMessage = "flow control limit"
		fcBad.Events = []*proxypb.SpanEvent{{Name: "exception"}}
		fcBad.Attributes = map[string]*proxypb.AttributeValue{"unexpected.attr": strAV("bad")}

		fcGood := goSpan(goodTID, testSpanID(20221), goodParentSID, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 2*time.Millisecond)
		fcGood.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
		fcGood.StatusMessage = "flow control limit"
		fcGood.Events = []*proxypb.SpanEvent{{Name: "exception"}}

		exp := &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				Messages: []PublishMessageExpectation{{
					ExpectError:       true,
					ErrorKind:         PublishErrorFlowControl,
					ParentTraceparent: parentTP,
				}},
			},
		}
		if res := Verify(goCaps, []*proxypb.TraceSpan{fcBad, fcGood}, exp); !res.Passed() {
			t.Fatalf("ParentTraceparent disambiguation expected fcGood PASS, got:\n%s", res.Format())
		}
	})

	t.Run("GAP-A7-T5_WireTraceparentFlagsMismatchOnMatchedCreateSpan", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 1, 2031)
		cs, fc, batch, _ := makeGoPublishMessageSpans("p", "t", "m-flags", "", 5, 2031, pub, 1)
		// Create span has TraceFlags=1, while wireTP specifies -00 (unsampled).
		unsampledWireTP := fmt.Sprintf("00-%s-%s-00", cs.GetTraceId(), cs.GetSpanId())
		res := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:     1,
				BatchCount:       1,
				MessageIDs:       []string{"m-flags"},
				WireTraceparents: []string{unsampledWireTP},
			},
		})
		if res.Passed() || !hasRule(res, "publish.create.wire_traceparent", CodeBrokenTraceparent) {
			t.Fatalf("expected publish.create.wire_traceparent failure on sampled/unsampled flag mismatch, got:\n%s", res.Format())
		}
	})

	t.Run("GAP-A7-T6_DuplicateFlowControlAndBatchingChildrenRejected", func(t *testing.T) {
		pub := makeGoPublishBatchSpan("p", "t", 1, 2041)
		cs, fc1, batch1, tp := makeGoPublishMessageSpans("p", "t", "m-dup-ch", "", 5, 2041, pub, 1)
		fc2 := goSpan(cs.GetTraceId(), testSpanID(20418), cs.GetSpanId(), "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 2*time.Millisecond)
		batch2 := goSpan(cs.GetTraceId(), testSpanID(20419), cs.GetSpanId(), "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, 2*time.Millisecond)

		res := Verify(goCaps, []*proxypb.TraceSpan{cs, fc1, fc2, batch1, batch2, pub}, &TraceExpectation{
			Topic:                 "projects/p/topics/t",
			AllowExtraPubSubSpans: true,
			Publish: &PublishExpectation{
				MessageCount:     1,
				BatchCount:       1,
				MessageIDs:       []string{"m-dup-ch"},
				WireTraceparents: []string{tp},
			},
		})
		if res.Passed() ||
			!hasRule(res, "publish.flow_control.unique", CodeUnexpectedSpan) ||
			!hasRule(res, "publish.batching.unique", CodeUnexpectedSpan) {
			t.Fatalf("expected publish.flow_control.unique and publish.batching.unique failures, got:\n%s", res.Format())
		}
	})

	t.Run("GAP-A7-T7_InvalidWireTraceparentFormatDiagnostic", func(t *testing.T) {
		res := Verify(goCaps, nil, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				MessageCount:     1,
				WireTraceparents: []string{"not-a-traceparent"},
			},
		})
		if res.Passed() || !hasRule(res, "publish.create.wire_traceparent", CodeBrokenTraceparent) {
			t.Fatalf("expected CodeBrokenTraceparent publish.create.wire_traceparent for malformed W3C string, got:\n%s", res.Format())
		}
	})

	t.Run("GAP-A7-T8_BatchSlotAccounting_RemainingDroppedExpectedAndZeroLinkMissingBatchCount", func(t *testing.T) {
		// (a) Anchored batch exceeding linked(1)+remainingDropped(1)=2 emits Expected="<= 2".
		pub := makeGoPublishBatchSpan("p", "t", 5, 2051)
		cs, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m-anchored", "", 5, 2051, pub, 5)
		unsampledParent := fmt.Sprintf("00-%s-%s-00", testTraceID(2052), testSpanID(20520))
		resExcess := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages: []PublishMessageExpectation{
					{MessageID: "m-anchored", WireTraceparent: tp},
					{ExpectDroppedUnsampled: true, ParentTraceparent: unsampledParent},
				},
			},
		})
		foundExpectedBound := false
		for _, f := range resExcess.Failures {
			if f.Rule == "publish.batch.attr.message_count" && f.Code == CodeWrongAttributeValue && f.Expected == "<= 2" {
				foundExpectedBound = true
				break
			}
		}
		if resExcess.Passed() || !foundExpectedBound {
			t.Fatalf("expected publish.batch.attr.message_count with Expected=\"<= 2\", got:\n%s", resExcess.Format())
		}

		// (b) Zero-link publish span omitting messaging.batch.message_count still consumes 1 dropped slot,
		// preventing false publish.unsampled_batch_slot and traces.no_extra_pubsub_spans failures.
		pubZeroNoCount := makeGoPublishBatchSpan("p", "t", 1, 2053)
		delete(pubZeroNoCount.Attributes, "messaging.batch.message_count")
		resZeroLink := Verify(goCaps, []*proxypb.TraceSpan{pubZeroNoCount}, &TraceExpectation{
			Topic: "projects/p/topics/t",
			Publish: &PublishExpectation{
				BatchCount: 1,
				Messages:   []PublishMessageExpectation{{ExpectDroppedUnsampled: true, ParentTraceparent: unsampledParent}},
			},
		})
		if resZeroLink.Passed() ||
			!hasRule(resZeroLink, "publish.batch.attr.message_count", CodeMissingAttribute) ||
			hasRule(resZeroLink, "publish.unsampled_batch_slot", CodeMissingSpan) ||
			hasRule(resZeroLink, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
			t.Fatalf("0-link publish missing message_count should fail only missing attr and consume 1 dropped slot, got:\n%s", resZeroLink.Format())
		}
	})

	t.Run("GAP-A7-T9_ExpandSubscribeMessages_SliceLengthInference", func(t *testing.T) {
		tp1 := fmt.Sprintf("00-%s-%s-01", testTraceID(2061), testSpanID(20610))
		tp2 := fmt.Sprintf("00-%s-%s-01", testTraceID(2062), testSpanID(20620))
		cb1 := fmt.Sprintf("00-%s-%s-01", testTraceID(2063), testSpanID(20630))
		cb2 := fmt.Sprintf("00-%s-%s-01", testTraceID(2064), testSpanID(20640))
		cases := []struct {
			name   string
			subExp *SubscribeExpectation
		}{
			{"WireTraceparents", &SubscribeExpectation{WireTraceparents: []string{tp1, tp2}}},
			{"CallbackTraceparents", &SubscribeExpectation{CallbackTraceparents: []string{cb1, cb2}}},
			{"AckIDs", &SubscribeExpectation{AckIDs: []string{"a1", "a2"}}},
			{"MessageIDs", &SubscribeExpectation{MessageIDs: []string{"m1", "m2"}}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if expanded := expandSubscribeMessages(tc.subExp); len(expanded) != 2 {
					t.Fatalf("expandSubscribeMessages(%s) len = %d, want 2", tc.name, len(expanded))
				}
				res := Verify(goCaps, nil, &TraceExpectation{
					Subscription: "projects/p/subscriptions/s",
					Subscribe:    tc.subExp,
				})
				if res.Passed() || countRule(res, "subscribe.present", CodeMissingSpan) != 2 {
					t.Fatalf("%s expected 2 subscribe.present failures on nil spans, got:\n%s", tc.name, res.Format())
				}
			})
		}
	})

	t.Run("GAP-A7-T13_QueryHelpers_55CharNoDash_PartialSlash_And_NilSpan", func(t *testing.T) {
		if _, err := ParseTraceparent(strings.Repeat("a", 55)); err == nil || !strings.Contains(err.Error(), "dash-separated parts") {
			t.Fatalf("ParseTraceparent(55*'a') err = %v, want dash-separated parts error", err)
		}

		p1, s1, f1 := ExtractResourceIDs("custom/my-topic", "topics", "")
		if p1 != "" || s1 != "my-topic" || f1 != "" {
			t.Fatalf("ExtractResourceIDs(\"custom/my-topic\", \"topics\", \"\") = (%q, %q, %q), want (\"\", \"my-topic\", \"\")", p1, s1, f1)
		}
		p2, s2, f2 := ExtractResourceIDs("custom/my-topic", "topics", "proj")
		if p2 != "proj" || s2 != "my-topic" || f2 != "projects/proj/topics/my-topic" {
			t.Fatalf("ExtractResourceIDs(\"custom/my-topic\", \"topics\", \"proj\") = (%q, %q, %q)", p2, s2, f2)
		}

		resNil := Verify(goCaps, []*proxypb.TraceSpan{nil}, nil)
		if resNil.Passed() || !hasRule(resNil, "span.non_nil", CodeInvalidSpanID) {
			t.Fatalf("Verify([nil]) expected span.non_nil / CodeInvalidSpanID, got:\n%s", resNil.Format())
		}
	})
}

func TestVerify_IsPubSubSpanNameSchedulerAndPublishHash(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	hasRule := func(res TraceResult, rule string, code ViolationCode) bool {
		for _, f := range res.Failures {
			if f.Rule == rule && (code == "" || f.Code == code) {
				return true
			}
		}
		return false
	}

	for _, tc := range []struct {
		name     string
		spanName string
		wantPass bool
	}{
		{"empty_scope_subscriber_scheduler", "subscriber scheduler", false},
		{"empty_scope_publish_hash", "publish #0", false},
		{"empty_scope_unrelated_ignored", "unrelated.operation", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := makeGoPublishBatchSpan("p", "t", 1, 2101)
			cs, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m1", "", 5, 2101, pub, 1)
			stray := &proxypb.TraceSpan{
				TraceId:                  testTraceID(2102),
				SpanId:                   testSpanID(2102),
				TraceFlags:               1,
				Name:                     tc.spanName,
				Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
				InstrumentationScopeName: "",
			}
			res := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub, stray}, &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					MessageCount:     1,
					BatchCount:       1,
					MessageIDs:       []string{"m1"},
					WireTraceparents: []string{tp},
				},
			})
			if tc.wantPass {
				if !res.Passed() {
					t.Fatalf("expected PASS for unrelated empty-scope span %q, got:\n%s", tc.spanName, res.Format())
				}
				return
			}
			if res.Passed() || !hasRule(res, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
				t.Fatalf("expected traces.no_extra_pubsub_spans failure for %q, got:\n%s", tc.spanName, res.Format())
			}
		})
	}
}

func TestVerify_GoAutoPublishErrorWithBatchingClassifiesAsRPC(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	cs, fc, batch, _ := makeGoPublishMessageSpans("p", "t", "", "", 5, 2111, nil, 1)
	cs.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_ERROR
	cs.StatusMessage = "rpc failed without publish start"
	cs.Events = []*proxypb.SpanEvent{{Name: "exception"}}

	res := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch}, &TraceExpectation{
		Topic:                 "projects/p/topics/t",
		AllowExtraPubSubSpans: true,
		Publish: &PublishExpectation{
			MessageCount:   1,
			ExpectError:    true,
			ErrorKind:      PublishErrorAuto,
			ErrorSubstring: "rpc failed",
		},
	})
	found := false
	for _, f := range res.Failures {
		if f.Rule == "publish.create.event.publish_start" && f.Code == CodeMissingEvent {
			found = true
			break
		}
	}
	if res.Passed() || !found {
		t.Fatalf("expected publish.create.event.publish_start (CodeMissingEvent) via PublishErrorRPC classification, got:\n%s", res.Format())
	}
}

func TestVerify_ListAttrValueMismatchViolationCode(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	for _, tc := range []struct {
		name     string
		key      string
		actual   *proxypb.AttributeValue
		matchOK  AttrMatcher
		mismatch AttrMatcher
	}{
		{
			name:     "Int64List",
			key:      "test.int64_list",
			actual:   intListAV(1, 2),
			matchOK:  MatchInt64List([]int64{1, 2}),
			mismatch: MatchInt64List([]int64{1, 3}),
		},
		{
			name:     "DoubleList",
			key:      "test.double_list",
			actual:   dblListAV(1.5, 2.5),
			matchOK:  MatchDoubleList([]float64{1.5, 2.5}),
			mismatch: MatchDoubleList([]float64{1.5, 3.5}),
		},
		{
			name:     "BoolList",
			key:      "test.bool_list",
			actual:   boolListAV(true, false),
			matchOK:  MatchBoolList([]bool{true, false}),
			mismatch: MatchBoolList([]bool{true, true}),
		},
		{
			name:     "StringList",
			key:      "test.string_list",
			actual:   strListAV("a", "b"),
			matchOK:  MatchStringList([]string{"a", "b"}),
			mismatch: MatchStringList([]string{"a", "c"}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := makeGoPublishBatchSpan("p", "t", 1, 2201)
			cs, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m-list", "", 5, 2201, pub, 1)
			cs.Attributes[tc.key] = tc.actual

			resOK := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub}, &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					MessageCount:      1,
					BatchCount:        1,
					MessageIDs:        []string{"m-list"},
					WireTraceparents:  []string{tp},
					RequireAttributes: map[string]AttrMatcher{tc.key: tc.matchOK},
				},
			})
			if !resOK.Passed() {
				t.Fatalf("%s matching list expected PASS, got:\n%s", tc.name, resOK.Format())
			}

			resFail := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub}, &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					MessageCount:      1,
					BatchCount:        1,
					MessageIDs:        []string{"m-list"},
					WireTraceparents:  []string{tp},
					RequireAttributes: map[string]AttrMatcher{tc.key: tc.mismatch},
				},
			})
			if resFail.Status != StatusFail || len(resFail.Failures) != 1 {
				t.Fatalf("%s mismatch expected 1 failure, got:\n%s", tc.name, resFail.Format())
			}
			if gotCode := resFail.Failures[0].Code; gotCode != CodeWrongAttributeValue {
				t.Fatalf("%s element mismatch ViolationCode = %s, want %s (format:\n%s)",
					tc.name, gotCode, CodeWrongAttributeValue, resFail.Format())
			}
		})
	}
}

func TestVerify_CppUnsampledBlockingPublisherRejectsBogusPublishSpan(t *testing.T) {
	cppCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "cpp", SupportsMonitoring: true}
	unsampledParent := fmt.Sprintf("00-%s-%s-00", testTraceID(9011), testSpanID(90110))
	exp := &TraceExpectation{
		Topic: "projects/p/topics/t",
		Publish: &PublishExpectation{
			Messages: []PublishMessageExpectation{{
				BlockingPublisher:      true,
				ExpectDroppedUnsampled: true,
				ParentTraceparent:      unsampledParent,
			}},
		},
	}

	t.Run("zero_spans_passes", func(t *testing.T) {
		resOK := Verify(cppCaps, nil, exp)
		if !resOK.Passed() {
			t.Fatalf("unsampled C++ BlockingPublisher with 0 spans expected PASS, got:\n%s", resOK.Format())
		}
	})

	t.Run("bogus_zero_link_publish_span_fails", func(t *testing.T) {
		bogusPub := cppSpan(testTraceID(9012), testSpanID(90120), "", "t publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 10*time.Millisecond)
		bogusPub.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                strAV("p"),
			"messaging.system":              strAV("gcp_pubsub"),
			"messaging.destination.name":    strAV("t"),
			"messaging.batch.message_count": intAV(1),
			"messaging.operation.type":      strAV("publish"),
			"code.function.name":            strAV("BatchSink::AsyncPublish"),
			"gl-cpp.status_code":            strAV("OK"),
		}
		resFail := Verify(cppCaps, []*proxypb.TraceSpan{bogusPub}, exp)
		if resFail.Status != StatusFail {
			t.Fatalf("expected FAIL when bogus 0-link publish span emitted for BlockingPublisher, got PASS:\n%s", resFail.Format())
		}
		foundSlotErr := false
		for _, f := range resFail.Failures {
			if f.Rule == "publish.batch.attr.message_count" && f.Code == CodeWrongAttributeValue {
				foundSlotErr = true
				break
			}
		}
		if !foundSlotErr {
			t.Fatalf("expected publish.batch.attr.message_count (CodeWrongAttributeValue) failure, got:\n%s", resFail.Format())
		}
	})
}

func TestVerify_Iter11Tier5AdversarialGaps(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}
	hasRule := func(res TraceResult, rule string, code ViolationCode) bool {
		for _, f := range res.Failures {
			if f.Rule == rule && (code == "" || f.Code == code) {
				return true
			}
		}
		return false
	}

	t.Run("GAP-A11-T1_IsPubSubSpanNameEmptyScopeLiterals", func(t *testing.T) {
		for _, spanName := range []string{
			"publisher flow control",
			"publisher batching",
			"subscriber concurrency control",
			"subscribe scheduler",
		} {
			t.Run(strings.ReplaceAll(spanName, " ", "_"), func(t *testing.T) {
				pub := makeGoPublishBatchSpan("p", "t", 1, 9201)
				cs, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m1", "", 5, 9201, pub, 1)
				stray := &proxypb.TraceSpan{
					TraceId:                  testTraceID(9202),
					SpanId:                   testSpanID(9202),
					TraceFlags:               1,
					Name:                     spanName,
					Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
					InstrumentationScopeName: "",
				}
				res := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub, stray}, &TraceExpectation{
					Topic: "projects/p/topics/t",
					Publish: &PublishExpectation{
						MessageCount:     1,
						BatchCount:       1,
						MessageIDs:       []string{"m1"},
						WireTraceparents: []string{tp},
					},
				})
				if res.Passed() || !hasRule(res, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
					t.Fatalf("expected traces.no_extra_pubsub_spans for empty-scope %q, got:\n%s", spanName, res.Format())
				}
			})
		}
	})

	t.Run("GAP-A11-T2_IsPubSubSpanNameEmptyScopeSuffixes", func(t *testing.T) {
		for _, suffix := range []string{
			" publish",
			" subscribe",
			" process",
			" ack",
			" nack",
			" modack",
			" receive",
		} {
			t.Run(strings.TrimSpace(suffix), func(t *testing.T) {
				pub := makeGoPublishBatchSpan("p", "t", 1, 9211)
				cs, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m1", "", 5, 9211, pub, 1)
				stray := &proxypb.TraceSpan{
					TraceId:                  testTraceID(9212),
					SpanId:                   testSpanID(9212),
					TraceFlags:               1,
					Name:                     "x" + suffix,
					Kind:                     proxypb.SpanKind_SPAN_KIND_INTERNAL,
					InstrumentationScopeName: "",
				}
				res := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub, stray}, &TraceExpectation{
					Topic: "projects/p/topics/t",
					Publish: &PublishExpectation{
						MessageCount:     1,
						BatchCount:       1,
						MessageIDs:       []string{"m1"},
						WireTraceparents: []string{tp},
					},
				})
				if res.Passed() || !hasRule(res, "traces.no_extra_pubsub_spans", CodeUnexpectedSpan) {
					t.Fatalf("expected traces.no_extra_pubsub_spans for empty-scope %q, got:\n%s", "x"+suffix, res.Format())
				}
			})
		}
	})

	t.Run("GAP-A11-T3_DroppedSubtreeLeakedSchedulerAndConcurrency", func(t *testing.T) {
		unsampTID := testTraceID(9301)
		unsampTP := fmt.Sprintf("00-%s-%s-00", unsampTID, testSpanID(93010))
		for _, leakedName := range []string{
			"subscriber concurrency control",
			"subscribe scheduler",
			"subscriber scheduler",
		} {
			t.Run(strings.ReplaceAll(leakedName, " ", "_"), func(t *testing.T) {
				sub, cc, sc, pr, _ := makeGoSubscribeMessageSpans("p", "s", "ms1", "as1", "", 5, 0, false, 1, "", "acked", 9302)
				pr.Events = append(pr.Events, &proxypb.SpanEvent{Name: "ack called"})
				ack := makeGoSettlementSpan("p", "s", "ack", 1, 0, false, 9302, []*proxypb.TraceSpan{sub})
				leaked := goSpan(unsampTID, testSpanID(93099), testSpanID(93010), leakedName, proxypb.SpanKind_SPAN_KIND_INTERNAL, 0)
				res := Verify(goCaps, []*proxypb.TraceSpan{sub, cc, sc, pr, ack, leaked}, &TraceExpectation{
					Subscription:          "projects/p/subscriptions/s",
					AllowExtraPubSubSpans: true,
					Subscribe: &SubscribeExpectation{
						ExactlyOnce: BoolPtr(false),
						Messages: []SubscribeMessageExpectation{
							{AckID: "as1", MessageID: "ms1", BatchCount: 1, Settlements: []SettlementExpectation{{Operation: "ack"}}},
							{WireTraceparent: unsampTP, ExpectDroppedUnsampled: true, ExpectedOutcome: "expired"},
						},
					},
				})
				if res.Passed() || !hasRule(res, "subscribe.unsampled_dropped", CodeUnexpectedSpan) {
					t.Fatalf("expected subscribe.unsampled_dropped for leaked %q, got:\n%s", leakedName, res.Format())
				}
			})
		}
	})

	t.Run("GAP-A11-T4_InferSubIDFromDroppedNackAndModAck", func(t *testing.T) {
		t.Run("nack", func(t *testing.T) {
			unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(9401), testSpanID(94010))
			nackSpan := makeGoSettlementSpan("p", "s", "nack", 1, 0, false, 9401, nil)
			res := Verify(goCaps, []*proxypb.TraceSpan{nackSpan}, &TraceExpectation{
				Subscription: "",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce: BoolPtr(false),
					Messages: []SubscribeMessageExpectation{{
						WireTraceparent:        unsampTP,
						ExpectDroppedUnsampled: true,
						ExpectedOutcome:        "nacked",
						Settlements:            []SettlementExpectation{{Operation: "nack"}},
					}},
				},
			})
			if !res.Passed() {
				t.Fatalf("expected PASS inferring subID from dropped <subID> nack span, got:\n%s", res.Format())
			}
		})

		t.Run("modack", func(t *testing.T) {
			unsampTP := fmt.Sprintf("00-%s-%s-00", testTraceID(9402), testSpanID(94020))
			modackSpan := makeGoSettlementSpan("p", "s", "modack", 1, 10, true, 9402, nil)
			res := Verify(goCaps, []*proxypb.TraceSpan{modackSpan}, &TraceExpectation{
				Subscription: "",
				Subscribe: &SubscribeExpectation{
					ExactlyOnce: BoolPtr(false),
					Messages: []SubscribeMessageExpectation{{
						WireTraceparent:        unsampTP,
						ExpectDroppedUnsampled: true,
						ExpectedOutcome:        "expired",
					}},
				},
			})
			if !res.Passed() {
				t.Fatalf("expected PASS inferring subID from dropped <subID> modack span, got:\n%s", res.Format())
			}
		})
	})

	t.Run("GAP-A11-T5_AnalyzeSubSpanTerminalEvidenceEvents", func(t *testing.T) {
		goProf := ProfileGoCanonical()
		for _, tc := range []struct {
			eventName  string
			conflictOp string
			allowOp    string
		}{
			{eventName: "ack end", conflictOp: "nack", allowOp: "ack"},
			{eventName: "nack end", conflictOp: "ack", allowOp: "nack"},
			{eventName: "gl-cpp.ack_start", conflictOp: "nack", allowOp: "ack"},
			{eventName: "gl-cpp.ack_end", conflictOp: "nack", allowOp: "ack"},
			{eventName: "gl-cpp.message_nack", conflictOp: "ack", allowOp: "nack"},
		} {
			t.Run(tc.eventName, func(t *testing.T) {
				subSpan := &proxypb.TraceSpan{
					Name:   "s subscribe",
					Events: []*proxypb.SpanEvent{{Name: tc.eventName}},
				}
				if sampledSubSpanAllowsSettlementOp(subSpan, SubscribeMessageExpectation{}, tc.conflictOp, false, goProf) {
					t.Fatalf("event %q should disallow conflicting op %q", tc.eventName, tc.conflictOp)
				}
				if !sampledSubSpanAllowsSettlementOp(subSpan, SubscribeMessageExpectation{}, tc.allowOp, false, goProf) {
					t.Fatalf("event %q should allow matching op %q", tc.eventName, tc.allowOp)
				}
			})
		}
	})
}

func TestVerify_SameAttrTypeForStringContainsOneOfAndDouble(t *testing.T) {
	goCaps := &proxypb.GetCapabilitiesResponse{ClientLanguage: "go", SupportsMonitoring: true}

	for _, tc := range []struct {
		name     string
		key      string
		actual   *proxypb.AttributeValue
		matchOK  AttrMatcher
		mismatch AttrMatcher
	}{
		{
			name:     "MatchStringContains",
			key:      "test.contains",
			actual:   strAV("hello world"),
			matchOK:  MatchStringContains("world"),
			mismatch: MatchStringContains("missing"),
		},
		{
			name:     "MatchStringOneOf",
			key:      "test.oneof",
			actual:   strAV("alpha"),
			matchOK:  MatchStringOneOf("alpha", "beta"),
			mismatch: MatchStringOneOf("beta", "gamma"),
		},
		{
			name:     "MatchDouble",
			key:      "test.double",
			actual:   dblAV(3.14),
			matchOK:  MatchDouble(3.14),
			mismatch: MatchDouble(2.71),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := makeGoPublishBatchSpan("p", "t", 1, 9501)
			cs, fc, batch, tp := makeGoPublishMessageSpans("p", "t", "m-attr", "", 5, 9501, pub, 1)
			cs.Attributes[tc.key] = tc.actual

			resOK := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub}, &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					MessageCount:      1,
					BatchCount:        1,
					MessageIDs:        []string{"m-attr"},
					WireTraceparents:  []string{tp},
					RequireAttributes: map[string]AttrMatcher{tc.key: tc.matchOK},
				},
			})
			if !resOK.Passed() {
				t.Fatalf("%s matching value expected PASS, got:\n%s", tc.name, resOK.Format())
			}

			resFail := Verify(goCaps, []*proxypb.TraceSpan{cs, fc, batch, pub}, &TraceExpectation{
				Topic: "projects/p/topics/t",
				Publish: &PublishExpectation{
					MessageCount:      1,
					BatchCount:        1,
					MessageIDs:        []string{"m-attr"},
					WireTraceparents:  []string{tp},
					RequireAttributes: map[string]AttrMatcher{tc.key: tc.mismatch},
				},
			})
			if resFail.Status != StatusFail || len(resFail.Failures) != 1 {
				t.Fatalf("%s mismatch expected 1 failure, got:\n%s", tc.name, resFail.Format())
			}
			if gotCode := resFail.Failures[0].Code; gotCode != CodeWrongAttributeValue {
				t.Fatalf("%s mismatch ViolationCode = %s, want %s (format:\n%s)",
					tc.name, gotCode, CodeWrongAttributeValue, resFail.Format())
			}
		})
	}
}

func TestVerify_SubSpanTerminalEvidenceEventsViaVerifyDispatch(t *testing.T) {
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

	buildCppSub := func(seq int, msgID, ackID, evName string) []*proxypb.TraceSpan {
		tid := testTraceID(9600 + seq)
		sid := testSpanID(96000 + seq*10)
		mkC := func(spanID, parentID, name string, kind proxypb.SpanKind) *proxypb.TraceSpan {
			sp := cppSpan(tid, spanID, parentID, name, kind, time.Duration(seq)*time.Millisecond)
			sp.StatusCode = proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
			sp.Attributes = map[string]*proxypb.AttributeValue{
				"gcp.project_id":             strAV("p"),
				"messaging.system":           strAV("gcp_pubsub"),
				"messaging.destination.name": strAV("s"),
			}
			return sp
		}
		sub := mkC(sid, "", "s subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER)
		sub.Attributes["messaging.operation.type"] = strAV("subscribe")
		sub.Attributes["messaging.message.id"] = strAV(msgID)
		sub.Attributes["messaging.message.envelope.size"] = intAV(25)
		sub.Attributes["messaging.gcp_pubsub.message.ack_id"] = strAV(ackID)
		sub.Attributes["messaging.gcp_pubsub.subscription.exactly_once_delivery"] = boolAV(true)
		sub.Events = []*proxypb.SpanEvent{{Name: evName}}
		cc := mkC(testSpanID(96000+seq*10+1), sid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL)
		sc := mkC(testSpanID(96000+seq*10+2), sid, "subscriber scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL)
		pr := mkC(testSpanID(96000+seq*10+3), sid, "s process", proxypb.SpanKind_SPAN_KIND_INTERNAL)
		pr.Attributes["messaging.message.id"] = strAV(msgID)
		pr.Attributes["messaging.message.envelope.size"] = intAV(25)
		pr.Attributes["messaging.gcp_pubsub.message.ordering_key"] = strAV("")
		pr.Attributes["messaging.operation.type"] = strAV("process")
		return []*proxypb.TraceSpan{sub, cc, sc, pr}
	}

	buildCppSettle := func(seq int, op, ackID string, subSpan *proxypb.TraceSpan) *proxypb.TraceSpan {
		sp := cppSpan(testTraceID(9700+seq), testSpanID(97000+seq), "", "s "+op, proxypb.SpanKind_SPAN_KIND_INTERNAL, time.Duration(20+seq)*time.Millisecond)
		sp.Attributes = map[string]*proxypb.AttributeValue{
			"gcp.project_id":                             strAV("p"),
			"messaging.system":                           strAV("gcp_pubsub"),
			"messaging.destination.name":                 strAV("s"),
			"code.function.name":                         strAV("pubsub::AckHandler::" + op),
			"messaging.gcp_pubsub.message.ack_id":        strAV(ackID),
			"messaging.gcp_pubsub.subscription.template": strAV("projects/p/subscriptions/s"),
			"messaging.operation.type":                   strAV("settle"),
			"messaging.batch.message_count":              intAV(1),
			"gl-cpp.status_code":                         strAV("OK"),
		}
		sp.Links = []*proxypb.SpanLink{{TraceId: subSpan.TraceId, SpanId: subSpan.SpanId}}
		return sp
	}

	for idx, tc := range []struct {
		name       string
		isCPP      bool
		eventName  string
		conflictOp string
		allowOp    string
	}{
		{name: "go_ack_end", eventName: "ack end", conflictOp: "nack", allowOp: "ack"},
		{name: "go_nack_end", eventName: "nack end", conflictOp: "ack", allowOp: "nack"},
		{name: "cpp_ack_start", isCPP: true, eventName: "gl-cpp.ack_start", conflictOp: "nack", allowOp: "ack"},
		{name: "cpp_ack_end", isCPP: true, eventName: "gl-cpp.ack_end", conflictOp: "nack", allowOp: "ack"},
		{name: "cpp_nack_start", isCPP: true, eventName: "gl-cpp.nack_start", conflictOp: "ack", allowOp: "nack"},
		{name: "cpp_nack_end", isCPP: true, eventName: "gl-cpp.nack_end", conflictOp: "ack", allowOp: "nack"},
		{name: "cpp_message_nack", isCPP: true, eventName: "gl-cpp.message_nack", conflictOp: "ack", allowOp: "nack"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msgID := fmt.Sprintf("m-ev-%d", idx)
			ackID := fmt.Sprintf("a-ev-%d", idx)
			exp := &TraceExpectation{
				Subscription: "projects/p/subscriptions/s",
				Subscribe: &SubscribeExpectation{
					Messages: []SubscribeMessageExpectation{{
						MessageID:       msgID,
						AckID:           ackID,
						ExactlyOnce:     BoolPtr(true),
						ExpectedOutcome: "",
					}},
				},
			}

			if tc.isCPP {
				cAllow := buildCppSub(idx*2, msgID, ackID, tc.eventName)
				sAllow := buildCppSettle(idx*2, tc.allowOp, ackID, cAllow[0])
				if resAllow := Verify(cppCaps, append(cAllow, sAllow), exp); !resAllow.Passed() {
					t.Fatalf("%s matching op %q via Verify(cppCaps) expected PASS, got:\n%s", tc.name, tc.allowOp, resAllow.Format())
				}

				cConflict := buildCppSub(idx*2+1, msgID, ackID, tc.eventName)
				sConflict := buildCppSettle(idx*2+1, tc.conflictOp, ackID, cConflict[0])
				resConflict := Verify(cppCaps, append(cConflict, sConflict), exp)
				if resConflict.Passed() || !hasRule(resConflict, "subscribe.settlement.link_incompatible_op", CodeBrokenBidirectionalLink) {
					t.Fatalf("%s conflicting op %q via Verify(cppCaps) expected subscribe.settlement.link_incompatible_op, got:\n%s", tc.name, tc.conflictOp, resConflict.Format())
				}
				return
			}

			seqBase := 9800 + idx*2
			gAllow, ccA, scA, prA, _ := makeGoSubscribeMessageSpans("p", "s", msgID, ackID, "", 5, 0, true, 1, "", "", seqBase)
			delete(gAllow.Attributes, "messaging.gcp_pubsub.result")
			prA.Events = append(prA.Events, &proxypb.SpanEvent{Name: tc.allowOp + " called"})
			modAllow := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, seqBase, []*proxypb.TraceSpan{gAllow})
			settleAllow := makeGoSettlementSpan("p", "s", tc.allowOp, 1, 0, false, seqBase+100, []*proxypb.TraceSpan{gAllow})
			gAllow.Events = []*proxypb.SpanEvent{{Name: tc.eventName}}
			if resAllow := Verify(goCaps, []*proxypb.TraceSpan{gAllow, ccA, scA, prA, modAllow, settleAllow}, exp); !resAllow.Passed() {
				t.Fatalf("%s matching op %q via Verify(goCaps) expected PASS, got:\n%s", tc.name, tc.allowOp, resAllow.Format())
			}

			gConf, ccC, scC, prC, _ := makeGoSubscribeMessageSpans("p", "s", msgID, ackID, "", 5, 0, true, 1, "", "", seqBase+1)
			delete(gConf.Attributes, "messaging.gcp_pubsub.result")
			prC.Events = append(prC.Events, &proxypb.SpanEvent{Name: tc.conflictOp + " called"})
			modConf := makeGoSettlementSpan("p", "s", "modack", 1, 60, true, seqBase+1, []*proxypb.TraceSpan{gConf})
			settleConf := makeGoSettlementSpan("p", "s", tc.conflictOp, 1, 0, false, seqBase+101, []*proxypb.TraceSpan{gConf})
			gConf.Events = []*proxypb.SpanEvent{{Name: tc.eventName}}
			resConflict := Verify(goCaps, []*proxypb.TraceSpan{gConf, ccC, scC, prC, modConf, settleConf}, exp)
			if resConflict.Passed() || !hasRule(resConflict, "subscribe.settlement.link_incompatible_op", CodeBrokenBidirectionalLink) {
				t.Fatalf("%s conflicting op %q via Verify(goCaps) expected subscribe.settlement.link_incompatible_op, got:\n%s", tc.name, tc.conflictOp, resConflict.Format())
			}
		})
	}
}
