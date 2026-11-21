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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/traces"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type traceStore struct {
	mu    sync.Mutex
	spans []*proxypb.TraceSpan
}

func (s *traceStore) reset() {
	s.mu.Lock()
	s.spans = nil
	s.mu.Unlock()
}

func (s *traceStore) add(sp ...*proxypb.TraceSpan) {
	s.mu.Lock()
	for _, item := range sp {
		if item != nil {
			s.spans = append(s.spans, item)
		}
	}
	s.mu.Unlock()
}

func (s *traceStore) drain() []*proxypb.TraceSpan {
	s.mu.Lock()
	out := slices.Clone(s.spans)
	s.spans = nil
	s.mu.Unlock()
	return out
}

func randHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	b[0] |= 1
	return hex.EncodeToString(b)
}

func parseTPOrNew(tp string) (string, string) {
	if p, err := traces.ParseTraceparent(tp); err == nil {
		return p.TraceID, p.SpanID
	}
	return randHex(16), ""
}

func splitRes(full string) (proj, short string) {
	if parts := strings.Split(full, "/"); len(parts) >= 4 {
		return parts[1], parts[3]
	}
	return "conformance-project", full
}

func mkAttrs(kv ...any) map[string]*proxypb.AttributeValue {
	m := make(map[string]*proxypb.AttributeValue, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		k := kv[i].(string)
		switch v := kv[i+1].(type) {
		case string:
			m[k] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_StringValue{StringValue: v}}
		case int64:
			m[k] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_Int64Value{Int64Value: v}}
		case bool:
			m[k] = &proxypb.AttributeValue{Value: &proxypb.AttributeValue_BoolValue{BoolValue: v}}
		}
	}
	return m
}

func baseSpanAttrs(proj, topicOrSub string, kv ...any) map[string]*proxypb.AttributeValue {
	m := mkAttrs("gcp.project_id", proj, "messaging.system", "gcp_pubsub", "messaging.destination.name", topicOrSub)
	maps.Copy(m, mkAttrs(kv...))
	return m
}

func newTraceSpan(cppMode bool, tid, sid, psid, name string, kind proxypb.SpanKind, sc proxypb.SpanStatusCode, attrs map[string]*proxypb.AttributeValue, evs []*proxypb.SpanEvent, links []*proxypb.SpanLink) *proxypb.TraceSpan {
	now, scope := time.Now(), traces.GoPubSubScopeName
	if cppMode {
		scope = traces.CppScopeName
	}
	return &proxypb.TraceSpan{
		TraceId: tid, SpanId: sid, ParentSpanId: psid, TraceFlags: 1, Name: name, Kind: kind, StatusCode: sc,
		StartTime: timestamppb.New(now), EndTime: timestamppb.New(now.Add(time.Millisecond)),
		InstrumentationScopeName: scope, Attributes: attrs, Events: evs, Links: links,
	}
}

type pubSpanSet struct {
	pubSpan *proxypb.TraceSpan
	items   []pubItemSpans
	grpc    *proxypb.TraceSpan
}

type pubItemSpans struct {
	create, fc, batch *proxypb.TraceSpan
	wireTP            string
}

func buildPublishSpans(cppMode bool, faults map[string]bool, topic string, batch []*asyncPubItem, emit bool) *pubSpanSet {
	if !emit {
		return &pubSpanSet{items: make([]pubItemSpans, len(batch))}
	}
	proj, topicID := splitRes(topic)
	res := &pubSpanSet{items: make([]pubItemSpans, len(batch))}

	pubKind := proxypb.SpanKind_SPAN_KIND_INTERNAL
	pubSC := proxypb.SpanStatusCode_SPAN_STATUS_CODE_UNSET
	pubAttrs := baseSpanAttrs(proj, topicID, "messaging.batch.message_count", int64(len(batch)))
	if cppMode {
		pubKind = proxypb.SpanKind_SPAN_KIND_CLIENT
		pubSC = proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK
		maps.Copy(pubAttrs, mkAttrs("code.function.name", "BatchSink::AsyncPublish", "messaging.operation.type", "publish", "gl-cpp.status_code", "OK"))
	} else {
		maps.Copy(pubAttrs, mkAttrs("gcp.resource.name", topic, "code.function", "publishMessageBundle"))
	}
	pubParent := ""
	if faults["wrong-parent-span-id"] {
		pubParent = randHex(8)
	}
	res.pubSpan = newTraceSpan(cppMode, randHex(16), randHex(8), pubParent, topicID+" publish", pubKind, pubSC, pubAttrs, nil, nil)

	for i, it := range batch {
		ptp, tpErr := traces.ParseTraceparent(it.parentTP)
		unsampled := it.parentTP != "" && (tpErr != nil || !ptp.IsSampled())
		tid, psid := parseTPOrNew(it.parentTP)
		if faults["wrong-create-parent-id"] {
			psid = randHex(8)
		}
		csid := randHex(8)

		var item pubItemSpans
		wireTID := tid
		if faults["wrong-trace-id"] {
			wireTID = randHex(16)
		}
		wireSID := csid
		if faults["wrong-wire-span-id"] {
			wireSID = randHex(8)
		}
		flag := "01"
		if unsampled || faults["unsampled-traces"] {
			flag = "00"
		}
		if it.enableMon && !faults["missing-traces"] {
			item.wireTP = fmt.Sprintf("00-%s-%s-%s", wireTID, wireSID, flag)
		}

		if !unsampled && !faults["missing-spans"] && !faults["missing-traces"] {
			createKind := proxypb.SpanKind_SPAN_KIND_PRODUCER
			if faults["wrong-span-kind"] {
				createKind = proxypb.SpanKind_SPAN_KIND_CLIENT
			}
			createName := topicID + " create"
			if faults["invalid-span-name"] {
				createName = "broken.create.span"
			}
			if cppMode {
				item.create = newTraceSpan(true, tid, csid, psid, createName, createKind, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
					baseSpanAttrs(proj, topicID, "code.function.name", "pubsub::PublisherConnection::Publish", "messaging.operation.type", "create", "messaging.message.envelope.size", int64(20+len(it.data)+len(it.key)), "gl-cpp.status_code", "OK"),
					[]*proxypb.SpanEvent{{Name: "gl-cpp.added_to_batch"}, {Name: "gl-cpp.publish_start"}, {Name: "gl-cpp.publish_end"}},
					[]*proxypb.SpanLink{{TraceId: res.pubSpan.TraceId, SpanId: res.pubSpan.SpanId}})
				item.fc = newTraceSpan(true, tid, randHex(8), csid, "publisher flow control", proxypb.SpanKind_SPAN_KIND_CLIENT, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, mkAttrs("code.function.name", "pubsub::FlowControlledPublisherConnection::Publish"), nil, nil)
				item.batch = newTraceSpan(true, tid, randHex(8), csid, "publisher batching", proxypb.SpanKind_SPAN_KIND_CLIENT, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK, mkAttrs("code.function.name", "pubsub::BatchingPublisherConnection::Publish"), nil, nil)
			} else {
				var createLinks []*proxypb.SpanLink
				if !faults["missing-links"] && !faults["missing-reverse-link"] {
					linkAttrs := mkAttrs("messaging.operation.name", "publish")
					if faults["missing-link-attr"] {
						linkAttrs = nil
					}
					createLinks = []*proxypb.SpanLink{{TraceId: res.pubSpan.TraceId, SpanId: res.pubSpan.SpanId, Attributes: linkAttrs}}
				}
				evs := []*proxypb.SpanEvent{{Name: "publish start", Attributes: mkAttrs("messaging.batch.message_count", int64(len(batch)))}, {Name: "publish end"}}
				if faults["missing-events"] {
					evs = nil
				}
				item.create = newTraceSpan(false, tid, csid, psid, createName, createKind, 0,
					baseSpanAttrs(proj, topicID, "gcp.resource.name", topic, "code.function", "Publish", "messaging.message.body.size", int64(len(it.data)), "messaging.gcp_pubsub.message.ordering_key", it.key),
					evs, createLinks)
				item.fc = newTraceSpan(false, tid, randHex(8), csid, "publisher flow control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil)
				item.batch = newTraceSpan(false, tid, randHex(8), csid, "publisher batching", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil)
			}
			if !faults["missing-links"] {
				res.pubSpan.Links = append(res.pubSpan.Links, &proxypb.SpanLink{TraceId: tid, SpanId: csid})
			}
			if faults["unsampled-traces"] {
				item.create.TraceFlags, item.fc.TraceFlags, item.batch.TraceFlags, res.pubSpan.TraceFlags = 0, 0, 0, 0
			}
		}
		res.items[i] = item
	}
	if cppMode && !faults["missing-spans"] && !faults["missing-traces"] {
		res.grpc = newTraceSpan(true, randHex(16), randHex(8), "", "google.pubsub.v1.Publisher/Publish", proxypb.SpanKind_SPAN_KIND_CLIENT, 0, nil, nil, nil)
	}
	return res
}

func emitPublishSpans(store *traceStore, set *pubSpanSet, faults map[string]bool, msgIDs []string) {
	if set == nil || faults["missing-spans"] || faults["missing-traces"] {
		return
	}
	store.add(set.pubSpan, set.grpc)
	for i, item := range set.items {
		if item.create == nil {
			continue
		}
		item.create.Attributes["messaging.message.id"] = mkAttrs("k", msgIDs[i])["k"]
		store.add(item.fc, item.batch, item.create)
	}
}

func buildSubscribeSpansOnDelivery(cppMode bool, faults map[string]bool, store *traceStore, sub, msgID, ackID string, data []byte, attrs map[string]string, emit bool) (*proxypb.TraceSpan, string) {
	if !emit {
		return nil, ""
	}
	wtp, _ := traces.ExtractTraceparent(attrs)
	tid, psid := parseTPOrNew(wtp)
	if faults["wrong-trace-id"] {
		tid = randHex(16)
	}
	if faults["broken-sub-parent"] {
		psid = randHex(8)
	}
	ssid, procSID := randHex(8), randHex(8)
	flag := "01"
	if faults["unsampled-traces"] {
		flag = "00"
	}
	cbSID := procSID
	if faults["wrong-cb-tp"] {
		cbSID = randHex(8)
	}
	cbTP := fmt.Sprintf("00-%s-%s-%s", tid, cbSID, flag)
	if faults["missing-spans"] || faults["missing-traces"] {
		return nil, cbTP
	}

	proj, subID := splitRes(sub)
	sa := baseSpanAttrs(proj, subID, "messaging.message.id", msgID, "messaging.gcp_pubsub.message.ack_id", ackID)
	var subSpan *proxypb.TraceSpan
	if cppMode {
		maps.Copy(sa, mkAttrs("messaging.operation.type", "subscribe", "messaging.message.envelope.size", int64(20+len(data)+len(msgID)), "messaging.gcp_pubsub.subscription.exactly_once_delivery", false, "messaging.gcp_pubsub.result", "ack"))
		subSpan = newTraceSpan(true, tid, ssid, psid, subID+" subscribe", proxypb.SpanKind_SPAN_KIND_CONSUMER, 0, sa, []*proxypb.SpanEvent{{Name: "gl-cpp.ack_start"}, {Name: "gl-cpp.message_ack"}, {Name: "gl-cpp.ack_end"}}, nil)
		for _, n := range []struct{ id, nm string }{{randHex(8), "subscriber concurrency control"}, {randHex(8), "subscriber scheduler"}, {procSID, subID + " process"}} {
			store.add(newTraceSpan(true, tid, n.id, ssid, n.nm, proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, mkAttrs("messaging.system", "gcp_pubsub"), nil, nil))
		}
		return subSpan, cbTP
	}

	maps.Copy(sa, mkAttrs("gcp.resource.name", sub, "messaging.message.body.size", int64(len(data)), "messaging.gcp_pubsub.exactly_once_delivery", false, "messaging.gcp_pubsub.message.ordering_key", "", "messaging.batch.message_count", int64(1), "code.function", "receive", "messaging.gcp_pubsub.result", "acked"))
	procKind := proxypb.SpanKind_SPAN_KIND_CONSUMER
	if faults["wrong-span-kind"] {
		procKind = proxypb.SpanKind_SPAN_KIND_INTERNAL
	}
	subSpan = newTraceSpan(false, tid, ssid, psid, subID+" subscribe", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, sa, nil, nil)
	procEvs := []*proxypb.SpanEvent{{Name: "ack called"}}
	if faults["missing-events"] {
		procEvs = nil
	}
	ccSID, schedSID := randHex(8), randHex(8)
	if faults["dup-span-id"] {
		schedSID = ccSID
	}
	ccSpan := newTraceSpan(false, tid, ccSID, ssid, "subscriber concurrency control", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil)
	schedSpan := newTraceSpan(false, tid, schedSID, ssid, "subscribe scheduler", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0, nil, nil, nil)
	procSpan := newTraceSpan(false, tid, procSID, ssid, subID+" process", procKind, 0,
		baseSpanAttrs(proj, subID, "gcp.resource.name", sub, "messaging.message.id", msgID, "messaging.message.body.size", int64(len(data)), "messaging.gcp_pubsub.message.ordering_key", "", "messaging.operation.type", "process"),
		procEvs, nil)
	if faults["unsampled-traces"] {
		subSpan.TraceFlags, ccSpan.TraceFlags, schedSpan.TraceFlags, procSpan.TraceFlags = 0, 0, 0, 0
	}
	store.add(ccSpan, schedSpan, procSpan)
	return subSpan, cbTP
}

func emitSubscribeAckSpans(cppMode bool, faults map[string]bool, store *traceStore, sub, ackID string, subSpan *proxypb.TraceSpan) {
	if subSpan == nil || faults["missing-spans"] || faults["missing-traces"] {
		return
	}
	proj, subID := splitRes(sub)
	if cppMode {
		ackSpan := newTraceSpan(true, randHex(16), randHex(8), "", subID+" ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, proxypb.SpanStatusCode_SPAN_STATUS_CODE_OK,
			baseSpanAttrs(proj, subID, "code.function.name", "pubsub::AckHandler::ack", "messaging.gcp_pubsub.message.ack_id", ackID, "messaging.gcp_pubsub.subscription.template", sub, "messaging.operation.type", "settle", "messaging.gcp_pubsub.message.delivery_attempt", int64(1), "gl-cpp.status_code", "OK"),
			nil, []*proxypb.SpanLink{{TraceId: subSpan.TraceId, SpanId: subSpan.SpanId}})
		store.add(ackSpan, subSpan)
		return
	}
	if !faults["missing-events"] {
		subSpan.Events = []*proxypb.SpanEvent{{Name: "ack start", Attributes: mkAttrs("messaging.batch.message_count", int64(1))}, {Name: "ack end"}}
	}
	if !faults["missing-sub-ack-span"] {
		var ackLinks []*proxypb.SpanLink
		if !faults["missing-links"] {
			ackLinks = []*proxypb.SpanLink{{TraceId: subSpan.TraceId, SpanId: subSpan.SpanId}}
		}
		ackParent := ""
		if faults["wrong-parent-span-id"] {
			ackParent = randHex(8)
		}
		ackSpan := newTraceSpan(false, randHex(16), randHex(8), ackParent, subID+" ack", proxypb.SpanKind_SPAN_KIND_INTERNAL, 0,
			baseSpanAttrs(proj, subID, "gcp.resource.name", sub, "messaging.batch.message_count", int64(1), "code.function", "sendAck"),
			nil, ackLinks)
		if faults["unsampled-traces"] {
			ackSpan.TraceFlags = 0
		}
		if !faults["missing-links"] && !faults["missing-reverse-link"] {
			var linkAttrs map[string]*proxypb.AttributeValue
			if !faults["missing-link-attr"] {
				linkAttrs = mkAttrs("messaging.operation.name", "ack")
			}
			subSpan.Links = []*proxypb.SpanLink{{TraceId: ackSpan.TraceId, SpanId: ackSpan.SpanId, Attributes: linkAttrs}}
		}
		store.add(ackSpan)
	}
	store.add(subSpan)
}

func emitUnexpectedDisabledSpan(store *traceStore, topic string) {
	proj, topicID := splitRes(topic)
	store.add(newTraceSpan(false, randHex(16), randHex(8), "", topicID+" create", proxypb.SpanKind_SPAN_KIND_PRODUCER, 0,
		baseSpanAttrs(proj, topicID, "gcp.resource.name", topic), nil, nil))
}
