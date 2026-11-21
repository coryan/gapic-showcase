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
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
)

func TestEODScenarios_Unit(t *testing.T) {
	m3dAssertUnitMeta(t, scenarios.ExportEODScenarios(), []harness.Scenario{
		{ID: "EOD-01", Category: "eod", Timeout: 75 * time.Second, RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery}}, {ID: "EOD-01b", Category: "eod", RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery}}, {ID: "EOD-02", Category: "eod", IsolateProxy: true, RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery, harness.CapAckWithResult}},
		{ID: "EOD-03", Category: "eod", RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery, harness.CapAckWithResult}}, {ID: "EOD-03b", Category: "eod", RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery, harness.CapAckWithResult}}, {ID: "EOD-04", Category: "eod", RequiredCapabilities: []harness.Capability{harness.CapExactlyOnceDelivery, harness.CapEodReceiptModack}},
	})
}
func TestEODScenarios_Profiles(t *testing.T) {
	list := scenarios.ExportEODScenarios()
	m3dRunProfileTests(t, list, 6, 5*time.Second, map[string]int{"cpp": 1, "rust": 1, "php": 6})
	m3dRunFaultCases(t, list, []m3dFaultCase{
		{prof: "cpp", fault: "cpp-single-ack", sc: "EOD-03b", timeout: 5 * time.Second, want: harness.StatusPass}, {prof: "go", fault: "no-ack-res", sc: "EOD-01b", timeout: 5 * time.Second, want: harness.StatusPass, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsAckWithResult = false }},
		{prof: "go", fault: "no-rec-modack", sc: "EOD-01b", timeout: 5 * time.Second, want: harness.StatusPass, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsEodReceiptModack = false }},
	})
}
func TestEODScenarios_FaultDetection(t *testing.T) {
	m3dRunFaultCases(t, scenarios.ExportEODScenarios(), []m3dFaultCase{
		m3dGoF("EOD-01", "eod01-no-60s-floor", "invalid EOD ack_deadline_seconds=10"), m3dGoF("EOD-01", "eod01-wrong-is-eod", "expected is_exactly_once=true"), m3dGoF("EOD-01", "eod-auto-settled-interactive", "auto_settled=true"), m3dGoF("EOD-01", "eod-premature-ack", "premature Ack"), m3dGoF("EOD-01", "eod01-ignores-min-15s", "invalid EOD ack_deadline_seconds=60"),
		{prof: "java", fault: "eod01-no-60s-floor", sc: "EOD-01", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout}, {prof: "cpp", fault: "eod01-ignores-min-15s", sc: "EOD-01", sub: "UpdateStreamDeadline=15"}, {prof: "cpp", fault: "eod01-no-periodic", sc: "EOD-01", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout},
		m3dGoF("EOD-01b", "eod-premature-ack", "premature Ack"), m3dGoF("EOD-01b", "eod01b-wrong-eod-phase1", "is_exactly_once=false"), {prof: "rust", fault: "eod01b-ignores-toggle", sc: "EOD-01b", sub: "is_exactly_once=true"}, m3dGoF("EOD-01b", "eod04-skips-receipt-modack", "before receipt ModAck"),
		m3dGoF("EOD-01b", "eod01b-swallows-eod-err", "want status=ACK_STATUS_PERMISSION_DENIED"), {prof: "go", fault: "eod01b-eod-rec-when-false", sc: "EOD-01b", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout}, m3dGoF("EOD-01b", "eod01b-retries-when-false", "expected 1 non-retried Ack"), m3dGoF("EOD-01b", "eod01b-detached-retry-when-false", "expected 1 non-retried Ack"),
		{prof: "go", fault: "eod01b-retries-when-false", sc: "EOD-01b", sub: "expected 1 non-retried Ack", capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsAckWithResult = false }}, {prof: "go", fault: "eod01b-detached-retry-when-false", sc: "EOD-01b", sub: "expected 1 non-retried Ack", capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsAckWithResult = false }},
		{prof: "go", fault: "eod03-no-retry-transient", sc: "EOD-01b", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsAckWithResult = false }}, m3dGoF("EOD-01b", "eod01b-multi-stream", "on 1 stream, got"), m3dGoF("EOD-01b", "emit-unexpected-span", "expected zero spans"),
		m3dGoF("EOD-02", "eod-auto-settled-interactive", "auto_settled=false"), m3dGoF("EOD-02", "eod02-wrong-status", "want status=ACK_STATUS_PERMISSION_DENIED"), m3dGoF("EOD-02", "eod02-aa-wrong-status", "want status=ACK_STATUS_PERMISSION_DENIED"), m3dGoF("EOD-02", "eod02-an-wrong-status", "want status=ACK_STATUS_PERMISSION_DENIED"), m3dGoF("EOD-02", "eod02-nonzero-code-on-success", "grpc_error_code=0"), m3dGoF("EOD-02", "eod01b-keeps-eod-err", "want status=ACK_STATUS_SUCCESS"), m3dGoF("EOD-02", "eod02-skips-noneod-ack", "acked ack_ids set mismatch"), m3dGoF("EOD-02", "eod01-wrong-is-eod", "expected is_exactly_once=true"), m3dGoF("EOD-02", "eod02-auto-wrong-is-eod", "expected is_exactly_once=true"), m3dGoF("EOD-02", "eod01b-wrong-eod-phase1", "expected is_exactly_once=false"),
		m3dGoF("EOD-02", "eod02-auto-missing-flag", "expected auto_settled=true"), m3dGoF("EOD-02", "eod02-unexpected-md", "unexpected MessageDeliveredEvent"), m3dGoF("EOD-02", "eod02-stops-on-settle", "stop on settle"), m3dGoF("EOD-02", "sub-send-eof-with-stopped", "stopped-before-settle"), m3dGoF("EOD-02", "sub-send-eof-only", "settle EOF"), m3dGoF("EOD-02", "wrong-ack-sub", "subscription mismatch"), m3dGoF("EOD-02", "missing-ack-req-params", "x-goog-request-params"),
		{prof: "cpp", fault: "eod02-noneod-raw-status", sc: "EOD-02", sub: "ACK_STATUS_PERMISSION_DENIED"}, {prof: "java", fault: "eod02-noneod-hangs-ack-res", sc: "EOD-02", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout},
		m3dGoF("EOD-03", "eod03-no-retry-transient", "want status=ACK_STATUS_SUCCESS"), m3dGoF("EOD-03", "eod03-no-fallback-retry", "want status=ACK_STATUS_SUCCESS"), m3dGoF("EOD-03", "eod03-retries-permanent", "expected 1 RPC attempt"), m3dGoF("EOD-03", "eod03-nack-no-retry", "want status=ACK_STATUS_SUCCESS"), m3dGoF("EOD-03", "eod03-nack-retries-perm", "expected 1 RPC attempt"), m3dGoF("EOD-03", "eod01-wrong-is-eod", "expected is_exactly_once=true"),
		m3dGoF("EOD-03b", "eod02-auto-missing-flag", "expected auto_settled=true"), m3dGoF("EOD-03b", "eod03b-wrong-status", "want status=ACK_STATUS_INVALID_ACK_ID"), m3dGoF("EOD-03b", "eod03b-no-split-perm", "improperly retried"), m3dGoF("EOD-03b", "eod03b-retries-perm", "improperly retried"), m3dGoF("EOD-03b", "eod01-wrong-is-eod", "expected is_exactly_once=true"), m3dGoF("EOD-03b", "eod03b-retries-ok", "already-succeeded ack_id"), m3dGoF("EOD-03b", "eod03b-unbatched-ack", "containing all permFailedIDs"), m3dGoF("EOD-03b", "eod03-dups-in-retry", "duplicate ack_id"),
		{prof: "cpp", fault: "eod03b-cpp-ack-retries-perm", sc: "EOD-03b", sub: "unexpected per-ID ack counts"}, {prof: "cpp", fault: "eod03b-cpp-modack-retries-perm", sc: "EOD-03b", sub: "improperly retried"}, {prof: "cpp", fault: "eod03b-cpp-sb-wrong-is-eod", sc: "EOD-03b", sub: "expected is_exactly_once=true", timeout: 5 * time.Second}, {prof: "cpp", fault: "cpp-ignores-use-exactly-once", sc: "EOD-01b", sub: "FailedPrecondition"}, {prof: "cpp", fault: "cpp-ignores-use-exactly-once", sc: "EOD-03b", want: harness.StatusUnsupportedByClient},
		m3dGoF("EOD-04", "eod-premature-ack", "premature Ack"), m3dGoF("EOD-04", "eod04-skips-receipt-modack", "wait receipt ModifyAckDeadline"), m3dGoF("EOD-04", "eod04-skips-receipt-modack-slow", "wait receipt ModifyAckDeadline"), m3dGoF("EOD-04", "eod04-no-gate", "delivered before receipt ModifyAckDeadline completed"), m3dGoF("EOD-04", "eod04-no-retry-transient", "expected >= 2 receipt ModAck attempts"), {prof: "go", fault: "eod04-hangs-receipt-modack", sc: "EOD-04", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout},
		m3dGoF("EOD-04", "eod04-retries-perm", "permanent eod04-perm should not be retried"), m3dGoF("EOD-04", "eod04-delivers-perm-failed", "expected only msg-eod04-surv"), m3dGoF("EOD-04", "eod04-perm-first", "expected only msg-eod04-surv"), m3dGoF("EOD-04", "eod04-perm-second", "expected only msg-eod04-surv"), m3dGoF("EOD-04", "eod01-wrong-is-eod", "expected is_exactly_once=true"),
		m3dGoF("EOD-01", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("EOD-01b", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("EOD-02", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("EOD-03", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("EOD-03b", "sub-stop-inband-unimpl", "UNIMPLEMENTED"), m3dGoF("EOD-04", "sub-stop-inband-unimpl", "UNIMPLEMENTED"),
	})
}
func TestEODScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	m3dRunNoLeakFaults(t, scenarios.ExportEODScenarios(), 650*time.Millisecond, [3]string{"cpp", "eod01-no-periodic", "^EOD-01$"}, [3]string{"go", "eod04-hangs-receipt-modack", "^EOD-04$"}, [3]string{"java", "eod02-noneod-hangs-ack-res", "^EOD-02$"})
}

func TestEODScenarios_StreamingUnencodedRequestParams(t *testing.T) {
	m3dRunFaultCases(t, scenarios.ExportEODScenarios(), []m3dFaultCase{
		{prof: "java", fault: "unencoded-streaming-pull", sc: "EOD-01", timeout: 5 * time.Second, want: harness.StatusPass},
		{prof: "python", fault: "unencoded-streaming-pull", sc: "EOD-01", timeout: 5 * time.Second, want: harness.StatusPass},
		{prof: "node", fault: "unencoded-streaming-pull", sc: "EOD-01", timeout: 5 * time.Second, want: harness.StatusPass},
		{prof: "cpp", fault: "unencoded-streaming-pull", sc: "EOD-01", sub: "x-goog-request-params", timeout: 5 * time.Second, want: harness.StatusFail},
	})
}
