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

func TestCompressionHedgingScenarios_Unit(t *testing.T) {
	reqCmp, reqHdg := []harness.Capability{harness.CapPublishCompression}, []harness.Capability{harness.CapPublishHedging}
	m3bAssertUnitMetadata(t, scenarios.ExportCompressionHedgingScenarios(), []harness.Scenario{
		{ID: "CMP-01", Category: "compression", RequiredCapabilities: reqCmp},
		{ID: "HDG-01", Category: "hedging", IsolateProxy: true, RequiredCapabilities: reqHdg},
		{ID: "HDG-02", Category: "hedging", IsolateProxy: true, RequiredCapabilities: reqHdg},
		{ID: "HDG-03", Category: "hedging", RequiredCapabilities: reqHdg},
		{ID: "HDG-04", Category: "hedging", RequiredCapabilities: reqHdg},
	})
}

func TestCompressionHedgingScenarios_Profiles(t *testing.T) {
	m3bRunProfileTests(t, scenarios.ExportCompressionHedgingScenarios(), 5, 8*time.Second,
		map[string]int{"go": 5, "java": 5, "cpp": 1, "ruby": 1, "php": 1, "dotnet": 1, "rust": 4, "node": 4},
		[]m3bFaultCase{
			{prof: "go", fault: "runtime-unimpl-create-pub", sc: "CMP-01", timeout: 3 * time.Second, want: harness.StatusUnsupportedByClient},
			{prof: "go", fault: "runtime-unimpl-create-pub", sc: "HDG-01", timeout: 3 * time.Second, want: harness.StatusUnsupportedByClient},
		})
}

func TestCompressionHedgingScenarios_FaultDetection(t *testing.T) {
	m3bRunFaultCases(t, scenarios.ExportCompressionHedgingScenarios(), []m3bFaultCase{
		{prof: "go", sc: "CMP-01", sub: "disabled by default", timeout: 2 * time.Second, want: harness.StatusFail, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.Defaults.PublishCompressionEnabledByDefault = true }},
		{prof: "go", sc: "CMP-01", sub: "defaults compression algorithm mismatch", timeout: 2 * time.Second, want: harness.StatusFail, capsOverride: func(c *proxypb.GetCapabilitiesResponse) {
			c.Defaults.PublishCompressionAlgorithm = proxypb.CompressionAlgorithm_COMPRESSION_ALGORITHM_DEFLATE
		}},
		{prof: "go", fault: "cmp-default-on", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "cmp-ignores-disable", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "java", fault: "cmp-ignores-threshold", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "cmp-compresses-below", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "cmp-no-compress-above", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "cmp-wrong-boundary", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "cpp", fault: "cmp-wrong-boundary", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "cmp-wrong-codec", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "cpp", fault: "cmp-wrong-codec", sc: "CMP-01", sub: "compression mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-accepts-invalid-go", sc: "HDG-01", sub: "expected invalid hedging settings", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "java", fault: "hdg-accepts-invalid-go", sc: "HDG-01", sub: "expected invalid hedging settings", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-invalid-sends-rpc", sc: "HDG-01", sub: "unexpected call after", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "rust", fault: "hdg-wrong-clamp-rust", sc: "HDG-01", sub: "Rust hedged too early", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "rust", fault: "hdg-wrong-clamp-refill-rust", sc: "HDG-01", sub: "returned prematurely", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-init-nonzero", sc: "HDG-01", sub: "returned prematurely", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-refill-too-fast", sc: "HDG-01", sub: "4 OKs below 1 token", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-ignores-max-tokens", sc: "HDG-01", sub: "max_tokens=1 cap", timeout: 4 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg01-slow6-zero-tel", sc: "HDG-01", sub: "expected hedge attempt >= 1", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-no-cancel-loser", sc: "HDG-01", sub: "did not cancel within 2s", timeout: 5 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-never-refills", sc: "HDG-01", sub: "TIMEOUT:", timeout: 650 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "hdg-inband-unimpl", sc: "HDG-01", sub: "UNIMPLEMENTED", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-no-hedge", sc: "HDG-02", sub: "TIMEOUT:", timeout: 500 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "hdg-no-cancel-loser", sc: "HDG-02", sub: "did not cancel within 2s", timeout: 4 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-loser-not-canceled", sc: "HDG-02", sub: "not canceled by client", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-slow-cancel-loser", sc: "HDG-02", sub: "after winner", timeout: 4 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-no-cancel-fast-primary", sc: "HDG-02", sub: "fast primary triggered", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-fails-losing-hedge-err", sc: "HDG-02", sub: "losing hedge failed", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-all-fail-returns-ok", sc: "HDG-02", sub: "all-failed hedged publish", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-all-fail-inband-unimpl", sc: "HDG-02", sub: "UNIMPLEMENTED", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-hang-all", sc: "HDG-02", sub: "TIMEOUT:", timeout: 900 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "hdg-wrong-msgid", sc: "HDG-02", sub: "message_id mismatch", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-no-cancel-loser", sc: "HDG-03", sub: "did not cancel within 2s", timeout: 4 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-missing-telemetry", sc: "HDG-03", sub: "message_id mismatch", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-wrong-attempt-seq", sc: "HDG-03", sub: "message_id mismatch", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-nil-start-time", sc: "HDG-03", sub: "publish_start_time is nil", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-epoch-zero-start-time", sc: "HDG-03", sub: "zero/epoch", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-divergent-start-time", sc: "HDG-03", sub: "publish_start_time differs", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "hdg-ord-allows-go", sc: "HDG-04", sub: "expected hedging + ordering conflict", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "rust", fault: "hdg-ord-hedges-keyed-rust", sc: "HDG-04", sub: "returned prematurely", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "rust", fault: "hdg-ord-keyed-nonzero-tel", sc: "HDG-04", sub: "hedged unexpectedly", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "rust", fault: "hdg-ord-no-unkeyed-rust", sc: "HDG-04", sub: "TIMEOUT:", timeout: 500 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "hdg-ord-inband-unimpl", sc: "HDG-04", sub: "UNIMPLEMENTED", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "emit-unexpected-span", sc: "CMP-01", sub: "expected zero spans", timeout: 2 * time.Second, want: harness.StatusFail},
	})
}

func TestCompressionHedgingScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	m3bRunNoLeakFaults(t, scenarios.ExportCompressionHedgingScenarios(), 250*time.Millisecond,
		[2]string{"hdg-no-hedge", "^HDG-02$"},
		[2]string{"hdg-hang-all", "^HDG-02$"},
		[2]string{"hdg-no-cancel-loser", "^HDG-02$"},
	)
}
