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

func TestPublishRetryScenarios_Unit(t *testing.T) {
	m3bAssertUnitMetadata(t, scenarios.ExportPublishRetryScenarios(), []harness.Scenario{
		{ID: "PRET-01", Category: "retry"},
		{ID: "PRET-01b", Category: "retry", RequiredCapabilities: []harness.Capability{harness.CapCustomRetryCodes}},
		{ID: "PRET-02", Category: "retry"},
	})
}

func TestPublishRetryScenarios_Profiles(t *testing.T) {
	list := scenarios.ExportPublishRetryScenarios()
	m3bRunProfileTests(t, list, 3, 5*time.Second,
		map[string]int{"go": 3, "cpp": 3, "rust": 3, "java": 3, "python": 3, "ruby": 3, "php": 3, "node": 3, "dotnet": 3},
		[]m3bFaultCase{
			{prof: "go", fault: "runtime-unimpl-create-pub", sc: "PRET-01", timeout: 3 * time.Second, want: harness.StatusUnsupportedByClient},
			{prof: "go", sc: "PRET-01b", timeout: 3 * time.Second, want: harness.StatusUnsupportedByClient, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsCustomRetryCodes = false }},
		})
	t.Run("missing_custom_retry_cap_others_pass", func(t *testing.T) {
		if rep := m3bRunScenarios(t, m3bMockConfig{profile: "go", capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsCustomRetryCodes = false }}, list, "", 3*time.Second); rep.Summary.Pass != 2 || rep.Summary.UnsupportedByClient != 1 {
			t.Fatalf("unexpected custom retry skip summary=%+v scenarios=%+v", rep.Summary, rep.Scenarios)
		}
	})
}

func TestPublishRetryScenarios_FaultDetection(t *testing.T) {
	m3bRunFaultCases(t, scenarios.ExportPublishRetryScenarios(), []m3bFaultCase{
		{prof: "go", fault: "pret-no-retry-transient", sc: "PRET-01", sub: "Unavailable", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-corrupt-retry-payload", sc: "PRET-01", sub: "data mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-retries-permanent", sc: "PRET-01", sub: "wire attempts", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-swallows-perm", sc: "PRET-01", sub: "expected m-perm to fail", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-inband-unimpl", sc: "PRET-01", sub: "UNIMPLEMENTED", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-ignores-custom-code", sc: "PRET-01b", sub: "DataLoss", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-zero-backoff", sc: "PRET-01b", sub: "retry backoff", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-still-retries-unavail", sc: "PRET-01b", sub: "wire attempts", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-custom-inband-unimpl", sc: "PRET-01b", sub: "UNIMPLEMENTED", timeout: 3 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-ignores-timeout", sc: "PRET-02", sub: "TIMEOUT:", timeout: 500 * time.Millisecond, want: harness.StatusTimeout},
		{prof: "go", fault: "pret-no-retry-fast-fail", sc: "PRET-02", sub: ">= 2 wire Publish", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-leaks-background-retry", sc: "PRET-02", sub: "call count mismatch", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-timeout-returns-ok", sc: "PRET-02", sub: "expected timed-out publish", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", sc: "PRET-02", sub: "invalid negative", timeout: 2 * time.Second, want: harness.StatusFail, capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.Defaults.PublishTotalTimeoutMs = -1 }},
		{prof: "java", fault: "pret-timeout-java-clamps-or-fails", sc: "PRET-02", sub: "MIN_TOTAL_TIMEOUT", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "pret-timeout-inband-unimpl", sc: "PRET-02", sub: "UNIMPLEMENTED", timeout: 2 * time.Second, want: harness.StatusFail},
		{prof: "go", fault: "emit-unexpected-span", sc: "PRET-01", sub: "expected zero spans", timeout: 2 * time.Second, want: harness.StatusFail},
	})
}

func TestPublishRetryScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	m3bRunNoLeakFaults(t, scenarios.ExportPublishRetryScenarios(), 250*time.Millisecond, [2]string{"pret-ignores-timeout", "^PRET-02$"})
}
