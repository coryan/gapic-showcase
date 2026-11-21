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
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
)

func TestPublishOrderingScenarios_Unit(t *testing.T) {
	list := scenarios.ExportPublishOrderingScenarios()
	wantIDs := []string{"PORD-01", "PORD-03", "PORD-04"}
	if len(list) != len(wantIDs) {
		t.Fatalf("publishOrderingScenarios count: want %d, got %d", len(wantIDs), len(list))
	}
	smokeRE, noMatchRE, reg := regexp.MustCompile(`^(PUB-01|PORD-02|PULL-01|PULL-04|TRAC-01|TRAC-03)$`), regexp.MustCompile(`^NOMATCH$`), scenarios.NewRegistry()
	for i, sc := range list {
		if sc.ID != wantIDs[i] || sc.Category != "ordering" || sc.Timeout != 0 || sc.TraceOnly || sc.Run == nil || sc.Name == "" || sc.Feature == "" || sc.Description == "" {
			t.Errorf("invalid ordering scenario[%d]: %+v", i, sc)
		}
		if smokeRE.MatchString(sc.ID) || smokeRE.MatchString(sc.Name) || smokeRE.MatchString(sc.Category) || noMatchRE.MatchString(sc.ID) || noMatchRE.MatchString(sc.Name) || noMatchRE.MatchString(sc.Category) {
			t.Errorf("filter safety violation in %+v", sc)
		}
		reg.MustRegister(sc)
	}
	if !slices.Equal(list[0].RequiredCapabilities, []harness.Capability{harness.CapOrderingOptIn}) || !slices.Equal(list[1].RequiredCapabilities, []harness.Capability{harness.CapResumePublish}) || len(list[2].RequiredCapabilities) != 0 {
		t.Fatalf("unexpected required capabilities: %v %v %v", list[0].RequiredCapabilities, list[1].RequiredCapabilities, list[2].RequiredCapabilities)
	}
	if reg.Len() != len(wantIDs) || scenarios.Len() != 65 {
		t.Fatalf("registry isolation violation: reg=%d default=%d", reg.Len(), scenarios.Len())
	}
}

func TestPublishOrderingScenarios_Profiles(t *testing.T) {
	list := scenarios.ExportPublishOrderingScenarios()
	t.Run("Go and Cpp profiles pass all 3", func(t *testing.T) {
		for _, prof := range []string{"go", "cpp"} {
			rep := pubRunScenarios(t, pubMockConfig{profile: prof}, list, "", 5*time.Second)
			if rep.Summary.Pass != 3 {
				t.Fatalf("profile=%s want 3 PASS, got %+v scenarios=%+v", prof, rep.Summary, rep.Scenarios)
			}
			if diags := pubStatusMap(rep)["PORD-04"].Diagnostics; len(diags) == 0 || !strings.Contains(diags[0], "publish_total_timeout_ms=") {
				t.Errorf("profile=%s PORD-04 missing timeout diagnostic: %v", prof, diags)
			}
		}
	})
	t.Run("Rust skips PORD-01 and passes PORD-03 PORD-04; Java and Python pass PORD-04", func(t *testing.T) {
		byID := pubStatusMap(pubRunScenarios(t, pubMockConfig{profile: "rust"}, list, "", 5*time.Second))
		if byID["PORD-01"].Status != harness.StatusUnsupportedByClient || byID["PORD-03"].Status != harness.StatusPass || byID["PORD-04"].Status != harness.StatusPass {
			t.Fatalf("rust ordering mismatch: %+v", byID)
		}
		for _, prof := range []string{"java", "python"} {
			if rep := pubRunScenarios(t, pubMockConfig{profile: prof}, list, "^PORD-04$", 5*time.Second); rep.Summary.Pass != 1 {
				t.Fatalf("profile=%s PORD-04 failed: %+v", prof, rep.Scenarios)
			}
		}
	})
	t.Run("Missing CapResumePublish and runtime Unimplemented skip PORD-03", func(t *testing.T) {
		for _, cfg := range []pubMockConfig{
			{profile: "go", capsOverride: func(c *proxypb.GetCapabilitiesResponse) { c.SupportsResumePublish = false }},
			{profile: "go", fault: "runtime-unimpl-resume"},
		} {
			if st := pubRunScenarios(t, cfg, list, "^PORD-03$", 3*time.Second).Scenarios[0].Status; st != harness.StatusUnsupportedByClient {
				t.Errorf("cfg=%+v want UNSUPPORTED, got %s", cfg.fault, st)
			}
		}
	})
}

func TestPublishOrderingScenarios_FaultDetection(t *testing.T) {
	list := scenarios.ExportPublishOrderingScenarios()
	for _, tc := range []struct {
		prof, fault, sc, sub string
		timeout              time.Duration
		want                 harness.Status
		negDefaults          bool
	}{
		{"go", "optin-allows-key", "PORD-01", "ordering_key publish", 2 * time.Second, harness.StatusFail, false},
		{"go", "optin-fails-after-rpc", "PORD-01", "expected 0 wire RPCs", 2 * time.Second, harness.StatusFail, false},
		{"go", "optin-inband-unimpl", "PORD-01", "UNIMPLEMENTED", 2 * time.Second, harness.StatusFail, false},
		{"go", "pord-retry-perm-err", "PORD-03", "expected k1-1 to fail", 2 * time.Second, harness.StatusFail, false},
		{"go", "pord-no-pause-on-err", "PORD-03", "to fail", 2 * time.Second, harness.StatusFail, false},
		{"go", "pord-pause-leaks-k2", "PORD-03", "k2-2 while k1 paused", 2 * time.Second, harness.StatusFail, false},
		{"go", "pord-resume-noop", "PORD-03", "key paused", 2 * time.Second, harness.StatusFail, false},
		{"go", "pord-concurrent-same-key", "PORD-03", "expected queued k1-2 to fail", 2 * time.Second, harness.StatusFail, false},
		{"go", "pord-k2-blocked-while-k1-gated", "PORD-03", "TIMEOUT:", 650 * time.Millisecond, harness.StatusTimeout, false},
		{"go", "", "PORD-04", "invalid negative", 2 * time.Second, harness.StatusFail, true},
		{"go", "pord-no-retry-unavail", "PORD-04", "AwaitPublish failed (Unavailable)", 2 * time.Second, harness.StatusFail, false},
		{"go", "pord-k2-or-p2-leapfrogs-p1", "PORD-04", "ordered Publish FIFO mismatch", 2 * time.Second, harness.StatusFail, false},
		{"go", "pord-ignore-total-timeout", "PORD-04", "TIMEOUT:", 1 * time.Second, harness.StatusTimeout, false},
		{"java", "pord-ext-timeout-on-java", "PORD-04", "DeadlineExceeded", 2 * time.Second, harness.StatusFail, false},
		{"go", "emit-unexpected-span", "PORD-01", "expected zero spans", 2 * time.Second, harness.StatusFail, false},
	} {
		name, cfg := tc.sc+"_"+tc.fault, pubMockConfig{profile: tc.prof, fault: tc.fault}
		if tc.negDefaults {
			name = tc.sc + "_negative_default_timeout"
			cfg.capsOverride = func(c *proxypb.GetCapabilitiesResponse) {
				c.Defaults = &proxypb.ClientDefaults{PublishTotalTimeoutMs: -1}
			}
		}
		t.Run(name, func(t *testing.T) {
			rep := pubRunScenarios(t, cfg, list, "^"+tc.sc+"$", tc.timeout)
			sr := rep.Scenarios[0]
			if sr.Status != tc.want || (tc.sub != "" && !strings.Contains(sr.Error, tc.sub)) {
				t.Errorf("%s: want %s (%q), got %s (%q)", name, tc.want, tc.sub, sr.Status, sr.Error)
			}
		})
	}
}

func TestPublishOrderingScenarios_NoGoroutineLeakOnTimeout(t *testing.T) {
	list := scenarios.ExportPublishOrderingScenarios()
	runTO := func() {
		t.Helper()
		if sr := pubRunScenarios(t, pubMockConfig{profile: "go", fault: "pord-ignore-total-timeout"}, list, "^PORD-04$", 250*time.Millisecond).Scenarios[0]; sr.Status != harness.StatusTimeout {
			t.Fatalf("want TIMEOUT, got %s (%s)", sr.Status, sr.Error)
		}
	}
	waitSettled := func(maxG int) {
		t.Helper()
		deadline, tk, buf := time.Now().Add(2*time.Second), time.NewTicker(10*time.Millisecond), make([]byte, 256<<10)
		defer tk.Stop()
		for {
			s := string(buf[:runtime.Stack(buf, true)])
			if !strings.Contains(s, "execBatch") && !strings.Contains(s, "runOrderedWorker") && (maxG < 0 || runtime.NumGoroutine() <= maxG) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("leaked goroutines after StatusTimeout: max=%d now=%d\n%s", maxG, runtime.NumGoroutine(), s)
			}
			<-tk.C
		}
	}
	runTO()
	waitSettled(-1)
	base := runtime.NumGoroutine()
	for range 3 {
		runTO()
	}
	waitSettled(base + 2)
}
