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

package scenarios

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"unicode"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
)

// Registry maintains an insertion-ordered, thread-safe collection of
// conformance scenarios keyed by unique Scenario.ID.
type Registry struct {
	mu    sync.RWMutex
	order []string
	byID  map[string]harness.Scenario
}

// DefaultRegistry is the global registry populated with built-in conformance
// scenarios during package initialization.
var DefaultRegistry = NewRegistry()

// NewRegistry returns an empty conformance scenario registry.
func NewRegistry() *Registry {
	return &Registry{
		byID: make(map[string]harness.Scenario),
	}
}

func cloneScenario(sc harness.Scenario) harness.Scenario {
	sc.RequiredCapabilities = slices.Clone(sc.RequiredCapabilities)
	sc.FakeServerOptions = slices.Clone(sc.FakeServerOptions)
	return sc
}

// Register adds sc to the registry while preserving insertion order.
// It rejects nil registries, empty/whitespace IDs, nil Run hooks, negative
// Timeouts, and duplicate scenario IDs.
func (r *Registry) Register(sc harness.Scenario) error {
	if r == nil {
		return fmt.Errorf("scenarios: registry cannot be nil")
	}
	if strings.TrimSpace(sc.ID) == "" || strings.ContainsFunc(sc.ID, unicode.IsSpace) {
		return fmt.Errorf("scenarios: invalid scenario ID %q", sc.ID)
	}
	if sc.Run == nil {
		return fmt.Errorf("scenarios: scenario %q must have non-nil Run function", sc.ID)
	}
	if sc.Timeout < 0 {
		return fmt.Errorf("scenarios: scenario %q has negative timeout %v", sc.ID, sc.Timeout)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byID == nil {
		r.byID = make(map[string]harness.Scenario)
	}
	if _, dup := r.byID[sc.ID]; dup {
		return fmt.Errorf("scenarios: duplicate scenario ID %q", sc.ID)
	}
	r.order = append(r.order, sc.ID)
	r.byID[sc.ID] = cloneScenario(sc)
	return nil
}

// MustRegister registers sc and panics if registration fails.
func (r *Registry) MustRegister(sc harness.Scenario) {
	if err := r.Register(sc); err != nil {
		panic(err)
	}
}

// All returns defensive copies of all registered scenarios in deterministic
// insertion order.
func (r *Registry) All() []harness.Scenario {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]harness.Scenario, 0, len(r.order))
	for _, id := range r.order {
		out = append(out, cloneScenario(r.byID[id]))
	}
	return out
}

// Filter returns all scenarios matching the Go regular expression pattern
// (delegating to harness.FilterScenarios).
func (r *Registry) Filter(pattern string) ([]harness.Scenario, error) {
	return harness.FilterScenarios(r.All(), pattern)
}

// ByID returns a defensive copy of the scenario registered under id.
func (r *Registry) ByID(id string) (harness.Scenario, bool) {
	if r == nil {
		return harness.Scenario{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	sc, ok := r.byID[id]
	if !ok {
		return harness.Scenario{}, false
	}
	return cloneScenario(sc), true
}

// Len returns the number of registered scenarios.
func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.order)
}

// Register adds sc to DefaultRegistry.
func Register(sc harness.Scenario) error { return DefaultRegistry.Register(sc) }

// MustRegister registers sc into DefaultRegistry and panics on error.
func MustRegister(sc harness.Scenario) { DefaultRegistry.MustRegister(sc) }

// All returns defensive copies of all scenarios in DefaultRegistry.
func All() []harness.Scenario { return DefaultRegistry.All() }

// Filter filters DefaultRegistry scenarios by regular expression pattern.
func Filter(pattern string) ([]harness.Scenario, error) { return DefaultRegistry.Filter(pattern) }

// ByID looks up a scenario by ID in DefaultRegistry.
func ByID(id string) (harness.Scenario, bool) { return DefaultRegistry.ByID(id) }

// Len returns the number of scenarios registered in DefaultRegistry.
func Len() int { return DefaultRegistry.Len() }

func init() {
	if err := RegisterDefaults(DefaultRegistry); err != nil {
		panic(err)
	}
}

// RegisterDefaults registers all 65 built-in conformance scenarios into r in
// deterministic order (6 baseline smoke scenarios first, followed by the
// remaining scenario category groups).
func RegisterDefaults(r *Registry) error {
	if r == nil {
		return fmt.Errorf("scenarios: registry cannot be nil")
	}
	groups := [][]harness.Scenario{
		smokeScenarios(),
		publishScenarios(),
		publishOrderingScenarios(),
		publishFlowControlScenarios(),
		compressionHedgingScenarios(),
		publishRetryScenarios(),
		pullScenarios(),
		leaseScenarios(),
		dlqScenarios(),
		subscriberFlowScenarios(),
		eodScenarios(),
		lifecycleScenarios(),
		tracingExtraScenarios(),
		configScenarios(),
	}
	for _, g := range groups {
		for _, sc := range g {
			if err := r.Register(sc); err != nil {
				return err
			}
		}
	}
	return nil
}
