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

// Package scenarios provides an extensible, deterministic registry of Pub/Sub
// data-plane conformance scenarios (65 scenarios across 13 categories).
//
// During package initialization, registry.go calls RegisterDefaults to populate
// DefaultRegistry with the 6 baseline smoke scenarios (`PUB-01`, `PORD-02`,
// `PULL-01`, `PULL-04`, `TRAC-01`, `TRAC-03`) first, followed deterministically by
// publish, ordering, flow_control, compression, hedging, retry, pull, lease, dlq,
// eod, lifecycle, traces, and config suites.
//
// Callers needing isolated subsets can construct an empty Registry via
// NewRegistry and register selected scenarios or call RegisterDefaults directly.
package scenarios
