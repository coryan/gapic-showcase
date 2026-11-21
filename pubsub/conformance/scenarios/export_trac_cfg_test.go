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

import "github.com/googleapis/gapic-showcase/pubsub/conformance/harness"

// ExportTracingExtraScenarios returns a copy of the extended tracing scenarios
// for isolated unit/profile verification in package scenarios_test.
func ExportTracingExtraScenarios() []harness.Scenario {
	return append([]harness.Scenario(nil), tracingExtraScenarios()...)
}

// ExportConfigScenarios returns a copy of the configuration, header, and
// resource scenarios for isolated unit/profile verification in package
// scenarios_test.
func ExportConfigScenarios() []harness.Scenario {
	return append([]harness.Scenario(nil), configScenarios()...)
}
