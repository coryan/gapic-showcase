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

package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Scenario defines one self-contained Datastore conformance test case.
type Scenario struct {
	Name     string
	Category string
	SpecRef  string
	Run      func(ctx context.Context, env *ScenarioEnv) error
}

// ScenarioEnv provides access to the simulated Datastore server and the fixture client
// for the duration of a single scenario execution.
type ScenarioEnv struct {
	Fixture   fixturepb.DatastoreFixtureClient
	Server    *SimServer
	Transport fixturepb.Transport
}

// NewClient creates a fixture-managed Datastore client targeting env.Server and registers
// automatic CloseClient cleanup via returned closure.
func (e *ScenarioEnv) NewClient(ctx context.Context, projectID, databaseID, namespaceID string) (string, func(), error) {
	resp, err := e.Fixture.CreateClient(ctx, &fixturepb.CreateClientRequest{
		ProjectId:   projectID,
		DatabaseId:  databaseID,
		NamespaceId: namespaceID,
		Endpoint:    e.Server.Addr(),
		Transport:   e.Transport,
	})
	if err != nil {
		return "", nil, fmt.Errorf("CreateClient gRPC failed: %w", err)
	}
	if err := CheckNoError(resp.Error, "CreateClient"); err != nil {
		return "", nil, err
	}
	if resp.ClientId == "" {
		return "", nil, errors.New("CreateClient returned empty client_id")
	}
	clientID := resp.ClientId
	cleanup := func() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = e.Fixture.CloseClient(cctx, &fixturepb.CloseClientRequest{ClientId: clientID})
	}
	return clientID, cleanup, nil
}

// ScenarioOutcome represents the result of running a single scenario on a transport.
type ScenarioOutcome struct {
	Scenario   string `json:"scenario"`
	Category   string `json:"category"`
	SpecRef    string `json:"spec_ref"`
	Transport  string `json:"transport"`
	Status     string `json:"status"` // PASS, FAIL, SKIP
	DurationMS int64  `json:"duration_ms"`
	Reason     string `json:"reason,omitempty"`
}

// SuiteReport summarizes all scenario outcomes in a conformance run.
type SuiteReport struct {
	Passed   int               `json:"passed"`
	Failed   int               `json:"failed"`
	Skipped  int               `json:"skipped"`
	Outcomes []ScenarioOutcome `json:"outcomes"`
}

// SuiteConfig configures a conformance suite run.
type SuiteConfig struct {
	FixtureAddr string
	FixtureCmd  string
	Transports  []fixturepb.Transport
	RunPattern  string
	SkipPattern string
	Strict      bool
	Timeout     time.Duration
}

// AllScenarios returns all registered conformance scenarios in deterministic order.
func AllScenarios() []Scenario {
	var all []Scenario
	all = append(all, headerRoutingScenarios()...)
	return all
}

// RunSuite executes matching scenarios against the configured fixture address or command.
func RunSuite(ctx context.Context, cfg SuiteConfig) (*SuiteReport, error) {
	if len(cfg.Transports) == 0 {
		cfg.Transports = []fixturepb.Transport{fixturepb.Transport_TRANSPORT_GRPC}
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}

	var runRe, skipRe *regexp.Regexp
	var err error
	if cfg.RunPattern != "" {
		if runRe, err = regexp.Compile(cfg.RunPattern); err != nil {
			return nil, fmt.Errorf("invalid --run regex %q: %w", cfg.RunPattern, err)
		}
	}
	if cfg.SkipPattern != "" {
		if skipRe, err = regexp.Compile(cfg.SkipPattern); err != nil {
			return nil, fmt.Errorf("invalid --skip regex %q: %w", cfg.SkipPattern, err)
		}
	}

	addr := cfg.FixtureAddr
	if addr == "" && cfg.FixtureCmd == "" {
		return nil, errors.New("either --fixture-addr or --fixture-cmd must be specified")
	}
	if addr != "" && cfg.FixtureCmd != "" {
		return nil, errors.New("cannot specify both --fixture-addr and --fixture-cmd")
	}

	if cfg.FixtureCmd != "" {
		launchedAddr, stopCmd, err := launchFixtureCmd(ctx, cfg.FixtureCmd)
		if err != nil {
			return nil, err
		}
		defer stopCmd()
		addr = launchedAddr
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial fixture %s: %w", addr, err)
	}
	defer conn.Close()

	fixtureClient := fixturepb.NewDatastoreFixtureClient(conn)

	sim, err := NewSimServer()
	if err != nil {
		return nil, fmt.Errorf("start simulated backend: %w", err)
	}
	defer sim.Close()

	scenarios := AllScenarios()
	report := &SuiteReport{}

	for _, tr := range cfg.Transports {
		trName := transportLabel(tr)
		for _, sc := range scenarios {
			if runRe != nil && !runRe.MatchString(sc.Name) {
				continue
			}
			if skipRe != nil && skipRe.MatchString(sc.Name) {
				report.Skipped++
				report.Outcomes = append(report.Outcomes, ScenarioOutcome{
					Scenario:  sc.Name,
					Category:  sc.Category,
					SpecRef:   sc.SpecRef,
					Transport: trName,
					Status:    "SKIP",
					Reason:    "matched --skip filter",
				})
				continue
			}

			sim.Reset()
			scCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			start := time.Now()
			env := &ScenarioEnv{
				Fixture:   fixtureClient,
				Server:    sim,
				Transport: tr,
			}
			scErr := sc.Run(scCtx, env)
			if scErr == nil {
				scErr = sim.VerifyAllExpectationsMet()
			}
			dur := time.Since(start).Milliseconds()
			cancel()

			switch {
			case scErr == nil:
				report.Passed++
				report.Outcomes = append(report.Outcomes, ScenarioOutcome{
					Scenario:   sc.Name,
					Category:   sc.Category,
					SpecRef:    sc.SpecRef,
					Transport:  trName,
					Status:     "PASS",
					DurationMS: dur,
				})
			case errors.Is(scErr, ErrScenarioSkipped) && !cfg.Strict:
				report.Skipped++
				report.Outcomes = append(report.Outcomes, ScenarioOutcome{
					Scenario:   sc.Name,
					Category:   sc.Category,
					SpecRef:    sc.SpecRef,
					Transport:  trName,
					Status:     "SKIP",
					DurationMS: dur,
					Reason:     scErr.Error(),
				})
			default:
				report.Failed++
				report.Outcomes = append(report.Outcomes, ScenarioOutcome{
					Scenario:   sc.Name,
					Category:   sc.Category,
					SpecRef:    sc.SpecRef,
					Transport:  trName,
					Status:     "FAIL",
					DurationMS: dur,
					Reason:     scErr.Error(),
				})
			}
		}
	}

	return report, nil
}

func transportLabel(t fixturepb.Transport) string {
	switch t {
	case fixturepb.Transport_TRANSPORT_GRPC:
		return "grpc"
	case fixturepb.Transport_TRANSPORT_HTTP_JSON:
		return "rest"
	default:
		return t.String()
	}
}

// ParseTransports parses a comma-separated list of transport names ("grpc", "rest").
func ParseTransports(raw string) ([]fixturepb.Transport, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("empty transports list")
	}
	var out []fixturepb.Transport
	for _, part := range strings.Split(raw, ",") {
		switch strings.ToLower(strings.TrimSpace(part)) {
		case "grpc":
			out = append(out, fixturepb.Transport_TRANSPORT_GRPC)
		case "rest", "http", "http_json", "http-json":
			out = append(out, fixturepb.Transport_TRANSPORT_HTTP_JSON)
		default:
			return nil, fmt.Errorf("unknown transport %q (supported: grpc, rest)", part)
		}
	}
	return out, nil
}

// FormatReport writes a human-readable or JSON report to w.
func FormatReport(w io.Writer, report *SuiteReport, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	for _, o := range report.Outcomes {
		if o.Reason != "" {
			fmt.Fprintf(w, "[%-4s] [%s] %-45s (%dms) %s\n", o.Status, o.Transport, o.Scenario, o.DurationMS, o.Reason)
		} else {
			fmt.Fprintf(w, "[%-4s] [%s] %-45s (%dms)\n", o.Status, o.Transport, o.Scenario, o.DurationMS)
		}
	}
	fmt.Fprintf(w, "\nSummary: %d passed, %d failed, %d skipped (total %d)\n",
		report.Passed, report.Failed, report.Skipped, len(report.Outcomes))
	return nil
}
