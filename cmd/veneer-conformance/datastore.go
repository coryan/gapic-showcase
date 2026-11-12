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

package main

import (
	"errors"
	"fmt"
	"io"
	"time"

	ds "github.com/googleapis/gapic-showcase/conformance/datastore"
	"github.com/spf13/cobra"
)

func newDatastoreCmd(out io.Writer, asJSON *bool) *cobra.Command {
	var (
		fixtureAddr string
		fixtureCmd  string
		transports  string
		runPattern  string
		skipPattern string
		strict      bool
		timeout     time.Duration
	)

	cmd := &cobra.Command{
		Use:   "datastore",
		Short: "Run Cloud Datastore data-plane veneer conformance tests",
		RunE: func(cmd *cobra.Command, args []string) error {
			if fixtureAddr == "" && fixtureCmd == "" {
				return errors.New("must specify either --fixture-addr or --fixture-cmd")
			}
			parsedTransports, err := ds.ParseTransports(transports)
			if err != nil {
				return err
			}

			report, err := ds.RunSuite(cmd.Context(), ds.SuiteConfig{
				FixtureAddr: fixtureAddr,
				FixtureCmd:  fixtureCmd,
				Transports:  parsedTransports,
				RunPattern:  runPattern,
				SkipPattern: skipPattern,
				Strict:      strict,
				Timeout:     timeout,
			})
			if err != nil {
				return err
			}
			if err := ds.FormatReport(out, report, *asJSON); err != nil {
				return err
			}
			if report.Failed > 0 {
				return fmt.Errorf("%d conformance scenario(s) failed", report.Failed)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&fixtureAddr, "fixture-addr", "", "gRPC address (host:port) of an already-running DatastoreFixture server")
	cmd.Flags().StringVar(&fixtureCmd, "fixture-cmd", "", "Command to launch a DatastoreFixture subprocess (receives --port <N> or replaces {port})")
	cmd.Flags().StringVar(&transports, "transports", "grpc", "Comma-separated transports to test: grpc, rest")
	cmd.Flags().StringVar(&runPattern, "run", "", "Regex filter selecting scenario names to run")
	cmd.Flags().StringVar(&skipPattern, "skip", "", "Regex filter selecting scenario names to skip")
	cmd.Flags().BoolVar(&strict, "strict", false, "Treat FEATURE_NOT_IMPLEMENTED responses as failures instead of skips")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Per-scenario execution timeout")

	return cmd
}
