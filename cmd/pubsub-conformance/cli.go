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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
	"github.com/googleapis/gapic-showcase/pubsub/conformance/scenarios"
	"github.com/spf13/cobra"
)

type cliOptions struct {
	ProxyCmd          string
	ProxyArgs         []string
	ProxyTargets      []string
	Compare           []string
	Filter            string
	ScenarioTimeout   time.Duration
	SuiteTimeout      time.Duration
	ReportJSON        string
	ReportMD          string
	StrictUnsupported bool
	List              bool
}

func runCLI(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return runCLIWithScenarios(ctx, args, stdin, stdout, stderr, scenarios.All())
}

func runCLIWithScenarios(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, scens []harness.Scenario) int {
	if ctx == nil {
		ctx = context.Background()
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}

	var opts cliOptions
	exitCode := 0
	cmd := newRootCmd(&opts)
	cmd.SetArgs(args)
	cmd.SetIn(stdin)
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	cmd.RunE = func(c *cobra.Command, _ []string) error {
		code, err := executeCLI(c.Context(), c, opts, stdout, stderr, scens)
		exitCode = code
		return err
	}
	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}
	return exitCode
}

func newRootCmd(opts *cliOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "pubsub-conformance",
		Short:             "Pub/Sub client library data-plane conformance test runner",
		Args:              cobra.NoArgs,
		SilenceUsage:      true,
		SilenceErrors:     true,
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	f := cmd.Flags()
	f.StringVar(&opts.ProxyCmd, "proxy-cmd", "", "Command to launch a single proxy subprocess")
	f.StringArrayVar(&opts.ProxyArgs, "proxy-arg", nil, "Extra argument forwarded to --proxy-cmd (repeatable)")
	f.StringArrayVar(&opts.ProxyTargets, "proxy-target", nil, "Named proxy target in name=command format (repeatable)")
	f.StringArrayVar(&opts.Compare, "compare", nil, "Compare two or more JSON conformance reports offline (comma-separated or repeatable)")
	f.StringVar(&opts.Filter, "filter", "", "Go regex filtering scenarios by ID, name, or category")
	f.DurationVar(&opts.ScenarioTimeout, "scenario-timeout", harness.DefaultScenarioTimeout, "Per-scenario execution timeout (must be > 0)")
	f.DurationVar(&opts.SuiteTimeout, "suite-timeout", 0, "Overall suite execution timeout (must be > 0 when set)")
	f.StringVar(&opts.ReportJSON, "report-json", "", "Write structured JSON conformance report to file")
	f.StringVar(&opts.ReportMD, "report-md", "", "Write Markdown conformance report to file")
	f.BoolVar(&opts.StrictUnsupported, "strict-unsupported", false, "Exit non-zero if any live scenario reports UNSUPPORTED_BY_CLIENT")
	f.BoolVar(&opts.List, "list", false, "List conformance scenarios matching --filter and exit")
	return cmd
}

func executeCLI(ctx context.Context, cmd *cobra.Command, opts cliOptions, stdout, stderr io.Writer, scens []harness.Scenario) (int, error) {
	if err := validateCLIOptions(cmd, opts); err != nil {
		return 1, err
	}
	if opts.List {
		return runListMode(opts, stdout, stderr, scens)
	}
	if cmd.Flags().Changed("compare") {
		return runCompareMode(opts, stdout)
	}
	return runLiveMode(ctx, cmd, opts, stdout, stderr, scens)
}

func validateCLIOptions(cmd *cobra.Command, opts cliOptions) error {
	flags := cmd.Flags()
	hasCompare := flags.Changed("compare")
	hasProxyCmd := flags.Changed("proxy-cmd")
	hasProxyArg := flags.Changed("proxy-arg")
	hasProxyTarget := flags.Changed("proxy-target")
	hasReportJSON := flags.Changed("report-json")
	hasReportMD := flags.Changed("report-md")
	hasFilter := flags.Changed("filter")

	if flags.Changed("scenario-timeout") && opts.ScenarioTimeout <= 0 {
		return fmt.Errorf("--scenario-timeout must be > 0 (got %s)", opts.ScenarioTimeout)
	}
	if flags.Changed("suite-timeout") && opts.SuiteTimeout <= 0 {
		return fmt.Errorf("--suite-timeout must be > 0 (got %s)", opts.SuiteTimeout)
	}
	if opts.Filter != "" {
		if _, err := regexp.Compile(opts.Filter); err != nil {
			return fmt.Errorf("invalid --filter regex %q: %w", opts.Filter, err)
		}
	}
	if hasProxyCmd && strings.TrimSpace(opts.ProxyCmd) == "" {
		return fmt.Errorf("--proxy-cmd cannot be empty")
	}
	if hasProxyArg && !hasProxyCmd {
		return fmt.Errorf("--proxy-arg requires --proxy-cmd")
	}
	if opts.List {
		if hasCompare || hasProxyCmd || hasProxyArg || hasProxyTarget || hasReportJSON || hasReportMD || opts.StrictUnsupported {
			return fmt.Errorf("--list cannot be combined with --proxy-cmd, --proxy-arg, --proxy-target, --compare, --report-*, or --strict-unsupported")
		}
		return nil
	}
	if hasReportJSON {
		if err := validateOutputPath("--report-json", opts.ReportJSON); err != nil {
			return err
		}
	}
	if hasReportMD {
		if err := validateOutputPath("--report-md", opts.ReportMD); err != nil {
			return err
		}
	}
	if hasReportJSON && hasReportMD {
		absJ, errJ := filepath.Abs(opts.ReportJSON)
		absM, errM := filepath.Abs(opts.ReportMD)
		if errJ == nil && errM == nil && absJ == absM && absJ != os.DevNull {
			return fmt.Errorf("--report-json and --report-md cannot resolve to the same file %q", opts.ReportJSON)
		}
	}
	if hasCompare {
		if hasProxyCmd || hasProxyArg || hasProxyTarget || hasFilter || opts.StrictUnsupported {
			return fmt.Errorf("--compare cannot be combined with --proxy-cmd, --proxy-arg, --proxy-target, --filter, or --strict-unsupported")
		}
		return nil
	}
	if hasProxyCmd && hasProxyTarget {
		return fmt.Errorf("cannot combine --proxy-cmd and --proxy-target")
	}
	if !hasProxyCmd && !hasProxyTarget {
		return fmt.Errorf("specify --proxy-cmd, --proxy-target, --compare, or --list")
	}
	return nil
}

func validateOutputPath(flagName, path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("%s cannot be empty", flagName)
	}
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return fmt.Errorf("%s path %q is a directory", flagName, path)
	}
	parent := filepath.Dir(path)
	pInfo, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("%s parent directory %q is invalid: %w", flagName, parent, err)
	}
	if !pInfo.IsDir() {
		return fmt.Errorf("%s parent %q is not a directory", flagName, parent)
	}
	return nil
}

func runListMode(opts cliOptions, stdout, stderr io.Writer, scens []harness.Scenario) (int, error) {
	matched, err := harness.FilterScenarios(scens, opts.Filter)
	if err != nil {
		return 1, err
	}
	if len(matched) == 0 {
		fmt.Fprintf(stderr, "No scenarios matched filter %q.\n", opts.Filter)
		return 0, nil
	}
	for _, sc := range matched {
		var notes []string
		if sc.TraceOnly {
			notes = append(notes, "trace-only")
		}
		if sc.UseEmulatorEnv {
			notes = append(notes, "emulator-env")
		}
		if len(sc.RequiredCapabilities) > 0 {
			caps := make([]string, len(sc.RequiredCapabilities))
			for i, c := range sc.RequiredCapabilities {
				caps[i] = string(c)
			}
			notes = append(notes, "requires:"+strings.Join(caps, ","))
		}
		suffix := ""
		if len(notes) > 0 {
			suffix = " [" + strings.Join(notes, "; ") + "]"
		}
		if _, err := fmt.Fprintf(stdout, "%-10s  %-10s  %s%s\n", sc.ID, sc.Category, sc.Name, suffix); err != nil {
			return 1, err
		}
	}
	return 0, nil
}

func runCompareMode(opts cliOptions, stdout io.Writer) (int, error) {
	rep, err := harness.CompareJSONFiles(opts.Compare)
	if err != nil {
		return 1, err
	}
	if err := emitReports(rep, opts, stdout); err != nil {
		return 1, err
	}
	return 0, nil
}

func runLiveMode(ctx context.Context, cmd *cobra.Command, opts cliOptions, stdout, stderr io.Writer, scens []harness.Scenario) (int, error) {
	targets, err := buildLiveTargets(cmd, opts)
	if err != nil {
		return 1, err
	}
	runner, err := harness.NewRunner(harness.RunConfig{
		Targets:           targets,
		Scenarios:         scens,
		Filter:            opts.Filter,
		FailOnEmptyFilter: true,
		StrictUnsupported: opts.StrictUnsupported,
		ScenarioTimeout:   opts.ScenarioTimeout,
		SuiteTimeout:      opts.SuiteTimeout,
		ProgressWriter:    stderr,
	})
	if err != nil {
		return 1, err
	}
	rep, err := runner.RunAll(ctx)
	if err != nil {
		return 1, err
	}
	if rep == nil {
		return 1, fmt.Errorf("conformance runner returned nil report")
	}
	if rep.InfraError != "" {
		fmt.Fprintf(stderr, "Infra error: %s\n", rep.InfraError)
	}
	for _, t := range rep.Targets {
		if t.TargetError != "" {
			fmt.Fprintf(stderr, "Target %s error: %s\n", t.Target, t.TargetError)
		}
	}
	if writeErr := emitReports(rep, opts, stdout); writeErr != nil {
		return 1, writeErr
	}
	if ctx.Err() != nil {
		return 1, nil
	}
	return harness.ExitCode(rep, opts.StrictUnsupported), nil
}

func buildLiveTargets(cmd *cobra.Command, opts cliOptions) ([]harness.TargetSpec, error) {
	if cmd.Flags().Changed("proxy-cmd") {
		command, proxyArgs, err := harness.ParseCommandSpec(opts.ProxyCmd, opts.ProxyArgs)
		if err != nil {
			return nil, err
		}
		targets := []harness.TargetSpec{{Name: "default", Command: command, Args: proxyArgs}}
		if err := harness.ValidateTargets(targets); err != nil {
			return nil, err
		}
		return targets, nil
	}
	targets := make([]harness.TargetSpec, 0, len(opts.ProxyTargets))
	for _, raw := range opts.ProxyTargets {
		spec, err := harness.ParseTargetSpec(raw)
		if err != nil {
			return nil, err
		}
		targets = append(targets, spec)
	}
	if err := harness.ValidateTargets(targets); err != nil {
		return nil, err
	}
	return targets, nil
}

func emitReports(rep *harness.Report, opts cliOptions, stdout io.Writer) error {
	var errs []error
	if opts.ReportJSON != "" {
		if err := harness.WriteJSONReport(opts.ReportJSON, rep); err != nil {
			errs = append(errs, err)
		}
	}
	if opts.ReportMD != "" {
		if err := harness.WriteMarkdownReport(opts.ReportMD, rep); err != nil {
			errs = append(errs, err)
		}
	}
	md, err := harness.FormatMarkdownReport(rep)
	if err != nil {
		errs = append(errs, err)
	} else if _, werr := io.WriteString(stdout, md); werr != nil {
		errs = append(errs, werr)
	}
	return errors.Join(errs...)
}
