// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0

package harness

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/fake"
)

const (
	DefaultStartupTimeout  = 15 * time.Second
	DefaultShutdownTimeout = 5 * time.Second
	DefaultScenarioTimeout = 30 * time.Second
	DefaultProjectID       = "conformance-project"
)

type TargetSpec struct {
	Name, Command, WorkDir          string
	Args, Env                       []string
	StartupTimeout, ShutdownTimeout time.Duration
	Connect                         func(ctx context.Context, spec TargetSpec) (ProxyHandle, error)
}

func (s TargetSpec) CommandString() string {
	if strings.TrimSpace(s.Command) == "" {
		return map[bool]string{true: "<in-process:" + s.Name + ">"}[s.Connect != nil]
	}
	parts := make([]string, 0, 1+len(s.Args))
	for _, a := range append([]string{s.Command}, s.Args...) {
		if a == "" || strings.ContainsAny(a, " \t\n\"'\\") {
			a = fmt.Sprintf("%q", a)
		}
		parts = append(parts, a)
	}
	return strings.Join(parts, " ")
}

type RunConfig struct {
	Targets                                   []TargetSpec
	Scenarios                                 []Scenario
	Filter, ProjectID                         string
	FailOnEmptyFilter, StrictUnsupported      bool
	IsolateProxyPerScenario, StopOnProxyCrash bool
	ScenarioTimeout, SuiteTimeout             time.Duration
	ProgressWriter                            io.Writer
	FakeServerOptions                         []fake.Option
}

func (c RunConfig) Validate() error {
	if c.ScenarioTimeout < 0 || c.SuiteTimeout < 0 {
		return fmt.Errorf("timeouts must be >= 0 (scenario=%v, suite=%v)", c.ScenarioTimeout, c.SuiteTimeout)
	}
	if _, err := regexp.Compile(c.Filter); c.Filter != "" && err != nil {
		return fmt.Errorf("invalid filter regex %q: %w", c.Filter, err)
	}
	if len(c.Targets) > 0 {
		return ValidateTargets(c.Targets)
	}
	return nil
}
func ParseTargetSpec(raw string) (TargetSpec, error) {
	name, cmdPart, ok := strings.Cut(raw, "=")
	if name = strings.TrimSpace(name); !ok || name == "" {
		return TargetSpec{}, fmt.Errorf("target spec %q must be in non-empty name=command format", raw)
	}
	cmd, args, err := ParseCommandSpec(cmdPart, nil)
	if err != nil {
		return TargetSpec{}, fmt.Errorf("target %q: %w", name, err)
	}
	return TargetSpec{Name: name, Command: cmd, Args: args}, nil
}
func ParseCommandSpec(rawCmd string, extraArgs []string) (string, []string, error) {
	trimmed := strings.TrimSpace(rawCmd)
	if trimmed == "" {
		return "", nil, fmt.Errorf("command cannot be empty")
	}
	if info, err := os.Stat(trimmed); err == nil && !info.IsDir() && info.Mode().IsRegular() {
		return trimmed, slices.Clone(extraArgs), nil
	}
	tokens, err := SplitCommandLine(trimmed)
	if err != nil {
		return "", nil, err
	}
	return tokens[0], append(slices.Clone(tokens[1:]), extraArgs...), nil
}
func SplitCommandLine(raw string) ([]string, error) {
	var tokens []string
	var cur strings.Builder
	inToken, quote := false, rune(0)
	for i := 0; i < len(raw); {
		r, size := utf8.DecodeRuneInString(raw[i:])
		i += size
		switch {
		case quote != 0 && r == quote:
			quote = 0
		case quote == '"' && r == '\\':
			if i >= len(raw) {
				return nil, fmt.Errorf("unterminated quote in %q", raw)
			}
			next, ns := utf8.DecodeRuneInString(raw[i:])
			if strings.ContainsRune("\"\\$`", next) {
				cur.WriteString(raw[i : i+ns])
				i += ns
			} else {
				cur.WriteByte('\\')
			}
		case quote != 0:
			cur.WriteString(raw[i-size : i])
		case unicode.IsSpace(r):
			if inToken {
				tokens, inToken = append(tokens, cur.String()), false
				cur.Reset()
			}
		case r == '\'' || r == '"':
			quote, inToken = r, true
		case r == '\\':
			if i >= len(raw) {
				return nil, fmt.Errorf("trailing escape backslash in %q", raw)
			}
			_, ns := utf8.DecodeRuneInString(raw[i:])
			inToken = true
			cur.WriteString(raw[i : i+ns])
			i += ns
		default:
			inToken = true
			cur.WriteString(raw[i-size : i])
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote in %q", raw)
	}
	if inToken {
		tokens = append(tokens, cur.String())
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("command cannot be empty")
	}
	return tokens, nil
}
func ValidateTargets(targets []TargetSpec) error {
	if len(targets) == 0 {
		return fmt.Errorf("at least one target is required")
	}
	seen := make(map[string]struct{}, len(targets))
	for i, t := range targets {
		name := strings.TrimSpace(t.Name)
		if name == "" || (t.Connect == nil && strings.TrimSpace(t.Command) == "") || t.StartupTimeout < 0 || t.ShutdownTimeout < 0 {
			return fmt.Errorf("target[%d] (%q): invalid target spec", i, name)
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("duplicate target name %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func HasPortFlag(args []string) bool {
	return slices.ContainsFunc(args, func(a string) bool {
		return a == "--port" || a == "-port" || strings.HasPrefix(a, "--port=") || strings.HasPrefix(a, "-port=")
	})
}
func EnsurePortFlag(args []string) []string {
	if HasPortFlag(args) {
		return slices.Clone(args)
	}
	return append(slices.Clone(args), "--port=0")
}
func ParseCompareArgs(raw []string) ([]string, error) {
	var paths []string
	for _, item := range raw {
		for _, part := range strings.Split(item, ",") {
			p := strings.TrimSpace(part)
			if p == "" {
				return nil, fmt.Errorf("empty file path in --compare argument %q", item)
			}
			paths = append(paths, p)
		}
	}
	return paths, nil
}
