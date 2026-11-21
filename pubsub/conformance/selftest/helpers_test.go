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

package selftest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/harness"
)

var (
	selfExecPath  string
	repoRootDir   string
	cliBinaryPath string
)

func TestMain(m *testing.M) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") == "1" {
		switch helperRoleFromArgs(os.Args[1:]) {
		case "descendant":
			os.Exit(runDescendantHelper(os.Args[1:]))
		default:
			os.Exit(runProxyHelper(os.Args[1:]))
		}
	}
	os.Exit(runSelftestMain(m))
}

func runSelftestMain(m *testing.M) int {
	exe, err := os.Executable()
	if err != nil || strings.TrimSpace(exe) == "" {
		exe = os.Args[0]
	}
	selfExecPath = exe

	root, err := findShowcaseRepoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "selftest setup: locate repo root: %v\n", err)
		return 1
	}
	repoRootDir = root

	tmpDir, err := os.MkdirTemp("", "pubsub-conformance-selftest-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "selftest setup: create temp dir: %v\n", err)
		return 1
	}
	defer os.RemoveAll(tmpDir)

	cliBin := filepath.Join(tmpDir, "pubsub-conformance")
	buildCmd := exec.Command("go", "build", "-mod=readonly", "-o", cliBin, "./cmd/pubsub-conformance")
	buildCmd.Dir = repoRootDir
	if out, err := buildCmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "selftest setup: build ./cmd/pubsub-conformance failed: %v\n%s\n", err, out)
		return 1
	}
	cliBinaryPath = cliBin
	return m.Run()
}

func findShowcaseRepoRoot() (string, error) {
	isRoot := func(dir string) bool {
		modBytes, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err != nil || !strings.Contains(string(modBytes), "module github.com/googleapis/gapic-showcase") {
			return false
		}
		info, err := os.Stat(filepath.Join(dir, "cmd", "pubsub-conformance", "cli.go"))
		return err == nil && !info.IsDir()
	}
	var starts []string
	if wd, err := os.Getwd(); err == nil {
		starts = append(starts, wd)
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		starts = append(starts, filepath.Dir(file))
	}
	for _, start := range starts {
		for dir := filepath.Clean(start); ; {
			if isRoot(dir) {
				return dir, nil
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	return "", fmt.Errorf("could not locate gapic-showcase repo root from %v", starts)
}

func parseKVFlags(args []string, strs map[string]*string, bools map[string]*bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if dst := bools[a]; dst != nil {
			*dst = true
			continue
		}
		if k, v, ok := strings.Cut(a, "="); ok {
			if dst := strs[k]; dst != nil {
				*dst = v
			}
			continue
		}
		if dst := strs[a]; dst != nil && i+1 < len(args) {
			i++
			*dst = args[i]
		}
	}
}

func helperRoleFromArgs(args []string) string {
	role := "proxy"
	parseKVFlags(args, map[string]*string{"--helper-role": &role}, nil)
	return role
}

func runDescendantHelper(args []string) int {
	var pidLog string
	tag := "descendant"
	var ignoreSIGTERM bool
	parseKVFlags(args, map[string]*string{"--pid-log": &pidLog, "--tag": &tag}, map[string]*bool{"--ignore-sigterm": &ignoreSIGTERM})
	if ignoreSIGTERM {
		signal.Ignore(syscall.SIGTERM)
	}
	pid := os.Getpid()
	pgid, err := syscall.Getpgid(0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "descendant getpgid: %v\n", err)
		return 1
	}
	if err := appendPIDLog(pidLog, "descendant", pid, pgid, tag); err != nil {
		fmt.Fprintf(os.Stderr, "descendant pid log: %v\n", err)
		return 1
	}
	fmt.Printf("DESCENDANT_READY %d %d\n", pid, pgid)
	_ = os.Stdout.Sync()
	select {}
}

func appendLine(path, format string, args ...any) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	_, werr := fmt.Fprintf(f, format+"\n", args...)
	if cerr := f.Close(); werr == nil {
		return cerr
	}
	return werr
}

func appendPIDLog(path, role string, pid, pgid int, tag string) error {
	return appendLine(path, "%s %d %d %s", role, pid, pgid, strings.TrimSpace(tag))
}

func envWith(base []string, overrides ...string) []string {
	strip := make(map[string]struct{}, len(overrides))
	for _, kv := range overrides {
		if k, _, ok := strings.Cut(kv, "="); ok {
			strip[k] = struct{}{}
		}
	}
	out := slices.DeleteFunc(slices.Clone(base), func(kv string) bool {
		k, _, ok := strings.Cut(kv, "=")
		_, drop := strip[k]
		return ok && drop
	})
	return append(out, overrides...)
}

type pidEntry struct {
	Role string
	PID  int
	PGID int
	Tag  string
}

func trackPIDLog(t *testing.T, pidLog string) {
	t.Helper()
	t.Cleanup(func() {
		entries, err := readPIDLog(pidLog)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.PGID > 0 {
				_ = syscall.Kill(-e.PGID, syscall.SIGKILL)
			}
			if e.PID > 0 {
				_ = syscall.Kill(e.PID, syscall.SIGKILL)
			}
		}
	})
}

func readPIDLog(path string) ([]pidEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []pidEntry
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		pid, err1 := strconv.Atoi(fields[1])
		pgid, err2 := strconv.Atoi(fields[2])
		if err1 != nil || err2 != nil {
			continue
		}
		tag := ""
		if len(fields) >= 4 {
			tag = fields[3]
		}
		out = append(out, pidEntry{Role: fields[0], PID: pid, PGID: pgid, Tag: tag})
	}
	return out, nil
}

func procStat(pid int) (state string, pgrp int, ok bool) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", 0, false
	}
	s := string(raw)
	idx := strings.LastIndexByte(s, ')')
	if idx < 0 || idx+2 >= len(s) {
		return "", 0, false
	}
	fields := strings.Fields(s[idx+2:])
	if len(fields) < 3 {
		return "", 0, false
	}
	pg, err := strconv.Atoi(fields[2])
	if err != nil {
		return fields[0], 0, true
	}
	return fields[0], pg, true
}

func procCmdline(pid int) string {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(string(raw), "\x00", " "))
}

func assertAllPIDsDead(t *testing.T, pidLog string, minEntries int, requireDescendant bool) []pidEntry {
	t.Helper()
	entries, err := readPIDLog(pidLog)
	if err != nil {
		t.Fatalf("read pid log %s: %v", pidLog, err)
	}
	if len(entries) < minEntries {
		t.Fatalf("pid log %s has %d entries, want >= %d: %+v", pidLog, len(entries), minEntries, entries)
	}
	proxyPGIDs := map[int]bool{}
	hasDescendant := false
	for _, e := range entries {
		if e.Role == "proxy" {
			if e.PGID != e.PID {
				t.Fatalf("proxy entry %+v did not isolate PGID via Setpgid: pid=%d pgid=%d", e, e.PID, e.PGID)
			}
			proxyPGIDs[e.PGID] = true
		}
	}
	for _, e := range entries {
		if e.Role == "descendant" {
			hasDescendant = true
			if !proxyPGIDs[e.PGID] {
				t.Fatalf("descendant entry %+v PGID %d does not match any logged proxy PGID %v", e, e.PGID, proxyPGIDs)
			}
		}
	}
	if requireDescendant && !hasDescendant {
		t.Fatalf("pid log %s missing required descendant entry: %+v", pidLog, entries)
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	for _, e := range entries {
		for {
			proc, findErr := os.FindProcess(e.PID)
			var sigErr error
			if findErr == nil {
				sigErr = proc.Signal(syscall.Signal(0))
			}
			killErr := syscall.Kill(e.PID, 0)
			pgidErr := syscall.Kill(-e.PGID, 0)

			sigGone := errors.Is(sigErr, os.ErrProcessDone) || errors.Is(sigErr, syscall.ESRCH)
			if sigGone && errors.Is(killErr, syscall.ESRCH) && errors.Is(pgidErr, syscall.ESRCH) {
				break
			}
			if !time.Now().Before(deadline) {
				st, pg, hasStat := procStat(e.PID)
				if hasStat && st != "Z" && st != "X" {
					t.Fatalf("leaked live %s process pid=%d pgid=%d tag=%q state=%q pgrp=%d cmd=%q (sigErr=%v killErr=%v pgidErr=%v)",
						e.Role, e.PID, e.PGID, e.Tag, st, pg, procCmdline(e.PID), sigErr, killErr, pgidErr)
				}
				t.Fatalf("process/pgid did not reap within deadline: entry=%+v sigErr=%v killErr=%v pgidErr=%v stat=(%q,%d,%t) cmd=%q",
					e, sigErr, killErr, pgidErr, st, pg, hasStat, procCmdline(e.PID))
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return entries
}

type cliRunResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
}

func runCLI(t *testing.T, timeout time.Duration, extraEnv []string, args ...string) cliRunResult {
	t.Helper()
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, cliBinaryPath, args...)
	cmd.Env = envWith(os.Environ(), append([]string{"GO_WANT_HELPER_PROCESS=1"}, extraEnv...)...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &outBuf, &errBuf
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("CLI timed out after %v (%v): args=%v\nstdout:\n%s\nstderr:\n%s", timeout, ctx.Err(), args, outBuf.String(), errBuf.String())
	}
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("run CLI %v failed unexpectedly: %v\nstderr:\n%s", args, err, errBuf.String())
		}
		code = ee.ExitCode()
	}
	return cliRunResult{ExitCode: code, Stdout: outBuf.String(), Stderr: errBuf.String()}
}

func proxyCmdArgs(extraProxyArgs ...string) []string {
	out := []string{"--proxy-cmd=" + selfExecPath, "--proxy-arg=--helper-role=proxy"}
	for _, a := range extraProxyArgs {
		out = append(out, "--proxy-arg="+a)
	}
	return out
}

func proxyTargetFlag(name string, extraProxyArgs ...string) string {
	parts := append(make([]string, 0, 2+len(extraProxyArgs)), fmt.Sprintf("%q", selfExecPath), "--helper-role=proxy")
	for _, a := range extraProxyArgs {
		if strings.ContainsAny(a, " \t\n\"'\\") {
			a = fmt.Sprintf("%q", a)
		}
		parts = append(parts, a)
	}
	return "--proxy-target=" + name + "=" + strings.Join(parts, " ")
}

func helperTargetSpec(t *testing.T, name, pidLog string, extraArgs ...string) harness.TargetSpec {
	t.Helper()
	args := []string{"--helper-role=proxy"}
	if pidLog != "" {
		args = append(args, "--pid-log="+pidLog)
	}
	return harness.TargetSpec{
		Name: name, Command: selfExecPath, Args: append(args, extraArgs...),
		Env: []string{"GO_WANT_HELPER_PROCESS=1"}, StartupTimeout: 5 * time.Second, ShutdownTimeout: 250 * time.Millisecond,
	}
}

func mustLoadJSONReport(t *testing.T, path string) (*harness.Report, map[string]any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read JSON report %s: %v", path, err)
	}
	rep, err := harness.LoadJSONReport(path)
	if err != nil {
		t.Fatalf("LoadJSONReport(%s): %v\nraw:\n%s", path, err, raw)
	}
	var rawMap map[string]any
	if err := json.Unmarshal(raw, &rawMap); err != nil {
		t.Fatalf("unmarshal raw JSON %s: %v", path, err)
	}
	return rep, rawMap
}

func statusByID(results []harness.ScenarioResult) map[string]harness.ScenarioResult {
	out := make(map[string]harness.ScenarioResult, len(results))
	for _, r := range results {
		out[r.ID] = r
	}
	return out
}

func rawScenarioByID(t *testing.T, rawMap map[string]any, id string) map[string]any {
	t.Helper()
	scens, ok := rawMap["scenarios"].([]any)
	if !ok {
		t.Fatalf("raw JSON missing scenarios array: %+v", rawMap)
	}
	for _, item := range scens {
		if m, ok := item.(map[string]any); ok && m["id"] == id {
			return m
		}
	}
	t.Fatalf("scenario %q not found in raw JSON scenarios", id)
	return nil
}

func assertDifferencesContain(t *testing.T, diffs []harness.Difference, wantKinds ...string) {
	t.Helper()
	seen := map[string]bool{}
	for _, d := range diffs {
		seen[d.Kind] = true
	}
	for _, k := range wantKinds {
		if !seen[k] {
			t.Fatalf("differences missing kind %q (seen=%v, diffs=%+v)", k, seen, diffs)
		}
	}
}

func assertStringsContainAll(t *testing.T, text string, needles ...string) {
	t.Helper()
	for _, n := range needles {
		if !strings.Contains(text, n) {
			t.Fatalf("expected text to contain %q, got:\n%s", n, text)
		}
	}
}

func mustNewRunner(t *testing.T, cfg harness.RunConfig) *harness.Runner {
	t.Helper()
	r, err := harness.NewRunner(cfg)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	return r
}

func mustRunTarget(t *testing.T, ctx context.Context, cfg harness.RunConfig, target harness.TargetSpec) *harness.TargetReport {
	t.Helper()
	rep, err := mustNewRunner(t, cfg).RunTarget(ctx, target)
	if err != nil {
		t.Fatalf("RunTarget(%s): %v", target.Name, err)
	}
	return rep
}

func mustReadString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func waitForFile(path string, timeout time.Duration) bool {
	for dl := time.Now().Add(timeout); time.Now().Before(dl); time.Sleep(15 * time.Millisecond) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}
	_, err := os.Stat(path)
	return err == nil
}
