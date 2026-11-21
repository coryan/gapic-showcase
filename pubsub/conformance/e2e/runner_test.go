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

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	repoRootDir     string
	sharedTempDir   string
	proxyBinaryPath string

	cliBuildOnce  sync.Once
	cliBinaryPath string
	cliBuildErr   error

	raceProxyBuildOnce sync.Once
	raceProxyBinPath   string
	raceProxyBuildErr  error

	runMarkerRegexp = regexp.MustCompile(`e2e-run-\d+-\d+`)
)

// CLIResult captures the outcome of running cmd/pubsub-conformance as an external subprocess.
type CLIResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Duration time.Duration
	TimedOut bool
	Err      error
}

func TestMain(m *testing.M) {
	if os.Getenv("E2E_SLOW_SHUTDOWN_PROXY") == "1" {
		os.Exit(runSlowShutdownTestProxy())
	}
	if os.Getenv("GO_WANT_T4LIVE_SLEEP_HELPER") == "1" {
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	root, err := findRepoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e TestMain: failed to resolve repo root: %v\n", err)
		return 1
	}
	repoRootDir = root

	tmp, err := os.MkdirTemp("", "pubsub-conformance-e2e-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e TestMain: failed to create temp dir: %v\n", err)
		return 1
	}
	defer os.RemoveAll(tmp)
	sharedTempDir = tmp

	// Build ONLY ./pubsub/conformance/e2e/cmd/e2e-test-proxy eagerly.
	// ./cmd/pubsub-conformance is built lazily on demand by buildConformanceCLIBinary.
	proxyBin := filepath.Join(tmp, "e2e-test-proxy")
	cmd := exec.Command("go", "build", "-mod=readonly", "-o", proxyBin, "./pubsub/conformance/e2e/cmd/e2e-test-proxy")
	cmd.Dir = repoRootDir
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e TestMain: failed to build e2e-test-proxy: %v\n%s\n", err, out)
		return 1
	}
	proxyBinaryPath = proxyBin

	return m.Run()
}

func isShowcaseRepoRoot(dir string) bool {
	data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil || !strings.Contains(string(data), "module github.com/googleapis/gapic-showcase") {
		return false
	}
	fi, err := os.Stat(filepath.Join(dir, "pubsub", "conformance", "e2e", "runner_test.go"))
	return err == nil && !fi.IsDir()
}

func findRepoRoot() (string, error) {
	if wd, err := os.Getwd(); err == nil {
		cur := wd
		for {
			if isShowcaseRepoRoot(cur) {
				return cur, nil
			}
			parent := filepath.Dir(cur)
			if parent == cur {
				break
			}
			cur = parent
		}
	}

	if _, file, _, ok := runtime.Caller(0); ok {
		candidate := filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
		if isShowcaseRepoRoot(candidate) {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("could not locate go.mod + pubsub/conformance/e2e/runner_test.go for github.com/googleapis/gapic-showcase")
}

// buildConformanceCLIBinary lazily compiles ./cmd/pubsub-conformance once per test process.
// Errors are recorded inside sync.Once and reported via t.Fatalf outside once.Do (never t.Skip).
func buildConformanceCLIBinary(t *testing.T) string {
	t.Helper()
	cliBuildOnce.Do(func() {
		if repoRootDir == "" || sharedTempDir == "" {
			cliBuildErr = fmt.Errorf("TestMain did not initialize repoRootDir/sharedTempDir")
			return
		}
		target := filepath.Join(sharedTempDir, "pubsub-conformance")
		cmd := exec.Command("go", "build", "-mod=readonly", "-o", target, "./cmd/pubsub-conformance")
		cmd.Dir = repoRootDir
		if out, err := cmd.CombinedOutput(); err != nil {
			cliBuildErr = fmt.Errorf("go build ./cmd/pubsub-conformance failed: %v\n%s", err, string(out))
			return
		}
		info, err := os.Stat(target)
		if err != nil {
			cliBuildErr = fmt.Errorf("stat compiled CLI binary %q failed: %v", target, err)
			return
		}
		if info.IsDir() {
			cliBuildErr = fmt.Errorf("compiled CLI binary path %q is a directory", target)
			return
		}
		cliBinaryPath = target
	})

	if cliBuildErr != nil || cliBinaryPath == "" {
		t.Fatalf("buildConformanceCLIBinary: %v", cliBuildErr)
	}
	info, err := os.Stat(cliBinaryPath)
	if err != nil || info.IsDir() {
		t.Fatalf("buildConformanceCLIBinary: binary unavailable at %q: %v", cliBinaryPath, err)
	}
	return cliBinaryPath
}

func buildRaceProxyBinary(t *testing.T) string {
	t.Helper()
	raceProxyBuildOnce.Do(func() {
		if repoRootDir == "" || sharedTempDir == "" {
			raceProxyBuildErr = fmt.Errorf("TestMain did not initialize repoRootDir/sharedTempDir")
			return
		}
		target := filepath.Join(sharedTempDir, "e2e-test-proxy-race")
		cmd := exec.Command("go", "build", "-mod=readonly", "-race", "-o", target, "./pubsub/conformance/e2e/cmd/e2e-test-proxy")
		cmd.Dir = repoRootDir
		if out, err := cmd.CombinedOutput(); err != nil {
			raceProxyBuildErr = fmt.Errorf("go build -race ./pubsub/conformance/e2e/cmd/e2e-test-proxy failed: %v\n%s", err, string(out))
			return
		}
		raceProxyBinPath = target
	})
	if raceProxyBuildErr != nil || raceProxyBinPath == "" {
		t.Fatalf("buildRaceProxyBinary: %v", raceProxyBuildErr)
	}
	return raceProxyBinPath
}

// runCLI executes cmd/pubsub-conformance with a bounded timeout, collects output/exit status,
// and asserts zero leaked proxy processes for all run markers present in args.
func mergeEnv(base, overrides []string) []string {
	merged := make([]string, 0, len(base)+len(overrides))
	idxByKey := make(map[string]int, len(base)+len(overrides))
	apply := func(kv string) {
		k, _, hasEq := strings.Cut(kv, "=")
		if !hasEq || k == "" {
			return
		}
		if pos, ok := idxByKey[k]; ok {
			merged[pos] = kv
		} else {
			idxByKey[k] = len(merged)
			merged = append(merged, kv)
		}
	}
	for _, kv := range base {
		apply(kv)
	}
	for _, kv := range overrides {
		apply(kv)
	}
	return merged
}

func runCLI(t *testing.T, timeout time.Duration, args ...string) CLIResult {
	t.Helper()
	return runCLIWithEnv(t, timeout, nil, args...)
}

func runCLIWithEnv(t *testing.T, timeout time.Duration, extraEnv []string, args ...string) CLIResult {
	t.Helper()
	if timeout <= 0 {
		t.Fatalf("runCLI: timeout must be > 0, got %v", timeout)
	}
	cliBin := buildConformanceCLIBinary(t)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, cliBin, args...)
	cmd.Dir = repoRootDir
	if len(extraEnv) > 0 {
		cmd.Env = mergeEnv(os.Environ(), extraEnv)
	}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := cmd.Process.Signal(syscall.SIGTERM)
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	// Harness teardown worst case: 200ms timeoutUnwindGrace + 1s ShutdownProxy + 1s waitGone
	// + 5s DefaultShutdownTimeout + 2s..4.25s killAndReap (9.2s-11.45s, up to ~12.2s if
	// detectProxyCrash overlapped). cmd.Wait() still returns immediately when the CLI exits.
	cmd.WaitDelay = 15 * time.Second

	var stdoutBuf, stderrBuf syncBuffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)

	res := CLIResult{
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
		Duration: elapsed,
		TimedOut: errors.Is(ctx.Err(), context.DeadlineExceeded),
		Err:      err,
	}

	if err == nil {
		res.ExitCode = 0
	} else {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			res.ExitCode = exitErr.ExitCode()
		} else {
			res.ExitCode = -1
		}
	}

	markerInputs := make([]string, 0, len(args)+len(extraEnv))
	markerInputs = append(markerInputs, args...)
	markerInputs = append(markerInputs, extraEnv...)
	markers := extractRunMarkers(markerInputs)
	for _, marker := range markers {
		assertNoOrphanProxy(t, marker)
		m := marker
		t.Cleanup(func() {
			killProcessesWithMarker(m)
		})
	}

	return res
}

func extractRunMarkers(args []string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, a := range args {
		for _, m := range runMarkerRegexp.FindAllString(a, -1) {
			if _, ok := seen[m]; !ok {
				seen[m] = struct{}{}
				out = append(out, m)
			}
		}
	}
	return out
}

type procMatch struct {
	PID     int
	State   string
	Cmdline string
}

// assertNoOrphanProxy verifies no non-zombie processes matching marker remain alive on the host.
func assertNoOrphanProxy(t *testing.T, marker string) {
	t.Helper()
	assertNoOrphanProxyTB(t, marker)
}

func assertNoOrphanProxyTB(tb testing.TB, marker string) {
	tb.Helper()
	if strings.TrimSpace(marker) == "" {
		tb.Fatalf("assertNoOrphanProxy: marker must not be empty")
	}

	const maxWait = 500 * time.Millisecond
	const pollStep = 25 * time.Millisecond

	deadline := time.Now().Add(maxWait)
	var matches []procMatch
	for {
		var err error
		matches, err = findProcessesWithMarker(marker)
		if err != nil {
			tb.Fatalf("assertNoOrphanProxy: scan /proc failed: %v", err)
		}
		if len(matches) == 0 {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(pollStep)
	}

	var desc []string
	for _, m := range matches {
		desc = append(desc, fmt.Sprintf("pid=%d state=%s cmd=%q", m.PID, m.State, m.Cmdline))
	}
	tb.Errorf("orphan proxy process(es) still running for marker %q: %s", marker, strings.Join(desc, "; "))
}

func findProcessesWithMarker(marker string) ([]procMatch, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	excluded := collectSelfAndAncestorPIDs()
	var matches []procMatch

	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(ent.Name())
		if err != nil || pid <= 0 {
			continue
		}
		if excluded[pid] {
			continue
		}

		statRaw, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "stat"))
		if err != nil {
			continue
		}
		state, _, ok := parseProcStat(string(statRaw))
		if !ok || state == "Z" || state == "X" {
			continue
		}

		cmdRaw, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "cmdline"))
		if err != nil || len(cmdRaw) == 0 {
			continue
		}
		args := strings.Split(strings.TrimRight(string(cmdRaw), "\x00"), "\x00")
		joined := strings.Join(args, " ")
		matched := false
		for _, a := range args {
			if hasExactMarkerToken(a, marker) {
				matched = true
				break
			}
		}
		if matched {
			matches = append(matches, procMatch{
				PID:     pid,
				State:   state,
				Cmdline: joined,
			})
		}
	}
	return matches, nil
}

func isMarkerIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '-'
}

func hasExactMarkerToken(arg, marker string) bool {
	if marker == "" || arg == "" {
		return false
	}
	if runMarkerRegexp.MatchString(marker) && runMarkerRegexp.FindString(marker) == marker {
		for _, loc := range runMarkerRegexp.FindAllStringIndex(arg, -1) {
			if arg[loc[0]:loc[1]] == marker && (loc[1] == len(arg) || !isMarkerIdentChar(arg[loc[1]])) {
				return true
			}
		}
		return false
	}
	for start := 0; start <= len(arg)-len(marker); {
		idx := strings.Index(arg[start:], marker)
		if idx < 0 {
			return false
		}
		pos := start + idx
		end := pos + len(marker)
		leftOK := pos == 0 || !isMarkerIdentChar(arg[pos-1])
		rightOK := end == len(arg) || !isMarkerIdentChar(arg[end])
		if leftOK && rightOK {
			return true
		}
		start = pos + 1
	}
	return false
}

func collectSelfAndAncestorPIDs() map[int]bool {
	excluded := make(map[int]bool)
	pid := os.Getpid()
	for depth := 0; pid > 1 && depth < 64; depth++ {
		if excluded[pid] {
			break
		}
		excluded[pid] = true
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			break
		}
		_, ppid, ok := parseProcStat(string(raw))
		if !ok || ppid <= 1 {
			break
		}
		pid = ppid
	}
	return excluded
}

func parseProcStat(raw string) (state string, ppid int, ok bool) {
	idx := strings.LastIndexByte(raw, ')')
	if idx < 0 || idx+1 >= len(raw) {
		return "", 0, false
	}
	fields := strings.Fields(raw[idx+1:])
	if len(fields) < 2 {
		return "", 0, false
	}
	p, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", 0, false
	}
	return fields[0], p, true
}

func killProcessesWithMarker(marker string) {
	if strings.TrimSpace(marker) == "" {
		return
	}
	matches, err := findProcessesWithMarker(marker)
	if err != nil {
		return
	}
	for _, m := range matches {
		if proc, err := os.FindProcess(m.PID); err == nil {
			_ = proc.Signal(syscall.SIGKILL)
		}
	}
}
