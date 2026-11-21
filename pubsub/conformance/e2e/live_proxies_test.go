//go:build live_proxies

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

/*
Live Cross-Client Conformance Verification (`//go:build live_proxies`)

Invokes the standalone `cmd/pubsub-conformance` CLI binary against real (or
env-configured) Go (`cloud.google.com/go/pubsub/v2`), C++ (`google-cloud-cpp`),
and Rust (`google-cloud-rust`) Pub/Sub conformance proxies, then runs `--compare`
to verify:
  - Bounded non-crashing completion across all 65 scenarios / 13 categories
  - R2 optional OTel trace verification (`supports_monitoring=true` on Go & C++ vs
    `supports_monitoring=false` on Rust with `TRAC-03` functional pass +
    `SKIPPED_NO_MONITORING` trace status)
  - Structured cross-client JSON and Markdown behavioral difference reporting
  - Zero leaked proxy processes via `E2E_LIVE_MARKER` `/proc/<pid>/environ`
    tracking (never passing unsupported `--marker` CLI flags to real proxies).
*/

package e2e_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

// --- Local Unexported Constants, Types & Helpers (stdlib-only, t4Live* prefix) ---

var t4LiveAll13Categories = []string{
	"publish",
	"ordering",
	"flow_control",
	"compression",
	"hedging",
	"retry",
	"pull",
	"lease",
	"dlq",
	"eod",
	"lifecycle",
	"traces",
	"config",
}

type t4LiveTargetSpec struct {
	Name   string
	EnvVar string
	Lang   string
}

type t4LiveResolvedProxy struct {
	Spec    t4LiveTargetSpec
	Command string
	Args    []string
	Source  string
	Found   bool
	Probed  []string
	Err     error
}

type t4LiveRunOutcome struct {
	Target   t4LiveResolvedProxy
	Res      CLIResult
	JSONPath string
	MDPath   string
	Marker   string
}

func t4LiveRawString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

func t4LiveCheckExecutableFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("%q is a directory", path)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%q is not a regular file", path)
	}
	if info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%q is not executable", path)
	}
	return nil
}

func t4LiveSplitCommandLine(raw string) ([]string, error) {
	var tokens []string
	var cur strings.Builder
	inToken := false
	quote := rune(0)

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
				tokens = append(tokens, cur.String())
				cur.Reset()
				inToken = false
			}
		case r == '\'' || r == '"':
			quote = r
			inToken = true
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

func t4LiveParseCommandSpec(raw string) (string, []string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil, fmt.Errorf("command cannot be empty")
	}
	if info, err := os.Stat(trimmed); err == nil && !info.IsDir() {
		if err := t4LiveCheckExecutableFile(trimmed); err != nil {
			return "", nil, err
		}
		abs, absErr := filepath.Abs(trimmed)
		if absErr != nil {
			return "", nil, absErr
		}
		return abs, nil, nil
	}

	tokens, err := t4LiveSplitCommandLine(trimmed)
	if err != nil {
		return "", nil, err
	}
	exe := tokens[0]
	var resolvedExe string
	if strings.ContainsRune(exe, '/') {
		abs, absErr := filepath.Abs(exe)
		if absErr != nil {
			return "", nil, absErr
		}
		if err := t4LiveCheckExecutableFile(abs); err != nil {
			return "", nil, err
		}
		resolvedExe = abs
	} else {
		lp, lpErr := exec.LookPath(exe)
		if lpErr != nil {
			return "", nil, lpErr
		}
		abs, absErr := filepath.Abs(lp)
		if absErr != nil {
			return "", nil, absErr
		}
		if err := t4LiveCheckExecutableFile(abs); err != nil {
			return "", nil, err
		}
		resolvedExe = abs
	}
	return resolvedExe, slices.Clone(tokens[1:]), nil
}

func t4LiveWorkspaceRoots() []string {
	if raw := strings.TrimSpace(os.Getenv("CONFORMANCE_WORKSPACES_ROOT")); raw != "" {
		return []string{filepath.Clean(raw)}
	}
	var roots []string
	seen := make(map[string]bool)
	addRoot := func(p string) {
		c := filepath.Clean(p)
		if c != "" && c != "." && !seen[c] {
			seen[c] = true
			roots = append(roots, c)
		}
	}
	if repoRootDir != "" {
		addRoot(filepath.Dir(filepath.Dir(repoRootDir)))
		addRoot(filepath.Dir(repoRootDir))
	}
	return roots
}

func t4LiveIsRegularFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode().IsRegular()
}

func t4LiveResolveGoProxy(t *testing.T, spec t4LiveTargetSpec, roots []string, worktreeNames []string) t4LiveResolvedProxy {
	t.Helper()
	res := t4LiveResolvedProxy{Spec: spec}
	for _, root := range roots {
		for _, wt := range worktreeNames {
			wtRoot := filepath.Join(root, "google-cloud-go", wt)
			pkgDir := filepath.Join(wtRoot, "pubsub", "conformanceproxy")
			for _, bin := range []string{
				filepath.Join(pkgDir, "go-conformance-proxy"),
				filepath.Join(pkgDir, "conformanceproxy"),
				filepath.Join(wtRoot, "bin", "go-conformance-proxy"),
			} {
				res.Probed = append(res.Probed, bin)
				if err := t4LiveCheckExecutableFile(bin); err == nil {
					res.Command = bin
					res.Source = "prebuilt:" + bin
					res.Found = true
					return res
				}
			}

			goMod := filepath.Join(pkgDir, "go.mod")
			mainGo := filepath.Join(pkgDir, "main.go")
			res.Probed = append(res.Probed, pkgDir)
			if t4LiveIsRegularFile(goMod) && t4LiveIsRegularFile(mainGo) {
				goBin, err := exec.LookPath("go")
				if err != nil {
					res.Err = fmt.Errorf("found Go proxy source at %q, but 'go' binary not found in PATH: %w", pkgDir, err)
					return res
				}
				buildDir := t.TempDir()
				outBin := filepath.Join(buildDir, "go-conformance-proxy")
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				cmd := exec.CommandContext(ctx, goBin, "build", "-C", pkgDir, "-mod=readonly", "-o", outBin, ".")
				out, buildErr := cmd.CombinedOutput()
				cancel()
				if buildErr != nil {
					res.Err = fmt.Errorf("failed building Go proxy at %q: %v\n%s", pkgDir, buildErr, string(out))
					return res
				}
				if err := t4LiveCheckExecutableFile(outBin); err != nil {
					res.Err = fmt.Errorf("built Go proxy binary %q invalid: %v", outBin, err)
					return res
				}
				res.Command = outBin
				res.Source = "go-build:" + pkgDir
				res.Found = true
				return res
			}
		}
	}
	return res
}

func t4LiveBazelCacheBases() []string {
	var bases []string
	seen := make(map[string]bool)
	add := func(p string) {
		p = strings.TrimSpace(p)
		if p == "" {
			return
		}
		c := filepath.Clean(p)
		if !seen[c] {
			seen[c] = true
			bases = append(bases, c)
		}
	}
	if home := strings.TrimSpace(os.Getenv("HOME")); home != "" {
		add(filepath.Join(home, ".cache", "bazel"))
	}
	if cacheDir, err := os.UserCacheDir(); err == nil && cacheDir != "" {
		add(filepath.Join(cacheDir, "bazel"))
	}
	if testTmp := strings.TrimSpace(os.Getenv("TEST_TMPDIR")); testTmp != "" {
		add(testTmp)
	}
	return bases
}

func t4LiveBazelMD5Hashes(wtRoot string) []string {
	clean := filepath.Clean(wtRoot)
	candidates := []string{clean}
	if eval, err := filepath.EvalSymlinks(clean); err == nil && eval != "" && eval != clean {
		candidates = append(candidates, eval)
	}
	var out []string
	seen := make(map[string]bool)
	for _, p := range candidates {
		sum := md5.Sum([]byte(p))
		h := hex.EncodeToString(sum[:])
		if !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	return out
}

func t4LiveIsBazelExecConfig(p string) bool {
	slash := filepath.ToSlash(p)
	idx := strings.Index(slash, "/bazel-out/")
	if idx < 0 {
		return false
	}
	rest := slash[idx+len("/bazel-out/"):]
	cfg, _, ok := strings.Cut(rest, "/")
	return ok && (strings.HasSuffix(cfg, "-exec") || strings.Contains(cfg, "-exec-"))
}

func t4LiveHasSanitizerToken(seg string) bool {
	for _, tok := range strings.FieldsFunc(strings.ToLower(seg), func(r rune) bool { return r == '-' || r == '_' }) {
		switch tok {
		case "asan", "tsan", "msan", "ubsan":
			return true
		}
	}
	return false
}

func t4LiveIsSanitizerConfig(p, wtRoot string) bool {
	slash := filepath.ToSlash(p)
	if idx := strings.Index(slash, "/bazel-out/"); idx >= 0 {
		rest := slash[idx+len("/bazel-out/"):]
		if cfg, _, ok := strings.Cut(rest, "/"); ok && t4LiveHasSanitizerToken(cfg) {
			return true
		}
	}
	if wtRoot != "" {
		if rel, err := filepath.Rel(wtRoot, p); err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".." {
			for _, seg := range strings.Split(filepath.ToSlash(rel), "/") {
				if t4LiveHasSanitizerToken(seg) {
					return true
				}
			}
		}
	}
	return false
}

func t4LiveIsExcludedCppCandidate(p, wtRoot string) bool {
	if t4LiveIsBazelExecConfig(p) || t4LiveIsSanitizerConfig(p, wtRoot) {
		return true
	}
	if eval, err := filepath.EvalSymlinks(p); err == nil && eval != "" {
		if t4LiveIsBazelExecConfig(eval) || t4LiveIsSanitizerConfig(eval, wtRoot) {
			return true
		}
	}
	return false
}

func t4LiveResolveCppProxy(spec t4LiveTargetSpec, roots []string, worktreeNames []string) t4LiveResolvedProxy {
	res := t4LiveResolvedProxy{Spec: spec}
	bazelBases := t4LiveBazelCacheBases()

	type candidate struct {
		path    string
		source  string
		modTime time.Time
		tier    int
	}
	var candidates []candidate

	for _, root := range roots {
		for _, wt := range worktreeNames {
			wtRoot := filepath.Join(root, "google-cloud-cpp", wt)
			wtInfo, err := os.Stat(wtRoot)
			if err != nil || !wtInfo.IsDir() {
				res.Probed = append(res.Probed, wtRoot)
				continue
			}

			directPaths := []string{
				filepath.Join(wtRoot, "bazel-bin", "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main"),
				filepath.Join(wtRoot, "cmake-out", "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main"),
				filepath.Join(wtRoot, "cmake-out", "google", "cloud", "pubsub", "pubsub_test_proxy_main"),
				filepath.Join(wtRoot, "build", "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main"),
				filepath.Join(wtRoot, "build", "google", "cloud", "pubsub", "pubsub_test_proxy_main"),
				filepath.Join(wtRoot, "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main"),
			}
			for _, bin := range directPaths {
				res.Probed = append(res.Probed, bin)
				if t4LiveIsExcludedCppCandidate(bin, wtRoot) {
					continue
				}
				if err := t4LiveCheckExecutableFile(bin); err != nil {
					continue
				}
				if info, sErr := os.Stat(bin); sErr == nil {
					candidates = append(candidates, candidate{path: bin, source: "prebuilt:" + bin, modTime: info.ModTime(), tier: 0})
				}
			}

			hashes := t4LiveBazelMD5Hashes(wtRoot)
			for _, base := range bazelBases {
				for _, hash := range hashes {
					pattern := filepath.Join(base, "_bazel_*", hash, "execroot", "*", "bazel-out", "*", "bin", "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main")
					res.Probed = append(res.Probed, pattern)
					matches, gErr := filepath.Glob(pattern)
					if gErr != nil {
						continue
					}
					for _, m := range matches {
						if t4LiveIsExcludedCppCandidate(m, wtRoot) {
							continue
						}
						if err := t4LiveCheckExecutableFile(m); err != nil {
							continue
						}
						if info, sErr := os.Stat(m); sErr == nil {
							candidates = append(candidates, candidate{path: m, source: "bazel-cache:" + m, modTime: info.ModTime(), tier: 1})
						}
					}
				}
			}
		}
	}

	if len(candidates) > 0 {
		slices.SortFunc(candidates, func(a, b candidate) int {
			if a.modTime.After(b.modTime) {
				return -1
			}
			if a.modTime.Before(b.modTime) {
				return 1
			}
			if a.tier != b.tier {
				if a.tier < b.tier {
					return -1
				}
				return 1
			}
			return strings.Compare(a.path, b.path)
		})
		best := candidates[0]
		res.Command = best.path
		res.Source = best.source
		res.Found = true
	}
	return res
}

func t4LiveResolveRustProxy(spec t4LiveTargetSpec, roots []string, worktreeNames []string) t4LiveResolvedProxy {
	res := t4LiveResolvedProxy{Spec: spec}
	cargoTargetDir := strings.TrimSpace(os.Getenv("CARGO_TARGET_DIR"))

	type candidate struct {
		path      string
		modTime   time.Time
		isRelease bool
	}
	var hits []candidate

	addCandidate := func(bin string, isRelease bool) {
		res.Probed = append(res.Probed, bin)
		if err := t4LiveCheckExecutableFile(bin); err != nil {
			return
		}
		info, err := os.Stat(bin)
		if err != nil {
			return
		}
		hits = append(hits, candidate{path: bin, modTime: info.ModTime(), isRelease: isRelease})
	}

	for _, root := range roots {
		for _, wt := range worktreeNames {
			wtRoot := filepath.Join(root, "google-cloud-rust", wt)
			wtInfo, err := os.Stat(wtRoot)
			if err != nil || !wtInfo.IsDir() {
				res.Probed = append(res.Probed, wtRoot)
				continue
			}

			addCandidate(filepath.Join(wtRoot, "target", "release", "pubsub-conformance-proxy"), true)
			addCandidate(filepath.Join(wtRoot, "target", "debug", "pubsub-conformance-proxy"), false)
			addCandidate(filepath.Join(wtRoot, "tests", "pubsub-conformance-proxy", "target", "release", "pubsub-conformance-proxy"), true)
			addCandidate(filepath.Join(wtRoot, "tests", "pubsub-conformance-proxy", "target", "debug", "pubsub-conformance-proxy"), false)

			if cargoTargetDir != "" && t4LiveIsRegularFile(filepath.Join(wtRoot, "tests", "pubsub-conformance-proxy", "Cargo.toml")) {
				addCandidate(filepath.Join(cargoTargetDir, "release", "pubsub-conformance-proxy"), true)
				addCandidate(filepath.Join(cargoTargetDir, "debug", "pubsub-conformance-proxy"), false)
			}
		}
	}

	if len(hits) > 0 {
		slices.SortFunc(hits, func(a, b candidate) int {
			if a.modTime.After(b.modTime) {
				return -1
			}
			if a.modTime.Before(b.modTime) {
				return 1
			}
			if a.isRelease != b.isRelease {
				if a.isRelease {
					return -1
				}
				return 1
			}
			return strings.Compare(a.path, b.path)
		})
		best := hits[0].path
		res.Command = best
		res.Source = "prebuilt:" + best
		res.Found = true
	}
	return res
}

func t4LiveParseRequireEnv(t *testing.T) bool {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CONFORMANCE_REQUIRE_LIVE_PROXIES"))
	switch strings.ToLower(raw) {
	case "", "0", "false", "no":
		return false
	case "1", "true", "yes":
		return true
	default:
		t.Fatalf("invalid CONFORMANCE_REQUIRE_LIVE_PROXIES=%q (expected 1/true/yes or 0/false/no)", raw)
		return false
	}
}

func t4LiveResolveAllProxies(t *testing.T) []t4LiveResolvedProxy {
	t.Helper()

	specs := []t4LiveTargetSpec{
		{Name: "go", EnvVar: "CONFORMANCE_GO_PROXY_CMD", Lang: "go"},
		{Name: "cpp", EnvVar: "CONFORMANCE_CPP_PROXY_CMD", Lang: "cpp"},
		{Name: "rust", EnvVar: "CONFORMANCE_RUST_PROXY_CMD", Lang: "rust"},
	}

	resolved := make([]t4LiveResolvedProxy, len(specs))

	// Step 1: Check explicit env vars FIRST for all 3 targets.
	for i, sp := range specs {
		raw := strings.TrimSpace(os.Getenv(sp.EnvVar))
		if raw == "" {
			continue
		}
		cmd, args, err := t4LiveParseCommandSpec(raw)
		if err != nil {
			t.Fatalf("invalid %s=%q: %v", sp.EnvVar, raw, err)
		}
		resolved[i] = t4LiveResolvedProxy{
			Spec:    sp,
			Command: cmd,
			Args:    args,
			Source:  "env:" + sp.EnvVar,
			Found:   true,
		}
	}

	// Step 2: Auto-discover any targets whose env var was unset/empty.
	roots := t4LiveWorkspaceRoots()
	worktreeNames := []string{"pubsub-conformance-proxy", "pubsub-conformance"}
	for i, sp := range specs {
		if resolved[i].Found {
			continue
		}
		var r t4LiveResolvedProxy
		switch sp.Name {
		case "go":
			r = t4LiveResolveGoProxy(t, sp, roots, worktreeNames)
		case "cpp":
			r = t4LiveResolveCppProxy(sp, roots, worktreeNames)
		case "rust":
			r = t4LiveResolveRustProxy(sp, roots, worktreeNames)
		}
		resolved[i] = r
	}

	// Step 3: Require vs Skip if any target is missing/unbuilt or errored during resolution.
	requireLive := t4LiveParseRequireEnv(t)
	var missing []t4LiveResolvedProxy
	for _, r := range resolved {
		if !r.Found || r.Err != nil {
			missing = append(missing, r)
		}
	}
	if len(missing) > 0 {
		rootHint := "<workspaces_root>"
		if len(roots) > 0 {
			rootHint = roots[0]
		}
		var b strings.Builder
		fmt.Fprintf(&b, "live proxy test missing %d/%d proxy binary(ies):\n", len(missing), len(resolved))
		for _, m := range missing {
			if m.Err != nil {
				fmt.Fprintf(&b, "  - %s (env %s): resolution error: %v; probed: %s\n", m.Spec.Name, m.Spec.EnvVar, m.Err, strings.Join(m.Probed, ", "))
			} else {
				fmt.Fprintf(&b, "  - %s (env %s): not found; probed: %s\n", m.Spec.Name, m.Spec.EnvVar, strings.Join(m.Probed, ", "))
			}
		}
		fmt.Fprintf(&b, "Build or configure proxies via:\n")
		fmt.Fprintf(&b, "  Go:   GOPROXY=off go build -C %s/google-cloud-go/pubsub-conformance-proxy/pubsub/conformanceproxy -mod=readonly -o /tmp/go-conformance-proxy . (or set CONFORMANCE_GO_PROXY_CMD)\n", rootHint)
		fmt.Fprintf(&b, "  C++:  bazelisk build //google/cloud/pubsub/test_proxy:pubsub_test_proxy_main (in %s/google-cloud-cpp/pubsub-conformance-proxy, or set CONFORMANCE_CPP_PROXY_CMD)\n", rootHint)
		fmt.Fprintf(&b, "  Rust: cargo build --offline --locked -p pubsub-conformance-proxy --bin pubsub-conformance-proxy (in %s/google-cloud-rust/pubsub-conformance-proxy, or set CONFORMANCE_RUST_PROXY_CMD)\n", rootHint)
		if requireLive {
			t.Fatalf("%s", b.String())
		}
		t.Skipf("%s", b.String())
	}

	return resolved
}

func t4LiveFindProcessesByEnv(envKey, envVal string) ([]procMatch, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	excluded := collectSelfAndAncestorPIDs()
	needle := []byte(envKey + "=" + envVal)
	var matches []procMatch

	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(ent.Name())
		if err != nil || pid <= 0 || excluded[pid] {
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

		envRaw, err := os.ReadFile(filepath.Join("/proc", ent.Name(), "environ"))
		if err != nil || len(envRaw) == 0 {
			continue
		}
		matched := false
		for _, kv := range bytes.Split(envRaw, []byte{0}) {
			if bytes.Equal(kv, needle) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}

		cmdline := ""
		if cmdRaw, cErr := os.ReadFile(filepath.Join("/proc", ent.Name(), "cmdline")); cErr == nil && len(cmdRaw) > 0 {
			cmdline = strings.Join(strings.Split(strings.TrimRight(string(cmdRaw), "\x00"), "\x00"), " ")
		}
		matches = append(matches, procMatch{
			PID:     pid,
			State:   state,
			Cmdline: cmdline,
		})
	}
	return matches, nil
}

func t4LiveAssertNoOrphanByEnv(t *testing.T, marker string) {
	t.Helper()
	const maxWait = 500 * time.Millisecond
	const pollStep = 25 * time.Millisecond

	deadline := time.Now().Add(maxWait)
	var matches []procMatch
	for {
		var err error
		matches, err = t4LiveFindProcessesByEnv("E2E_LIVE_MARKER", marker)
		if err != nil {
			t.Fatalf("t4LiveAssertNoOrphanByEnv(%q): scan /proc failed: %v", marker, err)
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
	t.Errorf("orphan proxy process(es) still running for E2E_LIVE_MARKER=%q: %s", marker, strings.Join(desc, "; "))
}

func t4LiveKillProcessesByEnv(marker string) {
	if strings.TrimSpace(marker) == "" {
		return
	}
	matches, err := t4LiveFindProcessesByEnv("E2E_LIVE_MARKER", marker)
	if err != nil {
		return
	}
	for _, m := range matches {
		if proc, err := os.FindProcess(m.PID); err == nil {
			_ = proc.Signal(syscall.SIGKILL)
		}
	}
}

func t4LiveCreateLauncher(t *testing.T, baseDir string, res t4LiveResolvedProxy, marker string) string {
	t.Helper()
	// Place the run marker as a directory segment so runner_test.go's
	// extractRunMarkers & hasExactMarkerToken match cleanly at '/', and
	// NEVER forward --marker on the proxy CLI (real Go/C++/Rust proxies fail on unknown flags).
	markerDir := filepath.Join(baseDir, marker)
	if err := os.MkdirAll(markerDir, 0o755); err != nil {
		t.Fatalf("create launcher dir %q: %v", markerDir, err)
	}
	launcherPath := filepath.Join(markerDir, "live-"+res.Spec.Name)
	if len(res.Args) == 0 {
		if err := os.Symlink(res.Command, launcherPath); err != nil {
			t.Fatalf("symlink launcher %q -> %q: %v", launcherPath, res.Command, err)
		}
		return launcherPath
	}

	var b strings.Builder
	b.WriteString("#!/bin/sh\nexec ")
	b.WriteString(shellQuote(res.Command))
	for _, arg := range res.Args {
		b.WriteString(" ")
		b.WriteString(shellQuote(arg))
	}
	b.WriteString(" \"$@\"\n")
	writeExecutableFile(t, launcherPath, []byte(b.String()))
	return launcherPath
}

func t4LiveFindMDTable(t *testing.T, md *ParsedMarkdownReport, wantHeaders ...string) *MarkdownTable {
	t.Helper()
	if md == nil {
		t.Fatalf("t4LiveFindMDTable: nil Markdown report")
	}
	for i := range md.Tables {
		if slices.Equal(md.Tables[i].Headers, wantHeaders) {
			return &md.Tables[i]
		}
	}
	var got [][]string
	for _, tbl := range md.Tables {
		got = append(got, tbl.Headers)
	}
	t.Fatalf("Markdown table with headers %v not found; got tables=%v", wantHeaders, got)
	return nil
}

func t4LiveFindMDTableRow(t *testing.T, tbl *MarkdownTable, match map[string]string) map[string]string {
	t.Helper()
	if tbl == nil {
		t.Fatalf("t4LiveFindMDTableRow: nil table")
	}
	for _, row := range tbl.Rows {
		matched := true
		for k, want := range match {
			if row[k] != want {
				matched = false
				break
			}
		}
		if matched {
			return row
		}
	}
	t.Fatalf("Markdown table %v missing row matching %v; rows=%v", tbl.Headers, match, tbl.Rows)
	return nil
}

func t4LiveMustTarget(t *testing.T, rep *ParsedReport, name string) *ParsedTargetReport {
	t.Helper()
	tr, ok := rep.Targets[name]
	if !ok || tr == nil {
		t.Fatalf("target %q not found in report", name)
	}
	return tr
}

func t4LiveMustTargetScenario(t *testing.T, tr *ParsedTargetReport, id string) *ParsedScenarioResult {
	t.Helper()
	for i := range tr.Scenarios {
		if tr.Scenarios[i].ID == id {
			return &tr.Scenarios[i]
		}
	}
	t.Fatalf("scenario %q not found in target %q (total=%d)", id, tr.TargetName, len(tr.Scenarios))
	return nil
}

func t4LiveMustDiff(t *testing.T, rep *ParsedReport, kind, idOrField string) *ParsedDiffEntry {
	t.Helper()
	for i := range rep.Differences {
		d := &rep.Differences[i]
		if d.Kind != kind {
			continue
		}
		if idOrField == "" || d.ScenarioID == idOrField || d.Field == idOrField {
			return d
		}
	}
	t.Fatalf("difference kind=%q target=%q not found (total=%d)", kind, idOrField, len(rep.Differences))
	return nil
}

func t4LiveAssertAll13Categories(t *testing.T, scope string, scenarios []ParsedScenarioResult) {
	t.Helper()
	seen := make(map[string]int, len(t4LiveAll13Categories))
	for _, sc := range scenarios {
		if sc.Category != "" {
			seen[sc.Category]++
		}
	}
	for _, cat := range t4LiveAll13Categories {
		if seen[cat] == 0 {
			t.Errorf("%s: missing scenario category %q (seen=%v)", scope, cat, seen)
		}
	}
}

func t4LiveSuiteTimeout(t *testing.T) time.Duration {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CONFORMANCE_LIVE_PROXY_SUITE_TIMEOUT"))
	if raw == "" {
		return 8 * time.Minute
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		t.Fatalf("invalid CONFORMANCE_LIVE_PROXY_SUITE_TIMEOUT=%q (expected positive Go duration): err=%v", raw, err)
	}
	return d
}

// --- Live Cross-Client E2E Diff Test ---

func TestT4_Live_GoCppRustProxiesDiff(t *testing.T) {
	// Sequential at top level to avoid CPU contention with concurrent E2E tests.
	resolved := t4LiveResolveAllProxies(t)
	suiteTimeout := t4LiveSuiteTimeout(t)
	cliTimeout := suiteTimeout + 60*time.Second

	// Ensure CLI binary is built once before launching concurrent target goroutines.
	buildConformanceCLIBinary(t)

	// Pre-flight scenario catalog check (--list).
	listRes := runCLI(t, 15*time.Second, "--list")
	if listRes.ExitCode != 0 || listRes.TimedOut || listRes.Err != nil {
		t.Fatalf("pre-flight --list failed: exit=%d timedOut=%v err=%v\nstderr:\n%s",
			listRes.ExitCode, listRes.TimedOut, listRes.Err, listRes.Stderr)
	}
	var listLines []string
	listedCats := make(map[string]bool)
	for _, line := range strings.Split(strings.ReplaceAll(listRes.Stdout, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) >= 2 {
			listedCats[fields[1]] = true
		}
		listLines = append(listLines, trimmed)
	}
	if len(listLines) != 65 {
		t.Fatalf("pre-flight --list returned %d scenarios, want 65", len(listLines))
	}
	for _, cat := range t4LiveAll13Categories {
		if !listedCats[cat] {
			t.Errorf("pre-flight --list missing category %q", cat)
		}
	}

	outDir := t.TempDir()
	outcomes := make([]t4LiveRunOutcome, len(resolved))
	launchers := make([]string, len(resolved))

	for i, r := range resolved {
		marker := newRunMarker(t)
		launchers[i] = t4LiveCreateLauncher(t, outDir, r, marker)
		outcomes[i] = t4LiveRunOutcome{
			Target:   r,
			JSONPath: filepath.Join(outDir, r.Spec.Name+".json"),
			MDPath:   filepath.Join(outDir, r.Spec.Name+".md"),
			Marker:   marker,
		}
		m := marker
		t.Cleanup(func() {
			t4LiveKillProcessesByEnv(m)
		})
	}

	// Execute Go, C++, and Rust targets concurrently and store outcomes safely in slice.
	var wg sync.WaitGroup
	for i := range outcomes {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			oc := &outcomes[idx]
			res := runCLIWithEnv(
				t, cliTimeout,
				[]string{"E2E_LIVE_MARKER=" + oc.Marker},
				"--proxy-cmd="+launchers[idx],
				"--scenario-timeout=45s",
				"--suite-timeout="+suiteTimeout.String(),
				"--report-json="+oc.JSONPath,
				"--report-md="+oc.MDPath,
			)
			oc.Res = res
		}()
	}
	wg.Wait()

	// Verify zero orphan processes via E2E_LIVE_MARKER /proc/<pid>/environ inspection.
	for _, oc := range outcomes {
		t4LiveAssertNoOrphanByEnv(t, oc.Marker)
	}

	// Validate each target report.
	for _, oc := range outcomes {
		name := oc.Target.Spec.Name
		if oc.Res.TimedOut || (oc.Res.ExitCode != 0 && oc.Res.ExitCode != 1) {
			t.Fatalf("%s proxy run aborted (source=%s cmd=%q args=%v): exit=%d timedOut=%v err=%v\nstdout:\n%s\nstderr:\n%s",
				name, oc.Target.Source, oc.Target.Command, oc.Target.Args,
				oc.Res.ExitCode, oc.Res.TimedOut, oc.Res.Err, oc.Res.Stdout, oc.Res.Stderr)
		}

		rep := ReadJSONReport(t, oc.JSONPath)
		md := ReadMarkdownReport(t, oc.MDPath)
		if len(md.Tables) < 3 {
			t.Errorf("%s Markdown expected >= 3 tables, got %d", name, len(md.Tables))
		}
		if rep.Mode != "single" || rep.InfraError != "" || rep.TimedOut {
			t.Fatalf("%s single report envelope invalid (source=%s): mode=%q infra=%q timedOut=%v",
				name, oc.Target.Source, rep.Mode, rep.InfraError, rep.TimedOut)
		}
		if len(rep.Targets) != 1 {
			t.Fatalf("%s targets len = %d, want 1", name, len(rep.Targets))
		}
		tr := t4LiveMustTarget(t, rep, "default")
		if tr.TargetError != "" || tr.TimedOut {
			t.Fatalf("%s target error/timeout (source=%s env=%s cmd=%q): target_error=%q timed_out=%v",
				name, oc.Target.Source, oc.Target.Spec.EnvVar, oc.Target.Command, tr.TargetError, tr.TimedOut)
		}
		if tr.Summary == nil || tr.Summary.Total != 65 {
			t.Fatalf("%s summary = %+v, want Total=65", name, tr.Summary)
		}
		AssertSummaryMatchesScenarios(t, rep)
		t4LiveAssertAll13Categories(t, name, tr.Scenarios)

		if tr.Summary.Pass <= 0 {
			t.Errorf("%s expected Pass > 0, got %+v", name, *tr.Summary)
		}
		pub01 := t4LiveMustTargetScenario(t, tr, "PUB-01")
		if pub01.Status != StatusPass {
			t.Errorf("%s/PUB-01 expected PASS, got %+v", name, *pub01)
		}
		pull01 := t4LiveMustTargetScenario(t, tr, "PULL-01")
		if pull01.Status != StatusPass {
			t.Errorf("%s/PULL-01 expected PASS, got %+v", name, *pull01)
		}

		if tr.Capabilities.ClientLanguage != oc.Target.Spec.Lang {
			t.Errorf("%s client_language = %q, want %q", name, tr.Capabilities.ClientLanguage, oc.Target.Spec.Lang)
		}
		if tr.Capabilities.ClientLibrary == "" || tr.Capabilities.ClientVersion == "" || tr.Capabilities.Defaults == nil {
			t.Errorf("%s incomplete capabilities metadata: %+v", name, tr.Capabilities)
		}

		// R2 Optional Monitoring verification.
		switch name {
		case "go", "cpp":
			if tr.Capabilities.SupportsMonitoring == nil || !*tr.Capabilities.SupportsMonitoring {
				t.Errorf("%s expected supports_monitoring=true, got %+v", name, tr.Capabilities.SupportsMonitoring)
			}
			if tr.Summary.SkippedNoMonitoring != 0 {
				t.Errorf("%s expected SkippedNoMonitoring=0, got %d", name, tr.Summary.SkippedNoMonitoring)
			}
			trac01 := t4LiveMustTargetScenario(t, tr, "TRAC-01")
			if trac01.Status != StatusPass || trac01.TraceStatus != StatusPass {
				t.Errorf("%s/TRAC-01 expected PASS + trace_status=PASS, got %+v", name, *trac01)
			}
			if pub01.TraceStatus != StatusPass {
				t.Errorf("%s/PUB-01 expected trace_status=PASS, got %q", name, pub01.TraceStatus)
			}
		case "rust":
			if tr.Capabilities.SupportsMonitoring == nil || *tr.Capabilities.SupportsMonitoring {
				t.Errorf("rust expected supports_monitoring=false, got %+v", tr.Capabilities.SupportsMonitoring)
			}
			if tr.Summary.SkippedNoMonitoring != 4 {
				t.Errorf("rust expected SkippedNoMonitoring=4, got %d", tr.Summary.SkippedNoMonitoring)
			}
			for _, id := range []string{"TRAC-01", "TRAC-01b", "TRAC-02", "TRAC-04"} {
				sc := t4LiveMustTargetScenario(t, tr, id)
				if sc.Status != StatusSkippedNoMonitoring {
					t.Errorf("rust/%s expected SKIPPED_NO_MONITORING, got %+v", id, *sc)
				}
			}
			trac03 := t4LiveMustTargetScenario(t, tr, "TRAC-03")
			if trac03.Status != StatusPass || trac03.TraceStatus != StatusSkippedNoMonitoring {
				t.Errorf("rust/TRAC-03 expected PASS + trace_status=SKIPPED_NO_MONITORING, got %+v", *trac03)
			}
			if pub01.TraceStatus != StatusSkippedNoMonitoring {
				t.Errorf("rust/PUB-01 expected trace_status=SKIPPED_NO_MONITORING, got %q", pub01.TraceStatus)
			}
		}

		t.Logf("Target %-4s (%s, %s %s): total=%d pass=%d fail=%d skipped=%d unsupported=%d dur=%s",
			name, oc.Target.Source, tr.Capabilities.ClientLibrary, tr.Capabilities.ClientVersion,
			tr.Summary.Total, tr.Summary.Pass, tr.Summary.Fail,
			tr.Summary.SkippedNoMonitoring, tr.Summary.UnsupportedByClient, oc.Res.Duration.Round(time.Millisecond))
	}

	// Offline 3-way --compare across go.json, cpp.json, rust.json.
	diffJSON := filepath.Join(outDir, "live_compare.json")
	diffMD := filepath.Join(outDir, "live_compare.md")
	cmpRes := runCLI(
		t, 30*time.Second,
		"--compare="+strings.Join([]string{outcomes[0].JSONPath, outcomes[1].JSONPath, outcomes[2].JSONPath}, ","),
		"--report-json="+diffJSON,
		"--report-md="+diffMD,
	)
	if cmpRes.ExitCode != 0 || cmpRes.TimedOut || cmpRes.Err != nil {
		t.Fatalf("offline 3-way --compare failed: exit=%d timedOut=%v err=%v\nstdout:\n%s\nstderr:\n%s",
			cmpRes.ExitCode, cmpRes.TimedOut, cmpRes.Err, cmpRes.Stdout, cmpRes.Stderr)
	}

	diffRep := ReadJSONReport(t, diffJSON)
	if diffRep.Mode != "compare" || t4LiveRawString(diffRep.Raw, "schema_version") != "1.0" {
		t.Errorf("compare report header invalid: mode=%q schema_version=%#v", diffRep.Mode, diffRep.Raw["schema_version"])
	}
	AssertSummaryMatchesScenarios(t, diffRep)
	if len(diffRep.Targets) != 3 {
		t.Fatalf("compare targets len = %d, want 3", len(diffRep.Targets))
	}
	for _, name := range []string{"go", "cpp", "rust"} {
		tr := t4LiveMustTarget(t, diffRep, name)
		if tr.TargetError != "" || tr.TimedOut || tr.Summary == nil || tr.Summary.Total != 65 {
			t.Errorf("compare target %q invalid: %+v", name, tr)
		}
	}
	if len(diffRep.Differences) == 0 {
		t.Fatalf("expected non-empty behavioral differences across Go, C++, and Rust")
	}
	for _, d := range diffRep.Differences {
		if d.Kind == "target_error" {
			t.Errorf("unexpected target_error diff in live comparison: %+v", d)
		}
	}

	dMon := t4LiveMustDiff(t, diffRep, "capability", "supports_monitoring")
	wantMon := map[string]string{"go": "true", "cpp": "true", "rust": "false"}
	for k, w := range wantMon {
		if dMon.Verdicts[k] != w {
			t.Errorf("supports_monitoring diff[%q] = %q, want %q", k, dMon.Verdicts[k], w)
		}
	}
	dTrac01 := t4LiveMustDiff(t, diffRep, "scenario_verdict", "TRAC-01")
	wantTrac01 := map[string]string{"go": StatusPass, "cpp": StatusPass, "rust": StatusSkippedNoMonitoring}
	for k, w := range wantTrac01 {
		if dTrac01.Verdicts[k] != w {
			t.Errorf("TRAC-01 diff[%q] = %q, want %q", k, dTrac01.Verdicts[k], w)
		}
	}
	dPub01 := t4LiveMustDiff(t, diffRep, "scenario_trace_delta", "PUB-01")
	wantPub01 := map[string]string{"go": StatusPass, "cpp": StatusPass, "rust": StatusSkippedNoMonitoring}
	for k, w := range wantPub01 {
		if dPub01.Verdicts[k] != w {
			t.Errorf("PUB-01 scenario_trace_delta[%q] = %q, want %q", k, dPub01.Verdicts[k], w)
		}
	}

	diffMDRep := ReadMarkdownReport(t, diffMD)
	if !diffMDRep.HasHeadingContaining("Pub/Sub Cross-Client Conformance Comparison Report") {
		t.Errorf("compare Markdown missing comparison heading")
	}
	sumTbl := t4LiveFindMDTable(t, diffMDRep, "Target", "Language", "Library", "Monitoring", "Total", "Pass", "Fail", "Skipped No Monitoring", "Unsupported By Client")
	for _, name := range []string{"go", "cpp", "rust"} {
		tr := t4LiveMustTarget(t, diffRep, name)
		monStr := "false"
		if tr.Capabilities.SupportsMonitoring != nil && *tr.Capabilities.SupportsMonitoring {
			monStr = "true"
		}
		t4LiveFindMDTableRow(t, sumTbl, map[string]string{
			"Target":                name,
			"Language":              tr.Capabilities.ClientLanguage,
			"Monitoring":            monStr,
			"Total":                 "65",
			"Pass":                  strconv.Itoa(tr.Summary.Pass),
			"Fail":                  strconv.Itoa(tr.Summary.Fail),
			"Skipped No Monitoring": strconv.Itoa(tr.Summary.SkippedNoMonitoring),
			"Unsupported By Client": strconv.Itoa(tr.Summary.UnsupportedByClient),
		})
	}

	capTbl := t4LiveFindMDTable(t, diffMDRep, "Field", "go", "cpp", "rust")
	t4LiveFindMDTableRow(t, capTbl, map[string]string{"Field": "supports_monitoring", "go": "true", "cpp": "true", "rust": "false"})

	matTbl := t4LiveFindMDTable(t, diffMDRep, "Scenario", "Category", "go", "cpp", "rust")
	if len(matTbl.Rows) != 65 {
		t.Errorf("Scenario Matrix rows = %d, want 65", len(matTbl.Rows))
	}
	t4LiveFindMDTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-01", "go": StatusPass, "cpp": StatusPass, "rust": StatusSkippedNoMonitoring})
	t4LiveFindMDTableRow(t, matTbl, map[string]string{"Scenario": "TRAC-03", "go": StatusPass, "rust": StatusPass})

	diffTbl := t4LiveFindMDTable(t, diffMDRep, "Kind", "Scenario / Field", "go", "cpp", "rust", "Description")
	if len(diffTbl.Rows) == 0 {
		t.Errorf("Behavioral Differences table has 0 rows")
	}
	t4LiveFindMDTableRow(t, diffTbl, map[string]string{"Kind": "capability", "Scenario / Field": "supports_monitoring"})
	t4LiveFindMDTableRow(t, diffTbl, map[string]string{"Kind": "scenario_verdict", "Scenario / Field": "TRAC-01"})

	t.Logf("3-way compare complete: total_differences=%d", len(diffRep.Differences))
}

func TestT4LiveIsBazelExecConfig(t *testing.T) {
	cases := []struct {
		path string
		want bool
	}{
		{"/tmp/cache/execroot/ws/bazel-out/k8-opt-exec/bin/pubsub_test_proxy_main", true},
		{"/tmp/cache/execroot/ws/bazel-out/k8-opt-exec-ST-abc123/bin/pubsub_test_proxy_main", true},
		{"/tmp/cache/execroot/ws/bazel-out/k8-fastbuild/bin/pubsub_test_proxy_main", false},
		{"/tmp/cache/execroot/ws/bazel-out/k8-opt/bin/pubsub_test_proxy_main", false},
		{"/tmp/no-bazel-out/k8-opt-exec/bin/pubsub_test_proxy_main", false},
	}
	for _, tc := range cases {
		if got := t4LiveIsBazelExecConfig(tc.path); got != tc.want {
			t.Errorf("t4LiveIsBazelExecConfig(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestT4LiveResolveCppProxy(t *testing.T) {
	mkExec := func(t *testing.T, p string, mt time.Time) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		writeExecutableFile(t, p, []byte("#!/bin/sh\nexit 0\n"))
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	spec := t4LiveTargetSpec{Name: "cpp", EnvVar: "CONFORMANCE_CPP_PROXY_CMD", Lang: "cpp"}

	t.Run("NewerBazelCacheBeatsStaleDirect_AndExcludesSanitizersAndExecST", func(t *testing.T) {
		root := t.TempDir()
		cacheBase := t.TempDir()
		t.Setenv("HOME", t.TempDir())
		t.Setenv("XDG_CACHE_HOME", t.TempDir())
		t.Setenv("TEST_TMPDIR", cacheBase)

		wtRoot := filepath.Join(root, "google-cloud-cpp", "pubsub-conformance")
		if err := os.MkdirAll(wtRoot, 0o755); err != nil {
			t.Fatal(err)
		}
		directBin := filepath.Join(wtRoot, "cmake-out", "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main")
		hashes := t4LiveBazelMD5Hashes(wtRoot)
		cacheHit := func(cfg string) string {
			return filepath.Join(cacheBase, "_bazel_u", hashes[0], "execroot", "ws", "bazel-out", cfg, "bin", "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main")
		}
		tOld := time.Now().Add(-2 * time.Hour)
		tNew := time.Now().Add(-1 * time.Hour)
		tNewest := time.Now().Add(-5 * time.Minute)
		mkExec(t, directBin, tOld)
		goodCache := cacheHit("k8-fastbuild")
		mkExec(t, goodCache, tNew)
		mkExec(t, cacheHit("k8-opt-asan"), tNewest)
		mkExec(t, cacheHit("k8-tsan"), tNewest)
		mkExec(t, cacheHit("k8-opt-msan"), tNewest)
		mkExec(t, cacheHit("k8-opt-exec-ST-123"), tNewest)

		got := t4LiveResolveCppProxy(spec, []string{root}, []string{"pubsub-conformance"})
		if !got.Found || got.Command != goodCache || !strings.HasPrefix(got.Source, "bazel-cache:") {
			t.Fatalf("expected newest valid bazel cache %q, got %+v", goodCache, got)
		}
	})

	t.Run("DirectSymlinkToSanitizerExcluded", func(t *testing.T) {
		root := t.TempDir()
		cacheBase := t.TempDir()
		t.Setenv("HOME", t.TempDir())
		t.Setenv("XDG_CACHE_HOME", t.TempDir())
		t.Setenv("TEST_TMPDIR", cacheBase)

		wtRoot := filepath.Join(root, "google-cloud-cpp", "pubsub-conformance")
		asanTarget := filepath.Join(cacheBase, "bazel-out", "k8-opt-asan", "bin", "pubsub_test_proxy_main")
		mkExec(t, asanTarget, time.Now())
		symlinkDirect := filepath.Join(wtRoot, "bazel-bin", "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main")
		if err := os.MkdirAll(filepath.Dir(symlinkDirect), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(asanTarget, symlinkDirect); err != nil {
			t.Fatal(err)
		}
		fallbackDirect := filepath.Join(wtRoot, "cmake-out", "google", "cloud", "pubsub", "test_proxy", "pubsub_test_proxy_main")
		mkExec(t, fallbackDirect, time.Now().Add(-time.Minute))

		got := t4LiveResolveCppProxy(spec, []string{root}, []string{"pubsub-conformance"})
		if !got.Found || got.Command != fallbackDirect {
			t.Fatalf("expected asan symlink excluded and %q selected, got %+v", fallbackDirect, got)
		}
	})
}

func TestT4LiveResolveGoProxy_ErrorRespectsRequireEnv_Helper(t *testing.T) {
	if os.Getenv("CONFORMANCE_LIVE_RESOLVE_HELPER") != "1" {
		return
	}
	t4LiveResolveAllProxies(t)
}

func TestT4LiveResolveGoProxy_ErrorRespectsRequireEnv(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	root := t.TempDir()
	pkgDir := filepath.Join(root, "google-cloud-go", "pubsub-conformance", "pubsub", "conformanceproxy")
	if err := os.MkdirAll(pkgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"go.mod", "main.go"} {
		if err := os.WriteFile(filepath.Join(pkgDir, f), []byte("module dummy\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("LookPath(go): %v", err)
	}
	fakeBin := t.TempDir()
	fakeGoScript := fmt.Sprintf("#!/bin/sh\ncase \"$*\" in\n  *conformanceproxy*)\n    echo 'synthetic go toolchain error' >&2\n    exit 1\n    ;;\nesac\nexec %s \"$@\"\n", shellQuote(realGo))
	writeExecutableFile(t, filepath.Join(fakeBin, "go"), []byte(fakeGoScript))
	fakeProxy := filepath.Join(t.TempDir(), "ok-proxy")
	writeExecutableFile(t, fakeProxy, []byte("#!/bin/sh\nexit 0\n"))

	for _, tc := range []struct {
		name, reqVal string
		wantFail     bool
	}{
		{"UnsetSkips", "", false},
		{"ZeroSkips", "0", false},
		{"OneFails", "1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(exe, "-test.v", "-test.run=^TestT4LiveResolveGoProxy_ErrorRespectsRequireEnv_Helper$")
			cmd.Dir = repoRootDir
			cmd.Env = mergeEnv(os.Environ(), []string{
				"PATH=" + fakeBin + ":" + os.Getenv("PATH"),
				"CONFORMANCE_LIVE_RESOLVE_HELPER=1",
				"CONFORMANCE_WORKSPACES_ROOT=" + root,
				"CONFORMANCE_GO_PROXY_CMD=",
				"CONFORMANCE_CPP_PROXY_CMD=" + fakeProxy,
				"CONFORMANCE_RUST_PROXY_CMD=" + fakeProxy,
				"CONFORMANCE_REQUIRE_LIVE_PROXIES=" + tc.reqVal,
			})
			outBytes, runErr := cmd.CombinedOutput()
			out := string(outBytes)
			if tc.wantFail {
				if runErr == nil || !strings.Contains(out, "resolution error") || !strings.Contains(out, "synthetic go toolchain error") {
					t.Fatalf("expected failure with resolution error, err=%v out:\n%s", runErr, out)
				}
			} else {
				if runErr != nil || !strings.Contains(out, "--- SKIP:") || !strings.Contains(out, "synthetic go toolchain error") {
					t.Fatalf("expected skip with resolution error, err=%v out:\n%s", runErr, out)
				}
			}
		})
	}
}

func TestT4LiveOrphanDetectionAndKillByEnv(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	marker := newRunMarker(t) + "-live-orphan"

	cmd := exec.Command(exe, "-test.run=^$")
	cmd.Dir = repoRootDir
	cmd.Env = mergeEnv(os.Environ(), []string{
		"GO_WANT_T4LIVE_SLEEP_HELPER=1",
		"E2E_LIVE_MARKER=" + marker,
	})
	if err := cmd.Start(); err != nil {
		t.Fatalf("start orphan helper: %v", err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()
	t.Cleanup(func() {
		t4LiveKillProcessesByEnv(marker)
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		select {
		case <-waitDone:
		case <-time.After(2 * time.Second):
		}
		t4LiveAssertNoOrphanByEnv(t, marker)
	})

	// Poll briefly until child /proc/<pid>/environ is observable.
	deadline := time.Now().Add(2 * time.Second)
	var found []procMatch
	for {
		matches, fErr := t4LiveFindProcessesByEnv("E2E_LIVE_MARKER", marker)
		if fErr != nil {
			t.Fatalf("t4LiveFindProcessesByEnv: %v", fErr)
		}
		if len(matches) > 0 {
			found = matches
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting to detect orphan pid=%d for E2E_LIVE_MARKER=%q", cmd.Process.Pid, marker)
		}
		time.Sleep(15 * time.Millisecond)
	}
	if len(found) != 1 || found[0].PID != cmd.Process.Pid {
		t.Fatalf("t4LiveFindProcessesByEnv = %+v, want single match pid=%d", found, cmd.Process.Pid)
	}
	if other, err := t4LiveFindProcessesByEnv("E2E_LIVE_MARKER", marker+"-nonexistent"); err != nil || len(other) != 0 {
		t.Fatalf("unexpected match for unrelated marker: %+v err=%v", other, err)
	}
	prefix := marker[:len(marker)-1]
	if other, err := t4LiveFindProcessesByEnv("E2E_LIVE_MARKER", prefix); err != nil || len(other) != 0 {
		t.Fatalf("unexpected match for marker prefix %q: %+v err=%v", prefix, other, err)
	}

	t4LiveKillProcessesByEnv(marker)
	select {
	case wErr := <-waitDone:
		if wErr == nil {
			t.Fatalf("expected non-nil error after SIGKILL of sleeping orphan")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for killed orphan pid=%d to exit", cmd.Process.Pid)
	}
	t4LiveAssertNoOrphanByEnv(t, marker)
}

func TestT4LiveResolveRustProxyAndCommandSpec(t *testing.T) {
	mkExec := func(t *testing.T, p string, modTime time.Time) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		writeExecutableFile(t, p, []byte("#!/bin/sh\necho \"$@\"\n"))
		if err := os.Chtimes(p, modTime, modTime); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("rust_cargo_target_dir_requires_Cargo_toml_and_release_beats_debug_tie", func(t *testing.T) {
		root := t.TempDir()
		wtRoot := filepath.Join(root, "google-cloud-rust", "pubsub-conformance-proxy")
		if err := os.MkdirAll(wtRoot, 0o755); err != nil {
			t.Fatal(err)
		}
		cargoDir := filepath.Join(t.TempDir(), "cargo-target")
		eqTime := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
		relBin := filepath.Join(cargoDir, "release", "pubsub-conformance-proxy")
		dbgBin := filepath.Join(cargoDir, "debug", "pubsub-conformance-proxy")
		mkExec(t, relBin, eqTime)
		mkExec(t, dbgBin, eqTime)
		t.Setenv("CARGO_TARGET_DIR", cargoDir)

		spec := t4LiveTargetSpec{Name: "rust", EnvVar: "CONFORMANCE_RUST_PROXY_CMD", Lang: "rust"}
		// Without tests/pubsub-conformance-proxy/Cargo.toml, CARGO_TARGET_DIR must NOT be used.
		noManifest := t4LiveResolveRustProxy(spec, []string{root}, []string{"pubsub-conformance-proxy"})
		if noManifest.Found {
			t.Fatalf("expected CARGO_TARGET_DIR ignored without Cargo.toml, got %+v", noManifest)
		}

		// With Cargo.toml present and equal mtime, release wins tie-break over debug.
		cargoToml := filepath.Join(wtRoot, "tests", "pubsub-conformance-proxy", "Cargo.toml")
		if err := os.MkdirAll(filepath.Dir(cargoToml), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cargoToml, []byte("[package]\nname = \"pubsub-conformance-proxy\"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		gotRel := t4LiveResolveRustProxy(spec, []string{root}, []string{"pubsub-conformance-proxy"})
		if !gotRel.Found || gotRel.Command != relBin {
			t.Fatalf("expected CARGO_TARGET_DIR release tie-break winner %q, got %+v", relBin, gotRel)
		}

		// Newer in-tree debug build beats older CARGO_TARGET_DIR release build.
		inTreeDbg := filepath.Join(wtRoot, "target", "debug", "pubsub-conformance-proxy")
		mkExec(t, inTreeDbg, eqTime.Add(time.Minute))
		gotNewer := t4LiveResolveRustProxy(spec, []string{root}, []string{"pubsub-conformance-proxy"})
		if !gotNewer.Found || gotNewer.Command != inTreeDbg {
			t.Fatalf("expected newer debug %q to win, got %+v", inTreeDbg, gotNewer)
		}
	})

	t.Run("parse_command_spec_quoting_spaces_and_errors", func(t *testing.T) {
		spacedDir := filepath.Join(t.TempDir(), "dir with spaces")
		binPath := filepath.Join(spacedDir, "my proxy")
		mkExec(t, binPath, time.Now())

		// Direct path containing spaces (unquoted single executable file).
		cmdDirect, argsDirect, err := t4LiveParseCommandSpec(binPath)
		if err != nil || cmdDirect != binPath || len(argsDirect) != 0 {
			t.Fatalf("direct spaced path got (%q, %v, %v), want (%q, nil, nil)", cmdDirect, argsDirect, err, binPath)
		}

		// Quoted binary + quoted & escaped args.
		raw := fmt.Sprintf("'%s' --label=\"two words\" 'single quoted' plain\\ arg", binPath)
		cmdParsed, argsParsed, err := t4LiveParseCommandSpec(raw)
		wantArgs := []string{"--label=two words", "single quoted", "plain arg"}
		if err != nil || cmdParsed != binPath || !slices.Equal(argsParsed, wantArgs) {
			t.Fatalf("quoted spec got (%q, %v, %v), want (%q, %v, nil)", cmdParsed, argsParsed, err, binPath, wantArgs)
		}

		// Error vectors: empty, unterminated quote, trailing backslash, non-executable, nonexistent.
		nonExec := filepath.Join(t.TempDir(), "not-exec")
		if err := os.WriteFile(nonExec, []byte("#!/bin/sh\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		badSpecs := []struct {
			raw     string
			wantSub string
		}{
			{"   ", "empty"},
			{"\"unterminated", "unterminated quote"},
			{binPath + " trailing\\", "trailing escape"},
			{nonExec, ""},
			{"/definitely/nonexistent/proxy-binary-xyz --arg", ""},
		}
		for _, tc := range badSpecs {
			if _, _, err := t4LiveParseCommandSpec(tc.raw); err == nil || (tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub)) {
				t.Errorf("t4LiveParseCommandSpec(%q) = %v, want error containing %q", tc.raw, err, tc.wantSub)
			}
		}
	})

	t.Run("create_launcher_symlink_and_wrapper_args", func(t *testing.T) {
		targetBin := filepath.Join(t.TempDir(), "echo-args.sh")
		writeExecutableFile(t, targetBin, []byte("#!/bin/sh\nprintf '[%s]' \"$@\"\n"))
		baseDir := t.TempDir()

		// Zero-arg launcher creates a symlink.
		symLauncher := t4LiveCreateLauncher(t, baseDir, t4LiveResolvedProxy{
			Spec:    t4LiveTargetSpec{Name: "rust"},
			Command: targetBin,
		}, "marker-sym")
		linkDest, err := os.Readlink(symLauncher)
		if err != nil || linkDest != targetBin {
			t.Fatalf("symlink launcher %q Readlink = (%q, %v), want %q", symLauncher, linkDest, err, targetBin)
		}

		// Non-empty Args writes wrapper preserving spaces/quotes and forwarding "$@"!
		wrapLauncher := t4LiveCreateLauncher(t, baseDir, t4LiveResolvedProxy{
			Spec:    t4LiveTargetSpec{Name: "cpp"},
			Command: targetBin,
			Args:    []string{"--opt=val with space", "it's"},
		}, "marker-wrap")
		outBytes, err := exec.Command(wrapLauncher, "--extra=1").CombinedOutput()
		if err != nil {
			t.Fatalf("run wrapper launcher: %v (%s)", err, string(outBytes))
		}
		wantOut := "[--opt=val with space][it's][--extra=1]"
		if string(outBytes) != wantOut {
			t.Fatalf("wrapper launcher output = %q, want %q", string(outBytes), wantOut)
		}
	})
}
