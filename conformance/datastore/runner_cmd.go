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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func launchFixtureCmd(ctx context.Context, cmdStr string) (string, func(), error) {
	port, err := pickFreePort()
	if err != nil {
		return "", nil, fmt.Errorf("allocate fixture port: %w", err)
	}
	parts := splitCommandArgs(cmdStr)
	if len(parts) == 0 {
		return "", nil, errors.New("empty --fixture-cmd")
	}

	replacedPort := false
	for i, p := range parts {
		if strings.Contains(p, "{port}") {
			parts[i] = strings.ReplaceAll(p, "{port}", fmt.Sprintf("%d", port))
			replacedPort = true
		}
	}
	if !replacedPort {
		parts = append(parts, "--port", fmt.Sprintf("%d", port))
	}

	cmd := exec.Command(parts[0], parts[1:]...)
	configureFixtureProcess(cmd)

	var stderrBuf syncBuffer
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", nil, fmt.Errorf("fixture stdout pipe: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "", nil, fmt.Errorf("fixture stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return "", nil, fmt.Errorf("start fixture command %q: %w", cmdStr, err)
	}

	go func() {
		_, _ = io.Copy(io.Discard, stdoutPipe)
	}()
	go func() {
		_, _ = io.Copy(&stderrBuf, stderrPipe)
	}()

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	stop := func() {
		if cmd.Process != nil {
			interruptFixtureProcess(cmd)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				killFixtureProcess(cmd)
				select {
				case <-done:
				case <-time.After(1 * time.Second):
				}
			}
		}
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if err := waitForGRPCReady(ctx, addr, 15*time.Second); err != nil {
		stop()
		return "", nil, fmt.Errorf("fixture command did not become ready at %s: %w (stderr: %s)", addr, err, stderrBuf.String())
	}

	return addr, stop, nil
}

func waitForGRPCReady(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.Connect()

	for time.Now().Before(deadline) {
		if conn.GetState() == connectivity.Ready {
			return nil
		}
		waitCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		changed := conn.WaitForStateChange(waitCtx, conn.GetState())
		cancel()
		if conn.GetState() == connectivity.Ready {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !changed {
			conn.Connect()
		}
	}
	return fmt.Errorf("timed out after %s waiting for gRPC server readiness", timeout)
}

func pickFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func splitCommandArgs(s string) []string {
	var args []string
	var cur strings.Builder
	var quote rune
	escaped := false

	for _, r := range s {
		if escaped {
			cur.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
			continue
		}
		if r == '\'' || r == '"' {
			quote = r
			continue
		}
		if r == ' ' || r == '\t' || r == '\n' {
			if cur.Len() > 0 {
				args = append(args, cur.String())
				cur.Reset()
			}
			continue
		}
		cur.WriteRune(r)
	}
	if cur.Len() > 0 {
		args = append(args, cur.String())
	}
	return args
}
