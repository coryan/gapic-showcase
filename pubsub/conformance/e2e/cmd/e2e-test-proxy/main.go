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
	"compress/zlib"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/googleapis/gapic-showcase/pubsub/conformance/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/gzip"
)

const maxProxyGRPCMsgBytes = 64 << 20 // 64 MiB (>= 32 MiB required by proxy.proto)

type deflateCompressor struct {
	writerPool sync.Pool
	readerPool sync.Pool
}

type deflateWriteCloser struct {
	*zlib.Writer
	pool *sync.Pool
}

func (w *deflateWriteCloser) Close() error {
	defer w.pool.Put(w)
	return w.Writer.Close()
}

type deflateReadCloser struct {
	io.ReadCloser
	pool *sync.Pool
}

func (r *deflateReadCloser) Close() error {
	defer r.pool.Put(r)
	return r.ReadCloser.Close()
}

func (d *deflateCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	if v := d.writerPool.Get(); v != nil {
		zw := v.(*deflateWriteCloser)
		zw.Reset(w)
		return zw, nil
	}
	return &deflateWriteCloser{Writer: zlib.NewWriter(w), pool: &d.writerPool}, nil
}

func (d *deflateCompressor) Decompress(r io.Reader) (io.Reader, error) {
	if v := d.readerPool.Get(); v != nil {
		zr := v.(*deflateReadCloser)
		if resetter, ok := zr.ReadCloser.(zlib.Resetter); ok {
			if err := resetter.Reset(r, nil); err == nil {
				return zr, nil
			}
		}
		d.readerPool.Put(zr)
	}
	rc, err := zlib.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &deflateReadCloser{ReadCloser: rc, pool: &d.readerPool}, nil
}

func (d *deflateCompressor) Name() string {
	return "deflate"
}

func init() {
	encoding.RegisterCompressor(&deflateCompressor{})
}

func main() {
	port := flag.Int("port", 0, "TCP port to bind on 127.0.0.1 (0 for ephemeral)")
	mode := flag.String("mode", "conformant-go", "synthetic proxy behavior profile")
	language := flag.String("language", "", "override GetCapabilitiesResponse.client_language")
	monitoring := flag.String("monitoring", "auto", "tri-state monitoring override: auto|true|false")
	unsupported := flag.String("unsupported", "", "comma-separated unsupported feature names")
	fault := flag.String("fault", "", "comma-separated fault injection tokens")
	marker := flag.String("marker", "", "diagnostic run marker")
	delay := flag.Duration("delay", 0, "artificial RPC delay")
	crashPhase := flag.String("crash-phase", "", "crash phase: startup|ready|scenario")
	keepalivePing := flag.Duration("keepalive-ping", 0, "optional keepalive ping interval override")
	keepalivePong := flag.Duration("keepalive-pong", 0, "optional keepalive pong timeout override")
	fastTimings := flag.Bool("fast-timings", false, "use compressed timings for fast E2E conformance execution")
	flag.Parse()

	cfg, err := ParseProfileConfig(*mode, *language, *monitoring, *unsupported, *fault, *marker, *crashPhase, *delay)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-test-proxy: %v\n", err)
		os.Exit(2)
	}
	if *fastTimings {
		cfg.ApplyFastTimings()
	}
	if cfg.SupportsKeepalive {
		if *keepalivePing > 0 {
			ms := keepalivePing.Milliseconds()
			if ms < 1 {
				ms = 1
			}
			cfg.KeepalivePingOverride = time.Duration(ms) * time.Millisecond
			if cfg.Defaults != nil {
				cfg.Defaults.KeepalivePingIntervalMs = ms
			}
		}
		if *keepalivePong > 0 {
			ms := keepalivePong.Milliseconds()
			if ms < 1 {
				ms = 1
			}
			cfg.KeepalivePongOverride = time.Duration(ms) * time.Millisecond
			if cfg.Defaults != nil {
				cfg.Defaults.KeepalivePongTimeoutMs = ms
			}
		}
		if cfg.Defaults != nil && (*keepalivePing > 0 || *keepalivePong > 0) {
			pingMs := cfg.Defaults.KeepalivePingIntervalMs
			if pingMs <= 0 {
				pingMs = 30_000
			}
			pongMs := cfg.Defaults.KeepalivePongTimeoutMs
			if pongMs <= 0 {
				pongMs = 15_000
			}
			holdTail := pongMs / 2
			if holdTail > 5000 {
				holdTail = 5000
			}
			extraMs := int64(0)
			if pongMs >= 10_000 {
				extraMs = 6000
			}
			needMaxExtMs := pingMs + pongMs + holdTail + extraMs + 200
			if cfg.Defaults.SubscriberMaxExtensionMs > 0 && needMaxExtMs > cfg.Defaults.SubscriberMaxExtensionMs {
				cfg.Defaults.SubscriberMaxExtensionMs = needMaxExtMs
			}
		}
	}

	if cfg.Marker != "" {
		fmt.Fprintf(os.Stderr, "e2e-test-proxy starting mode=%s marker=%s\n", cfg.Mode, cfg.Marker)
	}

	if cfg.CrashPhase == "startup" {
		fmt.Fprintf(os.Stderr, "e2e-test-proxy: simulated startup crash\n")
		_ = os.Stderr.Sync()
		os.Exit(2)
	}

	if cfg.HasFault("bad-ready") {
		if cfg.HasFault("silent-exit") {
			os.Exit(0)
		}
		fmt.Print("READY not-a-host-port\n")
		_ = os.Stdout.Sync()
		os.Exit(0)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-test-proxy: listen failed: %v\n", err)
		os.Exit(1)
	}
	tcpAddr := lis.Addr().(*net.TCPAddr)

	if cfg.CrashPhase == "ready" {
		fmt.Printf("READY 127.0.0.1:%d\n", tcpAddr.Port)
		_ = os.Stdout.Sync()
		fmt.Fprintln(os.Stderr, "e2e-test-proxy: simulated post-ready crash")
		_ = os.Stderr.Sync()
		_ = lis.Close()
		os.Exit(2)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxProxyGRPCMsgBytes),
		grpc.MaxSendMsgSize(maxProxyGRPCMsgBytes),
	)
	proxySrv := NewProxyServer(cfg, grpcServer)
	proxypb.RegisterPubSubProxyServer(grpcServer, proxySrv)

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- grpcServer.Serve(lis)
	}()

	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for sig := range sigCh {
			if sig == syscall.SIGTERM && cfg.HasFault("ignore-sigterm") {
				fmt.Fprintf(os.Stderr, "e2e-test-proxy: ignoring SIGTERM due to fault\n")
				continue
			}
			proxySrv.Shutdown(0)
			return
		}
	}()

	fmt.Printf("READY 127.0.0.1:%d\n", tcpAddr.Port)
	_ = os.Stdout.Sync()

	if err := <-serveErrCh; err != nil {
		fmt.Fprintf(os.Stderr, "e2e-test-proxy: gRPC server exited: %v\n", err)
		os.Exit(1)
	}
}
