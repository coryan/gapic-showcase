// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fake

import (
	"compress/zlib"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/gzip" // Register "gzip" gRPC compressor.
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

const deflateCompressorName = "deflate"

func init() {
	if encoding.GetCompressor(deflateCompressorName) == nil {
		c := &deflateCompressor{}
		c.poolCompressor.New = func() any {
			return &deflateWriter{
				Writer: zlib.NewWriter(io.Discard),
				pool:   &c.poolCompressor,
			}
		}
		encoding.RegisterCompressor(c)
	}
}

// deflateCompressor implements the gRPC "deflate" compressor using RFC 1950
// zlib framing (matching C++ gRPC GRPC_COMPRESS_DEFLATE).
type deflateCompressor struct {
	poolCompressor   sync.Pool
	poolDecompressor sync.Pool
}

func (c *deflateCompressor) Name() string {
	return deflateCompressorName
}

func (c *deflateCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	zw := c.poolCompressor.Get().(*deflateWriter)
	zw.Writer.Reset(w)
	return zw, nil
}

func (c *deflateCompressor) Decompress(r io.Reader) (io.Reader, error) {
	if dr, ok := c.poolDecompressor.Get().(*deflateReader); ok {
		if err := dr.rc.(zlib.Resetter).Reset(r, nil); err != nil {
			_ = dr.rc.Close()
			return nil, err
		}
		return dr, nil
	}
	rc, err := zlib.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &deflateReader{
		rc:   rc,
		pool: &c.poolDecompressor,
	}, nil
}

type deflateWriter struct {
	*zlib.Writer
	pool *sync.Pool
}

func (w *deflateWriter) Close() error {
	defer w.pool.Put(w)
	return w.Writer.Close()
}

var _ io.Closer = (*deflateReader)(nil)

type deflateReader struct {
	rc   io.ReadCloser
	pool *sync.Pool
}

func (r *deflateReader) Read(p []byte) (int, error) {
	return r.rc.Read(p)
}

func (r *deflateReader) Close() error {
	defer r.pool.Put(r)
	return r.rc.Close()
}

// PayloadSizes records uncompressed and compressed wire sizes for an incoming
// RPC payload frame via grpc/stats.Handler.
type PayloadSizes struct {
	UncompressedBytes int
	CompressedBytes   int
	WireBytes         int
}

type rpcMetaKey struct{}
type connCtxKey struct{}

type rpcMeta struct {
	fullMethod       string
	compression      string
	headerWireLength int
	header           metadata.MD
	startTime        time.Time
	ep               any
	conn             net.Conn

	mu          sync.Mutex
	payloads    []PayloadSizes
	intercepted atomic.Bool
}

func rpcMetaFromContext(ctx context.Context) *rpcMeta {
	if ctx == nil {
		return nil
	}
	m, _ := ctx.Value(rpcMetaKey{}).(*rpcMeta)
	return m
}

func (m *rpcMeta) snapshotHeader() (compression string, headerWireLen int, md metadata.MD) {
	if m == nil {
		return "", 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.header != nil {
		md = m.header.Copy()
	}
	return m.compression, m.headerWireLength, md
}

func (m *rpcMeta) payloadAt(idx int) (PayloadSizes, bool) {
	if m == nil {
		return PayloadSizes{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx < 0 || idx >= len(m.payloads) {
		return PayloadSizes{}, false
	}
	return m.payloads[idx], true
}

func (m *rpcMeta) latestPayload() (PayloadSizes, bool) {
	if m == nil {
		return PayloadSizes{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.payloads) == 0 {
		return PayloadSizes{}, false
	}
	return m.payloads[len(m.payloads)-1], true
}

type serverStatsHandler struct {
	tagConn        func(info *stats.ConnTagInfo) net.Conn
	tagEpoch       func() any
	onTransportErr func(ctx context.Context, meta *rpcMeta, end *stats.End)
	afterTagRPC    func(fullMethod string, ep any)
	onRPCEnd       func(fullMethod string, ep any)
}

func (h *serverStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	var ep any
	if h.tagEpoch != nil {
		ep = h.tagEpoch()
	}
	var conn net.Conn
	if c, ok := ctx.Value(connCtxKey{}).(net.Conn); ok {
		conn = c
	}
	meta := &rpcMeta{
		fullMethod: info.FullMethodName,
		startTime:  time.Now(),
		ep:         ep,
		conn:       conn,
	}
	if h.afterTagRPC != nil {
		h.afterTagRPC(info.FullMethodName, ep)
	}
	return context.WithValue(ctx, rpcMetaKey{}, meta)
}

func (h *serverStatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	meta := rpcMetaFromContext(ctx)
	if meta == nil {
		return
	}
	switch ev := s.(type) {
	case *stats.InHeader:
		meta.mu.Lock()
		if meta.fullMethod == "" {
			meta.fullMethod = ev.FullMethod
		}
		meta.compression = ev.Compression
		meta.headerWireLength = ev.WireLength
		if ev.Header != nil {
			meta.header = ev.Header.Copy()
		}
		meta.mu.Unlock()
	case *stats.InPayload:
		meta.mu.Lock()
		meta.payloads = append(meta.payloads, PayloadSizes{
			UncompressedBytes: ev.Length,
			CompressedBytes:   ev.CompressedLength,
			WireBytes:         ev.WireLength,
		})
		meta.mu.Unlock()
	case *stats.End:
		if !meta.intercepted.Load() && h.onTransportErr != nil {
			h.onTransportErr(ctx, meta, ev)
		}
		if h.onRPCEnd != nil {
			h.onRPCEnd(meta.fullMethod, meta.ep)
		}
	}
}

func (h *serverStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	if h.tagConn != nil && info != nil {
		if conn := h.tagConn(info); conn != nil {
			return context.WithValue(ctx, connCtxKey{}, conn)
		}
	}
	return ctx
}

func (h *serverStatsHandler) HandleConn(_ context.Context, _ stats.ConnStats) {}
