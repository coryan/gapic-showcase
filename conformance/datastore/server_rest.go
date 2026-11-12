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
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"github.com/googleapis/gapic-showcase/util/genrest/resttools"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func (s *SimServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeRESTError(w, status.Error(codes.Unimplemented, fmt.Sprintf("unsupported HTTP method %s", r.Method)))
		return
	}

	const prefix = "/v1/projects/"
	escPath := r.URL.EscapedPath()
	if !strings.HasPrefix(escPath, prefix) {
		writeRESTError(w, status.Error(codes.InvalidArgument, fmt.Sprintf("unexpected HTTP path %q", escPath)))
		return
	}
	rest := strings.TrimPrefix(escPath, prefix)
	colonIdx := strings.LastIndex(rest, ":")
	sepLen := 1
	if colonIdx < 0 {
		colonIdx = strings.LastIndex(strings.ToUpper(rest), "%3A")
		sepLen = 3
	}
	if colonIdx < 0 {
		writeRESTError(w, status.Error(codes.InvalidArgument, fmt.Sprintf("missing custom verb in HTTP path %q", escPath)))
		return
	}
	rawProjectID := rest[:colonIdx]
	verb := rest[colonIdx+sepLen:]
	projectID, err := url.PathUnescape(rawProjectID)
	if err != nil {
		writeRESTError(w, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid project_id escape %q: %v", rawProjectID, err)))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeRESTError(w, status.Error(codes.InvalidArgument, fmt.Sprintf("read body: %v", err)))
		return
	}

	var method string
	var req proto.Message
	var setProjectID func(string)
	switch verb {
	case "lookup":
		method = "Lookup"
		m := &pb.LookupRequest{}
		req = m
		setProjectID = func(p string) {
			if m.ProjectId == "" {
				m.ProjectId = p
			}
		}
	case "runQuery":
		method = "RunQuery"
		m := &pb.RunQueryRequest{}
		req = m
		setProjectID = func(p string) {
			if m.ProjectId == "" {
				m.ProjectId = p
			}
		}
	case "runAggregationQuery":
		method = "RunAggregationQuery"
		m := &pb.RunAggregationQueryRequest{}
		req = m
		setProjectID = func(p string) {
			if m.ProjectId == "" {
				m.ProjectId = p
			}
		}
	case "beginTransaction":
		method = "BeginTransaction"
		m := &pb.BeginTransactionRequest{}
		req = m
		setProjectID = func(p string) {
			if m.ProjectId == "" {
				m.ProjectId = p
			}
		}
	case "commit":
		method = "Commit"
		m := &pb.CommitRequest{}
		req = m
		setProjectID = func(p string) {
			if m.ProjectId == "" {
				m.ProjectId = p
			}
		}
	case "rollback":
		method = "Rollback"
		m := &pb.RollbackRequest{}
		req = m
		setProjectID = func(p string) {
			if m.ProjectId == "" {
				m.ProjectId = p
			}
		}
	case "allocateIds":
		method = "AllocateIds"
		m := &pb.AllocateIdsRequest{}
		req = m
		setProjectID = func(p string) {
			if m.ProjectId == "" {
				m.ProjectId = p
			}
		}
	case "reserveIds":
		method = "ReserveIds"
		m := &pb.ReserveIdsRequest{}
		req = m
		setProjectID = func(p string) {
			if m.ProjectId == "" {
				m.ProjectId = p
			}
		}
	default:
		writeRESTError(w, status.Error(codes.Unimplemented, fmt.Sprintf("unknown Datastore HTTP verb %q", verb)))
		return
	}

	if len(strings.TrimSpace(string(body))) > 0 {
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(body, req); err != nil {
			writeRESTError(w, status.Error(codes.InvalidArgument, fmt.Sprintf("unmarshal %s JSON: %v", method, err)))
			return
		}
	}
	setProjectID(projectID)

	md := make(metadata.MD, len(r.Header))
	for k, vals := range r.Header {
		lk := strings.ToLower(k)
		md[lk] = append(md[lk], vals...)
	}

	resp, rpcErr := s.dispatch(method, fixturepb.Transport_TRANSPORT_HTTP_JSON, md, req)
	if rpcErr != nil {
		writeRESTError(w, rpcErr)
		return
	}

	data, err := (protojson.MarshalOptions{UseProtoNames: false}).Marshal(resp)
	if err != nil {
		writeRESTError(w, status.Error(codes.Internal, fmt.Sprintf("marshal %s response JSON: %v", method, err)))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func writeRESTError(w http.ResponseWriter, err error) {
	st := status.Convert(err)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	resttools.ErrorResponse(w, resttools.NoCodeHTTP, st.Code(), st.Message(), st.Details()...)
}
