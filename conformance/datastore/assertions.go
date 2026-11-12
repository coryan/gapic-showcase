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
	"errors"
	"fmt"
	"math"
	"net/url"
	"slices"
	"strings"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/googleapis/gapic-showcase/conformance/datastore/v1/fixturepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// ErrScenarioSkipped signals that the fixture reported FEATURE_NOT_IMPLEMENTED for a scenario.
var ErrScenarioSkipped = errors.New("feature not implemented by fixture")

// SkipError wraps an unimplemented feature error as ErrScenarioSkipped.
func SkipError(errProto *fixturepb.VeneerError, contextMsg string) error {
	return fmt.Errorf("%w (%s): %s", ErrScenarioSkipped, contextMsg, FormatVeneerError(errProto))
}

// CheckNoError verifies that a fixture VeneerError is nil, returning ErrScenarioSkipped
// if the fixture reports ERROR_KIND_FEATURE_NOT_IMPLEMENTED.
func CheckNoError(errProto *fixturepb.VeneerError, op string) error {
	if errProto == nil {
		return nil
	}
	if errProto.Kind == fixturepb.ErrorKind_ERROR_KIND_FEATURE_NOT_IMPLEMENTED {
		return SkipError(errProto, op)
	}
	return fmt.Errorf("%s returned unexpected error: %s", op, FormatVeneerError(errProto))
}

// AssertVeneerError asserts that errProto is non-nil and matches the expected ErrorKind
// (and optional RpcCode when kind is ERROR_KIND_SERVICE). If the fixture returned
// ERROR_KIND_FEATURE_NOT_IMPLEMENTED, it returns ErrScenarioSkipped.
func AssertVeneerError(errProto *fixturepb.VeneerError, wantKind fixturepb.ErrorKind, wantCode ...fixturepb.RpcCode) error {
	if errProto == nil {
		return fmt.Errorf("expected error of kind %s, got nil", wantKind)
	}
	if errProto.Kind == fixturepb.ErrorKind_ERROR_KIND_FEATURE_NOT_IMPLEMENTED && wantKind != fixturepb.ErrorKind_ERROR_KIND_FEATURE_NOT_IMPLEMENTED {
		return SkipError(errProto, fmt.Sprintf("want %s", wantKind))
	}
	if errProto.Kind != wantKind {
		return fmt.Errorf("expected error kind %s, got %s", wantKind, FormatVeneerError(errProto))
	}
	if len(wantCode) > 0 {
		if errProto.RpcCode == nil {
			return fmt.Errorf("expected rpc_code %s on error %s, got nil", wantCode[0], FormatVeneerError(errProto))
		}
		if errProto.GetRpcCode() != wantCode[0] {
			return fmt.Errorf("expected rpc_code %s, got %s (%s)", wantCode[0], errProto.GetRpcCode(), FormatVeneerError(errProto))
		}
	}
	return nil
}

// AssertRoutingHeaders checks the required Spec §3.2 headers on a Datastore RPC.
func AssertRoutingHeaders(md metadata.MD, projectID, databaseID string, wantTransport fixturepb.Transport) error {
	if err := assertRequestParamsHeader(md.Get("x-goog-request-params"), projectID, databaseID); err != nil {
		return err
	}
	if err := assertResourcePrefixHeader(md.Get("google-cloud-resource-prefix"), projectID, databaseID); err != nil {
		return err
	}
	if err := assertAPIClientHeader(md.Get("x-goog-api-client"), wantTransport); err != nil {
		return err
	}
	return nil
}

func assertRequestParamsHeader(vals []string, projectID, databaseID string) error {
	if len(vals) == 0 {
		return errors.New("missing x-goog-request-params header")
	}
	joined := strings.Join(vals, "&")
	// Split either comma-separated header values or & query-param pairs.
	tokens := strings.FieldsFunc(joined, func(r rune) bool {
		return r == '&' || r == ',' || r == ' '
	})
	wantProj := "project_id=" + url.QueryEscape(projectID)
	wantProjAlt := "project_id=" + url.PathEscape(projectID)
	hasProj := false
	hasDB := false
	wantDB := "database_id=" + url.QueryEscape(databaseID)
	wantDBAlt := "database_id=" + url.PathEscape(databaseID)

	for _, tok := range tokens {
		if tok == wantProj || tok == wantProjAlt {
			hasProj = true
		}
		if strings.HasPrefix(tok, "database_id=") {
			if databaseID == "" {
				return fmt.Errorf("unexpected database_id param %q in x-goog-request-params for default database", tok)
			}
			if tok == wantDB || tok == wantDBAlt {
				hasDB = true
			} else {
				return fmt.Errorf("x-goog-request-params database_id mismatch: got %q, want %q", tok, wantDB)
			}
		}
	}
	if !hasProj {
		return fmt.Errorf("x-goog-request-params %v missing %q", vals, wantProj)
	}
	if databaseID != "" && !hasDB {
		return fmt.Errorf("x-goog-request-params %v missing %q", vals, wantDB)
	}
	return nil
}

func assertResourcePrefixHeader(vals []string, projectID, databaseID string) error {
	if len(vals) == 0 {
		return errors.New("missing google-cloud-resource-prefix header")
	}
	raw := "projects/" + projectID
	escaped := "projects/" + url.QueryEscape(projectID)
	if databaseID != "" {
		raw += "/databases/" + databaseID
		escaped += "/databases/" + url.QueryEscape(databaseID)
	}
	for _, v := range vals {
		if v == raw || v == escaped {
			return nil
		}
	}
	return fmt.Errorf("google-cloud-resource-prefix %v did not match %q or %q", vals, raw, escaped)
}

func assertAPIClientHeader(vals []string, wantTransport fixturepb.Transport) error {
	if len(vals) == 0 {
		return errors.New("missing x-goog-api-client header")
	}
	joined := strings.Join(vals, " ")
	if !strings.Contains(joined, "gccl/") {
		return fmt.Errorf("x-goog-api-client %q missing handwritten veneer token gccl/", joined)
	}
	switch wantTransport {
	case fixturepb.Transport_TRANSPORT_GRPC:
		if !strings.Contains(joined, "grpc/") {
			return fmt.Errorf("x-goog-api-client %q missing gRPC transport token grpc/", joined)
		}
	case fixturepb.Transport_TRANSPORT_HTTP_JSON:
		if !strings.Contains(joined, "rest/") {
			return fmt.Errorf("x-goog-api-client %q missing REST transport token rest/", joined)
		}
	}
	return nil
}

// AssertWireKeyMatches checks that a wire pb.Key matches expected path, namespace, and
// has valid partition fields (either unset or matching project/database).
func AssertWireKeyMatches(got *pb.Key, want *fixturepb.KeySpec, projectID, databaseID string) error {
	if got == nil {
		return errors.New("got nil wire key")
	}
	if got.PartitionId != nil {
		if p := got.PartitionId.ProjectId; p != "" && p != projectID {
			return fmt.Errorf("wire key partition project_id=%q, want %q or empty", p, projectID)
		}
		if db := got.PartitionId.DatabaseId; db != "" && db != databaseID {
			return fmt.Errorf("wire key partition database_id=%q, want %q or empty", db, databaseID)
		}
		wantNS := ""
		if want.GetPartitionId() != nil {
			wantNS = want.PartitionId.NamespaceId
		}
		if got.PartitionId.NamespaceId != wantNS {
			return fmt.Errorf("wire key namespace_id=%q, want %q", got.PartitionId.NamespaceId, wantNS)
		}
	} else if want.GetPartitionId().GetNamespaceId() != "" {
		return fmt.Errorf("wire key missing partition_id with namespace %q", want.PartitionId.NamespaceId)
	}

	if len(got.Path) != len(want.Path) {
		return fmt.Errorf("wire key path len=%d, want %d", len(got.Path), len(want.Path))
	}
	for i := range want.Path {
		if got.Path[i].Kind != want.Path[i].Kind {
			return fmt.Errorf("wire key path[%d].kind=%q, want %q", i, got.Path[i].Kind, want.Path[i].Kind)
		}
		if got.Path[i].GetId() != want.Path[i].GetId() || got.Path[i].GetName() != want.Path[i].GetName() {
			return fmt.Errorf("wire key path[%d] id mismatch: got %v, want %v", i, got.Path[i], want.Path[i])
		}
	}
	return nil
}

// AssertFixtureKeyEqual compares two fixturepb.KeySpecs strictly (excluding project/database
// partition fields unless non-empty on both, preserving client-independent equality checks).
func AssertFixtureKeyEqual(got, want *fixturepb.KeySpec) error {
	if got == nil && want == nil {
		return nil
	}
	if got == nil || want == nil {
		return fmt.Errorf("key mismatch: got=%v want=%v", got, want)
	}
	if got.GetPartitionId().GetNamespaceId() != want.GetPartitionId().GetNamespaceId() {
		return fmt.Errorf("key namespace mismatch: got %q, want %q", got.GetPartitionId().GetNamespaceId(), want.GetPartitionId().GetNamespaceId())
	}
	if len(got.Path) != len(want.Path) {
		return fmt.Errorf("key path length mismatch: got %d (%v), want %d (%v)", len(got.Path), got.Path, len(want.Path), want.Path)
	}
	for i := range want.Path {
		gp, wp := got.Path[i], want.Path[i]
		if gp.Kind != wp.Kind || gp.GetId() != wp.GetId() || gp.GetName() != wp.GetName() {
			return fmt.Errorf("key path[%d] mismatch: got %v, want %v", i, gp, wp)
		}
	}
	return nil
}

// AssertValueSpecEqual compares two ValueSpecs handling float64 NaN/-0.0 and slice/nil details.
func AssertValueSpecEqual(got, want *fixturepb.ValueSpec) error {
	if got == nil && want == nil {
		return nil
	}
	if got == nil || want == nil {
		return fmt.Errorf("value mismatch: got=%v want=%v", got, want)
	}
	if got.ExcludeFromIndexes != want.ExcludeFromIndexes {
		return fmt.Errorf("exclude_from_indexes mismatch: got %v, want %v (value=%v)", got.ExcludeFromIndexes, want.ExcludeFromIndexes, want)
	}
	switch wv := want.ValueType.(type) {
	case *fixturepb.ValueSpec_IsNull:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_IsNull)
		if !ok || gv.IsNull != wv.IsNull {
			return fmt.Errorf("expected null=%v, got %v", wv.IsNull, got)
		}
	case *fixturepb.ValueSpec_BooleanValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_BooleanValue)
		if !ok || gv.BooleanValue != wv.BooleanValue {
			return fmt.Errorf("expected bool %v, got %v", wv.BooleanValue, got)
		}
	case *fixturepb.ValueSpec_IntegerValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_IntegerValue)
		if !ok || gv.IntegerValue != wv.IntegerValue {
			return fmt.Errorf("expected int64 %d, got %v", wv.IntegerValue, got)
		}
	case *fixturepb.ValueSpec_DoubleValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_DoubleValue)
		if !ok {
			return fmt.Errorf("expected double %v, got %v", wv.DoubleValue, got)
		}
		if math.IsNaN(wv.DoubleValue) {
			if !math.IsNaN(gv.DoubleValue) {
				return fmt.Errorf("expected NaN double, got %v", gv.DoubleValue)
			}
		} else if math.Float64bits(gv.DoubleValue) != math.Float64bits(wv.DoubleValue) {
			return fmt.Errorf("expected double %v (bits=%x), got %v (bits=%x)",
				wv.DoubleValue, math.Float64bits(wv.DoubleValue),
				gv.DoubleValue, math.Float64bits(gv.DoubleValue))
		}
	case *fixturepb.ValueSpec_TimestampValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_TimestampValue)
		if !ok || !gv.TimestampValue.AsTime().Equal(wv.TimestampValue.AsTime()) {
			return fmt.Errorf("expected timestamp %v, got %v", wv.TimestampValue.AsTime(), got)
		}
	case *fixturepb.ValueSpec_KeyValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_KeyValue)
		if !ok {
			return fmt.Errorf("expected key value, got %v", got)
		}
		return AssertFixtureKeyEqual(gv.KeyValue, wv.KeyValue)
	case *fixturepb.ValueSpec_StringValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_StringValue)
		if !ok || gv.StringValue != wv.StringValue {
			return fmt.Errorf("expected string %q, got %v", wv.StringValue, got)
		}
	case *fixturepb.ValueSpec_BlobValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_BlobValue)
		if !ok || !bytes.Equal(gv.BlobValue, wv.BlobValue) {
			return fmt.Errorf("expected bytes %x, got %v", wv.BlobValue, got)
		}
	case *fixturepb.ValueSpec_GeoPointValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_GeoPointValue)
		if !ok || gv.GeoPointValue == nil ||
			gv.GeoPointValue.Latitude != wv.GeoPointValue.Latitude ||
			gv.GeoPointValue.Longitude != wv.GeoPointValue.Longitude {
			return fmt.Errorf("expected geopoint %v, got %v", wv.GeoPointValue, got)
		}
	case *fixturepb.ValueSpec_EntityValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_EntityValue)
		if !ok {
			return fmt.Errorf("expected entity value, got %v", got)
		}
		return AssertEntitySpecEqual(gv.EntityValue, wv.EntityValue)
	case *fixturepb.ValueSpec_ArrayValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_ArrayValue)
		if !ok || len(gv.ArrayValue.GetValues()) != len(wv.ArrayValue.GetValues()) {
			return fmt.Errorf("expected array %v, got %v", wv.ArrayValue, got)
		}
		for i := range wv.ArrayValue.Values {
			if err := AssertValueSpecEqual(gv.ArrayValue.Values[i], wv.ArrayValue.Values[i]); err != nil {
				return fmt.Errorf("array[%d]: %w", i, err)
			}
		}
	case *fixturepb.ValueSpec_VectorValue:
		gv, ok := got.ValueType.(*fixturepb.ValueSpec_VectorValue)
		if !ok || !slices.Equal(gv.VectorValue.GetElements(), wv.VectorValue.GetElements()) {
			return fmt.Errorf("expected vector %v, got %v", wv.VectorValue, got)
		}
	default:
		if !proto.Equal(got, want) {
			return fmt.Errorf("value mismatch: got %v, want %v", got, want)
		}
	}
	return nil
}

// AssertEntitySpecEqual checks key and properties equality between two EntitySpecs.
func AssertEntitySpecEqual(got, want *fixturepb.EntitySpec) error {
	if got == nil && want == nil {
		return nil
	}
	if got == nil || want == nil {
		return fmt.Errorf("entity mismatch: got=%v want=%v", got, want)
	}
	if err := AssertFixtureKeyEqual(got.Key, want.Key); err != nil {
		return err
	}
	if len(got.Properties) != len(want.Properties) {
		return fmt.Errorf("entity properties count mismatch: got %d (%v), want %d (%v)",
			len(got.Properties), got.Properties, len(want.Properties), want.Properties)
	}
	for k, wv := range want.Properties {
		gv, ok := got.Properties[k]
		if !ok {
			return fmt.Errorf("entity missing property %q", k)
		}
		if err := AssertValueSpecEqual(gv, wv); err != nil {
			return fmt.Errorf("property %q: %w", k, err)
		}
	}
	return nil
}

// AssertNoMeaning18 ensures no wire pb.Value anywhere in an entity has meaning == 18.
func AssertNoMeaning18(e *pb.Entity) error {
	if e == nil {
		return nil
	}
	for k, v := range e.Properties {
		if err := assertValueNoMeaning18(v, k); err != nil {
			return err
		}
	}
	return nil
}

func assertValueNoMeaning18(v *pb.Value, path string) error {
	if v == nil {
		return nil
	}
	if v.Meaning == 18 {
		return fmt.Errorf("property %q leaked meaning=18 on write-back", path)
	}
	if ev := v.GetEntityValue(); ev != nil {
		for k, child := range ev.Properties {
			if err := assertValueNoMeaning18(child, path+"."+k); err != nil {
				return err
			}
		}
	}
	if av := v.GetArrayValue(); av != nil {
		for i, child := range av.Values {
			if err := assertValueNoMeaning18(child, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}
