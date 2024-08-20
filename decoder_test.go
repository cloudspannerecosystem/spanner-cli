//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"encoding/base64"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"math/big"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/cloudspannerecosystem/spanner-cli/testdata/protos"
)

func createRow(t *testing.T, values []interface{}) *spanner.Row {
	t.Helper()

	// column names are not important in this test, so use dummy name
	names := make([]string, len(values))
	for i := 0; i < len(names); i++ {
		names[i] = "dummy"
	}

	row, err := spanner.NewRow(names, values)
	if err != nil {
		t.Fatalf("Creating spanner row failed unexpectedly: %v", err)
	}
	return row
}

func createColumnValue(t *testing.T, value interface{}) spanner.GenericColumnValue {
	t.Helper()

	row := createRow(t, []interface{}{value})
	var cv spanner.GenericColumnValue
	if err := row.Column(0, &cv); err != nil {
		t.Fatalf("Creating spanner column value failed unexpectedly: %v", err)
	}

	return cv
}

func equalStringSlice(a []string, b []string) bool {
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type jsonMessage struct {
	Msg string `json:"msg"`
}

func TestDecodeColumn(t *testing.T) {
	tests := []struct {
		desc  string
		value interface{}
		want  string
	}{
		// non-nullable
		{
			desc:  "bool",
			value: true,
			want:  "true",
		},
		{
			desc:  "bytes",
			value: []byte{'a', 'b', 'c', 'd'},
			want:  "YWJjZA==", // base64 encoded 'abc'
		},
		{
			desc:  "float32",
			value: float32(1.23),
			want:  "1.230000",
		},
		{
			desc:  "float64",
			value: 1.23,
			want:  "1.230000",
		},
		{
			desc:  "int64",
			value: 123,
			want:  "123",
		},
		{
			desc:  "numeric",
			value: big.NewRat(123, 100),
			want:  "1.23",
		},
		{
			desc:  "string",
			value: "foo",
			want:  "foo",
		},
		{
			desc:  "timestamp",
			value: time.Unix(1516676400, 0),
			want:  "2018-01-23T03:00:00Z",
		},
		{
			desc:  "date",
			value: civil.DateOf(time.Unix(1516676400, 0)),
			want:  "2018-01-23",
		},
		{
			desc:  "json",
			value: spanner.NullJSON{Value: jsonMessage{Msg: "foo"}, Valid: true},
			want:  `{"msg":"foo"}`,
		},
		{
			desc:  "json null is not NULL",
			value: spanner.NullJSON{Value: nil, Valid: true},
			want:  `null`,
		},

		// nullable
		{
			desc:  "null bool",
			value: spanner.NullBool{Bool: false, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null bytes",
			value: []byte(nil),
			want:  "NULL",
		},
		{
			desc:  "null float32",
			value: spanner.NullFloat32{Float32: 0, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null float64",
			value: spanner.NullFloat64{Float64: 0, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null int64",
			value: spanner.NullInt64{Int64: 0, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null numeric",
			value: spanner.NullNumeric{Numeric: big.Rat{}, Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null string",
			value: spanner.NullString{StringVal: "", Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null time",
			value: spanner.NullTime{Time: time.Unix(0, 0), Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null date",
			value: spanner.NullDate{Date: civil.DateOf(time.Unix(0, 0)), Valid: false},
			want:  "NULL",
		},
		{
			desc:  "null json",
			value: spanner.NullJSON{Value: nil, Valid: false},
			want:  "NULL",
		},

		// array non-nullable
		{
			desc:  "empty array",
			value: []bool{},
			want:  "[]",
		},
		{
			desc:  "array bool",
			value: []bool{true, false},
			want:  "[true, false]",
		},
		{
			desc:  "array bytes",
			value: [][]byte{{'a', 'b', 'c', 'd'}, {'e', 'f', 'g', 'h'}},
			want:  "[YWJjZA==, ZWZnaA==]",
		},
		{
			desc:  "array float32",
			value: []float32{1.23, 2.45},
			want:  "[1.230000, 2.450000]",
		},
		{
			desc:  "array float64",
			value: []float64{1.23, 2.45},
			want:  "[1.230000, 2.450000]",
		},
		{
			desc:  "array int64",
			value: []int64{123, 456},
			want:  "[123, 456]",
		},
		{
			desc:  "array numeric",
			value: []*big.Rat{big.NewRat(123, 100), big.NewRat(456, 1)},
			want:  "[1.23, 456]",
		},
		{
			desc:  "array string",
			value: []string{"foo", "bar"},
			want:  "[foo, bar]",
		},
		{
			desc:  "array timestamp",
			value: []time.Time{time.Unix(1516676400, 0), time.Unix(1516680000, 0)},
			want:  "[2018-01-23T03:00:00Z, 2018-01-23T04:00:00Z]",
		},
		{
			desc:  "array date",
			value: []civil.Date{civil.DateOf(time.Unix(1516676400, 0)), civil.DateOf(time.Unix(1516762800, 0))},
			want:  "[2018-01-23, 2018-01-24]",
		},
		{
			desc: "array struct",
			value: []struct {
				X int64
				Y spanner.NullString
			}{
				{
					X: 10,
					Y: spanner.NullString{StringVal: "Hello", Valid: true},
				},
				{
					X: 20,
					Y: spanner.NullString{StringVal: "", Valid: false},
				},
			},
			want: "[[10, Hello], [20, NULL]]",
		},
		{
			desc: "array json",
			value: []spanner.NullJSON{
				{Value: jsonMessage{Msg: "foo"}, Valid: true},
				{Value: jsonMessage{Msg: "bar"}, Valid: true},
			},
			want: `[{"msg":"foo"}, {"msg":"bar"}]`,
		},

		// array nullable
		{
			desc:  "null array bool",
			value: []bool(nil),
			want:  "NULL",
		},
		{
			desc:  "null array bytes",
			value: [][]byte(nil),
			want:  "NULL",
		},
		{
			desc:  "null array float32",
			value: []float32(nil),
			want:  "NULL",
		},
		{
			desc:  "null array float64",
			value: []float64(nil),
			want:  "NULL",
		},
		{
			desc:  "null array int64",
			value: []int64(nil),
			want:  "NULL",
		},
		{
			desc:  "null array numeric",
			value: []*big.Rat(nil),
			want:  "NULL",
		},
		{
			desc:  "null array string",
			value: []string(nil),
			want:  "NULL",
		},
		{
			desc:  "null array timestamp",
			value: []time.Time(nil),
			want:  "NULL",
		},
		{
			desc:  "null array date",
			value: []civil.Date(nil),
			want:  "NULL",
		},
		{
			desc:  "null array json",
			value: []spanner.NullJSON(nil),
			want:  "NULL",
		},

		// PROTO
		// This table tests only have null proto cases because of non-stability
		// See also TestDecodeColumnProtoArray and TestDecodeColumnProto
		{
			desc:  "null proto",
			value: (*protos.SingerInfo)(nil),
			want:  "NULL",
		},
		{
			desc:  "null array proto",
			value: []*protos.SingerInfo(nil),
			want:  "NULL",
		},

		// ENUM
		{
			desc:  "enum",
			value: protos.Genre_ROCK.Enum(),
			want:  "3",
		},
		{
			desc:  "null enum",
			value: spanner.NullProtoEnum{ProtoEnumVal: (*protos.Genre)(nil), Valid: true}, // must use typed nil and Valid=true, see godoc of spanner.NullProtoEnum
			want:  "NULL",
		},
		{
			desc:  "array enum",
			value: []*protos.Genre{protos.Genre_POP.Enum(), protos.Genre_FOLK.Enum()},
			want:  "[0, 2]",
		},
		{
			desc:  "null array enum",
			value: []*protos.Genre(nil),
			want:  "NULL",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			got, err := DecodeColumn(createColumnValue(t, test.value))
			if err != nil {
				t.Error(err)
			}
			if got != test.want {
				t.Errorf("DecodeColumn(%v) = %v, want = %v", test.value, got, test.want)
			}
		})
	}
}

func TestDecodeColumnProto(t *testing.T) {
	singerInfo := &protos.SingerInfo{
		SingerId:    proto.Int64(1),
		BirthDate:   proto.String("1970-01-01"),
		Nationality: proto.String("Japanese"),
		Genre:       protos.Genre_JAZZ.Enum(),
	}

	decoded, err := DecodeColumn(createColumnValue(t, singerInfo))
	if err != nil {
		t.Error(err)
	}

	b, err := base64.StdEncoding.DecodeString(decoded)
	if err != nil {
		t.Error(err)
	}

	gotSingerInfo := &protos.SingerInfo{}
	if err := proto.Unmarshal(b, gotSingerInfo); err != nil {
		t.Error(err)
	}

	if !cmp.Equal(gotSingerInfo, singerInfo, protocmp.Transform()) {
		t.Errorf("differ: %v", cmp.Diff(gotSingerInfo, singerInfo, protocmp.Transform()))
	}
}

func TestDecodeColumnProtoArray(t *testing.T) {
	singerInfos := []*protos.SingerInfo{
		{SingerId: proto.Int64(1)},
		{SingerId: proto.Int64(2)},
		{SingerId: proto.Int64(3)},
	}

	decoded, err := DecodeColumn(createColumnValue(t, singerInfos))
	if err != nil {
		t.Fatal(err)
	}
	var gotSingerInfos []*protos.SingerInfo
	for _, s := range strings.Split(strings.Trim(decoded, "[]"), ",") {
		b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
		if err != nil {
			t.Fatal(err)
		}

		gotSingerInfo := &protos.SingerInfo{}
		if err := proto.Unmarshal(b, gotSingerInfo); err != nil {
			t.Fatal(err)
		}
		gotSingerInfos = append(gotSingerInfos, gotSingerInfo)
	}

	if !cmp.Equal(gotSingerInfos, singerInfos, protocmp.Transform()) {
		t.Fatalf("differ: %v", cmp.Diff(gotSingerInfos, singerInfos, protocmp.Transform()))
	}
}

func TestDecodeRow(t *testing.T) {
	tests := []struct {
		desc   string
		values []interface{}
		want   []string
	}{
		{
			desc:   "non-null columns",
			values: []interface{}{"foo", 123},
			want:   []string{"foo", "123"},
		},
		{
			desc:   "non-null column and null column",
			values: []interface{}{"foo", spanner.NullString{StringVal: "", Valid: false}},
			want:   []string{"foo", "NULL"},
		},
	}
	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			got, err := DecodeRow(createRow(t, test.values))
			if err != nil {
				t.Error(err)
			}
			if !equalStringSlice(got, test.want) {
				t.Errorf("DecodeRow(%v) = %v, want = %v", test.values, got, test.want)
			}
		})
	}
}
