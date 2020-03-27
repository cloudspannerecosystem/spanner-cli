package main

import (
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
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

func TestDecodeColumn(t *testing.T) {
	tests := []struct {
		desc  string
		value interface{}
		want  string
	}{
		// non-nullable
		{
			desc:  "bool",
			value:  true,
			want:  "true",
		},
		{
			desc:  "bytes",
			value: []byte{'a', 'b', 'c'},
			want:  "YWJj", // base64 encoded 'abc'
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
			value: [][]byte{{'a', 'b', 'c'}, {'e', 'f', 'g'}},
			want:  "[YWJj, ZWZn]",
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
			value: []struct{
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
			desc:  "nul array float64",
			value: []float64(nil),
			want:  "NULL",
		},
		{
			desc:  "null array int64",
			value: []int64(nil),
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

func TestDecodeRow(t *testing.T) {
	tests := []struct {
		desc  string
		values    []interface{}
		want []string
	}{
		{
			desc:  "non-null columns",
			values:  []interface{}{"foo", 123},
			want:  []string{"foo", "123"},
		},
		{
			desc:  "non-null column and null column",
			values:  []interface{}{"foo", spanner.NullString{StringVal: "", Valid: false}},
			want:  []string{"foo", "NULL"},
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
