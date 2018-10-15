package main

import (
	"testing"

	"cloud.google.com/go/spanner"
)

func createRow(t *testing.T, columnValues []interface{}) *spanner.Row {
	// column names are not matter for this test, so use dummy name
	columnNames := make([]string, len(columnValues))
	for i := 0; i < len(columnNames); i++ {
		columnNames[i] = "dummy"
	}

	row, err := spanner.NewRow(columnNames, columnValues)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	return row
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

func TestDecodeRow(t *testing.T) {
	validTests := []struct {
		Input    *spanner.Row
		Expected []string
	}{
		{createRow(t, []interface{}{true}), []string{"true"}},
		{createRow(t, []interface{}{[]byte{'a', 'b', 'c'}}), []string{"YWJj"}}, // base64 encode of 'abc'
		{createRow(t, []interface{}{1.23}), []string{"1.230000"}},
		{createRow(t, []interface{}{"foo"}), []string{"foo"}},
		{createRow(t, []interface{}{1}), []string{"1"}},
	}

	for _, test := range validTests {
		got, err := DecodeRow(test.Input)
		if err != nil {
			t.Error(err)
		}

		if !equalStringSlice(got, test.Expected) {
			t.Errorf("invalid decode: expected = %v, but got = %v", test.Expected, got)
		}
	}
}
