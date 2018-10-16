package main

import (
	"testing"
)

func equalInputStatementSlice(a []InputStatement, b []InputStatement) bool {
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i].Statement != b[i].Statement {
			return false
		}
		if a[i].Delimiter != b[i].Delimiter {
			return false
		}
	}
	return true
}

func TestSeparateInput(t *testing.T) {
	tests := []struct {
		Input    string
		Expected []InputStatement
	}{
		{`SELECT * FROM t1`, []InputStatement{InputStatement{"SELECT * FROM t1", DelimiterHorizontal}}},
		{`SELECT * FROM t1;`, []InputStatement{InputStatement{"SELECT * FROM t1", DelimiterHorizontal}}},
		{"SELECT * FROM t1\n", []InputStatement{InputStatement{"SELECT * FROM t1", DelimiterHorizontal}}},
		{`SELECT * FROM t1\G`, []InputStatement{InputStatement{"SELECT * FROM t1", DelimiterVertical}}},
		{`SELECT * FROM t1; SELECT * FROM t2\G`, []InputStatement{InputStatement{"SELECT * FROM t1", DelimiterHorizontal}, InputStatement{"SELECT * FROM t2", DelimiterVertical}}},
		{`SELECT * FROM t1\G SELECT * FROM t2`, []InputStatement{InputStatement{"SELECT * FROM t1", DelimiterVertical}, InputStatement{"SELECT * FROM t2", DelimiterHorizontal}}},
		{`SELECT * FROM t1; abcd `, []InputStatement{InputStatement{"SELECT * FROM t1", DelimiterHorizontal}, InputStatement{"abcd", DelimiterHorizontal}}},
	}

	for _, test := range tests {
		got := separateInput(test.Input)

		if !equalInputStatementSlice(got, test.Expected) {
			t.Errorf("invalid separation: expected = %v, but got = %v", test.Expected, got)
		}
	}
}
