package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

type nopCloser struct {
	io.Reader
}

func (n *nopCloser) Close() error {
	return nil
}

func equalInputStatementSlice(a []inputStatement, b []inputStatement) bool {
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i].statement != b[i].statement {
			return false
		}
		if a[i].delimiter != b[i].delimiter {
			return false
		}
	}
	return true
}

func TestSeparateInput(t *testing.T) {
	tests := []struct {
		Input    string
		Expected []inputStatement
	}{
		{`SELECT * FROM t1`, []inputStatement{inputStatement{"SELECT * FROM t1", delimiterHorizontal}}},
		{`SELECT * FROM t1;`, []inputStatement{inputStatement{"SELECT * FROM t1", delimiterHorizontal}}},
		{"SELECT * FROM t1\n", []inputStatement{inputStatement{"SELECT * FROM t1", delimiterHorizontal}}},
		{`SELECT * FROM t1\G`, []inputStatement{inputStatement{"SELECT * FROM t1", delimiterVertical}}},
		{`SELECT * FROM t1; SELECT * FROM t2\G`, []inputStatement{inputStatement{"SELECT * FROM t1", delimiterHorizontal}, inputStatement{"SELECT * FROM t2", delimiterVertical}}},
		{`SELECT * FROM t1\G SELECT * FROM t2`, []inputStatement{inputStatement{"SELECT * FROM t1", delimiterVertical}, inputStatement{"SELECT * FROM t2", delimiterHorizontal}}},
		{`SELECT * FROM t1; abcd `, []inputStatement{inputStatement{"SELECT * FROM t1", delimiterHorizontal}, inputStatement{"abcd", delimiterHorizontal}}},
		{"SELECT\n*\nFROM t1;", []inputStatement{inputStatement{"SELECT\n*\nFROM t1", delimiterHorizontal}}},
	}

	for _, test := range tests {
		got := separateInput(test.Input)

		if !equalInputStatementSlice(got, test.Expected) {
			t.Errorf("invalid separation: expected = %v, but got = %v", test.Expected, got)
		}
	}
}

func TestBuildDdlStatements(t *testing.T) {
	tests := []struct {
		Input    string
		Expected Statement
	}{
		{`SELECT * FROM t1;`, nil},
		{`CREATE TABLE t1;`, &DdlStatements{[]string{"CREATE TABLE t1"}}},
		{`CREATE TABLE t1(pk INT64) PRIMARY KEY(pk); ALTER TABLE t1 ADD COLUMN col INT64; CREATE INDEX i1 ON t1(col); DROP INDEX i1; DROP TABLE t1;`,
			&DdlStatements{[]string{
				"CREATE TABLE t1(pk INT64) PRIMARY KEY(pk)",
				"ALTER TABLE t1 ADD COLUMN col INT64",
				"CREATE INDEX i1 ON t1(col)",
				"DROP INDEX i1",
				"DROP TABLE t1",
			}}},
		{`CREATE TABLE t1(pk INT64) PRIMARY KEY(pk); SELECT * FROM t1;`, nil},
	}

	for _, test := range tests {
		got := buildDdlStatements(test.Input)

		if !cmp.Equal(got, test.Expected) {
			t.Errorf("invalid result: %v", cmp.Diff(test.Expected, got))
		}
	}
}

func TestPrintResult(t *testing.T) {
	t.Run("DisplayModeTable", func(t *testing.T) {
		out := &bytes.Buffer{}
		result := &Result{
			ColumnNames: []string{"foo", "bar"},
			Rows: []Row{
				Row{[]string{"1", "2"}},
				Row{[]string{"3", "4"}},
			},
			Stats:      Stats{},
			IsMutation: false,
		}
		printResult(out, result, DisplayModeTable, false, false)

		expected := strings.TrimPrefix(`
+-----+-----+
| foo | bar |
+-----+-----+
| 1   | 2   |
| 3   | 4   |
+-----+-----+
`, "\n")

		got := out.String()
		if got != expected {
			t.Errorf("invalid print: expected = %s, but got = %s", expected, got)
		}
	})

	t.Run("DisplayModeVertical", func(t *testing.T) {
		out := &bytes.Buffer{}
		result := &Result{
			ColumnNames: []string{"foo", "bar"},
			Rows: []Row{
				Row{[]string{"1", "2"}},
				Row{[]string{"3", "4"}},
			},
			Stats:      Stats{},
			IsMutation: false,
		}
		printResult(out, result, DisplayModeVertical, false, false)

		expected := strings.TrimPrefix(`
*************************** 1. row ***************************
foo: 1
bar: 2
*************************** 2. row ***************************
foo: 3
bar: 4
`, "\n")

		got := out.String()
		if got != expected {
			t.Errorf("invalid print: expected = %s, but got = %s", expected, got)
		}
	})

	t.Run("DisplayModeTab", func(t *testing.T) {
		out := &bytes.Buffer{}
		result := &Result{
			ColumnNames: []string{"foo", "bar"},
			Rows: []Row{
				Row{[]string{"1", "2"}},
				Row{[]string{"3", "4"}},
			},
			Stats:      Stats{},
			IsMutation: false,
		}
		printResult(out, result, DisplayModeTab, false, false)

		expected := "foo\tbar\n" +
			"1\t2\n" +
			"3\t4\n"

		got := out.String()
		if got != expected {
			t.Errorf("invalid print: expected = %s, but got = %s", expected, got)
		}
	})
}
