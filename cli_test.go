package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

type nopCloser struct {
	io.Reader
}

func (n *nopCloser) Close() error {
	return nil
}

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
		{"SELECT\n*\nFROM t1;", []InputStatement{InputStatement{"SELECT\n*\nFROM t1", DelimiterHorizontal}}},
	}

	for _, test := range tests {
		got := separateInput(test.Input)

		if !equalInputStatementSlice(got, test.Expected) {
			t.Errorf("invalid separation: expected = %v, but got = %v", test.Expected, got)
		}
	}
}

func TestPrintResult(t *testing.T) {
	t.Run("DisplayModeTable", func(t *testing.T) {
		stdin := &nopCloser{&bytes.Buffer{}}
		stdout := &bytes.Buffer{}
		stderr := &bytes.Buffer{}
		cli, err := NewCli("test", "test", "test", "", stdin, stdout, stderr)
		if err != nil {
			t.Fatal(err)
		}

		result := &Result{
			ColumnNames: []string{"foo", "bar"},
			Rows: []Row{
				Row{[]string{"1", "2"}},
				Row{[]string{"3", "4"}},
			},
			Stats:      Stats{},
			IsMutation: false,
		}
		cli.PrintResult(result, DisplayModeTable, false)

		expected := strings.TrimPrefix(`
+-----+-----+
| foo | bar |
+-----+-----+
| 1   | 2   |
| 3   | 4   |
+-----+-----+
`, "\n")

		got := stdout.String()
		if got != expected {
			t.Errorf("invalid print: expected = %s, but got = %s", expected, got)
		}
	})

	t.Run("DisplayModeVertical", func(t *testing.T) {
		stdin := &nopCloser{&bytes.Buffer{}}
		stdout := &bytes.Buffer{}
		stderr := &bytes.Buffer{}
		cli, err := NewCli("test", "test", "test", "", stdin, stdout, stderr)
		if err != nil {
			t.Fatal(err)
		}

		result := &Result{
			ColumnNames: []string{"foo", "bar"},
			Rows: []Row{
				Row{[]string{"1", "2"}},
				Row{[]string{"3", "4"}},
			},
			Stats:      Stats{},
			IsMutation: false,
		}
		cli.PrintResult(result, DisplayModeVertical, false)

		expected := strings.TrimPrefix(`
*************************** 1. row ***************************
foo: 1
bar: 2
*************************** 2. row ***************************
foo: 3
bar: 4
`, "\n")

		got := stdout.String()
		if got != expected {
			t.Errorf("invalid print: expected = %s, but got = %s", expected, got)
		}
	})

	t.Run("DisplayModeTab", func(t *testing.T) {
		stdin := &nopCloser{&bytes.Buffer{}}
		stdout := &bytes.Buffer{}
		stderr := &bytes.Buffer{}
		cli, err := NewCli("test", "test", "test", "", stdin, stdout, stderr)
		if err != nil {
			t.Fatal(err)
		}

		result := &Result{
			ColumnNames: []string{"foo", "bar"},
			Rows: []Row{
				Row{[]string{"1", "2"}},
				Row{[]string{"3", "4"}},
			},
			Stats:      Stats{},
			IsMutation: false,
		}
		cli.PrintResult(result, DisplayModeTab, false)

		expected := "foo\tbar\n" +
			"1\t2\n" +
			"3\t4\n"

		got := stdout.String()
		if got != expected {
			t.Errorf("invalid print: expected = %s, but got = %s", expected, got)
		}
	})
}
