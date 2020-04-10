package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/chzyer/readline"
	"github.com/google/go-cmp/cmp"
)

type nopCloser struct {
	io.Reader
}

func (n *nopCloser) Close() error {
	return nil
}

func TestBuildStatementsWithFlag(t *testing.T) {
	tests := []struct {
		Input    string
		Expected []*statementWithFlag
	}{
		{`SELECT * FROM t1;`, []*statementWithFlag{{&SelectStatement{"SELECT * FROM t1"}, false}}},
		{`CREATE TABLE t1;`, []*statementWithFlag{{&BulkDdlStatement{[]string{"CREATE TABLE t1"}}, false}}},
		{`CREATE TABLE t1(pk INT64) PRIMARY KEY(pk); ALTER TABLE t1 ADD COLUMN col INT64; CREATE INDEX i1 ON t1(col); DROP INDEX i1; DROP TABLE t1;`,
			[]*statementWithFlag{{&BulkDdlStatement{[]string{
				"CREATE TABLE t1(pk INT64) PRIMARY KEY(pk)",
				"ALTER TABLE t1 ADD COLUMN col INT64",
				"CREATE INDEX i1 ON t1(col)",
				"DROP INDEX i1",
				"DROP TABLE t1",
			}}, false}}},
		{`CREATE TABLE t1(pk INT64) PRIMARY KEY(pk);
                CREATE TABLE t2(pk INT64) PRIMARY KEY(pk);
                SELECT * FROM t1\G
                DROP TABLE t1;
                DROP TABLE t2;
                SELECT 1;`,
			[]*statementWithFlag{
				{&BulkDdlStatement{[]string{"CREATE TABLE t1(pk INT64) PRIMARY KEY(pk)", "CREATE TABLE t2(pk INT64) PRIMARY KEY(pk)"}}, false},
				{&SelectStatement{"SELECT * FROM t1"}, true},
				{&BulkDdlStatement{[]string{"DROP TABLE t1", "DROP TABLE t2"}}, false},
				{&SelectStatement{"SELECT 1"}, false},
			}},
	}

	for _, test := range tests {
		got, err := buildStatementsWithFlag(test.Input)
		if err != nil {
			t.Errorf("err: %v, input: %v", err, test.Input)
		}

		if !cmp.Equal(got, test.Expected) {
			t.Errorf("invalid result: %v", cmp.Diff(test.Expected, got))
		}
	}
}

func TestReadInteractiveInput(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		input string
		want  *inputStatement
		wantError bool
	}{
		{
			desc:  "single line",
			input: "SELECT 1;\n",
			want: &inputStatement{
				statement: "SELECT 1",
				delim:     delimiterHorizontal,
			},
		},
		{
			desc:  "multi lines",
			input: "SELECT\n* FROM\n t1\n;\n",
			want: &inputStatement{
				statement: "SELECT\n* FROM\n t1",
				delim:     delimiterHorizontal,
			},
		},
		{
			desc:  "multi lines with vertical delimiter",
			input: "SELECT\n* FROM\n t1\\G\n",
			want: &inputStatement{
				statement: "SELECT\n* FROM\n t1",
				delim:     delimiterVertical,
			},
		},
		{
			desc:  "multi lines with multiple comments",
			input: "SELECT\n/* comment */1,\n# comment\n2;\n",
			want: &inputStatement{
				statement: "SELECT\n1,\n2",
				delim:     delimiterHorizontal,
			},
		},
		{
			desc:  "multiple statements",
			input: "SELECT 1; SELECT 2;",
			want: nil,
			wantError: true,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			rl, err := readline.NewEx(&readline.Config{
				Stdin:  ioutil.NopCloser(strings.NewReader(tt.input)),
				Stdout: ioutil.Discard,
				Stderr: ioutil.Discard,
			})
			if err != nil {
				t.Fatalf("unexpected readline.NewEx() error: %v", err)
			}

			got, err := readInteractiveInput(rl, "")
			if err != nil && !tt.wantError {
				t.Errorf("readInteractiveInput(%q) got error: %v", tt.input, err)
			}
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(inputStatement{})); diff != "" {
				t.Errorf("difference in statement: (-want +got):\n%s", diff)
			}
		})
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
