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
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/chzyer/readline"
	"github.com/google/go-cmp/cmp"
)

type nopCloser struct {
	io.Reader
}

func (n *nopCloser) Close() error {
	return nil
}

func TestBuildCommands(t *testing.T) {
	tests := []struct {
		Input       string
		Expected    []*command
		ExpectError bool
	}{
		{Input: `SELECT * FROM t1;`, Expected: []*command{{&SelectStatement{"SELECT * FROM t1"}, false}}},
		{Input: `CREATE TABLE t1;`, Expected: []*command{{&BulkDdlStatement{[]string{"CREATE TABLE t1"}}, false}}},
		{
			Input: `CREATE TABLE t1(pk INT64) PRIMARY KEY(pk); ALTER TABLE t1 ADD COLUMN col INT64; CREATE INDEX i1 ON t1(col); DROP INDEX i1; DROP TABLE t1;`,
			Expected: []*command{{&BulkDdlStatement{[]string{
				"CREATE TABLE t1(pk INT64) PRIMARY KEY(pk)",
				"ALTER TABLE t1 ADD COLUMN col INT64",
				"CREATE INDEX i1 ON t1(col)",
				"DROP INDEX i1",
				"DROP TABLE t1",
			}}, false}},
		},
		{
			Input: `CREATE TABLE t1(pk INT64) PRIMARY KEY(pk);
                CREATE TABLE t2(pk INT64) PRIMARY KEY(pk);
                SELECT * FROM t1\G
                DROP TABLE t1;
                DROP TABLE t2;
                SELECT 1;`,
			Expected: []*command{
				{&BulkDdlStatement{[]string{"CREATE TABLE t1(pk INT64) PRIMARY KEY(pk)", "CREATE TABLE t2(pk INT64) PRIMARY KEY(pk)"}}, false},
				{&SelectStatement{"SELECT * FROM t1"}, true},
				{&BulkDdlStatement{[]string{"DROP TABLE t1", "DROP TABLE t2"}}, false},
				{&SelectStatement{"SELECT 1"}, false},
			},
		},
		{
			Input: `
			CREATE TABLE t1(pk INT64 /* NOT NULL*/, col INT64) PRIMARY KEY(pk);
			INSERT t1(pk/*, col*/) VALUES(1/*, 2*/);
			UPDATE t1 SET col = /* pk + */ col + 1 WHERE TRUE;
			DELETE t1 WHERE TRUE /* AND pk = 1 */;
			SELECT 0x1/**/A`,
			Expected: []*command{
				{&BulkDdlStatement{[]string{"CREATE TABLE t1(pk INT64  , col INT64) PRIMARY KEY(pk)"}}, false},
				{&DmlStatement{"INSERT t1(pk/*, col*/) VALUES(1/*, 2*/)"}, false},
				{&DmlStatement{"UPDATE t1 SET col = /* pk + */ col + 1 WHERE TRUE"}, false},
				{&DmlStatement{"DELETE t1 WHERE TRUE /* AND pk = 1 */"}, false},
				{&SelectStatement{"SELECT 0x1/**/A"}, false},
			},
		},
		{
			// spanner-cli don't permit empty statements.
			Input:       `SELECT 1; /* comment */; SELECT 2`,
			ExpectError: true,
		},
		{
			Input:       `SELECT 1; /* comment 1 */; /* comment 2 */`,
			ExpectError: true,
		},
		{
			// A comment after the last semicolon is permitted.
			Input: `SELECT 1; /* comment */`,
			Expected: []*command{
				{&SelectStatement{"SELECT 1"}, false},
			},
		},
	}

	for _, test := range tests {
		got, err := buildCommands(test.Input)
		if test.ExpectError && err == nil {
			t.Errorf("expect error but not error, input: %v", test.Input)
		}
		if !test.ExpectError && err != nil {
			t.Errorf("err: %v, input: %v", err, test.Input)
		}

		if !cmp.Equal(got, test.Expected) {
			t.Errorf("invalid result: %v", cmp.Diff(test.Expected, got))
		}
	}
}

func TestReadInteractiveInput(t *testing.T) {
	for _, tt := range []struct {
		desc      string
		input     string
		want      *inputStatement
		wantError bool
	}{
		{
			desc:  "single line",
			input: "SELECT 1;\n",
			want: &inputStatement{
				statement:                "SELECT 1",
				statementWithoutComments: "SELECT 1",
				delim:                    delimiterHorizontal,
			},
		},
		{
			desc:  "multi lines",
			input: "SELECT\n* FROM\n t1\n;\n",
			want: &inputStatement{
				statement:                "SELECT\n* FROM\n t1",
				statementWithoutComments: "SELECT\n* FROM\n t1",
				delim:                    delimiterHorizontal,
			},
		},
		{
			desc:  "multi lines with vertical delimiter",
			input: "SELECT\n* FROM\n t1\\G\n",
			want: &inputStatement{
				statement:                "SELECT\n* FROM\n t1",
				statementWithoutComments: "SELECT\n* FROM\n t1",
				delim:                    delimiterVertical,
			},
		},
		{
			desc:  "multi lines with multiple comments",
			input: "SELECT\n/* comment */1,\n# comment\n2;\n",
			want: &inputStatement{
				statement:                "SELECT\n/* comment */1,\n# comment\n2",
				statementWithoutComments: "SELECT\n 1,\n 2",
				delim:                    delimiterHorizontal,
			},
		},
		{
			desc:      "multiple statements",
			input:     "SELECT 1; SELECT 2;",
			want:      nil,
			wantError: true,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			rl, err := readline.NewEx(&readline.Config{
				Stdin:  io.NopCloser(strings.NewReader(tt.input)),
				Stdout: io.Discard,
				Stderr: io.Discard,
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
				{[]string{"1", "2"}},
				{[]string{"3", "4"}},
			},
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
				{[]string{"1", "2"}},
				{[]string{"3", "4"}},
			},
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
				{[]string{"1", "2"}},
				{[]string{"3", "4"}},
			},
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

func TestResultLine(t *testing.T) {
	timestamp := "2020-04-01T15:00:00.999999999+09:00"
	ts, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		t.Fatalf("unexpected time.Parse error: %v", err)
	}

	for _, tt := range []struct {
		desc    string
		result  *Result
		verbose bool
		want    string
	}{
		{
			desc: "mutation in normal mode",
			result: &Result{
				AffectedRows: 3,
				IsMutation:   true,
				Stats: QueryStats{
					ElapsedTime: "10 msec",
				},
			},
			verbose: false,
			want:    "Query OK, 3 rows affected (10 msec)\n",
		},
		{
			desc: "mutation in verbose mode (timestamp exist)",
			result: &Result{
				AffectedRows: 3,
				IsMutation:   true,
				Stats: QueryStats{
					ElapsedTime: "10 msec",
				},
				Timestamp: ts,
			},
			verbose: true,
			want:    fmt.Sprintf("Query OK, 3 rows affected (10 msec)\ntimestamp:      %s\n", timestamp),
		},
		{
			desc: "mutation in verbose mode (both of timestamp and mutation count exist)",
			result: &Result{
				AffectedRows: 3,
				IsMutation:   true,
				Stats: QueryStats{
					ElapsedTime: "10 msec",
				},
				CommitStats: &sppb.CommitResponse_CommitStats{MutationCount: 6},
				Timestamp:   ts,
			},
			verbose: true,
			want:    fmt.Sprintf("Query OK, 3 rows affected (10 msec)\ntimestamp:      %s\nmutation_count: 6\n", timestamp),
		},
		{
			desc: "mutation in verbose mode (timestamp not exist)",
			result: &Result{
				AffectedRows: 0,
				IsMutation:   true,
				Stats: QueryStats{
					ElapsedTime: "10 msec",
				},
			},
			verbose: true,
			want:    "Query OK, 0 rows affected (10 msec)\n",
		},
		{
			desc: "query in normal mode (rows exist)",
			result: &Result{
				AffectedRows: 3,
				IsMutation:   false,
				Stats: QueryStats{
					ElapsedTime: "10 msec",
				},
			},
			verbose: false,
			want:    "3 rows in set (10 msec)\n",
		},
		{
			desc: "query in normal mode (no rows exist)",
			result: &Result{
				AffectedRows: 0,
				IsMutation:   false,
				Stats: QueryStats{
					ElapsedTime: "10 msec",
				},
			},
			verbose: false,
			want:    "Empty set (10 msec)\n",
		},
		{
			desc: "query in verbose mode (all stats fields exist)",
			result: &Result{
				AffectedRows: 3,
				IsMutation:   false,
				Stats: QueryStats{
					ElapsedTime:                "10 msec",
					CPUTime:                    "5 msec",
					RowsScanned:                "10",
					RowsReturned:               "3",
					DeletedRowsScanned:         "1",
					OptimizerVersion:           "2",
					OptimizerStatisticsPackage: "auto_20210829_05_22_28UTC",
				},
				Timestamp: ts,
			},
			verbose: true,
			want: fmt.Sprintf(`3 rows in set (10 msec)
timestamp:            %s
cpu time:             5 msec
rows scanned:         10 rows
deleted rows scanned: 1 rows
optimizer version:    2
optimizer statistics: auto_20210829_05_22_28UTC
`, timestamp),
		},
		{
			desc: "query in verbose mode (only stats fields supported by Cloud Spanner Emulator)",
			result: &Result{
				AffectedRows: 3,
				IsMutation:   false,
				Stats: QueryStats{
					ElapsedTime:  "10 msec",
					RowsReturned: "3",
				},
				Timestamp: ts,
			},
			verbose: true,
			want:    fmt.Sprintf("3 rows in set (10 msec)\ntimestamp:            %s\n", timestamp),
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			if got := resultLine(tt.result, tt.verbose); tt.want != got {
				t.Errorf("resultLine(%v, %v) = %q, but want = %q", tt.result, tt.verbose, got, tt.want)
			}
		})
	}
}
