package main

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSeparator(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		input string
		want  []inputStatement
	}{
		{
			desc: "single query",
			input: `SELECT "123";`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "double queries",
			input: `SELECT "123"; SELECT "456";`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT "456"`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "triple-quoted string",
			input: `SELECT """123"""; SELECT '''456''';`,
			want: []inputStatement{
				{
					statement: `SELECT """123"""`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT '''456'''`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "triple-quoted string with new line",
			input: "SELECT \"\"\"1\n2\n3\"\"\"; SELECT '''4\n5\n6''';",
			want: []inputStatement{
				{
					statement: "SELECT \"\"\"1\n2\n3\"\"\"",
					delim:     delimiterHorizontal,
				},
				{
					statement: "SELECT '''4\n5\n6'''",
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "raw string",
			input: `SELECT r"123\a\n"; SELECT R"456\\";`,
			want: []inputStatement{
				{
					statement: `SELECT r"123\a\n"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT R"456\\"`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "bytes string",
			input: `SELECT b"123"; SELECT b'\x12\x34';`,
			want: []inputStatement{
				{
					statement: `SELECT b"123"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT b'\x12\x34'`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "raw bytes string",
			input: `SELECT rb"123\a"; SELECT RB'\x12\x34\a';`,
			want: []inputStatement{
				{
					statement: `SELECT rb"123\a"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT RB'\x12\x34\a'`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "quoted identifier",
			input: "SELECT `1`, `2`; SELECT `3`, `4`;",
			want: []inputStatement{
				{
					statement: "SELECT `1`, `2`",
					delim:     delimiterHorizontal,
				},
				{
					statement: "SELECT `3`, `4`",
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "vertical delim",
			input: `SELECT "123"\G`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delim:     delimiterVertical,
				},
			},
		},
		{
			desc: "mixed delim",
			input: `SELECT "123"; SELECT "456"\G SELECT "789";`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT "456"`,
					delim:     delimiterVertical,
				},
				{
					statement: `SELECT "789"`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "sql query",
			input: `SELECT * FROM t1 WHERE id = "123" AND "456"; DELETE FROM t2 WHERE true;`,
			want: []inputStatement{
				{
					statement: `SELECT * FROM t1 WHERE id = "123" AND "456"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `DELETE FROM t2 WHERE true`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "second query is empty",
			input: `SELECT 1; ;`,
			want: []inputStatement{
				{
					statement: `SELECT 1`,
					delim:     delimiterHorizontal,
				},
				{
					statement: ``,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: "new line just after delim",
			input: "SELECT 1;\n SELECT 2\\G\n",
			want: []inputStatement{
				{
					statement: `SELECT 1`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT 2`,
					delim:     delimiterVertical,
				},
			},
		},
		{
			desc: "horizontal delim in string",
			input: `SELECT "1;2;3"; SELECT 'TL;DR';`,
			want: []inputStatement{
				{
					statement: `SELECT "1;2;3"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT 'TL;DR'`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: `vertical delim in string`,
			input: `SELECT r"1\G2\G3"\G SELECT r'4\G5\G6'\G`,
			want: []inputStatement{
				{
					statement: `SELECT r"1\G2\G3"`,
					delim:     delimiterVertical,
				},
				{
					statement: `SELECT r'4\G5\G6'`,
					delim:     delimiterVertical,
				},
			},
		},
		{
			desc: "delim in quoted identifier",
			input: "SELECT `1;2`; SELECT `3;4`;",
			want: []inputStatement{
				{
					statement: "SELECT `1;2`",
					delim:     delimiterHorizontal,
				},
				{
					statement: "SELECT `3;4`",
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: `multi-byte character in string`,
			input: `SELECT "テスト"; SELECT '世界';`,
			want: []inputStatement{
				{
					statement: `SELECT "テスト"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT '世界'`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: `query has new line just before delim`,
			input: "SELECT '123'\n; SELECT '456'\n\\G",
			want: []inputStatement{
				{
					statement: `SELECT '123'`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT '456'`,
					delim:     delimiterVertical,
				},
			},
		},
		{
			desc: `escaped quote in string`,
			input: `SELECT "1\"2\"3"; SELECT '4\'5\'6';`,
			want: []inputStatement{
				{
					statement: `SELECT "1\"2\"3"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT '4\'5\'6'`,
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: `DDL`,
			input: "CREATE t1 (\nId INT64 NOT NULL\n) PRIMARY KEY (Id);",
			want: []inputStatement{
				{
					statement: "CREATE t1 (\nId INT64 NOT NULL\n) PRIMARY KEY (Id)",
					delim:     delimiterHorizontal,
				},
			},
		},
		{
			desc: `second query ends in the middle of string`,
			input: `SELECT "123"; SELECT "45`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delim:     delimiterHorizontal,
				},
				{
					statement: `SELECT "45`,
					delim:     delimiterUndefined,
				},
			},
		},
		{
			desc: `totally incorrect query`,
			input: `a"""""""""'''''''''b`,
			want: []inputStatement{
				{
					statement: `a"""""""""'''''''''b`,
					delim:     delimiterUndefined,
				},
			},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got := separateInput(tt.input)
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(inputStatement{})); diff != "" {
				t.Errorf("difference in statements: (-want +got):\n%s", diff)
			}
		})
	}
}
