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
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "double queries",
			input: `SELECT "123"; SELECT "456";`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT "456"`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "triple-quoted string",
			input: `SELECT """123"""; SELECT '''456''';`,
			want: []inputStatement{
				{
					statement: `SELECT """123"""`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT '''456'''`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "triple-quoted string with new line",
			input: "SELECT \"\"\"1\n2\n3\"\"\"; SELECT '''4\n5\n6''';",
			want: []inputStatement{
				{
					statement: "SELECT \"\"\"1\n2\n3\"\"\"",
					delimiter: delimiterHorizontal,
				},
				{
					statement: "SELECT '''4\n5\n6'''",
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "raw string",
			input: `SELECT r"123\a\n"; SELECT R"456\\";`,
			want: []inputStatement{
				{
					statement: `SELECT r"123\a\n"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT R"456\\"`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "bytes string",
			input: `SELECT b"123"; SELECT b'\x12\x34';`,
			want: []inputStatement{
				{
					statement: `SELECT b"123"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT b'\x12\x34'`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "raw bytes string",
			input: `SELECT rb"123\a"; SELECT RB'\x12\x34\a';`,
			want: []inputStatement{
				{
					statement: `SELECT rb"123\a"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT RB'\x12\x34\a'`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "vertical delimiter",
			input: `SELECT "123"\G`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delimiter: delimiterVertical,
				},
			},
		},
		{
			desc: "mixed delimiter",
			input: `SELECT "123"; SELECT "456"\G SELECT "789";`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT "456"`,
					delimiter: delimiterVertical,
				},
				{
					statement: `SELECT "789"`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "sql query",
			input: `SELECT * FROM t1 WHERE id = "123" AND "456"; DELETE FROM t2 WHERE true;`,
			want: []inputStatement{
				{
					statement: `SELECT * FROM t1 WHERE id = "123" AND "456"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `DELETE FROM t2 WHERE true`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "second query is empty",
			input: `SELECT 1; ;`,
			want: []inputStatement{
				{
					statement: `SELECT 1`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: ``,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: "horizontal delimiter in string",
			input: `SELECT "1;2;3"; SELECT 'TL;DR';`,
			want: []inputStatement{
				{
					statement: `SELECT "1;2;3"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT 'TL;DR'`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: `vertical delimiter in string`,
			input: `SELECT r"1\G2\G3"\G SELECT r'4\G5\G6'\G`,
			want: []inputStatement{
				{
					statement: `SELECT r"1\G2\G3"`,
					delimiter: delimiterVertical,
				},
				{
					statement: `SELECT r'4\G5\G6'`,
					delimiter: delimiterVertical,
				},
			},
		},
		{
			desc: `multi-byte character in string`,
			input: `SELECT "テスト"; SELECT '世界'`,
			want: []inputStatement{
				{
					statement: `SELECT "テスト"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT '世界'`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: `query has new line just before delimiter`,
			input: "SELECT '123'\n; SELECT '456'\n\\G",
			want: []inputStatement{
				{
					statement: `SELECT '123'`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT '456'`,
					delimiter: delimiterVertical,
				},
			},
		},
		{
			desc: `second query ends in the middle of string`,
			input: `SELECT "123"; SELECT "45`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT "45`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			desc: `totally incorrect query`,
			input: `a"""""""""'''''''''b;`,
			want: []inputStatement{
				{
					statement: `a"""""""""'''''''''b;`,
					delimiter: delimiterHorizontal,
				},
			},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			got := newSeparator(tt.input).separate()
			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(inputStatement{})); diff != "" {
				t.Errorf("difference in statements: (-want +got):\n%s", diff)
			}
		})
	}
}
