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
			input: `SELECT "123";`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
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
			input: `SELECT r"123"; SELECT R"456";`,
			want: []inputStatement{
				{
					statement: `SELECT r"123"`,
					delimiter: delimiterHorizontal,
				},
				{
					statement: `SELECT R"456"`,
					delimiter: delimiterHorizontal,
				},
			},
		},
		{
			input: `SELECT "123"\G`,
			want: []inputStatement{
				{
					statement: `SELECT "123"`,
					delimiter: delimiterVertical,
				},
			},
		},
	} {
		s := newSeparator(tt.input)
		got, err := s.separate()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		t.Logf("got:\n%#v, want:\n%#v", got, tt.want)

		if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(inputStatement{})); diff != "" {
			t.Errorf("difference in statements: (-want +got):\n%s", diff)
		}
	}
}
