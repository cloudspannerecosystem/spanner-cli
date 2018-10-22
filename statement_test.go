package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildStatement(t *testing.T) {
	validTests := []struct {
		Input    string
		Expected Statement
	}{
		{"SELECT * FROM t1", &SelectStatement{}},
		{"SELECT\n*\nFROM t1", &SelectStatement{}},
		{"CREATE DATABASE d1", &CreateDatabaseStatement{}},
		{"CREATE TABLE t1 (id INT64 NOT NULL) PRIMARY KEY (id)", &DdlStatement{}},
		{"ALTER TABLE t1 ADD COLUMN name STRING(16) NOT NULL", &DdlStatement{}},
		{"DROP TABLE t1", &DdlStatement{}},
		{"CREATE INDEX idx_name ON t1 (name DESC)", &DdlStatement{}},
		{"DROP INDEX idx_name", &DdlStatement{}},
		{"INSERT INTO t1 (id, name) VALUES (1, 'yuki')", &DmlStatement{}},
		{"UPDATE t1 SET name = hello WHERE id = 1", &DmlStatement{}},
		{"DELETE FROM t1 WHERE id = 1", &DmlStatement{}},
		{"BEGIN", &BeginRwStatement{}},
		{"BEGIN RW", &BeginRwStatement{}},
		{"BEGIN RO", &BeginRoStatement{}},
		{"COMMIT", &CommitStatement{}},
		{"ROLLBACK", &RollbackStatement{}},
		{"CLOSE", &CloseStatement{}},
		{"EXIT", &ExitStatement{}},
		{"USE database2", &UseStatement{}},
		{"SHOW DATABASES", &ShowDatabasesStatement{}},
		{"SHOW CREATE TABLE t1", &ShowCreateTableStatement{}},
		{"SHOW TABLES", &ShowTablesStatement{}},
		{"SHOW COLUMNS FROM t1", &ShowColumnsStatement{}},
		{"SHOW INDEX FROM t1", &ShowIndexStatement{}},
		{"SHOW INDEXES FROM t1", &ShowIndexStatement{}},
		{"SHOW KEYS FROM t1", &ShowIndexStatement{}},
		{"EXPLAIN t1", &ShowColumnsStatement{}},
		{"DESCRIBE t1", &ShowColumnsStatement{}},
		{"DESC t1", &ShowColumnsStatement{}},
		{"EXPLAIN SELECT * FROM t1", &ExplainStatement{}},
		{"DESCRIBE SELECT * FROM t1", &ExplainStatement{}},
		{"DESC SELECT * FROM t1", &ExplainStatement{}},
	}

	for _, test := range validTests {
		for i := 0; i < 2; i++ {
			input := test.Input
			if i == 1 {
				// check case insensitivility
				input = strings.ToLower(input)
			}
			got, err := BuildStatement(input)
			if err != nil {
				t.Fatal(err)
			}
			gotType := reflect.TypeOf(got)
			expectedType := reflect.TypeOf(test.Expected)

			if gotType != expectedType {
				t.Errorf("invalid statement type: expected = %s, but got = %s", expectedType, gotType)
			}
		}
	}

	invalidTests := []struct {
		Input string
	}{
		{"FOO BAR"},
		{"SELEC T FROM t1"},
		{"SET @a = 1"},
	}

	for _, test := range invalidTests {
		got, err := BuildStatement(test.Input)
		if err == nil {
			t.Errorf("expected error, but got = %#v", got)
		}
	}
}
