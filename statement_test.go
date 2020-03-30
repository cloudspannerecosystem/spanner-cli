package main

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestBuildStatement(t *testing.T) {
	timestamp, err := time.Parse(time.RFC3339Nano, "2020-03-30T22:54:44.834017+09:00")
	if err != nil {
		t.Fatalf("unexpected time parse error: %v", err)
	}

	validTests := []struct {
		Input    string
		Expected Statement
	}{
		{"SELECT * FROM t1", &SelectStatement{Query: "SELECT * FROM t1"}},
		{"SELECT\n*\nFROM t1", &SelectStatement{Query: "SELECT\n*\nFROM t1"}},
		{"WITH sub AS (SELECT 1) SELECT * FROM sub", &SelectStatement{Query: "WITH sub AS (SELECT 1) SELECT * FROM sub"}},
		{"CREATE DATABASE d1", &CreateDatabaseStatement{CreateStatement: "CREATE DATABASE d1"}},
		{"CREATE TABLE t1 (id INT64 NOT NULL) PRIMARY KEY (id)", &DdlStatement{Ddl: "CREATE TABLE t1 (id INT64 NOT NULL) PRIMARY KEY (id)"}},
		{"ALTER TABLE t1 ADD COLUMN name STRING(16) NOT NULL", &DdlStatement{Ddl: "ALTER TABLE t1 ADD COLUMN name STRING(16) NOT NULL"}},
		{"DROP TABLE t1", &DdlStatement{Ddl: "DROP TABLE t1"}},
		{"CREATE INDEX idx_name ON t1 (name DESC)", &DdlStatement{Ddl: "CREATE INDEX idx_name ON t1 (name DESC)"}},
		{"DROP INDEX idx_name", &DdlStatement{Ddl: "DROP INDEX idx_name"}},
		{"INSERT INTO t1 (id, name) VALUES (1, 'yuki')", &DmlStatement{Dml: "INSERT INTO t1 (id, name) VALUES (1, 'yuki')"}},
		{"UPDATE t1 SET name = hello WHERE id = 1", &DmlStatement{Dml: "UPDATE t1 SET name = hello WHERE id = 1"}},
		{"DELETE FROM t1 WHERE id = 1", &DmlStatement{Dml: "DELETE FROM t1 WHERE id = 1"}},
		{"BEGIN", &BeginRwStatement{}},
		{"BEGIN RW", &BeginRwStatement{}},
		{"BEGIN RO", &BeginRoStatement{TimestampBoundType: strong}},
		{"BEGIN RO 10", &BeginRoStatement{Staleness: time.Duration(10 * time.Second), TimestampBoundType: exactStaleness}},
		{"BEGIN RO 2020-03-30T22:54:44.834017+09:00", &BeginRoStatement{Timestamp: timestamp, TimestampBoundType: readTimestamp}},
		{"COMMIT", &CommitStatement{}},
		{"ROLLBACK", &RollbackStatement{}},
		{"CLOSE", &CloseStatement{}},
		{"EXIT", &ExitStatement{}},
		{"USE database2", &UseStatement{Database: "database2"}},
		{"SHOW DATABASES", &ShowDatabasesStatement{}},
		{"SHOW CREATE TABLE t1", &ShowCreateTableStatement{Table: "t1"}},
		{"SHOW TABLES", &ShowTablesStatement{}},
		{"SHOW INDEX FROM t1", &ShowIndexStatement{Table: "t1"}},
		{"SHOW INDEXES FROM t1", &ShowIndexStatement{Table: "t1"}},
		{"SHOW KEYS FROM t1", &ShowIndexStatement{Table: "t1"}},
		{"SHOW COLUMNS FROM t1", &ShowColumnsStatement{Table: "t1"}},
		{"EXPLAIN t1", &ShowColumnsStatement{Table: "t1"}},
		{"DESCRIBE t1", &ShowColumnsStatement{Table: "t1"}},
		{"DESC t1", &ShowColumnsStatement{Table: "t1"}},
		{"EXPLAIN SELECT * FROM t1", &ExplainStatement{Explain: "SELECT * FROM t1"}},
		{"DESCRIBE SELECT * FROM t1", &ExplainStatement{Explain: "SELECT * FROM t1"}},
		{"DESC SELECT * FROM t1", &ExplainStatement{Explain: "SELECT * FROM t1"}},
	}

	for _, test := range validTests {
		// try with case-sensitive input
		input := test.Input
		expected := test.Expected
		got, err := BuildStatement(input)
		if err != nil {
			t.Fatalf("BuildStatement(%q) got error: %v", input, err)
		}
		if !cmp.Equal(got, expected) {
			t.Errorf("BuildStatement(%q) = %v, but expected = %v", input, got, expected)
		}

		// TODO: refactor
		// try with case-insensitive input
		//input = strings.ToLower(input)
		//got, err = BuildStatement(input)
		//if err != nil {
		//	t.Fatalf("BuildStatement(%q) got error: %v", input, err)
		//}
		//// check only type
		//gotType := reflect.TypeOf(got)
		//expectedType := reflect.TypeOf(expected)
		//if gotType != expectedType {
		//	t.Errorf("BuildStatement(%q) has invalid statement type: got = %q, but expected = %s", input, gotType, expectedType)
		//}
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
			t.Errorf("BuildStatement(%q) expected error, but got = %#v", test.Input, got)
		}
	}
}
