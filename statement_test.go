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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

func TestBuildStatement(t *testing.T) {
	timestamp, err := time.Parse(time.RFC3339Nano, "2020-03-30T22:54:44.834017+09:00")
	if err != nil {
		t.Fatalf("unexpected time parse error: %v", err)
	}

	// valid tests
	for _, test := range []struct {
		desc          string
		input         string
		want          Statement
		skipLowerCase bool
	}{
		{
			desc:  "SELECT statement",
			input: "SELECT * FROM t1",
			want:  &SelectStatement{Query: "SELECT * FROM t1"},
		},
		{
			desc:  "SELECT statement in multiple lines",
			input: "SELECT\n*\nFROM t1",
			want:  &SelectStatement{Query: "SELECT\n*\nFROM t1"},
		},
		{
			desc:  "SELECT statement with comment",
			input: "SELECT 0x1/**/A",
			want:  &SelectStatement{Query: "SELECT 0x1/**/A"},
		},
		{
			desc:  "WITH statement",
			input: "WITH sub AS (SELECT 1) SELECT * FROM sub",
			want:  &SelectStatement{Query: "WITH sub AS (SELECT 1) SELECT * FROM sub"},
		},
		{
			// https://cloud.google.com/spanner/docs/query-syntax#statement-hints
			desc:  "SELECT statement with statement hint",
			input: "@{USE_ADDITIONAL_PARALLELISM=TRUE} SELECT * FROM t1",
			want:  &SelectStatement{Query: "@{USE_ADDITIONAL_PARALLELISM=TRUE} SELECT * FROM t1"},
		},
		{
			desc:  "SELECT statement in parenthesis",
			input: "(SELECT * FROM t1)",
			want:  &SelectStatement{Query: "(SELECT * FROM t1)"},
		},
		{
			desc:  "WITH statement in parenthesis",
			input: "(WITH sub AS (SELECT 1) SELECT * FROM sub)",
			want:  &SelectStatement{Query: "(WITH sub AS (SELECT 1) SELECT * FROM sub)"},
		},
		{
			desc:  "SELECT statement in parenthesis with statement hint",
			input: "@{USE_ADDITIONAL_PARALLELISM=TRUE} (SELECT * FROM t1)",
			want:  &SelectStatement{Query: "@{USE_ADDITIONAL_PARALLELISM=TRUE} (SELECT * FROM t1)"},
		},
		{
			desc:  "CREATE DATABASE statement",
			input: "CREATE DATABASE d1",
			want:  &CreateDatabaseStatement{CreateStatement: "CREATE DATABASE d1"},
		},
		{
			desc:  "DROP DATABASE statement",
			input: "DROP DATABASE d1",
			want:  &DropDatabaseStatement{DatabaseId: "d1"},
		},
		{
			desc:  "DROP DATABASE statement with escaped database name",
			input: "DROP DATABASE `TABLE`",
			want:  &DropDatabaseStatement{DatabaseId: "TABLE"},
		},
		{
			desc:  "ALTER DATABASE statement",
			input: "ALTER DATABASE d1 SET OPTIONS ( version_retention_period = '7d' )",
			want:  &DdlStatement{Ddl: "ALTER DATABASE d1 SET OPTIONS ( version_retention_period = '7d' )"},
		},
		{
			desc:  "CREATE TABLE statement",
			input: "CREATE TABLE t1 (id INT64 NOT NULL) PRIMARY KEY (id)",
			want:  &DdlStatement{Ddl: "CREATE TABLE t1 (id INT64 NOT NULL) PRIMARY KEY (id)"},
		},
		{
			desc:  "RENAME TABLE statement",
			input: "RENAME TABLE t1 TO t2, t3 TO t4",
			want:  &DdlStatement{Ddl: "RENAME TABLE t1 TO t2, t3 TO t4"},
		},
		{
			desc:  "ALTER TABLE statement",
			input: "ALTER TABLE t1 ADD COLUMN name STRING(16) NOT NULL",
			want:  &DdlStatement{Ddl: "ALTER TABLE t1 ADD COLUMN name STRING(16) NOT NULL"},
		},
		{
			desc:  "DROP TABLE statement",
			input: "DROP TABLE t1",
			want:  &DdlStatement{Ddl: "DROP TABLE t1"},
		},
		{
			desc:  "CREATE INDEX statement",
			input: "CREATE INDEX idx_name ON t1 (name DESC)",
			want:  &DdlStatement{Ddl: "CREATE INDEX idx_name ON t1 (name DESC)"},
		},
		{
			desc:  "DROP INDEX statement",
			input: "DROP INDEX idx_name",
			want:  &DdlStatement{Ddl: "DROP INDEX idx_name"},
		},
		{
			desc:  "TRUNCATE TABLE statement",
			input: "TRUNCATE TABLE t1",
			want:  &TruncateTableStatement{Table: "t1"},
		},
		{
			desc:  "CREATE VIEW statement",
			input: "CREATE VIEW t1view SQL SECURITY INVOKER AS SELECT t1.Id FROM t1",
			want:  &DdlStatement{Ddl: "CREATE VIEW t1view SQL SECURITY INVOKER AS SELECT t1.Id FROM t1"},
		},
		{
			desc:  "CREATE OR REPLACE VIEW statement",
			input: "CREATE OR REPLACE VIEW t1view SQL SECURITY INVOKER AS SELECT t1.Id FROM t1",
			want:  &DdlStatement{Ddl: "CREATE OR REPLACE VIEW t1view SQL SECURITY INVOKER AS SELECT t1.Id FROM t1"},
		},
		{
			desc:  "DROP VIEW statement",
			input: "DROP VIEW t1view",
			want:  &DdlStatement{Ddl: "DROP VIEW t1view"},
		},
		{
			desc:  "CREATE CHANGE STREAM FOR ALL statement",
			input: "CREATE CHANGE STREAM EverythingStream FOR ALL",
			want:  &DdlStatement{Ddl: "CREATE CHANGE STREAM EverythingStream FOR ALL"},
		},
		{
			desc:  "CREATE CHANGE STREAM FOR specific columns statement",
			input: "CREATE CHANGE STREAM NamesAndTitles FOR Singers(FirstName, LastName), Albums(Title)",
			want:  &DdlStatement{Ddl: "CREATE CHANGE STREAM NamesAndTitles FOR Singers(FirstName, LastName), Albums(Title)"},
		},
		{
			desc:  "ALTER CHANGE STREAM SET FOR statement",
			input: "ALTER CHANGE STREAM NamesAndAlbums SET FOR Singers(FirstName, LastName), Albums, Songs",
			want:  &DdlStatement{Ddl: "ALTER CHANGE STREAM NamesAndAlbums SET FOR Singers(FirstName, LastName), Albums, Songs"},
		},
		{
			desc:  "ALTER CHANGE STREAM SET OPTIONS statement",
			input: "ALTER CHANGE STREAM NamesAndAlbums SET OPTIONS( retention_period = '36h' )",
			want:  &DdlStatement{Ddl: "ALTER CHANGE STREAM NamesAndAlbums SET OPTIONS( retention_period = '36h' )"},
		},
		{
			desc:  "ALTER CHANGE STREAM DROP FOR ALL statement",
			input: "ALTER CHANGE STREAM MyStream DROP FOR ALL",
			want:  &DdlStatement{Ddl: "ALTER CHANGE STREAM MyStream DROP FOR ALL"},
		},
		{
			desc:  "DROP CHANGE STREAM statement",
			input: "DROP CHANGE STREAM NamesAndAlbums",
			want:  &DdlStatement{Ddl: "DROP CHANGE STREAM NamesAndAlbums"},
		},
		{
			desc:  "GRANT statement",
			input: "GRANT SELECT ON TABLE employees TO ROLE hr_rep",
			want:  &DdlStatement{Ddl: "GRANT SELECT ON TABLE employees TO ROLE hr_rep"},
		},
		{
			desc:  "REVOKE statement",
			input: "REVOKE SELECT ON TABLE employees FROM ROLE hr_rep",
			want:  &DdlStatement{Ddl: "REVOKE SELECT ON TABLE employees FROM ROLE hr_rep"},
		},
		{
			desc:  "ALTER STATISTICS statement",
			input: "ALTER STATISTICS package SET OPTIONS (allow_gc = false)",
			want:  &DdlStatement{Ddl: "ALTER STATISTICS package SET OPTIONS (allow_gc = false)"},
		},
		{
			desc:  "ANALYZE statement",
			input: "ANALYZE",
			want:  &DdlStatement{Ddl: "ANALYZE"},
		},
		{
			desc:  "INSERT statement",
			input: "INSERT INTO t1 (id, name) VALUES (1, 'yuki')",
			want:  &DmlStatement{Dml: "INSERT INTO t1 (id, name) VALUES (1, 'yuki')"},
		},
		{
			desc:  "UPDATE statement",
			input: "UPDATE t1 SET name = hello WHERE id = 1",
			want:  &DmlStatement{Dml: "UPDATE t1 SET name = hello WHERE id = 1"},
		},
		{
			desc:  "DELETE statement",
			input: "DELETE FROM t1 WHERE id = 1",
			want:  &DmlStatement{Dml: "DELETE FROM t1 WHERE id = 1"},
		},
		{
			desc:  "PARTITIONED UPDATE statement",
			input: "PARTITIONED UPDATE t1 SET name = hello WHERE id > 1",
			want:  &PartitionedDmlStatement{Dml: "UPDATE t1 SET name = hello WHERE id > 1"},
		},
		{
			desc:  "PARTITIONED DELETE statement",
			input: "PARTITIONED DELETE FROM t1 WHERE id > 1",
			want:  &PartitionedDmlStatement{Dml: "DELETE FROM t1 WHERE id > 1"},
		},
		{
			desc:  "EXPLAIN INSERT statement",
			input: "EXPLAIN INSERT INTO t1 (id, name) VALUES (1, 'yuki')",
			want:  &ExplainStatement{Explain: "INSERT INTO t1 (id, name) VALUES (1, 'yuki')", IsDML: true},
		},
		{
			desc:  "EXPLAIN UPDATE statement",
			input: "EXPLAIN UPDATE t1 SET name = hello WHERE id = 1",
			want:  &ExplainStatement{Explain: "UPDATE t1 SET name = hello WHERE id = 1", IsDML: true},
		},
		{
			desc:  "EXPLAIN DELETE statement",
			input: "EXPLAIN DELETE FROM t1 WHERE id = 1",
			want:  &ExplainStatement{Explain: "DELETE FROM t1 WHERE id = 1", IsDML: true},
		},
		{
			desc:  "DESCRIBE DELETE statement",
			input: "DESCRIBE DELETE FROM t1 WHERE id = 1",
			want:  &DescribeStatement{Statement: "DELETE FROM t1 WHERE id = 1", IsDML: true},
		},
		{
			desc:  "EXPLAIN ANALYZE INSERT statement",
			input: "EXPLAIN ANALYZE INSERT INTO t1 (id, name) VALUES (1, 'yuki')",
			want:  &ExplainAnalyzeDmlStatement{Dml: "INSERT INTO t1 (id, name) VALUES (1, 'yuki')"},
		},
		{
			desc:  "EXPLAIN ANALYZE UPDATE statement",
			input: "EXPLAIN ANALYZE UPDATE t1 SET name = hello WHERE id = 1",
			want:  &ExplainAnalyzeDmlStatement{Dml: "UPDATE t1 SET name = hello WHERE id = 1"},
		},
		{
			desc:  "EXPLAIN ANALYZE DELETE statement",
			input: "EXPLAIN ANALYZE DELETE FROM t1 WHERE id = 1",
			want:  &ExplainAnalyzeDmlStatement{Dml: "DELETE FROM t1 WHERE id = 1"},
		},
		{
			desc:  "BEGIN statement",
			input: "BEGIN",
			want:  &BeginRwStatement{},
		},
		{
			desc:  "BEGIN RW statement",
			input: "BEGIN RW",
			want:  &BeginRwStatement{},
		},
		{
			desc:  "BEGIN PRIORITY statement",
			input: "BEGIN PRIORITY MEDIUM",
			want: &BeginRwStatement{
				Priority: pb.RequestOptions_PRIORITY_MEDIUM,
			},
		},
		{
			desc:  "BEGIN RW PRIORITY statement",
			input: "BEGIN RW PRIORITY LOW",
			want: &BeginRwStatement{
				Priority: pb.RequestOptions_PRIORITY_LOW,
			},
		},
		{
			desc:  "BEGIN statement with TAG",
			input: "BEGIN TAG app=spanner-cli,env=test",
			want: &BeginRwStatement{
				Tag: "app=spanner-cli,env=test",
			},
		},
		{
			desc:  "BEGIN RW statement with TAG",
			input: "BEGIN RW TAG app=spanner-cli,env=test",
			want: &BeginRwStatement{
				Tag: "app=spanner-cli,env=test",
			},
		},
		{
			desc:  "BEGIN PRIORITY statement with TAG",
			input: "BEGIN PRIORITY MEDIUM TAG app=spanner-cli,env=test",
			want: &BeginRwStatement{
				Priority: pb.RequestOptions_PRIORITY_MEDIUM,
				Tag:      "app=spanner-cli,env=test",
			},
		},
		{
			desc:  "BEGIN statement with TAG whitespace",
			input: "BEGIN TAG app=spanner-cli env=test",
			want: &BeginRwStatement{
				Tag: "app=spanner-cli env=test",
			},
		},
		{
			desc:  "BEGIN RW statement with TAG whitespace",
			input: "BEGIN RW TAG app=spanner-cli env=test",
			want: &BeginRwStatement{
				Tag: "app=spanner-cli env=test",
			},
		},
		{
			desc:  "BEGIN PRIORITY statement with TAG whitespace",
			input: "BEGIN PRIORITY MEDIUM TAG app=spanner-cli env=test",
			want: &BeginRwStatement{
				Priority: pb.RequestOptions_PRIORITY_MEDIUM,
				Tag:      "app=spanner-cli env=test",
			},
		},
		{
			desc:  "BEGIN statement with TAG quoted",
			input: "BEGIN TAG app=\"spanner-cli\" env='dev'",
			want: &BeginRwStatement{
				Tag: "app=\"spanner-cli\" env='dev'",
			},
		},
		{
			desc:  "BEGIN RO statement",
			input: "BEGIN RO",
			want:  &BeginRoStatement{TimestampBoundType: strong},
		},
		{
			desc:  "BEGIN RO staleness statement",
			input: "BEGIN RO 10",
			want:  &BeginRoStatement{Staleness: time.Duration(10 * time.Second), TimestampBoundType: exactStaleness},
		},
		{
			desc:          "BEGIN RO read timestamp statement",
			input:         "BEGIN RO 2020-03-30T22:54:44.834017+09:00",
			want:          &BeginRoStatement{Timestamp: timestamp, TimestampBoundType: readTimestamp},
			skipLowerCase: true,
		},
		{
			desc:  "BEGIN RO PRIORITY statement",
			input: "BEGIN RO PRIORITY LOW",
			want:  &BeginRoStatement{TimestampBoundType: strong, Priority: pb.RequestOptions_PRIORITY_LOW},
		},
		{
			desc:  "BEGIN RO staleness with PRIORITY statement",
			input: "BEGIN RO 10 PRIORITY HIGH",
			want: &BeginRoStatement{
				Staleness:          time.Duration(10 * time.Second),
				TimestampBoundType: exactStaleness,
				Priority:           pb.RequestOptions_PRIORITY_HIGH,
			},
		},
		{
			desc:  "BEGIN RO statement with TAG",
			input: "BEGIN RO TAG app=spanner-cli,env=test",
			want: &BeginRoStatement{
				TimestampBoundType: strong,
				Tag:                "app=spanner-cli,env=test",
			},
		},
		{
			desc:  "BEGIN RO staleness statement with TAG",
			input: "BEGIN RO 10 TAG app=spanner-cli,env=test",
			want: &BeginRoStatement{
				Staleness:          time.Duration(10 * time.Second),
				TimestampBoundType: exactStaleness,
				Tag:                "app=spanner-cli,env=test",
			},
		},
		{
			desc:  "BEGIN RO read timestamp statement with TAG",
			input: "BEGIN RO 2020-03-30T22:54:44.834017+09:00 TAG app=spanner-cli,env=test",
			want: &BeginRoStatement{
				Timestamp:          timestamp,
				TimestampBoundType: readTimestamp,
				Tag:                "app=spanner-cli,env=test",
			},
			skipLowerCase: true,
		},
		{
			desc:  "BEGIN RO PRIORITY statement with TAG",
			input: "BEGIN RO PRIORITY LOW TAG app=spanner-cli,env=test",
			want: &BeginRoStatement{
				TimestampBoundType: strong,
				Priority:           pb.RequestOptions_PRIORITY_LOW,
				Tag:                "app=spanner-cli,env=test",
			},
		},
		{
			desc:  "BEGIN RO staleness with PRIORITY statement with TAG",
			input: "BEGIN RO 10 PRIORITY HIGH TAG app=spanner-cli,env=test",
			want: &BeginRoStatement{
				Staleness:          time.Duration(10 * time.Second),
				TimestampBoundType: exactStaleness,
				Priority:           pb.RequestOptions_PRIORITY_HIGH,
				Tag:                "app=spanner-cli,env=test",
			},
		},
		{
			desc:  "COMMIT statement",
			input: "COMMIT",
			want:  &CommitStatement{},
		},
		{
			desc:  "ROLLBACK statement",
			input: "ROLLBACK",
			want:  &RollbackStatement{},
		},
		{
			desc:  "CLOSE statement",
			input: "CLOSE",
			want:  &CloseStatement{},
		},
		{
			desc:  "EXIT statement",
			input: "EXIT",
			want:  &ExitStatement{},
		},
		{
			desc:  "USE statement",
			input: "USE database2",
			want:  &UseStatement{Database: "database2"},
		},
		{
			desc:  "USE statement with quoted identifier",
			input: "USE `my-database`",
			want:  &UseStatement{Database: "my-database"},
		},
		{
			desc:  "USE statement with role",
			input: "USE database2 ROLE role2",
			want:  &UseStatement{Database: "database2", Role: "role2"},
		},
		{
			desc:  "USE statement with quoted identifier",
			input: "USE `my-database` ROLE `my-role`",
			want:  &UseStatement{Database: "my-database", Role: "my-role"},
		},
		{
			desc:  "SHOW DATABASES statement",
			input: "SHOW DATABASES",
			want:  &ShowDatabasesStatement{},
		},
		{
			desc:  "SHOW CREATE TABLE statement",
			input: "SHOW CREATE TABLE t1",
			want:  &ShowCreateTableStatement{Table: "t1"},
		},
		{
			desc:  "SHOW CREATE TABLE statement with a named schema",
			input: "SHOW CREATE TABLE sch1.t1",
			want:  &ShowCreateTableStatement{Schema: "sch1", Table: "t1"},
		},
		{
			desc:  "SHOW CREATE TABLE statement with quoted identifier",
			input: "SHOW CREATE TABLE `TABLE`",
			want:  &ShowCreateTableStatement{Table: "TABLE"},
		},
		{
			desc:  "SHOW TABLES statement",
			input: "SHOW TABLES",
			want:  &ShowTablesStatement{},
		},
		{
			desc:  "SHOW TABLES statement with schema",
			input: "SHOW TABLES sch1",
			want:  &ShowTablesStatement{Schema: "sch1"},
		},
		{
			desc:  "SHOW TABLES statement with quoted schema",
			input: "SHOW TABLES `sch1`",
			want:  &ShowTablesStatement{Schema: "sch1"},
		},
		{
			desc:  "SHOW INDEX statement",
			input: "SHOW INDEX FROM t1",
			want:  &ShowIndexStatement{Table: "t1"},
		},
		{
			desc:  "SHOW INDEX statement with a named schema",
			input: "SHOW INDEX FROM sch1.t1",
			want:  &ShowIndexStatement{Schema: "sch1", Table: "t1"},
		},
		{
			desc:  "SHOW INDEXES statement",
			input: "SHOW INDEXES FROM t1",
			want:  &ShowIndexStatement{Table: "t1"},
		},
		{
			desc:  "SHOW INDEX statement with quoted identifier",
			input: "SHOW INDEX FROM `TABLE`",
			want:  &ShowIndexStatement{Table: "TABLE"},
		},
		{
			desc:  "SHOW KEYS statement",
			input: "SHOW KEYS FROM t1",
			want:  &ShowIndexStatement{Table: "t1"},
		},
		{
			desc:  "SHOW COLUMNS statement",
			input: "SHOW COLUMNS FROM t1",
			want:  &ShowColumnsStatement{Table: "t1"},
		},
		{
			desc:  "SHOW COLUMNS statement with a named schema",
			input: "SHOW COLUMNS FROM sch1.t1",
			want:  &ShowColumnsStatement{Schema: "sch1", Table: "t1"},
		},
		{
			desc:  "SHOW COLUMNS statement with quoted identifier",
			input: "SHOW COLUMNS FROM `TABLE`",
			want:  &ShowColumnsStatement{Table: "TABLE"},
		},
		{
			desc:  "EXPLAIN SELECT statement",
			input: "EXPLAIN SELECT * FROM t1",
			want:  &ExplainStatement{Explain: "SELECT * FROM t1"},
		},
		{
			desc:  "EXPLAIN SELECT statement with statement hint",
			input: "EXPLAIN @{OPTIMIZER_VERSION=latest} SELECT * FROM t1",
			want:  &ExplainStatement{Explain: "@{OPTIMIZER_VERSION=latest} SELECT * FROM t1"},
		},
		{
			desc:  "EXPLAIN SELECT statement with WITH",
			input: "EXPLAIN WITH t1 AS (SELECT 1) SELECT * FROM t1",
			want:  &ExplainStatement{Explain: "WITH t1 AS (SELECT 1) SELECT * FROM t1"},
		},
		{
			desc:  "GRAPH statement",
			input: "GRAPH FinGraph MATCH (n) RETURN LABELS(n) AS label, n.id",
			want:  &SelectStatement{Query: "GRAPH FinGraph MATCH (n) RETURN LABELS(n) AS label, n.id"},
		},
		{
			desc:  "EXPLAIN GRAPH statement",
			input: "EXPLAIN GRAPH FinGraph MATCH (n) RETURN LABELS(n) AS label, n.id",
			want:  &ExplainStatement{Explain: "GRAPH FinGraph MATCH (n) RETURN LABELS(n) AS label, n.id"},
		},
		{
			desc:  "EXPLAIN ANALYZE GRAPH statement",
			input: "EXPLAIN ANALYZE GRAPH FinGraph MATCH (n) RETURN LABELS(n) AS label, n.id",
			want:  &ExplainAnalyzeStatement{Query: "GRAPH FinGraph MATCH (n) RETURN LABELS(n) AS label, n.id"},
		},
		{
			desc:  "DESCRIBE SELECT statement",
			input: "DESCRIBE SELECT * FROM t1",
			want:  &DescribeStatement{Statement: "SELECT * FROM t1"},
		},
		{
			desc:  "Stored system procedures",
			input: `CALL cancel_query("1234567890123456789")`,
			want:  &SelectStatement{Query: `CALL cancel_query("1234567890123456789")`},
		},
		{
			desc:  "EXPLAIN Stored system procedures",
			input: `EXPLAIN CALL cancel_query("1234567890123456789")`,
			want:  &ExplainStatement{Explain: `CALL cancel_query("1234567890123456789")`},
		},
		{
			desc:  "EXPLAIN ANALYZE Stored system procedures",
			input: `EXPLAIN ANALYZE CALL cancel_query("1234567890123456789")`,
			want:  &ExplainAnalyzeStatement{Query: `CALL cancel_query("1234567890123456789")`},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			got, err := BuildStatement(test.input)
			if err != nil {
				t.Fatalf("BuildStatement(%q) got error: %v", test.input, err)
			}
			if !cmp.Equal(got, test.want) {
				t.Errorf("BuildStatement(%q) = %v, but want = %v", test.input, got, test.want)
			}
		})

		if !test.skipLowerCase {
			input := strings.ToLower(test.input)
			t.Run("Lower "+test.desc, func(t *testing.T) {
				got, err := BuildStatement(input)
				if err != nil {
					t.Fatalf("BuildStatement(%q) got error: %v", input, err)
				}
				// check only type
				gotType := reflect.TypeOf(got)
				wantType := reflect.TypeOf(test.want)
				if gotType != wantType {
					t.Errorf("BuildStatement(%q) has invalid statement type: got = %q, but want = %q", input, gotType, wantType)
				}
			})
		}
	}

	// invalid tests
	for _, test := range []struct {
		input string
	}{
		{"FOO BAR"},
		{"SELEC T FROM t1"},
		{"SET @a = 1"},
		{"BEGIN PRIORITY CRITICAL"},
	} {
		got, err := BuildStatement(test.input)
		if err == nil {
			t.Errorf("BuildStatement(%q) = %#v, but want error", got, test.input)
		}
	}
}

func TestIsCreateTableDDL(t *testing.T) {
	for _, tt := range []struct {
		desc   string
		ddl    string
		schema string
		table  string
		want   bool
	}{
		{
			desc:  "exact match",
			ddl:   "CREATE TABLE t1 (\n",
			table: "t1",
			want:  true,
		},
		{
			desc:  "given table is prefix of DDL's table",
			ddl:   "CREATE TABLE t12 (\n",
			table: "t1",
			want:  false,
		},
		{
			desc:  "DDL's table is prefix of given table",
			ddl:   "CREATE TABLE t1 (\n",
			table: "t12",
			want:  false,
		},
		{
			desc:  "given table has reserved word",
			ddl:   "CREATE TABLE `create` (\n",
			table: "create",
			want:  true,
		},
		{
			desc:  "given table is regular expression",
			ddl:   "CREATE TABLE t1 (\n",
			table: `..`,
			want:  false,
		},
		{
			desc:  "given table is invalid regular expression",
			ddl:   "CREATE TABLE t1 (\n",
			table: `[\]`,
			want:  false,
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			if got := isCreateTableDDL(tt.ddl, tt.schema, tt.table); got != tt.want {
				t.Errorf("isCreateTableDDL(%q, %q) = %v, but want %v", tt.ddl, tt.table, got, tt.want)
			}
		})
	}
}

func TestExtractSchemaAndTable(t *testing.T) {
	for _, tt := range []struct {
		desc   string
		input  string
		schema string
		table  string
	}{
		{
			desc:   "raw table",
			input:  "table",
			schema: "",
			table:  "table",
		},
		{
			desc:   "quoted table",
			input:  "`table`",
			schema: "",
			table:  "table",
		},
		{
			desc:   "FQN",
			input:  "schema.table",
			schema: "schema",
			table:  "table",
		},
		{
			desc:   "FQN with spaces",
			input:  "schema . table",
			schema: "schema",
			table:  "table",
		},
		{
			desc:   "FQN, both schema and table are quoted",
			input:  "`schema`.`table`",
			schema: "schema",
			table:  "table",
		},
		{
			desc:   "FQN with spaces, both schema and table are quoted",
			input:  "`schema` . `table`",
			schema: "schema",
			table:  "table",
		},
		{
			desc:   "FQN, only schema is quoted",
			input:  "`schema`.table",
			schema: "schema",
			table:  "table",
		},
		{
			desc:   "FQN with spaces, only schema is quoted",
			input:  "`schema` . table",
			schema: "schema",
			table:  "table",
		},
		{
			desc:   "FQN, only table is quoted",
			input:  "schema.`table`",
			schema: "schema",
			table:  "table",
		},
		{
			desc:   "FQN with spaces, only table is quoted",
			input:  "schema . `table`",
			schema: "schema",
			table:  "table",
		},
		{
			desc:   "whole quoted FQN",
			input:  "`schema.table`",
			schema: "schema",
			table:  "table",
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			if schema, table := extractSchemaAndTable(tt.input); schema != tt.schema || table != tt.table {
				t.Errorf("extractSchemaAndTable(%q) = (%v, %v), but want (%v, %v)", tt.input, schema, table, tt.schema, tt.table)
			}
		})
	}
}
