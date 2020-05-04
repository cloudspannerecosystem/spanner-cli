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
			desc:  "CREATE TABLE statement",
			input: "CREATE TABLE t1 (id INT64 NOT NULL) PRIMARY KEY (id)",
			want:  &DdlStatement{Ddl: "CREATE TABLE t1 (id INT64 NOT NULL) PRIMARY KEY (id)"},
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
			desc:  "SHOW INDEX statement",
			input: "SHOW INDEX FROM t1",
			want:  &ShowIndexStatement{Table: "t1"},
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
			desc:  "DESCRIBE SELECT statement",
			input: "DESCRIBE SELECT * FROM t1",
			want:  &ExplainStatement{Explain: "SELECT * FROM t1"},
		},
		{
			desc:  "DESC SELECT statement",
			input: "DESC SELECT * FROM t1",
			want:  &ExplainStatement{Explain: "SELECT * FROM t1"},
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
	} {
		got, err := BuildStatement(test.input)
		if err == nil {
			t.Errorf("BuildStatement(%q) = %#v, but want error", got, test.input)
		}
	}
}
