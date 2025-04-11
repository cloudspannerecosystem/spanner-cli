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
	"context"
	"fmt"
	"google.golang.org/protobuf/testing/protocmp"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

const (
	envTestProjectId  = "SPANNER_CLI_INTEGRATION_TEST_PROJECT_ID"
	envTestInstanceId = "SPANNER_CLI_INTEGRATION_TEST_INSTANCE_ID"
	envTestDatabaseId = "SPANNER_CLI_INTEGRATION_TEST_DATABASE_ID"
	envTestCredential = "SPANNER_CLI_INTEGRATION_TEST_CREDENTIAL"
)

var (
	skipIntegrateTest bool

	testProjectId  string
	testInstanceId string
	testDatabaseId string
	testCredential string

	tableIdCounter uint32
)

type testTableSchema struct {
	Id     int64 `spanner:"id"`
	Active bool  `spanner:"active"`
}

func TestMain(m *testing.M) {
	initialize()
	os.Exit(m.Run())
}

func initialize() {
	if os.Getenv(envTestProjectId) == "" || os.Getenv(envTestInstanceId) == "" || os.Getenv(envTestDatabaseId) == "" {
		skipIntegrateTest = true
		return
	}

	testProjectId = os.Getenv(envTestProjectId)
	testInstanceId = os.Getenv(envTestInstanceId)
	testDatabaseId = os.Getenv(envTestDatabaseId)
	testCredential = os.Getenv(envTestCredential)
}

func generateUniqueTableId() string {
	count := atomic.AddUint32(&tableIdCounter, 1)
	return fmt.Sprintf("spanner_cli_test_%d_%d", time.Now().UnixNano(), count)
}

func setup(t *testing.T, ctx context.Context, dmls []string) (*Session, string, func()) {
	var options []option.ClientOption
	if testCredential != "" {
		options = append(options, option.WithCredentialsJSON([]byte(testCredential)))
	}
	session, err := NewSession(testProjectId, testInstanceId, testDatabaseId, pb.RequestOptions_PRIORITY_UNSPECIFIED, "", nil, nil, options...)
	if err != nil {
		t.Fatalf("failed to create test session: err=%s", err)
	}

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProjectId, testInstanceId, testDatabaseId)

	tableId := generateUniqueTableId()
	tableSchema := fmt.Sprintf(`
	CREATE TABLE %s (
	  id INT64 NOT NULL,
	  active BOOL NOT NULL
	) PRIMARY KEY (id)
	`, tableId)

	op, err := session.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: []string{tableSchema},
	})
	if err != nil {
		t.Fatalf("failed to create table: err=%s", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to create table: err=%s", err)
	}

	for _, dml := range dmls {
		dml = strings.Replace(dml, "[[TABLE]]", tableId, -1)
		stmt := spanner.NewStatement(dml)
		_, err := session.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err = txn.Update(ctx, stmt)
			if err != nil {
				t.Fatalf("failed to apply DML: dml=%s, err=%s", dml, err)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("failed to apply DML: dml=%s, err=%s", dml, err)
		}
	}

	tearDown := func() {
		op, err = session.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   dbPath,
			Statements: []string{fmt.Sprintf("DROP TABLE %s", tableId)},
		})
		if err != nil {
			t.Fatalf("failed to drop table: err=%s", err)
		}
		if err := op.Wait(ctx); err != nil {
			t.Fatalf("failed to drop table: err=%s", err)
		}
	}
	return session, tableId, tearDown
}

func compareResult(t *testing.T, got *Result, expected *Result) {
	t.Helper()
	opts := []cmp.Option{
		cmpopts.IgnoreFields(Result{}, "Stats"),
		cmpopts.IgnoreFields(Result{}, "Timestamp"),
		// Commit Stats is only provided by real instances
		cmpopts.IgnoreFields(Result{}, "CommitStats"),
		protocmp.Transform(),
	}
	if !cmp.Equal(got, expected, opts...) {
		t.Errorf("diff: %s", cmp.Diff(got, expected, opts...))
	}
}

func TestSelect(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	session, tableId, tearDown := setup(t, ctx, []string{
		"INSERT INTO [[TABLE]] (id, active) VALUES (1, true), (2, false)",
	})
	defer tearDown()

	stmt, err := BuildStatement(fmt.Sprintf("SELECT id, active FROM %s ORDER BY id ASC", tableId))
	if err != nil {
		t.Fatalf("invalid statement: error=%s", err)
	}

	result, err := stmt.Execute(ctx, session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	compareResult(t, result, &Result{
		ColumnNames: []string{"id", "active"},
		Rows: []Row{
			Row{[]string{"1", "true"}},
			Row{[]string{"2", "false"}},
		},
		AffectedRows: 2,
		ColumnTypes: []*pb.StructType_Field{
			{Name: "id", Type: &pb.Type{Code: pb.TypeCode_INT64}},
			{Name: "active", Type: &pb.Type{Code: pb.TypeCode_BOOL}},
		},
		IsMutation: false,
	})
}

func TestDml(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	session, tableId, tearDown := setup(t, ctx, []string{})
	defer tearDown()

	stmt, err := BuildStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (1, true), (2, false)", tableId))
	if err != nil {
		t.Fatalf("invalid statement: error=%s", err)
	}

	result, err := stmt.Execute(ctx, session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	compareResult(t, result, &Result{
		AffectedRows: 2,
		IsMutation:   true,
	})

	// check by query
	query := spanner.NewStatement(fmt.Sprintf("SELECT id, active FROM %s ORDER BY id ASC", tableId))
	iter := session.client.Single().Query(ctx, query)
	defer iter.Stop()
	var gotStructs []testTableSchema
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		var got testTableSchema
		if err := row.ToStruct(&got); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		gotStructs = append(gotStructs, got)
	}
	expectedStructs := []testTableSchema{
		{1, true},
		{2, false},
	}
	if !cmp.Equal(gotStructs, expectedStructs) {
		t.Errorf("diff: %s", cmp.Diff(gotStructs, expectedStructs))
	}
}

func TestReadWriteTransaction(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	t.Run("begin, insert, and commit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		session, tableId, tearDown := setup(t, ctx, []string{})
		defer tearDown()

		// begin
		stmt, err := BuildStatement("BEGIN")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err := stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			AffectedRows: 0,
			IsMutation:   true,
		})

		// insert
		stmt, err = BuildStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (1, true), (2, false)", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			AffectedRows: 2,
			IsMutation:   true,
		})

		// commit
		stmt, err = BuildStatement("COMMIT")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			AffectedRows: 0,
			IsMutation:   true,
		})

		// check by query
		query := spanner.NewStatement(fmt.Sprintf("SELECT id, active FROM %s ORDER BY id ASC", tableId))
		iter := session.client.Single().Query(ctx, query)
		defer iter.Stop()
		var gotStructs []testTableSchema
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			var got testTableSchema
			if err := row.ToStruct(&got); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			gotStructs = append(gotStructs, got)
		}
		expectedStructs := []testTableSchema{
			{1, true},
			{2, false},
		}
		if !cmp.Equal(gotStructs, expectedStructs) {
			t.Errorf("diff: %s", cmp.Diff(gotStructs, expectedStructs))
		}
	})

	t.Run("begin, insert, and rollback", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		session, tableId, tearDown := setup(t, ctx, []string{})
		defer tearDown()

		// begin
		stmt, err := BuildStatement("BEGIN")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err := stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			AffectedRows: 0,
			IsMutation:   true,
		})

		// insert
		stmt, err = BuildStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (1, true), (2, false)", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			AffectedRows: 2,
			IsMutation:   true,
		})

		// rollback
		stmt, err = BuildStatement("ROLLBACK")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			AffectedRows: 0,
			IsMutation:   true,
		})

		// check by query
		query := spanner.NewStatement(fmt.Sprintf("SELECT id, active FROM %s ORDER BY id ASC", tableId))
		iter := session.client.Single().Query(ctx, query)
		defer iter.Stop()
		iter.Do(func(row *spanner.Row) error {
			t.Errorf("rollbacked, but written row found: %#v", row)
			return nil
		})
	})

	t.Run("heartbeat: transaction is not aborted even if the transaction is idle", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		session, tableId, tearDown := setup(t, ctx, []string{
			"INSERT INTO [[TABLE]] (id, active) VALUES (1, true), (2, false)",
		})
		defer tearDown()

		// begin
		stmt, err := BuildStatement("BEGIN")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		if _, err := stmt.Execute(ctx, session); err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		// first query
		query := spanner.NewStatement(fmt.Sprintf("SELECT id, active FROM %s", tableId))
		iter := session.client.Single().Query(ctx, query)
		defer iter.Stop()
		if _, err := iter.Next(); err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		// default transaction idle time is 10 secs
		time.Sleep(10 * time.Second)

		// second query
		query = spanner.NewStatement(fmt.Sprintf("SELECT id, active FROM %s", tableId))
		iter = session.client.Single().Query(ctx, query)
		defer iter.Stop()
		if _, err := iter.Next(); err != nil {
			t.Fatalf("error should not happen: %s", err)
		}
	})
}

func TestReadOnlyTransaction(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	t.Run("begin ro, query, and close", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		session, tableId, tearDown := setup(t, ctx, []string{
			"INSERT INTO [[TABLE]] (id, active) VALUES (1, true), (2, false)",
		})
		defer tearDown()

		// begin
		stmt, err := BuildStatement("BEGIN RO")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err := stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			AffectedRows: 0,
			IsMutation:   true,
		})

		// query
		stmt, err = BuildStatement(fmt.Sprintf("SELECT id, active FROM %s ORDER BY id ASC", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			ColumnNames: []string{"id", "active"},
			Rows: []Row{
				Row{[]string{"1", "true"}},
				Row{[]string{"2", "false"}},
			},

			ColumnTypes: []*pb.StructType_Field{
				{Name: "id", Type: &pb.Type{Code: pb.TypeCode_INT64}},
				{Name: "active", Type: &pb.Type{Code: pb.TypeCode_BOOL}},
			},
			AffectedRows: 2,
			IsMutation:   false,
		})

		// close
		stmt, err = BuildStatement("CLOSE")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			AffectedRows: 0,
			IsMutation:   true,
		})
	})

	t.Run("begin ro with stale read", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		session, tableId, tearDown := setup(t, ctx, []string{
			"INSERT INTO [[TABLE]] (id, active) VALUES (1, true), (2, false)",
		})
		defer tearDown()

		// stale read also can't recognize the recent created table itself,
		// so sleep for a while
		time.Sleep(10 * time.Second)

		// insert more fixture
		stmt, err := BuildStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (3, true), (4, false)", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}
		if _, err := stmt.Execute(ctx, session); err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		// begin with stale read
		stmt, err = BuildStatement("BEGIN RO 5")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		if _, err := stmt.Execute(ctx, session); err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		// query
		stmt, err = BuildStatement(fmt.Sprintf("SELECT id, active FROM %s ORDER BY id ASC", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err := stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		// should not include id=3 and id=4
		compareResult(t, result, &Result{
			ColumnNames: []string{"id", "active"},
			Rows: []Row{
				Row{[]string{"1", "true"}},
				Row{[]string{"2", "false"}},
			},
			ColumnTypes: []*pb.StructType_Field{
				{Name: "id", Type: &pb.Type{Code: pb.TypeCode_INT64}},
				{Name: "active", Type: &pb.Type{Code: pb.TypeCode_BOOL}},
			},
			AffectedRows: 2,
			IsMutation:   false,
		})

		// close
		stmt, err = BuildStatement("CLOSE")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		_, err = stmt.Execute(ctx, session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}
	})
}

func TestShowCreateTable(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	session, tableId, tearDown := setup(t, ctx, []string{})
	defer tearDown()

	stmt, err := BuildStatement(fmt.Sprintf("SHOW CREATE TABLE %s", tableId))
	if err != nil {
		t.Fatalf("invalid statement: error=%s", err)
	}

	result, err := stmt.Execute(ctx, session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	compareResult(t, result, &Result{
		ColumnNames: []string{"Table", "Create Table"},
		Rows: []Row{
			Row{[]string{tableId, fmt.Sprintf("CREATE TABLE %s (\n  id INT64 NOT NULL,\n  active BOOL NOT NULL,\n) PRIMARY KEY(id)", tableId)}},
		},
		AffectedRows: 1,
		IsMutation:   false,
	})
}

func TestShowColumns(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	session, tableId, tearDown := setup(t, ctx, []string{})
	defer tearDown()

	stmt, err := BuildStatement(fmt.Sprintf("SHOW COLUMNS FROM %s", tableId))
	if err != nil {
		t.Fatalf("invalid statement: error=%s", err)
	}

	result, err := stmt.Execute(ctx, session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	compareResult(t, result, &Result{
		ColumnNames: []string{"Field", "Type", "NULL", "Key", "Key_Order", "Options"},
		Rows: []Row{
			Row{[]string{"id", "INT64", "NO", "PRIMARY_KEY", "ASC", "NULL"}},
			Row{[]string{"active", "BOOL", "NO", "NULL", "NULL", "NULL"}},
		},
		AffectedRows: 2,
		IsMutation:   false,
	})
}

func TestShowIndexes(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	session, tableId, tearDown := setup(t, ctx, []string{})
	defer tearDown()

	stmt, err := BuildStatement(fmt.Sprintf("SHOW INDEXES FROM %s", tableId))
	if err != nil {
		t.Fatalf("invalid statement: error=%s", err)
	}

	result, err := stmt.Execute(ctx, session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	compareResult(t, result, &Result{
		ColumnNames: []string{"Table", "Parent_table", "Index_name", "Index_type", "Is_unique", "Is_null_filtered", "Index_state"},
		Rows: []Row{
			Row{[]string{tableId, "", "PRIMARY_KEY", "PRIMARY_KEY", "true", "false", "NULL"}},
		},
		AffectedRows: 1,
		IsMutation:   false,
	})
}

func TestTruncateTable(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	session, tableId, tearDown := setup(t, ctx, []string{
		"INSERT INTO [[TABLE]] (id, active) VALUES (1, true), (2, false)",
	})
	defer tearDown()

	stmt, err := BuildStatement(fmt.Sprintf("TRUNCATE TABLE %s", tableId))
	if err != nil {
		t.Fatalf("invalid statement: %v", err)
	}

	if _, err := stmt.Execute(ctx, session); err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	// We don't use the TRUNCATE TABLE's result since PartitionedUpdate may return estimated affected row counts.
	// Instead, we check if rows are remained in the table.
	var count int64
	countStmt := spanner.NewStatement(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableId))
	if err := session.client.Single().Query(ctx, countStmt).Do(func(r *spanner.Row) error {
		return r.Column(0, &count)
	}); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if count != 0 {
		t.Errorf("TRUNCATE TABLE executed, but %d rows are remained", count)
	}
}

func TestPartitionedDML(t *testing.T) {
	if skipIntegrateTest {
		t.Skip("Integration tests skipped")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	session, tableId, tearDown := setup(t, ctx, []string{
		"INSERT INTO [[TABLE]] (id, active) VALUES (1, false)",
	})
	defer tearDown()

	stmt, err := BuildStatement(fmt.Sprintf("PARTITIONED UPDATE %s SET active = true WHERE true", tableId))
	if err != nil {
		t.Fatalf("invalid statement: %v", err)
	}

	if _, err := stmt.Execute(ctx, session); err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	selectStmt := spanner.NewStatement(fmt.Sprintf("SELECT active FROM %s", tableId))
	var got bool
	if err := session.client.Single().Query(ctx, selectStmt).Do(func(r *spanner.Row) error {
		return r.Column(0, &got)
	}); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if want := true; want != got {
		t.Errorf("PARTITIONED UPDATE was executed, but rows were not updated")
	}
}
