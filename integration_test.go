package main

import (
	"context"
	"fmt"
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
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
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
	if os.Getenv(envTestProjectId) == "" || os.Getenv(envTestInstanceId) == "" || os.Getenv(envTestDatabaseId) == "" || os.Getenv(envTestCredential) == "" {
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
	session, err := NewSession(ctx, testProjectId, testInstanceId, testDatabaseId, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{WriteSessions: 0.2},
	}, option.WithCredentialsJSON([]byte(testCredential)))
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

	// Wait until created table becomes visible.
	// TODO(furuyama): Change to a sophisticated way for checking table creation
	time.Sleep(10 * time.Second)

	for _, dml := range dmls {
		dml = strings.Replace(dml, "[[TABLE]]", tableId, -1)
		stmt := spanner.NewStatement(dml)
		_, err := session.client.ReadWriteTransaction(session.ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
	opts := []cmp.Option{
		cmpopts.IgnoreFields(Stats{}, "ElapsedTime"),
	}
	if !cmp.Equal(got, expected, opts...) {
		t.Errorf("diff: %s", cmp.Diff(got, expected, opts...))
	}
}

func TestSelect(t *testing.T) {
	t.Parallel()
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

	result, err := stmt.Execute(session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	compareResult(t, result, &Result{
		ColumnNames: []string{"id", "active"},
		Rows: []Row{
			Row{[]string{"1", "true"}},
			Row{[]string{"2", "false"}},
		},
		Stats: Stats{
			AffectedRows: 2,
		},
		IsMutation: false,
	})
}

func TestDml(t *testing.T) {
	t.Parallel()
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

	result, err := stmt.Execute(session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	compareResult(t, result, &Result{
		Stats: Stats{
			AffectedRows: 2,
		},
		IsMutation: true,
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
	t.Parallel()
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

		result, err := stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			Stats: Stats{
				AffectedRows: 0,
			},
			IsMutation: true,
		})

		// insert
		stmt, err = BuildStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (1, true), (2, false)", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			Stats: Stats{
				AffectedRows: 2,
			},
			IsMutation: true,
		})

		// commit
		stmt, err = BuildStatement("COMMIT")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			Stats: Stats{
				AffectedRows: 0,
			},
			IsMutation: true,
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

		result, err := stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			Stats: Stats{
				AffectedRows: 0,
			},
			IsMutation: true,
		})

		// insert
		stmt, err = BuildStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (1, true), (2, false)", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			Stats: Stats{
				AffectedRows: 2,
			},
			IsMutation: true,
		})

		// rollback
		stmt, err = BuildStatement("ROLLBACK")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			Stats: Stats{
				AffectedRows: 0,
			},
			IsMutation: true,
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

		if _, err := stmt.Execute(session); err != nil {
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
		time.Sleep(10)

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
	t.Parallel()
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

		result, err := stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			Stats: Stats{
				AffectedRows: 0,
			},
			IsMutation: true,
		})

		// query
		stmt, err = BuildStatement(fmt.Sprintf("SELECT id, active FROM %s ORDER BY id ASC", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			ColumnNames: []string{"id", "active"},
			Rows: []Row{
				Row{[]string{"1", "true"}},
				Row{[]string{"2", "false"}},
			},
			Stats: Stats{
				AffectedRows: 2,
			},
			IsMutation: false,
		})

		// close
		stmt, err = BuildStatement("CLOSE")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err = stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		compareResult(t, result, &Result{
			Stats: Stats{
				AffectedRows: 0,
			},
			IsMutation: true,
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
		time.Sleep(30 * time.Second)

		// insert more fixture
		stmt, err := BuildStatement(fmt.Sprintf("INSERT INTO %s (id, active) VALUES (3, true), (4, false)", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}
		if _, err := stmt.Execute(session); err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		// begin with stale read
		stmt, err = BuildStatement("BEGIN RO 30")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		if _, err := stmt.Execute(session); err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}

		// query
		stmt, err = BuildStatement(fmt.Sprintf("SELECT id, active FROM %s ORDER BY id ASC", tableId))
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		result, err := stmt.Execute(session)
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
			Stats: Stats{
				AffectedRows: 2,
			},
			IsMutation: false,
		})

		// close
		stmt, err = BuildStatement("CLOSE")
		if err != nil {
			t.Fatalf("invalid statement: error=%s", err)
		}

		_, err = stmt.Execute(session)
		if err != nil {
			t.Fatalf("unexpected error happened: %s", err)
		}
	})
}

func TestShowCreateTable(t *testing.T) {
	t.Parallel()
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

	result, err := stmt.Execute(session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	compareResult(t, result, &Result{
		ColumnNames: []string{"Table", "Create Table"},
		Rows: []Row{
			Row{[]string{tableId, fmt.Sprintf("CREATE TABLE %s (\n  id INT64 NOT NULL,\n  active BOOL NOT NULL,\n) PRIMARY KEY(id)", tableId)}},
		},
		Stats: Stats{
			AffectedRows: 1,
		},
		IsMutation: false,
	})
}
