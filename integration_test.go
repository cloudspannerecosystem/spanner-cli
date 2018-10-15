package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
	testProjectId  string
	testInstanceId string
	testDatabaseId string
	testCredential string

	tableIdCounter uint32
)

func TestMain(m *testing.M) {
	_ = initialize()
	// if err != nil {
	// fmt.Println(err)
	// os.Exit(0)
	// } else {
	os.Exit(m.Run())
	// }
}

func initialize() error {
	if os.Getenv(envTestProjectId) == "" || os.Getenv(envTestInstanceId) == "" || os.Getenv(envTestDatabaseId) == "" || os.Getenv(envTestCredential) == "" {
		return fmt.Errorf(
			"some environment variables are missing, so integration tests skipped: required env=%s",
			strings.Join([]string{envTestProjectId, envTestInstanceId, envTestDatabaseId, envTestCredential}, ", "),
		)
	}

	testProjectId = os.Getenv(envTestProjectId)
	testInstanceId = os.Getenv(envTestInstanceId)
	testDatabaseId = os.Getenv(envTestDatabaseId)
	testCredential = os.Getenv(envTestCredential)

	return nil
}

// func generateUniqueTableId() string {
// count := atomic(&tableIdCounter, 1)
// return fmt.Sprintf("spanner_cli_test_%d_%d", time.Now().UnixNano(), count)
// }

func TestSelect(t *testing.T) {
	t.Skip("Integration tests skipped")

	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", testProjectId, testInstanceId, testDatabaseId)

	session, err := NewSession(ctx, testProjectId, testInstanceId, testDatabaseId, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{WriteSessions: 0.2},
	}, option.WithCredentialsJSON([]byte(testCredential)))
	if err != nil {
		t.Fatalf("failed to create test session: err=%s", err)
	}

	// tableId = generateUniqueTableId()
	// tableSchema := fmt.Sprintf(`
	// CREATE TABLE %s (
	// id INT64 NOT NULL,
	// bool INT64 NOT NULL
	// ) PRIMARY KEY (id)
	// `, tableId)

	// prepare
	op, err := session.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: []string{"CREATE TABLE t100 (id INT64 NOT NULL) PRIMARY KEY (id) "},
	})
	if err != nil {
		t.Fatalf("failed to create table: err=%s", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to create table: err=%s", err)
	}

	stmt, err := BuildStatement("SELECT id FROM t100")
	if err != nil {
		t.Fatalf("invalid statement: error=%s", err)
	}

	result, err := stmt.Execute(session)
	if err != nil {
		t.Fatalf("unexpected error happened: %s", err)
	}

	opts := []cmp.Option{
		cmpopts.IgnoreFields(Stats{}, "ElapsedTime"),
	}
	expected := &Result{
		ColumnNames: []string{},
		Rows:        []Row{},
		Stats: Stats{
			AffectedRows: 0,
		},
		IsMutation: false,
	}

	if !cmp.Equal(result, expected, opts...) {
		t.Errorf("diff: %s", cmp.Diff(result, expected, opts...))
	}

	// teardown
	op, err = session.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: []string{"DROP TABLE t100"},
	})
	if err != nil {
		t.Fatalf("failed to drop table: err=%s", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("failed to drop table: err=%s", err)
	}
}
