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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
)

// Partitioned DML tends to take long time to be finished.
// See: https://github.com/cloudspannerecosystem/spanner-cli/issues/102
const pdmlTimeout = time.Hour * 24

type Statement interface {
	Execute(ctx context.Context, session *Session) (*Result, error)
}

// rowCountType is type of modified rows count by DML.
type rowCountType int

const (
	// rowCountTypeExact is exact count type for DML result.
	rowCountTypeExact rowCountType = iota
	// rowCountTypeLowerBound is lower bound type for Partitioned DML result.
	rowCountTypeLowerBound
)

type Result struct {
	ColumnNames      []string
	Rows             []Row
	Predicates       []string
	AffectedRows     int
	AffectedRowsType rowCountType
	Stats            QueryStats
	IsMutation       bool
	Timestamp        time.Time
	ForceVerbose     bool
	CommitStats      *pb.CommitResponse_CommitStats
}

type Row struct {
	Columns []string
}

// QueryStats contains query statistics.
// Some fields may not have a valid value depending on the environment.
// For example, only ElapsedTime and RowsReturned has valid value for Cloud Spanner Emulator.
type QueryStats struct {
	ElapsedTime                string
	CPUTime                    string
	RowsReturned               string
	RowsScanned                string
	DeletedRowsScanned         string
	OptimizerVersion           string
	OptimizerStatisticsPackage string
}

var (
	// SQL
	selectRe = regexp.MustCompile(`(?is)^(?:WITH|@{.+|SELECT)\s.+$`)

	// DDL
	createDatabaseRe = regexp.MustCompile(`(?is)^CREATE\s+DATABASE\s.+$`)
	dropDatabaseRe   = regexp.MustCompile(`(?is)^DROP\s+DATABASE\s+(.+)$`)
	createRe         = regexp.MustCompile(`(?is)^CREATE\s.+$`)
	dropRe           = regexp.MustCompile(`(?is)^DROP\s.+$`)
	grantRe          = regexp.MustCompile(`(?is)^GRANT\s.+$`)
	revokeRe         = regexp.MustCompile(`(?is)^REVOKE\s.+$`)
	alterRe          = regexp.MustCompile(`(?is)^ALTER\s.+$`)
	truncateTableRe  = regexp.MustCompile(`(?is)^TRUNCATE\s+TABLE\s+(.+)$`)
	analyzeRe        = regexp.MustCompile(`(?is)^ANALYZE$`)

	// DML
	dmlRe = regexp.MustCompile(`(?is)^(INSERT|UPDATE|DELETE)\s+.+$`)

	// Partitioned DML
	// In fact, INSERT is not supported in a Partitioned DML, but accept it for showing better error message.
	// https://cloud.google.com/spanner/docs/dml-partitioned#features_that_arent_supported
	pdmlRe = regexp.MustCompile(`(?is)^PARTITIONED\s+((?:INSERT|UPDATE|DELETE)\s+.+$)`)

	// Transaction
	beginRwRe  = regexp.MustCompile(`(?is)^BEGIN(?:\s+RW)?(?:\s+PRIORITY\s+(HIGH|MEDIUM|LOW))?(?:\s+TAG\s+(.+))?$`)
	beginRoRe  = regexp.MustCompile(`(?is)^BEGIN\s+RO(?:\s+([^\s]+))?(?:\s+PRIORITY\s+(HIGH|MEDIUM|LOW))?(?:\s+TAG\s+(.+))?$`)
	commitRe   = regexp.MustCompile(`(?is)^COMMIT$`)
	rollbackRe = regexp.MustCompile(`(?is)^ROLLBACK$`)
	closeRe    = regexp.MustCompile(`(?is)^CLOSE$`)

	// Other
	exitRe            = regexp.MustCompile(`(?is)^EXIT$`)
	useRe             = regexp.MustCompile(`(?is)^USE\s+(.+)$`)
	showDatabasesRe   = regexp.MustCompile(`(?is)^SHOW\s+DATABASES$`)
	showCreateTableRe = regexp.MustCompile(`(?is)^SHOW\s+CREATE\s+TABLE\s+(.+)$`)
	showTablesRe      = regexp.MustCompile(`(?is)^SHOW\s+TABLES$`)
	showColumnsRe     = regexp.MustCompile(`(?is)^(?:SHOW\s+COLUMNS\s+FROM)\s+(.+)$`)
	showIndexRe       = regexp.MustCompile(`(?is)^SHOW\s+(?:INDEX|INDEXES|KEYS)\s+FROM\s+(.+)$`)
	explainRe         = regexp.MustCompile(`(?is)^(?:EXPLAIN|DESC(?:RIBE)?)\s+(ANALYZE\s+)?(.+)$`)
)

var (
	explainColumnNames        = []string{"ID", "Query_Execution_Plan"}
	explainAnalyzeColumnNames = []string{"ID", "Query_Execution_Plan", "Rows_Returned", "Executions", "Total_Latency"}
)

func BuildStatement(input string) (Statement, error) {
	switch {
	case exitRe.MatchString(input):
		return &ExitStatement{}, nil
	case useRe.MatchString(input):
		matched := useRe.FindStringSubmatch(input)
		return &UseStatement{Database: unquoteIdentifier(matched[1])}, nil
	case selectRe.MatchString(input):
		return &SelectStatement{Query: input}, nil
	case createDatabaseRe.MatchString(input):
		return &CreateDatabaseStatement{CreateStatement: input}, nil
	case createRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case dropDatabaseRe.MatchString(input):
		matched := dropDatabaseRe.FindStringSubmatch(input)
		return &DropDatabaseStatement{DatabaseId: unquoteIdentifier(matched[1])}, nil
	case dropRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case alterRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case grantRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case revokeRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case truncateTableRe.MatchString(input):
		matched := truncateTableRe.FindStringSubmatch(input)
		return &TruncateTableStatement{Table: unquoteIdentifier(matched[1])}, nil
	case analyzeRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case showDatabasesRe.MatchString(input):
		return &ShowDatabasesStatement{}, nil
	case showCreateTableRe.MatchString(input):
		matched := showCreateTableRe.FindStringSubmatch(input)
		return &ShowCreateTableStatement{Table: unquoteIdentifier(matched[1])}, nil
	case showTablesRe.MatchString(input):
		return &ShowTablesStatement{}, nil
	case explainRe.MatchString(input):
		matched := explainRe.FindStringSubmatch(input)
		isAnalyze := matched[1] != ""
		isDML := dmlRe.MatchString(matched[2])
		switch {
		case isAnalyze && isDML:
			return &ExplainAnalyzeDmlStatement{Dml: matched[2]}, nil
		case isAnalyze:
			return &ExplainAnalyzeStatement{Query: matched[2]}, nil
		case isDML:
			return &ExplainDmlStatement{Dml: matched[2]}, nil
		default:
			return &ExplainStatement{Explain: matched[2]}, nil
		}
	case showColumnsRe.MatchString(input):
		matched := showColumnsRe.FindStringSubmatch(input)
		return &ShowColumnsStatement{Table: unquoteIdentifier(matched[1])}, nil
	case showIndexRe.MatchString(input):
		matched := showIndexRe.FindStringSubmatch(input)
		return &ShowIndexStatement{Table: unquoteIdentifier(matched[1])}, nil
	case dmlRe.MatchString(input):
		return &DmlStatement{Dml: input}, nil
	case pdmlRe.MatchString(input):
		matched := pdmlRe.FindStringSubmatch(input)
		return &PartitionedDmlStatement{Dml: matched[1]}, nil
	case beginRwRe.MatchString(input):
		return newBeginRwStatement(input)
	case beginRoRe.MatchString(input):
		return newBeginRoStatement(input)
	case commitRe.MatchString(input):
		return &CommitStatement{}, nil
	case rollbackRe.MatchString(input):
		return &RollbackStatement{}, nil
	case closeRe.MatchString(input):
		return &CloseStatement{}, nil
	}

	return nil, errors.New("invalid statement")
}

func unquoteIdentifier(input string) string {
	return strings.Trim(input, "`")
}

type SelectStatement struct {
	Query string
}

func (s *SelectStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Query)

	iter, roTxn := session.RunQueryWithStats(ctx, stmt)
	defer iter.Stop()

	rows, columnNames, err := parseQueryResult(iter)
	if err != nil {
		if session.InReadWriteTransaction() && spanner.ErrCode(err) == codes.Aborted {
			// Need to call rollback to free the acquired session in underlying google-cloud-go/spanner.
			rollback := &RollbackStatement{}
			rollback.Execute(ctx, session)
		}
		return nil, err
	}
	result := &Result{
		ColumnNames: columnNames,
		Rows:        rows,
	}

	queryStats := parseQueryStats(iter.QueryStats)
	rowsReturned, err := strconv.Atoi(queryStats.RowsReturned)
	if err != nil {
		return nil, fmt.Errorf("rowsReturned is invalid: %v", err)
	}

	result.AffectedRows = rowsReturned
	result.Stats = queryStats

	// ReadOnlyTransaction.Timestamp() is invalid until read.
	if roTxn != nil {
		result.Timestamp, _ = roTxn.Timestamp()
	}

	return result, nil
}

// parseQueryResult parses rows and columnNames from spanner.RowIterator.
// A caller is responsible for calling iterator.Stop().
func parseQueryResult(iter *spanner.RowIterator) ([]Row, []string, error) {
	var rows []Row
	var columnNames []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if len(columnNames) == 0 {
			columnNames = row.ColumnNames()
		}

		columns, err := DecodeRow(row)
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, Row{
			Columns: columns,
		})
	}
	return rows, columnNames, nil
}

// parseQueryStats parses spanner.RowIterator.QueryStats.
func parseQueryStats(stats map[string]interface{}) QueryStats {
	var queryStats QueryStats

	if v, ok := stats["elapsed_time"]; ok {
		if elapsed, ok := v.(string); ok {
			queryStats.ElapsedTime = elapsed
		}
	}

	if v, ok := stats["rows_returned"]; ok {
		if returned, ok := v.(string); ok {
			queryStats.RowsReturned = returned
		}
	}

	if v, ok := stats["rows_scanned"]; ok {
		if scanned, ok := v.(string); ok {
			queryStats.RowsScanned = scanned
		}
	}

	if v, ok := stats["deleted_rows_scanned"]; ok {
		if deletedRowsScanned, ok := v.(string); ok {
			queryStats.DeletedRowsScanned = deletedRowsScanned
		}
	}

	if v, ok := stats["cpu_time"]; ok {
		if cpu, ok := v.(string); ok {
			queryStats.CPUTime = cpu
		}
	}

	if v, ok := stats["optimizer_version"]; ok {
		if version, ok := v.(string); ok {
			queryStats.OptimizerVersion = version
		}
	}

	if v, ok := stats["optimizer_statistics_package"]; ok {
		if pkg, ok := v.(string); ok {
			queryStats.OptimizerStatisticsPackage = pkg
		}
	}

	return queryStats
}

type CreateDatabaseStatement struct {
	CreateStatement string
}

func (s *CreateDatabaseStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	op, err := session.adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          session.InstancePath(),
		CreateStatement: s.CreateStatement,
	})
	if err != nil {
		return nil, err
	}
	if _, err := op.Wait(ctx); err != nil {
		return nil, err
	}

	return &Result{IsMutation: true}, nil
}

type DropDatabaseStatement struct {
	DatabaseId string
}

func (s *DropDatabaseStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if err := session.adminClient.DropDatabase(ctx, &adminpb.DropDatabaseRequest{
		Database: fmt.Sprintf("projects/%s/instances/%s/databases/%s", session.projectId, session.instanceId, s.DatabaseId),
	}); err != nil {
		return nil, err
	}

	return &Result{IsMutation: true}, nil
}

type DdlStatement struct {
	Ddl string
}

func (s *DdlStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	return executeDdlStatements(ctx, session, []string{s.Ddl})
}

type BulkDdlStatement struct {
	Ddls []string
}

func (s *BulkDdlStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	return executeDdlStatements(ctx, session, s.Ddls)
}

func executeDdlStatements(ctx context.Context, session *Session, ddls []string) (*Result, error) {
	op, err := session.adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   session.DatabasePath(),
		Statements: ddls,
	})
	if err != nil {
		return nil, err
	}
	if err := op.Wait(ctx); err != nil {
		return nil, err
	}

	return &Result{IsMutation: true}, nil
}

type ShowDatabasesStatement struct {
}

func (s *ShowDatabasesStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	result := &Result{ColumnNames: []string{"Database"}}

	dbIter := session.adminClient.ListDatabases(ctx, &adminpb.ListDatabasesRequest{
		Parent: session.InstancePath(),
	})

	for {
		database, err := dbIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		re := regexp.MustCompile(`projects/[^/]+/instances/[^/]+/databases/(.+)`)
		matched := re.FindStringSubmatch(database.GetName())
		dbname := matched[1]
		resultRow := Row{
			Columns: []string{dbname},
		}
		result.Rows = append(result.Rows, resultRow)
	}

	result.AffectedRows = len(result.Rows)

	return result, nil
}

type ShowCreateTableStatement struct {
	Table string
}

func (s *ShowCreateTableStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	result := &Result{ColumnNames: []string{"Table", "Create Table"}}

	ddlResponse, err := session.adminClient.GetDatabaseDdl(ctx, &adminpb.GetDatabaseDdlRequest{
		Database: session.DatabasePath(),
	})
	if err != nil {
		return nil, err
	}
	for _, stmt := range ddlResponse.Statements {
		if isCreateTableDDL(stmt, s.Table) {
			resultRow := Row{
				Columns: []string{s.Table, stmt},
			}
			result.Rows = append(result.Rows, resultRow)
			break
		}
	}
	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("table %q doesn't exist", s.Table)
	}

	result.AffectedRows = len(result.Rows)

	return result, nil
}

func isCreateTableDDL(ddl string, table string) bool {
	table = regexp.QuoteMeta(table)
	re := fmt.Sprintf("(?i)^CREATE TABLE (%s|`%s`)\\s*\\(", table, table)
	return regexp.MustCompile(re).MatchString(ddl)
}

type ShowTablesStatement struct{}

func (s *ShowTablesStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if session.InReadWriteTransaction() {
		// INFORMATION_SCHEMA can not be used in read-write transaction.
		// https://cloud.google.com/spanner/docs/information-schema
		return nil, errors.New(`"SHOW TABLES" can not be used in a read-write transaction`)
	}

	alias := fmt.Sprintf("Tables_in_%s", session.databaseId)
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT t.TABLE_NAME AS `%s` FROM INFORMATION_SCHEMA.TABLES AS t WHERE t.TABLE_CATALOG = '' and t.TABLE_SCHEMA = ''", alias))

	iter, _ := session.RunQuery(ctx, stmt)
	defer iter.Stop()

	rows, columnNames, err := parseQueryResult(iter)
	if err != nil {
		return nil, err
	}

	return &Result{
		ColumnNames:  columnNames,
		Rows:         rows,
		AffectedRows: len(rows),
	}, nil
}

type ExplainStatement struct {
	Explain string
}

func (s *ExplainStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Explain)
	queryPlan, err := session.client.Single().AnalyzeQuery(ctx, stmt)
	if err != nil {
		return nil, err
	}

	rows, predicates, err := processPlanWithoutStats(queryPlan)
	if err != nil {
		return nil, err
	}

	result := &Result{
		ColumnNames:  explainColumnNames,
		AffectedRows: 1,
		Rows:         rows,
		Predicates:   predicates,
	}

	return result, nil
}

type ExplainAnalyzeStatement struct {
	Query string
}

func (s *ExplainAnalyzeStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Query)

	iter, roTxn := session.RunQueryWithStats(ctx, stmt)

	// consume iter
	err := iter.Do(func(*spanner.Row) error {
		return nil
	})
	if err != nil {
		return nil, err
	}

	queryStats := parseQueryStats(iter.QueryStats)
	rowsReturned, err := strconv.Atoi(queryStats.RowsReturned)
	if err != nil {
		return nil, fmt.Errorf("rowsReturned is invalid: %v", err)
	}

	// Cloud Spanner Emulator doesn't set query plan nodes to the result.
	// See: https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/blob/77188b228e7757cd56ecffb5bc3ee85dce5d6ae1/frontend/handlers/queries.cc#L224-L230
	if iter.QueryPlan == nil {
		return nil, errors.New("EXPLAIN ANALYZE statement is not supported for Cloud Spanner Emulator.")
	}

	rows, predicates, err := processPlanWithStats(iter.QueryPlan)
	if err != nil {
		return nil, err
	}

	// ReadOnlyTransaction.Timestamp() is invalid until read.
	var timestamp time.Time
	if roTxn != nil {
		timestamp, _ = roTxn.Timestamp()
	}

	result := &Result{
		ColumnNames:  explainAnalyzeColumnNames,
		ForceVerbose: true,
		AffectedRows: rowsReturned,
		Stats:        queryStats,
		Timestamp:    timestamp,
		Rows:         rows,
		Predicates:   predicates,
	}
	return result, nil
}

func processPlanWithStats(plan *pb.QueryPlan) (rows []Row, predicates []string, err error) {
	return processPlanImpl(plan, true)
}

func processPlanWithoutStats(plan *pb.QueryPlan) (rows []Row, predicates []string, err error) {
	return processPlanImpl(plan, false)
}

func processPlanImpl(plan *pb.QueryPlan, withStats bool) (rows []Row, predicates []string, err error) {
	planNodes := plan.GetPlanNodes()
	maxWidthOfNodeID := len(fmt.Sprint(getMaxRelationalNodeID(plan)))
	widthOfNodeIDWithIndicator := maxWidthOfNodeID + 1

	tree := BuildQueryPlanTree(plan, 0)

	treeRows, err := tree.RenderTreeWithStats(planNodes)
	if err != nil {
		return nil, nil, err
	}
	for _, row := range treeRows {
		var formattedID string
		if len(row.Predicates) > 0 {
			formattedID = fmt.Sprintf("%*s", widthOfNodeIDWithIndicator, "*"+fmt.Sprint(row.ID))
		} else {
			formattedID = fmt.Sprintf("%*d", widthOfNodeIDWithIndicator, row.ID)
		}
		if withStats {
			rows = append(rows, Row{[]string{formattedID, row.Text, row.RowsTotal, row.Execution, row.LatencyTotal}})
		} else {
			rows = append(rows, Row{[]string{formattedID, row.Text}})
		}
		for i, predicate := range row.Predicates {
			var prefix string
			if i == 0 {
				prefix = fmt.Sprintf("%*d:", maxWidthOfNodeID, row.ID)
			} else {
				prefix = strings.Repeat(" ", maxWidthOfNodeID+1)
			}
			predicates = append(predicates, fmt.Sprintf("%s %s", prefix, predicate))
		}
	}
	return rows, predicates, nil
}

type ShowColumnsStatement struct {
	Table string
}

func (s *ShowColumnsStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if session.InReadWriteTransaction() {
		// INFORMATION_SCHEMA can not be used in read-write transaction.
		// https://cloud.google.com/spanner/docs/information-schema
		return nil, errors.New(`"SHOW COLUMNS" can not be used in a read-write transaction`)
	}

	stmt := spanner.Statement{SQL: `SELECT
  C.COLUMN_NAME as Field,
  C.SPANNER_TYPE as Type,
  C.IS_NULLABLE as ` + "`NULL`" + `,
  I.INDEX_TYPE as Key,
  IC.COLUMN_ORDERING as Key_Order,
  CONCAT(CO.OPTION_NAME, "=", CO.OPTION_VALUE) as Options
FROM
  INFORMATION_SCHEMA.COLUMNS C
LEFT JOIN
  INFORMATION_SCHEMA.INDEX_COLUMNS IC USING(TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME)
LEFT JOIN
  INFORMATION_SCHEMA.INDEXES I USING(TABLE_SCHEMA, TABLE_NAME, INDEX_NAME)
LEFT JOIN
  INFORMATION_SCHEMA.COLUMN_OPTIONS CO USING(TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME)
WHERE
  C.TABLE_SCHEMA = '' AND LOWER(C.TABLE_NAME) = LOWER(@table_name)
ORDER BY
  C.ORDINAL_POSITION ASC`,
		Params: map[string]interface{}{"table_name": s.Table}}

	iter, _ := session.RunQuery(ctx, stmt)
	defer iter.Stop()

	rows, columnNames, err := parseQueryResult(iter)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("table %q doesn't exist", s.Table)
	}

	return &Result{
		ColumnNames:  columnNames,
		Rows:         rows,
		AffectedRows: len(rows),
	}, nil
}

type ShowIndexStatement struct {
	Table string
}

func (s *ShowIndexStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if session.InReadWriteTransaction() {
		// INFORMATION_SCHEMA can not be used in read-write transaction.
		// https://cloud.google.com/spanner/docs/information-schema
		return nil, errors.New(`"SHOW INDEX" can not be used in a read-write transaction`)
	}

	stmt := spanner.Statement{
		SQL: `SELECT
  TABLE_NAME as Table,
  PARENT_TABLE_NAME as Parent_table,
  INDEX_NAME as Index_name,
  INDEX_TYPE as Index_type,
  IS_UNIQUE as Is_unique,
  IS_NULL_FILTERED as Is_null_filtered,
  INDEX_STATE as Index_state
FROM
  INFORMATION_SCHEMA.INDEXES I
WHERE
  I.TABLE_SCHEMA = '' AND LOWER(TABLE_NAME) = LOWER(@table_name)`,
		Params: map[string]interface{}{"table_name": s.Table}}

	iter, _ := session.RunQuery(ctx, stmt)
	defer iter.Stop()

	rows, columnNames, err := parseQueryResult(iter)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("table %q doesn't exist", s.Table)
	}

	return &Result{
		ColumnNames:  columnNames,
		Rows:         rows,
		AffectedRows: len(rows),
	}, nil
}

type TruncateTableStatement struct {
	Table string
}

func (s *TruncateTableStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if session.InReadWriteTransaction() {
		// PartitionedUpdate creates a new transaction and it could cause dead lock with the current running transaction.
		return nil, errors.New(`"TRUNCATE TABLE" can not be used in a read-write transaction`)
	}
	if session.InReadOnlyTransaction() {
		// Just for user-friendly.
		return nil, errors.New(`"TRUNCATE TABLE" can not be used in a read-only transaction`)
	}

	stmt := spanner.NewStatement(fmt.Sprintf("DELETE FROM `%s` WHERE true", s.Table))
	ctx, cancel := context.WithTimeout(ctx, pdmlTimeout)
	defer cancel()

	count, err := session.client.PartitionedUpdate(ctx, stmt)
	if err != nil {
		return nil, err
	}
	return &Result{
		IsMutation:   true,
		AffectedRows: int(count),
	}, nil
}

type DmlStatement struct {
	Dml string
}

func (s *DmlStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Dml)

	result := &Result{IsMutation: true}

	var numRows int64
	var err error
	if session.InReadWriteTransaction() {
		numRows, err = session.RunUpdate(ctx, stmt)
		if err != nil {
			// Need to call rollback to free the acquired session in underlying google-cloud-go/spanner.
			rollback := &RollbackStatement{}
			rollback.Execute(ctx, session)
			return nil, fmt.Errorf("transaction was aborted: %v", err)
		}
	} else {
		// Start implicit transaction.
		begin := BeginRwStatement{}
		if _, err = begin.Execute(ctx, session); err != nil {
			return nil, err
		}

		numRows, err = session.RunUpdate(ctx, stmt)
		if err != nil {
			// once error has happened, escape from implicit transaction
			rollback := &RollbackStatement{}
			rollback.Execute(ctx, session)
			return nil, err
		}

		commit := CommitStatement{}
		txnResult, err := commit.Execute(ctx, session)
		if err != nil {
			return nil, err
		}
		result.Timestamp = txnResult.Timestamp
		result.CommitStats = txnResult.CommitStats
	}

	result.AffectedRows = int(numRows)

	return result, nil
}

type PartitionedDmlStatement struct {
	Dml string
}

func (s *PartitionedDmlStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if session.InReadWriteTransaction() {
		// PartitionedUpdate creates a new transaction and it could cause dead lock with the current running transaction.
		return nil, errors.New(`Partitioned DML statement can not be run in a read-write transaction`)
	}
	if session.InReadOnlyTransaction() {
		// Just for user-friendly.
		return nil, errors.New(`Partitioned DML statement can not be run in a read-only transaction`)
	}

	stmt := spanner.NewStatement(s.Dml)
	ctx, cancel := context.WithTimeout(ctx, pdmlTimeout)
	defer cancel()

	count, err := session.client.PartitionedUpdate(ctx, stmt)
	if err != nil {
		return nil, err
	}
	return &Result{
		IsMutation:       true,
		AffectedRows:     int(count),
		AffectedRowsType: rowCountTypeLowerBound,
	}, nil
}

type ExplainDmlStatement struct {
	Dml string
}

func (s *ExplainDmlStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	_, timestamp, queryPlan, err := runInNewOrExistRwTxForExplain(ctx, session, func() (int64, *pb.QueryPlan, error) {
		plan, err := session.RunAnalyzeQuery(ctx, spanner.NewStatement(s.Dml))
		return 0, plan, err
	})
	if err != nil {
		return nil, err
	}

	rows, predicates, err := processPlanWithoutStats(queryPlan)
	if err != nil {
		return nil, err
	}
	result := &Result{
		IsMutation:   true,
		ColumnNames:  explainColumnNames,
		AffectedRows: 0,
		Rows:         rows,
		Predicates:   predicates,
		Timestamp:    timestamp,
	}

	return result, nil
}

type ExplainAnalyzeDmlStatement struct {
	Dml string
}

func (s *ExplainAnalyzeDmlStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Dml)

	affectedRows, timestamp, queryPlan, err := runInNewOrExistRwTxForExplain(ctx, session, func() (int64, *pb.QueryPlan, error) {
		iter, _ := session.RunQueryWithStats(ctx, stmt)
		defer iter.Stop()
		err := iter.Do(func(r *spanner.Row) error { return nil })
		if err != nil {
			return 0, nil, err
		}
		return iter.RowCount, iter.QueryPlan, nil
	})
	if err != nil {
		return nil, err
	}

	rows, predicates, err := processPlanWithStats(queryPlan)
	if err != nil {
		return nil, err
	}
	result := &Result{
		IsMutation:   true,
		ColumnNames:  explainAnalyzeColumnNames,
		ForceVerbose: true,
		AffectedRows: int(affectedRows),
		Rows:         rows,
		Predicates:   predicates,
		Timestamp:    timestamp,
	}

	return result, nil
}

// runInNewOrExistRwTxForExplain is a helper function for ExplainDmlStatement and ExplainAnalyzeDmlStatement.
// It execute a function in the current RW transaction or an implicit RW transaction.
func runInNewOrExistRwTxForExplain(ctx context.Context, session *Session, f func() (affected int64, plan *pb.QueryPlan, err error)) (affected int64, ts time.Time, plan *pb.QueryPlan, err error) {
	if session.InReadWriteTransaction() {
		affected, plan, err := f()
		if err != nil {
			// Need to call rollback to free the acquired session in underlying google-cloud-go/spanner.
			rollback := &RollbackStatement{}
			rollback.Execute(ctx, session)
			return 0, time.Time{}, nil, fmt.Errorf("transaction was aborted: %v", err)
		}
		return affected, time.Time{}, plan, nil
	} else {
		// Start implicit transaction.
		begin := BeginRwStatement{}
		if _, err := begin.Execute(ctx, session); err != nil {
			return 0, time.Time{}, nil, err
		}

		affected, plan, err := f()
		if err != nil {
			// once error has happened, escape from implicit transaction
			rollback := &RollbackStatement{}
			rollback.Execute(ctx, session)
			return 0, time.Time{}, nil, err
		}

		commit := CommitStatement{}
		txnResult, err := commit.Execute(ctx, session)
		if err != nil {
			return 0, time.Time{}, nil, err
		}
		return affected, txnResult.Timestamp, plan, nil
	}
}

type BeginRwStatement struct {
	Priority pb.RequestOptions_Priority
	Tag      string
}

func newBeginRwStatement(input string) (*BeginRwStatement, error) {
	matched := beginRwRe.FindStringSubmatch(input)
	stmt := &BeginRwStatement{}

	if matched[1] != "" {
		priority, err := parsePriority(matched[1])
		if err != nil {
			return nil, err
		}
		stmt.Priority = priority
	}

	if matched[2] != "" {
		stmt.Tag = matched[2]
	}

	return stmt, nil
}

func (s *BeginRwStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if session.InReadWriteTransaction() {
		return nil, errors.New("you're in read-write transaction. Please finish the transaction by 'COMMIT;' or 'ROLLBACK;'")
	}
	if session.InReadOnlyTransaction() {
		return nil, errors.New("you're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}

	if err := session.BeginReadWriteTransaction(ctx, s.Priority, s.Tag); err != nil {
		return nil, err
	}

	return &Result{IsMutation: true}, nil
}

type CommitStatement struct{}

func (s *CommitStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	result := &Result{IsMutation: true}
	if session.InReadOnlyTransaction() {
		return nil, errors.New("you're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}
	if !session.InReadWriteTransaction() {
		return result, nil
	}

	resp, err := session.CommitReadWriteTransaction(ctx)
	if err != nil {
		return nil, err
	}

	result.Timestamp = resp.CommitTs
	result.CommitStats = resp.CommitStats
	return result, nil
}

type RollbackStatement struct{}

func (s *RollbackStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	result := &Result{IsMutation: true}
	if session.InReadOnlyTransaction() {
		return nil, errors.New("you're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}
	if !session.InReadWriteTransaction() {
		return result, nil
	}

	if err := session.RollbackReadWriteTransaction(ctx); err != nil {
		return nil, err
	}

	return result, nil
}

type timestampBoundType int

const (
	strong timestampBoundType = iota
	exactStaleness
	readTimestamp
)

type BeginRoStatement struct {
	TimestampBoundType timestampBoundType
	Staleness          time.Duration
	Timestamp          time.Time
	Priority           pb.RequestOptions_Priority
	Tag                string
}

func newBeginRoStatement(input string) (*BeginRoStatement, error) {
	stmt := &BeginRoStatement{
		TimestampBoundType: strong,
	}

	matched := beginRoRe.FindStringSubmatch(input)
	if matched[1] != "" {
		if t, err := time.Parse(time.RFC3339Nano, matched[1]); err == nil {
			stmt = &BeginRoStatement{
				TimestampBoundType: readTimestamp,
				Timestamp:          t,
			}
		}
		if i, err := strconv.Atoi(matched[1]); err == nil {
			stmt = &BeginRoStatement{
				TimestampBoundType: exactStaleness,
				Staleness:          time.Duration(i) * time.Second,
			}
		}
	}

	if matched[2] != "" {
		priority, err := parsePriority(matched[2])
		if err != nil {
			return nil, err
		}
		stmt.Priority = priority
	}

	if matched[3] != "" {
		stmt.Tag = matched[3]
	}

	return stmt, nil
}

func (s *BeginRoStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if session.InReadWriteTransaction() {
		return nil, errors.New("You're in read-write transaction. Please finish the transaction by 'COMMIT;' or 'ROLLBACK;'")
	}
	if session.InReadOnlyTransaction() {
		// close current transaction implicitly
		close := &CloseStatement{}
		close.Execute(ctx, session)
	}

	ts, err := session.BeginReadOnlyTransaction(ctx, s.TimestampBoundType, s.Staleness, s.Timestamp, s.Priority, s.Tag)
	if err != nil {
		return nil, err
	}

	return &Result{
		IsMutation: true,
		Timestamp:  ts,
	}, nil
}

type CloseStatement struct{}

func (s *CloseStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	if session.InReadWriteTransaction() {
		return nil, errors.New("You're in read-write transaction. Please finish the transaction by 'COMMIT;' or 'ROLLBACK;'")
	}

	result := &Result{IsMutation: true}

	if !session.InReadOnlyTransaction() {
		return result, nil
	}

	if err := session.CloseReadOnlyTransaction(); err != nil {
		return nil, err
	}

	return result, nil
}

type NopStatement struct{}

func (s *NopStatement) Execute(ctx context.Context, session *Session) (*Result, error) {
	// do nothing
	return &Result{}, nil
}

type ExitStatement struct {
	NopStatement
}

type UseStatement struct {
	Database string
	NopStatement
}

func parsePriority(priority string) (pb.RequestOptions_Priority, error) {
	switch strings.ToUpper(priority) {
	case "HIGH":
		return pb.RequestOptions_PRIORITY_HIGH, nil
	case "MEDIUM":
		return pb.RequestOptions_PRIORITY_MEDIUM, nil
	case "LOW":
		return pb.RequestOptions_PRIORITY_LOW, nil
	default:
		return pb.RequestOptions_PRIORITY_UNSPECIFIED, fmt.Errorf("invalid priority: %q", priority)
	}
}
