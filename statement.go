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
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	pb "google.golang.org/genproto/googleapis/spanner/v1"
)

type Statement interface {
	Execute(session *Session) (*Result, error)
}

type Result struct {
	ColumnNames  []string
	Rows         []Row
	Predicates   []string
	AffectedRows int
	Stats        QueryStats
	IsMutation   bool
	Timestamp    time.Time
	ForceVerbose bool
}

type Row struct {
	Columns []string
}

// QueryStats contains query statistics.
// Some fields may not have a valid value depending on the environment.
// For example, only ElapsedTime and RowsReturned has valid value for Cloud Spanner Emulator.
type QueryStats struct {
	ElapsedTime      string
	CPUTime          string
	RowsReturned     string
	RowsScanned      string
	OptimizerVersion string
}

var (
	// SQL
	selectRe = regexp.MustCompile(`(?is)^(?:WITH|@{.+|SELECT)\s.+$`)

	// DDL
	createDatabaseRe = regexp.MustCompile(`(?is)^CREATE\s+DATABASE\s.+$`)
	dropDatabaseRe   = regexp.MustCompile(`(?is)^DROP\s+DATABASE\s+(.+)$`)
	createTableRe    = regexp.MustCompile(`(?is)^CREATE\s+TABLE\s.+$`)
	alterTableRe     = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s.+$`)
	dropTableRe      = regexp.MustCompile(`(?is)^DROP\s+TABLE\s.+$`)
	createIndexRe    = regexp.MustCompile(`(?is)^CREATE\s+(UNIQUE\s+)?(NULL_FILTERED\s+)?INDEX\s.+$`)
	dropIndexRe      = regexp.MustCompile(`(?is)^DROP\s+INDEX\s.+$`)
	truncateTableRe  = regexp.MustCompile(`(?is)^TRUNCATE\s+TABLE\s+(.+)$`)

	// DML
	dmlRe = regexp.MustCompile(`(?is)^(INSERT|UPDATE|DELETE)\s+.+$`)

	// Transaction
	beginRwRe  = regexp.MustCompile(`(?is)^BEGIN(\s+RW)?$`)
	beginRoRe  = regexp.MustCompile(`(?is)^BEGIN\s+RO(?:\s+([^\s]+))?$`)
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
	explainColumnNames        = []string{"ID", "Query_Execution_Plan (EXPERIMENTAL)"}
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
	case dropDatabaseRe.MatchString(input):
		matched := dropDatabaseRe.FindStringSubmatch(input)
		return &DropDatabaseStatement{DatabaseId: unquoteIdentifier(matched[1])}, nil
	case createTableRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case alterTableRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case dropTableRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case createIndexRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case dropIndexRe.MatchString(input):
		return &DdlStatement{Ddl: input}, nil
	case truncateTableRe.MatchString(input):
		matched := truncateTableRe.FindStringSubmatch(input)
		return &TruncateTableStatement{Table: unquoteIdentifier(matched[1])}, nil
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
	case beginRwRe.MatchString(input):
		return &BeginRwStatement{}, nil
	case beginRoRe.MatchString(input):
		if s := newBeginRoStatement(input); s != nil {
			return s, nil
		}
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

func (s *SelectStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Query)
	var iter *spanner.RowIterator

	var targetRoTxn *spanner.ReadOnlyTransaction
	if session.InRwTxn() {
		iter = session.rwTxn.QueryWithStats(session.ctx, stmt)
	} else if session.InRoTxn() {
		targetRoTxn = session.roTxn
		iter = targetRoTxn.QueryWithStats(session.ctx, stmt)
	} else {
		targetRoTxn = session.client.Single()
		iter = targetRoTxn.QueryWithStats(session.ctx, stmt)
	}
	defer iter.Stop()

	rows, columnNames, err := parseQueryResult(iter)
	if err != nil {
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
	if targetRoTxn != nil {
		result.Timestamp, _ = targetRoTxn.Timestamp()
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

	return queryStats
}

type CreateDatabaseStatement struct {
	CreateStatement string
}

func (s *CreateDatabaseStatement) Execute(session *Session) (*Result, error) {
	op, err := session.adminClient.CreateDatabase(session.ctx, &adminpb.CreateDatabaseRequest{
		Parent:          session.InstancePath(),
		CreateStatement: s.CreateStatement,
	})
	if err != nil {
		return nil, err
	}
	if _, err := op.Wait(session.ctx); err != nil {
		return nil, err
	}

	return &Result{IsMutation: true}, nil
}

type DropDatabaseStatement struct {
	DatabaseId string
}

func (s *DropDatabaseStatement) Execute(session *Session) (*Result, error) {
	if err := session.adminClient.DropDatabase(session.ctx, &adminpb.DropDatabaseRequest{
		Database: fmt.Sprintf("projects/%s/instances/%s/databases/%s", session.projectId, session.instanceId, s.DatabaseId),
	}); err != nil {
		return nil, err
	}

	return &Result{IsMutation: true}, nil
}

type DdlStatement struct {
	Ddl string
}

func (s *DdlStatement) Execute(session *Session) (*Result, error) {
	return executeDdlStatements(session, []string{s.Ddl})
}

type BulkDdlStatement struct {
	Ddls []string
}

func (s *BulkDdlStatement) Execute(session *Session) (*Result, error) {
	return executeDdlStatements(session, s.Ddls)
}

func executeDdlStatements(session *Session, ddls []string) (*Result, error) {
	op, err := session.adminClient.UpdateDatabaseDdl(session.ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   session.DatabasePath(),
		Statements: ddls,
	})
	if err != nil {
		return nil, err
	}
	if err := op.Wait(session.ctx); err != nil {
		return nil, err
	}

	return &Result{IsMutation: true}, nil
}

type ShowDatabasesStatement struct {
}

func (s *ShowDatabasesStatement) Execute(session *Session) (*Result, error) {
	result := &Result{ColumnNames: []string{"Database"}}

	dbIter := session.adminClient.ListDatabases(session.ctx, &adminpb.ListDatabasesRequest{
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

func (s *ShowCreateTableStatement) Execute(session *Session) (*Result, error) {
	result := &Result{ColumnNames: []string{"Table", "Create Table"}}

	ddlResponse, err := session.adminClient.GetDatabaseDdl(session.ctx, &adminpb.GetDatabaseDdlRequest{
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

func (s *ShowTablesStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
		// INFORMATION_SCHEMA can not be used in read-write transaction.
		// https://cloud.google.com/spanner/docs/information-schema
		return nil, errors.New(`"SHOW TABLES" can not be used in a read-write transaction`)
	}

	alias := fmt.Sprintf("Tables_in_%s", session.databaseId)
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT t.TABLE_NAME AS `%s` FROM INFORMATION_SCHEMA.TABLES AS t WHERE t.TABLE_CATALOG = '' and t.TABLE_SCHEMA = ''", alias))

	var txn *spanner.ReadOnlyTransaction
	if session.InRoTxn() {
		txn = session.roTxn
	} else {
		txn = session.client.Single()
	}
	iter := txn.Query(session.ctx, stmt)
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

func (s *ExplainStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Explain)
	queryPlan, err := session.client.Single().AnalyzeQuery(session.ctx, stmt)
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

func (s *ExplainAnalyzeStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Query)
	var iter *spanner.RowIterator

	var targetRoTxn *spanner.ReadOnlyTransaction
	if session.InRwTxn() {
		iter = session.rwTxn.QueryWithStats(session.ctx, stmt)
	} else if session.InRoTxn() {
		targetRoTxn = session.roTxn
		iter = targetRoTxn.QueryWithStats(session.ctx, stmt)
	} else {
		targetRoTxn = session.client.Single()
		iter = targetRoTxn.QueryWithStats(session.ctx, stmt)
	}
	defer iter.Stop()
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
	if targetRoTxn != nil {
		timestamp, _ = targetRoTxn.Timestamp()
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

func (s *ShowColumnsStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
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

	var txn *spanner.ReadOnlyTransaction
	if session.InRoTxn() {
		txn = session.roTxn
	} else {
		txn = session.client.Single()
	}
	iter := txn.Query(session.ctx, stmt)
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

func (s *ShowIndexStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
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

	var txn *spanner.ReadOnlyTransaction
	if session.InRoTxn() {
		txn = session.roTxn
	} else {
		txn = session.client.Single()
	}

	iter := txn.Query(session.ctx, stmt)
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

func (s *TruncateTableStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
		// PartitionedUpdate creates a new transaction and it could cause dead lock with the current running transaction.
		return nil, errors.New(`"TRUNCATE TABLE" can not be used in a read-write transaction`)
	}
	if session.InRoTxn() {
		// Just for user-friendly.
		return nil, errors.New(`"TRUNCATE TABLE" can not be used in a read-only transaction`)
	}

	stmt := spanner.NewStatement(fmt.Sprintf("DELETE FROM `%s` WHERE true", s.Table))
	count, err := session.client.PartitionedUpdate(session.ctx, stmt)
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

func (s *DmlStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Dml)

	result := &Result{IsMutation: true}

	var numRows int64
	var err error
	if session.InRwTxn() {
		numRows, err = session.rwTxn.Update(session.ctx, stmt)
		if err != nil {
			// Need to call rollback to free the acquired session in underlying google-cloud-go/spanner.
			rollback := &RollbackStatement{}
			rollback.Execute(session)
			return nil, fmt.Errorf("transaction was aborted: %v", err)
		}
	} else {
		// Start implicit transaction.
		begin := BeginRwStatement{}
		if _, err = begin.Execute(session); err != nil {
			return nil, err
		}

		numRows, err = session.rwTxn.Update(session.ctx, stmt)
		if err != nil {
			// once error has happened, escape from implicit transaction
			rollback := &RollbackStatement{}
			rollback.Execute(session)
			return nil, err
		}

		commit := CommitStatement{}
		txnResult, err := commit.Execute(session)
		if err != nil {
			return nil, err
		}
		result.Timestamp = txnResult.Timestamp
	}

	result.AffectedRows = int(numRows)

	return result, nil
}

type ExplainDmlStatement struct {
	Dml string
}

func (s *ExplainDmlStatement) Execute(session *Session) (*Result, error) {
	_, timestamp, queryPlan, err := runInNewOrExistRwTxForExplain(session, func() (int64, *pb.QueryPlan, error) {
		plan, err := session.rwTxn.AnalyzeQuery(session.ctx, spanner.NewStatement(s.Dml))
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

func (s *ExplainAnalyzeDmlStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Dml)

	affectedRows, timestamp, queryPlan, err := runInNewOrExistRwTxForExplain(session, func() (int64, *pb.QueryPlan, error) {
		iter := session.rwTxn.QueryWithStats(session.ctx, stmt)
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
func runInNewOrExistRwTxForExplain(session *Session, f func() (affected int64, plan *pb.QueryPlan, err error)) (affected int64, ts time.Time, plan *pb.QueryPlan, err error) {
	if session.InRwTxn() {
		affected, plan, err := f()
		if err != nil {
			// Need to call rollback to free the acquired session in underlying google-cloud-go/spanner.
			rollback := &RollbackStatement{}
			rollback.Execute(session)
			return 0, time.Time{}, nil, fmt.Errorf("transaction was aborted: %v", err)
		}
		return affected, time.Time{}, plan, nil
	} else {
		// Start implicit transaction.
		begin := BeginRwStatement{}
		if _, err := begin.Execute(session); err != nil {
			return 0, time.Time{}, nil, err
		}

		affected, plan, err := f()
		if err != nil {
			// once error has happened, escape from implicit transaction
			rollback := &RollbackStatement{}
			rollback.Execute(session)
			return 0, time.Time{}, nil, err
		}

		commit := CommitStatement{}
		txnResult, err := commit.Execute(session)
		if err != nil {
			return 0, time.Time{}, nil, err
		}
		return affected, txnResult.Timestamp, plan, nil
	}
}

type BeginRwStatement struct{}

func (s *BeginRwStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
		return nil, errors.New("you're in read-write transaction. Please finish the transaction by 'COMMIT;' or 'ROLLBACK;'")
	}
	if session.InRoTxn() {
		return nil, errors.New("you're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}

	txn, err := spanner.NewReadWriteStmtBasedTransaction(session.ctx, session.client)
	if err != nil {
		return nil, err
	}
	session.StartRwTxn(txn)
	session.StartHeartbeat()

	return &Result{IsMutation: true}, nil
}

type CommitStatement struct{}

func (s *CommitStatement) Execute(session *Session) (*Result, error) {
	result := &Result{IsMutation: true}
	if session.InRoTxn() {
		return nil, errors.New("you're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}
	if !session.InRwTxn() {
		return result, nil
	}

	// Stop heartbeat before calling commit
	// because once commit has finished there is no guarantee that transaction object can be used.
	session.StopHeartbeat()

	ts, err := session.rwTxn.Commit(session.ctx)
	session.FinishRwTxn()
	if err != nil {
		return nil, err
	}
	result.Timestamp = ts

	return result, nil
}

type RollbackStatement struct{}

func (s *RollbackStatement) Execute(session *Session) (*Result, error) {
	result := &Result{IsMutation: true}
	if session.InRoTxn() {
		return nil, errors.New("you're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}
	if !session.InRwTxn() {
		return result, nil
	}

	// Stop heartbeat before calling rollback
	// because once rollback has finished there is no guarantee that transaction object can be used.
	session.StopHeartbeat()

	session.rwTxn.Rollback(session.ctx)
	session.FinishRwTxn()

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
}

func newBeginRoStatement(input string) *BeginRoStatement {
	matched := beginRoRe.FindStringSubmatch(input)
	if matched[1] == "" {
		return &BeginRoStatement{
			TimestampBoundType: strong,
		}
	}
	if t, err := time.Parse(time.RFC3339Nano, matched[1]); err == nil {
		return &BeginRoStatement{
			TimestampBoundType: readTimestamp,
			Timestamp:          t,
		}
	}
	if i, err := strconv.Atoi(matched[1]); err == nil {
		return &BeginRoStatement{
			TimestampBoundType: exactStaleness,
			Staleness:          time.Duration(time.Duration(i) * time.Second),
		}
	}
	return nil
}

func (s *BeginRoStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
		return nil, errors.New("You're in read-write transaction. Please finish the transaction by 'COMMIT;' or 'ROLLBACK;'")
	}
	if session.InRoTxn() {
		// close current transaction implicitly
		close := &CloseStatement{}
		close.Execute(session)
	}

	txn := session.client.ReadOnlyTransaction()
	switch s.TimestampBoundType {
	case strong:
		txn = txn.WithTimestampBound(spanner.StrongRead())
	case exactStaleness:
		txn = txn.WithTimestampBound(spanner.ExactStaleness(s.Staleness))
	case readTimestamp:
		txn = txn.WithTimestampBound(spanner.ReadTimestamp(s.Timestamp))
	}
	session.StartRoTxn(txn)

	return &Result{
		IsMutation: true,
		Timestamp:  determineReadTimestamp(session.ctx, txn),
	}, nil
}

// determineReadTimestamp ensures BeginTransaction RPC is called
// and returns the read timestamp of the read only transaction
func determineReadTimestamp(ctx context.Context, txn *spanner.ReadOnlyTransaction) time.Time {
	iter := txn.Query(ctx, spanner.NewStatement("SELECT 1"))
	defer iter.Stop()
	ts, _ := txn.Timestamp()
	return ts
}

type CloseStatement struct{}

func (s *CloseStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
		return nil, errors.New("You're in read-write transaction. Please finish the transaction by 'COMMIT;' or 'ROLLBACK;'")
	}

	result := &Result{IsMutation: true}

	if !session.InRoTxn() {
		return result, nil
	}

	session.FinishRoTxn()

	return result, nil
}

type NopStatement struct{}

func (s *NopStatement) Execute(session *Session) (*Result, error) {
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
