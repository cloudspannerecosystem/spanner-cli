package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type Statement interface {
	Execute(session *Session) (*Result, error)
}

type Result struct {
	ColumnNames []string
	Rows        []Row
	Stats       Stats
	IsMutation  bool
}

type Row struct {
	Columns []string
}

type Stats struct {
	AffectedRows int
	ElapsedTime  string
}

var (
	// SQL
	selectRe = regexp.MustCompile(`(?is)^SELECT\s.+$`)

	// DDL
	createDatabaseRe = regexp.MustCompile(`(?is)^CREATE\s+DATABASE\s.+$`)
	createTableRe    = regexp.MustCompile(`(?is)^CREATE\s+TABLE\s.+$`)
	alterTableRe     = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s.+$`)
	dropTableRe      = regexp.MustCompile(`(?is)^DROP\s+TABLE\s.+$`)
	createIndexRe    = regexp.MustCompile(`(?is)^CREATE\s+(UNIQUE\s+)?(NULL_FILTERED\s+)?INDEX\s.+$`)
	dropIndexRe      = regexp.MustCompile(`(?is)^DROP\s+INDEX\s.+$`)

	// DML
	insertRe = regexp.MustCompile(`(?is)^INSERT\s+.+$`)
	updateRe = regexp.MustCompile(`(?is)^UPDATE\s+.+$`)
	deleteRe = regexp.MustCompile(`(?is)^DELETE\s+.+$`)

	// Transaction
	beginRwRe  = regexp.MustCompile(`(?is)^BEGIN(\s+RW)?$`)
	beginRoRe  = regexp.MustCompile(`(?is)^BEGIN\s+RO(?:\s+(\d+))?$`)
	commitRe   = regexp.MustCompile(`(?is)^COMMIT$`)
	rollbackRe = regexp.MustCompile(`(?is)^ROLLBACK$`)
	closeRe    = regexp.MustCompile(`(?is)^CLOSE$`)

	// Other
	exitRe            = regexp.MustCompile(`(?is)^EXIT$`)
	useRe             = regexp.MustCompile(`(?is)^USE\s+(.+)$`)
	showDatabasesRe   = regexp.MustCompile(`(?is)^SHOW\s+DATABASES$`)
	showCreateTableRe = regexp.MustCompile(`(?is)^SHOW\s+CREATE\s+TABLE\s+(.+)$`)
	showTablesRe      = regexp.MustCompile(`(?is)^SHOW\s+TABLES$`)
	showColumnsRe     = regexp.MustCompile(`(?is)^(?:SHOW\s+COLUMNS\s+FROM|EXPLAIN|DESC(?:RIBE)?)\s+(.+)$`)
	showIndexRe       = regexp.MustCompile(`(?is)^SHOW\s+(?:INDEX|INDEXES|KEYS)\s+FROM\s+(.+)$`)
	explainRe         = regexp.MustCompile(`(?is)^(?:EXPLAIN|DESC(?:RIBE)?)\s+(SELECT\s+.+)$`)
)

var (
	txnRetryError = errors.New("transaction was aborted")
	rollbackError = errors.New("rollback")
)

func BuildStatement(input string) (Statement, error) {
	var stmt Statement

	if exitRe.MatchString(input) {
		stmt = &ExitStatement{}
	} else if useRe.MatchString(input) {
		matched := useRe.FindStringSubmatch(input)
		stmt = &UseStatement{
			Database: matched[1],
		}
	} else if selectRe.MatchString(input) {
		stmt = &SelectStatement{
			Query: input,
		}
	} else if createDatabaseRe.MatchString(input) {
		stmt = &CreateDatabaseStatement{
			CreateStatement: input,
		}
	} else if createTableRe.MatchString(input) || alterTableRe.MatchString(input) || dropTableRe.MatchString(input) || createIndexRe.MatchString(input) || dropIndexRe.MatchString(input) {
		stmt = &DdlStatement{
			Ddl: input,
		}
	} else if showDatabasesRe.MatchString(input) {
		stmt = &ShowDatabasesStatement{}
	} else if showCreateTableRe.MatchString(input) {
		matched := showCreateTableRe.FindStringSubmatch(input)
		stmt = &ShowCreateTableStatement{
			Table: matched[1],
		}
	} else if showTablesRe.MatchString(input) {
		stmt = &ShowTablesStatement{}
	} else if explainRe.MatchString(input) {
		matched := explainRe.FindStringSubmatch(input)
		stmt = &ExplainStatement{
			Explain: matched[1],
		}
	} else if showColumnsRe.MatchString(input) {
		matched := showColumnsRe.FindStringSubmatch(input)
		stmt = &ShowColumnsStatement{
			Table: matched[1],
		}
	} else if showIndexRe.MatchString(input) {
		matched := showIndexRe.FindStringSubmatch(input)
		stmt = &ShowIndexStatement{
			Table: matched[1],
		}
	} else if insertRe.MatchString(input) || updateRe.MatchString(input) || deleteRe.MatchString(input) {
		stmt = &DmlStatement{
			Dml: input,
		}
	} else if beginRwRe.MatchString(input) {
		stmt = &BeginRwStatement{}
	} else if beginRoRe.MatchString(input) {
		matched := beginRoRe.FindStringSubmatch(input)
		if matched[1] == "" {
			stmt = &BeginRoStatement{}
		} else {
			i, err := strconv.Atoi(matched[1])
			if err == nil {
				stmt = &BeginRoStatement{
					Staleness: time.Duration(time.Duration(i) * time.Second),
				}
			}
		}
	} else if commitRe.MatchString(input) {
		stmt = &CommitStatement{}
	} else if rollbackRe.MatchString(input) {
		stmt = &RollbackStatement{}
	} else if closeRe.MatchString(input) {
		stmt = &CloseStatement{}
	}

	if stmt == nil {
		return nil, errors.New("invalid statement")
	}

	return stmt, nil
}

type SelectStatement struct {
	Query string
}

func (s *SelectStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Query)
	var iter *spanner.RowIterator

	if session.InRwTxn() {
		iter = session.rwTxn.QueryWithStats(session.ctx, stmt)
	} else if session.InRoTxn() {
		iter = session.roTxn.QueryWithStats(session.ctx, stmt)
	} else {
		iter = session.client.Single().QueryWithStats(session.ctx, stmt)
	}

	result := &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  false,
	}

	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(result.ColumnNames) == 0 {
			result.ColumnNames = row.ColumnNames()
		}

		columns, err := DecodeRow(row)
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, Row{
			Columns: columns,
		})
	}

	rowsReturned, _ := strconv.Atoi(iter.QueryStats["rows_returned"].(string))
	elapsedTime := iter.QueryStats["elapsed_time"].(string)
	result.Stats = Stats{
		AffectedRows: rowsReturned,
		ElapsedTime:  elapsedTime,
	}

	return result, nil
}

type CreateDatabaseStatement struct {
	CreateStatement string
}

func (s *CreateDatabaseStatement) Execute(session *Session) (*Result, error) {
	op, err := session.adminClient.CreateDatabase(session.ctx, &adminpb.CreateDatabaseRequest{
		Parent:          session.GetInstancePath(),
		CreateStatement: s.CreateStatement,
	})
	if err != nil {
		return nil, err
	}
	if _, err := op.Wait(session.ctx); err != nil {
		return nil, err
	}

	return &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}, nil
}

type DdlStatement struct {
	Ddl string
}

func (s *DdlStatement) Execute(session *Session) (*Result, error) {
	op, err := session.adminClient.UpdateDatabaseDdl(session.ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   session.GetDatabasePath(),
		Statements: []string{s.Ddl},
	})
	if err != nil {
		return nil, err
	}
	if err := op.Wait(session.ctx); err != nil {
		return nil, err
	}

	return &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}, nil
}

type ShowDatabasesStatement struct {
}

func (s *ShowDatabasesStatement) Execute(session *Session) (*Result, error) {
	result := &Result{
		ColumnNames: []string{"Database"},
		Rows:        make([]Row, 0),
		IsMutation:  false,
	}

	dbIter := session.adminClient.ListDatabases(session.ctx, &adminpb.ListDatabasesRequest{
		Parent: session.GetInstancePath(),
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

	result.Stats.AffectedRows = len(result.Rows)

	return result, nil
}

type ShowCreateTableStatement struct {
	Table string
}

func (s *ShowCreateTableStatement) Execute(session *Session) (*Result, error) {
	result := &Result{
		ColumnNames: []string{"Table", "Create Table"},
		Rows:        make([]Row, 0),
		IsMutation:  false,
	}

	ddlResponse, err := session.adminClient.GetDatabaseDdl(session.ctx, &adminpb.GetDatabaseDdlRequest{
		Database: session.GetDatabasePath(),
	})
	if err != nil {
		return nil, err
	}
	for _, stmt := range ddlResponse.Statements {
		if regexp.MustCompile(`(?i)^CREATE TABLE ` + s.Table).MatchString(stmt) {
			resultRow := Row{
				Columns: []string{s.Table, stmt},
			}
			result.Rows = append(result.Rows, resultRow)
			break
		}
	}
	if len(result.Rows) == 0 {
		return nil, errors.New(fmt.Sprintf("Table '%s' doesn't exist", s.Table))
	}

	result.Stats.AffectedRows = len(result.Rows)

	return result, nil
}

type ShowTablesStatement struct{}

func (s *ShowTablesStatement) Execute(session *Session) (*Result, error) {
	alias := fmt.Sprintf("Tables_in_%s", session.databaseId)
	query := SelectStatement{
		Query: fmt.Sprintf(`SELECT t.table_name AS %s FROM information_schema.tables AS t WHERE t.table_catalog = '' and t.table_schema = ''`, alias),
	}

	result, err := query.Execute(session)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type ExplainStatement struct {
	Explain string
}

func (s *ExplainStatement) Execute(session *Session) (*Result, error) {
	result := &Result{
		ColumnNames: []string{"Query_Execution_Plan (EXPERIMENTAL)"},
		Rows:        make([]Row, 1),
		IsMutation:  false,
		Stats: Stats{
			AffectedRows: 1,
		},
	}

	stmt := spanner.NewStatement(s.Explain)
	queryPlan, err := session.client.Single().AnalyzeQuery(session.ctx, stmt)
	if err != nil {
		return nil, err
	}

	tree := BuildQueryPlanTree(queryPlan, 0)
	rendered := tree.Render()
	result.Rows[0] = Row{[]string{rendered}}

	return result, nil
}

type ShowColumnsStatement struct {
	Table string
}

func (s *ShowColumnsStatement) Execute(session *Session) (*Result, error) {
	query := SelectStatement{
		Query: fmt.Sprintf(`SELECT
  C.COLUMN_NAME as Field,
  C.SPANNER_TYPE as Type,
  C.IS_NULLABLE as `+"`NULL`"+`,
  C.COLUMN_DEFAULT as `+"`Default`"+`,
  IC.INDEX_TYPE as `+"`Key`"+`,
  IC.COLUMN_ORDERING as Key_Order,
  CONCAT(CO.OPTION_NAME, "=", CO.OPTION_VALUE) as `+"`Options`"+`
FROM
  INFORMATION_SCHEMA.COLUMNS C
LEFT JOIN
  INFORMATION_SCHEMA.INDEX_COLUMNS IC
ON
  C.COLUMN_NAME = IC.COLUMN_NAME AND C.TABLE_NAME = IC.TABLE_NAME
LEFT JOIN
  INFORMATION_SCHEMA.COLUMN_OPTIONS CO
ON
  C.COLUMN_NAME = CO.COLUMN_NAME AND C.TABLE_NAME = CO.TABLE_NAME
WHERE
  C.TABLE_NAME = '%s'
ORDER BY
  C.ORDINAL_POSITION ASC`, s.Table),
	}

	result, err := query.Execute(session)
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 {
		return nil, errors.New(fmt.Sprintf("Table '%s' doesn't exist", s.Table))
	}

	return result, nil
}

type ShowIndexStatement struct {
	Table string
}

func (s *ShowIndexStatement) Execute(session *Session) (*Result, error) {
	query := SelectStatement{
		Query: fmt.Sprintf(`SELECT
  TABLE_NAME as Table,
  PARENT_TABLE_NAME as Parent_table,
  INDEX_NAME as Index_name,
  INDEX_TYPE as Index_type,
  IS_UNIQUE as Is_unique,
  IS_NULL_FILTERED as Is_null_filtered,
  INDEX_STATE as Index_state
FROM
  INFORMATION_SCHEMA.INDEXES
WHERE
  TABLE_NAME = '%s'`, s.Table),
	}

	result, err := query.Execute(session)
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 {
		return nil, errors.New(fmt.Sprintf("Table '%s' doesn't exist", s.Table))
	}

	return result, nil
}

type DmlStatement struct {
	Dml string
}

func (s *DmlStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.Dml)

	result := &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}

	var numRows int64
	var err error
	if session.InRwTxn() {
		numRows, err = session.rwTxn.Update(session.ctx, stmt)
		if err != nil {
			// abort transaction
			rollback := &RollbackStatement{}
			rollback.Execute(session)
			return nil, fmt.Errorf("error has happend during update, so abort transaction: %s", err)
		}
	} else {
		// start implicit transaction
		begin := BeginRwStatement{}
		_, err = begin.Execute(session)
		if err != nil {
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
		_, err = commit.Execute(session)
		if err != nil {
			return nil, err
		}
	}

	result.Stats.AffectedRows = int(numRows)

	return result, nil
}

type BeginRwStatement struct{}

func (s *BeginRwStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
		return nil, errors.New("You're in read-write transaction. Please finish the transaction by 'COMMIT;' or 'ROLLBACK;'")
	}
	if session.InRoTxn() {
		return nil, errors.New("You're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}

	txnStarted := make(chan bool)

	go func() {
		txnExecuted := false
		_, err := session.client.ReadWriteTransaction(session.ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			// ReadWriteTransaction might retry this function, but retry is not allowed in this tool.
			if txnExecuted {
				return txnRetryError
			}
			txnExecuted = true

			finish := session.StartRwTxn(ctx, txn)
			defer finish()

			txnStarted <- true

			// wait for mutations...
			isCommitted := <-session.committedChan

			if isCommitted {
				return nil
			} else {
				return rollbackError
			}
		})
		session.txnFinished <- err
	}()

	select {
	case <-txnStarted:
		// go
	case err := <-session.txnFinished:
		// error happened before starting transaction
		return nil, err
	}

	return &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}, nil
}

type CommitStatement struct{}

func (s *CommitStatement) Execute(session *Session) (*Result, error) {
	if session.InRoTxn() {
		return nil, errors.New("You're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}

	result := &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}

	if !session.InRwTxn() {
		return result, nil
	}

	session.committedChan <- true

	err := <-session.txnFinished
	if err != nil {
		return nil, err
	}

	return result, nil
}

type RollbackStatement struct{}

func (s *RollbackStatement) Execute(session *Session) (*Result, error) {
	if session.InRoTxn() {
		return nil, errors.New("You're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}

	result := &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}

	if !session.InRwTxn() {
		return result, nil
	}

	session.committedChan <- false

	err := <-session.txnFinished
	if err != nil && err != rollbackError {
		return nil, err
	}

	return result, nil
}

type BeginRoStatement struct {
	Staleness time.Duration
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
	if s.Staleness != time.Duration(0) {
		txn = txn.WithTimestampBound(spanner.ExactStaleness(s.Staleness))
	}
	session.StartRoTxn(txn)

	return &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}, nil
}

type CloseStatement struct{}

func (s *CloseStatement) Execute(session *Session) (*Result, error) {
	if session.InRwTxn() {
		return nil, errors.New("You're in read-write transaction. Please finish the transaction by 'COMMIT;' or 'ROLLBACK;'")
	}

	result := &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}

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
