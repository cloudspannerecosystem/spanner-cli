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
	createDatabaseRe = regexp.MustCompile(`(?is)^CREATE\s+DATABASE\s+(.+)$`)
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
	beginRoRe  = regexp.MustCompile(`(?is)^BEGIN\s+RO$`)
	commitRe   = regexp.MustCompile(`(?is)^COMMIT$`)
	rollbackRe = regexp.MustCompile(`(?is)^ROLLBACK$`)
	closeRe    = regexp.MustCompile(`(?is)^CLOSE$`)

	// Other
	exitRe            = regexp.MustCompile(`(?is)^EXIT$`)
	useRe             = regexp.MustCompile(`(?is)^USE\s+(.+)$`)
	showDatabasesRe   = regexp.MustCompile(`(?is)^SHOW\s+DATABASES$`)
	showCreateTableRe = regexp.MustCompile(`(?is)^SHOW\s+CREATE\s+TABLE\s+(.+)$`)
	showTablesRe      = regexp.MustCompile(`(?is)^SHOW\s+TABLES$`)
	showColumnsRe     = regexp.MustCompile(`(?is)^(?:SHOW\s+COLUMNS|EXPLAIN|DESC(?:RIBE)?)\s+(.+)$`)
	showIndexRe       = regexp.MustCompile(`(?is)^SHOW\s+(?:INDEX|INDEXES|KEYS)\s+FROM\s+(.+)$`)
	explainRe         = regexp.MustCompile(`(?is)^(?:EXPLAIN|DESC(?:RIBE)?)\s+(SELECT\s+.+)$`)
)

var (
	txnRetryError = errors.New("transaction temporarily failed")
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
			text: input,
		}
	} else if createDatabaseRe.MatchString(input) {
		matched := createDatabaseRe.FindStringSubmatch(input)
		stmt = &CreateDatabaseStatement{
			text:     input,
			database: matched[1],
		}
	} else if createTableRe.MatchString(input) || alterTableRe.MatchString(input) || dropTableRe.MatchString(input) || createIndexRe.MatchString(input) || dropIndexRe.MatchString(input) {
		stmt = &DdlStatement{
			text: input,
		}
	} else if showDatabasesRe.MatchString(input) {
		stmt = &ShowDatabasesStatement{}
	} else if showCreateTableRe.MatchString(input) {
		matched := showCreateTableRe.FindStringSubmatch(input)
		stmt = &ShowCreateTableStatement{
			table: matched[1],
		}
	} else if showTablesRe.MatchString(input) {
		stmt = &ShowTablesStatement{}
	} else if explainRe.MatchString(input) {
		matched := explainRe.FindStringSubmatch(input)
		stmt = &ExplainStatement{
			text: matched[1],
		}
	} else if showColumnsRe.MatchString(input) {
		matched := showColumnsRe.FindStringSubmatch(input)
		stmt = &ShowColumnsStatement{
			table: matched[1],
		}
	} else if showIndexRe.MatchString(input) {
		matched := showIndexRe.FindStringSubmatch(input)
		stmt = &ShowIndexStatement{
			table: matched[1],
		}
	} else if insertRe.MatchString(input) || updateRe.MatchString(input) || deleteRe.MatchString(input) {
		stmt = &DmlStatement{
			text: input,
		}
	} else if beginRwRe.MatchString(input) {
		stmt = &BeginRwStatement{}
	} else if beginRoRe.MatchString(input) {
		stmt = &BeginRoStatement{}
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
	text string
}

func (s *SelectStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.text)
	var iter *spanner.RowIterator

	if session.inRwTxn() {
		iter = session.rwTxn.QueryWithStats(session.ctx, stmt)
	} else if session.inRoTxn() {
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
	text     string
	database string
}

func (s *CreateDatabaseStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
		fmt.Println(s.text)
		op, err := session.adminClient.CreateDatabase(session.ctx, &adminpb.CreateDatabaseRequest{
			Parent:          session.GetInstancePath(),
			CreateStatement: s.text,
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
	})
}

type DdlStatement struct {
	text string
}

func (s *DdlStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
		result := &Result{
			ColumnNames: make([]string, 0),
			Rows:        make([]Row, 0),
			IsMutation:  true,
		}

		op, err := session.adminClient.UpdateDatabaseDdl(session.ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   session.GetDatabasePath(),
			Statements: []string{s.text},
		})
		if err != nil {
			return nil, err
		}
		if err := op.Wait(session.ctx); err != nil {
			return nil, err
		}

		return result, nil
	})
}

type ShowDatabasesStatement struct {
}

func (s *ShowDatabasesStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
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
	})
}

type ShowCreateTableStatement struct {
	table string
}

func (s *ShowCreateTableStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
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
			if regexp.MustCompile(`(?i)^CREATE TABLE ` + s.table).MatchString(stmt) {
				resultRow := Row{
					Columns: []string{s.table, stmt},
				}
				result.Rows = append(result.Rows, resultRow)
				break
			}
		}
		if len(result.Rows) == 0 {
			return nil, errors.New(fmt.Sprintf("Table '%s' doesn't exist", s.table))
		}

		result.Stats.AffectedRows = len(result.Rows)

		return result, nil
	})
}

type ShowTablesStatement struct{}

func (s *ShowTablesStatement) Execute(session *Session) (*Result, error) {
	alias := fmt.Sprintf("Tables_in_%s", session.databaseId)
	query := SelectStatement{
		text: fmt.Sprintf(`SELECT t.table_name AS %s FROM information_schema.tables AS t WHERE t.table_catalog = '' and t.table_schema = ''`, alias),
	}

	result, err := query.Execute(session)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type ExplainStatement struct {
	text string
}

func (s *ExplainStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
		result := &Result{
			ColumnNames: []string{"Query_Execution_Plan (EXPERIMENTAL)"},
			Rows:        make([]Row, 1),
			IsMutation:  false,
			Stats: Stats{
				AffectedRows: 1,
			},
		}

		stmt := spanner.NewStatement(s.text)
		queryPlan, err := session.client.Single().AnalyzeQuery(session.ctx, stmt)
		if err != nil {
			return nil, err
		}

		tree := BuildQueryPlanTree(queryPlan, 0)
		rendered := tree.Render()
		result.Rows[0] = Row{[]string{rendered}}

		return result, nil
	})
}

type ShowColumnsStatement struct {
	table string
}

func (s *ShowColumnsStatement) Execute(session *Session) (*Result, error) {
	query := SelectStatement{
		text: fmt.Sprintf(`SELECT
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
  C.ORDINAL_POSITION ASC`, s.table),
	}

	result, err := query.Execute(session)
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 {
		return nil, errors.New(fmt.Sprintf("Table '%s' doesn't exist", s.table))
	}

	return result, nil
}

type ShowIndexStatement struct {
	table string
}

func (s *ShowIndexStatement) Execute(session *Session) (*Result, error) {
	return nil, nil
}

type DmlStatement struct {
	text string
}

func (s *DmlStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
		stmt := spanner.NewStatement(s.text)

		result := &Result{
			ColumnNames: make([]string, 0),
			Rows:        make([]Row, 0),
			IsMutation:  true,
		}

		var numRows int64
		var err error
		if session.inRwTxn() {
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
	})
}

type BeginRwStatement struct{}

func (s *BeginRwStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
		if session.inRwTxn() {
			// commit current transaction implicitly like MySQL
			// cf. https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html
			commit := CommitStatement{}
			_, err := commit.Execute(session)
			if err != nil {
				return nil, err
			}
		}

		contextChanged := make(chan bool)

		go func() {
			txnExecuted := false
			_, err := session.client.ReadWriteTransaction(session.ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				// ReadWriteTransaction might retry this function, but it's not supported in this tool.
				if txnExecuted {
					return txnRetryError
				}
				txnExecuted = true

				// switch session context to transaction context
				oldCtx := session.ctx
				session.ctx = ctx
				defer func() {
					session.ctx = oldCtx
				}()
				session.rwTxn = txn
				contextChanged <- true

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
		case <-contextChanged:
			// go
		case err := <-session.txnFinished:
			return nil, err
		}

		return &Result{
			ColumnNames: make([]string, 0),
			Rows:        make([]Row, 0),
			IsMutation:  true,
		}, nil
	})
}

type CommitStatement struct{}

func (s *CommitStatement) Execute(session *Session) (*Result, error) {
	if session.inRoTxn() {
		return nil, errors.New("You're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}

	return withElapsedTime(func() (*Result, error) {
		result := &Result{
			ColumnNames: make([]string, 0),
			Rows:        make([]Row, 0),
			IsMutation:  true,
		}

		if !session.inRwTxn() {
			return result, nil
		}

		defer session.finishRwTxn()
		session.committedChan <- true

		err := <-session.txnFinished
		if err != nil {
			return nil, err
		}

		return result, nil
	})
}

type RollbackStatement struct{}

func (s *RollbackStatement) Execute(session *Session) (*Result, error) {
	if session.inRoTxn() {
		return nil, errors.New("You're in read-only transaction. Please finish the transaction by 'CLOSE;'")
	}

	return withElapsedTime(func() (*Result, error) {
		result := &Result{
			ColumnNames: make([]string, 0),
			Rows:        make([]Row, 0),
			IsMutation:  true,
		}

		if !session.inRwTxn() {
			return result, nil
		}

		defer session.finishRwTxn()
		session.committedChan <- false

		err := <-session.txnFinished
		if err != nil && err != rollbackError {
			return nil, err
		}

		return result, nil
	})
}

type BeginRoStatement struct{}

func (s *BeginRoStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
		if session.inRoTxn() {
			// close current transaction implicitly
			close := &CloseStatement{}
			close.Execute(session)
		}

		txn := session.client.ReadOnlyTransaction()
		session.roTxn = txn

		return &Result{
			ColumnNames: make([]string, 0),
			Rows:        make([]Row, 0),
			IsMutation:  true,
		}, nil
	})
}

type CloseStatement struct{}

func (s *CloseStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
		result := &Result{
			ColumnNames: make([]string, 0),
			Rows:        make([]Row, 0),
			IsMutation:  true,
		}

		if !session.inRoTxn() {
			return result, nil
		}

		session.finishRoTxn()

		return result, nil
	})
}

type ExitStatement struct{}

func (s *ExitStatement) Execute(session *Session) (*Result, error) {
	// do nothing
	return &Result{}, nil
}

type UseStatement struct {
	Database string
}

func (s *UseStatement) Execute(session *Session) (*Result, error) {
	// do nothing
	return &Result{}, nil
}

func withElapsedTime(f func() (*Result, error)) (*Result, error) {
	t1 := time.Now()
	result, err := f()
	elapsed := time.Since(t1).Seconds()

	if err != nil {
		return nil, err
	}
	result.Stats.ElapsedTime = fmt.Sprintf("%0.2f sec", elapsed)
	return result, nil
}
