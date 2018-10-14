package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
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
	selectRe = regexp.MustCompile(`(?i)^SELECT\s.+$`)

	// DDL
	createDatabaseRe = regexp.MustCompile(`(?i)^CREATE\s+DATABASE\s+(.+)$`)
	createTableRe    = regexp.MustCompile(`(?i)^CREATE\s+TABLE\s.+$`)
	alterTableRe     = regexp.MustCompile(`(?i)^ALTER\s+TABLE\s.+$`)
	dropTableRe      = regexp.MustCompile(`(?i)^DROP\s+TABLE\s.+$`)
	createIndexRe    = regexp.MustCompile(`(?i)^CREATE\s+(UNIQUE\s+)?(NULL_FILTERED\s+)?INDEX\s.+$`)
	dropIndexRe      = regexp.MustCompile(`(?i)^DROP\s+INDEX\s.+$`)

	// DML
	insertRe = regexp.MustCompile(`(?i)^INSERT\s+.+$`)
	updateRe = regexp.MustCompile(`(?i)^UPDATE\s+.+$`)
	deleteRe = regexp.MustCompile(`(?i)^DELETE\s+.+$`)

	// Transaction
	beginRwRe  = regexp.MustCompile(`(?i)^BEGIN(\s+RW)?$`)
	beginRoRe  = regexp.MustCompile(`(?i)^BEGIN\s+RO$`)
	commitRe   = regexp.MustCompile(`(?i)^COMMIT$`)
	rollbackRe = regexp.MustCompile(`(?i)^ROLLBACK$`)
	closeRe    = regexp.MustCompile(`(?i)^CLOSE$`)

	// Other
	exitRe            = regexp.MustCompile(`(?i)^EXIT$`)
	useRe             = regexp.MustCompile(`(?i)^USE\s+(.+)$`)
	showDatabasesRe   = regexp.MustCompile(`(?i)^SHOW\s+DATABASES$`)
	showCreateTableRe = regexp.MustCompile(`(?i)^SHOW\s+CREATE\s+TABLE\s+(.+)$`)
	showTablesRe      = regexp.MustCompile(`(?i)^SHOW\s+TABLES$`)
)

var (
	txnRetryError = errors.New("transaction retried, but it's not supported.")
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

		resultRow := Row{
			Columns: make([]string, row.Size()),
		}

		result.ColumnNames = row.ColumnNames() // TODO

		for i := 0; i < row.Size(); i++ {
			var column spanner.GenericColumnValue
			err := row.Column(i, &column)
			if err != nil {
				return nil, err
			}
			decoded, err := decodeColumn(column)
			if err != nil {
				return nil, err
			}
			resultRow.Columns[i] = decoded
		}

		result.Rows = append(result.Rows, resultRow)
	}

	rowsReturned, _ := strconv.Atoi(iter.QueryStats["rows_returned"].(string))
	elapsedTime := iter.QueryStats["elapsed_time"].(string)
	result.Stats = Stats{
		AffectedRows: rowsReturned,
		ElapsedTime:  elapsedTime,
	}

	return result, nil
}

func decodeColumn(column spanner.GenericColumnValue) (string, error) {
	//Allowable types: https://cloud.google.com/spanner/docs/data-types#allowable-types
	switch column.Type.Code {
	case sppb.TypeCode_ARRAY:
		var decoded []string
		switch column.Type.GetArrayElementType().Code {
		case sppb.TypeCode_BOOL:
			var vs []spanner.NullBool
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			for _, v := range vs {
				decoded = append(decoded, nullBoolToString(v))
			}
		case sppb.TypeCode_BYTES:
			var vs [][]byte
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			for _, v := range vs {
				decoded = append(decoded, nullBytesToString(v))
			}
		case sppb.TypeCode_FLOAT64:
			var vs []spanner.NullFloat64
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			for _, v := range vs {
				decoded = append(decoded, nullFloat64ToString(v))
			}
		case sppb.TypeCode_INT64:
			var vs []spanner.NullInt64
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			for _, v := range vs {
				decoded = append(decoded, nullInt64ToString(v))
			}
		case sppb.TypeCode_STRING:
			var vs []spanner.NullString
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			for _, v := range vs {
				decoded = append(decoded, nullStringToString(v))
			}
		case sppb.TypeCode_TIMESTAMP:
			var vs []spanner.NullTime
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			for _, v := range vs {
				decoded = append(decoded, nullTimeToString(v))
			}
		case sppb.TypeCode_DATE:
			var vs []spanner.NullDate
			if err := column.Decode(&vs); err != nil {
				return "", err
			}
			for _, v := range vs {
				decoded = append(decoded, nullDateToString(v))
			}
		case sppb.TypeCode_STRUCT:
			// TODO
			return "", errors.New("STRUCT data type is not supported yet.")
		}
		return fmt.Sprintf("[%s]", strings.Join(decoded, ", ")), nil
	case sppb.TypeCode_BOOL:
		var v spanner.NullBool
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullBoolToString(v), nil
	case sppb.TypeCode_BYTES:
		var v []byte
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullBytesToString(v), nil
	case sppb.TypeCode_FLOAT64:
		var v spanner.NullFloat64
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullFloat64ToString(v), nil
	case sppb.TypeCode_INT64:
		var v spanner.NullInt64
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullInt64ToString(v), nil
	case sppb.TypeCode_STRING:
		var v spanner.NullString
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullStringToString(v), nil
	case sppb.TypeCode_TIMESTAMP:
		var v spanner.NullTime
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullTimeToString(v), nil
	case sppb.TypeCode_DATE:
		var v spanner.NullDate
		if err := column.Decode(&v); err != nil {
			return "", err
		}
		return nullDateToString(v), nil
	default:
		return fmt.Sprintf("%s", column.Value), nil
	}
}

func nullBoolToString(v spanner.NullBool) string {
	if v.Valid {
		return fmt.Sprintf("%t", v.Bool)
	} else {
		return "NULL"
	}
}

func nullBytesToString(v []byte) string {
	if v != nil {
		return base64.RawStdEncoding.EncodeToString(v)
	} else {
		return "NULL"
	}
}

func nullFloat64ToString(v spanner.NullFloat64) string {
	if v.Valid {
		return fmt.Sprintf("%f", v.Float64)
	} else {
		return "NULL"
	}
}

func nullInt64ToString(v spanner.NullInt64) string {
	if v.Valid {
		return fmt.Sprintf("%d", v.Int64)
	} else {
		return "NULL"
	}
}

func nullStringToString(v spanner.NullString) string {
	if v.Valid {
		return v.StringVal
	} else {
		return "NULL"
	}
}

func nullTimeToString(v spanner.NullTime) string {
	if v.Valid {
		return fmt.Sprintf("%s", v.Time.Format(time.RFC3339Nano))
	} else {
		return "NULL"
	}
}

func nullDateToString(v spanner.NullDate) string {
	if v.Valid {
		return strings.Trim(v.String(), `"`)
	} else {
		return "NULL"
	}
}

type CreateDatabaseStatement struct {
	text     string
	database string
}

func (s *CreateDatabaseStatement) Execute(session *Session) (*Result, error) {
	return withElapsedTime(func() (*Result, error) {
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
		result.Stats.AffectedRows = len(result.Rows)

		return result, nil
	})
}

type ShowTablesStatement struct{}

func (s *ShowTablesStatement) Execute(session *Session) (*Result, error) {
	query := SelectStatement{
		text: `SELECT t.table_name FROM information_schema.tables AS t WHERE t.table_catalog = '' and t.table_schema = ''`,
	}

	result, err := query.Execute(session)
	if err != nil {
		return nil, err
	}

	// rename column name
	if len(result.ColumnNames) == 1 {
		result.ColumnNames[0] = fmt.Sprintf("Tables_in_%s", session.databaseId)
	}

	return result, nil
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
				return nil, err
			}
		} else {
			begin := BeginRwStatement{}
			_, err = begin.Execute(session)
			if err != nil {
				return nil, err
			}

			numRows, err = session.rwTxn.Update(session.ctx, stmt)
			if err != nil {
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
			// commit implicitly like MySQL (https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html)
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
		// TODO
		// if session.roTxn() {
		// }

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
	elapsed := time.Since(t1).String()

	if err != nil {
		return nil, err
	}
	result.Stats.ElapsedTime = elapsed
	return result, nil
}
