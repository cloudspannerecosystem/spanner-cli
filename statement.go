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
	spannerpb "google.golang.org/genproto/googleapis/spanner/v1"
)

type Statement interface {
	Execute(session *Session) (*Result, error)
}

type Result struct {
	ColumnNames []string
	Rows        []Row
	QueryStats  QueryStats
	IsMutation  bool
}

type Row struct {
	Columns []string
}

type QueryStats struct {
	Rows        int
	ElapsedTime string
}

var (
	exitRe            = regexp.MustCompile(`(?i)^EXIT$`)
	selectRe          = regexp.MustCompile(`(?i)^SELECT\s.+$`)
	createTableRe     = regexp.MustCompile(`(?i)^CREATE\s+TABLE\s.+$`)
	showDatabasesRe   = regexp.MustCompile(`(?i)^SHOW\s+DATABASES$`)
	showCreateTableRe = regexp.MustCompile(`(?i)^SHOW\s+CREATE\s+TABLE\s+(.*)$`)
	showTablesRe      = regexp.MustCompile(`(?i)^SHOW\s+TABLES$`)
	insertRe          = regexp.MustCompile(`(?i)^INSERT\s+INTO.+$`)
	updateRe          = regexp.MustCompile(`(?i)^UPDATE\s+.+$`)
	deleteRe          = regexp.MustCompile(`(?i)^DELETE\s+.+$`)
)

var (
	statementExitError = errors.New("exit")
)

func buildStatement(input string) (Statement, error) {
	var stmt Statement

	if exitRe.MatchString(input) {
		return nil, statementExitError
	} else if selectRe.MatchString(input) {
		stmt = &QueryStatement{
			text: input,
		}
	} else if createTableRe.MatchString(input) {
		stmt = &CreateTableStatement{
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
	}

	if stmt == nil {
		return nil, errors.New("invalid statement")
	}

	return stmt, nil
}

type QueryStatement struct {
	text string
}

func (s *QueryStatement) Execute(session *Session) (*Result, error) {
	stmt := spanner.NewStatement(s.text)
	iter := session.client.Single().QueryWithStats(session.ctx, stmt)

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
			// fmt.Println(column.Type.Code)
			switch column.Type.Code {
			case spannerpb.TypeCode_INT64:
				var v int64
				if err := column.Decode(&v); err != nil {
					return nil, err
				}
				resultRow.Columns[i] = fmt.Sprintf("%d", v)
			case spannerpb.TypeCode_STRING:
				var v string
				if err := column.Decode(&v); err != nil {
					return nil, err
				}
				resultRow.Columns[i] = v
			default:
				resultRow.Columns[i] = fmt.Sprintf("%s", column.Value)
			}
		}

		result.Rows = append(result.Rows, resultRow)
	}

	rowsReturned, _ := strconv.Atoi(iter.QueryStats["rows_returned"].(string))
	elapsedTime := iter.QueryStats["elapsed_time"].(string)
	result.QueryStats = QueryStats{
		Rows:        rowsReturned,
		ElapsedTime: elapsedTime,
	}

	return result, nil
}

type CreateTableStatement struct {
	text string
}

func (s *CreateTableStatement) Execute(session *Session) (*Result, error) {
	result := &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}

	t1 := time.Now()
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
	elapsed := time.Since(t1).String()

	result.QueryStats = QueryStats{
		Rows:        0,
		ElapsedTime: elapsed,
	}

	return result, nil
}

type ShowDatabasesStatement struct {
}

func (s *ShowDatabasesStatement) Execute(session *Session) (*Result, error) {
	result := &Result{
		ColumnNames: []string{"Database"},
		Rows:        make([]Row, 0),
		IsMutation:  false,
	}

	t1 := time.Now()

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

	elapsed := time.Since(t1).String()

	result.QueryStats = QueryStats{
		Rows:        len(result.Rows),
		ElapsedTime: elapsed,
	}

	return result, nil
}

type ShowCreateTableStatement struct {
	table string
}

func (s *ShowCreateTableStatement) Execute(session *Session) (*Result, error) {
	result := &Result{
		ColumnNames: []string{"Table", "Create Table"},
		Rows:        make([]Row, 0),
		IsMutation:  false,
	}

	t1 := time.Now()

	ddlResponse, err := session.adminClient.GetDatabaseDdl(session.ctx, &adminpb.GetDatabaseDdlRequest{
		Database: session.GetDatabasePath(),
	})
	if err != nil {
		return nil, err
	}
	for _, statement := range ddlResponse.Statements {
		if strings.HasPrefix(statement, fmt.Sprintf("CREATE TABLE %s", s.table)) {
			resultRow := Row{
				Columns: []string{s.table, statement},
			}
			result.Rows = append(result.Rows, resultRow)
			break
		}
	}

	elapsed := time.Since(t1).String()

	result.QueryStats = QueryStats{
		Rows:        len(result.Rows),
		ElapsedTime: elapsed,
	}

	return result, nil
}

type ShowTablesStatement struct{}

func (s *ShowTablesStatement) Execute(session *Session) (*Result, error) {
	query := QueryStatement{
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
	stmt := spanner.NewStatement(s.text)

	result := &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
		IsMutation:  true,
	}

	t1 := time.Now()
	_, err := session.client.ReadWriteTransaction(session.ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		numRows, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		result.QueryStats.Rows = int(numRows) // TODO: int64
		return nil
	})
	if err != nil {
		return nil, err
	}

	elapsed := time.Since(t1).String()
	result.QueryStats.ElapsedTime = elapsed

	return result, nil
}
