package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/iterator"
)

type Statement interface {
	Execute(client *spanner.Client, adminClient *adminapi.DatabaseAdminClient) (*Result, error)
}

type Result struct {
	ColumnNames []string
	Rows        []Row
	QueryStats  QueryStats
}

type Row struct {
	Columns []string
}

type QueryStats struct {
	RowsReturned int
	ElapsedTime  string
}

func buildStatement(input string) (Statement, error) {
	if strings.HasPrefix(input, "select") {
		return &StatementQuery{
			text: input,
		}, nil
	}
	return nil, errors.New("invalid statement")
}

type StatementQuery struct {
	text string
}

func (s *StatementQuery) Execute(client *spanner.Client, adminClient *adminapi.DatabaseAdminClient) (*Result, error) {
	stmt := spanner.NewStatement(s.text)
	ctx := context.Background() // TODO
	iter := client.Single().QueryWithStats(ctx, stmt)

	result := &Result{
		ColumnNames: make([]string, 0),
		Rows:        make([]Row, 0),
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
			resultRow.Columns[i] = fmt.Sprintf("%s", column.Value)
		}

		result.Rows = append(result.Rows, resultRow)
	}

	rowsReturned, _ := strconv.Atoi(iter.QueryStats["rows_returned"].(string))
	elapsedTime := iter.QueryStats["elapsed_time"].(string)
	result.QueryStats = QueryStats{
		RowsReturned: rowsReturned,
		ElapsedTime:  elapsedTime,
	}

	return result, nil
}
