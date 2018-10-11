package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	// adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type Cli struct {
	projectId   string
	instanceId  string
	databaseId  string
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient
}

func NewCli(projectId, instanceId, databaseId string) (*Cli, error) {
	ctx := context.Background() // TODO
	dbPath := buildDatabasePath(projectId, instanceId, databaseId)

	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return nil, err
	}

	adminClient, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Connected.\n")

	return &Cli{
		projectId:   projectId,
		instanceId:  instanceId,
		databaseId:  databaseId,
		client:      client,
		adminClient: adminClient,
	}, nil
}

func (c *Cli) Run() {
	for {
		// fmt.Printf("[%s]\n", dbPath)
		fmt.Printf("> ")
		input := readInput(os.Stdin)
		if input == "exit;" {
			os.Exit(0)
		}
		fmt.Println(input)
	}
}

func readInput(in io.Reader) string {
	scanner := bufio.NewScanner(in)
	lines := make([]string, 0)
	for {
		scanner.Scan()
		text := scanner.Text()
		text = strings.Trim(text, " ")
		lines = append(lines, text)

		if text[len(text)-1] == ';' {
			return strings.Join(lines, " ")
		}
	}
}

func buildInstancePath(projectId, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId)
}

func buildDatabasePath(projectId, instanceId, databaseId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
}

// type Statement string

// const (
// StatementQuery Statement =
// )

// func detectStatement(input string) Statement {
// }

// func (s *Statement) Execute() {
// stmt := spanner.NewStatement(input)
// iter := client.Single().QueryWithStats(ctx, stmt)

// defer iter.Stop()
// table := tablewriter.NewWriter(os.Stdout)
// rows := make([]*spanner.Row, 0, 0)
// for {
// row, err := iter.Next()
// if err == iterator.Done {
// break
// }
// if err != nil {
// log.Fatalf("failed to query: %v", err)
// }
// rows = append(rows, row)

// columns := make([]string, row.Size())
// for i := 0; i < row.Size(); i++ {
// var column spanner.GenericColumnValue
// err := row.Column(i, &column)
// if err != nil {
// log.Fatalf("failed to parse column: %v", err)
// }
// columns[i] = fmt.Sprintf("%s", column.Value)
// }
// table.Append(columns)
// table.SetHeader(row.ColumnNames())
// }
// if len(rows) > 0 {
// table.SetAutoFormatHeaders(false)
// table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
// table.SetAlignment(tablewriter.ALIGN_LEFT)
// table.Render()
// }

// rowsReturned, _ := strconv.Atoi(iter.QueryStats["rows_returned"].(string))
// elapsedTime := iter.QueryStats["elapsed_time"].(string)
// if rowsReturned == 0 {
// fmt.Printf("Empty set (%s)\n", elapsedTime)
// } else {
// fmt.Printf("%d rows in set (%s)\n", rowsReturned, elapsedTime)
// }
// fmt.Printf("\n")
// }
// }
