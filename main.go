package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

func main() {
	var projectId string
	var instanceId string
	var databaseId string

	flag.StringVar(&projectId, "project", "", "")
	flag.StringVar(&instanceId, "instance", "", "")
	flag.StringVar(&databaseId, "database", "", "")
	flag.Parse()

	ctx := context.Background()
	dbname := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
	client, err := spanner.NewClient(ctx, dbname)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer client.Close()

	fmt.Printf("Connected.\n")

	for {
		fmt.Printf("> ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		input := scanner.Text()

		if strings.HasPrefix(input, "create table") {
			adminClient, err := adminapi.NewDatabaseAdminClient(ctx)
			if err != nil {
				log.Fatalf("failed to create database admin client: %v", err)
			}
			op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
				Database:   dbname,
				Statements: []string{input},
			})
			if err != nil {
				fmt.Printf("failed to update database ddl: %v", err)
				continue
			}
			if err := op.Wait(ctx); err != nil {
				fmt.Printf("failed to update database ddl: %v", err)
				continue
			}
			fmt.Printf("Created table in database [%s]\n", databaseId)
			continue
		}

		stmt := spanner.NewStatement(input)
		iter := client.Single().QueryWithStats(ctx, stmt)

		defer iter.Stop()
		table := tablewriter.NewWriter(os.Stdout)
		rows := make([]*spanner.Row, 0, 0)
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Fatalf("failed to query: %v", err)
			}
			rows = append(rows, row)

			columns := make([]string, row.Size())
			for i := 0; i < row.Size(); i++ {
				var column spanner.GenericColumnValue
				err := row.Column(i, &column)
				if err != nil {
					log.Fatalf("failed to parse column: %v", err)
				}
				columns[i] = fmt.Sprintf("%s", column.Value)
			}
			table.Append(columns)
			table.SetHeader(row.ColumnNames())
		}
		if len(rows) > 0 {
			table.SetAutoFormatHeaders(false)
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.Render()
		}

		rowsReturned, _ := strconv.Atoi(iter.QueryStats["rows_returned"].(string))
		elapsedTime := iter.QueryStats["elapsed_time"].(string)
		if rowsReturned == 0 {
			fmt.Printf("Empty set (%s)\n", elapsedTime)
		} else {
			fmt.Printf("%d rows in set (%s)\n", rowsReturned, elapsedTime)
		}
		fmt.Printf("\n")
	}
}
