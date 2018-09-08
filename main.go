package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
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
	instancePath := fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId)
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
	client, err := spanner.NewClient(ctx, dbPath)
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

		if input == "exit;" {
			os.Exit(0)
		}

		if strings.HasPrefix(input, "create table") {
			adminClient, err := adminapi.NewDatabaseAdminClient(ctx)
			if err != nil {
				log.Fatalf("failed to create database admin client: %v", err)
			}
			op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
				Database:   dbPath,
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

		if strings.HasPrefix(input, "show databases") {
			adminClient, err := adminapi.NewDatabaseAdminClient(ctx)
			if err != nil {
				log.Fatalf("failed to create database admin client: %v", err)
			}
			dbIter := adminClient.ListDatabases(ctx, &adminpb.ListDatabasesRequest{
				Parent: instancePath,
			})
			table := tablewriter.NewWriter(os.Stdout)
			table.SetHeader([]string{"Database"})
			dbnames := make([]string, 0, 0)
			for {
				database, err := dbIter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					log.Printf("failed to list database: %v", err)
					break
				}
				dbname, _ := getDatabaseFromPath(database.GetName())
				dbnames = append(dbnames, dbname)
				table.Append([]string{dbname})
			}
			if len(dbnames) > 0 {
				table.SetAutoFormatHeaders(false)
				table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
				table.SetAlignment(tablewriter.ALIGN_LEFT)
				table.Render()
			}
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
			table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
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

func getDatabaseFromPath(dbpath string) (string, error) {
	re := regexp.MustCompile(`projects/[^/]+/instances/[^/]+/databases/(.+)`)
	matched := re.FindStringSubmatch(dbpath)
	if len(matched) == 2 {
		return matched[1], nil
	} else {
		return "", fmt.Errorf("invalid db path: %s", dbpath)
	}
}
