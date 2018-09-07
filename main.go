package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/spanner"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/api/iterator"
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
		// fmt.Printf("OK! We'll try to do %s\n", input)

		stmt := spanner.NewStatement(input)
		iter := client.Single().Query(ctx, stmt)

		defer iter.Stop()
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Fatalf("failed to query: %v", err)
			}

			table := tablewriter.NewWriter(os.Stdout)
			columns := make([]string, 0, row.Size())
			for i := 0; i < row.Size(); i++ {
				var column string
				err := row.Column(0, &column)
				if err != nil {
					log.Fatalf("failed to parse column: %v", err)
				}
				columns[i] = column
			}
			table.Append(columns)
			table.setHeader(row.ColumnNames())
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.Render()
		}
		fmt.Printf("%d rows in set (0.00 sec)\n")
		fmt.Printf("\n")
	}
}
