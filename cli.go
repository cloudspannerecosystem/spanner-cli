package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/olekukonko/tablewriter"
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
		prompt := fmt.Sprintf("\x1b[36m[%s]\x1b[0m\n> ", buildDatabasePath(c.projectId, c.instanceId, c.databaseId))
		fmt.Print(prompt)

		input := readInput(os.Stdin)
		if input == "exit" {
			os.Exit(0)
		}

		statement, err := buildStatement(input)
		if err != nil {
			fmt.Println(err)
			continue
		}

		ticker := printProgressingMark()
		result, err := statement.Execute(c)
		ticker.Stop()
		fmt.Printf("\r")
		if err != nil {
			fmt.Println(err)
			continue
		}

		table := tablewriter.NewWriter(os.Stdout)
		for _, row := range result.Rows {
			table.Append(row.Columns)
		}
		table.SetHeader(result.ColumnNames)
		if len(result.Rows) > 0 {
			table.SetAutoFormatHeaders(false)
			table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
			table.SetAlignment(tablewriter.ALIGN_LEFT)
			table.Render()
		}

		if result.IsMutation {
			fmt.Printf("Query OK, %d rows affected (%s)\n", result.QueryStats.Rows, result.QueryStats.ElapsedTime)
		} else {
			if result.QueryStats.Rows == 0 {
				fmt.Printf("Empty set (%s)\n", result.QueryStats.ElapsedTime)
			} else {
				fmt.Printf("%d rows in set (%s)\n", result.QueryStats.Rows, result.QueryStats.ElapsedTime)
			}
		}
		fmt.Printf("\n")
	}
}

func (c *Cli) GetDatabasePath() string {
	return buildDatabasePath(c.projectId, c.instanceId, c.databaseId)
}

func (c *Cli) GetInstancePath() string {
	return buildInstancePath(c.projectId, c.instanceId)
}

func readInput(in io.Reader) string {
	scanner := bufio.NewScanner(in)
	lines := make([]string, 0)
	for {
		scanner.Scan()
		text := scanner.Text()
		text = strings.Trim(text, " ")
		if len(text) != 0 && text[len(text)-1] == ';' {
			text = strings.TrimRight(text, ";")
			lines = append(lines, text)
			return strings.Join(lines, " ")
		} else {
			lines = append(lines, text)
		}
	}
}

func printProgressingMark() *time.Ticker {
	progressMarks := []string{"-", "\\", "|", "/"}
	ticker := time.NewTicker(time.Millisecond * 100)
	go func() {
		i := 0
		for {
			<-ticker.C
			mark := progressMarks[i%len(progressMarks)]
			fmt.Printf("\r%s", mark)
			i++
		}
	}()
	return ticker
}

func buildInstancePath(projectId, instanceId string) string {
	return fmt.Sprintf("projects/%s/instances/%s", projectId, instanceId)
}

func buildDatabasePath(projectId, instanceId, databaseId string) string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
}
