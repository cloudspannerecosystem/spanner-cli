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

type Session struct {
	ctx           context.Context
	projectId     string
	instanceId    string
	databaseId    string
	client        *spanner.Client
	adminClient   *adminapi.DatabaseAdminClient
	rwTxn         *spanner.ReadWriteTransaction
	txnFinished   chan bool
	committedChan chan bool
}

func (s *Session) inTxn() bool {
	return s.rwTxn != nil
}

type Cli struct {
	Session *Session
}

func NewCli(projectId, instanceId, databaseId string) (*Cli, error) {
	ctx := context.Background()
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectId, instanceId, databaseId)
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return nil, err
	}

	adminClient, err := adminapi.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}

	session := &Session{
		ctx:           ctx,
		projectId:     projectId,
		instanceId:    instanceId,
		databaseId:    databaseId,
		client:        client,
		adminClient:   adminClient,
		txnFinished:   make(chan bool),
		committedChan: make(chan bool),
	}

	fmt.Printf("Connected.\n")

	return &Cli{
		Session: session,
	}, nil
}

func (c *Cli) Run() {
	for {
		prompt := fmt.Sprintf("\x1b[36m[%s]\x1b[0m\n> ", c.Session.GetDatabasePath())
		fmt.Print(prompt)

		input := readInput(os.Stdin)
		statement, err := buildStatement(input)
		if err != nil {
			if err == statementExitError {
				c.Session.client.Close()
				c.Session.adminClient.Close()
				os.Exit(0)
			}
			fmt.Println(err)
			continue
		}

		ticker := printProgressingMark()
		result, err := statement.Execute(c.Session)
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

func (s *Session) GetDatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.projectId, s.instanceId, s.databaseId)
}

func (s *Session) GetInstancePath() string {
	return fmt.Sprintf("projects/%s/instances/%s", s.projectId, s.instanceId)
}

func readInput(in io.Reader) string {
	scanner := bufio.NewScanner(in)
	lines := make([]string, 0)
	for {
		scanner.Scan()
		text := strings.Trim(scanner.Text(), " ")
		if len(text) != 0 && text[len(text)-1] == ';' { // terminated
			text = strings.TrimRight(text, ";")
			lines = append(lines, text)
			return strings.Trim(strings.Join(lines, " "), " ")
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
