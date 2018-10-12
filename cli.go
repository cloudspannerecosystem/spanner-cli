package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/chzyer/readline"
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

func NewSession(projectId string, instanceId string, databaseId string) (*Session, error) {
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

	return &Session{
		ctx:           ctx,
		projectId:     projectId,
		instanceId:    instanceId,
		databaseId:    databaseId,
		client:        client,
		adminClient:   adminClient,
		txnFinished:   make(chan bool),
		committedChan: make(chan bool),
	}, nil
}

func (s *Session) inTxn() bool {
	return s.rwTxn != nil
}

type Cli struct {
	Session *Session
}

func NewCli(projectId, instanceId, databaseId string) (*Cli, error) {
	session, err := NewSession(projectId, instanceId, databaseId)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Connected.\n")

	return &Cli{
		Session: session,
	}, nil
}

func (c *Cli) Run() {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:      "spanner> ",
		HistoryFile: "/tmp/spanner_cli_readline.tmp",
	})
	if err != nil {
		panic(err)
	}

	for {
		input := readInput(rl)
		statement, err := buildStatement(input)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if _, ok := statement.(*ExitStatement); ok {
			c.Session.client.Close()
			c.Session.adminClient.Close()
			os.Exit(0)
		}

		if stmt, ok := statement.(*UseStatement); ok {
			c.Session.client.Close()
			c.Session.adminClient.Close()
			newSession, err := NewSession(c.Session.projectId, c.Session.instanceId, stmt.database)
			if err != nil {
				fmt.Println(err)
				continue
			}
			c.Session = newSession
			fmt.Println("Database changed")
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

func readInput(rl *readline.Instance) string {
	lines := make([]string, 0)
	for {
		line, _ := rl.Readline()
		line = strings.TrimSpace(line)
		if len(line) != 0 && line[len(line)-1] == ';' { // terminated
			line = strings.TrimRight(line, ";")
			lines = append(lines, line)
			return strings.TrimSpace(strings.Join(lines, " "))
		} else {
			lines = append(lines, line)
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
