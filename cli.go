package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"
)

type Session struct {
	ctx         context.Context
	projectId   string
	instanceId  string
	databaseId  string
	client      *spanner.Client
	adminClient *adminapi.DatabaseAdminClient

	// for read-write transaction
	rwTxn         *spanner.ReadWriteTransaction
	txnFinished   chan error
	committedChan chan bool

	// for read-only transaction
	roTxn *spanner.ReadOnlyTransaction
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
		txnFinished:   make(chan error),
		committedChan: make(chan bool),
	}, nil
}

func (s *Session) inRwTxn() bool {
	return s.rwTxn != nil
}

func (s *Session) inRoTxn() bool {
	return s.roTxn != nil
}

func (s *Session) finishRwTxn() {
	s.rwTxn = nil
}

func (s *Session) finishRoTxn() {
	s.roTxn.Close()
	s.roTxn = nil
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

	// HACK: for speed up first execution
	go func() {
		stmt := &SelectStatement{"SELECT 1"}
		stmt.Execute(session)
	}()

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
		if c.Session.inRwTxn() {
			rl.SetPrompt("spanner(rw txn)> ")
		} else if c.Session.inRoTxn() {
			rl.SetPrompt("spanner(ro txn)> ")
		} else {
			rl.SetPrompt("spanner> ")
		}
		input, err := readInput(rl)
		if err == io.EOF {
			c.Exit()
		}

		stmt, err := BuildStatement(input)
		if err != nil {
			fmt.Printf("ERROR: %s\n", err)
			continue
		}

		if _, ok := stmt.(*ExitStatement); ok {
			c.Exit()
		}

		if s, ok := stmt.(*UseStatement); ok {
			c.Session.client.Close()
			c.Session.adminClient.Close()
			newSession, err := NewSession(c.Session.projectId, c.Session.instanceId, s.Database)
			if err != nil {
				fmt.Printf("ERROR: %s\n", err)
				continue
			}
			c.Session = newSession
			fmt.Println("Database changed")
			continue
		}

		ticker := printProgressingMark()
		result, err := stmt.Execute(c.Session)
		ticker.Stop()
		fmt.Printf("\r") // clear progressing mark
		if err != nil {
			fmt.Printf("ERROR: %s\n", err)
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
			fmt.Printf("Query OK, %d rows affected (%s)\n", result.Stats.AffectedRows, result.Stats.ElapsedTime)
		} else {
			if result.Stats.AffectedRows == 0 {
				fmt.Printf("Empty set (%s)\n", result.Stats.ElapsedTime)
			} else {
				fmt.Printf("%d rows in set (%s)\n", result.Stats.AffectedRows, result.Stats.ElapsedTime)
			}
		}
		fmt.Printf("\n")
	}
}

func (c *Cli) Exit() {
	c.Session.client.Close()
	c.Session.adminClient.Close()
	fmt.Println("Bye")
	os.Exit(0)
}

func (s *Session) GetDatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.projectId, s.instanceId, s.databaseId)
}

func (s *Session) GetInstancePath() string {
	return fmt.Sprintf("projects/%s/instances/%s", s.projectId, s.instanceId)
}

func readInput(rl *readline.Instance) (string, error) {
	lines := make([]string, 0)
	origPrompt := rl.Config.Prompt
	defer rl.SetPrompt(origPrompt)

	for {
		line, err := rl.Readline()
		if err != nil {
			return "", err
		}
		// rl.SetPrompt(fmt.Sprintf("%s-> ", strings.Repeat(" ", len(origPrompt)-3))) // same length to original prompt
		rl.SetPrompt("      -> ") // same length to original prompt

		line = strings.TrimSpace(line)
		if len(line) != 0 && line[len(line)-1] == ';' { // terminated
			line = strings.TrimRight(line, ";")
			lines = append(lines, line)
			return strings.TrimSpace(strings.Join(lines, " ")), nil
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
