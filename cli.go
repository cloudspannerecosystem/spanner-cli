package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"
)

type Delimiter string

const (
	DelimiterHorizontal Delimiter = ";"
	DelimiterVertical   Delimiter = "\\G"
)

type Cli struct {
	Session *Session
}

func NewCli(projectId, instanceId, databaseId string) (*Cli, error) {
	ctx := context.Background()
	session, err := NewSession(ctx, projectId, instanceId, databaseId, spanner.ClientConfig{})
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
		input, delimiter, err := readInput(rl)
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
			ctx := context.Background()
			newSession, err := NewSession(ctx, c.Session.projectId, c.Session.instanceId, s.Database, spanner.ClientConfig{})
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

		// Show results
		if delimiter == DelimiterHorizontal {
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
		} else if delimiter == DelimiterVertical {
			max := 0
			for _, columnName := range result.ColumnNames {
				if len(columnName) > max {
					max = len(columnName)
				}
			}
			format := fmt.Sprintf("%%%ds: %%s\n", max) // for align right
			for i, row := range result.Rows {
				fmt.Printf("*************************** %d. row ***************************\n", i+1)
				for j, column := range row.Columns {
					fmt.Printf(format, result.ColumnNames[j], column)
				}
			}
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

func readInput(rl *readline.Instance) (string, Delimiter, error) {
	lines := make([]string, 0)
	origPrompt := rl.Config.Prompt
	defer rl.SetPrompt(origPrompt)

	for {
		line, err := rl.Readline()
		if err != nil {
			return "", Delimiter("UNKNOWN"), err
		}
		rl.SetPrompt("      -> ") // same length to original prompt

		line = strings.TrimSpace(line)
		if len(line) != 0 {
			for _, delimiter := range []Delimiter{DelimiterHorizontal, DelimiterVertical} {
				if strings.HasSuffix(line, string(delimiter)) {
					line = strings.TrimRight(line, string(delimiter))
					lines = append(lines, line)
					return strings.TrimSpace(strings.Join(lines, " ")), delimiter, nil
				}
			}
		}
		lines = append(lines, line)
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
