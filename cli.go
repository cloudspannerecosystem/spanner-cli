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

type DisplayMode int

const (
	DisplayModeTable DisplayMode = iota
	DisplayModeVertical
	DisplayModeTab
)

const (
	DefaultPrompt = `spanner\t> `
)

type Cli struct {
	Session *Session
	Prompt  string
}

func NewCli(projectId, instanceId, databaseId string, prompt string) (*Cli, error) {
	ctx := context.Background()
	session, err := NewSession(ctx, projectId, instanceId, databaseId, spanner.ClientConfig{})
	if err != nil {
		return nil, err
	}

	if prompt == "" {
		prompt = DefaultPrompt
	}

	return &Cli{
		Session: session,
		Prompt:  prompt,
	}, nil
}

func (c *Cli) RunInteractive() {
	rl, err := readline.NewEx(&readline.Config{
		HistoryFile: "/tmp/spanner_cli_readline.tmp",
	})
	if err != nil {
		c.ExitOnError(err)
	}

	exists, err := c.Session.DatabaseExists()
	if err != nil {
		c.ExitOnError(err)
	}
	if exists {
		fmt.Printf("Connected.\n")
	} else {
		c.ExitOnError(fmt.Errorf("Unknown database '%s'", c.Session.databaseId))
	}

	for {
		prompt := c.Session.InterpolatePromptVariable(c.Prompt)
		rl.SetPrompt(prompt)

		input, delimiter, err := readInteractiveInput(rl)
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
			ctx := context.Background()
			newSession, err := NewSession(ctx, c.Session.projectId, c.Session.instanceId, s.Database, spanner.ClientConfig{})
			if err != nil {
				fmt.Printf("ERROR: %s\n", err)
				continue
			}

			exists, err := newSession.DatabaseExists()
			if err != nil {
				fmt.Printf("ERROR: %s\n", err)
				continue
			}
			if !exists {
				fmt.Printf("ERROR: Unknown database '%s'\n", s.Database)
				continue
			}

			c.Session.client.Close()
			c.Session.adminClient.Close()
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

		if delimiter == DelimiterHorizontal {
			printResult(result, DisplayModeTable, true)
		} else {
			printResult(result, DisplayModeVertical, true)
		}

		fmt.Printf("\n")
	}
}

func (c *Cli) RunBatch(input string, displayTable bool) {
	for _, separated := range separateInput(input) {
		stmt, err := BuildStatement(separated.Statement)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
			return
		}

		result, err := stmt.Execute(c.Session)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
			return
		}

		if displayTable {
			printResult(result, DisplayModeTable, false)
		} else if separated.Delimiter == DelimiterVertical {
			printResult(result, DisplayModeVertical, false)
		} else {
			printResult(result, DisplayModeTab, false)
		}
	}
}

func (c *Cli) Exit() {
	c.Session.client.Close()
	c.Session.adminClient.Close()
	fmt.Println("Bye")
	os.Exit(0)
}

func (c *Cli) ExitOnError(err error) {
	c.Session.client.Close()
	c.Session.adminClient.Close()
	fmt.Printf("ERROR: %s\n", err)
	os.Exit(1)
}

func printResult(result *Result, mode DisplayMode, withStats bool) {
	if mode == DisplayModeTable {
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
	} else if mode == DisplayModeVertical {
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
	} else if mode == DisplayModeTab {
		if len(result.ColumnNames) > 0 {
			fmt.Println(strings.Join(result.ColumnNames, "\t"))
			for _, row := range result.Rows {
				fmt.Println(strings.Join(row.Columns, "\t"))
			}
		}
	}

	if withStats {
		if result.IsMutation {
			fmt.Printf("Query OK, %d rows affected (%s)\n", result.Stats.AffectedRows, result.Stats.ElapsedTime)
		} else {
			if result.Stats.AffectedRows == 0 {
				fmt.Printf("Empty set (%s)\n", result.Stats.ElapsedTime)
			} else {
				fmt.Printf("%d rows in set (%s)\n", result.Stats.AffectedRows, result.Stats.ElapsedTime)
			}
		}
	}
}

type InputStatement struct {
	Statement string
	Delimiter Delimiter
}

func readInteractiveInput(rl *readline.Instance) (string, Delimiter, error) {
	lines := make([]string, 0)
	origPrompt := rl.Config.Prompt
	defer rl.SetPrompt(origPrompt)

	for {
		line, err := rl.Readline()
		if err != nil {
			return "", Delimiter("UNKNOWN"), err
		}

		line = strings.TrimSpace(line)
		if len(line) != 0 {
			// check comment literal
			if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "--") {
				continue
			}
			for _, delimiter := range []Delimiter{DelimiterHorizontal, DelimiterVertical} {
				if strings.HasSuffix(line, string(delimiter)) {
					line = strings.TrimRight(line, string(delimiter))
					lines = append(lines, line)
					return strings.TrimSpace(strings.Join(lines, " ")), delimiter, nil
				}
			}
		}
		lines = append(lines, line)
		rl.SetPrompt("      -> ") // same length to original prompt
	}
}

// separate to each statement
func separateInput(input string) []InputStatement {
	input = strings.TrimSpace(input)
	statements := make([]InputStatement, 0)

	// NOTE: This logic doesn't do syntactic analysis, but just checks the delimiter position,
	// so it's fragile for the case that delimiters appear in strings.
	for input != "" {
		if idx := strings.Index(input, string(DelimiterHorizontal)); idx != -1 {
			statements = append(statements, InputStatement{
				Statement: strings.TrimSpace(input[:idx]),
				Delimiter: DelimiterHorizontal,
			})
			input = strings.TrimSpace(input[idx+1:])
		} else if idx := strings.Index(input, string(DelimiterVertical)); idx != -1 {
			statements = append(statements, InputStatement{
				Statement: strings.TrimSpace(input[:idx]),
				Delimiter: DelimiterVertical,
			})
			input = strings.TrimSpace(input[idx+2:]) // +2 for \ and G
		} else {
			statements = append(statements, InputStatement{
				Statement: strings.TrimSpace(input),
				Delimiter: DelimiterHorizontal, // default horizontal
			})
			break
		}
	}
	return statements
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
