package main

import (
	"context"
	"fmt"
	"io"
	"regexp"
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

const (
	ExitCodeSuccess = 0
	ExitCodeError   = 1
)

var (
	promptReInTransaction = regexp.MustCompile(`\\t`)
	promptReProjectId     = regexp.MustCompile(`\\p`)
	promptReInstanceId    = regexp.MustCompile(`\\i`)
	promptReDatabaseId    = regexp.MustCompile(`\\d`)
)

type Cli struct {
	Session   *Session
	Prompt    string
	InStream  io.ReadCloser
	OutStream io.Writer
	ErrStream io.Writer
}

func NewCli(projectId, instanceId, databaseId string, prompt string, inStream io.ReadCloser, outStream io.Writer, errStream io.Writer) (*Cli, error) {
	ctx := context.Background()
	session, err := NewSession(ctx, projectId, instanceId, databaseId, spanner.ClientConfig{})
	if err != nil {
		return nil, err
	}

	if prompt == "" {
		prompt = DefaultPrompt
	}

	return &Cli{
		Session:   session,
		Prompt:    prompt,
		InStream:  inStream,
		OutStream: outStream,
		ErrStream: errStream,
	}, nil
}

func (c *Cli) RunInteractive() int {
	rl, err := readline.NewEx(&readline.Config{
		Stdin:       c.InStream,
		HistoryFile: "/tmp/spanner_cli_readline.tmp",
	})
	if err != nil {
		return c.ExitOnError(err)
	}

	exists, err := c.Session.DatabaseExists()
	if err != nil {
		return c.ExitOnError(err)
	}
	if exists {
		fmt.Fprintf(c.OutStream, "Connected.\n")
	} else {
		return c.ExitOnError(fmt.Errorf("Unknown database '%s'", c.Session.databaseId))
	}

	for {
		rl.SetPrompt(c.GetInterpolatedPrompt())

		input, delimiter, err := readInteractiveInput(rl)
		if err == io.EOF {
			return c.Exit()
		}

		stmt, err := BuildStatement(input)
		if err != nil {
			c.PrintInteractiveError(err)
			continue
		}

		if _, ok := stmt.(*ExitStatement); ok {
			return c.Exit()
		}

		if s, ok := stmt.(*UseStatement); ok {
			ctx := context.Background()
			newSession, err := NewSession(ctx, c.Session.projectId, c.Session.instanceId, s.Database, spanner.ClientConfig{})
			if err != nil {
				c.PrintInteractiveError(err)
				continue
			}

			exists, err := newSession.DatabaseExists()
			if err != nil {
				c.PrintInteractiveError(err)
				continue
			}
			if !exists {
				c.PrintInteractiveError(fmt.Errorf("ERROR: Unknown database '%s'\n", s.Database))
				continue
			}

			c.Session.Close()
			c.Session = newSession
			fmt.Fprintf(c.OutStream, "Database changed")
			continue
		}

		// execute
		stop := c.PrintProgressingMark()
		t0 := time.Now()
		result, err := stmt.Execute(c.Session)
		elapsed := time.Since(t0).Seconds()
		stop()
		if err != nil {
			c.PrintInteractiveError(err)
			continue
		}

		// only SELECT statement has the elapsed time measured by the server
		if result.Stats.ElapsedTime == "" {
			result.Stats.ElapsedTime = fmt.Sprintf("%0.2f sec", elapsed)
		}

		if delimiter == DelimiterHorizontal {
			c.PrintResult(result, DisplayModeTable, true)
		} else {
			c.PrintResult(result, DisplayModeVertical, true)
		}

		fmt.Fprintf(c.OutStream, "\n")
	}
}

func (c *Cli) RunBatch(input string, displayTable bool) int {
	for _, separated := range separateInput(input) {
		stmt, err := BuildStatement(separated.Statement)
		if err != nil {
			c.PrintBatchError(err)
			return ExitCodeError
		}

		result, err := stmt.Execute(c.Session)
		if err != nil {
			c.PrintBatchError(err)
			return ExitCodeError
		}

		if displayTable {
			c.PrintResult(result, DisplayModeTable, false)
		} else if separated.Delimiter == DelimiterVertical {
			c.PrintResult(result, DisplayModeVertical, false)
		} else {
			c.PrintResult(result, DisplayModeTab, false)
		}
	}
	return ExitCodeSuccess
}

func (c *Cli) Exit() int {
	c.Session.Close()
	fmt.Fprintln(c.OutStream, "Bye")
	return ExitCodeSuccess
}

func (c *Cli) ExitOnError(err error) int {
	c.Session.Close()
	fmt.Fprintf(c.ErrStream, "ERROR: %s\n", err)
	return ExitCodeError
}

func (c *Cli) PrintInteractiveError(err error) {
	fmt.Fprintf(c.OutStream, "ERROR: %s\n", err)
}

func (c *Cli) PrintBatchError(err error) {
	fmt.Fprintf(c.ErrStream, "ERROR: %s\n", err)
}

func (c *Cli) PrintResult(result *Result, mode DisplayMode, withStats bool) {
	printResult(c.OutStream, result, mode, withStats)
}

func (c *Cli) PrintProgressingMark() func() {
	progressMarks := []string{`-`, `\`, `|`, `/`}
	ticker := time.NewTicker(time.Millisecond * 100)
	go func() {
		i := 0
		for {
			<-ticker.C
			mark := progressMarks[i%len(progressMarks)]
			fmt.Fprintf(c.OutStream, "\r%s", mark)
			i++
		}
	}()

	stop := func() {
		ticker.Stop()
		fmt.Fprintf(c.OutStream, "\r") // clear progressing mark
	}
	return stop
}

func (c *Cli) GetInterpolatedPrompt() string {
	prompt := c.Prompt
	prompt = promptReProjectId.ReplaceAllString(prompt, c.Session.projectId)
	prompt = promptReInstanceId.ReplaceAllString(prompt, c.Session.instanceId)
	prompt = promptReDatabaseId.ReplaceAllString(prompt, c.Session.databaseId)

	if c.Session.InRwTxn() {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "(rw txn)")
	} else if c.Session.InRoTxn() {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "(ro txn)")
	} else {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "")
	}

	return prompt
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

func printResult(out io.Writer, result *Result, mode DisplayMode, withStats bool) {
	if mode == DisplayModeTable {
		table := tablewriter.NewWriter(out)
		table.SetAutoFormatHeaders(false)
		table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.SetAutoWrapText(false)

		for _, row := range result.Rows {
			table.Append(row.Columns)
		}
		table.SetHeader(result.ColumnNames)
		if len(result.Rows) > 0 {
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
			fmt.Fprintf(out, "*************************** %d. row ***************************\n", i+1)
			for j, column := range row.Columns {
				fmt.Fprintf(out, format, result.ColumnNames[j], column)
			}
		}
	} else if mode == DisplayModeTab {
		if len(result.ColumnNames) > 0 {
			fmt.Fprintln(out, strings.Join(result.ColumnNames, "\t"))
			for _, row := range result.Rows {
				fmt.Fprintln(out, strings.Join(row.Columns, "\t"))
			}
		}
	}

	if withStats {
		if result.IsMutation {
			fmt.Fprintf(out, "Query OK, %d rows affected (%s)\n", result.Stats.AffectedRows, result.Stats.ElapsedTime)
		} else {
			if result.Stats.AffectedRows == 0 {
				fmt.Fprintf(out, "Empty set (%s)\n", result.Stats.ElapsedTime)
			} else {
				fmt.Fprintf(out, "%d rows in set (%s)\n", result.Stats.AffectedRows, result.Stats.ElapsedTime)
			}
		}
	}
}
