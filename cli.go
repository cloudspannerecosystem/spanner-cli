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
	"google.golang.org/api/option"
)

type DisplayMode int

const (
	DisplayModeTable DisplayMode = iota
	DisplayModeVertical
	DisplayModeTab

	delimiterHorizontal = ";"
	delimiterVertical   = "\\G"

	defaultPrompt = `spanner\t> `

	exitCodeSuccess = 0
	exitCodeError   = 1
)

var (
	promptReInTransaction = regexp.MustCompile(`\\t`)
	promptReProjectId     = regexp.MustCompile(`\\p`)
	promptReInstanceId    = regexp.MustCompile(`\\i`)
	promptReDatabaseId    = regexp.MustCompile(`\\d`)
)

type Cli struct {
	Session    *Session
	Prompt     string
	Credential []byte
	InStream   io.ReadCloser
	OutStream  io.Writer
	ErrStream  io.Writer
}

var defaultClientConfig = spanner.ClientConfig{
	NumChannels: 1,
	SessionPoolConfig: spanner.SessionPoolConfig{
		MaxOpened: 1,
		MinOpened: 1,
	},
}

func NewCli(projectId, instanceId, databaseId string, prompt string, credential []byte, inStream io.ReadCloser, outStream io.Writer, errStream io.Writer) (*Cli, error) {
	ctx := context.Background()
	session, err := createSession(ctx, projectId, instanceId, databaseId, credential)
	if err != nil {
		return nil, err
	}

	if prompt == "" {
		prompt = defaultPrompt
	}

	return &Cli{
		Session:    session,
		Prompt:     prompt,
		Credential: credential,
		InStream:   inStream,
		OutStream:  outStream,
		ErrStream:  errStream,
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
		return c.ExitOnError(fmt.Errorf("unknown database %q", c.Session.databaseId))
	}

	for {
		rl.SetPrompt(c.getInterpolatedPrompt())

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
			newSession, err := createSession(ctx, c.Session.projectId, c.Session.instanceId, s.Database, c.Credential)
			if err != nil {
				c.PrintInteractiveError(err)
				continue
			}

			exists, err := newSession.DatabaseExists()
			if err != nil {
				newSession.Close()
				c.PrintInteractiveError(err)
				continue
			}
			if !exists {
				newSession.Close()
				c.PrintInteractiveError(fmt.Errorf("ERROR: Unknown database %q\n", s.Database))
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

		if delimiter == delimiterHorizontal {
			c.PrintResult(result, DisplayModeTable, true)
		} else {
			c.PrintResult(result, DisplayModeVertical, true)
		}

		fmt.Fprintf(c.OutStream, "\n")
	}
}

func (c *Cli) RunBatch(input string, displayTable bool) int {
	// execute batched only if all statements are DDL statements
	if stmts := buildDdlStatements(input); stmts != nil {
		result, err := stmts.Execute(c.Session)
		if err != nil {
			c.PrintBatchError(err)
			return exitCodeError
		}

		if displayTable {
			c.PrintResult(result, DisplayModeTable, false)
		} else {
			c.PrintResult(result, DisplayModeTab, false)
		}
		return exitCodeSuccess
	}

	for _, separated := range separateInput(input) {
		stmt, err := BuildStatement(separated.statement)
		if err != nil {
			c.PrintBatchError(err)
			return exitCodeError
		}

		result, err := stmt.Execute(c.Session)
		if err != nil {
			c.PrintBatchError(err)
			return exitCodeError
		}

		if displayTable {
			c.PrintResult(result, DisplayModeTable, false)
		} else if separated.delimiter == delimiterVertical {
			c.PrintResult(result, DisplayModeVertical, false)
		} else {
			c.PrintResult(result, DisplayModeTab, false)
		}
	}
	return exitCodeSuccess
}

func (c *Cli) Exit() int {
	c.Session.Close()
	fmt.Fprintln(c.OutStream, "Bye")
	return exitCodeSuccess
}

func (c *Cli) ExitOnError(err error) int {
	c.Session.Close()
	fmt.Fprintf(c.ErrStream, "ERROR: %s\n", err)
	return exitCodeError
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

func (c *Cli) getInterpolatedPrompt() string {
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

func createSession(ctx context.Context, projectId string, instanceId string, databaseId string, credential []byte) (*Session, error) {
	if credential != nil {
		credentialOption := option.WithCredentialsJSON(credential)
		return NewSession(ctx, projectId, instanceId, databaseId, defaultClientConfig, credentialOption)
	} else {
		return NewSession(ctx, projectId, instanceId, databaseId, defaultClientConfig)
	}
}

type inputStatement struct {
	statement string
	delimiter string
}

func readInteractiveInput(rl *readline.Instance) (string, string, error) {
	origPrompt := rl.Config.Prompt
	defer rl.SetPrompt(origPrompt)

	var lines []string
	for {
		line, err := rl.Readline()
		if err != nil {
			return "", "", err
		}

		line = strings.TrimSpace(line)
		if len(line) != 0 {
			// check comment literal
			if strings.HasPrefix(line, "#") || strings.HasPrefix(line, "--") {
				continue
			}
			for _, d := range []string{delimiterHorizontal, delimiterVertical} {
				if strings.HasSuffix(line, d) {
					line = strings.TrimRight(line, d)
					lines = append(lines, line)
					return strings.TrimSpace(strings.Join(lines, " ")), d, nil
				}
			}
		}
		lines = append(lines, line)
		rl.SetPrompt("      -> ") // same length to original prompt
	}
}

// Separate input to each statement.
func separateInput(input string) []inputStatement {
	input = strings.TrimSpace(input)

	// NOTE: This logic doesn't do syntactic analysis, but just checks the delimiter position,
	// so it's fragile for the case that delimiters appear in strings.
	var statements []inputStatement
	for input != "" {
		if idx := strings.Index(input, delimiterHorizontal); idx != -1 {
			statements = append(statements, inputStatement{
				statement: strings.TrimSpace(input[:idx]),
				delimiter: delimiterHorizontal,
			})
			input = strings.TrimSpace(input[idx+1:])
		} else if idx := strings.Index(input, delimiterVertical); idx != -1 {
			statements = append(statements, inputStatement{
				statement: strings.TrimSpace(input[:idx]),
				delimiter: delimiterVertical,
			})
			input = strings.TrimSpace(input[idx+2:]) // +2 for \ and G
		} else {
			statements = append(statements, inputStatement{
				statement: strings.TrimSpace(input),
				delimiter: delimiterHorizontal, // default horizontal
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

// buildDdlStatements build batched statement only if all statements are DDL statements
func buildDdlStatements(input string) Statement {
	var ddls []string
	for _, separated := range separateInput(input) {
		stmt, err := BuildStatement(separated.statement)
		if err != nil {
			return nil
		}

		if ddl, ok := stmt.(*DdlStatement); ok {
			ddls = append(ddls, ddl.Ddl)
		} else {
			return nil
		}
	}
	return &DdlStatements{ddls}
}
