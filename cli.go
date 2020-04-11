package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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
	Verbose    bool
}

type command struct {
	Stmt     Statement
	Vertical bool
}

var defaultClientConfig = spanner.ClientConfig{
	NumChannels: 1,
	SessionPoolConfig: spanner.SessionPoolConfig{
		MaxOpened: 1,
		MinOpened: 1,
	},
}

func NewCli(projectId, instanceId, databaseId string, prompt string, credential []byte, inStream io.ReadCloser, outStream io.Writer, errStream io.Writer, verbose bool) (*Cli, error) {
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
		Verbose:    verbose,
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
		prompt := c.getInterpolatedPrompt()
		rl.SetPrompt(prompt)

		input, err := readInteractiveInput(rl, prompt)
		if err == io.EOF {
			return c.Exit()
		}
		if err != nil {
			c.PrintInteractiveError(err)
			continue
		}

		stmt, err := BuildStatement(input.statement)
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

		if s, ok := stmt.(*DropDatabaseStatement); ok {
			if c.Session.databaseId == s.DatabaseId {
				c.PrintInteractiveError(fmt.Errorf("database %q is currently used, it can not be dropped", s.DatabaseId))
				continue
			}

			if !confirm(c.OutStream, fmt.Sprintf("Database %q will be dropped.\nDo you want to continue?", s.DatabaseId)) {
				continue
			}
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

		if input.delim == delimiterHorizontal {
			c.PrintResult(result, DisplayModeTable, true)
		} else {
			c.PrintResult(result, DisplayModeVertical, true)
		}

		fmt.Fprintf(c.OutStream, "\n")
	}
}

func (c *Cli) RunBatch(input string, displayTable bool) int {
	cmds, err := buildCommands(input)
	if err != nil {
		c.PrintBatchError(err)
		return exitCodeError
	}

	for _, cmd := range cmds {
		result, err := cmd.Stmt.Execute(c.Session)
		if err != nil {
			c.PrintBatchError(err)
			return exitCodeError
		}

		if displayTable {
			c.PrintResult(result, DisplayModeTable, false)
		} else if cmd.Vertical {
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
	printResult(c.OutStream, result, mode, withStats, c.Verbose)
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

func readInteractiveInput(rl *readline.Instance, prompt string) (*inputStatement, error) {
	defer rl.SetPrompt(prompt)

	var input string
	for {
		line, err := rl.Readline()
		if err != nil {
			return nil, err
		}
		input += line + "\n"

		statements := separateInput(input)
		switch len(statements) {
		case 0:
			// read next input
		case 1:
			if statements[0].delim != delimiterUndefined {
				return &statements[0], nil
			}
			// read next input
		default:
			return nil, errors.New("sql queries are limited to single statements")
		}

		// show prompt to urge next input
		var margin string
		if l := len(prompt); l >= 3 {
			margin = strings.Repeat(" ", l-3)
		}
		rl.SetPrompt(margin + "-> ")
	}
}

func printResult(out io.Writer, result *Result, mode DisplayMode, withStats bool, verbose bool) {
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
		var timestampStr string
		if verbose && !result.Timestamp.IsZero() {
			timestampStr = fmt.Sprintf(", timestamp: %s", result.Timestamp.Format(time.RFC3339Nano))
		}
		if result.IsMutation {
			fmt.Fprintf(out, "Query OK, %d rows affected (%s)%s\n", result.Stats.AffectedRows, result.Stats.ElapsedTime, timestampStr)
		} else {
			if result.Stats.AffectedRows == 0 {
				fmt.Fprintf(out, "Empty set (%s)%s\n", result.Stats.ElapsedTime, timestampStr)
			} else {
				fmt.Fprintf(out, "%d rows in set (%s)%s\n", result.Stats.AffectedRows, result.Stats.ElapsedTime, timestampStr)
			}
		}
	}
}

func buildCommands(input string) ([]*command, error) {
	var cmds []*command
	var pendingDdls []string
	for _, separated := range separateInput(input) {
		stmt, err := BuildStatement(separated.statement)
		if err != nil {
			return nil, err
		}
		if ddl, ok := stmt.(*DdlStatement); ok {
			pendingDdls = append(pendingDdls, ddl.Ddl)
			continue
		}

		// Flush pending DDLs
		if len(pendingDdls) > 0 {
			cmds = append(cmds, &command{&BulkDdlStatement{pendingDdls}, false})
			pendingDdls = nil
		}

		cmds = append(cmds, &command{stmt, separated.delim == delimiterVertical})
	}

	// Flush pending DDLs
	if len(pendingDdls) > 0 {
		cmds = append(cmds, &command{&BulkDdlStatement{pendingDdls}, false})
	}

	return cmds, nil
}

func confirm(out io.Writer, msg string) bool {
	fmt.Fprintf(out, "%s [yes/no] ", msg)

	s := bufio.NewScanner(os.Stdin)
	for {
		s.Scan()
		switch strings.ToLower(s.Text()) {
		case "yes":
			return true
		case "no":
			return false
		default:
			fmt.Fprint(out, "Please answer yes or no: ")
		}
	}
}
