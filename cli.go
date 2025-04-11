//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/chzyer/readline"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
)

type DisplayMode int

const (
	DisplayModeTable DisplayMode = iota
	DisplayModeVertical
	DisplayModeTab

	defaultPrompt      = `spanner\t> `
	defaultHistoryFile = `/tmp/spanner_cli_readline.tmp`

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
	Session       *Session
	Prompt        string
	HistoryFile   string
	Credential    []byte
	InStream      io.ReadCloser
	OutStream     io.Writer
	ErrStream     io.Writer
	Verbose       bool
	Priority      pb.RequestOptions_Priority
	Endpoint      string
	SkipTLSVerify bool
}

type command struct {
	Stmt     Statement
	Vertical bool
}

func NewCli(projectId, instanceId, databaseId, prompt, historyFile string, credential []byte,
	inStream io.ReadCloser, outStream, errStream io.Writer, verbose bool,
	priority pb.RequestOptions_Priority, role, endpoint string, directedRead *pb.DirectedReadOptions,
	skipTLSVerify bool, protoDescriptor []byte) (*Cli, error) {
	session, err := createSession(projectId, instanceId, databaseId, credential, priority, role, endpoint, directedRead, skipTLSVerify, protoDescriptor)
	if err != nil {
		return nil, err
	}

	if prompt == "" {
		prompt = defaultPrompt
	}

	if historyFile == "" {
		historyFile = defaultHistoryFile
	}

	return &Cli{
		Session:       session,
		Prompt:        prompt,
		HistoryFile:   historyFile,
		Credential:    credential,
		InStream:      inStream,
		OutStream:     outStream,
		ErrStream:     errStream,
		Verbose:       verbose,
		Endpoint:      endpoint,
		SkipTLSVerify: skipTLSVerify,
	}, nil
}

func (c *Cli) RunInteractive() int {
	rl, err := readline.NewEx(&readline.Config{
		Stdin:       c.InStream,
		HistoryFile: c.HistoryFile,
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
		if err == readline.ErrInterrupt {
			return c.Exit()
		}
		if err != nil {
			c.PrintInteractiveError(err)
			continue
		}

		stmt, err := BuildStatementWithComments(input.statementWithoutComments, input.statement)
		if err != nil {
			c.PrintInteractiveError(err)
			continue
		}

		if _, ok := stmt.(*ExitStatement); ok {
			return c.Exit()
		}

		if s, ok := stmt.(*UseStatement); ok {
			newSession, err := createSession(c.Session.projectId, c.Session.instanceId, s.Database, c.Credential, c.Priority,
				s.Role, c.Endpoint, c.Session.directedRead, c.SkipTLSVerify, c.Session.protoDescriptor)
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

		// Execute the statement.
		ctx, cancel := context.WithCancel(context.Background())
		go handleInterrupt(cancel)
		stop := c.PrintProgressingMark()
		t0 := time.Now()
		result, err := stmt.Execute(ctx, c.Session)
		elapsed := time.Since(t0).Seconds()
		stop()
		if err != nil {
			if spanner.ErrCode(err) == codes.Aborted {
				// Once the transaction is aborted, the underlying session gains higher lock priority for the next transaction.
				// This makes the result of subsequent transaction in spanner-cli inconsistent, so we recreate the client to replace
				// the Cloud Spanner's session with new one to revert the lock priority of the session.
				// See: https://cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions#retrying-aborted-transactions
				c.Session.RecreateClient()
			}
			c.PrintInteractiveError(err)
			cancel()
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
		cancel()
	}
}

func (c *Cli) RunBatch(input string, displayTable bool) int {
	cmds, err := buildCommands(input)
	if err != nil {
		c.PrintBatchError(err)
		return exitCodeError
	}

	ctx, cancel := context.WithCancel(context.Background())
	go handleInterrupt(cancel)

	for _, cmd := range cmds {
		result, err := cmd.Stmt.Execute(ctx, c.Session)
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

func (c *Cli) PrintResult(result *Result, mode DisplayMode, interactive bool) {
	printResult(c.OutStream, result, mode, interactive, c.Verbose)
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

	if c.Session.InReadWriteTransaction() {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "(rw txn)")
	} else if c.Session.InReadOnlyTransaction() {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "(ro txn)")
	} else {
		prompt = promptReInTransaction.ReplaceAllString(prompt, "")
	}

	return prompt
}

func createSession(projectId string, instanceId string, databaseId string, credential []byte,
	priority pb.RequestOptions_Priority, role string, endpoint string, directedRead *pb.DirectedReadOptions,
	skipTLSVerify bool, protoDescriptor []byte) (*Session, error) {
	var opts []option.ClientOption
	if credential != nil {
		opts = append(opts, option.WithCredentialsJSON(credential))
	}
	if endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
	}
	if skipTLSVerify {
		creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
		opts = append(opts, option.WithGRPCDialOption(grpc.WithTransportCredentials(creds)))
	}
	return NewSession(projectId, instanceId, databaseId, priority, role, directedRead, protoDescriptor, opts...)
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

func printResult(out io.Writer, result *Result, mode DisplayMode, interactive, verbose bool) {
	if mode == DisplayModeTable {
		table := tablewriter.NewWriter(out)
		table.SetAutoFormatHeaders(false)
		table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.SetAutoWrapText(false)

		var forceTableRender bool
		// This condition is true if statement is SelectStatement or DmlStatement
		if verbose && len(result.ColumnTypes) > 0 {
			forceTableRender = true
			var headers []string
			for _, field := range result.ColumnTypes {
				typename := formatTypeSimple(field.GetType())
				headers = append(headers, field.GetName()+"\n"+typename)
			}
			table.SetHeader(headers)
		} else {
			table.SetHeader(result.ColumnNames)
		}

		for _, row := range result.Rows {
			table.Append(row.Columns)
		}

		if forceTableRender || len(result.Rows) > 0 {
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

	if len(result.Predicates) > 0 {
		fmt.Fprintln(out, "Predicates(identified by ID):")
		for _, s := range result.Predicates {
			fmt.Fprintf(out, " %s\n", s)
		}
		fmt.Fprintln(out)
	}

	if verbose || result.ForceVerbose {
		fmt.Fprint(out, resultLine(result, true))
	} else if interactive {
		fmt.Fprint(out, resultLine(result, verbose))
	}
}

func resultLine(result *Result, verbose bool) string {
	var timestamp string
	if !result.Timestamp.IsZero() {
		timestamp = result.Timestamp.Format(time.RFC3339Nano)
	}

	if result.IsMutation {
		var affectedRowsPrefix string
		if result.AffectedRowsType == rowCountTypeLowerBound {
			// For Partitioned DML the result's row count is lower bounded number, so we add "at least" to express ambiguity.
			// See https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1?hl=en#resultsetstats
			affectedRowsPrefix = "at least "
		}

		var detail string
		if verbose {
			if timestamp != "" {
				detail += fmt.Sprintf("timestamp:      %s\n", timestamp)
			}
			if result.CommitStats != nil {
				detail += fmt.Sprintf("mutation_count: %d\n", result.CommitStats.GetMutationCount())
			}
		}
		return fmt.Sprintf("Query OK, %s%d rows affected (%s)\n%s",
			affectedRowsPrefix, result.AffectedRows, result.Stats.ElapsedTime, detail)
	}

	var set string
	if result.AffectedRows == 0 {
		set = "Empty set"
	} else {
		set = fmt.Sprintf("%d rows in set", result.AffectedRows)
	}

	if verbose {
		// detail is aligned with max length of key (current: 20)
		var detail string
		if timestamp != "" {
			detail += fmt.Sprintf("timestamp:            %s\n", timestamp)
		}
		if result.Stats.CPUTime != "" {
			detail += fmt.Sprintf("cpu time:             %s\n", result.Stats.CPUTime)
		}
		if result.Stats.RowsScanned != "" {
			detail += fmt.Sprintf("rows scanned:         %s rows\n", result.Stats.RowsScanned)
		}
		if result.Stats.DeletedRowsScanned != "" {
			detail += fmt.Sprintf("deleted rows scanned: %s rows\n", result.Stats.DeletedRowsScanned)
		}
		if result.Stats.OptimizerVersion != "" {
			detail += fmt.Sprintf("optimizer version:    %s\n", result.Stats.OptimizerVersion)
		}
		if result.Stats.OptimizerStatisticsPackage != "" {
			detail += fmt.Sprintf("optimizer statistics: %s\n", result.Stats.OptimizerStatisticsPackage)
		}
		return fmt.Sprintf("%s (%s)\n%s", set, result.Stats.ElapsedTime, detail)
	}
	return fmt.Sprintf("%s (%s)\n", set, result.Stats.ElapsedTime)
}

func buildCommands(input string) ([]*command, error) {
	var cmds []*command
	var pendingDdls []string
	for _, separated := range separateInput(input) {
		// Ignore the last empty statement
		if separated.delim == delimiterUndefined && separated.statementWithoutComments == "" {
			continue
		}

		stmt, err := BuildStatementWithComments(separated.statementWithoutComments, separated.statement)
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

func handleInterrupt(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	cancel()
}
