package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"

	flags "github.com/jessevdk/go-flags"
)

type GlobalOptions struct {
	Spanner SpannerOptions `group:"spanner"`
}

type SpannerOptions struct {
	ProjectId  string `short:"p" long:"project" description:"(required) GCP Project ID."`
	InstanceId string `short:"i" long:"instance" description:"(required) Cloud Spanner Instance ID"`
	DatabaseId string `short:"d" long:"database" description:"(required) Cloud Spanner Database ID."`
	Execute    string `short:"e" long:"execute" description:"Execute SQL statement and quit."`
	Table      bool   `short:"t" long:"table" description:"Display output in table format for batch mode."`
	Prompt     string `long:"prompt" description:"Set the prompt to the specified format"`
}

func main() {
	var gopts GlobalOptions
	parser := flags.NewParser(&gopts, flags.Default)

	// check config file at first
	if err := readConfigFile(parser); err != nil {
		panic("invalid config file format")
	}

	// then, parse command line options
	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	opts := gopts.Spanner
	if opts.ProjectId == "" || opts.InstanceId == "" || opts.DatabaseId == "" {
		parser.WriteHelp(os.Stderr)
		os.Exit(1)
	}

	cli, err := NewCli(opts.ProjectId, opts.InstanceId, opts.DatabaseId, opts.Prompt)
	if err != nil {
		log.Fatalf("failed to connect to Spanner: %s", err)
	}

	stdin, err := readStdin()
	if err != nil {
		panic(err)
	}

	var exitCode int
	if opts.Execute != "" {
		exitCode = cli.RunBatch(opts.Execute, opts.Table)
	} else if stdin != "" {
		exitCode = cli.RunBatch(stdin, opts.Table)
	} else {
		exitCode = cli.RunInteractive()
	}
	os.Exit(exitCode)
}

func readConfigFile(parser *flags.Parser) error {
	currentUser, err := user.Current()
	if err != nil {
		// ignore error
		return nil
	}

	// TODO: customize config path
	cnfFile := path.Join(currentUser.HomeDir, ".spanner_cli.cnf")

	// check config file existence
	if _, err := os.Stat(cnfFile); err != nil {
		return nil
	}

	iniParser := flags.NewIniParser(parser)
	if err := iniParser.ParseFile(cnfFile); err != nil {
		return err
	}

	return nil
}

func readStdin() (string, error) {
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return "", err
		}
		return string(b), nil
	} else {
		return "", nil
	}
}
