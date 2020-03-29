// Package main is a command line tool for Cloud Spanner
package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"

	flags "github.com/jessevdk/go-flags"
)

type globalOptions struct {
	Spanner spannerOptions `group:"spanner"`
}

type spannerOptions struct {
	ProjectId  string `short:"p" long:"project" description:"(required) GCP Project ID."`
	InstanceId string `short:"i" long:"instance" description:"(required) Cloud Spanner Instance ID"`
	DatabaseId string `short:"d" long:"database" description:"(required) Cloud Spanner Database ID."`
	Execute    string `short:"e" long:"execute" description:"Execute SQL statement and quit."`
	Table      bool   `short:"t" long:"table" description:"Display output in table format for batch mode."`
	Credential string `long:"credential" description:"Use the specific credential file"`
	Prompt     string `long:"prompt" description:"Set the prompt to the specified format"`
}

func main() {
	var gopts globalOptions
	parser := flags.NewParser(&gopts, flags.Default)

	// check config file at first
	if err := readConfigFile(parser); err != nil {
		exitf("Invalid config file format\n")
	}

	// then, parse command line options
	if _, err := parser.Parse(); err != nil {
		exitf("Invalid options\n")
	}

	opts := gopts.Spanner
	if opts.ProjectId == "" || opts.InstanceId == "" || opts.DatabaseId == "" {
		exitf("Missing parameters: -p, -i, -d are required\n")
	}

	var cred []byte
	if opts.Credential != "" {
		var err error
		if cred, err = readCredentialFile(opts.Credential); err != nil {
			exitf("Failed to read the credential file: %v\n", err)
		}
	}

	cli, err := NewCli(opts.ProjectId, opts.InstanceId, opts.DatabaseId, opts.Prompt, cred, os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		exitf("Failed to connect to Spanner: %v", err)
	}

	stdin, err := readStdin()
	if err != nil {
		exitf("Read from stdin failed: %v", err)
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

func exitf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
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

func readCredentialFile(filepath string) ([]byte, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(f)
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
