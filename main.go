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

// Package main is a command line tool for Cloud Spanner
package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"

	flags "github.com/jessevdk/go-flags"
)

type globalOptions struct {
	Spanner spannerOptions `group:"spanner"`
}

type spannerOptions struct {
	ProjectId  string `short:"p" long:"project" env:"SPANNER_PROJECT_ID" description:"(required) GCP Project ID."`
	InstanceId string `short:"i" long:"instance" env:"SPANNER_INSTANCE_ID" description:"(required) Cloud Spanner Instance ID"`
	DatabaseId string `short:"d" long:"database" env:"SPANNER_DATABASE_ID" description:"(required) Cloud Spanner Database ID."`
	Execute    string `short:"e" long:"execute" description:"Execute SQL statement and quit."`
	File       string `short:"f" long:"file" description:"Execute SQL statement from file and quit."`
	Table      bool   `short:"t" long:"table" description:"Display output in table format for batch mode."`
	Verbose    bool   `short:"v" long:"verbose" description:"Display verbose output."`
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

	if opts.File != "" && opts.Execute != "" {
		exitf("Invalid combination: -e, -f are exclusive\n")
	}
	var cred []byte
	if opts.Credential != "" {
		var err error
		if cred, err = readCredentialFile(opts.Credential); err != nil {
			exitf("Failed to read the credential file: %v\n", err)
		}
	}

	cli, err := NewCli(opts.ProjectId, opts.InstanceId, opts.DatabaseId, opts.Prompt, cred, os.Stdin, os.Stdout, os.Stderr, opts.Verbose)
	if err != nil {
		exitf("Failed to connect to Spanner: %v", err)
	}

	var input string
	if opts.Execute != "" {
		input = opts.Execute
	} else if opts.File == "-" {
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			exitf("Read from stdin failed: %v", err)
		}
		input = string(b)
	} else if opts.File != "" {
		b, err := ioutil.ReadFile(opts.File)
		if err != nil {
			exitf("Read from file %v failed: %v", opts.File, err)
		}
		input = string(b)
	} else {
		input, err = readStdin()
		if err != nil {
			exitf("Read from stdin failed: %v", err)
		}
	}

	var exitCode int
	if input != "" {
		exitCode = cli.RunBatch(input, opts.Table)
	} else {
		exitCode = cli.RunInteractive()
	}
	os.Exit(exitCode)
}

func exitf(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
	os.Exit(1)
}

const cnfFileName = ".spanner_cli.cnf"

func readConfigFile(parser *flags.Parser) error {
	currentUser, err := user.Current()
	if err != nil {
		// ignore error
		return nil
	}

	// TODO: customize config path
	homeCnfFile := filepath.Join(currentUser.HomeDir, cnfFileName)

	cwd, _ := os.Getwd() // ignore err
	cwdCnfFile := filepath.Join(cwd, cnfFileName)

	iniParser := flags.NewIniParser(parser)
	for _, cnfFile := range []string{homeCnfFile, cwdCnfFile} {
		if _, err := os.Stat(cnfFile); err != nil {
			continue
		}
		if err := iniParser.ParseFile(cnfFile); err != nil {
			return err
		}
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
