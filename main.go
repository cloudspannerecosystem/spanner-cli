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
	"io"
	"os"
	"os/user"
	"path/filepath"
	"runtime/debug"

	pb "cloud.google.com/go/spanner/apiv1/spannerpb"
	flags "github.com/jessevdk/go-flags"
)

type globalOptions struct {
	Spanner spannerOptions `group:"spanner"`
}

type spannerOptions struct {
	ProjectId           string `short:"p" long:"project" env:"SPANNER_PROJECT_ID" description:"(required) GCP Project ID."`
	InstanceId          string `short:"i" long:"instance" env:"SPANNER_INSTANCE_ID" description:"(required) Cloud Spanner Instance ID"`
	DatabaseId          string `short:"d" long:"database" env:"SPANNER_DATABASE_ID" description:"(required) Cloud Spanner Database ID."`
	Execute             string `short:"e" long:"execute" description:"Execute SQL statement and quit."`
	File                string `short:"f" long:"file" description:"Execute SQL statement from file and quit."`
	Table               bool   `short:"t" long:"table" description:"Display output in table format for batch mode."`
	Verbose             bool   `short:"v" long:"verbose" description:"Display verbose output."`
	Credential          string `long:"credential" description:"Use the specific credential file"`
	Prompt              string `long:"prompt" description:"Set the prompt to the specified format"`
	HistoryFile         string `long:"history" description:"Set the history file to the specified path"`
	Priority            string `long:"priority" description:"Set default request priority (HIGH|MEDIUM|LOW)"`
	Role                string `long:"role" description:"Use the specific database role"`
	Endpoint            string `long:"endpoint" description:"Set the Spanner API endpoint (host:port)"`
	DirectedRead        string `long:"directed-read" description:"Directed read option (replica_location:replica_type). The replicat_type is optional and either READ_ONLY or READ_WRITE"`
	SkipTLSVerify       bool   `long:"skip-tls-verify" description:"Insecurely skip TLS verify"`
	ProtoDescriptorFile string `long:"proto-descriptor-file" description:"Path of a file that contains a protobuf-serialized google.protobuf.FileDescriptorSet message to use in this invocation."`
	Version             bool   `long:"version" description:"Show version of spanner-cli"`
}

func main() {
	var gopts globalOptions
	// process config files at first
	if err := readConfigFile(flags.NewParser(&gopts, flags.Default)); err != nil {
		exitf("Invalid config file format\n")
	}

	// then, process environment variables and command line options
	// use another parser to process environment variable
	if _, err := flags.NewParser(&gopts, flags.Default).Parse(); err != nil {
		exitf("Invalid options\n")
	}

	if gopts.Spanner.Version {
		fmt.Fprintf(os.Stdout, "%s\n", versionInfo())
		os.Exit(0)
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

	var priority pb.RequestOptions_Priority
	if opts.Priority != "" {
		var err error
		priority, err = parsePriority(opts.Priority)
		if err != nil {
			exitf("priority must be either HIGH, MEDIUM, or LOW\n")
		}
	}

	var directedRead *pb.DirectedReadOptions
	if opts.DirectedRead != "" {
		var err error
		directedRead, err = parseDirectedReadOption(opts.DirectedRead)
		if err != nil {
			exitf("Invalid directed read option: %v\n", err)
		}
	}

	// Don't need to unmarshal into descriptorpb.FileDescriptorSet because the UpdateDDL API just accepts []byte.
	var protoDescriptor []byte
	if opts.ProtoDescriptorFile != "" {
		var err error
		protoDescriptor, err = os.ReadFile(opts.ProtoDescriptorFile)
		if err != nil {
			exitf("Failed to read proto descriptor file: %v\n", err)
		}
	}

	cli, err := NewCli(opts.ProjectId, opts.InstanceId, opts.DatabaseId, opts.Prompt, opts.HistoryFile, cred,
		os.Stdin, os.Stdout, os.Stderr, opts.Verbose, priority, opts.Role, opts.Endpoint, directedRead,
		opts.SkipTLSVerify, protoDescriptor)
	if err != nil {
		exitf("Failed to connect to Spanner: %v", err)
	}

	var input string
	if opts.Execute != "" {
		input = opts.Execute
	} else if opts.File == "-" {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			exitf("Read from stdin failed: %v", err)
		}
		input = string(b)
	} else if opts.File != "" {
		b, err := os.ReadFile(opts.File)
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
	var cnfFiles []string
	if currentUser, err := user.Current(); err == nil {
		cnfFiles = append(cnfFiles, filepath.Join(currentUser.HomeDir, cnfFileName))
	}

	cwd, _ := os.Getwd() // ignore err
	cwdCnfFile := filepath.Join(cwd, cnfFileName)
	cnfFiles = append(cnfFiles, cwdCnfFile)

	iniParser := flags.NewIniParser(parser)
	for _, cnfFile := range cnfFiles {
		// skip if missing
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
	return io.ReadAll(f)
}

func readStdin() (string, error) {
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return "", err
		}
		return string(b), nil
	} else {
		return "", nil
	}
}

func versionInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	return info.Main.Version
}
