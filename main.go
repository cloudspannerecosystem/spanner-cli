package main

import (
	"io/ioutil"
	"log"
	"os"

	flags "github.com/jessevdk/go-flags"
)

type options struct {
	ProjectId  string `short:"p" long:"project" required:"true" description:"(required) GCP Project ID."`
	InstanceId string `short:"i" long:"instance" required:"true" description:"(required) Cloud Spanner Instance ID"`
	DatabaseId string `short:"d" long:"database" required:"true" description:"(required) Cloud Spanner Database ID."`
	Execute    string `short:"e" long:"execute" description:"Execute SQL statement and quit."`
	Table      bool   `short:"t" long:"table" description:"Display output in table format for batch mode."`
	Prompt     string `long:"prompt" description:"Set the prompt to the specified format"`
}

func main() {
	var opts options
	parser := flags.NewParser(&opts, flags.Default)
	if _, err := parser.Parse(); err != nil {
		os.Exit(-1)
	}

	cli, err := NewCli(opts.ProjectId, opts.InstanceId, opts.DatabaseId, opts.Prompt)
	if err != nil {
		log.Fatalf("failed to connect to Spanner: %s", err)
	}

	stdin, err := readStdin()
	if err != nil {
		panic(err)
	}

	if opts.Execute != "" {
		cli.RunBatch(opts.Execute, opts.Table)
	} else if stdin != "" {
		cli.RunBatch(stdin, opts.Table)
	} else {
		cli.RunInteractive()
	}
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
