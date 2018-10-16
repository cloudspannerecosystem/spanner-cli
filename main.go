package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"

	flags "github.com/jessevdk/go-flags"
)

type options struct {
	ProjectId  string `short:"p" long:"project" description:"GCP Project ID. (default: gcloud config value of \"core/project\")"`
	InstanceId string `short:"i" long:"instance" description:"Cloud Spanner Instance ID. (default: gcloud config value of \"spanner/instance\")"`
	DatabaseId string `short:"d" long:"database" description:"Cloud Spanner Database ID." required:"true"`
	Execute    string `short:"e" long:"execute" description:"Execute SQL statement and quit."`
	Table      bool   `short:"t" long:"table" description:"Display output in table format for batch mode."`
}

func main() {
	var opts options
	parser := flags.NewParser(&opts, flags.Default)
	if _, err := parser.Parse(); err != nil {
		os.Exit(-1)
	}

	if opts.ProjectId == "" {
		opts.ProjectId = getDefaultProjectId()
	}
	if opts.InstanceId == "" {
		opts.InstanceId = getDefaultInstanceId()
	}

	if opts.ProjectId == "" || opts.InstanceId == "" {
		parser.WriteHelp(os.Stderr)
		os.Exit(-1)
	}

	cli, err := NewCli(opts.ProjectId, opts.InstanceId, opts.DatabaseId)
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

func getDefaultProjectId() string {
	cmd := exec.Command("gcloud", "config", "get-value", "core/project")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(string(out), "\n")
}

func getDefaultInstanceId() string {
	cmd := exec.Command("gcloud", "config", "get-value", "spanner/instance")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSuffix(string(out), "\n")
}
