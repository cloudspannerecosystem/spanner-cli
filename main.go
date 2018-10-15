package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

var usage = `Usage:
    spanner-cli [options...]

Example:
    spanner-cli --project=myproject --instance=myinstance --database=mydb

Options:
    --project=PROJECT   (optional)    GCP Project ID            (default: gcloud config value of "core/project")
    --instance=INSTANCE (optional)    Cloud Spanner Instance ID (default: gcloud config value of "spanner/instance")
    --database=DATABASE (required)    Cloud Spanner Database ID
`

func main() {
	var projectId string
	var instanceId string
	var databaseId string

	flag.StringVar(&projectId, "project", "", "")
	flag.StringVar(&instanceId, "instance", "", "")
	flag.StringVar(&databaseId, "database", "", "")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if projectId == "" {
		projectId = getDefaultProjectId()
	}
	if instanceId == "" {
		instanceId = getDefaultInstanceId()
	}

	if projectId == "" || instanceId == "" || databaseId == "" {
		flag.Usage()
		os.Exit(-1)
	}

	cli, err := NewCli(projectId, instanceId, databaseId)
	if err != nil {
		log.Fatalf("failed to connect to Spanner: %s", err)
	}

	cli.Run()
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
