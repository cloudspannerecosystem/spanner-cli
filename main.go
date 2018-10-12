package main

import (
	"flag"
	"log"
)

func main() {
	var projectId string
	var instanceId string
	var databaseId string

	flag.StringVar(&projectId, "project", "", "")
	flag.StringVar(&instanceId, "instance", "", "")
	flag.StringVar(&databaseId, "database", "", "")
	flag.Parse()

	cli, err := NewCli(projectId, instanceId, databaseId)
	if err != nil {
		log.Fatalf("failed to connect to Spanner: %s", err)
	}

	cli.Run()
}
