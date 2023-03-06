package main

import (
	"fmt"
	"regexp"
)

func main() {
	re := regexp.MustCompile(`(?is)^USE\s+(.+?)(?:\s+WITH\s+ROLE\s+(.+))?$`)
	for _, s := range []string{
		"USE database1 a",
		"USE database1 WITH ROLE",
		"USE database1 WITH ROLE role1",
	} {
		fmt.Printf("%#v\n", re.FindStringSubmatch(s))
	}
}
