package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	for {
		fmt.Printf("> ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		input := scanner.Text()
		fmt.Printf("OK! We'll try to do %s\n", input)
	}
}
