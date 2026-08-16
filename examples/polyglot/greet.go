package main

import (
	"fmt"
	"os/exec"
	"strings"
)

func main() {
	output, err := exec.Command(
		"tickr-ctx", "get", "greeting", "--signal", "--default", "Hello from Tickr",
	).Output()
	if err != nil {
		panic(err)
	}
	fmt.Printf("go: %s\n", strings.TrimSpace(string(output)))
}
