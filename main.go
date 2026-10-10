/*
Copyright © 2026
*/
package main

import (
	"fmt"
	"os"

	"github.com/loicalleyne/bufka/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
