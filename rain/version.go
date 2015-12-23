package main

import (
	"fmt"
	"runtime"
)

const (
	VERSION = "0.1"
)

var cmdVersion = &Command{
	Run:       runVersion,
	UsageLine: "version",
	Short:     "print RainDFS version",
	Long:      `Version prints the RainDFS version`,
}

func runVersion(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	fmt.Printf("version %s %s %s\n", VERSION, runtime.GOOS, runtime.GOARCH)
	return true
}
