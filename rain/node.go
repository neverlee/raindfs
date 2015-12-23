package main

import (
	"github.com/neverlee/raindfs/glog"
)

func init() {
	cmdNode.Run = runNode // break init cycle
}

var cmdNode = &Command{
	UsageLine: "node -port=9333",
	Short:     "start a node storager",
	Long:      `start a node storager to manage the disks and files`,
}

var (
	mport = cmdNode.Flag.Int("port", 9333, "http listen port")
)

func runNode(cmd *Command, args []string) bool {
	glog.V(0).Infoln("Start Node")
	return true
}
