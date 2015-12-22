package main

import (
	"github.com/neverlee/glog"
	"os"
	"raindfs/config"
	"syscall"
)

func main() {
	var err error
	cfgpath := "vts.conf"
	if len(os.Args) >= 2 {
		cfgpath = os.Args[1]
	}
	glog.Infoln("config path: ", cfgpath)

	if err = config.LoadFile(cfgpath); err != nil {
		glog.Errorf("Load configure file %s fail: %v", cfgpath, err)
		return
	}
	glog.Infoln("Load configure: ", *config.Conf)

	var rLimit syscall.Rlimit
	rLimit.Max = config.Conf.Rlimit
	rLimit.Cur = config.Conf.Rlimit
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		glog.Error("Error Setting Rlimit: ", err)
	}
	startServer()
}
