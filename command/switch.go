package command

import (
	"net/http"
	"time"

	"raindfs/server"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/neverlee/glog"
)

type switchServerOption struct {
	addr    *string
	mserver *string
	timeout *int
	//pulseSeconds          *int
	//idleConnectionTimeout *int
	//maxCpu                *int
}

var ssopt switchServerOption

func init() {
	cmdSwitch.Run = runSwitch // break init cycle
	ssopt = switchServerOption{
		addr:    cmdSwitch.Flag.String("addr", "0.0.0.0:10100", "switch http server bind addr"),
		mserver: cmdSwitch.Flag.String("mserver", "", "raindfs masters addr"),
		timeout: cmdMaster.Flag.Int("idletimeout", 10, "connection idle seconds"),
	}
}

var cmdSwitch = &Command{
	UsageLine: "switch -addr=8080 -mserver=127.0.0.1:10000,127.0.0.1:10001",
	Short:     "start a switch server",
	Long: `start a switch server to provide api for client operation

  `,
}

func runSwitch(cmd *Command, args []string) bool {
	//if *ssopt.maxCpu > 0 {
	//	runtime.GOMAXPROCS(*ssopt.maxCpu)
	//}

	router := mux.NewRouter()

	addr := *ssopt.addr
	mserver := *ssopt.mserver
	timeout := *ssopt.timeout

	masters := util.StrSplit(mserver, ",")
	if len(masters) < 1 {
		glog.Fatalf("Switch server need mserver addr")
	}

	_ = server.NewSwitchServer(masters, router)

	glog.V(0).Infoln("Start Rain switch server", util.VERSION, "at", addr)
	listener, e := util.NewListener(addr, time.Duration(timeout)*time.Second)
	if e != nil {
		glog.Fatalf("Switch server listener error:%v", e)
	}

	OnInterrupt(func() {
		//switchServer.Shutdown()
	})

	if e := http.Serve(listener, router); e != nil {
		glog.Fatalf("Switch server fail to serve: %v", e)
	}
	return true
}
