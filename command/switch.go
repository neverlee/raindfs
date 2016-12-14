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
	masters *string
	//pulseSeconds          *int
	//idleConnectionTimeout *int
	//maxCpu                *int
}

var ssopt switchServerOption

func init() {
	cmdSwitch.Run = runSwitch // break init cycle
	ssopt = switchServerOption{
		addr:    cmdSwitch.Flag.String("addr", "0.0.0.0:20200", "ip or server name"),
		masters: cmdSwitch.Flag.String("master", "127.0.0.1:10000,", "master hosts"),
	}
}

var cmdSwitch = &Command{
	UsageLine: "switch -addr=8080 -mserver=localhost:9333",
	Short:     "start a switch server",
	Long: `start a switch server to provide api for client

  `,
}

func runSwitch(cmd *Command, args []string) bool {
	//if *ssopt.maxCpu > 0 {
	//	runtime.GOMAXPROCS(*ssopt.maxCpu)
	//}

	router := mux.NewRouter()

	addr := *ssopt.addr
	masters := *ssopt.masters

	_ = server.NewSwitchServer(addr, masters, router)

	glog.V(0).Infoln("Start Rain switch server", util.VERSION, "at", addr)
	listener, e := util.NewListener(addr, 10*time.Second) // TODO  timeout
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
