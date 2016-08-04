package command

import (
	"net/http"
	"runtime"
	"strconv"
	"time"

	"raindfs/server"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/neverlee/glog"
)

type volumeServerOption struct {
	ip                    string
	port                  int
	master                string
	data                  string
	pulseSeconds          int
	idleConnectionTimeout int
	maxCpu                int
}

var vsopt volumeServerOption

func init() {
	cmdVolume.Run = runVolume // break init cycle
	vsopt = volumeServerOption{
		ip:                    *cmdVolume.Flag.String("ip", "0.0.0.0", "ip or server name"),
		port:                  *cmdVolume.Flag.Int("port", 8080, "http listen port"),
		master:                *cmdVolume.Flag.String("master", "127.0.0.1:10000", "master host"),
		data:                  *cmdVolume.Flag.String("-dir", "./data", "data dir"),
		pulseSeconds:          *cmdVolume.Flag.Int("pulseseconds", 5, "number of seconds between heartbeats, must be smaller than or equal to the master's setting"),
		idleConnectionTimeout: *cmdVolume.Flag.Int("idletimeout", 10, "connection idle seconds"),
		maxCpu:                *cmdVolume.Flag.Int("maxcpu", 0, "maximum number of CPUs. 0 means all available CPUs"),
	}
}

var cmdVolume = &Command{
	UsageLine: "volume -port=8080 -dir=/tmp -max=5 -ip=server_name -mserver=localhost:9333",
	Short:     "start a volume server",
	Long: `start a volume server to provide storage spaces

  `,
}

func runVolume(cmd *Command, args []string) bool {
	if vsopt.maxCpu > 0 {
		runtime.GOMAXPROCS(vsopt.maxCpu)
	}

	router := mux.NewRouter()

	volumeServer := server.NewVolumeServer(vsopt.ip, vsopt.port, vsopt.data, router, vsopt.pulseSeconds)

	listeningAddress := vsopt.ip + ":" + strconv.Itoa(vsopt.port)
	glog.V(0).Infoln("Start Seaweed volume server", util.VERSION, "at", listeningAddress)
	listener, e := util.NewListener(listeningAddress, time.Duration(vsopt.idleConnectionTimeout)*time.Second)
	if e != nil {
		glog.Fatalf("Volume server listener error:%v", e)
	}

	OnInterrupt(func() {
		volumeServer.Shutdown()
	})

	if e := http.Serve(listener, router); e != nil {
		glog.Fatalf("Volume server fail to serve: %v", e)
	}
	return true
}
