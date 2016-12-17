package command

import (
	"net/http"
	"runtime"
	"time"

	"raindfs/server"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/neverlee/glog"
)

type volumeServerOption struct {
	addr         *string
	mserver      *string
	data         *string
	pulseSeconds *int
	timeout      *int
	maxCpu       *int
}

var vsopt volumeServerOption

func init() {
	cmdVolume.Run = runVolume // break init cycle
	vsopt = volumeServerOption{
		addr:         cmdVolume.Flag.String("addr", "0.0.0.0:20000", "addr to bind"),
		mserver:      cmdVolume.Flag.String("mserver", "127.0.0.1:10000,", "master hosts"),
		data:         cmdVolume.Flag.String("dir", "./data", "data dir"),
		pulseSeconds: cmdVolume.Flag.Int("pulseseconds", 5, "number of seconds between heartbeats, must be smaller than or equal to the master's setting"),
		timeout:      cmdVolume.Flag.Int("idletimeout", 10, "connection idle seconds"),
		maxCpu:       cmdVolume.Flag.Int("maxcpu", 0, "maximum number of CPUs. 0 means all available CPUs"),
	}
}

var cmdVolume = &Command{
	UsageLine: "volume -dir=/tmp -mserver=localhost:10000",
	Short:     "start a volume server",
	Long: `start a volume server to provide storage spaces

  `,
}

func runVolume(cmd *Command, args []string) bool {
	if *vsopt.maxCpu > 0 {
		runtime.GOMAXPROCS(*vsopt.maxCpu)
	}

	router := mux.NewRouter()

	addr := *vsopt.addr
	data := *vsopt.data
	mserver := *vsopt.mserver
	pulseSeconds := *vsopt.pulseSeconds
	timeout := *vsopt.timeout

	masters := util.StrSplit(mserver, ",")
	if len(masters) < 1 {
		glog.Fatalf("Volume server need mserver addr")
	}

	if err := util.MkdirOrExist(data); err != nil {
		glog.Fatalf("Check data Folder (-dir) Writable %s : %s", vsopt.data, err)
	}

	volumeServer := server.NewVolumeServer(addr, data, masters, router, pulseSeconds)

	glog.V(0).Infoln("Start Rain volume server", util.VERSION, "at", addr)
	listener, e := util.NewListener(addr, time.Duration(timeout)*time.Second)
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
