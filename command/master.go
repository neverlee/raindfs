package command

import (
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"raindfs/server"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/neverlee/glog"
)

var cmdMaster = &Command{
	UsageLine: "master -port=10000",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

type masterServerOption struct {
	addr       *string
	bindAll    *bool
	single     *bool
	metaFolder *string
	clusters   *string
	pulse      *int
	timeout    *int
	maxCpu     *int
	cpuProfile *string
}

var msopt masterServerOption

func init() {
	cmdMaster.Run = runMaster // break init cycle
	msopt = masterServerOption{
		addr:       cmdMaster.Flag.String("addr", "0.0.0.0:10000", "address to bind to"),
		bindAll:    cmdMaster.Flag.Bool("bindall", true, "bind address to 0.0.0.0 default except one ip"),
		metaFolder: cmdMaster.Flag.String("mdir", "./meta", "data directory to store meta data"),
		single:     cmdMaster.Flag.Bool("single", false, "run with single node mode"),
		clusters:   cmdMaster.Flag.String("clusters", "", "master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094"),
		pulse:      cmdMaster.Flag.Int("pulseseconds", 5, "number of seconds between heartbeats"),
		timeout:    cmdMaster.Flag.Int("idletimeout", 10, "connection idle seconds"),
		maxCpu:     cmdMaster.Flag.Int("maxcpu", 0, "maximum number of CPUs. 0 means all available CPUs"),
		cpuProfile: cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file"),
	}
}

func runMaster(cmd *Command, args []string) bool {
	if *msopt.maxCpu > 0 {
		runtime.GOMAXPROCS(*msopt.maxCpu)
	}
	if *msopt.cpuProfile != "" {
		f, err := os.Create(*msopt.cpuProfile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		OnInterrupt(func() {
			pprof.StopCPUProfile()
		})
	}

	addr := *msopt.addr
	bindall := *msopt.bindAll
	single := *msopt.single
	clusters := strings.Split(*msopt.clusters, ",")
	metaFolder := *msopt.metaFolder
	pulse := *msopt.pulse
	timeout := time.Duration(*msopt.timeout) * time.Second
	if err := util.MkdirOrExist(metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", metaFolder, err)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		glog.Fatalf("Master startup error: %v", err)
		return false
	}

	router := mux.NewRouter()
	ms := server.NewMaster(listener, router, addr, bindall, single, clusters, metaFolder, pulse, timeout)
	ms.SetMasterServer(router)

	glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", addr)

	if e := http.Serve(listener, router); e != nil {
		glog.Fatalf("Fail to serve: %v", e)
	}
	return true
}
