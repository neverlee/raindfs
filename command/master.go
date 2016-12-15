package command

import (
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"raindfs/raftlayer"
	"raindfs/server"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/neverlee/glog"
	"github.com/soheilhy/cmux"
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
	metaDir    *string
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
		metaDir:    cmdMaster.Flag.String("mdir", "./meta", "data directory to store meta data"),
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
	clusters := strings.Split(*msopt.clusters, ",")
	metaDir := *msopt.metaDir
	pulse := *msopt.pulse
	timeout := time.Duration(*msopt.timeout) * time.Second
	if err := util.MkdirOrExist(metaDir); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", metaDir, err)
	}

	baddr := addr
	if bindall {
		ipport := strings.Split(addr, ":")
		baddr = "0.0.0.0:" + ipport[1]
	}

	listener, err := net.Listen("tcp", baddr)
	if err != nil {
		glog.Fatalf("Master startup error: %v", err)
		return false
	}

	lmux := cmux.New(listener)
	HTTPListener := &util.Listener{
		Listener:     lmux.Match(cmux.HTTP1Fast()),
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
	RaftListener := lmux.Match(cmux.Any())

	raftserver := raftlayer.NewRaftServer(RaftListener, addr, clusters, metaDir, pulse, timeout)
	go lmux.Serve()

	router := mux.NewRouter()
	ms := server.NewMasterServer(raftserver, pulse)
	if ms == nil {
		glog.Fatalf("Fail to serve")
	}

	ms.SetMasterServer(router)

	glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", addr)

	if e := http.Serve(HTTPListener, router); e != nil {
		glog.Fatalf("Fail to serve: %v", e)
	}
	return true
}
