package command

import (
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
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
	ip         *string
	port       *int
	metaFolder *string
	cluster    *string
	pulse      *int
	timeout    *int
	maxCpu     *int
	cpuProfile *string
}

var msopt masterServerOption

func init() {
	cmdMaster.Run = runMaster // break init cycle
	msopt = masterServerOption{
		ip:         cmdMaster.Flag.String("ip", "0.0.0.0", "ip address to bind to"),
		port:       cmdMaster.Flag.Int("port", 10000, "http listen port"),
		metaFolder: cmdMaster.Flag.String("mdir", "./meta", "data directory to store meta data"),
		cluster:    cmdMaster.Flag.String("cluster", "", "other master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094"),
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

	ip := *msopt.ip
	port := *msopt.port
	pulse := *msopt.pulse
	metaFolder := *msopt.metaFolder
	timeout := *msopt.timeout
	if err := util.MkdirOrExist(metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", metaFolder, err)
	}

	router := mux.NewRouter()
	_ = server.NewMasterServer(router, port, metaFolder, pulse)

	listeningAddress := ip + ":" + strconv.Itoa(port)

	glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", listeningAddress)

	listener, e := util.NewListener(listeningAddress, time.Duration(timeout)*time.Second)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	if e := http.Serve(listener, router); e != nil {
		glog.Fatalf("Fail to serve: %v", e)
	}
	return true
}
