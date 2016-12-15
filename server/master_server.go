package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"

	"raindfs/operation"
	"raindfs/raftlayer"
	"raindfs/storage"
	"raindfs/topology"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
)

type MasterServer struct {
	addr         string
	clusters     []string
	metaFolder   string
	pulseSeconds int

	Topo *topology.Topology

	raftLayer *raftlayer.RaftLayer
	raftTrans *raft.NetworkTransport
	raft      *raft.Raft

	fsm *raftlayer.FSM
	mux cmux.CMux

	listener     net.Listener
	RaftListener net.Listener
	HTTPListener *util.Listener //net.Listener
	//vgLock sync.Mutex
	//bounedLeaderChan chan int
}

func NewMasterServer(l net.Listener, addr string, bindall bool, clusters []string, metaFolder string, pulse int, timeout time.Duration) *MasterServer {
	ms := &MasterServer{
		addr:         addr,
		metaFolder:   metaFolder,
		pulseSeconds: pulse,
	}

	ms.listener = l
	mux := cmux.New(l)
	ms.HTTPListener = &util.Listener{
		Listener:     mux.Match(cmux.HTTP1Fast()),
		ReadTimeout:  timeout,
		WriteTimeout: timeout,
	}
	ms.RaftListener = mux.Match(cmux.Any())

	if !util.StrInSlice(clusters, addr) {
		return nil
	}

	advertise, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil
	}
	layer := raftlayer.NewRaftLayer(ms.RaftListener, advertise)
	trans := raft.NewNetworkTransport(
		layer,
		len(clusters),
		time.Second,
		os.Stderr,
	)
	fsm := raftlayer.NewFSM()

	// setup raft
	raft, err := raftlayer.NewRaft(metaFolder, fsm, trans, clusters, time.Second, 5)
	if err != nil {
		// TODO log err
		return nil
	}

	ms.raftLayer = layer
	ms.raftTrans = trans
	ms.raft = raft
	ms.fsm = fsm
	ms.mux = mux

	return ms
}

func (ms *MasterServer) SetMasterServer(r *mux.Router) {
	ms.Topo = topology.NewTopology(nil, ms.pulseSeconds) //  TODO fix seq

	//r.HandleFunc("/", ms.uiStatusHandler) r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
	//r.HandleFunc("/dir/status", ms.proxyToLeader(ms.dirStatusHandler))
	//r.HandleFunc("/vol/grow",   ms.proxyToLeader(ms.volumeGrowHandler))
	//r.HandleFunc("/vol/status", ms.proxyToLeader(ms.volumeStatusHandler))
	//r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.volumeVacuumHandler))
	//r.HandleFunc("/submit", ms.submitFromMasterServerHandler)

	r.HandleFunc("/admin/assign_fileid", ms.assignFileidHandler)

	r.HandleFunc("/node/join", ms.nodeJoinHandler) // proxy
	r.HandleFunc("/cluster/status", ms.clusterStatusHandler)

	r.HandleFunc("/stats/nodes", ms.statsNodesHandler)
	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)

	r.HandleFunc("/test", ms.testHandler)

	ms.Topo.StartRefreshWritableVolumes()
}

func (ms *MasterServer) Serve() error {
	go ms.mux.Serve()
	return nil
}

func (ms *MasterServer) Close() error {
	ms.raftLayer.Close()
	ms.raftTrans.Close()
	ret := ms.raft.Shutdown()
	// wait raft shutdown
	ret.Error()
	ms.listener.Close()
	return nil
}

func (ms *MasterServer) clusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	// leader 放最前面
	ret := operation.ClusterStatusResult{
		Leader:   ms.addr,
		Clusters: ms.clusters,
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (ms *MasterServer) statsNodesHandler(w http.ResponseWriter, r *http.Request) {
	ret := ms.Topo.ToData()
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (ms *MasterServer) testHandler(w http.ResponseWriter, r *http.Request) {
	ret := ms.Topo.ToData()
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (ms *MasterServer) nodeJoinHandler(w http.ResponseWriter, r *http.Request) {
	//glog.Extraln(">>>>>>", r.RemoteAddr)
	if blob, err := ioutil.ReadAll(r.Body); err == nil {
		fmt.Fprint(w, string(blob))
		var jmsg operation.JoinMessage
		if jerr := json.Unmarshal(blob, &jmsg); jerr == nil {
			jmsg.Addr = r.RemoteAddr
			//if strings.HasPrefix(jmsg.Ip, "0.0.0.0") || strings.HasPrefix(jmsg.Ip, "[::]") {
			//}
			ms.Topo.ProcessJoinMessage(&jmsg)
		}
	}
}

func (ms *MasterServer) assignFileidHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, err := ms.Topo.PickForWrite()
	if err == nil {
		key := util.GenID()
		fid := storage.NewFileId(vid, key)
		ret := operation.AssignResult{
			Fid: fid.String(),
		}
		writeJsonQuiet(w, r, http.StatusOK, ret)
		return
	}
	writeJsonError(w, r, http.StatusOK, err)
}
