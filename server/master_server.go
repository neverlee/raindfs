package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"raindfs/operation"
	"raindfs/raftlayer"
	"raindfs/storage"
	"raindfs/topology"

	"github.com/gorilla/mux"
)

type MasterServer struct {
	Topo *topology.Topology

	listener net.Listener
	//vgLock sync.Mutex
	//bounedLeaderChan chan int
}

func NewMasterServer(raft *raftlayer.RaftServer, pulse int) *MasterServer {
	ms := &MasterServer{}
	ms.Topo = topology.NewTopology(raft, pulse) //  TODO fix seq

	return ms
}

func (ms *MasterServer) SetMasterServer(r *mux.Router) {
	r.HandleFunc("/volume/{vid}", ms.pickVolumeHandler)

	r.HandleFunc("/node/join", ms.nodeJoinHandler) // proxy
	r.HandleFunc("/cluster/status", ms.clusterStatusHandler)

	r.HandleFunc("/stats/nodes", ms.statsNodesHandler)
	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)

	r.HandleFunc("/ping", ms.pingHandler)

	ms.Topo.StartRefreshWritableVolumes()
}

func (ms *MasterServer) Close() error {
	ms.Topo.Raft.Close()
	ms.listener.Close()
	return nil
}

func (ms *MasterServer) clusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	// leader 放最前面
	ret := operation.ClusterStatusResult{
		//Leader:   ms.Topo.Raft.Leader(),
		Clusters: ms.Topo.Raft.Peers(),
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (ms *MasterServer) statsNodesHandler(w http.ResponseWriter, r *http.Request) {
	ret := ms.Topo.ToData()
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (ms *MasterServer) nodeJoinHandler(w http.ResponseWriter, r *http.Request) {
	//glog.Extraln(">>>>>>", r.RemoteAddr)
	if blob, err := ioutil.ReadAll(r.Body); err == nil {
		fmt.Fprint(w, string(blob))
		var jmsg operation.JoinMessage
		if jerr := json.Unmarshal(blob, &jmsg); jerr == nil {
			if strings.HasPrefix(jmsg.Addr, "0.0.0.0") { // strings.HasPrefix(jmsg.Ip, "[::]")
				inaddr := strings.Split(r.RemoteAddr, ":")
				upaddr := strings.Split(jmsg.Addr, ":")
				jmsg.Addr = fmt.Sprintf("%s:%s", inaddr[0], upaddr[1])
			}
			ms.Topo.ProcessJoinMessage(&jmsg)
		}
	}
}

func (ms *MasterServer) pickVolumeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr := vars["vid"]
	if vidstr == "_writable" {
		vid, nodes, err := ms.Topo.PickForWrite()
		if err == nil {
			// key := util.GenID() fid := storage.NewFileId(vid, key)
			ret := operation.PickResult{
				Vid:   vid.String(),
				Nodes: nodes.ToNameList(),
			}
			writeJsonQuiet(w, r, http.StatusOK, ret)
		} else {
			writeJsonError(w, r, http.StatusOK, err)
		}
	} else {
		if vid, err := storage.NewVolumeId(vidstr); err == nil {
			dns := ms.Topo.Lookup(vid)
			nodes := make([]string, len(dns))
			for i, dn := range dns {
				nodes[i] = dn.Url()
			}
			ret := operation.PickResult{
				Vid:   vid.String(),
				Nodes: nodes,
			}
			writeJsonQuiet(w, r, http.StatusOK, ret)
		} else {
			writeJsonError(w, r, http.StatusOK, err)
		}
	}
}

func (ms *MasterServer) pingHandler(w http.ResponseWriter, r *http.Request) {
	ret := ms.Topo.ToData()
	writeJsonQuiet(w, r, http.StatusOK, ret)
	//writeJsonQuiet(w, r, http.StatusOK, "ping")
}
