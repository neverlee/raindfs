package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

	//vgLock sync.Mutex
	//bounedLeaderChan chan int
}

func NewMasterServer(raft *raftlayer.RaftServer, pulse int) *MasterServer {
	ms := &MasterServer{}
	ms.Topo = topology.NewTopology(raft, pulse) //  TODO fix seq

	return ms
}

func (ms *MasterServer) SetMasterServer(r *mux.Router) {
	r.HandleFunc("/ms/vol/{vid}", ms.volumeHandler)
	r.HandleFunc("/ms/node/join", ms.nodeJoinHandler)
	r.HandleFunc("/ms/node/status", ms.nodeStatusHandler)

	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)
	r.HandleFunc("/ping", ms.pingHandler).Methods("GET")

	ms.Topo.StartRefreshWritableVolumes()
}

func (ms *MasterServer) Close() error {
	ms.Topo.Raft.Close()
	return nil
}

func (ms *MasterServer) nodeStatusHandler(w http.ResponseWriter, r *http.Request) {
	ret := ms.Topo.ToData()
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (ms *MasterServer) nodeJoinHandler(w http.ResponseWriter, r *http.Request) {
	if blob, err := ioutil.ReadAll(r.Body); err == nil {
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

func (ms *MasterServer) volumeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr := vars["vid"]
	if vidstr == "_pick" {
		vid, nodes, err := ms.Topo.PickForWrite()
		if err == nil {
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
