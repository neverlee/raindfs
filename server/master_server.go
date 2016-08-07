package server

import (
	"fmt"
	"net/http"

	"raindfs/operation"
	"raindfs/topology"

	"github.com/gorilla/mux"
)

type MasterServer struct {
	port         int
	metaFolder   string
	pulseSeconds int

	router *mux.Router

	//Topo   *topology.Topology
	//vg     *topology.VolumeGrowth
	//vgLock sync.Mutex

	//bounedLeaderChan chan int
}

func NewMasterServer(r *mux.Router, port int, metaFolder string, pulseSeconds int) *MasterServer {
	ms := &MasterServer{
		port:         port,
		metaFolder:   metaFolder,
		pulseSeconds: pulseSeconds,
		router:       r,
	}

	//r.HandleFunc("/", ms.uiStatusHandler) r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
	//r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.dirAssignHandler))
	//r.HandleFunc("/dir/lookup", ms.proxyToLeader(ms.dirLookupHandler))
	//r.HandleFunc("/dir/join",   ms.proxyToLeader(ms.dirJoinHandler))
	//r.HandleFunc("/dir/status", ms.proxyToLeader(ms.dirStatusHandler))
	//r.HandleFunc("/col/delete", ms.proxyToLeader(ms.collectionDeleteHandler))
	//r.HandleFunc("/vol/lookup", ms.proxyToLeader(ms.volumeLookupHandler))
	//r.HandleFunc("/vol/grow",   ms.proxyToLeader(ms.volumeGrowHandler))
	//r.HandleFunc("/vol/status", ms.proxyToLeader(ms.volumeStatusHandler))
	//r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.volumeVacuumHandler))
	//r.HandleFunc("/submit", ms.submitFromMasterServerHandler)
	//r.HandleFunc("/delete", ms.deleteFromMasterServerHandler)
	//r.HandleFunc("/{fileId}",   ms.proxyToLeader(ms.redirectHandler))

	r.HandleFunc("/cluster/status", ms.clusterStatusHandler)

	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)

	return ms
}

func (m *MasterServer) clusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	hi := fmt.Sprintf("127.0.0.1:%d", m.port)
	ret := operation.ClusterStatusResult{
		Leader:   hi,
		LeaderId: 0,
		Clusters: []string{hi},
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}
