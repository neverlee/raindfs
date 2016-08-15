package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"

	"raindfs/operation"
	"raindfs/sequence"
	"raindfs/storage"
	"raindfs/topology"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/neverlee/glog"
)

const (
	seqFileName = "seq.json"
)

type MasterServer struct {
	port         int
	metaFolder   string
	pulseSeconds int

	router *mux.Router

	Topo *topology.Topology
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
	seq := sequence.NewSequencer(path.Join(metaFolder, seqFileName)) // setMax
	ms.Topo = topology.NewTopology(seq, ms.pulseSeconds)

	//r.HandleFunc("/", ms.uiStatusHandler) r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
	//r.HandleFunc("/dir/status", ms.proxyToLeader(ms.dirStatusHandler))
	//r.HandleFunc("/vol/lookup", ms.proxyToLeader(ms.volumeLookupHandler))
	//r.HandleFunc("/vol/grow",   ms.proxyToLeader(ms.volumeGrowHandler))
	//r.HandleFunc("/vol/status", ms.proxyToLeader(ms.volumeStatusHandler))
	//r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.volumeVacuumHandler))
	//r.HandleFunc("/submit", ms.submitFromMasterServerHandler)
	//r.HandleFunc("/delete", ms.deleteFromMasterServerHandler)
	//r.HandleFunc("/{fileId}",   ms.proxyToLeader(ms.redirectHandler))
	//r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.dirAssignHandler))
	//r.HandleFunc("/dir/lookup", ms.proxyToLeader(ms.dirLookupHandler))

	r.HandleFunc("/admin/assign_fileid", ms.assignFileidHandler)
	r.HandleFunc("/admin/put/{fid}", ms.putHandler)
	r.HandleFunc("/admin/get/{fid}", ms.getHandler)
	r.HandleFunc("/admin/delete/{fid}", ms.deleteHandler)


	r.HandleFunc("/node/join", ms.nodeJoinHandler) // proxy
	r.HandleFunc("/cluster/status", ms.clusterStatusHandler)

	r.HandleFunc("/stats/nodes", ms.statsNodesHandler)
	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)

	r.HandleFunc("/test", ms.testHandler)

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

func (m *MasterServer) statsNodesHandler(w http.ResponseWriter, r *http.Request) {
	ret := m.Topo.ToMap()
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (m *MasterServer) testHandler(w http.ResponseWriter, r *http.Request) {
	ret := m.Topo.ToMap()
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (m *MasterServer) nodeJoinHandler(w http.ResponseWriter, r *http.Request) {
	ip, _ := util.Ipport(r.RemoteAddr)
	glog.Extraln(">>>>>>", ip)
	if blob, err := ioutil.ReadAll(r.Body); err == nil {
		glog.Extraln(">>>", string(blob))
		fmt.Fprint(w, string(blob))
		var jmsg operation.JoinMessage
		if jerr := json.Unmarshal(blob, &jmsg); jerr == nil {
			if jmsg.Ip != "0.0.0.0" && jmsg.Ip != "[::]" {
				jmsg.Ip = ip
			}
			m.Topo.ProcessJoinMessage(&jmsg)
		}
	}
}

func (m *MasterServer) assignFileidHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, err := m.Topo.PickForWrite()
	if err == nil {
		key := util.GenID()
		fid := storage.NewFileId(vid, key)
		ret := operation.AssignResult{
			Fid: fid.String(),
		}
		writeJsonQuiet(w, r, http.StatusOK, ret)
	}
	writeJsonError(w, r, http.StatusOK, err)
}

func (m *MasterServer) putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fidstr := vars["fid"]
	fid, err := storage.ParseFileId(fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err)
		return
	}
	volume := vs.store.Location.GetVolume(fid.VolumeId)
	if volume == nil {
		writeJsonError(w, r, http.StatusOK, errors.New("No such volume")) // TODO
		return
	}
	err = volume.SaveFile(fid, r.Body)
	defer r.Body.Close()
	if err == nil {
		writeJsonQuiet(w, r, http.StatusOK, "success")
		return
	}
	writeJsonError(w, r, http.StatusOK, err) // TODO
}

func (m *MasterServer) getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fidstr := vars["fid"]
	fid, err := storage.ParseFileId(fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err)
		return
	}
	volume := vs.store.Location.GetVolume(fid.VolumeId)
	if volume == nil {
		writeJsonError(w, r, http.StatusOK, errors.New("No such volume")) // TODO
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if err = volume.LoadFile(fid, w); err != nil {
		writeJsonError(w, r, http.StatusNotFound, err) // TODO
	}
}

func (m *MasterServer) deleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fidstr := vars["fid"]
	fid, err := storage.ParseFileId(fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err)
		return
	}
	volume := vs.store.Location.GetVolume(fid.VolumeId)
	ret := operation.DeleteResult{}
	if volume == nil {
		ret.Status = 1
		writeJsonError(w, r, http.StatusOK, errors.New("No such volume")) // TODO
		return
	}
	volume.DeleteFile(fid)
	writeJsonQuiet(w, r, http.StatusOK, ret) // TODO
}
