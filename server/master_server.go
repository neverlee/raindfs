package server

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path"

	"raindfs/operation"
	"raindfs/sequence"
	"raindfs/storage"
	"raindfs/topology"
	"raindfs/util"

	"github.com/gorilla/mux"
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

	r.HandleFunc("/node/join", ms.nodeJoinHandler) // proxy
	r.HandleFunc("/cluster/status", ms.clusterStatusHandler)

	r.HandleFunc("/stats/nodes", ms.statsNodesHandler)
	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)

	r.HandleFunc("/test", ms.testHandler)

	ms.Topo.StartRefreshWritableVolumes()

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
	//glog.Extraln(">>>>>>", r.RemoteAddr)
	if blob, err := ioutil.ReadAll(r.Body); err == nil {
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
		return
	}
	writeJsonError(w, r, http.StatusOK, err)
}

func postFile(uri string, fidstr string, r io.Reader, status chan<- error) {
	url := fmt.Sprintf("http://%s/admin/put/%s", uri, fidstr)
	req, err := http.NewRequest("POST", url, r)
	if err != nil {
		status <- err
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		status <- err
		return
	}
	if resp.StatusCode == http.StatusOK {
		status <- nil
		return
	} else {
		status <- fmt.Errorf("Fail") // TODO
		return
	}
}

func (m *MasterServer) putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fidstr := vars["fid"]
	fid, err := storage.ParseFileId(fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err)
		return
	}
	nodes := m.Topo.Lookup(fid.VolumeId)
	if len(nodes) == 2 { // TODO check writable
		defer r.Body.Close()
		var rs [2]io.ReadCloser
		var ws [2]io.Writer
		rs[0], ws[0] = io.Pipe()
		rs[1], ws[1] = io.Pipe()
		ww := io.MultiWriter(ws[0], ws[1])
		c := make(chan error)
		defer close(c)
		go postFile(nodes[0].Url(), fidstr, rs[0], c)
		go postFile(nodes[1].Url(), fidstr, rs[1], c)
		_, _ = io.Copy(ww, r.Body)
		rs[0].Close()
		rs[1].Close()
		rerr1 := <-c
		rerr2 := <-c
		if rerr1 == nil && rerr2 == nil {
			writeJsonQuiet(w, r, http.StatusOK, "Done!")
		}
		return
	}
	writeJsonError(w, r, http.StatusOK, fmt.Errorf("Volume is not writable!"))
}

func (m *MasterServer) getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fidstr := vars["fid"]
	fid, err := storage.ParseFileId(fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err)
		return
	}

	nodes := m.Topo.Lookup(fid.VolumeId)
	if len(nodes) > 0 {
		node := nodes[rand.Intn(len(nodes))]

		uri := fmt.Sprintf("http://%s/admin/get/%s", node.Url(), fidstr)
		err := util.GetUrlStream(uri, nil, func(r io.Reader) error {
			_, err := io.Copy(w, r)
			return err
		})
		if err != nil {
			writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("No such volume")) // TODO not 200
		}
		return
	}

	writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("No such volume"))
}
