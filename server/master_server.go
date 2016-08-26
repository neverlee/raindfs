package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path"
	"strconv"

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
	glog.V(4).Infoln("Sequence:", seq)
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
			if jmsg.Ip == "0.0.0.0" || jmsg.Ip == "[::]" {
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

func postFile(uri string, fidstr string, fsize int, index bool, r io.Reader, ret chan<- operation.UploadBlockResult) (reterr error) {
	var ubret operation.UploadBlockResult
	defer func() {
		if reterr != nil {
			ubret.Error = reterr.Error()
		} else {
			ubret.Error = ""
		}
		ret <- ubret
	}()

	url := fmt.Sprintf("http://%s/admin/put/%s?filesize=%d&index=%v", uri, fidstr, fsize, index)
	//req.Header.Set("Content-Length", strconv.Itoa(fsize))
	req, err := http.NewRequest("POST", url, r)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		if blob, err := ioutil.ReadAll(resp.Body); err == nil {
			if err := json.Unmarshal(blob, &ubret); err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("Status %d", resp.StatusCode) // TODO
}

func (m *MasterServer) putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fidstr := vars["fid"]
	fid, err := storage.ParseFileId(fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err)
		return
	}

	content_length, _ := strconv.Atoi(r.Header.Get("Content-Length"))
	if content_length == 0 {
		writeJsonError(w, r, http.StatusOK, fmt.Errorf("No Content-Length or error Content-Length"))
		return
	}

	index := r.URL.Query().Get("index") == "true"
	nodes := m.Topo.Lookup(fid.VolumeId)
	if len(nodes) == 2 { // TODO check writable
		defer r.Body.Close()
		var rs [2]*io.PipeReader
		var ws [2]*io.PipeWriter
		rs[0], ws[0] = io.Pipe()
		rs[1], ws[1] = io.Pipe()
		ww := io.MultiWriter(ws[0], ws[1])
		c := make(chan operation.UploadBlockResult)
		defer close(c)
		go postFile(nodes[0].Url(), fidstr, content_length, index, rs[0], c)
		go postFile(nodes[1].Url(), fidstr, content_length, index, rs[1], c)
		bodylen, berr := io.Copy(ww, r.Body)
		ws[0].Close()
		ws[1].Close()
		//ws[0].CloseWithError(io.EOF) ws[1].CloseWithError(io.EOF)
		ubret1 := <-c
		ubret2 := <-c
		if ubret1.Error == "" && ubret2.Error == "" && ubret1.Crc32 == ubret2.Crc32 {
			writeJsonQuiet(w, r, http.StatusOK, ubret1)
			return
		} else {
			glog.Extraln("put error", ubret1, ubret2)
			writeJsonError(w, r, http.StatusOK, fmt.Errorf("Writable fail!!"))
			return
		}
	}
	writeJsonError(w, r, http.StatusOK, fmt.Errorf("Volume is not writable!"))
}

func (m *MasterServer) downloadFile(fidstr string, w http.ResponseWriter) error {
	fid, err := storage.ParseFileId(fidstr)
	if err != nil {
		return err
	}

	nodes := m.Topo.Lookup(fid.VolumeId)
	if len(nodes) > 0 {
		node := nodes[rand.Intn(len(nodes))]

		uri := fmt.Sprintf("http://%s/admin/get/%s", node.Url(), fidstr)
		resp, err := http.DefaultClient.Get(uri)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.Header.Get("Flag") == "1" {
			bufreader := bufio.NewReader(resp.Body)
			for {
				ln, err := util.Readln(bufreader)
				if err != nil || len(ln) == 0 {
					break
				}
				if err = m.downloadFile(string(ln), w); err != nil {
					return err
				}
			}
		} else {
			if _, err := io.Copy(w, resp.Body); err != nil {
				return err
			}
			return nil
		}
		return nil
	}
	return fmt.Errorf("No such volume")
}

func (m *MasterServer) getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fidstr := vars["fid"]

	if err := m.downloadFile(fidstr, w); err != nil {
		writeJsonError(w, r, http.StatusNotFound, err)
	}
}
