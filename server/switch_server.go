package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

type SwitchServer struct {
	mserver []string
}

func NewSwitchServer(mserver []string, r *mux.Router) *SwitchServer {
	ss := &SwitchServer{
		mserver: mserver,
	}

	r.HandleFunc("/test", ss.testHandler)
	//r.HandleFunc("/admin/put/{fid}", ms.putHandler)
	//r.HandleFunc("/admin/get/{fid}", ms.getHandler)

	return ss
}

func (ss *SwitchServer) testHandler(w http.ResponseWriter, r *http.Request) {
	writeJsonQuiet(w, r, http.StatusOK, ss.mserver)
}

//func (ss *SwitchServer) putHandler(w http.ResponseWriter, r *http.Request) {
//	vars := mux.Vars(r)
//	fidstr := vars["fid"]
//	fid, err := storage.ParseFileId(fidstr)
//	if err != nil {
//		writeJsonError(w, r, http.StatusOK, err)
//		return
//	}
//
//	content_length, _ := strconv.Atoi(r.Header.Get("Content-Length"))
//	if content_length == 0 {
//		writeJsonError(w, r, http.StatusOK, fmt.Errorf("No Content-Length or error Content-Length"))
//		return
//	}
//
//	index := r.URL.Query().Get("index") == "true"
//	nodes := m.Topo.Lookup(fid.VolumeId)
//	if len(nodes) == 2 { // TODO check writable
//		defer r.Body.Close()
//		var rs [2]*io.PipeReader
//		var ws [2]*io.PipeWriter
//		rs[0], ws[0] = io.Pipe()
//		rs[1], ws[1] = io.Pipe()
//		ww := io.MultiWriter(ws[0], ws[1])
//		c := make(chan operation.UploadBlockResult)
//		defer close(c)
//		go operation.PostFile(nodes[0].Url(), fidstr, content_length, index, rs[0], c)
//		go operation.PostFile(nodes[1].Url(), fidstr, content_length, index, rs[1], c)
//		if _, berr := io.Copy(ww, r.Body); berr != nil {
//			writeJsonError(w, r, http.StatusOK, berr)
//			return
//		}
//		ws[0].Close()
//		ws[1].Close()
//		//ws[0].CloseWithError(io.EOF) ws[1].CloseWithError(io.EOF)
//		ubret1 := <-c
//		ubret2 := <-c
//		if ubret1.Error == "" && ubret2.Error == "" && ubret1.Crc32 == ubret2.Crc32 {
//			writeJsonQuiet(w, r, http.StatusOK, ubret1)
//			return
//		} else {
//			glog.Extraln("put error", ubret1, ubret2)
//			writeJsonError(w, r, http.StatusOK, fmt.Errorf("Writable fail!!"))
//			return
//		}
//	}
//	writeJsonError(w, r, http.StatusOK, fmt.Errorf("Volume is not writable!"))
//}
//
//func (ss *SwitchServer) downloadFile(fidstr string, w http.ResponseWriter) error {
//	fid, err := storage.ParseFileId(fidstr)
//	if err != nil {
//		return err
//	}
//
//	nodes := m.Topo.Lookup(fid.VolumeId)
//	if len(nodes) > 0 {
//		node := nodes[rand.Intn(len(nodes))]
//
//		uri := fmt.Sprintf("http://%s/admin/get/%s", node.Url(), fidstr)
//		resp, err := http.DefaultClient.Get(uri)
//		if err != nil {
//			return err
//		}
//		defer resp.Body.Close()
//		if resp.Header.Get("Flag") == "1" {
//			bufreader := bufio.NewReader(resp.Body)
//			for {
//				ln, err := util.Readln(bufreader)
//				if err != nil || len(ln) == 0 {
//					break
//				}
//				if err = m.downloadFile(string(ln), w); err != nil {
//					return err
//				}
//			}
//		} else {
//			if _, err := io.Copy(w, resp.Body); err != nil {
//				return err
//			}
//			return nil
//		}
//		return nil
//	}
//	return fmt.Errorf("No such volume")
//}
//
//func (ss *SwitchServer) getHandler(w http.ResponseWriter, r *http.Request) {
//	vars := mux.Vars(r)
//	fidstr := vars["fid"]
//
//	if err := m.downloadFile(fidstr, w); err != nil {
//		writeJsonError(w, r, http.StatusNotFound, err)
//	}
//}
