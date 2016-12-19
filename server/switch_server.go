package server

import (
	"net/http"
	"math/rand"
	"strconv"
	"fmt"
	"io"
	"bufio"
	"strings"

	"raindfs/operation"
	"raindfs/storage"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/neverlee/glog"
)

type SwitchServer struct {
	mserver []string
}

func NewSwitchServer(mserver []string, r *mux.Router) *SwitchServer {
	ss := &SwitchServer{
		mserver: mserver,
	}
	//* /ss/fs/{vid}/{fid} post multipart方式上传
	//* /vs/stats
	r.HandleFunc("/ss/fs/_put", ss.putHandler).Methods("PUT")
	r.HandleFunc("/ss/fs/{vid:[0-9a-fA-F]+}/{fid:[0-9a-fA-F]+}", ss.getHandler).Methods("GET")
	r.HandleFunc("/ping", ss.pingHandler)

	return ss
}

func (ss *SwitchServer) pingHandler(w http.ResponseWriter, r *http.Request) {
	writeJsonQuiet(w, r, http.StatusOK, "ping")
}

func (ss *SwitchServer) putHandler(w http.ResponseWriter, r *http.Request) {
	mserver := ss.mserver[rand.Intn(len(ss.mserver))]
	pr, prerr := operation.PickVolume(mserver)
	if prerr != nil {
		writeJsonError(w, r, http.StatusOK, prerr)
		return
	}

	svid := pr.Vid
	sfid := storage.GenFileId().String()

	content_length, _ := strconv.Atoi(r.Header.Get("Content-Length"))
	if content_length == 0 {
		writeJsonError(w, r, http.StatusOK, fmt.Errorf("No Content-Length or error Content-Length"))
		return
	}

	index := r.URL.Query().Get("index") == "true"
	nodes := pr.Nodes
	if len(nodes) == 2 {
		defer r.Body.Close()
		var rs [2]*io.PipeReader
		var ws [2]*io.PipeWriter
		rs[0], ws[0] = io.Pipe()
		rs[1], ws[1] = io.Pipe()
		ww := io.MultiWriter(ws[0], ws[1])
		c := make(chan operation.UploadBlockResult)
		defer close(c)

		go operation.PutFile(nodes[0], svid, sfid, content_length, index, rs[0], c)
		go operation.PutFile(nodes[1], svid, sfid, content_length, index, rs[1], c)
		if _, berr := io.Copy(ww, r.Body); berr != nil {
			writeJsonError(w, r, http.StatusOK, berr)
			return
		}
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

func (ss *SwitchServer) downloadFile(vidstr, fidstr string, w http.ResponseWriter) error {
	mserver := ss.mserver[rand.Intn(len(ss.mserver))]
	pr, prerr := operation.PickVolumeByID(mserver, vidstr)
	glog.Extraln(">>>>>>>> pick", pr, prerr)
	if prerr != nil {
		return prerr
	}

	nodes := pr.Nodes
	if len(nodes) > 0 {
		node := nodes[rand.Intn(len(nodes))]

		uri := fmt.Sprintf("http://%s/vs/fs/%s/%s", node, vidstr, fidstr)
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
				py := strings.Split(string(ln), "/")
				if err = ss.downloadFile(py[0], py[1], w); err != nil {
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

func (ss *SwitchServer) getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr, fidstr := vars["vid"], vars["fid"]
	glog.Extraln(">>>>>>>", vidstr, fidstr)

	if err := ss.downloadFile(vidstr, fidstr, w); err != nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}
}
