package server

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"raindfs/operation"
	"raindfs/stats"
	"raindfs/storage"
	"raindfs/util"

	"github.com/gorilla/mux"
	"github.com/neverlee/glog"
)

type VolumeServer struct {
	mnLock       sync.RWMutex
	pulseSeconds int
	mserver      []string
	store        *storage.Store
}

func NewVolumeServer(addr string, data string, mserver []string, r *mux.Router, pulseSeconds int) *VolumeServer {
	vs := &VolumeServer{
		pulseSeconds: pulseSeconds,
		mserver:      mserver,
	}

	vs.store = storage.NewStore(data, addr, mserver)

	go vs.heartBeat()
	r.HandleFunc("/status", vs.statusHandler)
	r.HandleFunc("/vs/vol/{vid}", vs.assignVolumeHandler).Methods("PUT")
	r.HandleFunc("/vs/vol/{vid}", vs.deleteVolumeHandler).Methods("DELETE")
	r.HandleFunc("/vs/vol/{vid}", vs.getVolumeInfoHandler).Methods("GET")
	r.HandleFunc("/vs/fs/{vid}/{fid}", vs.putHandler).Methods("PUT")
	r.HandleFunc("/vs/fs/{vid}/{fid}", vs.deleteHandler).Methods("DELETE")
	r.HandleFunc("/vs/fs/{vid}/{fid}", vs.getHandler).Methods("GET")
	r.HandleFunc("/vs/fs/{vid}/{fid}/info", vs.getVolumeFilesHandler).Methods("GET")

	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)
	r.HandleFunc("/stats/disk", vs.statsDiskHandler)
	r.HandleFunc("/ping", vs.pingHandler)

	return vs
}

func (vs *VolumeServer) assignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr := vars["vid"]
	vid, err := storage.NewVolumeId(vidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err) // TODO 改为参数错误的状态码400
	}
	ret := operation.AssignVolumeResult{}
	if err = vs.store.Location.AddVolume(vid); err != nil { // && err != storage.ErrExistVolume {
		ret.Error = err.Error()
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (vs *VolumeServer) deleteVolumeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr := vars["vid"]
	vid, err := storage.NewVolumeId(vidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err) // TODO 改为参数错误的状态码400
	}
	vs.store.Location.DeleteVolume(vid)
	writeJsonQuiet(w, r, http.StatusOK, true)
}

func (vs *VolumeServer) statsDiskHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	ds := stats.NewDiskStatus(vs.store.Location.Directory())
	m["DiskStatuses"] = ds
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Volumes"] = vs.store.Status()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (vs *VolumeServer) putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr, fidstr := vars["vid"], vars["fid"]
	vid, fid, err := storage.NewVFId(vidstr, fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}

	fsize, _ := strconv.Atoi(r.Header.Get("Content-Length"))
	if fsize == 0 {
		fsize, _ = strconv.Atoi(r.FormValue("filesize"))
	}

	if fsize == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("No filesize arguement"))
		return
	}

	volume := vs.store.Location.GetVolume(vid)
	if volume == nil {
		writeJsonError(w, r, http.StatusNotFound, errors.New("No such volume")) // TODO
		return
	}
	flag := byte(0)

	if r.URL.Query().Get("index") == "true" {
		flag = 1
	}
	needle, err := volume.SaveFile(fid, fsize, flag, r.Body)
	defer r.Body.Close()
	if err == nil {
		ret := operation.UploadBlockResult{
			Fid:   fidstr,
			Crc32: uint32(needle.Checksum),
		}
		writeJsonQuiet(w, r, http.StatusOK, ret)
		return
	}
	writeJsonError(w, r, http.StatusInternalServerError, err) // TODO
}

func (vs *VolumeServer) getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr, fidstr := vars["vid"], vars["fid"]
	vid, fid, err := storage.NewVFId(vidstr, fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err)
		return
	}
	volume := vs.store.Location.GetVolume(vid)
	if volume == nil {
		writeJsonError(w, r, http.StatusOK, errors.New("No such volume")) // TODO
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if err = volume.LoadFile(fid, w); err != nil {
		writeJsonError(w, r, http.StatusNotFound, err) // TODO
	}
}

func (vs *VolumeServer) deleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr, fidstr := vars["vid"], vars["fid"]
	vid, fid, err := storage.NewVFId(vidstr, fidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err)
		return
	}
	volume := vs.store.Location.GetVolume(vid)
	ret := operation.DeleteResult{}
	if volume == nil {
		ret.Status = 1
		writeJsonError(w, r, http.StatusOK, errors.New("No such volume")) // TODO
		return
	}
	volume.DeleteFile(fid)
	writeJsonQuiet(w, r, http.StatusOK, ret) // TODO
}
func (vs *VolumeServer) getVolumeInfoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr := vars["vid"]

	vid, err := storage.NewVolumeId(vidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err) // TODO
		return
	}

	volume := vs.store.Location.GetVolume(vid)
	if volume == nil {
		writeJsonError(w, r, http.StatusOK, errors.New("No such volume")) // TODO
		return
	}
	stat, err := volume.GetStat()
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err) // TODO
		return
	}
	ret := struct{ ModTime int64 }{
		ModTime: stat.ModTime().Unix(),
	}
	writeJsonQuiet(w, r, http.StatusOK, ret) // TODO
}

func (vs *VolumeServer) getVolumeFilesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr := vars["vid"]
	vid, err := storage.NewVolumeId(vidstr)

	if err != nil {
		writeJsonError(w, r, http.StatusOK, err) // TODO
		return
	}

	volume := vs.store.Location.GetVolume(vid)
	if volume == nil {
		writeJsonError(w, r, http.StatusOK, errors.New("No such volume")) // TODO
		return
	}

	dir := volume.Directory()
	fdir, err := os.Open(dir)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, errors.New("No such volume")) // TODO
		return
	}
	defer fdir.Close()
	for {
		dlist, err := fdir.Readdir(40)
		if err != nil {
			break
		}
		for _, di := range dlist {
			if !di.IsDir() {
				fmt.Fprintln(w, di.Size(), di.ModTime().Unix(), di.Name())
			}
		}
	}
}

func (vs *VolumeServer) heartBeat() {
	connected := true

	for {
		//glog.V(0).Infof("Volume server sending to master ")
		master, err := vs.store.SendHeartbeatToMaster()
		if err == nil {
			if !connected {
				connected = true
				//vs.SetMasterNode(master)
				glog.V(0).Infoln("Volume Server Connected with master at", master)
			}
		} else {
			glog.V(1).Infof("Volume Server Failed to talk with master %s: %v", vs.mserver, err)
			if connected {
				connected = false
			}
		}
		if connected {
			time.Sleep(time.Duration(float32(vs.pulseSeconds*1e3)*(1+rand.Float32())) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(float32(vs.pulseSeconds*1e3)*0.25) * time.Millisecond)
		}
	}
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	//vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}

func (v *VolumeServer) pingHandler(w http.ResponseWriter, r *http.Request) {
	writeJsonQuiet(w, r, http.StatusOK, "ping")
}
