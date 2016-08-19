package server

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
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
	masterNode   string
	mnLock       sync.RWMutex
	pulseSeconds int
	store        *storage.Store
}

func NewVolumeServer(ip string, port int, data string, masters string, r *mux.Router, pulseSeconds int) *VolumeServer {
	vs := &VolumeServer{
		pulseSeconds: pulseSeconds,
	}
	vs.store = storage.NewStore(ip, port, data)
	vs.store.SetClusters(strings.Split(masters, ","))

	go vs.heartBeat()
	r.HandleFunc("/status", vs.statusHandler)
	r.HandleFunc("/admin/assign_volume/{vid}", vs.assignVolumeHandler)
	r.HandleFunc("/admin/delete_volume/{vid}", vs.deleteVolumeHandler)
	r.HandleFunc("/admin/assign_fileid", vs.assignFileidHandler)
	r.HandleFunc("/admin/put/{fid}", vs.putHandler)
	r.HandleFunc("/admin/get/{fid}", vs.getHandler)
	r.HandleFunc("/admin/delete/{fid}", vs.deleteHandler)
	r.HandleFunc("/admin/getvinfo/{vid}", vs.getVolumeInfoHandler)
	r.HandleFunc("/admin/getvfiles/{vid}", vs.getVolumeFilesHandler)
	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)
	r.HandleFunc("/stats/disk", vs.statsDiskHandler)
	r.HandleFunc("/test", vs.testHandler)
	//adminMux.HandleFunc("/", vs.privateStoreHandler)

	return vs
}

func (v *VolumeServer) assignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr := vars["vid"]
	vid, err := storage.NewVolumeId(vidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err) // TODO 改为参数错误的状态码400
	}
	ret := operation.AssignVolumeResult{}
	if err = v.store.Location.AddVolume(vid); err != nil { // && err != storage.ErrExistVolume {
		ret.Error = err.Error()
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (v *VolumeServer) deleteVolumeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	vidstr := vars["vid"]
	vid, err := storage.NewVolumeId(vidstr)
	if err != nil {
		writeJsonError(w, r, http.StatusOK, err) // TODO 改为参数错误的状态码400
	}
	v.store.Location.DeleteVolume(vid)
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

func (vs *VolumeServer) assignFileidHandler(w http.ResponseWriter, r *http.Request) {
	volume := vs.store.Location.PickWritableVolume()
	fid := volume.GenFileId()
	ret := operation.AssignResult{
		Fid: fid.String(),
	}
	writeJsonQuiet(w, r, http.StatusOK, ret)
}

func (vs *VolumeServer) putHandler(w http.ResponseWriter, r *http.Request) {
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

func (vs *VolumeServer) getHandler(w http.ResponseWriter, r *http.Request) {
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

func (vs *VolumeServer) deleteHandler(w http.ResponseWriter, r *http.Request) {
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
			glog.V(1).Infof("Volume Server Failed to talk with master %s: %v", vs.masterNode, err)
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

func (v *VolumeServer) testHandler(w http.ResponseWriter, r *http.Request) {
	v.store.Test()
	writeJsonQuiet(w, r, http.StatusOK, "test")
}
