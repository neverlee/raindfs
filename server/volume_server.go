package server

import (
	"errors"
	"net/http"
	"sync"

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

func NewVolumeServer(ip string, port int, data string, r *mux.Router, pulseSeconds int) *VolumeServer {
	vs := &VolumeServer{
		pulseSeconds: pulseSeconds,
	}
	vs.store = storage.NewStore(ip, port, data)

	//r.HandleFunc("/ui/index.html", vs.uiStatusHandler)
	r.HandleFunc("/status", vs.statusHandler)
	r.HandleFunc("/admin/assign_volume/{vid}", vs.assignVolumeHandler)
	r.HandleFunc("/admin/delete_volume/{vid}", vs.deleteVolumeHandler)
	r.HandleFunc("/admin/assign_fileid", vs.assignFileidHandler)
	r.HandleFunc("/admin/put/{fid}", vs.putHandler)
	r.HandleFunc("/admin/get/{fid}", vs.getHandler)
	r.HandleFunc("/admin/delete/{fid}", vs.deleteHandler)
	//r.HandleFunc("/admin/disable_volume", vs.vacuumVolumeCheckHandler)
	//r.HandleFunc("/admin/vacuum/compact", vs.vacuumVolumeCompactHandler)
	//r.HandleFunc("/admin/vacuum/commit", vs.vacuumVolumeCommitHandler)
	//r.HandleFunc("/admin/sync/status", vs.getVolumeSyncStatusHandler)
	//r.HandleFunc("/admin/sync/index", vs.getVolumeIndexContentHandler)
	//r.HandleFunc("/admin/sync/data", vs.getVolumeDataContentHandler)
	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)
	r.HandleFunc("/stats/disk", vs.statsDiskHandler)
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

//func heartBeat() {
//	connected := true
//
//	glog.V(0).Infof("Volume server bootstraps with master %s", vs.GetMasterNode())
//	vs.store.SetBootstrapMaster(vs.GetMasterNode())
//	for {
//		glog.V(4).Infof("Volume server sending to master %s", vs.GetMasterNode())
//		master, err := vs.store.SendHeartbeatToMaster()
//		if err == nil {
//			if !connected {
//				connected = true
//				vs.SetMasterNode(master)
//				glog.V(0).Infoln("Volume Server Connected with master at", master)
//			}
//		} else {
//			glog.V(1).Infof("Volume Server Failed to talk with master %s: %v", vs.masterNode, err)
//			if connected {
//				connected = false
//			}
//		}
//		if connected {
//			time.Sleep(time.Duration(float32(vs.pulseSeconds*1e3)*(1+rand.Float32())) * time.Millisecond)
//		} else {
//			time.Sleep(time.Duration(float32(vs.pulseSeconds*1e3)*0.25) * time.Millisecond)
//		}
//	}
//}

func (vs *VolumeServer) GetMasterNode() string {
	vs.mnLock.RLock()
	defer vs.mnLock.RUnlock()
	return vs.masterNode
}

func (vs *VolumeServer) SetMasterNode(masterNode string) {
	vs.mnLock.Lock()
	defer vs.mnLock.Unlock()
	vs.masterNode = masterNode
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	//vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}
