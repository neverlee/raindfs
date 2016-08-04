package server

import (
	"sync"

	"raindfs/storage"

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

	//adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
	//adminMux.HandleFunc("/status", (vs.statusHandler))
	//adminMux.HandleFunc("/admin/assign_volume", (vs.assignVolumeHandler))
	//adminMux.HandleFunc("/admin/vacuum/check", (vs.vacuumVolumeCheckHandler))
	//adminMux.HandleFunc("/admin/vacuum/compact", (vs.vacuumVolumeCompactHandler))
	//adminMux.HandleFunc("/admin/vacuum/commit", (vs.vacuumVolumeCommitHandler))
	//adminMux.HandleFunc("/admin/delete_collection", (vs.deleteCollectionHandler))
	//adminMux.HandleFunc("/admin/sync/status", (vs.getVolumeSyncStatusHandler))
	//adminMux.HandleFunc("/admin/sync/index", (vs.getVolumeIndexContentHandler))
	//adminMux.HandleFunc("/admin/sync/data", (vs.getVolumeDataContentHandler))
	r.HandleFunc("/stats/counter", statsCounterHandler)
	r.HandleFunc("/stats/memory", statsMemoryHandler)
	//adminMux.HandleFunc("/stats/disk", (vs.statsDiskHandler))
	//adminMux.HandleFunc("/delete", (vs.batchDeleteHandler))
	//adminMux.HandleFunc("/", vs.privateStoreHandler)
	// separated admin and public port
	//publicMux.HandleFunc("/", vs.publicReadOnlyHandler)

	return vs
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
