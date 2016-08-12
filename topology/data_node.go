package topology

import (
	"fmt"
	"strconv"
	"sync"

	"raindfs/storage"

	"github.com/neverlee/glog"
)

type DataNode struct {
	//id                string
	ip   string
	port int

	volumeCount       int
	activeVolumeCount int

	volumes  map[storage.VolumeId]storage.VolumeInfo
	LastSeen int64 // unix time in seconds
	Dead     bool  // TODO 状态，enable，dead，close

	mutex sync.RWMutex
}

func NewDataNode(ip string, port int) *DataNode {
	s := &DataNode{ip: ip, port: port}
	s.volumes = make(map[storage.VolumeId]storage.VolumeInfo)
	return s
}

func (dn *DataNode) String() string {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return fmt.Sprintf("Node> volumes:%v, Ip:%s, Port:%d, Dead:%v", dn.volumes, dn.ip, dn.port, dn.Dead)
}

func (dn *DataNode) AddOrUpdateVolume(v storage.VolumeInfo) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	if _, ok := dn.volumes[v.Id]; !ok {
		dn.volumes[v.Id] = v
	} else {
		dn.volumes[v.Id] = v
	}
}

func (dn *DataNode) UpdateVolumes(actualVolumes []storage.VolumeInfo) (deletedVolumes []storage.VolumeInfo) {
	actualVolumeMap := make(map[storage.VolumeId]storage.VolumeInfo)
	for _, v := range actualVolumes {
		actualVolumeMap[v.Id] = v
	}
	dn.mutex.RLock()
	for vid, v := range dn.volumes {
		if _, ok := actualVolumeMap[vid]; !ok {
			glog.V(0).Infoln("Deleting volume id:", vid)
			delete(dn.volumes, vid)
			deletedVolumes = append(deletedVolumes, v)
		}
	} //TODO: adjust max volume id, if need to reclaim volume ids
	dn.mutex.RUnlock()
	return
}

func (dn *DataNode) GetVolumes() (ret []storage.VolumeInfo) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	for _, v := range dn.volumes {
		ret = append(ret, v)
	}
	return ret
}

func (dn *DataNode) MatchLocation(ip string, port int) bool {
	return dn.ip == ip && dn.port == port
}

func (dn *DataNode) Url() string {
	// ip and port 固定
	return dn.ip + ":" + strconv.Itoa(dn.port)
}

func (dn *DataNode) ToMap() interface{} {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	ret := make(map[string]interface{})
	ret["Url"] = dn.Url()
	return ret
}
