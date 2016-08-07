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
	Ip   string
	Port int

	volumeCount       int
	activeVolumeCount int

	volumes  map[storage.VolumeId]storage.VolumeInfo
	LastSeen int64 // unix time in seconds
	Dead     bool

	topo  *Topology
	mutex sync.RWMutex
}

func NewDataNode(ip string, port int, topo *Topology) *DataNode {
	s := &DataNode{Ip: ip, Port: port, topo: topo}
	s.volumes = make(map[storage.VolumeId]storage.VolumeInfo)
	return s
}

func (dn *DataNode) String() string {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return fmt.Sprintf("Node> volumes:%v, Ip:%s, Port:%d, Dead:%v", dn.volumes, dn.Ip, dn.Port, dn.Dead)
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
	return dn.Ip == ip && dn.Port == port
}

func (dn *DataNode) Url() string {
	return dn.Ip + ":" + strconv.Itoa(dn.Port)
}

func (dn *DataNode) ToMap() interface{} {
	ret := make(map[string]interface{})
	ret["Url"] = dn.Url()
	return ret
}
