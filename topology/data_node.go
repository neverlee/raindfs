package topology

import (
	"fmt"
	"strconv"
	"sync"

	"raindfs/operation"
	"raindfs/storage"

	"github.com/neverlee/glog"
)

type DataNode struct {
	//id  string volumeCount int activeVolumeCount int
	ip   string
	port int

	freeSpace int

	volumes  map[storage.VolumeId]storage.VolumeInfo
	writable map[storage.VolumeId]struct{}
	LastSeen int64 // unix time in seconds
	Dead     bool  // TODO 状态，enable，dead，close

	mutex sync.RWMutex
}

func NewDataNode(ip string, port int) *DataNode {
	s := &DataNode{ip: ip, port: port}
	s.volumes = make(map[storage.VolumeId]storage.VolumeInfo)
	s.writable = make(map[storage.VolumeId]struct{})
	return s
}

func (dn *DataNode) String() string {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return fmt.Sprintf("Node> volumes:%v, Ip:%s, Port:%d, Dead:%v", dn.volumes, dn.ip, dn.port, dn.Dead)
}

func (dn *DataNode) VolumeCount() int {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return len(dn.volumes)
}

func (dn *DataNode) WritableVolumeCount() int {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return len(dn.writable)
}

func (dn *DataNode) SetWritableVolume(id storage.VolumeId) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	dn.writable[id] = struct{}{}
}

func (dn *DataNode) DelWritableVolume(id storage.VolumeId) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	delete(dn.writable, id)
}

func (dn *DataNode) AddOrUpdateVolume(v storage.VolumeInfo) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	dn.volumes[v.Id] = v
	//if _, ok := dn.volumes[v.Id]; !ok {dn.volumes[v.Id] = v }
}

func (dn *DataNode) SetFreeSpace(fs int) {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	dn.freeSpace = fs
}

func (dn *DataNode) FreeSpace() int {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	return dn.freeSpace
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
	// ip and port 固定，不需加锁
	return dn.ip + ":" + strconv.Itoa(dn.port)
}

func (dn *DataNode) AssignVolume(vid storage.VolumeId) (*storage.VolumeInfo, error) {
	err := operation.AssignVolume(dn.Url(), vid.String())
	if err == nil {
		dn.mutex.RLock()
		defer dn.mutex.RUnlock()
		vi := storage.VolumeInfo{
			Id:        vid,
			Size:      0,
			FileCount: 0,
			ReadOnly:  false,
			//Uptime:,
		}
		dn.AddOrUpdateVolume(vi)
		return &vi, nil
	}
	return nil, err
}

func (dn *DataNode) ToMap() interface{} {
	dn.mutex.RLock()
	defer dn.mutex.RUnlock()
	ret := make(map[string]interface{})
	ret["Url"] = dn.Url()
	return ret
}
