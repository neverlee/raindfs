package topology

import (
	"fmt"
	"sync"

	"raindfs/storage"

	"github.com/neverlee/glog"
)

// mapping from volume to its locations, inverted from server to volume
type VolumeLayout struct {
	vid2location     map[storage.VolumeId]*VolumeLocationList
	writables        []storage.VolumeId        // transient array of writable volume id
	oversizedVolumes map[storage.VolumeId]bool // set of oversized volumes
	accessLock       sync.RWMutex
}

func NewVolumeLayout() *VolumeLayout {
	return &VolumeLayout{
		vid2location: make(map[storage.VolumeId]*VolumeLocationList),
		//writables:        make([]storage.VolumeId),
		oversizedVolumes: make(map[storage.VolumeId]bool),
	}
}

func (vl *VolumeLayout) String() string {
	return fmt.Sprintf("vid2location:%v, writables:%v, oversized:%v", vl.vid2location, vl.writables, vl.oversizedVolumes)
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewVolumeLocationList()
	}
	vl.vid2location[v.Id].Set(dn)
	glog.V(4).Infoln("volume", v.Id, "added to dn", dn.Url(), "len", vl.vid2location[v.Id].Length())
	if vl.vid2location[v.Id].Length() == 2 && vl.isWritable(v) {
		if _, ok := vl.oversizedVolumes[v.Id]; !ok {
			vl.addToWritable(v.Id)
		}
	} else {
		vl.removeFromWritable(v.Id)
	}
}

func (vl *VolumeLayout) UnRegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vl.removeFromWritable(v.Id)
	delete(vl.vid2location, v.Id)
}

func (vl *VolumeLayout) addToWritable(vid storage.VolumeId) {
	for _, id := range vl.writables {
		if vid == id {
			return
		}
	}
	vl.writables = append(vl.writables, vid)
}

func (vl *VolumeLayout) isWritable(v *storage.VolumeInfo) bool {
	//return !v.ReadOnly
	return true
}

func (vl *VolumeLayout) Lookup(vid storage.VolumeId) []*DataNode {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	if location := vl.vid2location[vid]; location != nil {
		return location.list
	}
	return nil
}

func (vl *VolumeLayout) ListVolumeServers() (nodes []*DataNode) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	for _, location := range vl.vid2location {
		nodes = append(nodes, location.list...)
	}
	return
}

//func (vl *VolumeLayout) PickForWrite(count uint64) (*storage.VolumeId, uint64, *VolumeLocationList, error) {
func (vl *VolumeLayout) GetActiveVolumeCount() int {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	return len(vl.writables)
}

func (vl *VolumeLayout) removeFromWritable(vid storage.VolumeId) bool {
	toDeleteIndex := -1
	for k, id := range vl.writables {
		if id == vid {
			toDeleteIndex = k
			break
		}
	}
	if toDeleteIndex >= 0 {
		glog.V(0).Infoln("Volume", vid, "becomes unwritable")
		vl.writables = append(vl.writables[0:toDeleteIndex], vl.writables[toDeleteIndex+1:]...)
		return true
	}
	return false
}
func (vl *VolumeLayout) setVolumeWritable(vid storage.VolumeId) bool {
	for _, v := range vl.writables {
		if v == vid {
			return false
		}
	}
	glog.V(0).Infoln("Volume", vid, "becomes writable")
	vl.writables = append(vl.writables, vid)
	return true
}

func (vl *VolumeLayout) SetVolumeUnavailable(dn *DataNode, vid storage.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if location, ok := vl.vid2location[vid]; ok {
		if location.Remove(dn) {
			if location.Length() < 2 {
				glog.V(0).Infoln("Volume", vid, "has", location.Length(), "replica, less than required")
				return vl.removeFromWritable(vid)
			}
		}
	}
	return false
}
func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid storage.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vl.vid2location[vid].Set(dn)
	if vl.vid2location[vid].Length() >= 2 {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) SetVolumeCapacityFull(vid storage.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	return vl.removeFromWritable(vid)
}

func (vl *VolumeLayout) ToMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["writables"] = vl.writables
	m["locations"] = vl.vid2location
	return m
}