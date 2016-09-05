package topology

import (
	"fmt"

	"raindfs/storage"
)

type VolumeLocationList struct {
	list []*DataNode
}

func NewVolumeLocationList() *VolumeLocationList {
	return &VolumeLocationList{}
}

func (dnll *VolumeLocationList) String() string {
	return fmt.Sprintf("%v", dnll.list)
}

func (dull *VolumeLocationList) SetWritableVolume(vid storage.VolumeId) {
	for _, dn := range dull.list {
		dn.SetWritableVolume(vid)
	}
}

func (dull *VolumeLocationList) DelWritableVolume(vid storage.VolumeId) {
	for _, dn := range dull.list {
		dn.DelWritableVolume(vid)
	}
}

func (dnll *VolumeLocationList) Head() *DataNode {
	//mark first node as master volume
	if len(dnll.list) > 0 {
		return dnll.list[0]
	}
	return nil
}

func (dnll *VolumeLocationList) Length() int {
	return len(dnll.list)
}

func (dnll *VolumeLocationList) Set(loc *DataNode) {
	for i := 0; i < len(dnll.list); i++ {
		if dnll.list[i].Url() == loc.Url() {
			dnll.list[i] = loc
			return
		}
	}
	dnll.list = append(dnll.list, loc)
}

func (dnll *VolumeLocationList) Remove(loc *DataNode) bool {
	for i, dnl := range dnll.list {
		if loc.Url() == dnl.Url() {
			dnll.list = append(dnll.list[:i], dnll.list[i+1:]...)
			return true
		}
	}
	return false
}

func (dnll *VolumeLocationList) ToList() []*DataNode {
	return dnll.list[:]
}

func (dnll *VolumeLocationList) ToNameList() []string {
	ret := make([]string, len(dnll.list))
	for i, v := range dnll.list {
		ret[i] = v.Url()
	}
	return ret
}

//func (dnll *VolumeLocationList) Refresh(freshThreshHold int64) {
//	var changed bool
//	for _, dnl := range dnll.list {
//		if dnl.LastSeen < freshThreshHold {
//			changed = true
//			break
//		}
//	}
//	if changed {
//		var l []*DataNode
//		for _, dnl := range dnll.list {
//			if dnl.LastSeen >= freshThreshHold {
//				l = append(l, dnl)
//			}
//		}
//		dnll.list = l
//	}
//}
