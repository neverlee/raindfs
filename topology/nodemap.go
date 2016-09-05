package topology

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type DataNodeMap struct {
	nodes map[string]*DataNode
	mutex sync.Mutex
}

func NewDataNodeMap() *DataNodeMap {
	dnm := &DataNodeMap{nodes: make(map[string]*DataNode)}
	return dnm
}

func (dnm *DataNodeMap) LinkChildNode(dn *DataNode) {
	dnm.mutex.Lock()
	defer dnm.mutex.Unlock()
	if dnm.nodes[dn.Url()] == nil {
		dnm.nodes[dn.Url()] = dn
	}
}

func (dnm *DataNodeMap) UnlinkChildNode(host string) {
	dnm.mutex.Lock()
	defer dnm.mutex.Unlock()
	//删除时不需要判断是否存在 node := dnm.nodes[host] //if node != nil {}
	delete(dnm.nodes, host)
}

func (dnm *DataNodeMap) FindDataNode(ip string, port int) *DataNode {
	dnm.mutex.Lock()
	defer dnm.mutex.Unlock()
	key := fmt.Sprintf("%s:%d", ip, port)
	dn, _ := dnm.nodes[key]
	return dn
}

func (dnm *DataNodeMap) GetOrCreateDataNode(ip string, port int, maxVolumeCount int) *DataNode {
	dnm.mutex.Lock()
	defer dnm.mutex.Unlock()
	key := fmt.Sprintf("%s:%d", ip, port)
	if dn, ok := dnm.nodes[key]; ok {
		dn.LastSeen = time.Now().Unix()
		if dn.Dead {
			dn.Dead = false
			//dnm.chanRecoveredDataNodes <- dn
		}
		return dn
	}

	dn := NewDataNode(ip, port)
	dn.LastSeen = time.Now().Unix()
	dnm.nodes[key] = dn
	return dn
}

func (dnm *DataNodeMap) GetWritableNodes() []*DataNode {
	dnm.mutex.Lock()
	defer dnm.mutex.Unlock()
	nodes := make([]*DataNode, len(dnm.nodes))
	i := 0
	for _, dn := range dnm.nodes {
		if !dn.Dead { // 不是dead并且无故障，有空间
			nodes[i] = dn
			i++
		}
	}
	return nodes[:i]
}

func (dnm *DataNodeMap) CollectDeadNode(freshThreshHold int64) []*DataNode {
	dnm.mutex.Lock()
	defer dnm.mutex.Unlock()
	var dnodes []*DataNode
	for _, dn := range dnm.nodes {
		if dn.LastSeen < freshThreshHold {
			if !dn.Dead {
				dn.Dead = true
				dnodes = append(dnodes, dn)
				// 删除 //dnm.chanDeadDataNodes <- dn
				delete(dnm.nodes, dn.Url())
			}
		}
	}
	return dnodes
}

type sortWritableNodes struct {
	nodes    []*DataNode
	writable []int
}

func (sn sortWritableNodes) Len() int {
	return len(sn.writable)
}
func (sn sortWritableNodes) Less(i, j int) bool {
	return sn.writable[i] < sn.writable[j]
}
func (sn sortWritableNodes) Swap(i, j int) {
	sn.nodes[i], sn.nodes[j] = sn.nodes[j], sn.nodes[i]
	sn.writable[i], sn.writable[j] = sn.writable[j], sn.writable[i]
}

func (dnm *DataNodeMap) CollectNodeNeedNewVolume() []*DataNode {
	dnm.mutex.Lock()
	nodenum := len(dnm.nodes)
	swn := sortWritableNodes{
		nodes:    make([]*DataNode, len(dnm.nodes)+1),
		writable: make([]int, len(dnm.nodes)),
	}
	i := 0
	for _, dn := range dnm.nodes {
		swn.nodes[i] = dn
		swn.writable[i] = dn.WritableVolumeCount()
		i++
	}
	dnm.mutex.Unlock()

	if nodenum < replicate {
		return nil
	}

	sort.Sort(swn)

	// 如果可写volume数小于，最多可写volume节点的一半加1，则需要分配
	minw := swn.writable[len(swn.writable)-1]/2 + 1
	for id, w := range swn.writable {
		if w > minw {
			i = id
			break
		}
	}
	if i == 1 {
		i = 2
	} else if i%2 == 1 {
		swn.nodes[i] = swn.nodes[0]
		i++
	}

	return swn.nodes[:i]
}

func (dnm *DataNodeMap) ToData() []*DataNodeData {
	dnm.mutex.Lock()
	defer dnm.mutex.Unlock()

	ret := make([]*DataNodeData, len(dnm.nodes))
	i := 0
	for _, dn := range dnm.nodes {
		ret[i] = dn.ToData()
		i++
	}
	return ret
}
