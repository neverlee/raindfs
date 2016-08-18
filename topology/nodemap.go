package topology

import (
	"fmt"
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
	dnm.mutex.Unlock()
	if dnm.nodes[dn.Url()] == nil {
		dnm.nodes[dn.Url()] = dn
	}
}

func (dnm *DataNodeMap) UnlinkChildNode(host string) {
	dnm.mutex.Lock()
	dnm.mutex.Unlock()
	//删除时不需要判断是否存在 node := dnm.nodes[host] //if node != nil {}
	delete(dnm.nodes, host)
}

func (dnm *DataNodeMap) FindDataNode(ip string, port int) *DataNode {
	dnm.mutex.Lock()
	dnm.mutex.Unlock()
	key := fmt.Sprintf("%s:%d", ip, port)
	dn, _ := dnm.nodes[key]
	return dn
}

func (dnm *DataNodeMap) GetOrCreateDataNode(ip string, port int, maxVolumeCount int) *DataNode {
	dnm.mutex.Lock()
	dnm.mutex.Unlock()
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
	dnm.mutex.Unlock()
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
	dnm.mutex.Unlock()
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

func (dnm *DataNodeMap) ToMap() []interface{} {
	dnm.mutex.Lock()
	dnm.mutex.Unlock()
	ret := make([]interface{}, len(dnm.nodes))
	i := 0
	for _, dn := range dnm.nodes {
		ret[i] = dn.ToMap()
		i++
	}
	return ret
}
