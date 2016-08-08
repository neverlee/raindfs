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
	return &DataNodeMap{nodes: make(map[string]*DataNode)}
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
	node := dnm.nodes[host]
	if node != nil {
		delete(dnm.nodes, host)
	}
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
		//if dn.Dead {
		//	dn.Dead = false
		//	t.chanRecoveredDataNodes <- dn
		//}
		return dn
	}

	dn := NewDataNode(ip, port)
	dn.Ip = ip
	dn.Port = port
	dn.LastSeen = time.Now().Unix()
	dnm.nodes[key] = dn
	return dn
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
