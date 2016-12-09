package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"raindfs/operation"
	"raindfs/stats"
	"raindfs/util"

	"github.com/neverlee/glog"
)

//MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes

type MasterNodes struct {
	nodes []string
	//lastNode int
	leader string
	mutex  sync.Mutex
}

func (mn *MasterNodes) String() string {
	return fmt.Sprintf("nodes:%v, leader:%s", mn.nodes, mn.leader)
}

func NewMasterNodes(bootstrapNode []string) (mn *MasterNodes) {
	mn = &MasterNodes{
		nodes: bootstrapNode,
	}
	return
}

//func (mn *MasterNodes) Reset() {
//	mn.mutex.Lock()
//	defer mn.mutex.Unlock()
//	glog.V(4).Infof("Resetting master nodes: %v", mn)
//	if len(mn.nodes) > 1 && mn.lastNode >= 0 {
//		glog.V(0).Infof("Reset master %s from: %v", mn.nodes[mn.lastNode], mn.nodes)
//		mn.lastNode = -mn.lastNode - 1
//	}
//}

func (mn *MasterNodes) FindMaster() ([]string, error) {
	mn.mutex.Lock()
	defer mn.mutex.Unlock()
	if len(mn.nodes) == 0 {
		return nil, errors.New("No master node found!")
	}
	i := rand.Intn(len(mn.nodes))
	for k := 0; k < len(mn.nodes); k++ {
		m := mn.nodes[(k+i)%len(mn.nodes)]
		glog.V(4).Infof("Listing masters on %s", m)
		if masters, e := operation.ListMasters(m); e == nil {
			if len(masters.Clusters) == 0 {
				continue
			}
			if masters.Leader != "" {
				mn.nodes = masters.Clusters
				mn.leader = masters.Leader
				glog.V(2).Infof("current master nodes is %v", mn)
			}
			break
		} else {
			glog.V(4).Infof("Failed listing masters on %s: %v", m, e)
		}
	}
	return mn.nodes[:], nil
}

/*
 * A VolumeServer contains one Store
 */
type Store struct {
	Ip          string
	Port        int
	Location    *DiskLocation
	lconnected  bool
	masterNodes *MasterNodes
}

func (s *Store) String() (str string) {
	str = fmt.Sprintf("Ip:%s, Port:%d, masterNodes:%s", s.Ip, s.Port, s.masterNodes)
	return
}

func NewStore(ip string, port int, dirname string) (s *Store) {
	s = &Store{Port: port, Ip: ip}
	s.Location = NewDiskLocation(dirname)
	s.Location.loadExistingVolumes()
	return
}

func (s *Store) Test() {
	glog.Extraln(s.masterNodes.FindMaster())
}

func (s *Store) SetClusters(clusters []string) {
	s.masterNodes = NewMasterNodes(clusters)
}

func (s *Store) Status() []*VolumeInfo {
	return s.Location.ToMap()
}

func (s *Store) SendHeartbeatToMaster() (masterNode []string, e error) {
	masters, e := s.masterNodes.FindMaster()
	if e != nil {
		return
	}
	var volumeMessages []*operation.VolumeInformationMessage
	maxVolumeCount := 0
	volumes := s.Location.GetAllVolume()
	for _, v := range volumes {
		vinfo := v.GetInfo()
		volumeMessage := &operation.VolumeInformationMessage{
			Id:        uint32(v.Id),
			Size:      uint64(vinfo.Size),
			FileCount: uint64(vinfo.FileCount),
			ReadOnly:  vinfo.ReadOnly,
		}
		volumeMessages = append(volumeMessages, volumeMessage)
	}

	diskStatus := stats.NewDiskStatus(s.Location.Directory())
	joinMessage := &operation.JoinMessage{
		//IsInit:         !s.connected,
		Ip:             s.Ip,
		Port:           uint32(s.Port),
		MaxVolumeCount: uint32(maxVolumeCount),
		Volumes:        volumeMessages,
		FreeSpace:      diskStatus.Free,
	}

	data, err := json.Marshal(joinMessage)
	if err != nil {
		return nil, err
	}

	for _, m := range masters {
		joinUrl := "http://" + m + "/node/join"
		glog.V(4).Infof("Connecting to %s ...", joinUrl)

		util.PostBytes(joinUrl, data)
		//if err != nil {
		//	s.masterNodes.Reset()
		//	return "", err
		//}
		//var ret operation.JoinResult
		//if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		//	glog.V(0).Infof("Failed to join %s with response: %s", joinUrl, string(jsonBlob))
		//	return "", nil
		//}
		//s.connected = true
	}
	return
}

func (s *Store) Close() {
	for _, v := range s.Location.volumes {
		v.Close()
	}
}

//func (s *Store) Write(i VolumeId, n *Needle) (size uint32, err error) { }
//func (s *Store) Delete(i VolumeId, n *Needle) (uint32, error) { }
