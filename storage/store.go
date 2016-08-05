package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"raindfs/operation"
	"raindfs/util"

	"github.com/neverlee/glog"
)

//MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes

type MasterNodes struct {
	nodes    []string
	lastNode int
}

func (mn *MasterNodes) String() string {
	return fmt.Sprintf("nodes:%v, lastNode:%d", mn.nodes, mn.lastNode)
}

func NewMasterNodes(bootstrapNode []string) (mn *MasterNodes) {
	mn = &MasterNodes{
		nodes:    bootstrapNode,
		lastNode: -1,
	}
	return
}

func (mn *MasterNodes) Reset() {
	glog.V(4).Infof("Resetting master nodes: %v", mn)
	if len(mn.nodes) > 1 && mn.lastNode >= 0 {
		glog.V(0).Infof("Reset master %s from: %v", mn.nodes[mn.lastNode], mn.nodes)
		mn.lastNode = -mn.lastNode - 1
	}
}

func (mn *MasterNodes) FindMaster() (string, error) {
	if len(mn.nodes) == 0 {
		return "", errors.New("No master node found!")
	}
	if mn.lastNode < 0 {
		for _, m := range mn.nodes {
			glog.V(4).Infof("Listing masters on %s", m)
			if masters, e := operation.ListMasters(m); e == nil {
				if len(masters.Clusters) == 0 {
					continue
				}
				mn.nodes = masters.Clusters
				mn.lastNode = rand.Intn(len(mn.nodes))
				glog.V(2).Infof("current master nodes is %v", mn)
				break
			} else {
				glog.V(4).Infof("Failed listing masters on %s: %v", m, e)
			}
		}
	}
	if mn.lastNode < 0 {
		return "", errors.New("No master node available!")
	}
	return mn.nodes[mn.lastNode], nil
}

/*
 * A VolumeServer contains one Store
 */
type Store struct {
	Ip          string
	Port        int
	Location    *DiskLocation
	connected   bool
	masterNodes *MasterNodes
}

func (s *Store) String() (str string) {
	str = fmt.Sprintf("Ip:%s, Port:%d, connected:%v, masterNodes:%s", s.Ip, s.Port, s.connected, s.masterNodes)
	return
}

func NewStore(ip string, port int, dirname string) (s *Store) {
	s = &Store{Port: port, Ip: ip}
	s.Location = NewDiskLocation(dirname)
	s.Location.loadExistingVolumes()
	return
}

func (s *Store) Status() []*VolumeInfo {
	return s.Location.ToMap()
}

func (s *Store) SendHeartbeatToMaster() (masterNode string, e error) {
	masterNode, e = s.masterNodes.FindMaster()
	if e != nil {
		return
	}
	var volumeMessages []*operation.VolumeInformationMessage
	maxVolumeCount := 0
	for k, _ := range s.Location.volumes {
		volumeMessage := &operation.VolumeInformationMessage{
			Id: uint32(k),
			//Size:             uint64(v.Size()),
			//FileCount:        uint64(v.FileCount()),
			//DeleteCount:      uint64(v.DeletedCount()),
			//DeletedByteCount: v.DeletedSize(),
		}
		volumeMessages = append(volumeMessages, volumeMessage)
	}

	joinMessage := &operation.JoinMessage{
		IsInit:         !s.connected,
		Ip:             s.Ip,
		Port:           uint32(s.Port),
		MaxVolumeCount: uint32(maxVolumeCount),
		Volumes:        volumeMessages,
	}

	data, err := json.Marshal(joinMessage)
	if err != nil {
		return "", err
	}

	joinUrl := "http://" + masterNode + "/dir/join"
	glog.V(4).Infof("Connecting to %s ...", joinUrl)

	_, err = util.PostBytes(joinUrl, data)
	if err != nil {
		s.masterNodes.Reset()
		return "", err
	}
	//var ret operation.JoinResult
	//if err := json.Unmarshal(jsonBlob, &ret); err != nil {
	//	glog.V(0).Infof("Failed to join %s with response: %s", joinUrl, string(jsonBlob))
	//	s.masterNodes.Reset()
	//	return masterNode, err
	//}
	//if ret.Error != "" {
	//	s.masterNodes.Reset()
	//	return masterNode, errors.New(ret.Error)
	//}
	s.connected = true
	return
}

func (s *Store) Close() {
	for _, v := range s.Location.volumes {
		v.Close()
	}
}

//func (s *Store) Write(i VolumeId, n *Needle) (size uint32, err error) { }
//func (s *Store) Delete(i VolumeId, n *Needle) (uint32, error) { }
