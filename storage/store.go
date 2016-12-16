package storage

import (
	"encoding/json"
	"fmt"

	"raindfs/operation"
	"raindfs/stats"
	"raindfs/util"

	"github.com/neverlee/glog"
)

/*
 * A VolumeServer contains one Store
 */
type Store struct {
	Addr       string
	Location   *DiskLocation
	lconnected bool
	mserver    []string
}

func (s *Store) String() (str string) {
	str = fmt.Sprintf("Addr:%s, mservers:%s", s.Addr, s.mserver)
	return
}

func NewStore(dirname string, addr string, mserver []string) (s *Store) {
	s = &Store{Addr: addr, mserver: mserver}
	s.Location = NewDiskLocation(dirname)
	s.Location.loadExistingVolumes()
	return
}

func (s *Store) Status() []*VolumeInfo {
	return s.Location.ToMap()
}

func (s *Store) SendHeartbeatToMaster() (masterNode []string, e error) {
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
		Addr:           s.Addr,
		MaxVolumeCount: uint32(maxVolumeCount),
		Volumes:        volumeMessages,
		FreeSpace:      diskStatus.Free,
	}

	data, err := json.Marshal(joinMessage)
	if err != nil {
		return nil, err
	}

	for _, m := range s.mserver {
		joinUrl := "http://" + m + "/node/join"
		glog.V(4).Infof("Connecting to %s ...", joinUrl)

		go util.PostBytes(joinUrl, data)
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
