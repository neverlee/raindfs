package storage

import (
	"errors"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/neverlee/glog"
)

var (
	ErrExistVolume = errors.New("Volune was already exist!")
)

type DiskLocation struct {
	directory string
	volumes   map[VolumeId]*Volume
	mutex     sync.Mutex
}

func NewDiskLocation(dir string) *DiskLocation {
	location := &DiskLocation{directory: dir}
	location.volumes = make(map[VolumeId]*Volume)
	return location
}

func (l *DiskLocation) Directory() string {
	return l.directory
}

func (l *DiskLocation) loadExistingVolumes() {
	l.mutex.Lock()
	l.mutex.Unlock()
	if dirs, err := ioutil.ReadDir(l.directory); err == nil {
		for _, dir := range dirs {
			name := dir.Name()
			if dir.IsDir() && strings.HasSuffix(name, ".vol") {
				base := name[:len(name)-len(".vol")]
				if vid, err := NewVolumeId(base); err == nil {
					if l.volumes[vid] == nil {
						if v, e := NewVolume(l.directory, vid); e == nil {
							l.volumes[vid] = v
							glog.V(0).Infof("volume directory %s", l.directory+"/"+name)
						} else {
							glog.V(0).Infof("new volume %s error %s", name, e)
						}
					}
				}
			}
		}
	}
	glog.V(0).Infoln("Store started on dir:", l.directory, "with", len(l.volumes), "volumes")
}

func (l *DiskLocation) GetVolume(vid VolumeId) *Volume {
	l.mutex.Lock()
	l.mutex.Unlock()
	glog.Extraln(">>>>>>>>>", vid)
	if v, ok := l.volumes[vid]; ok {
		return v
	}
	return nil
}

func (l *DiskLocation) GetAllVolume() []*Volume {
	l.mutex.Lock()
	l.mutex.Unlock()
	ret := make([]*Volume, len(l.volumes))
	i := 0
	for _, v := range l.volumes {
		ret[i] = v
		i++
	}
	return ret
}

func (l *DiskLocation) AddVolume(vid VolumeId) error {
	l.mutex.Lock()
	l.mutex.Unlock()
	if _, ok := l.volumes[vid]; ok {
		return ErrExistVolume
	}
	v, err := NewVolume(l.directory, vid)
	if err == nil {
		l.volumes[vid] = v
	}
	return err
}

func (l *DiskLocation) DeleteVolume(vid VolumeId) {
	l.mutex.Lock()
	l.mutex.Unlock()
	v, ok := l.volumes[vid]

	if !ok {
		return
	}
	v.Destroy()
	delete(l.volumes, vid)
}

func (l *DiskLocation) ToMap() []*VolumeInfo {
	l.mutex.Lock()
	l.mutex.Unlock()
	stats := make([]*VolumeInfo, len(l.volumes))
	i := 0
	for _, v := range l.volumes {
		s := v.GetInfo()
		//&VolumeInfo //Size:   //FileCount:  //DeleteCount: //DeletedByteCount:
		stats[i] = &s
		i++
	}
	sortVolumeInfos(stats)
	return stats
}
