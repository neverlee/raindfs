package storage

import (
	"io/ioutil"
	"strings"

	"github.com/neverlee/glog"
)

type DiskLocation struct {
	Directory string
	volumes   map[VolumeId]*Volume
}

func NewDiskLocation(dir string) *DiskLocation {
	location := &DiskLocation{Directory: dir}
	location.volumes = make(map[VolumeId]*Volume)
	return location
}

func (l *DiskLocation) loadExistingVolumes() {
	if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
		for _, dir := range dirs {
			name := dir.Name()
			if dir.IsDir() && strings.HasSuffix(name, ".vol") {
				base := name[:len(name)-len(".vol")]
				if vid, err := NewVolumeId(base); err == nil {
					if l.volumes[vid] == nil {
						if v, e := NewVolume(l.Directory, vid); e == nil {
							l.volumes[vid] = v
							glog.V(0).Infof("data file %s, v=%d", l.Directory+"/"+name)
						} else {
							glog.V(0).Infof("new volume %s error %s", name, e)
						}
					}
				}
			}
		}
	}
	glog.V(0).Infoln("Store started on dir:", l.Directory, "with", len(l.volumes), "volumes")
}

func (l *DiskLocation) DeleteVolumeById(vid VolumeId) {
	v, ok := l.volumes[vid]
	if !ok {
		return
	}
	v.Destroy()
	delete(l.volumes, vid)
}
