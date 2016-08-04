package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

const (
	VolumeExtension = ".vol"
	MetaName        = "meta.json"
)

type VolumeMeta struct {
	LastModifiedTime uint64     `json:"modtime"`
	LastFileId       uint64     `json:"maxfid"`
	Info             VolumeInfo `json:"Info"`
}

func (v *VolumeMeta) load(path string) error {
	blob, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, v)
}

func (v *VolumeMeta) dump(path string) error {
	blob, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, blob, 0644)
}

type Volume struct {
	Id  VolumeId
	dir string

	dataFileAccessLock sync.Mutex
	VolumeMeta
}

func NewVolume(dirname string, id VolumeId) (*Volume, error) {
	v := &Volume{dir: dirname, Id: id}
	if err := os.MkdirAll(v.PathName(), 0755); os.IsExist(err) {
		return v, v.load(v.MetaFilePath())
	} else if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *Volume) String() string {
	return fmt.Sprintf("Id:%v, dir:%s", v.Id, v.dir)
}

func (v *Volume) PathName() string {
	return path.Join(v.dir, v.Id.String()) + VolumeExtension
}

func (v *Volume) MetaFilePath() string {
	return path.Join(path.Join(v.dir, v.Id.String())+VolumeExtension, MetaName)
}

func (v *Volume) Destroy() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	// TODO: first remove then async delete, 保证不出错
	_ = os.RemoveAll(v.PathName())
}

func (v *Volume) Sync() error {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	return v.dump(v.MetaFilePath())
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	v.dump(v.MetaFilePath())
}
