package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"

	"raindfs/util"

	"github.com/satori/go.uuid"
)

const (
	VolumeExtension = ".vol"
	MetaName        = "meta.json"
)

type VolumeMeta struct {
	LastModifiedTime uint64     `json:"ModTime"`
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
	v := &Volume{
		Id:  id,
		dir: path.Join(dirname, id.String()) + VolumeExtension,
	}
	if err := os.MkdirAll(v.dir, 0755); os.IsExist(err) {
		return v, v.load(v.MetaPath())
	} else if err != nil {
		return nil, err
	}
	v.dump(v.MetaPath())
	return v, nil
}

func (v *Volume) String() string {
	return fmt.Sprintf("Id:%v, dir:%s", v.Id, v.dir)
}

func (v *Volume) MetaPath() string {
	return path.Join(v.dir, MetaName)
}

func (v *Volume) Destroy() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	// TODO: first remove then async delete, 保证不出错
	_ = os.RemoveAll(v.dir)
}

func (v *Volume) Sync() error {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	return v.dump(v.MetaPath())
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	v.dump(v.MetaPath())
}

func (v *Volume) GenFileId() *FileId {
	key := binary.LittleEndian.Uint64(uuid.NewV4().Bytes())
	return NewFileId(v.Id, key)
}

func (v *Volume) SaveFile(fid *FileId, r io.Reader) error {
	fidstr := strconv.FormatUint(fid.Key, 16)
	fpath := path.Join(v.dir, fidstr)
	file, err := os.Create(fpath)
	defer file.Close()
	if err != nil {
		return err
	}

	_, err = io.Copy(file, r)
	return err
}

func (v *Volume) LoadFile(fid *FileId, w http.ResponseWriter) error {
	fidstr := strconv.FormatUint(fid.Key, 16)
	fpath := path.Join(v.dir, fidstr)
	file, err := os.Open(fpath)
	defer file.Close()
	if err != nil {
		return err
	}
	if fsize, err := util.GetFileSize(file); err == nil {
		w.Header().Set("Content-length", strconv.FormatInt(fsize, 10))
	} else {
		return err
	}

	_, err = io.Copy(w, file)

	return err
}

func (v *Volume) DeleteFile(fid *FileId) {
	fidstr := strconv.FormatUint(fid.Key, 16)
	fpath := path.Join(v.dir, fidstr)
	os.Remove(fpath) // TOTO async delete, no errro
}
