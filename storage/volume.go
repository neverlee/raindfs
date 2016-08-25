package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/satori/go.uuid"
)

const (
	VolumeExtension = ".vol"
	MetaName        = "meta.json"
)

type Volume struct {
	Id  VolumeId
	dir string

	mutex sync.Mutex
	Info  VolumeInfo
}

func NewVolume(dirname string, id VolumeId) (*Volume, error) {
	v := &Volume{
		Id:  id,
		dir: path.Join(dirname, id.String()) + VolumeExtension,

		Info: VolumeInfo{
			Id: id,
		},
	}
	if err := os.MkdirAll(v.dir, 0755); os.IsExist(err) {
		return v, v.Info.load(v.MetaPath())
	} else if err != nil {
		return nil, err
	}
	v.Info.dump(v.MetaPath())
	return v, nil
}

func (v *Volume) String() string {
	return fmt.Sprintf("Id:%v, dir:%s", v.Id, v.dir)
}

func (v *Volume) GetInfo() VolumeInfo {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	return v.Info
}

func (v *Volume) GetStat() (os.FileInfo, error) {
	return os.Stat(v.dir)
}

func (v *Volume) Directory() string {
	return v.dir
}

func (v *Volume) MetaPath() string {
	return path.Join(v.dir, MetaName)
}

func (v *Volume) Destroy() {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	// TODO: first remove then async delete, 保证不出错
	_ = os.RemoveAll(v.dir)
}

func (v *Volume) Sync() error {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	return v.Info.dump(v.MetaPath())
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	v.Info.dump(v.MetaPath())
}

func (v *Volume) GenFileId() *FileId {
	key := binary.LittleEndian.Uint64(uuid.NewV4().Bytes())
	return NewFileId(v.Id, key)
}

func (v *Volume) SaveFile(fid *FileId, fsize int, flag byte, r io.Reader) error {
	fidstr := strconv.FormatUint(fid.Key, 16)
	fpath := path.Join(v.dir, fidstr)
	err := WriteFile(fpath, fsize, flag, r)
	return err
}

func (v *Volume) LoadFile(fid *FileId, w http.ResponseWriter) error {
	fidstr := strconv.FormatUint(fid.Key, 16)
	fpath := path.Join(v.dir, fidstr)
	err := ReadFile(fpath, func(n *Needle, r io.Reader) error {
		w.Header().Set("Content-length", strconv.FormatInt(int64(n.DataSize), 10))
		w.Header().Set("CRC32", strconv.FormatUint(uint64(n.Checksum), 16))
		w.Header().Set("Flag", strconv.FormatUint(uint64(n.Flags), 16))
		_, err := io.Copy(w, r)
		return err
	})
	return err
}

func (v *Volume) DeleteFile(fid *FileId) {
	fidstr := strconv.FormatUint(fid.Key, 16)
	fpath := path.Join(v.dir, fidstr)
	os.Remove(fpath) // TOTO async delete, no errro
}
