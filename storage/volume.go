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

	"github.com/satori/go.uuid"
)

const (
	VolumeExtension = ".vol"
	MetaName        = "meta.json"
)

type Volume struct {
	Id        VolumeId `json:"id"`
	Size      uint64   `json:"size"`
	FileCount int      `json:"file_count"`
	ReadOnly  bool     `json:"read_only"`
	Uptime    uint64   `json:"uptime"`
	Status    int      `json:"status"` //正常 恢复(恢复中)

	mutex sync.Mutex

	dir string
}

func NewVolume(dirname string, id VolumeId) (*Volume, error) {
	v := &Volume{
		Id:        id,
		Size:      0,
		FileCount: 0,
		ReadOnly:  false,
		Uptime:    0,
		Status:    0,

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

func (v *Volume) GetInfo() VolumeInfo {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	return VolumeInfo{
		Id:        v.Id,
		Size:      v.Size,
		FileCount: v.FileCount,
		ReadOnly:  v.ReadOnly,
		Uptime:    v.Uptime,
		// TODO status 正常 恢复(恢复中)
	}
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
	return v.dump(v.MetaPath())
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	v.dump(v.MetaPath())
}

func (v *Volume) GenFileId() *FileId {
	key := binary.LittleEndian.Uint64(uuid.NewV4().Bytes())
	return NewFileId(v.Id, key)
}

func (v *Volume) SaveFile(fid *FileId, fsize int, flag byte, r io.Reader) (*Needle, error) {
	fidstr := strconv.FormatUint(fid.Key, 16)
	fpath := path.Join(v.dir, fidstr)
	needle, err := WriteFile(fpath, fsize, flag, r)
	if err == nil {
		v.mutex.Lock()
		v.FileCount += 1
		v.Size += uint64(fsize) + uint64(binary.Size(needle))
		v.mutex.Unlock()
	}
	return needle, err
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
	os.Remove(fpath) // TODO async delete, no errro
}

func (v *Volume) load(path string) error {
	blob, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, v)
}

func (v *Volume) dump(path string) error {
	blob, err := json.Marshal(v)
	if err != nil {
		return err
	}
	newpath := path + "_new"
	if err = ioutil.WriteFile(newpath, blob, 0644); err != nil {
		return err
	}
	return os.Rename(newpath, path)
}
