package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	"raindfs/operation"
)

type VolumeInfo struct {
	Id               VolumeId
	Size             uint64
	FileCount        int
	ReadOnly         bool
	Uptime           uint64
}

func NewVolumeInfo(vim *operation.VolumeInformationMessage) *VolumeInfo {
	vi := &VolumeInfo{
		Id:               VolumeId(vim.Id),
		Size:             uint64(vim.Size),
		FileCount:        int(vim.FileCount),
		ReadOnly:         vim.ReadOnly,
	}
	return vi
}

func (vi VolumeInfo) String() string {
	return fmt.Sprintf("Id:%d, Size:%d, FileCount:%d", vi.Id, vi.Size, vi.FileCount)
}

func (vi *VolumeInfo) load(path string) error {
	blob, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, vi)
}

func (vi *VolumeInfo) dump(path string) error {
	blob, err := json.Marshal(vi)
	if err != nil {
		return err
	}
	newpath := path + "_new"
	if err = ioutil.WriteFile(newpath, blob, 0644); err != nil {
		return err
	}
	return os.Rename(newpath, path)
}

/*VolumesInfo sorting*/

type volumeInfos []*VolumeInfo

func (vis volumeInfos) Len() int {
	return len(vis)
}

func (vis volumeInfos) Less(i, j int) bool {
	return vis[i].Id < vis[j].Id
}

func (vis volumeInfos) Swap(i, j int) {
	vis[i], vis[j] = vis[j], vis[i]
}

func sortVolumeInfos(vis volumeInfos) {
	sort.Sort(vis)
}
