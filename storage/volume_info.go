package storage

import (
	"fmt"
	"sort"

	"raindfs/operation"
)

type VolumeInfo struct {
	Id        VolumeId
	Size      uint64
	FileCount int
	ReadOnly  bool
	Uptime    uint64
	// TODO status 正常 恢复(恢复中)
}

func NewVolumeInfo(vim *operation.VolumeInformationMessage) *VolumeInfo {
	vi := &VolumeInfo{
		Id:        VolumeId(vim.Id),
		Size:      uint64(vim.Size),
		FileCount: int(vim.FileCount),
		ReadOnly:  vim.ReadOnly,
		Uptime:    vim.Uptime,
	}
	return vi
}

func (vi VolumeInfo) String() string {
	return fmt.Sprintf("Id:%d, Size:%d, FileCount:%d, ReadOnly:%v, Uptime:%d", vi.Id, vi.Size, vi.FileCount, vi.ReadOnly, vi.Uptime)
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
