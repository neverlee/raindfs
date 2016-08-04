package storage

import (
	"fmt"
	"sort"
)

type VolumeInfo struct {
	Id               VolumeId
	Size             uint64
	FileCount        int
	DeleteCount      int
	DeletedByteCount uint64
}

func NewVolumeInfo() *VolumeInfo {
	vi := &VolumeInfo{}
	return vi
}

func (vi VolumeInfo) String() string {
	return fmt.Sprintf("Id:%d, Size:%d, FileCount:%d, DeleteCount:%d, DeletedByteCount:%d",
		vi.Id, vi.Size, vi.FileCount, vi.DeleteCount, vi.DeletedByteCount)
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
