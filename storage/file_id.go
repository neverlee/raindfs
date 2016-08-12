package storage

import (
	"errors"
	"fmt"
)

type FileId struct {
	VolumeId VolumeId
	Key      uint64
}

func NewFileId(VolumeId VolumeId, Key uint64) *FileId {
	return &FileId{VolumeId: VolumeId, Key: Key}
}

func ParseFileId(fid string) (*FileId, error) {
	var vid VolumeId
	var key uint64
	if n, _ := fmt.Sscanf(fid, "%x-%x", &vid, &key); n == 2 {
		return &FileId{VolumeId: vid, Key: key}, nil
	}
	return nil, errors.New("Invalid fid " + fid)
}

func (n *FileId) String() string {
	return fmt.Sprintf("%x-%x", uint32(n.VolumeId), n.Key)
}
