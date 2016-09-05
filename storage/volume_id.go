package storage

import (
	"strconv"
)

type VolumeId uint32

func NewVolumeId(vid string) (VolumeId, error) {
	volumeId, err := strconv.ParseUint(vid, 16, 64)
	return VolumeId(volumeId), err
}
func (vid VolumeId) String() string {
	return strconv.FormatUint(uint64(vid), 16)
}
func (vid VolumeId) MarshalJSON() ([]byte, error) {
	s := strconv.FormatUint(uint64(vid), 16)
	return []byte(s), nil
}
func (vid VolumeId) UnmarshalJSON(raw []byte) error {
	id, err := strconv.ParseUint(string(raw), 16, 64)
	if err == nil {
		vid = VolumeId(id)
	}
	return err
}

func (vid VolumeId) Next() VolumeId {
	return VolumeId(uint32(vid) + 1)
}
