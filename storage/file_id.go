package storage

import (
	"strconv"
	"encoding/binary"

	"github.com/satori/go.uuid"
)

type FileId uint64

func GenFileId() FileId {
	key := binary.LittleEndian.Uint64(uuid.NewV4().Bytes())
	return FileId(key)
}

func NewFileId(fidstr string) (FileId, error) {
	fid, err := strconv.ParseUint(fidstr, 16, 64)
	return FileId(fid), err
}

func (fid FileId) String() string {
	return strconv.FormatUint(uint64(fid), 16)
}

func (fid FileId) MarshalJSON() ([]byte, error) {
	s := strconv.FormatUint(uint64(fid), 16)
	return []byte(s), nil
}
func (fid FileId) UnmarshalJSON(raw []byte) error {
	id, err := strconv.ParseUint(string(raw), 16, 64)
	if err == nil {
		fid = FileId(id)
	}
	return err
}

func NewVFId(vidstr, fidstr string) (VolumeId, FileId, error) {
	vid, verr := NewVolumeId(vidstr)
	if verr != nil {
		return 0, 0, verr
	}
	fid, ferr := NewFileId(fidstr)
	if ferr != nil {
		return 0, 0, ferr
	}
	return vid, fid, nil
}


