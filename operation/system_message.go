package operation

type VolumeInformationMessage struct {
	Id               uint32 `json:"id,omitempty"`
	Size             uint64 `json:"size,omitempty"`
	FileCount        uint64 `json:"file_count,omitempty"`
	DeleteCount      uint64 `json:"delete_count,omitempty"`
	DeletedByteCount uint64 `json:"deleted_byte_count,omitempty"`
	ReadOnly         bool   `json:"read_only,omitempty"`
}

type JoinMessage struct {
	IsInit         bool                        `json:"is_init,omitempty"`
	Ip             string                      `json:"ip,omitempty"`
	Port           uint32                      `json:"port,omitempty"`
	MaxVolumeCount uint32                      `json:"max_volume_count,omitempty"`
	Volumes        []*VolumeInformationMessage `json:"volumes,omitempty"`
}

type JoinResult struct {
	VolumeSizeLimit uint64 `json:"VolumeSizeLimit,omitempty"`
	Error           string `json:"error,omitempty"`
}
