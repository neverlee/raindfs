package operation

type VolumeInformationMessage struct {
	Id               uint32 `json:"id"`
	Size             uint64 `json:"size"`
	FileCount        uint64 `json:"file_count"`
	DeleteCount      uint64 `json:"delete_count"`
	DeletedByteCount uint64 `json:"deleted_byte_count"`
	ReadOnly         bool   `json:"read_only"`
}

type JoinMessage struct {
	IsInit         bool                        `json:"is_init"`
	Ip             string                      `json:"ip"`
	Port           uint32                      `json:"port"`
	MaxVolumeCount uint32                      `json:"max_volume_count"`
	Volumes        []*VolumeInformationMessage `json:"volumes"`
}

type JoinResult struct {
	VolumeSizeLimit uint64          `json:"VolumeSizeLimit,omitempty"`
	Writable        map[uint32]bool `json:"Writable",omitempty"`
	Error           string          `json:"error,omitempty"`
}
