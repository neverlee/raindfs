package operation

type VolumeInformationMessage struct {
	Id        uint32 `json:"id"`
	Size      uint64 `json:"size"`
	FileCount uint64 `json:"file_count"`
	ReadOnly  bool   `json:"read_only"`
	Uptime    uint64 `json:"uptime"`
}

type JoinMessage struct {
	//IsInit         bool                        `json:"is_init"`
	Ip             string                      `json:"ip"`
	Port           uint32                      `json:"port"`
	MaxVolumeCount uint32                      `json:"max_volume_count"`
	Volumes        []*VolumeInformationMessage `json:"volumes"`
	FreeSpace      uint64                      `json:"free_space"`
}

type JoinResult struct {
	VolumeSizeLimit uint64         `json:"VolumeSizeLimit"`
	VolumeStatus    map[uint32]int `json:"VolumeStatus"`
	Error           string         `json:"error"`
}
