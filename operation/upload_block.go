package operation

import ()

type UploadBlockResult struct {
	Fid   string `json:"fid,omitempty"`
	Crc32 uint32 `json:"crc32,omitempty"`
	Error string `json:"error,omitempty"`
}

func UploadBlock(server string) (*UploadBlockResult, error) {
	return nil, nil
}
