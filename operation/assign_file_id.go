package operation

import (
	"encoding/json"
	"fmt"

	"raindfs/util"
)

type AssignResult struct {
	Fid   string `json:"fid,omitempty"`
	Error string `json:"error,omitempty"`
}

func Assign(server string) (*AssignResult, error) {
	jsonBlob, err := util.Get("http://" + server + "/admin/assign_fileid")
	if err != nil {
		return nil, err
	}
	var ret AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, fmt.Errorf("/dir/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
	}
	return &ret, nil
}
