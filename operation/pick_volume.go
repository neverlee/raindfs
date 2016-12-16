package operation

import (
	"encoding/json"
	"fmt"

	"raindfs/util"
)

type PickResult struct {
	Vid   string   `json:"volumeid"`
	Nodes []string `json:"nodes,"`
	Error string   `json:"error,omitempty"`
}

func PickVolume(server string) (*PickResult, error) {
	jsonBlob, err := util.Get("http://" + server + "/ms/vol/_pick")
	if err != nil {
		return nil, err
	}
	var ret PickResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, fmt.Errorf("result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
	}
	return &ret, nil
}
