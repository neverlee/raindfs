package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"raindfs/util"

	"github.com/neverlee/glog"
)

type VolumeAssignRequest struct {
	Count    uint64
	DataNode string
}

type AssignResult struct {
	Fid   string `json:"fid,omitempty"`
	Url   string `json:"url,omitempty"`
	Count uint64 `json:"count,omitempty"`
	Error string `json:"error,omitempty"`
}

func Assign(server string, r *VolumeAssignRequest) (*AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.FormatUint(r.Count, 10))
	if r.DataNode != "" {
		values.Add("dataNode", r.DataNode)
	}

	jsonBlob, err := util.Post("http://"+server+"/dir/assign", values)
	glog.V(2).Info("assign result :", string(jsonBlob))
	if err != nil {
		return nil, err
	}
	var ret AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, fmt.Errorf("/dir/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
	}
	if ret.Count <= 0 {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
