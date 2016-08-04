package operation

import (
	"encoding/json"

	"raindfs/util"
)

type ClusterStatusResult struct {
	Leader   string   `json:"Leader,omitempty"`
	LeaderId int      `json:"LeaderId,omitempty"`
	Clusters []string `json:"Clusters,omitempty"`
}

func ListMasters(server string) (*ClusterStatusResult, error) {
	jsonBlob, err := util.Get("http://" + server + "/cluster/status")
	if err != nil {
		return nil, err
	}
	var ret ClusterStatusResult
	err = json.Unmarshal(jsonBlob, &ret)
	return &ret, err
}
