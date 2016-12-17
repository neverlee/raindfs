package operation

import (
	"encoding/json"
	"errors"
	"fmt"

	"raindfs/util"
)

type AssignVolumeResult struct {
	Error string
}

func AssignVolume(server string, vidstr string) error {
	uri := fmt.Sprintf("http://%s/vs/vol/%s", server, vidstr)
	jsonBlob, err := util.Put(uri)
	if err != nil {
		return err
	}
	var ret AssignVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return fmt.Errorf("Invalid JSON result for %s: %s", "/admin/assign_volum", string(jsonBlob))
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
