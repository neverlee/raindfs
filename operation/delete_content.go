package operation

import (
	"encoding/json"
	"errors"
	"fmt"

	"raindfs/util"
)

type DeleteResult struct {
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
}

func DeleteFile(server string, fileId string) error {
	uri := fmt.Sprintf("http://%s/admin/delete/%s", server, fileId)
	jsonBlob, err := util.Get(uri)
	if err != nil {
		return err
	}
	var ret DeleteResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return fmt.Errorf("Failed to delete %s:%v", fileId, err)
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
