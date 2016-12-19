package operation

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

type UploadBlockResult struct {
	Vid   string `json:"vid,omitempty"`
	Fid   string `json:"fid,omitempty"`
	Crc32 uint32 `json:"crc32,omitempty"`
	Error string `json:"error,omitempty"`
}

func PutFile(vserver string, vidstr, fidstr string, fsize int, index bool, r io.Reader, ret chan<- UploadBlockResult) (reterr error) {
	var ubret UploadBlockResult
	defer func() {
		if reterr != nil {
			ubret.Error = reterr.Error()
		} else {
			ubret.Error = ""
		}
		ret <- ubret
	}()

	url := fmt.Sprintf("http://%s/vs/fs/%s/%s?filesize=%d&index=%v", vserver, vidstr, fidstr, fsize, index)
	
	//req.Header.Set("Content-Length", strconv.Itoa(fsize))
	req, err := http.NewRequest("PUT", url, r)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		defer resp.Body.Close()
		if blob, err := ioutil.ReadAll(resp.Body); err == nil {
			if err := json.Unmarshal(blob, &ubret); err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("Status %d", resp.StatusCode) // TODO
}
