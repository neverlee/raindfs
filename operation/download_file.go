package operation

import (
	"os"
	"io"

	"raindfs/util"
)

func Download(server string, fid string, path string) error {
	url := "http://" + server + "/admin/get/" + fid
	os.Remove(path)
	file, err := os.Create(path)
	defer file.Close()
	if err != nil {
		return err
	}

	err = util.GetUrlStream(url, nil, func(r io.Reader) error {
		_, err := io.Copy(file, r)
		return err
	})
	return err
}
