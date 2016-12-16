package command

import (
	"fmt"
	"io"
	"os"
)

var (
	d DownloadOptions
)

type DownloadOptions struct {
	server *string
	dir    *string
}

func init() {
	cmdDownload.Run = runDownload // break init cycle
	d.server = cmdDownload.Flag.String("sserver", "localhost:100200", "RainDFS switch location")
	d.dir = cmdDownload.Flag.String("dir", ".", "Download the whole folder recursively if specified.")
}

var cmdDownload = &Command{
	UsageLine: "download -server=localhost:9333 -dir=one_directory fid1 [fid2 fid3 ...]",
	Short:     "download files by file id",
	Long: `download files by file id.

  Usually you just need to use curl to lookup the file's volume server, and then download them directly.
  This download tool combine the two steps into one.

  `,
}

func runDownload(cmd *Command, args []string) bool {
	for _, fid := range args {
		if e := downloadToFile(*d.server, fid, *d.dir); e != nil {
			fmt.Println("Download Error: ", fid, e)
		}
	}
	return true
}

func downloadToFile(server, fileId, saveDir string) error {
	return nil
}

func WriteFile(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	return err
}
