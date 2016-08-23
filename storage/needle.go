package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	NeedleHeaderSize   = 16 //should never change this
	NeedleChecksumSize = 4
	//MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
	FlagBlockFile = 0
	FlagIndexFile = 1
	bufferSize    = 4 * 1024
)

/*
* A Needle means a uploaded and stored file.
* Needle file size is limited to 4GB for now.
 */
type Needle struct {
	Size uint32 // `comment:"sum of All"`

	Flags  byte
	Uptime uint64

	Checksum CRC    // `comment:"CRC32 to check integrity"`
	DataSize uint32 // `comment:"Data size"` //version2
}

func (n *Needle) String() (str string) {
	return fmt.Sprintf("Size:%d, DataSize:%d", n.Size, n.DataSize)
}

func WriteFile(fpath string, fid *FileId, fsize int, flag byte, r io.Reader) error {
	n := Needle{
		Size:     0,
		Flags:    flag,
		Uptime:   uint64(time.Now().Unix()),
		DataSize: uint32(fsize),
	}

	file, err := os.Create(fpath)
	if err != nil {
		return err
	}

	if err := binary.Write(file, binary.BigEndian, n); err != nil {
		file.Close()
		os.Remove(fpath)
		return err
	}

	buf := make([]byte, bufferSize)
	nread := 0
	for {
		if nb, err := r.Read(buf); err == nil {
			min := fsize - nread
			if nb > min {
				min = nb
			}
			data := buf[:min]

			n.Checksum = n.Checksum.Update(data)
		} else if err == io.EOF {
			break
		} else {
			file.Close()
			os.Remove(fpath)
			return err
		}
	}

	file.Seek(0, os.SEEK_SET)

	if err := binary.Write(file, binary.BigEndian, n); err != nil {
		file.Close()
		os.Remove(fpath)
		return err
	}

	file.Close()
	return nil
}
