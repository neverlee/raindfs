package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"raindfs/util"
)

const (
	needleHeaderSize = 21 //should never change this
	//NeedleChecksumSize = 4
	//MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
	FlagBlockFile = 0
	FlagIndexFile = 1
	bufferSize    = 32 * 1024
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

func WriteFile(fpath string, fsize int, flag byte, r io.Reader) (*Needle, error) {
	n := Needle{
		Flags:    flag,
		Uptime:   uint64(time.Now().Unix()),
		DataSize: uint32(fsize),
	}
	n.Size = uint32(binary.Size(n)) + uint32(fsize)

	file, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}

	var reterr error
oneloop:
	for oneloop := true; oneloop; oneloop = false {

		if err := binary.Write(file, binary.BigEndian, n); err != nil {
			reterr = err
			break oneloop
		}

		buf := make([]byte, bufferSize)
		nread := 0
		for {
			nb, err := r.Read(buf)
			if nb > 0 {
				min := fsize - nread
				if nb < min {
					min = nb
				}
				nread += min
				data := buf[:min]
				if nw, werr := file.Write(data); nw != min || werr != nil {
					reterr = errors.New("Write fail")
					break oneloop
				}
				n.Checksum = n.Checksum.Update(data)
			}
			if err == io.EOF { // io.ErrClosedPipe
				break
			} else if err != nil {
				reterr = err
				break oneloop
			}
		}

		file.Seek(0, os.SEEK_SET)
		//n.Checksum = n.Checksum.Value()
		if err := binary.Write(file, binary.BigEndian, n); err != nil {
			reterr = err
			break oneloop
		}
	}

	file.Close()
	if reterr != nil {
		os.Remove(fpath)
		return nil, reterr
	}
	return &n, nil
}

func ReadFile(fpath string, f func(*Needle, io.Reader) error) error {
	file, err := os.Open(fpath)
	defer file.Close()
	if err != nil {
		return err
	}
	wholesize, err := util.GetFileSize(file)
	if err != nil {
		return err
	}

	var needle Needle
	err = binary.Read(file, binary.BigEndian, &needle)
	if err != nil {
		return err
	}
	if int64(needle.Size) != wholesize {
		return errors.New("Error needle file size")
	}
	return f(&needle, file)
}
