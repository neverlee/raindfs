package storage

import (
	"fmt"
)

const (
	NeedleHeaderSize      = 16 //should never change this
	NeedleChecksumSize    = 4
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
)

/*
* A Needle means a uploaded and stored file.
* Needle file size is limited to 4GB for now.
 */
type Needle struct {
	Id   uint64 // `comment:"needle id"`
	Size uint32 // `comment:"sum of All"`

	Flags        byte      // `comment:"boolean flags"`
	Name         [256]byte // `comment:"maximum 256 characters"`
	Extension    [8]byte   // `comment:"maximum 256 characters"`
	LastModified uint64

	Checksum CRC    // `comment:"CRC32 to check integrity"`
	DataSize uint32 // `comment:"Data size"` //version2
}

func (n *Needle) String() (str string) {
	return fmt.Sprintf("Id:%d, Size:%d, DataSize:%d, Name: %s, Extension: %s", n.Id, n.Size, n.DataSize, n.Name, n.Extension)
}
