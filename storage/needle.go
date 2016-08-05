package storage

import (
	"fmt"
)

const (
	NeedleHeaderSize      = 16 //should never change this
	NeedleChecksumSize    = 4
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
	FlagBlockFile         = 0
	FlagIndexFile         = 1
)

/*
* A Needle means a uploaded and stored file.
* Needle file size is limited to 4GB for now.
 */
type Needle struct {
	Id   uint64 // `comment:"needle id"`
	Size uint32 // `comment:"sum of All"`

	Flags        byte
	LastModified uint64

	Checksum CRC    // `comment:"CRC32 to check integrity"`
	DataSize uint32 // `comment:"Data size"` //version2
}

func (n *Needle) String() (str string) {
	return fmt.Sprintf("Id:%d, Size:%d, DataSize:%d", n.Id, n.Size, n.DataSize)
}
