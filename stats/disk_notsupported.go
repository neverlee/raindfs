// +build windows openbsd netbsd plan9 solaris

package stats

func (disk *DiskStatus) fillInStatus() {
	disk.Free = 1024 * 1024 * 1024 // unlimited free space
	return
}
