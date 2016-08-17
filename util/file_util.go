package util

import (
	"bufio"
	"errors"
	"os"
	"path"
)

func TestFolderWritable(folder string) (err error) {
	fileInfo, err := os.Stat(folder)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return errors.New("Not a valid folder!")
	}
	perm := fileInfo.Mode().Perm()
	if 0200&perm != 0 {
		return nil
	}
	return errors.New("Not writable!")
}

func MkdirOrExist(path string) error {
	err := os.MkdirAll(path, 0755)
	if os.IsExist(err) {
		return TestFolderWritable(path)
	}
	return err
}

func Readln(r *bufio.Reader) ([]byte, error) {
	var (
		isPrefix = true
		err      error
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return ln, err
}

func GetFileSize(file *os.File) (size int64, err error) {
	var fi os.FileInfo
	if fi, err = file.Stat(); err == nil {
		size = fi.Size()
	}
	return
}

func Dirstat(dir string) (tsize int, tcount int) {
	size, count := 0, 0
	f, err := os.Open(dir)
	defer f.Close()
	if err != nil {
		return 0, 0
	}
	for {
		if list, err := f.Readdir(40); err == nil {
			for _, v := range list {
				if v.IsDir() {
					csize, ccount := Dirstat(path.Join(dir, v.Name()))
					size += csize
					count += ccount
				} else {
					size += int(v.Size())
					count++
				}
			}
		} else {
			break
		}
	}
	return size, count
}
