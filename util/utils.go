package util

import (
	"encoding/binary"
	"math/rand"
	"strconv"
	"strings"

	"github.com/satori/go.uuid"
)

func Ipport(ipport string) (string, int) {
	index := strings.LastIndex(ipport, ":")
	if index < 0 {
		return "", 0
	}
	ip := ipport[:index]
	port, _ := strconv.Atoi(ipport[index+1:])
	return ip, port
}

func GenID() uint64 {
	return binary.LittleEndian.Uint64(uuid.NewV4().Bytes())
}

func RandTwo(n int) []int {
	a := rand.Intn(n)
	b := rand.Intn(n - 1)
	if b >= a {
		b++
	}
	return []int{a, b}
}

func StrSplit(str string, seq string) []string {
	ay := strings.Split(str, seq)
	j := 0
	for i := 0; i < len(ay); i++ {
		if ay[i] != "" {
			ay[j] = ay[i]
			j++
		}
	}
	return ay[:j]
}

//func GenFileId() *FileId {
//	key := binary.LittleEndian.Uint64(uuid.NewV4().Bytes())
//	return NewFileId(v.Id, key)
//}
