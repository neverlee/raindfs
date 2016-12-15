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

func StrInSlice(ay []string, a string) bool {
	for _, e := range ay {
		if e == a {
			return true
		}
	}
	return false
}
