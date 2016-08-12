package util

import (
	"encoding/binary"
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
