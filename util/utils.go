package util

import (
	"strconv"
	"strings"
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
