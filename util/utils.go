package util

import (
	"math/rand"
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


