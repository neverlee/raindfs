package main

import (
	"fmt"
	"github.com/neverlee/glog"
	"net"
)

func startServer() {
	listener, err := net.Listen("tcp", "localhost:7777")
	checkError(err)
	for {
		if conn, err := listener.Accept(); err == nil {
			go doServerStuff(conn)
		} else {
			glog.Infoln(err)
		}
	}
}

func doServerStuff(conn net.Conn) {
	header := make([]byte, 32)
	_, err := conn.Read(header)
	checkError(err)

	for {
		buf := make([]byte, 512)
		_, err := conn.Read(buf) //读取客户机发的消息
		flag := checkError(err)
		if flag == 0 {
			break
		}
		fmt.Println(string(buf)) //打印出来
	}
}

//检查错误
//func checkError(err error) int {
//    if err != nil {
//        if err.Error() == "EOF" {
//            //fmt.Println("用户退出了")
//            return 0
//        }
//        log.Fatal("an error!", err.Error())
//        return -1
//    }
//    return 1
//}
