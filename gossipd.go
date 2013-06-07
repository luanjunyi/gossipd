package main

import (
	"flag"
	"log"
	"net"
	"io"
)

var g_debug = flag.Bool("d", false, "enable debug message")

type MqttFixedHeader struct {
    MessageType uint8
    DupFlag bool
	Retain bool
    QosLevel uint8
    Length uint32
}

func handleConnection(conn *net.Conn) {
	log.Println("Got new conection")
	for {
		var buf = make([]byte, 2)
		n, _ := io.ReadFull(*conn, buf)
		if n != len(buf) {
			log.Println("read header failed")
			continue
		}
		log.Println("header:", string(buf))
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("Gossipd kicking off")

	link, _ := net.Listen("tcp", ":2001")
	
	for {
		conn, err := link.Accept()
		if err != nil {
			continue
		}
		go handleConnection(&conn)
	}
	defer link.Close()
}