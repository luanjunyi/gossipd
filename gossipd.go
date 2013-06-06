package main

import (
	"flag"
	"log"
	"net"
)

var g_debug = flag.Bool("d", false, "enable debug message")

func handleConnection(conn *net.Conn) {
	log.Println("handling connection")
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