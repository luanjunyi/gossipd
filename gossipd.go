package main

import (
	"fmt"
	"flag"
	"log"
	"net"
	//"io"
	"github.com/luanjunyi/gossipd/mqtt"
)

var g_debug = flag.Bool("d", false, "enable debug message")
var g_port = flag.Int("p", 2001, "port of the broker to listen")

func handleConnection(conn *net.Conn) {
	log.Println("Got new conection")
	for {
		// Read fixed header
		header := mqtt.ParseHeader(conn)
		if (header == nil) {
			log.Println("reading header returned nil, will disconnect")
			// FIXME: add disconnect mechanics
			return;
		}
		header.Show()
		//log.Println("header:", string(header))
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("Gossipd kicking off, listening localhost:%d", *g_port)

	link, _ := net.Listen("tcp", fmt.Sprintf(":%d", *g_port))
	
	for {
		conn, err := link.Accept()
		if err != nil {
			continue
		}
		go handleConnection(&conn)
	}
	defer link.Close()
}