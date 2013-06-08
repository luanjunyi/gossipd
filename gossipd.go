package main

import (
	"fmt"
	"flag"
	"log"
	"net"
	//"io"
	"github.com/luanjunyi/gossipd/mqtt"
)

type CmdFunc func(header *mqtt.FixedHeader, conn *net.Conn)

var g_debug = flag.Bool("d", false, "enable debug message")
var g_port = flag.Int("p", 2001, "port of the broker to listen")

var g_cmd_route = map[uint8]CmdFunc {
	mqtt.CONNECT: mqtt.HandleConnect,
}

func handleConnection(conn *net.Conn) {
	remoteAddr := (*conn).RemoteAddr()
	log.Println("Got new conection", remoteAddr.Network(), remoteAddr.String())
	for {
		// Read fixed header
		header := mqtt.ParseFixedHeader(conn)
		if (header == nil) {
			log.Println("reading header returned nil, will disconnect")
			// FIXME: add full disconnect mechanics

			return;
		}
		header.Show()

		proc, found := g_cmd_route[header.MessageType]
		if !found {
			log.Panicf("Handler func not found for message type: %d", header.MessageType)
		}
		proc(header, conn)
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