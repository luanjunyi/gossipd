package main

import (
	"fmt"
	"flag"
	"log"
	"net"
	//"io"
	"github.com/luanjunyi/gossipd/mqtt"
)

type CmdFunc func(mqtt *mqtt.Mqtt, conn *net.Conn)

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
        fixed_header, body := mqtt.ReadCompleteCommand(conn)
		if (fixed_header == nil) {
			log.Println("reading header returned nil, will disconnect")
			// FIXME: add full disconnect mechanics

			return;
		}

		mqtt, err := mqtt.DecodeAfterFixedHeader(fixed_header, body)
		if (err != nil) {
			log.Println("read command body failed:", err.Error())
		}

		proc, found := g_cmd_route[fixed_header.MessageType]
		if !found {
			log.Panicf("Handler func not found for message type: %d", fixed_header.MessageType)
		}
		proc(mqtt, conn)
	}
	defer func() {
		if r := recover(); r != nil {
			log.Println("got panic:", r, "will close connection")
		}
		(*conn).Close()
	}()
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