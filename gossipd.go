package main

import (
	"fmt"
	"flag"
	"log"
	"net"
	"runtime/debug"
	"github.com/luanjunyi/gossipd/mqtt"
)

type CmdFunc func(mqtt *mqtt.Mqtt, conn *net.Conn, client **mqtt.ClientRep)

var g_debug = flag.Bool("d", false, "enable debug message")
var g_port = flag.Int("p", 2001, "port of the broker to listen")

var g_cmd_route = map[uint8]CmdFunc {
	mqtt.CONNECT: mqtt.HandleConnect,
	mqtt.SUBSCRIBE: mqtt.HandleSubscribe,
	mqtt.UNSUBSCRIBE: mqtt.HandleUnsubscribe,
}

func handleConnection(conn *net.Conn) {

	remoteAddr := (*conn).RemoteAddr()

	defer func() {
		log.Println("executing defered func in handleConnection")
		if r := recover(); r != nil {
			log.Println("got panic:", r, "will close connection from", remoteAddr.Network(), remoteAddr.String())
			debug.PrintStack()
		}
		(*conn).Close()
	}()

	var client *mqtt.ClientRep = nil

	log.Println("Got new conection", remoteAddr.Network(), remoteAddr.String())
	for {
		// Read fixed header
        fixed_header, body := mqtt.ReadCompleteCommand(conn)
		if (fixed_header == nil) {
			log.Println("reading header returned nil, will disconnect")
			// FIXME: add full disconnect mechanics
			return
		}

		mqtt_parsed, err := mqtt.DecodeAfterFixedHeader(fixed_header, body)
		if (err != nil) {
			log.Println("read command body failed:", err.Error())
		}

		var client_id string
		if client == nil {
			client_id = ""
		} else {
			client_id = client.ClientId
		}
		log.Printf("Got request: %s from %s\n", mqtt.MessageTypeStr(fixed_header.MessageType), client_id)
		proc, found := g_cmd_route[fixed_header.MessageType]
		if !found {
 			log.Printf("Handler func not found for message type: %d(%s)\n",
				fixed_header.MessageType, mqtt.MessageTypeStr(fixed_header.MessageType))
			return
		}
		proc(mqtt_parsed, conn, &client)
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