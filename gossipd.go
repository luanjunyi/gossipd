package main

import (
	"fmt"
	"flag"
	"log"
	"net"
	"time"
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
	mqtt.PINGREQ: mqtt.HandlePingreq,
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
	var conn_str string = fmt.Sprintf("%s:%s", string(remoteAddr.Network()), remoteAddr.String())
	log.Println("Got new conection", conn_str)
	for {
		// Read fixed header
        fixed_header, body := mqtt.ReadCompleteCommand(conn)
		if (fixed_header == nil) {
			log.Println(conn_str, "reading header returned nil, will disconnect")
			// FIXME: add full disconnect mechanics
			return
		}

		mqtt_parsed, err := mqtt.DecodeAfterFixedHeader(fixed_header, body)
		if (err != nil) {
			log.Println(conn_str, "read command body failed:", err.Error())
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

func CheckTimeout() {
	defer func() {
		mqtt.G_clients_lock.Unlock()
	}()
	for {
		log.Printf("Checking clients timeout")
		mqtt.G_clients_lock.Lock()
		now := time.Now().Unix()
		for client_id, client := range mqtt.G_clients {
			last := client.LastTime
			timeout := int64(client.Mqtt.KeepAliveTimer)
			deadline := int64(float64(last) + float64(timeout) * 1.5)
			if deadline < now {
				mqtt.DoDisconnect(client)
				log.Printf("clinet(%s) is timeout, kicked out",
					client_id)
			} else {
				log.Printf("client(%s) will be kicked out in %d seconds\n",
					client_id,
					now - deadline)
			}
		}
		mqtt.G_clients_lock.Unlock()

		// FIXME: use longer sleep time like 60 seconds
		time.Sleep(5 * time.Second)
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("Gossipd kicking off, listening localhost:%d", *g_port)

	link, _ := net.Listen("tcp", fmt.Sprintf(":%d", *g_port))

	go CheckTimeout()
	
	for {
		conn, err := link.Accept()
		if err != nil {
			continue
		}
		go handleConnection(&conn)
	}
	defer link.Close()
}