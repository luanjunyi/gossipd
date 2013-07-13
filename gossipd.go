package main

import (
	"fmt"
	"flag"
	log "github.com/cihub/seelog"
	"net"
	"os"
	"runtime/debug"
	"github.com/luanjunyi/gossipd/mqtt"
)

type CmdFunc func(mqtt *mqtt.Mqtt, conn *net.Conn, client **mqtt.ClientRep)

var g_debug = flag.Bool("d", false, "enable debug message")
var g_port = flag.Int("p", 2001, "port of the broker to listen")
var g_redis_port = flag.Int("r", 6379, "port of the broker to listen")

var g_cmd_route = map[uint8]CmdFunc {
	mqtt.CONNECT: mqtt.HandleConnect,
	mqtt.PUBLISH: mqtt.HandlePublish,
	mqtt.SUBSCRIBE: mqtt.HandleSubscribe,
	mqtt.UNSUBSCRIBE: mqtt.HandleUnsubscribe,
	mqtt.PINGREQ: mqtt.HandlePingreq,
	mqtt.DISCONNECT: mqtt.HandleDisconnect,
	mqtt.PUBACK: mqtt.HandlePuback,
}

func handleConnection(conn *net.Conn) {
	remoteAddr := (*conn).RemoteAddr()
	var client *mqtt.ClientRep = nil

	defer func() {
		log.Debug("executing defered func in handleConnection")
		if r := recover(); r != nil {
			log.Debugf("got panic:(%s) will close connection from %s:%s", r, remoteAddr.Network(), remoteAddr.String())
			debug.PrintStack()
		}
		if client != nil {
			mqtt.ForceDisconnect(client, mqtt.G_clients_lock, mqtt.SEND_WILL)
		}
		(*conn).Close()
	}()

	var conn_str string = fmt.Sprintf("%s:%s", string(remoteAddr.Network()), remoteAddr.String())
	log.Debug("Got new conection", conn_str)
	for {
		// Read fixed header
        fixed_header, body := mqtt.ReadCompleteCommand(conn)
		if (fixed_header == nil) {
			log.Debug(conn_str, "reading header returned nil, will disconnect")
			return
		}

		mqtt_parsed, err := mqtt.DecodeAfterFixedHeader(fixed_header, body)
		if (err != nil) {
			log.Debug(conn_str, "read command body failed:", err.Error())
		}

		var client_id string
		if client == nil {
			client_id = ""
		} else {
			client_id = client.ClientId
		}
		log.Debugf("Got request: %s from %s", mqtt.MessageTypeStr(fixed_header.MessageType), client_id)
		proc, found := g_cmd_route[fixed_header.MessageType]
		if !found {
 			log.Debugf("Handler func not found for message type: %d(%s)",
				fixed_header.MessageType, mqtt.MessageTypeStr(fixed_header.MessageType))
			return
		}
		proc(mqtt_parsed, conn, &client)
	}
}

func setup_logging() {
	config := `
<seelog type="sync">
	<outputs formatid="main">
		<console/>
	</outputs>
	<formats>
		<format id="main" format="%Date %Time [%LEVEL] %File|%FuncShort|%Line: %Msg%n"/>
	</formats>
</seelog>`
	
	logger, err := log.LoggerFromConfigAsBytes([]byte(config))

	if err != nil {
		fmt.Println("Failed to config logging:", err)
		os.Exit(1)
	}

	log.ReplaceLogger(logger)

	log.Info("Logging config is successful")
}

func main() {
	flag.Parse()

	setup_logging()

	mqtt.RecoverFromRedis()

	log.Debugf("Gossipd kicking off, listening localhost:%d", *g_port)

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