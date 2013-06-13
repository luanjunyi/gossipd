package mqtt

import (
	"net"
	"log"
	"sync"
)

// Handle CONNECT

func HandleConnect(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	mqtt.Show()
	client_id := mqtt.ClientId

	log.Println("Hanling CONNECT, client id:", client_id)

	if len(client_id) > 23 {
		log.Printf("client id(%s) is longer than 23, will send IDENTIFIER_REJECTED\n", client_id)
		SendConnack(IDENTIFIER_REJECTED, conn)
		return
	}

	if mqtt.ProtocolName != "MQIsdp" || mqtt.ProtocolVersion != 3 {
		log.Printf("ProtocolName(%s) and/or version(%d) not supported, will send UNACCEPTABLE_PROTOCOL_VERSION\n",
			mqtt.ProtocolName, mqtt.ProtocolVersion)
		SendConnack(UNACCEPTABLE_PROTOCOL_VERSION, conn)
		return
	}

	client_rep, existed := G_clients[client_id]
	if existed {
		log.Printf("%s existed, will close old connection", client_id)
		(*client_rep.Conn).Close()
		client_rep.Conn = conn
		// FIXME: Do we need extra steps for the locks?
		client_rep.WriteLock = new(sync.Mutex)
	} else {
		log.Printf("Appears to be new client, will create ClientRep")
		client_rep = CreateClientRep(client_id, conn, mqtt)
	}

	G_clients[client_id] = client_rep
	*client = client_rep
	SendConnack(ACCEPTED, conn)
	log.Printf("New client is all set and CONNACK is sent")
}

func SendConnack(rc uint8, conn *net.Conn) {
	resp := CreateMqtt(CONNACK)
	resp.ReturnCode = rc

	bytes, _ := Encode(resp)
	(*conn).Write(bytes)
}

// Handle SUBSCRIBE

func HandleSubscribe(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	log.Printf("Handling SUBSCRIBE, client_id: %s\n", (*client).mqtt.ClientId)
	mqtt.Show()
}


