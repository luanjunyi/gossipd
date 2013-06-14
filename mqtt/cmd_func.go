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
		SendConnack(IDENTIFIER_REJECTED, conn, nil)
		return
	}

	if mqtt.ProtocolName != "MQIsdp" || mqtt.ProtocolVersion != 3 {
		log.Printf("ProtocolName(%s) and/or version(%d) not supported, will send UNACCEPTABLE_PROTOCOL_VERSION\n",
			mqtt.ProtocolName, mqtt.ProtocolVersion)
		SendConnack(UNACCEPTABLE_PROTOCOL_VERSION, conn, nil)
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
	SendConnack(ACCEPTED, conn, client_rep.WriteLock)
	log.Printf("New client is all set and CONNACK is sent")
}

func SendConnack(rc uint8, conn *net.Conn, lock *sync.Mutex) {
	resp := CreateMqtt(CONNACK)
	resp.ReturnCode = rc

	bytes, _ := Encode(resp)
	MqttSendToClient(bytes, conn, lock)
}


/* Handle SUBSCRIBE */

func HandleSubscribe(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	client_id := (*client).mqtt.ClientId
	log.Printf("Handling SUBSCRIBE, client_id: %s\n", client_id)
	mqtt.Show()

	client_rep := G_clients[client_id]
	if client_rep == nil {
		log.Panicf("client_id(%d) not found in the list, will close connection\n", client_id)
		return
	}

	if client_rep != *client {
		log.Panicf("client_id(%d) has inconsistent ClientRep, will close current connecton\n", client_id)
		return
	}

	for i := 0; i < len(mqtt.Topics); i++ {
		topic := mqtt.Topics[i]
		qos := mqtt.Topics_qos[i]
		log.Printf("subscribing client(%s) to topic(%s) with qos=%d\n",
			client_id, topic, qos)

		subs := G_subs[topic]
		if subs == nil {
			log.Println("current subscription is the first client to topic ", topic)
			subs = make(map[string]uint8)
			G_subs[topic] = subs
		}

		// FIXME: this may override existing subscription with higher QOS
		subs[client_id] = qos
	}
	log.Println("Subscriptions are all processed, will send SUBACK")
	SendSuback(mqtt.MessageId, mqtt.Topics_qos, conn, client_rep.WriteLock)
}

func SendSuback(msg_id uint16, qos_list []uint8, conn *net.Conn, lock *sync.Mutex) {
	resp := CreateMqtt(SUBACK)
	resp.MessageId = msg_id
	resp.Topics_qos = qos_list

	bytes, _ := Encode(resp)
	MqttSendToClient(bytes, conn, lock)
}

/* Helper functions */

func MqttSendToClient(bytes []byte, conn *net.Conn, lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer func() {
			lock.Unlock()
		}()
	}

	(*conn).Write(bytes)
}
