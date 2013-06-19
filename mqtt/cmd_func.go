package mqtt

import (
	"net"
	"log"
	"time"
	"sync"
	"fmt"
)

// Handle CONNECT

func HandleConnect(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	//mqtt.Show()
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

	G_clients_lock.Lock()
	client_rep, existed := G_clients[client_id]
	if existed {
		log.Printf("%s existed, will close old connection", client_id)
		ForceDisconnect(client_rep, nil)

	} else {
		log.Printf("Appears to be new client, will create ClientRep")
	}

	client_rep = CreateClientRep(client_id, conn, mqtt)

	G_clients[client_id] = client_rep
	G_clients_lock.Unlock()

	*client = client_rep
	go CheckTimeout(client_rep)
	log.Println("Timeout checker go-routine started")

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
	if *client == nil {
		log.Panicf("client_resp is nil, that means we don't have ClientRep for this client sending SUBSCRIBE")
		return
	}

	client_id := (*client).Mqtt.ClientId
	log.Printf("Handling SUBSCRIBE, client_id: %s\n", client_id)
	client_rep := *client
	client_rep.UpdateLastTime()

	defer func() {
		G_subs_lock.Unlock()
		SendSuback(mqtt.MessageId, mqtt.Topics_qos, conn, client_rep.WriteLock)
	}()

	G_subs_lock.Lock()
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
	showSubscriptions()
}

func SendSuback(msg_id uint16, qos_list []uint8, conn *net.Conn, lock *sync.Mutex) {
	resp := CreateMqtt(SUBACK)
	resp.MessageId = msg_id
	resp.Topics_qos = qos_list

	bytes, _ := Encode(resp)
	MqttSendToClient(bytes, conn, lock)
}

/* Handle UNSUBSCRIBE */

func HandleUnsubscribe(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	if *client == nil {
		log.Panicf("client_resp is nil, that means we don't have ClientRep for this client sending UNSUBSCRIBE")
		return
	}

	client_id := (*client).Mqtt.ClientId
	log.Printf("Handling UNSUBSCRIBE, client_id: %s\n", client_id)
	client_rep := *client
	client_rep.UpdateLastTime()

	defer func() {
		G_subs_lock.Unlock()
		SendUnsuback(mqtt.MessageId, conn, client_rep.WriteLock)
	}()

	G_subs_lock.Lock()
	for i := 0; i < len(mqtt.Topics); i++ {
		topic := mqtt.Topics[i]

		log.Printf("unsubscribing client(%s) from topic(%s)\n",
			client_id, topic)

		subs := G_subs[topic]
		if subs == nil {
			log.Printf("topic(%s) has no subscription, no need to unsubscribe\n", topic)
		} else {
			delete(subs, client_id)
			if len(subs) == 0 {
				delete(G_subs, topic)
				log.Printf("last subscription of topic(%s) is removed, so this topic is removed as well\n", topic)
			}
		}
	}
	log.Println("unsubscriptions are all processed, will send UNSUBACK")

	showSubscriptions()
}

func SendUnsuback(msg_id uint16, conn *net.Conn, lock *sync.Mutex) {
	resp := CreateMqtt(UNSUBACK)
	resp.MessageId = msg_id
	bytes, _ := Encode(resp)
	MqttSendToClient(bytes, conn, lock)
}

/* Handle PINGREQ */

func HandlePingreq(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	if *client == nil {
		log.Panicf("client_resp is nil, that means we don't have ClientRep for this client sending PINGREQ")
		return
	}

	client_id := (*client).Mqtt.ClientId
	log.Printf("Handling PINGREQ, client_id: %s\n", client_id)
	client_rep := *client
	client_rep.UpdateLastTime()

	SendPingresp(conn, client_rep.WriteLock)
}

func SendPingresp(conn *net.Conn, lock *sync.Mutex) {
	resp := CreateMqtt(PINGRESP)
	bytes, _ := Encode(resp)
	MqttSendToClient(bytes, conn, lock)
}


/* Helper functions */

// This is the main place to change if we need to use channel rather than lock
func MqttSendToClient(bytes []byte, conn *net.Conn, lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer func() {
			lock.Unlock()
		}()
	}
	(*conn).Write(bytes)
}

/* Checking timeout */
func CheckTimeout(client *ClientRep) {
	interval := client.Mqtt.KeepAliveTimer
	client_id := client.ClientId
	ticker := time.NewTicker(time.Duration(interval) * time.Second)

	for {
		select {
		case <- ticker.C:
			now := time.Now().Unix()
			lastTimestamp := client.LastTime
			deadline := int64(float64(lastTimestamp) + float64(interval) * 1.5)

			if deadline < now {
				ForceDisconnect(client, G_clients_lock)
				log.Printf("clinet(%s) is timeout, kicked out",
					client_id)
			} else {
				log.Printf("client(%s) will be kicked out in %d seconds\n",
					client_id,
					deadline - now)
			}
		case <- client.Shuttingdown:
			log.Printf("client(%s) is being shutting down, stopped timeout checker")
			return
		}

	}
}

func ForceDisconnect(client *ClientRep, lock *sync.Mutex) {

	client_id := client.Mqtt.ClientId

	log.Println("Disconnecting client:", client_id)

	if lock != nil {
		lock.Lock()
		log.Println("lock accuired")
	}

	delete(G_clients, client_id)

	if lock != nil {
		lock.Unlock()
		log.Println("lock released")
	}

	// FIXME: add code to deal with session
	if client.Mqtt.ConnectFlags.CleanSession {

	} else {

	}

	client.Shuttingdown <- 1
	log.Println("Sent 1 to shutdown channel")

	log.Printf("Closing socket of %s\n", client_id)
	(*client.Conn).Close()
}

func showSubscriptions() {
	fmt.Printf("Global Subscriptions: %d topics\n", len(G_subs))
	for topic, subs := range(G_subs) {
		fmt.Printf("\t%s: %d subscriptions\n", topic, len(subs))
		for client_id, qos := range(subs) {
			fmt.Println("\t\t", client_id, qos)
		}
	}
}

