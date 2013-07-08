package mqtt

import (
	"net"
	"log"
	"time"
	"sync"
	"fmt"
)

const (
	SEND_WILL = uint8(iota)
	DONT_SEND_WILL
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
		ForceDisconnect(client_rep, nil, DONT_SEND_WILL)

	} else {
		log.Printf("Appears to be new client, will create ClientRep")
	}

	client_rep = CreateClientRep(client_id, conn, mqtt)

	G_clients[client_id] = client_rep
	G_clients_lock.Unlock()

	*client = client_rep
	go CheckTimeout(client_rep)
	log.Println("Timeout checker go-routine started")

	if !client_rep.Mqtt.ConnectFlags.CleanSession {
		// deliver flying messages
		DeliverOnConnection(client_id)
		// restore subscriptions to client_rep
		subs := make(map[string]uint8)
		key := fmt.Sprintf("gossipd.client-subs.%s", client_id)
		G_redis_client.Fetch(key, &subs)
		client_rep.Subscriptions = subs

	} else {
		// Remove subscriptions and flying message
		RemoveAllSubscriptionsOnConnect(client_id)
		empty := make(map[uint16]FlyingMessage)
		G_redis_client.SetFlyingMessagesForClient(client_id, &empty)
	}

	SendConnack(ACCEPTED, conn, client_rep.WriteLock)
	log.Printf("New client is all set and CONNACK is sent")
}

func SendConnack(rc uint8, conn *net.Conn, lock *sync.Mutex) {
	resp := CreateMqtt(CONNACK)
	resp.ReturnCode = rc

	bytes, _ := Encode(resp)
	MqttSendToClient(bytes, conn, lock)
}

/* Handle PUBLISH*/
// FIXME: support qos = 2
func HandlePublish(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	if *client == nil {
		log.Panicf("client_resp is nil, that means we don't have ClientRep for this client sending PUBLISH")
		return
	}

	client_id := (*client).Mqtt.ClientId
	client_rep := *client
	client_rep.UpdateLastTime()
	topic := mqtt.TopicName
	payload := string(mqtt.Data)
	qos := mqtt.FixedHeader.QosLevel
	retain := mqtt.FixedHeader.Retain
	message_id := mqtt.MessageId
	timestamp := time.Now().Unix()
	log.Printf("Handling PUBLISH, client_id: %s, topic:(%s), payload:(%s), qos=%d, retain=%t, message_id=%d\n",
		client_id, topic, payload, qos, retain, message_id)

	// Create new MQTT message
	mqtt_msg := CreateMqttMessage(topic, payload, client_id, qos, message_id, timestamp, retain)
	msg_internal_id := mqtt_msg.InternalId
	log.Println("Created new MQTT message, internal id:", msg_internal_id)
	
	PublishMessage(mqtt_msg)

	// Send PUBACK if QOS is 1
	if qos == 1 {
		SendPuback(message_id, conn, client_rep.WriteLock)
		log.Printf("PUBACK sent to client(%s)", client_id)
	}
}

func SendPuback(msg_id uint16, conn *net.Conn, lock *sync.Mutex) {
	resp := CreateMqtt(PUBACK)
	resp.MessageId = msg_id
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
		log.Printf("will subscribe client(%s) to topic(%s) with qos=%d\n",
			client_id, topic, qos)

		subs := G_subs[topic]
		if subs == nil {
			log.Println("current subscription is the first client to topic:", topic)
			subs = make(map[string]uint8)
			G_subs[topic] = subs
		}

		// FIXME: this may override existing subscription with higher QOS
		subs[client_id] = qos
		client_rep.Subscriptions[topic] = qos

		if !client_rep.Mqtt.ConnectFlags.CleanSession {
			// Store subscriptions to redis
			key := fmt.Sprintf("gossipd.client-subs.%s", client_id)
			G_redis_client.Store(key, client_rep.Subscriptions)			
		}

		log.Printf("finding retained message for (%s)", topic)
		retained_msg := G_redis_client.GetRetainMessage(topic)
		if retained_msg != nil {
			go Deliver(client_id, qos, retained_msg)
			log.Printf("delivered retained message for (%s)", topic)
		}
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

		delete(client_rep.Subscriptions, topic)

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

/* Handle DISCONNECT */

func HandleDisconnect(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	if *client == nil {
		log.Panicf("client_resp is nil, that means we don't have ClientRep for this client sending DISCONNECT")
		return
	}

	ForceDisconnect(*client, G_clients_lock, DONT_SEND_WILL)
}

/* Handle PUBACK */
func HandlePuback(mqtt *Mqtt, conn *net.Conn, client **ClientRep) {
	if *client == nil {
		log.Panicf("client_resp is nil, that means we don't have ClientRep for this client sending DISCONNECT")
		return
	}

	client_id := (*client).Mqtt.ClientId
	message_id := mqtt.MessageId
	log.Printf("Handling PUBACK, client:(%s), message_id:(%d)", client_id, message_id)

	messages := G_redis_client.GetFlyingMessagesForClient(client_id)

	flying_msg, found := (*messages)[message_id]

	if !found || flying_msg.Status != PENDING_ACK {
		log.Printf("message(id=%d, client=%s) is not PENDING_ACK, will ignore this PUBACK",
			message_id, client_id)
	} else {
		delete(*messages, message_id)
		G_redis_client.SetFlyingMessagesForClient(client_id, messages)
		log.Printf("acked flying message(id=%d), client:(%s)", message_id, client_id)
	}	
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
				ForceDisconnect(client, G_clients_lock, SEND_WILL)
				log.Printf("client(%s) is timeout, kicked out",
					client_id)
			} else {
				log.Printf("client(%s) will be kicked out in %d seconds\n",
					client_id,
					deadline - now)
			}
		case <- client.Shuttingdown:
			log.Printf("client(%s) is being shutting down, stopped timeout checker", client_id)
			return
		}

	}
}

func ForceDisconnect(client *ClientRep, lock *sync.Mutex, send_will uint8) {
	if client.Disconnected == true {
		return
	}

	client.Disconnected = true

	client_id := client.Mqtt.ClientId

	log.Printf("Disconnecting client(%s), clean-session:%d",
		client_id, client.Mqtt.ConnectFlags.CleanSession)

	if lock != nil {
		lock.Lock()
		log.Println("lock accuired")
	}

	delete(G_clients, client_id)

	if client.Mqtt.ConnectFlags.CleanSession {
		// remove her subscriptions
		log.Printf("Removing subscriptions for (%s)", client_id)
		G_subs_lock.Lock()
		for topic, _ := range(client.Subscriptions) {
			delete(G_subs[topic], client_id)
			if len(G_subs[topic]) == 0 {
				delete(G_subs, topic)
				log.Printf("last subscription of topic(%s) is removed, so this topic is removed as well\n", topic)
			}
		}
		showSubscriptions()
		G_subs_lock.Unlock()
		log.Printf("Removed all subscriptions for (%s)", client_id)

		// remove her flying messages
		log.Printf("Removing all flying messages for (%s)", client_id)
		G_redis_client.RemoveAllFlyingMessagesForClient(client_id)
		log.Printf("Removed all flying messages for (%s)", client_id)
	}

	if lock != nil {
		lock.Unlock()
		log.Println("lock released")
	}


	// FIXME: Send will if requested
	if send_will == SEND_WILL && client.Mqtt.ConnectFlags.WillFlag {
		will_topic := client.Mqtt.WillTopic
		will_payload := client.Mqtt.WillMessage
		will_qos := client.Mqtt.ConnectFlags.WillQos
		will_retain := client.Mqtt.ConnectFlags.WillRetain

		mqtt_msg := CreateMqttMessage(will_topic, will_payload, client_id, will_qos,
			0, // message id won't be used here
			time.Now().Unix(), will_retain)
		PublishMessage(mqtt_msg)

		log.Printf("Sent will for %s, topic:(%s), payload:(%s)\n",
			client_id, will_topic, will_payload)
	}

	client.Shuttingdown <- 1
	log.Println("Sent 1 to shutdown channel")

	log.Printf("Closing socket of %s\n", client_id)
	(*client.Conn).Close()
}

func PublishMessage(mqtt_msg *MqttMessage) {
	topic := mqtt_msg.Topic
	payload := mqtt_msg.Payload
	log.Printf("Publishing job, topic(%s), payload(%s)", topic, payload)
	// Update global topic record

	if mqtt_msg.Retain {
		G_redis_client.SetRetainMessage(topic, mqtt_msg)
		log.Printf("Set the message(%s) as the current retain content of topic:%s\n", payload, topic)
	}

	// Dispatch delivering jobs
	G_subs_lock.Lock()
	subs, found := G_subs[topic]
	if found {
		for dest_id, dest_qos := range(subs) {
			go Deliver(dest_id, dest_qos, mqtt_msg)
			log.Printf("Started deliver job for %s", dest_id)
		}
	}
	G_subs_lock.Unlock()
	log.Println("All delivering job dispatched")
}

func DeliverOnConnection(client_id string) {
	log.Printf("client(%s) just reconnected, delivering on the fly messages", client_id)
	messages := G_redis_client.GetFlyingMessagesForClient(client_id)
	empty := make(map[uint16]FlyingMessage)
	G_redis_client.SetFlyingMessagesForClient(client_id, &empty)
	log.Printf("client(%s), all flying messages put in pipeline, removed records in redis", client_id)

	for message_id, msg := range(*messages) {
		internal_id := msg.MessageInternalId
		mqtt_msg := GetMqttMessageById(internal_id)
		log.Printf("re-delivering message(id=%d, internal_id=%d) for %s",
			message_id, internal_id, client_id)
		switch msg.Status {
		case PENDING_PUB:
			go Deliver(client_id, msg.Qos, mqtt_msg)
		case PENDING_ACK:
			go Deliver(client_id, msg.Qos, mqtt_msg)
		default:
			log.Panicf("can't re-deliver message at status(%d)", msg.Status)
		}
	}
}

func Deliver(dest_client_id string, dest_qos uint8, msg *MqttMessage) {
	log.Printf("Delivering msg(internal_id=%d) to client(%s)", msg.InternalId, dest_client_id)

	// Get effective qos: the smaller of the publisher and the subscriber
	qos := msg.Qos
	if dest_qos < msg.Qos {
		qos = dest_qos
	}

	G_clients_lock.Lock()
	client_rep, found := G_clients[dest_client_id]
	G_clients_lock.Unlock()
	var conn *net.Conn
	var lock *sync.Mutex
	message_id := NextOutMessageIdForClient(dest_client_id)
	fly_msg := CreateFlyingMessage(dest_client_id, msg.InternalId, qos, PENDING_PUB, message_id)

	if found {
		conn = client_rep.Conn
		lock = client_rep.WriteLock
	} else {
		G_redis_client.AddFlyingMessage(dest_client_id, fly_msg)
		log.Printf("client(%s) is offline, added flying message to Redis, message id=%d",
			dest_client_id, message_id)
		return
	}

	// FIXME: Add code to deal with failure
	resp := CreateMqtt(PUBLISH)
	resp.TopicName = msg.Topic
	if qos > 0 {
		resp.MessageId = message_id
	}
	resp.FixedHeader.QosLevel = qos
	resp.Data = []byte(msg.Payload)
	
	bytes, _ := Encode(resp)


	lock.Lock()
	defer func() {
		lock.Unlock()
	}()
	// FIXME: add write deatline
	(*conn).Write(bytes)
	log.Printf("message sent by Write()")

	if qos == 1 {
		fly_msg.Status = PENDING_ACK
		G_redis_client.AddFlyingMessage(dest_client_id, fly_msg)
		log.Printf("message(msg_id=%d) sent to client(%s), waiting for ACK, added to redis",
			message_id, dest_client_id)
	}
}

// On connection, if clean session is set, call this method
// to clear all connections. This is the senario when previous
// CONNECT didn't set clean session bit but current one does
func RemoveAllSubscriptionsOnConnect(client_id string) {
	subs := new(map[string]uint8)
	key := fmt.Sprintf("gossipd.client-subs.%s", client_id)
	G_redis_client.Fetch(key, subs)

	G_redis_client.Delete(key)

	G_subs_lock.Lock()
	for topic, _ := range(*subs) {
		delete(G_subs[topic], client_id)
	}
	G_subs_lock.Unlock()
	
}

func showSubscriptions() {
	// Disable for now
	return
	fmt.Printf("Global Subscriptions: %d topics\n", len(G_subs))
	for topic, subs := range(G_subs) {
		fmt.Printf("\t%s: %d subscriptions\n", topic, len(subs))
		for client_id, qos := range(subs) {
			fmt.Println("\t\t", client_id, qos)
		}
	}
}

