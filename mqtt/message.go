package mqtt

import (
	"sync"
	"sync/atomic"
	"fmt"
)

/*
 This is the type represents a message received from publisher.
 FlyingMessage(message should be delivered to specific subscribers)
 reference MqttMessage
*/
type MqttMessage struct {
	Topic string
	Payload string
	Qos uint8
	SenderClientId string
	MessageId uint16
	InternalId uint64
	CreatedAt int64
	Retain bool
}

func (msg *MqttMessage) Show() {
	fmt.Printf("MQTT Message:\n")
	fmt.Println("Topic:", msg.Topic)
	fmt.Println("Payload:", msg.Payload)
	fmt.Println("Qos:", msg.Qos)
	fmt.Println("SenderClientId:", msg.SenderClientId)
	fmt.Println("MessageId:", msg.MessageId)
	fmt.Println("InternalId:", msg.InternalId)
	fmt.Println("CreatedAt:", msg.CreatedAt)
	fmt.Println("Retain:", msg.Retain)
}

func (msg *MqttMessage) RedisKey() string {
	return fmt.Sprintf("gossipd.mqtt-msg.%d", msg.InternalId)
}

func (msg *MqttMessage) Store() {
	key := msg.RedisKey()
	G_redis_client.Store(key, msg)
	G_redis_client.Expire(key, 7 * 24 * 3600)
}

// InternalId -> Message
// FIXME: Add code to store G_messages to disk
var G_messages map[uint64]*MqttMessage = make(map[uint64]*MqttMessage)
var G_messages_lock *sync.Mutex = new(sync.Mutex)

func CreateMqttMessage(topic, payload, sender_id string,
	qos uint8, message_id uint16,
	created_at int64, retain bool) *MqttMessage {

	msg := new(MqttMessage)
	msg.Topic = topic
	msg.Payload = payload
	msg.Qos = qos
	msg.SenderClientId = sender_id
	msg.MessageId = message_id
	msg.InternalId = GetNextMessageInternalId()
	msg.CreatedAt = created_at
	msg.Retain = retain

	G_messages_lock.Lock()
	G_messages[msg.InternalId] = msg
	G_messages_lock.Unlock()

	msg.Store()

	return msg
}

var g_next_mqtt_message_internal_id uint64 = 0
func GetNextMessageInternalId() uint64 {
	return atomic.AddUint64(&g_next_mqtt_message_internal_id, 1)
}

// This is thread-safe
func GetMqttMessageById(internal_id uint64) *MqttMessage{
	key := fmt.Sprintf("gossipd.mqtt-msg.%d", internal_id)
	
	msg := new(MqttMessage)
	G_redis_client.Fetch(key, msg)
	return msg
}


/* 
 This is the type represents a message should be delivered to
 specific client
*/
type FlyingMessage struct {
	Qos uint8 // the Qos in effect
	DestClientId string
	MessageInternalId uint64 // The MqttMessage of interest
	Status uint8  // The status of this message, like PENDING_PUB(deliver occured
	              // when client if offline), PENDING_ACK, etc
	ClientMessageId uint16 // The message id to be used in MQTT packet
}

const(
    PENDING_PUB = uint8(iota)
	PENDING_ACK
)

func CreateFlyingMessage(dest_id string, message_internal_id uint64,
	qos uint8, status uint8, message_id uint16) *FlyingMessage {
	msg := new(FlyingMessage)
	msg.Qos = qos
	msg.DestClientId = dest_id
	msg.MessageInternalId = message_internal_id
	msg.Status = status
	msg.ClientMessageId = message_id
	return msg
}

