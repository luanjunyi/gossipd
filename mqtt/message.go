package mqtt

import (
	"sync"
	"sync/atomic"
)

// This is the type represents a message received from publisher.
// FlyingMessage(message should be delivered to specific subscribers)
// reference MqttMessage
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

	return msg
}

var g_next_mqtt_message_internal_id uint64 = 0
func GetNextMessageInternalId() uint64 {
	return atomic.AddUint64(&g_next_mqtt_message_internal_id, 1)
}

// This is thread-safe
func GetMqttMessageById(internal_id uint64) *MqttMessage{
	G_messages_lock.Lock()
	msg := G_messages[internal_id]
	G_messages_lock.Unlock()
	return msg
}

type FlyingMessage struct {
	
}