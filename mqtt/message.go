package mqtt

type Message struct {
	Topic string
	Payload string
	Qos uint8
	SenderClientId string
	MessageId uint16
	CreatedAt int64
}

var 