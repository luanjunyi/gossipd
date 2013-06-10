package mqtt

import (
	"net"
	"log"
)

// Handle CONNECT

func HandleConnect(mqtt *Mqtt, conn *net.Conn) {
	log.Println("Hanling CONNECT")
	mqtt.Show()

	resp := CreateMqtt(CONNACK)
	resp.ReturnCode = ACCEPTED

	bytes, _ := Encode(resp)
	(*conn).Write(bytes)
}

