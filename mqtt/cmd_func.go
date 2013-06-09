package mqtt

import (
	"net"
	"log"
)

// Handle CONNECT

func HandleConnect(mqtt *Mqtt, conn *net.Conn) {
	log.Println("Hanling CONNECT")
	mqtt.Show()
}

