package mqtt

import (
	"net"
	"log"
	"io"
)

// Handle CONNECT

func HandleConnect(header *FixedHeader, conn *net.Conn) {
	log.Println("Hanling CONNECT")
	length := header.Length
	buf := make([]byte, length)
	n, _ := io.ReadFull(*conn, buf)
	if uint32(n) != length {
		log.Panicf("failed to read %d bytes specified in fixed header, only %d read", length, n)
	}
	log.Println("Complete CONNECT read into buffer")

	info := parseConnectInfo(buf)
	info.Show()
}

