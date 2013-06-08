package mqtt

import (
	"fmt"
	"net"
	"io"
	"log"
)

type FixedHeader struct {
    MessageType uint8
    DupFlag bool
	Retain bool
    QosLevel uint8
    Length uint32
}

const(
    CONNECT = uint8(iota + 1)
    CONNACK
    PUBLISH
    PUBACK
    PUBREC
    PUBREL
    PUBCOMP
    SUBSCRIBE
    SUBACK
    UNSUBSCRIBE
    UNSUBACK
    PINGREQ
    PINGRESP
    DISCONNECT
)

const(
    ACCEPTED = uint8(iota)
    UNACCEPTABLE_PROTOCOL_VERSION
    IDENTIFIER_REJECTED
    SERVER_UNAVAILABLE
    BAD_USERNAME_OR_PASSWORD
    NOT_AUTHORIZED
)

type ConnectFlags struct{
    UsernameFlag, PasswordFlag, WillRetain, WillFlag, CleanSession bool
    WillQos uint8
}
type Mqtt struct{
    Header *FixedHeader
    ProtocolName, TopicName, ClientId, WillTopic, WillMessage, Username, Password string
    ProtocolVersion uint8
    ConnectFlags *ConnectFlags
    KeepAliveTimer, MessageId uint16
    Data []byte
    Topics []string
    Topics_qos []uint8
    ReturnCode uint8
}



func ParseHeader(conn *net.Conn) *FixedHeader{
	var buf = make([]byte, 2)
	n, _ := io.ReadFull(*conn, buf)
	if n != len(buf) {
		log.Println("read header failed")
		return nil
	}


    byte1 := buf[0]
    header := new(FixedHeader)
    header.MessageType = uint8(byte1 & 0xF0 >> 4)
    header.DupFlag = byte1 & 0x08 > 0
    header.QosLevel = uint8(byte1 & 0x06 >> 1)
    header.Retain = byte1 & 0x01 > 0

	byte2 := buf[1]
    header.Length = decodeVarLength(byte2, conn)
    return header
}

func decodeVarLength(cur byte, conn *net.Conn) uint32{
	length := uint32(0)
	multi := uint32(1)

	for {
		length += multi * uint32(cur & 0x7f)
		if cur & 0x80 == 0 {
			break
		}
		buf := make([]byte, 1)
		n, _ := io.ReadFull(*conn, buf)
		if n != 1 {
			log.Panic("failed to read variable length in MQTT header")
		}
		cur = buf[0]
		multi *= 128
	}
	
	return length
}

func (header *FixedHeader)Show() {
	fmt.Println("header detail:")
	fmt.Println("message type: ", MessageTypeStr(header.MessageType))
	fmt.Println("DupFlag: ", header.DupFlag)
	fmt.Println("Retain: ", header.Retain)
	fmt.Println("QOS: ", header.QosLevel)
	fmt.Println("length: ", header.Length)
}

func MessageTypeStr(mt uint8) string {
	var strArray = []string {
		"reserved",
		"CONNECT",
		"CONNACK",
		"PUBLISH",
		"PUBACK",
		"PUBREC",
		"PUBREL",
		"PUBCOMP",
		"SUBSCRIBE",
		"SUBACK",
		"UNSUBSCRIBE",
		"UNSUBACK",
		"PINGREQ",
		"PINGRESP",
		"DISCONNEC"}
	return strArray[mt]
}

