/* Client representation*/

package mqtt

import (
	"net"
	"sync"
)

type ClientRep struct {
	ClientId string
	Conn *net.Conn
	WriteLock *sync.Mutex

	mqtt *Mqtt
}

func CreateClientRep(client_id string, conn *net.Conn, mqtt *Mqtt) *ClientRep {
	rep := new(ClientRep)
	rep.ClientId = client_id
	rep.Conn = conn
	rep.WriteLock = new(sync.Mutex)
	rep.mqtt = mqtt
	return rep
}