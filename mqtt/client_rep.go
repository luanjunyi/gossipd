/* Client representation*/

package mqtt

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ClientRep struct {
	ClientId string
	Conn *net.Conn
	WriteLock *sync.Mutex
	LastTime int64 // Last Unix timestamp when recieved message from this client

	Mqtt *Mqtt
}

func (cr *ClientRep) UpdateLastTime() {
	atomic.StoreInt64(&cr.LastTime, time.Now().Unix())
}

func CreateClientRep(client_id string, conn *net.Conn, mqtt *Mqtt) *ClientRep {
	rep := new(ClientRep)
	rep.ClientId = client_id
	rep.Conn = conn
	rep.WriteLock = new(sync.Mutex)
	rep.Mqtt = mqtt
	rep.LastTime = time.Now().Unix()
	return rep
}