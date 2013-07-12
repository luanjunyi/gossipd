// FIXME: handle disconnection from redis

package mqtt

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"bytes"
    "encoding/gob"
	"sync"
	"fmt"
	"time"
)

var g_redis_lock *sync.Mutex = new(sync.Mutex)

type RedisClient struct {
	Conn *redis.Conn
}


func StartRedisClient() *RedisClient {
	conn, err := redis.Dial("tcp", ":6379")

	if err != nil {
		log.Panicf("Failed to connect to Redis at port 6379")
	} else {
		log.Println("Redis client started")
	}

	client := new(RedisClient)
	client.Conn = &conn

	go ping_pong_redis(client, 240)
	return client
}

func ping_pong_redis(client *RedisClient, interval int) {
	c := time.Tick(time.Duration(interval) * time.Second)
	for _ = range c {
		g_redis_lock.Lock()
		(*client.Conn).Do("PING")
		g_redis_lock.Unlock()
		log.Printf("sent PING to redis")
	}
}

func (client* RedisClient) Reconnect() {
	log.Printf("aqquiring g_redis_lock")

	conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		log.Panicf("Failed to connect to Redis at port 6379")
	} else {
		log.Println("Redis client reconncted")
	}
	client.Conn = &conn
}

func (client *RedisClient) Store(key string, value interface{}) {
	log.Printf("aqquiring g_redis_lock, store key=(%s)", key)
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()
	log.Printf("aqquired g_redis_lock, store key=(%s)", key)

	client.StoreNoLock(key, value)
}

func (client *RedisClient) StoreNoLock(key string, value interface{}) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		log.Panic("gob encoding failed", err)
	}

	ret, err := (*client.Conn).Do("SET", key, buf.Bytes())
	if err != nil {
		if err.Error() == "use of closed network connection" {
			client.Reconnect()
			client.StoreNoLock(key, value)
			return
		} else {
			log.Panicf("redis failed to set key(%s): %s", key, err)
		}
	}
	log.Printf("stored to redis, key=%s, val(some bytes), returned=%s",
		key, ret)
}

func (client *RedisClient) Fetch(key string, value interface{}) int {
	log.Printf("aqquiring g_redis_lock, fetch key=(%s)", key)
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()
	log.Printf("aqquired g_redis_lock, fetch key=(%s)", key)

	return client.FetchNoLock(key, value)
}

func (client *RedisClient) FetchNoLock(key string, value interface{}) int {
	str, err := redis.Bytes((*client.Conn).Do("GET", key))
	if err != nil {
		if err.Error() == "use of closed network connection" {
			client.Reconnect()
			return client.FetchNoLock(key, value)
		} else {
			log.Printf("redis failed to fetch key(%s): %s\n",
				key, err)
			return 1
		}
	}
	buf := bytes.NewBuffer(str)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(value)

	if (err != nil) {
		log.Panicf("gob decode failed, key=(%s), value=(%s), error:%s", key, str, err)
	}
	return 0
}

func (client *RedisClient) GetSubsClients() []string {
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()
	keys, _ := redis.Values((*client.Conn).Do("KEYS", "gossipd.client-subs.*"))
	clients := make([]string, 0)
	for _, key := range(keys) {
		clients = append(clients, string(key.([]byte)))
	}
	return clients
}

func (client *RedisClient) Delete(key string) {
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()
	(*client.Conn).Do("DEL", key)
}

func (client *RedisClient) GetRetainMessage(topic string) *MqttMessage {
	msg := new(MqttMessage)
	key := fmt.Sprintf("gossipd.topic-retained.%s", topic)
	var internal_id uint64
	ret := client.Fetch(key, &internal_id)
	if ret != 0 {
		log.Printf("retained message internal id not found in redis for topic(%s)", topic)
		return nil
	}

	key = fmt.Sprintf("gossipd.mqtt-msg.%d", internal_id)
	ret = client.Fetch(key, &msg)
	if ret != 0 {
		log.Printf("retained message, though internal id found, not found in redis for topic(%s)", topic)
		return nil
	}
	return msg
}

func (client *RedisClient) SetRetainMessage(topic string, msg *MqttMessage) {
	key := fmt.Sprintf("gossipd.topic-retained.%s", topic)
	internal_id := msg.InternalId
	client.Store(key, internal_id)
}

func (client *RedisClient) GetFlyingMessagesForClient(client_id string) *map[uint16]FlyingMessage {
	key := fmt.Sprintf("gossipd.client-msg.%s", client_id)
	messages := make(map[uint16]FlyingMessage)
	client.Fetch(key, &messages)
	return &messages
}

func (client *RedisClient) SetFlyingMessagesForClient(client_id string,
	messages *map[uint16]FlyingMessage) {
	key := fmt.Sprintf("gossipd.client-msg.%s", client_id)
	client.Store(key, messages)
}

func (client *RedisClient) RemoveAllFlyingMessagesForClient(client_id string) {
	key := fmt.Sprintf("gossipd.client-msg.%s", client_id)
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()
	(*client.Conn).Do("DEL", key)
}

func (client *RedisClient) AddFlyingMessage(dest_id string,
	                                        fly_msg *FlyingMessage) {

	messages := *client.GetFlyingMessagesForClient(dest_id)
	messages[fly_msg.ClientMessageId] = *fly_msg
	client.SetFlyingMessagesForClient(dest_id, &messages)
	log.Printf("Added flying message to redis client:(%s), message_id:(%d)",
		dest_id, fly_msg.ClientMessageId)
}

func (client *RedisClient) IsFlyingMessagePendingAck(client_id string, message_id uint16) bool {
	messages := G_redis_client.GetFlyingMessagesForClient(client_id)

	flying_msg, found := (*messages)[message_id]

	return found && flying_msg.Status == PENDING_ACK
}

func (client *RedisClient) Expire(key string, sec uint64) {
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()

	_, err := (*client.Conn).Do("EXPIRE", key, sec)
	if err != nil {
		log.Panic("failed to expire key(%s)", key)
	}
}