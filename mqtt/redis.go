package mqtt

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"bytes"
    "encoding/gob"
	"sync"
)

var g_redis_lock *sync.Mutex = new(sync.Mutex)

type RedisClient struct {
	conn *redis.Conn
}

func StartRedisClient() *RedisClient {
	conn, err := redis.Dial("tcp", ":6379")

	if err != nil {
		log.Panicf("Failed to connect to Redis at port 6379")
	} else {
		log.Println("Redis client started")
	}

	client := new(RedisClient)
	client.conn = &conn
	return client
}

func (client *RedisClient) Store(key string, value interface{}) {
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		log.Panic("gob encoding failed", err)
	}

	ret, err := (*client.conn).Do("SET", key, buf.Bytes())
	if err != nil {
		log.Panic("redis failed to set key(%s): %s", key, err)
	}
	log.Printf("stored to redis, key=%s, val(some bytes), returned=%s",
		key, ret)
}

func (client *RedisClient) Fetch(key string, value interface{}) {
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()

	str, err := redis.Bytes((*client.conn).Do("GET", key))
	if err != nil {
		log.Printf("redis failed to fetch key(%s): %s",
			key, err)
		return
	}
	buf := bytes.NewBuffer(str)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(value)

	if (err != nil) {
		log.Panic("gob decode failed:", err)
	}
}

func (client *RedisClient) Expire(key string, sec uint64) {
	g_redis_lock.Lock()
	defer g_redis_lock.Unlock()

	_, err := (*client.conn).Do("EXPIRE", key, sec)
	if err != nil {
		log.Panic("failed to expire key(%s)", key)
	}
}