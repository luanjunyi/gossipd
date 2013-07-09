package main

import (
	"github.com/garyburd/redigo/redis"
	"fmt"
	"encoding/gob"
	"bytes"
	"log"
)

type P struct {
    X, Y, Z int
    Name    string
}

func main() {
	fmt.Println("Testing Redis...")
	c, err := redis.Dial("tcp", "localhost:6379")
	
	if err != nil {
		fmt.Println("failed to connec")
	} else {
		fmt.Println("connected")
	}

	defer c.Close()

	v, err := redis.Int64(c.Do("GET", "target"))
	fmt.Println("val", v, err)
	if err != nil {
		fmt.Println(err)
	}

	ret, _ := redis.Values(c.Do("KEYS", "*"))
	for _, key := range(ret) {
		sk := string(key.([]byte))
		fmt.Println(sk)
	}

	p := P{50, 210, 46, "AdaBoost"}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(&p)
	if (err != nil) {
		log.Fatal("encode error:", err)
	}

	c.Do("SET", "gob", buf.Bytes())

	val, _ := redis.Bytes(c.Do("GET", "gob"))
	buf = *bytes.NewBuffer(val)
	var q P
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(&q)
	if (err != nil) {
		log.Fatal("decode error:", err)
	}

	fmt.Printf("P is: %q: {%d,%d,%d}\n", q.Name, q.X, q.Y, q.Z)
}